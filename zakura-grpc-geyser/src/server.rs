use std::{
    collections::{HashMap, VecDeque},
    future::pending,
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant},
};

use bytes::Bytes;
use futures_core::Stream;
use parking_lot::Mutex;
use prost::Message;
use tokio::{
    sync::{broadcast, mpsc},
    time::MissedTickBehavior,
};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, info, warn};
use zakura_grpc_proto::geyser::{
    geyser_server::{Geyser, GeyserServer},
    subscribe_update, EventType, GetVersionRequest, GetVersionResponse, PingRequest, PingUpdate,
    PongResponse, PongUpdate, SubscribeReplayInfoRequest, SubscribeReplayInfoResponse,
    SubscribeRequest, SubscribeUpdate,
};

use crate::{
    auth::{SubscriptionTracker, TokenAuth},
    config::{Config, FilterLimits},
    event::block_height,
    filter::EventFilter,
};

type SubscribeResult = Result<SubscribeUpdate, Status>;

#[derive(Debug)]
struct PublishedUpdate {
    message: SubscribeUpdate,
    encoded_len: usize,
}

impl PublishedUpdate {
    fn new(message: SubscribeUpdate) -> Self {
        let encoded_len = message.encoded_len();
        Self {
            message,
            encoded_len,
        }
    }
}

pub(crate) struct SharedState {
    live: broadcast::Sender<Arc<PublishedUpdate>>,
    replay: Mutex<ReplayBuffer>,
    client_channel_capacity: usize,
}

impl SharedState {
    pub(crate) fn new(config: &Config) -> Self {
        let (live, _) = broadcast::channel(config.broadcast_capacity);
        Self {
            live,
            replay: Mutex::new(ReplayBuffer::new(ReplayLimits {
                height_capacity: config.replay_stored_blocks,
                event_capacity: config.replay_max_events,
                byte_capacity: config.replay_max_bytes,
                max_age: config.replay_max_age_seconds.map(Duration::from_secs),
            })),
            client_channel_capacity: config.client_channel_capacity,
        }
    }

    pub(crate) fn publish_batch(&self, updates: Vec<SubscribeUpdate>) {
        let updates: Vec<_> = updates
            .into_iter()
            .map(PublishedUpdate::new)
            .map(Arc::new)
            .collect();

        let replay_lock_started = Instant::now();
        self.replay.lock().push_batch(&updates);
        metrics::histogram!("plugin.grpc.replay.lock.duration")
            .record(replay_lock_started.elapsed().as_secs_f64());

        for update in updates {
            metrics::counter!(
                "plugin.grpc.messages.published.bytes.total",
                "event" => event_type_label(update.message.event_type),
            )
            .increment(u64::try_from(update.encoded_len).unwrap_or(u64::MAX));
            let _ = self.live.send(update);
        }
        let subscriber_count = self.live.receiver_count();
        let subscriber_count = u32::try_from(subscriber_count).unwrap_or(u32::MAX);
        metrics::gauge!("plugin.grpc.subscribers").set(f64::from(subscriber_count));
    }

    fn subscribe(
        &self,
        from_height: Option<u32>,
    ) -> Result<(broadcast::Receiver<Arc<PublishedUpdate>>, ReplaySnapshot), Status> {
        // Subscribe first so events racing with the snapshot remain in the live ring.
        // The sequence watermark removes the overlap without creating a gap.
        let receiver = self.live.subscribe();
        let replay_started = Instant::now();
        let snapshot = self.replay.lock().snapshot(from_height)?;
        metrics::histogram!("plugin.grpc.replay.snapshot.duration")
            .record(replay_started.elapsed().as_secs_f64());
        metrics::histogram!("plugin.grpc.replay.snapshot.events")
            .record(usize_metric_value(snapshot.updates.len()));
        Ok((receiver, snapshot))
    }

    fn replay_info(&self) -> SubscribeReplayInfoResponse {
        let mut replay = self.replay.lock();
        replay.evict(Instant::now());
        SubscribeReplayInfoResponse {
            first_available_height: replay.first_available_height(),
            latest_height: replay.latest_height(),
            retained_block_capacity: u32::try_from(replay.limits.height_capacity)
                .unwrap_or(u32::MAX),
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct ReplayLimits {
    height_capacity: usize,
    event_capacity: usize,
    byte_capacity: usize,
    max_age: Option<Duration>,
}

#[derive(Debug)]
struct ReplayBucket {
    height: u32,
    inserted_at: Instant,
    updates: Vec<Arc<PublishedUpdate>>,
    encoded_bytes: usize,
}

struct ReplayBuffer {
    limits: ReplayLimits,
    buckets: VecDeque<ReplayBucket>,
    height_counts: HashMap<u32, usize>,
    event_count: usize,
    encoded_bytes: usize,
}

impl ReplayBuffer {
    fn new(limits: ReplayLimits) -> Self {
        Self {
            limits,
            buckets: VecDeque::new(),
            height_counts: HashMap::new(),
            event_count: 0,
            encoded_bytes: 0,
        }
    }

    fn push_batch(&mut self, updates: &[Arc<PublishedUpdate>]) {
        self.push_batch_at(updates, Instant::now());
    }

    fn push_batch_at(&mut self, updates: &[Arc<PublishedUpdate>], now: Instant) {
        if self.limits.height_capacity == 0 {
            self.update_metrics();
            return;
        }
        let Some(height) = updates
            .iter()
            .find_map(|update| block_height(&update.message))
        else {
            self.evict(now);
            return;
        };
        let replay_updates: Vec<_> = updates
            .iter()
            .filter(|update| block_height(&update.message) == Some(height))
            .cloned()
            .collect();
        debug_assert_eq!(
            replay_updates.len(),
            updates
                .iter()
                .filter(|update| block_height(&update.message).is_some())
                .count(),
            "one source event must not contain updates from multiple block heights"
        );
        let encoded_bytes = replay_updates
            .iter()
            .map(|update| update.encoded_len)
            .sum::<usize>();

        *self.height_counts.entry(height).or_default() += 1;
        self.event_count = self.event_count.saturating_add(replay_updates.len());
        self.encoded_bytes = self.encoded_bytes.saturating_add(encoded_bytes);
        self.buckets.push_back(ReplayBucket {
            height,
            inserted_at: now,
            updates: replay_updates,
            encoded_bytes,
        });
        self.evict(now);
    }

    fn evict(&mut self, now: Instant) {
        loop {
            let reason = if self.buckets.front().is_some_and(|bucket| {
                self.limits.max_age.is_some_and(|max_age| {
                    now.saturating_duration_since(bucket.inserted_at) > max_age
                })
            }) {
                Some("age")
            } else if self.height_counts.len() > self.limits.height_capacity {
                Some("height")
            } else if self.event_count > self.limits.event_capacity {
                Some("events")
            } else if self.encoded_bytes > self.limits.byte_capacity {
                Some("bytes")
            } else {
                None
            };
            let Some(reason) = reason else {
                break;
            };
            self.evict_front(reason);
        }
        self.update_metrics();
    }

    fn evict_front(&mut self, reason: &'static str) {
        let Some(bucket) = self.buckets.pop_front() else {
            return;
        };
        self.event_count = self.event_count.saturating_sub(bucket.updates.len());
        self.encoded_bytes = self.encoded_bytes.saturating_sub(bucket.encoded_bytes);
        if let Some(count) = self.height_counts.get_mut(&bucket.height) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                self.height_counts.remove(&bucket.height);
            }
        }
        metrics::counter!("plugin.grpc.replay.evicted.buckets.total", "reason" => reason)
            .increment(1);
        metrics::counter!("plugin.grpc.replay.evicted.events.total", "reason" => reason)
            .increment(u64::try_from(bucket.updates.len()).unwrap_or(u64::MAX));
        metrics::counter!("plugin.grpc.replay.evicted.bytes.total", "reason" => reason)
            .increment(u64::try_from(bucket.encoded_bytes).unwrap_or(u64::MAX));
    }

    fn snapshot(&mut self, from_height: Option<u32>) -> Result<ReplaySnapshot, Status> {
        self.evict(Instant::now());
        if let (Some(requested), Some(first_available)) =
            (from_height, self.first_available_height())
        {
            if requested < first_available {
                return Err(Status::out_of_range(format!(
                    "events from height {requested} are not available; first available height is {first_available}"
                )));
            }
        }

        let watermark = self
            .buckets
            .back()
            .and_then(|bucket| bucket.updates.last())
            .map_or(0, |update| update.message.sequence);
        let updates = from_height.map_or_else(Vec::new, |requested| {
            self.buckets
                .iter()
                .filter(|bucket| bucket.height >= requested)
                .flat_map(|bucket| bucket.updates.iter().cloned())
                .collect()
        });

        Ok(ReplaySnapshot { updates, watermark })
    }

    fn first_available_height(&self) -> Option<u32> {
        self.height_counts.keys().copied().min()
    }

    fn latest_height(&self) -> Option<u32> {
        self.height_counts.keys().copied().max()
    }

    fn update_metrics(&self) {
        metrics::gauge!("plugin.grpc.replay.buckets").set(usize_metric_value(self.buckets.len()));
        metrics::gauge!("plugin.grpc.replay.heights")
            .set(usize_metric_value(self.height_counts.len()));
        metrics::gauge!("plugin.grpc.replay.events").set(usize_metric_value(self.event_count));
        metrics::gauge!("plugin.grpc.replay.bytes").set(usize_metric_value(self.encoded_bytes));
    }
}

#[derive(Debug)]
struct ReplaySnapshot {
    updates: Vec<Arc<PublishedUpdate>>,
    watermark: u64,
}

fn usize_metric_value(value: usize) -> f64 {
    f64::from(u32::try_from(value).unwrap_or(u32::MAX))
}

fn event_type_label(event_type: i32) -> &'static str {
    EventType::try_from(event_type)
        .unwrap_or(EventType::Unspecified)
        .as_str_name()
}

enum PingSend {
    Sent(i32),
    Skipped,
    Closed,
}

fn subscription_pong(id: i32) -> SubscribeUpdate {
    SubscribeUpdate {
        event_type: EventType::Unspecified.into(),
        update: Some(subscribe_update::Update::Pong(PongUpdate { id })),
        ..SubscribeUpdate::default()
    }
}

fn send_subscription_ping(outbound: &mpsc::Sender<SubscribeResult>, ping_id: i32) -> PingSend {
    let ping = SubscribeUpdate {
        event_type: EventType::Unspecified.into(),
        update: Some(subscribe_update::Update::Ping(PingUpdate { id: ping_id })),
        ..SubscribeUpdate::default()
    };
    match outbound.try_send(Ok(ping)) {
        Ok(()) => {
            metrics::counter!("plugin.grpc.subscription_pings.total").increment(1);
            PingSend::Sent(ping_id.checked_sub(1).unwrap_or(-1))
        }
        Err(mpsc::error::TrySendError::Full(_)) => {
            metrics::counter!(
                "plugin.grpc.subscription_pings.skipped.total",
                "reason" => "outbound_queue_full"
            )
            .increment(1);
            PingSend::Skipped
        }
        Err(mpsc::error::TrySendError::Closed(_)) => PingSend::Closed,
    }
}

#[derive(Clone)]
pub(crate) struct GrpcService {
    state: Arc<SharedState>,
    auth: TokenAuth,
    subscriptions: SubscriptionTracker,
    filter_limits: Arc<FilterLimits>,
    subscription_ping_interval: Option<Duration>,
}

impl GrpcService {
    pub(crate) fn new(state: Arc<SharedState>, config: &Config) -> Self {
        Self {
            state,
            auth: TokenAuth::new(config.x_token.clone()),
            subscriptions: SubscriptionTracker::new(
                config.subscription_limit,
                config.subscription_limit_enforce,
            ),
            filter_limits: Arc::new(config.filter_limits.clone()),
            subscription_ping_interval: config
                .subscription_ping_interval_seconds
                .map(Duration::from_secs),
        }
    }

    pub(crate) fn into_server(self, config: &Config) -> GeyserServer<Self> {
        let mut server = GeyserServer::new(self)
            .max_decoding_message_size(config.max_decoding_message_size)
            .max_encoding_message_size(config.max_encoding_message_size);
        for encoding in &config.compression.accept {
            server = server.accept_compressed((*encoding).into());
        }
        for encoding in &config.compression.send {
            server = server.send_compressed((*encoding).into());
        }
        server
    }
}

#[tonic::async_trait]
impl Geyser for GrpcService {
    type SubscribeStream = Pin<Box<dyn Stream<Item = SubscribeResult> + Send + Sync + 'static>>;

    async fn subscribe(
        &self,
        request: Request<Streaming<SubscribeRequest>>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        self.auth.authorize(&request)?;
        let subscription_guard = self.subscriptions.acquire(&request)?;
        let mut inbound = request.into_inner();
        let initial = inbound
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("the first subscribe request is required"))?;
        let mut filter = EventFilter::new(&initial, &self.filter_limits)?;
        let (mut live, replay) = self.state.subscribe(initial.from_height)?;
        let (outbound_tx, outbound_rx) = mpsc::channel(self.state.client_channel_capacity);
        let filter_limits = Arc::clone(&self.filter_limits);
        let subscription_ping_interval = self.subscription_ping_interval;

        metrics::counter!("plugin.grpc.connections.total").increment(1);
        tokio::spawn(async move {
            let _subscription_guard = subscription_guard;
            for update in replay.updates {
                if !send_filtered(&outbound_tx, &filter, &update, "replay").await {
                    return;
                }
            }

            let mut replay_watermark = replay.watermark;
            let mut ping_interval = subscription_ping_interval.map(tokio::time::interval);
            if let Some(interval) = ping_interval.as_mut() {
                interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
                interval.tick().await;
            }
            let mut ping_id = -1i32;
            loop {
                tokio::select! {
                    request = inbound.message() => {
                        match request {
                            Ok(Some(request)) => {
                                if let Some(ping) = request.ping {
                                    if outbound_tx.send(Ok(subscription_pong(ping.id))).await.is_err() {
                                        break;
                                    }
                                } else {
                                    match EventFilter::new(&request, &filter_limits) {
                                        Ok(updated_filter) => filter = updated_filter,
                                        Err(status) => {
                                            let _ = outbound_tx.send(Err(status)).await;
                                            break;
                                        }
                                    }
                                    if request.from_height.is_some() {
                                        warn!("ignoring from_height on a non-initial subscription update");
                                    }
                                }
                            }
                            Ok(None) => break,
                            Err(error) => {
                                debug!(?error, "gRPC subscriber request stream failed");
                                break;
                            }
                        }
                    }
                    update = live.recv() => {
                        match update {
                            Ok(update) => {
                                if update.message.sequence <= replay_watermark {
                                    continue;
                                }
                                replay_watermark = update.message.sequence;
                                if !send_filtered(&outbound_tx, &filter, &update, "live").await {
                                    break;
                                }
                            }
                            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                                metrics::counter!("plugin.grpc.client_lagged.total").increment(1);
                                metrics::counter!("plugin.grpc.client_lagged.events.total")
                                    .increment(skipped);
                                let _ = outbound_tx.try_send(Err(Status::resource_exhausted(format!(
                                        "subscriber lagged by {skipped} events; reconnect with from_height"
                                    ))));
                                break;
                            }
                            Err(broadcast::error::RecvError::Closed) => break,
                        }
                    }
                    () = async {
                        match ping_interval.as_mut() {
                            Some(interval) => {
                                interval.tick().await;
                            }
                            None => pending::<()>().await,
                        }
                    } => {
                        match send_subscription_ping(&outbound_tx, ping_id) {
                            PingSend::Sent(next_ping_id) => ping_id = next_ping_id,
                            PingSend::Skipped => {}
                            PingSend::Closed => break,
                        }
                    }
                }
            }
        });

        let stream: Self::SubscribeStream = Box::pin(ReceiverStream::new(outbound_rx));
        Ok(Response::new(stream))
    }

    async fn subscribe_replay_info(
        &self,
        request: Request<SubscribeReplayInfoRequest>,
    ) -> Result<Response<SubscribeReplayInfoResponse>, Status> {
        self.auth.authorize(&request)?;
        Ok(Response::new(self.state.replay_info()))
    }

    async fn ping(&self, request: Request<PingRequest>) -> Result<Response<PongResponse>, Status> {
        self.auth.authorize(&request)?;
        Ok(Response::new(PongResponse {
            count: request.into_inner().count,
        }))
    }

    async fn get_version(
        &self,
        request: Request<GetVersionRequest>,
    ) -> Result<Response<GetVersionResponse>, Status> {
        self.auth.authorize(&request)?;
        Ok(Response::new(GetVersionResponse {
            version: env!("CARGO_PKG_VERSION").to_owned(),
            interface_version: zakura_geyser_plugin_interface::GEYSER_INTERFACE_VERSION,
            event_schema_version: zakura_geyser_plugin_interface::EVENT_SCHEMA_VERSION,
        }))
    }
}

async fn send_filtered(
    outbound: &mpsc::Sender<SubscribeResult>,
    filter: &EventFilter,
    update: &PublishedUpdate,
    delivery: &'static str,
) -> bool {
    let filter_started = Instant::now();
    let filter_match = filter.matched(&update.message);
    metrics::histogram!(
        "plugin.grpc.filter.duration",
        "event" => event_type_label(update.message.event_type),
    )
    .record(filter_started.elapsed().as_secs_f64());
    let Some(filter_match) = filter_match else {
        return true;
    };

    let available = outbound.capacity();
    let maximum = outbound.max_capacity();
    let used = maximum.saturating_sub(available);
    let utilization = if maximum == 0 {
        1.0
    } else {
        usize_metric_value(used) / usize_metric_value(maximum)
    };
    metrics::histogram!("plugin.grpc.outbound.queue.utilization").record(utilization);

    let wait_started = Instant::now();
    let Ok(permit) = outbound.reserve().await else {
        return false;
    };
    metrics::histogram!("plugin.grpc.outbound.queue.wait.duration")
        .record(wait_started.elapsed().as_secs_f64());

    let mut message = update.message.clone();
    message.filters = filter_match.names;
    if let Some(subscribe_update::Update::Block(block)) = message.update.as_mut() {
        block.payload = filter_match.block_payload.into();
        if filter_match.block_payload == zakura_grpc_proto::geyser::BlockPayload::MetaOnly {
            block.block = Bytes::default();
        }
    }
    let encoded_len =
        if filter_match.block_payload == zakura_grpc_proto::geyser::BlockPayload::MetaOnly {
            message.encoded_len()
        } else {
            update.encoded_len
        };
    permit.send(Ok(message));
    metrics::counter!(
        "plugin.grpc.messages_sent.total",
        "event" => event_type_label(update.message.event_type),
        "delivery" => delivery,
    )
    .increment(1);
    metrics::counter!(
        "plugin.grpc.messages_sent.bytes.total",
        "event" => event_type_label(update.message.event_type),
        "delivery" => delivery,
    )
    .increment(u64::try_from(encoded_len).unwrap_or(u64::MAX));
    true
}

pub(crate) async fn mark_serving(reporter: &mut tonic_health::server::HealthReporter) {
    reporter.set_serving::<GeyserServer<GrpcService>>().await;
    info!("Zakura gRPC health service is serving");
}

#[cfg(test)]
mod tests {
    use super::*;
    use zakura_grpc_proto::geyser::{BlockPayload, BlockUpdate, SubscribeRequestFilter};

    fn replay_limits(max_heights: usize) -> ReplayLimits {
        ReplayLimits {
            height_capacity: max_heights,
            event_capacity: 100,
            byte_capacity: 1024 * 1024,
            max_age: None,
        }
    }

    fn block_update(height: u32, sequence: u64) -> SubscribeUpdate {
        SubscribeUpdate {
            sequence,
            event_type: EventType::BlockFinalized.into(),
            update: Some(subscribe_update::Update::Block(BlockUpdate {
                height,
                finalized: true,
                ..BlockUpdate::default()
            })),
            ..SubscribeUpdate::default()
        }
    }

    fn published_block(height: u32, sequence: u64) -> Arc<PublishedUpdate> {
        Arc::new(PublishedUpdate::new(block_update(height, sequence)))
    }

    #[test]
    fn replay_retains_distinct_block_heights() {
        let mut replay = ReplayBuffer::new(replay_limits(2));
        replay.push_batch(&[published_block(10, 1), published_block(10, 2)]);
        replay.push_batch(&[published_block(11, 3)]);
        replay.push_batch(&[published_block(12, 4)]);

        assert_eq!(replay.first_available_height(), Some(11));
        let snapshot = replay.snapshot(Some(11)).unwrap();
        assert_eq!(snapshot.updates.len(), 2);
        assert_eq!(snapshot.watermark, 4);
    }

    #[test]
    fn replay_rejects_evicted_height() {
        let mut replay = ReplayBuffer::new(replay_limits(1));
        replay.push_batch(&[published_block(20, 1)]);
        replay.push_batch(&[published_block(21, 2)]);

        let status = replay.snapshot(Some(20)).unwrap_err();
        assert_eq!(status.code(), tonic::Code::OutOfRange);
    }

    #[test]
    fn replay_evicts_whole_buckets_by_event_limit() {
        let mut limits = replay_limits(10);
        limits.event_capacity = 2;
        let mut replay = ReplayBuffer::new(limits);
        replay.push_batch(&[published_block(10, 1), published_block(10, 2)]);
        replay.push_batch(&[published_block(11, 3)]);

        let snapshot = replay.snapshot(Some(11)).unwrap();
        assert_eq!(snapshot.updates.len(), 1);
        assert_eq!(snapshot.updates[0].message.sequence, 3);
        assert_eq!(replay.event_count, 1);
    }

    #[test]
    fn replay_evicts_whole_buckets_by_byte_limit() {
        let first = published_block(10, 1);
        let second = published_block(11, 2);
        let mut limits = replay_limits(10);
        limits.byte_capacity = first.encoded_len.max(second.encoded_len);
        let mut replay = ReplayBuffer::new(limits);
        replay.push_batch(&[first]);
        replay.push_batch(&[second]);

        assert_eq!(replay.first_available_height(), Some(11));
        assert_eq!(replay.event_count, 1);
    }

    #[test]
    fn replay_evicts_expired_buckets() {
        let mut limits = replay_limits(10);
        limits.max_age = Some(Duration::from_secs(10));
        let mut replay = ReplayBuffer::new(limits);
        let inserted_at = Instant::now();
        replay.push_batch_at(&[published_block(10, 1)], inserted_at);

        replay.evict(inserted_at + Duration::from_secs(11));

        assert!(replay.buckets.is_empty());
        assert_eq!(replay.event_count, 0);
        assert_eq!(replay.encoded_bytes, 0);
    }

    #[test]
    fn replay_range_uses_height_values_not_arrival_order() {
        let mut replay = ReplayBuffer::new(replay_limits(10));
        replay.push_batch(&[published_block(200, 1)]);
        replay.push_batch(&[published_block(100, 2)]);

        assert_eq!(replay.first_available_height(), Some(100));
        assert_eq!(replay.latest_height(), Some(200));
    }

    #[tokio::test]
    async fn meta_only_filter_strips_raw_block_bytes() {
        let filter = EventFilter::new(
            &SubscribeRequest {
                filters: HashMap::from([(
                    "metadata".to_owned(),
                    SubscribeRequestFilter {
                        event_types: vec![EventType::BlockFinalized.into()],
                        block_payload: BlockPayload::MetaOnly.into(),
                        ..SubscribeRequestFilter::default()
                    },
                )]),
                ..SubscribeRequest::default()
            },
            &FilterLimits::default(),
        )
        .unwrap();
        let mut message = block_update(10, 1);
        let Some(subscribe_update::Update::Block(block)) = message.update.as_mut() else {
            panic!("test update is a block");
        };
        block.block = vec![1, 2, 3, 4].into();
        block.payload = BlockPayload::Full.into();
        let published = PublishedUpdate::new(message);
        let (outbound, mut received) = mpsc::channel(1);

        assert!(send_filtered(&outbound, &filter, &published, "test").await);
        let delivered = received.recv().await.unwrap().unwrap();
        assert_eq!(delivered.filters, vec!["metadata"]);
        let Some(subscribe_update::Update::Block(block)) = delivered.update else {
            panic!("delivered update is a block");
        };
        assert!(block.block.is_empty());
        assert_eq!(block.payload, BlockPayload::MetaOnly as i32);
    }
}
