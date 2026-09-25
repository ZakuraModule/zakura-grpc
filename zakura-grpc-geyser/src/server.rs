use std::{
    collections::{HashSet, VecDeque},
    pin::Pin,
    sync::Arc,
};

use futures_core::Stream;
use parking_lot::Mutex;
use tokio::sync::{broadcast, mpsc};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, info, warn};
use zakura_grpc_proto::geyser::{
    geyser_server::{Geyser, GeyserServer},
    subscribe_update, EventType, GetVersionRequest, GetVersionResponse, PingRequest, PongResponse,
    PongUpdate, SubscribeReplayInfoRequest, SubscribeReplayInfoResponse, SubscribeRequest,
    SubscribeUpdate,
};

use crate::{
    auth::{SubscriptionTracker, TokenAuth},
    config::{Config, FilterLimits},
    event::block_height,
    filter::EventFilter,
};

type SubscribeResult = Result<SubscribeUpdate, Status>;

pub(crate) struct SharedState {
    live: broadcast::Sender<Arc<SubscribeUpdate>>,
    replay: Mutex<ReplayBuffer>,
    replay_capacity: usize,
    client_channel_capacity: usize,
}

impl SharedState {
    pub(crate) fn new(config: &Config) -> Self {
        let (live, _) = broadcast::channel(config.broadcast_capacity);
        Self {
            live,
            replay: Mutex::new(ReplayBuffer::new(config.replay_stored_blocks)),
            replay_capacity: config.replay_stored_blocks,
            client_channel_capacity: config.client_channel_capacity,
        }
    }

    pub(crate) fn publish(&self, update: SubscribeUpdate) {
        let update = Arc::new(update);
        if block_height(&update).is_some() {
            self.replay.lock().push(Arc::clone(&update));
        }
        let subscriber_count = self.live.send(update).unwrap_or(0);
        let subscriber_count = u32::try_from(subscriber_count).unwrap_or(u32::MAX);
        metrics::gauge!("plugin.grpc.subscribers").set(f64::from(subscriber_count));
    }

    fn subscribe(
        &self,
        from_height: Option<u32>,
    ) -> Result<(broadcast::Receiver<Arc<SubscribeUpdate>>, ReplaySnapshot), Status> {
        // Subscribe first so events racing with the snapshot remain in the live ring.
        // The sequence watermark removes the overlap without creating a gap.
        let receiver = self.live.subscribe();
        let snapshot = self.replay.lock().snapshot(from_height)?;
        Ok((receiver, snapshot))
    }

    fn replay_info(&self) -> SubscribeReplayInfoResponse {
        let replay = self.replay.lock();
        SubscribeReplayInfoResponse {
            first_available_height: replay.first_available_height(),
            latest_height: replay.latest_height(),
            retained_block_capacity: u32::try_from(self.replay_capacity).unwrap_or(u32::MAX),
        }
    }
}

struct ReplayBuffer {
    capacity: usize,
    heights: VecDeque<u32>,
    height_set: HashSet<u32>,
    updates: VecDeque<Arc<SubscribeUpdate>>,
}

impl ReplayBuffer {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            heights: VecDeque::new(),
            height_set: HashSet::new(),
            updates: VecDeque::new(),
        }
    }

    fn push(&mut self, update: Arc<SubscribeUpdate>) {
        if self.capacity == 0 {
            return;
        }
        let Some(height) = block_height(&update) else {
            return;
        };

        if self.height_set.insert(height) {
            self.heights.push_back(height);
        }
        self.updates.push_back(update);

        while self.heights.len() > self.capacity {
            let evicted_height = self
                .heights
                .pop_front()
                .expect("a height exists because the buffer exceeds its capacity");
            self.height_set.remove(&evicted_height);
            self.updates
                .retain(|stored| block_height(stored) != Some(evicted_height));
        }
    }

    fn snapshot(&self, from_height: Option<u32>) -> Result<ReplaySnapshot, Status> {
        if let (Some(requested), Some(first_available)) =
            (from_height, self.first_available_height())
        {
            if requested < first_available {
                return Err(Status::out_of_range(format!(
                    "events from height {requested} are not available; first available height is {first_available}"
                )));
            }
        }

        let watermark = self.updates.back().map_or(0, |update| update.sequence);
        let updates = from_height.map_or_else(Vec::new, |requested| {
            self.updates
                .iter()
                .filter(|update| block_height(update).is_some_and(|height| height >= requested))
                .cloned()
                .collect()
        });

        Ok(ReplaySnapshot { updates, watermark })
    }

    fn first_available_height(&self) -> Option<u32> {
        self.heights.front().copied()
    }

    fn latest_height(&self) -> Option<u32> {
        self.heights.back().copied()
    }
}

#[derive(Debug)]
struct ReplaySnapshot {
    updates: Vec<Arc<SubscribeUpdate>>,
    watermark: u64,
}

#[derive(Clone)]
pub(crate) struct GrpcService {
    state: Arc<SharedState>,
    auth: TokenAuth,
    subscriptions: SubscriptionTracker,
    filter_limits: Arc<FilterLimits>,
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

        metrics::counter!("plugin.grpc.connections.total").increment(1);
        tokio::spawn(async move {
            let _subscription_guard = subscription_guard;
            for update in replay.updates {
                if !send_filtered(&outbound_tx, &filter, &update).await {
                    return;
                }
            }

            let mut replay_watermark = replay.watermark;
            loop {
                tokio::select! {
                    request = inbound.message() => {
                        match request {
                            Ok(Some(request)) => {
                                if let Some(ping) = request.ping {
                                    let pong = SubscribeUpdate {
                                        event_type: EventType::Unspecified.into(),
                                        update: Some(subscribe_update::Update::Pong(PongUpdate { id: ping.id })),
                                        ..SubscribeUpdate::default()
                                    };
                                    if outbound_tx.send(Ok(pong)).await.is_err() {
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
                                if update.sequence <= replay_watermark {
                                    continue;
                                }
                                replay_watermark = update.sequence;
                                if !send_filtered(&outbound_tx, &filter, &update).await {
                                    break;
                                }
                            }
                            Err(broadcast::error::RecvError::Lagged(skipped)) => {
                                metrics::counter!("plugin.grpc.client_lagged.total").increment(1);
                                let _ = outbound_tx
                                    .send(Err(Status::resource_exhausted(format!(
                                        "subscriber lagged by {skipped} events; reconnect with from_height"
                                    ))))
                                    .await;
                                break;
                            }
                            Err(broadcast::error::RecvError::Closed) => break,
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
    update: &SubscribeUpdate,
) -> bool {
    let Some(names) = filter.matched_names(update) else {
        return true;
    };
    let mut update = update.clone();
    update.filters = names;
    let event_type = update.event_type.to_string();
    if outbound.send(Ok(update)).await.is_err() {
        return false;
    }
    metrics::counter!(
        "plugin.grpc.messages_sent.total",
        "event" => event_type
    )
    .increment(1);
    true
}

pub(crate) async fn mark_serving(reporter: &mut tonic_health::server::HealthReporter) {
    reporter.set_serving::<GeyserServer<GrpcService>>().await;
    info!("Zakura gRPC health service is serving");
}

#[cfg(test)]
mod tests {
    use super::*;
    use zakura_grpc_proto::geyser::BlockUpdate;

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

    #[test]
    fn replay_retains_distinct_block_heights() {
        let mut replay = ReplayBuffer::new(2);
        replay.push(Arc::new(block_update(10, 1)));
        replay.push(Arc::new(block_update(10, 2)));
        replay.push(Arc::new(block_update(11, 3)));
        replay.push(Arc::new(block_update(12, 4)));

        assert_eq!(replay.first_available_height(), Some(11));
        let snapshot = replay.snapshot(Some(11)).unwrap();
        assert_eq!(snapshot.updates.len(), 2);
        assert_eq!(snapshot.watermark, 4);
    }

    #[test]
    fn replay_rejects_evicted_height() {
        let mut replay = ReplayBuffer::new(1);
        replay.push(Arc::new(block_update(20, 1)));
        replay.push(Arc::new(block_update(21, 2)));

        let status = replay.snapshot(Some(20)).unwrap_err();
        assert_eq!(status.code(), tonic::Code::OutOfRange);
    }
}
