use std::{collections::BTreeMap, time::Duration};

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{transport::Endpoint, Code, Status, Streaming};
use tracing::warn;
use zakura_grpc_proto::geyser::{
    best_chain_update, subscribe_update, BlockCommitment, CanonicalBlock, ResumeCheckpoint,
    SubscribeRequest, SubscribeRequestPing, SubscribeUpdate,
};

use crate::{
    dedup::update_height, geyser_client, ClientOptions, DedupState, MetadataInterceptor,
    ZakuraGrpcClientError, DEFAULT_HEIGHT_RETENTION, SUBSCRIPTION_REQUEST_CAPACITY,
};

/// Exponential retry schedule for one connection outage.
#[derive(Clone, Debug)]
pub struct Backoff {
    /// Delay before the first retry.
    pub initial_interval: Duration,
    /// Multiplier applied after each retry.
    pub multiplier: f64,
    /// Number of retries after the first reconnect attempt.
    pub max_retries: u32,
}

impl Backoff {
    /// Creates a retry schedule.
    #[must_use]
    pub const fn new(initial_interval: Duration, multiplier: f64, max_retries: u32) -> Self {
        Self {
            initial_interval,
            multiplier,
            max_retries,
        }
    }
}

impl Default for Backoff {
    fn default() -> Self {
        // Covers a normal graceful node restart while still terminating a dead
        // stream after a bounded amount of time (about 25 seconds).
        Self::new(Duration::from_millis(100), 2.0, 8)
    }
}

/// Whether reconnect should resume from a retained block checkpoint.
#[derive(Clone, Debug)]
pub enum ReconnectionPolicy {
    /// Reconnect at the live head and intentionally discard events from the outage.
    SkipMissedData,
    /// Replay from the latest checkpoint and suppress duplicates already delivered.
    RecoverMissedData {
        /// Number of distinct block heights retained by client-side deduplication.
        height_retention: usize,
    },
}

/// Action taken when the server can no longer replay the requested checkpoint.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplayGapPolicy {
    /// Surface `OUT_OF_RANGE` and stop instead of silently losing events.
    Fail,
    /// Explicitly discard the unavailable gap and resume at the live head.
    SkipToLive,
}

/// Automatic reconnect behavior for subscription streams.
#[derive(Clone, Debug)]
pub struct ReconnectConfig {
    /// Retry schedule applied independently to every outage.
    pub backoff: Backoff,
    /// Gap recovery policy.
    pub policy: ReconnectionPolicy,
    /// Heights replayed before the latest observed height to cover same-height ordering races.
    pub checkpoint_height_buffer: u32,
    /// Behavior when the requested replay checkpoint has been evicted.
    pub replay_gap_policy: ReplayGapPolicy,
}

impl Default for ReconnectConfig {
    fn default() -> Self {
        Self {
            backoff: Backoff::default(),
            policy: ReconnectionPolicy::RecoverMissedData {
                height_retention: DEFAULT_HEIGHT_RETENTION,
            },
            checkpoint_height_buffer: 2,
            replay_gap_policy: ReplayGapPolicy::Fail,
        }
    }
}

impl ReconnectConfig {
    /// Replaces the retry schedule.
    #[must_use]
    pub fn with_backoff(mut self, backoff: Backoff) -> Self {
        self.backoff = backoff;
        self
    }

    /// Selects gap recovery with the requested deduplication retention.
    #[must_use]
    pub fn with_height_retention(mut self, height_retention: usize) -> Self {
        self.policy = ReconnectionPolicy::RecoverMissedData { height_retention };
        self
    }

    /// Selects the behavior for an unavailable replay checkpoint.
    #[must_use]
    pub const fn with_replay_gap_policy(mut self, policy: ReplayGapPolicy) -> Self {
        self.replay_gap_policy = policy;
        self
    }
}

struct ActiveSubscription {
    requests: mpsc::Sender<SubscribeRequest>,
    updates: Streaming<SubscribeUpdate>,
}

enum Disconnect {
    Ended,
    Status(Status),
}

#[derive(Debug, Default)]
struct ChainResumeState {
    finalized: Option<(u32, String)>,
    partial: BTreeMap<(u32, String), CanonicalBlock>,
}

impl ChainResumeState {
    fn from_request(request: &SubscribeRequest) -> Self {
        let mut state = Self::default();
        if let Some(checkpoint) = request.resume.as_ref() {
            state.finalized = Some((
                checkpoint.finalized_height,
                checkpoint.finalized_block_hash.clone(),
            ));
            for block in &checkpoint.partial_blocks {
                state
                    .partial
                    .insert((block.height, block.hash.clone()), block.clone());
            }
        }
        state
    }

    fn observe(&mut self, update: &SubscribeUpdate) {
        match update.update.as_ref() {
            Some(subscribe_update::Update::Block(block)) if block.finalized => {
                self.observe_block(block.height, block.hash.clone(), true);
            }
            Some(subscribe_update::Update::Block(block)) => {
                self.observe_block(block.height, block.hash.clone(), false);
            }
            Some(subscribe_update::Update::BestChain(change)) => match change.change.as_ref() {
                Some(best_chain_update::Change::Grow(grow)) => {
                    self.insert_partial(CanonicalBlock {
                        height: change.height,
                        hash: change.hash.clone(),
                        previous_block_hash: grow.previous_block_hash.clone(),
                    });
                }
                Some(best_chain_update::Change::Reset(reset)) => {
                    for block in &reset.disconnected_blocks {
                        self.partial.remove(&(block.height, block.hash.clone()));
                    }
                    for block in &reset.connected_blocks {
                        self.insert_partial(block.clone());
                    }
                }
                None => {}
            },
            Some(subscribe_update::Update::Transaction(transaction)) => {
                self.observe_block(
                    transaction.height,
                    transaction.block_hash.clone(),
                    transaction.commitment == BlockCommitment::Finalized as i32,
                );
            }
            Some(subscribe_update::Update::Utxo(utxo)) => {
                self.observe_block(
                    utxo.height,
                    utxo.block_hash.clone(),
                    utxo.commitment == BlockCommitment::Finalized as i32,
                );
            }
            Some(subscribe_update::Update::Reconnect(reconnect)) => {
                for block in &reconnect.discarded_blocks {
                    self.partial.remove(&(block.height, block.hash.clone()));
                }
            }
            _ => {}
        }
    }

    fn insert_partial(&mut self, block: CanonicalBlock) {
        if self
            .finalized
            .as_ref()
            .is_none_or(|(height, _)| block.height > *height)
        {
            self.partial
                .insert((block.height, block.hash.clone()), block);
        }
    }

    fn observe_block(&mut self, height: u32, hash: String, finalized: bool) {
        if finalized {
            self.finalized = Some((height, hash));
            self.partial
                .retain(|(partial_height, _), _| *partial_height > height);
        } else {
            self.insert_partial(CanonicalBlock {
                height,
                hash,
                previous_block_hash: String::new(),
            });
        }
    }

    fn checkpoint(&self) -> Option<ResumeCheckpoint> {
        let (finalized_height, finalized_block_hash) = self.finalized.as_ref()?;
        Some(ResumeCheckpoint {
            finalized_height: *finalized_height,
            finalized_block_hash: finalized_block_hash.clone(),
            partial_blocks: self.partial.values().rev().cloned().collect(),
        })
    }

    fn clear(&mut self) {
        self.finalized = None;
        self.partial.clear();
    }
}

async fn handle_update(
    active: &mut ActiveSubscription,
    output: &mpsc::Sender<Result<SubscribeUpdate, Status>>,
    checkpoint: &mut Option<u32>,
    chain: &mut ChainResumeState,
    dedup: &mut Option<DedupState>,
    update: SubscribeUpdate,
) -> Result<Option<Disconnect>, ()> {
    match update.update.as_ref() {
        Some(subscribe_update::Update::Ping(ping)) => {
            let response = SubscribeRequest {
                ping: Some(SubscribeRequestPing { id: ping.id }),
                ..SubscribeRequest::default()
            };
            if active.requests.send(response).await.is_err() {
                Ok(Some(Disconnect::Status(Status::unavailable(
                    "subscription request stream closed while replying to server ping",
                ))))
            } else {
                Ok(None)
            }
        }
        Some(subscribe_update::Update::Pong(pong)) if pong.id < 0 => Ok(None),
        _ => {
            chain.observe(&update);
            if let Some(height) = update_height(&update) {
                *checkpoint = Some(height);
            }
            if dedup.as_mut().is_none_or(|state| state.observe(&update))
                && output.send(Ok(update)).await.is_err()
            {
                Err(())
            } else {
                Ok(None)
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn_subscription(
    updates: Streaming<SubscribeUpdate>,
    requests: mpsc::Sender<SubscribeRequest>,
    commands: mpsc::Receiver<SubscribeRequest>,
    output: mpsc::Sender<Result<SubscribeUpdate, Status>>,
    initial: SubscribeRequest,
    endpoint: Endpoint,
    interceptor: MetadataInterceptor,
    options: ClientOptions,
    reconnect: Option<ReconnectConfig>,
) {
    tokio::spawn(run_subscription(
        ActiveSubscription { requests, updates },
        commands,
        output,
        initial,
        endpoint,
        interceptor,
        options,
        reconnect,
    ));
}

#[allow(clippy::too_many_arguments)]
async fn run_subscription(
    mut active: ActiveSubscription,
    mut commands: mpsc::Receiver<SubscribeRequest>,
    output: mpsc::Sender<Result<SubscribeUpdate, Status>>,
    mut current_request: SubscribeRequest,
    endpoint: Endpoint,
    interceptor: MetadataInterceptor,
    options: ClientOptions,
    reconnect: Option<ReconnectConfig>,
) {
    let mut commands_open = true;
    let mut checkpoint = current_request.from_height;
    let mut chain = ChainResumeState::from_request(&current_request);
    let mut dedup = reconnect.as_ref().and_then(|config| match config.policy {
        ReconnectionPolicy::SkipMissedData => None,
        ReconnectionPolicy::RecoverMissedData { height_retention } => {
            Some(DedupState::with_height_retention(height_retention))
        }
    });

    loop {
        let disconnect = tokio::select! {
            command = commands.recv(), if commands_open => if let Some(mut request) = command {
                if request.ping.is_none() {
                    request.from_height = None;
                    request.resume = None;
                    request.include_mempool_snapshot = current_request.include_mempool_snapshot;
                    current_request = request.clone();
                }
                if active.requests.send(request).await.is_err() {
                    Some(Disconnect::Status(Status::unavailable(
                        "subscription request stream closed",
                    )))
                } else {
                    None
                }
            } else {
                commands_open = false;
                None
            },
            message = active.updates.message() => match message {
                Ok(Some(update)) => match handle_update(
                    &mut active,
                    &output,
                    &mut checkpoint,
                    &mut chain,
                    &mut dedup,
                    update,
                ).await {
                    Ok(disconnect) => disconnect,
                    Err(()) => break,
                },
                Ok(None) => Some(Disconnect::Ended),
                Err(status) => Some(Disconnect::Status(status)),
            },
        };

        let Some(disconnect) = disconnect else {
            continue;
        };
        let Some(config) = reconnect.as_ref() else {
            if let Disconnect::Status(status) = disconnect {
                let _ = output.send(Err(status)).await;
            }
            break;
        };

        if let Disconnect::Status(status) = &disconnect {
            if !is_recoverable_status(status.code()) {
                let _ = output.send(Err(status.clone())).await;
                break;
            }
            if status.code() == Code::OutOfRange {
                match config.replay_gap_policy {
                    ReplayGapPolicy::Fail => {
                        let _ = output.send(Err(status.clone())).await;
                        break;
                    }
                    ReplayGapPolicy::SkipToLive => {
                        checkpoint = None;
                        chain.clear();
                        current_request.from_height = None;
                        current_request.resume = None;
                    }
                }
            }
        }

        match reconnect_subscription(
            &endpoint,
            &interceptor,
            &options,
            &mut current_request,
            &mut checkpoint,
            &mut chain,
            config,
        )
        .await
        {
            Ok(subscription) => active = subscription,
            Err(error) => {
                let status = match error {
                    ZakuraGrpcClientError::Status(status) => status,
                    ZakuraGrpcClientError::Transport(error) => {
                        Status::unavailable(format!("subscription reconnect failed: {error}"))
                    }
                };
                let _ = output.send(Err(status)).await;
                break;
            }
        }
    }
}

async fn reconnect_subscription(
    endpoint: &Endpoint,
    interceptor: &MetadataInterceptor,
    options: &ClientOptions,
    current_request: &mut SubscribeRequest,
    checkpoint: &mut Option<u32>,
    chain: &mut ChainResumeState,
    config: &ReconnectConfig,
) -> Result<ActiveSubscription, ZakuraGrpcClientError> {
    let mut reconnect_request = current_request.clone();
    match config.policy {
        ReconnectionPolicy::SkipMissedData => {
            reconnect_request.from_height = None;
            reconnect_request.resume = None;
        }
        ReconnectionPolicy::RecoverMissedData { .. } => {
            reconnect_request.resume = chain.checkpoint();
            reconnect_request.from_height = reconnect_request
                .resume
                .as_ref()
                .map(|resume| resume.finalized_height)
                .or_else(|| {
                    checkpoint
                        .map(|height| height.saturating_sub(config.checkpoint_height_buffer))
                        .or(current_request.from_height)
                });
        }
    }

    warn!(
        from_height = reconnect_request.from_height,
        "Zakura gRPC subscription disconnected; reconnecting"
    );
    let result = connect_with_backoff(
        endpoint,
        interceptor,
        options,
        reconnect_request.clone(),
        &config.backoff,
    )
    .await;
    if !matches!(&result, Err(error) if is_out_of_range_error(error))
        || config.replay_gap_policy == ReplayGapPolicy::Fail
    {
        return result;
    }

    *checkpoint = None;
    chain.clear();
    current_request.from_height = None;
    current_request.resume = None;
    reconnect_request.from_height = None;
    connect_with_backoff(
        endpoint,
        interceptor,
        options,
        reconnect_request,
        &config.backoff,
    )
    .await
}

async fn connect_with_backoff(
    endpoint: &Endpoint,
    interceptor: &MetadataInterceptor,
    options: &ClientOptions,
    request: SubscribeRequest,
    backoff: &Backoff,
) -> Result<ActiveSubscription, ZakuraGrpcClientError> {
    let mut delay = backoff.initial_interval;
    let mut last_error = None;
    for attempt in 0..=backoff.max_retries {
        if attempt > 0 {
            tokio::time::sleep(delay).await;
            delay = delay.mul_f64(backoff.multiplier);
        }

        match connect_once(endpoint, interceptor, options, request.clone()).await {
            Ok(subscription) => return Ok(subscription),
            Err(error) => {
                let retryable = is_recoverable_client_error(&error);
                last_error = Some(error);
                if !retryable {
                    break;
                }
            }
        }
    }
    Err(last_error.expect("at least one reconnect attempt is always made"))
}

async fn connect_once(
    endpoint: &Endpoint,
    interceptor: &MetadataInterceptor,
    options: &ClientOptions,
    request: SubscribeRequest,
) -> Result<ActiveSubscription, ZakuraGrpcClientError> {
    let channel = endpoint.connect().await?;
    let mut client = geyser_client(channel, interceptor.clone(), options);
    let (requests, receiver) = mpsc::channel(SUBSCRIPTION_REQUEST_CAPACITY);
    requests
        .try_send(request)
        .expect("a newly created subscription request channel has capacity");
    let updates = client
        .subscribe(ReceiverStream::new(receiver))
        .await?
        .into_inner();
    Ok(ActiveSubscription { requests, updates })
}

fn is_recoverable_client_error(error: &ZakuraGrpcClientError) -> bool {
    match error {
        ZakuraGrpcClientError::Transport(_) => true,
        ZakuraGrpcClientError::Status(status) => {
            status.code() != Code::OutOfRange && is_recoverable_status(status.code())
        }
    }
}

fn is_out_of_range_error(error: &ZakuraGrpcClientError) -> bool {
    matches!(error, ZakuraGrpcClientError::Status(status) if status.code() == Code::OutOfRange)
}

const fn is_recoverable_status(code: Code) -> bool {
    matches!(
        code,
        Code::Cancelled
            | Code::Unknown
            | Code::DeadlineExceeded
            | Code::ResourceExhausted
            | Code::Aborted
            | Code::Internal
            | Code::Unavailable
            | Code::DataLoss
            | Code::OutOfRange
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use zakura_grpc_proto::geyser::{BlockUpdate, ReconnectUpdate};

    #[test]
    fn retryable_codes_match_stream_failures() {
        assert!(is_recoverable_status(Code::Unavailable));
        assert!(is_recoverable_status(Code::OutOfRange));
        assert!(!is_recoverable_status(Code::Unauthenticated));
        assert!(!is_recoverable_status(Code::InvalidArgument));
    }

    #[test]
    fn default_policy_recovers_missed_data() {
        let config = ReconnectConfig::default();
        assert!(matches!(
            config.policy,
            ReconnectionPolicy::RecoverMissedData { .. }
        ));
        assert_eq!(config.replay_gap_policy, ReplayGapPolicy::Fail);
    }

    #[test]
    fn chain_resume_anchors_at_finalized_hash_and_discards_partial_blocks() {
        let mut chain = ChainResumeState::default();
        chain.observe(&SubscribeUpdate {
            update: Some(subscribe_update::Update::Block(BlockUpdate {
                height: 100,
                hash: "final".to_owned(),
                finalized: true,
                ..BlockUpdate::default()
            })),
            ..SubscribeUpdate::default()
        });
        chain.observe(&SubscribeUpdate {
            update: Some(subscribe_update::Update::Block(BlockUpdate {
                height: 101,
                hash: "partial".to_owned(),
                ..BlockUpdate::default()
            })),
            ..SubscribeUpdate::default()
        });

        let checkpoint = chain.checkpoint().unwrap();
        assert_eq!(checkpoint.finalized_height, 100);
        assert_eq!(checkpoint.finalized_block_hash, "final");
        assert_eq!(checkpoint.partial_blocks.len(), 1);

        chain.observe(&SubscribeUpdate {
            update: Some(subscribe_update::Update::Reconnect(ReconnectUpdate {
                finalized_height: 100,
                finalized_block_hash: "final".to_owned(),
                discarded_blocks: checkpoint.partial_blocks,
            })),
            ..SubscribeUpdate::default()
        });
        assert!(chain.partial.is_empty());
    }
}
