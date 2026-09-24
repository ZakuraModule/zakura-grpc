//! Reusable Rust client for Zakura Geyser gRPC.

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{
    transport::{Channel, Endpoint},
    Response, Status, Streaming,
};
use zakura_grpc_proto::geyser::{
    geyser_client::GeyserClient, EventType, GetVersionRequest, GetVersionResponse, PingRequest,
    PongResponse, SubscribeReplayInfoRequest, SubscribeReplayInfoResponse, SubscribeRequest,
    SubscribeRequestPing, SubscribeUpdate,
};

const SUBSCRIPTION_REQUEST_CAPACITY: usize = 16;

/// A Zakura gRPC client backed by a Tonic channel.
#[derive(Debug, Clone)]
pub struct ZakuraGrpcClient {
    inner: GeyserClient<Channel>,
}

impl ZakuraGrpcClient {
    /// Connects to a Zakura gRPC endpoint.
    pub async fn connect<D>(destination: D) -> Result<Self, tonic::transport::Error>
    where
        D: TryInto<Endpoint>,
        D::Error: Into<tonic::codegen::StdError>,
    {
        let inner = GeyserClient::connect(destination).await?;
        Ok(Self { inner })
    }

    /// Opens a bidirectional subscription and sends its initial filter.
    pub async fn subscribe(
        &mut self,
        initial: SubscribeRequest,
    ) -> Result<(SubscriptionSink, Streaming<SubscribeUpdate>), Status> {
        let (sender, receiver) = mpsc::channel(SUBSCRIPTION_REQUEST_CAPACITY);
        sender
            .try_send(initial)
            .expect("a newly created subscription request channel has capacity");
        let stream = self
            .inner
            .subscribe(ReceiverStream::new(receiver))
            .await?
            .into_inner();
        Ok((SubscriptionSink { sender }, stream))
    }

    /// Returns the height range available in the process-local replay buffer.
    pub async fn subscribe_replay_info(
        &mut self,
    ) -> Result<Response<SubscribeReplayInfoResponse>, Status> {
        self.inner
            .subscribe_replay_info(SubscribeReplayInfoRequest {})
            .await
    }

    /// Checks unary request connectivity.
    pub async fn ping(&mut self, count: i32) -> Result<Response<PongResponse>, Status> {
        self.inner.ping(PingRequest { count }).await
    }

    /// Returns server and Zakura Geyser interface versions.
    pub async fn get_version(&mut self) -> Result<Response<GetVersionResponse>, Status> {
        self.inner.get_version(GetVersionRequest {}).await
    }
}

/// Sender half of a live bidirectional subscription.
#[derive(Debug, Clone)]
pub struct SubscriptionSink {
    sender: mpsc::Sender<SubscribeRequest>,
}

impl SubscriptionSink {
    /// Replaces the active event filter without starting another replay.
    pub async fn set_event_types(
        &self,
        event_types: impl IntoIterator<Item = EventType>,
    ) -> Result<(), mpsc::error::SendError<SubscribeRequest>> {
        self.send(SubscribeRequest {
            event_types: event_types.into_iter().map(Into::into).collect(),
            ..SubscribeRequest::default()
        })
        .await
    }

    /// Sends an application-level keepalive through the subscription stream.
    pub async fn ping(&self, id: i32) -> Result<(), mpsc::error::SendError<SubscribeRequest>> {
        self.send(SubscribeRequest {
            ping: Some(SubscribeRequestPing { id }),
            ..SubscribeRequest::default()
        })
        .await
    }

    /// Sends a raw subscription request.
    pub async fn send(
        &self,
        request: SubscribeRequest,
    ) -> Result<(), mpsc::error::SendError<SubscribeRequest>> {
        self.sender.send(request).await
    }
}
