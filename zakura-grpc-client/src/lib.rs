//! Reusable Rust client for Zakura Geyser gRPC.

mod builder;
mod dedup;
mod reconnect;

use std::{
    pin::Pin,
    task::{Context, Poll},
};

use bytes::Bytes;
use futures_core::Stream;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{
    codec::{CompressionEncoding, Streaming},
    metadata::AsciiMetadataValue,
    service::{interceptor::InterceptedService, Interceptor},
    transport::{Channel, Endpoint},
    Request, Status,
};
use tonic_health::pb::{health_client::HealthClient, HealthCheckRequest, HealthCheckResponse};
use zakura_grpc_proto::geyser::{
    geyser_client::GeyserClient, EventType, GetVersionRequest, GetVersionResponse, PingRequest,
    PongResponse, SubscribeReplayInfoRequest, SubscribeReplayInfoResponse, SubscribeRequest,
    SubscribeRequestPing, SubscribeUpdate,
};

pub use builder::{ZakuraGrpcBuilder, ZakuraGrpcBuilderError, ZakuraGrpcBuilderResult};
pub use dedup::{DedupState, DEFAULT_HEIGHT_RETENTION};
pub use reconnect::{Backoff, ReconnectConfig, ReconnectionPolicy};
pub use tonic::transport::ClientTlsConfig;

const SUBSCRIPTION_REQUEST_CAPACITY: usize = 1_000;
const SUBSCRIPTION_RESPONSE_CAPACITY: usize = 1_000;

type InterceptedChannel = InterceptedService<Channel, MetadataInterceptor>;

#[derive(Clone, Debug)]
pub(crate) struct MetadataInterceptor {
    pub(crate) x_token: Option<AsciiMetadataValue>,
    pub(crate) subscription_id: Option<AsciiMetadataValue>,
}

impl Interceptor for MetadataInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        if let Some(value) = self.x_token.clone() {
            request.metadata_mut().insert("x-token", value);
        }
        if let Some(value) = self.subscription_id.clone() {
            request.metadata_mut().insert("x-subscription-id", value);
        }
        Ok(request)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct ClientOptions {
    pub(crate) send_compressed: Option<CompressionEncoding>,
    pub(crate) accept_compressed: Option<CompressionEncoding>,
    pub(crate) max_decoding_message_size: Option<usize>,
    pub(crate) max_encoding_message_size: Option<usize>,
}

pub(crate) fn geyser_client(
    channel: Channel,
    interceptor: MetadataInterceptor,
    options: &ClientOptions,
) -> GeyserClient<InterceptedChannel> {
    let mut client = GeyserClient::with_interceptor(channel, interceptor);
    if let Some(encoding) = options.send_compressed {
        client = client.send_compressed(encoding);
    }
    if let Some(encoding) = options.accept_compressed {
        client = client.accept_compressed(encoding);
    }
    if let Some(limit) = options.max_decoding_message_size {
        client = client.max_decoding_message_size(limit);
    }
    if let Some(limit) = options.max_encoding_message_size {
        client = client.max_encoding_message_size(limit);
    }
    client
}

/// Errors returned by an established Zakura gRPC client.
#[derive(Debug, thiserror::Error)]
pub enum ZakuraGrpcClientError {
    /// The remote gRPC service returned a status.
    #[error("gRPC status: {0}")]
    Status(#[from] Status),
    /// A transport connection could not be established.
    #[error("gRPC transport error: {0}")]
    Transport(#[from] tonic::transport::Error),
}

/// Result returned by an established Zakura gRPC client.
pub type ZakuraGrpcClientResult<T> = Result<T, ZakuraGrpcClientError>;

/// A Zakura gRPC client backed by a Tonic channel.
#[derive(Clone)]
pub struct ZakuraGrpcClient {
    pub(crate) geyser: GeyserClient<InterceptedChannel>,
    pub(crate) health: HealthClient<InterceptedChannel>,
    pub(crate) endpoint: Endpoint,
    pub(crate) interceptor: MetadataInterceptor,
    pub(crate) options: ClientOptions,
    pub(crate) reconnect: Option<ReconnectConfig>,
}

impl ZakuraGrpcClient {
    /// Creates a configurable client builder.
    pub fn build_from_shared(
        endpoint: impl Into<Bytes>,
    ) -> ZakuraGrpcBuilderResult<ZakuraGrpcBuilder> {
        ZakuraGrpcBuilder::from_shared(endpoint)
    }

    /// Creates a configurable client builder from a static endpoint.
    #[must_use]
    pub fn build_from_static(endpoint: &'static str) -> ZakuraGrpcBuilder {
        ZakuraGrpcBuilder::from_static(endpoint)
    }

    /// Connects with production-oriented default message limits.
    pub async fn connect(endpoint: impl Into<Bytes>) -> ZakuraGrpcBuilderResult<Self> {
        Self::build_from_shared(endpoint)?.connect().await
    }

    /// Checks standard gRPC health for the Zakura Geyser service.
    pub async fn health_check(&mut self) -> ZakuraGrpcClientResult<HealthCheckResponse> {
        Ok(self
            .health
            .check(HealthCheckRequest {
                service: "zakura.geyser.v1.Geyser".to_owned(),
            })
            .await?
            .into_inner())
    }

    /// Watches standard gRPC health for the Zakura Geyser service.
    pub async fn health_watch(&mut self) -> ZakuraGrpcClientResult<Streaming<HealthCheckResponse>> {
        Ok(self
            .health
            .watch(HealthCheckRequest {
                service: "zakura.geyser.v1.Geyser".to_owned(),
            })
            .await?
            .into_inner())
    }

    /// Opens a subscription to every event type.
    pub async fn subscribe(
        &mut self,
    ) -> ZakuraGrpcClientResult<(SubscriptionSink, SubscriptionStream)> {
        self.subscribe_with_request(SubscribeRequest::default())
            .await
    }

    /// Opens a bidirectional subscription and sends its initial filter.
    pub async fn subscribe_with_request(
        &mut self,
        initial: SubscribeRequest,
    ) -> ZakuraGrpcClientResult<(SubscriptionSink, SubscriptionStream)> {
        let (network_tx, network_rx) = mpsc::channel(SUBSCRIPTION_REQUEST_CAPACITY);
        network_tx
            .try_send(initial.clone())
            .expect("a newly created subscription request channel has capacity");
        let stream = self
            .geyser
            .subscribe(ReceiverStream::new(network_rx))
            .await?
            .into_inner();

        let (command_tx, command_rx) = mpsc::channel(SUBSCRIPTION_REQUEST_CAPACITY);
        let (output_tx, output_rx) = mpsc::channel(SUBSCRIPTION_RESPONSE_CAPACITY);
        reconnect::spawn_subscription(
            stream,
            network_tx,
            command_rx,
            output_tx,
            initial,
            self.endpoint.clone(),
            self.interceptor.clone(),
            self.options.clone(),
            self.reconnect.clone(),
        );

        Ok((
            SubscriptionSink { sender: command_tx },
            SubscriptionStream {
                inner: ReceiverStream::new(output_rx),
            },
        ))
    }

    /// Opens one subscription and discards the request sink.
    pub async fn subscribe_once(
        &mut self,
        request: SubscribeRequest,
    ) -> ZakuraGrpcClientResult<SubscriptionStream> {
        let (_sink, stream) = self.subscribe_with_request(request).await?;
        Ok(stream)
    }

    /// Returns the height range available in the process-local replay buffer.
    pub async fn subscribe_replay_info(
        &mut self,
    ) -> ZakuraGrpcClientResult<SubscribeReplayInfoResponse> {
        Ok(self
            .geyser
            .subscribe_replay_info(SubscribeReplayInfoRequest {})
            .await?
            .into_inner())
    }

    /// Checks unary request connectivity.
    pub async fn ping(&mut self, count: i32) -> ZakuraGrpcClientResult<PongResponse> {
        Ok(self.geyser.ping(PingRequest { count }).await?.into_inner())
    }

    /// Returns server and Zakura Geyser interface versions.
    pub async fn get_version(&mut self) -> ZakuraGrpcClientResult<GetVersionResponse> {
        Ok(self
            .geyser
            .get_version(GetVersionRequest {})
            .await?
            .into_inner())
    }
}

/// Sender half of a live bidirectional subscription.
#[derive(Debug, Clone)]
pub struct SubscriptionSink {
    sender: mpsc::Sender<SubscribeRequest>,
}

impl SubscriptionSink {
    /// Replaces the active legacy event filter without starting another replay.
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

/// Stream of subscription updates, including terminal gRPC status errors.
pub struct SubscriptionStream {
    inner: ReceiverStream<Result<SubscribeUpdate, Status>>,
}

impl SubscriptionStream {
    /// Receives the next update, matching Tonic's `Streaming::message` ergonomics.
    pub async fn message(&mut self) -> Result<Option<SubscribeUpdate>, Status> {
        match self.inner.as_mut().recv().await {
            Some(Ok(update)) => Ok(Some(update)),
            Some(Err(status)) => Err(status),
            None => Ok(None),
        }
    }
}

impl Stream for SubscriptionStream {
    type Item = Result<SubscribeUpdate, Status>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }
}
