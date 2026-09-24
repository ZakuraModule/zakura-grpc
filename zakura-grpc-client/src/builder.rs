use std::time::Duration;

use bytes::Bytes;
use tonic::{
    codec::CompressionEncoding,
    metadata::{errors::InvalidMetadataValue, AsciiMetadataValue},
    transport::{Channel, ClientTlsConfig, Endpoint},
};
use tonic_health::pb::health_client::HealthClient;

use crate::{geyser_client, ClientOptions, MetadataInterceptor, ReconnectConfig, ZakuraGrpcClient};

/// Error while configuring or connecting a Zakura gRPC client.
#[derive(Debug, thiserror::Error)]
pub enum ZakuraGrpcBuilderError {
    /// Metadata such as `x-token` was not valid ASCII metadata.
    #[error("invalid gRPC metadata value: {0}")]
    Metadata(#[from] InvalidMetadataValue),
    /// The channel endpoint or transport could not be configured.
    #[error("gRPC transport error: {0}")]
    Transport(#[from] tonic::transport::Error),
}

/// Result returned while building a Zakura gRPC client.
pub type ZakuraGrpcBuilderResult<T> = Result<T, ZakuraGrpcBuilderError>;

/// Fluent transport and streaming configuration for [`ZakuraGrpcClient`].
#[derive(Debug)]
pub struct ZakuraGrpcBuilder {
    endpoint: Endpoint,
    x_token: Option<AsciiMetadataValue>,
    subscription_id: Option<AsciiMetadataValue>,
    send_compressed: Option<CompressionEncoding>,
    accept_compressed: Option<CompressionEncoding>,
    max_decoding_message_size: Option<usize>,
    max_encoding_message_size: Option<usize>,
    reconnect: Option<ReconnectConfig>,
}

impl ZakuraGrpcBuilder {
    const fn new(endpoint: Endpoint) -> Self {
        Self {
            endpoint,
            x_token: None,
            subscription_id: None,
            send_compressed: None,
            accept_compressed: None,
            max_decoding_message_size: Some(50 * 1024 * 1024),
            max_encoding_message_size: Some(50 * 1024 * 1024),
            reconnect: None,
        }
    }

    /// Creates a builder from a runtime endpoint string.
    pub fn from_shared(endpoint: impl Into<Bytes>) -> ZakuraGrpcBuilderResult<Self> {
        Ok(Self::new(
            Endpoint::from_shared(endpoint)?.http2_adaptive_window(true),
        ))
    }

    /// Creates a builder from a static endpoint string.
    #[must_use]
    pub fn from_static(endpoint: &'static str) -> Self {
        Self::new(Endpoint::from_static(endpoint).http2_adaptive_window(true))
    }

    fn build(self, channel: Channel) -> ZakuraGrpcClient {
        let interceptor = MetadataInterceptor {
            x_token: self.x_token,
            subscription_id: self.subscription_id,
        };
        let options = ClientOptions {
            send_compressed: self.send_compressed,
            accept_compressed: self.accept_compressed,
            max_decoding_message_size: self.max_decoding_message_size,
            max_encoding_message_size: self.max_encoding_message_size,
        };
        let geyser = geyser_client(channel.clone(), interceptor.clone(), &options);
        let health = HealthClient::with_interceptor(channel, interceptor.clone());
        ZakuraGrpcClient {
            geyser,
            health,
            endpoint: self.endpoint,
            interceptor,
            options,
            reconnect: self.reconnect,
        }
    }

    /// Connects immediately and returns an error if the endpoint is unavailable.
    pub async fn connect(self) -> ZakuraGrpcBuilderResult<ZakuraGrpcClient> {
        let channel = self.endpoint.connect().await?;
        Ok(self.build(channel))
    }

    /// Builds a channel which connects when first used.
    pub fn connect_lazy(self) -> ZakuraGrpcClient {
        let channel = self.endpoint.connect_lazy();
        self.build(channel)
    }

    /// Adds the shared authentication token to every request.
    pub fn x_token<T>(mut self, value: Option<T>) -> ZakuraGrpcBuilderResult<Self>
    where
        T: TryInto<AsciiMetadataValue, Error = InvalidMetadataValue>,
    {
        self.x_token = value.map(TryInto::try_into).transpose()?;
        Ok(self)
    }

    /// Adds a stable identity used by server-side concurrent stream limits.
    pub fn subscription_id<T>(mut self, value: Option<T>) -> ZakuraGrpcBuilderResult<Self>
    where
        T: TryInto<AsciiMetadataValue, Error = InvalidMetadataValue>,
    {
        self.subscription_id = value.map(TryInto::try_into).transpose()?;
        Ok(self)
    }

    /// Sets the connection establishment timeout.
    #[must_use]
    pub fn connect_timeout(mut self, duration: Duration) -> Self {
        self.endpoint = self.endpoint.connect_timeout(duration);
        self
    }

    /// Sets the whole-request timeout.
    #[must_use]
    pub fn timeout(mut self, duration: Duration) -> Self {
        self.endpoint = self.endpoint.timeout(duration);
        self
    }

    /// Enables or disables adaptive HTTP/2 flow control.
    #[must_use]
    pub fn http2_adaptive_window(mut self, enabled: bool) -> Self {
        self.endpoint = self.endpoint.http2_adaptive_window(enabled);
        self
    }

    /// Sets the HTTP/2 keepalive interval.
    #[must_use]
    pub fn http2_keep_alive_interval(mut self, interval: Duration) -> Self {
        self.endpoint = self.endpoint.http2_keep_alive_interval(interval);
        self
    }

    /// Sets the keepalive acknowledgement timeout.
    #[must_use]
    pub fn keep_alive_timeout(mut self, duration: Duration) -> Self {
        self.endpoint = self.endpoint.keep_alive_timeout(duration);
        self
    }

    /// Sends keepalives while no streams are active.
    #[must_use]
    pub fn keep_alive_while_idle(mut self, enabled: bool) -> Self {
        self.endpoint = self.endpoint.keep_alive_while_idle(enabled);
        self
    }

    /// Sets the initial HTTP/2 connection window.
    #[must_use]
    pub fn initial_connection_window_size(mut self, size: impl Into<Option<u32>>) -> Self {
        self.endpoint = self.endpoint.initial_connection_window_size(size);
        self
    }

    /// Sets the initial HTTP/2 stream window.
    #[must_use]
    pub fn initial_stream_window_size(mut self, size: impl Into<Option<u32>>) -> Self {
        self.endpoint = self.endpoint.initial_stream_window_size(size);
        self
    }

    /// Sets TCP keepalive on the socket.
    #[must_use]
    pub fn tcp_keepalive(mut self, duration: Option<Duration>) -> Self {
        self.endpoint = self.endpoint.tcp_keepalive(duration);
        self
    }

    /// Enables or disables Nagle's algorithm.
    #[must_use]
    pub fn tcp_nodelay(mut self, enabled: bool) -> Self {
        self.endpoint = self.endpoint.tcp_nodelay(enabled);
        self
    }

    /// Configures TLS for the channel.
    pub fn tls_config(mut self, config: ClientTlsConfig) -> ZakuraGrpcBuilderResult<Self> {
        self.endpoint = self.endpoint.tls_config(config)?;
        Ok(self)
    }

    /// Compresses requests using the selected encoding.
    #[must_use]
    pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
        self.send_compressed = Some(encoding);
        self
    }

    /// Accepts compressed responses using the selected encoding.
    #[must_use]
    pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
        self.accept_compressed = Some(encoding);
        self
    }

    /// Sets the largest decoded response accepted by this client.
    #[must_use]
    pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
        self.max_decoding_message_size = Some(limit);
        self
    }

    /// Sets the largest encoded request emitted by this client.
    #[must_use]
    pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
        self.max_encoding_message_size = Some(limit);
        self
    }

    /// Enables automatic stream reconnection using the selected policy.
    #[must_use]
    pub fn set_reconnect_config(mut self, config: ReconnectConfig) -> Self {
        self.reconnect = Some(config);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_non_ascii_token() {
        let result =
            ZakuraGrpcBuilder::from_static("http://127.0.0.1:10000").x_token(Some("bad\ntoken"));
        assert!(matches!(result, Err(ZakuraGrpcBuilderError::Metadata(_))));
    }
}
