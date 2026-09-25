use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    num::NonZeroUsize,
};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use tonic::{codec::CompressionEncoding, metadata::AsciiMetadataValue};

/// Configuration owned by one Zakura gRPC plugin instance.
#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct Config {
    /// Address used by the gRPC server.
    pub listen_addr: SocketAddr,
    /// Emit one transaction update for every transaction in a block event.
    pub transaction_updates: bool,
    /// Emit complete verified transactions when they enter the mempool.
    pub mempool_transaction_updates: bool,
    /// Emit transparent UTXO changes derived from every transaction in a block event.
    pub utxo_updates: bool,
    /// Number of distinct block heights retained for short reconnect replay.
    pub replay_stored_blocks: usize,
    /// Maximum number of protobuf updates retained in the replay window.
    pub replay_max_events: usize,
    /// Maximum encoded protobuf bytes retained in the replay window.
    pub replay_max_bytes: usize,
    /// Maximum replay entry age in seconds, or no age limit when omitted.
    pub replay_max_age_seconds: Option<u64>,
    /// Capacity of the live event broadcast ring.
    pub broadcast_capacity: usize,
    /// Bounded outbound queue capacity for each connected client.
    pub client_channel_capacity: usize,
    /// Maximum decoded inbound gRPC message size in bytes.
    pub max_decoding_message_size: usize,
    /// Maximum encoded outbound gRPC message size in bytes.
    pub max_encoding_message_size: usize,
    /// Accepted and emitted gRPC compression formats.
    pub compression: CompressionConfig,
    /// Optional shared token required in the `x-token` metadata header.
    pub x_token: Option<String>,
    /// Maximum concurrent subscriptions for one `x-subscription-id` or IP.
    pub subscription_limit: NonZeroUsize,
    /// Reject excessive subscriptions instead of only recording a metric and warning.
    pub subscription_limit_enforce: bool,
    /// Limits applied whenever a client installs a subscription filter.
    pub filter_limits: FilterLimits,
    /// Number of dedicated threads used to encode block and mempool transaction batches.
    pub event_encoding_threads: usize,
    /// Minimum transaction count required before parallel batch encoding is used.
    pub parallel_encoding_min_transactions: usize,
    /// Enable Tonic's adaptive HTTP/2 flow-control window.
    pub server_http2_adaptive_window: Option<bool>,
    /// HTTP/2 keepalive interval in milliseconds.
    pub server_http2_keepalive_interval_ms: Option<u64>,
    /// HTTP/2 keepalive timeout in milliseconds.
    pub server_http2_keepalive_timeout_ms: Option<u64>,
    /// Initial connection flow-control window in bytes.
    pub server_initial_connection_window_size: Option<u32>,
    /// Initial stream flow-control window in bytes.
    pub server_initial_stream_window_size: Option<u32>,
}

impl Config {
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.broadcast_capacity == 0 {
            return Err(ConfigError::ZeroBroadcastCapacity);
        }
        if self.replay_max_events == 0 {
            return Err(ConfigError::ZeroReplayMaxEvents);
        }
        if self.replay_max_bytes == 0 {
            return Err(ConfigError::ZeroReplayMaxBytes);
        }
        if self.replay_max_age_seconds == Some(0) {
            return Err(ConfigError::ZeroReplayMaxAge);
        }
        if self.client_channel_capacity == 0 {
            return Err(ConfigError::ZeroClientChannelCapacity);
        }
        if self.max_decoding_message_size == 0 {
            return Err(ConfigError::ZeroMaxDecodingMessageSize);
        }
        if self.max_encoding_message_size == 0 {
            return Err(ConfigError::ZeroMaxEncodingMessageSize);
        }
        if self.event_encoding_threads == 0 {
            return Err(ConfigError::ZeroEventEncodingThreads);
        }
        if self.parallel_encoding_min_transactions == 0 {
            return Err(ConfigError::ZeroParallelEncodingMinTransactions);
        }
        if self.x_token.as_ref().is_some_and(String::is_empty) {
            return Err(ConfigError::EmptyToken);
        }
        if self
            .x_token
            .as_ref()
            .is_some_and(|token| token.parse::<AsciiMetadataValue>().is_err())
        {
            return Err(ConfigError::InvalidToken);
        }
        self.filter_limits.validate()?;
        Ok(())
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            listen_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 10_000),
            transaction_updates: true,
            mempool_transaction_updates: true,
            utxo_updates: true,
            replay_stored_blocks: 150,
            replay_max_events: 250_000,
            replay_max_bytes: 512 * 1024 * 1024,
            replay_max_age_seconds: Some(4 * 60 * 60),
            broadcast_capacity: 100_000,
            client_channel_capacity: 10_000,
            max_decoding_message_size: 4 * 1024 * 1024,
            max_encoding_message_size: 50 * 1024 * 1024,
            compression: CompressionConfig::default(),
            x_token: None,
            subscription_limit: NonZeroUsize::new(1_000)
                .expect("the default subscription limit is non-zero"),
            subscription_limit_enforce: false,
            filter_limits: FilterLimits::default(),
            event_encoding_threads: 1,
            parallel_encoding_min_transactions: 32,
            server_http2_adaptive_window: Some(true),
            server_http2_keepalive_interval_ms: None,
            server_http2_keepalive_timeout_ms: None,
            server_initial_connection_window_size: None,
            server_initial_stream_window_size: None,
        }
    }
}

/// Supported gRPC compression algorithm.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Compression {
    /// Gzip compression.
    Gzip,
    /// Zstandard compression.
    Zstd,
}

impl From<Compression> for CompressionEncoding {
    fn from(value: Compression) -> Self {
        match value {
            Compression::Gzip => Self::Gzip,
            Compression::Zstd => Self::Zstd,
        }
    }
}

/// Compression formats accepted from and sent to clients.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct CompressionConfig {
    /// Formats accepted on inbound requests.
    pub accept: Vec<Compression>,
    /// Formats available for outbound responses.
    pub send: Vec<Compression>,
}

impl Default for CompressionConfig {
    fn default() -> Self {
        Self {
            accept: vec![Compression::Gzip, Compression::Zstd],
            send: vec![Compression::Gzip, Compression::Zstd],
        }
    }
}

/// Limits for legacy and named subscription filters.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct FilterLimits {
    /// Maximum named filters in one request.
    pub max_named_filters: usize,
    /// Maximum event types in each legacy or named filter.
    pub max_event_types: usize,
    /// Maximum UTF-8 byte length of a filter name.
    pub max_name_bytes: usize,
    /// Maximum transaction IDs in one named filter.
    pub max_transaction_ids: usize,
    /// Maximum transparent addresses in one named filter.
    pub max_transparent_addresses: usize,
    /// Allow an empty filter to subscribe to every event type.
    pub allow_all: bool,
}

impl FilterLimits {
    fn validate(&self) -> Result<(), ConfigError> {
        if self.max_named_filters == 0 {
            return Err(ConfigError::ZeroMaxNamedFilters);
        }
        if self.max_event_types == 0 {
            return Err(ConfigError::ZeroMaxEventTypes);
        }
        if self.max_name_bytes == 0 {
            return Err(ConfigError::ZeroMaxFilterNameBytes);
        }
        if self.max_transaction_ids == 0 {
            return Err(ConfigError::ZeroMaxTransactionIds);
        }
        if self.max_transparent_addresses == 0 {
            return Err(ConfigError::ZeroMaxTransparentAddresses);
        }
        Ok(())
    }
}

impl Default for FilterLimits {
    fn default() -> Self {
        Self {
            max_named_filters: 32,
            max_event_types: 7,
            max_name_bytes: 128,
            max_transaction_ids: 256,
            max_transparent_addresses: 256,
            allow_all: true,
        }
    }
}

/// Invalid plugin configuration.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ConfigError {
    /// Replay must have an event limit even when height retention is disabled.
    #[error("replay_max_events must be greater than zero")]
    ZeroReplayMaxEvents,
    /// Replay must have a byte limit even when height retention is disabled.
    #[error("replay_max_bytes must be greater than zero")]
    ZeroReplayMaxBytes,
    /// An enabled age limit must retain entries for a positive duration.
    #[error("replay_max_age_seconds must be omitted or greater than zero")]
    ZeroReplayMaxAge,
    /// The live broadcast ring cannot be empty.
    #[error("broadcast_capacity must be greater than zero")]
    ZeroBroadcastCapacity,
    /// Every client needs a bounded non-empty outbound queue.
    #[error("client_channel_capacity must be greater than zero")]
    ZeroClientChannelCapacity,
    /// Tonic must accept at least one byte per inbound request.
    #[error("max_decoding_message_size must be greater than zero")]
    ZeroMaxDecodingMessageSize,
    /// Tonic must be able to encode at least one byte per response.
    #[error("max_encoding_message_size must be greater than zero")]
    ZeroMaxEncodingMessageSize,
    /// Event encoding needs at least one worker thread.
    #[error("event_encoding_threads must be greater than zero")]
    ZeroEventEncodingThreads,
    /// Parallel encoding needs a positive activation threshold.
    #[error("parallel_encoding_min_transactions must be greater than zero")]
    ZeroParallelEncodingMinTransactions,
    /// Empty tokens can be confused with disabled authentication.
    #[error("x_token must be omitted or contain at least one character")]
    EmptyToken,
    /// The token must be representable as ASCII gRPC metadata.
    #[error("x_token must be valid ASCII gRPC metadata")]
    InvalidToken,
    /// At least one named filter must be representable.
    #[error("filter_limits.max_named_filters must be greater than zero")]
    ZeroMaxNamedFilters,
    /// At least one event type must be representable.
    #[error("filter_limits.max_event_types must be greater than zero")]
    ZeroMaxEventTypes,
    /// Filter names need a positive size limit.
    #[error("filter_limits.max_name_bytes must be greater than zero")]
    ZeroMaxFilterNameBytes,
    /// At least one transaction ID must be representable in a filter.
    #[error("filter_limits.max_transaction_ids must be greater than zero")]
    ZeroMaxTransactionIds,
    /// At least one transparent address must be representable in a filter.
    #[error("filter_limits.max_transparent_addresses must be greater than zero")]
    ZeroMaxTransparentAddresses,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_short_reconnect_replay() {
        let config = Config::default();
        assert_eq!(config.listen_addr, "127.0.0.1:10000".parse().unwrap());
        assert_eq!(config.replay_stored_blocks, 150);
        assert_eq!(config.replay_max_events, 250_000);
        assert_eq!(config.replay_max_bytes, 512 * 1024 * 1024);
        assert_eq!(config.replay_max_age_seconds, Some(4 * 60 * 60));
        assert!(config.validate().is_ok());
    }

    #[test]
    fn unknown_fields_are_rejected() {
        let error = serde_json::from_value::<Config>(serde_json::json!({
            "unknown": true
        }))
        .unwrap_err();
        assert!(error.to_string().contains("unknown field"));
    }

    #[test]
    fn empty_token_is_rejected() {
        let config = Config {
            x_token: Some(String::new()),
            ..Config::default()
        };
        assert_eq!(config.validate(), Err(ConfigError::EmptyToken));
    }

    #[test]
    fn zero_replay_resource_limits_are_rejected() {
        let no_events = Config {
            replay_max_events: 0,
            ..Config::default()
        };
        assert_eq!(no_events.validate(), Err(ConfigError::ZeroReplayMaxEvents));

        let no_bytes = Config {
            replay_max_bytes: 0,
            ..Config::default()
        };
        assert_eq!(no_bytes.validate(), Err(ConfigError::ZeroReplayMaxBytes));

        let zero_age = Config {
            replay_max_age_seconds: Some(0),
            ..Config::default()
        };
        assert_eq!(zero_age.validate(), Err(ConfigError::ZeroReplayMaxAge));
    }

    #[test]
    fn zero_encoding_limits_are_rejected() {
        let no_threads = Config {
            event_encoding_threads: 0,
            ..Config::default()
        };
        assert_eq!(
            no_threads.validate(),
            Err(ConfigError::ZeroEventEncodingThreads)
        );

        let no_threshold = Config {
            parallel_encoding_min_transactions: 0,
            ..Config::default()
        };
        assert_eq!(
            no_threshold.validate(),
            Err(ConfigError::ZeroParallelEncodingMinTransactions)
        );
    }
}
