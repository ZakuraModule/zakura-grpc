use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Configuration owned by one Zakura gRPC plugin instance.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, default)]
pub struct Config {
    /// Address used by the gRPC server.
    pub listen_addr: SocketAddr,
    /// Number of distinct block heights retained for short reconnect replay.
    pub replay_stored_blocks: usize,
    /// Capacity of the live event broadcast ring.
    pub broadcast_capacity: usize,
    /// Bounded outbound queue capacity for each connected client.
    pub client_channel_capacity: usize,
    /// Maximum decoded inbound gRPC message size in bytes.
    pub max_decoding_message_size: usize,
}

impl Config {
    pub(crate) fn validate(&self) -> Result<(), ConfigError> {
        if self.broadcast_capacity == 0 {
            return Err(ConfigError::ZeroBroadcastCapacity);
        }
        if self.client_channel_capacity == 0 {
            return Err(ConfigError::ZeroClientChannelCapacity);
        }
        if self.max_decoding_message_size == 0 {
            return Err(ConfigError::ZeroMaxDecodingMessageSize);
        }
        Ok(())
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            listen_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 10_000),
            replay_stored_blocks: 150,
            broadcast_capacity: 100_000,
            client_channel_capacity: 10_000,
            max_decoding_message_size: 4 * 1024 * 1024,
        }
    }
}

/// Invalid plugin configuration.
#[derive(Clone, Debug, Error, Eq, PartialEq)]
pub enum ConfigError {
    /// The live broadcast ring cannot be empty.
    #[error("broadcast_capacity must be greater than zero")]
    ZeroBroadcastCapacity,
    /// Every client needs a bounded non-empty outbound queue.
    #[error("client_channel_capacity must be greater than zero")]
    ZeroClientChannelCapacity,
    /// Tonic must accept at least one byte per inbound request.
    #[error("max_decoding_message_size must be greater than zero")]
    ZeroMaxDecodingMessageSize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_match_short_reconnect_replay() {
        let config = Config::default();
        assert_eq!(config.listen_addr, "127.0.0.1:10000".parse().unwrap());
        assert_eq!(config.replay_stored_blocks, 150);
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
}
