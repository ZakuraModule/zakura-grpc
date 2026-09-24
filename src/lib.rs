//! Compatibility facade for the split Zakura gRPC crates.

pub use zakura_grpc_client as client;
pub use zakura_grpc_geyser as geyser;
pub use zakura_grpc_geyser::{from_plugin_config, register, Config, ConfigError, GrpcPlugin};
pub use zakura_grpc_proto as proto;
