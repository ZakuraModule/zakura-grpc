//! Generated protocol types shared by the Zakura gRPC server and clients.

/// Version 1 of the Zakura Geyser protocol.
#[allow(clippy::all, clippy::pedantic)]
pub mod geyser {
    tonic::include_proto!("zakura.geyser.v1");

    /// Encoded descriptor set for reflection and downstream code generation.
    pub const FILE_DESCRIPTOR_SET: &[u8] =
        tonic::include_file_descriptor_set!("zakura_geyser_descriptor");
}
