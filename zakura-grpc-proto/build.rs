use std::{env, error::Error, path::PathBuf};

fn main() -> Result<(), Box<dyn Error>> {
    let protoc = protoc_bin_vendored::protoc_bin_path()?;
    env::set_var("PROTOC", protoc);

    let descriptor_path = PathBuf::from(env::var("OUT_DIR")?).join("zakura_geyser_descriptor.bin");
    tonic_prost_build::configure()
        .file_descriptor_set_path(descriptor_path)
        .compile_protos(&["proto/zakura/geyser/v1/geyser.proto"], &["proto"])?;

    println!("cargo:rerun-if-changed=proto/zakura/geyser/v1/geyser.proto");
    Ok(())
}
