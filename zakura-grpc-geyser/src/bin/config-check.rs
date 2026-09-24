use std::{error::Error, fs, path::PathBuf};

use clap::Parser;
use zakura_grpc_geyser::Config;

#[derive(Debug, Parser)]
#[command(about = "Validate a Zakura gRPC plugin JSON or TOML configuration")]
struct Args {
    /// Path to a file containing the fields from `[geyser.plugins.plugin]`.
    config: PathBuf,
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    let contents = fs::read_to_string(&args.config)?;
    let config: Config = match args.config.extension().and_then(|value| value.to_str()) {
        Some("json") => serde_json::from_str(&contents)?,
        Some("toml") => toml::from_str(&contents)?,
        _ => return Err("configuration path must end in .json or .toml".into()),
    };
    config.validate()?;
    println!("{}: valid", args.config.display());
    Ok(())
}
