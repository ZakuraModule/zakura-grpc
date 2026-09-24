use std::{collections::HashMap, error::Error};

use clap::{Parser, Subcommand, ValueEnum};
use tonic::codec::CompressionEncoding;
use zakura_grpc_client::{ReconnectConfig, ZakuraGrpcClient};
use zakura_grpc_proto::geyser::{
    subscribe_update, EventType, SubscribeRequest, SubscribeRequestFilter, SubscribeUpdate,
};

#[derive(Debug, Parser)]
#[command(about = "Zakura Geyser gRPC example client")]
struct Args {
    /// Zakura gRPC endpoint.
    #[arg(long, default_value = "http://127.0.0.1:10000")]
    endpoint: String,

    /// Shared token sent in the x-token metadata header.
    #[arg(long, env = "ZAKURA_GRPC_X_TOKEN")]
    x_token: Option<String>,

    /// Stable identity used by the server's concurrent subscription limit.
    #[arg(long, default_value = "zakura-rust-example")]
    subscription_id: String,

    /// Enable gzip request and response compression.
    #[arg(long)]
    gzip: bool,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Stream retained events followed by live events.
    Subscribe {
        /// First retained block height to replay.
        #[arg(long)]
        from_height: Option<u32>,
        /// Event filters; omit to receive every event type.
        #[arg(long, value_enum)]
        event: Vec<EventArg>,
        /// Exit after this many updates; omit to follow indefinitely.
        #[arg(long)]
        max_updates: Option<usize>,
        /// Reconnect automatically and replay missed retained blocks.
        #[arg(long)]
        reconnect: bool,
        /// Optional named filter returned on matching updates.
        #[arg(long)]
        filter_name: Option<String>,
        /// Ignore block-scoped updates below this height inside the named filter.
        #[arg(long, requires = "filter_name")]
        min_height: Option<u32>,
    },
    /// Show the process-local replay range.
    ReplayInfo,
    /// Call the unary ping endpoint.
    Ping {
        #[arg(default_value_t = 1)]
        count: i32,
    },
    /// Show server and interface versions.
    GetVersion,
    /// Query standard gRPC health.
    Health,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum EventArg {
    BlockAccepted,
    BestChainChanged,
    BlockFinalized,
    MempoolChanged,
}

impl From<EventArg> for EventType {
    fn from(value: EventArg) -> Self {
        match value {
            EventArg::BlockAccepted => Self::BlockAccepted,
            EventArg::BestChainChanged => Self::BestChainChanged,
            EventArg::BlockFinalized => Self::BlockFinalized,
            EventArg::MempoolChanged => Self::MempoolChanged,
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    let mut builder = ZakuraGrpcClient::build_from_shared(args.endpoint)?
        .x_token(args.x_token)?
        .subscription_id(Some(args.subscription_id))?;
    if args.gzip {
        builder = builder
            .send_compressed(CompressionEncoding::Gzip)
            .accept_compressed(CompressionEncoding::Gzip);
    }

    match args.command {
        Command::Subscribe {
            from_height,
            event,
            max_updates,
            reconnect,
            filter_name,
            min_height,
        } => {
            if reconnect {
                builder = builder.set_reconnect_config(ReconnectConfig::default());
            }
            let event_types: Vec<_> = event
                .into_iter()
                .map(EventType::from)
                .map(Into::into)
                .collect();
            let (event_types, filters) = match filter_name {
                Some(name) => (
                    Vec::new(),
                    HashMap::from([(
                        name,
                        SubscribeRequestFilter {
                            event_types,
                            min_height,
                        },
                    )]),
                ),
                None => (event_types, HashMap::new()),
            };
            let request = SubscribeRequest {
                event_types,
                from_height,
                filters,
                ..SubscribeRequest::default()
            };
            let mut client = builder.connect().await?;
            let (_requests, mut updates) = client.subscribe_with_request(request).await?;
            let mut received = 0usize;
            while let Some(update) = updates.message().await? {
                print_update(&update);
                received = received.saturating_add(1);
                if max_updates.is_some_and(|limit| received >= limit) {
                    break;
                }
            }
        }
        Command::ReplayInfo => {
            let mut client = builder.connect().await?;
            println!("{:?}", client.subscribe_replay_info().await?);
        }
        Command::Ping { count } => {
            let mut client = builder.connect().await?;
            println!("{:?}", client.ping(count).await?);
        }
        Command::GetVersion => {
            let mut client = builder.connect().await?;
            println!("{:?}", client.get_version().await?);
        }
        Command::Health => {
            let mut client = builder.connect().await?;
            println!("{:?}", client.health_check().await?);
        }
    }

    Ok(())
}

fn print_update(update: &SubscribeUpdate) {
    let event_type = EventType::try_from(update.event_type).map_or_else(
        |_| update.event_type.to_string(),
        |kind| kind.as_str_name().to_owned(),
    );
    match update.update.as_ref() {
        Some(subscribe_update::Update::Block(block)) => println!(
            "sequence={} event={} filters={:?} height={} hash={} block_bytes={} finalized={}",
            update.sequence,
            event_type,
            update.filters,
            block.height,
            block.hash,
            block.block.len(),
            block.finalized
        ),
        Some(subscribe_update::Update::BestChain(change)) => println!(
            "sequence={} event={} filters={:?} height={} hash={}",
            update.sequence, event_type, update.filters, change.height, change.hash
        ),
        Some(subscribe_update::Update::Mempool(change)) => println!(
            "sequence={} event={} action={} transactions={}",
            update.sequence,
            event_type,
            change.action,
            change.transaction_ids.len()
        ),
        Some(subscribe_update::Update::Pong(pong)) => {
            println!("subscription_pong id={}", pong.id);
        }
        None => println!(
            "sequence={} event={} payload=none",
            update.sequence, event_type
        ),
    }
}
