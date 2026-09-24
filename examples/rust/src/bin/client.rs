use std::error::Error;

use clap::{Parser, Subcommand, ValueEnum};
use zakura_grpc_client::ZakuraGrpcClient;
use zakura_grpc_proto::geyser::{subscribe_update, EventType, SubscribeRequest, SubscribeUpdate};

#[derive(Debug, Parser)]
#[command(about = "Zakura Geyser gRPC example client")]
struct Args {
    /// Zakura gRPC endpoint.
    #[arg(long, default_value = "http://127.0.0.1:10000")]
    endpoint: String,

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
    let mut client = ZakuraGrpcClient::connect(args.endpoint).await?;

    match args.command {
        Command::Subscribe {
            from_height,
            event,
            max_updates,
        } => {
            let request = SubscribeRequest {
                event_types: event
                    .into_iter()
                    .map(EventType::from)
                    .map(Into::into)
                    .collect(),
                from_height,
                ..SubscribeRequest::default()
            };
            let (_requests, mut updates) = client.subscribe(request).await?;
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
            println!("{:?}", client.subscribe_replay_info().await?.into_inner());
        }
        Command::Ping { count } => {
            println!("{:?}", client.ping(count).await?.into_inner());
        }
        Command::GetVersion => {
            println!("{:?}", client.get_version().await?.into_inner());
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
            "sequence={} event={} height={} hash={} block_bytes={} finalized={}",
            update.sequence,
            event_type,
            block.height,
            block.hash,
            block.block.len(),
            block.finalized
        ),
        Some(subscribe_update::Update::BestChain(change)) => println!(
            "sequence={} event={} height={} hash={}",
            update.sequence, event_type, change.height, change.hash
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
