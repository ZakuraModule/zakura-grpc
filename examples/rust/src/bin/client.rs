use std::{collections::HashMap, error::Error};

use clap::{Parser, Subcommand, ValueEnum};
use tonic::codec::CompressionEncoding;
use zakura_grpc_client::{ReconnectConfig, ZakuraGrpcClient};
use zakura_grpc_proto::geyser::{
    subscribe_update, transparent_input, utxo_change, BlockCommitment, EventType, Outpoint,
    SubscribeRequest, SubscribeRequestFilter, SubscribeUpdate, TransactionUpdate,
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
        /// Match one transaction ID; repeat to match multiple IDs.
        #[arg(long, requires = "filter_name")]
        transaction_id: Vec<String>,
        /// Match one transparent address; repeat to match multiple addresses.
        #[arg(long, requires = "filter_name")]
        address: Vec<String>,
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
    Transaction,
    Utxo,
}

impl From<EventArg> for EventType {
    fn from(value: EventArg) -> Self {
        match value {
            EventArg::BlockAccepted => Self::BlockAccepted,
            EventArg::BestChainChanged => Self::BestChainChanged,
            EventArg::BlockFinalized => Self::BlockFinalized,
            EventArg::MempoolChanged => Self::MempoolChanged,
            EventArg::Transaction => Self::Transaction,
            EventArg::Utxo => Self::Utxo,
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
            transaction_id,
            address,
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
                            transaction_ids: transaction_id,
                            transparent_addresses: address,
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
        Some(subscribe_update::Update::Transaction(transaction)) => {
            print_transaction_update(update, &event_type, transaction);
        }
        Some(subscribe_update::Update::Utxo(utxo)) => {
            println!(
                "sequence={} source_sequence={} event={} filters={:?} commitment={} height={} block={} transaction_index={} transaction_id={} changes={}",
                update.sequence,
                update.source_sequence,
                event_type,
                update.filters,
                commitment_name(utxo.commitment),
                utxo.height,
                utxo.block_hash,
                utxo.transaction_index,
                utxo.transaction_id,
                utxo.changes.len()
            );
            for change in &utxo.changes {
                match change.change.as_ref() {
                    Some(utxo_change::Change::Created(created)) => println!(
                        "  created outpoint={} value_zat={} address={} lock_script_bytes={}",
                        format_outpoint(created.outpoint.as_ref()),
                        created.value_zat,
                        created.address.as_deref().unwrap_or("non-standard"),
                        created.lock_script.len()
                    ),
                    Some(utxo_change::Change::Spent(spent)) => println!(
                        "  spent outpoint={} input_index={} sequence={} unlock_script_bytes={} previous_value_zat={:?} previous_address={} previous_height={:?} previous_from_coinbase={:?}",
                        format_outpoint(spent.outpoint.as_ref()),
                        spent.input_index,
                        spent.sequence,
                        spent.unlock_script.len(),
                        spent.previous_value_zat,
                        spent.previous_address.as_deref().unwrap_or("unavailable"),
                        spent.previous_height,
                        spent.previous_from_coinbase
                    ),
                    None => println!("  utxo_change payload=none"),
                }
            }
        }
        Some(subscribe_update::Update::Pong(pong)) => {
            println!("subscription_pong id={}", pong.id);
        }
        None => println!(
            "sequence={} event={} payload=none",
            update.sequence, event_type
        ),
    }
}

fn print_transaction_update(
    update: &SubscribeUpdate,
    event_type: &str,
    transaction: &TransactionUpdate,
) {
    println!(
        "sequence={} source_sequence={} event={} filters={:?} commitment={} network={} height={} block={} transaction_index={} transaction_id={} unmined_transaction_id={} auth_digest={} version={} lock_time={} lock_time_is_time={} expiry_height={:?} transaction_bytes={} coinbase={} transparent_inputs={} transparent_outputs={} transparent_input_value_zat={:?} transparent_output_value_zat={}",
        update.sequence,
        update.source_sequence,
        event_type,
        update.filters,
        commitment_name(transaction.commitment),
        transaction.network,
        transaction.height,
        transaction.block_hash,
        transaction.transaction_index,
        transaction.transaction_id,
        transaction.unmined_transaction_id,
        transaction.auth_digest.as_deref().unwrap_or("none"),
        transaction.version,
        transaction.lock_time,
        transaction.lock_time_is_time,
        transaction.expiry_height,
        transaction.transaction.len(),
        transaction.coinbase,
        transaction.transparent_inputs.len(),
        transaction.transparent_outputs.len(),
        transaction.transparent_input_value_zat,
        transaction.transparent_output_value_zat
    );
    for input in &transaction.transparent_inputs {
        match input.input.as_ref() {
            Some(transparent_input::Input::Prevout(prevout)) => println!(
                "  input index={} prevout={} sequence={} unlock_script_bytes={} previous_value_zat={:?} previous_address={} previous_height={:?} previous_from_coinbase={:?}",
                input.input_index,
                format_outpoint(prevout.previous_output.as_ref()),
                input.sequence,
                prevout.unlock_script.len(),
                prevout.previous_value_zat,
                prevout.previous_address.as_deref().unwrap_or("unavailable"),
                prevout.previous_height,
                prevout.previous_from_coinbase
            ),
            Some(transparent_input::Input::Coinbase(coinbase)) => println!(
                "  input index={} coinbase_height={} sequence={} data_bytes={}",
                input.input_index,
                coinbase.height,
                input.sequence,
                coinbase.data.len()
            ),
            None => println!("  input index={} payload=none", input.input_index),
        }
    }
    for output in &transaction.transparent_outputs {
        println!(
            "  output index={} value_zat={} address={} lock_script_bytes={}",
            output.output_index,
            output.value_zat,
            output.address.as_deref().unwrap_or("non-standard"),
            output.lock_script.len()
        );
    }
}

fn commitment_name(commitment: i32) -> String {
    BlockCommitment::try_from(commitment).map_or_else(
        |_| commitment.to_string(),
        |commitment| commitment.as_str_name().to_owned(),
    )
}

fn format_outpoint(outpoint: Option<&Outpoint>) -> String {
    outpoint.map_or_else(
        || "missing".to_owned(),
        |outpoint| format!("{}:{}", outpoint.transaction_id, outpoint.output_index),
    )
}
