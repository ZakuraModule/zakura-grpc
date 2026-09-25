use std::{
    collections::BTreeMap,
    error::Error,
    io,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use clap::{Parser, ValueEnum};
use prost::Message;
use tokio::{task::JoinSet, time::Instant};
use tonic::codec::CompressionEncoding;
use zakura_grpc_client::{ReconnectConfig, ZakuraGrpcClient};
use zakura_grpc_proto::geyser::{EventType, SubscribeRequest, SubscribeUpdate};

type DynError = Box<dyn Error + Send + Sync>;

const MAX_LATENCY_MS: usize = 10_000;

#[derive(Clone, Debug, Parser)]
#[command(about = "Measure Zakura Geyser gRPC throughput and end-to-end latency")]
struct Args {
    /// Zakura gRPC endpoint.
    #[arg(long, default_value = "http://127.0.0.1:10000")]
    endpoint: String,

    /// Shared token sent in the x-token metadata header.
    #[arg(long, env = "ZAKURA_GRPC_X_TOKEN")]
    x_token: Option<String>,

    /// Number of concurrent subscriptions.
    #[arg(long, default_value_t = 1)]
    clients: usize,

    /// Measurement duration after each client has subscribed.
    #[arg(long, default_value_t = 30)]
    duration_seconds: u64,

    /// First retained block height to replay.
    #[arg(long)]
    from_height: Option<u32>,

    /// Event types to consume; omit to receive every event type.
    #[arg(long, value_enum)]
    event: Vec<EventArg>,

    /// Reconnect streams and recover retained updates after disconnection.
    #[arg(long)]
    reconnect: bool,

    /// Enable gzip request and response compression.
    #[arg(long)]
    gzip: bool,
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

#[derive(Debug)]
struct Stats {
    messages: u64,
    bytes: u64,
    active_duration: Duration,
    events: BTreeMap<&'static str, u64>,
    latency: LatencyHistogram,
}

impl Stats {
    fn new() -> Self {
        Self {
            messages: 0,
            bytes: 0,
            active_duration: Duration::ZERO,
            events: BTreeMap::new(),
            latency: LatencyHistogram::new(),
        }
    }

    fn record(&mut self, update: &SubscribeUpdate) {
        self.messages = self.messages.saturating_add(1);
        self.bytes = self
            .bytes
            .saturating_add(u64::try_from(update.encoded_len()).unwrap_or(u64::MAX));
        let event = EventType::try_from(update.event_type)
            .unwrap_or(EventType::Unspecified)
            .as_str_name();
        let count = self.events.entry(event).or_default();
        *count = count.saturating_add(1);
        if let Some(latency_ms) = update_latency_ms(update) {
            self.latency.record(latency_ms);
        }
    }

    fn merge(&mut self, other: Self) {
        self.messages = self.messages.saturating_add(other.messages);
        self.bytes = self.bytes.saturating_add(other.bytes);
        self.active_duration = self.active_duration.max(other.active_duration);
        for (event, count) in other.events {
            let total = self.events.entry(event).or_default();
            *total = total.saturating_add(count);
        }
        self.latency.merge(&other.latency);
    }
}

#[derive(Debug)]
struct LatencyHistogram {
    buckets: Vec<u64>,
    samples: u64,
}

impl LatencyHistogram {
    fn new() -> Self {
        Self {
            buckets: vec![0; MAX_LATENCY_MS + 1],
            samples: 0,
        }
    }

    fn record(&mut self, latency_ms: u64) {
        let bucket = usize::try_from(latency_ms)
            .unwrap_or(usize::MAX)
            .min(MAX_LATENCY_MS);
        self.buckets[bucket] = self.buckets[bucket].saturating_add(1);
        self.samples = self.samples.saturating_add(1);
    }

    fn merge(&mut self, other: &Self) {
        self.samples = self.samples.saturating_add(other.samples);
        for (bucket, count) in self.buckets.iter_mut().zip(&other.buckets) {
            *bucket = bucket.saturating_add(*count);
        }
    }

    fn percentile(&self, percentile: u64) -> Option<usize> {
        if self.samples == 0 {
            return None;
        }
        let target = self.samples.saturating_mul(percentile).saturating_add(99) / 100;
        let mut seen = 0u64;
        self.buckets.iter().position(|count| {
            seen = seen.saturating_add(*count);
            seen >= target
        })
    }
}

#[tokio::main]
async fn main() -> Result<(), DynError> {
    let args = Args::parse();
    if args.clients == 0 {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "clients must be non-zero").into());
    }
    if args.duration_seconds == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "duration-seconds must be non-zero",
        )
        .into());
    }

    let mut workers = JoinSet::new();
    for client_index in 0..args.clients {
        workers.spawn(run_client(args.clone(), client_index));
    }

    let mut total = Stats::new();
    while let Some(result) = workers.join_next().await {
        total.merge(result??);
    }
    print_summary(&args, &total);
    Ok(())
}

async fn run_client(args: Args, client_index: usize) -> Result<Stats, DynError> {
    let mut builder = ZakuraGrpcClient::build_from_shared(args.endpoint)?
        .x_token(args.x_token)?
        .subscription_id(Some(format!("zakura-load-test-{client_index}")))?;
    if args.gzip {
        builder = builder
            .send_compressed(CompressionEncoding::Gzip)
            .accept_compressed(CompressionEncoding::Gzip);
    }
    if args.reconnect {
        builder = builder.set_reconnect_config(ReconnectConfig::default());
    }
    let mut client = builder.connect().await?;
    let request = SubscribeRequest {
        event_types: args
            .event
            .into_iter()
            .map(EventType::from)
            .map(Into::into)
            .collect(),
        from_height: args.from_height,
        ..SubscribeRequest::default()
    };
    let mut stream = client.subscribe_once(request).await?;
    let started = Instant::now();
    let deadline = started + Duration::from_secs(args.duration_seconds);
    let mut stats = Stats::new();

    loop {
        let update = match tokio::time::timeout_at(deadline, stream.message()).await {
            Ok(Ok(Some(update))) => update,
            Ok(Ok(None)) | Err(_) => break,
            Ok(Err(error)) => return Err(error.into()),
        };
        stats.record(&update);
    }
    stats.active_duration = started.elapsed();
    Ok(stats)
}

fn update_latency_ms(update: &SubscribeUpdate) -> Option<u64> {
    let observed_at = update.observed_at.as_ref()?;
    let observed_seconds = u64::try_from(observed_at.seconds).ok()?;
    let observed_nanos = u32::try_from(observed_at.nanos).ok()?;
    if observed_nanos >= 1_000_000_000 {
        return None;
    }
    let observed = Duration::new(observed_seconds, observed_nanos);
    let now = SystemTime::now().duration_since(UNIX_EPOCH).ok()?;
    let latency = now.checked_sub(observed)?;
    u64::try_from(latency.as_millis()).ok()
}

fn print_summary(args: &Args, stats: &Stats) {
    let elapsed = stats.active_duration.as_secs_f64().max(f64::EPSILON);
    let messages_per_second = counter_as_f64(stats.messages) / elapsed;
    let mebibytes_per_second = counter_as_f64(stats.bytes) / elapsed / (1024.0 * 1024.0);
    println!("clients={}", args.clients);
    println!("active_seconds={elapsed:.3}");
    println!("messages={}", stats.messages);
    println!("encoded_bytes={}", stats.bytes);
    println!("messages_per_second={messages_per_second:.2}");
    println!("mebibytes_per_second={mebibytes_per_second:.2}");
    println!("latency_samples={}", stats.latency.samples);
    for percentile in [50, 95, 99] {
        if let Some(milliseconds) = stats.latency.percentile(percentile) {
            let suffix = if milliseconds == MAX_LATENCY_MS {
                "+"
            } else {
                ""
            };
            println!("latency_p{percentile}_ms={milliseconds}{suffix}");
        }
    }
    for (event, count) in &stats.events {
        let event = event.strip_prefix("EVENT_TYPE_").unwrap_or(event);
        println!("event_{}_total={count}", event.to_ascii_lowercase());
    }
}

#[allow(clippy::cast_precision_loss)]
fn counter_as_f64(value: u64) -> f64 {
    value as f64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn latency_histogram_reports_percentiles() {
        let mut histogram = LatencyHistogram::new();
        for latency in [10, 20, 30, 40] {
            histogram.record(latency);
        }

        assert_eq!(histogram.percentile(50), Some(20));
        assert_eq!(histogram.percentile(95), Some(40));
        assert_eq!(histogram.percentile(99), Some(40));
    }

    #[test]
    fn latency_histogram_caps_large_samples() {
        let mut histogram = LatencyHistogram::new();
        histogram.record(u64::MAX);

        assert_eq!(histogram.percentile(50), Some(MAX_LATENCY_MS));
    }
}
