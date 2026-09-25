//! Yellowstone-style gRPC streaming plugin for Zakura.
//!
//! The plugin keeps a bounded in-memory replay window for reconnects and never
//! puts socket backpressure on Zakura's state or consensus tasks.

mod auth;
mod config;
mod event;
mod filter;
mod server;

use std::{
    fmt,
    net::TcpListener as StdTcpListener,
    sync::Arc,
    time::{Duration, Instant},
};

pub use config::{Compression, CompressionConfig, Config, ConfigError, FilterLimits};
use rayon::{ThreadPool, ThreadPoolBuilder};
use server::{GrpcService, SharedState};
use tokio::{net::TcpListener, task::JoinHandle};
use tokio_stream::wrappers::TcpListenerStream;
use tokio_util::sync::CancellationToken;
use tonic::transport::Server;
use tracing::{error, info};
use zakura_geyser_plugin_interface::{
    EventEnvelope, EventSubscriptions, GeyserPlugin, PluginError, PluginResult,
};
use zakura_geyser_plugin_manager::{PluginConfig, PluginRegistry};

/// Registers the `grpc` factory with a Zakura plugin registry.
pub fn register(registry: &mut PluginRegistry) {
    registry.register("grpc", |plugin_config| {
        Ok(Box::new(from_plugin_config(plugin_config)?))
    });
}

/// Creates a plugin directly from its manager descriptor.
pub fn from_plugin_config(config: &PluginConfig) -> PluginResult<GrpcPlugin> {
    let grpc_config: Config = serde_json::from_value(config.plugin.clone()).map_err(|error| {
        PluginError::new(format!("invalid Zakura gRPC plugin configuration: {error}"))
    })?;
    grpc_config
        .validate()
        .map_err(|error| PluginError::new(error.to_string()))?;
    Ok(GrpcPlugin::new(grpc_config, config.subscriptions))
}

/// One in-process Zakura gRPC server plugin.
pub struct GrpcPlugin {
    config: Config,
    subscriptions: EventSubscriptions,
    state: Arc<SharedState>,
    next_sequence: u64,
    encoding_pool: Option<ThreadPool>,
    shutdown: Option<CancellationToken>,
    server: Option<JoinHandle<()>>,
}

impl GrpcPlugin {
    /// Creates a stopped plugin instance.
    #[must_use]
    pub fn new(config: Config, subscriptions: EventSubscriptions) -> Self {
        let state = Arc::new(SharedState::new(&config));
        Self {
            config,
            subscriptions,
            state,
            next_sequence: 0,
            encoding_pool: None,
            shutdown: None,
            server: None,
        }
    }
}

impl fmt::Debug for GrpcPlugin {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GrpcPlugin")
            .field("config", &self.config)
            .field("subscriptions", &self.subscriptions)
            .field("running", &self.server.is_some())
            .finish_non_exhaustive()
    }
}

impl GeyserPlugin for GrpcPlugin {
    fn name(&self) -> &'static str {
        "zakura-grpc"
    }

    fn subscriptions(&self) -> EventSubscriptions {
        self.subscriptions
    }

    fn on_load(&mut self) -> PluginResult {
        if self.server.is_some() {
            return Err(PluginError::new("Zakura gRPC plugin is already running"));
        }
        if self.config.event_encoding_threads > 1 && self.encoding_pool.is_none() {
            let pool = ThreadPoolBuilder::new()
                .num_threads(self.config.event_encoding_threads)
                .thread_name(|index| format!("zakura-grpc-encode-{index}"))
                .build()
                .map_err(|error| {
                    PluginError::new(format!("failed to create gRPC encoding pool: {error}"))
                })?;
            self.encoding_pool = Some(pool);
        }

        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|error| PluginError::new(format!("Tokio runtime is unavailable: {error}")))?;
        let std_listener = StdTcpListener::bind(self.config.listen_addr).map_err(|error| {
            PluginError::new(format!(
                "failed to bind Zakura gRPC listener {}: {error}",
                self.config.listen_addr
            ))
        })?;
        std_listener.set_nonblocking(true).map_err(|error| {
            PluginError::new(format!("failed to configure gRPC listener: {error}"))
        })?;
        let listener = TcpListener::from_std(std_listener).map_err(|error| {
            PluginError::new(format!("failed to create Tokio gRPC listener: {error}"))
        })?;

        let shutdown = CancellationToken::new();
        let server_shutdown = shutdown.clone();
        let service =
            GrpcService::new(Arc::clone(&self.state), &self.config).into_server(&self.config);
        let listen_addr = self.config.listen_addr;
        let adaptive_window = self.config.server_http2_adaptive_window;
        let keepalive_interval = self
            .config
            .server_http2_keepalive_interval_ms
            .map(Duration::from_millis);
        let keepalive_timeout = self
            .config
            .server_http2_keepalive_timeout_ms
            .map(Duration::from_millis);
        let connection_window = self.config.server_initial_connection_window_size;
        let stream_window = self.config.server_initial_stream_window_size;
        self.server = Some(runtime.spawn(async move {
            let (mut health_reporter, health_service) = tonic_health::server::health_reporter();
            server::mark_serving(&mut health_reporter).await;
            info!(%listen_addr, "Zakura gRPC plugin listening");

            let result = Server::builder()
                .http2_adaptive_window(adaptive_window)
                .http2_keepalive_interval(keepalive_interval)
                .http2_keepalive_timeout(keepalive_timeout)
                .initial_connection_window_size(connection_window)
                .initial_stream_window_size(stream_window)
                .add_service(health_service)
                .add_service(service)
                .serve_with_incoming_shutdown(
                    TcpListenerStream::new(listener),
                    server_shutdown.cancelled_owned(),
                )
                .await;
            if let Err(error) = result {
                error!(?error, "Zakura gRPC server stopped with an error");
            }
        }));
        self.shutdown = Some(shutdown);
        metrics::gauge!("plugin.grpc.ready").set(1.0);
        Ok(())
    }

    fn on_event(&mut self, event: Arc<EventEnvelope>) -> PluginResult {
        let event_kind = event.kind().as_str();
        let handler_started = Instant::now();
        let encode_started = Instant::now();
        let encoded = event::encode_event(
            &event,
            self.config.transaction_updates,
            self.config.utxo_updates,
            self.encoding_pool.as_ref(),
            self.config.parallel_encoding_min_transactions,
        );
        metrics::histogram!("plugin.grpc.encode.duration", "event" => event_kind)
            .record(encode_started.elapsed().as_secs_f64());
        let mut updates = encoded?;
        metrics::histogram!("plugin.grpc.updates.per_event", "event" => event_kind)
            .record(u32::try_from(updates.len()).map_or(f64::from(u32::MAX), f64::from));

        for update in &mut updates {
            self.next_sequence = self
                .next_sequence
                .checked_add(1)
                .ok_or_else(|| PluginError::new("Zakura gRPC update sequence exhausted"))?;
            update.sequence = self.next_sequence;
            metrics::counter!(
                "plugin.grpc.updates.total",
                "event" => update.event_type.to_string()
            )
            .increment(1);
        }
        let publish_started = Instant::now();
        self.state.publish_batch(updates);
        metrics::histogram!("plugin.grpc.publish.duration", "event" => event_kind)
            .record(publish_started.elapsed().as_secs_f64());
        if let Ok(event_age) = event.observed_at.elapsed() {
            metrics::histogram!("plugin.grpc.event_to_publish.duration", "event" => event_kind)
                .record(event_age.as_secs_f64());
        }
        metrics::counter!(
            "plugin.grpc.events.total",
            "event" => event_kind
        )
        .increment(1);
        metrics::histogram!("plugin.grpc.handler.duration", "event" => event_kind)
            .record(handler_started.elapsed().as_secs_f64());
        Ok(())
    }

    fn on_unload(&mut self) -> PluginResult {
        if let Some(shutdown) = self.shutdown.take() {
            shutdown.cancel();
        }
        self.server.take();
        metrics::gauge!("plugin.grpc.ready").set(0.0);
        info!("Zakura gRPC plugin stopped");
        Ok(())
    }
}
