# Zakura gRPC

Yellowstone-style gRPC streaming for Zakura. The repository uses the same
separation of concerns as `yellowstone-grpc`:

```text
zakura-grpc-proto       generated protobuf messages and gRPC services
zakura-grpc-client      reusable Rust client and bidirectional subscription API
zakura-grpc-geyser      in-process Zakura plugin and gRPC server
examples/rust           command-line client examples
```

The plugin keeps a short, process-local replay window. Kafka and durable
historical replay remain separate downstream services.

The implementation follows Yellowstone's component boundaries and operational
model, adapted to Zcash heights and Zakura events rather than Solana slots,
accounts, and transaction notifications:

- bidirectional subscriptions with live filter replacement and subscription ping;
- named filters, with every matching name returned on the update;
- bounded live/replay/client queues and lagging-client disconnects;
- optional `x-token` authentication and per-subscriber concurrent stream limits;
- gzip/zstd, gRPC health, HTTP/2 flow-control and keepalive configuration;
- per-transaction raw consensus payloads and transparent UTXO create/spend updates;
- a Rust client builder with TLS, transport tuning, automatic reconnect,
  checkpoint replay, and client-side duplicate suppression.

## Development layout

Until the Zakura Geyser interface crates are published, clone both repositories
as siblings:

```text
zakura/
├── zakura/
└── zakura-grpc/
```

## Node configuration

```toml
[geyser]
enabled = true
shutdown_timeout = "5s"
total_queue_bytes = 67108864

[[geyser.plugins]]
name = "grpc-main"
kind = "grpc"
required_at_startup = true
failure_policy = "disable_plugin"

[geyser.plugins.subscriptions]
block_accepted = true
best_chain = true
finalized_blocks = true
mempool = true

[geyser.plugins.queue]
capacity_events = 4096
capacity_bytes = 67108864
overflow = "disable_plugin"

[geyser.plugins.plugin]
listen_addr = "127.0.0.1:10000"
transaction_updates = true
utxo_updates = true
replay_stored_blocks = 150
broadcast_capacity = 100000
client_channel_capacity = 10000
max_decoding_message_size = 4194304
max_encoding_message_size = 52428800
compression = { accept = ["gzip", "zstd"], send = ["gzip", "zstd"] }
# x_token = "replace-me"
subscription_limit = 1000
subscription_limit_enforce = false
filter_limits = { max_named_filters = 32, max_event_types = 6, max_name_bytes = 128, allow_all = true }
server_http2_adaptive_window = true
# server_http2_keepalive_interval_ms = 30000
# server_http2_keepalive_timeout_ms = 10000
```

Validate a standalone JSON or TOML file containing the fields from the plugin
table:

```sh
cargo run -p zakura-grpc-geyser --bin config-check -- ./grpc-plugin.toml
```

## Example client

Inspect the retained replay range:

```sh
cargo run -p zakura-grpc-client-example -- \
  --endpoint http://127.0.0.1:10000 replay-info
```

Replay from a retained height and continue following live events:

```sh
cargo run -p zakura-grpc-client-example -- \
  --endpoint http://127.0.0.1:10000 subscribe \
  --from-height 1000000 --reconnect
```

Filter to finalized blocks only:

```sh
cargo run -p zakura-grpc-client-example -- \
  --endpoint http://127.0.0.1:10000 subscribe \
  --event block-finalized --filter-name finalized --min-height 1000000
```

Stream consensus-encoded transactions and their block metadata:

```sh
cargo run -p zakura-grpc-client-example -- \
  --endpoint http://127.0.0.1:10000 subscribe \
  --event transaction --filter-name transactions --reconnect
```

Stream transparent outputs created and previous outpoints spent by each
transaction:

```sh
cargo run -p zakura-grpc-client-example -- \
  --endpoint http://127.0.0.1:10000 subscribe \
  --event utxo --filter-name utxos --reconnect
```

Transaction and UTXO updates carry `ACCEPTED` or `FINALIZED` commitment. An
accepted block is validated but can still belong to a side chain or be removed
by a reorganization. Consumers that require stable canonical data should apply
finalized updates.

Use `--x-token` (or `ZAKURA_GRPC_X_TOKEN`) when authentication is configured,
and `--gzip` to negotiate gzip. `health`, `ping`, `get-version`, and
`replay-info` exercise the unary/health APIs.

The reusable client exposes the same builder shape used by Yellowstone:

```rust,no_run
use std::time::Duration;

use zakura_grpc_client::{ReconnectConfig, ZakuraGrpcClient};

# async fn connect() -> Result<(), Box<dyn std::error::Error>> {
let mut client = ZakuraGrpcClient::build_from_shared("https://node.example:10000")?
    .x_token(Some("secret"))?
    .subscription_id(Some("indexer-a"))?
    .connect_timeout(Duration::from_secs(5))
    .set_reconnect_config(ReconnectConfig::default())
    .connect()
    .await?;
let (_requests, mut updates) = client.subscribe().await?;
while let Some(update) = updates.message().await? {
    println!("{}", update.sequence);
}
# Ok(())
# }
```

`from_height` is accepted on the initial subscription request. If the requested
height has been evicted, the server returns `OUT_OF_RANGE`; clients can query
`SubscribeReplayInfo` to discover the first retained height.

## Delivery model

- The Zakura plugin manager isolates node callbacks from gRPC work.
- The live broadcast ring and every client queue are bounded.
- Lagging clients are disconnected and can reconnect using `from_height`.
- Replay data disappears when the node restarts.
- The reconnecting Rust client resumes from a height checkpoint and suppresses
  replay duplicates with `(session_id, sequence)`.
- Full block payloads contain consensus-encoded Zcash block bytes.
- Transaction payloads contain consensus-encoded transaction bytes, txid,
  unmined ID, optional ZIP-244 auth digest, block position, and commitment.
- UTXO payloads contain ordered transparent create/spend effects. Spend updates
  identify the previous outpoint but do not repeat the previous output value.

## Yellowstone scope mapping

| Yellowstone concept | Zakura equivalent |
| --- | --- |
| slot replay (`from_slot`) | block-height replay (`from_height`) |
| account/transaction/entry filters | UTXO, transaction, block, best-chain, finalized, and mempool event filters |
| commitment broadcasts | explicit accepted/best-chain/finalized event types |
| validator Geyser callbacks | Zakura plugin-interface event envelopes |
| short in-memory replay | short in-memory replay (same operational role) |
| Kafka persistence | separate downstream plugin/service, not part of this server |

Solana-specific account memcmp filters, deshred/gossip streams, and Solana
unary methods are intentionally not mirrored. Zakura UTXO updates model the
transparent UTXO effects of Zcash transactions rather than Solana accounts.
Durable historical replay should remain a separate Kafka or storage consumer
so consensus and state tasks never wait for it.
