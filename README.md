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
replay_stored_blocks = 150
broadcast_capacity = 100000
client_channel_capacity = 10000
max_decoding_message_size = 4194304
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
  --endpoint http://127.0.0.1:10000 subscribe --from-height 1000000
```

Filter to finalized blocks only:

```sh
cargo run -p zakura-grpc-client-example -- \
  --endpoint http://127.0.0.1:10000 subscribe \
  --event block-finalized
```

`from_height` is accepted on the initial subscription request. If the requested
height has been evicted, the server returns `OUT_OF_RANGE`; clients can query
`SubscribeReplayInfo` to discover the first retained height.

## Delivery model

- The Zakura plugin manager isolates node callbacks from gRPC work.
- The live broadcast ring and every client queue are bounded.
- Lagging clients are disconnected and can reconnect using `from_height`.
- Replay data disappears when the node restarts.
- Delivery across reconnects is at-least-once; deduplicate with
  `(session_id, sequence)`.
- Full block payloads contain consensus-encoded Zcash block bytes.
