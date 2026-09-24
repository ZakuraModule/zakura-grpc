# Rust client example

```sh
cargo run -p zakura-grpc-client-example -- --help
cargo run -p zakura-grpc-client-example -- replay-info
cargo run -p zakura-grpc-client-example -- subscribe --from-height 0 --reconnect
cargo run -p zakura-grpc-client-example -- subscribe \
  --event block-finalized --filter-name finalized --min-height 100
```

The subscribe command keeps the request half of the bidirectional stream alive,
so the connection continues from replay into live delivery. Use
`--max-updates` for finite smoke tests. Use `--x-token` (or
`ZAKURA_GRPC_X_TOKEN`), `--gzip`, and `--subscription-id` to exercise the
corresponding production client options.
