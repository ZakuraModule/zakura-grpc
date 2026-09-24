# Rust client example

```sh
cargo run -p zakura-grpc-client-example -- --help
cargo run -p zakura-grpc-client-example -- replay-info
cargo run -p zakura-grpc-client-example -- subscribe --from-height 0
```

The subscribe command keeps the request half of the bidirectional stream alive,
so the connection continues from replay into live delivery. Use
`--max-updates` for finite smoke tests.

