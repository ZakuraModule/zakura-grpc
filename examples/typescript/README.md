# TypeScript quickstart

This example loads the repository's protobuf schema at runtime, so generated
files cannot drift from the server API.

```sh
cd examples/typescript
npm install
npm run start -- get-version
npm run start -- replay-info
npm run start -- subscribe --event transaction --event utxo
npm run start -- subscribe --event mempool-changed --mempool-snapshot
```

Use `--endpoint https://node.example:10000` for TLS and either
`--x-token secret` or `ZAKURA_GRPC_X_TOKEN=secret` for authentication. Add
`--from-height 1000000` to replay retained block-scoped events before following
the live stream. Subscriptions consume the Node stream with an async iterator,
honor write backpressure, reconnect with exponential backoff, and resume from a
verified finalized block when one has been observed. `--mempool-snapshot`
sends `BEGIN`, the snapshot contents, and `END` before revisioned live deltas.
