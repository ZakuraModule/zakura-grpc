# Python quickstart

Create an isolated environment, install gRPC, and generate bindings from the
repository's canonical protobuf file:

```sh
cd examples/python
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements.txt
./generate.sh
python client.py get-version
python client.py replay-info
python client.py subscribe --event transaction --event utxo
```

Use `--endpoint https://node.example:10000` for TLS and either
`--x-token secret` or `ZAKURA_GRPC_X_TOKEN=secret` for authentication. Add
`--from-height 1000000` to replay retained block-scoped events before following
the live stream.
