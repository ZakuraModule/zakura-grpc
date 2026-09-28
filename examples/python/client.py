#!/usr/bin/env python3
"""Minimal Python client for the Zakura Geyser API."""

import argparse
import os
import queue
import sys
from pathlib import Path
from typing import Iterator, Optional, Tuple

import grpc
from google.protobuf.json_format import MessageToJson

GENERATED = Path(__file__).resolve().parent / "generated"
sys.path.insert(0, str(GENERATED))

try:
    from zakura.geyser.v1 import geyser_pb2, geyser_pb2_grpc
except ImportError as error:
    raise SystemExit(
        "Python bindings are missing. Run ./generate.sh from examples/python first."
    ) from error


EVENT_TYPES = {
    "block-accepted": geyser_pb2.EVENT_TYPE_BLOCK_ACCEPTED,
    "best-chain-changed": geyser_pb2.EVENT_TYPE_BEST_CHAIN_CHANGED,
    "block-finalized": geyser_pb2.EVENT_TYPE_BLOCK_FINALIZED,
    "mempool-changed": geyser_pb2.EVENT_TYPE_MEMPOOL_CHANGED,
    "transaction": geyser_pb2.EVENT_TYPE_TRANSACTION,
    "utxo": geyser_pb2.EVENT_TYPE_UTXO,
    "mempool-transaction": geyser_pb2.EVENT_TYPE_MEMPOOL_TRANSACTION,
}


def non_negative(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be a non-negative integer")
    return parsed


def parse_endpoint(endpoint: str, force_tls: bool) -> Tuple[str, bool]:
    if endpoint.startswith("https://"):
        return endpoint.removeprefix("https://"), True
    if endpoint.startswith("http://"):
        return endpoint.removeprefix("http://"), force_tls
    return endpoint, force_tls


def metadata(args: argparse.Namespace) -> Tuple[Tuple[str, str], ...]:
    values = [("x-subscription-id", args.subscription_id)]
    token = args.x_token or os.environ.get("ZAKURA_GRPC_X_TOKEN")
    if token:
        values.append(("x-token", token))
    return tuple(values)


def print_message(message: object) -> None:
    print(
        MessageToJson(
            message,
            preserving_proto_field_name=True,
            indent=2,
        )
    )


def subscribe(
    stub: "geyser_pb2_grpc.GeyserStub", args: argparse.Namespace
) -> None:
    initial = geyser_pb2.SubscribeRequest(
        event_types=[EVENT_TYPES[event] for event in args.event],
        include_mempool_snapshot=args.mempool_snapshot,
    )
    if args.from_height is not None:
        initial.from_height = args.from_height

    pending = queue.Queue()

    def requests() -> Iterator["geyser_pb2.SubscribeRequest"]:
        yield initial
        while True:
            request = pending.get()
            if request is None:
                return
            yield request

    updates = stub.Subscribe(requests(), metadata=metadata(args))
    received = 0
    try:
        for update in updates:
            if update.WhichOneof("update") == "ping":
                pending.put(
                    geyser_pb2.SubscribeRequest(
                        ping=geyser_pb2.SubscribeRequestPing(id=update.ping.id)
                    )
                )
                continue
            print_message(update)
            received += 1
            if args.max_updates is not None and received >= args.max_updates:
                updates.cancel()
                return
    finally:
        pending.put(None)


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument(
        "--endpoint",
        default="http://127.0.0.1:10000",
        help="gRPC endpoint, including an optional http:// or https:// scheme",
    )
    result.add_argument("--tls", action="store_true", help="use TLS without a URL scheme")
    result.add_argument("--x-token", help="shared x-token; also read from ZAKURA_GRPC_X_TOKEN")
    result.add_argument(
        "--subscription-id",
        default="zakura-python-example",
        help="identity used by the server's concurrent subscription limit",
    )
    commands = result.add_subparsers(dest="command", required=True)
    commands.add_parser("get-version")
    commands.add_parser("replay-info")
    stream = commands.add_parser("subscribe")
    stream.add_argument(
        "--event",
        action="append",
        choices=sorted(EVENT_TYPES),
        default=[],
        help="event type to receive; repeat the option to select several",
    )
    stream.add_argument("--from-height", type=non_negative)
    stream.add_argument(
        "--mempool-snapshot",
        action="store_true",
        help="receive a revisioned mempool snapshot before live deltas",
    )
    stream.add_argument("--max-updates", type=non_negative)
    return result


def main(argv: Optional[list] = None) -> None:
    args = parser().parse_args(argv)
    target, use_tls = parse_endpoint(args.endpoint, args.tls)
    channel = (
        grpc.secure_channel(target, grpc.ssl_channel_credentials())
        if use_tls
        else grpc.insecure_channel(target)
    )
    try:
        stub = geyser_pb2_grpc.GeyserStub(channel)
        if args.command == "get-version":
            print_message(
                stub.GetVersion(geyser_pb2.GetVersionRequest(), metadata=metadata(args))
            )
        elif args.command == "replay-info":
            print_message(
                stub.SubscribeReplayInfo(
                    geyser_pb2.SubscribeReplayInfoRequest(), metadata=metadata(args)
                )
            )
        else:
            subscribe(stub, args)
    except grpc.RpcError as error:
        details = error.details() or str(error)
        raise SystemExit(f"gRPC {error.code().name}: {details}") from error
    except KeyboardInterrupt:
        pass
    finally:
        channel.close()


if __name__ == "__main__":
    main()
