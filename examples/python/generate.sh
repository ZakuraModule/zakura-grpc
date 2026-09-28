#!/usr/bin/env sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
PROTO_ROOT="$SCRIPT_DIR/../../zakura-grpc-proto/proto"
OUTPUT_DIR="$SCRIPT_DIR/generated"

mkdir -p "$OUTPUT_DIR"
python3 -m grpc_tools.protoc \
  --proto_path="$PROTO_ROOT" \
  --python_out="$OUTPUT_DIR" \
  --grpc_python_out="$OUTPUT_DIR" \
  "$PROTO_ROOT/zakura/geyser/v1/geyser.proto"

echo "Generated Python bindings in $OUTPUT_DIR"
