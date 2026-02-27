#!/usr/bin/env bash
# Generate Python protobuf classes from the Esri FeatureCollection .proto file.
#
# Usage:
#   ./scripts/generate_proto.sh
#
# Requires: protoc (Protocol Buffers compiler)
#   Install: apt-get install protobuf-compiler
#   Or via pip: pip install grpcio-tools
#       Then use: python -m grpc_tools.protoc ...

set -euo pipefail

PROTO_DIR="src/iceberg_geo/geoservices/proto"
PROTO_FILE="${PROTO_DIR}/FeatureCollection.proto"

if ! command -v protoc &> /dev/null; then
    echo "protoc not found, trying grpc_tools..."
    python -m grpc_tools.protoc \
        --python_out="${PROTO_DIR}/" \
        --proto_path="${PROTO_DIR}/" \
        "${PROTO_FILE}"
else
    protoc \
        --python_out="${PROTO_DIR}/" \
        --proto_path="${PROTO_DIR}/" \
        "${PROTO_FILE}"
fi

echo "Generated: ${PROTO_DIR}/FeatureCollection_pb2.py"
