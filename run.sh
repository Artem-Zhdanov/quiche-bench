#!/bin/bash
set -euo pipefail
set -x

if [ $# -lt 1 ]; then
    echo "Usage: $0 <instance>"
    exit 1
fi

INSTANCE="$1"

OTEL_RESOURCE_ATTRIBUTES=service.namespace=abc,service.instance.id=instance-${INSTANCE} \
OTEL_EXPORTER_OTLP_METRICS_ENDPOINT=http://n25.servers.gosh.sh:4316 \
RUST_LOG=info  \
cargo run --bin main --release  -- --config test-configs/config-${INSTANCE}.yaml
