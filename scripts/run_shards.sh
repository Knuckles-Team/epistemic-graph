#!/usr/bin/env bash
# Launch N epistemic-graph-server shards, one per core, each on its own UDS
# path, sharing the HMAC secret via the environment (Plan 01 Step 7).
#
# Usage:
#   EPISTEMIC_GRAPH_SECRET=... scripts/run_shards.sh [N]
#
# Env:
#   EPISTEMIC_GRAPH_SECRET     HMAC secret shared by all shards (required in prod).
#   EPISTEMIC_GRAPH_SHARD_DIR  Directory for shard sockets (default /run/epistemic-graph).
#   EPISTEMIC_GRAPH_MAX_INFLIGHT  Per-shard backpressure cap (default 1024).
#   N (arg 1)                  Shard count (default: number of CPU cores).
set -euo pipefail

BIN="${EPISTEMIC_GRAPH_BIN:-$(dirname "$0")/../target/release/epistemic-graph-server}"
SHARD_DIR="${EPISTEMIC_GRAPH_SHARD_DIR:-/run/epistemic-graph}"
N="${1:-$(nproc 2>/dev/null || echo 4)}"

if [[ ! -x "$BIN" ]]; then
  echo "error: server binary not found at $BIN (build with: cargo build --release --features server)" >&2
  exit 1
fi
if [[ -z "${EPISTEMIC_GRAPH_SECRET:-}" ]]; then
  echo "warning: EPISTEMIC_GRAPH_SECRET is unset; shards will run unauthenticated" >&2
fi
# The server binary reads the HMAC secret from GRAPH_SERVICE_AUTH_SECRET.
export GRAPH_SERVICE_AUTH_SECRET="${EPISTEMIC_GRAPH_SECRET:-${GRAPH_SERVICE_AUTH_SECRET:-}}"

mkdir -p "$SHARD_DIR"
echo "Launching $N shard(s) under $SHARD_DIR"

pids=()
cleanup() {
  echo "Stopping shards..."
  for pid in "${pids[@]}"; do kill "$pid" 2>/dev/null || true; done
}
trap cleanup INT TERM EXIT

endpoints=()
for ((i = 0; i < N; i++)); do
  sock="$SHARD_DIR/shard-$i.sock"
  endpoints+=("$sock")
  "$BIN" --socket-path "$sock" &
  pids+=($!)
  echo "  shard $i -> $sock (pid ${pids[-1]})"
done

# Emit the endpoint list so callers can export GRAPH_SERVICE_ENDPOINTS,
# which AgentConfig reads to populate the ShardRouter (config.py:382).
printf 'GRAPH_SERVICE_ENDPOINTS=%s\n' "$(IFS=,; echo "${endpoints[*]}")"

wait
