#!/usr/bin/env bash
# Launch N medulla-server shards, one per core, each on its own UDS
# path, sharing the HMAC secret via the environment (Plan 01 Step 7).
#
# Usage:
#   MEDULLA_SECRET=... scripts/run_shards.sh [N]
#
# Env:
#   MEDULLA_SECRET     HMAC secret shared by all shards (required).
#   MEDULLA_SHARD_DIR  Directory for shard sockets (default /run/medulla).
#   MEDULLA_SHARD_STATE_DIR  Durable state root, one subdirectory per shard.
#   MEDULLA_MAX_INFLIGHT  Per-shard backpressure cap (default 1024).
#   N (arg 1)                  Shard count (default: number of CPU cores).
set -euo pipefail

BIN="${MEDULLA_BIN:-$(dirname "$0")/../target/release/medulla-server}"
SHARD_DIR="${MEDULLA_SHARD_DIR:-/run/medulla}"
STATE_DIR="${MEDULLA_SHARD_STATE_DIR:-}"
N="${1:-$(nproc 2>/dev/null || echo 4)}"

if [[ ! -x "$BIN" ]]; then
  echo "error: server binary not found at $BIN (build with: cargo build --release --features server)" >&2
  exit 1
fi
if [[ -z "${MEDULLA_SECRET:-}" && -z "${GRAPH_SERVICE_AUTH_SECRET:-}" ]]; then
  echo "error: no auth secret set — set MEDULLA_SECRET or GRAPH_SERVICE_AUTH_SECRET." >&2
  exit 1
fi
if [[ -z "$STATE_DIR" ]]; then
  echo "error: MEDULLA_SHARD_STATE_DIR is required for durable policy and replay state." >&2
  exit 1
fi
for required in MEDULLA_AUDIENCE MEDULLA_TENANT MEDULLA_POLICY_VERSION MEDULLA_SIGNER_KEYS_JSON; do
  if [[ -z "${!required:-}" ]]; then
    echo "error: $required is required by the current verified-context protocol." >&2
    exit 1
  fi
fi
# The server binary reads the HMAC secret from GRAPH_SERVICE_AUTH_SECRET.
export GRAPH_SERVICE_AUTH_SECRET="${MEDULLA_SECRET:-${GRAPH_SERVICE_AUTH_SECRET:-}}"

mkdir -p "$SHARD_DIR" "$STATE_DIR"
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
  "$BIN" --socket-path "$sock" --persist-dir "$STATE_DIR/shard-$i" &
  pids+=($!)
  echo "  shard $i -> $sock (pid ${pids[-1]})"
done

# Emit the endpoint list so callers can export GRAPH_SERVICE_ENDPOINTS,
# which AgentConfig reads to populate the ShardRouter (config.py:382).
printf 'GRAPH_SERVICE_ENDPOINTS=%s\n' "$(IFS=,; echo "${endpoints[*]}")"

wait
