#!/usr/bin/env bash
# Promote a freshly-built epistemic-graph engine binary into the live homelab.
#
# The engine runs as a Docker Swarm service whose binary is BIND-MOUNTED from the
# host path below — so "deploy" is: build → atomically replace the host binary →
# force-restart the service (which re-execs the new binary). The KG data is in a
# separate snapshot volume and survives the restart. See
# docs/deploy/binary-promotion.md for the full runbook + rationale.
#
# Usage:
#   scripts/promote_engine.sh [--build] [--no-restart] [--restart-consumers]
#
#   --build              cargo build --release --features full first (default: reuse target/release)
#   --no-restart         install the binary only; do not restart the service
#   --restart-consumers  also restart graph-os + messaging (they pick up new agent-utilities)
#
# Env overrides (defaults are the homelab):
#   ENGINE_BIN_DEST   host bind-mount path of the engine binary  (default below)
#   SWARM_MANAGER     ssh host of the swarm MANAGER (updates run there; this node is a worker)
#   ENGINE_SERVICE / GRAPHOS_SERVICE / MESSAGING_SERVICE   swarm service names
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENGINE_BIN_DEST="${ENGINE_BIN_DEST:-/home/apps/workspace/.venv/bin/epistemic-graph-server}"
SWARM_MANAGER="${SWARM_MANAGER:-R820}"
ENGINE_SERVICE="${ENGINE_SERVICE:-epistemic-graph_epistemic-graph}"
GRAPHOS_SERVICE="${GRAPHOS_SERVICE:-graph-os_graph-os}"
MESSAGING_SERVICE="${MESSAGING_SERVICE:-agent-utilities-messaging_agent-utilities-messaging}"
FEATURES="${FEATURES:-full}"   # production needs finance/quant/datascience/reasoning — NOT server-only

DO_BUILD=0; DO_RESTART=1; DO_CONSUMERS=0
for a in "$@"; do case "$a" in
  --build) DO_BUILD=1 ;;
  --no-restart) DO_RESTART=0 ;;
  --restart-consumers) DO_CONSUMERS=1 ;;
  *) echo "unknown arg: $a" >&2; exit 2 ;;
esac; done

SRC="$REPO/target/release/epistemic-graph-server"

if [[ "$DO_BUILD" == 1 ]]; then
  echo ">> building engine (--features $FEATURES) …"
  ( cd "$REPO" && cargo build --release --features "$FEATURES" )
fi
[[ -x "$SRC" ]] || { echo "!! no engine binary at $SRC (run with --build)"; exit 1; }

# Guard: the binary MUST carry the full method surface. A server-only build is
# missing finance/quant and silently breaks emerald/quant callers at runtime.
if ! strings "$SRC" | grep -q "FinanceAvellaneda"; then
  echo "!! $SRC lacks finance symbols — was it built with --features $FEATURES? refusing."; exit 1
fi

echo ">> backing up + atomically installing → $ENGINE_BIN_DEST"
# Same-dir temp + mv = atomic rename: the running engine keeps its old inode
# (no ETXTBSY); the next container start mmaps the new one.
cp -p "$ENGINE_BIN_DEST" "${ENGINE_BIN_DEST}.bak-$(date -u +%Y%m%dT%H%M%SZ)" 2>/dev/null || true
cp "$SRC" "${ENGINE_BIN_DEST}.new"
mv -f "${ENGINE_BIN_DEST}.new" "$ENGINE_BIN_DEST"
echo "   installed $(ls -la "$ENGINE_BIN_DEST" | awk '{print $5, $9}')"

if [[ "$DO_RESTART" == 0 ]]; then
  echo ">> --no-restart: binary staged; restart $ENGINE_SERVICE to activate."; exit 0
fi

# IMPORTANT: --update-order stop-first. The engine binds a single UDS socket, so
# start-first fails ("address in use" → task exits 1). stop-first releases the
# socket before the new task binds it. Brief (~seconds) engine downtime; the KG
# snapshot volume persists, consumers reconnect.
restart() { echo ">> restart $1"; ssh -o BatchMode=yes "$SWARM_MANAGER" \
  "docker service update --update-order stop-first --force $1" 2>&1 | tail -2; }

restart "$ENGINE_SERVICE"
if [[ "$DO_CONSUMERS" == 1 ]]; then
  restart "$GRAPHOS_SERVICE"
  restart "$MESSAGING_SERVICE"
fi

echo ">> done. verify with:"
echo "   python -c \"from epistemic_graph.client import SyncEpistemicGraphClient as C; \\"
echo "   print(C.connect(socket_path='/run/epistemic-graph/epistemic-graph.sock').graph.match_ontology_terms('portainer'))\""
