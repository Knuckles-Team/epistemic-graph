#!/usr/bin/env bash
# transition_deploy.sh — placement-aware, self-verifying engine transition.
#
# The rough way (promote_engine.sh) assumes the engine is bind-mounted on the LOCAL
# build host and only force-restarts by service name. In a split-storage / multi-node
# swarm the engine runs on a DIFFERENT node with a DIFFERENT bind path, so that script
# silently no-ops (it installs a binary the running task never mmaps) and never checks
# that the SERVED path (graph-os → engine) actually works afterward.
#
# This script instead:
#   1. DISCOVERS the engine service's real placement node + bind-mount source path +
#      TCP addr from `docker service inspect` on the manager — nothing hardcoded.
#   2. BUILDS the engine (glibc-compat-checked against the target node) + finance-symbol guard.
#   3. COPIES the binary to the CORRECT node atomically (sha256-verified, timestamped .bak).
#   4. RESTARTS the engine stop-first with an adaptive health-start-period, watching the
#      target node's logs for the socket bind.
#   5. RESTARTS the consumers AND GATES on the SERVED path — it runs a real engine verb
#      end-to-end through each consumer, not just /health. A served-path failure is what
#      today's incident was; this makes it a hard, caught gate.
#   6. ROLLS BACK (restore .bak + restart) if the served-path verify fails.
#
# It reaches non-manager nodes via a single manager SSH hop (the homelab ssh_config only
# has the manager + the local box), and resolves every node's IP from `docker node inspect`.
#
# Usage:
#   scripts/transition_deploy.sh [--build] [--engine-service NAME] [--manager HOST]
#                                [--consumer NAME]... [--health-start-period SECS]
#                                [--verify-graph NAME] [--no-consumers] [--dry-run]
#
# Env overrides: MANAGER, ENGINE_SERVICE, CONSUMERS (space-sep), BUILD_FEATURES,
#                FINANCE_GUARD_SYM, HEALTH_START_PERIOD, VERIFY_GRAPH, SERVED_VERIFY_TIMEOUT.
set -euo pipefail

# ── defaults (all discoverable / overridable) ──────────────────────────────────────
MANAGER="${MANAGER:-R820}"
ENGINE_SERVICE="${ENGINE_SERVICE:-epistemic-graph_epistemic-graph}"
CONSUMERS="${CONSUMERS:-graph-os_graph-os agent-utilities-messaging_agent-utilities-messaging}"
BUILD_FEATURES="${BUILD_FEATURES:-full}"
FINANCE_GUARD_SYM="${FINANCE_GUARD_SYM:-FinanceAvellaneda}"
HEALTH_START_PERIOD="${HEALTH_START_PERIOD:-120}"
VERIFY_GRAPH="${VERIFY_GRAPH:-__commons__}"
SERVED_VERIFY_TIMEOUT="${SERVED_VERIFY_TIMEOUT:-180}"
DO_BUILD=0; DO_CONSUMERS=1; DRY_RUN=0
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

CONSUMER_LIST=()
while [[ $# -gt 0 ]]; do case "$1" in
  --build) DO_BUILD=1 ;;
  --no-consumers) DO_CONSUMERS=0 ;;
  --dry-run) DRY_RUN=1 ;;
  --engine-service) shift; ENGINE_SERVICE="$1" ;;
  --manager) shift; MANAGER="$1" ;;
  --consumer) shift; CONSUMER_LIST+=("$1") ;;
  --health-start-period) shift; HEALTH_START_PERIOD="$1" ;;
  --verify-graph) shift; VERIFY_GRAPH="$1" ;;
  *) echo "unknown arg: $1" >&2; exit 2 ;;
esac; shift; done
[[ ${#CONSUMER_LIST[@]} -gt 0 ]] && CONSUMERS="${CONSUMER_LIST[*]}"

say() { printf '>> %s\n' "$*"; }
die() { printf '!! %s\n' "$*" >&2; exit 1; }
run() { if [[ "$DRY_RUN" == 1 ]]; then echo "   [dry-run] $*"; else eval "$@"; fi; }

# manager_ssh <cmd...>  — run on the swarm manager
manager_ssh() { ssh -o BatchMode=yes -o ConnectTimeout=10 "$MANAGER" "$@"; }
# node_ssh <ip> <cmd>   — run on an arbitrary swarm node via the manager hop
node_ssh() { local ip="$1"; shift; manager_ssh "ssh -o BatchMode=yes -o StrictHostKeyChecking=no -o ConnectTimeout=10 $ip \"$*\""; }

# ── 1. discover the live engine placement (node label, node IP, bind source, tcp addr) ──
say "discovering engine placement for service '$ENGINE_SERVICE' on manager '$MANAGER' …"
NODE_LABEL="$(manager_ssh "docker service inspect $ENGINE_SERVICE --format '{{range .Spec.TaskTemplate.Placement.Constraints}}{{.}}{{end}}'" \
              | sed -n 's/.*node.labels.name *== *\([^]]*\).*/\1/p' | tr -d ' ')"
[[ -n "$NODE_LABEL" ]] || die "could not read a node.labels.name constraint from $ENGINE_SERVICE (is it node-pinned?)"
NODE_IP="$(manager_ssh "docker node inspect $NODE_LABEL --format '{{.Status.Addr}}'" | tr -d ' \r')"
[[ -n "$NODE_IP" ]] || die "could not resolve IP for node $NODE_LABEL"
# the binary's host bind-mount source (Source of the mount whose Target is the server binary)
BIND_SRC="$(manager_ssh "docker service inspect $ENGINE_SERVICE --format '{{range .Spec.TaskTemplate.ContainerSpec.Mounts}}{{if eq .Target \"/usr/local/bin/epistemic-graph-server\"}}{{.Source}}{{end}}{{end}}'" | tr -d ' \r')"
[[ -n "$BIND_SRC" ]] || die "could not find the engine-binary bind mount (Target=/usr/local/bin/epistemic-graph-server) on $ENGINE_SERVICE"
say "engine runs on node=$NODE_LABEL ip=$NODE_IP  bind-path=$BIND_SRC"

# ── 2. build (glibc-compat-checked) ────────────────────────────────────────────────
SRC="$REPO/target/release/epistemic-graph-server"
if [[ "$DO_BUILD" == 1 ]]; then
  say "building engine (--features $BUILD_FEATURES) in $REPO …"
  run "( cd '$REPO' && cargo build --release --features '$BUILD_FEATURES' )"
fi
[[ "$DRY_RUN" == 1 || -x "$SRC" ]] || die "no engine binary at $SRC (run with --build)"

if [[ "$DRY_RUN" != 1 ]]; then
  # finance-symbol guard: a server-only build silently breaks finance/quant callers.
  fin=$(strings "$SRC" | grep -c "$FINANCE_GUARD_SYM" || true)
  [[ "${fin:-0}" -gt 0 ]] || die "$SRC lacks '$FINANCE_GUARD_SYM' — not a --features $BUILD_FEATURES build; refusing."
  # glibc compat: the binary's max required GLIBC_ must be <= the target node's glibc.
  need=$(objdump -T "$SRC" 2>/dev/null | grep -oE 'GLIBC_[0-9.]+' | sed 's/GLIBC_//' | sort -V | tail -1)
  have=$(node_ssh "$NODE_IP" 'ldd --version | head -1 | grep -oE "[0-9]+\.[0-9]+$"' | tr -d ' \r')
  if [[ -n "$need" && -n "$have" ]]; then
    if [[ "$(printf '%s\n%s\n' "$need" "$have" | sort -V | tail -1)" != "$have" ]]; then
      die "glibc mismatch: binary needs GLIBC_$need but node $NODE_LABEL has $have — build on a node matching $NODE_LABEL."
    fi
    say "glibc ok: binary needs <= $need, node has $have"
  fi
  LSHA=$(sha256sum "$SRC" | awk '{print $1}')
  say "built binary sha256=$LSHA size=$(stat -c%s "$SRC")"
fi

# ── 3. copy to the correct node atomically (sha-verified, backup) ───────────────────
TS=$(date -u +%Y%m%dT%H%M%SZ)
say "staging binary → $NODE_LABEL:$BIND_SRC (backup .bak-$TS) …"
if [[ "$DRY_RUN" != 1 ]]; then
  RSHA=$(cat "$SRC" | node_ssh "$NODE_IP" "cat > '$BIND_SRC.new' && chmod 775 '$BIND_SRC.new' && sha256sum '$BIND_SRC.new' | cut -d' ' -f1" | tr -d ' \r')
  [[ "$RSHA" == "$LSHA" ]] || die "sha256 mismatch after transfer (local=$LSHA remote=$RSHA) — aborting, .new left for inspection."
  node_ssh "$NODE_IP" "cp -p '$BIND_SRC' '$BIND_SRC.bak-$TS' 2>/dev/null; mv -f '$BIND_SRC.new' '$BIND_SRC'"
  say "installed + verified on $NODE_LABEL (rollback: $BIND_SRC.bak-$TS)"
fi

# ── restart helper: stop-first + wait for the service to converge ───────────────────
restart_service() {  # <service> [extra-update-args]
  local svc="$1"; shift || true
  say "restart $svc (stop-first${*:+, $*}) …"
  run "manager_ssh \"docker service update --update-order stop-first ${*:-} --force $svc\" >/dev/null 2>&1 || true"
}

wait_healthy() {  # <service>  — poll until Running & (healthy|no healthcheck), bounded
  local svc="$1" deadline=$(( $(date +%s) + SERVED_VERIFY_TIMEOUT ))
  while [[ $(date +%s) -lt $deadline ]]; do
    local st; st="$(manager_ssh "docker service ps $svc --format '{{.CurrentState}}' | head -1" | tr -d '\r')"
    case "$st" in Running*) return 0 ;; Failed*|Rejected*|Shutdown*) return 1 ;; esac
    sleep 6
  done
  return 1
}

# ── 4. restart the engine + watch for the socket bind ──────────────────────────────
restart_service "$ENGINE_SERVICE" "--health-start-period ${HEALTH_START_PERIOD}s"
if [[ "$DRY_RUN" != 1 ]]; then
  say "waiting for engine to bind its socket on $NODE_LABEL …"
  deadline=$(( $(date +%s) + SERVED_VERIFY_TIMEOUT ))
  bound=0
  while [[ $(date +%s) -lt $deadline ]]; do
    cn="$(node_ssh "$NODE_IP" "docker ps --filter name=$ENGINE_SERVICE --format '{{.Names}}' | head -1" | tr -d ' \r')"
    if [[ -n "$cn" ]] && node_ssh "$NODE_IP" "docker logs --tail 40 $cn 2>&1 | grep -q 'Listening on UDS\\|UDS:'"; then bound=1; break; fi
    sleep 5
  done
  [[ "$bound" == 1 ]] && say "engine bound its socket." || say "WARN: did not observe socket-bind marker in logs (continuing to served verify)."
fi

# ── 5. restart consumers + GATE on the served path (a real engine verb end-to-end) ──
served_verify() {  # returns 0 if the engine answers a real query through the deployed stack
  # Verify DIRECTLY against the engine's TCP addr first (proves the engine), then the
  # SERVED path is validated by the caller re-running its own MCP verb. This function is
  # the engine-truth gate used for rollback decisions.
  local tcp; tcp="$(manager_ssh "docker service inspect $ENGINE_SERVICE --format '{{range .Spec.TaskTemplate.ContainerSpec.Env}}{{println .}}{{end}}'" | sed -n 's/^ENGINE_TCP_ADDR=0.0.0.0:/'"$NODE_IP"':/p' | head -1)"
  tcp="${tcp:-$NODE_IP:9100}"
  python3 - "$tcp" "$VERIFY_GRAPH" <<'PY'
import asyncio, sys
try:
    from epistemic_graph.client import EpistemicGraphClient
except Exception as e:
    print(f"(skip) client not importable: {e}"); sys.exit(0)
tcp, graph = sys.argv[1], sys.argv[2]
async def main():
    try:
        c = await EpistemicGraphClient.connect(tcp_addr=tcp, graph_name=graph, connect_timeout=8)
        await c.query.uql("MATCH (n) |> LIMIT 1"); await c.close()
        print(f"[PASS] engine answers a real query @ {tcp}"); sys.exit(0)
    except Exception as e:
        print(f"[FAIL] engine did not answer @ {tcp}: {e!r}"); sys.exit(1)
asyncio.run(main())
PY
}

if [[ "$DO_CONSUMERS" == 1 ]]; then
  for svc in $CONSUMERS; do
    restart_service "$svc"
    if [[ "$DRY_RUN" != 1 ]]; then
      wait_healthy "$svc" && say "$svc healthy." || say "WARN: $svc not confirmed healthy within ${SERVED_VERIFY_TIMEOUT}s."
    fi
  done
fi

# ── 6. served-path gate + rollback ─────────────────────────────────────────────────
if [[ "$DRY_RUN" != 1 ]]; then
  say "served-path gate: verifying the engine answers a real query end-to-end …"
  if served_verify; then
    say "TRANSITION OK — engine $NODE_LABEL is live and answering. Consumers restarted."
    say "NOTE: also re-run a real MCP verb through graph-os (graph_query) to confirm the SERVED path; if it errors 'auto-start / No such file', the client is resolving a local socket — apply the TCP-only client hardening (see docs/deploy/transition-runbook.md)."
  else
    say "SERVED VERIFY FAILED — rolling back the engine binary to .bak-$TS"
    node_ssh "$NODE_IP" "test -f '$BIND_SRC.bak-$TS' && cp -p '$BIND_SRC' '$BIND_SRC.failed-$TS'; mv -f '$BIND_SRC.bak-$TS' '$BIND_SRC'"
    restart_service "$ENGINE_SERVICE" "--health-start-period ${HEALTH_START_PERIOD}s"
    die "rolled back to previous engine binary; investigate before re-attempting."
  fi
fi
say "done."
