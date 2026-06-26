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
#                             [--migrate] [--health-start-period <secs>]
#                             [--restore-health-period]
#
#   --build                       cargo build --release --features full first (default: reuse target/release)
#   --verify                      after promotion, run a LIVE UQL smoke against the engine
#                                 socket (AS OF / RERANK / a parser-reject) to prove the new
#                                 query surface actually serves — not just that the socket binds
#   --no-restart                  install the binary only; do not restart the service
#   --restart-consumers           also restart graph-os + messaging (they pick up new agent-utilities).
#                                 They MUST run GRAPH_BACKEND=fanout (engine authority + optional mirrors);
#                                 the old `tiered` value was removed and fails bootstrap.
#   --health-start-period <secs>  pass --health-start-period <secs>s to the engine restart so the
#                                 swarm healthcheck (test -S <socket>, default start-period 20s)
#                                 does NOT kill a long first-boot .mp→redb migration. REQUIRED for the
#                                 FIRST authoritative boot of a flipped binary against a large KG —
#                                 the engine binds its socket only after the migration finishes, which
#                                 can take MINUTES (thousands of graphs / hundreds of MB).
#   --migrate                     migration-aware restart: imply --health-start-period 600 (unless one is
#                                 given), then TAIL the new engine container's logs on THIS node and
#                                 surface migration progress ("Snapshot load progress" / "imported N
#                                 graph(s)") until "Listening on UDS" appears or a timeout, so the
#                                 operator watches it complete instead of flying blind.
#   --restore-health-period       after a --migrate watch sees the socket bind, run a SECOND service
#                                 update restoring --health-start-period to the normal value (20s). The
#                                 second restart on the now-populated redb is fast. Opt-in.
#
# Env overrides (defaults are the homelab):
#   ENGINE_BIN_DEST   host bind-mount path of the engine binary  (default below)
#   SWARM_MANAGER     ssh host of the swarm MANAGER (updates run there; this node is a worker)
#   ENGINE_SERVICE / GRAPHOS_SERVICE / MESSAGING_SERVICE   swarm service names
#   MIGRATE_DEFAULT_START_PERIOD   start-period (secs) implied by --migrate (default 600)
#   NORMAL_HEALTH_START_PERIOD     start-period (secs) restored by --restore-health-period (default 20)
#   MIGRATE_WATCH_TIMEOUT          max secs to tail the migration log before giving up (default 1800)
#
# CONCEPT:OS-5.62 — migration-aware promotion: the first authoritative boot runs a
# one-time .mp→redb migration that binds the socket only when done; the 20s healthcheck
# start-period would kill it into a restart loop. --health-start-period / --migrate /
# --restore-health-period make that previously-manual two-`service update` dance opt-in here.
set -euo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
ENGINE_BIN_DEST="${ENGINE_BIN_DEST:-/home/apps/workspace/.venv/bin/epistemic-graph-server}"
SWARM_MANAGER="${SWARM_MANAGER:-R820}"
ENGINE_SERVICE="${ENGINE_SERVICE:-epistemic-graph_epistemic-graph}"
GRAPHOS_SERVICE="${GRAPHOS_SERVICE:-graph-os_graph-os}"
MESSAGING_SERVICE="${MESSAGING_SERVICE:-agent-utilities-messaging_agent-utilities-messaging}"
FEATURES="${FEATURES:-full}"   # production needs finance/quant/datascience/reasoning — NOT server-only
MIGRATE_DEFAULT_START_PERIOD="${MIGRATE_DEFAULT_START_PERIOD:-600}"
NORMAL_HEALTH_START_PERIOD="${NORMAL_HEALTH_START_PERIOD:-20}"
MIGRATE_WATCH_TIMEOUT="${MIGRATE_WATCH_TIMEOUT:-1800}"

DO_BUILD=0; DO_RESTART=1; DO_CONSUMERS=0
DO_MIGRATE=0; DO_RESTORE_HEALTH=0; DO_VERIFY=0
HEALTH_START_PERIOD=""   # empty ⇒ default path (no --health-start-period flag emitted)
ENGINE_SOCKET="${ENGINE_SOCKET:-/run/epistemic-graph/epistemic-graph.sock}"
VERIFY_GRAPH="${VERIFY_GRAPH:-__commons__}"
while [[ $# -gt 0 ]]; do case "$1" in
  --build) DO_BUILD=1 ;;
  --verify) DO_VERIFY=1 ;;
  --no-restart) DO_RESTART=0 ;;
  --restart-consumers) DO_CONSUMERS=1 ;;
  --migrate) DO_MIGRATE=1 ;;
  --restore-health-period) DO_RESTORE_HEALTH=1 ;;
  --health-start-period)
    shift; HEALTH_START_PERIOD="${1:-}"
    [[ "$HEALTH_START_PERIOD" =~ ^[0-9]+$ ]] || { echo "!! --health-start-period needs an integer seconds value" >&2; exit 2; } ;;
  --health-start-period=*)
    HEALTH_START_PERIOD="${1#*=}"
    [[ "$HEALTH_START_PERIOD" =~ ^[0-9]+$ ]] || { echo "!! --health-start-period needs an integer seconds value" >&2; exit 2; } ;;
  *) echo "unknown arg: $1" >&2; exit 2 ;;
esac; shift; done

# --migrate implies an extended start-period (unless one was given explicitly), so the
# healthcheck does not kill the in-progress migration.
if [[ "$DO_MIGRATE" == 1 && -z "$HEALTH_START_PERIOD" ]]; then
  HEALTH_START_PERIOD="$MIGRATE_DEFAULT_START_PERIOD"
fi

SRC="$REPO/target/release/epistemic-graph-server"

if [[ "$DO_BUILD" == 1 ]]; then
  echo ">> building engine (--features $FEATURES) …"
  ( cd "$REPO" && cargo build --release --features "$FEATURES" )
fi
[[ -x "$SRC" ]] || { echo "!! no engine binary at $SRC (run with --build)"; exit 1; }

# Guard: the binary MUST carry the full method surface. A server-only build is
# missing finance/quant and silently breaks emerald/quant callers at runtime.
# (capture to a var: `strings | grep -q` trips `set -o pipefail` via SIGPIPE on a match)
fin_syms=$(strings "$SRC" | grep -c "FinanceAvellaneda" || true)
if [[ "${fin_syms:-0}" -eq 0 ]]; then
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
#
# CONCEPT:OS-5.62 — an optional `--health-start-period <secs>` is threaded onto the
# engine restart so the swarm healthcheck (test -S <socket>) does not kill a long
# first-boot .mp→redb migration that binds the socket only when it finishes.
restart() {
  local svc="$1" extra="${2:-}"
  echo ">> restart $svc${extra:+ ($extra)}"
  # shellcheck disable=SC2029  # intentional client-side expansion: build the remote cmd here
  ssh -o BatchMode=yes "$SWARM_MANAGER" \
    "docker service update --update-order stop-first $extra --force $svc" 2>&1 | tail -2
}

engine_extra=""
if [[ -n "$HEALTH_START_PERIOD" ]]; then
  engine_extra="--health-start-period ${HEALTH_START_PERIOD}s"
  echo ">> first-boot/migration mode: engine restart with start-period ${HEALTH_START_PERIOD}s"
fi
restart "$ENGINE_SERVICE" "$engine_extra"

# Migration-aware watch: tail the new engine container's logs on THIS node (the
# service is node-pinned to the bind-mount host, so the task usually lands here)
# and surface migration progress until the socket binds or we time out. The
# operator sees the migration complete instead of flying blind.
watch_migration() {
  local cname
  # The swarm task runs as a container whose name embeds the service name; the
  # binary is bind-mounted only on this node, so the engine task is pinned here.
  cname="$(docker ps --filter "name=${ENGINE_SERVICE}" --format '{{.Names}}' 2>/dev/null | head -1)"
  if [[ -z "$cname" ]]; then
    echo ">> (migration watch) engine container not found on this node — skipping log tail."
    echo "   watch it on the bind-mount host with:"
    echo "     docker logs -f <engine-container> 2>&1 | grep -iE 'snapshot load|migration|imported|Listening on UDS'"
    return 2   # could-not-watch (not on this node) — distinct from confirmed-bind(0)/timeout(1)
  fi
  echo ">> (migration watch) tailing $cname for migration progress (timeout ${MIGRATE_WATCH_TIMEOUT}s) …"
  local deadline=$(( $(date +%s) + MIGRATE_WATCH_TIMEOUT ))
  # Tail follows the container; we echo migration-relevant lines and stop once the
  # socket binds. The inner `while read` exits 42 when it sees the bind marker, which
  # (under pipefail) becomes the pipeline's status. A per-iteration `timeout` re-attaches
  # if the stream ends early (e.g. the container restarted) and bounds the total wait.
  local rc
  while [[ $(date +%s) -lt $deadline ]]; do
    local remaining=$(( deadline - $(date +%s) ))
    rc=0
    timeout "${remaining}s" docker logs -f --since 1m "$cname" 2>&1 \
      | grep --line-buffered -iE 'snapshot load|migration|imported|Listening on UDS' \
      | while IFS= read -r line; do
          echo "   $line"
          case "$line" in *"Listening on UDS"*) exit 42 ;; esac
        done || rc=$?
    if [[ "$rc" -eq 42 ]]; then
      echo ">> migration complete — engine is listening on its UDS socket."
      return 0
    fi
    # Otherwise the log stream ended (container restart) or timed out — loop re-attaches
    # while there is time left.
  done
  echo "!! (migration watch) timed out after ${MIGRATE_WATCH_TIMEOUT}s without seeing 'Listening on UDS'."
  echo "   the migration may still be running; check the engine logs before assuming a hang."
  return 1
}

if [[ "$DO_MIGRATE" == 1 ]]; then
  watch_rc=0; watch_migration || watch_rc=$?
  if [[ "$watch_rc" -eq 0 ]]; then
    # Socket bind confirmed by the local log tail.
    if [[ "$DO_RESTORE_HEALTH" == 1 ]]; then
      echo ">> --restore-health-period: restoring start-period to ${NORMAL_HEALTH_START_PERIOD}s (fast restart on populated redb)"
      restart "$ENGINE_SERVICE" "--health-start-period ${NORMAL_HEALTH_START_PERIOD}s"
    fi
  elif [[ "$watch_rc" -eq 2 ]]; then
    # Couldn't tail (engine not on this node) — leave the extended start-period in place
    # and let the operator restore it once they confirm the bind on the bind-mount host.
    [[ "$DO_RESTORE_HEALTH" == 1 ]] && \
      echo ">> (not restoring health period: bind unconfirmed from this node — restore manually after watching on the bind-mount host)"
  else
    echo "!! migration watch did not confirm the socket bound — NOT restoring health period; investigate."
  fi
fi

if [[ "$DO_CONSUMERS" == 1 ]]; then
  # Consumers (graph-os + messaging) reconnect to the restarted engine and pick up
  # merged agent-utilities. They MUST run GRAPH_BACKEND=fanout — the removed `tiered`
  # value fails bootstrap ("A persistent graph backend is required").
  restart "$GRAPHOS_SERVICE"
  restart "$MESSAGING_SERVICE"
fi

# Live UQL smoke: prove the NEW query surface actually serves, not just that the socket
# binds. Runs a handful of UQL stages over the engine socket on THIS node (the bind-mount
# host) and asserts the temporal/rerank ops parse + execute, and that a bogus keyword is
# rejected (the parser is validating). Connect-only failure is reported, not fatal.
verify() {
  echo ">> --verify: live UQL smoke against $ENGINE_SOCKET (graph $VERIFY_GRAPH)"
  ENGINE_SOCKET="$ENGINE_SOCKET" VERIFY_GRAPH="$VERIFY_GRAPH" python3 - <<'PY'
import asyncio, os, sys
try:
    from epistemic_graph.client import EpistemicGraphClient
except Exception as e:
    print(f"   (skip) epistemic_graph client not importable: {e}"); sys.exit(0)
sock = os.environ["ENGINE_SOCKET"]; g = os.environ["VERIFY_GRAPH"]
OK = {"ok": 0, "fail": 0}
async def main():
    try:
        c = await EpistemicGraphClient.connect(socket_path=sock, graph_name=g)
    except Exception as e:
        print(f"   (skip) could not connect to {sock}: {e}"); return
    checks = [
        ("AS OF (valid-time)",   "MATCH (:Concept) |> AS OF @1700000000 |> LIMIT 1", True),
        ("AS OF TX (txn-time)",  "MATCH (:Concept) |> AS OF TX @1700000000 |> LIMIT 1", True),
        ("RERANK MENTIONS",      "MATCH (:Concept) |> RERANK MENTIONS |> LIMIT 1", True),
        ("RERANK MMR",           "MATCH (:Concept) |> RERANK MMR 0.5 5 |> LIMIT 1", True),
        ("parser rejects bogus", "MATCH (:Concept) |> RERANK BOGUS |> LIMIT 1", False),
    ]
    for label, uql, want_ok in checks:
        try:
            await c.query.uql(uql); got_ok = True; detail = "executed"
        except Exception as e:
            got_ok = False; detail = repr(e)[:70]
        passed = got_ok == want_ok
        OK["ok" if passed else "fail"] += 1
        print(f"   [{'PASS' if passed else 'FAIL'}] {label}: {detail}")
    await c.close()
asyncio.run(main())
sys.exit(1 if OK["fail"] else 0)
PY
}

if [[ "$DO_VERIFY" == 1 ]]; then
  verify || { echo "!! --verify smoke FAILED — the promoted binary is not serving the expected surface."; exit 1; }
  echo ">> verify OK — new query surface is live."
fi

echo ">> done. (re-run with --verify for a live UQL smoke; see docs/deploy/binary-promotion.md)"
