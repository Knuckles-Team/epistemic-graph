---
name: epistemic-graph-deploy
skill_type: skill
description: >
  Promote and deploy the epistemic-graph engine binary (the AI-native database) into the
  live Swarm fleet, and restart its clients, safely. Use when shipping a new engine build,
  rolling out Rust-side changes, or restarting graph-os/messaging to pick up merged
  agent-utilities. Wraps scripts/promote_engine.sh.
domain: operations
license: MIT
tags: [epistemic-graph, database, operations, deploy, promotion, swarm]
metadata:
  author: Genius
  version: '0.1.0'
---

# epistemic-graph deploy / promote

The engine binary is **bind-mounted** from the host into the Swarm task, so deploy =
**replace the host binary → restart the service** (the new container re-execs it). The KG
lives in a separate snapshot/redb volume and survives the restart. Python clients
(`graph-os`, `messaging`) are **editable** installs of the canonical checkout, so Python
changes ship by **merge-to-`main` + restart** — no rebuild. Full runbook +
rationale: `docs/deploy/binary-promotion.md`.

## The one command

```bash
# build (node/full tier), atomically stage the binary, restart the engine through the
# manager, watch the first-boot migration, then live-smoke the new query surface:
scripts/promote_engine.sh --build --migrate --restore-health-period --verify
# also restart the Python consumers (they reconnect + pick up merged agent-utilities):
scripts/promote_engine.sh --restart-consumers
```

What the script does (and why), step by step:

1. **Build** `--features full` (production needs finance/quant/datascience/reasoning; a
   `server`-only build silently drops those + redb). A finance-symbol guard refuses a
   mis-built binary.
2. **Backup + atomic stage** — `cp` the live binary to `.bak-<ts>`, write the new one to a
   same-dir temp, `mv -f` over the target (atomic rename; the running engine keeps its old
   inode — no `ETXTBSY`; the next start mmaps the new one).
3. **Restart via the manager** — `ssh R820 docker service update --update-order stop-first
   --force`. `stop-first` because the engine binds a single UDS socket (start-first can't
   bind twice). This node (`RW710`) is a **worker**; service commands run on the manager.
4. **Migration-aware** (`--migrate`) — extends the healthcheck `--health-start-period` so the
   one-time `.mp`→redb first-boot migration (which binds the socket only when done) isn't
   killed into a restart loop; tails the log until `Listening on UDS`.
   `--restore-health-period` then resets it to 20s on the now-fast restart.
5. **`--verify`** — live UQL smoke (`AS OF`, `RERANK`, a parser-reject) proving the new
   surface *serves*, not just that the socket bound.

## Consumers must declare a valid backend
`graph-os` / `messaging` must run `GRAPH_BACKEND=fanout` (engine authority + durable
mirrors) or `epistemic_graph` (self-contained authority). The removed `tiered` value is
auto-migrated forward with a warning — silence it on the manager:
`docker service update --env-add GRAPH_BACKEND=fanout <svc>`.

## Worker-only fallback (no manager SSH)
If you cannot reach `R820`: `--no-restart` to stage the binary, then
`docker rm -f <engine-container-on-this-node>` to let Swarm reschedule onto the new binary.
Expect transient duplicate/zombie churn — reconcile to one healthy task (see
`epistemic-graph-troubleshooting`). Prefer the manager path.

## Rollback
`mv ${WORKSPACE_ROOT}/.venv/bin/epistemic-graph-server.bak-<ts>
   ${WORKSPACE_ROOT}/.venv/bin/epistemic-graph-server` then restart the engine service.

## Related
- `epistemic-graph-migrations` — snapshot/redb + config/schema migrations, version flips.
- `epistemic-graph-troubleshooting` — when a deploy leaves something unhealthy.
