---
name: epistemic-graph-migrations
description: >
  Upgrade and migrate a live epistemic-graph engine (the AI-native database): binary
  version flips, the one-time snapshot/.mp -> redb authoritative migration, config/env
  migrations (e.g. retired GRAPH_BACKEND values), and on-disk data-format evolution
  (bi-temporal fields, persisted blobs). Use when raising the engine version, flipping to
  authoritative persistence, or evolving a stored format.
domain: operations
license: MIT
tags: [epistemic-graph, database, operations, migration, upgrade, redb, persistence]
metadata:
  author: Genius
  version: '0.1.0'
---

# epistemic-graph migrations & upgrades

Principles (from the engine's No-Legacy rule): **code carries no back-compat**, but
**persisted state and deployment config may need a one-time migration** (read-old →
write-new, then drop the old reader). This skill is that boundary for the AI-native DB.

## 1. Binary version flip
Build + promote per `epistemic-graph-deploy`. New optional fields on persisted structs use
`#[serde(default)]`, so an old snapshot/redb loads under a new binary **without** a rewrite
(e.g. the bi-temporal `tx_from`/`tx_to` added to nodes/edges — old blobs decode as `None`,
EG-KG.compute.preserved). Verify with `--verify` (live UQL smoke). Roll back by restoring the `.bak-<ts>`
binary.

## 2. First authoritative boot — `.mp` → redb migration (CONCEPT:EG-OS.deployment.binary-promotion)
Flipping to the redb-authoritative tier runs a **one-time** migration on first boot: it
imports the legacy snapshot (`.mp`/WAL) into the durable redb store and **binds the socket
only when done** — minutes on a large KG (thousands of graphs / hundreds of MB). The 20s
healthcheck start-period would kill it into a restart loop, so:
```bash
scripts/promote_engine.sh --migrate --restore-health-period   # extends start-period, watches
```
It tails the engine logs for `Snapshot load progress` / `imported N graph(s)` /
`Listening on UDS`, then restores the normal healthcheck on the now-fast restart. Never
`docker kill` a migrating engine — let it finish (check the log before assuming a hang).

## 3. Config / deployment-env migration
Settings live in `config.json` (XDG, injected into env with **`setdefault`** — a baked
container env always wins) read via typed `AgentConfig` fields / `config.setting(...)`.
A retired value is migrated forward in code where it's read, not by editing every
deployment. Example shipped: `GRAPH_BACKEND=tiered` (removed in the engine-authority
consolidation) now maps forward to `epistemic_graph` at boot with a warning, so a stale
deployment keeps booting. **Still** update the manager service env to the new value
(`docker service update --env-add GRAPH_BACKEND=fanout <svc>`) to silence the warning —
the shim is a migration aid, not a permanent alias.

## 4. On-disk data-format evolution
- **Additive struct fields** → `#[serde(default)]` + round-trip test (msgpack + JSON
  props). No bulk rewrite; optionally lazy-backfill on next write.
- **Breaking format change** → add a one-time read-old/write-new pass keyed off the first
  authoritative boot hook, then remove the old-format reader (no permanent dual reader).
- **Ontology / SHACL evolution** (KG-2.252 temporal facts, etc.) is additive in the
  canonical `ontology.ttl` + `shapes/*.ttl`; validated by `check_ontology`.

## Pre-flight & verify checklist
1. Back up: the binary (`.bak-<ts>`, automatic) and, for risky data migrations, snapshot the
   redb/`graph_snapshots` volume.
2. Promote with `--migrate` if flipping authoritative persistence; otherwise plain restart.
3. `--verify` (live UQL smoke) + a targeted query for any migrated data.
4. Confirm clients reconnect (breaker `closed`) — see `epistemic-graph-troubleshooting`.
5. On failure: restore the `.bak-<ts>` binary (and the volume snapshot if data was touched),
   restart, re-verify.

## Related
- `epistemic-graph-deploy` — build/stage/restart mechanics.
- `epistemic-graph-troubleshooting` — recovery when a migration leaves the engine unhealthy.
- `docs/deploy/binary-promotion.md`, `docs/uql.md`.
