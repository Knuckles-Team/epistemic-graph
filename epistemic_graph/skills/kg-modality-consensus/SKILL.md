---
name: kg-modality-consensus
description: >-
  Operate the engine's distributed substrate — openraft consensus with automatic failover,
  multi-Raft groups, online resharding (ownership move / hibernate / rehydrate), cross-shard
  2PC, and the per-tenant catalog — plus the scope-gated ADMIN tier: RBAC policy
  administration and ops backup/restore. Use when scaling the engine across nodes, moving
  shards, inspecting Raft/cluster health, provisioning/routing tenants, managing RBAC
  policies, or running an admin backup/restore; via the engine_consensus, engine_resharding,
  engine_tenants, engine_rbac, and engine_admin MCP tools.
domain: modality
license: MIT
tags: [epistemic-graph, engine, consensus, raft, resharding, tenants, cluster, rbac, admin, modality]
tier: modality
wraps: [engine_consensus, engine_resharding, engine_tenants, engine_rbac, engine_admin]
metadata:
  author: Genius
  version: '0.2.0'
---

# kg-modality-consensus — distributed consensus, resharding & tenants

The engine's durability/distribution layer makes the AI-native database horizontally
scalable and crash-safe. It runs **openraft** replication with automatic failover
(`raft`/`cluster` features, `src/raft/`), **multi-Raft groups** (an N-group ring with a
`GroupRouter`, KG-2.266/267/268), **online resharding** (`reshard.rs` moves ownership live,
hibernate/rehydrate groups), and **cross-shard 2PC** (presumed-abort, crash-recoverable) with
parallel-commit / read-only-participant fast paths and a Paxos-Commit-lite (and opt-in Calvin
deterministic) commit branch. A cross-region async read-replica tier with per-tenant quota +
backpressure guardrails rounds it out. See `docs/capabilities.md` → *Durability & distribution*.

## The MCP way (through graph-os)
```
load_tools(tools=["engine_consensus", "engine_resharding", "engine_tenants",
                  "engine_rbac", "engine_admin"])
# engine_consensus   — Raft membership / leadership / cluster health / failover
# engine_resharding  — multi-Raft group ring, online ownership move, hibernate/rehydrate
# engine_tenants     — per-tenant catalog: provision, list, route
# engine_rbac        — RBAC policy administration: roles + resource/action grants
# engine_admin       — ops/maintenance: online backup + restore
```
or the REST twins graph-os exposes for these engine domains.

## Admin tier (scope-gated)
All five domains above are classified **ADMIN** (`agent_utilities.mcp.tools.engine_tools.
ADMIN_DOMAINS`): every action they expose is denied fail-closed to an acting identity
that lacks the `kg:admin` scope/role (`_enforce_admin_scope`), never merely hidden.
`engine_rbac`/`engine_admin` complete that ADMIN set — RBAC policy administration
(roles + resource/action grants) and the ops backup/restore surface — so use this
skill whenever you need to grant/revoke a role, inspect a resource's ACL, or run an
online backup/restore, exactly like reaching for `engine_consensus`/`engine_tenants`
for cluster/tenant admin.

## The wire way
Consensus is cluster control, not a query wire — the `cluster` prebuilt tier adds `pgwire` so
tenants are reachable over SQL once provisioned. Federated **reads** across peer engines are a
separate surface (`/federated`, `EPISTEMIC_GRAPH_FEDERATED_ADDR`, default `127.0.0.1:7900`);
per-node addressing + tiers live in `docs/interfaces/connecting.md` and
`docs/operations/runbook.md`.

## Cross-modal seam
Consensus is what makes the **unified cross-modal transaction** durable at scale: a single
`WriteTransaction` spanning graph + RDF + vector + blob + time-series is redb-authoritative
(commit-before-ack, `kill -9`-safe) and, across shards, is committed by cross-shard 2PC over
the multi-Raft groups. Every other modality skill's write ultimately lands through this layer.

## Related
- `kg-modality-sql` / `kg-modality-sparql` — the query wires whose writes this layer commits.
