---
name: kg-modality-consensus
description: >-
  Operate the engine's distributed substrate — openraft consensus with automatic failover,
  multi-Raft groups, online resharding (ownership move / hibernate / rehydrate), cross-shard
  2PC, and the per-tenant catalog. Use when scaling the engine across nodes, moving shards,
  inspecting Raft/cluster health, or provisioning/routing tenants; via the engine_consensus,
  engine_resharding, and engine_tenants MCP tools.
domain: modality
license: MIT
tags: [epistemic-graph, engine, consensus, raft, resharding, tenants, cluster, modality]
tier: modality
wraps: [engine_consensus, engine_resharding, engine_tenants]
metadata:
  author: Genius
  version: '0.1.0'
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
load_tools(tools=["engine_consensus", "engine_resharding", "engine_tenants"])
# engine_consensus   — Raft membership / leadership / cluster health / failover
# engine_resharding  — multi-Raft group ring, online ownership move, hibernate/rehydrate
# engine_tenants     — per-tenant catalog: provision, list, route
```
or the REST twins graph-os exposes for these engine domains.

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
