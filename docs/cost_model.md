# Cost model & capacity planning (CONCEPT:EG-KG.compute.lane-v, Lane V)

> **Evidence status:** runtime budget enforcement is `IMPLEMENTED`; focused
> source fixtures are `UNIT-PROVEN` where named below. Capacity examples are
> `DESIGNED` planning arithmetic. Nothing on this page is `LIVE`, `LAB-PROVEN`,
> or `1M-CERTIFIED` without an exact artifact, environment, and report reference.

This maps the engine's footprint from a single Raspberry Pi 4+ node up to an HA `cluster` onto resource footprints so an
operator can plan capacity: *given N tenants with an average working set, how much RAM /
how many shards do I need?* It pairs with the runtime **per-tenant memory budget** + the
**autoscale signals** (`Method::ResourceStats`) that drive the agent-utilities autoscaler
(AU-OS.config.health-gated-deploy-rollback).

## How the engine spends memory

A graph's resident RAM is dominated by its **node + edge property blobs** (MessagePack)
plus the **embedding vectors**, with a small per-node/per-edge structural overhead for the
petgraph topology + id strings + map slots. `GraphCore::memory_estimate()` sums exactly
this (it is an *estimate* — order-of-magnitude, not allocator-exact, which is all a soft
budget needs):

```
memory_bytes ≈ Σ_nodes (id_bytes + prop_blob_bytes + 64)
             + Σ_edges (key_bytes + prop_blob_bytes + 48)
             + Σ_vectors (dim × 4)            # f32 embeddings
```

A **tenant** owns one or more graphs; the tenant is the graph-name prefix before the first
`:` (`agent:planner` → `agent`, `enterprise-acme:billing` → `enterprise-acme`,
`__commons__` → its own tenant). `tenant_of()` is the one place this mapping lives.

## The per-tenant budget (runtime enforcement)

The budget enforcer (`src/cost.rs::enforce_memory_budgets`) runs a periodic sweep:

1. **Roll up** resident memory per tenant.
2. For each tenant **over its effective budget** — the smaller of its configured budget and
   its **fair share** `global_ceiling / active_tenants` — reclaim its **coldest** graphs
   (fewest nodes first, preserving the hot working set):
   - **Evict** the graph's LRU nodes, durability-gated (a node is dropped from RAM only
     after redb confirms it on disk — reuses the EG-KG.storage.read-through-seam-exercised no-loss gate). An evicted node
     still reads back through the read-through seam.
   - If still resident and still over budget, **hibernate** the graph (`GraphCore::hibernate`,
     EG-KG.storage.100m-tenant) — drop all its in-RAM state; the durable redb tier + read-through stay
     intact, so it rehydrates on access.
3. The **global ceiling** caps total resident RAM; the **fair per-tenant cap** stops one hot
   tenant from consuming the whole ceiling and starving others.

Data is never lost: every reclamation path is durability-gated or hibernation-only (RAM,
not the durable tier).

### Configuration — one knob

| Env var | Meaning | Default |
|---------|---------|---------|
| `EPISTEMIC_GRAPH_MEMORY_BUDGET` | Global resident-memory ceiling (`512m`, `2g`, or plain bytes). A positive explicit value may lower the automatic ceiling but is clamped to the cgroup-aware limit; `0`, negative, and malformed values reject startup. **There is no disable value.** | 40% of effective cgroup-aware RAM |
| `EPISTEMIC_GRAPH_TENANT_BUDGET` | Optional positive per-tenant budget override; zero, negative, and malformed values reject startup. | = the global ceiling |
| `EPISTEMIC_GRAPH_BUDGET_INTERVAL` | Sweep cadence, seconds; must be in the bounded `1..=3600` range. | 15 |

The default auto-sizes from effective cgroup-aware memory, so a stock build on any host
budgets sensibly with no tuning. Budgeting cannot be disabled by an environment override.

## Autoscale signals

`Method::ResourceStats` returns a `ResourceSnapshot` in one round-trip — the structured
signals an autoscaler needs (also exported on the Prometheus `/metrics` endpoint):

| Signal | Per-graph | Per-tenant | Aggregate |
|--------|:---------:|:----------:|:---------:|
| resident memory bytes | ✓ | ✓ | ✓ (`total_memory_bytes`) |
| current process RSS (hard-ceiling calibration; peak fallback) | | | ✓ (`process_rss_bytes`) |
| node / edge counts | ✓ | ✓ | ✓ |
| hibernated vs resident | ✓ | ✓ | ✓ |
| in-flight / queue depth | | | ✓ (`in_flight`, `inflight_permits_available`) |
| eviction / hibernation rate | | | ✓ (`budget_evictions_total`, `budget_hibernations_total`) |

Prometheus series: `epistemic_graph_graph_memory_bytes{graph}`,
`epistemic_graph_graph_hibernated{graph}`, `epistemic_graph_budget_evictions_total`,
`epistemic_graph_budget_hibernations_total`, plus the existing
`epistemic_graph_in_flight_requests` / `epistemic_graph_inflight_permits_available`.

**Scale-out trigger (typical autoscaler rule):** sustained `budget_evictions_total` rate > 0
**and** `inflight_permits_available` near zero ⇒ the shard is memory- + admission-bound ⇒
add capacity through governed membership and placement movement.

## Capacity estimate

`capacity_estimate(tenants, avg_working_set_bytes, per_shard_budget_bytes, resident_fraction)`
maps a workload to a shard count (pure arithmetic, callable offline):

```
total_working_set   = tenants × avg_working_set
budgeted_resident   = total_working_set × resident_fraction   # the share kept HOT at once
recommended_shards  = ceil(budgeted_resident / per_shard_budget)
```

`resident_fraction` is the share of tenants expected hot simultaneously (the rest hibernate
/ serve read-through from the durable tier) — the lever that makes 100M cold tenants fit on
far fewer resident bytes.

### Tier footprints (worked examples)

| Tier | Per-shard budget | Workload | resident_fraction | Resident RAM | Shards |
|------|------------------|----------|-------------------|--------------|--------|
| **Pi** (Raspberry Pi 4/5) | 512 MiB | 1k tenants × 256 KiB | 0.25 | 64 MiB | 1 |
| **Node** (small server) | 8 GiB | 100k tenants × 1 MiB | 0.10 | ~9.8 GiB | 2 |
| **Cluster** (HA) | 16 GiB/shard | 10M tenants × 1 MiB | 0.02 | ~195 GiB | 13 |

(Derive any row with `capacity_estimate`; e.g. the Node row =
`capacity_estimate(100_000, 1<<20, 8<<30, 0.10)` ⇒ `recommended_shards = 2`.)

The key property: because cold tenants evict/hibernate to the durable tier and rehydrate on
access, a shard's resident RAM is designed to track the **hot working set**, not the total
tenant count. The "100M agents, a local engine each" statement is a capacity hypothesis
across supported deployment topologies, not a 1M-user/agent certification or a live result.

## Deferred

- **Spot / preemptible**-aware placement (drain a shard before a spot reclaim).
- **Carbon / energy** cost accounting per tenant.
- **Predictive** autoscale (forecast working-set growth instead of reacting to eviction
  rate). The current model is reactive (eviction-rate + saturation triggers).
