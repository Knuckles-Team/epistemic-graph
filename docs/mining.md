# Data Mining — the `graph_mine` / `/api/mining` surface

> CONCEPT:EG-KG.mining.frequent-itemset-mining
>
> A unified, cross-modal **data-mining** surface on the engine. Every algorithm
> runs **compute-near-data** — one round-trip over data already resident in the
> graph/snapshot — and mined patterns **write back** into the KG as typed nodes for
> OWL reasoning and the next mining pass (the discovery flywheel).

The surface exposes three actions today:

- **`associate`** (Phase 1) — association-rule mining (frequent itemsets + rules
  with **support / confidence / lift**).
- **`cluster`** (Phase 2) — DBSCAN, hierarchical agglomerative, GMM (EM),
  k-medoids (PAM), completing the family beyond the existing k-Means/spectral.
- **`anomaly`** (Phase 2) — z-score/MAD, Isolation Forest, LOF, One-Class SVM.

Later phases add sequential patterns, forecasting, and frequent-subgraph mining
onto this *same* surface — so every later phase is "add an algorithm", not "add a
surface".

## The one surface, five layers

| Layer | Where |
|-------|-------|
| Engine impl | `crates/eg-compute/src/mining/{association,cluster,anomaly}.rs` — pure-Rust, dependency-light, deterministic |
| Protocol | `Method::Mine{Associate,Cluster,Anomaly}` in the `// ── Mining ──` section of `crates/eg-types/src/protocol.rs` (feature `mining`) |
| Handler | `src/server/handlers/mining.rs` — graph-derived rows + write-back over the live `GraphCore` |
| Client | `client.mining.{associate,cluster,anomaly}(...)` (`epistemic_graph/client.py`) |
| graph-os MCP | `graph_mine action="associate\|cluster\|anomaly"` (agent-utilities `engine_surface_tools.py`) — plus the granular `engine_mining` verb |
| REST twin | `POST /api/mining/{associate,cluster,anomaly}` (agent-utilities `kg_server.py`, same `_execute_tool` core — surface parity is a build gate) |

## Algorithms

Three interchangeable frequent-itemset engines, selected by `algorithm`:

- **`fpgrowth`** (default) — frequency-ordered prefix tree (FP-tree) + conditional
  pattern bases; no candidate generation, fastest on dense baskets.
- **`apriori`** — breadth-first, level-wise candidate generation + downward-closure prune.
- **`eclat`** — depth-first over the vertical layout (transaction-id-set intersection).

All three are exact and, for the same `min_support`, produce the **same** frequent
itemsets (asserted by a parity test), so rule generation is shared. The metrics:

- **support**(A∪C) — fraction of transactions holding every item.
- **confidence**(A⇒C) = support(A∪C) / support(A) = P(C | A).
- **lift**(A⇒C) = confidence / support(C) — `>1` ⇒ positively correlated.

## Two ways in: explicit transactions or a graph-derived source

`MineAssociate` accepts **either** an explicit `transactions` list **or** a
graph-derived `source` — never both (explicit wins).

```python
from epistemic_graph.client import SyncEpistemicGraphClient
c = SyncEpistemicGraphClient.connect(socket_path="/run/epistemic-graph/shard-0.sock")

# (1) explicit market-basket transactions
res = c.mining.associate(
    [["bread", "butter", "milk"],
     ["bread", "butter"],
     ["bread", "milk"],
     ["butter", "milk"],
     ["bread", "butter", "milk"]],
    min_support=0.4, min_confidence=0.5, algorithm="fpgrowth",
)
for r in res["rules"]:
    print(r["antecedent"], "=>", r["consequent"],
          f"conf={r['confidence']:.2f} lift={r['lift']:.2f}")
# {bread,butter} => {milk}  conf=0.67 lift=0.83  …
```

The **graph-derived** path (`source`) turns node neighborhoods into transactions —
this is the cross-modal hook. The spec:

| field | meaning |
|-------|---------|
| `node_label` | the label whose instances each become one basket owner |
| `direction` | `out` (successors, default) · `in` (predecessors) · `any` |
| `item_field` | `label` (neighbor's type) · `prop:<key>` (a neighbor property) · omit ⇒ the neighbor's node id |
| `relation` | only follow edges whose `relation`/`type` property equals this |
| `limit` | cap the basket owners scanned (0 = uncapped) |

## Cross-modal example — mine over a graph neighborhood

Because the source reads resident graph data, `retrieve → mine` is one fused,
compute-near-data operation (no second round-trip, no client-side marshalling):

```python
# For each :Doc, mine which cited topics co-occur — traversing the CITES graph.
res = c.mining.associate(
    source={"node_label": "Doc", "direction": "out",
            "item_field": "prop:topic", "relation": "CITES"},
    min_support=0.1, min_confidence=0.6, algorithm="fpgrowth",
    writeback=True,   # materialize :AssociationRule nodes
)
```

No vector-only or stitched stack can express "traverse a neighborhood, then mine
frequent patterns over its labels/edges, then write the patterns back" as one plan.

## Write-back — the discovery flywheel

With `writeback=true`, each rule is materialized as a typed `:AssociationRule`
node (deterministic id, so re-mining is idempotent) carrying `antecedent`,
`consequent`, `support`, `confidence`, `lift` as queryable properties, and linked
(`RULE_ITEM` edges) to any item that is itself a resident node. OWL reasoning,
retrieval, and the *next* mining pass then consume these nodes — closing knowledge
discovery back into the graph.

```python
res = c.mining.associate(..., writeback=True)
rules = c.nodes.list_by_label("AssociationRule", 0)   # queryable typed nodes
```

## First consumer — agent-utilities-evolution (concept↔capability rules)

The headline use case: mine **concept↔capability co-occurrence** to auto-suggest
implementations. Seed a small graph where each `Paper` TOUCHES some `Concept`s and
IMPLEMENTS a `Capability`; each paper's neighborhood is one transaction over
`{concepts ∪ capability}`, and the mined rules read like
`{concept A, concept B} ⇒ capability Z` ("papers touching A and B usually
implement Z"):

```python
from epistemic_graph.client import SyncEpistemicGraphClient
c = SyncEpistemicGraphClient.connect(socket_path="/run/epistemic-graph/shard-0.sock")
c.graph.clear()

concepts = {"cA": "concept:cA", "cB": "concept:cB", "cC": "concept:cC"}
caps = {"capX": "capability:capX", "capZ": "capability:capZ"}
for nid in concepts.values(): c.nodes.add(nid, {"type": "Concept"})
for nid in caps.values():     c.nodes.add(nid, {"type": "Capability"})

papers = {
    "p1": (["cA", "cB"], "capZ"), "p2": (["cA", "cB"], "capZ"),
    "p3": (["cA", "cB"], "capZ"), "p4": (["cA", "cC"], "capX"),
    "p5": (["cB", "cC"], "capX"),
}
for pid, (cs, cap) in papers.items():
    c.nodes.add(pid, {"type": "Paper"})
    for x in cs: c.edges.add(pid, concepts[x], {"relation": "TOUCHES"})
    c.edges.add(pid, caps[cap], {"relation": "IMPLEMENTS"})

# Mine each Paper's neighborhood (concepts + capability) → co-occurrence rules,
# and write the rules back as :AssociationRule nodes.
res = c.mining.associate(
    source={"node_label": "Paper", "direction": "out"},
    min_support=0.4, min_confidence=0.9, algorithm="fpgrowth", writeback=True,
)
for r in res["rules"]:
    if set(r["antecedent"]) == {"concept:cA", "concept:cB"}:
        print(r["antecedent"], "=>", r["consequent"],
              f"conf={r['confidence']:.2f} lift={r['lift']:.2f}")
# ['concept:cA', 'concept:cB'] => ['capability:capZ']  conf=1.00 lift=1.67
print("wrote back", res["written_back"], ":AssociationRule nodes")
```

`graph_loops` + the `agent-utilities-expert` already read typed KG nodes, so the
mined `:AssociationRule` nodes feed the evolution queue directly.

## MCP + REST

```jsonc
// MCP (graph-os multiplexer)
graph_mine { "action": "associate",
             "params_json": "{\"transactions\":[[\"a\",\"b\"],[\"a\",\"c\"]],\"min_support\":0.5}" }

// REST twin (same _execute_tool core)
POST /api/mining/associate
{ "transactions": [["a","b"],["a","c"]], "min_support": 0.5, "algorithm": "fpgrowth" }
```

Both dispatch the identical engine call — surface parity (MCP ⇄ REST) is enforced as
a ship-together gate, not a follow-up.

---

# Clustering — `action="cluster"`

Completes the clustering family beyond k-Means/spectral with four interchangeable,
pure-Rust engines selected by `algorithm`:

| `algorithm` | What | Key params |
|-------------|------|------------|
| `dbscan` (default) | Density clustering (CONCEPT:EG-KG.mining.dbscan-density); labels un-dense points **noise** (`cluster_id = -1`) | `eps`, `min_pts` |
| `hierarchical` | Agglomerative single/complete/average linkage cut to `k` (CONCEPT:EG-KG.mining.hierarchical-linkage) | `k`, `linkage` |
| `gmm` | Diagonal-covariance Gaussian mixture via EM; soft **responsibilities** + argmax label (CONCEPT:EG-KG.mining.gmm-em) | `k`, `max_iter`, `seed` |
| `kmedoids` | Partitioning Around Medoids — centers are real data points (CONCEPT:EG-KG.mining.kmedoids-pam) | `k`, `max_iter` |

Rows come from **either** an explicit `features` matrix **or** a graph-derived
`source` (node embeddings). Output rows are `{cluster_id, members, centroid,
score}` (score = mean member→centroid distance; GMM also returns
`responsibilities`).

```python
from epistemic_graph.client import SyncEpistemicGraphClient
c = SyncEpistemicGraphClient.connect(socket_path="/run/epistemic-graph/shard-0.sock")

# Explicit feature matrix — DBSCAN two blobs + a noise point.
res = c.mining.cluster(
    [[0.0, 0.0], [0.1, 0.1], [10.0, 10.0], [10.1, 9.9], [50.0, 50.0]],
    algorithm="dbscan", eps=1.0, min_pts=2,
)
# res["labels"] == [0, 0, 1, 1, -1]   (-1 = noise)
```

## Cross-modal — cluster the embeddings of a node set (the differentiator)

The `source` spec `{node_label, limit}` gathers the **stored embedding** of every
node with that label as the feature rows — so "retrieve the vectors of these nodes,
then cluster them, then write the clusters back" is **one** compute-near-data plan
(CONCEPT:EG-KG.mining.node-embedding-source), no marshalling, no second round-trip:

```python
# Cluster the embeddings of every :Doc, then materialize :Cluster nodes.
res = c.mining.cluster(
    source={"node_label": "Doc"},   # rows = the Docs' embedding vectors
    algorithm="kmedoids", k=5, writeback=True,
)
for cl in res["clusters"]:
    print(cl["cluster_id"], "size", len(cl["members"]), "score", round(cl["score"], 3))
clusters = c.nodes.list_by_label("Cluster", 0)   # queryable typed nodes
```

With `writeback=true` each non-noise cluster becomes a typed `:Cluster` node
(`algo`, `cluster_id`, `size`, `members`, `centroid`, `score`; deterministic id =
digest of algo + sorted member ids, so replay is idempotent) linked
(`CLUSTER_MEMBER`) to its resident member nodes — the discovery flywheel
(CONCEPT:EG-KG.mining.cluster-writeback). No vector-only stack expresses
"ANN-neighborhood → cluster → write typed nodes back" as one plan.

---

# Anomaly detection — `action="anomaly"`

Four interchangeable detectors, each returning a per-row `anomaly_score` (**higher
= more anomalous**) so a single `threshold` (or a per-algorithm default) yields
`is_anomaly`:

| `algorithm` | What | Key params | Default threshold |
|-------------|------|------------|-------------------|
| `zscore` (default) | Robust modified z-score / MAD (CONCEPT:EG-KG.mining.zscore-mad) | — | 3.5 |
| `isoforest` | Isolation Forest — path-length score (CONCEPT:EG-KG.mining.isolation-forest) | `n_trees`, `sample_size`, `seed` | 0.6 |
| `lof` | Local Outlier Factor — k-neighbor density ratio (CONCEPT:EG-KG.mining.lof-local-density) | `k` | 1.5 |
| `ocsvm` | One-Class ν-SVM boundary via SMO (CONCEPT:EG-KG.mining.oneclass-svm) | `nu`, `kernel`, `gamma` | 0.0 |

Rows come from **explicit `features`**, a **1-D `values` series** (each scalar → one
row), **or** a graph-derived `source` (node embeddings). Output rows are `{id,
anomaly_score, is_anomaly}`.

## Cross-modal — anomaly-detect a time-series window (RCA)

Feed a tsdb series window as `values` — each value becomes a one-element row — to
flag the anomalous points for root-cause analysis, then link the anomaly back to
its entity:

```python
# Pull a metric series window from the engine's TSDB, then Isolation-Forest it.
series = c.timeseries.range("cpu.util", t0, t1)               # [(ts_ns, [value]), ...]
res = c.mining.anomaly(
    values=[vals[0] for _ts, vals in series],
    algorithm="isoforest", n_trees=200, seed=7,
)
spikes = [r["id"] for r in res["rows"] if r["is_anomaly"]]     # row indices of the anomalies
```

Or run over node embeddings directly and write the anomalies back:

```python
# Flag :Metric nodes whose embeddings are outliers; materialize :Anomaly nodes.
res = c.mining.anomaly(
    source={"node_label": "Metric"}, algorithm="zscore", writeback=True,
)
anoms = c.nodes.list_by_label("Anomaly", 0)
```

With `writeback=true` each flagged row becomes a typed `:Anomaly` node (`algo`,
`score`, `source`; deterministic id) linked (`ANOMALY_OF`) to its resident source
node (CONCEPT:EG-KG.mining.anomaly-writeback) — so the RCA result feeds OWL
reasoning + the next mining pass. This directly serves the evolution use case:
anomaly-detect our own **concept-implementation coverage** to surface divergent /
under-implemented areas for the wiring sweep.

## MCP + REST (cluster / anomaly)

```jsonc
// MCP
graph_mine { "action": "cluster",
             "params_json": "{\"features\":[[0,0],[10,10]],\"algorithm\":\"dbscan\",\"eps\":1.0,\"min_pts\":2}" }
graph_mine { "action": "anomaly",
             "params_json": "{\"values\":[1,1,1,100],\"algorithm\":\"zscore\"}" }

// REST twins (same _execute_tool core)
POST /api/mining/cluster  { "source": {"node_label": "Doc"}, "algorithm": "kmedoids", "k": 5, "writeback": true }
POST /api/mining/anomaly  { "source": {"node_label": "Metric"}, "algorithm": "isoforest", "writeback": true }
```
