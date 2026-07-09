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

## Fused `retrieve → mine → writeback` (cross-modal plans, Phase 5)

`source={"node_label": ...}` (above) is already a fused, compute-near-data
retrieval — but it can only express "every node with this label". The `plan`
parameter (CONCEPT:EG-KG.mining.fused-plan-source) generalizes that to an
**arbitrary upstream cross-modal retrieval plan** — the SAME `Op` algebra
`unified_query` runs (`Scan`/`Filter`/`Traverse`/`Rank`/`RankText`/`Reason`/…):
the plan executes FIRST, over the resident graph/vector/SQL/RDF modalities, and
the resulting rows' stored embeddings become the mining op's feature matrix —
so "retrieve a candidate set, then mine it, then write back" is ONE round trip,
never two. `cluster`, `anomaly`, `classify_fit`, `classify_predict`, and
`reduce` all accept it (precedence: explicit `features`/`x` > `plan` >
`source`).

```python
# Vector-retrieve a neighborhood (Scan + Rank + Limit), THEN cluster it, THEN
# write :Cluster nodes back — one call, no client round-trip in between.
res = c.mining.cluster(
    plan=[
        {"Scan": {"label": "Doc"}},
        {"Rank": {"query": [0.1, 0.1, 0.0, 0.0]}},
        {"Limit": {"k": 50}},
    ],
    algorithm="kmedoids", k=3, writeback=True,
)
```

A plan is graph-derived like `source`, so WAL replay re-executes it
deterministically against the current graph state. A plan leg that matches no
rows degrades to an empty feature set (never an error) — the same "no match ⇒
empty" contract every other mining source honors. **Scope cut:** the
synchronous, graph-scoped mining dispatch does not thread the live committed
tsdb store through (unlike the async `UnifiedQuery` handler), so an
`Op::TsScan` leg inside a mining `plan` currently degrades to no rows for that
leg — a follow-up to wire the tsdb handle into the mining dispatch path.

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

---

# Classification — `action="classify_fit"` / `action="classify_predict"`

Classification is **predictive**: `classify_fit` trains a model and returns a
serializable blob; `classify_predict` takes the blob back plus a feature matrix and
returns per-row labels + a per-class probability matrix — exactly the
`fit_estimator`/`predict_estimator` pattern the datascience estimators use. This
completes the classifier family beyond the datascience tree/forest/boosting
estimators with the four classical linear/probabilistic/instance classifiers:

- **`gaussiannb`** (default) — Gaussian Naive Bayes; per-class prior + per-feature
  (mean, variance) likelihood for continuous features.
- **`multinomialnb`** — Multinomial Naive Bayes; Laplace-smoothed (`alpha`)
  class-conditional log-probabilities over non-negative count features.
- **`knn`** — brute k-nearest-neighbor majority vote (`k` neighbors); `proba` is the
  per-class vote fraction. (A brute scan over the feature rows — the ANN index would
  accelerate this at 1M+ scale; the exact vote keeps parity deterministic.)
- **`logistic`** — one-vs-rest logistic regression fit by batch gradient descent
  (`lr`, `epochs`, L2 `l2`). Handles binary and multiclass.
- **`svc`** — one-vs-rest linear SVM (Pegasos sub-gradient hinge loss, `c`, `epochs`,
  `lr`).

All are deterministic (GD/Pegasos are seed-free: zero init, index-ordered batch
updates). Parity is asserted against known separable fixtures (NB/logistic/SVC recover
a linear boundary; k-NN classifies blobs).

```python
# Fit on a labeled feature matrix → model blob, then predict fresh rows.
fit = await c.mining.classify_fit(
    x=[[0,0],[0.5,0.3],[0.2,0.8],[10,10],[10.5,9.7],[9.8,10.4]],
    y=[0,0,0,1,1,1],
    algorithm="logistic", lr=0.5, epochs=500,
)
out = await c.mining.classify_predict(fit["model"], x=[[0.3,0.3],[10,10]])
# out["rows"] == [{"id":0,"label":0,"proba":[...]}, {"id":1,"label":1,"proba":[...]}]
```

## Cross-modal — classify nodes by their embeddings + ontology features

`classify_predict` (and `classify_fit`) accept a vector `source` `{node_label,
limit}` in place of explicit rows — the stored embedding (and any OWL-inferred
property vector) of each node becomes a feature row. This is "classify these nodes
using their embeddings" running compute-near-data, with the prediction linked back to
its source.

```python
# Train once, then classify every :Sample node by its embedding + write :Classification.
out = await c.mining.classify_predict(
    model, source={"node_label": "Sample"}, writeback=True,
)
# → one :Classification{label, proba, source} node per row, linked CLASSIFIED_AS → the source node.
```

`classify_fit` is **read-only** (it returns a model, mutates nothing).
`classify_predict` with `writeback=True` is a **WAL-durable** graph write (replayed by
re-running the fitted model deterministically).

---

# Dimensionality reduction — `action="reduce"`

Reduction is **descriptive**: it transforms the feature rows into a low-dimensional
embedding `{id, coords}`. Beyond the datascience PCA:

- **`svd`** (default) — truncated SVD via the eigendecomposition of the Gram matrix
  XᵀX; `coords = X·V = U·Σ`. Exact linear algebra; reconstructs a low-rank matrix to
  within round-off (returns the retained `singular_values`).
- **`lda`** — Fisher Linear Discriminant Analysis (**supervised** — requires
  `labels`): whiten by the within-class scatter, then take the leading eigenvectors of
  the between-class scatter. Projects onto ≤ (n_classes−1) discriminants.
- **`umap`** — a fuzzy k-NN neighbor graph (`n_neighbors`, `min_dist`) laid out by
  attractive/repulsive SGD (`epochs`, `seed`).
- **`tsne`** — perplexity-calibrated Gaussian affinities matched to a Student-t low-D
  layout by gradient descent (`perplexity`, `epochs`, `lr`, `seed`).

**Scope (honest):** SVD and LDA are exact, deterministic, parity-checkable linear
algebra. **UMAP and t-SNE are approximate, iterative, and intended for small N**
(viz-scale — hundreds to low thousands of rows); they preserve neighborhood/cluster
structure, **not** exact coordinates, and are deterministic per `seed` (verified by a
neighbor-preservation sanity check on planted clusters, not an exact-coordinate
assertion).

```python
# Truncated SVD of an explicit matrix → 2-D coords + singular values.
out = await c.mining.reduce(x=rows, algorithm="svd", n_components=2)

# Supervised LDA needs one label per row.
out = await c.mining.reduce(x=rows, labels=y, algorithm="lda", n_components=1)
```

## Cross-modal — reduce node embeddings for the web-UI graphviz

Point `reduce` at a vector `source` to project the stored embeddings of a node label
into 2-D and, with `writeback=True`, materialize each as an `:Embedding2D{coords}`
node linked `REDUCED_FROM` → its source — the coordinates the web-UI graphviz renders,
and a downstream feed for clustering.

```python
# UMAP the :Doc embeddings for the graphviz, writing :Embedding2D back.
out = await c.mining.reduce(
    source={"node_label": "Doc"}, algorithm="umap", n_components=2, writeback=True,
)
# → one :Embedding2D{coords, source} node per :Doc, linked REDUCED_FROM → the :Doc.
```

`reduce` with `writeback=True` is a **WAL-durable** graph write (replayed by re-running
the reduction deterministically; UMAP/t-SNE reproduce per `seed`).

## MCP + REST (classify / reduce)

```jsonc
// MCP
graph_mine { "action": "classify_fit",
             "params_json": "{\"x\":[[0,0],[10,10]],\"y\":[0,1],\"algorithm\":\"logistic\"}" }
graph_mine { "action": "classify_predict",
             "params_json": "{\"model\":{...},\"source\":{\"node_label\":\"Sample\"},\"writeback\":true}" }
graph_mine { "action": "reduce",
             "params_json": "{\"source\":{\"node_label\":\"Doc\"},\"algorithm\":\"umap\",\"writeback\":true}" }

// REST twins (same _execute_tool core)
POST /api/mining/classify_fit     { "x": [[0,0],[10,10]], "y": [0,1], "algorithm": "svc" }
POST /api/mining/classify_predict { "model": {...}, "x": [[0.1,0.1]] }
POST /api/mining/reduce           { "source": {"node_label": "Doc"}, "algorithm": "svd", "n_components": 2 }
```

---

# Sequential-pattern mining — `action="sequence"`

Phase 4 begins the final family. Sequential-pattern mining finds frequent ORDERED
subsequences over a set of sequences — an item may repeat within a sequence, and a
pattern need not be contiguous (only order-preserving) to count. Two interchangeable
engines, selected by `algorithm`:

- **`prefixspan`** (default) — projection-based growth: count frequent next-items over
  the current projected database (the suffix of each sequence after its first
  occurrence of the pattern-so-far's last item), emit, recurse. No candidate
  generation.
- **`gsp`** (Generalized Sequential Pattern) — the sequence analog of Apriori:
  level-wise candidate generation (join two frequent (k-1)-patterns whose overlap
  matches) + a downward-closure prune, support counted by direct subsequence scan.

Both are exact and agree on the frequent-pattern set for a given `min_support`
(parity-tested, like Apriori/FP-Growth/Eclat).

```python
# Explicit ordered sequences (e.g. a user-journey event log).
out = await c.mining.sequence(
    sequences=[
        ["login", "browse", "purchase"],
        ["login", "search", "browse", "purchase"],
        ["login", "browse"],
        ["login", "browse", "purchase"],
    ],
    min_support=0.5,
)
# out["patterns"] includes {"items": ["login", "browse", "purchase"], "support": 0.75, "count": 3}
```

## Cross-modal — mine each node's ordered neighbor history (evolution/commit timelines)

Point `sequence` at a graph-derived `source` to turn each node's chronological
out/in-edge history into one sequence — "what reliably follows what" over the
evolution/commit timeline or an event-log graph, with `writeback=True` materializing
the discovered patterns as `:SequentialPattern` nodes.

```python
# Each :Session node's ordered "out" edges to :Event nodes become one sequence.
out = await c.mining.sequence(
    source={"node_label": "Session", "direction": "out"},
    algorithm="gsp",
    min_support=0.5,
    writeback=True,
)
# → one :SequentialPattern{items, support, count} node per frequent pattern, linked
#   PATTERN_ITEM → any item that is a resident node.
```

`sequence` with `writeback=True` is a **WAL-durable** graph write (replayed by
re-mining deterministically — explicit sequences reproduce byte-identically; a
graph-derived source re-derives from the current graph state).

## MCP + REST (sequence)

```jsonc
// MCP
graph_mine { "action": "sequence",
             "params_json": "{\"sequences\":[[\"login\",\"browse\",\"purchase\"]],\"min_support\":0.5}" }
graph_mine { "action": "sequence",
             "params_json": "{\"source\":{\"node_label\":\"Session\"},\"algorithm\":\"gsp\",\"writeback\":true}" }

// REST twin (same _execute_tool core)
POST /api/mining/sequence { "source": {"node_label": "Session"}, "min_support": 0.5 }
```

---

# Classical forecasting — `action="forecast"`

Forecasts `horizon` future points from a 1-D `values` series (a tsdb window handed in
by the caller — the same client-supplied cut `anomaly` took in Phase 2; the native
in-handler TsScan source is the same documented follow-up). Three hand-rolled,
dependency-free engines, selected by `algorithm`:

- **`arima`** (default) — ARIMA(`p`,`d`,`q`): `d`-order differencing to stationarity,
  then AR(`p`)/MA(`q`) coefficients fit by the **Hannan-Rissanen** two-stage method (a
  long auxiliary AR gives a residual proxy, then AR+MA terms are jointly estimated by
  OLS). Deterministic (closed-form least squares, no randomness).
- **`holtwinters`** — additive level/trend/seasonal exponential smoothing
  (`alpha`/`beta`/`gamma`, seasonal `period`); degrades to Holt's linear-trend method
  (ETS(A,A,N)) when `period` is 0 or the series is shorter than two seasonal cycles.
- **`stl`** — a classical (moving-average) trend/seasonal/residual decomposition, then
  a linear-trend + repeated-last-cycle forecast extension. A lightweight stand-in for
  full iterative Loess-STL (also returns the fitted `trend`/`seasonal`/`residual`).

Every algorithm returns an approximate confidence band (`lower`/`upper`) at the
two-sided `confidence` level (default `0.95`), widening with the forecast horizon.

```python
# ARIMA(1,1,0) — a pure linear trend needs one difference to become ~constant.
out = await c.mining.forecast(values=[5 + 3*t for t in range(30)], algorithm="arima", p=1, d=1, horizon=5)
# out["forecast"] tracks the trend's continuation; out["lower"]/out["upper"] widen with h.

# Holt-Winters over a seasonal series (period=12).
out = await c.mining.forecast(values=series, algorithm="holtwinters", period=12, horizon=12)

# STL decomposition + extrapolation, also returning the fitted components.
out = await c.mining.forecast(values=series, algorithm="stl", period=12, horizon=12)
# out["trend"], out["seasonal"], out["residual"] are each len(values) long.
```

## Write-back — materialize the forecast for the loop engine

With `writeback=True`, the forecast is materialized as a typed `:Forecast{horizon,
values, lower, upper}` node — linked `FORECAST_OF` to a resident node named
`series_id` when one is given and exists (e.g. a `:Metric` node whose id you pass as
`series_id`) — feeding the evolution flywheel's "anticipate where to invest" use case
(forecasting research-topic trajectories, capacity trends, etc.).

```python
# Forecast a named metric's series and write the result back, linked to its node.
out = await c.mining.forecast(
    values=metric_history, algorithm="holtwinters", period=7, horizon=7,
    series_id="metric:daily_active_users", writeback=True,
)
# → one :Forecast{horizon, values, lower, upper, series_id} node, linked FORECAST_OF
#   → the :metric:daily_active_users node (if resident).
```

`forecast` with `writeback=True` is a **WAL-durable** graph write (replayed by
re-forecasting deterministically from the same `values`).

## MCP + REST (forecast)

```jsonc
// MCP
graph_mine { "action": "forecast",
             "params_json": "{\"values\":[5,8,11,14],\"algorithm\":\"arima\",\"p\":1,\"d\":1,\"horizon\":5}" }
graph_mine { "action": "forecast",
             "params_json": "{\"values\":[...],\"algorithm\":\"stl\",\"period\":12,\"horizon\":12}" }

// REST twin (same _execute_tool core)
POST /api/mining/forecast { "values": [5, 8, 11, 14], "algorithm": "holtwinters", "period": 0, "horizon": 5 }
```

---

# Text mining — `action="text"`

Mines a text corpus: TF-IDF term weighting or `k`-topic modeling. Pure-Rust
tokenization (lowercase, alnum-run split) and algorithms — no Tantivy/eg-text
dependency (the corpus is either handed in pre-tokenized, or tokenized from a node
text property, compute-near-data):

- **`tfidf`** (default) — descriptive, read-only: per-document term weights
  (`term_frequency * smoothed_inverse_document_frequency`).
- **`lda`** — Latent Dirichlet Allocation fit by **collapsed Gibbs sampling**
  (symmetric Dirichlet priors `alpha` over doc-topic, `beta` over topic-term;
  `iterations` sweeps). Deterministic per `seed`.
- **`nmf`** — Non-negative Matrix Factorization of the TF-IDF matrix by
  **multiplicative updates** (Lee & Seung); `seed` only determines the initial
  factors. Deterministic given them.

```python
# TF-IDF over an explicit, pre-tokenized corpus.
out = await c.mining.text(docs=[
    ["the", "cat", "sat", "on", "the", "mat"],
    ["the", "rocket", "launched", "into", "orbit"],
])
# out["doc_terms"][1] ranks "rocket"/"launched"/"orbit" above the common "the".

# LDA topic model — k=2 over two thematically distinct document groups.
out = await c.mining.text(docs=pet_and_finance_docs, algorithm="lda", k=2, iterations=200, seed=42)
# out["topics"] recovers one topic per theme; out["doc_topics"][i] is doc i's mixture.
```

## Cross-modal — topic-model a node label's text property

Point `text` at a graph-derived `source` to tokenize a text property off every
instance of a node label (e.g. every `:Doc`'s `body` field) into the corpus, with
`writeback=True` (lda/nmf only) materializing each discovered topic as a `:Topic`
node — linked `HAS_TOPIC` from every document whose DOMINANT topic (the argmax of
its topic-membership distribution) it is.

```python
# Topic-model every :Doc's "body" text, writing :Topic nodes back.
out = await c.mining.text(
    source={"node_label": "Doc", "field": "body"}, algorithm="lda", k=5, writeback=True,
)
# → one :Topic{terms} node per topic, linked HAS_TOPIC ← every :Doc whose dominant
#   topic it is — a downstream feed for association-mining topics↔entities.
```

`text` with `writeback=True` is a **WAL-durable** graph write for `lda`/`nmf`
(replayed by re-mining deterministically); `tfidf` never writes back (no topics to
materialize — the flag is ignored).

## MCP + REST (text)

```jsonc
// MCP
graph_mine { "action": "text",
             "params_json": "{\"docs\":[[\"the\",\"cat\",\"sat\"]],\"algorithm\":\"tfidf\"}" }
graph_mine { "action": "text",
             "params_json": "{\"source\":{\"node_label\":\"Doc\",\"field\":\"body\"},\"algorithm\":\"lda\",\"k\":5,\"writeback\":true}" }

// REST twin (same _execute_tool core)
POST /api/mining/text { "docs": [["cat", "dog"], ["stock", "market"]], "algorithm": "nmf", "k": 2 }
```

---

# Frequent subgraph mining + motifs — `action="subgraph"` (the graph-native differentiator)

Every other mining action takes rows/vectors/sequences/series/docs as input. `subgraph`
is different: it mines the **resident graph's own topology directly** — no input rows
at all. This is the cross-modal headline: "structures that recur" over the graph
itself, not over a derived feature matrix.

- **`gspan`** (default) — level-wise frequent connected-subgraph PATTERN growth (the
  graph analog of Apriori/GSP): start from every frequent single labeled edge, then
  repeatedly extend by one edge (to a new node or closing a cycle) up to `max_edges`,
  canonicalizing each candidate (brute-force permutation — patterns stay tiny) and
  exactly re-counting its embeddings by backtracking subgraph isomorphism.
  `min_support` is a fraction of the host's total edge count. **Scope (honest):**
  support here is the raw EMBEDDING COUNT, not minimum-node-image support — a
  documented simplification, like the brute k-NN scan or small-N UMAP/t-SNE cuts
  elsewhere in this family.
- **`motif`** — a classical, label-agnostic topological census (Milo-style): open
  wedges (2-paths), triangles (closed triads), and directed 3-cycles.

`label`, when given, restricts the scanned host graph to nodes of that ONE type (both
edge endpoints must match) — `None` scans the whole resident graph heterogeneously.

```python
# Frequent 1-edge patterns across the whole resident graph.
out = await c.mining.subgraph(min_support=0.05, max_edges=1)
# out["patterns"] == [{"nodes": [...], "edges": [{"from":0,"to":1,"label":...}], "support":..., "count":...}, ...]

# Motif census restricted to :Concept nodes.
out = await c.mining.subgraph(label="Concept", algorithm="motif")
# out["motifs"] == {"wedge": ..., "triangle": ..., "directed_cycle3": ...}
```

## Write-back — the discovery flywheel closes on the graph itself

With `writeback=True` (`gspan` only), each frequent pattern is materialized as a typed
`:FrequentSubgraph{nodes, edges, support, count}` node, linked `SUBGRAPH_MEMBER` to
every host node appearing in any of its embeddings — feeding the next
OWL-reason + mine cycle: recurring structures the KG discovers about ITSELF.

```python
# Mine frequent :Concept--touches-->:Capability-shaped structures and write them back.
out = await c.mining.subgraph(min_support=0.1, max_edges=2, writeback=True)
# → one :FrequentSubgraph{nodes, edges, support, count} node per pattern, linked
#   SUBGRAPH_MEMBER → every node instance that participates in it.
```

`subgraph` with `writeback=True` is a **WAL-durable** graph write for `gspan`
(replayed by re-mining the current graph state deterministically); `motif` never
writes back (a pure census, no patterns to materialize — the flag is ignored).

## MCP + REST (subgraph)

```jsonc
// MCP
graph_mine { "action": "subgraph",
             "params_json": "{\"min_support\":0.1,\"max_edges\":2,\"writeback\":true}" }
graph_mine { "action": "subgraph",
             "params_json": "{\"label\":\"Concept\",\"algorithm\":\"motif\"}" }

// REST twin (same _execute_tool core)
POST /api/mining/subgraph { "min_support": 0.1, "max_edges": 2, "algorithm": "gspan" }
```
