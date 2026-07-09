# Graph Learning — KAN link prediction (`graph_learn` / `/api/graphlearn`)

`CONCEPT:EG-KG.graphlearn.link-predictor`

Track G / **EpiGNN**: a learnable link-predictor over the resident graph whose
learned edge-function is *itself* a queryable KG node. The headline is
**interpretability, not raw accuracy** — a vector-similarity or embedding-only stack
can rank missing links, but it cannot answer *why*. Here the scoring function that
combines structural signals into a link probability is a small set of learned,
inspectable univariate curves (one per feature), materialized as `:EdgeFunction`
nodes you can Cypher-query.

This is a pure-Rust, resident-graph-scale primitive (comparable in ambition to the
existing Jaccard/Louvain machinery). Deep multi-layer KAN-GNNs (GraphKAN/KAGAT at
scale, torch, GPU) stay a **data-science-mcp** job whose distilled outputs
(bulk embeddings, a distilled edge-function) flow back through the same write-back
seam — the engine never runs heavy training, only consumes its result.

## The KAN design as built

A **Kolmogorov-Arnold Network** replaces an MLP's fixed node nonlinearity + linear
edge weight with a *learnable univariate function on each edge*.

- **`KanEdgeFn`** (`CONCEPT:EG-KG.graphlearn.kan-edge-function`) — a learnable
  `f(x) = Σ_k c_k · B_k(tanh x)` over an orthogonal-polynomial basis. `tanh` squashes
  the input into the basis's `[-1, 1]` orthogonality domain so arbitrary-scale
  features are admissible. Forward + **analytic** gradients (w.r.t. the coefficients
  for training; w.r.t. the input for message passing) — no autodiff engine, because a
  1-D closed-form function needs none. The **coefficients are the interpretable
  artifact**.
- **Basis** (`CONCEPT:EG-KG.graphlearn.chebyshev-basis`) — Chebyshev (first kind) by
  default, Jacobi optional. Both are closed-form three-term recurrences with **zero
  special functions**, deliberately NOT B-spline/RBF — matching the engine's
  no-BLAS/no-libm Raspberry-Pi contract. (`ChebyKAN`/`JacobiKAN`/`TaylorKAN` validate
  the polynomial basis as a competitive spline substitute.)
- **Link predictor** (`CONCEPT:EG-KG.graphlearn.link-predictor`) — per candidate pair
  `(u, v)` a small structural feature vector is built from what is already resident:

  | feature | source |
  |---|---|
  | `common_neighbors` | undirected neighbour-set intersection |
  | `jaccard` | `graph_algos::similarity` |
  | `adamic_adar` | `Σ 1/ln deg(w)` over common neighbours |
  | `preferential_attachment` | `deg(u)·deg(v)` |
  | `pagerank_product` | `graph_algos::pagerank` |
  | `neighbor_cosine` | neighbour-set cosine |
  | `node_feature_dot` | dot of 1-hop-aggregated node features |

  These are z-standardised then fed through a **1–2 layer KAN** — each layer output is
  a **sum of `KanEdgeFn`s over its inputs** (the exact KAN definition) — down to a
  sigmoid link probability. Default `hidden = 0` ⇒ a single layer with **one edge
  function per feature**, so every learned curve is directly inspectable. Training is
  **batch, one round-trip**: observed edges are positives, sampled non-edges are
  negatives, K epochs of forward + analytic backward + **Adam** (reusing
  `datascience::training::adam_step`) run inside one engine call.
- **1-hop neighbour aggregation** (`CONCEPT:EG-KG.graphlearn.neighbor-aggregation`) —
  message-passing lite: `x'_v = α·x_v + (1−α)·mean(x_u : u ∈ N(v))` refines node
  features before scoring, optionally edge-weighted by a learned `KanEdgeFn`.

## Surfaces

The five-layer pattern (mirrors `mining`):
`Method::{GraphLearnFit, GraphLearnPredict}` (feature `graphlearn`) →
`handlers/graphlearn.rs` → `client.graphlearn.{fit, predict}` → the `graph_learn`
MCP verb + the `/api/graphlearn/{fit,predict}` REST twin.

### Fit

```python
res = await client.graphlearn.fit(
    node_label="Person", direction="any",
    basis="chebyshev", degree=4, epochs=200, writeback=True,
)
# {model, n_nodes, n_edges, train_auc,
#  edge_functions: [{feature, coefficients}, ...], edge_functions_written}
```

`GraphLearnFit` learns over a **graph-derived subgraph** (every node carrying
`node_label`; edges among them — following `direction`, optionally filtered to
`relation` — are the positives). With `writeback=True` each learned per-feature curve
becomes a typed **`:EdgeFunction`** node
(`CONCEPT:EG-KG.graphlearn.edge-function-writeback`).

### Predict

```python
pred = await client.graphlearn.predict(
    model=res["model"], node_label="Person", top_k=20, writeback=True,
)
# {predicted: [{src, dst, score}, ...], n_predicted, model, written_back}
```

`GraphLearnPredict` scores explicit `candidate_pairs` or the top-`k` highest-probability
**missing** links. With `writeback=True` each becomes a typed **`:PredictedEdge`** node
linked to its endpoints (`CONCEPT:EG-KG.graphlearn.predicted-edge-writeback`).

Both write-back paths are graph **mutations** → classify as writes and **WAL-replay**
by re-deriving from the current graph (deterministic given the seed), exactly like
mining.

## Interpretability example

After a fit with `writeback=True`, the learned scoring curve is queryable:

```cypher
MATCH (e:EdgeFunction) RETURN e.feature, e.coefficients ORDER BY e.feature
```

Each row is the Chebyshev coefficient vector of the univariate function that maps that
structural feature to its contribution to the link score — you can plot it, diff it
across fits, or reason over it in the KG like any other node.

## Honest ceiling

A lightweight, resident-graph-scale link-predictor / embedding-refiner. It is *not* a
research-grade GNN; deep multi-layer KAN-GNNs stay a data-science-mcp/torch job. This
track fills a real, well-scoped gap (there was no link-prediction / message-passing
primitive) without introducing a new architectural posture — small/analytic/batch/
in-engine vs. deep/GPU/torch/external is the line this codebase already draws.
