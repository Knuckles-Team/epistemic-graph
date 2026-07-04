# eg-ann — native IVF-PQ + OPQ + SQ8-refine ANN index (CONCEPT:EG-KG.sharding.semantic-embedding-store-backed)

A pure-Rust, Pi-lean approximate-nearest-neighbour index that replaces the
rebuild-on-load `hnsw_rs` store in `eg-core::compute::semantic`. Increment 1 of a
multi-week vector-at-scale track; productionises the throwaway `spike/quantized-ann`
spike, with **production-real recall** (the spike deliberately under-delivered it).

## Where it sits in the workspace DAG

```
eg-types → eg-ann → eg-core → eg-compute → epistemic-graph
                ↘ (leaf: serde + memmap2 + rayon + rand; redb optional)
```

`eg-ann` is a leaf crate (no workspace deps); `eg-core` depends on it under the
`ann` feature. Serving links **no GPU / no faiss / no native-ML** — proven by
`cargo tree -e normal | grep -iE 'cuda|cudarc|faiss'` returning nothing in the
`default`, `pi`, and `full` facade builds.

## Design

The index is the standard production recall recipe (`OPQ + IVF + PQ + Refine`):

1. **OPQ rotation** `R` (`linalg.rs`, `ivfpq::train`) — an orthogonal `dim×dim`
   matrix learned at build time (alternating PQ-retrain ↔ `R = polar(Σx·x̂ᵀ)` via
   a dependency-free one-sided Jacobi SVD) that re-aligns the axes so PQ
   distortion is minimised. Vectors and queries are rotated by `R` before
   encoding/search. Biggest single lever on raw-PQ candidate quality.
2. **IVF coarse quantizer** — `nlist ≈ √N` k-means cells (k-means++ seeded,
   `kmeans.rs`); a vector lands in its nearest cell's posting list; search probes
   the `nprobe` nearest cells.
3. **PQ codes** — the rotated residual is split into `m` subspaces, each → one of
   256 codebook entries ⇒ `m` bytes/vec. Search scores candidates with an ADC
   table (integer code → precomputed sub-distance, summed; no decode).
4. **SQ8 refine tier** — a scalar-quantized (1 byte/dim, per-row min/scale) copy of
   each rotated vector. Search over-fetches `refine_factor·k` ADC candidates and
   re-ranks them by the near-exact SQ8 distance. **This recovers recall to target**:
   ADC finds the right neighbourhood cheaply, SQ8 orders it accurately.

### Measured recall / latency / footprint

`cargo bench -p eg-ann --bench recall -- 100000 128 1024 32 32 200` (clustered
synthetic embeddings, 24-core box):

| Metric | Value |
|---|---|
| Raw f32 footprint | 0.051 GB |
| PQ codes footprint | 0.003 GB (**16× compression**, m=32 @ dim=128) |
| SQ8 refine footprint | 0.013 GB (4× vs raw, mmappable) |
| **ADC-only recall@10** | 0.745 @ ~41 ms/query |
| **ADC+refine recall@10** | **0.9675** @ ~40 ms/query — **clears the ≥0.95 bar** |

At dim=768/m=96 the PQ codes are 32× compression; the resident RAM for 1B vectors
with codes mmapped is ≈16 GB (vs 3 TB raw). `tests::recall_at_10_meets_target`
asserts ≥0.95 at 40k×128 (a CI-runnable scale; the bench scales to 1M). Build
(OPQ + k-means training) is the cost centre, as the spike predicted — query is
cheap CPU integer-table lookups.

## Persistence — no-rebuild reopen

`persist::save` writes a directory: `meta.bin` (rotation, centroids, codebooks,
ids, `list_of`, tombstones, SQ8 min/scale), `codes.bin` (PQ codes), `refine.bin`
(SQ8 codes). `persist::open` mmaps the two code files and rebuilds posting lists
with **one O(N) integer pass** — no k-means, no SVD, no f32 reconstruction.
`tests::persist_reopen_no_rebuild_matches` proves identical results after reopen.
With the `redb` feature, `redb_store::{save_redb,open_redb}` persist into the
engine's redb durable tier (CONCEPT:AU-KG.backend.backend-modes) instead.

## Updates

- **Insert** — `add` rotates + encodes + appends to the IVF list (no rebuild).
- **Delete** — `delete` tombstones every row for an id (overwrite = re-add + delete).
- **Compaction / VACUUM** — `persist::compact` rewrites the index dropping
  tombstoned rows **without retraining** (rotation + codebooks kept), reclaiming
  posting-list bloat. `SemanticStore` triggers it past a 30% tombstone ratio.

## SemanticStore wiring (feature `ann`)

`eg-core::compute::semantic::SemanticStore` selects between the default `hnsw_rs`
backend and this one at compile time (`#[path]`-mux in `semantic.rs`). The ANN
backend keeps the **identical public API** (`add_embedding` / `semantic_search` /
`force_compact`) and **identical serde shape** (only the embeddings map persists,
so snapshots are interchangeable). It uses brute-force cosine below
`ANN_BUILD_THRESHOLD` and the eg-ann index above it; cosine is realised as
squared-L2 over L2-normalised vectors (`cos = 1 − d/2`). A persisted eg-ann index
reopens via `load_index` WITHOUT rebuilding from raw vectors. The facade folds
`ann` into the durable serving tiers (`pi`/`node`/`cluster`/`full`).

## Deferred (later increments)

- **Cross-shard scatter-gather kNN merge** (pairs with CONCEPT:EG-KG.ingest.ingest-lane-affinity) — the
  index is per-graph/per-shard today; the k-way distance merge over the existing
  HRW shard fan-out is a separate increment.
- **Hybrid filtered search** (metadata pre/post-filter pushed into the posting-list
  scan; lexical+vector RRF fusion).
- **GPU / training acceleration** — gated behind the `gpu` feature, OUT of
  `pi`/`full`; serving is CPU integer-table lookups and needs no accelerator.
- **Multi-vector / multimodal** — one IVF-PQ index per embedding space; ColBERT
  max-sim is a bigger lift.
- **Mini-batch / sub-linear coarse assignment** to cut build time at billions.
