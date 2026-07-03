# Analytics Program — "one kernel, two surfaces"

> The program that turns epistemic-graph into an **analytical system embedded in the data**:
> a slim, BLAS/LAPACK-free Rust numeric kernel (`crates/eg-numeric`) that serves **both**
> Python-side array math (replacing numpy in agent-utilities) **and** — in later phases —
> in-database analytics over engine-resident data. This page is the map; the shipped P1 kernel
> is documented in full at **[numeric-kernel](numeric_kernel.md)**.

## Thesis

epistemic-graph already does **compute-near-data** for vectors (`batch_cosine_similarity`,
`semantic_search` run server-side in Rust, zero numpy). The Analytics Program generalizes that
proven pattern into **one numeric kernel exposed on two surfaces**:

- **Surface A — in-process Python.** `epistemic_graph.numeric` (a pyo3 extension) + the
  agent-utilities `xp` numpy-shim (`CONCEPT:KG-2.312`, transparent numpy fallback). For
  **transient** data (finance dataframes, ad-hoc KG math) computed in-process.
- **Surface B — engine operators.** The *same* rlib exposed as DataFusion SQL UDFs/UDAFs +
  graph/vector/timeseries analytics, for **engine-resident** data (embeddings, columnar,
  graph) with no FFI round-trip.

**Decision rule:** data already in the engine → Surface B (compute-near-data). Data transient
in Python → Surface A. Never round-trip transient data into the DB just to compute.

## Status — honest, phased

| Phase | Scope | Status |
|-------|-------|:------:|
| **P1** | `eg-numeric` kernel (reductions/stats · element-wise · faer linalg · seedable random) + **Surface A** Python extension + the AU `xp` shim; 847 `np.allclose` parity checks | ✅ **shipped this release** (`CONCEPT:EG-321`) |
| **P2–P3** | mechanical `import numpy as np` → `from agent_utilities.numeric import xp as np` migration (light-op files, then linalg files) | 🗺 follow-on |
| **P4** | **Surface B** — DataFusion SQL UDFs/UDAFs over the kernel (`cosine_sim`/`l2_normalize`/`zscore`/`covariance`) + the `BatchL2Normalize` engine Method, all behind `numeric` (out of `pi`) | ▶ **first increment shipped** (`CONCEPT:EG-329`/`EG-330`) |
| **P4 (cont.)** | `svd(vec_col)` / `pca(vec_col,k)` column→matrix UDAFs (`MatrixAcc` row-buffering accumulator → dense `ndarray::Array2` → faer `svdvals`/`eigh`; results render as JSON arrays) | ✅ **shipped** (`CONCEPT:EG-336`/`EG-335`) |
| **P4 (cont.)** | `kmeans(vec_col,k)` column→matrix clustering UDAF (new pure-Rust `eg-numeric::cluster` Lloyd + k-means++ kernel, **no linfa/BLAS**; one `List<Int64>` label per row) | ✅ **shipped** (`CONCEPT:EG-344`) |
| **P4 — the differentiator** | **cross-modal join → PCA/cluster in-engine** — join graph ⋈ vector ⋈ timeseries, then `pca`/`kmeans`/`covariance` over the joined result set in-engine (**impossible in numpy** — no data layer). E2E-proven in `crates/eg-query/tests/cross_modal_analytics.rs` | ✅ **shipped** (`CONCEPT:EG-345`) |
| **P4 (next)** | graph/timeseries unification under the kernel via native `Method` surfaces (beyond SQL) | 🗺 follow-on |
| **P5** | drop numpy/scipy from agent-utilities; the `eg-numeric` wheel is the dep (the `xp` shim stays so future backend swaps remain mechanical) | 🗺 follow-on |

> **Release policy.** P1 ships in the current release. **P2–P5 + full Surface B** proceed as a
> parallel follow-on program *after* this release — they do **not** block the current publish.

## Pi contract

The `numeric` cargo feature (`numeric = ["dep:eg-numeric"]`) is in the **`full`** tier only —
out of `pi`/`pi-max`/`node` and the size-optimized `release-tiny` profile, so faer/ndarray never
bloat the lean Raspberry-Pi tier (`cargo tree --features pi` links no eg-numeric/faer/ndarray).
pyo3 sits behind eg-numeric's own `python` feature, off in every engine build.

## Synergies

- **LTAP / lakehouse (EG-317)** exposes engine data as Parquet/Delta; Surface B adds native
  in-engine analytics over that same columnar data → the engine is the OLAP **store AND compute**.
- **GPU extras (`gpu-cuda`, EG-327)** accelerate exactly the kernel's hot ops
  (distance/matmul/SVD) behind the same optional `full-extras` flag —
  see [distribution-robotics-gpu](distribution_robotics_gpu.md).

## References

- Shipped P1 kernel & op-surface: **[numeric-kernel](numeric_kernel.md)** (`CONCEPT:EG-321`).
- AU-side `xp` shim & migration: `agent_utilities/numeric/` (`CONCEPT:KG-2.312`) and the
  agent-utilities KV-cache-layering / numeric docs.
- Full end-to-end program tracker: `plans/epistemic-graph-analytics_program.md` (workspace).

---

**See also:** [Capabilities matrix](../capabilities.md) · [Numeric Kernel](numeric_kernel.md) · [Vector / ANN](../interfaces/vector.md) · [SQL & pgwire](../interfaces/sql.md) · [Time-series](../interfaces/timeseries.md).
