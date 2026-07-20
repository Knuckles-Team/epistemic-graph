# Analytics Program — "one kernel, two surfaces"

> The program that turns epistemic-graph into an **analytical system embedded in the data**:
> a slim, BLAS/LAPACK-free Rust numeric kernel (`crates/eg-numeric`) that serves **both**
> Python-side array math (replacing direct numpy/scipy use in agent-utilities) **and**
> in-database analytics over engine-resident data. This page is the map; the kernel
> is documented in full at **[numeric-kernel](numeric_kernel.md)**.

## Thesis

epistemic-graph already does **compute-near-data** for vectors (`semantic_search` and
`batch_l2_normalize` run server-side in Rust, zero numpy). The Analytics Program generalizes that
proven pattern into **one numeric kernel exposed on two surfaces**:

- **Surface A — in-process Python.** `epistemic_graph.numeric` (a pyo3 extension) + the
  kernel-required agent-utilities `xp` surface (`CONCEPT:AU-KG.compute.surface-analytics-program`). For
  **transient** data (finance dataframes, ad-hoc KG math) computed in-process.
- **Surface B — engine operators.** The *same* rlib exposed as DataFusion SQL UDFs/UDAFs +
  graph/vector/timeseries analytics, for **engine-resident** data (embeddings, columnar,
  graph) with no FFI round-trip.

**Decision rule:** data already in the engine → Surface B (compute-near-data). Data transient
in Python → Surface A. Never round-trip transient data into the DB just to compute.

## Current capability

| Surface | Scope |
|---------|-------|
| Rust kernel | `eg-numeric` reductions, statistics, element-wise operations, faer linear algebra, seedable random operations, and 847 `np.allclose` parity checks (`CONCEPT:AU-KG.compute.numeric-kernel`) |
| Python | `epistemic_graph.numeric` plus `from agent_utilities.numeric import xp as np`; the compiled kernel is required |
| SQL analytics | DataFusion `cosine_sim`, `l2_normalize`, `zscore`, `covariance`, `svd`, `pca`, and `kmeans` operators over resident data |
| Native method | `BatchL2Normalize` through the engine client |
| Cross-modal analytics | Graph ⋈ vector ⋈ time-series joins followed by PCA, clustering, or covariance in the shared query path (`CONCEPT:EG-KG.query.eg-3`) |
| Agent Utilities dependency | `epistemic-graph[full]`; Python `[full]` includes numeric interoperability dependencies, while the `xp` surface retains a kernel-owned numpy tail for unsupported shapes |

> **Release policy.** Agent Utilities installs the approved
> `epistemic-graph[full]` artifact. Importing its numeric compatibility
> surface without the compiled kernel fails immediately; there is no package-level
> fallback or partial engine profile.

## Rust/Python feature boundary

Cargo `full` includes the `numeric` Rust feature, so the main engine links the pure
faer/ndarray kernel. The Python `[full]` extra applies only at installation time
and includes `[numeric]`; it does not select Rust features. pyo3 sits behind the
kernel crate's `python` feature and remains off in the server binary
(`cargo tree | grep -ci pyo3` = 0).

## Synergies

- **LTAP / lakehouse (EG-KG.storage.lsn-as-snapshot-returns)** exposes engine data as Parquet/Delta; Surface B adds native
  in-engine analytics over that same columnar data → the engine is the OLAP **store AND compute**.
- **GPU extras (`gpu-cuda`, EG-KG.backend.real-cuda-tensor-backend)** accelerate exactly the kernel's hot ops
  (distance/matmul/SVD) behind the same optional `full-extras` flag —
  see [distribution-robotics-gpu](distribution_robotics_gpu.md).

## References

- Rust kernel and operation surface: **[numeric-kernel](numeric_kernel.md)** (`CONCEPT:AU-KG.compute.numeric-kernel`).
- Agent Utilities `xp` surface: `agent_utilities/numeric/` (`CONCEPT:AU-KG.compute.surface-analytics-program`) and the
  agent-utilities KV-cache-layering / numeric docs.
- Full end-to-end program tracker: `plans/epistemic-graph-analytics_program.md` (workspace).

---

**See also:** [Capabilities matrix](../capabilities.md) · [Numeric Kernel](numeric_kernel.md) · [Vector / ANN](../interfaces/vector.md) · [SQL & pgwire](../interfaces/sql.md) · [Time-series](../interfaces/timeseries.md).
