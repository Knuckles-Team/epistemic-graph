# Numeric kernel — one kernel, two surfaces (CONCEPT:EG-321)

> **P1 of the Analytics Program** (`plans/epistemic-graph-analytics-program.md`,
> design `reports/epistemic-graph-numeric-kernel-handoff.md`). A slim, **BLAS/LAPACK-free**
> Rust numeric kernel that serves **both** Python-side array math (replacing numpy in
> agent-utilities) **and** — in later phases — in-database analytics over engine-resident
> data. This doc covers the shipped P1 kernel; P2–P5 are on the [roadmap](../roadmap.md).

## Thesis

epistemic-graph already does **compute-near-data** for vectors
(`batch_cosine_similarity`, `semantic_search` run server-side in Rust, zero numpy).
The Analytics Program generalizes that proven pattern into **one numeric kernel**
(`crates/eg-numeric`) exposed on **two surfaces**.

```mermaid
flowchart TD
    subgraph K["crates/eg-numeric — pure Rust kernel (rlib)"]
        direction TB
        ND["ndarray 0.16<br/>arrays · reductions · element-wise"]
        FA["faer 0.20<br/>svd · eigh · solve · pinv · lstsq · qr · cholesky<br/>(NO system BLAS/LAPACK)"]
        RN["rand / rand_distr<br/>seedable RNG"]
        ERR["NumericError → LinAlgError<br/>(numpy parity)"]
    end

    K -->|"feature: python<br/>(pyo3 + rust-numpy,<br/>zero-copy + allow_threads)"| SA
    K -->|"rlib link<br/>(NO pyo3 — python feature OFF)"| SB

    subgraph SA["Surface A — in-process Python"]
        M1["epistemic_graph.numeric<br/>(extension module)"]
        M2["agent_utilities.numeric.xp<br/>(np-shim, numpy fallback) — KG-2.312"]
        M1 --> M2
    end

    subgraph SB["Surface B — engine operators (P4, 🗺)"]
        D1["DataFusion SQL UDFs/UDAFs<br/>pca · covariance · zscore · svd"]
        D2["graph / vector / timeseries analytics"]
        D3["cross-modal join → PCA/cluster in-engine"]
    end

    TA["transient data<br/>(finance dataframes, ad-hoc KG math)"] --> SA
    TB["engine-resident data<br/>(embeddings, graph, columnar, timeseries)"] --> SB

    classDef done fill:#d5f5e3,stroke:#1e8449;
    classDef todo fill:#fdebd0,stroke:#b9770e;
    class K,SA done;
    class SB todo;
```

**Decision rule (which surface):** data **already in the engine** → Surface B
(compute-near-data, no FFI). Data **transient in Python** (API dataframes, in-memory
arrays) → Surface A (in-process). Never round-trip transient data into the DB just to
compute (anti-pattern).

## Crate layout & feature gating

`crates/eg-numeric` is a workspace member with `crate-type = ["cdylib", "rlib"]`:

- **`rlib`** — the pure kernel (`reductions`, `elementwise`, `linalg`, `random`,
  `error`). No pyo3. This is what the engine links for Surface B.
- **`cdylib`** — the Surface-A Python extension, built only with `--features python`
  (pulls `pyo3` + `numpy`/`rust-numpy`).

Feature discipline (the Pi contract):

| build | pulls eg-numeric? | faer/ndarray? | pyo3? |
|-------|:---:|:---:|:---:|
| `--features pi` / `pi-max` / `node` / default | ❌ | ❌ | ❌ |
| `--features full` (→ `numeric`) | ✅ (rlib) | ✅ | ❌ |
| `maturin ... -m crates/eg-numeric/Cargo.toml --features python` | ✅ (cdylib) | ✅ | ✅ |

- The top-level `numeric` cargo feature (`numeric = ["dep:eg-numeric"]`) is in `full`
  only — **out of `pi`/`pi-max`/`node`** and the size-optimized `release-tiny` profile,
  so faer/ndarray never bloat the lean Raspberry-Pi tier. Verified:
  `cargo tree --no-default-features --features pi` links **no** eg-numeric/faer/ndarray.
- pyo3 is behind eg-numeric's **own** `python` feature (off in every engine build), so a
  `numeric` engine build links the kernel rlib but **no Python extension** — the Plan-01
  "no Python extension in the engine binary" contract holds (`scripts/check_no_pyo3.sh`
  stays green; the main wheel remains `bindings = "bin"`).

## Op-surface (P1)

Curated from the real audit (`grep np\.` over `agent_utilities/`) — **not** "all of numpy":

- **Reductions / stats:** `sum · prod · mean · var(ddof) · std(ddof) · min · max ·
  argmin · argmax · argsort · cumsum · cumprod · percentile · quantile`.
- **Element-wise:** `sqrt · log · exp · abs · tanh · clip · maximum · minimum · where ·
  nan_to_num · isnan`.
- **Linalg (faer):** `norm(+ord) · dot · matmul · solve · svd · svdvals · eigh · pinv ·
  lstsq · qr · cholesky · det · inv · matrix_power`, plus a `LinAlgError` exception that
  mirrors `numpy.linalg.LinAlgError` (raised on singular / non-PD).
- **Random:** `normal · uniform · integers` (seedable ChaCha20; deterministic, and
  distribution-parity — bit-for-bit parity with numpy's PCG64 is a non-goal).

## Parity

Every op is asserted `np.allclose` vs numpy on randomized inputs, with mandatory edge
cases (nan/inf, singular matrices, empty arrays). P1 landed **847 parity checks, 0
failures**. The corpus lives in agent-utilities (`tests/test_numeric_parity.py`, KG-2.312)
and runs against the compiled kernel when present, else the numpy fallback. A **second,
engine-side** corpus (`crates/eg-numeric/tests/test_kernel_parity.py`, CONCEPT:EG-346)
tests the compiled kernel DIRECTLY (not the shim) and is what the CI gate runs — see below.

## Building the Surface-A wheel & the kernel-live shim (CONCEPT:EG-346 / KG-2.315)

The Surface-A extension ships as an **installable pyo3 cdylib wheel**, so
`agent_utilities.numeric.xp` runs **kernel-LIVE** (`HAVE_KERNEL == True`) instead of the
numpy fallback (KG-2.315). Build it standalone with maturin:

```bash
# On CPython ≤ 3.13 (pyo3 0.22's supported range) no flag is needed.
# On a newer interpreter (e.g. 3.14) pyo3 0.22 refuses to build unless you opt in:
export PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1
maturin build --release -m crates/eg-numeric/Cargo.toml --features python --out target/wheels
pip install --no-index --find-links target/wheels eg-numeric
```

The `#[pymodule]` is named `numeric` with `m.add("__kernel__", "eg-numeric")`, so the
wheel installs a top-level `numeric` package (`import numeric`); a folded product build
homes it at `epistemic_graph.numeric`. The `xp` shim probes **both** names and checks the
`__kernel__` marker, so either layout activates the kernel. With the wheel installed:

```python
>>> from agent_utilities.numeric import xp, HAVE_KERNEL, KERNEL_SOURCE
>>> HAVE_KERNEL, KERNEL_SOURCE
(True, 'numeric')          # every routed 1-D/2-D float64 op now hits faer/ndarray
```

**This is a SEPARATE wheel from the engine facade** (`bindings = "bin"` — the server
binary, no pyo3). `scripts/check_no_pyo3.sh` guards the *facade* (`src/`,
`epistemic_graph/`, top-level `Cargo.toml`/`pyproject.toml`) and never scans `crates/`, and
the Pi contract holds unchanged — `cargo tree --features pi | grep -ci pyo3` = 0 (pyo3 is
only ever pulled by eg-numeric's own `python` feature, which no engine tier enables).

### numpy-parity CI gate

The `numeric-parity` job in `.github/workflows/rust-ci.yml` maturin-builds the wheel,
installs it, and runs `crates/eg-numeric/tests/test_kernel_parity.py` — a self-contained
corpus that asserts **every** compiled-kernel op equals its numpy reference (`np.allclose`)
across the full op-surface, including the mandatory edge cases (nan/inf, singular matrix,
empty). The job **FAILS CI if the Rust kernel ever diverges from numpy**, which is what
makes it safe to run the `xp` shim kernel-live in production. CI uses Python 3.12 (inside
pyo3-0.22's native range, so no forward-compat flag is needed there).

## Surfaces in later phases

- **Surface A migration (P2–P3):** mechanical `import numpy as np` →
  `from agent_utilities.numeric import xp as np` across the 32 light-op files then the 6
  linalg files.
- **Surface B (P4):** the SAME rlib exposed as DataFusion UDFs/UDAFs + graph/vector/
  timeseries operators, re-homing KG-resident numerics (spectral_navigator, world_model)
  to compute-near-data; cross-modal joins then PCA/cluster in-engine. **First increment
  shipped — see below.**
- **P5:** drop numpy/scipy from agent-utilities; the `eg-numeric` wheel is the dep. The
  `xp` shim stays so future backend swaps remain mechanical.

## Surface B — in-database analytics operators (P4, CONCEPT:EG-329/EG-330/EG-335/EG-336)

The pure kernel rlib is wired into the engine's query surface so analytics run **where the
data lives** — no fetch-to-Python, no FFI. Two reach paths:

```mermaid
flowchart TD
    subgraph kernel["eg-numeric (rlib, feature numeric — faer + ndarray, NO pyo3)"]
      K1["linalg: dot · norm · svd · batch_l2_normalize"]
      K2["reductions: mean · std · var"]
    end
    subgraph sql["Surface B / SQL  (eg-query, feature numeric ⊃ sql)"]
      U1["cosine_sim(a,b) → Float64  (scalar)"]
      U2["l2_normalize(v) → List&lt;Float32&gt;  (scalar)"]
      U3["zscore(col) → Float64  (scalar-over-batch)"]
      U4["covariance(a,b) → Float64  (UDAF)"]
      U5["svd(vec_col) → List&lt;Float64&gt;  (UDAF, col→matrix)"]
      U6["pca(vec_col,k) → List&lt;List&lt;Float64&gt;&gt;  (UDAF, col→matrix)"]
    end
    subgraph rpc["Surface B / Method  (src/server/handlers)"]
      M1["BatchL2Normalize { vectors }"]
    end
    SQL["SELECT zscore(price) …\nSELECT svd(emb) …\nSELECT pca(emb,3) …"] --> U1 & U2 & U3 & U4 & U5 & U6
    U1 & U2 & U3 & U4 & U5 & U6 --> kernel
    client["client.batch_l2_normalize(vectors)"] --> M1 --> kernel
    kernel --> pi{{"Pi-contract: numeric ∉ pi\n→ no eg-numeric/faer in the pi tree"}}
```

**SQL operators** (registered on the graph-exec AND obs-tables `SessionContext`, gated
`#[cfg(feature = "numeric")]` in `crates/eg-query/src/sql/exec.rs::register_numeric`,
implemented in `crates/eg-query/src/sql/numeric.rs`):

| Operator | Kind | Kernel | Semantics |
|----------|------|--------|-----------|
| `cosine_sim(a, b)` | scalar → `Float64` | `linalg::dot`/`norm` | `a·b/(‖a‖‖b‖)`; the raw-similarity complement to EG-115 `vector_cosine` (distance). Accepts a stored `List<Float{32,64}>` column or a `'[1,2,3]'` text literal; NULL on a dimension mismatch. |
| `l2_normalize(v)` | scalar → `List<Float32>` | `linalg::norm` | unit vector `v/‖v‖` (pgvector type — feeds `cosine_sim`/ANN in-query); zero-norm returned unchanged. |
| `zscore(col)` | scalar-over-batch → `Float64` | `reductions::mean`/`std` | standardize `(x-mean)/std` (population `ddof=0`) over the materialized batch. Exact for the engine's single-partition MemTable/`NodesTableProvider` (one batch/table); a global two-pass is the `(x-avg(x) OVER())/stddev(x) OVER()` window form. |
| `covariance(a, b)` | UDAF → `Float64` | `reductions::mean` | sample covariance `Σ(aᵢ-ā)(bᵢ-b̄)/(n-1)`; buffers aligned non-null pairs, merge state = two `List<Float64>` columns. |
| `svd(vec_col)` (EG-336) | UDAF → `List<Float64>` | `linalg::svdvals` | **column→matrix**: stacks the aggregated vector column into an `n×d` matrix (each row = one matrix row; same operand forms as `cosine_sim` — a `List<Float{32,64}>` column or `'[..]'` text) and returns its singular values (descending). |
| `pca(vec_col, k)` (EG-335) | UDAF → `List<List<Float64>>` | `reductions::mean` + `linalg::eigh` | **column→matrix**: mean-centers the `n×d` matrix, eigendecomposes the `d×d` sample covariance (`ddof=1`), and returns the top-`k` principal-component DIRECTIONS as `k` unit vectors of length `d`, **descending by explained variance** (sign arbitrary; `k` clamped to `d`; projected coords = `X_centered·componentsᵀ` downstream). |

**Column→matrix marshalling** (the deferred-in-P4 bridge, `svd`/`pca`): both are UDAFs whose
`Accumulator` (`MatrixAcc`) decodes each ingested row via `row_to_vector` (the same operand
forms `cosine_sim` accepts) and buffers it row-major into a flat `Vec<f64>` + fixed `dim`
(ragged rows skipped, NULL-safe); `evaluate()` reshapes to a dense `ndarray::Array2` and runs
the faer kernel. Partial-aggregate state is the flat buffer as a `List<Float64>` plus `dim`
(and `k` for `pca`) as `Int64`, so multi-phase grouping merges losslessly. The `List<Float64>`
/ `List<List<Float64>>` results render as structural JSON arrays via `cell_to_json`
(alongside the pgvector `List<Float32>` path).

Example — standardize a price column, rank rows by similarity, and reduce embeddings, all in-engine:

```sql
SELECT id, zscore(price) AS z FROM nodes ORDER BY z DESC;
SELECT a.id, cosine_sim(a.emb, b.emb) AS sim FROM nodes a JOIN nodes b ON a.id <> b.id;
SELECT svd(emb) AS singular_values FROM nodes;        -- singular values of the emb matrix
SELECT pca(emb, 3) AS top3_components FROM nodes;     -- top-3 principal-component directions
```

**Batch Method** — `Method::BatchL2Normalize { vectors }` (`src/server/handlers/graph_ops.rs`,
handler `#[cfg(feature = "numeric")]`) L2-normalizes a batch in-engine via
`eg_numeric::linalg::batch_l2_normalize`; the kernel-backed successor to the deprecated
`BatchCosineSimilarity` on the same client path (`client.batch_l2_normalize()`).

**Feature wiring & Pi-contract.** eg-query gains a `numeric` feature (`["sql", "dep:eg-numeric",
"dep:ndarray"]`); the engine's top-level `numeric = ["dep:eg-numeric", "eg-query?/numeric"]`
turns the SQL operators on whenever the query surface is also built (i.e. `full`). `numeric`
stays OUT of `pi`/`node`/`pi-max`/`release-tiny`, and eg-numeric's pyo3 (`python`) feature is
off in every engine build, so a `numeric` engine links faer/ndarray but **no Python extension**.
Verified: `cargo tree --no-default-features --features pi` links **no** eg-numeric/faer/ndarray.

**Shipped this increment:** `svd(vec_col)` (EG-336) / `pca(vec_col, k)` (EG-335) — the
column→`ndarray::Array2` marshalling UDAFs above; the faer SVD/eigh kernels
(`linalg::svdvals`/`eigh`) drive them, so this closed the columnar↔matrix bridge.
**Still deferred (later P4):** graph-algo/timeseries unification under the one kernel and
cross-modal joins → PCA/cluster in-engine.
