# Analytics in UQL — the native analytical database (CONCEPT:EG-353)

epistemic-graph is an **analytical database with native analytical tools**: the linear-
algebra / statistics kernel (`eg-numeric` — faer + ndarray, BLAS/LAPACK-free) is exposed
as first-class SQL functions on the engine's unified query surface (UQL). You run PCA,
SVD, k-means, covariance, cosine similarity, z-score standardization and the full suite of
statistical aggregates **directly in a `SELECT`**, over data that already lives in the
engine — **compute-near-data, no fetch-to-Python, no numpy round-trip, no FFI**.

```sql
SELECT pca(embedding, 3)      FROM docs;          -- top-3 principal components, in-engine
SELECT kmeans(embedding, 8)   FROM docs;          -- k-means labels over resident vectors
SELECT covariance(revenue, spend) FROM ledger;    -- kernel sample covariance
SELECT corr(x, y), stddev(x), median(x) FROM t;   -- native statistical aggregates
```

Because these are ordinary SQL functions, they compose with joins, filters, `GROUP BY`,
CTEs and window frames — and they run **wherever the engine speaks SQL**.

## Every SQL entry point has them — one registration site

The analytical functions are registered on **every** DataFusion `SessionContext` the query
surface builds, through a single shared function (`exec::register_numeric`,
`crates/eg-query/src/sql/exec.rs`). There is no privileged path:

| SQL entry point | How it reaches the functions |
|---|---|
| **RPC `Method::Sql`** (`exec_sql` / `exec_sql_typed`) | `run` / `run_typed` → `build_ctx` → `register_numeric` |
| **Wire protocols** — pgwire (Postgres), MySQL, SQLite, MS-SQL, all via the shared `WireSession` | `exec_sql_typed_with_tables` → `run_typed` → `build_ctx` → `register_numeric` |
| **Embedded API** (`epistemic-graph` crate, in-process) | `exec_sql` / `exec_sql_typed_with_tables` (same as above) |
| **Observability log-search** (`exec_sql_over_tables`) | `register_numeric` on the tables-only context |

So a JDBC/ODBC/DBI client — DBeaver, `psql`, a BI tool — issuing `SELECT pca(...)` hits the
exact same in-engine kernel as the native RPC path. This is proven end-to-end in
`crates/eg-query/tests/analytics_through_uql.rs`, which drives every operator below through
`exec_sql_typed_with_tables` (the wire read path) **and** `exec_sql` (the RPC path) and
asserts identical results.

Everything here is gated behind eg-query's `numeric` cargo feature (part of the engine's one
main build, `default`/`full`). A minimal `--no-default-features --features server` build links
neither `eg-numeric` nor `faer`; the engine binary never links pyo3 (`cargo tree | grep -ci pyo3`
= 0).

## Catalog — kernel-backed analytical functions (`eg-numeric`)

These are the `eg-numeric`-backed operators (CONCEPT:EG-329/335/336/344). They accept the
same vector-operand forms as the pgvector operators: a stored `List<Float{32,64}>` column
**or** a `'[1,2,3]'` text literal.

| Function | Kind | Returns | What it computes |
|---|---|---|---|
| `cosine_sim(a, b)` | scalar | `Float64` | Cosine similarity `a·b / (‖a‖‖b‖)` — the raw-similarity complement to EG-115's `vector_cosine` distance. NULL on dimension mismatch. |
| `l2_normalize(v)` | scalar | `List<Float32>` (pgvector) | The unit vector `v/‖v‖`; feeds `cosine_sim` / ANN in-query. |
| `zscore(col)` | scalar-over-batch | `Float64` | Standardize `(x-mean)/std` (population, ddof=0) within the materialized batch. For a true global two-pass use the window form `(x - avg(x) OVER ()) / stddev(x) OVER ()`. |
| `covariance(a, b)` | UDAF | `Float64` | Sample covariance (ddof=1), `Σ(aᵢ-ā)(bᵢ-b̄)/(n-1)`, kernel means. |
| `svd(vec_col)` | UDAF | `List<Float64>` | Singular values (descending) of the `n×d` matrix whose rows are the aggregated vectors. |
| `pca(vec_col, k)` | UDAF | `List<List<Float64>>` | Top-`k` principal-component **directions** (loadings), descending by explained variance; each a `d`-length unit vector (sign arbitrary). Projected coords = `X_centered · componentsᵀ`. |
| `kmeans(vec_col, k)` | UDAF | `List<Int64>` | One hard cluster label (`0..k`) per aggregated row, in ingestion order. Pure-Rust Lloyd + k-means++, seeded (deterministic). |

## Catalog — native statistical aggregates (DataFusion)

The engine's SQL surface also carries DataFusion's always-registered statistical
aggregates, so the analytical-DB surface is complete **without duplicating** them in the
kernel. These run in-engine over resident columns just like the kernel operators:

| Function | What it computes |
|---|---|
| `corr(a, b)` | Pearson correlation coefficient |
| `covar_samp(a, b)` / `covar_pop(a, b)` | Sample / population covariance |
| `var(x)` / `var_pop(x)` | Sample / population variance |
| `stddev(x)` / `stddev_pop(x)` | Sample / population standard deviation |
| `avg(x)`, `sum(x)`, `min(x)`, `max(x)`, `count(x)` | Standard aggregates |
| `median(x)` | Median |
| `approx_percentile_cont(x, p)` | Approximate continuous percentile at fraction `p` (e.g. `0.5` = median, `0.95` = p95) |
| `approx_median(x)`, `approx_distinct(x)` | Approximate median / distinct count |

> **Why not kernel-backed duplicates?** The task guidance is *prefer DataFusion built-ins;
> add kernel-backed operators only where genuinely missing.* `corr`/`stddev`/`var`/`median`/
> `approx_percentile_cont` are all present and correct on every SQL path (verified in the
> test), so re-implementing them in the kernel would only add drift risk. The kernel earns
> its place for the operations DataFusion has **no** built-in for — PCA, SVD, k-means,
> cosine similarity, L2-normalization — which are the analytical differentiators.

## Cross-modal join → analytics (the numpy-surpassing differentiator, EG-345)

Because the analytics are SQL functions, they compose with joins across modalities —
graph/relational columns, vector embeddings, and timeseries — in **one statement**. numpy
has no data layer to join across; here it is a single query:

```sql
WITH ts AS (
  SELECT nid, avg(reading) AS avg_reading FROM readings GROUP BY nid
)
SELECT covariance(n.x, ts.avg_reading) AS cov,   -- relational × timeseries, kernel-backed
       corr(n.x, ts.avg_reading)       AS r,     -- … and the native aggregate
       kmeans(n.emb, 2)                AS cluster -- over the joined vectors
FROM nodes n
JOIN ts ON n.id = ts.nid;
```

This joins the resident `nodes` table (a relational scalar `x` + a per-node vector `emb`) to
a per-node timeseries aggregate and computes statistics that **span two modalities aligned
by the join** — all compute-near-data, in-engine. See
`crates/eg-query/tests/cross_modal_analytics.rs` and the
`cross_modal_join_then_analytics_through_wire_eg353` test.

## Example queries

```sql
-- Nearest neighbours by cosine similarity, ranked in-engine
SELECT id, cosine_sim(embedding, '[0.1,0.2,0.3, ...]') AS sim
FROM docs
ORDER BY sim DESC
LIMIT 10;

-- Dimensionality of an embedding column: are the top singular values dominant?
SELECT svd(embedding) AS singular_values FROM docs;

-- Cluster a corpus and get per-row labels
SELECT kmeans(embedding, 8) AS labels FROM docs;

-- Per-group statistics
SELECT category,
       corr(price, demand)        AS price_demand_corr,
       stddev(price)              AS price_sd,
       approx_percentile_cont(price, 0.95) AS p95_price
FROM products
GROUP BY category;

-- Standardize a feature, then filter on the z-score
SELECT id FROM signals WHERE zscore(value) > 3.0;   -- outliers, in one pass
```

## See also

- `docs/architecture/numeric-kernel.md` — the `eg-numeric` kernel internals.
- `docs/architecture/analytics-program.md` — the Analytics Program (Surface-A Python wheel
  + Surface-B SQL).
- `crates/eg-query/src/sql/numeric.rs` — the UDF/UDAF implementations.
- `crates/eg-query/src/sql/exec.rs` — `register_numeric` + the shared `build_ctx`.
