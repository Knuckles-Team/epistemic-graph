# Analytics on the unified query surface — SQL functions and UQL stages (CONCEPT:EG-KG.query.concept-8)

epistemic-graph is an **analytical database with native analytical tools**. There are two
surfaces, and this page keeps them apart:

* **SQL** (DataFusion, every SQL entry point): the linear-algebra / statistics kernel
  (`eg-numeric` — faer + ndarray, BLAS/LAPACK-free) is exposed as SQL functions — PCA, SVD,
  k-means, covariance, cosine similarity, z-score standardization and the statistical
  aggregates — **directly in a `SELECT`**, compute-near-data, no fetch-to-Python. Everything
  in the catalogs below is SQL; none of it is UQL syntax.
* **UQL** ([`uql.md`](uql.md)): analytics that are pipeline *stages* over the RowSet — tumbling
  window aggregates, probabilistic scoring over stored distributions, tensor reductions, and
  named score channels so several scores survive into one result:

```uql
TSSCAN ['cpu'] FROM 0 TO 3600 |> WINDOW 60 s MEAN |> RETURN window |> LIMIT 60
```

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

These are the `eg-numeric`-backed operators (CONCEPT:EG-KG.query.surface-b-numeric-operators/335/336/344). They accept the
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

## Cross-modal join → analytics (the numpy-surpassing differentiator, EG-KG.query.eg-3)

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

## Series analytics: one kernel for UQL, SQL, PromQL and derived series (EH-522/524)

The incremental series operators (`eg_numeric::series`) are served four ways, with the same
values bit for bit:

```uql
TSSCAN ['svc.p95'] FROM 0 TO 3600 |> DERIVE zscore(ewma(v0, 12), 60) AS lat_z |> RETURN lat_z
```

```sql
SELECT ts, eg_zscore(v, 60) OVER (PARTITION BY series ORDER BY ts) AS z FROM points;
```

* PromQL `stddev_over_time` / `stdvar_over_time` / `avg_over_time` share the same moments.
* `TsDefineSeries { series_id, source, expr }` materialises an expression as a stored series
  maintained incrementally on every `TsAppend` to its source (points `[value, revision,
  known_at_ms]`; a corrected source point appends revisions, never edits).
* `SKILL f AGAINST y HORIZONS [...] WINDOW w` reports a feature's rolling rank-IC decay, ICIR,
  a bootstrap interval and the breadth across horizons (`eg_numeric::evaluation::skill`).

## Motif / discord search and shape events (EH-529)

Query-by-example and anomalous-subsequence search over a value channel (`eg_numeric::series::
{mass, matrix_profile}`), and declared shape predicates turned into events an existing `CEP`
stage matches — no new pattern language, the same bounded NFA `Op::Cep` already runs:

```uql
TSSCAN ['svc.p95'] FROM 0 TO 3600 |> MOTIF OF v0 LIKE [0, 1.5, -2.25, 4] TOP 5 |> RETURN distance
```

```uql
TSSCAN ['svc.p95'] FROM 0 TO 3600 |> DISCORD OF v0 LENGTH 120 TOP 3 |> RETURN distance, start
```

```uql
TSSCAN ['svc.p95'] FROM 0 TO 3600 |> DERIVE gt(v0, 100) AS spike |> EVENTS spike |> RETURN spike
```

`MOTIF … LIKE` is MASS (a z-normalised distance profile to the query shape, FFT-accelerated,
O(n log n)); `MOTIF … LENGTH` / `DISCORD … LENGTH` build the anytime SCRIMP++ matrix profile
under the plan's `Budget::max_series_work` and return its nearest (motifs) or farthest
(discords) subsequences — a cut run reports its best-so-far with `approximate = 1`. Every
reported `distance` is re-derived from a direct dot product, so it is identical whether or
not the search was cut. `EVENTS c {, c}` turns a non-zero `DERIVE` shape predicate (`gt`/
`lt`/`greatest`/`least`) into a row `<series>#<c>@<ts>` a following `CEP SEQ ({KEY 'c'}) …`
matches by key — the streaming left profile (`DERIVE mprofile(v, m, history)`, EH-529 too)
needs neither: it is an ordinary O(1) incremental kernel, always built.

## See also

- `pages/architecture/numeric-kernel.md` — the `eg-numeric` kernel internals.
- `pages/architecture/analytics-program.md` — the Analytics Program (Surface-A Python wheel
  + Surface-B SQL).
- `crates/eg-query/src/sql/numeric.rs` — the UDF/UDAF implementations.
- `crates/eg-query/src/sql/exec.rs` — `register_numeric` + the shared `build_ctx`.
