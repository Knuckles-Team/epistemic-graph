# Time-series interface

The `tsdb` feature (`eg-tsdb`) gives a native, redb-backed time-series store with the analytic
primitives a time-series workload needs — including ASOF joins, which DataFusion 43 lacks. It is
pure-Rust and shares the engine's one durability model.

> Status snapshot: the store and time-ops are supported as native functions, now bound into the unified
> planner (`Op::Window` execution, EG-067) with per-point retention trim (EG-068). A columnar segment
> layout backs analytical/SQL-window scans (EG-089), and a **PromQL** `/api/v1/query` HTTP API (EG-172)
> serves the metric series. TimescaleDB hypertables + continuous aggregates (EG-117) layer on over pgwire.
> See the [capability matrix](../capabilities.md#time-series-eg-tsdb).

## The store

`SeriesStore` keeps columnar chunks under a composite redb key `(series_id, bucket_start)`:

- `append_batch` amortizes the fsync across a group of points in one transaction;
- `range` does a bounded scan over a `[from, to)` window;
- `evict_before` applies retention, trimming straddling buckets at the boundary (per-point trim, EG-068),
  not just whole-bucket eviction.

A **columnar (struct-of-arrays) segment layout** (EG-089) backs analytical scans over the tsdb/table store
— the same layout the SQL window functions read (see [sql](sql.md#window-functions--columnar-scans-eg-089)).

## Time operations (native, no DataFusion)

| Function | What it does |
|----------|--------------|
| `time_bucket` | aligned buckets `(ts/w)*w` with avg / min / max / sum / count / first / last |
| `asof_join_backward` | ASOF (as-of-or-before) join with tolerance, O(L+R) merge |
| `gap_fill_locf` | last-observation-carried-forward over a fixed grid |
| `ohlc_bars` | open / high / low / close + volume bars |
| `downsample` | rollup to a coarser resolution |
| `decay_weighted_mean` | Ebbinghaus decay-weighted mean (shared `eg_core::decay` curve) |
| `series_ewma`, `series_rolling_zscore` | EWMA and rolling z-score (reuse the finance kernels) |

## Unified-planner binding (EG-067)

The time-ops are wired behind the planner's `Op::Window` (with the eg-plan→eg-tsdb dependency edge +
cost-based reorder), so a temporal aggregate composes inline in a [UQL](../uql.md) pipeline with
graph/vector/SQL stages. The bi-temporal `Op::AsOf` point-in-time filter also executes today:

```text
MATCH (:Reading)
  |> AS OF VALID @1700000000
  |> LIMIT 100
```

## PromQL / Prometheus HTTP API (EG-172, feature `promql` on the `obs` listener)

A PromQL evaluator (instant/range vectors, selectors, `rate`/`sum`/`avg`/`max`/`min`/histogram functions,
binary ops) over the eg-tsdb metric series is exposed as a Prometheus-compatible HTTP API on the obs
listener (`EPISTEMIC_GRAPH_OBS_ADDR`, default `127.0.0.1:5080`):

```bash
curl -s 'http://127.0.0.1:5080/api/v1/query?query=up'
curl -s 'http://127.0.0.1:5080/api/v1/query_range?query=rate(http_requests[5m])&start=…&end=…&step=15s'
```

Point a Grafana **Prometheus** data source at the obs listener. This is one leg of the full observability
surface (logs + PromQL + traces) — see [observability](observability.md).

## TimescaleDB compatibility (EG-117, over pgwire)

`CREATE EXTENSION timescaledb` lights up `create_hypertable()`, `time_bucket()` gap-fill, and
`CREATE MATERIALIZED VIEW … WITH (timescaledb.continuous)` continuous aggregates, lowered onto this
time-series store + `Op::Window` — see [sql](sql.md#postgres-extensions--create-extension-eg-102).
</content>

---

**See also:** [Capabilities matrix](../capabilities.md) · [SQL & pgwire](sql.md) · [Observability](observability.md) · [Messaging & Broker](messaging.md) · [Connecting (per-wire guide)](connecting.md).
