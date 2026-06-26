# Time-series interface

The `tsdb` feature (`eg-tsdb`) gives a native, redb-backed time-series store with the analytic
primitives a time-series workload needs — including ASOF joins, which DataFusion 43 lacks. It is
pure-Rust and shares the engine's one durability model.

> Status snapshot: the store and time-ops are supported as native functions. Binding the time-ops into
> the unified planner (`Op::Window` execution) is 🔶 in-progress. See the
> [capability matrix](../capabilities.md#time-series-eg-tsdb).

## The store

`SeriesStore` keeps columnar chunks under a composite redb key `(series_id, bucket_start)`:

- `append_batch` amortizes the fsync across a group of points in one transaction;
- `range` does a bounded scan over a `[from, to)` window;
- `evict_before` applies retention by dropping whole buckets (per-point trim is a roadmap item).

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

## Unified-planner binding (in progress)

The time-ops above are callable functions today. Wiring them behind the planner's `Op::Window` (so a
temporal aggregate composes inline in a [UQL](../uql.md) pipeline with graph/vector/SQL stages) is the
"D-bind" increment — `Op::Window` is currently a pass-through seam in `eg-plan`. The bi-temporal
`Op::AsOf` point-in-time filter, by contrast, **is** wired and executes today:

```text
MATCH (:Reading)
  |> AS OF VALID @1700000000
  |> LIMIT 100
```

See the [roadmap](../roadmap.md#time-series).
</content>
