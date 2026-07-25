//! The incremental circuit (CONCEPT:EG-KG.storage.incremental-matview).
//!
//! Compiles a `wire::Plan` into a [`Circuit`] over the SUBSET of `Op` that is provably
//! safe to maintain by delta, and maintains the materialized result as [`Delta`]s arrive
//! — the direct generalization of `src/server/cdc.rs`'s hand-rolled two-aggregate
//! `ContinuousQuery`/`maintain()` to an arbitrary supported plan.
//!
//! ## The faithfulness contract (why this mirrors `exec.rs`, not an idealized model)
//!
//! An `Incremental`-mode view and its `Recompute` fallback must be INTERCHANGEABLE: a
//! view can flip modes on a reseed / CDC-ring-lag fallback, and the differential oracle
//! (`tests/incremental_execute_oracle.rs`) asserts `Circuit::current()` is row-equal to
//! the REAL `eg_plan::execute` recompute after every mutation. So this reproduces
//! `exec.rs`'s semantics EXACTLY, including two that an idealized DBSP model would miss:
//!
//! * **`Scan { label }`** — `label_matches` tests ONLY `props["type"]` (a string equal to
//!   `label`), exactly like `exec::scan_label` (NOT `node_type`/`label`).
//! * **`Filter { preds }`** — `Eq`/`GtNum`/`LtNum` only, mirroring `exec::where_clause`.
//! * **`AsOf { ts, axis }`** — `asof_holds` mirrors `exec::live_at` (u64 coercion, `from`
//!   defaults 0, `until` open when absent, query instant `ts.max(0)`).
//! * **The `exec` "empty input ⇒ act as source" pipeline rule.** `exec::filter_op` and
//!   `exec::as_of_filter` treat an EMPTY input RowSet as "I am the source" and scan the
//!   WHOLE graph for their own predicate (not a narrowing of the prior op). So
//!   `Scan{Note} |> Filter{year<2003}` over a graph with NO `Note` nodes yields ALL
//!   year<2003 nodes, not `∅`. A plain conjunction would diverge. The circuit reproduces
//!   this with a per-stage membership map + the read-time recurrence
//!   `R_i = if R_{i-1} == ∅ { M_i } else { R_{i-1} ∩ M_i }` where `M_i = {x : pred_i(x)}`
//!   (`Scan` is the true source, `R_0 = M_0`). This is maintained in O(stages · |delta|)
//!   per delta and evaluated in O(stages · view) at read.
//! * **`WindowAgg { secs, agg }`** — a tumbling per-bucket accumulator reproducing
//!   `exec::window_aggregate` (the `timeseries`-gated path): event time is `valid_from`
//!   aligned to `(vf/width)*width` (`width = secs as i64`, exactly
//!   `eg_tsdb::query::time_bucket`), value is the `value` property (a v1 plan carries no
//!   `Rank` score, so `exec`'s `score`|`value` resolves to `value`), a row missing either
//!   is dropped, buckets order ASCENDING by start. Only `count`/`sum`/`mean`|`avg` (pure-
//!   subtraction) are supported. **Restricted to `Scan → WindowAgg` (no intervening
//!   `Filter`/`AsOf`)**: the empty-input-source rule above would let a single delta that
//!   empties an upstream stage flip the WHOLE aggregate (non-local, not O(Δ)), so a
//!   `Filter`/`AsOf` before a `WindowAgg` falls back with a typed reason. **Gated on
//!   `#[cfg(feature = "timeseries")]`: without it `exec` passes `WindowAgg` through, so
//!   the circuit falls the plan back (its recompute passes it through to the same set).**
//! * **`Limit { k }`** — truncates the maintained body at read: window plans in ascending
//!   start order (so the same `k` survive as the recompute), membership in id-sorted order.
//!
//! Everything else (`Traverse`, `Reason`, `SparqlBgp`, `ForeignScan`, `RankMmr`,
//! `FuseRrf`, `Udf`, `TsScan`, `min`/`max` window aggs, `JsonPath`/spatial preds, …)
//! makes [`Circuit::compile`] return [`UnsupportedOp`] naming the first offending op, so
//! the caller keeps that view on today's recompute-on-`Get` path — a per-view fallback,
//! never a silently-wrong incremental answer.
//!
//! ## Cost contract (O(Δ) maintenance)
//!
//! [`Circuit::apply`] folds one delta into the maintained state in O(stages · |delta|) —
//! it touches ONLY the delta rows against a bounded number of stages, never the rest of
//! the view — and RETURNS how many state entries it touched (the O(Δ) instrument).
//! Projecting the result RowSet ([`Circuit::current`]) is O(view) and runs at READ, not
//! per delta.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use serde_json::{Map, Value};

use eg_types::wire::{Op, Plan, Pred, TimeAxis};

use super::zset::Delta;
use crate::rowset::RowSet;

/// The first `Op` in a plan with no incremental form — why a view falls back to
/// recompute. `index` is the op's position in the plan; `reason` names it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnsupportedOp {
    pub index: usize,
    pub reason: String,
}

impl std::fmt::Display for UnsupportedOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "op #{} is not incrementally maintainable: {}",
            self.index, self.reason
        )
    }
}

impl std::error::Error for UnsupportedOp {}

/// One membership stage's per-row predicate — the `exec` op it reproduces.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
enum StagePred {
    /// `Scan { label }`: `props["type"] == label`.
    Scan { label: String },
    /// `Filter { preds }`: ALL preds hold (one `exec::filter_op` call ANDs them).
    Filter { preds: Vec<Pred> },
    /// `AsOf { ts, axis }`: live at `ts` on the timeline (`exec::live_at`).
    AsOf { ts: f64, axis: TimeAxis },
}

impl StagePred {
    fn holds(&self, props: &Map<String, Value>) -> bool {
        match self {
            StagePred::Scan { label } => label_matches(props, label),
            StagePred::Filter { preds } => preds.iter().all(|p| pred_holds(props, p)),
            StagePred::AsOf { ts, axis } => asof_holds(props, *ts, *axis),
        }
    }
}

/// One maintained membership stage: its per-row predicate + the ids currently matching it
/// (`id → net signed weight`, present iff `> 0`). `M_i` in the read-time recurrence.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct Stage {
    pred: StagePred,
    members: HashMap<String, i32>,
}

impl Stage {
    fn new(pred: StagePred) -> Self {
        Stage {
            pred,
            members: HashMap::new(),
        }
    }

    /// The set of ids currently present at this stage (`{x : pred(x)}`).
    fn set(&self) -> BTreeSet<&str> {
        self.members
            .iter()
            .filter(|(_, w)| **w > 0)
            .map(|(id, _)| id.as_str())
            .collect()
    }
}

/// The linear tumbling aggregate a `WindowAgg` maintains — the subset that retracts by
/// pure subtraction (`count`/`sum`/`mean`). `min`/`max`/`first`/`last` are NOT here.
#[cfg_attr(not(feature = "timeseries"), allow(dead_code))]
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
enum Agg {
    Count,
    Sum,
    Mean,
}

/// The property a `WindowAgg` reads the aggregated value from (`value`) and the one it
/// reads the event time from (`valid_from`) — the SAME fields `exec::window_aggregate`
/// reads for a graph-node row.
const VALUE_FIELD: &str = "value";
const TS_FIELD: &str = "valid_from";

/// One bucket accumulator: a running weighted sum and a signed count. Present in the
/// result iff `count > 0`; for integer inputs (the property-test domain) the running sum
/// is exact regardless of fold order.
#[cfg_attr(not(feature = "timeseries"), allow(dead_code))]
#[derive(Clone, Copy, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
struct Bucket {
    sum: f64,
    count: i64,
}

/// The maintained `WindowAgg` stage: the `Scan` source `label` it aggregates, the integer
/// window width (`secs as i64`, matching `exec`), the aggregate, and the per-bucket
/// accumulators keyed by ALIGNED bucket start (`(valid_from/width)*width`).
#[cfg_attr(not(feature = "timeseries"), allow(dead_code))]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct WindowState {
    label: String,
    width: i64,
    agg: Agg,
    buckets: BTreeMap<i64, Bucket>,
}

/// A compiled incremental circuit for one supported plan. Serializable so its maintained
/// state (stage membership maps / bucket accumulators) persists in the
/// `matview_operator_state` redb table — the analogue of turso's `dbsp_state` btree.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Circuit {
    /// MEMBERSHIP mode: the ordered stages (Scan first, then Filter/AsOf). The read-time
    /// recurrence over these reproduces `exec`'s pipeline (source-on-empty). Empty in
    /// window mode.
    stages: Vec<Stage>,
    /// WINDOW mode: `Some` ⇒ aggregate the Scan set into tumbling buckets (no membership
    /// stages — a Filter/AsOf before the WindowAgg falls back).
    window: Option<WindowState>,
    /// Trailing `Limit`, applied at read.
    limit: Option<usize>,
}

impl Circuit {
    /// Compile a plan into a circuit. `Ok` only when EVERY op is in the v1 supported set
    /// (see the module docs); the FIRST unsupported op short-circuits with its index+kind
    /// so the caller can log exactly why a view fell back to recompute.
    ///
    /// Supported shape: `Scan` (required, first) → any number of `Filter`/`AsOf` → at
    /// most one `WindowAgg` (which must directly follow the `Scan`) → an optional trailing
    /// `Limit`.
    pub fn compile(plan: &Plan) -> Result<Circuit, UnsupportedOp> {
        let ops = &plan.ops;
        if ops.is_empty() {
            return Err(UnsupportedOp {
                index: 0,
                reason: "empty plan (nothing to incrementalize)".into(),
            });
        }
        let label = match &ops[0] {
            Op::Scan { label } => label.clone(),
            other => {
                return Err(UnsupportedOp {
                    index: 0,
                    reason: format!("plan must start with Scan, found {}", op_name(other)),
                })
            }
        };

        let mut stages: Vec<Stage> = vec![Stage::new(StagePred::Scan {
            label: label.clone(),
        })];
        let mut window: Option<WindowState> = None;
        let mut limit: Option<usize> = None;

        for (i, op) in ops.iter().enumerate().skip(1) {
            if limit.is_some() {
                return Err(UnsupportedOp {
                    index: i,
                    reason: "no op may follow Limit".into(),
                });
            }
            match op {
                Op::Filter { preds } => {
                    if window.is_some() {
                        return Err(UnsupportedOp {
                            index: i,
                            reason: "Filter after WindowAgg is not supported".into(),
                        });
                    }
                    for p in preds {
                        match p {
                            Pred::Eq { .. } | Pred::GtNum { .. } | Pred::LtNum { .. } => {}
                            _ => {
                                return Err(UnsupportedOp {
                                    index: i,
                                    reason: "Filter carries a non-relational predicate \
                                             (JsonPath/spatial)"
                                        .into(),
                                })
                            }
                        }
                    }
                    stages.push(Stage::new(StagePred::Filter {
                        preds: preds.clone(),
                    }));
                }
                Op::AsOf { ts, axis } => {
                    if window.is_some() {
                        return Err(UnsupportedOp {
                            index: i,
                            reason: "AsOf after WindowAgg is not supported".into(),
                        });
                    }
                    stages.push(Stage::new(StagePred::AsOf {
                        ts: *ts,
                        axis: *axis,
                    }));
                }
                Op::WindowAgg { secs, agg } => {
                    if window.is_some() {
                        return Err(UnsupportedOp {
                            index: i,
                            reason: "more than one WindowAgg is not supported".into(),
                        });
                    }
                    // Only Scan may precede the WindowAgg (stages == [Scan]); a Filter/AsOf
                    // before it would source-on-empty and make the aggregate non-local.
                    if stages.len() > 1 {
                        return Err(UnsupportedOp {
                            index: i,
                            reason: "WindowAgg over a Filter/AsOf-narrowed set is not \
                                     incrementally maintainable in v1 (exec's empty-input- \
                                     source rule makes it non-local); only Scan → WindowAgg"
                                .into(),
                        });
                    }
                    window = Some(compile_window_agg(i, label.clone(), *secs, agg)?);
                }
                Op::Limit { k } => limit = Some(*k),
                other => {
                    return Err(UnsupportedOp {
                        index: i,
                        reason: format!("{} has no incremental form in v1", op_name(other)),
                    })
                }
            }
        }

        // Window mode doesn't use the membership stages (its `label` gate lives in
        // `WindowState`); drop them so `apply`/`current` branch cleanly on `window`.
        if window.is_some() {
            stages.clear();
        }

        Ok(Circuit {
            stages,
            window,
            limit,
        })
    }

    /// Whether this circuit's plan is aggregate (window) mode.
    pub fn is_window(&self) -> bool {
        self.window.is_some()
    }

    /// INCREMENTAL maintenance: fold one delta into the maintained state, returning HOW
    /// MANY state entries it touched (the O(Δ) instrument — bounded by
    /// `stages · delta.len()`, independent of view size). A row applies its signed weight
    /// to every membership stage whose predicate it satisfies (set mode), or to its
    /// time-bucket accumulator (window mode).
    pub fn apply(&mut self, delta: &Delta) -> usize {
        let mut touched = 0usize;
        match &mut self.window {
            Some(w) => {
                for row in &delta.rows {
                    if row.weight == 0 || !label_matches(&row.props, &w.label) {
                        continue;
                    }
                    // Faithful to `exec::window_aggregate`: a graph-node row needs BOTH a
                    // `valid_from` event time and a numeric `value`; either absent drops it.
                    let (Some(ts), Some(val)) =
                        (int_field(&row.props, TS_FIELD), row.num(VALUE_FIELD))
                    else {
                        continue;
                    };
                    let Some(start) = w.bucket_start(ts) else {
                        continue;
                    };
                    let b = w.buckets.entry(start).or_default();
                    b.sum += row.weight as f64 * val;
                    b.count += row.weight as i64;
                    if b.count <= 0 {
                        w.buckets.remove(&start);
                    }
                    touched += 1;
                }
            }
            None => {
                for row in &delta.rows {
                    if row.weight == 0 {
                        continue;
                    }
                    for stage in &mut self.stages {
                        if !stage.pred.holds(&row.props) {
                            continue;
                        }
                        let e = stage.members.entry(row.id.clone()).or_insert(0);
                        *e += row.weight;
                        if *e <= 0 {
                            stage.members.remove(&row.id);
                        }
                        touched += 1;
                    }
                }
            }
        }
        touched
    }

    /// The current materialized result from the MAINTAINED state (the hot-path read a
    /// `Mode::Incremental` `Get` serves). O(view) — a projection, run at read, NOT per
    /// delta (see the cost contract).
    pub fn current(&self) -> RowSet {
        match &self.window {
            Some(w) => finalize_buckets(w.buckets.iter().map(|(k, b)| (*k, *b)), w, self.limit),
            None => {
                let ids = self.recurrence(self.stages.iter().map(|s| s.set()));
                finalize_members(ids, self.limit)
            }
        }
    }

    /// INDEPENDENT full recompute from the complete current node set — NO maintained
    /// state, a from-scratch scan reproducing `exec`. Used by the circuit-vs-circuit
    /// oracle to isolate delta-maintenance from the shared per-row predicates. `nodes` is
    /// `id → decoded props` for every node currently present.
    pub fn recompute(&self, nodes: &BTreeMap<String, Map<String, Value>>) -> RowSet {
        match &self.window {
            Some(w) => {
                let mut buckets: BTreeMap<i64, Bucket> = BTreeMap::new();
                for props in nodes.values() {
                    if !label_matches(props, &w.label) {
                        continue;
                    }
                    let (Some(ts), Some(val)) =
                        (int_field(props, TS_FIELD), num_field(props, VALUE_FIELD))
                    else {
                        continue;
                    };
                    let Some(start) = w.bucket_start(ts) else {
                        continue;
                    };
                    let b = buckets.entry(start).or_default();
                    b.sum += val;
                    b.count += 1;
                }
                finalize_buckets(buckets, w, self.limit)
            }
            None => {
                let per_stage = self.stages.iter().map(|stage| {
                    nodes
                        .iter()
                        .filter(|(_, props)| stage.pred.holds(props))
                        .map(|(id, _)| id.as_str())
                        .collect::<BTreeSet<&str>>()
                });
                let ids = self.recurrence(per_stage);
                finalize_members(ids, self.limit)
            }
        }
    }

    /// Evaluate `exec`'s membership pipeline over the per-stage id sets:
    /// `R_0 = M_0` (Scan is the source); `R_i = if R_{i-1} == ∅ { M_i } else { R_{i-1} ∩
    /// M_i }` (a Filter/AsOf with EMPTY input sources its own predicate over the graph).
    /// Returns the final id set, sorted (the canonical serving order).
    fn recurrence<'a, I>(&self, stage_sets: I) -> Vec<String>
    where
        I: IntoIterator<Item = BTreeSet<&'a str>>,
    {
        let mut r: Option<BTreeSet<&str>> = None;
        for m in stage_sets {
            r = Some(match r {
                None => m,
                Some(prev) if prev.is_empty() => m,
                Some(prev) => prev.intersection(&m).copied().collect(),
            });
        }
        r.unwrap_or_default()
            .into_iter()
            .map(str::to_string)
            .collect()
    }
}

/// Compile one `WindowAgg` op into a maintained stage. `#[cfg(feature = "timeseries")]`:
/// only `count`/`sum`/`mean`|`avg` (the pure-subtraction aggregates) are supported; every
/// other selector falls back. WITHOUT `timeseries`, `exec::window_agg_op` is an identity
/// pass-through, so incrementalizing an aggregate here would DIVERGE — the plan falls back
/// (its recompute passes the rows through to the same set).
#[cfg(feature = "timeseries")]
fn compile_window_agg(
    index: usize,
    label: String,
    secs: f64,
    agg: &str,
) -> Result<WindowState, UnsupportedOp> {
    if !secs.is_finite() {
        return Err(UnsupportedOp {
            index,
            reason: "WindowAgg width must be finite".into(),
        });
    }
    let agg = match agg.to_ascii_lowercase().as_str() {
        "count" => Agg::Count,
        "sum" => Agg::Sum,
        "mean" | "avg" | "average" => Agg::Mean,
        other => {
            return Err(UnsupportedOp {
                index,
                reason: format!(
                    "WindowAgg '{other}' is not a linear aggregate (only count/sum/mean/avg; \
                     min/max/first/last need per-bucket multiset retraction)"
                ),
            })
        }
    };
    Ok(WindowState {
        label,
        // `exec` uses `secs.max(0.0) as i64`; a width <= 0 yields an empty result there,
        // which `bucket_start` reproduces (returns `None`).
        width: secs.max(0.0) as i64,
        agg,
        buckets: BTreeMap::new(),
    })
}

#[cfg(not(feature = "timeseries"))]
fn compile_window_agg(
    index: usize,
    _label: String,
    _secs: f64,
    _agg: &str,
) -> Result<WindowState, UnsupportedOp> {
    Err(UnsupportedOp {
        index,
        reason: "WindowAgg is not incrementally maintainable without the timeseries feature \
                 (exec passes it through to the membership set; recompute serves it)"
            .into(),
    })
}

impl WindowState {
    /// The ALIGNED bucket start for an event time, matching `eg_tsdb::query::time_bucket`
    /// (`(ts/width)*width`). `width <= 0` ⇒ no bucket (an empty windowed result, exactly
    /// `time_bucket`'s `width <= 0` guard).
    fn bucket_start(&self, ts: i64) -> Option<i64> {
        if self.width <= 0 {
            return None;
        }
        Some((ts / self.width) * self.width)
    }
}

/// Finalize a set-mode result: id-sorted (the recurrence already returns sorted ids),
/// unscored, truncated to `limit`. (`exec`'s membership order is `HashMap`-nondeterministic
/// — a SET — so a stable id-sort is the canonical serving order.)
fn finalize_members(ids: Vec<String>, limit: Option<usize>) -> RowSet {
    let mut ids = ids;
    if let Some(k) = limit {
        ids.truncate(k);
    }
    RowSet::from_rows(ids.into_iter().map(|id| (id, None)))
}

/// Finalize a window-mode result: one row per non-empty bucket (id = bucket start, score =
/// the aggregate), ordered ASCENDING by bucket start (exactly `exec::window_aggregate`:
/// `time_bucket` emits buckets ts-ascending and `from_scored` preserves that), truncated
/// to `limit`.
fn finalize_buckets(
    buckets: impl IntoIterator<Item = (i64, Bucket)>,
    w: &WindowState,
    limit: Option<usize>,
) -> RowSet {
    let mut rows: Vec<(i64, f64)> = buckets
        .into_iter()
        .filter(|(_, b)| b.count > 0)
        .map(|(start, b)| {
            let agg = match w.agg {
                Agg::Count => b.count as f64,
                Agg::Sum => b.sum,
                Agg::Mean => b.sum / b.count as f64,
            };
            (start, agg)
        })
        .collect();
    rows.sort_by_key(|(start, _)| *start);
    let scored = rows
        .into_iter()
        .map(|(start, agg)| (start.to_string(), agg as f32));
    let mut rs = RowSet::from_scored(scored);
    if let Some(k) = limit {
        rs = rs.limit(k);
    }
    rs
}

/// A short op name for `UnsupportedOp` messages (avoids leaning on `Debug`, which pulls
/// full nested payloads).
fn op_name(op: &Op) -> &'static str {
    // The `_` arm is reachable only under feature sets that add more `Op` variants
    // (owl/text/federation/…); under a bare `query` build the listed arms are total.
    #[allow(unreachable_patterns)]
    match op {
        Op::Scan { .. } => "Scan",
        Op::Filter { .. } => "Filter",
        Op::Traverse { .. } => "Traverse",
        Op::Rank { .. } => "Rank",
        Op::RankEmbed { .. } => "RankEmbed",
        Op::RankNodeDistance { .. } => "RankNodeDistance",
        Op::RankMentions { .. } => "RankMentions",
        Op::RankMmr { .. } => "RankMmr",
        Op::AsOf { .. } => "AsOf",
        Op::Window { .. } => "Window",
        Op::WindowAgg { .. } => "WindowAgg",
        Op::Foreign { .. } => "Foreign",
        Op::Limit { .. } => "Limit",
        _ => "unsupported-op",
    }
}

// ── shared per-row predicate primitives (faithful to `exec.rs`) ───────────────

/// The `Scan` label test: `props["type"]` is a string equal to `label`. Byte-for-byte
/// `exec::scan_label` — ONLY the `type` key (not `node_type`/`label`).
fn label_matches(props: &Map<String, Value>, label: &str) -> bool {
    matches!(props.get("type"), Some(Value::String(s)) if s == label)
}

/// Evaluate one relational `Filter` predicate against a row's props (mirrors
/// `exec::where_clause`'s `prop = lit` / `prop > n` / `prop < n`).
fn pred_holds(props: &Map<String, Value>, pred: &Pred) -> bool {
    match pred {
        Pred::Eq { prop, value } => match props.get(prop) {
            Some(Value::String(s)) => s == value,
            Some(Value::Number(n)) => match value.parse::<f64>() {
                Ok(v) => n.as_f64() == Some(v),
                Err(_) => n.to_string() == *value,
            },
            Some(Value::Bool(b)) => b.to_string() == *value,
            _ => false,
        },
        Pred::GtNum { prop, n } => props
            .get(prop)
            .and_then(Value::as_f64)
            .is_some_and(|v| v > *n),
        Pred::LtNum { prop, n } => props
            .get(prop)
            .and_then(Value::as_f64)
            .is_some_and(|v| v < *n),
        // Non-relational preds never reach here (compile rejects them).
        _ => false,
    }
}

/// The `AsOf` temporal predicate: is the row live at `ts` on the given axis? Mirrors
/// `exec::live_at` EXACTLY — u64 coercion, `from` defaults 0, `until` open when absent,
/// query instant `ts.max(0)`; the window is half-open `[from, until)`.
fn asof_holds(props: &Map<String, Value>, ts: f64, axis: TimeAxis) -> bool {
    let (from_key, until_key) = match axis {
        TimeAxis::Valid => ("valid_from", "valid_until"),
        TimeAxis::Transaction => ("tx_from", "tx_to"),
    };
    let q = ts.max(0.0) as u64;
    let from = props.get(from_key).and_then(Value::as_u64).unwrap_or(0);
    let until = props.get(until_key).and_then(Value::as_u64);
    from <= q && until.is_none_or(|u| q < u)
}

/// Read an integer property (`None` if absent / non-integer), matching `exec`'s
/// `v.get(k).as_i64()` for the `valid_from` event time.
fn int_field(props: &Map<String, Value>, field: &str) -> Option<i64> {
    props.get(field).and_then(Value::as_i64)
}

/// Read a numeric property as f64 (the recompute-side `value` read).
fn num_field(props: &Map<String, Value>, field: &str) -> Option<f64> {
    props.get(field).and_then(Value::as_f64)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn props(v: Value) -> Map<String, Value> {
        v.as_object().unwrap().clone()
    }

    fn node_map(items: &[(&str, Value)]) -> BTreeMap<String, Map<String, Value>> {
        items
            .iter()
            .map(|(id, v)| (id.to_string(), props(v.clone())))
            .collect()
    }

    fn insert(id: &str, v: Value) -> Delta {
        Delta::from(vec![super::super::zset::ZRow::insert(id, props(v))])
    }
    fn retract(id: &str, v: Value) -> Delta {
        Delta::from(vec![super::super::zset::ZRow::retract(id, props(v))])
    }

    // ── compile classification ──

    #[test]
    fn compile_accepts_supported_shapes() {
        assert!(Circuit::compile(&Plan::new(vec![Op::Scan {
            label: "Doc".into()
        }]))
        .is_ok());
        assert!(Circuit::compile(&Plan::new(vec![
            Op::Scan {
                label: "Doc".into()
            },
            Op::Filter {
                preds: vec![Pred::GtNum {
                    prop: "year".into(),
                    n: 2000.0
                }]
            },
            Op::AsOf {
                ts: 5.0,
                axis: TimeAxis::Valid
            },
            Op::Limit { k: 10 },
        ]))
        .is_ok());
    }

    #[cfg(feature = "timeseries")]
    #[test]
    fn compile_accepts_scan_window_under_timeseries() {
        assert!(Circuit::compile(&Plan::new(vec![
            Op::Scan { label: "M".into() },
            Op::WindowAgg {
                secs: 10.0,
                agg: "sum".into()
            },
            Op::Limit { k: 3 },
        ]))
        .is_ok());
    }

    #[cfg(feature = "timeseries")]
    #[test]
    fn compile_rejects_window_over_filtered_set() {
        // Filter before WindowAgg → fallback (exec's empty-input-source makes it non-local).
        let e = Circuit::compile(&Plan::new(vec![
            Op::Scan { label: "M".into() },
            Op::Filter {
                preds: vec![Pred::GtNum {
                    prop: "year".into(),
                    n: 1.0,
                }],
            },
            Op::WindowAgg {
                secs: 10.0,
                agg: "sum".into(),
            },
        ]))
        .unwrap_err();
        assert_eq!(e.index, 2);
    }

    #[test]
    fn compile_rejects_unsupported_ops_with_index() {
        // Traverse → fallback, naming its index.
        let e = Circuit::compile(&Plan::new(vec![
            Op::Scan {
                label: "Doc".into(),
            },
            Op::Traverse {
                rel: "CITES".into(),
                min: 1,
                max: 2,
            },
        ]))
        .unwrap_err();
        assert_eq!(e.index, 1);

        // Must start with Scan.
        let e = Circuit::compile(&Plan::new(vec![Op::Limit { k: 1 }])).unwrap_err();
        assert_eq!(e.index, 0);

        // Nothing after Limit.
        let e = Circuit::compile(&Plan::new(vec![
            Op::Scan {
                label: "Doc".into(),
            },
            Op::Limit { k: 1 },
            Op::Filter { preds: vec![] },
        ]))
        .unwrap_err();
        assert_eq!(e.index, 2);
    }

    #[cfg(feature = "timeseries")]
    #[test]
    fn compile_rejects_nonlinear_window_agg() {
        let e = Circuit::compile(&Plan::new(vec![
            Op::Scan { label: "M".into() },
            Op::WindowAgg {
                secs: 10.0,
                agg: "max".into(),
            },
        ]))
        .unwrap_err();
        assert_eq!(e.index, 1);
    }

    #[cfg(not(feature = "timeseries"))]
    #[test]
    fn compile_falls_back_window_agg_without_timeseries() {
        let e = Circuit::compile(&Plan::new(vec![
            Op::Scan { label: "M".into() },
            Op::WindowAgg {
                secs: 10.0,
                agg: "sum".into(),
            },
        ]))
        .unwrap_err();
        assert_eq!(e.index, 1);
    }

    // ── per-operator: apply-delta result == full recompute ──

    #[test]
    fn scan_matches_only_type_key() {
        // node_type/label must NOT match — only `type` (faithful to exec::scan_label).
        let plan = Plan::new(vec![Op::Scan {
            label: "Doc".into(),
        }]);
        let mut c = Circuit::compile(&plan).unwrap();
        let mut model = node_map(&[]);
        c.apply(&insert("a", json!({"type": "Doc"})));
        model.insert("a".into(), props(json!({"type": "Doc"})));
        c.apply(&insert("b", json!({"node_type": "Doc"})));
        model.insert("b".into(), props(json!({"node_type": "Doc"})));
        assert_eq!(c.current(), c.recompute(&model));
        assert_eq!(c.current().ids(), vec!["a"]);
    }

    #[test]
    fn scan_add_update_remove_matches_recompute() {
        let plan = Plan::new(vec![Op::Scan {
            label: "Doc".into(),
        }]);
        let mut c = Circuit::compile(&plan).unwrap();
        let mut model = node_map(&[]);

        c.apply(&insert("a", json!({"type": "Doc"})));
        model.insert("a".into(), props(json!({"type": "Doc"})));
        c.apply(&insert("x", json!({"type": "Other"})));
        model.insert("x".into(), props(json!({"type": "Other"})));
        assert_eq!(c.current(), c.recompute(&model));
        assert_eq!(c.current().ids(), vec!["a"]);

        c.apply(&Delta::from(vec![
            super::super::zset::ZRow::retract("a", props(json!({"type": "Doc"}))),
            super::super::zset::ZRow::insert("a", props(json!({"type": "Other"}))),
        ]));
        model.insert("a".into(), props(json!({"type": "Other"})));
        assert_eq!(c.current(), c.recompute(&model));
        assert!(c.current().is_empty());
    }

    #[test]
    fn empty_scan_makes_filter_source_all() {
        // The exec empty-input-source quirk: Scan{Note} with NO Note nodes ⇒ Filter
        // sources ALL year>2000 nodes. The recurrence reproduces it.
        let plan = Plan::new(vec![
            Op::Scan {
                label: "Note".into(),
            },
            Op::Filter {
                preds: vec![Pred::GtNum {
                    prop: "year".into(),
                    n: 2000.0,
                }],
            },
        ]);
        let mut c = Circuit::compile(&plan).unwrap();
        let mut model = node_map(&[]);
        // No Note nodes at all — only Docs. year>2000 Docs are sourced by the Filter.
        for (id, year) in [("a", 2005), ("b", 1990)] {
            let v = json!({"type": "Doc", "year": year});
            c.apply(&insert(id, v.clone()));
            model.insert(id.into(), props(v));
        }
        assert_eq!(c.current(), c.recompute(&model));
        assert_eq!(c.current().ids(), vec!["a"]); // year>2000 sourced

        // Add a Note (year 2001) → the Scan set is now NON-empty, so the Filter NARROWS to
        // that Note only (the Docs drop out).
        let n = json!({"type": "Note", "year": 2001});
        c.apply(&insert("note", n.clone()));
        model.insert("note".into(), props(n));
        assert_eq!(c.current(), c.recompute(&model));
        assert_eq!(c.current().ids(), vec!["note"]);
    }

    #[test]
    fn apply_touches_are_bounded_by_delta_not_view() {
        let plan = Plan::new(vec![Op::Scan {
            label: "Doc".into(),
        }]);
        let mut c = Circuit::compile(&plan).unwrap();
        for i in 0..1000 {
            c.apply(&insert(&format!("n{i}"), json!({"type": "Doc"})));
        }
        // One more single-row delta into a 1000-row view touches exactly 1 stage entry.
        assert_eq!(c.apply(&insert("z", json!({"type": "Doc"}))), 1);
        // A filtered-out row (type != Doc) touches nothing.
        assert_eq!(c.apply(&insert("q", json!({"type": "Other"}))), 0);
    }

    #[test]
    fn filter_predicate_matches_recompute() {
        let plan = Plan::new(vec![
            Op::Scan {
                label: "Doc".into(),
            },
            Op::Filter {
                preds: vec![Pred::GtNum {
                    prop: "year".into(),
                    n: 2000.0,
                }],
            },
        ]);
        let mut c = Circuit::compile(&plan).unwrap();
        let mut model = node_map(&[]);

        for (id, year) in [("a", 1999), ("b", 2001), ("c", 2005)] {
            let v = json!({"type": "Doc", "year": year});
            c.apply(&insert(id, v.clone()));
            model.insert(id.into(), props(v));
        }
        assert_eq!(c.current(), c.recompute(&model));
        assert_eq!(c.current().ids(), vec!["b", "c"]);

        c.apply(&Delta::from(vec![
            super::super::zset::ZRow::retract("b", props(json!({"type": "Doc", "year": 2001}))),
            super::super::zset::ZRow::insert("b", props(json!({"type": "Doc", "year": 1990}))),
        ]));
        model.insert("b".into(), props(json!({"type": "Doc", "year": 1990})));
        assert_eq!(c.current(), c.recompute(&model));
        assert_eq!(c.current().ids(), vec!["c"]);
    }

    #[test]
    fn asof_predicate_matches_recompute() {
        let plan = Plan::new(vec![
            Op::Scan { label: "E".into() },
            Op::AsOf {
                ts: 100.0,
                axis: TimeAxis::Valid,
            },
        ]);
        let mut c = Circuit::compile(&plan).unwrap();
        let mut model = node_map(&[]);
        let live = json!({"type": "E", "valid_from": 50, "valid_until": 150});
        let dead = json!({"type": "E", "valid_from": 0, "valid_until": 100});
        c.apply(&insert("live", live.clone()));
        model.insert("live".into(), props(live));
        c.apply(&insert("dead", dead.clone()));
        model.insert("dead".into(), props(dead));
        assert_eq!(c.current(), c.recompute(&model));
        assert_eq!(c.current().ids(), vec!["live"]);
    }

    #[cfg(feature = "timeseries")]
    #[test]
    fn window_sum_matches_recompute_across_retracts() {
        let plan = Plan::new(vec![
            Op::Scan { label: "M".into() },
            Op::WindowAgg {
                secs: 10.0,
                agg: "sum".into(),
            },
        ]);
        let mut c = Circuit::compile(&plan).unwrap();
        let mut model = node_map(&[]);

        // bucket 0 [0,10): vf 3 val 5, vf 7 val 2 → sum 7; bucket 10 [10,20): vf 12 val 4 → 4
        for (id, vf, val) in [("a", 3, 5), ("b", 7, 2), ("c", 12, 4)] {
            let v = json!({"type": "M", "valid_from": vf, "value": val});
            c.apply(&insert(id, v.clone()));
            model.insert(id.into(), props(v));
        }
        assert_eq!(c.current(), c.recompute(&model));
        assert_eq!(c.current().ids(), vec!["0", "10"]);
        assert_eq!(c.current().rows()[0].score, Some(7.0));

        c.apply(&retract("a", json!({"type": "M", "valid_from": 3, "value": 5})));
        model.remove("a");
        assert_eq!(c.current(), c.recompute(&model));

        c.apply(&Delta::from(vec![
            super::super::zset::ZRow::retract(
                "c",
                props(json!({"type": "M", "valid_from": 12, "value": 4})),
            ),
            super::super::zset::ZRow::insert(
                "c",
                props(json!({"type": "M", "valid_from": 8, "value": 4})),
            ),
        ]));
        model.insert(
            "c".into(),
            props(json!({"type": "M", "valid_from": 8, "value": 4})),
        );
        assert_eq!(c.current(), c.recompute(&model));
        assert_eq!(c.current().ids(), vec!["0"]);
        assert_eq!(c.current().rows()[0].score, Some(6.0));
    }

    #[cfg(feature = "timeseries")]
    #[test]
    fn window_count_and_mean_match_recompute() {
        for agg in ["count", "mean", "avg"] {
            let plan = Plan::new(vec![
                Op::Scan { label: "M".into() },
                Op::WindowAgg {
                    secs: 10.0,
                    agg: agg.into(),
                },
            ]);
            let mut c = Circuit::compile(&plan).unwrap();
            let mut model = node_map(&[]);
            for (id, vf, val) in [("a", 1, 6), ("b", 2, 9), ("c", 11, 3)] {
                let v = json!({"type": "M", "valid_from": vf, "value": val});
                c.apply(&insert(id, v.clone()));
                model.insert(id.into(), props(v));
            }
            assert_eq!(c.current(), c.recompute(&model), "agg={agg}");
        }
    }

    #[cfg(feature = "timeseries")]
    #[test]
    fn window_drops_rows_missing_valid_from_or_value() {
        let plan = Plan::new(vec![
            Op::Scan { label: "M".into() },
            Op::WindowAgg {
                secs: 10.0,
                agg: "sum".into(),
            },
        ]);
        let mut c = Circuit::compile(&plan).unwrap();
        let mut model = node_map(&[]);
        for (id, v) in [
            ("ok", json!({"type": "M", "valid_from": 1, "value": 5})),
            ("no_vf", json!({"type": "M", "value": 9})),
            ("no_val", json!({"type": "M", "valid_from": 2})),
        ] {
            c.apply(&insert(id, v.clone()));
            model.insert(id.into(), props(v));
        }
        assert_eq!(c.current(), c.recompute(&model));
        assert_eq!(c.current().ids(), vec!["0"]);
        assert_eq!(c.current().rows()[0].score, Some(5.0));
    }

    #[test]
    fn limit_truncates_and_matches_recompute() {
        let plan = Plan::new(vec![
            Op::Scan {
                label: "Doc".into(),
            },
            Op::Limit { k: 2 },
        ]);
        let mut c = Circuit::compile(&plan).unwrap();
        let mut model = node_map(&[]);
        for id in ["d", "a", "c", "b"] {
            let v = json!({"type": "Doc"});
            c.apply(&insert(id, v.clone()));
            model.insert(id.into(), props(v));
        }
        assert_eq!(c.current().ids(), vec!["a", "b"]);
        assert_eq!(c.current(), c.recompute(&model));
    }
}
