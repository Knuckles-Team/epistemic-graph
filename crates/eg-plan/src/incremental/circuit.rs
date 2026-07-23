//! The incremental circuit (CONCEPT:EG-KG.storage.incremental-matview).
//!
//! Compiles a `wire::Plan` into a [`Circuit`] over the SUBSET of `Op` that is provably
//! safe to maintain by delta (DBSP's linear operators + the linear tumbling aggregate),
//! and maintains the materialized result as [`Delta`]s arrive — the direct
//! generalization of `src/server/cdc.rs`'s hand-rolled two-aggregate
//! `ContinuousQuery`/`maintain()` to an arbitrary supported plan.
//!
//! ## v1 supported operators
//!
//! * **Linear / stateless** (`∂L(ΔD) = L(ΔD)`, no operator state):
//!   `Scan { label }` (the source label predicate), `Filter { preds }`
//!   (`Eq`/`GtNum`/`LtNum` only), `AsOf { ts, axis }` (a per-row temporal predicate).
//!   Each is a pure per-row filter evaluated against the delta row's carried props.
//! * **Linear aggregate** (`WindowAgg { secs, agg }` for `count`/`sum`/`mean`|`avg`):
//!   a tumbling per-bucket accumulator (`sum`, `count`) that retracts by SUBTRACTING the
//!   weighted value — turso's `aggregate_operator.rs` easy case. `min`/`max`/`first`/
//!   `last` retraction needs a per-bucket multiset (the hard case) and are DELIBERATELY
//!   left unsupported → the plan falls back to full recompute.
//! * **`Limit { k }`**: applied by truncating the fully-maintained body at read time.
//!   Correct (a retraction near the cut is reflected because the whole body is
//!   maintained); it is NOT yet the bounded top-k-plus-margin index the design sketches
//!   — a documented perf follow-up, not a correctness gap.
//!
//! Everything else (`Traverse`, `Reason`, `SparqlBgp`, `ForeignScan`, `RankMmr`,
//! `FuseRrf`, `Udf`, `TsScan`, `min`/`max` window aggs, `JsonPath`/spatial preds, …)
//! makes [`Circuit::compile`] return [`UnsupportedOp`] naming the first offending op, so
//! the caller keeps that view on today's recompute-on-`Get` path — a per-view fallback,
//! never a silently-wrong incremental answer.
//!
//! ## Correctness contract
//!
//! [`Circuit::apply`] maintains state by folding signed weights; [`Circuit::recompute`]
//! rebuilds the SAME result from a full node scan with NO maintained state. The two MUST
//! agree after every mutation — the differential property test in
//! `tests/incremental_oracle.rs`. The per-row predicate ([`Circuit::row_passes`]) is a
//! shared primitive (the analogue of DataFusion being shared by both paths); the tested
//! surface is the delta-maintenance of the membership map / bucket accumulators.

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};

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

/// The linear tumbling aggregate a `WindowAgg` maintains — the subset that retracts by
/// pure subtraction (`count`/`sum`/`mean`). `min`/`max`/`first`/`last` are NOT here.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
enum Agg {
    Count,
    Sum,
    Mean,
}

/// Field a `WindowAgg` reads the aggregated value from (self-contained model: `value`),
/// and the field it reads the event timestamp from (`ts`).
const VALUE_FIELD: &str = "value";
const TS_FIELD: &str = "ts";

/// One bucket accumulator: a running weighted sum and a signed count. Present in the
/// result iff `count > 0`; for integer inputs (the property-test domain) the running sum
/// is exact regardless of fold order.
#[derive(Clone, Copy, Debug, Default, PartialEq, serde::Serialize, serde::Deserialize)]
struct Bucket {
    sum: f64,
    count: i64,
}

/// The maintained `WindowAgg` stage: the window width, the aggregate, and the per-bucket
/// accumulators.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct WindowState {
    secs: f64,
    agg: Agg,
    buckets: BTreeMap<i64, Bucket>,
}

/// A compiled incremental circuit for one supported plan. Serializable so its maintained
/// state (membership map / bucket accumulators) persists in the `matview_operator_state`
/// redb table — the analogue of turso's `dbsp_state` btree.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Circuit {
    /// The `Scan` source label (`type`/`node_type`/`label` property must equal this).
    label: String,
    /// `Filter` predicates (`Eq`/`GtNum`/`LtNum`), ANDed per row.
    filters: Vec<Pred>,
    /// `AsOf` temporal predicates, ANDed per row.
    asofs: Vec<(f64, TimeAxis)>,
    /// `Some` ⇒ aggregate (window) mode; `None` ⇒ plain membership set.
    window: Option<WindowState>,
    /// Trailing `Limit`, applied at read.
    limit: Option<usize>,
    /// Non-window maintained membership: id → net signed weight (present iff `> 0`).
    members: HashMap<String, i32>,
}

impl Circuit {
    /// Compile a plan into a circuit. `Ok` only when EVERY op is in the v1 supported set
    /// (see the module docs); the FIRST unsupported op short-circuits with its index+kind
    /// so the caller can log exactly why a view fell back to recompute.
    ///
    /// Supported shape: `Scan` (required, first) → any number of `Filter`/`AsOf` → at
    /// most one `WindowAgg` → an optional trailing `Limit`. After a `WindowAgg` only
    /// `Limit` may follow (a node-property filter over bucket rows is nonsensical).
    pub fn compile(plan: &Plan) -> Result<Circuit, UnsupportedOp> {
        let ops = &plan.ops;
        if ops.is_empty() {
            return Err(UnsupportedOp {
                index: 0,
                reason: "empty plan (nothing to incrementalize)".into(),
            });
        }
        // ops[0] must be a Scan source.
        let label = match &ops[0] {
            Op::Scan { label } => label.clone(),
            other => {
                return Err(UnsupportedOp {
                    index: 0,
                    reason: format!("plan must start with Scan, found {}", op_name(other)),
                })
            }
        };

        let mut filters: Vec<Pred> = Vec::new();
        let mut asofs: Vec<(f64, TimeAxis)> = Vec::new();
        let mut window: Option<WindowState> = None;
        let mut limit: Option<usize> = None;

        for (i, op) in ops.iter().enumerate().skip(1) {
            // Nothing may follow a Limit.
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
                        // Only the pure relational preds are incrementalizable per-row.
                        match p {
                            Pred::Eq { .. } | Pred::GtNum { .. } | Pred::LtNum { .. } => {
                                filters.push(p.clone())
                            }
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
                }
                Op::AsOf { ts, axis } => {
                    if window.is_some() {
                        return Err(UnsupportedOp {
                            index: i,
                            reason: "AsOf after WindowAgg is not supported".into(),
                        });
                    }
                    asofs.push((*ts, *axis));
                }
                Op::WindowAgg { secs, agg } => {
                    if window.is_some() {
                        return Err(UnsupportedOp {
                            index: i,
                            reason: "more than one WindowAgg is not supported".into(),
                        });
                    }
                    if *secs <= 0.0 {
                        return Err(UnsupportedOp {
                            index: i,
                            reason: "WindowAgg width must be positive".into(),
                        });
                    }
                    let agg = match agg.to_ascii_lowercase().as_str() {
                        "count" => Agg::Count,
                        "sum" => Agg::Sum,
                        "mean" | "avg" => Agg::Mean,
                        other => {
                            return Err(UnsupportedOp {
                                index: i,
                                reason: format!(
                                    "WindowAgg '{other}' is not a linear aggregate \
                                     (only count/sum/mean/avg)"
                                ),
                            })
                        }
                    };
                    window = Some(WindowState {
                        secs: *secs,
                        agg,
                        buckets: BTreeMap::new(),
                    });
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

        Ok(Circuit {
            label,
            filters,
            asofs,
            window,
            limit,
            members: HashMap::new(),
        })
    }

    /// Whether this circuit's plan is aggregate (window) mode.
    pub fn is_window(&self) -> bool {
        self.window.is_some()
    }

    /// Evaluate the per-row predicate (label ∧ all filters ∧ all AsOf) against a decoded
    /// property map. The SHARED primitive both the incremental and the recompute paths
    /// use — the analogue of DataFusion being shared, so the differential test isolates
    /// the delta-maintenance, not predicate representation.
    pub fn row_passes(&self, props: &Map<String, Value>) -> bool {
        if !label_matches(props, &self.label) {
            return false;
        }
        for p in &self.filters {
            if !pred_holds(props, p) {
                return false;
            }
        }
        for (ts, axis) in &self.asofs {
            if !asof_holds(props, *ts, *axis) {
                return false;
            }
        }
        true
    }

    /// INCREMENTAL maintenance: fold one delta into the maintained state in O(|delta|).
    /// Each passing row applies its signed weight to the membership map (set mode) or to
    /// its time-bucket accumulator (window mode). A row failing the predicate contributes
    /// nothing — for an update's retract(old)+insert(new) pair, an old that no longer
    /// passes simply drops, exactly reproducing the recompute.
    pub fn apply(&mut self, delta: &Delta) {
        for row in &delta.rows {
            if row.weight == 0 || !self.row_passes(&row.props) {
                continue;
            }
            match &mut self.window {
                Some(w) => {
                    let ts = row.num(TS_FIELD).unwrap_or(0.0);
                    let val = row.num(VALUE_FIELD).unwrap_or(0.0);
                    let k = bucket_index(ts, w.secs);
                    let b = w.buckets.entry(k).or_default();
                    b.sum += row.weight as f64 * val;
                    b.count += row.weight as i64;
                    if b.count <= 0 {
                        w.buckets.remove(&k);
                    }
                }
                None => {
                    let e = self.members.entry(row.id.clone()).or_insert(0);
                    *e += row.weight;
                    if *e <= 0 {
                        self.members.remove(&row.id);
                    }
                }
            }
        }
    }

    /// The current materialized result from the MAINTAINED state (the hot-path read a
    /// `Mode::Incremental` `Get` serves).
    pub fn current(&self) -> RowSet {
        match &self.window {
            Some(w) => finalize_buckets(w.buckets.iter().map(|(k, b)| (*k, *b)), w, self.limit),
            None => {
                let ids = self
                    .members
                    .iter()
                    .filter(|(_, wt)| **wt > 0)
                    .map(|(id, _)| id.clone());
                finalize_members(ids, self.limit)
            }
        }
    }

    /// INDEPENDENT full recompute from the complete current node set — NO maintained
    /// state, a from-scratch scan. The differential oracle `current()` must equal after
    /// every mutation. `nodes` is `id → decoded props` for every node currently present.
    pub fn recompute(&self, nodes: &BTreeMap<String, Map<String, Value>>) -> RowSet {
        match &self.window {
            Some(w) => {
                let mut buckets: BTreeMap<i64, Bucket> = BTreeMap::new();
                for props in nodes.values() {
                    if !self.row_passes(props) {
                        continue;
                    }
                    let ts = props.get(TS_FIELD).and_then(Value::as_f64).unwrap_or(0.0);
                    let val = props
                        .get(VALUE_FIELD)
                        .and_then(Value::as_f64)
                        .unwrap_or(0.0);
                    let k = bucket_index(ts, w.secs);
                    let b = buckets.entry(k).or_default();
                    b.sum += val;
                    b.count += 1;
                }
                finalize_buckets(buckets, w, self.limit)
            }
            None => {
                let ids = nodes
                    .iter()
                    .filter(|(_, props)| self.row_passes(props))
                    .map(|(id, _)| id.clone());
                finalize_members(ids, self.limit)
            }
        }
    }
}

/// Finalize a set-mode result: id-sorted, unscored, truncated to `limit`.
fn finalize_members(ids: impl IntoIterator<Item = String>, limit: Option<usize>) -> RowSet {
    let mut ids: Vec<String> = ids.into_iter().collect();
    ids.sort();
    if let Some(k) = limit {
        ids.truncate(k);
    }
    RowSet::from_rows(ids.into_iter().map(|id| (id, None)))
}

/// Finalize a window-mode result: one row per non-empty bucket (id = bucket start,
/// score = the aggregate), ordered by score DESC then id ASC, truncated to `limit`.
fn finalize_buckets(
    buckets: impl IntoIterator<Item = (i64, Bucket)>,
    w: &WindowState,
    limit: Option<usize>,
) -> RowSet {
    let mut rows: Vec<(String, Option<f32>)> = buckets
        .into_iter()
        .filter(|(_, b)| b.count > 0)
        .map(|(k, b)| {
            let agg = match w.agg {
                Agg::Count => b.count as f64,
                Agg::Sum => b.sum,
                Agg::Mean => b.sum / b.count as f64,
            };
            (bucket_id(k, w.secs), Some(agg as f32))
        })
        .collect();
    rows.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(Ordering::Equal)
            .then_with(|| a.0.cmp(&b.0))
    });
    if let Some(k) = limit {
        rows.truncate(k);
    }
    RowSet::from_rows(rows)
}

/// The tumbling-bucket index for `ts` under window width `secs`.
fn bucket_index(ts: f64, secs: f64) -> i64 {
    (ts / secs).floor() as i64
}

/// The stable string id for a bucket (its aligned start instant). Both the incremental
/// and the recompute paths format it identically so the RowSet ids compare exactly.
fn bucket_id(k: i64, secs: f64) -> String {
    format!("{}", (k as f64 * secs) as i64)
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

// ── shared per-row predicate primitives ──────────────────────────────────────

/// The `Scan` label test: the first present of `type`/`node_type`/`label` (the same keys
/// `cdc::extract_label` uses) equals `label`.
fn label_matches(props: &Map<String, Value>, label: &str) -> bool {
    for key in ["type", "node_type", "label"] {
        if let Some(Value::String(s)) = props.get(key) {
            return s == label;
        }
    }
    false
}

/// Evaluate one relational `Filter` predicate against a row's props.
fn pred_holds(props: &Map<String, Value>, pred: &Pred) -> bool {
    match pred {
        Pred::Eq { prop, value } => match props.get(prop) {
            Some(Value::String(s)) => s == value,
            Some(Value::Number(n)) => {
                // numeric-aware equality: compare as numbers when the target parses,
                // else as the stringified form (deterministic, shared by both paths).
                match value.parse::<f64>() {
                    Ok(v) => n.as_f64() == Some(v),
                    Err(_) => n.to_string() == *value,
                }
            }
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

/// The `AsOf` temporal predicate: is the row live at `ts` on the given axis? Missing
/// bounds are open (`valid_from`/`tx_from` default `-inf`, `valid_until`/`tx_to`
/// default `+inf`); the window is `[from, until)`.
fn asof_holds(props: &Map<String, Value>, ts: f64, axis: TimeAxis) -> bool {
    let (from_key, until_key) = match axis {
        TimeAxis::Valid => ("valid_from", "valid_until"),
        TimeAxis::Transaction => ("tx_from", "tx_to"),
    };
    let from = props
        .get(from_key)
        .and_then(Value::as_f64)
        .unwrap_or(f64::NEG_INFINITY);
    let until = props
        .get(until_key)
        .and_then(Value::as_f64)
        .unwrap_or(f64::INFINITY);
    from <= ts && ts < until
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

        // min/max window agg → fallback.
        let e = Circuit::compile(&Plan::new(vec![
            Op::Scan { label: "M".into() },
            Op::WindowAgg {
                secs: 10.0,
                agg: "max".into(),
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

    // ── per-operator: apply-delta result == full recompute ──

    #[test]
    fn scan_add_update_remove_matches_recompute() {
        let plan = Plan::new(vec![Op::Scan {
            label: "Doc".into(),
        }]);
        let mut c = Circuit::compile(&plan).unwrap();
        let mut model = node_map(&[]);

        // add a Doc + a non-Doc
        c.apply(&insert("a", json!({"type": "Doc"})));
        model.insert("a".into(), props(json!({"type": "Doc"})));
        c.apply(&insert("x", json!({"type": "Other"})));
        model.insert("x".into(), props(json!({"type": "Other"})));
        assert_eq!(c.current(), c.recompute(&model));
        assert_eq!(c.current().ids(), vec!["a"]);

        // update a's label away from Doc (retract old, insert new)
        c.apply(&Delta::from(vec![
            super::super::zset::ZRow::retract("a", props(json!({"type": "Doc"}))),
            super::super::zset::ZRow::insert("a", props(json!({"type": "Other"}))),
        ]));
        model.insert("a".into(), props(json!({"type": "Other"})));
        assert_eq!(c.current(), c.recompute(&model));
        assert!(c.current().is_empty());
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

        for (id, year) in [("a", 1999.0), ("b", 2001.0), ("c", 2005.0)] {
            let v = json!({"type": "Doc", "year": year});
            c.apply(&insert(id, v.clone()));
            model.insert(id.into(), props(v));
        }
        assert_eq!(c.current(), c.recompute(&model));
        assert_eq!(c.current().ids(), vec!["b", "c"]);

        // update b below the cut → drops
        c.apply(&Delta::from(vec![
            super::super::zset::ZRow::retract("b", props(json!({"type": "Doc", "year": 2001.0}))),
            super::super::zset::ZRow::insert("b", props(json!({"type": "Doc", "year": 1990.0}))),
        ]));
        model.insert("b".into(), props(json!({"type": "Doc", "year": 1990.0})));
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
        // live at 100: [50,150); not live: [0,100) (until is exclusive)
        let live = json!({"type": "E", "valid_from": 50.0, "valid_until": 150.0});
        let dead = json!({"type": "E", "valid_from": 0.0, "valid_until": 100.0});
        c.apply(&insert("live", live.clone()));
        model.insert("live".into(), props(live));
        c.apply(&insert("dead", dead.clone()));
        model.insert("dead".into(), props(dead));
        assert_eq!(c.current(), c.recompute(&model));
        assert_eq!(c.current().ids(), vec!["live"]);
    }

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

        // bucket 0 [0,10): ts 3 val 5, ts 7 val 2 → sum 7
        // bucket 1 [10,20): ts 12 val 4 → sum 4
        for (id, ts, val) in [("a", 3.0, 5.0), ("b", 7.0, 2.0), ("c", 12.0, 4.0)] {
            let v = json!({"type": "M", "ts": ts, "value": val});
            c.apply(&insert(id, v.clone()));
            model.insert(id.into(), props(v));
        }
        assert_eq!(c.current(), c.recompute(&model));
        // bucket 0 sum 7 > bucket 1 sum 4 → order [ "0", "10" ]
        assert_eq!(c.current().ids(), vec!["0", "10"]);
        assert_eq!(c.current().rows()[0].score, Some(7.0));

        // retract a (ts3 val5) → bucket 0 now sum 2
        c.apply(&retract("a", json!({"type": "M", "ts": 3.0, "value": 5.0})));
        model.remove("a");
        assert_eq!(c.current(), c.recompute(&model));

        // move c from bucket 1 to bucket 0 (update ts 12→8) via retract+insert
        c.apply(&Delta::from(vec![
            super::super::zset::ZRow::retract(
                "c",
                props(json!({"type": "M", "ts": 12.0, "value": 4.0})),
            ),
            super::super::zset::ZRow::insert(
                "c",
                props(json!({"type": "M", "ts": 8.0, "value": 4.0})),
            ),
        ]));
        model.insert(
            "c".into(),
            props(json!({"type": "M", "ts": 8.0, "value": 4.0})),
        );
        assert_eq!(c.current(), c.recompute(&model));
        // bucket 1 now empty (pruned), bucket 0 = 2+4 = 6
        assert_eq!(c.current().ids(), vec!["0"]);
        assert_eq!(c.current().rows()[0].score, Some(6.0));
    }

    #[test]
    fn window_count_and_mean_match_recompute() {
        for (agg, _lbl) in [("count", "M"), ("mean", "M"), ("avg", "M")] {
            let plan = Plan::new(vec![
                Op::Scan { label: "M".into() },
                Op::WindowAgg {
                    secs: 10.0,
                    agg: agg.into(),
                },
            ]);
            let mut c = Circuit::compile(&plan).unwrap();
            let mut model = node_map(&[]);
            for (id, ts, val) in [("a", 1.0, 6.0), ("b", 2.0, 9.0), ("c", 11.0, 3.0)] {
                let v = json!({"type": "M", "ts": ts, "value": val});
                c.apply(&insert(id, v.clone()));
                model.insert(id.into(), props(v));
            }
            assert_eq!(c.current(), c.recompute(&model), "agg={agg}");
        }
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
        // id-sorted → a,b,c,d; limit 2 → a,b
        assert_eq!(c.current().ids(), vec!["a", "b"]);
        assert_eq!(c.current(), c.recompute(&model));
    }
}
