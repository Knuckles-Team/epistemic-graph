//! Time as a score-producing operator dimension in the unified RowSet algebra
//! (CONCEPT:EG-KG.compute.handled-outside-single-anchor — SKETCH / seam for the eg-plan binding increment).
//!
//! ## Why this is a sketch, not a wired planner op
//!
//! The unified-plan track (`eg-plan`) defines the contract: every operator is
//! `(RowSet) -> RowSet`, where a `RowSet` row is `(id, Option<score>)`; Filter (SQL),
//! Traverse (graph), Rank (vector), Reason (datalog) all obey it, so a plan is a
//! reorderable `Vec<Op>`. That crate is built on a sibling branch. To keep eg-tsdb's
//! DAG position clean (it does NOT depend on eg-plan), this module RESTATES the
//! minimal RowSet contract and implements the Time ops against it — proving the Op
//! SHAPE compiles + composes, and leaving a `move`-not-redesign seam for eg-plan to
//! bind later. **The planner binding (registering these in the `Op` enum + the
//! cost-based reorder) is the D-bind increment, deliberately NOT done here.**
//!
//! ## The insight that makes Time fit
//!
//! A `RowSet` row already carries an optional **score**, and a time-series collapses
//! to *"a score per id as-of a time."* So the Time ops are **score-producing ops
//! exactly like vector `Rank`** — they differ only in HOW the score is computed
//! (nearest-in-time / windowed-agg / recency, vs cosine). The only RowSet extension
//! Time needs is an optional `event_ts` per row (a `:Memory`'s `valid_from`, a
//! `:Trade`'s exec ts); plain graph/vector rows leave it `None`.
//!
//! | Time Op       | effect on the RowSet                                            |
//! |---------------|----------------------------------------------------------------|
//! | `Asof`        | each row's `event_ts` ↦ the series value as-of that time → score|
//! | `Window`      | each row ↦ a windowed aggregate ending at `event_ts` → score    |
//! | `DecayWeight` | multiply each row's score by its Ebbinghaus recency weight      |
//!
//! ### The seam left for eg-plan
//! `Row { id, score, event_ts }` is shape-compatible with the eg-plan row (a strict
//! superset: the extra `event_ts` is what binding adds). When eg-plan lands, the
//! integration is: (1) add `event_ts: Option<Ts>` to the canonical `Row`; (2) add
//! `Asof`/`Window`/`DecayWeight` variants to the `Op` enum carrying a `&SeriesStore`
//! handle (or a series-id the executor resolves); (3) move `TimeOp::apply`'s bodies
//! into the executor's match arm. No algorithm changes — the bodies below ARE the
//! executor logic.

use crate::point::Ts;

/// Minimal restatement of the unified-plan `Row` (shape-compatible; the `event_ts`
/// is the Time extension eg-plan adds when binding).
#[derive(Clone, Debug, PartialEq)]
pub struct Row {
    pub id: String,
    pub score: Option<f32>,
    /// The TIME extension: an optional event time (a `:Memory`'s `valid_from`, a
    /// `:Trade`'s exec ts). Time ops read this to resolve a series value as-of the
    /// row. Plain graph/vector rows have `None`.
    pub event_ts: Option<Ts>,
}

/// Minimal restatement of the unified-plan `RowSet`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RowSet {
    pub rows: Vec<Row>,
}

impl RowSet {
    /// Build a RowSet of timed rows (id + event_ts, no score yet).
    pub fn from_timed<I: IntoIterator<Item = (String, Ts)>>(it: I) -> Self {
        RowSet {
            rows: it
                .into_iter()
                .map(|(id, ts)| Row {
                    id,
                    score: None,
                    event_ts: Some(ts),
                })
                .collect(),
        }
    }

    /// LIMIT (top-k by current order) — same as the unified algebra.
    pub fn limit(mut self, k: usize) -> RowSet {
        self.rows.truncate(k);
        self
    }

    /// Re-sort by score descending (RANK-style reorder), so a downstream LIMIT picks
    /// the top-scored rows.
    pub fn rank_by_score(mut self) -> RowSet {
        self.rows.sort_by(|a, b| {
            b.score
                .unwrap_or(f32::NEG_INFINITY)
                .partial_cmp(&a.score.unwrap_or(f32::NEG_INFINITY))
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        self
    }

    /// `DecayWeight` as a standalone (store-free) op: multiply each scored row by its
    /// Ebbinghaus recency weight at `now`, the SAME `eg_core::decay` curve the memory
    /// layer uses. Available without the `redb-store` feature (no series needed).
    pub fn decay_weight(mut self, now: Ts, half_life_secs: f64) -> RowSet {
        for row in self.rows.iter_mut() {
            if let (Some(s), Some(t)) = (row.score, row.event_ts) {
                let age_secs = (now - t) as f64 / 1e9;
                let w = eg_core::decay::ebbinghaus_weight(age_secs, half_life_secs) as f32;
                row.score = Some(s * w);
            }
        }
        self
    }
}

// The store-backed Time ops (Asof/Window) + the one composed example need a
// `SeriesStore`, so they are gated on the store feature. `DecayWeight` lives on
// `RowSet` above and is always available.
#[cfg(feature = "redb-store")]
mod store_ops {
    use std::collections::HashMap;

    use super::{Row, RowSet};
    use crate::point::{Point, Ts};
    use crate::query::{asof_join_backward, time_bucket, Agg};
    use crate::store::SeriesStore;

    /// The store-backed Time operators. Each is `(RowSet) -> RowSet`, the same
    /// contract as Filter/Traverse/Rank/Reason. This is the executor logic eg-plan
    /// will move into its `Op` match arm.
    pub enum TimeOp<'a> {
        /// ASOF — for each row's `event_ts`, look up the named series' value as-of
        /// that time and store it as the row's score.
        Asof {
            store: &'a SeriesStore,
            series_id: &'a str,
            tolerance: Option<i64>,
        },
        /// WINDOW — for each row, a windowed `agg` over `[event_ts - width, event_ts]`
        /// → score (last bucket value).
        Window {
            store: &'a SeriesStore,
            series_id: &'a str,
            width: Ts,
            agg: Agg,
        },
    }

    impl<'a> TimeOp<'a> {
        pub fn apply(&self, input: RowSet) -> Result<RowSet, String> {
            match self {
                TimeOp::Asof {
                    store,
                    series_id,
                    tolerance,
                } => {
                    let mut rows = input.rows;
                    // Sort events by ts and reuse the O(L+R) merge-asof primitive.
                    let mut order: Vec<usize> = (0..rows.len()).collect();
                    order.sort_by_key(|&i| rows[i].event_ts.unwrap_or(Ts::MIN));
                    let left: Vec<Point> = order
                        .iter()
                        .filter_map(|&i| rows[i].event_ts.map(|t| Point::single(t, 0.0)))
                        .collect();
                    let right = store.scan_all(series_id).map_err(|e| e.to_string())?;
                    let joined = asof_join_backward(&left, &right, *tolerance);
                    let mut by_ts: HashMap<Ts, Option<f64>> = HashMap::new();
                    for r in joined {
                        by_ts.insert(r.ts, r.right);
                    }
                    for row in rows.iter_mut() {
                        if let Some(t) = row.event_ts {
                            row.score = by_ts.get(&t).copied().flatten().map(|v| v as f32);
                        }
                    }
                    Ok(RowSet { rows })
                }
                TimeOp::Window {
                    store,
                    series_id,
                    width,
                    agg,
                } => {
                    let mut rows = input.rows;
                    for row in rows.iter_mut() {
                        if let Some(t) = row.event_ts {
                            let pts = store
                                .range(series_id, t - width, t + 1)
                                .map_err(|e| e.to_string())?;
                            let bars = time_bucket(&pts, *width, *agg);
                            row.score = bars.last().map(|b| b.value as f32);
                        }
                    }
                    Ok(RowSet { rows })
                }
            }
        }
    }

    /// THE ONE COMPOSED EXAMPLE — the time leg of `filter(SQL) → traverse(graph) →
    /// asof(time) → rank(vector)`. The graph/SQL legs hand us a `RowSet` of candidate
    /// nodes each carrying an `event_ts`; here we run the time ops and return a
    /// scored, ranked, limited RowSet — exactly what the next op / final result
    /// consumes.
    ///
    /// Plan: `Asof(price series) → DecayWeight(now) → rank_by_score → limit(k)`
    /// = "of these candidate events, score each by the price as-of the event, decayed
    /// by how long ago it happened, keep the freshest-and-highest k."
    pub fn composed_example(
        handoff: RowSet,
        store: &SeriesStore,
        series_id: &str,
        now: Ts,
        half_life_secs: f64,
        k: usize,
    ) -> Result<RowSet, String> {
        let after_asof = TimeOp::Asof {
            store,
            series_id,
            tolerance: None,
        }
        .apply(handoff)?;
        Ok(after_asof
            .decay_weight(now, half_life_secs)
            .rank_by_score()
            .limit(k))
    }

    // Keep `Row` referenced from this module for the doc-seam (eg-plan moves these
    // bodies into the executor, which constructs `Row`s directly).
    #[allow(unused_imports)]
    use super::Row as _RowSeam;
    #[allow(dead_code)]
    fn _row_shape_seam(r: Row) -> Option<Ts> {
        r.event_ts
    }
}

#[cfg(feature = "redb-store")]
pub use store_ops::{composed_example, TimeOp};
