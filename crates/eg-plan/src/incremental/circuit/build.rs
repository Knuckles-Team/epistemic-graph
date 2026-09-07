//! Assembling a [`super::Circuit`] from a plan, one op at a time
//! (CONCEPT:EG-KG.query.incremental-view-maintenance).
//!
//! Each supported op's incrementalization rule — and the reason an unsupported op has no
//! incremental form — is stated in its own method here rather than inside one loop body,
//! so the v1 supported shape (`Scan` → `Filter`/`AsOf`* → at most one `WindowAgg` → an
//! optional trailing `Limit`) is readable as a set of named rules.

use eg_types::wire::{Op, Pred, TimeAxis};

use super::{compile_window_agg, op_name, Circuit, Stage, StagePred, UnsupportedOp, WindowState};

/// A circuit under construction, folding a plan's ops in one at a time. It exists so each
/// op's incrementalization rule — and the reason an op has no incremental form — is stated
/// once, in its own place, instead of inside one loop body.
pub(super) struct CircuitBuild {
    /// The `Scan` label, needed again when a `WindowAgg` compiles its bucket state.
    label: String,
    stages: Vec<Stage>,
    window: Option<WindowState>,
    limit: Option<usize>,
}

impl CircuitBuild {
    /// A build seeded with the plan's leading `Scan`.
    pub(super) fn scanning(label: String) -> Self {
        CircuitBuild {
            stages: vec![Stage::new(StagePred::Scan {
                label: label.clone(),
            })],
            label,
            window: None,
            limit: None,
        }
    }

    /// Fold op `i` into the build, or reject it with the reason it has no incremental form.
    pub(super) fn push(&mut self, i: usize, op: &Op) -> Result<(), UnsupportedOp> {
        if self.limit.is_some() {
            return Err(unsupported(i, "no op may follow Limit"));
        }
        match op {
            Op::Filter { preds } => self.push_filter(i, preds),
            Op::AsOf { ts, axis } => self.push_asof(i, *ts, *axis),
            Op::WindowAgg { secs, agg } => self.push_window_agg(i, *secs, agg),
            Op::Limit { k } => {
                self.limit = Some(*k);
                Ok(())
            }
            other => Err(unsupported(
                i,
                format!("{} has no incremental form in v1", op_name(other)),
            )),
        }
    }

    /// A `Filter` is a membership stage. It must carry only relational predicates, and it
    /// cannot follow a `WindowAgg` (whose output rows are buckets, not nodes).
    fn push_filter(&mut self, i: usize, preds: &[Pred]) -> Result<(), UnsupportedOp> {
        if self.window.is_some() {
            return Err(unsupported(i, "Filter after WindowAgg is not supported"));
        }
        for p in preds {
            match p {
                Pred::Eq { .. } | Pred::GtNum { .. } | Pred::LtNum { .. } => {}
                _ => {
                    return Err(unsupported(
                        i,
                        "Filter carries a non-relational predicate (JsonPath/spatial)",
                    ))
                }
            }
        }
        self.stages.push(Stage::new(StagePred::Filter {
            preds: preds.to_vec(),
        }));
        Ok(())
    }

    /// An `AsOf` is a membership stage, under the same post-`WindowAgg` restriction.
    fn push_asof(&mut self, i: usize, ts: f64, axis: TimeAxis) -> Result<(), UnsupportedOp> {
        if self.window.is_some() {
            return Err(unsupported(i, "AsOf after WindowAgg is not supported"));
        }
        self.stages.push(Stage::new(StagePred::AsOf { ts, axis }));
        Ok(())
    }

    /// At most one `WindowAgg`, and only directly after the `Scan`: a Filter/AsOf before it
    /// would source-on-empty and make the aggregate non-local.
    fn push_window_agg(&mut self, i: usize, secs: f64, agg: &str) -> Result<(), UnsupportedOp> {
        if self.window.is_some() {
            return Err(unsupported(i, "more than one WindowAgg is not supported"));
        }
        if self.stages.len() > 1 {
            return Err(unsupported(
                i,
                "WindowAgg over a Filter/AsOf-narrowed set is not incrementally \
                 maintainable in v1 (exec's empty-input-source rule makes it non-local); \
                 only Scan → WindowAgg",
            ));
        }
        self.window = Some(compile_window_agg(i, self.label.clone(), secs, agg)?);
        Ok(())
    }

    /// Window mode does not use the membership stages (its `label` gate lives in
    /// `WindowState`); dropping them lets `apply`/`current` branch cleanly on `window`.
    pub(super) fn finish(mut self) -> Circuit {
        if self.window.is_some() {
            self.stages.clear();
        }
        Circuit {
            stages: self.stages,
            window: self.window,
            limit: self.limit,
        }
    }
}

/// The rejection carried back when op `index` has no incremental form.
fn unsupported(index: usize, reason: impl Into<String>) -> UnsupportedOp {
    UnsupportedOp {
        index,
        reason: reason.into(),
    }
}
