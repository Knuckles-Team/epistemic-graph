//! Materialised derived series (EH-524): the pure maintenance logic — no store here.
//!
//! A derived series is an ordinary series whose points are `[value, revision,
//! known_at_ms]` ([`DERIVED_FIELDS`]); its definition and incremental state live in the
//! [`DerivedState`] the store keeps in the series' metadata. The state advances from
//! its checkpoint on every new source point (O(new points)), never recomputing history.
//!
//! A source point at or before the last consumed timestamp is a REVISION (a late or
//! corrected point — the store keeps every version, equal timestamps in arrival order).
//! [`DerivedState::replay`] restores the last checkpoint before it, recomputes forward,
//! and APPENDS a new version (`revision + 1`, a later `known_at_ms`) wherever a value
//! changed — nothing is edited in place, so the view as of any earlier moment
//! ([`as_of`]) is exactly what it was.

use std::collections::BTreeMap;

use eg_types::series_expr::SeriesExpr;
use serde::{Deserialize, Serialize};

use super::{expr_digest, Program};
use crate::point::{Point, Ts};

/// The field layout of a derived series' points.
pub const DERIVED_FIELDS: [&str; 3] = ["value", "revision", "known_at_ms"];

/// A checkpoint is kept every this many consumed source points.
pub const CHECKPOINT_EVERY: u64 = 256;

/// At most this many checkpoints are kept; the oldest go first.
pub const MAX_CHECKPOINTS: usize = 16;

/// The largest encoded state a derived series may keep in its metadata.
pub const MAX_STATE_BYTES: usize = 1 << 20;

/// The program state right after consuming the source point at `ts`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Checkpoint {
    pub ts: Ts,
    pub seen: u64,
    pub program: Program,
}

/// A derived series' definition, provenance and incremental state.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DerivedState {
    /// The source series (local id, in the derived series' own scope).
    pub source: String,
    pub expr: SeriesExpr,
    /// The expression's canonical UQL spelling.
    pub canonical: String,
    /// `sha256:` over the canonical spelling and the kernel generation.
    pub digest: String,
    /// The program after the last consumed source point.
    pub program: Program,
    pub last_ts: Option<Ts>,
    /// Source points consumed so far.
    pub seen: u64,
    pub checkpoints: Vec<Checkpoint>,
}

/// What one maintenance step produced.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Step {
    /// Derived points to append (new values and new versions of changed ones).
    pub points: Vec<Point>,
    /// Source points consumed.
    pub consumed: usize,
    /// Whether every source point offered was consumed (`false`: the work budget
    /// stopped it; the next step continues from the checkpoint).
    pub caught_up: bool,
}

impl DerivedState {
    /// A fresh derived series over `source`.
    pub fn define(source: &str, expr: SeriesExpr) -> Result<Self, String> {
        Ok(Self {
            source: source.to_string(),
            canonical: eg_types::wire::uql_series_expr(&expr).map_err(|e| e.to_string())?,
            digest: expr_digest(&expr)?,
            program: Program::compile(&expr)?,
            expr,
            last_ts: None,
            seen: 0,
            checkpoints: Vec::new(),
        })
    }

    /// The bytes the store keeps (MessagePack), refused over [`MAX_STATE_BYTES`] (the
    /// state is O(window × checkpoints)).
    pub fn encode(&self) -> Result<Vec<u8>, String> {
        let bytes = rmp_serde::to_vec(self).map_err(|e| e.to_string())?;
        if bytes.len() > MAX_STATE_BYTES {
            return Err(format!(
                "the derived series state is {} bytes, over the {MAX_STATE_BYTES}-byte limit; use smaller windows",
                bytes.len()
            ));
        }
        Ok(bytes)
    }

    /// Restore from [`Self::encode`] bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        rmp_serde::from_slice(bytes).map_err(|e| e.to_string())
    }

    /// Consume the source points after `last_ts` (latest version per timestamp, in
    /// timestamp order), at most `budget` of them.
    pub fn advance(&mut self, source: &[Point], known_at_ms: u64, budget: usize) -> Step {
        let fresh: Vec<&Point> = source
            .iter()
            .filter(|p| self.last_ts.is_none_or(|t| p.ts > t))
            .collect();
        let consumed = fresh.len().min(budget);
        let points = fresh[..consumed]
            .iter()
            .filter_map(|p| {
                self.consume(p)
                    .map(|v| derived_point(p.ts, v, 0, known_at_ms))
            })
            .collect();
        Step {
            points,
            consumed,
            caught_up: consumed == fresh.len(),
        }
    }

    /// Where a replay for a revision at `revised_from` starts reading: the timestamp of
    /// the checkpoint it restores (exclusive), or `None` for the whole history.
    pub fn replay_start(&self, revised_from: Ts) -> Option<Ts> {
        self.restore_point(revised_from).map(|c| c.ts)
    }

    /// Recompute from the last checkpoint before `revised_from` over `source` (the
    /// latest version of every source point after [`Self::replay_start`]), comparing
    /// with `current` (the derived series' latest versions); a changed value is
    /// appended as the next revision. Refused when `source` exceeds `budget`.
    pub fn replay(
        &mut self,
        revised_from: Ts,
        source: &[Point],
        current: &[Point],
        known_at_ms: u64,
        budget: usize,
    ) -> Result<Step, String> {
        if source.len() > budget {
            return Err(format!(
                "derived series replay needs {} source points, over the work budget of {budget}",
                source.len()
            ));
        }
        self.rewind(revised_from)?;
        let current = revisions(current);
        let mut points = Vec::new();
        for p in source {
            let fresh = self.consume(p);
            points.extend(revised_point(p.ts, fresh, current.get(&p.ts), known_at_ms));
        }
        Ok(Step {
            points,
            consumed: source.len(),
            caught_up: true,
        })
    }

    /// Restore the last checkpoint before `revised_from` (or a fresh program) and drop
    /// every later checkpoint.
    fn rewind(&mut self, revised_from: Ts) -> Result<(), String> {
        let restored = self.restore_point(revised_from).cloned();
        self.checkpoints.retain(|c| c.ts < revised_from);
        match restored {
            Some(c) => {
                self.program = c.program;
                self.seen = c.seen;
                self.last_ts = Some(c.ts);
            }
            None => {
                self.program = Program::compile(&self.expr)?;
                self.seen = 0;
                self.last_ts = None;
            }
        }
        Ok(())
    }

    fn restore_point(&self, revised_from: Ts) -> Option<&Checkpoint> {
        self.checkpoints.iter().rev().find(|c| c.ts < revised_from)
    }

    /// Step the program over one source point, checkpointing on schedule.
    fn consume(&mut self, point: &Point) -> Option<f64> {
        let value = self.program.step(&|name| field(point, name));
        self.last_ts = Some(point.ts);
        self.seen += 1;
        if self.seen % CHECKPOINT_EVERY == 0 {
            self.checkpoints.push(Checkpoint {
                ts: point.ts,
                seen: self.seen,
                program: self.program.clone(),
            });
            let excess = self.checkpoints.len().saturating_sub(MAX_CHECKPOINTS);
            self.checkpoints.drain(..excess);
        }
        value
    }
}

/// A source point's field `v<i>`.
fn field(point: &Point, name: &str) -> Option<f64> {
    let i: usize = name.strip_prefix('v')?.parse().ok()?;
    point.values.get(i).copied()
}

fn derived_point(ts: Ts, value: f64, revision: u64, known_at_ms: u64) -> Point {
    Point {
        ts,
        values: vec![value, revision as f64, known_at_ms as f64],
    }
}

/// The point a replay appends at `ts`, if its value changed: a first value is revision
/// 0, a changed one the next revision. A value that became undefined appends nothing.
fn revised_point(
    ts: Ts,
    fresh: Option<f64>,
    old: Option<&(f64, u64)>,
    known_at_ms: u64,
) -> Option<Point> {
    let value = fresh?;
    match old {
        None => Some(derived_point(ts, value, 0, known_at_ms)),
        Some((old, _)) if old.to_bits() == value.to_bits() => None,
        Some((_, revision)) => Some(derived_point(ts, value, revision + 1, known_at_ms)),
    }
}

/// `ts → (value, revision)` of a derived series' latest versions.
fn revisions(points: &[Point]) -> BTreeMap<Ts, (f64, u64)> {
    latest_versions(points)
        .into_iter()
        .filter_map(|p| Some((p.ts, (*p.values.first()?, *p.values.get(1)? as u64))))
        .collect()
}

/// One point per timestamp: the last version (the store keeps equal timestamps in
/// arrival order, so the last is the latest correction). Timestamp order.
pub fn latest_versions(points: &[Point]) -> Vec<Point> {
    let mut latest: BTreeMap<Ts, &Point> = BTreeMap::new();
    for p in points {
        latest.insert(p.ts, p);
    }
    latest.into_values().cloned().collect()
}

/// A derived series as it was known at `known_at_ms`: per timestamp, the latest version
/// already known then.
pub fn as_of(points: &[Point], known_at_ms: u64) -> Vec<Point> {
    let known: Vec<Point> = points
        .iter()
        .filter(|p| p.values.get(2).is_some_and(|&k| k <= known_at_ms as f64))
        .cloned()
        .collect();
    latest_versions(&known)
}

#[cfg(test)]
#[path = "maintain_tests.rs"]
mod tests;
