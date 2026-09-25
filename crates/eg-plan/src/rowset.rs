//! `RowSet` — the shared intermediate that flows between cross-modal operators.
//!
//! **The load-bearing design choice (CONCEPT:AU-KG.compute.vector).** Graph traversals,
//! relational filters and vector kNN all *produce and consume the same shape* — an
//! ordered set of node ids, each optionally carrying a score. So instead of three
//! incompatible result types (Arrow `RecordBatch` ↔ `Vec<NodeIndex>` ↔
//! `Vec<(id, f32)>`) every operator normalizes its output to a `RowSet`. That is
//! what makes the operators a *closed algebra*: the output of any op is a legal
//! input to any other op, so a plan is just `RowSet -> RowSet -> RowSet`.
//!
//! A `RowSet` is intentionally minimal — id + optional score, in order.
//! Order matters because RANK produces a meaningful order a downstream LIMIT must
//! respect; FILTER/TRAVERSE produce a *set* (discovery order, not semantically
//! meaningful until a RANK imposes one).
//!
//! Projected columns (carrying full property rows across the boundary instead of
//! bare ids) are an EXPLICIT later increment — see the crate docs. Graph rows carry
//! ids+scores only and re-materialize from the snapshot when an operator needs a column
//! (the FILTER leg decodes the blob itself).
//!
//! **Value channels (EH-521).** A series row (`TSSCAN`, `DERIVE`) also carries exact
//! `f64` values under names (`v0..vk`, a `DERIVE … AS name` alias) in a side table keyed
//! by row id — the `f32` score stays the canonical rank currency and is untouched. Ops
//! that keep their input rows (`limit`, `intersect_keep_order`, `DERIVE`) keep the
//! table; ops that rebuild rows from scratch start empty, and the UQL serve path has
//! already recorded the values it may `RETURN` by then.

use std::collections::{BTreeMap, HashSet};

/// Row id → channel name → exact value.
pub type ValueChannels = BTreeMap<String, BTreeMap<String, f64>>;

/// The row id of point `ts_ns` of `series` (EH-521): `series@ts`. Per-series ids keep two
/// series' points at one timestamp distinct (a bare-ts id let the first series win).
pub fn series_row_id(series: &str, ts_ns: i64) -> String {
    format!("{series}@{ts_ns}")
}

/// Split a series row id into `(series, ts)`. The ts is the text after the LAST `@`, so a
/// series name may itself contain `@`. A bare integer id — a `WINDOW` bucket start — is
/// `("", ts)`. `None` for anything else (a graph node id).
pub fn parse_series_row_id(id: &str) -> Option<(&str, i64)> {
    match id.rsplit_once('@') {
        Some((series, ts)) => ts.parse().ok().map(|ts| (series, ts)),
        None => id.parse().ok().map(|ts| ("", ts)),
    }
}

/// One row: a node id and an optional score (similarity, pagerank, etc.). When a
/// `RowSet` has not been ranked, `score` is `None` for every row.
#[derive(Clone, Debug, PartialEq)]
pub struct Row {
    pub id: String,
    pub score: Option<f32>,
}

/// An ordered set of rows. Deduplicated by id (a node appears at most once); the
/// retained occurrence is the FIRST inserted (or, after a rank, the ranked order).
///
/// This is the cross-modal currency: every operator returns a `RowSet`, so a plan
/// is just `RowSet -> RowSet -> RowSet`.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RowSet {
    rows: Vec<Row>,
    values: ValueChannels,
}

impl RowSet {
    pub fn new() -> Self {
        Self::default()
    }

    /// Build from ids with no scores (a FILTER / TRAVERSE result — an unranked set).
    pub fn from_ids<I: IntoIterator<Item = String>>(ids: I) -> Self {
        let mut seen = HashSet::new();
        let rows = ids
            .into_iter()
            .filter(|id| seen.insert(id.clone()))
            .map(|id| Row { id, score: None })
            .collect();
        Self::of_rows(rows)
    }

    fn of_rows(rows: Vec<Row>) -> Self {
        Self {
            rows,
            values: ValueChannels::new(),
        }
    }

    /// Attach exact value channels (row id → name → value). Channels of ids not in the
    /// set are dropped, so the table never names a row the set does not hold.
    pub fn with_values(mut self, mut values: ValueChannels) -> Self {
        let ids = self.id_set();
        values.retain(|id, _| ids.contains(id.as_str()));
        self.values = values;
        self
    }

    /// Row `id`'s exact value under channel `name`.
    pub fn value(&self, id: &str, name: &str) -> Option<f64> {
        self.values.get(id).and_then(|m| m.get(name)).copied()
    }

    /// The whole value-channel table.
    pub fn values(&self) -> &ValueChannels {
        &self.values
    }

    /// Split into rows and the value table (an op that keeps its rows re-attaches it).
    pub fn into_parts(self) -> (Vec<Row>, ValueChannels) {
        (self.rows, self.values)
    }

    /// Build from scored ids (a RANK result — already in score order). Dedup keeps
    /// the first (highest-scoring, since the caller passes them ranked) occurrence.
    pub fn from_scored<I: IntoIterator<Item = (String, f32)>>(scored: I) -> Self {
        let mut seen = HashSet::new();
        let rows = scored
            .into_iter()
            .filter(|(id, _)| seen.insert(id.clone()))
            .map(|(id, s)| Row { id, score: Some(s) })
            .collect();
        Self::of_rows(rows)
    }

    /// Build from `(id, score?)` pairs preserving order, deduping by id (first wins).
    /// The inverse of reading `rows()` out — used by the WASM `Udf` op to rebuild the
    /// RowSet from a UDF's output rows (CONCEPT:EG-KG.query.rowset-execution).
    pub fn from_rows<I: IntoIterator<Item = (String, Option<f32>)>>(rows: I) -> Self {
        let mut seen = HashSet::new();
        let rows = rows
            .into_iter()
            .filter(|(id, _)| seen.insert(id.clone()))
            .map(|(id, score)| Row { id, score })
            .collect();
        Self::of_rows(rows)
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn rows(&self) -> &[Row] {
        &self.rows
    }

    /// The id set (membership), order-independent — used by TRAVERSE/RANK to know
    /// "which nodes are still candidates".
    pub fn id_set(&self) -> HashSet<&str> {
        self.rows.iter().map(|r| r.id.as_str()).collect()
    }

    pub fn ids(&self) -> Vec<String> {
        self.rows.iter().map(|r| r.id.clone()).collect()
    }

    /// Keep only rows whose id is in `keep` (the cross-modal AND: e.g. "vector-ranked
    /// rows that ALSO passed the relational filter"). Preserves *self*'s order, so a
    /// vector-first plan that later intersects the filter set stays in rank order.
    /// This is the predicate-pushdown-across-a-modality-boundary primitive.
    pub fn intersect_keep_order(&self, keep: &HashSet<&str>) -> RowSet {
        let rows = self
            .rows
            .iter()
            .filter(|r| keep.contains(r.id.as_str()))
            .cloned()
            .collect();
        RowSet::of_rows(rows).with_values(self.values.clone())
    }

    /// Truncate to the top-k (LIMIT). Order-respecting: after a RANK this is top-k by
    /// score; on an unranked set it is the first k discovered.
    pub fn limit(mut self, k: usize) -> RowSet {
        self.rows.truncate(k);
        let values = std::mem::take(&mut self.values);
        self.with_values(values)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dedup_keeps_first_occurrence() {
        let rs = RowSet::from_ids(["a".into(), "b".into(), "a".into()]);
        assert_eq!(rs.ids(), vec!["a", "b"]);
    }

    #[test]
    fn intersect_preserves_self_order() {
        let ranked = RowSet::from_scored([("b".into(), 0.9), ("a".into(), 0.5), ("c".into(), 0.1)]);
        let keep: HashSet<&str> = ["a", "b"].into_iter().collect();
        // Filter set is {a,b} but the RANK order (b,a) must be preserved.
        assert_eq!(ranked.intersect_keep_order(&keep).ids(), vec!["b", "a"]);
    }

    #[test]
    fn series_row_ids_round_trip() {
        assert_eq!(series_row_id("cpu@host", 7), "cpu@host@7");
        assert_eq!(parse_series_row_id("cpu@host@7"), Some(("cpu@host", 7)));
        assert_eq!(parse_series_row_id("-5"), Some(("", -5)));
        assert_eq!(parse_series_row_id("node-1"), None);
        assert_eq!(parse_series_row_id("a@b"), None);
    }

    #[test]
    fn value_channels_follow_kept_rows_only() {
        let values: ValueChannels = [("a", 1.5), ("b", 2.5), ("z", 9.0)]
            .into_iter()
            .map(|(id, v)| (id.to_string(), BTreeMap::from([("v0".to_string(), v)])))
            .collect();
        let rs = RowSet::from_ids(["a".into(), "b".into()]).with_values(values);
        assert_eq!(
            rs.value("z", "v0"),
            None,
            "a channel of an absent row is dropped"
        );
        let limited = rs.limit(1);
        assert_eq!(limited.value("a", "v0"), Some(1.5));
        assert_eq!(
            limited.value("b", "v0"),
            None,
            "LIMIT drops the cut row's channels"
        );
    }

    #[test]
    fn limit_truncates_in_order() {
        let rs = RowSet::from_ids(["a".into(), "b".into(), "c".into()]).limit(2);
        assert_eq!(rs.ids(), vec!["a", "b"]);
    }
}
