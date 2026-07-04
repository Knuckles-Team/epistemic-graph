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
//! bare ids) are an EXPLICIT later increment — see the crate docs. This increment
//! carries ids+scores only and re-materializes from the snapshot when an operator
//! needs a column (the FILTER leg decodes the blob itself).

use std::collections::HashSet;

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
        Self { rows }
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
        Self { rows }
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
        Self { rows }
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
        RowSet {
            rows: self
                .rows
                .iter()
                .filter(|r| keep.contains(r.id.as_str()))
                .cloned()
                .collect(),
        }
    }

    /// Truncate to the top-k (LIMIT). Order-respecting: after a RANK this is top-k by
    /// score; on an unranked set it is the first k discovered.
    pub fn limit(mut self, k: usize) -> RowSet {
        self.rows.truncate(k);
        self
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
    fn limit_truncates_in_order() {
        let rs = RowSet::from_ids(["a".into(), "b".into(), "c".into()]).limit(2);
        assert_eq!(rs.ids(), vec!["a", "b"]);
    }
}
