//! # KnowledgeSet — RowSet v2, additive (CONCEPT:EG-KG.query.knowledge-set)
//!
//! [`RowSet`] is deliberately minimal — id + optional score, in order — and stays
//! that way (see `rowset.rs`'s module docs): it is the closed-algebra currency every
//! `Op` produces and consumes, and widening it would touch the hot execution loop.
//!
//! `KnowledgeSet` is the OPT-IN enriched shape a caller asks for AFTER a plan has
//! run: [`KnowledgeSet::from_rowset`] takes a finished `RowSet` plus the `GraphView`
//! snapshot it was computed over and re-materializes, per row, the fields a
//! knowledge-consuming caller wants (kind, confidence, bitemporal window, a
//! requested column projection) — the SAME re-materialize-from-the-snapshot pattern
//! the FILTER leg already uses (`exec::scan_label`/`row_json` decode the identical
//! `node_properties` blob via `rmp_serde -> serde_json::Value`). It sits ABOVE the
//! op loop exactly like [`crate::leanrag`] does: a library helper a caller composes
//! after `execute`/`execute_ops`, NOT a wire `Op`.
//!
//! ## What this does NOT do
//!
//!  * It does NOT change `RowSet` or `Op::execute` — both are byte-for-byte
//!    unchanged; `KnowledgeSet` is only built when a caller explicitly asks for the
//!    enriched shape (`from_rowset`), so every existing plan is unaffected.
//!  * It does NOT memoize across requests — a `GraphView` is already a single-request
//!    immutable snapshot (see its module docs), so v1 computes every field fresh on
//!    each `from_rowset` call. A per-view memo (mirroring `plan_stats_memo`) is a
//!    follow-up if profiling shows repeated `from_rowset` calls over the same view
//!    are hot.
//!  * `source_refs` / `evidence_refs` / `policy_labels` resolution is gated behind a
//!    (not-yet-added) `epistemic` feature; this tier always yields them empty so the
//!    module compiles under plain `query`. See the `#[cfg(feature = "epistemic")]`
//!    seam below.

use std::collections::HashSet;

use eg_core::graph::GraphView;

use crate::rowset::RowSet;

/// A lazy handle onto a row's stored property blob — the node id plus whether a
/// decodable payload was actually found for it. Deliberately does NOT carry the
/// decoded/cloned blob itself: a `KnowledgeRow` is built for every row in a
/// `RowSet`, so eagerly cloning `Arc<Vec<u8>>` payloads here would duplicate work
/// the caller may never need (the `projection` field already carries the columns
/// that WERE requested). A caller that needs the full raw object re-reads
/// `view.node_row_object(&payload_ref.node_id)` off the same snapshot.
#[derive(Clone, Debug, PartialEq)]
pub struct PayloadRef {
    pub node_id: String,
    pub has_payload: bool,
}

/// Which columns were requested for the projection, and which of those were
/// actually present on at least one decoded row. v1 keeps this minimal — just the
/// two name lists a caller checks "did I get what I asked for" against.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct ProjectionSchema {
    pub requested: Vec<String>,
    pub present: Vec<String>,
}

/// The resolution mode used to populate `source_refs`/`evidence_refs` on the rows
/// of a `KnowledgeSet`. Kept as a minimal marker enum for v1 — no resolution work
/// happens under plain `query`; the `epistemic` feature (a follow-up) fills
/// `Resolved` in and does the actual per-row lookups.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ProvenanceFrame {
    /// No provenance resolution ran — every row's `source_refs`/`evidence_refs`
    /// are empty. The only mode this tier (plain `query`) produces.
    #[default]
    None,
    /// The `epistemic` feature resolved provenance refs per row.
    Resolved,
}

/// The resolution mode used to populate `policy_labels` on the rows of a
/// `KnowledgeSet`. Mirrors [`ProvenanceFrame`]; kept minimal for v1.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum PolicyFrame {
    /// No policy-label resolution ran — every row's `policy_labels` is empty.
    #[default]
    None,
    /// The `epistemic` feature resolved policy labels per row.
    Resolved,
}

/// One enriched row of a [`KnowledgeSet`] — a `RowSet` `Row` (id + score) widened
/// with the fields re-materialized from the `GraphView` snapshot: kind, a lazy
/// payload handle, an optional column projection, the bitemporal window, belief
/// confidence, and (v1: always empty) provenance/policy ref lists.
#[derive(Clone, Debug, PartialEq)]
pub struct KnowledgeRow {
    pub id: String,
    /// The node's `node_type` (falling back to the legacy `type` key), or `""`
    /// when neither is present / the blob did not decode.
    pub kind: String,
    pub score: Option<f32>,
    pub payload_ref: Option<PayloadRef>,
    /// The requested-columns projection (a JSON object of just the `cols` that
    /// were present on this row), or `None` when no columns were requested.
    pub projection: Option<serde_json::Value>,
    /// `(valid_from, valid_until)` — the fact's validity window in the world.
    pub valid_time: (Option<u64>, Option<u64>),
    /// `(tx_from, tx_to)` — when the engine began/stopped believing this fact.
    pub tx_time: (Option<u64>, Option<u64>),
    /// Belief confidence (`NodeData::confidence`, default `1.0` when absent/undecodable).
    pub confidence: f64,
    pub source_refs: Vec<String>,
    pub evidence_refs: Vec<String>,
    pub policy_labels: Vec<String>,
}

/// RowSet v2 (additive): the enriched, ready-to-consume shape a caller builds
/// AFTER a plan finishes, when it wants more than bare ids+scores — a `kind`, a
/// column projection, the bitemporal window, confidence, and (with `epistemic`)
/// provenance/policy refs — without widening the hot `RowSet` currency the
/// algebra's operators pass between each other.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct KnowledgeSet {
    pub rows: Vec<KnowledgeRow>,
    pub schema: ProjectionSchema,
    pub provenance_frame: ProvenanceFrame,
    pub policy_frame: PolicyFrame,
}

impl KnowledgeSet {
    /// Build a `KnowledgeSet` from a finished `RowSet` plus the `GraphView` it was
    /// computed over. For each row, decode `view.node_properties[id]` (via
    /// [`GraphView::node_row_object`] — the SAME `rmp_serde -> serde_json::Value`
    /// decode the FILTER leg uses) and populate `kind`/`confidence`/the bitemporal
    /// window/`projection` (restricted to `cols`). `RowSet` itself is only read
    /// (`rs.rows()`), never mutated — this is purely additive.
    ///
    /// v1 computes every row fresh (no cross-request memoization — see the module
    /// docs) and leaves `source_refs`/`evidence_refs`/`policy_labels` empty
    /// (`ProvenanceFrame::None`/`PolicyFrame::None`); the `epistemic` feature is the
    /// seam that fills them in (see the `#[cfg(feature = "epistemic")]` note below).
    pub fn from_rowset(rs: &RowSet, view: &GraphView, cols: &[&str]) -> KnowledgeSet {
        let mut present: HashSet<String> = HashSet::new();
        let rows = rs
            .rows()
            .iter()
            .map(|row| {
                let obj = view.node_row_object(&row.id);

                let kind = obj
                    .as_ref()
                    .and_then(|o| o.get("node_type").or_else(|| o.get("type")))
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string();

                let confidence = obj
                    .as_ref()
                    .and_then(|o| o.get("confidence"))
                    .and_then(|v| v.as_f64())
                    .unwrap_or(1.0);

                let get_u64 = |key: &str| -> Option<u64> {
                    obj.as_ref()
                        .and_then(|o| o.get(key))
                        .and_then(|v| v.as_u64())
                };
                let valid_time = (get_u64("valid_from"), get_u64("valid_until"));
                let tx_time = (get_u64("tx_from"), get_u64("tx_to"));

                let projection = if cols.is_empty() {
                    None
                } else {
                    let mut proj = serde_json::Map::new();
                    if let Some(o) = obj.as_ref() {
                        for &col in cols {
                            if let Some(v) = o.get(col) {
                                proj.insert(col.to_string(), v.clone());
                                present.insert(col.to_string());
                            }
                        }
                    }
                    Some(serde_json::Value::Object(proj))
                };

                let payload_ref = Some(PayloadRef {
                    node_id: row.id.clone(),
                    has_payload: obj.is_some(),
                });

                KnowledgeRow {
                    id: row.id.clone(),
                    kind,
                    score: row.score,
                    payload_ref,
                    projection,
                    valid_time,
                    tx_time,
                    confidence,
                    // v1 (plain `query`): no epistemic resolution ran — always empty.
                    // TODO(epistemic): under `#[cfg(feature = "epistemic")]`, resolve
                    // per-row `PROVENANCE_OF`/`EVIDENCE_FOR` edges + policy-label
                    // properties off this SAME `view` snapshot and set
                    // `ProvenanceFrame::Resolved` / `PolicyFrame::Resolved` below.
                    source_refs: Vec::new(),
                    evidence_refs: Vec::new(),
                    policy_labels: Vec::new(),
                }
            })
            .collect();

        let mut present: Vec<String> = present.into_iter().collect();
        present.sort();

        KnowledgeSet {
            rows,
            schema: ProjectionSchema {
                requested: cols.iter().map(|c| c.to_string()).collect(),
                present,
            },
            // v1 (plain `query`): no epistemic resolution ran for either frame.
            provenance_frame: ProvenanceFrame::None,
            policy_frame: PolicyFrame::None,
        }
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use eg_core::graph::GraphCore;
    use serde_json::json;

    fn blob(v: serde_json::Value) -> Vec<u8> {
        rmp_serde::to_vec_named(&v).unwrap()
    }

    /// A tiny fixture: two `Doc` nodes (one with a full bitemporal/confidence
    /// payload, one bare-minimum) plus one undecodable/absent node id in the
    /// RowSet (simulates a stale id — `node_row_object` returns `None` for it).
    fn fixture() -> GraphView {
        let core = GraphCore::new();
        core.add_node(
            "d1".into(),
            blob(json!({
                "node_type": "Doc",
                "title": "First",
                "confidence": 0.75,
                "valid_from": 100,
                "valid_until": 200,
                "tx_from": 10,
                "tx_to": null,
            })),
        );
        core.add_node(
            "d2".into(),
            blob(json!({
                "type": "Doc", // legacy key — no node_type
            })),
        );
        core.analysis_snapshot()
    }

    #[test]
    fn projection_materializes_requested_fields() {
        let view = fixture();
        let rs = RowSet::from_scored([("d1".to_string(), 0.9_f32)]);

        let ks = KnowledgeSet::from_rowset(&rs, &view, &["title", "confidence"]);

        assert_eq!(ks.len(), 1);
        let row = &ks.rows[0];
        assert_eq!(row.id, "d1");
        assert_eq!(row.kind, "Doc");
        assert_eq!(row.score, Some(0.9));
        assert_eq!(
            row.projection,
            Some(json!({ "title": "First", "confidence": 0.75 }))
        );
        assert_eq!(row.confidence, 0.75);
        assert_eq!(row.valid_time, (Some(100), Some(200)));
        assert_eq!(row.tx_time, (Some(10), None));
        assert_eq!(ks.schema.requested, vec!["title", "confidence"]);
        assert_eq!(ks.schema.present, vec!["confidence", "title"]);
    }

    #[test]
    fn no_epistemic_yields_empty_provenance_and_policy_but_correct_kind_confidence_times() {
        let view = fixture();
        // d2 has no bitemporal/confidence fields and uses the legacy `type` key.
        let rs = RowSet::from_ids(["d2".to_string()]);

        let ks = KnowledgeSet::from_rowset(&rs, &view, &[]);

        assert_eq!(ks.provenance_frame, ProvenanceFrame::None);
        assert_eq!(ks.policy_frame, PolicyFrame::None);

        let row = &ks.rows[0];
        assert_eq!(row.kind, "Doc"); // fell back to legacy `type` key
        assert_eq!(row.confidence, 1.0); // NodeData's default_confidence
        assert_eq!(row.valid_time, (None, None));
        assert_eq!(row.tx_time, (None, None));
        assert!(row.source_refs.is_empty());
        assert!(row.evidence_refs.is_empty());
        assert!(row.policy_labels.is_empty());
        assert!(row.projection.is_none());
        assert_eq!(
            row.payload_ref,
            Some(PayloadRef {
                node_id: "d2".to_string(),
                has_payload: true,
            })
        );
    }

    #[test]
    fn missing_node_decodes_to_no_payload_and_empty_kind() {
        let view = fixture();
        // "ghost" was never added to the graph — node_row_object returns None.
        let rs = RowSet::from_ids(["ghost".to_string()]);

        let ks = KnowledgeSet::from_rowset(&rs, &view, &["title"]);

        let row = &ks.rows[0];
        assert_eq!(row.kind, "");
        assert_eq!(row.confidence, 1.0);
        assert_eq!(
            row.payload_ref,
            Some(PayloadRef {
                node_id: "ghost".to_string(),
                has_payload: false,
            })
        );
        assert_eq!(row.projection, Some(json!({})));
    }

    /// `KnowledgeSet::from_rowset` only READS the `RowSet` (`rows()`) — building
    /// one must leave the source `RowSet` byte-for-byte unchanged.
    #[test]
    fn rowset_is_untouched() {
        let view = fixture();
        let rs = RowSet::from_scored([("d1".to_string(), 0.5_f32), ("d2".to_string(), 0.1_f32)]);
        let before = rs.clone();

        let _ks = KnowledgeSet::from_rowset(&rs, &view, &["title"]);

        assert_eq!(rs, before);
        assert_eq!(rs.ids(), vec!["d1", "d2"]);
    }
}
