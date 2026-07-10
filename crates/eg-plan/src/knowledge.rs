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
//!  * (D24, closing the X1 residue) `source_refs` and `policy_labels` are now ALSO
//!    resolved under the `epistemic` feature, alongside `evidence_refs` (X1,
//!    CONCEPT:E4) — see `resolve_evidence` and `resolve_provenance_and_policy`
//!    below. A plain-`query` build (no `epistemic`) still yields all three empty,
//!    unchanged.
//!
//! ## `evidence_refs` (X1, CONCEPT:E4) — located, not just node ids
//!
//! With the `epistemic` feature on, [`KnowledgeSet::from_rowset`] additionally tries
//! to decode each row's OWN stored node properties (the SAME `obj` already decoded
//! for `kind`/`confidence`/the bitemporal window, above) as one of the modality
//! value types that have a REAL `eg_modality::ModalityContract::evidence()` resolver
//! — today: `eg_compute::ast::symbol::Symbol` (a row whose `kind == "Symbol"`),
//! `eg_tsdb::traces::Span` (`kind == "Span"`), and (CONCEPT:E4 follow-up)
//! `eg_image::ImageData` (`kind == "Image"`), `eg_audio::AudioData`
//! (`kind == "Audio"`), `eg_video::VideoData` (`kind == "Video"`) — and, on a
//! successful decode, calls `evidence()` and pushes the resulting LOCATED
//! `eg_modality::EvidenceSpan` (e.g. a `CodeSymbol{file_path, symbol, start_line,
//! end_line}`, a `TraceSpan{trace_id, span_id}`, an `ImageRegion{image_id, x, y,
//! width, height}`, an `AudioSegment{audio_id, start_ms, end_ms}`, or a
//! `VideoShot{video_id, start_ms, end_ms}`) into `evidence_refs`. A row whose `kind`
//! matches neither (or whose properties don't structurally decode as that type, or
//! whose modality value has no region/segment/shot to report — e.g. an `ImageData`
//! with an empty region index) gets an empty `evidence_refs` — never a fabricated
//! span. `provenance_frame` becomes `ProvenanceFrame::Resolved` whenever `epistemic`
//! is on, exactly mirroring how `ExplainProvenanceResult::resolved` reports
//! `cfg!(feature = "epistemic")` (see the facade's `explain_provenance`) — even
//! though `source_refs` itself is a separate, not-yet-wired-here dimension of that
//! same frame (see the module docs above).
//!
//! `TextHit` (eg-text) and `ProofNode` (eg-rdf) are deliberately NOT in this decode
//! list: a `TextHit`'s `{id, score}` shape is not a safe/distinguishing structural
//! signature (almost any scored row would false-positive-match it), and a
//! `ProofNode` is a query-time-derived value that is never itself a stored node
//! (see `eg-rdf/src/contract.rs`'s `evidence()` doc for why it stays `None` anyway).
//!
//! ## `source_refs` / `policy_labels` (D24, closing the X1 residue)
//!
//! With `epistemic` on, [`KnowledgeSet::from_rowset`] also builds ONE
//! `eg_epistemic::BeliefGraph::from_graph_view(view)` (the same blob-decode
//! `Op::EvidenceFor`/`Op::Contradicts` already run per-op — see
//! `eg-epistemic/src/adapter.rs`) and, per row, reads its incoming
//! `SUPPORTS`/`SUPPORTS_BELIEF`/`HAS_EVIDENCE`/`CORROBORATES`/`CONTRADICTS`/
//! `ATTACKS`-classified edges (`eg_epistemic::classify_relationship`) straight off
//! `bg.in_edges` (see `resolve_provenance_and_policy`):
//!
//!  * `source_refs` — the ids of the incoming edges classified
//!    `eg_epistemic::EdgeKind::Supports` — i.e. exactly the nodes
//!    `Op::EvidenceFor { claim_id: row.id }` would seed (the SAME resolution
//!    `explain_provenance`'s `source_refs` already runs today, just built once per
//!    `from_rowset` call and reused across every row instead of once per one-op plan).
//!  * `policy_labels` — a classification of that same incoming neighbourhood,
//!    mirroring the derivation `eg-epistemic`'s (`contract`-feature-gated,
//!    test/capability-discovery only) `ModalityContract::policy_labels` impl for
//!    `BeliefState` uses: `"epistemic:contested"` when any incoming
//!    `Contradicts`/`Attacks` edge exists, `"epistemic:corroborated"` when 2+
//!    `Supports` edges exist, `"epistemic:asserted"` when exactly one. Deliberately
//!    narrower than calling `eg_epistemic::propagate_confidence` +
//!    `BeliefState::policy_labels()` directly: that real impl labels EVERY node
//!    `"epistemic:asserted"` even with zero evidence edges (a legitimate "claim
//!    asserted with no counter-evidence" reading for something already known to be a
//!    claim) — `from_rowset` has no such prior and runs over EVERY row kind, so a row
//!    with NO classified incoming edge at all gets an empty `policy_labels`, never a
//!    fabricated tag on an ordinary non-epistemic row (same "never fabricate"
//!    discipline as `resolve_evidence`, above). The impl's `as_of:<axis>` label is
//!    also not emitted here — `BeliefGraph::from_graph_view`'s `as_of` is always
//!    `None` (this is not an `AS OF`-pinned resolution).
//!
//! A row whose kind/shape carries no classified evidence edge at all — the common
//! case for most stored data — gets empty `source_refs`/`policy_labels` either way;
//! only a node that genuinely sits in the support/contradiction/attack graph gets
//! non-empty values. `provenance_frame`/`policy_frame` become `Resolved` whenever
//! `epistemic` is on, regardless of whether any individual row's lists end up
//! non-empty (mirrors `evidence_refs`/`ProvenanceFrame::Resolved`, above).

use std::collections::HashSet;

use eg_core::graph::GraphView;
use eg_modality::EvidenceSpan;
use serde::{Deserialize, Serialize};

use crate::rowset::RowSet;

/// Try to decode `obj` (a row's own stored node properties) as one of the modality
/// value types that have a REAL `ModalityContract::evidence()` resolver, dispatched
/// by `kind` (the SAME `node_type`/`type` string [`KnowledgeSet::from_rowset`]
/// already derives) so an unrelated node shape can never accidentally structurally
/// match. `kind` matching neither known modality — or a `kind` match whose
/// properties don't ALSO decode as that modality's real Rust type (e.g. a legacy/
/// partial shape) — yields an empty `Vec`, never a fabricated span. See the module
/// docs above for why `TextHit`/`ProofNode` are not attempted here.
#[cfg(feature = "epistemic")]
fn resolve_evidence(
    obj: &Option<serde_json::Map<String, serde_json::Value>>,
    kind: &str,
    row_id: &str,
) -> Vec<EvidenceSpan> {
    use eg_modality::ModalityContract;

    let Some(o) = obj else {
        return Vec::new();
    };
    let value = serde_json::Value::Object(o.clone());
    match kind {
        "Symbol" => serde_json::from_value::<eg_compute::ast::symbol::Symbol>(value)
            .ok()
            .and_then(|sym| sym.evidence(row_id))
            .into_iter()
            .collect(),
        "Span" => serde_json::from_value::<eg_tsdb::traces::Span>(value)
            .ok()
            .and_then(|span| span.evidence(row_id))
            .into_iter()
            .collect(),
        // CONCEPT:E4 follow-up — image/audio/video evidence. Each decode ALSO calls
        // the value's REAL `evidence()`, which itself yields `None` (not a fabricated
        // span) when the modality value carries no region/segment/shot index — see
        // `eg_image`/`eg_audio`/`eg_video`'s `contract.rs` docs.
        "Image" => serde_json::from_value::<eg_image::ImageData>(value)
            .ok()
            .and_then(|img| img.evidence(row_id))
            .into_iter()
            .collect(),
        "Audio" => serde_json::from_value::<eg_audio::AudioData>(value)
            .ok()
            .and_then(|audio| audio.evidence(row_id))
            .into_iter()
            .collect(),
        "Video" => serde_json::from_value::<eg_video::VideoData>(value)
            .ok()
            .and_then(|video| video.evidence(row_id))
            .into_iter()
            .collect(),
        _ => Vec::new(),
    }
}

/// Resolve `(source_refs, policy_labels)` for one row id off a shared
/// [`eg_epistemic::BeliefGraph`] (D24, closing the X1 residue — see the module docs'
/// `source_refs`/`policy_labels` section for the derivation rationale). `bg` is built
/// ONCE per [`KnowledgeSet::from_rowset`] call and shared across every row — the same
/// `BeliefGraph::from_graph_view` blob-decode `Op::EvidenceFor`/`Op::Contradicts`
/// already run, just amortized instead of rebuilt per op.
///
/// A row with no classified incoming edge at all (`bg.in_edges.get(row_id)` is
/// `None`) returns `(Vec::new(), Vec::new())` — never fabricated.
#[cfg(feature = "epistemic")]
fn resolve_provenance_and_policy(
    bg: &eg_epistemic::BeliefGraph,
    row_id: &str,
) -> (Vec<String>, Vec<String>) {
    use eg_epistemic::EdgeKind;

    let Some(incoming) = bg.in_edges.get(row_id) else {
        return (Vec::new(), Vec::new());
    };

    // `source_refs`: the same incoming-`Supports` ids `Op::EvidenceFor` seeds from.
    let source_refs: Vec<String> = incoming
        .iter()
        .filter(|(_, k)| *k == EdgeKind::Supports)
        .map(|(src, _)| src.clone())
        .collect();

    // `policy_labels`: mirrors eg-epistemic's `BeliefState::policy_labels` contested/
    // corroborated/asserted classification (see the module docs for why this stays
    // gated on "has at least one classified edge" rather than always emitting a label).
    let contested = incoming
        .iter()
        .any(|(_, k)| matches!(k, EdgeKind::Contradicts | EdgeKind::Attacks));
    let policy_labels = if contested {
        vec!["epistemic:contested".to_string()]
    } else if source_refs.len() > 1 {
        vec!["epistemic:corroborated".to_string()]
    } else {
        vec!["epistemic:asserted".to_string()]
    };

    (source_refs, policy_labels)
}

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
/// `Serialize`/`Deserialize` derived so a [`crate::dag::PlanNode`]'s optional
/// `input_schema`/`output_schema` round-trips over the wire — needed for the X4
/// cross-shard EXCHANGE operator (`src/raft/exchange.rs`) to ship a whole `PlanDag`
/// branch to a remote Raft group.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
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
    /// The `epistemic` feature resolved provenance refs per row: `evidence_refs` (a
    /// row's decodable modality value's located `EvidenceSpan`, X1/CONCEPT:E4, see
    /// `resolve_evidence`) AND, as of D24, `source_refs` (the row's own incoming
    /// `Supports`-classified edges over the `eg_epistemic::BeliefGraph`, see
    /// `resolve_provenance_and_policy` and the module docs) — both per-row lookups
    /// run off the SAME `GraphView` snapshot, not a re-run plan.
    Resolved,
}

/// The resolution mode used to populate `policy_labels` on the rows of a
/// `KnowledgeSet`. Mirrors [`ProvenanceFrame`]; kept minimal for v1.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum PolicyFrame {
    /// No policy-label resolution ran — every row's `policy_labels` is empty.
    #[default]
    None,
    /// The `epistemic` feature resolved policy labels per row (D24, closing the X1
    /// residue): the contested/corroborated/asserted classification of the row's own
    /// incoming evidence neighbourhood over the `eg_epistemic::BeliefGraph` (see
    /// `resolve_provenance_and_policy` and the module docs).
    Resolved,
}

/// One enriched row of a [`KnowledgeSet`] — a `RowSet` `Row` (id + score) widened
/// with the fields re-materialized from the `GraphView` snapshot: kind, a lazy
/// payload handle, an optional column projection, the bitemporal window, belief
/// confidence, and provenance/policy ref lists (`evidence_refs` is X1-resolved;
/// `source_refs`/`policy_labels` are D24-resolved — both under `epistemic`, see the
/// module docs).
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
    /// The row's own incoming `Supports`-classified (`SUPPORTS`/`SUPPORTS_BELIEF`/
    /// `HAS_EVIDENCE`/`CORROBORATES`) edge sources (D24, closing the X1 residue) —
    /// exactly the ids `Op::EvidenceFor { claim_id: id }` would seed, resolved via
    /// the row's own `eg_epistemic::BeliefGraph` neighbourhood when `epistemic` is on
    /// (see `resolve_provenance_and_policy`). Always `Vec::new()` without `epistemic`,
    /// and also empty (not fabricated) for a row with no classified incoming edge.
    pub source_refs: Vec<String>,
    /// Located evidence for this row (X1, CONCEPT:E4) — e.g. a `CodeSymbol`'s exact
    /// file/line range or a `TraceSpan`'s trace/span id — resolved via the row's own
    /// modality `ModalityContract::evidence()` when `epistemic` is on and the row's
    /// stored shape decodes as a known modality value type (see `resolve_evidence`).
    /// Always `Vec::new()` without `epistemic` (byte-for-byte the v1 default), and
    /// also empty (not fabricated) for a row whose kind/shape isn't a known modality.
    pub evidence_refs: Vec<EvidenceSpan>,
    /// `"epistemic:contested"` / `"epistemic:corroborated"` / `"epistemic:asserted"`
    /// (D24, closing the X1 residue) — derived from the SAME incoming evidence
    /// neighbourhood as `source_refs` when `epistemic` is on (see
    /// `resolve_provenance_and_policy`). Always `Vec::new()` without `epistemic`, and
    /// also empty (not fabricated) for a row with no classified incoming edge at all.
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

        // D24 (closing the X1 residue): build ONE `BeliefGraph` off `view` — shared
        // across every row's `source_refs`/`policy_labels` lookup below, rather than
        // rebuilt per row (or per one-op plan, as `explain_provenance` does today).
        #[cfg(feature = "epistemic")]
        let belief_graph = eg_epistemic::BeliefGraph::from_graph_view(view);

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

                // X1 (CONCEPT:E4): under `epistemic`, try to decode this row's OWN
                // stored properties (`obj`, already fetched above) as a known
                // modality value type and call its REAL `evidence()`. Off, or when
                // `kind`/the shape doesn't match a known modality, this is the v1
                // empty default — never fabricated. See `resolve_evidence` + the
                // module docs for which modalities are covered and why.
                #[cfg(feature = "epistemic")]
                let evidence_refs = resolve_evidence(&obj, &kind, &row.id);
                #[cfg(not(feature = "epistemic"))]
                let evidence_refs: Vec<EvidenceSpan> = Vec::new();

                // D24 (closing the X1 residue): under `epistemic`, resolve this row's
                // own incoming evidence-classified edges off the SAME shared
                // `belief_graph` — never fabricated for a row with no classified
                // incoming edge. See `resolve_provenance_and_policy` + the module docs.
                #[cfg(feature = "epistemic")]
                let (source_refs, policy_labels) =
                    resolve_provenance_and_policy(&belief_graph, &row.id);
                #[cfg(not(feature = "epistemic"))]
                let (source_refs, policy_labels): (Vec<String>, Vec<String>) =
                    (Vec::new(), Vec::new());

                KnowledgeRow {
                    id: row.id.clone(),
                    kind,
                    score: row.score,
                    payload_ref,
                    projection,
                    valid_time,
                    tx_time,
                    confidence,
                    source_refs,
                    evidence_refs,
                    policy_labels,
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
            // `provenance_frame`/`policy_frame` reflect that `evidence_refs`/
            // `source_refs` and `policy_labels` respectively WERE resolved when
            // `epistemic` is on (mirrors `ExplainProvenanceResult::resolved` in the
            // facade, which reports the same `cfg!(feature = "epistemic")`).
            provenance_frame: if cfg!(feature = "epistemic") {
                ProvenanceFrame::Resolved
            } else {
                ProvenanceFrame::None
            },
            policy_frame: if cfg!(feature = "epistemic") {
                PolicyFrame::Resolved
            } else {
                PolicyFrame::None
            },
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

        // `provenance_frame`/`policy_frame` track whether `epistemic` resolution ran
        // AT ALL (X1/D24) — a `Doc` row with no evidence edges at all still gets no
        // `evidence_refs`/`source_refs`/`policy_labels` either way (asserted below),
        // since "Doc" is not a known modality kind and `d2` has no incoming edges.
        let expected_provenance = if cfg!(feature = "epistemic") {
            ProvenanceFrame::Resolved
        } else {
            ProvenanceFrame::None
        };
        let expected_policy = if cfg!(feature = "epistemic") {
            PolicyFrame::Resolved
        } else {
            PolicyFrame::None
        };
        assert_eq!(ks.provenance_frame, expected_provenance);
        assert_eq!(ks.policy_frame, expected_policy);

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

    /// X1 (CONCEPT:E4): a row whose stored node properties decode losslessly as an
    /// `eg_compute::ast::symbol::Symbol` (kind `"Symbol"`) gets a REAL, located
    /// `EvidenceSpan::CodeSymbol` — not just its node id.
    #[cfg(feature = "epistemic")]
    #[test]
    fn epistemic_resolves_code_symbol_evidence() {
        let core = GraphCore::new();
        core.add_node(
            "sym1".into(),
            blob(json!({
                "node_type": "Symbol",
                "id": "sym:abc123",
                "name": "handle_request",
                "qualified_name": "crate::server::handle_request",
                "symbol_type": "Function",
                "file_path": "src/server.rs",
                "line_start": 42,
                "line_end": 88,
                "column": 0,
                "ast_hash": "deadbeef",
                "dependencies": [],
                "documentation": "",
                "language": "rust",
                "is_exported": true,
                "annotations": [],
                "byte_start": 900,
                "byte_end": 1500,
            })),
        );
        let view = core.analysis_snapshot();
        let rs = RowSet::from_ids(["sym1".to_string()]);

        let ks = KnowledgeSet::from_rowset(&rs, &view, &[]);

        assert_eq!(ks.provenance_frame, ProvenanceFrame::Resolved);
        let row = &ks.rows[0];
        assert_eq!(row.kind, "Symbol");
        assert_eq!(
            row.evidence_refs,
            vec![EvidenceSpan::CodeSymbol {
                file_path: "src/server.rs".to_string(),
                symbol: "handle_request".to_string(),
                start_line: 42,
                end_line: 88,
            }]
        );
    }

    /// X1 (CONCEPT:E4): a row whose stored node properties decode losslessly as an
    /// `eg_tsdb::traces::Span` (kind `"Span"`) gets a REAL, located
    /// `EvidenceSpan::TraceSpan` — not just its node id.
    #[cfg(feature = "epistemic")]
    #[test]
    fn epistemic_resolves_trace_span_evidence() {
        let core = GraphCore::new();
        core.add_node(
            "span1".into(),
            blob(json!({
                "node_type": "Span",
                "trace_id": "trace-42",
                "span_id": "span-7",
                "parent_span_id": "",
                "service": "gateway",
                "operation": "GET /",
                "start_time": 1_700_000_000_000_000_000i64,
                "duration": 500_000,
                "status": "OK",
                "attributes": {},
                "events": [],
            })),
        );
        let view = core.analysis_snapshot();
        let rs = RowSet::from_ids(["span1".to_string()]);

        let ks = KnowledgeSet::from_rowset(&rs, &view, &[]);

        let row = &ks.rows[0];
        assert_eq!(row.kind, "Span");
        assert_eq!(
            row.evidence_refs,
            vec![EvidenceSpan::TraceSpan {
                trace_id: "trace-42".to_string(),
                span_id: "span-7".to_string(),
            }]
        );
    }

    /// CONCEPT:E4 follow-up — a row whose stored node properties decode losslessly as
    /// an `eg_image::ImageData` (kind `"Image"`) gets a REAL, located
    /// `EvidenceSpan::ImageRegion` from its first region — not just its node id.
    #[cfg(feature = "epistemic")]
    #[test]
    fn epistemic_resolves_image_region_evidence() {
        let core = GraphCore::new();
        core.add_node(
            "img1".into(),
            blob(json!({
                "node_type": "Image",
                "width": 800,
                "height": 600,
                "blob_ref": "abc123",
                "regions": [
                    { "label": "face", "x": 10.0, "y": 20.0, "width": 100.0, "height": 80.0 }
                ],
            })),
        );
        let view = core.analysis_snapshot();
        let rs = RowSet::from_ids(["img1".to_string()]);

        let ks = KnowledgeSet::from_rowset(&rs, &view, &[]);

        let row = &ks.rows[0];
        assert_eq!(row.kind, "Image");
        assert_eq!(
            row.evidence_refs,
            vec![EvidenceSpan::ImageRegion {
                image_id: "img1".to_string(),
                x: 10.0,
                y: 20.0,
                width: 100.0,
                height: 80.0,
            }]
        );
    }

    /// CONCEPT:E4 follow-up — an `ImageData` row with NO region index decodes fine
    /// but yields NO fabricated evidence (never a whole-image fallback span).
    #[cfg(feature = "epistemic")]
    #[test]
    fn epistemic_image_with_no_regions_yields_no_fabricated_evidence() {
        let core = GraphCore::new();
        core.add_node(
            "img2".into(),
            blob(json!({
                "node_type": "Image",
                "width": 10,
                "height": 10,
                "blob_ref": "h",
            })),
        );
        let view = core.analysis_snapshot();
        let rs = RowSet::from_ids(["img2".to_string()]);

        let ks = KnowledgeSet::from_rowset(&rs, &view, &[]);

        assert_eq!(ks.rows[0].kind, "Image");
        assert!(ks.rows[0].evidence_refs.is_empty());
    }

    /// CONCEPT:E4 follow-up — a row whose stored node properties decode losslessly as
    /// an `eg_audio::AudioData` (kind `"Audio"`) gets a REAL, located
    /// `EvidenceSpan::AudioSegment` from its first segment — not just its node id.
    #[cfg(feature = "epistemic")]
    #[test]
    fn epistemic_resolves_audio_segment_evidence() {
        let core = GraphCore::new();
        core.add_node(
            "audio1".into(),
            blob(json!({
                "node_type": "Audio",
                "sample_rate": 44_100,
                "duration_ms": 5000,
                "blob_ref": "x",
                "segments": [
                    { "label": "speaker-1", "start_ms": 0, "end_ms": 2500 }
                ],
            })),
        );
        let view = core.analysis_snapshot();
        let rs = RowSet::from_ids(["audio1".to_string()]);

        let ks = KnowledgeSet::from_rowset(&rs, &view, &[]);

        let row = &ks.rows[0];
        assert_eq!(row.kind, "Audio");
        assert_eq!(
            row.evidence_refs,
            vec![EvidenceSpan::AudioSegment {
                audio_id: "audio1".to_string(),
                start_ms: 0,
                end_ms: 2500,
            }]
        );
    }

    /// CONCEPT:E4 follow-up — a row whose stored node properties decode losslessly as
    /// an `eg_video::VideoData` (kind `"Video"`) gets a REAL, located
    /// `EvidenceSpan::VideoShot` from its first shot — not just its node id.
    #[cfg(feature = "epistemic")]
    #[test]
    fn epistemic_resolves_video_shot_evidence() {
        let core = GraphCore::new();
        core.add_node(
            "video1".into(),
            blob(json!({
                "node_type": "Video",
                "duration_ms": 9000,
                "blob_ref": "y",
                "shots": [
                    { "label": "scene-1", "start_ms": 0, "end_ms": 4000 }
                ],
            })),
        );
        let view = core.analysis_snapshot();
        let rs = RowSet::from_ids(["video1".to_string()]);

        let ks = KnowledgeSet::from_rowset(&rs, &view, &[]);

        let row = &ks.rows[0];
        assert_eq!(row.kind, "Video");
        assert_eq!(
            row.evidence_refs,
            vec![EvidenceSpan::VideoShot {
                video_id: "video1".to_string(),
                start_ms: 0,
                end_ms: 4000,
            }]
        );
    }

    /// X1 (CONCEPT:E4): `provenance_frame` reports `Resolved` under `epistemic`, but
    /// a "Doc" row (not a known modality kind/shape) still gets NO fabricated
    /// evidence — empty stays empty, only now it means "genuinely nothing to
    /// report" rather than "resolution didn't run".
    #[cfg(feature = "epistemic")]
    #[test]
    fn epistemic_unknown_kind_yields_no_fabricated_evidence() {
        let view = fixture();
        let rs = RowSet::from_ids(["d1".to_string()]);

        let ks = KnowledgeSet::from_rowset(&rs, &view, &[]);

        assert_eq!(ks.provenance_frame, ProvenanceFrame::Resolved);
        assert!(ks.rows[0].evidence_refs.is_empty());
    }

    /// D24 (closing the X1 residue): a claim with a SINGLE incoming `SUPPORTS` edge
    /// gets that source's id in `source_refs` and the `"epistemic:asserted"` label
    /// (mirrors `eg-epistemic`'s `BeliefState::policy_labels` classification for a
    /// single, uncontested supporter — see the module docs for why `from_rowset`
    /// only emits this when the row genuinely has a classified incoming edge).
    #[cfg(feature = "epistemic")]
    #[test]
    fn epistemic_resolves_source_refs_and_asserted_policy_label() {
        let core = GraphCore::new();
        core.add_node(
            "claim1".into(),
            blob(json!({ "node_type": "Claim", "confidence": 0.5 })),
        );
        core.add_node(
            "evidence1".into(),
            blob(json!({ "node_type": "Evidence", "confidence": 0.9 })),
        );
        core.add_edge(
            "evidence1".into(),
            "claim1".into(),
            blob(json!({ "relationship_type": "SUPPORTS" })),
        )
        .unwrap();
        let view = core.analysis_snapshot();
        let rs = RowSet::from_ids(["claim1".to_string()]);

        let ks = KnowledgeSet::from_rowset(&rs, &view, &[]);

        assert_eq!(ks.provenance_frame, ProvenanceFrame::Resolved);
        assert_eq!(ks.policy_frame, PolicyFrame::Resolved);
        let row = &ks.rows[0];
        assert_eq!(row.source_refs, vec!["evidence1".to_string()]);
        assert_eq!(row.policy_labels, vec!["epistemic:asserted".to_string()]);
    }

    /// D24: TWO incoming `SUPPORTS` edges (no contradiction/attack) yield BOTH
    /// source ids and the `"epistemic:corroborated"` label.
    #[cfg(feature = "epistemic")]
    #[test]
    fn epistemic_two_supporters_yield_corroborated_policy_label() {
        let core = GraphCore::new();
        core.add_node(
            "claim1".into(),
            blob(json!({ "node_type": "Claim", "confidence": 0.5 })),
        );
        core.add_node(
            "evidence1".into(),
            blob(json!({ "node_type": "Evidence", "confidence": 0.9 })),
        );
        core.add_node(
            "evidence2".into(),
            blob(json!({ "node_type": "Evidence", "confidence": 0.9 })),
        );
        core.add_edge(
            "evidence1".into(),
            "claim1".into(),
            blob(json!({ "relationship_type": "SUPPORTS" })),
        )
        .unwrap();
        core.add_edge(
            "evidence2".into(),
            "claim1".into(),
            blob(json!({ "relationship_type": "HAS_EVIDENCE" })),
        )
        .unwrap();
        let view = core.analysis_snapshot();
        let rs = RowSet::from_ids(["claim1".to_string()]);

        let ks = KnowledgeSet::from_rowset(&rs, &view, &[]);

        let row = &ks.rows[0];
        let mut refs = row.source_refs.clone();
        refs.sort();
        assert_eq!(
            refs,
            vec!["evidence1".to_string(), "evidence2".to_string()]
        );
        assert_eq!(
            row.policy_labels,
            vec!["epistemic:corroborated".to_string()]
        );
    }

    /// D24: an incoming `CONTRADICTS`/`ATTACKS` edge yields `"epistemic:contested"` —
    /// `source_refs` still only carries the `SUPPORTS`-classified id (the contradictor/
    /// attacker is evidence AGAINST the claim, not a source FOR it).
    #[cfg(feature = "epistemic")]
    #[test]
    fn epistemic_contradicted_claim_is_contested() {
        let core = GraphCore::new();
        core.add_node(
            "claim1".into(),
            blob(json!({ "node_type": "Claim", "confidence": 0.5 })),
        );
        core.add_node(
            "evidence1".into(),
            blob(json!({ "node_type": "Evidence", "confidence": 0.9 })),
        );
        core.add_node(
            "counter1".into(),
            blob(json!({ "node_type": "Evidence", "confidence": 0.9 })),
        );
        core.add_edge(
            "evidence1".into(),
            "claim1".into(),
            blob(json!({ "relationship_type": "SUPPORTS" })),
        )
        .unwrap();
        core.add_edge(
            "counter1".into(),
            "claim1".into(),
            blob(json!({ "relationship_type": "CONTRADICTS" })),
        )
        .unwrap();
        let view = core.analysis_snapshot();
        let rs = RowSet::from_ids(["claim1".to_string()]);

        let ks = KnowledgeSet::from_rowset(&rs, &view, &[]);

        let row = &ks.rows[0];
        assert_eq!(row.source_refs, vec!["evidence1".to_string()]);
        assert_eq!(row.policy_labels, vec!["epistemic:contested".to_string()]);
    }

    /// D24: `evidence1` itself has no INCOMING classified edge (it is the SOURCE of
    /// one, not the target) — `source_refs`/`policy_labels` stay empty for it, never
    /// fabricated, even though `provenance_frame`/`policy_frame` are `Resolved`.
    #[cfg(feature = "epistemic")]
    #[test]
    fn epistemic_row_with_no_incoming_edge_stays_empty_not_fabricated() {
        let core = GraphCore::new();
        core.add_node(
            "claim1".into(),
            blob(json!({ "node_type": "Claim", "confidence": 0.5 })),
        );
        core.add_node(
            "evidence1".into(),
            blob(json!({ "node_type": "Evidence", "confidence": 0.9 })),
        );
        core.add_edge(
            "evidence1".into(),
            "claim1".into(),
            blob(json!({ "relationship_type": "SUPPORTS" })),
        )
        .unwrap();
        let view = core.analysis_snapshot();
        let rs = RowSet::from_ids(["evidence1".to_string()]);

        let ks = KnowledgeSet::from_rowset(&rs, &view, &[]);

        assert_eq!(ks.provenance_frame, ProvenanceFrame::Resolved);
        assert_eq!(ks.policy_frame, PolicyFrame::Resolved);
        let row = &ks.rows[0];
        assert!(row.source_refs.is_empty());
        assert!(row.policy_labels.is_empty());
    }

    /// D24: a plain, non-epistemic build yields byte-identical empty
    /// `source_refs`/`policy_labels` + `None` frames even when the underlying graph
    /// DOES have provenance edges — the feature gate, not the data, decides.
    #[test]
    fn non_epistemic_source_refs_and_policy_labels_stay_empty_even_with_edges() {
        let core = GraphCore::new();
        core.add_node(
            "claim1".into(),
            blob(json!({ "node_type": "Claim", "confidence": 0.5 })),
        );
        core.add_node(
            "evidence1".into(),
            blob(json!({ "node_type": "Evidence", "confidence": 0.9 })),
        );
        core.add_edge(
            "evidence1".into(),
            "claim1".into(),
            blob(json!({ "relationship_type": "SUPPORTS" })),
        )
        .unwrap();
        let view = core.analysis_snapshot();
        let rs = RowSet::from_ids(["claim1".to_string()]);

        let ks = KnowledgeSet::from_rowset(&rs, &view, &[]);

        let expected_provenance = if cfg!(feature = "epistemic") {
            ProvenanceFrame::Resolved
        } else {
            ProvenanceFrame::None
        };
        let expected_policy = if cfg!(feature = "epistemic") {
            PolicyFrame::Resolved
        } else {
            PolicyFrame::None
        };
        assert_eq!(ks.provenance_frame, expected_provenance);
        assert_eq!(ks.policy_frame, expected_policy);
        #[cfg(not(feature = "epistemic"))]
        {
            assert!(ks.rows[0].source_refs.is_empty());
            assert!(ks.rows[0].policy_labels.is_empty());
        }
    }
}
