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
//! With the `epistemic` feature on, [`KnowledgeSet::from_rowset`] decodes the row's
//! single `evidence_locus` property as a complete [`EvidenceLocus`]. Its opaque
//! identity, subject, policy, derivation, and validated address were committed by
//! ingestion. Query execution never derives those fields from raw modality values;
//! an absent or malformed property yields an empty `evidence_refs` list.
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
//!
//! ## `contradiction_ids` / `proof_ids` / `transformation_ids` / `alternative_ids`
//! (L22/CONCEPT:EG-P3-1, closing the `KnowledgeBatch` reserved-column gap)
//!
//! `KnowledgeBatch`'s `transformation_ids`/`proof_ids`/`alternative_ids`/
//! `contradiction_ids` `List<Utf8>` columns used to be UNCONDITIONALLY empty —
//! `KnowledgeRow` had no per-row lineage/contradiction/alternative data to give them
//! (see `knowledge_batch.rs`'s "Known-lossy / reserved fields" section, which named
//! this exact follow-up). Under `epistemic`, `KnowledgeSet::from_rowset` now resolves
//! all four, still never fabricated:
//!
//!  * `contradiction_ids` — SYMMETRIC: unlike `source_refs` (which only reads
//!    `bg.in_edges`, i.e. the TARGET's perspective), a `CONTRADICTS`/`ATTACKS` edge
//!    bears on BOTH endpoints — if claim A contradicts claim B, that fact belongs on
//!    BOTH rows, not just B's. Resolved via [`AuxEdgeIndex`], a small second index
//!    built by ONE extra pass over `view.edge_properties` (mirrors
//!    `BeliefGraph::from_graph_view`'s own decode loop, reusing
//!    `eg_epistemic::classify_relationship`) that records the relation on EVERY
//!    endpoint, not just the target.
//!  * `proof_ids` — the transitive justification/premise chain for this row, via
//!    `eg_epistemic::explain_belief` over the SAME shared `belief_graph` (deduped,
//!    excluding the row's own id). A direct `Supports`/`Contradicts` premise is
//!    already in `source_refs`/`contradiction_ids`; `proof_ids` additionally carries
//!    the TRANSITIVE chain underneath (evidence for the row's evidence, and so on) —
//!    the "why, all the way down" a caller gets from `EXPLAIN BELIEF` today, now
//!    riding every row instead of a one-off explain call. Not memoized across rows
//!    (same v1 tradeoff as `resolve_evidence`/`resolve_provenance_and_policy` — a
//!    follow-up if profiling shows it hot).
//!  * `transformation_ids` — the row's OWN generating Activity/derivation, via a NEW
//!    `GENERATED_BY` edge convention (`claim --GENERATED_BY--> activity`, PROV-style
//!    "this entity was generated by that activity"): source = the derived claim,
//!    target = an `:Activity` node carrying the algo/model/code/env-version +
//!    input-snapshot lineage (CONCEPT:EG-P3-1's writeback-lineage tuple — see
//!    `src/server/handlers/mining.rs`'s `materialize_claim` and
//!    `eg-jobs::claim::commit_result_claim`, the two writers of this edge).
//!    `GENERATED_BY` is deliberately NOT one of `classify_relationship`'s whitelisted
//!    values, so — exactly like the mining handlers' plain `relation`-keyed
//!    structural edges — it is automatically epistemically NEUTRAL: `BeliefGraph`
//!    ignores it and it never bears on confidence propagation.
//!  * `alternative_ids` — SYMMETRIC, mirroring `contradiction_ids`: a NEW
//!    `ALTERNATIVE_TO` edge convention between two claims that are mutually-exclusive
//!    hypotheses about the same subject (e.g. two different forecast/classification
//!    outcomes for the same target). Also epistemically neutral (not in
//!    `classify_relationship`'s whitelist). No writeback path in this workstream yet
//!    emits `ALTERNATIVE_TO` edges (an honest, documented follow-up — see the
//!    struct-level ledger note); the read side is real and tested so a future
//!    producer needs no `KnowledgeSet` change to light this column up.
//!
//! All four stay `Vec::new()` (never fabricated) for a row with no such edge, exactly
//! like `source_refs`/`policy_labels`/`evidence_refs` above — and, under a plain
//! non-`epistemic` build, are `Vec::new()` unconditionally (the feature gate, not the
//! data, decides).

use std::collections::HashSet;
// Only the `epistemic`-gated `AuxEdgeIndex` below uses HashMap.
#[cfg(feature = "epistemic")]
use std::collections::HashMap;

use eg_core::graph::GraphView;
use eg_modality::EvidenceLocus;
use serde::{Deserialize, Serialize};

use crate::rowset::RowSet;

/// Decode the row's certified governed locus. There is one property and one shape:
/// ingestion must persist a complete `EvidenceLocus`; query execution never derives
/// opaque identities, policy, or lineage from raw modality fields.
#[cfg(feature = "epistemic")]
fn resolve_evidence(
    obj: &Option<serde_json::Map<String, serde_json::Value>>,
) -> Vec<EvidenceLocus> {
    let Some(o) = obj else {
        return Vec::new();
    };
    o.get("evidence_locus")
        .and_then(|value| serde_json::from_value::<EvidenceLocus>(value.clone()).ok())
        .filter(|locus| locus.validate().is_ok())
        .into_iter()
        .collect()
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

/// A second, narrower index over `view`'s OWN edges (L22/CONCEPT:EG-P3-1) — built
/// alongside `eg_epistemic::BeliefGraph` by ONE more pass over
/// `view.edge_properties`, for the three relations `BeliefGraph::in_edges` cannot
/// answer directly: `contradiction_ids`/`alternative_ids` need BOTH endpoints of a
/// symmetric relation (not just the target's incoming view), and
/// `transformation_ids` needs the SOURCE's outgoing `GENERATED_BY` targets (not
/// incoming at all). See the module docs' "contradiction_ids / proof_ids /
/// transformation_ids / alternative_ids" section for the full rationale.
#[cfg(feature = "epistemic")]
#[derive(Default)]
struct AuxEdgeIndex {
    /// SYMMETRIC: node id → ids of every OTHER node it contradicts/attacks or is
    /// contradicted/attacked by (both endpoints of a classified `Contradicts`/
    /// `Attacks` edge get the other one added).
    contradiction_ids: HashMap<String, Vec<String>>,
    /// source (a derived claim) → target (its generating `:Activity`) — one entry
    /// per outgoing `GENERATED_BY` edge; NOT symmetric (an Activity does not itself
    /// carry a `transformation_ids` back-reference).
    transformation_ids: HashMap<String, Vec<String>>,
    /// SYMMETRIC: node id → ids of every OTHER node it is an `ALTERNATIVE_TO`
    /// counterpart of.
    alternative_ids: HashMap<String, Vec<String>>,
}

#[cfg(feature = "epistemic")]
impl AuxEdgeIndex {
    /// Build once per [`KnowledgeSet::from_rowset`] call — the SAME single scan cost
    /// class as `eg_epistemic::BeliefGraph::from_graph_view`, just decoding a
    /// different projection of the same edge blobs.
    fn build(view: &GraphView) -> Self {
        use eg_epistemic::{classify_relationship, EdgeKind};

        let mut idx = AuxEdgeIndex::default();
        for ((source, target), blobs) in &view.edge_properties {
            for blob in blobs {
                let Ok(val) = eg_types::msgpack::decode_property_value(blob) else {
                    continue;
                };
                let Some(rel) = val.get("relationship").and_then(|r| r.as_str()) else {
                    continue;
                };
                if matches!(
                    classify_relationship(rel),
                    Some(EdgeKind::Contradicts | EdgeKind::Attacks)
                ) {
                    idx.contradiction_ids
                        .entry(source.clone())
                        .or_default()
                        .push(target.clone());
                    idx.contradiction_ids
                        .entry(target.clone())
                        .or_default()
                        .push(source.clone());
                    continue;
                }
                match rel.to_ascii_uppercase().as_str() {
                    "GENERATED_BY" => {
                        idx.transformation_ids
                            .entry(source.clone())
                            .or_default()
                            .push(target.clone());
                    }
                    "ALTERNATIVE_TO" => {
                        idx.alternative_ids
                            .entry(source.clone())
                            .or_default()
                            .push(target.clone());
                        idx.alternative_ids
                            .entry(target.clone())
                            .or_default()
                            .push(source.clone());
                    }
                    _ => {}
                }
            }
        }
        idx
    }

    /// Sorted, deduped lookup — a row can appear twice in a symmetric map (e.g. two
    /// distinct edges between the same pair) and this normalizes that away.
    fn get(map: &HashMap<String, Vec<String>>, id: &str) -> Vec<String> {
        let mut v = map.get(id).cloned().unwrap_or_default();
        v.sort_unstable();
        v.dedup();
        v
    }
}

/// The transitive justification/premise chain for `row_id` (L22/CONCEPT:EG-P3-1) —
/// every node id appearing anywhere under `row_id`'s `eg_epistemic::explain_belief`
/// proof tree, deduped, EXCLUDING `row_id` itself. `bg` is the SAME shared
/// `BeliefGraph` `resolve_provenance_and_policy` already reads. A row with no
/// classified incoming edge at all yields an empty tree (`explain_belief` renders it
/// as a single `Asserted` leaf with no premises) ⇒ `Vec::new()`, never fabricated.
#[cfg(feature = "epistemic")]
fn resolve_proof_ids(bg: &eg_epistemic::BeliefGraph, row_id: &str) -> Vec<String> {
    let policy = eg_epistemic::AuthorityPolicy::default();
    let justification = eg_epistemic::explain_belief(bg, row_id, &policy);

    let mut seen: HashSet<String> = HashSet::new();
    let mut out: Vec<String> = Vec::new();
    collect_premise_ids(&justification.root.premises, &mut seen, &mut out);
    out
}

#[cfg(feature = "epistemic")]
fn collect_premise_ids(
    nodes: &[eg_epistemic::ProofNode],
    seen: &mut HashSet<String>,
    out: &mut Vec<String>,
) {
    for n in nodes {
        if seen.insert(n.claim.clone()) {
            out.push(n.claim.clone());
        }
        collect_premise_ids(&n.premises, seen, out);
    }
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
/// of a `KnowledgeSet`. No resolution work happens under plain `query`; the
/// `epistemic` feature fills `Resolved` and performs the per-row lookups.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ProvenanceFrame {
    /// No provenance resolution ran — every row's `source_refs`/`evidence_refs`
    /// are empty. This is the only mode a plain-`query` build produces.
    #[default]
    None,
    /// The `epistemic` feature resolved provenance refs per row: `evidence_refs` (a
    /// row's complete governed `EvidenceLocus`, see `resolve_evidence`) and
    /// `source_refs` (the row's own incoming
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
    /// The node's canonical `node_type`, or `""` when it is absent / the blob did
    /// not decode. An ordinary `type` property is not structural metadata.
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
    /// Complete governed evidence for this row, decoded only from its certified
    /// `evidence_locus` property. Always empty without `epistemic`, or when no valid
    /// locus was committed.
    pub evidence_refs: Vec<EvidenceLocus>,
    /// `"epistemic:contested"` / `"epistemic:corroborated"` / `"epistemic:asserted"`
    /// (D24, closing the X1 residue) — derived from the SAME incoming evidence
    /// neighbourhood as `source_refs` when `epistemic` is on (see
    /// `resolve_provenance_and_policy`). Always `Vec::new()` without `epistemic`, and
    /// also empty (not fabricated) for a row with no classified incoming edge at all.
    pub policy_labels: Vec<String>,
    /// Ids of nodes this row CONTRADICTS/ATTACKS or is contradicted/attacked BY
    /// (L22/CONCEPT:EG-P3-1) — SYMMETRIC, unlike `source_refs` (see [`AuxEdgeIndex`]
    /// and the module docs). Always `Vec::new()` without `epistemic`, and also empty
    /// (not fabricated) for a row with no classified contradiction/attack edge at all.
    pub contradiction_ids: Vec<String>,
    /// The transitive justification/premise chain underneath this row's belief
    /// (L22/CONCEPT:EG-P3-1) — every id in its `eg_epistemic::explain_belief` proof
    /// tree, deduped, excluding the row's own id (see `resolve_proof_ids`). Always
    /// `Vec::new()` without `epistemic`, and also empty for a row with no evidence
    /// neighbourhood at all.
    pub proof_ids: Vec<String>,
    /// The id(s) of this row's own generating Activity/derivation — its outgoing
    /// `GENERATED_BY` edge targets (L22/CONCEPT:EG-P3-1, see the module docs' PROV
    /// convention). Always `Vec::new()` without `epistemic`, and also empty for a row
    /// that was not itself materialized as a derived claim.
    pub transformation_ids: Vec<String>,
    /// Ids of mutually-exclusive alternative-claim counterparts (L22/CONCEPT:EG-P3-1)
    /// — SYMMETRIC `ALTERNATIVE_TO` edges (see the module docs; no writeback path
    /// produces these yet, an honest documented follow-up). Always `Vec::new()`
    /// without `epistemic`, and also empty when no such edge exists.
    pub alternative_ids: Vec<String>,
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

        // L22/CONCEPT:EG-P3-1: the second, narrower index for contradiction_ids/
        // transformation_ids/alternative_ids — see `AuxEdgeIndex`'s docs for why
        // `belief_graph.in_edges` alone cannot answer these three.
        #[cfg(feature = "epistemic")]
        let aux_edges = AuxEdgeIndex::build(view);

        let rows = rs
            .rows()
            .iter()
            .map(|row| {
                let obj = view.node_row_object(&row.id);

                let kind = obj
                    .as_ref()
                    .and_then(|o| o.get("node_type"))
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

                // A query can only surface the complete governed locus committed by
                // ingestion; it never derives identity or policy from raw fields.
                #[cfg(feature = "epistemic")]
                let evidence_refs = resolve_evidence(&obj);
                #[cfg(not(feature = "epistemic"))]
                let evidence_refs: Vec<EvidenceLocus> = Vec::new();

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

                // L22/CONCEPT:EG-P3-1: the four previously-always-empty
                // `KnowledgeBatch` reserved columns, now resolved per row — see the
                // module docs and `AuxEdgeIndex`/`resolve_proof_ids`.
                #[cfg(feature = "epistemic")]
                let contradiction_ids = AuxEdgeIndex::get(&aux_edges.contradiction_ids, &row.id);
                #[cfg(feature = "epistemic")]
                let transformation_ids = AuxEdgeIndex::get(&aux_edges.transformation_ids, &row.id);
                #[cfg(feature = "epistemic")]
                let alternative_ids = AuxEdgeIndex::get(&aux_edges.alternative_ids, &row.id);
                #[cfg(feature = "epistemic")]
                let proof_ids = resolve_proof_ids(&belief_graph, &row.id);
                #[cfg(not(feature = "epistemic"))]
                let (contradiction_ids, transformation_ids, alternative_ids, proof_ids): (
                    Vec<String>,
                    Vec<String>,
                    Vec<String>,
                    Vec<String>,
                ) = (Vec::new(), Vec::new(), Vec::new(), Vec::new());

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
                    contradiction_ids,
                    proof_ids,
                    transformation_ids,
                    alternative_ids,
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
            // `epistemic` is on (mirrors `EvidenceBundle::resolved` in the
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
                "node_type": "Doc",
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
        // d2 has no bitemporal/confidence fields.
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
        assert_eq!(row.kind, "Doc");
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

    /// A complete governed locus is decoded without deriving identity from raw
    /// modality fields.
    #[cfg(feature = "epistemic")]
    #[test]
    fn epistemic_resolves_certified_evidence_locus() {
        let core = GraphCore::new();
        core.add_node(
            "sym1".into(),
            blob(json!({
                "node_type": "Symbol",
                "evidence_locus": {
                    "id": "eg:locus:0000000000000001",
                    "subject": {
                        "kind": "artifact",
                        "id": "eg:artifact:0000000000000002"
                    },
                    "address": {
                        "kind": "code_symbol",
                        "revision_ref": "eg:revision:0000000000000003",
                        "symbol_ref": "eg:symbol:0000000000000004",
                        "start_line": 42,
                        "end_line": 88
                    },
                    "policy_ref": "eg:policy:0000000000000005",
                    "derivation_ref": "eg:derivation:0000000000000006"
                }
            })),
        );
        let view = core.analysis_snapshot();
        let rs = RowSet::from_ids(["sym1".to_string()]);

        let ks = KnowledgeSet::from_rowset(&rs, &view, &[]);

        assert_eq!(ks.provenance_frame, ProvenanceFrame::Resolved);
        let row = &ks.rows[0];
        assert_eq!(row.kind, "Symbol");
        assert_eq!(row.evidence_refs.len(), 1);
        assert!(matches!(
            &row.evidence_refs[0].address,
            eg_modality::EvidenceAddress::CodeSymbol {
                start_line: 42,
                end_line: 88,
                ..
            }
        ));
    }

    /// Raw trace ids are not promoted into governed evidence identities.
    #[cfg(feature = "epistemic")]
    #[test]
    fn epistemic_does_not_derive_locus_from_raw_trace_fields() {
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
        assert!(row.evidence_refs.is_empty());
    }

    /// Raw image metadata is not promoted into a governed evidence identity.
    #[cfg(feature = "epistemic")]
    #[test]
    fn epistemic_does_not_derive_locus_from_raw_image_fields() {
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
        assert!(row.evidence_refs.is_empty());
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

    /// Raw audio metadata is not promoted into a governed evidence identity.
    #[cfg(feature = "epistemic")]
    #[test]
    fn epistemic_does_not_derive_locus_from_raw_audio_fields() {
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
        assert!(row.evidence_refs.is_empty());
    }

    /// Raw video metadata is not promoted into a governed evidence identity.
    #[cfg(feature = "epistemic")]
    #[test]
    fn epistemic_does_not_derive_locus_from_raw_video_fields() {
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
        assert!(row.evidence_refs.is_empty());
    }

    /// Raw document metadata is not promoted into a governed evidence identity.
    #[cfg(feature = "epistemic")]
    #[test]
    fn epistemic_does_not_derive_locus_from_raw_document_fields() {
        let core = GraphCore::new();
        core.add_node(
            "doc1".into(),
            blob(json!({
                "node_type": "Document",
                "blob_ref": "deadbeefcafefeed00000000000000000",
                "language": "en",
                "pages": [
                    {
                        "number": 1,
                        "blocks": [
                            {
                                "kind": "Paragraph",
                                "spans": [
                                    { "start": 0, "end": 42, "label": "intro" }
                                ]
                            }
                        ]
                    }
                ]
            })),
        );
        let view = core.analysis_snapshot();
        let rs = RowSet::from_ids(["doc1".to_string()]);

        let ks = KnowledgeSet::from_rowset(&rs, &view, &[]);

        assert_eq!(ks.provenance_frame, ProvenanceFrame::Resolved);
        let row = &ks.rows[0];
        assert_eq!(row.kind, "Document");
        assert!(row.evidence_refs.is_empty());
    }

    /// L20 (CONCEPT:EG-P1-3) — a `Document` row with NO page/block/span structure
    /// decodes fine but yields NO fabricated evidence (never a whole-document
    /// fallback span), mirroring the image-with-no-regions case above.
    #[cfg(feature = "epistemic")]
    #[test]
    fn epistemic_document_with_no_structure_yields_no_fabricated_evidence() {
        let core = GraphCore::new();
        core.add_node(
            "doc2".into(),
            blob(json!({
                "node_type": "Document",
                "blob_ref": "h",
            })),
        );
        let view = core.analysis_snapshot();
        let rs = RowSet::from_ids(["doc2".to_string()]);

        let ks = KnowledgeSet::from_rowset(&rs, &view, &[]);

        assert_eq!(ks.rows[0].kind, "Document");
        assert!(ks.rows[0].evidence_refs.is_empty());
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
            blob(json!({ "relationship": "SUPPORTS" })),
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
            blob(json!({ "relationship": "SUPPORTS" })),
        )
        .unwrap();
        core.add_edge(
            "evidence2".into(),
            "claim1".into(),
            blob(json!({ "relationship": "HAS_EVIDENCE" })),
        )
        .unwrap();
        let view = core.analysis_snapshot();
        let rs = RowSet::from_ids(["claim1".to_string()]);

        let ks = KnowledgeSet::from_rowset(&rs, &view, &[]);

        let row = &ks.rows[0];
        let mut refs = row.source_refs.clone();
        refs.sort();
        assert_eq!(refs, vec!["evidence1".to_string(), "evidence2".to_string()]);
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
            blob(json!({ "relationship": "SUPPORTS" })),
        )
        .unwrap();
        core.add_edge(
            "counter1".into(),
            "claim1".into(),
            blob(json!({ "relationship": "CONTRADICTS" })),
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
            blob(json!({ "relationship": "SUPPORTS" })),
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
            blob(json!({ "relationship": "SUPPORTS" })),
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

    // ── L22/CONCEPT:EG-P3-1 — the four previously-reserved `KnowledgeBatch` columns ──

    /// `transformation_ids`: a claim's outgoing `GENERATED_BY` edge surfaces its
    /// generating Activity id — the convention `materialize_claim`/
    /// `commit_result_claim` write (see their crates' docs).
    #[cfg(feature = "epistemic")]
    #[test]
    fn epistemic_resolves_transformation_ids_via_generated_by_edge() {
        let core = GraphCore::new();
        core.add_node(
            "claim1".into(),
            blob(json!({ "node_type": "Claim", "confidence": 0.7 })),
        );
        core.add_node(
            "activity1".into(),
            blob(json!({ "node_type": "Activity", "family": "mining.association" })),
        );
        core.add_edge(
            "claim1".into(),
            "activity1".into(),
            blob(json!({ "relationship": "GENERATED_BY" })),
        )
        .unwrap();
        let view = core.analysis_snapshot();
        let rs = RowSet::from_ids(["claim1".to_string()]);

        let ks = KnowledgeSet::from_rowset(&rs, &view, &[]);

        assert_eq!(ks.rows[0].transformation_ids, vec!["activity1".to_string()]);
        // The Activity itself carries NO `transformation_ids` back-reference (the
        // relation is not symmetric).
        let rs2 = RowSet::from_ids(["activity1".to_string()]);
        let ks2 = KnowledgeSet::from_rowset(&rs2, &view, &[]);
        assert!(ks2.rows[0].transformation_ids.is_empty());
    }

    /// `GENERATED_BY` is epistemically NEUTRAL — it must never show up as a
    /// `source_refs`/`policy_labels` supporter, since it is not in
    /// `classify_relationship`'s whitelist.
    #[cfg(feature = "epistemic")]
    #[test]
    fn generated_by_edge_never_pollutes_belief() {
        let core = GraphCore::new();
        core.add_node(
            "claim1".into(),
            blob(json!({ "node_type": "Claim", "confidence": 0.7 })),
        );
        core.add_node("activity1".into(), blob(json!({ "node_type": "Activity" })));
        core.add_edge(
            "claim1".into(),
            "activity1".into(),
            blob(json!({ "relationship": "GENERATED_BY" })),
        )
        .unwrap();
        let view = core.analysis_snapshot();
        let rs = RowSet::from_ids(["activity1".to_string()]);

        let ks = KnowledgeSet::from_rowset(&rs, &view, &[]);
        assert!(ks.rows[0].source_refs.is_empty());
        assert!(ks.rows[0].policy_labels.is_empty());
    }

    /// `contradiction_ids` is SYMMETRIC: a single directed `CONTRADICTS` edge
    /// surfaces on BOTH rows, not just the target's — the explicit test the task
    /// asked for ("a contradiction between two claims surfaces on both rows").
    #[cfg(feature = "epistemic")]
    #[test]
    fn contradiction_surfaces_on_both_rows() {
        let core = GraphCore::new();
        core.add_node(
            "claim1".into(),
            blob(json!({ "node_type": "Claim", "confidence": 0.6 })),
        );
        core.add_node(
            "claim2".into(),
            blob(json!({ "node_type": "Claim", "confidence": 0.4 })),
        );
        core.add_edge(
            "claim1".into(),
            "claim2".into(),
            blob(json!({ "relationship": "CONTRADICTS" })),
        )
        .unwrap();
        let view = core.analysis_snapshot();
        let rs = RowSet::from_ids(["claim1".to_string(), "claim2".to_string()]);

        let ks = KnowledgeSet::from_rowset(&rs, &view, &[]);

        let row1 = ks.rows.iter().find(|r| r.id == "claim1").unwrap();
        let row2 = ks.rows.iter().find(|r| r.id == "claim2").unwrap();
        assert_eq!(row1.contradiction_ids, vec!["claim2".to_string()]);
        assert_eq!(row2.contradiction_ids, vec!["claim1".to_string()]);
    }

    /// `proof_ids`: a TRANSITIVE support chain (evidence1 -> mid -> claim1) surfaces
    /// BOTH the direct supporter (`mid`, already in `source_refs`) AND the deeper
    /// premise (`evidence1`) in `claim1`'s `proof_ids` — the "why, all the way down"
    /// `resolve_proof_ids` adds beyond the one-hop `source_refs`.
    #[cfg(feature = "epistemic")]
    #[test]
    fn proof_ids_carry_the_transitive_justification_chain() {
        let core = GraphCore::new();
        core.add_node(
            "claim1".into(),
            blob(json!({ "node_type": "Claim", "confidence": 0.5 })),
        );
        core.add_node(
            "mid".into(),
            blob(json!({ "node_type": "Claim", "confidence": 0.6 })),
        );
        core.add_node(
            "evidence1".into(),
            blob(json!({ "node_type": "Evidence", "confidence": 0.9 })),
        );
        core.add_edge(
            "mid".into(),
            "claim1".into(),
            blob(json!({ "relationship": "SUPPORTS" })),
        )
        .unwrap();
        core.add_edge(
            "evidence1".into(),
            "mid".into(),
            blob(json!({ "relationship": "SUPPORTS" })),
        )
        .unwrap();
        let view = core.analysis_snapshot();
        let rs = RowSet::from_ids(["claim1".to_string()]);

        let ks = KnowledgeSet::from_rowset(&rs, &view, &[]);

        let row = &ks.rows[0];
        assert_eq!(row.source_refs, vec!["mid".to_string()]);
        let mut proof = row.proof_ids.clone();
        proof.sort();
        assert_eq!(proof, vec!["evidence1".to_string(), "mid".to_string()]);
    }

    /// A row with no evidence neighbourhood at all yields empty `proof_ids` — never
    /// fabricated.
    #[cfg(feature = "epistemic")]
    #[test]
    fn proof_ids_empty_for_a_row_with_no_evidence() {
        let view = fixture();
        let rs = RowSet::from_ids(["d1".to_string()]);

        let ks = KnowledgeSet::from_rowset(&rs, &view, &[]);
        assert!(ks.rows[0].proof_ids.is_empty());
    }

    /// `alternative_ids` — SYMMETRIC, mirroring `contradiction_ids`, over the
    /// `ALTERNATIVE_TO` convention (no current writeback path emits it yet; this
    /// proves the READ side independently so a future producer needs no
    /// `KnowledgeSet` change).
    #[cfg(feature = "epistemic")]
    #[test]
    fn alternative_ids_resolve_symmetrically() {
        let core = GraphCore::new();
        core.add_node(
            "claim1".into(),
            blob(json!({ "node_type": "Claim", "confidence": 0.5 })),
        );
        core.add_node(
            "claim2".into(),
            blob(json!({ "node_type": "Claim", "confidence": 0.5 })),
        );
        core.add_edge(
            "claim1".into(),
            "claim2".into(),
            blob(json!({ "relationship": "ALTERNATIVE_TO" })),
        )
        .unwrap();
        let view = core.analysis_snapshot();
        let rs = RowSet::from_ids(["claim1".to_string(), "claim2".to_string()]);

        let ks = KnowledgeSet::from_rowset(&rs, &view, &[]);

        let row1 = ks.rows.iter().find(|r| r.id == "claim1").unwrap();
        let row2 = ks.rows.iter().find(|r| r.id == "claim2").unwrap();
        assert_eq!(row1.alternative_ids, vec!["claim2".to_string()]);
        assert_eq!(row2.alternative_ids, vec!["claim1".to_string()]);
    }

    /// A plain non-`epistemic` build leaves all four new columns empty even when the
    /// underlying graph has the edges — the feature gate, not the data, decides
    /// (mirrors `non_epistemic_source_refs_and_policy_labels_stay_empty_even_with_edges`).
    #[test]
    fn non_epistemic_new_columns_stay_empty_even_with_edges() {
        let core = GraphCore::new();
        core.add_node(
            "claim1".into(),
            blob(json!({ "node_type": "Claim", "confidence": 0.5 })),
        );
        core.add_node(
            "claim2".into(),
            blob(json!({ "node_type": "Claim", "confidence": 0.5 })),
        );
        core.add_edge(
            "claim1".into(),
            "claim2".into(),
            blob(json!({ "relationship": "CONTRADICTS" })),
        )
        .unwrap();
        let view = core.analysis_snapshot();
        let rs = RowSet::from_ids(["claim1".to_string()]);

        // `ks` is only asserted-on in the non-epistemic branch below; with the
        // `epistemic` feature that block is cfg'd out, leaving it unused.
        #[cfg_attr(feature = "epistemic", allow(unused_variables))]
        let ks = KnowledgeSet::from_rowset(&rs, &view, &[]);
        #[cfg(not(feature = "epistemic"))]
        {
            assert!(ks.rows[0].contradiction_ids.is_empty());
            assert!(ks.rows[0].proof_ids.is_empty());
            assert!(ks.rows[0].transformation_ids.is_empty());
            assert!(ks.rows[0].alternative_ids.is_empty());
        }
    }
}
