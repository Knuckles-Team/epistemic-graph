//! X-1 (CONCEPT:EG-X1) — the multimodal evidence-graph spine + citation resolver.
//!
//! **What already existed before this module** (grepped, not reinvented — see the
//! X-1 task's own investigation step): `eg_modality::EvidenceSpan` is the located
//! multimodal locus type (`DocumentSpan`/`TableCellRange`/`ImageRegion`/`PageBox`/
//! `AudioSegment`/`VideoShot`/`CodeSymbol`/`TraceSpan`); `eg_alignment` is the
//! shared in-memory alignment graph + `EvidenceResolver` seam connecting a locus to
//! its source blob; `src/server/blob/cas_resolver.rs`'s `CasEvidenceResolver`
//! already resolves a locus to REAL bytes off the engine's blob CAS;
//! `eg_plan::KnowledgeSet` already carries a resolved `evidence_refs: Vec<EvidenceSpan>`
//! per row for the handful of modality value types with a real
//! `ModalityContract::evidence()` impl. **What did NOT exist**: any link from
//! THIS crate's own `:Claim`/`:Evidence` epistemic substrate (`BeliefGraph`,
//! `explain_belief`'s `JustificationGraph`) to a located locus at all —
//! `ProofNode` names only bare node ids, and `BeliefGraph::from_graph_view` never
//! looked at anything but `confidence`/bitemporal fields. This module closes
//! exactly that gap: it lets an `:Evidence` node (the SAME typed-node-by-
//! convention shape `crate` module docs describe) carry an OPTIONAL located
//! [`EvidenceSpan`] plus the `AssetOccurrence`/`Blob` identity chain it was
//! extracted from, and turns a claim's evidence neighbourhood into a resolved
//! CITATION LIST — the "here is exactly where in the source this came from"
//! answer an LLM response quoting the claim can present.
//!
//! ## The `SourceObject -> AssetOccurrence -> Blob` identity chain
//!
//! No new persistence, no new store — same posture as the rest of this crate.
//! `NODE_TYPE_SOURCE_OBJECT`/`NODE_TYPE_ASSET_OCCURRENCE`/`NODE_TYPE_BLOB` are
//! plain `type`-tag string conventions a real ingestion pipeline would stamp onto
//! its own ordinary nodes (mirroring the `"type": "Claim"`/`"type": "Evidence"`
//! convention `src/server/handlers/mining.rs::materialize_claim` already writes).
//! This crate does not require those nodes to exist or walk to them itself — an
//! `:Evidence` node's `occurrence_id`/`blob_ref` properties (read below) are plain
//! optional strings carried alongside the resolved locus, exactly like `about`/
//! `family`/`provenance` ride the existing `:Claim`/`:Evidence` convention. A
//! caller that DOES want the real bytes at a resolved locus composes this
//! module's output with the existing `eg_alignment::EvidenceResolver` seam (e.g.
//! `CasEvidenceResolver`) — reading REAL content stays out of scope here, exactly
//! as it is out of scope for `eg-alignment` itself (see that crate's own docs).
//!
//! ## The two resolution directions
//!
//! * [`evidence_citations`] — given a claim id, return every supporting/
//!   contradicting/attacking node (walked transitively over the SAME
//!   `BeliefGraph::in_edges` topology `explain_belief` walks) that carries a
//!   located locus, as a citation list.
//! * [`resolve_locus`] — given ONE evidence node id, return its own locus +
//!   identity chain directly (no claim walk).
//! * [`justification_citations`] — the same citation list, keyed off an
//!   already-built [`crate::JustificationGraph`] (so `explain_belief`'s proof tree
//!   and the citation list agree on which topology they explain).

use std::collections::{HashSet, VecDeque};

use eg_modality::EvidenceSpan;
use serde::{Deserialize, Serialize};

use crate::adapter::BeliefGraph;
use crate::model::{EdgeKind, JustificationGraph};

/// `type`-tag convention for the source-artifact identity node (e.g. an uploaded
/// PDF/image/audio/video file) an [`EvidenceSpan`] locus was ultimately extracted
/// from. Not read or required by this module — documented here purely as the
/// convention a real ingestion pipeline stamps its own nodes with.
pub const NODE_TYPE_SOURCE_OBJECT: &str = "SourceObject";
/// `type`-tag convention for one materialized occurrence of a `SourceObject`
/// (e.g. "page 3 of revision 2 of this PDF", "this audio file's 16kHz mono
/// rendition") — the identity an `:Evidence` node's `occurrence_id` property
/// (read by [`EvidenceLocusRecord::decode`]) names.
pub const NODE_TYPE_ASSET_OCCURRENCE: &str = "AssetOccurrence";
/// `type`-tag convention for the content-addressed blob/rendition an
/// `AssetOccurrence` resolves to — the SAME `blob_ref` property key
/// `src/server/blob/cas_resolver.rs`'s `CasEvidenceResolver` already reads off an
/// artifact node (this module's `blob_ref`, read off the `:Evidence` node instead,
/// is the SAME convention one layer up the chain).
pub const NODE_TYPE_BLOB: &str = "Blob";

/// One node's decoded multimodal-evidence properties (CONCEPT:EG-X1) — the
/// `evidence_span`/`occurrence_id`/`blob_ref` fields [`BeliefGraph::from_graph_view`]
/// now reads off the SAME per-node property blob as `confidence`/bitemporal,
/// alongside this crate's usual "honest absence, never fabricated" posture: a node
/// whose blob carried none of the three decodes to [`Self::default`].
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct EvidenceLocusRecord {
    /// The located locus (page+box, image region, audio/video interval, …), when
    /// the node's `evidence_span` property decoded as a KNOWN [`EvidenceSpan`]
    /// shape. `None` when absent OR when present but undecodable (never a
    /// fabricated guess at what it might have meant).
    pub locus: Option<EvidenceSpan>,
    /// The `AssetOccurrence` node id this locus was extracted from, when the
    /// node's `occurrence_id` string property was set.
    pub occurrence_id: Option<String>,
    /// The content-addressed `Blob`/rendition reference the occurrence resolves
    /// to, when the node's `blob_ref` string property was set.
    pub blob_ref: Option<String>,
}

impl EvidenceLocusRecord {
    /// Decode from the SAME `serde_json::Value` `BeliefGraph::from_graph_view`
    /// already parsed once per node for `confidence`/bitemporal — no second
    /// msgpack decode.
    pub(crate) fn decode(parsed: Option<&serde_json::Value>) -> Self {
        let Some(v) = parsed else {
            return Self::default();
        };
        let locus = v
            .get("evidence_span")
            .and_then(|s| serde_json::from_value::<EvidenceSpan>(s.clone()).ok());
        let occurrence_id = v
            .get("occurrence_id")
            .and_then(|s| s.as_str())
            .map(str::to_string);
        let blob_ref = v
            .get("blob_ref")
            .and_then(|s| s.as_str())
            .map(str::to_string);
        Self {
            locus,
            occurrence_id,
            blob_ref,
        }
    }

    /// `true` iff none of the three fields were present — this node carries no
    /// multimodal-evidence data at all (the common case for most `:Claim`/
    /// `:Evidence` nodes today, and for every ordinary structural node).
    pub fn is_empty(&self) -> bool {
        self.locus.is_none() && self.occurrence_id.is_none() && self.blob_ref.is_none()
    }
}

/// One resolved citation: a node bearing on a claim (support/contradiction/
/// attack), together with its located [`EvidenceSpan`] and the
/// `AssetOccurrence`/`Blob` identity chain it was extracted from, when present.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EvidenceCitation {
    /// The evidence-bearing node's id.
    pub evidence_id: String,
    /// How this node bears on the claim the citation list was resolved for
    /// (the `BeliefGraph::in_edges` edge kind on the step that reached it).
    pub kind: EdgeKind,
    /// The located locus, or `None` when the node carried none.
    pub locus: Option<EvidenceSpan>,
    /// The `AssetOccurrence` node id, when set.
    pub occurrence_id: Option<String>,
    /// The content-addressed `Blob`/rendition reference, when set.
    pub blob_ref: Option<String>,
}

impl EvidenceCitation {
    fn from_record(evidence_id: String, kind: EdgeKind, record: &EvidenceLocusRecord) -> Self {
        Self {
            evidence_id,
            kind,
            locus: record.locus.clone(),
            occurrence_id: record.occurrence_id.clone(),
            blob_ref: record.blob_ref.clone(),
        }
    }
}

/// Resolve `evidence_id`'s OWN locus + identity chain directly — no claim walk.
/// `None` when the id has no entry in `bg.evidence_loci` at all (e.g. it never
/// appeared in the `GraphView` this `BeliefGraph` was built from); `Some` with all
/// fields `None` when the node exists but carries no multimodal-evidence data.
pub fn resolve_locus(bg: &BeliefGraph, evidence_id: &str) -> Option<EvidenceLocusRecord> {
    bg.evidence_loci.get(evidence_id).cloned()
}

/// Resolve the fully-cited evidence for `claim_id`: every node transitively
/// reachable by walking `BeliefGraph::in_edges` backward from `claim_id` (the
/// SAME support/contradiction/attack topology [`crate::explain_belief`] walks to
/// build its proof tree, bounded by the same depth cap and cycle guard) that
/// carries a non-empty [`EvidenceLocusRecord`], returned as a flat citation list —
/// the "here is exactly where in the source this claim's evidence came from"
/// answer. Order is breadth-first from `claim_id`; a node with no located
/// evidence (the common case — most support/contradiction edges are between
/// ordinary claims, not located evidence) contributes nothing to the list, but IS
/// still walked through to reach evidence further back.
pub fn evidence_citations(bg: &BeliefGraph, claim_id: &str) -> Vec<EvidenceCitation> {
    let mut citations = Vec::new();
    let mut visited: HashSet<String> = HashSet::new();
    let mut queue: VecDeque<(String, usize)> = VecDeque::new();
    visited.insert(claim_id.to_string());
    queue.push_back((claim_id.to_string(), 0));

    while let Some((id, depth)) = queue.pop_front() {
        if depth >= crate::propagate::MAX_EXPLAIN_DEPTH {
            continue;
        }
        let Some(ins) = bg.in_edges.get(&id) else {
            continue;
        };
        for (src, kind) in ins {
            if !visited.insert(src.clone()) {
                continue; // already visited (or on this path) — cycle guard.
            }
            if let Some(record) = bg.evidence_loci.get(src) {
                if !record.is_empty() {
                    citations.push(EvidenceCitation::from_record(src.clone(), *kind, record));
                }
            }
            queue.push_back((src.clone(), depth + 1));
        }
    }

    citations
}

/// The same citation list as [`evidence_citations`], keyed off an already-built
/// [`JustificationGraph`] (from [`crate::explain_belief`]) rather than a bare
/// claim id — a caller who already ran `explain_belief` and holds its proof tree
/// doesn't need to separately know `bg.in_edges` is the shared substrate both
/// walk. Equivalent to `evidence_citations(bg, &jg.root.claim)`.
pub fn justification_citations(bg: &BeliefGraph, jg: &JustificationGraph) -> Vec<EvidenceCitation> {
    evidence_citations(bg, &jg.root.claim)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::EdgeKind;
    use crate::propagate::explain_belief;
    use crate::AuthorityPolicy;

    fn page_box(document_id: &str, page: u32) -> EvidenceSpan {
        EvidenceSpan::PageBox {
            document_id: document_id.to_string(),
            page,
            x: 10.0,
            y: 20.0,
            width: 100.0,
            height: 40.0,
        }
    }

    fn located(locus: EvidenceSpan, occurrence_id: &str, blob_ref: &str) -> EvidenceLocusRecord {
        EvidenceLocusRecord {
            locus: Some(locus),
            occurrence_id: Some(occurrence_id.to_string()),
            blob_ref: Some(blob_ref.to_string()),
        }
    }

    /// Decoding a node blob with no multimodal fields at all yields an empty
    /// record — never fabricated.
    #[test]
    fn decode_is_empty_when_the_blob_carries_none_of_the_three_fields() {
        let v = serde_json::json!({ "confidence": 0.9 });
        let record = EvidenceLocusRecord::decode(Some(&v));
        assert!(record.is_empty());
        assert_eq!(record, EvidenceLocusRecord::default());
    }

    /// The flagship X1 example: a node whose blob carries a real `PageBox`
    /// locus (`"page N, box (x,y,w,h)"`) plus its occurrence/blob identity
    /// decodes losslessly.
    #[test]
    fn decode_recovers_a_page_box_locus_and_identity_chain() {
        let v = serde_json::json!({
            "type": "Evidence",
            "confidence": 0.8,
            "evidence_span": {
                "PageBox": {
                    "document_id": "doc-1",
                    "page": 4,
                    "x": 12.0, "y": 34.0, "width": 200.0, "height": 50.0
                }
            },
            "occurrence_id": "occ-1",
            "blob_ref": "sha256:deadbeef",
        });
        let record = EvidenceLocusRecord::decode(Some(&v));
        assert_eq!(
            record.locus,
            Some(EvidenceSpan::PageBox {
                document_id: "doc-1".to_string(),
                page: 4,
                x: 12.0,
                y: 34.0,
                width: 200.0,
                height: 50.0,
            })
        );
        assert_eq!(record.occurrence_id.as_deref(), Some("occ-1"));
        assert_eq!(record.blob_ref.as_deref(), Some("sha256:deadbeef"));
        assert!(!record.is_empty());
    }

    /// `resolve_locus` is a direct, one-hop lookup — no claim/edge walk needed.
    #[test]
    fn resolve_locus_looks_up_a_single_node_directly() {
        let bg =
            BeliefGraph::from_parts([("evidence-1", 0.9)], Vec::<(&str, &str, EdgeKind)>::new())
                .with_evidence_locus(
                    "evidence-1",
                    located(page_box("doc-1", 4), "occ-1", "blob-1"),
                );

        let resolved = resolve_locus(&bg, "evidence-1").expect("node exists");
        assert_eq!(resolved.locus, Some(page_box("doc-1", 4)));
        assert_eq!(resolved.occurrence_id.as_deref(), Some("occ-1"));

        // A node that was never in the graph at all resolves to `None`, not an
        // empty-but-`Some` record.
        assert_eq!(resolve_locus(&bg, "ghost"), None);
    }

    /// The end-to-end vertical slice (CONCEPT:EG-X1's acceptance test): a direct
    /// `evidence --SUPPORTS--> claim` edge, where the evidence node carries a
    /// `PageBox` locus. `evidence_citations` returns exactly that citation.
    #[test]
    fn evidence_citations_resolves_a_direct_supporting_locus() {
        let bg = BeliefGraph::from_parts(
            [("claim-1", 0.5), ("evidence-1", 0.9)],
            [("evidence-1", "claim-1", EdgeKind::Supports)],
        )
        .with_evidence_locus(
            "evidence-1",
            located(page_box("doc-1", 4), "occ-1", "blob-1"),
        );

        let citations = evidence_citations(&bg, "claim-1");
        assert_eq!(citations.len(), 1);
        assert_eq!(citations[0].evidence_id, "evidence-1");
        assert_eq!(citations[0].kind, EdgeKind::Supports);
        assert_eq!(citations[0].locus, Some(page_box("doc-1", 4)));
        assert_eq!(citations[0].occurrence_id.as_deref(), Some("occ-1"));
        assert_eq!(citations[0].blob_ref.as_deref(), Some("blob-1"));
    }

    /// Multi-hop: `span --SUPPORTS--> intermediate --SUPPORTS--> claim`. The
    /// located span is several hops back from the claim being explained;
    /// `evidence_citations` still surfaces it (walks the FULL justification
    /// topology, not just direct premises).
    #[test]
    fn evidence_citations_walks_transitively_through_an_intermediate_claim() {
        let bg = BeliefGraph::from_parts(
            [("claim-1", 0.5), ("mid-1", 0.7), ("span-1", 0.9)],
            [
                ("mid-1", "claim-1", EdgeKind::Supports),
                ("span-1", "mid-1", EdgeKind::Supports),
            ],
        )
        .with_evidence_locus("span-1", located(page_box("doc-1", 7), "occ-2", "blob-2"));

        let citations = evidence_citations(&bg, "claim-1");
        assert_eq!(citations.len(), 1);
        assert_eq!(citations[0].evidence_id, "span-1");
        assert_eq!(citations[0].locus, Some(page_box("doc-1", 7)));
    }

    /// A node with no located evidence contributes nothing to the citation list
    /// — never a fabricated citation for a plain structural premise.
    #[test]
    fn evidence_citations_is_empty_when_no_premise_carries_a_locus() {
        let bg = BeliefGraph::from_parts(
            [("claim-1", 0.5), ("evidence-1", 0.9)],
            [("evidence-1", "claim-1", EdgeKind::Supports)],
        );
        assert!(evidence_citations(&bg, "claim-1").is_empty());
    }

    /// `justification_citations` agrees with `evidence_citations` over the SAME
    /// `BeliefGraph` (the shared substrate `explain_belief`'s proof tree and the
    /// citation list both walk) — wiring `explain_belief`/the justification graph
    /// to surface loci.
    #[test]
    fn justification_citations_matches_evidence_citations_for_the_same_claim() {
        let bg = BeliefGraph::from_parts(
            [("claim-1", 0.5), ("evidence-1", 0.9)],
            [("evidence-1", "claim-1", EdgeKind::Supports)],
        )
        .with_evidence_locus(
            "evidence-1",
            located(page_box("doc-1", 1), "occ-1", "blob-1"),
        );

        let jg = explain_belief(&bg, "claim-1", &AuthorityPolicy::default());
        assert_eq!(jg.root.claim, "claim-1");

        let via_jg = justification_citations(&bg, &jg);
        let via_claim = evidence_citations(&bg, "claim-1");
        assert_eq!(via_jg, via_claim);
        assert_eq!(via_jg.len(), 1);
        assert_eq!(via_jg[0].evidence_id, "evidence-1");
    }
}
