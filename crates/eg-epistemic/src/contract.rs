//! `ModalityContract` retrofit for [`BeliefState`] (CONCEPT:E4) — the reference
//! "does everything" implementation per the `eg-modality` README's retrofit order
//! (step 3): once the contract has been exercised on tensor/geo/tsdb/stream/rdf, this
//! is where a from-scratch modality implements it FORWARD, overriding every
//! default-empty hook that is genuinely meaningful here.
//!
//! [`BeliefState`] is the crate's central computed value — "what the engine believes
//! about one node, given its evidence neighbourhood" (`propagate::propagate_confidence`'s
//! return type). Three of the four default-empty methods have real content for it:
//!
//! * `provenance()` — real: renders the belief's own supporting/contradicting/attacking
//!   evidence ids plus its bitemporal pin into `Provenance`, exactly the shape
//!   `eg-rdf::owl::ProofNode::provenance()` renders an OWL justification.
//! * `policy_labels()` — real: a `"contested"`/`"corroborated"`/`"asserted"`
//!   classification derived from the belief's own evidence-kind counts, plus an
//!   `as_of:<axis>` label when the belief was pinned to a bitemporal instant.
//! * `analytics_ops()` — real: the crate's actual compute entry points.
//!
//! `evidence()` is DELIBERATELY left at its default (`None`) — not a stub, a genuine
//! "does not apply" per the trait's own module docs (override ONLY where meaningful).
//! `BeliefState` has no located-evidence concept: its `supporting`/`contradicting`/
//! `attacking` fields are GRAPH NODE IDS, not spans into a document/table/image/audio/
//! video/code/trace artifact — none of `EvidenceSpan`'s variants fit a bare node id
//! without inventing a meaningless new one. That is a legitimate outcome the crate
//! README explicitly anticipates ("a modality overrides ONLY the ones that are
//! meaningful for it"), not a gap in this retrofit.

use eg_modality::{
    decode_staged, encode_staged, ConformanceTestable, EvidenceSpan, IngestReport,
    ModalityContract, ModalitySelfTest, Provenance, RowSetShape, StagedWrite, StorageStats,
    TckPoint,
};

use crate::model::{JustRule, TimeAxis};
use crate::BeliefState;

impl ModalityContract for BeliefState {
    fn storage_kind(&self) -> &'static str {
        "epistemic"
    }

    /// The propagated posterior confidence IS a natural rank — like `eg-rdf`'s
    /// `ProofNode` (an OWL entailment's own confidence), a belief's own confidence is
    /// intrinsic to the value, not query-relative, so this is a real `Some` score.
    fn to_rowset(&self, id: &str) -> RowSetShape {
        RowSetShape::scored(id, self.confidence as f32)
    }

    fn txn_stage(&self, id: &str) -> StagedWrite {
        StagedWrite::put(id, encode_staged(self))
    }

    /// A `BeliefState` is a computed VIEW — per the crate's own module docs, "no new
    /// persistence... nothing here is a new stored struct" — so it is genuinely never
    /// on the CDC surface (distinct from `eg-rdf::ProofNode`'s "not yet wired"; this one
    /// architecturally never will be, short of an explicit "materialize belief" op that
    /// writes back to `NodeData.confidence` instead — a DIFFERENT modality's write).
    fn cdc_topic(&self) -> Option<&'static str> {
        None
    }

    /// Real, non-stub provenance: this belief's own evidence neighbourhood + bitemporal
    /// pin, rendered exactly as `eg-rdf::owl::ProofNode::provenance()` renders an OWL
    /// justification (the two are deliberately parallel per `crate::model::JustRule`'s
    /// doc comment). `source` names the dominant [`JustRule`] this state resulted from;
    /// `detail` lists each contributing evidence id by kind; `confidence` is the belief's
    /// own posterior.
    fn provenance(&self, _id: &str) -> Option<Provenance> {
        let source = if !self.attacking.is_empty() || !self.contradicting.is_empty() {
            JustRule::DerivedContradiction
        } else if !self.supporting.is_empty() {
            JustRule::DerivedSupport
        } else {
            JustRule::Asserted
        };
        let mut detail: Vec<String> = Vec::new();
        detail.extend(self.supporting.iter().map(|n| format!("supports:{n}")));
        detail.extend(
            self.contradicting
                .iter()
                .map(|n| format!("contradicts:{n}")),
        );
        detail.extend(self.attacking.iter().map(|n| format!("attacks:{n}")));
        if !self.supporting.is_empty()
            && (!self.contradicting.is_empty() || !self.attacking.is_empty())
        {
            detail.push("rule:bayesian_update".to_string());
        }
        Some(Provenance {
            source: format!("{source:?}"),
            detail,
            confidence: self.confidence,
        })
    }

    /// See module docs: genuinely `None` — `supporting`/`contradicting`/`attacking` are
    /// graph node ids, not a located span into a document/table/image/audio/video/code/
    /// trace artifact that any `EvidenceSpan` variant could carry losslessly.
    fn evidence(&self, _id: &str) -> Option<EvidenceSpan> {
        None
    }

    /// Real, non-stub policy labels derived from this belief's own evidence-kind
    /// counts + bitemporal pin — not a placeholder tag list.
    fn policy_labels(&self, _id: &str) -> Vec<String> {
        let mut labels = Vec::new();
        if !self.attacking.is_empty() || !self.contradicting.is_empty() {
            labels.push("epistemic:contested".to_string());
        } else if self.supporting.len() > 1 {
            labels.push("epistemic:corroborated".to_string());
        } else {
            labels.push("epistemic:asserted".to_string());
        }
        if let Some((axis, _ts)) = self.as_of {
            let axis_name = match axis {
                TimeAxis::Valid => "valid",
                TimeAxis::Transaction => "transaction",
            };
            labels.push(format!("as_of:{axis_name}"));
        }
        labels
    }

    /// The crate's real compute entry points, listed exactly like every other pilot
    /// lists its actual ops.
    fn analytics_ops(&self) -> Vec<&'static str> {
        vec!["propagate_confidence", "explain_belief"]
    }

    // ── EG-P1-1 hooks — real implementations over BeliefState's serialization and
    // txn staging. BeliefState is a computed view, not a persisted store value. ──

    /// Batch ingest = parse a `BeliefState` back from its serialized form. Streaming
    /// is N/A: a belief is a scalar computed value, not an append stream.
    fn ingest_report(&self, id: &str) -> IngestReport {
        let staged = self.txn_stage(id);
        let batch = match decode_staged::<BeliefState>(&staged) {
            Ok(rt) if rt == *self => ModalitySelfTest::Passed,
            _ => ModalitySelfTest::Failed,
        };
        IngestReport {
            batch,
            streaming: ModalitySelfTest::NotApplicable(
                "a belief state is a scalar computed value, not an append stream",
            ),
        }
    }

    /// Real storage stats from the serialized BeliefState: logical size from encoded
    /// length; a belief is a single unit so element count is 1. Belief states are not
    /// secondary-indexed, so `has_secondary_index` is `false`.
    fn storage_stats(&self, _id: &str) -> Option<StorageStats> {
        let logical_bytes = encode_staged(self).len() as u64;
        Some(StorageStats {
            logical_bytes,
            element_count: 1,
            has_secondary_index: false,
        })
    }

    /// N/A: BeliefState is a computed view per the crate module docs ("no new
    /// persistence... nothing here is a new stored struct"). There is no backup codec —
    /// durability is a concern of the engine's own persistence layer, not the belief value itself.
    fn backup_selfcheck(&self, _id: &str) -> ModalitySelfTest {
        ModalitySelfTest::NotApplicable(
            "a belief state is a computed view, not a persisted store value — durability is maintained by the engine's propagation layer",
        )
    }

    /// Simulated single-node crash-and-recover through the txn staging path.
    /// Stage the belief as an in-txn write; the staged payload IS the WAL record;
    /// on "restart" replay-decode it and confirm the recovered belief is intact.
    fn recovery_selfcheck(&self, id: &str) -> ModalitySelfTest {
        let staged: StagedWrite = self.txn_stage(id);
        match decode_staged::<BeliefState>(&staged) {
            Ok(recovered) if recovered == *self => ModalitySelfTest::Passed,
            _ => ModalitySelfTest::Failed,
        }
    }

    /// Belief states are computed views with no CDC, policy, or independent provenance:
    /// CDC would require a materialized belief store (a separate step); policy is at
    /// the graph-node layer; provenance is computed live from evidence, not stored.
    fn tck_not_applicable(&self, point: TckPoint) -> Option<&'static str> {
        match point {
            TckPoint::CdcDeleteRetentionGc => Some(
                "belief states are computed views, not persisted values — CDC would require explicit materialization of beliefs as a separate store operation",
            ),
            TckPoint::TenantRowRegionPolicy => Some(
                "no modality-intrinsic policy surface — tenant/row/region policy is enforced at the graph-node/eg-core::isolation layer that owns the belief",
            ),
            TckPoint::ProvenanceEvidenceLineage => Some(
                "provenance is computed live from evidence; a belief state itself has no independent derivation history to store",
            ),
            _ => None,
        }
    }
}

impl ConformanceTestable for BeliefState {
    fn conformance_sample() -> Self {
        BeliefState {
            node_id: "claim-1".to_string(),
            confidence: 0.82,
            supporting: vec!["evidence-1".to_string(), "evidence-2".to_string()],
            contradicting: Vec::new(),
            attacking: Vec::new(),
            as_of: Some((TimeAxis::Transaction, 1_700_000_000)),
        }
    }
}

eg_modality::modality_conformance_tests!(BeliefState);

// Direct tests of the real provenance()/policy_labels() mappings, beyond the generic
// "never panics" conformance check — mirrors eg-rdf's `provenance_mapping` module.
#[cfg(test)]
mod overrides {
    use super::*;

    #[test]
    fn corroborated_asserted_belief_maps_to_derived_support() {
        let b = BeliefState::conformance_sample();
        let prov = b
            .provenance("x")
            .expect("a belief with evidence has provenance");
        assert_eq!(prov.source, format!("{:?}", JustRule::DerivedSupport));
        assert_eq!(prov.confidence, 0.82);
        assert!(prov.detail.iter().any(|d| d == "supports:evidence-1"));
        assert!(prov.detail.iter().any(|d| d == "supports:evidence-2"));
        assert_eq!(
            b.policy_labels("x"),
            vec![
                "epistemic:corroborated".to_string(),
                "as_of:transaction".to_string()
            ]
        );
    }

    #[test]
    fn contested_belief_labels_contested_and_derived_contradiction() {
        let b = BeliefState {
            node_id: "claim-2".to_string(),
            confidence: 0.3,
            supporting: Vec::new(),
            contradicting: vec!["evidence-3".to_string()],
            attacking: Vec::new(),
            as_of: None,
        };
        let prov = b.provenance("x").unwrap();
        assert_eq!(prov.source, format!("{:?}", JustRule::DerivedContradiction));
        assert_eq!(
            b.policy_labels("x"),
            vec!["epistemic:contested".to_string()]
        );
    }

    #[test]
    fn asserted_leaf_belief_has_no_evidence_labels() {
        let b = BeliefState {
            node_id: "claim-3".to_string(),
            confidence: 1.0,
            supporting: Vec::new(),
            contradicting: Vec::new(),
            attacking: Vec::new(),
            as_of: None,
        };
        let prov = b.provenance("x").unwrap();
        assert_eq!(prov.source, format!("{:?}", JustRule::Asserted));
        assert!(prov.detail.is_empty());
        assert_eq!(b.policy_labels("x"), vec!["epistemic:asserted".to_string()]);
        assert!(b.evidence("x").is_none());
    }
}
