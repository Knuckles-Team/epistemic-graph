//! `ModalityContract` retrofit for [`ProofNode`] (CONCEPT:E4) — the reference
//! non-trivial `provenance()` per the `eg-modality` README's retrofit order (step 2:
//! `eg-rdf` is the ONE modality crate with real derivation history).
//!
//! [`ProofNode`] is [`Classification::explain`]'s reconstructed OWL proof-tree node —
//! built directly from [`Justification`]s recorded during EL⁺/RL saturation, so it IS
//! the crate's own derivation-history value (as opposed to a bare RDF term/quad, which
//! has no provenance of its own to report). `provenance()` maps it onto
//! `eg_modality::Provenance` exactly as `eg-modality::provenance` module docs
//! prescribe: `rule -> source`, `axioms` (+ a rendering of each premise's
//! `sub ⊑ sup`) -> `detail`, `confidence -> confidence`.

use eg_modality::{
    encode_staged, ConformanceTestable, EvidenceSpan, ModalityContract, Provenance, RowSetShape,
    StagedWrite,
};

use crate::owl::ProofNode;

impl ModalityContract for ProofNode {
    fn storage_kind(&self) -> &'static str {
        "rdf"
    }

    /// A proof node's own confidence (CONCEPT:EG-KG.ontology.concept-13) IS a natural
    /// rank — unlike tensor/geo (no query-relative score), an OWL entailment's
    /// confidence is intrinsic to the value itself, so this is a real `Some` score,
    /// not an unranked placeholder.
    fn to_rowset(&self, id: &str) -> RowSetShape {
        RowSetShape::scored(id, self.confidence as f32)
    }

    fn txn_stage(&self, id: &str) -> StagedWrite {
        StagedWrite::put(id, encode_staged(self))
    }

    /// A reconstructed proof tree is a query/explain-time artifact, not a live write
    /// path — not (yet) on the CDC/streaming surface.
    fn cdc_topic(&self) -> Option<&'static str> {
        None
    }

    /// The reference non-trivial `provenance()`: maps this node's own
    /// `rule`/`axioms`/`confidence` (which is EXACTLY a reconstructed
    /// [`crate::owl::Justification`] plus the subsumption it proves) onto
    /// `Provenance` losslessly at this level — `rule -> source`, `axioms` plus a
    /// rendered `sub ⊑ sup : premise-rule` line per premise -> `detail`,
    /// `confidence -> confidence`. A LEAF node (`is_leaf()`, rule `"asserted"`) still
    /// reports a `Provenance` — asserted facts have real (`1.0`, empty-premise)
    /// provenance too, not "nothing to report".
    fn provenance(&self, _id: &str) -> Option<Provenance> {
        let mut detail = self.axioms.clone();
        for premise in &self.premises {
            detail.push(format!(
                "{} \u{2291} {} : {}",
                premise.sub, premise.sup, premise.rule
            ));
        }
        Some(Provenance {
            source: self.rule.clone(),
            detail,
            confidence: self.confidence,
        })
    }

    /// X1 (multimodal-evidence): deliberately `None`, not a gap. A `ProofNode` is a
    /// reconstructed OWL proof-tree node — its "location" IS its `provenance()`
    /// (`rule`/`axioms`/`premises`, already mapped above), not a byte/line/pixel/time
    /// range inside a source artifact. There is no artifact to point INTO here (the
    /// entailment is derived, not observed at a location) — provenance-only is the
    /// correct, real answer for this modality, not an unresolved TODO.
    fn evidence(&self, _id: &str) -> Option<EvidenceSpan> {
        None
    }

    /// The crate's real reasoning-surface operations, listed exactly like
    /// `eg-tensor`/`eg-geo` list their real ops.
    fn analytics_ops(&self) -> Vec<&'static str> {
        vec!["classify", "explain", "entails_subclass", "subclass_confidence"]
    }
}

impl ConformanceTestable for ProofNode {
    fn conformance_sample() -> Self {
        ProofNode {
            sub: "HumanHeart".to_string(),
            sup: "HumanComponent".to_string(),
            rule: "EL-exists-lhs".to_string(),
            axioms: vec!["HumanHeart \u{2291} \u{2203}partOf.Body".to_string()],
            confidence: 0.9,
            premises: vec![ProofNode {
                sub: "Body".to_string(),
                sup: "HumanComponent".to_string(),
                rule: "asserted".to_string(),
                axioms: Vec::new(),
                confidence: 1.0,
                premises: Vec::new(),
            }],
        }
    }
}

eg_modality::modality_conformance_tests!(ProofNode);

// A direct test of the `provenance()` mapping itself, beyond the generic
// "never panics" conformance check — this is the load-bearing proof that the
// Justification -> Provenance mapping is lossless AT THIS LEVEL (rule/axioms/
// confidence), not just "doesn't crash".
#[cfg(test)]
mod provenance_mapping {
    use super::*;

    #[test]
    fn maps_rule_axioms_and_confidence_losslessly() {
        let node = ProofNode::conformance_sample();
        let prov = node.provenance("x").expect("a derived ProofNode has provenance");
        assert_eq!(prov.source, "EL-exists-lhs");
        assert_eq!(prov.confidence, 0.9);
        assert!(prov.detail.iter().any(|d| d.contains("partOf")));
        assert!(prov.detail.iter().any(|d| d.contains("Body") && d.contains("asserted")));
    }

    #[test]
    fn leaf_node_still_reports_provenance() {
        let leaf = ProofNode {
            sub: "A".to_string(),
            sup: "A".to_string(),
            rule: "asserted".to_string(),
            axioms: Vec::new(),
            confidence: 1.0,
            premises: Vec::new(),
        };
        let prov = leaf.provenance("x").expect("even an asserted leaf reports provenance");
        assert_eq!(prov.source, "asserted");
        assert_eq!(prov.confidence, 1.0);
        assert!(prov.detail.is_empty());
    }
}
