//! `ModalityContract` retrofit for [`NodeResult`] (CONCEPT:E4).
//!
//! Behind the crate's own opt-in `contract` feature (default OFF) — see
//! `eg-modality`'s README retrofit order (step 4: eg-shacl/eg-shex are validation
//! surfaces sitting above eg-rdf).
//!
//! [`NodeResult`] is this crate's own analogue of "which rule this result is
//! against" — its `shape` label IS the rule the node was tested against, parallel
//! in spirit to `eg-rdf::owl::ProofNode::rule` and `eg-shacl`'s
//! `constraint_component`. `provenance()` maps it onto `eg_modality::Provenance`
//! accordingly (see the method doc comment below for the full mapping).

use eg_modality::{
    encode_staged, ConformanceTestable, EvidenceSpan, ModalityContract, Provenance, RowSetShape,
    StagedWrite,
};

use crate::report::NodeResult;

impl ModalityContract for NodeResult {
    fn storage_kind(&self) -> &'static str {
        "shex"
    }

    /// A node result is a structural conformance fact, not an intrinsically ranked
    /// value — stays unranked, exactly like `eg-shacl::ValidationResult`.
    fn to_rowset(&self, id: &str) -> RowSetShape {
        RowSetShape::unranked(id)
    }

    fn txn_stage(&self, id: &str) -> StagedWrite {
        StagedWrite::put(id, encode_staged(self))
    }

    /// A node result is a query/validate-time artifact, not a live write path — not
    /// (yet) on the CDC/streaming surface.
    fn cdc_topic(&self) -> Option<&'static str> {
        None
    }

    /// A real, honest mapping from this crate's own data: `shape` (the shape label
    /// tested against) -> `source`; `node` (always) plus `reason` (when present)
    /// rendered as `"key:value"` lines -> `detail`; `conforms` -> `confidence`.
    ///
    /// Conformance is a boolean fact (the node either conforms to the shape or it
    /// doesn't), mapped onto the `[0, 1]` confidence scale at its two endpoints:
    /// `true` -> `1.0`, `false` -> `0.0` — there is no partial-conformance notion in
    /// ShEx Core, so the two-value mapping is exact, not lossy.
    ///
    /// Always `Some` — every `NodeResult` has a `node` and `shape` by construction
    /// (a result only exists because some node was tested against some shape), so
    /// there is never "nothing to report" here.
    fn provenance(&self, _id: &str) -> Option<Provenance> {
        let mut detail = vec![format!("node:{}", self.node)];
        if let Some(reason) = &self.reason {
            detail.push(format!("reason:{reason}"));
        }
        let confidence = if self.conforms { 1.0 } else { 0.0 };
        Some(Provenance {
            source: self.shape.clone(),
            detail,
            confidence,
        })
    }

    /// No located-evidence concept applies to a node result — default `None`.
    fn evidence(&self, _id: &str) -> Option<EvidenceSpan> {
        None
    }

    /// The crate's real top-level validation entry points that produce a
    /// `NodeResult`-bearing report (`crate::validate::{validate, validate_shexj}`).
    fn analytics_ops(&self) -> Vec<&'static str> {
        vec!["validate", "validate_shexj"]
    }
}

impl ConformanceTestable for NodeResult {
    fn conformance_sample() -> Self {
        NodeResult {
            node: "<http://example.org/alice>".to_string(),
            shape: "<http://example.org/PersonShape>".to_string(),
            conforms: false,
            reason: Some("missing required predicate".to_string()),
        }
    }
}

eg_modality::modality_conformance_tests!(NodeResult);

// A direct test of the `provenance()` mapping itself, beyond the generic
// "never panics" conformance check — the load-bearing proof that the shape ->
// source and conforms -> confidence mapping is real, not stub boilerplate, for
// BOTH a conforming and a non-conforming sample.
#[cfg(test)]
mod provenance_mapping {
    use super::*;

    #[test]
    fn shape_maps_to_source_and_nonconformance_to_zero_confidence() {
        let sample = NodeResult::conformance_sample();
        let prov = sample.provenance("x").expect("a NodeResult always has provenance");
        assert_eq!(prov.source, "<http://example.org/PersonShape>");
        assert_eq!(prov.confidence, 0.0);
        assert!(prov.detail.iter().any(|d| d.contains("<http://example.org/alice>")));
        assert!(prov.detail.iter().any(|d| d.contains("missing required predicate")));
    }

    #[test]
    fn conforms_maps_to_full_confidence() {
        let conforming = NodeResult {
            node: "<http://example.org/bob>".to_string(),
            shape: "<http://example.org/PersonShape>".to_string(),
            conforms: true,
            reason: None,
        };
        let prov = conforming.provenance("x").unwrap();
        assert_eq!(prov.confidence, 1.0);
        assert_eq!(prov.source, "<http://example.org/PersonShape>");
        assert!(prov.detail.iter().any(|d| d.contains("<http://example.org/bob>")));
        assert_eq!(prov.detail.len(), 1, "no reason present, so no reason: line");
    }
}
