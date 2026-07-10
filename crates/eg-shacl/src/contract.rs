//! `ModalityContract` retrofit for [`ValidationResult`] (CONCEPT:E4).
//!
//! Behind the crate's own opt-in `contract` feature (default OFF) — see
//! `eg-modality`'s README retrofit order (step 4: eg-shacl/eg-shex are validation
//! surfaces sitting above eg-rdf).
//!
//! [`ValidationResult`] is this crate's own analogue of "which rule produced this
//! fact" — its `constraint_component` (the `sh:sourceConstraintComponent` IRI) IS the
//! rule that produced the violation, exactly as `eg-rdf::owl::ProofNode::rule` names
//! the rule that produced an entailment. `provenance()` maps it onto
//! `eg_modality::Provenance` accordingly (see the method doc comment below for the
//! full field-by-field mapping).

use eg_modality::{
    encode_staged, ConformanceTestable, EvidenceSpan, ModalityContract, Provenance, RowSetShape,
    StagedWrite,
};

use crate::report::{Severity, ValidationResult};

impl ModalityContract for ValidationResult {
    fn storage_kind(&self) -> &'static str {
        "shacl"
    }

    /// A validation result is a structural fact about a graph, not an intrinsically
    /// ranked value — stays unranked, exactly like `eg-geo::Geometry`.
    fn to_rowset(&self, id: &str) -> RowSetShape {
        RowSetShape::unranked(id)
    }

    fn txn_stage(&self, id: &str) -> StagedWrite {
        StagedWrite::put(id, encode_staged(self))
    }

    /// A validation result is a query/validate-time artifact, not a live write path —
    /// not (yet) on the CDC/streaming surface.
    fn cdc_topic(&self) -> Option<&'static str> {
        None
    }

    /// A real, honest mapping from this crate's own rule/axiom-shaped data:
    /// `constraint_component` (the `sh:sourceConstraintComponent` IRI — the SHACL
    /// constraint component that produced this violation) -> `source`; `focus_node`/
    /// `source_shape`/`path`/`value`/`message` (whichever are present) rendered as
    /// `"key:value"` lines -> `detail`; `severity` -> `confidence`.
    ///
    /// The confidence mapping is not a probability estimate — a `ValidationResult` is
    /// always a definite structural fact (the constraint either failed or it didn't).
    /// Instead `confidence` here reflects how strongly this severity level asserts
    /// non-conformance: a `Violation` is the SHACL-normative failure (`1.0`, it alone
    /// makes a report non-conforming per `ValidationReport::from_results`), a
    /// `Warning` is a softer, advisory signal (`0.7`), and `Info` is merely
    /// informational (`0.3`) — mirroring how `eg-rdf::ProofNode::provenance` uses
    /// `confidence` for "how strongly does this record assert its conclusion",
    /// not a statistical probability.
    ///
    /// Always `Some` — every `ValidationResult` has a `constraint_component` and
    /// `source_shape` by construction (SHACL always evaluates a result under some
    /// shape/constraint), so there is never "nothing to report" here.
    fn provenance(&self, _id: &str) -> Option<Provenance> {
        let mut detail = vec![
            format!("focus:{}", self.focus_node),
            format!("shape:{}", self.source_shape),
        ];
        if let Some(path) = &self.path {
            detail.push(format!("path:{path}"));
        }
        if let Some(value) = &self.value {
            detail.push(format!("value:{value}"));
        }
        if let Some(message) = &self.message {
            detail.push(format!("message:{message}"));
        }
        let confidence = match self.severity {
            Severity::Violation => 1.0,
            Severity::Warning => 0.7,
            Severity::Info => 0.3,
        };
        Some(Provenance {
            source: self.constraint_component.clone(),
            detail,
            confidence,
        })
    }

    /// No located-evidence concept applies to a validation result — default `None`.
    fn evidence(&self, _id: &str) -> Option<EvidenceSpan> {
        None
    }

    /// The crate's real top-level validation entry points that produce a
    /// `ValidationResult`-bearing report (`crate::validate::{validate, validate_turtle}`).
    fn analytics_ops(&self) -> Vec<&'static str> {
        vec!["validate", "validate_turtle"]
    }
}

impl ConformanceTestable for ValidationResult {
    fn conformance_sample() -> Self {
        ValidationResult {
            focus_node: "<http://example.org/subject>".to_string(),
            path: Some("<http://example.org/name>".to_string()),
            value: Some("<http://example.org/subject>".to_string()),
            source_shape: "<http://example.org/PersonShape>".to_string(),
            constraint_component: "<http://www.w3.org/ns/shacl#MinCountConstraintComponent>"
                .to_string(),
            message: Some("must have at least 1 value".to_string()),
            severity: Severity::Violation,
        }
    }
}

eg_modality::modality_conformance_tests!(ValidationResult);

// A direct test of the `provenance()` mapping itself, beyond the generic
// "never panics" conformance check — the load-bearing proof that the
// constraint_component -> source and severity -> confidence mapping is real, not
// stub boilerplate.
#[cfg(test)]
mod provenance_mapping {
    use super::*;

    #[test]
    fn constraint_component_maps_to_source() {
        let sample = ValidationResult::conformance_sample();
        let prov = sample.provenance("x").expect("a ValidationResult always has provenance");
        assert_eq!(
            prov.source,
            "<http://www.w3.org/ns/shacl#MinCountConstraintComponent>"
        );
    }

    #[test]
    fn violation_severity_is_full_confidence() {
        let sample = ValidationResult::conformance_sample();
        let prov = sample.provenance("x").unwrap();
        assert_eq!(prov.confidence, 1.0);
    }

    #[test]
    fn detail_contains_focus_node() {
        let sample = ValidationResult::conformance_sample();
        let prov = sample.provenance("x").unwrap();
        assert!(prov
            .detail
            .iter()
            .any(|d| d.contains("<http://example.org/subject>")));
    }

    #[test]
    fn warning_and_info_severity_scale_confidence() {
        let mut sample = ValidationResult::conformance_sample();
        sample.severity = Severity::Warning;
        assert_eq!(sample.provenance("x").unwrap().confidence, 0.7);
        sample.severity = Severity::Info;
        assert_eq!(sample.provenance("x").unwrap().confidence, 0.3);
    }
}
