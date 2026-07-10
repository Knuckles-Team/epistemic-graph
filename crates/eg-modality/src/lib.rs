//! # eg-modality — the `ModalityContract` seam (CONCEPT:E4)
//!
//! A tiny leaf crate — deps = `eg-types` ONLY, deliberately BELOW `eg-plan`/`eg-core`
//! in the workspace DAG (`eg-types -> eg-core -> eg-compute -> epistemic-graph`, per
//! `AGENTS.md`'s Module Structure section). Every existing modality leaf crate
//! (`eg-tensor`, `eg-geo`, `eg-tsdb`, `eg-rdf`, …) sits at or near the bottom of that
//! DAG too; if `eg-modality` depended on `eg-plan` (to reuse its REAL `RowSet`
//! type), every modality crate implementing `ModalityContract` would transitively
//! pull `eg-plan` -> `eg-core`, INVERTING the DAG for crates that must stay
//! Pi-lean/dependency-free. So this crate defines its OWN small, DAG-safe dup of the
//! shapes it needs (`RowSetShape`, `StagedWrite`) rather than re-exporting the real
//! ones — see `rowset.rs`/`txn.rs` module docs for the full rationale.
//!
//! ## What's here
//!
//! * [`ModalityContract`] — the trait (4 core methods + 4 default-empty methods;
//!   see `contract.rs` module docs for the v1-scoping rationale).
//! * [`RowSetShape`] — the DAG-safe `{id, score}` dup of `eg_plan::rowset::Row`.
//! * [`StagedWrite`]/[`WriteKind`] — the transaction-staging shape, plus
//!   [`encode_staged`]/[`decode_staged`] helpers.
//! * [`Provenance`] — a generalized derivation record, drafted against
//!   `eg-rdf::owl::Justification`.
//! * [`EvidenceSpan`] — the X1 (multimodal-evidence) seam: a located-evidence enum,
//!   default-unused, so E3's `KnowledgeSet::evidence_refs` can later carry it.
//! * [`ConformanceTestable`] + [`modality_conformance_tests!`] — the conformance
//!   harness: implement the trait once per pilot, invoke the macro once, get the
//!   full test battery (round-trip / provenance non-panic / txn-stage-rollback
//!   symmetry / cdc-topic-iff-declared) for free.
//!
//! ## Retrofit status (v1: pilot only, NOT all 19 crates)
//!
//! Implemented (behind each crate's own opt-in `contract` feature, default OFF):
//! `eg-tensor::Tensor`, `eg-geo::Geometry`.
//!
//! **Documented retrofit order for the rest** (see this crate's `README.md` for the
//! full rationale per step): `eg-tsdb`/`eg-stream` next (both already have a
//! staging-shaped concept — `StagedSeries`/the CEP window — that maps directly onto
//! `txn_stage`) -> `eg-rdf` (the reference non-trivial `provenance()`, mapping
//! `owl::Justification`) -> `eg-epistemic` (the reference "does everything"
//! implementation once the shape has proven itself on 4 real modalities) -> the
//! remaining leaf/mid-tier crates (`eg-ann`, `eg-text`, `eg-shacl`, `eg-shex`,
//! `eg-lake`, `eg-kvcache`, …), lowest-friction (pure-serde leaves) first.
//!
//! This crate itself does NOT retrofit anything — it only defines the seam. Adding
//! it to a crate's `Cargo.toml`/feature list and implementing the trait is each
//! pilot's own, separate, additive change (see `eg-tensor`/`eg-geo`'s `contract`
//! feature for the pattern to repeat).

mod contract;
mod evidence;
mod provenance;
mod rowset;
mod txn;

pub use contract::{ConformanceTestable, ModalityContract};
pub use evidence::EvidenceSpan;
pub use provenance::Provenance;
pub use rowset::RowSetShape;
pub use txn::{decode_staged, encode_staged, StagedWrite, WriteKind};

// Dogfood the harness on a minimal in-crate type so `cargo test -p eg-modality`
// exercises the trait + the whole `modality_conformance_tests!` battery directly
// (the pilots — eg-tensor/eg-geo — additionally prove it on real modality values).
// This also anchors the ONE modality (`SmokeValue`) that overrides EVERY method,
// so the default-vs-overridden split is compiled + tested here, not only in a pilot.
#[cfg(test)]
mod harness_selftest {
    use crate::{
        ConformanceTestable, EvidenceSpan, ModalityContract, Provenance, RowSetShape, StagedWrite,
    };
    use serde::{Deserialize, Serialize};

    /// A trivial modality value that overrides all 8 methods (4 core + 4 default) —
    /// the "does everything" shape the retrofit plan expects `eg-epistemic` to take,
    /// in miniature, purely to self-test the harness.
    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    struct SmokeValue {
        label: String,
        weight: f32,
    }

    impl ModalityContract for SmokeValue {
        fn storage_kind(&self) -> &'static str {
            "smoke"
        }
        fn to_rowset(&self, id: &str) -> RowSetShape {
            RowSetShape::scored(id, self.weight)
        }
        fn txn_stage(&self, id: &str) -> StagedWrite {
            StagedWrite::put(id, crate::encode_staged(self))
        }
        fn cdc_topic(&self) -> Option<&'static str> {
            Some("smoke.cdc")
        }
        fn provenance(&self, _id: &str) -> Option<Provenance> {
            Some(Provenance::asserted())
        }
        fn evidence(&self, _id: &str) -> Option<EvidenceSpan> {
            Some(EvidenceSpan::DocumentSpan {
                document_id: self.label.clone(),
                start: 0,
                end: 1,
            })
        }
        fn policy_labels(&self, _id: &str) -> Vec<String> {
            vec!["smoke:public".to_string()]
        }
        fn analytics_ops(&self) -> Vec<&'static str> {
            vec!["identity"]
        }
    }

    impl ConformanceTestable for SmokeValue {
        fn conformance_sample() -> Self {
            SmokeValue {
                label: "sample".to_string(),
                weight: 0.5,
            }
        }
    }

    // The generated battery (round-trip / rollback symmetry / provenance non-panic /
    // cdc-topic-iff-declared / …) — run against the override-everything value.
    crate::modality_conformance_tests!(SmokeValue);

    #[test]
    fn overrides_are_observed() {
        let v = SmokeValue::conformance_sample();
        assert_eq!(v.storage_kind(), "smoke");
        assert_eq!(v.cdc_topic(), Some("smoke.cdc"));
        assert!(v.provenance("x").is_some());
        assert!(v.evidence("x").is_some());
        assert_eq!(v.policy_labels("x"), vec!["smoke:public".to_string()]);
        assert_eq!(v.analytics_ops(), vec!["identity"]);
        assert_eq!(v.to_rowset("x").score, Some(0.5));
    }

    #[test]
    fn default_methods_are_empty_when_not_overridden() {
        // A second type that overrides ONLY the 4 core methods — proving the 4
        // default-empty methods truly default without any boilerplate.
        #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
        struct Bare;
        impl ModalityContract for Bare {
            fn storage_kind(&self) -> &'static str {
                "bare"
            }
            fn to_rowset(&self, id: &str) -> RowSetShape {
                RowSetShape::unranked(id)
            }
            fn txn_stage(&self, id: &str) -> StagedWrite {
                StagedWrite::delete(id)
            }
            fn cdc_topic(&self) -> Option<&'static str> {
                None
            }
        }
        let b = Bare;
        assert!(b.provenance("x").is_none());
        assert!(b.evidence("x").is_none());
        assert!(b.policy_labels("x").is_empty());
        assert!(b.analytics_ops().is_empty());
        // A Delete stages no payload.
        assert_eq!(b.txn_stage("x").kind, crate::WriteKind::Delete);
    }
}
