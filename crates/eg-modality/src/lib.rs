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
//! * [`ArtifactBundle`] and its six stable identity tiers — the universal governed
//!   artifact/evidence protocol. Every durable identity is an [`OpaqueRef`], and a
//!   privacy attestation is required before certification.
//! * [`ServedModalityRuntime`] — the adapter-neutral governed state machine for
//!   idempotent ingest/update/delete, policy checks, paging, CDC, lifecycle, and
//!   restart recovery.
//! * [`GovernedModality`] — the mandatory, leaf-specific privacy and invariant check
//!   a typed payload must pass before the served state machine persists it.
//! * [`EvidenceLocus`] and [`EvidenceAddress`] — the sole governed located-evidence
//!   identity and its modality-neutral address.
//! * [`ConformanceTestable`] + [`modality_conformance_tests!`] — the conformance
//!   harness: implement the trait once per pilot, invoke the macro once, get the
//!   full test battery (round-trip / provenance non-panic / txn-stage-rollback
//!   symmetry / cdc-topic-iff-declared / …) for free.
//! * The modality registry (module `registry`) — [`register_modality`] /
//!   [`registered_modalities`]: the mandatory runtime inventory of every modality
//!   that has registered itself. `modality_conformance_tests!` now calls
//!   `register_modality` as part of ITS generated battery, so every existing and
//!   future `ModalityContract` implementer is wired in for free (EG-P1-1). See
//!   `registry.rs` module docs for the `linkme`/`inventory`-vs-explicit-call design
//!   decision.
//! * [`TckReport`]/[`TckPoint`]/[`TckStatus`] + [`tck_report`]/[`render_fleet_table`]
//!   (module `tck`) — the first-class 12-point Test Compatibility Kit (EG-P1-1):
//!   a machine-readable, per-point `Pass`/`NotApplicable`/`NotImplemented`
//!   capability report for any `ConformanceTestable` modality. The TCK is COMPLETE —
//!   every one of the 12 points maps to a real trait hook (no structural gaps), and
//!   nothing is ever silently green. Production certification is stricter than
//!   first-class compatibility: it requires 12 `Pass` results, no N/A, and a passing
//!   concrete native codec/index/query/resource probe.
//!   See `tck.rs` module docs for how each point is decided.
//! * [`IngestReport`]/[`StorageStats`]/[`ModalitySelfTest`] (module `capability`) —
//!   the DTOs for the four EG-P1-1 hooks that closed the TCK's last structural gaps
//!   (ingest, storage/stats, backup, single-node recovery).
//!
//! ## Retrofit status
//!
//! `ModalityContract` is implemented across most modality-shaped leaf crates:
//! `eg-tensor::Tensor`, `eg-geo::Geometry`, `eg-tsdb`, `eg-stream`, `eg-rdf`,
//! `eg-epistemic`, `eg-ann`, `eg-text`, `eg-shacl`, `eg-shex`, `eg-lake`,
//! `eg-kvcache`, `eg-numeric`, `eg-image`, `eg-audio`, `eg-video`, `eg-compute`,
//! `eg-core`. See this crate's `README.md` for the full per-crate rationale and any
//! remaining gaps. Document, image, audio, and video additionally expose governed
//! serving runtimes and are held to the production 12/12 rule. See this crate's
//! `README.md` and the architecture guide for the supported feature surface.

pub mod artifact;
mod capability;
mod contract;
pub mod delta_store;
mod native;
mod provenance;
mod registry;
pub mod rows;
mod rowset;
pub mod served;
mod tck;
mod txn;

pub use artifact::{
    Artifact, ArtifactBundle, ArtifactId, Classification, Derivation, DerivationId,
    EvidenceAddress, EvidenceLocus, EvidenceLocusId, Feature, FeatureId, FeatureKind, ModalityKind,
    Occurrence, OccurrenceId, OpaqueRef, PolicyEnvelope, PrivacyAttestation, ProtocolError,
    Rendition, RenditionId, ResourceId, Segment, SegmentId, SegmentKind, ARTIFACT_PROTOCOL_VERSION,
};
pub use capability::{IngestReport, ModalitySelfTest, NativeProductionProbe, StorageStats};
pub use contract::{ConformanceTestable, GovernedModality, ModalityContract};
pub use delta_store::{
    export_all, verify_round_trip, ExportedRows, IndexMembership, MutationDelta,
};
pub use native::{
    signature_bands, spatial_cells, temporal_buckets, NativeIndexKey, NativePredicate,
    NativePredicateError, NativeQueryStats, MAX_PERCEPTUAL_HASH_DISTANCE,
    MAX_TEMPORAL_QUERY_BUCKETS, SPATIAL_GRID_WIDTH, TEMPORAL_BUCKET_MS,
};
pub use provenance::Provenance;
pub use registry::{register_modality, registered_modalities, ModalityDescriptor};
pub use rows::{
    events_are_contiguous, ChunkManifestV1, ModalityEventV1, ModalityRow, ModalityRowOperation,
    RowKey, RowSchemaError, MODALITY_ARTIFACT, MODALITY_CHUNK_MANIFEST, MODALITY_EVENT,
    MODALITY_EVIDENCE_LOCUS, MODALITY_FEATURE, MODALITY_IDEMPOTENCY, MODALITY_INDEX_POSTING,
    MODALITY_OCCURRENCE, MODALITY_PROJECTION_CURSOR, MODALITY_RENDITION,
    MODALITY_ROW_SCHEMA_VERSION, MODALITY_ROW_TABLES, MODALITY_SEGMENT,
};
pub use rowset::RowSetShape;
pub use served::{
    ApplyDisposition, ApplyOutcome, IdempotencyEntry, LifecycleState, ServedDelete, ServedError,
    ServedEvent, ServedEventKind, ServedIngest, ServedModalityRuntime, ServedNativeQuery,
    ServedPage, ServedPolicyScope, ServedQuery, ServedRecord, ServedRuntimeStats,
};
pub use tck::{render_fleet_table, tck_report, TckPoint, TckPointResult, TckReport, TckStatus};
pub use txn::{decode_staged, encode_staged, StagedWrite, WriteKind};

// Dogfood the harness on a minimal in-crate type so `cargo test -p eg-modality`
// exercises the trait + the whole `modality_conformance_tests!` battery directly
// (the pilots — eg-tensor/eg-geo — additionally prove it on real modality values).
// This also anchors the ONE modality (`SmokeValue`) that overrides EVERY method,
// so the default-vs-overridden split is compiled + tested here, not only in a pilot.
#[cfg(test)]
mod harness_selftest {
    use crate::{
        ConformanceTestable, EvidenceAddress, IngestReport, ModalityContract, ModalitySelfTest,
        Provenance, RowSetShape, StagedWrite, StorageStats,
    };
    use serde::{Deserialize, Serialize};

    /// A trivial modality value that overrides EVERY method (4 core + 4 original
    /// default + the 4 EG-P1-1 hooks) — the "does everything" shape the retrofit plan
    /// expects `eg-epistemic` to take, in miniature. It is the in-crate proof, needing
    /// no dev-deps, that a modality CAN reach a fully-green 12/12 first-class TCK.
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
        fn evidence_address(&self) -> Option<EvidenceAddress> {
            Some(EvidenceAddress::CharacterRange { start: 0, end: 1 })
        }
        fn policy_labels(&self, _id: &str) -> Vec<String> {
            vec!["smoke:public".to_string()]
        }
        fn analytics_ops(&self) -> Vec<&'static str> {
            vec!["identity"]
        }

        // ── the 4 EG-P1-1 hooks, each a REAL round-trip (no bare `Passed` literal) ──

        fn ingest_report(&self, _id: &str) -> IngestReport {
            // Batch and singleton-stream ingest both re-decode the encoded value.
            let encoded = crate::encode_staged(self);
            let batch = match serde_json::from_slice::<SmokeValue>(&encoded) {
                Ok(rt) if &rt == self => ModalitySelfTest::Passed,
                _ => ModalitySelfTest::Failed,
            };
            let streaming = if [encoded].into_iter().all(|item| {
                matches!(
                    serde_json::from_slice::<SmokeValue>(&item),
                    Ok(ref round_trip) if round_trip == self
                )
            }) {
                ModalitySelfTest::Passed
            } else {
                ModalitySelfTest::Failed
            };
            IngestReport { batch, streaming }
        }
        fn storage_stats(&self, _id: &str) -> Option<StorageStats> {
            Some(StorageStats {
                logical_bytes: crate::encode_staged(self).len() as u64,
                element_count: 1,
                has_secondary_index: false,
            })
        }
        fn backup_selfcheck(&self, _id: &str) -> ModalitySelfTest {
            // Backup→restore through the durable codec.
            let backup = crate::encode_staged(self);
            match serde_json::from_slice::<SmokeValue>(&backup) {
                Ok(restored) if &restored == self => ModalitySelfTest::Passed,
                _ => ModalitySelfTest::Failed,
            }
        }
        fn recovery_selfcheck(&self, id: &str) -> ModalitySelfTest {
            // Crash-recovery: stage → serialize the staged write (WAL analog) →
            // "restart" → deserialize → decode payload → confirm equality.
            let staged = self.txn_stage(id);
            let wal = match serde_json::to_vec(&staged) {
                Ok(bytes) => bytes,
                Err(_) => return ModalitySelfTest::Failed,
            };
            let replayed: StagedWrite = match serde_json::from_slice(&wal) {
                Ok(s) => s,
                Err(_) => return ModalitySelfTest::Failed,
            };
            match crate::decode_staged::<SmokeValue>(&replayed) {
                Ok(recovered) if &recovered == self => ModalitySelfTest::Passed,
                _ => ModalitySelfTest::Failed,
            }
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
    // cdc-topic-iff-declared / TCK generation+registration / …) — run against the
    // override-everything value.
    crate::modality_conformance_tests!(SmokeValue);

    #[test]
    fn overrides_are_observed() {
        let v = SmokeValue::conformance_sample();
        assert_eq!(v.storage_kind(), "smoke");
        assert_eq!(v.cdc_topic(), Some("smoke.cdc"));
        assert!(v.provenance("x").is_some());
        assert!(v.evidence_address().is_some());
        assert_eq!(v.policy_labels("x"), vec!["smoke:public".to_string()]);
        assert_eq!(v.analytics_ops(), vec!["identity"]);
        assert_eq!(v.to_rowset("x").score, Some(0.5));
    }

    /// The in-crate proof that 12/12 first-class is reachable: `SmokeValue` overrides
    /// every hook, so its TCK report must be fully green (12 PASS, no N/A, no gap).
    #[test]
    fn smoke_value_is_fully_first_class_12_of_12() {
        let report = crate::tck_report::<SmokeValue>();
        assert!(
            report.is_first_class(),
            "SmokeValue overrides every hook — must be first-class: {}",
            report.summary()
        );
        assert_eq!(
            report.pass_count(),
            12,
            "SmokeValue must PASS all 12 points (no N/A needed): {}",
            report.summary()
        );
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
        assert!(b.evidence_address().is_none());
        assert!(b.policy_labels("x").is_empty());
        assert!(b.analytics_ops().is_empty());
        // A Delete stages no payload.
        assert_eq!(b.txn_stage("x").kind, crate::WriteKind::Delete);
    }
}
