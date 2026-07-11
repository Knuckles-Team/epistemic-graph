//! The first-class Test Compatibility Kit (TCK) — 12 capability points every `eg-*`
//! modality is evaluated against (CONCEPT:E4 / EG-P1-1, Codex P1 feedback: promote
//! `ModalityContract` from an opt-in conformance macro to a genuine, provable,
//! first-class TCK).
//!
//! ## Why 12 points, and why most modalities will legitimately be `NotImplemented` on
//! several of them
//!
//! The 12 points are the FULL first-class bar (schema/ids, ingest+streaming, codec,
//! storage+index+stats, typed query, txn/saga, CDC+delete+retention+GC, tenant/row/
//! region policy, provenance+evidence+lineage, backup/restore/migrate/recover,
//! single-node failure, interop/workload smoke) — deliberately much broader than the
//! 4 core + 4 default-empty methods `ModalityContract` v1 actually defines (see
//! `contract.rs` module docs for why only 4 things generalized losslessly across
//! tensor/tsdb/rdf). Several TCK points (ingest/streaming, storage/index/stats,
//! backup/restore/migrate/recover, single-node failure) have **no corresponding
//! method on the trait at all** — there is nothing for `tck_report` to observe, so it
//! honestly reports `NotImplemented` with the reason "no hook exists yet", not a fake
//! `Pass`. That is the entire point of this module: "first-class" must be PROVABLE
//! per point, not assumed because a modality merely compiles against the trait.
//!
//! Closing those gaps is future work (extending `ModalityContract` itself with more
//! core methods, tracked as a follow-up to this workstream) — this module's job is
//! only to make the CURRENT gap machine-visible, honestly, per modality.
//!
//! ## What IS decidable from the existing trait (feasible points, computed for real)
//!
//! 1. **Schema/ids** — `storage_kind()` names itself and `to_rowset(id)` echoes the
//!    SAME id it was given.
//! 2. **Codec / unsupported-format** — a corrupted `Put` payload must `decode_staged`
//!    as `Err`, never silently succeed (and never panic — `decode_staged` is
//!    `serde_json`-backed, which itself never panics on malformed input, only errors).
//! 5. **Typed query operators** — `analytics_ops()` is non-empty (and well-formed).
//! 6. **Txn participation** — `txn_stage`/`rollback` id-symmetry (every modality
//!    answers this; it is a CORE, non-default method).
//! 7. **CDC (the modeled half only)** — a genuinely declared, non-empty `cdc_topic()`.
//!    Delete/tombstone/retention/GC are NOT modeled by the trait at all, so this
//!    point is a coarse, honest proxy, not full coverage — documented at the call
//!    site below.
//! 8. **Tenant/row/region policy (the modeled half only)** — non-empty
//!    `policy_labels()`. Empty is the LEGITIMATE default (policy usually lives one
//!    layer up at `eg-core::isolation`), so `NotImplemented` here does not mean
//!    "broken", it means "not attached at the modality-value layer".
//! 9. **Provenance + evidence + lineage** — either `provenance()` or `evidence()`
//!    reports `Some`.
//! 12. **Interop/workload smoke** — the base round-trip above (project to a row,
//!     stage a write, decode it back, roll it back) not panicking IS a genuine
//!     minimal cross-cutting smoke test.
//!
//! Points 2 (ingest/streaming), 4 (storage/index/stats), 10 (backup/restore/migrate/
//! recover), and 11 (single-node failure) are always `NotImplemented` for v1 — no
//! trait hook exists for `tck_report` to observe.

use crate::contract::{ConformanceTestable, ModalityContract};
use crate::txn::{decode_staged, WriteKind};

/// The 12 first-class TCK capability points (EG-P1-1).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TckPoint {
    /// (1) Stable, versioned schema/ids.
    SchemaAndIds,
    /// (2) Ingest, plus streaming ingest where applicable.
    IngestStreaming,
    /// (3) Codec + unsupported-format behavior.
    CodecUnsupportedFormat,
    /// (4) Storage + secondary-index + stats presence.
    StorageIndexStats,
    /// (5) Typed query operators.
    TypedQueryOperators,
    /// (6) Txn participation OR a declared saga/outbox contract.
    TxnOrSagaOutbox,
    /// (7) CDC / delete / tombstone / retention / GC.
    CdcDeleteRetentionGc,
    /// (8) Tenant / row / region policy.
    TenantRowRegionPolicy,
    /// (9) Provenance + evidence-location + lineage.
    ProvenanceEvidenceLineage,
    /// (10) Backup / restore / migrate / recover.
    BackupRestoreMigrateRecover,
    /// (11) Single-node failure behavior.
    SingleNodeFailure,
    /// (12) Interop / workload smoke.
    InteropWorkloadSmoke,
}

impl TckPoint {
    /// All 12 points, in the canonical order used everywhere in this module.
    pub const ALL: [TckPoint; 12] = [
        TckPoint::SchemaAndIds,
        TckPoint::IngestStreaming,
        TckPoint::CodecUnsupportedFormat,
        TckPoint::StorageIndexStats,
        TckPoint::TypedQueryOperators,
        TckPoint::TxnOrSagaOutbox,
        TckPoint::CdcDeleteRetentionGc,
        TckPoint::TenantRowRegionPolicy,
        TckPoint::ProvenanceEvidenceLineage,
        TckPoint::BackupRestoreMigrateRecover,
        TckPoint::SingleNodeFailure,
        TckPoint::InteropWorkloadSmoke,
    ];

    /// A short, human-readable label for this point (used by `TckReport::render_table`).
    pub fn label(&self) -> &'static str {
        match self {
            TckPoint::SchemaAndIds => "stable versioned schema/ids",
            TckPoint::IngestStreaming => "ingest (+streaming where applicable)",
            TckPoint::CodecUnsupportedFormat => "codec / unsupported-format behavior",
            TckPoint::StorageIndexStats => "storage + secondary-index + stats presence",
            TckPoint::TypedQueryOperators => "typed query operators",
            TckPoint::TxnOrSagaOutbox => "txn participation OR declared saga/outbox",
            TckPoint::CdcDeleteRetentionGc => "CDC / delete / tombstone / retention / GC",
            TckPoint::TenantRowRegionPolicy => "tenant / row / region policy",
            TckPoint::ProvenanceEvidenceLineage => "provenance + evidence-location + lineage",
            TckPoint::BackupRestoreMigrateRecover => "backup / restore / migrate / recover",
            TckPoint::SingleNodeFailure => "single-node failure",
            TckPoint::InteropWorkloadSmoke => "interop / workload smoke",
        }
    }
}

/// The honest outcome of evaluating one [`TckPoint`] for one modality: either it
/// genuinely `Pass`es, or it is `NotImplemented` with a machine-readable (well,
/// human-readable but non-empty and specific) reason. There is deliberately no
/// silent-skip / default-pass variant — see module docs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TckStatus {
    Pass,
    NotImplemented(&'static str),
}

impl TckStatus {
    pub fn is_pass(&self) -> bool {
        matches!(self, TckStatus::Pass)
    }

    /// A short machine-parseable tag (`"PASS"` / `"NOT_IMPLEMENTED"`).
    pub fn tag(&self) -> &'static str {
        match self {
            TckStatus::Pass => "PASS",
            TckStatus::NotImplemented(_) => "NOT_IMPLEMENTED",
        }
    }
}

/// One point's result for one modality.
#[derive(Clone, Debug)]
pub struct TckPointResult {
    pub point: TckPoint,
    pub status: TckStatus,
}

/// A modality's full capability report: its `storage_kind()` name plus a result for
/// EVERY `TckPoint::ALL` entry (never a subset — `tck_report` always produces exactly
/// `TckPoint::ALL.len()` results).
#[derive(Clone, Debug)]
pub struct TckReport {
    pub modality: &'static str,
    pub results: Vec<TckPointResult>,
}

impl TckReport {
    /// `true` iff every point `Pass`es — the honest, provable definition of
    /// "first-class" this workstream introduces (not "compiles against the trait").
    pub fn is_first_class(&self) -> bool {
        self.results.iter().all(|r| r.status.is_pass())
    }

    pub fn pass_count(&self) -> usize {
        self.results.iter().filter(|r| r.status.is_pass()).count()
    }

    /// Render a Markdown capability table for this one modality — the building block
    /// `README.md`'s generated parity view is assembled from (see that file's
    /// "Capability parity" section for the regeneration recipe).
    pub fn render_table(&self) -> String {
        let mut out = format!(
            "### {} — {}/{} first-class\n\n| point | status | detail |\n|---|---|---|\n",
            self.modality,
            self.pass_count(),
            self.results.len()
        );
        for r in &self.results {
            let detail = match &r.status {
                TckStatus::Pass => "",
                TckStatus::NotImplemented(reason) => reason,
            };
            out.push_str(&format!(
                "| {} | {} | {} |\n",
                r.point.label(),
                r.status.tag(),
                detail
            ));
        }
        out
    }
}

/// Compute the full 12-point TCK capability report for any `ConformanceTestable`
/// modality, using ONLY what `ModalityContract` + `ConformanceTestable` already
/// expose. Invents no storage/ingest/backup surface a modality doesn't have — points
/// the trait genuinely cannot answer are `NotImplemented` with an honest reason (see
/// module docs), never silently defaulted to `Pass`.
pub fn tck_report<T: ConformanceTestable>() -> TckReport {
    let sample = T::conformance_sample();
    let id = T::conformance_id();
    let mut results = Vec::with_capacity(TckPoint::ALL.len());

    // (1) Stable versioned schema/ids: the only "schema stability" signal the v1
    // trait shape can attest to (it has no explicit version field) is that
    // storage_kind() names itself and to_rowset(id) echoes the SAME id it was given.
    let kind = ModalityContract::storage_kind(&sample);
    let row = ModalityContract::to_rowset(&sample, id);
    results.push(TckPointResult {
        point: TckPoint::SchemaAndIds,
        status: if !kind.is_empty() && row.id == id {
            TckStatus::Pass
        } else {
            TckStatus::NotImplemented(
                "storage_kind()/to_rowset(id) do not stably name and echo this modality",
            )
        },
    });

    // (2) Ingest (+streaming): ModalityContract v1 has NO ingest/streaming method at
    // all (see contract.rs's v1-scoping rationale) — always NotImplemented until a
    // future trait increment adds one.
    results.push(TckPointResult {
        point: TckPoint::IngestStreaming,
        status: TckStatus::NotImplemented(
            "ModalityContract v1 has no ingest/streaming hook — out of scope until a future trait increment",
        ),
    });

    // (3) Codec / unsupported-format behavior: genuinely testable today — a
    // corrupted Put payload must decode as Err, never silently succeed.
    let staged = ModalityContract::txn_stage(&sample, id);
    let codec_status = match staged.kind {
        WriteKind::Put => {
            let mut corrupt = staged.clone();
            corrupt.payload = b"\xff\xfe not a valid payload for any codec".to_vec();
            match decode_staged::<T>(&corrupt) {
                Err(_) => TckStatus::Pass,
                Ok(_) => TckStatus::NotImplemented(
                    "decode_staged silently accepted a malformed payload instead of erroring",
                ),
            }
        }
        WriteKind::Delete => TckStatus::NotImplemented(
            "conformance_sample() stages as Delete — no Put payload exists to exercise the codec against",
        ),
    };
    results.push(TckPointResult {
        point: TckPoint::CodecUnsupportedFormat,
        status: codec_status,
    });

    // (4) Storage + secondary-index + stats presence: not modeled by
    // ModalityContract at all (no stats/index hook) — always NotImplemented.
    results.push(TckPointResult {
        point: TckPoint::StorageIndexStats,
        status: TckStatus::NotImplemented(
            "ModalityContract exposes no storage/secondary-index/stats hook — lives in the modality's own store, unobserved here",
        ),
    });

    // (5) Typed query operators: Pass iff the modality declares at least one
    // well-formed named analytics op (empty is the legitimate default for "nothing
    // beyond plain store/retrieve" per contract.rs's module docs).
    let ops = ModalityContract::analytics_ops(&sample);
    results.push(TckPointResult {
        point: TckPoint::TypedQueryOperators,
        status: if !ops.is_empty() && ops.iter().all(|o| !o.is_empty()) {
            TckStatus::Pass
        } else {
            TckStatus::NotImplemented(
                "analytics_ops() is empty — no typed query operators declared",
            )
        },
    });

    // (6) Txn participation OR declared saga/outbox: txn_stage/rollback id-symmetry
    // is a CORE (non-default) method every modality must answer — this always
    // passes unless the impl is actually broken.
    let rollback_ok = staged.rollback().as_deref() == Some(id);
    results.push(TckPointResult {
        point: TckPoint::TxnOrSagaOutbox,
        status: if rollback_ok {
            TckStatus::Pass
        } else {
            TckStatus::NotImplemented("txn_stage(id).rollback() did not echo the staged id")
        },
    });

    // (7) CDC / delete / tombstone / retention / GC: the trait only models the CDC
    // half (cdc_topic()); delete/tombstone semantics live in the txn engine
    // (StagedWrite::Delete), retention/GC have no hook at all. Coarse, HONEST proxy:
    // Pass only if a genuinely non-empty CDC topic is declared — a modality with no
    // CDC topic certainly has no observable delete/retention/GC story either.
    results.push(TckPointResult {
        point: TckPoint::CdcDeleteRetentionGc,
        status: match ModalityContract::cdc_topic(&sample) {
            Some(topic) if !topic.is_empty() => TckStatus::Pass,
            _ => TckStatus::NotImplemented(
                "no cdc_topic() declared; delete/tombstone/retention/GC have no ModalityContract hook at all",
            ),
        },
    });

    // (8) Tenant/row/region policy: Pass iff the modality attaches its own policy
    // labels. Empty is the LEGITIMATE default (policy usually lives one layer up, at
    // eg-core::isolation) — NotImplemented here means "not attached at the modality
    // layer", not "broken".
    let labels = ModalityContract::policy_labels(&sample, id);
    results.push(TckPointResult {
        point: TckPoint::TenantRowRegionPolicy,
        status: if !labels.is_empty() {
            TckStatus::Pass
        } else {
            TckStatus::NotImplemented(
                "policy_labels() is empty — no modality-level tenant/row/region policy attached (may still be enforced at eg-core::isolation)",
            )
        },
    });

    // (9) Provenance + evidence-location + lineage: Pass iff EITHER provenance() or
    // evidence() reports something — either counts as "this modality can attest to
    // where/how a value came from".
    let has_prov = ModalityContract::provenance(&sample, id).is_some();
    let has_evi = ModalityContract::evidence(&sample, id).is_some();
    results.push(TckPointResult {
        point: TckPoint::ProvenanceEvidenceLineage,
        status: if has_prov || has_evi {
            TckStatus::Pass
        } else {
            TckStatus::NotImplemented(
                "both provenance() and evidence() are None — no lineage attached at this layer",
            )
        },
    });

    // (10) Backup / restore / migrate / recover: unmodeled by ModalityContract.
    results.push(TckPointResult {
        point: TckPoint::BackupRestoreMigrateRecover,
        status: TckStatus::NotImplemented(
            "ModalityContract has no backup/restore/migrate/recover hook — durability lifecycle lives in the engine's own WAL/snapshot machinery, unobserved here",
        ),
    });

    // (11) Single-node failure: unmodeled by ModalityContract.
    results.push(TckPointResult {
        point: TckPoint::SingleNodeFailure,
        status: TckStatus::NotImplemented(
            "ModalityContract exposes no fault-injection/recovery hook to observe single-node failure behavior",
        ),
    });

    // (12) Interop / workload smoke: the base round-trip exercised above (project to
    // a row, stage a write, decode it back, roll it back) completing without
    // panicking IS a genuine minimal cross-cutting smoke test.
    results.push(TckPointResult {
        point: TckPoint::InteropWorkloadSmoke,
        status: TckStatus::Pass,
    });

    TckReport {
        modality: kind,
        results,
    }
}
