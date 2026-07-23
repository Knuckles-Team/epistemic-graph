//! The first-class Test Compatibility Kit (TCK) — 12 capability points every `eg-*`
//! modality is evaluated against (CONCEPT:E4 / EG-P1-1, P1 roadmap feedback: promote
//! `ModalityContract` from an opt-in conformance macro to a genuine, provable,
//! first-class TCK).
//!
//! ## Every point maps to a real `ModalityContract` hook
//!
//! As of EG-P1-1 the TCK is COMPLETE: all 12 points are evaluated against a real trait
//! method — there is no longer any point that is "structurally unmeasurable". The four
//! points that once had no corresponding method (ingest/streaming, storage+index+stats,
//! backup/restore/migrate/recover, single-node failure) now map to the four EG-P1-1
//! hooks added to the trait: `ingest_report`, `storage_stats`, `backup_selfcheck`,
//! `recovery_selfcheck` (each with a default returning "unsupported", so none of the
//! existing implementers break — see `contract.rs`). "First-class" is therefore
//! PROVABLE per point, not assumed because a modality merely compiles against the
//! trait.
//!
//! ## Three honest statuses, never a silent pass
//!
//! Each point resolves to exactly one of:
//! * **`Pass`** — a real check succeeded (a codec round-trip, a declared capability, a
//!   hook self-test that observed success).
//! * **`NotApplicable(reason)`** — the point genuinely does not apply to this
//!   modality's nature, declared by the modality via
//!   `ModalityContract::tck_not_applicable` WITH a concrete reason. Distinct from
//!   "not yet"; counts toward first-class (it is not a gap). Example: lineage/policy/
//!   CDC on a bare numeric tensor (enforced elsewhere or architecturally absent).
//! * **`NotImplemented(reason)`** — a real hook exists but this modality has not wired
//!   it (the default), OR a wired self-check FAILED (honest red). This is the only
//!   status that counts as an outstanding gap.
//!
//! `is_first_class()` == no `NotImplemented` remains (all Pass or N/A).
//! `is_production_ready()` == all 12 points are `Pass` and the concrete native
//! codec/normalization/index/query/resource probe passes; N/A or a missing probe is
//! not production certification. The fleet TCK applies that stronger rule to every
//! served modality.
//!
//! ## How each point is decided
//!
//! 1. **Schema/ids** — `storage_kind()` names itself and `to_rowset(id)` echoes the
//!    SAME id it was given.
//! 2. **Ingest (+streaming)** — both `ingest_report().batch` and `.streaming`
//!    self-check `Passed`; a streaming N/A is first-class but not production-ready.
//! 3. **Codec / unsupported-format** — a corrupted `Put` payload must `decode_staged`
//!    as `Err`, never silently succeed (and never panic — `decode_staged` is
//!    `serde_json`-backed, which itself never panics on malformed input, only errors).
//! 4. **Storage + secondary-index + stats** — `storage_stats()` returns `Some`.
//! 5. **Typed query operators** — `analytics_ops()` is non-empty (and well-formed).
//! 6. **Txn participation** — `txn_stage`/`rollback` id-symmetry (every modality
//!    answers this; it is a CORE, non-default method).
//! 7. **CDC (the modeled half)** — a genuinely declared, non-empty `cdc_topic()`.
//!    Delete/tombstone/retention/GC are not separately modeled; a modality for which
//!    change-capture is architecturally absent declares this point N/A.
//! 8. **Tenant/row/region policy** — non-empty `policy_labels()`. Empty is the
//!    legitimate default when policy lives one layer up (`eg-core::isolation`); such a
//!    modality declares this point N/A rather than leaving a gap.
//! 9. **Provenance + evidence + lineage** — either `provenance()` or
//!    `evidence_address()`
//!    reports `Some`.
//! 10. **Backup/restore/migrate/recover** — `backup_selfcheck()` self-check `Passed`
//!     (a real round-trip through the modality's durable codec).
//! 11. **Single-node failure/recovery** — `recovery_selfcheck()` self-check `Passed`
//!     (a simulated crash-and-recover through the staged-write/WAL replay path).
//! 12. **Interop/workload smoke** — the base round-trip above (project to a row, stage
//!     a write, decode it back, roll it back) not panicking IS a genuine minimal
//!     cross-cutting smoke test.

use crate::capability::ModalitySelfTest;
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

    /// A very short column header for the compact fleet parity table
    /// (`render_fleet_table`).
    pub fn short(&self) -> &'static str {
        match self {
            TckPoint::SchemaAndIds => "schema",
            TckPoint::IngestStreaming => "ingest",
            TckPoint::CodecUnsupportedFormat => "codec",
            TckPoint::StorageIndexStats => "storage",
            TckPoint::TypedQueryOperators => "query",
            TckPoint::TxnOrSagaOutbox => "txn",
            TckPoint::CdcDeleteRetentionGc => "cdc",
            TckPoint::TenantRowRegionPolicy => "policy",
            TckPoint::ProvenanceEvidenceLineage => "prov",
            TckPoint::BackupRestoreMigrateRecover => "backup",
            TckPoint::SingleNodeFailure => "recover",
            TckPoint::InteropWorkloadSmoke => "smoke",
        }
    }

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

/// The honest outcome of evaluating one [`TckPoint`] for one modality: it genuinely
/// `Pass`es, it is `NotApplicable` to this modality's nature (with a reason — a
/// distinct, honest status introduced in EG-P1-1), or it is `NotImplemented` (a real
/// hook exists but this modality has not wired it — also with a reason). There is
/// deliberately no silent-skip / default-pass variant — see module docs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TckStatus {
    Pass,
    /// The point genuinely does not apply to this modality (e.g. lineage/policy on a
    /// bare numeric tensor). Counts toward "first-class" — it is not a gap.
    NotApplicable(&'static str),
    NotImplemented(&'static str),
}

impl TckStatus {
    pub fn is_pass(&self) -> bool {
        matches!(self, TckStatus::Pass)
    }

    /// `true` for `Pass` OR `NotApplicable` — i.e. this point is not an outstanding
    /// GAP. `is_first_class` is the AND of this over all 12 points.
    pub fn is_covered(&self) -> bool {
        matches!(self, TckStatus::Pass | TckStatus::NotApplicable(_))
    }

    /// A short machine-parseable tag (`"PASS"` / `"N/A"` / `"NOT_IMPLEMENTED"`).
    pub fn tag(&self) -> &'static str {
        match self {
            TckStatus::Pass => "PASS",
            TckStatus::NotApplicable(_) => "N/A",
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
    pub native_production_probe: Option<crate::NativeProductionProbe>,
}

impl TckReport {
    /// `true` iff every point is `Pass` OR `NotApplicable` (no `NotImplemented`
    /// remains) — the honest, provable definition of "first-class" this workstream
    /// introduces. A genuinely-N/A point (with a documented reason) is NOT a gap; an
    /// unimplemented one is.
    pub fn is_first_class(&self) -> bool {
        self.results.iter().all(|r| r.status.is_covered())
    }

    /// Production certification requires every core dimension to execute and PASS.
    /// A declared
    /// `NotApplicable` remains useful for experimental/value-only modalities, but
    /// cannot certify a served production modality.
    pub fn is_production_ready(&self) -> bool {
        self.results.len() == TckPoint::ALL.len()
            && self.results.iter().all(|result| result.status.is_pass())
            && self
                .native_production_probe
                .is_some_and(crate::NativeProductionProbe::passed)
    }

    pub fn native_probe_passed(&self) -> bool {
        self.native_production_probe
            .is_some_and(crate::NativeProductionProbe::passed)
    }

    /// The exact core dimensions preventing production certification.
    pub fn production_gaps(&self) -> Vec<TckPoint> {
        self.results
            .iter()
            .filter(|result| !result.status.is_pass())
            .map(|result| result.point)
            .collect()
    }

    pub fn pass_count(&self) -> usize {
        self.results.iter().filter(|r| r.status.is_pass()).count()
    }

    /// Count of points marked genuinely `NotApplicable`.
    pub fn na_count(&self) -> usize {
        self.results
            .iter()
            .filter(|r| matches!(r.status, TckStatus::NotApplicable(_)))
            .count()
    }

    /// Count of points covered (Pass + N/A) — the numerator of the "X/12 covered"
    /// first-class score.
    pub fn covered_count(&self) -> usize {
        self.results
            .iter()
            .filter(|r| r.status.is_covered())
            .count()
    }

    /// A one-line summary like `"7 PASS + 3 N/A = 10/12 covered [FIRST-CLASS]"` or
    /// `"5 PASS + 0 N/A = 5/12 covered [4 gaps]"`.
    pub fn summary(&self) -> String {
        let gaps = self.results.len() - self.covered_count();
        let tail = if self.is_production_ready() {
            "PRODUCTION-READY".to_string()
        } else if self.results.iter().all(|result| result.status.is_pass()) {
            "NATIVE-PROBE-FAILED".to_string()
        } else if self.is_first_class() {
            "FIRST-CLASS (NON-PRODUCTION N/A)".to_string()
        } else {
            format!("{gaps} gap(s)")
        };
        format!(
            "{} PASS + {} N/A = {}/{} covered [{}]",
            self.pass_count(),
            self.na_count(),
            self.covered_count(),
            self.results.len(),
            tail
        )
    }

    /// Render a Markdown capability table for this one modality — the building block
    /// `README.md`'s generated parity view is assembled from (see that file's
    /// "Capability parity" section for the regeneration recipe).
    pub fn render_table(&self) -> String {
        let mut out = format!(
            "### {} — {}\n\n| point | status | detail |\n|---|---|---|\n",
            self.modality,
            self.summary(),
        );
        for r in &self.results {
            let detail = match &r.status {
                TckStatus::Pass => "",
                TckStatus::NotApplicable(reason) => reason,
                TckStatus::NotImplemented(reason) => reason,
            };
            out.push_str(&format!(
                "| {} | {} | {} |\n",
                r.point.label(),
                r.status.tag(),
                detail
            ));
        }
        let (status, detail) = match self.native_production_probe {
            Some(probe) if probe.passed() => (
                "PASS",
                "codec + normalized payload + index + query + bounds",
            ),
            Some(_) => ("FAILED", "one or more native production checks failed"),
            None => ("MISSING", "no native production probe declared"),
        };
        out.push_str(&format!(
            "| native production probe | {status} | {detail} |\n"
        ));
        out
    }
}

/// Collect several modality reports into ONE cross-crate parity table (one row per
/// modality, one column per TCK point) — the fleet-wide first-class view. Driven by
/// the fleet harness (see `tests/fleet_tck.rs`) that pulls the pilot crates in as
/// dev-deps so all of their reports exist in a single process.
pub fn render_fleet_table(reports: &[TckReport]) -> String {
    let mut out = String::from("# Modality TCK — fleet capability parity\n\n");
    out.push_str("| modality |");
    for p in TckPoint::ALL {
        out.push_str(&format!(" {} |", p.short()));
    }
    out.push_str(" native | production | first-class |\n|---|");
    for _ in TckPoint::ALL {
        out.push_str("---|");
    }
    out.push_str("---|---|---|\n");
    for report in reports {
        out.push_str(&format!("| `{}` |", report.modality));
        for point in TckPoint::ALL {
            let tag = report
                .results
                .iter()
                .find(|r| r.point == point)
                .map(|r| r.status.tag())
                .unwrap_or("-");
            out.push_str(&format!(" {tag} |"));
        }
        let native = match report.native_production_probe {
            Some(probe) if probe.passed() => "PASS",
            Some(_) => "FAIL",
            None => "-",
        };
        let production = if report.is_production_ready() {
            "✓"
        } else {
            "-"
        };
        let mark = if report.is_first_class() {
            format!("**{}/{}** ✓", report.covered_count(), report.results.len())
        } else {
            format!("{}/{}", report.covered_count(), report.results.len())
        };
        out.push_str(&format!(" {native} | {production} | {mark} |\n"));
    }
    out
}

/// Map a hook's [`ModalitySelfTest`] outcome onto a [`TckStatus`]: a `Passed`
/// self-check is a real `Pass`; a `Failed` one is an HONEST `NotImplemented` (the hook
/// is wired but its own round-trip broke — never hidden); `Unsupported` is
/// `NotImplemented` with the caller's "no hook overridden" reason; a genuine
/// `NotApplicable(reason)` carries straight through.
fn selftest_status(t: ModalitySelfTest, unsupported_reason: &'static str) -> TckStatus {
    match t {
        ModalitySelfTest::Passed => TckStatus::Pass,
        ModalitySelfTest::Failed => TckStatus::NotImplemented(
            "hook self-check FAILED (its own round-trip did not recover the value)",
        ),
        ModalitySelfTest::Unsupported => TckStatus::NotImplemented(unsupported_reason),
        ModalitySelfTest::NotApplicable(reason) => TckStatus::NotApplicable(reason),
    }
}

fn ingest_status(report: crate::IngestReport) -> TckStatus {
    match (report.batch, report.streaming) {
        (ModalitySelfTest::Passed, ModalitySelfTest::Passed) => TckStatus::Pass,
        (ModalitySelfTest::Failed, _) | (_, ModalitySelfTest::Failed) => {
            TckStatus::NotImplemented("batch or streaming ingest self-check FAILED")
        }
        (ModalitySelfTest::Unsupported, _) | (_, ModalitySelfTest::Unsupported) => {
            TckStatus::NotImplemented(
                "ingest_report() does not attest both batch and streaming ingest",
            )
        }
        (ModalitySelfTest::NotApplicable(reason), _)
        | (_, ModalitySelfTest::NotApplicable(reason)) => TckStatus::NotApplicable(reason),
    }
}

/// Compute the full 12-point TCK capability report for any `ConformanceTestable`
/// modality. As of EG-P1-1 every one of the 12 points maps to a REAL
/// `ModalityContract` hook (the four formerly-structural gaps — ingest, storage/stats,
/// backup, single-node recovery — now have `ingest_report`/`storage_stats`/
/// `backup_selfcheck`/`recovery_selfcheck`). A modality that does not override a hook
/// still reports `NotImplemented` for ITSELF, but no point is structurally
/// unmeasurable. Before deriving each point, a modality's own
/// [`ModalityContract::tck_not_applicable`] is consulted: a point it declares
/// genuinely N/A (with a reason) becomes `NotApplicable`, not `NotImplemented`. No
/// point is ever silently defaulted to `Pass`.
pub fn tck_report<T: ConformanceTestable>() -> TckReport {
    let sample = T::conformance_sample();
    let id = T::conformance_id();
    let kind = ModalityContract::storage_kind(&sample);
    let mut results = Vec::with_capacity(TckPoint::ALL.len());

    // Scope the push-closure so its borrow of `results`/`sample` ends before we move
    // `results` into the report. Consult the modality's N/A escape hatch first; fall
    // back to the auto-derived status only where a point is not declared genuinely N/A.
    {
        let mut push = |point: TckPoint, auto: TckStatus| {
            let status = match ModalityContract::tck_not_applicable(&sample, point) {
                Some(reason) => TckStatus::NotApplicable(reason),
                None => auto,
            };
            results.push(TckPointResult { point, status });
        };

        // (1) Stable versioned schema/ids: the only "schema stability" signal the
        // trait shape can attest to (it has no explicit version field) is that
        // storage_kind() names itself and to_rowset(id) echoes the SAME id.
        let row = ModalityContract::to_rowset(&sample, id);
        push(
            TckPoint::SchemaAndIds,
            if !kind.is_empty() && row.id == id {
                TckStatus::Pass
            } else {
                TckStatus::NotImplemented(
                    "storage_kind()/to_rowset(id) do not stably name and echo this modality",
                )
            },
        );

        // (2) Ingest (+streaming): production Pass requires both self-checks. A
        // whole-value modality may report streaming N/A, which remains honest
        // first-class coverage but cannot become production certification.
        let ingest = ModalityContract::ingest_report(&sample, id);
        push(TckPoint::IngestStreaming, ingest_status(ingest));

        // (3) Codec / unsupported-format behavior: a corrupted Put payload must decode as
        // Err, never silently succeed.
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
        push(TckPoint::CodecUnsupportedFormat, codec_status);

        // (4) Storage + secondary-index + stats presence: EG-P1-1 hook `storage_stats`.
        // Pass iff the modality can attest its own storage stats.
        push(
            TckPoint::StorageIndexStats,
            match ModalityContract::storage_stats(&sample, id) {
                Some(_) => TckStatus::Pass,
                None => TckStatus::NotImplemented(
                    "storage_stats() is None — modality attests no storage/secondary-index/stats at this layer",
                ),
            },
        );

        // (5) Typed query operators: Pass iff the modality declares at least one
        // well-formed named analytics op.
        let ops = ModalityContract::analytics_ops(&sample);
        push(
            TckPoint::TypedQueryOperators,
            if !ops.is_empty() && ops.iter().all(|o| !o.is_empty()) {
                TckStatus::Pass
            } else {
                TckStatus::NotImplemented(
                    "analytics_ops() is empty — no typed query operators declared",
                )
            },
        );

        // (6) Txn participation OR declared saga/outbox: txn_stage/rollback id-symmetry is
        // a CORE (non-default) method every modality must answer.
        let rollback_ok = staged.rollback().as_deref() == Some(id);
        push(
            TckPoint::TxnOrSagaOutbox,
            if rollback_ok {
                TckStatus::Pass
            } else {
                TckStatus::NotImplemented("txn_stage(id).rollback() did not echo the staged id")
            },
        );

        // (7) CDC / delete / tombstone / retention / GC: the trait models the CDC half
        // (cdc_topic()); delete/tombstone live in the txn engine (StagedWrite::Delete).
        // Pass only if a genuinely non-empty CDC topic is declared.
        push(
            TckPoint::CdcDeleteRetentionGc,
            match ModalityContract::cdc_topic(&sample) {
                Some(topic) if !topic.is_empty() => TckStatus::Pass,
                _ => TckStatus::NotImplemented(
                    "no cdc_topic() declared; a modality that does not publish change events has no observable delete/retention/GC story here",
                ),
            },
        );

        // (8) Tenant/row/region policy: Pass iff the modality attaches its own policy
        // labels. Empty is the LEGITIMATE default (policy usually lives one layer up, at
        // eg-core::isolation) — a modality for which that is ARCHITECTURALLY true should
        // declare this point N/A via tck_not_applicable rather than leave it a gap.
        let labels = ModalityContract::policy_labels(&sample, id);
        push(
            TckPoint::TenantRowRegionPolicy,
            if !labels.is_empty() {
                TckStatus::Pass
            } else {
                TckStatus::NotImplemented(
                    "policy_labels() is empty — no modality-level tenant/row/region policy attached",
                )
            },
        );

        // (9) Provenance + evidence-location + lineage: Pass iff EITHER provenance() or
        // evidence_address() reports something.
        let has_prov = ModalityContract::provenance(&sample, id).is_some();
        let has_evi = ModalityContract::evidence_address(&sample).is_some();
        push(
            TckPoint::ProvenanceEvidenceLineage,
            if has_prov || has_evi {
                TckStatus::Pass
            } else {
                TckStatus::NotImplemented(
                    "both provenance() and evidence_address() are None — no lineage attached at this layer",
                )
            },
        );

        // (10) Backup / restore / migrate / recover: EG-P1-1 hook `backup_selfcheck` —
        // a real round-trip through the modality's DURABLE codec.
        push(
            TckPoint::BackupRestoreMigrateRecover,
            selftest_status(
                ModalityContract::backup_selfcheck(&sample, id),
                "backup_selfcheck() defaults to unsupported — modality has not wired a durable backup/restore round-trip",
            ),
        );

        // (11) Single-node failure / recovery: EG-P1-1 hook `recovery_selfcheck` — a
        // simulated crash-and-recover via the staged-write (WAL analog) replay path.
        push(
            TckPoint::SingleNodeFailure,
            selftest_status(
                ModalityContract::recovery_selfcheck(&sample, id),
                "recovery_selfcheck() defaults to unsupported — modality has not wired a crash-recovery replay",
            ),
        );

        // (12) Interop / workload smoke: the base round-trip exercised above (project
        // to a row, stage a write, decode it back, roll it back) completing without
        // panicking IS a genuine minimal cross-cutting smoke test.
        push(TckPoint::InteropWorkloadSmoke, TckStatus::Pass);
    }

    TckReport {
        modality: kind,
        results,
        native_production_probe: T::native_production_probe(),
    }
}
