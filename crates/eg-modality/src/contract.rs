//! `ModalityContract` — the trait every `eg-*` modality value type (`Tensor`,
//! `Geometry`, a future `eg-tsdb` series point, `eg-rdf`'s RDF term/OWL individual,
//! …) can implement to describe itself in four DAG-safe, cross-cutting shapes,
//! independent of eg-plan/eg-core.
//!
//! ## Why these four core methods and no more (v1 scope)
//!
//! Drafted against three structurally different leaf crates (per the E4 brief):
//! `eg-tensor` (a `Tensor { dtype, shape, data }` — pure value, ANN-adjacent),
//! `eg-tsdb` (a time-keyed series store — append/scan/range, NOT a single value),
//! and `eg-rdf` (RDF terms + `owl::Justification` — has real provenance/confidence).
//! Across all three, exactly four things generalize losslessly:
//!
//! 1. **What kind of storage is this?** (`storage_kind`) — every modality can name
//!    itself; tensor says `"tensor"`, tsdb says `"tsdb"`, rdf says `"rdf"`.
//! 2. **How does a value become a cross-modal query result row?** (`to_rowset`) —
//!    every modality's values are stored under SOME id and can contribute an
//!    `{id, score}` row (a tensor's norm, an unranked geometry, an RDF individual's
//!    reasoning-derived confidence, …).
//! 3. **How is a write staged inside a transaction?** (`txn_stage`) — every
//!    modality's mutation goes through SOME staging step before durability (tsdb's
//!    `StagedSeries` overlay, the engine's WAL, …); `StagedWrite` is the shape that
//!    generalizes over all of them.
//! 4. **Does this modality participate in CDC?** (`cdc_topic`) — some do (tsdb could
//!    publish series-append events), some don't (a bare tensor value has no CDC
//!    topic today); `None` is a legitimate, common answer.
//!
//! What did NOT generalize cleanly enough for v1 (left as future increments, not
//! stubbed): a typed query/filter surface (tensor's slice/reduce vs. tsdb's ASOF vs.
//! RDF's SPARQL BGP share no common shape beyond the `analytics_ops()` name list
//! below); a typed provenance/evidence model (only RDF has real derivation history;
//! forcing one on tensor/geo would be stub boilerplate) — hence those four are
//! **default-empty**, not core.
//!
//! ## The four default-empty methods
//!
//! `provenance`/`evidence_address`/`policy_labels`/`analytics_ops` all default to
//! "nothing to report". A modality overrides ONLY the ones that are meaningful for
//! it — e.g. `eg-rdf` overriding `provenance` to map `owl::Justification`, or
//! `eg-tensor` overriding `analytics_ops` to list `slice`/`reduce`/`elementwise`.
//! Tensor/geo (the v1 pilots) do not need to override `provenance`/`evidence_address`/
//! `policy_labels` at all — that is the point: no stub boilerplate on modalities
//! that have nothing to say there.
//!
//! ## The EG-P1-1 hooks (five more default methods)
//!
//! To make the first-class TCK (`crate::tck`) COMPLETE — every one of its 12 points
//! backed by a real trait method rather than a structural gap — four more
//! default-"unsupported" hooks were added: `ingest_report` (TCK point 2),
//! `storage_stats` (point 4), `backup_selfcheck` (point 10), `recovery_selfcheck`
//! (point 11). A modality overrides the ones it can genuinely satisfy with a REAL
//! self-check (see `eg-tensor`/`eg-geo`, which round-trip through their own durable
//! codecs). A fifth hook, `tck_not_applicable`, lets a modality declare a point
//! genuinely N/A (with a reason) — a distinct, honest status from "not implemented".
//! All five default such that the ~17 existing implementers keep compiling untouched.
//! `TckReport::is_production_ready` is intentionally stricter: a served production
//! modality must pass all 12 points and cannot use N/A to satisfy a core dimension.

use crate::artifact::EvidenceAddress;
use crate::capability::{IngestReport, ModalitySelfTest, NativeProductionProbe, StorageStats};
use crate::native::{NativeIndexKey, NativePredicate};
use crate::provenance::Provenance;
use crate::rowset::RowSetShape;
use crate::tck::TckPoint;
use crate::txn::StagedWrite;

/// The seam every `eg-*` modality value type can implement. See the module docs for
/// the v1-scoping rationale (4 core + 4 default-empty methods).
pub trait ModalityContract {
    // ── 4 core methods — generalize cleanly across tensor/tsdb/rdf; no default,
    // every implementer must answer them (even if the answer is `None`/empty). ──

    /// A short, stable, machine-readable name for this modality's storage substrate
    /// (`"tensor"`, `"geo"`, `"tsdb"`, `"rdf"`, …). For capability discovery/logging
    /// ONLY — dispatch stays the existing per-domain handler routing
    /// (`AGENTS.md` convention 4), this is not a new routing key.
    fn storage_kind(&self) -> &'static str;

    /// Project this value into the cross-modal `RowSetShape` currency under the
    /// caller-supplied `id` (the modality value itself does not know its own id —
    /// it lives in the KV/graph store one layer up, exactly like `eg-plan`'s
    /// operators are handed ids by the store they scan).
    fn to_rowset(&self, id: &str) -> RowSetShape;

    /// Stage this value as a write inside a transaction (see `crate::txn` module
    /// docs for the shape rationale). Only DESCRIBES the write; applying or rolling
    /// it back is the caller's (the txn engine's) job.
    fn txn_stage(&self, id: &str) -> StagedWrite;

    /// The CDC topic this modality's writes publish under, if any
    /// (`eg_types::wire::CdcEvent`-family surface). `None` is a legitimate answer
    /// for a modality that is not (yet) CDC-observable.
    fn cdc_topic(&self) -> Option<&'static str>;

    // ── 4 default-empty methods — override ONLY where meaningful. ──

    /// Provenance/derivation info for the value at `id`. Default: `None` (most
    /// modalities have nothing to report — see module docs). The reference
    /// non-trivial case is `eg-rdf` mapping `owl::Justification`.
    fn provenance(&self, _id: &str) -> Option<Provenance> {
        None
    }

    /// The exact numeric or opaque address this value contributes to a governed
    /// [`crate::EvidenceLocus`]. Identity, subject, policy, and derivation are
    /// supplied by the ingestion boundary; a modality value may not fabricate
    /// them from raw source identifiers. Default: `None`.
    fn evidence_address(&self) -> Option<EvidenceAddress> {
        None
    }

    /// Policy/security labels attached to this value (ABAC/RLS tags, classification
    /// level, …). Default: empty — labels are usually attached at the node/graph
    /// layer (`eg-core::isolation`), not the modality value itself; override where
    /// a modality embeds its own (e.g. a future document modality's redaction tags).
    fn policy_labels(&self, _id: &str) -> Vec<String> {
        Vec::new()
    }

    /// Named analytics operations this modality exposes to the cross-modal query
    /// planner beyond plain store/retrieve (tensor's `slice`/`reduce`/
    /// `elementwise`/`reshape`; geo's `within`/`distance`/`buffer`/…). Default:
    /// empty. Informational (capability discovery) — does NOT wire the ops
    /// themselves; that stays `eg-plan`'s executor + the modality's own API.
    fn analytics_ops(&self) -> Vec<&'static str> {
        Vec::new()
    }

    // ── 4 EG-P1-1 hooks — one per previously-structural TCK gap. All DEFAULT to
    // "unsupported", so none of the ~17 existing implementers break; a modality
    // overrides the ones it can genuinely satisfy (see eg-tensor/eg-geo). See
    // `crate::capability` for the DTOs and `crate::tck` for how each maps to a point.

    /// TCK point (2) — ingest (+streaming where applicable). Override to run a REAL
    /// ingest self-check (e.g. parse this value back from its durable wire form) and
    /// report [`IngestReport`]. Default: neither batch nor streaming ingest wired.
    fn ingest_report(&self, _id: &str) -> IngestReport {
        IngestReport::unsupported()
    }

    /// TCK point (4) — storage + secondary-index + stats presence. Override to report
    /// this value's real [`StorageStats`] (logical size, element count, whether it is
    /// secondary-indexed). Default: `None` (no storage stats attestable at this
    /// layer).
    fn storage_stats(&self, _id: &str) -> Option<StorageStats> {
        None
    }

    /// TCK point (10) — backup / restore / migrate / recover. Override to run a REAL
    /// backup→restore round-trip through this modality's DURABLE persistence form
    /// (the on-disk codec) and report whether the restored value equalled the
    /// original. Default: unsupported.
    fn backup_selfcheck(&self, _id: &str) -> ModalitySelfTest {
        ModalitySelfTest::Unsupported
    }

    /// TCK point (11) — single-node failure / recovery. Override to SIMULATE a
    /// crash-and-recover: stage this value as an in-txn write, serialize the staged
    /// write (the WAL analog), then replay it back after "restart" and report whether
    /// the recovered value equalled the original. Default: unsupported.
    fn recovery_selfcheck(&self, _id: &str) -> ModalitySelfTest {
        ModalitySelfTest::Unsupported
    }

    /// The honesty escape hatch (EG-P1-1): declare that a given [`TckPoint`] is
    /// genuinely NOT APPLICABLE to this modality's nature — a distinct, honest status
    /// from `NotImplemented` ("not yet") — returning a concrete reason. Default:
    /// `None` (every point is auto-derived from the concrete hooks above). Override
    /// ONLY for points that will NEVER apply to this modality, WITH a real reason
    /// (e.g. a bare numeric tensor has no derivation-lineage or tenant-policy of its
    /// own; those are enforced at the graph-node/`eg-core::isolation` layer that owns
    /// it, never in the array bytes). A caller MUST NOT use this to paper over a point
    /// it actually fails — the reason string is the accountability record.
    fn tck_not_applicable(&self, _point: TckPoint) -> Option<&'static str> {
        None
    }
}

/// Additional fail-closed validation required before a typed modality value may be
/// persisted by [`crate::ServedModalityRuntime`]. Implementations reject raw text,
/// display labels, source locations, personal identifiers, malformed coordinates,
/// and non-opaque references that the generic artifact envelope cannot inspect.
///
/// There is intentionally no default implementation: adding a modality to the
/// production served runtime requires an explicit privacy/invariant audit.
pub trait GovernedModality: ModalityContract {
    fn validate_governed_payload(&self) -> bool;

    /// Keys maintained atomically with this value by the served runtime.
    fn native_index_keys(&self) -> Vec<NativeIndexKey>;

    /// Exact predicate evaluation after bounded index candidate selection.
    fn matches_native_predicate(&self, predicate: &NativePredicate) -> bool;
}

/// A `ModalityContract` implementer that can also produce a deterministic sample of
/// itself, so `modality_conformance_tests!` can exercise the trait without the
/// pilot crate hand-writing fixture-construction boilerplate per test.
///
/// `Clone + PartialEq + Debug + Serialize + DeserializeOwned` are required because
/// the conformance suite's round-trip test stages the sample (`txn_stage`), decodes
/// it back (`decode_staged`), and asserts equality — every bound is there because a
/// concrete conformance test needs it, not speculatively.
pub trait ConformanceTestable:
    ModalityContract
    + Clone
    + std::fmt::Debug
    + PartialEq
    + serde::Serialize
    + serde::de::DeserializeOwned
{
    /// A deterministic, non-trivial sample value for the conformance suite.
    fn conformance_sample() -> Self;

    /// The id the sample is exercised under. Override only if the default
    /// collides with a real id space the pilot crate's own tests use.
    fn conformance_id() -> &'static str {
        "eg-modality-conformance-sample"
    }

    /// Execute the concrete native codec/index/query/resource probe. Value-only
    /// modalities may leave this absent; a served production modality may not.
    fn native_production_probe() -> Option<NativeProductionProbe> {
        None
    }
}

/// Generates the E4 conformance test battery for a `ConformanceTestable` type:
/// round-trip (`txn_stage` -> `decode_staged` -> equality), provenance-family
/// non-panic, txn-stage/rollback symmetry, cdc-topic-iff-declared (a declared topic
/// must be non-empty), a malformed-payload codec check, a well-formedness check on
/// `analytics_ops()`, and — as of EG-P1-1 — registration into the mandatory modality
/// registry ([`crate::register_modality`]) plus a full [`crate::TckReport`]
/// generation via [`crate::tck_report`] (the first-class 12-point TCK; see the `tck`
/// module docs). Invoke once per pilot, inside a `#[cfg(feature = "contract")]`
/// module so the tests only build under that opt-in feature:
///
/// ```ignore
/// #[cfg(feature = "contract")]
/// mod contract {
///     use eg_modality::{ConformanceTestable, ModalityContract, RowSetShape, StagedWrite};
///     use crate::tensor::Tensor;
///
///     impl ModalityContract for Tensor { /* ... */ }
///     impl ConformanceTestable for Tensor {
///         fn conformance_sample() -> Self { /* ... */ }
///     }
///
///     eg_modality::modality_conformance_tests!(Tensor);
/// }
/// ```
#[macro_export]
macro_rules! modality_conformance_tests {
    ($T:ty) => {
        #[cfg(test)]
        mod eg_modality_conformance {
            use super::*;
            #[allow(unused_imports)]
            use $crate::{ConformanceTestable, ModalityContract};

            #[test]
            fn storage_kind_is_non_empty() {
                let sample = <$T as ConformanceTestable>::conformance_sample();
                assert!(
                    !ModalityContract::storage_kind(&sample).is_empty(),
                    "storage_kind() must not be the empty string"
                );
            }

            #[test]
            fn to_rowset_echoes_the_given_id() {
                let sample = <$T as ConformanceTestable>::conformance_sample();
                let id = <$T as ConformanceTestable>::conformance_id();
                let row = ModalityContract::to_rowset(&sample, id);
                assert_eq!(
                    row.id, id,
                    "to_rowset(id) must carry the SAME id it was given"
                );
            }

            /// The load-bearing conformance test: a `Put` staged write's payload must
            /// decode back to a value equal to the one that staged it. This is what
            /// makes `txn_stage` a faithful description of the write (not a lossy one).
            #[test]
            fn txn_stage_round_trips_losslessly() {
                let sample = <$T as ConformanceTestable>::conformance_sample();
                let id = <$T as ConformanceTestable>::conformance_id();
                let staged = ModalityContract::txn_stage(&sample, id);
                assert_eq!(staged.id, id, "txn_stage(id) must stage under the SAME id");
                if staged.kind == $crate::WriteKind::Put {
                    let restored: $T = $crate::decode_staged(&staged)
                        .expect("a Put StagedWrite's payload must decode back to Self");
                    assert_eq!(
                        sample, restored,
                        "txn_stage's payload must round-trip losslessly"
                    );
                }
            }

            /// Rollback symmetry: an aborted transaction discards EXACTLY what it
            /// staged — `rollback()` must name the same id, nothing more/less.
            #[test]
            fn txn_stage_rollback_is_symmetric() {
                let sample = <$T as ConformanceTestable>::conformance_sample();
                let id = <$T as ConformanceTestable>::conformance_id();
                let staged = ModalityContract::txn_stage(&sample, id);
                assert_eq!(
                    staged.rollback().as_deref(),
                    Some(id),
                    "rollback() must echo exactly the id that was staged"
                );
            }

            #[test]
            fn provenance_family_never_panics() {
                let sample = <$T as ConformanceTestable>::conformance_sample();
                let id = <$T as ConformanceTestable>::conformance_id();
                let _ = ModalityContract::provenance(&sample, id);
                let _ = ModalityContract::evidence_address(&sample);
                let _ = ModalityContract::policy_labels(&sample, id);
                let _ = ModalityContract::analytics_ops(&sample);
            }

            #[test]
            fn cdc_topic_iff_declared() {
                let sample = <$T as ConformanceTestable>::conformance_sample();
                if let Some(topic) = ModalityContract::cdc_topic(&sample) {
                    assert!(
                        !topic.is_empty(),
                        "a DECLARED cdc_topic must not be the empty string (use None instead)"
                    );
                }
            }

            // ── EG-P1-1: first-class TCK additions ──────────────────────────────

            /// TCK point (3): a corrupted `Put` payload must decode as `Err`, never
            /// silently succeed (and `decode_staged` — `serde_json`-backed — never
            /// panics on malformed input in the first place, only errors).
            #[test]
            fn malformed_payload_is_rejected_not_panicked() {
                let sample = <$T as ConformanceTestable>::conformance_sample();
                let id = <$T as ConformanceTestable>::conformance_id();
                let staged = ModalityContract::txn_stage(&sample, id);
                if staged.kind == $crate::WriteKind::Put {
                    let mut corrupt = staged.clone();
                    corrupt.payload = b"\xff\xfe not a valid payload for any codec".to_vec();
                    let decoded: Result<$T, _> = $crate::decode_staged(&corrupt);
                    assert!(
                        decoded.is_err(),
                        "a malformed Put payload must decode as Err, never silently succeed"
                    );
                }
            }

            /// TCK point (5): every declared `analytics_ops()` entry must be a
            /// non-empty, well-formed name (empty list is a legitimate "no typed
            /// query operators" default — an empty STRING entry would not be).
            #[test]
            fn typed_query_operators_are_well_formed() {
                let sample = <$T as ConformanceTestable>::conformance_sample();
                for op in ModalityContract::analytics_ops(&sample) {
                    assert!(
                        !op.is_empty(),
                        "a declared analytics_ops() entry must not be the empty string"
                    );
                }
            }

            /// EG-P1-1: computing this modality's full 12-point `TckReport` must
            /// cover EVERY point (never a subset), and registering it must make it
            /// discoverable via `registered_modalities()` — the mandatory runtime
            /// registry every `ModalityContract` implementer is now wired into via
            /// this macro, with no per-pilot edits required.
            #[test]
            fn tck_report_is_generated_and_registered() {
                let sample = <$T as ConformanceTestable>::conformance_sample();
                let name = ModalityContract::storage_kind(&sample);
                let report = $crate::tck_report::<$T>();
                assert_eq!(
                    report.modality, name,
                    "tck_report() must report under the modality's OWN storage_kind()"
                );
                assert_eq!(
                    report.results.len(),
                    $crate::TckPoint::ALL.len(),
                    "tck_report() must cover EVERY first-class TCK point, not a subset"
                );
                $crate::register_modality($crate::ModalityDescriptor {
                    name,
                    tck_report: $crate::tck_report::<$T>,
                });
                assert!(
                    $crate::registered_modalities().iter().any(|d| d.name == name),
                    "register_modality() must make this modality discoverable via registered_modalities()"
                );
                // Capture with `cargo test -p <crate> --features contract -- --nocapture`
                // for the capability-parity view — see eg-modality/README.md.
                println!("{}", report.render_table());
            }
        }
    };
}
