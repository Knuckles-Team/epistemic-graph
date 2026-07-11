//! `ModalityRegistry` — the mandatory runtime inventory of every `ModalityContract`
//! implementer that has registered itself (CONCEPT:E4 / EG-P1-1).
//!
//! ## Design decision: explicit registration, not `linkme`/`inventory`
//!
//! Neither `linkme` nor `inventory` is a dependency ANYWHERE in this workspace today
//! (checked across every `Cargo.toml` before writing this). Both work by planting
//! entries in a linker section and walking it at startup — real capabilities, but
//! each is a new proc-macro/build-time dependency this crate would be the FIRST to
//! pull in, and `eg-modality` is deliberately the thinnest possible seam (`eg-types`
//! only, per `lib.rs`'s crate docs) that every modality leaf must be able to depend on
//! without weight. This crate already has an established, zero-new-dependency pattern
//! for a process-wide registry: `std::sync::OnceLock` (used today in
//! `eg-tensor::gpu`, `eg-core::index`/`graph`, `eg-ann::distance`, `eg-plan::runtime`/
//! `cost`, `eg-query::cypher::proc`, `eg-compute::reasoning_closure`). This module
//! follows that exact precedent: a `OnceLock<Mutex<Vec<ModalityDescriptor>>>`, with an
//! explicit `register_modality()` call as the "startup" step, instead of a
//! proc-macro-planted static inventory.
//!
//! The tradeoff, stated plainly: `linkme`/`inventory` auto-discover every registration
//! compiled into the FINAL binary with zero call-site wiring (true "mandatory,
//! nothing to forget"). Explicit `register_modality()` requires SOMETHING to actually
//! call it — nothing walks the binary's sections for you. This increment makes that
//! call mandatory at the SOURCE level instead: `modality_conformance_tests!` (the
//! macro every `ModalityContract` implementer already invokes once, unconditionally)
//! now calls `register_modality()` itself as part of its generated test battery — so
//! every existing and future pilot is wired into the registry FOR FREE, with no edits
//! to any of the ~19 existing `contract.rs` files, the moment their `#[cfg(test)]`
//! conformance suite runs. The registry is process-scoped (each crate's own test/
//! server binary is its own process), so a full cross-crate inventory in one place
//! needs something ABOVE all modality crates to import + exercise each one (a future
//! `epistemic-graph`-level TCK binary) — see `README.md`'s "Cross-process caveat".
//!
//! If a future need arises for true zero-call-site, whole-binary auto-discovery (e.g.
//! a real multi-modality SERVER process that must enumerate every linked modality
//! without an explicit registration call anywhere), revisit `linkme` then — this
//! module's `ModalityDescriptor` shape does not change either way.

use std::fmt;
use std::sync::{Mutex, OnceLock};

use crate::tck::TckReport;

/// One registered modality: its stable [`ModalityContract::storage_kind`] name plus a
/// function pointer that (re)computes its full TCK capability report on demand.
///
/// `tck_report` is a plain `fn() -> TckReport`, not a boxed closure or a `dyn
/// ModalityContract` trait object — `ConformanceTestable` requires `Sized + Clone +
/// ...`, which is not object-safe, so the registry cannot hold modality VALUES
/// generically. What it needs from a modality is exactly one fact: "compute your own
/// capability report" — and `crate::tck_report::<T>` (the generic function
/// monomorphized for a concrete `T: ConformanceTestable`) already IS a concrete
/// `fn() -> TckReport` item, so it coerces to this field with no boxing/allocation.
#[derive(Clone, Copy)]
pub struct ModalityDescriptor {
    /// The modality's own `storage_kind()` name (`"tensor"`, `"geo"`, `"rdf"`, …).
    pub name: &'static str,
    /// Recomputes this modality's full 12-point TCK capability report.
    pub tck_report: fn() -> TckReport,
}

impl fmt::Debug for ModalityDescriptor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ModalityDescriptor")
            .field("name", &self.name)
            .finish_non_exhaustive()
    }
}

fn registry() -> &'static Mutex<Vec<ModalityDescriptor>> {
    static REGISTRY: OnceLock<Mutex<Vec<ModalityDescriptor>>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(Vec::new()))
}

/// Register a modality's descriptor. Idempotent by `name`: registering the same name
/// twice (e.g. a conformance suite that runs more than once in the same process)
/// REPLACES the earlier descriptor rather than accumulating a duplicate entry.
pub fn register_modality(descriptor: ModalityDescriptor) {
    let mut guard = registry()
        .lock()
        .expect("eg-modality registry mutex poisoned");
    if let Some(existing) = guard.iter_mut().find(|d| d.name == descriptor.name) {
        *existing = descriptor;
    } else {
        guard.push(descriptor);
    }
}

/// A snapshot of every modality registered so far IN THIS PROCESS. Order is
/// registration order (stable, not sorted) — callers that want a deterministic
/// display order should sort by `.name` themselves.
pub fn registered_modalities() -> Vec<ModalityDescriptor> {
    registry()
        .lock()
        .expect("eg-modality registry mutex poisoned")
        .clone()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tck::{TckPoint, TckPointResult, TckStatus};

    fn stub_report() -> TckReport {
        TckReport {
            modality: "registry-test-stub",
            results: vec![TckPointResult {
                point: TckPoint::InteropWorkloadSmoke,
                status: TckStatus::Pass,
            }],
        }
    }

    #[test]
    fn register_then_list_round_trips() {
        register_modality(ModalityDescriptor {
            name: "registry-test-stub",
            tck_report: stub_report,
        });
        let all = registered_modalities();
        assert!(all.iter().any(|d| d.name == "registry-test-stub"));
    }

    #[test]
    fn re_registering_the_same_name_replaces_not_duplicates() {
        let before = registered_modalities()
            .iter()
            .filter(|d| d.name == "registry-test-stub-dup")
            .count();
        assert_eq!(before, 0);
        register_modality(ModalityDescriptor {
            name: "registry-test-stub-dup",
            tck_report: stub_report,
        });
        register_modality(ModalityDescriptor {
            name: "registry-test-stub-dup",
            tck_report: stub_report,
        });
        let after = registered_modalities()
            .iter()
            .filter(|d| d.name == "registry-test-stub-dup")
            .count();
        assert_eq!(after, 1, "re-registration must replace, not accumulate");
    }
}
