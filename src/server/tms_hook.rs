//! CONCEPT:EG-KG.epistemic.truth-maintenance — the server-side half of EPI-P3-2's
//! `eg_epistemic::recompute` module docs ("Follow-up: wiring a live CDC hook"): feeds a
//! COMMITTED base-fact mutation into `eg-epistemic`'s [`TruthMaintenance`] index so
//! anything transitively derived from a deleted/updated node/edge is marked `Stale`
//! rather than silently going stale unnoticed.
//!
//! ## Why a process-global, not a `ServerState` field
//!
//! Every other cross-cutting server facility (blob, kv, cdc, tsdb, …) is a
//! `ServerState` field, but that convention means ~40 test-fixture `ServerState { .. }`
//! literals across the tree would need a new cfg-gated field the moment this lands —
//! exactly the churn `lake`/`dataset_handles` already impose (see those fields' own
//! history). This hook instead mirrors the LEANER existing precedent,
//! [`crate::server::replica::global_log`]: a lazily-initialized `'static` singleton,
//! reached by a free function, with zero `ServerState` surface and zero cost when the
//! `epistemic-tms` feature is off (the whole module does not exist).
//!
//! ## Mapping — deliberately narrow
//!
//! [`change_event_for_method`] maps ONLY the two unambiguous single-row mutations:
//!   * `RemoveNode`/`RemoveEdge` → [`ChangeEvent::Deleted`] (the id/edge itself is gone).
//!   * `CompareAndSetNodeFields` → [`ChangeEvent::Updated`] (its whole contract is
//!     "still exists, content may have changed" — same as [`crate::server::cdc`]'s
//!     `emit_for_method`, which emits `UpdateNode` unconditionally for a CAS even when
//!     the compare failed and nothing actually changed; harmless idempotent restale).
//!
//! A plain `AddNode` is deliberately NOT mapped here: [`crate::server::cdc::emit_for_method`]
//! disambiguates create-vs-update by reading the node's PRE-image (`CdcPre::Node.before`)
//! before the write applies; this seam has no pre-image capture, so folding `AddNode` in
//! would either falsely `Updated` a brand-new id (nothing was tracking it yet, so
//! harmless in practice, but semantically wrong) or need the exact same `capture_before`
//! plumbing CDC already has. `PolicyChanged`/`ModelRetired`/`OntologyEvolved` are not on
//! ANY wire method yet (per the module docs of `eg_epistemic::recompute`) so they have no
//! `Method` to map from at all.
//!
//! ## Full wiring — what is NOT done here
//!
//! This hook fires on every commit path that already routes `RemoveNode`/`RemoveEdge`/
//! `CompareAndSetNodeFields` through [`crate::server::mutation::commit_finalize`] (the
//! GATEWAY_ROUTED majority of graph-core writes) and, for completeness, the legacy
//! dispatch-shell tail (`crate::server::dispatch`) that the NOT-yet-gateway-routed
//! remainder still uses. What it does NOT do:
//!   * **Populate `register()`.** Nothing calls [`TruthMaintenance::register`] yet — the
//!     index starts empty on every process, so `on_change` has nothing to stale until a
//!     caller registers materializations (EPI-P3-1's storage-level `:DerivedFrom`/
//!     `:GeneratedBy` edges are the intended future source — see the crate's own module
//!     docs). Proving the HOOK fires (this module's test) does not require that; it
//!     `register`s a materialization inline to observe the transition.
//!   * **Surface staleness anywhere.** `on_change`'s returned id set is currently
//!     dropped after logging — a real consumer (a recompute scheduler, a served
//!     `TmsStale` query method, a metric) is a separate, larger workstream.
//!   * **A `ServerState`-held, per-process-restart-durable index.** The global index is
//!     in-memory only and resets on restart, same posture as `eg-epistemic`'s existing
//!     in-memory `TruthMaintenance` (no persistence layer exists for it yet anywhere in
//!     the crate).

use std::sync::{Mutex, MutexGuard, OnceLock};

use eg_epistemic::{ChangeEvent, TruthMaintenance};

use crate::protocol::Method;

/// The process-global truth-maintenance index (CONCEPT:EG-KG.epistemic.truth-maintenance).
/// Lazily created on first use — mirrors [`crate::server::replica::global_log`]'s
/// singleton idiom. Unlike that log (armed only when an env var opts in), this index is
/// always live once the `epistemic-tms` feature is compiled in: tracking an empty
/// registry costs nothing, and there is no separate "off" posture to opt out of within
/// an `epistemic-tms` build.
pub fn global_index() -> &'static Mutex<TruthMaintenance> {
    static INDEX: OnceLock<Mutex<TruthMaintenance>> = OnceLock::new();
    INDEX.get_or_init(|| Mutex::new(TruthMaintenance::new()))
}

/// Lock the global index. A poisoned lock (a prior panic while held) recovers the
/// inner state rather than propagating the poison — a truth-maintenance index going
/// stale-tracking-stale is never worth taking the whole write path down.
fn lock_index() -> MutexGuard<'static, TruthMaintenance> {
    global_index()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Map a committed base-fact mutation onto the [`ChangeEvent`] vocabulary
/// `TruthMaintenance::on_change` consumes. See the module docs for exactly which
/// `Method` variants are covered and why. Returns `None` for every method this seam
/// does not (yet) map — never an error, since "not a base-fact mutation this hook
/// understands" is the overwhelmingly common case (reads, admin ops, everything this
/// module's docs list as future work).
pub fn change_event_for_method(method: &Method) -> Option<ChangeEvent> {
    match method {
        Method::RemoveNode { node_id } => Some(ChangeEvent::Deleted(node_id.clone())),
        Method::RemoveEdge {
            source_id,
            target_id,
        } => Some(ChangeEvent::Deleted(format!("{source_id}->{target_id}"))),
        Method::CompareAndSetNodeFields { node_id, .. } => {
            Some(ChangeEvent::Updated(node_id.clone()))
        }
        _ => None,
    }
}

/// Feed a committed mutation's [`ChangeEvent`] (if any) into the global
/// truth-maintenance index. Call AFTER the mutation has committed (durably, when a
/// durable tier is active) — mirroring the CDC-emit ordering at both call sites (see
/// module docs) — so a rolled-back/errored write never spuriously stales anything.
/// Returns every materialization id the event staled/retracted (empty when `method`
/// maps to no event, or nothing tracked depends on the changed id).
pub fn notify(method: &Method) -> std::collections::BTreeSet<String> {
    match change_event_for_method(method) {
        Some(event) => lock_index().on_change(&event),
        None => Default::default(),
    }
}

/// Serializes this module's tests against the shared process-global index (tests in
/// the same binary otherwise race each other through the ONE `global_index()`).
#[cfg(test)]
static TESTS_MUTEX: Mutex<()> = Mutex::new(());

#[cfg(test)]
mod tests {
    use super::*;

    /// A `RemoveNode` fed through [`notify`] must (a) map to `ChangeEvent::Deleted`
    /// and (b) actually reach the shared index and mark a registered dependent
    /// `Stale` — proving the hook end-to-end, not just the pure mapping function.
    #[test]
    fn remove_node_notify_stales_a_registered_dependent() {
        let _guard = TESTS_MUTEX.lock().unwrap_or_else(|p| p.into_inner());

        // Register a materialization derived from "fact_a" directly on the SAME
        // global index `notify` writes to (proving the process-global seam works,
        // not a private instance).
        {
            let mut idx = lock_index();
            idx.register("summary_1", ["fact_a"], None);
            assert_eq!(
                idx.status_of("summary_1"),
                Some(eg_epistemic::MaterializationStatus::Fresh)
            );
        }

        let method = Method::RemoveNode {
            node_id: "fact_a".to_string(),
        };
        assert_eq!(
            change_event_for_method(&method),
            Some(ChangeEvent::Deleted("fact_a".to_string()))
        );

        let staled = notify(&method);
        assert!(
            staled.contains("summary_1"),
            "RemoveNode(fact_a) must stale summary_1, which depends on it; got {staled:?}"
        );
        assert_eq!(
            lock_index().status_of("summary_1"),
            Some(eg_epistemic::MaterializationStatus::Stale)
        );

        // A method this hook does not map produces no event and stales nothing.
        assert_eq!(change_event_for_method(&Method::GetNodes), None);
        assert!(notify(&Method::GetNodes).is_empty());
    }

    /// `CompareAndSetNodeFields` maps to `Updated`, never `Deleted` — a dependent
    /// goes `Stale`, but re-registering the SAME id (as a fresh recompute would)
    /// must succeed, unlike a `Deleted` subject which the module docs say retracts
    /// the tracked-at-that-id materialization outright (not exercised here — this
    /// test only proves the `Updated` mapping + propagation).
    #[test]
    fn compare_and_set_notify_maps_to_updated_and_propagates() {
        let _guard = TESTS_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
        {
            let mut idx = lock_index();
            idx.register("summary_2", ["fact_b"], None);
        }
        let method = Method::CompareAndSetNodeFields {
            node_id: "fact_b".to_string(),
            conditions_msgpack: Vec::new(),
            updates_msgpack: Vec::new(),
        };
        assert_eq!(
            change_event_for_method(&method),
            Some(ChangeEvent::Updated("fact_b".to_string()))
        );
        let staled = notify(&method);
        assert!(staled.contains("summary_2"));
    }
}
