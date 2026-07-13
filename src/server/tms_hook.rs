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
//!   * **Populate `register()`.** [`register_materialization`] closes this — Seam 3
//!     (X-6 across the storage boundary) wires it onto `Method::RegisterMaterialization`
//!     (feature `epistemic`, handler additionally gated `epistemic-tms`), so a caller
//!     (e.g. agent-utilities, right after writing a derived claim/summary/classification
//!     node + its `:DerivedFrom`/`:GeneratedBy` provenance edges) registers it ONCE over
//!     the wire — no engine-internal-only path anymore. The index still starts empty on
//!     every process; only ids a caller actually registered (or this module's own
//!     inline test registrations) are tracked.
//!   * **Surface staleness anywhere BEYOND a per-id status read.** `on_change`'s returned
//!     id set is still dropped after logging in [`notify`]; [`status_of`] (wired to
//!     `Method::MaterializationStatus`) answers "is THIS id stale now", but a bulk
//!     "what's everything stale" feed (a recompute scheduler, a metric) is a separate,
//!     larger workstream.
//!   * **A `ServerState`-held, per-process-restart-durable index.** The global index is
//!     in-memory only and resets on restart, same posture as `eg-epistemic`'s existing
//!     in-memory `TruthMaintenance` (no persistence layer exists for it yet anywhere in
//!     the crate).

use std::path::{Path, PathBuf};
use std::sync::{Mutex, MutexGuard, OnceLock};

use eg_epistemic::{ChangeEvent, TruthMaintenance};

use crate::protocol::Method;

/// The process-global truth-maintenance index (CONCEPT:EG-KG.epistemic.truth-maintenance).
/// Lazily created on first use — mirrors [`crate::server::replica::global_log`]'s
/// singleton idiom. Unlike that log (armed only when an env var opts in), this index is
/// always live once the `epistemic-tms` feature is compiled in: tracking an empty
/// registry costs nothing, and there is no separate "off" posture to opt out of within
/// an `epistemic-tms` build. When `GRAPH_SERVICE_PERSIST_DIR` (the SAME persist-dir
/// convention `KvStore`/the blob store/the graph's own redb tier already use) is set,
/// the index is seeded from its last [`persist`]ed snapshot instead of starting empty —
/// see the "Persistence" section below.
pub fn global_index() -> &'static Mutex<TruthMaintenance> {
    static INDEX: OnceLock<Mutex<TruthMaintenance>> = OnceLock::new();
    INDEX.get_or_init(|| Mutex::new(load_persisted()))
}

// ── Persistence (CONCEPT:EG-KG.epistemic.truth-maintenance) ──────────────────────────
//
// The index above is a `'static` process-global singleton, deliberately NOT a
// `ServerState` field (see the module docs' "Why a process-global" section) — so
// persistence follows the SAME env-var-driven, zero-`ServerState`-surface shape rather
// than threading a `PersistenceBackend`/`KvStore` handle through. `GRAPH_SERVICE_PERSIST_DIR`
// is the canonical "durable when set, ephemeral scratch when absent" switch every other
// server-side substate (KvStore, the blob store, the graph's own redb tier) already
// honors — reusing it means an operator who already runs with a persist dir gets this
// for free, and one who doesn't (a scratch/test/in-memory deployment) pays nothing.
//
// This is a best-effort flat-file snapshot (msgpack + atomic rename), NOT wired onto
// the graph's own WAL/redb commit path: it is a side index over derived materializations,
// not a source of truth for the graph itself, so a crash between a mutation and its
// snapshot loses at most that one mutation's tracked state — self-healing, since
// `maybe_register_from_write`'s very next write to the SAME id re-derives its current
// provenance from the graph regardless of what the stale snapshot said.

/// `{GRAPH_SERVICE_PERSIST_DIR}/tms_index.msgpack`, or `None` when no persist dir is
/// configured (ephemeral, matches the graph's own "no persist dir ⇒ in-memory" posture).
fn persist_path() -> Option<PathBuf> {
    std::env::var("GRAPH_SERVICE_PERSIST_DIR")
        .ok()
        .filter(|d| !d.is_empty())
        .map(|dir| PathBuf::from(dir).join("tms_index.msgpack"))
}

/// Deserialize a [`TruthMaintenance`] snapshot from `path`. Any failure (missing file,
/// truncated/corrupt write, schema drift across a version bump) falls back to an EMPTY
/// index rather than erroring the whole server boot — a lost TMS index is, at worst, a
/// cold-start re-registration wave as callers re-register/writes re-derive, never a
/// correctness problem (see the module docs above).
fn load_from(path: &Path) -> TruthMaintenance {
    std::fs::read(path)
        .ok()
        .and_then(|bytes| rmp_serde::from_slice(&bytes).ok())
        .unwrap_or_default()
}

/// [`load_from`] off [`persist_path`], or an empty index when no persist dir is set.
fn load_persisted() -> TruthMaintenance {
    persist_path().map(|p| load_from(&p)).unwrap_or_default()
}

/// Best-effort snapshot of the WHOLE index to `path`: write to a `.tmp` sibling, then
/// atomically rename over the real path, so a reader (the next boot's [`load_from`])
/// never observes a partially-written file.
fn persist_to(path: &Path, idx: &TruthMaintenance) {
    let Ok(bytes) = rmp_serde::to_vec_named(idx) else {
        return;
    };
    let tmp = path.with_extension("msgpack.tmp");
    if let Some(parent) = path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    if std::fs::write(&tmp, &bytes).is_ok() {
        let _ = std::fs::rename(&tmp, path);
    }
}

/// [`persist_to`] off [`persist_path`] — a no-op (no disk I/O at all) when no persist
/// dir is configured.
fn persist(idx: &TruthMaintenance) {
    if let Some(path) = persist_path() {
        persist_to(&path, idx);
    }
}

/// Refresh the `epistemic_materializations_stale` gauge (CONCEPT:EG-OS.observability.engine-metrics)
/// to the index's CURRENT stale count — called after every mutation to the index so the
/// gauge is always the live truth, not a point-in-time snapshot that drifts.
fn refresh_stale_gauge(idx: &TruthMaintenance) {
    crate::metrics::set_epistemic_materializations_stale(idx.stale().len() as i64);
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
        Some(event) => {
            let mut idx = lock_index();
            let staled = idx.on_change(&event);
            if !staled.is_empty() {
                crate::metrics::epistemic_materializations_staled(staled.len() as u64);
            }
            refresh_stale_gauge(&idx);
            persist(&idx);
            staled
        }
        None => Default::default(),
    }
}

/// Live single-id auto-registration fast path (CONCEPT:EG-KG.epistemic.truth-maintenance,
/// SURPASS item "auto-register materializations on write"): resolve `id`'s CURRENT
/// provenance directly off the live `core` (its own `invalidation_deps` property plus
/// any outgoing `:DerivedFrom`/`:GeneratedBy` edge — the SAME two channels
/// [`eg_epistemic::register_from_provenance`] reads off a whole-graph snapshot) via
/// [`eg_epistemic::resolve_provenance`], and (re-)register it iff at least one
/// provenance channel is actually present. Reads are O(out-degree of `id`)
/// (`GraphCore::get_node_properties`/`get_successors`/`get_edge_properties`, all
/// targeted DashMap/adjacency lookups) — NOT a whole-graph `analysis_snapshot` clone —
/// so this is cheap enough to call on every commit touching `id`, unlike the explicit
/// `Method::RegisterMaterialization` RPC (which already pays for a snapshot the caller
/// requested).
///
/// A no-op for an id with NEITHER channel (an ordinary base fact, not a derived
/// materialization) — auto-registration must not flood the index with every plain node
/// ever written, only nodes actually carrying invalidation-dependency provenance.
/// Idempotent + self-healing: re-registering the SAME id replaces its dependency set
/// entirely with whatever the graph says NOW (mirrors [`TruthMaintenance::register`]'s
/// replace-not-merge semantics), so calling this on every write touching `id` converges
/// on the correct, current provenance even when `invalidation_deps` and the
/// `:GeneratedBy` edge are written in separate commits.
pub fn maybe_register_from_write(core: &eg_core::graph::GraphCore, id: &str) {
    let node_props = core.get_node_properties(id);
    let outgoing: Vec<(String, Vec<u8>)> = core
        .get_successors(id)
        .unwrap_or_default()
        .into_iter()
        .flat_map(|target| {
            core.get_edge_properties(id, &target)
                .into_iter()
                .map(move |blob| (target.clone(), blob))
        })
        .collect();
    let (depends_on, generating_activity) = eg_epistemic::resolve_provenance(
        node_props.as_deref(),
        outgoing.iter().map(|(t, b)| (t.clone(), b.as_slice())),
    );
    if depends_on.is_empty() && generating_activity.is_none() {
        return; // not a derived node -- nothing to track
    }
    let mut idx = lock_index();
    idx.register(id, depends_on, generating_activity);
    refresh_stale_gauge(&idx);
    persist(&idx);
}

/// The [`Method`]-dispatch counterpart of [`maybe_register_from_write`] — the ONE call
/// site both `src/server/mutation.rs::commit_finalize` and `src/server/dispatch.rs`'s
/// legacy tail invoke (mirroring `notify`'s own two call sites) right after a commit
/// succeeds. Covers the three single-row write shapes that can introduce or update
/// provenance: a fresh/upserted node (`AddNode`, in case its `invalidation_deps`
/// property was just written), a field update (`CompareAndSetNodeFields`, same reason),
/// and a fresh edge (`AddEdge`, in case it is a `:DerivedFrom`/`:GeneratedBy` edge) —
/// resolved for the SOURCE node, since that is the id [`eg_epistemic::resolve_provenance`]
/// tracks dependencies FOR. No-op for every other method (reads, removals — a removal
/// is [`notify`]'s job, not this one's).
pub fn auto_register_from_write(core: &eg_core::graph::GraphCore, method: &Method) {
    match method {
        Method::AddNode { node_id, .. } => maybe_register_from_write(core, node_id),
        Method::CompareAndSetNodeFields { node_id, .. } => maybe_register_from_write(core, node_id),
        Method::AddEdge { source_id, .. } => maybe_register_from_write(core, source_id),
        _ => {}
    }
}

/// Seam 3 (CONCEPT:EG-KG.epistemic.truth-maintenance, X-6 wire surface) — the
/// server-side body of `Method::RegisterMaterialization`: read `derived_id`'s OWN
/// already-stored provenance (`invalidation_deps` property + outgoing
/// `:DerivedFrom`/`:GeneratedBy` edges) off `view` via
/// [`eg_epistemic::register_from_provenance`] and register it on the SAME global
/// index [`notify`] feeds — from this point on, a committed mutation touching any
/// of its dependencies automatically stales/retracts it. Returns the freshly
/// registered [`eg_epistemic::Materialization`] (always `Fresh` immediately after
/// registering) so the caller can confirm exactly what dependency set was resolved.
pub fn register_materialization(
    view: &eg_core::graph::GraphView,
    derived_id: &str,
) -> eg_epistemic::Materialization {
    let mut idx = lock_index();
    eg_epistemic::register_from_provenance(&mut idx, view, derived_id);
    let m = idx
        .get(derived_id)
        .cloned()
        .expect("register_from_provenance always inserts derived_id");
    refresh_stale_gauge(&idx);
    persist(&idx);
    m
}

/// Seam 3 — the server-side body of `Method::MaterializationStatus`: the current
/// [`eg_epistemic::MaterializationStatus`] of `id` on the SAME global index
/// [`register_materialization`]/[`notify`] read and write, or `None` if `id` was
/// never registered.
pub fn status_of(id: &str) -> Option<eg_epistemic::MaterializationStatus> {
    lock_index().status_of(id)
}

/// Seam 3 follow-up (SURPASS item "give staleness a consumer") — the server-side body
/// of `Method::StaleMaterializations`: every currently `Stale` materialization id on
/// the SAME global index, so a caller (a scheduler, a recompute sweep, an operator) can
/// discover "what needs re-answering" WITHOUT already knowing which ids to poll one at
/// a time via [`status_of`]. The other consumer is the `epistemic_materializations_stale`
/// Prometheus gauge [`refresh_stale_gauge`] keeps live on every mutation to the index.
pub fn stale_ids() -> std::collections::BTreeSet<String> {
    lock_index().stale()
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

    // --- auto-register-on-write (SURPASS item: "auto-register materializations on write") ---

    fn node_blob(props: serde_json::Value) -> Vec<u8> {
        rmp_serde::to_vec_named(&props).unwrap()
    }

    fn edge_blob(relationship_type: &str) -> Vec<u8> {
        rmp_serde::to_vec_named(&serde_json::json!({ "relationship_type": relationship_type }))
            .unwrap()
    }

    /// A node carrying `invalidation_deps` auto-registers the moment
    /// [`maybe_register_from_write`] runs against it (no explicit
    /// `Method::RegisterMaterialization` call needed) — reading LIVE off `GraphCore`,
    /// not a whole-graph snapshot. A subsequent change to one of its dependencies
    /// stales it, proving the auto-registered entry is wired into the SAME live index
    /// [`notify`] feeds.
    #[test]
    fn maybe_register_from_write_auto_registers_a_node_with_invalidation_deps() {
        let _guard = TESTS_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
        let core = eg_core::graph::GraphCore::new();
        core.add_node(
            "claim:auto1".to_string(),
            node_blob(serde_json::json!({
                "type": "Claim",
                "invalidation_deps": ["snapshot:g1@1"],
            })),
        );

        maybe_register_from_write(&core, "claim:auto1");
        assert_eq!(
            lock_index().status_of("claim:auto1"),
            Some(eg_epistemic::MaterializationStatus::Fresh)
        );

        let staled = notify(&Method::RemoveNode {
            node_id: "snapshot:g1@1".to_string(),
        });
        assert!(
            staled.contains("claim:auto1"),
            "auto-registered claim:auto1 must go stale when its dependency is removed; got {staled:?}"
        );
    }

    /// An outgoing `:GENERATED_BY` edge, written via a SEPARATE call than the node
    /// itself, is picked up on ITS OWN [`maybe_register_from_write`] call — the live
    /// resolver re-derives the FULL current provenance from the graph each time
    /// (replace-not-merge), so a node's dependency set converges to correct even when
    /// its provenance channels arrive across multiple commits.
    #[test]
    fn maybe_register_from_write_picks_up_generated_by_edge_added_later() {
        let _guard = TESTS_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
        let core = eg_core::graph::GraphCore::new();
        core.add_node("claim:auto2".to_string(), node_blob(serde_json::json!({})));
        core.add_node(
            "activity:auto2".to_string(),
            node_blob(serde_json::json!({})),
        );
        // No provenance yet -- nothing to track.
        maybe_register_from_write(&core, "claim:auto2");
        assert_eq!(lock_index().status_of("claim:auto2"), None);

        core.add_edge(
            "claim:auto2".to_string(),
            "activity:auto2".to_string(),
            edge_blob("GENERATED_BY"),
        )
        .unwrap();
        maybe_register_from_write(&core, "claim:auto2");
        let m = lock_index().get("claim:auto2").cloned();
        assert_eq!(
            m.map(|m| m.generating_activity),
            Some(Some("activity:auto2".to_string()))
        );
    }

    /// A plain node with NEITHER provenance channel must NOT be auto-registered — the
    /// hook is scoped to derived materializations, not every node ever written.
    #[test]
    fn maybe_register_from_write_ignores_a_plain_base_fact() {
        let _guard = TESTS_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
        let core = eg_core::graph::GraphCore::new();
        core.add_node(
            "fact:plain".to_string(),
            node_blob(serde_json::json!({ "type": "Fact", "value": 42 })),
        );
        maybe_register_from_write(&core, "fact:plain");
        assert_eq!(lock_index().status_of("fact:plain"), None);
    }

    /// [`auto_register_from_write`] dispatches `AddNode`/`AddEdge`/
    /// `CompareAndSetNodeFields` to [`maybe_register_from_write`] against the right id
    /// (the SOURCE for `AddEdge`) and ignores every other method shape.
    #[test]
    fn auto_register_from_write_dispatches_the_three_write_shapes() {
        let _guard = TESTS_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
        let core = eg_core::graph::GraphCore::new();
        core.add_node(
            "claim:disp".to_string(),
            node_blob(serde_json::json!({ "invalidation_deps": ["base:1"] })),
        );
        auto_register_from_write(
            &core,
            &Method::AddNode {
                node_id: "claim:disp".to_string(),
                properties_msgpack: Vec::new(),
            },
        );
        assert_eq!(
            lock_index().status_of("claim:disp"),
            Some(eg_epistemic::MaterializationStatus::Fresh)
        );

        // A read-only method is a no-op.
        auto_register_from_write(&core, &Method::GetNodes);
        // (no assertion needed beyond "does not panic" -- GetNodes carries no id)
    }

    // --- stale_ids: the bulk "what's stale" consumer -----------------------------

    #[test]
    fn stale_ids_returns_every_currently_stale_materialization() {
        let _guard = TESTS_MUTEX.lock().unwrap_or_else(|p| p.into_inner());
        {
            let mut idx = lock_index();
            idx.register("stale_a", ["dep_x"], None);
            idx.register("stale_b", ["dep_x"], None);
            idx.register("untouched", ["dep_y"], None);
        }
        notify(&Method::RemoveNode {
            node_id: "dep_x".to_string(),
        });
        let stale = stale_ids();
        assert!(stale.contains("stale_a"));
        assert!(stale.contains("stale_b"));
        assert!(!stale.contains("untouched"));
    }

    // --- persistence round-trip ---------------------------------------------------

    /// [`persist_to`]/[`load_from`] round-trip a whole index byte-for-byte:
    /// registered materializations, their dependency sets, and a `Stale` status all
    /// survive the msgpack snapshot + reload, independent of the process-global
    /// singleton (this test drives the pure file-path functions directly, never
    /// touching `GRAPH_SERVICE_PERSIST_DIR` or the shared `global_index()`, so it is
    /// safe under parallel test execution).
    #[test]
    fn persist_to_and_load_from_round_trip_a_whole_index() {
        let dir = std::env::temp_dir().join(format!(
            "eg-tms-hook-test-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("tms_index.msgpack");

        let mut idx = TruthMaintenance::new();
        idx.register(
            "claim:persisted",
            ["dep_1", "dep_2"],
            Some("activity:1".to_string()),
        );
        idx.on_change(&ChangeEvent::Updated("dep_1".to_string()));
        assert_eq!(
            idx.status_of("claim:persisted"),
            Some(eg_epistemic::MaterializationStatus::Stale)
        );

        persist_to(&path, &idx);
        assert!(path.exists(), "persist_to must write the snapshot file");

        let reloaded = load_from(&path);
        assert_eq!(
            reloaded.status_of("claim:persisted"),
            Some(eg_epistemic::MaterializationStatus::Stale)
        );
        assert_eq!(
            reloaded.get("claim:persisted").unwrap().depends_on,
            idx.get("claim:persisted").unwrap().depends_on
        );
        assert_eq!(
            reloaded.get("claim:persisted").unwrap().generating_activity,
            Some("activity:1".to_string())
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Loading from a path that does not exist (a fresh deployment, or no persist dir
    /// configured) returns an EMPTY index rather than erroring — a lost/absent TMS
    /// snapshot is never a boot-blocking failure.
    #[test]
    fn load_from_a_missing_file_returns_an_empty_index() {
        let missing = std::env::temp_dir().join(format!(
            "eg-tms-hook-test-missing-{}-{:?}.msgpack",
            std::process::id(),
            std::thread::current().id()
        ));
        let idx = load_from(&missing);
        assert!(idx.stale().is_empty());
        assert_eq!(idx.status_of("anything"), None);
    }
}
