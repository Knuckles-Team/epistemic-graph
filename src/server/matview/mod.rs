//! Plan-backed materialized views (CONCEPT:EG-KG.storage.plan-backed-matview).
//!
//! GENERALIZES the algo-only distributed-compute matview (`raft::pregel::MatView`, a
//! `DistAlgo`+result blob) into a NAMED, DURABLE `wire::Plan` over one graph. Defining a
//! view executes its plan once through the eg-plan runtime and caches the RESULT in the
//! version-keyed, RLS-aware result cache on the graph's `GraphCore`; a committed write
//! bumps the graph version (retiring the cached result) and the CDC hub marks the view
//! stale ([`note_change`]), so the next `Get` recomputes — never serving a stale result.
//!
//!   * [`manager`] — the process-global registry + freshness tracking + CDC invalidation.
//!   * [`lifecycle`] — result materialization, the result cache-KEY derivation, and the
//!     durable (de)serialize of a definition for the disjoint `plan_matviews` redb table.
//!
//! The handler (`server::handlers::dist_compute`) drives Define / Get / Refresh / Drop;
//! `reload_plan_matviews` re-hydrates the manager from redb on boot.

mod lifecycle;
mod manager;

pub use lifecycle::{decode_def, encode_def, materialize, plan_hash};
pub use manager::{manager, PlanMatView, PlanMatViewManager};

/// CDC hook (CONCEPT:EG-KG.storage.matview-cdc-invalidation): mark every plan-backed
/// matview over `graph` stale. Called from `CdcHub::emit` per committed change so a write
/// to an underlying graph retires the affected views. Returns how many it retired.
pub fn note_change(graph: &str) -> usize {
    manager().note_change(graph)
}
