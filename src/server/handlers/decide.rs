//! The Decide layer's served surface (RF-ADR-010).
//!
//! Four methods, four files: assembly, its commit, the statistical executor
//! and the two admin jobs. Each entry point's signature is frozen by the
//! contract wave; the package that lands a handler replaces only its body.

/// The server state every Decide entry point is handed.
type SharedState = std::sync::Arc<tokio::sync::RwLock<crate::server::state::ServerState>>;

pub(crate) mod assemble;
pub(crate) mod commit;
pub(crate) mod jobs;
pub(crate) mod log;
#[cfg(feature = "decide")]
pub(crate) mod shape;
pub(crate) mod statistical;

// The statistical package (EH-056 ... EH-074): candidate sources, the
// decision executor, the NL template binding and the two admin jobs.
#[cfg(feature = "decide")]
mod candidates;
#[cfg(feature = "decide")]
mod stat_adapter;
#[cfg(feature = "decide")]
mod stat_belief;
#[cfg(feature = "decide")]
mod stat_classes;
#[cfg(feature = "decide")]
mod stat_decide;
#[cfg(feature = "decide")]
mod stat_evaluator;
#[cfg(feature = "decide")]
mod stat_executor;
#[cfg(feature = "decide")]
mod stat_generation;
#[cfg(feature = "decide")]
mod stat_jobs;
#[cfg(feature = "decide")]
mod stat_learning;
#[cfg(feature = "decide")]
mod stat_log;
#[cfg(feature = "decide")]
mod stat_nl;
#[cfg(feature = "decide")]
mod stat_pointer;
#[cfg(feature = "decide")]
mod stat_replay;
#[cfg(feature = "decide")]
mod stat_resolve;
#[cfg(feature = "decide")]
mod stat_retention;
#[cfg(feature = "decide")]
mod stat_slate;
#[cfg(feature = "decide")]
mod stat_support;
#[cfg(feature = "decide")]
mod stat_vectors;
// EH-394/395: retrieval outcomes on the decision log.
#[cfg(feature = "decide")]
mod stat_retrieval;
// EH-394..398: the retrieval-learning relations of the decision views.
#[cfg(all(feature = "decide", feature = "query"))]
mod stat_retrieval_views;
// EH-396: the query adapter on the served read paths.
#[cfg(feature = "decide")]
pub(crate) mod served_adapter;
#[cfg(all(test, feature = "decide"))]
mod stat_tests;
// EH-066 — the decision record views (SQL catalog relations + the UQL `DECISIONS` source).
#[cfg(all(feature = "decide", feature = "query"))]
mod stat_view;
#[cfg(feature = "decide")]
mod telemetry;

// Swarm topology through the assembly decision (SWARM-TOPOLOGY-DECIDE-DESIGN):
// the schema entailments and capacity headroom a topology question reads, and
// the publish-time topology shape check.
#[cfg(feature = "decide")]
mod capacity_premise;
#[cfg(feature = "decide")]
mod topology_read;
#[cfg(feature = "decide")]
mod topology_schema;
pub(crate) mod topology_shape;

pub(crate) use assemble::handle_agent_assemble;
pub(crate) use commit::handle_decision_commit;
pub(crate) use jobs::{handle_decision_eval, handle_decision_fit};
pub(crate) use log::handle_decision_log;
pub(crate) use statistical::handle_decide;

/// The caller's read-only SQL relations — the decision record views (EH-066) — when
/// `query` can see them; `None` otherwise, or in a build without the Decide layer.
#[cfg(feature = "query")]
pub(crate) async fn read_only_relations(
    state: &SharedState,
    authority: &crate::server::access::CarrierAuthority,
    query: &str,
) -> Option<crate::server::sql_catalog_acl::SharedRelations> {
    #[cfg(feature = "decide")]
    {
        if !crate::server::sql_catalog_acl::wants_read_only_relations(query) {
            return None;
        }
        let views = stat_view::DecisionViews::served(state, authority).await?;
        Some(views as crate::server::sql_catalog_acl::SharedRelations)
    }
    #[cfg(not(feature = "decide"))]
    {
        let _ = (state, authority, query);
        None
    }
}

/// The caller's visible decision log as a UQL `DECISIONS` source (EH-066).
#[cfg(all(feature = "decide", feature = "query"))]
pub(crate) async fn decision_source(
    state: &SharedState,
    authority: &crate::server::access::CarrierAuthority,
) -> Option<std::sync::Arc<dyn eg_plan::exec::DecisionSource>> {
    let views = stat_view::DecisionViews::served(state, authority).await?;
    Some(views as std::sync::Arc<dyn eg_plan::exec::DecisionSource>)
}
