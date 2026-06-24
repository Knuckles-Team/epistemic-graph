//! Write/read classification + isolation-ACL enforcement for graph ops.

use crate::isolation::{AccessLevel, IsolationLayer};
use crate::protocol::Method;

/// Whether a graph-targeted method mutates the target graph (Write) or only
/// reads from it (Read). Pure-compute methods (finance, datascience, parse)
/// never touch graph state and classify as Read.
pub(crate) fn requires_write(method: &Method) -> bool {
    // `AddTriples` (feature `rdf`) writes nodes + edges into the target graph.
    #[cfg(feature = "rdf")]
    if matches!(method, Method::AddTriples { .. }) {
        return true;
    }
    matches!(
        method,
        Method::AddNode { .. }
            | Method::RemoveNode { .. }
            | Method::CompareAndSetNodeFields { .. }
            | Method::AddEdge { .. }
            | Method::RemoveEdge { .. }
            | Method::ClearGraph
            | Method::AddEmbedding { .. }
            | Method::PruneByLifecycle { .. }
            | Method::BatchUpdate { .. }
            | Method::EvictLRU { .. }
            | Method::DecaySweep { .. }
            | Method::TouchNodes { .. }
            | Method::FromMsgpack { .. }
            | Method::ClearLedger
            | Method::ApplyLedger { .. }
            | Method::CompactNodesByType { .. }
            | Method::RunDatalogReasoning { .. }
            | Method::Reconcile { .. }
            | Method::ApplyMutation { .. }
            | Method::ApplyMultisigMutation { .. }
            | Method::ParseRepository { .. }
            | Method::DeleteGraph { .. }
    )
}

/// Enforce the isolation ACL for a graph-targeted operation.
///
/// Back-compat invariant: while no identities are registered the layer has no
/// rules and everything is allowed (single-tenant deployments are unchanged).
/// Once rules exist, `check_access` decides: peer agent graphs are denied,
/// managers reach subordinate graphs, team graphs are member-read/manager-write,
/// the `__commons__` stays open to all authenticated agents.
pub(crate) fn check_graph_access(
    isolation: &IsolationLayer,
    caller: Option<&str>,
    graph_name: &str,
    graph_type: crate::protocol::GraphType,
    graph_owner: Option<&str>,
    access: AccessLevel,
) -> Result<(), String> {
    if !isolation.has_rules() {
        return Ok(());
    }
    let agent = caller.unwrap_or("");
    if isolation.check_access(agent, graph_name, graph_type, graph_owner, access) {
        Ok(())
    } else {
        crate::metrics::access_denied();
        Err(format!(
            "ACCESS_DENIED: agent '{}' lacks {:?} access to graph '{}'",
            if agent.is_empty() {
                "<anonymous>"
            } else {
                agent
            },
            access,
            graph_name
        ))
    }
}
