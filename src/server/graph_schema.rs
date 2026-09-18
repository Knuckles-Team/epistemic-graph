//! `Method::GraphSchema` and `Method::GraphSchemaList` (X9): keyed shapes and
//! ontology sources on one request graph.
//!
//! The write path is GATEWAY-routed, exactly like `IcvConfigure`: the arm runs
//! inside the graph commit kernel, so an attach is audited, emits CDC and is
//! ordered against every other write to that graph. The read is a separate
//! method because a read op inside a gateway-routed method would need a
//! runtime-conditional gateway plan.
//!
//! Owned after S1 by the schema-sources package, which replaces both stub
//! bodies with composition, validation and the memoised composed digest. The
//! stubs return BEFORE `commit_gateway`, so nothing is staged.

pub(crate) mod attach_pack;

use std::sync::Arc;

use crate::graph::GraphCore;

use crate::protocol::{Method, Response};
use crate::server::contract_wave::not_yet_served;
use crate::server::mutation::{MutationCtx, MutationPlan};

/// Attach, replace or detach one keyed schema source, inside the gateway.
///
/// The op match is exhaustive and has no catch-all: `AttachPack` resolves a
/// connector's pack head and belongs to a different package from the operator
/// attach, so it must stay separately routed.
pub(crate) async fn handle_gateway(
    ctx: &MutationCtx<'_>,
    _plan: &MutationPlan,
    _method: &Method,
    op: &eg_types::graph_schema::GraphSchemaOp,
) -> Response {
    use eg_types::graph_schema::GraphSchemaOp;

    match op {
        GraphSchemaOp::Attach { .. } | GraphSchemaOp::Detach { .. } => {
            not_yet_served(ctx.req_id, "GraphSchema")
        }
        GraphSchemaOp::AttachPack { connector, .. } => attach_pack::stage(ctx, connector),
    }
}

/// List the request graph's schema sources and its composed digest.
pub(crate) async fn handle_list(
    req_id: u64,
    _graph_name: &str,
    _core: &Arc<GraphCore>,
) -> Response {
    not_yet_served(req_id, "GraphSchemaList")
}

#[cfg(test)]
mod graph_schema_stub_tests {
    /// Deleted by the package that lands these handlers (wave rule R6).
    #[test]
    fn both_declared_surfaces_refuse_until_their_handlers_land() {
        for surface in ["GraphSchema", "GraphSchemaList"] {
            crate::server::contract_wave::assert_refuses_by_name(surface);
        }
    }
}
