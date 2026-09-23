//! `Method::DecisionLog`: the statistical decision log.
//!
//! Served by [`super::stat_log`] in a build with the `decide` feature; a build
//! without it answers a typed refusal naming the feature.

use std::sync::Arc;

use tokio::sync::RwLock;

use crate::protocol::Response;
use crate::server::auth::VerifiedRequestContext;
use crate::server::state::ServerState;

/// Commit, evaluate or read the decision log.
pub(crate) async fn handle_decision_log(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified: &VerifiedRequestContext,
    op: eg_types::decision::statistical::log::DecisionLogOp,
) -> Response {
    #[cfg(feature = "decide")]
    {
        super::stat_log::handle_log(state, req_id, verified, op).await
    }
    #[cfg(not(feature = "decide"))]
    {
        let _ = (state, verified, op);
        Response::err(req_id, "DecisionLog requires the `decide` feature")
    }
}
