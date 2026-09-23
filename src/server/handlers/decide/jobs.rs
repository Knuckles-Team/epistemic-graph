//! `Method::DecisionFit` and `Method::DecisionEval`: the two admin jobs behind
//! a calibrated decision head.
//!
//! Served by [`super::stat_jobs`] in a build with the `decide` feature; a
//! build without it answers a typed refusal naming the feature.

use super::SharedState;
use crate::protocol::Response;
use crate::server::auth::VerifiedRequestContext;

/// Submit or read one head-fitting job.
pub(crate) async fn handle_decision_fit(
    state: &SharedState,
    req_id: u64,
    verified: &VerifiedRequestContext,
    op: eg_types::decision::DecisionFitOp,
) -> Response {
    #[cfg(feature = "decide")]
    {
        super::stat_jobs::handle_fit(state, req_id, verified, op).await
    }
    #[cfg(not(feature = "decide"))]
    {
        let _ = (state, verified, op);
        Response::err(req_id, "DecisionFit requires the `decide` feature")
    }
}

/// Submit or read one head-evaluation job.
pub(crate) async fn handle_decision_eval(
    state: &SharedState,
    req_id: u64,
    verified: &VerifiedRequestContext,
    op: eg_types::decision::DecisionEvalOp,
) -> Response {
    #[cfg(feature = "decide")]
    {
        super::stat_jobs::handle_eval(state, req_id, verified, op).await
    }
    #[cfg(not(feature = "decide"))]
    {
        let _ = (state, verified, op);
        Response::err(req_id, "DecisionEval requires the `decide` feature")
    }
}
