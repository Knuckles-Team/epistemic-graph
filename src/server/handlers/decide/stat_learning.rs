//! `DecisionLog.retrieval`, dispatched (EH-394..EH-396).
//!
//! The async half resolves what a retrieval op needs from outside the Agent
//! Library before the blocking store work starts: the verified carrier's
//! tenant scope (the partition adapter state is served from, the same key the
//! query paths derive from their own verified carrier) and, for a fit, the
//! ACL-checked graph whose stored vectors score the judged units.

use eg_types::decision::statistical::log::DecisionLogOp;
use eg_types::decision::statistical::retrieval::{RetrievalOp, RetrievalResult};

use super::stat_adapter::{
    activate, fit_adapter, graph_vectors, rollback, status, GraphVectors, PointerMove,
};
use super::stat_executor::ExecutionContext;
use super::stat_log::LogReader;
use super::stat_retrieval::{read_hard_negatives, read_paths, read_usage, record_outcome};
use super::SharedState;
use crate::server::access::CarrierAuthority;
use crate::server::auth::VerifiedRequestContext;

/// What a retrieval op resolved before its blocking store work.
pub(super) struct RetrievalInputs {
    /// The verified carrier's tenant scope: where adapter state lives.
    served_tenant: String,
    /// A fit's graph vectors, as the caller may see them.
    vectors: Option<GraphVectors>,
}

/// Resolve the inputs of a retrieval op; `None` for every other log op.
pub(super) async fn prepare(
    state: &SharedState,
    verified: &VerifiedRequestContext,
    op: &DecisionLogOp,
) -> Result<Option<RetrievalInputs>, String> {
    let DecisionLogOp::Retrieval { retrieval, .. } = op else {
        return Ok(None);
    };
    let served_tenant = CarrierAuthority::from_verified(verified)?
        .tenant_scope()
        .to_string();
    let vectors = if let RetrievalOp::FitAdapter { request } = retrieval {
        Some(graph_vectors(state, verified.agent_id(), &request.graph).await?)
    } else {
        None
    };
    Ok(Some(RetrievalInputs {
        served_tenant,
        vectors,
    }))
}

fn pointer<'a>(
    ctx: &ExecutionContext,
    reader: &'a LogReader,
    inputs: &'a RetrievalInputs,
    space_digest: &'a str,
) -> PointerMove<'a> {
    PointerMove {
        tenant: &inputs.served_tenant,
        space_digest,
        principal: &reader.principal,
        now_ms: ctx.now_ms,
    }
}

/// Serve one retrieval op.
pub(super) fn dispatch(
    ctx: &ExecutionContext,
    reader: &LogReader,
    op: RetrievalOp,
    inputs: Option<&RetrievalInputs>,
) -> Result<RetrievalResult, String> {
    let inputs = inputs.ok_or("retrieval op inputs were not prepared")?;
    Ok(match op {
        RetrievalOp::RecordOutcome { outcome } => {
            RetrievalResult::Recorded(Box::new(record_outcome(ctx, reader, *outcome)?))
        }
        RetrievalOp::HardNegatives { request } => {
            RetrievalResult::HardNegatives(read_hard_negatives(ctx.store, reader, &request)?)
        }
        RetrievalOp::Usage { window } => {
            RetrievalResult::Usage(read_usage(ctx.store, reader, window)?)
        }
        RetrievalOp::Paths { request } => {
            RetrievalResult::Paths(read_paths(ctx.store, reader, &request)?)
        }
        RetrievalOp::FitAdapter { request } => {
            let vectors = inputs
                .vectors
                .as_ref()
                .ok_or("an adapter fit resolved no graph")?;
            let fitted = fit_adapter(ctx, reader, &request, vectors, &inputs.served_tenant)?;
            RetrievalResult::Fitted(Box::new(fitted))
        }
        RetrievalOp::ActivateAdapter {
            space_digest,
            adapter_digest,
            receipt_digest,
        } => {
            let at = pointer(ctx, reader, inputs, &space_digest);
            let state = activate(ctx.store, &at, &adapter_digest, &receipt_digest)?;
            RetrievalResult::Adapter(Box::new(state))
        }
        RetrievalOp::RollbackAdapter { space_digest } => {
            let at = pointer(ctx, reader, inputs, &space_digest);
            RetrievalResult::Adapter(Box::new(rollback(ctx.store, &at)?))
        }
        RetrievalOp::AdapterStatus { space_digest } => {
            let at = pointer(ctx, reader, inputs, &space_digest);
            RetrievalResult::Adapter(Box::new(status(ctx.store, &at)?))
        }
    })
}
