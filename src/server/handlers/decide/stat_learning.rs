//! `DecisionLog.retrieval`, dispatched (EH-394..EH-397).
//!
//! The async half resolves what a retrieval op needs from outside the Agent
//! Library before the blocking store work starts: the verified carrier's
//! tenant scope (the partition pointers are served from -- the same key the
//! query paths derive from their own verified carrier) and the ACL-checked
//! graphs a fit or a generation evaluation probes.

use eg_types::decision::statistical::log::DecisionLogOp;
use eg_types::decision::statistical::retrieval::{RetrievalOp, RetrievalResult};
use eg_types::decision::statistical::retrieval_adapter::AdapterFitRequest;
use eg_types::decision::statistical::retrieval_generation::GenerationEvalRequest;
use eg_types::decision::statistical::retrieval_pointer::PointerState;

use super::stat_adapter::{adapter_pointer, fit_adapter, qualified};
use super::stat_executor::ExecutionContext;
use super::stat_generation::{
    evaluate_generation, generation_pointer, qualified_generation, Generations,
};
use super::stat_log::LogReader;
use super::stat_pointer::{activate, rollback, status, PointerMove};
use super::stat_retrieval::{read_hard_negatives, read_paths, read_usage, record_outcome};
use super::stat_vectors::{graph_vectors, GraphVectors};
use super::SharedState;
use crate::server::access::CarrierAuthority;
use crate::server::auth::VerifiedRequestContext;

/// What a retrieval op resolved before its blocking store work.
pub(super) struct RetrievalInputs {
    /// The verified carrier's tenant scope: where pointers live.
    served_tenant: String,
    /// The graphs the op probes, in the order it names them.
    graphs: Vec<GraphVectors>,
}

/// One dispatch: the log context, the resolved inputs and the clock.
struct Serving<'a> {
    ctx: &'a ExecutionContext<'a>,
    reader: &'a LogReader,
    inputs: &'a RetrievalInputs,
}

impl Serving<'_> {
    fn graph(&self, index: usize) -> Result<&GraphVectors, String> {
        self.inputs
            .graphs
            .get(index)
            .ok_or_else(|| "a retrieval op resolved too few graphs".to_string())
    }

    fn tenant(&self) -> &str {
        &self.inputs.served_tenant
    }

    fn at(&self, key: String) -> PointerMove<'_> {
        PointerMove {
            tenant: &self.inputs.served_tenant,
            key,
            principal: &self.reader.principal,
            now_ms: self.ctx.now_ms,
        }
    }

    fn fit(&self, request: &AdapterFitRequest) -> Result<RetrievalResult, String> {
        let fitted = fit_adapter(
            self.ctx,
            self.reader,
            request,
            self.graph(0)?,
            self.tenant(),
        )?;
        Ok(RetrievalResult::Fitted(Box::new(fitted)))
    }

    fn evaluate(&self, request: &GenerationEvalRequest) -> Result<RetrievalResult, String> {
        let gens = Generations {
            active: self.graph(0)?,
            shadow: self.graph(1)?,
        };
        let evaluated = evaluate_generation(self.ctx, self.reader, request, &gens, self.tenant())?;
        Ok(RetrievalResult::Generation(Box::new(evaluated)))
    }

    fn activate_adapter(
        &self,
        graph: &str,
        adapter_digest: &str,
        receipt_digest: &str,
    ) -> Result<RetrievalResult, String> {
        let store = self.ctx.store;
        let to = qualified(store, self.tenant(), graph, adapter_digest, receipt_digest)?;
        pointer(activate(store, &self.at(adapter_pointer(graph)), &to))
    }

    fn activate_generation(
        &self,
        logical: &str,
        shadow_graph: &str,
        receipt_digest: &str,
    ) -> Result<RetrievalResult, String> {
        let store = self.ctx.store;
        let to = qualified_generation(
            store,
            self.tenant(),
            (logical, shadow_graph),
            receipt_digest,
        )?;
        pointer(activate(store, &self.at(generation_pointer(logical)), &to))
    }
}

fn pointer(result: Result<PointerState, String>) -> Result<RetrievalResult, String> {
    Ok(RetrievalResult::Pointer(Box::new(result?)))
}

fn graphs_of(op: &RetrievalOp) -> Vec<&str> {
    match op {
        RetrievalOp::FitAdapter { request } => vec![request.graph.as_str()],
        RetrievalOp::EvaluateGeneration { request } => {
            vec![request.active_graph.as_str(), request.shadow_graph.as_str()]
        }
        RetrievalOp::RecordOutcome { .. }
        | RetrievalOp::HardNegatives { .. }
        | RetrievalOp::Usage { .. }
        | RetrievalOp::Paths { .. }
        | RetrievalOp::ActivateAdapter { .. }
        | RetrievalOp::RollbackAdapter { .. }
        | RetrievalOp::AdapterStatus { .. }
        | RetrievalOp::ActivateGeneration { .. }
        | RetrievalOp::RollbackGeneration { .. }
        | RetrievalOp::GenerationStatus { .. } => Vec::new(),
    }
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
    let mut graphs = Vec::new();
    for graph in graphs_of(retrieval) {
        graphs.push(graph_vectors(state, verified.agent_id(), graph).await?);
    }
    Ok(Some(RetrievalInputs {
        served_tenant,
        graphs,
    }))
}

/// Serve one retrieval op.
pub(super) fn dispatch(
    ctx: &ExecutionContext,
    reader: &LogReader,
    op: RetrievalOp,
    inputs: Option<&RetrievalInputs>,
) -> Result<RetrievalResult, String> {
    let inputs = inputs.ok_or("retrieval op inputs were not prepared")?;
    let s = Serving {
        ctx,
        reader,
        inputs,
    };
    let store = ctx.store;
    match op {
        RetrievalOp::RecordOutcome { outcome } => Ok(RetrievalResult::Recorded(Box::new(
            record_outcome(ctx, reader, *outcome)?,
        ))),
        RetrievalOp::HardNegatives { request } => Ok(RetrievalResult::HardNegatives(
            read_hard_negatives(store, reader, &request)?,
        )),
        RetrievalOp::Usage { window } => {
            Ok(RetrievalResult::Usage(read_usage(store, reader, window)?))
        }
        RetrievalOp::Paths { request } => {
            Ok(RetrievalResult::Paths(read_paths(store, reader, &request)?))
        }
        RetrievalOp::FitAdapter { request } => s.fit(&request),
        RetrievalOp::ActivateAdapter {
            graph,
            adapter_digest,
            receipt_digest,
        } => s.activate_adapter(&graph, &adapter_digest, &receipt_digest),
        RetrievalOp::RollbackAdapter { graph } => {
            pointer(rollback(store, &s.at(adapter_pointer(&graph))))
        }
        RetrievalOp::AdapterStatus { graph } => {
            pointer(status(store, &s.at(adapter_pointer(&graph))))
        }
        RetrievalOp::EvaluateGeneration { request } => s.evaluate(&request),
        RetrievalOp::ActivateGeneration {
            logical,
            shadow_graph,
            receipt_digest,
        } => s.activate_generation(&logical, &shadow_graph, &receipt_digest),
        RetrievalOp::RollbackGeneration { logical } => {
            pointer(rollback(store, &s.at(generation_pointer(&logical))))
        }
        RetrievalOp::GenerationStatus { logical } => {
            pointer(status(store, &s.at(generation_pointer(&logical))))
        }
    }
}
