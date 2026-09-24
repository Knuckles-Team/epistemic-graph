//! `DecisionLog.learn`, dispatched (EH-394..EH-397): the one retrieval-learning
//! WRITE. Reads of what was learned are the decision views' relations
//! ([`super::stat_retrieval`]).
//!
//! The async half resolves what a write needs from outside the Agent Library
//! before the blocking store work starts: the verified carrier's tenant scope
//! (the partition pointers are served from -- the same key the query paths
//! derive from their own verified carrier) and the ACL-checked graphs a fit or
//! a generation evaluation probes.

use eg_types::decision::statistical::log::DecisionLogOp;
use eg_types::decision::statistical::retrieval::{
    LearningRecorded, LearningWrite, PointerMovement, PointerRef,
};
use eg_types::decision::statistical::retrieval_adapter::AdapterFitRequest;
use eg_types::decision::statistical::retrieval_generation::GenerationEvalRequest;

use super::stat_adapter::{adapter_pointer, fit_adapter, qualified};
use super::stat_executor::ExecutionContext;
use super::stat_generation::{
    evaluate_generation, generation_pointer, qualified_generation, Generations,
};
use super::stat_log::LogReader;
use super::stat_pointer::{activate, rollback, PointerMove, Qualified};
use super::stat_retrieval::record_outcome;
use super::stat_vectors::{graph_vectors, GraphVectors};
use super::SharedState;
use crate::server::access::CarrierAuthority;
use crate::server::auth::VerifiedRequestContext;

/// What a learning write resolved before its blocking store work.
pub(super) struct LearningInputs {
    /// The verified carrier's tenant scope: where pointers live.
    served_tenant: String,
    /// The graphs the write probes, in the order it names them.
    graphs: Vec<GraphVectors>,
}

/// One dispatch: the log context and the resolved inputs.
struct Serving<'a> {
    ctx: &'a ExecutionContext<'a>,
    reader: &'a LogReader,
    inputs: &'a LearningInputs,
}

impl Serving<'_> {
    fn graph(&self, index: usize) -> Result<&GraphVectors, String> {
        self.inputs
            .graphs
            .get(index)
            .ok_or_else(|| "a learning write resolved too few graphs".to_string())
    }

    fn tenant(&self) -> &str {
        &self.inputs.served_tenant
    }

    fn fit(&self, request: &AdapterFitRequest) -> Result<LearningRecorded, String> {
        let fitted = fit_adapter(
            self.ctx,
            self.reader,
            request,
            self.graph(0)?,
            self.tenant(),
        )?;
        Ok(LearningRecorded::Fitted(Box::new(fitted)))
    }

    fn evaluate(&self, request: &GenerationEvalRequest) -> Result<LearningRecorded, String> {
        let gens = Generations {
            active: self.graph(0)?,
            shadow: self.graph(1)?,
        };
        let evaluated = evaluate_generation(self.ctx, self.reader, request, &gens, self.tenant())?;
        Ok(LearningRecorded::Generation(Box::new(evaluated)))
    }

    /// The target a move may activate: the named receipt must have passed
    /// and qualified exactly this target for this pointer.
    fn qualify(
        &self,
        pointer: &PointerRef,
        target: &str,
        receipt: &str,
    ) -> Result<Qualified, String> {
        let store = self.ctx.store;
        match pointer {
            PointerRef::Adapter { graph } => {
                qualified(store, self.tenant(), graph, target, receipt)
            }
            PointerRef::Generation { logical } => {
                qualified_generation(store, self.tenant(), (logical, target), receipt)
            }
        }
    }

    fn move_pointer(
        &self,
        pointer: &PointerRef,
        movement: &PointerMovement,
    ) -> Result<LearningRecorded, String> {
        let key = match pointer {
            PointerRef::Adapter { graph } => adapter_pointer(graph),
            PointerRef::Generation { logical } => generation_pointer(logical),
        };
        let at = PointerMove {
            tenant: self.tenant(),
            key,
            principal: &self.reader.principal,
            now_ms: self.ctx.now_ms,
        };
        let state = match movement {
            PointerMovement::Activate {
                target,
                receipt_digest,
            } => activate(
                self.ctx.store,
                &at,
                &self.qualify(pointer, target, receipt_digest)?,
            )?,
            PointerMovement::Rollback => rollback(self.ctx.store, &at)?,
        };
        Ok(LearningRecorded::Pointer(Box::new(state)))
    }
}

fn graphs_of(write: &LearningWrite) -> Vec<&str> {
    match write {
        LearningWrite::FitAdapter { request } => vec![request.graph.as_str()],
        LearningWrite::EvaluateGeneration { request } => {
            vec![request.active_graph.as_str(), request.shadow_graph.as_str()]
        }
        LearningWrite::RecordOutcome { .. } | LearningWrite::MovePointer { .. } => Vec::new(),
    }
}

/// Resolve the inputs of a learning write; `None` for every other log op.
pub(super) async fn prepare(
    state: &SharedState,
    verified: &VerifiedRequestContext,
    op: &DecisionLogOp,
) -> Result<Option<LearningInputs>, String> {
    let DecisionLogOp::Learn { write, .. } = op else {
        return Ok(None);
    };
    let served_tenant = CarrierAuthority::from_verified(verified)?
        .tenant_scope()
        .to_string();
    let mut graphs = Vec::new();
    for graph in graphs_of(write) {
        graphs.push(graph_vectors(state, verified.agent_id(), graph).await?);
    }
    Ok(Some(LearningInputs {
        served_tenant,
        graphs,
    }))
}

/// Serve one learning write.
pub(super) fn dispatch(
    ctx: &ExecutionContext,
    reader: &LogReader,
    write: LearningWrite,
    inputs: Option<&LearningInputs>,
) -> Result<LearningRecorded, String> {
    let inputs = inputs.ok_or("learning write inputs were not prepared")?;
    let s = Serving {
        ctx,
        reader,
        inputs,
    };
    match write {
        LearningWrite::RecordOutcome { outcome } => Ok(LearningRecorded::Outcome(Box::new(
            record_outcome(ctx, reader, *outcome)?,
        ))),
        LearningWrite::FitAdapter { request } => s.fit(&request),
        LearningWrite::EvaluateGeneration { request } => s.evaluate(&request),
        LearningWrite::MovePointer { pointer, movement } => s.move_pointer(&pointer, &movement),
    }
}
