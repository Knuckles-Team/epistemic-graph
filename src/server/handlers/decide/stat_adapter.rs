//! `DecisionLog.retrieval` adapter operations (EH-396): fit and evaluate a
//! query-side adapter, activate it, roll it back, read its state.
//!
//! A fit reads ONLY independently judged successful runs whose query was
//! ranked in the requested space, scores their cited units and hard negatives
//! by the graph's stored vectors -- through the caller's graph ACL and
//! row-level security -- and holds a deterministic share of them out. The
//! receipt is a paired sign test on that held-out share, run on the
//! QUANTISED body, i.e. exactly what would be served. Activation requires the
//! passing receipt that qualified exactly this body; every pointer move is an
//! audited event, and rollback returns to the adapter active before.

use std::sync::Arc;

use eg_core::graph::GraphCore;
use eg_numeric::decision::adapter::{directions_of, evaluate, fit, to_body, to_q16, JudgedItem};
use eg_numeric::decision::retrieval::{hard_negatives, Verdict};
use eg_types::contract::BoundedVec;
use eg_types::decision::digest::digest_text;
use eg_types::decision::statistical::body::{canonical_body_bytes, content_digest_of};
use eg_types::decision::statistical::retrieval::RetrievalOutcome;
use eg_types::decision::statistical::retrieval_adapter::{
    AdapterEvalReceipt, AdapterEvent, AdapterFitRequest, AdapterFitted, AdapterState,
    AdapterTransition, MAX_ADAPTER_GAIN_Q16, MAX_ADAPTER_HISTORY, MAX_ADAPTER_RANK,
    QUERY_ADAPTER_SCHEMA_VERSION,
};
use eg_types::decision::statistical::StatisticalErrorCode;

use super::served_adapter::{adapter_key, read_fitted, read_state, state_key};
use super::stat_executor::ExecutionContext;
use super::stat_log::LogReader;
use super::stat_retrieval::{judged_outcomes, Scope};
use super::stat_support::refusal;
use crate::isolation::IsolationLayer;
use crate::server::persistence::agent_library::AgentLibraryStore;
use crate::server::persistence::decision_jobs::encode_artifact;

const TRAINING_DOMAIN: &str = "eg/adapter-training/v1";
const HOLDOUT_DOMAIN: &str = "eg/adapter-holdout/v1";
const Q16: f64 = 65_536.0;

fn invalid(detail: impl std::fmt::Display) -> String {
    refusal(StatisticalErrorCode::ParameterInvalid, detail)
}

/// The stored vectors of one graph, as the caller may see them.
pub(super) struct GraphVectors {
    pub(super) core: Arc<GraphCore>,
    pub(super) isolation: IsolationLayer,
    pub(super) caller: String,
}

impl GraphVectors {
    fn space_digest(&self) -> Option<String> {
        self.core
            .semantic_store
            .read()
            .space()
            .map(|space| space.digest.clone())
    }

    #[cfg(feature = "security")]
    fn visible(&self, id: &str) -> bool {
        self.core.get_node_properties(id).is_some_and(|blob| {
            self.isolation
                .can_see_row(&self.caller, &crate::isolation::row_visibility(&blob))
        })
    }

    #[cfg(not(feature = "security"))]
    fn visible(&self, id: &str) -> bool {
        let _ = (&self.isolation, &self.caller);
        self.core.get_node_properties(id).is_some()
    }

    /// A visible unit's stored vector; `None` when invisible or unembedded.
    fn vector(&self, id: &str) -> Option<Vec<f64>> {
        if !self.visible(id) {
            return None;
        }
        let vector = self.core.semantic_store.read().get_embedding(id)?;
        Some(vector.into_iter().map(f64::from).collect())
    }

    fn vectors(&self, ids: impl Iterator<Item = String>) -> Vec<Vec<f64>> {
        ids.filter_map(|id| self.vector(&id)).collect()
    }
}

/// The graph a fit names, ACL-checked for `agent_id`.
pub(super) async fn graph_vectors(
    state: &Arc<tokio::sync::RwLock<crate::server::state::ServerState>>,
    agent_id: &str,
    graph: &str,
) -> Result<GraphVectors, String> {
    let guard = state.read().await;
    let entry = guard
        .registry
        .get(graph)
        .ok_or_else(|| invalid(format!("unknown graph {graph}")))?;
    crate::server::access::check_graph_access(
        &guard.isolation,
        Some(agent_id),
        graph,
        entry.graph_type,
        entry.owner.as_deref(),
        crate::isolation::AccessLevel::Read,
    )?;
    Ok(GraphVectors {
        core: Arc::clone(&entry.core),
        isolation: guard.isolation.clone(),
        caller: agent_id.to_string(),
    })
}

fn check_fit_request(request: &AdapterFitRequest) -> Result<(), String> {
    let rank_ok = (1..=MAX_ADAPTER_RANK).contains(&usize::from(request.rank));
    let gain_ok = (1..=MAX_ADAPTER_GAIN_Q16).contains(&request.max_gain_q16);
    let holdout_ok = (100..=500).contains(&request.holdout_per_mille);
    if !(rank_ok && gain_ok && holdout_ok && request.min_eval_items > 0) {
        return Err(invalid(
            "an adapter fit needs rank 1..=8, 0 < max gain <= 1/2, a 100..=500 per-mille \
             holdout and at least one evaluation item",
        ));
    }
    Ok(())
}

fn query_of(outcome: &RetrievalOutcome, space: &str) -> Option<Vec<f64>> {
    let query = outcome.query.as_ref()?;
    (query.space_digest == space).then(|| query.q16.iter().map(|v| f64::from(*v) / Q16).collect())
}

/// The judged item of one successful run, when both sides have vectors.
fn judged_item(
    outcome: &RetrievalOutcome,
    space: &str,
    vectors: &GraphVectors,
) -> Option<JudgedItem> {
    let item = JudgedItem {
        query: query_of(outcome, space)?,
        positives: vectors.vectors(outcome.cited.iter().cloned()),
        negatives: vectors.vectors(hard_negatives(outcome).into_iter().map(|n| n.evidence_id)),
    };
    (!item.positives.is_empty() && !item.negatives.is_empty()).then_some(item)
}

fn held_out(record_id: &str, per_mille: u16) -> bool {
    let digest = digest_text(HOLDOUT_DOMAIN, &record_id);
    let hex = digest.trim_start_matches("sha256:");
    let bucket = u32::from_str_radix(hex.get(..4).unwrap_or("0"), 16).unwrap_or(0) % 1000;
    bucket < u32::from(per_mille)
}

/// `(training ids, training items, held-out items)` in log key order.
type Split = (Vec<String>, Vec<JudgedItem>, Vec<JudgedItem>);

fn split_items(
    store: &AgentLibraryStore,
    reader: &LogReader,
    request: &AdapterFitRequest,
    vectors: &GraphVectors,
) -> Result<Split, String> {
    let scope = Scope {
        question_id: request.question_id.as_deref(),
        window: request.window,
    };
    let mut split: Split = (Vec::new(), Vec::new(), Vec::new());
    for judged in judged_outcomes(store, reader, scope)? {
        if judged.verdict != Verdict::Success {
            continue;
        }
        let outcome = &judged.stored.outcome;
        let Some(item) = judged_item(outcome, &request.space_digest, vectors) else {
            continue;
        };
        if held_out(&outcome.record_id, request.holdout_per_mille) {
            split.2.push(item);
        } else {
            split.0.push(outcome.record_id.clone());
            split.1.push(item);
        }
    }
    Ok(split)
}

fn receipt_of(
    adapter_digest: &str,
    request: &AdapterFitRequest,
    n_training: u64,
    held: &[JudgedItem],
    body: &eg_types::decision::statistical::retrieval_adapter::QueryAdapterBody,
) -> AdapterEvalReceipt {
    let eval = evaluate(held, &directions_of(body));
    let lower = to_q16(eval.win_rate_lower);
    AdapterEvalReceipt {
        schema_version: QUERY_ADAPTER_SCHEMA_VERSION,
        adapter_digest: adapter_digest.to_string(),
        space_digest: request.space_digest.clone(),
        n_training,
        n_eval: eval.n,
        wins: eval.wins,
        losses: eval.losses,
        ties: eval.ties,
        base_mrr_q16: to_q16(eval.base_mrr),
        adapted_mrr_q16: to_q16(eval.adapted_mrr),
        win_rate_lower_q16: lower,
        passed: eval.n >= u64::from(request.min_eval_items) && lower > to_q16(0.5),
    }
}

/// Fit, evaluate and store one adapter under the served tenant `tenant`.
pub(super) fn fit_adapter(
    ctx: &ExecutionContext,
    reader: &LogReader,
    request: &AdapterFitRequest,
    vectors: &GraphVectors,
    tenant: &str,
) -> Result<AdapterFitted, String> {
    check_fit_request(request)?;
    if vectors.space_digest().as_deref() != Some(request.space_digest.as_str()) {
        return Err(invalid(
            "the graph's embedding space is not the requested one",
        ));
    }
    let dim = vectors.core.semantic_store.read().dim();
    let (ids, train, held) = split_items(ctx.store, reader, request, vectors)?;
    let max_gain = f64::from(request.max_gain_q16) / Q16;
    let directions = fit(&train, dim, usize::from(request.rank), max_gain);
    if directions.is_empty() {
        return Err(refusal(
            StatisticalErrorCode::NoAdmissibleLabels,
            "no judged run in this space has both a cited unit and a hard negative to learn from",
        ));
    }
    let training = (digest_text(TRAINING_DOMAIN, &ids), ids.len() as u64);
    let body = to_body(&directions, &request.space_digest, dim, training)?;
    let adapter_digest = content_digest_of(&canonical_body_bytes(&body)?);
    let receipt = receipt_of(&adapter_digest, request, ids.len() as u64, &held, &body);
    let fitted = AdapterFitted {
        receipt_digest: content_digest_of(&canonical_body_bytes(&receipt)?),
        adapter_digest,
        body,
        receipt,
    };
    let key = adapter_key(&fitted.adapter_digest);
    ctx.store
        .put_decision_artifacts(tenant, &[(key, encode_artifact(&fitted)?)])?;
    Ok(fitted)
}

/// Who moves a pointer, where and when.
pub(super) struct PointerMove<'a> {
    pub(super) tenant: &'a str,
    pub(super) space_digest: &'a str,
    pub(super) principal: &'a str,
    pub(super) now_ms: u64,
}

fn bounded_push(
    list: &BoundedVec<AdapterEvent, MAX_ADAPTER_HISTORY>,
    event: AdapterEvent,
) -> Result<BoundedVec<AdapterEvent, MAX_ADAPTER_HISTORY>, String> {
    let mut events: Vec<AdapterEvent> = list.iter().cloned().collect();
    events.push(event);
    let excess = events.len().saturating_sub(MAX_ADAPTER_HISTORY);
    BoundedVec::new(events.split_off(excess))
}

fn write_state(
    store: &AgentLibraryStore,
    at: &PointerMove,
    expected: Option<Vec<u8>>,
    state: &AdapterState,
) -> Result<(), String> {
    let key = state_key(at.space_digest);
    store.swap_decision_artifact(at.tenant, &key, expected, encode_artifact(state)?)
}

fn event(
    at: &PointerMove,
    transition: AdapterTransition,
    fitted: Option<&AdapterFitted>,
) -> AdapterEvent {
    AdapterEvent {
        transition,
        adapter_digest: fitted.map(|f| f.adapter_digest.clone()),
        receipt_digest: fitted.map(|f| f.receipt_digest.clone()),
        principal: at.principal.to_string(),
        at_ms: at.now_ms,
    }
}

fn qualified(
    store: &AgentLibraryStore,
    at: &PointerMove,
    adapter_digest: &str,
    receipt_digest: &str,
) -> Result<AdapterFitted, String> {
    let mismatch = |detail: &str| refusal(StatisticalErrorCode::EvaluationReceiptMismatch, detail);
    let fitted = read_fitted(store, at.tenant, adapter_digest)?
        .ok_or_else(|| invalid("no fitted adapter with that digest"))?;
    if fitted.receipt_digest != receipt_digest || !fitted.receipt.passed {
        return Err(mismatch(
            "the adapter's receipt is not the named one, or did not pass",
        ));
    }
    if fitted.body.space_digest != at.space_digest {
        return Err(invalid("the adapter was fitted in another embedding space"));
    }
    Ok(fitted)
}

/// Make a qualified adapter the space's active one. Re-activating the active
/// adapter is a replay.
pub(super) fn activate(
    store: &AgentLibraryStore,
    at: &PointerMove,
    adapter_digest: &str,
    receipt_digest: &str,
) -> Result<AdapterState, String> {
    let fitted = qualified(store, at, adapter_digest, receipt_digest)?;
    let (expected, state) = read_state(store, at.tenant, at.space_digest)?;
    let active_digest = state
        .active
        .as_ref()
        .and_then(|e| e.adapter_digest.as_deref());
    if active_digest == Some(adapter_digest) {
        return Ok(state);
    }
    let activated = event(at, AdapterTransition::Activated, Some(&fitted));
    let stack = match state.active.clone() {
        Some(previous) => bounded_push(&state.stack, previous)?,
        None => state.stack.clone(),
    };
    let next = AdapterState {
        space_digest: at.space_digest.to_string(),
        active: Some(activated.clone()),
        stack,
        history: bounded_push(&state.history, activated)?,
    };
    write_state(store, at, expected, &next)?;
    Ok(next)
}

/// Return the space to the adapter active before the current one (or to no
/// adapter at all when there was none).
pub(super) fn rollback(
    store: &AgentLibraryStore,
    at: &PointerMove,
) -> Result<AdapterState, String> {
    let (expected, state) = read_state(store, at.tenant, at.space_digest)?;
    if state.active.is_none() {
        return Err(invalid("no adapter is active in this space"));
    }
    let mut stack: Vec<AdapterEvent> = state.stack.iter().cloned().collect();
    let previous = stack.pop();
    let restored = previous.as_ref().map(|p| AdapterEvent {
        transition: AdapterTransition::RolledBack,
        principal: at.principal.to_string(),
        at_ms: at.now_ms,
        ..p.clone()
    });
    let logged = restored
        .clone()
        .unwrap_or_else(|| event(at, AdapterTransition::RolledBack, None));
    let next = AdapterState {
        space_digest: at.space_digest.to_string(),
        active: restored,
        stack: BoundedVec::new(stack)?,
        history: bounded_push(&state.history, logged)?,
    };
    write_state(store, at, expected, &next)?;
    Ok(next)
}

/// A space's adapter state (empty when nothing was ever activated).
pub(super) fn status(store: &AgentLibraryStore, at: &PointerMove) -> Result<AdapterState, String> {
    Ok(read_state(store, at.tenant, at.space_digest)?.1)
}
