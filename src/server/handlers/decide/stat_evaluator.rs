//! Named evaluators (EH-395): the committer of a decision names the one
//! principal allowed to evaluate it.
//!
//! The grant is a control lease of kind `decision.evaluation` (the ControlLease
//! lifecycle, timing bounds and 24-hour span cap, reused as-is), stored in the
//! decision-log owner beside the record it names and written in the SAME
//! transaction as the record, so a record never exists with a half-issued
//! grant. It is read-only, record-scoped and expiring:
//!
//! * the named principal may join evaluations to exactly this record until the
//!   lease expires -- and nothing else: `get`, the SQL views, aggregates and
//!   every other read still filter by the record's own visibility;
//! * the committer may not name itself (self-evaluation is never independent);
//! * every evaluation admitted through a grant is audited.

use eg_types::control_lease::{
    ControlLeaseStatus, ControlLeaseView, IssueControlLeaseRequest, DECISION_EVALUATION_LEASE_KIND,
};
use eg_types::decision::statistical::log::{DecisionLogEntry, NamedEvaluator};
use eg_types::decision::statistical::StatisticalErrorCode;
use serde_json::{Map, Value};

use super::stat_executor::ExecutionContext;
use super::stat_log::{visible_entry, LogReader};
use super::stat_support::refusal;
use crate::server::persistence::agent_library::AgentLibraryStore;
use crate::server::persistence::decision_jobs::{decode_artifact, encode_artifact, record_key};

/// Prefix of a persistence id a grant may name.
const PRINCIPAL_PREFIX: &str = "principal:sha256:";

fn lease_key(record_id: &str) -> String {
    format!("evaluator-lease:{record_id}")
}

fn invalid(detail: impl std::fmt::Display) -> String {
    refusal(StatisticalErrorCode::ParameterInvalid, detail)
}

fn issue_request(
    ctx: &ExecutionContext,
    record_id: &str,
    committer: &str,
    evaluator: &NamedEvaluator,
) -> IssueControlLeaseRequest {
    let mut grant = Map::new();
    grant.insert("record_id".into(), record_id.into());
    grant.insert("evaluator".into(), evaluator.principal.clone().into());
    grant.insert("committed_by".into(), committer.into());
    IssueControlLeaseRequest {
        tenant: ctx.tenant_id.to_string(),
        lease_id: lease_key(record_id),
        kind: DECISION_EVALUATION_LEASE_KIND.to_string(),
        grant,
        issued_at_ms: ctx.now_ms,
        expires_at_ms: evaluator.expires_at_ms,
        hard_expires_at_ms: evaluator.expires_at_ms,
        idempotency_key: record_id.to_string(),
    }
}

/// The grant rows a commit writes with its record: none, or the one lease
/// naming `evaluator`. Refuses self-evaluation, a malformed principal and any
/// timing outside the control-lease bounds.
pub(super) fn grant_rows(
    ctx: &ExecutionContext,
    record_id: &str,
    committer: &str,
    evaluator: Option<&NamedEvaluator>,
) -> Result<Vec<(String, Vec<u8>)>, String> {
    let Some(evaluator) = evaluator else {
        return Ok(Vec::new());
    };
    if !evaluator.principal.starts_with(PRINCIPAL_PREFIX) {
        return Err(invalid("a named evaluator is a principal persistence id"));
    }
    if evaluator.principal == committer {
        return Err(invalid("a decision's committer cannot evaluate it"));
    }
    let request = issue_request(ctx, record_id, committer, evaluator);
    request.validate_body().map_err(invalid)?;
    let view = ControlLeaseView::from_row(&request.lease_id, &request.row())?;
    Ok(vec![(lease_key(record_id), encode_artifact(&view)?)])
}

fn grants(view: &ControlLeaseView, reader: &LogReader, now_ms: u64) -> bool {
    let named = view.grant.get("evaluator").and_then(Value::as_str);
    view.kind == DECISION_EVALUATION_LEASE_KIND
        && view.status == ControlLeaseStatus::Active
        && now_ms < view.expires_at_ms
        && named == Some(reader.principal.as_str())
}

fn granted_entry(
    store: &AgentLibraryStore,
    reader: &LogReader,
    record_id: &str,
    now_ms: u64,
) -> Result<Option<DecisionLogEntry>, String> {
    let Some(bytes) = store.decision_artifact(&reader.tenant_id, &lease_key(record_id))? else {
        return Ok(None);
    };
    let view: ControlLeaseView = decode_artifact(&bytes, "evaluator lease")?;
    if !grants(&view, reader, now_ms) {
        return Ok(None);
    }
    let Some(bytes) = store.decision_artifact(&reader.tenant_id, &record_key(record_id))? else {
        return Ok(None);
    };
    tracing::info!(
        target: "epistemic_graph::decide::evaluator_grant",
        record_id,
        lease_id = %view.lease_id,
        evaluator = %reader.principal,
        "evaluation admitted by a named-evaluator grant"
    );
    Ok(Some(decode_artifact(&bytes, "decision log entry")?))
}

/// The record `reader` may EVALUATE: one it may see, or one whose committer
/// named it in a live grant. Used by the evaluation join only.
pub(super) fn evaluable_entry(
    ctx: &ExecutionContext,
    reader: &LogReader,
    record_id: &str,
) -> Result<Option<DecisionLogEntry>, String> {
    if let Some(entry) = visible_entry(ctx.store, reader, record_id)? {
        return Ok(Some(entry));
    }
    granted_entry(ctx.store, reader, record_id, ctx.now_ms)
}
