//! Named evaluators (EH-395): the committer of a decision names the one
//! principal -- or the one declared policy role -- allowed to evaluate it.
//!
//! The grant is a control lease of kind `decision.evaluation` (the ControlLease
//! lifecycle, timing bounds and 24-hour span cap, reused as-is), stored in the
//! decision-log owner beside the record it names and written in the SAME
//! transaction as the record, so a record never exists with a half-issued
//! grant. It is read-only, record-scoped and expiring:
//!
//! * the named principal -- or any verified principal whose signed request
//!   context carries the named role -- may join evaluations to exactly this
//!   record until the lease expires, and nothing else: `get`, the SQL views,
//!   aggregates and every other read still filter by the record's own
//!   visibility;
//! * the committer may not name itself, and never evaluates through a role it
//!   also holds (self-evaluation is never independent);
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
/// Longest role name a grant may name.
const MAX_ROLE_BYTES: usize = 128;

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
    if let Some(principal) = &evaluator.principal {
        grant.insert("evaluator".into(), principal.clone().into());
    }
    if let Some(role) = &evaluator.role {
        grant.insert("evaluator_role".into(), role.clone().into());
    }
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

/// Whom `evaluator` names, refused unless it is exactly one well-formed
/// principal other than `committer`, or one plain role name.
fn check_named(evaluator: &NamedEvaluator, committer: &str) -> Result<(), String> {
    match (evaluator.principal.as_deref(), evaluator.role.as_deref()) {
        (Some(principal), None) if !principal.starts_with(PRINCIPAL_PREFIX) => {
            Err(invalid("a named evaluator is a principal persistence id"))
        }
        (Some(principal), None) if principal == committer => {
            Err(invalid("a decision's committer cannot evaluate it"))
        }
        (Some(_), None) => Ok(()),
        (None, Some(role)) if plain_role(role) => Ok(()),
        (None, Some(_)) => Err(invalid(
            "an evaluator role is a non-empty role name of at most 128 bytes, no wildcard",
        )),
        (Some(_), Some(_)) | (None, None) => Err(invalid(
            "a named evaluator is exactly one principal or one role",
        )),
    }
}

fn plain_role(role: &str) -> bool {
    !role.is_empty() && role.len() <= MAX_ROLE_BYTES && !role.contains('*')
}

/// The grant rows a commit writes with its record: none, or the one lease
/// naming `evaluator`. Refuses self-evaluation, a malformed principal or role
/// and any timing outside the control-lease bounds.
pub(super) fn grant_rows(
    ctx: &ExecutionContext,
    record_id: &str,
    committer: &str,
    evaluator: Option<&NamedEvaluator>,
) -> Result<Vec<(String, Vec<u8>)>, String> {
    let Some(evaluator) = evaluator else {
        return Ok(Vec::new());
    };
    check_named(evaluator, committer)?;
    let request = issue_request(ctx, record_id, committer, evaluator);
    request.validate_body().map_err(invalid)?;
    let view = ControlLeaseView::from_row(&request.lease_id, &request.row())?;
    Ok(vec![(lease_key(record_id), encode_artifact(&view)?)])
}

/// Whether the grant names `reader`: its principal, or a role it holds when
/// it is not the committer.
fn names(view: &ControlLeaseView, reader: &LogReader) -> bool {
    let field = |key: &str| view.grant.get(key).and_then(Value::as_str);
    let by_principal = field("evaluator") == Some(reader.principal.as_str());
    let by_role = field("evaluator_role").is_some_and(|role| {
        field("committed_by") != Some(reader.principal.as_str())
            && reader.roles.iter().any(|held| held == role)
    });
    by_principal || by_role
}

fn grants(view: &ControlLeaseView, reader: &LogReader, now_ms: u64) -> bool {
    view.kind == DECISION_EVALUATION_LEASE_KIND
        && view.status == ControlLeaseStatus::Active
        && now_ms < view.expires_at_ms
        && names(view, reader)
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
