//! Native control-lease rows (graph-os EG-2). Issue and transition run inside
//! the SAME durable WorkItem MutationBatch transaction as the WorkItem
//! transitions (replicated, audited, idempotent under the caller's key), and
//! write through the same row writer, so the revision the reads project is the
//! one [`write_work_item_props`] maintains.
//!
//! The lifecycle rules and the row shape are owned by
//! [`eg_types::control_lease`]; this module only reads and writes rows.

use eg_types::control_lease::{
    is_tenant_control_lease, validate_control_lease_get, ControlLeaseIssueOutcome,
    ControlLeaseIssued, ControlLeaseTransition, ControlLeaseTransitionOutcome, ControlLeaseView,
    IssueControlLeaseRequest, TransitionControlLeaseRequest,
};
use eg_types::result_contract::coordination::{IssueControlLease, TransitionControlLease};

use super::*;

type NodeRow = serde_json::Map<String, serde_json::Value>;

fn load_row(
    nodes: &ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    graph: &str,
    lease_id: &str,
    crypto: DurableCrypto<'_>,
) -> Result<Option<NodeRow>, String> {
    nodes
        .get((graph, lease_id))?
        .map(|value| crypto.unseal(value.value()))
        .transpose()?
        .map(|bytes| decode_durable(&bytes))
        .transpose()
}

/// The control-lease arm of the WorkItem-family applier: `None` for any
/// method that is not a control-lease write.
pub(crate) fn apply_control_lease_rows(
    graph: &str,
    method: &Method,
    nodes: &mut ScopedOwnerTableMut<'_, (&'static str, &'static str), &'static [u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<Option<crate::protocol::ResultPayload>, String> {
    match method {
        Method::IssueControlLease { request } => {
            apply_issue_control_lease_row(graph, request, nodes, crypto)
        }
        Method::TransitionControlLease { request } => {
            apply_transition_control_lease_row(graph, request, nodes, crypto)
        }
        _ => Ok(None),
    }
}

/// Issue one active lease; a row already holding the id is a `collision`,
/// whatever it is, and is left untouched.
pub(crate) fn apply_issue_control_lease_row(
    graph: &str,
    request: &IssueControlLeaseRequest,
    nodes: &mut ScopedOwnerTableMut<'_, (&'static str, &'static str), &'static [u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<Option<crate::protocol::ResultPayload>, String> {
    request.validate()?;
    if load_row(nodes, graph, &request.lease_id, crypto)?.is_some() {
        return crate::protocol::ResultPayload::of::<IssueControlLease>(ControlLeaseIssued {
            outcome: ControlLeaseIssueOutcome::Collision,
            lease: None,
            changed_work_item_ids: Vec::new(),
        })
        .map(Some);
    }
    let mut row = request.row();
    write_work_item_props(nodes, graph, &request.lease_id, &mut row, crypto)?;
    crate::protocol::ResultPayload::of::<IssueControlLease>(ControlLeaseIssued {
        outcome: ControlLeaseIssueOutcome::Issued,
        lease: Some(ControlLeaseView::from_row(&request.lease_id, &row)?),
        changed_work_item_ids: vec![request.lease_id.clone()],
    })
    .map(Some)
}

/// Consume or end one lease along a legal edge, compare-and-set on the
/// revision the caller read.
pub(crate) fn apply_transition_control_lease_row(
    graph: &str,
    request: &TransitionControlLeaseRequest,
    nodes: &mut ScopedOwnerTableMut<'_, (&'static str, &'static str), &'static [u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<Option<crate::protocol::ResultPayload>, String> {
    request.validate()?;
    let row = load_row(nodes, graph, &request.lease_id, crypto)?
        .filter(|row| is_tenant_control_lease(row, &request.tenant));
    let Some(mut row) = row else {
        return transition_result(ControlLeaseTransitionOutcome::NotFound, None, false);
    };
    let current = ControlLeaseView::from_row(&request.lease_id, &row)?;
    if !request.to.allowed_from(current.status) || current.revision != request.expected_revision {
        return transition_result(
            ControlLeaseTransitionOutcome::Conflict,
            Some(current),
            false,
        );
    }
    row.insert("status".into(), request.to.status().as_stored().into());
    write_work_item_props(nodes, graph, &request.lease_id, &mut row, crypto)?;
    let ended = ControlLeaseView::from_row(&request.lease_id, &row)?;
    transition_result(ControlLeaseTransitionOutcome::Applied, Some(ended), true)
}

fn transition_result(
    outcome: ControlLeaseTransitionOutcome,
    lease: Option<ControlLeaseView>,
    changed: bool,
) -> Result<Option<crate::protocol::ResultPayload>, String> {
    let changed_work_item_ids = lease
        .as_ref()
        .filter(|_| changed)
        .map(|lease| vec![lease.lease_id.clone()])
        .unwrap_or_default();
    crate::protocol::ResultPayload::of::<TransitionControlLease>(ControlLeaseTransition {
        outcome,
        lease,
        changed_work_item_ids,
    })
    .map(Some)
}

/// `GetControlLease`: the caller's view of one lease, `None` when no control
/// lease with this id belongs to `tenant`. An MVCC snapshot read.
pub(crate) fn read_control_lease(
    shard: &Shard,
    graph: &str,
    tenant: &str,
    lease_id: &str,
    crypto: DurableCrypto<'_>,
) -> Result<Option<ControlLeaseView>, String> {
    validate_control_lease_get(tenant, lease_id)?;
    let (_, row) = super::read::snapshot_row(shard, graph, lease_id, crypto)?;
    row.filter(|row| is_tenant_control_lease(row, tenant))
        .map(|row| ControlLeaseView::from_row(lease_id, &row))
        .transpose()
}

#[cfg(test)]
mod tests;
