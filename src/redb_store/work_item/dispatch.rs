//! WorkItem dispatch.rs transitions.

use super::*;

pub(crate) struct WorkItemApplyRequest<'args, 'table, 'crypto> {
    pub(crate) graph: &'args str,
    pub(crate) batch_id: &'args str,
    pub(crate) method: &'args Method,
    pub(crate) nodes:
        &'args mut ScopedOwnerTableMut<'table, (&'static str, &'static str), &'static [u8]>,
    pub(crate) holds:
        &'args mut ScopedOwnerTableMut<'table, (&'static str, &'static str), &'static [u8]>,
    pub(crate) work_item_index:
        &'args ScopedOwnerTableMut<'table, (&'static str, &'static str, u64), &'static str>,
    pub(crate) counters:
        &'args mut ScopedOwnerTableMut<'table, (&'static str, &'static str), &'static [u8]>,
    pub(crate) pressure_index: &'args mut ScopedOwnerTableMut<
        'table,
        (
            &'static str,
            &'static str,
            &'static str,
            &'static str,
            u64,
            &'static str,
        ),
        u8,
    >,
    pub(crate) policies:
        &'args ScopedOwnerTableMut<'table, (&'static str, &'static str), &'static [u8]>,
    pub(crate) native_work_items:
        &'args mut ScopedOwnerTableMut<'table, (&'static str, &'static str), &'static [u8]>,
    pub(crate) crypto: DurableCrypto<'crypto>,
}

pub(crate) fn apply_work_item_rows(
    request: WorkItemApplyRequest<'_, '_, '_>,
) -> Result<Option<crate::protocol::ResultPayload>, String> {
    let WorkItemApplyRequest {
        graph,
        batch_id,
        method,
        nodes,
        holds,
        work_item_index,
        counters,
        pressure_index,
        policies,
        native_work_items,
        crypto,
    } = request;
    match method {
        Method::ClaimWorkItem { request } => {
            apply_claim_work_item_row(graph, request, nodes, native_work_items, crypto)
        }
        Method::RenewWorkItemLease {
            tenant,
            work_item_id,
            worker_id,
            lease_epoch,
            fencing_token,
            now_ms,
            lease_ms,
        } => apply_renew_work_item_lease_row(RenewWorkItemLeaseInput {
            graph,
            tenant,
            work_item_id,
            worker_id,
            lease_epoch: *lease_epoch,
            fencing_token: *fencing_token,
            now_ms: *now_ms,
            lease_ms: *lease_ms,
            nodes: &mut *nodes,
            crypto,
        }),
        Method::CasWorkItemMetadata { request } => {
            apply_cas_work_item_metadata_row(graph, request, nodes, crypto)
        }
        Method::CommitWorkItemResult {
            tenant,
            work_item_id,
            worker_id,
            lease_epoch,
            fencing_token,
            outcome,
            result_ref,
            error_ref,
            retryable,
            now_ms,
            outcome_extension,
            idempotency_key: _,
        } => apply_commit_work_item_result_row(CommitWorkItemResultInput {
            graph,
            tenant,
            work_item_id,
            worker_id,
            lease_epoch: *lease_epoch,
            fencing_token: *fencing_token,
            outcome,
            result_ref,
            error_ref,
            retryable: *retryable,
            now_ms: *now_ms,
            outcome_extension: outcome_extension.as_deref(),
            batch_id,
            nodes: &mut *nodes,
            holds: &mut *holds,
            work_item_index,
            counters: &mut *counters,
            pressure_index: &mut *pressure_index,
            policies,
            crypto,
        }),
        Method::CancelWorkItem {
            tenant,
            work_item_id,
            reason_ref,
            now_ms,
            ..
        } => apply_cancel_work_item_row(CancelWorkItemInput {
            graph,
            tenant,
            work_item_id,
            reason_ref,
            now_ms: *now_ms,
            nodes: &mut *nodes,
            holds: &mut *holds,
            work_item_index,
            counters: &mut *counters,
            pressure_index: &mut *pressure_index,
            policies,
            crypto,
        }),
        Method::DeferWorkItem {
            tenant,
            work_item_id,
            worker_id,
            lease_epoch,
            fencing_token,
            next_retry_at_ms,
            reason_ref,
            now_ms,
            ..
        } => apply_defer_work_item_row(DeferWorkItemInput {
            graph,
            tenant,
            work_item_id,
            worker_id,
            lease_epoch: *lease_epoch,
            fencing_token: *fencing_token,
            next_retry_at_ms: *next_retry_at_ms,
            reason_ref,
            now_ms: *now_ms,
            nodes: &mut *nodes,
            crypto,
        }),
        _ => Ok(None),
    }
}
