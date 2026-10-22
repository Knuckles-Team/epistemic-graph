//! Reconstruct an existing admission without changing historical digest rules.

use std::sync::Arc;

use eg_types::mutation_batch::{MutationBatchRecord, MutationBatchStatus};
use eg_types::native_control::SubmitWorkItemRequest;

use crate::protocol::{Method, ResultPayload};
use crate::server::persistence::PersistenceBackend;

const CONFLICT: &str = "IDEMPOTENCY_CONFLICT: native run admission receipt differs";

/// Called under the same graph mutation lane as compilation and durable replay.
/// The current request has already passed request-boundary authorization. Only
/// the retained method is reused; current principal, nonce and route stay fresh.
pub(super) async fn retain_original_admission(
    persistence: &Arc<dyn PersistenceBackend>,
    graph: &str,
    batch_id: &str,
    principal: Option<&str>,
    method: Method,
) -> Result<Method, String> {
    let Method::SubmitWorkItem { request: proposed } = &method else {
        return Ok(method);
    };
    if proposed.run_id.is_none() {
        return Ok(method);
    }
    let fname = crate::persist::sanitize(graph);
    let Some(record) = persistence.read_mutation_batch(&fname, batch_id).await? else {
        return Ok(method);
    };
    let actor = super::super::digest::principal_fingerprint(principal.ok_or(CONFLICT)?)?;
    let retained = checked_admission(&record, batch_id, &actor, proposed)?;
    let work_item_id = retained_work_item_id(&record)?;
    check_owner_binding(
        persistence.as_ref(),
        &fname,
        graph,
        proposed,
        &actor,
        &work_item_id,
    )
    .await?;
    Ok(Method::SubmitWorkItem { request: retained })
}

/// The WorkItem id the retained admission's committed result names.
fn retained_work_item_id(record: &MutationBatchRecord) -> Result<String, String> {
    let payload: ResultPayload = eg_types::msgpack::decode_bounded(
        record.result_msgpack.as_deref().ok_or(CONFLICT)?,
        eg_types::msgpack::MsgpackLimits::new(64 * 1024 * 1024, 1_000_000, 64),
    )
    .map_err(|_| CONFLICT.to_string())?;
    Ok(crate::server::handlers::delegation::decode_submit_result(payload)?.work_item_id)
}

/// The retained WorkItem still carries the exact run owner binding.
async fn check_owner_binding(
    persistence: &dyn PersistenceBackend,
    fname: &str,
    graph: &str,
    proposed: &SubmitWorkItemRequest,
    actor: &str,
    work_item_id: &str,
) -> Result<(), String> {
    let row = persistence
        .read_node(fname, work_item_id)
        .await?
        .ok_or(CONFLICT)?;
    let props: serde_json::Value = rmp_serde::from_slice(&row).map_err(|_| CONFLICT)?;
    let expected = serde_json::json!({
        "graph": graph, "tenant": proposed.context.tenant_id,
        "run_id": proposed.run_id, "work_item_id": work_item_id,
        "principal": actor,
    });
    if props.get("run_owner_binding") != Some(&expected) {
        return Err(CONFLICT.into());
    }
    Ok(())
}

fn checked_admission(
    record: &MutationBatchRecord,
    batch_id: &str,
    actor: &str,
    proposed: &SubmitWorkItemRequest,
) -> Result<SubmitWorkItemRequest, String> {
    record.validate_identity()?;
    if record.batch.batch_id != batch_id
        || record.status != MutationBatchStatus::Committed
        || record.committing_actor()? != actor
        || record.committing_tenant()? != proposed.context.tenant_id
        || record.batch.operations.len() != 1
    {
        return Err(CONFLICT.into());
    }
    let Method::SubmitWorkItem { request: retained } = &record.batch.operations[0].method else {
        return Err(CONFLICT.into());
    };
    let mut proposed_comparison = proposed.clone();
    let mut retained_comparison = retained.clone();
    crate::server::handlers::delegation::normalize_replay_context(&mut proposed_comparison.context);
    crate::server::handlers::delegation::normalize_replay_context(&mut retained_comparison.context);
    if proposed_comparison != retained_comparison {
        return Err(CONFLICT.into());
    }
    Ok(retained.clone())
}
