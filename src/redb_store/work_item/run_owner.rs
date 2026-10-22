//! Immutable run ownership is admitted with the new WorkItem row.

use super::*;

pub(super) use eg_types::work_item_read::RUN_ADMISSION_REQUEST as ADMISSION_REQUEST;

pub(super) fn work_item_id(graph: &str, tenant: &str, run_id: &str) -> Result<String, String> {
    use sha2::{Digest, Sha256};
    if run_id.trim().is_empty() || run_id.len() > 512 {
        return Err("run admission requires a bounded nonempty run_id".into());
    }
    let mut digest = Sha256::new();
    for field in ["run-admission", graph, tenant, run_id] {
        digest.update((field.len() as u64).to_be_bytes());
        digest.update(field.as_bytes());
    }
    Ok(format!("work-item:{}", hex::encode(digest.finalize())))
}

pub(super) fn admit_owner(
    graph: &str,
    request: &eg_types::native_control::SubmitWorkItemRequest,
    actor: Option<&str>,
) -> Result<Option<serde_json::Value>, String> {
    let Some(run_id) = request.run_id.as_deref() else {
        return Ok(None);
    };
    if request.work_item_id.is_some() {
        return Err("run admission cannot attach a caller-selected WorkItem".into());
    }
    let work_item_id = work_item_id(graph, &request.context.tenant_id, run_id)?;
    let actor = actor
        .filter(|actor| !actor.is_empty())
        .ok_or("ACCESS_DENIED: run admission requires verified caller attribution")?;
    Ok(Some(
        serde_json::json!({"graph": graph, "tenant": request.context.tenant_id,
        "run_id": run_id, "work_item_id": work_item_id, "principal": actor}),
    ))
}

pub(super) fn owned(
    row: &serde_json::Map<String, serde_json::Value>,
    graph: &str,
    tenant: &str,
    run_id: &str,
    id: &str,
    actor: &str,
) -> bool {
    row.get("run_owner_binding")
        == Some(&serde_json::json!({
            "graph": graph, "tenant": tenant, "run_id": run_id, "work_item_id": id, "principal": actor
        }))
        && property_string(row, "node_type") == "WorkItem"
        && property_string(row, "tenant") == tenant
        && !actor.is_empty()
}

/// Historical owner-bound evidence, independent of current execution authority.
pub(crate) fn read_admission(
    shard: &Shard,
    graph: &str,
    tenant: &str,
    run_id: &str,
    actor: &str,
    crypto: DurableCrypto<'_>,
) -> Result<Option<eg_types::work_item_read::RunWorkItemView>, String> {
    use eg_types::native_control::SubmitWorkItemRequest;
    use eg_types::work_item_read::{validate_work_item_get, RunWorkItemView, WorkItemView};
    validate_work_item_get(tenant, run_id)?;
    let id = work_item_id(graph, tenant, run_id)?;
    let (_, row) = read::snapshot_row(shard, graph, &id, crypto)?;
    let Some(row) = row.filter(|row| owned(row, graph, tenant, run_id, &id, actor)) else {
        return Ok(None);
    };
    let unavailable = format!(
        "{}: immutable original submission is unavailable",
        eg_types::contract::ServerErrorCode::RunAdmissionUnavailable
    );
    let unavailable = unavailable.as_str();
    let admission: SubmitWorkItemRequest =
        serde_json::from_value(row.get(ADMISSION_REQUEST).cloned().ok_or(unavailable)?)
            .map_err(|_| unavailable)?;
    if admission.run_id.as_deref() != Some(run_id)
        || admission.context.tenant_id != tenant
        || sanitize(&admission.context.graph) != graph
        || admission.work_item_id.is_some()
    {
        return Err(unavailable.into());
    }
    let work_item = WorkItemView::from_tenant_row(&id, &row, tenant)?.ok_or(unavailable)?;
    Ok(Some(RunWorkItemView {
        work_item,
        admission,
    }))
}

#[cfg(test)]
mod tests;
