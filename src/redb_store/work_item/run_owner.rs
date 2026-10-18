//! Immutable run ownership is admitted with the new WorkItem row.
use super::*;

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
