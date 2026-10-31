//! Caller-context aggregation from the existing ordinary completion ledger.
use super::*;
use medulla_types::native_control::SubmitWorkItemRequest;
use medulla_types::run_usage::{
    aggregate_session_usage, SessionUsage, SessionUsageRecord, MAX_SESSION_USAGE_RUNS,
};

/// One graph snapshot supplies inventory, immutable admission, ledger and outbox.
/// Bounds apply to examined node rows, not just matching or measured runs.
pub(crate) fn read_session_usage(
    shard: &Shard,
    graph: &str,
    tenant: &str,
    context_id: &str,
    principal: &str,
    crypto: DurableCrypto<'_>,
) -> Result<SessionUsage, String> {
    medulla_types::work_item_read::validate_work_item_get(tenant, context_id)?;
    if principal.is_empty() {
        return Err("ACCESS_DENIED: usage owner required".into());
    }
    let handle = shard.graph(graph)?;
    let snapshot = shard.read(&handle)?;
    let nodes = snapshot.scoped_owner_table(NODES)?;
    let mut expected = Vec::new();
    let mut records = Vec::new();
    let mut complete = true;
    let mut bytes = 0usize;
    for (examined, item) in nodes.scope_rows_from((graph, ""))?.enumerate() {
        if examined >= MAX_SESSION_USAGE_RUNS {
            complete = false;
            break;
        }
        let (key, value) = item?;
        bytes = bytes.saturating_add(value.value().len());
        if bytes > 32 * 1024 * 1024 {
            complete = false;
            break;
        }
        let row: serde_json::Map<String, serde_json::Value> =
            decode_durable(&crypto.unseal(value.value())?)?;
        let id = key.value().1;
        let run = match session_membership(&row, graph, tenant, principal, id, context_id)? {
            SessionMembership::Other => continue,
            SessionMembership::Unavailable => {
                complete = false;
                continue;
            }
            SessionMembership::Owned(run) => run,
        };
        expected.push(run.clone());
        let Some(completion) =
            run_owner::completion_from_snapshot(&snapshot, &row, tenant, &run, id)?
        else {
            continue;
        };
        let Some(usage) = completion.payload.completion.usage else {
            continue;
        };
        let counts = usage_counts(usage);
        if !counts.is_empty() {
            records.push(SessionUsageRecord {
                run_id: run,
                context_id: context_id.into(),
                tenant: tenant.into(),
                principal: principal.into(),
                counts,
            });
        }
    }
    let mut result = aggregate_session_usage(tenant, principal, context_id, &expected, &records)?;
    result.inventory_complete = complete;
    Ok(result)
}

enum SessionMembership {
    Other,
    Unavailable,
    Owned(String),
}

fn session_membership(
    row: &serde_json::Map<String, serde_json::Value>,
    graph: &str,
    tenant: &str,
    principal: &str,
    id: &str,
    context_id: &str,
) -> Result<SessionMembership, String> {
    let Some(run) = row
        .get("run_owner_binding")
        .and_then(|v| v.get("run_id"))
        .and_then(serde_json::Value::as_str)
    else {
        return Ok(SessionMembership::Other);
    };
    if !run_owner::owned(row, graph, tenant, run, id, principal)
        || !matches!(
            property_string(row, "status"),
            "succeeded" | "failed" | "cancelled" | "dead_letter"
        )
    {
        return Ok(SessionMembership::Other);
    }
    // Missing original admission cannot be repaired with mutable metadata.
    let Some(original) = row.get(run_owner::ADMISSION_REQUEST) else {
        return Ok(SessionMembership::Unavailable);
    };
    let admission: SubmitWorkItemRequest = serde_json::from_value(original.clone())
        .map_err(|_| "invalid immutable session admission")?;
    validate_session_admission(&admission, graph, tenant, run, id)?;
    if admission
        .metadata
        .get("a2a_context_id")
        .and_then(serde_json::Value::as_str)
        != Some(context_id)
    {
        return Ok(SessionMembership::Other);
    }
    Ok(SessionMembership::Owned(run.to_owned()))
}

fn validate_session_admission(
    admission: &SubmitWorkItemRequest,
    graph: &str,
    tenant: &str,
    run: &str,
    id: &str,
) -> Result<(), String> {
    if admission.run_id.as_deref() != Some(run)
        || admission.context.tenant_id != tenant
        || sanitize(&admission.context.graph) != graph
        || admission.work_item_id.is_some()
        || run_owner::work_item_id(graph, tenant, run)? != id
    {
        return Err("session usage admission binding mismatch".into());
    }
    Ok(())
}

fn usage_counts(
    usage: medulla_types::run_completion::RunUsage,
) -> std::collections::BTreeMap<String, u64> {
    [
        ("requests", usage.requests),
        ("tool_calls", usage.tool_calls),
        ("input_tokens", usage.input_tokens),
        ("output_tokens", usage.output_tokens),
        ("total_tokens", usage.total_tokens),
    ]
    .into_iter()
    .filter_map(|(name, value)| value.map(|count| (name.to_string(), count)))
    .collect()
}
