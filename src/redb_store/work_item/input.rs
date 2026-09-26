//! EH-590 fenced pending-input writes. Both operations run in the native
//! WorkItem MutationBatch transaction, including the row revision CAS.

use super::*;
use eg_types::work_item_input::{
    AnswerWorkItemInput, RequestWorkItemInput, WorkItemInputAnswer, WorkItemInputOutcome,
    WorkItemInputTransition, WorkItemPendingInput,
};
use eg_types::work_item_read::WORK_ITEM_ROW_REVISION;

fn answer(
    outcome: WorkItemInputOutcome,
    work_item_id: &str,
    version: Option<u64>,
) -> WorkItemInputTransition {
    WorkItemInputTransition {
        outcome,
        work_item_id: work_item_id.to_string(),
        version,
        changed_work_item_ids: if outcome == WorkItemInputOutcome::Applied {
            vec![work_item_id.to_string()]
        } else {
            vec![]
        },
    }
}

fn current_version(props: &serde_json::Map<String, serde_json::Value>) -> u64 {
    property_u64(props, WORK_ITEM_ROW_REVISION).max(1)
}

fn submitted_origin_is_verified(props: &serde_json::Map<String, serde_json::Value>) -> bool {
    let submit_actor = property_string(props, "submit_principal_ref");
    let submitted_subject = props
        .get("context")
        .and_then(serde_json::Value::as_object)
        .and_then(|context| context.get("subject_id"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or("");
    !submit_actor.is_empty()
        && crate::server::mutation_batch::principal_fingerprint(submitted_subject)
            .map_or(false, |digest| digest == submit_actor)
}

pub(crate) fn apply_request_work_item_input_row(
    graph: &str,
    request: &RequestWorkItemInput,
    authoritative_now_ms: u64,
    nodes: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<Option<crate::protocol::ResultPayload>, String> {
    use eg_types::result_contract::coordination::RequestWorkItemInput as ResultTag;
    request.validate()?;
    let respond = |outcome, version| {
        crate::protocol::ResultPayload::of::<ResultTag>(answer(
            outcome,
            &request.work_item_id,
            version,
        ))
        .map(Some)
    };
    let Some(row) = nodes.get((graph, request.work_item_id.as_str()))? else {
        return respond(WorkItemInputOutcome::Missing, None);
    };
    let mut props: serde_json::Map<String, serde_json::Value> =
        decode_durable(&crypto.unseal(row.value())?)?;
    if property_string(&props, "node_type") != "WorkItem"
        || property_string(&props, "tenant") != request.tenant
    {
        return respond(WorkItemInputOutcome::Missing, None);
    }
    let version = current_version(&props);
    let now_s = authoritative_now_ms as f64 / 1000.0;
    if request.expires_at_ms <= authoritative_now_ms {
        return respond(WorkItemInputOutcome::Expired, Some(version));
    }
    if !matches!(property_string(&props, "status"), "leased" | "running")
        || property_string(&props, "lease_owner") != request.worker_id
        || property_u64(&props, "lease_epoch") != request.lease_epoch
        || property_u64(&props, "fencing_token") != request.fencing_token
        || property_f64(&props, "lease_expires_at") < now_s
    {
        return respond(WorkItemInputOutcome::Fenced, Some(version));
    }
    if version != request.expected_version || props.get("pending_input").is_some() {
        return respond(WorkItemInputOutcome::Conflict, Some(version));
    }
    // The native submit bound this digest to the authenticated origin. Old
    // rows without the binding cannot enter the human approval exchange.
    if !submitted_origin_is_verified(&props) {
        return respond(WorkItemInputOutcome::Denied, Some(version));
    }
    let next = version
        .checked_add(1)
        .ok_or("WorkItem row revision exhausted")?;
    let pending = WorkItemPendingInput {
        work_item_id: request.work_item_id.clone(),
        version: next,
        call_id: request.call_id.clone(),
        plan_ref: request.plan_ref.clone(),
        op: request.op.clone(),
        params_digest: request.params_digest.clone(),
        preview: request.preview.clone(),
        expires_at_ms: request.expires_at_ms,
    };
    #[cfg(feature = "statechart")]
    let pre_status = property_string(&props, "status").to_string();
    props.insert(
        "pending_input".into(),
        serde_json::to_value(pending).map_err(|e| e.to_string())?,
    );
    props.remove("input_answer");
    props.insert(
        "status".into(),
        serde_json::Value::String("input_required".into()),
    );
    props.insert(
        "state".into(),
        serde_json::Value::String("input_required".into()),
    );
    props.insert("lease_owner".into(), serde_json::Value::Null);
    props.insert("lease_expires_at".into(), serde_json::Value::Null);
    // A human wait is not an execution failure. Reclaiming after the answer
    // must continue the same attempt, including when max_attempts is one.
    let resumed_attempt = property_u64(&props, "attempt").saturating_sub(1);
    props.insert("attempt".into(), serde_json::Value::from(resumed_attempt));
    props.insert(
        "lease_epoch".into(),
        serde_json::Value::from(request.lease_epoch.saturating_add(1)),
    );
    props.insert(
        "fencing_token".into(),
        serde_json::Value::from(request.fencing_token.saturating_add(1)),
    );
    props.insert("updated_at".into(), serde_json::Value::from(now_s));
    #[cfg(feature = "statechart")]
    apply_work_item_mirror(
        &mut props,
        &request.work_item_id,
        &pre_status,
        crate::work_item_statechart::EV_INPUT_REQUESTED,
        serde_json::json!({ "fence_valid": true }),
        Some("input_required"),
    );
    write_work_item_props(nodes, graph, &request.work_item_id, &mut props, crypto)?;
    respond(WorkItemInputOutcome::Applied, Some(next))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn service_carrier_cannot_approve_a_spoofed_human_subject() {
        let service =
            crate::server::mutation_batch::principal_fingerprint("graphos-service").unwrap();
        let human = crate::server::mutation_batch::principal_fingerprint("human-a").unwrap();
        let service_row = serde_json::json!({
            "submit_principal_ref": service,
            "context": {"subject_id": "human-a"},
        })
        .as_object()
        .unwrap()
        .clone();
        assert!(!submitted_origin_is_verified(&service_row));
        let human_row = serde_json::json!({
            "submit_principal_ref": human,
            "context": {"subject_id": "human-a"},
        })
        .as_object()
        .unwrap()
        .clone();
        assert!(submitted_origin_is_verified(&human_row));
    }
}

pub(crate) fn apply_answer_work_item_input_row(
    graph: &str,
    actor: &str,
    request: &AnswerWorkItemInput,
    authoritative_now_ms: u64,
    nodes: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<Option<crate::protocol::ResultPayload>, String> {
    use eg_types::result_contract::coordination::AnswerWorkItemInput as ResultTag;
    request.validate()?;
    let respond = |outcome, version| {
        crate::protocol::ResultPayload::of::<ResultTag>(answer(
            outcome,
            &request.work_item_id,
            version,
        ))
        .map(Some)
    };
    let Some(row) = nodes.get((graph, request.work_item_id.as_str()))? else {
        return respond(WorkItemInputOutcome::Missing, None);
    };
    let mut props: serde_json::Map<String, serde_json::Value> =
        decode_durable(&crypto.unseal(row.value())?)?;
    if property_string(&props, "node_type") != "WorkItem"
        || property_string(&props, "tenant") != request.tenant
    {
        return respond(WorkItemInputOutcome::Missing, None);
    }
    let version = current_version(&props);
    if actor.is_empty() || property_string(&props, "submit_principal_ref") != actor {
        return respond(WorkItemInputOutcome::Denied, Some(version));
    }
    if property_string(&props, "status") != "input_required" || version != request.expected_version
    {
        return respond(WorkItemInputOutcome::Conflict, Some(version));
    }
    let pending: WorkItemPendingInput = serde_json::from_value(
        props
            .get("pending_input")
            .cloned()
            .ok_or("input_required WorkItem lacks pending_input")?,
    )
    .map_err(|_| "pending WorkItem input is malformed".to_string())?;
    if pending.version != version
        || pending.call_id != request.call_id
        || pending.plan_ref != request.plan_ref
        || pending.op != request.op
        || pending.params_digest != request.params_digest
    {
        return respond(WorkItemInputOutcome::Conflict, Some(version));
    }
    if authoritative_now_ms >= pending.expires_at_ms {
        return respond(WorkItemInputOutcome::Expired, Some(version));
    }
    let next = version
        .checked_add(1)
        .ok_or("WorkItem row revision exhausted")?;
    let receipt = WorkItemInputAnswer {
        work_item_id: request.work_item_id.clone(),
        version: next,
        call_id: request.call_id.clone(),
        plan_ref: request.plan_ref.clone(),
        op: request.op.clone(),
        params_digest: request.params_digest.clone(),
        decision: request.decision,
        answer_ref: request.answer_ref.clone(),
    };
    props.remove("pending_input");
    props.insert(
        "input_answer".into(),
        serde_json::to_value(receipt).map_err(|e| e.to_string())?,
    );
    props.insert("status".into(), serde_json::Value::String("ready".into()));
    props.insert("state".into(), serde_json::Value::String("ready".into()));
    props.insert("next_retry_at".into(), serde_json::Value::from(0.0));
    props.insert(
        "updated_at".into(),
        serde_json::Value::from(authoritative_now_ms as f64 / 1000.0),
    );
    #[cfg(feature = "statechart")]
    apply_work_item_mirror(
        &mut props,
        &request.work_item_id,
        "input_required",
        crate::work_item_statechart::EV_INPUT_ANSWERED,
        serde_json::json!({}),
        Some("ready"),
    );
    write_work_item_props(nodes, graph, &request.work_item_id, &mut props, crypto)?;
    respond(WorkItemInputOutcome::Applied, Some(next))
}

pub(crate) fn read_pending_work_item_input(
    shard: &Shard,
    graph: &str,
    tenant: &str,
    work_item_id: &str,
    actor: &str,
    crypto: DurableCrypto<'_>,
) -> Result<Option<WorkItemPendingInput>, String> {
    eg_types::work_item_read::validate_work_item_get(tenant, work_item_id)?;
    let (_, row) = super::read::snapshot_row(shard, graph, work_item_id, crypto)?;
    let Some(row) = row else {
        return Ok(None);
    };
    if property_string(&row, "node_type") != "WorkItem"
        || property_string(&row, "tenant") != tenant
        || actor.is_empty()
        || property_string(&row, "submit_principal_ref") != actor
    {
        return Ok(None);
    }
    if property_string(&row, "status") != "input_required" {
        return Ok(None);
    }
    row.get("pending_input")
        .cloned()
        .map(|value| {
            serde_json::from_value(value)
                .map_err(|_| "pending WorkItem input is malformed".to_string())
        })
        .transpose()
}

pub(crate) fn read_answered_work_item_input(
    shard: &Shard,
    graph: &str,
    tenant: &str,
    work_item_id: &str,
    worker_id: &str,
    actor: &str,
    lease_epoch: u64,
    fencing_token: u64,
    now_ms: u64,
    crypto: DurableCrypto<'_>,
) -> Result<Option<WorkItemInputAnswer>, String> {
    eg_types::work_item_read::validate_work_item_get(tenant, work_item_id)?;
    let (_, row) = super::read::snapshot_row(shard, graph, work_item_id, crypto)?;
    let Some(row) = row else {
        return Ok(None);
    };
    if property_string(&row, "node_type") != "WorkItem"
        || property_string(&row, "tenant") != tenant
        || property_string(&row, "lease_owner") != worker_id
        || actor.is_empty()
        || property_string(&row, "lease_principal_ref") != actor
        || !matches!(property_string(&row, "status"), "leased" | "running")
        || property_u64(&row, "lease_epoch") != lease_epoch
        || property_u64(&row, "fencing_token") != fencing_token
        || property_f64(&row, "lease_expires_at") < now_ms as f64 / 1000.0
    {
        return Ok(None);
    }
    row.get("input_answer")
        .cloned()
        .map(|value| {
            serde_json::from_value(value)
                .map_err(|_| "answered WorkItem input is malformed".to_string())
        })
        .transpose()
}
