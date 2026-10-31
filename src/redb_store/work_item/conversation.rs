//! Conversation order is reserved with native run admission, not a second store.
use super::*;
use medulla_types::native_control::{
    ConversationTurnAdmission, NativeControlSchemaVersion, SubmitWorkItemRequest,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Head {
    schema_version: NativeControlSchemaVersion,
    node_type: String,
    graph: String,
    tenant: String,
    owner: String,
    context_id: String,
    revision: u64,
    run_id: String,
}

fn head_id(tenant: &str, context: &str) -> String {
    let mut digest = Sha256::new();
    for field in ["conversation-head", tenant, context] {
        digest.update((field.len() as u64).to_be_bytes());
        digest.update(field.as_bytes());
    }
    format!("conversation-head:{}", hex::encode(digest.finalize()))
}

fn validate<'a>(
    request: &'a SubmitWorkItemRequest,
    turn: &ConversationTurnAdmission,
    actor: Option<&'a str>,
) -> Result<(&'a str, &'a str), String> {
    let actor = actor
        .filter(|s| !s.is_empty())
        .ok_or("ACCESS_DENIED: conversation owner is unavailable")?;
    let run = request
        .run_id
        .as_deref()
        .ok_or("CONVERSATION_INVALID: native run identity is required")?;
    if turn.schema_version != NativeControlSchemaVersion::V1
        || turn.context_id.trim().is_empty()
        || turn.context_id.len() > 512
        || turn
            .predecessor_run_id
            .as_ref()
            .is_some_and(|id| id.trim().is_empty() || id.len() > 512 || id == run)
        || (turn.expected_revision == 0) != turn.predecessor_run_id.is_none()
        || request
            .metadata
            .get("a2a_context_id")
            .is_some_and(|value| value.as_str() != Some(turn.context_id.as_str()))
    {
        return Err("CONVERSATION_INVALID: invalid context or predecessor binding".into());
    }
    Ok((actor, run))
}

fn preceding_run(
    head: &Head,
    turn: &ConversationTurnAdmission,
    nodes: &NativeNodeRows<'_>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    if head.revision != turn.expected_revision
        || turn.predecessor_run_id.as_deref() != Some(head.run_id.as_str())
    {
        return Err("CONVERSATION_CONFLICT: expected conversation head changed".into());
    }
    let id = super::run_owner::work_item_id(&head.graph, &head.tenant, &head.run_id)?;
    let bytes = nodes
        .get((head.graph.as_str(), id.as_str()))?
        .ok_or("CONVERSATION_UNAVAILABLE: predecessor is missing")?;
    let row: serde_json::Map<String, serde_json::Value> =
        decode_durable(&crypto.unseal(bytes.value())?)?;
    if !super::run_owner::owned(
        &row,
        &head.graph,
        &head.tenant,
        &head.run_id,
        &id,
        &head.owner,
    ) {
        return Err("CONVERSATION_UNAVAILABLE: predecessor ownership is invalid".into());
    }
    let original: SubmitWorkItemRequest = serde_json::from_value(
        row.get(super::run_owner::ADMISSION_REQUEST)
            .cloned()
            .ok_or("CONVERSATION_UNAVAILABLE: predecessor admission is missing")?,
    )
    .map_err(|_| "CONVERSATION_UNAVAILABLE: predecessor admission is invalid".to_string())?;
    if !admission_matches(&original, head) {
        return Err("CONVERSATION_UNAVAILABLE: predecessor admission binding changed".into());
    }
    match property_string(&row, "status") {
        "succeeded" => Ok(()),
        "failed" | "cancelled" | "dead_letter" => {
            Err("CONVERSATION_PREDECESSOR_FAILED: explicit recovery is required".into())
        }
        "submitted" | "ready" | "leased" | "running" => {
            Err("CONVERSATION_BUSY: preceding turn is still active".into())
        }
        _ => Err("CONVERSATION_UNAVAILABLE: predecessor state is unknown".into()),
    }
}

fn admission_matches(original: &SubmitWorkItemRequest, head: &Head) -> bool {
    original.run_id.as_deref() == Some(head.run_id.as_str())
        && original.context.tenant_id == head.tenant
        && sanitize(&original.context.graph) == head.graph
        && original.conversation.as_ref().is_some_and(|previous| {
            previous.context_id == head.context_id
                && previous.expected_revision.checked_add(1) == Some(head.revision)
        })
}

/// Called only inside SubmitWorkItem's existing MutationBatch transaction.
/// The protected NODES table inherits its graph-incarnation scope. No checkpoint
/// bytes, budgets, tool grants or private execution state are stored here.
pub(super) fn reserve(
    graph: &str,
    request: &SubmitWorkItemRequest,
    actor: Option<&str>,
    nodes: &mut NativeNodeRows<'_>,
    crypto: DurableCrypto<'_>,
) -> Result<Option<String>, String> {
    let Some(turn) = &request.conversation else {
        if let Some(context) = request
            .metadata
            .get("a2a_context_id")
            .and_then(serde_json::Value::as_str)
        {
            let id = head_id(&request.context.tenant_id, context);
            if nodes.get((graph, id.as_str()))?.is_some() {
                return Err(
                    "CONVERSATION_INVALID: an ordered context requires typed admission".into(),
                );
            }
        }
        return Ok(None);
    };
    let (owner, run) = validate(request, turn, actor)?;
    let tenant = request.context.tenant_id.as_str();
    let id = head_id(tenant, &turn.context_id);
    let existing = nodes
        .get((graph, id.as_str()))?
        .map(|value| {
            let bytes = crypto.unseal(value.value())?;
            decode_durable::<Head>(&bytes)
        })
        .transpose()?;
    match existing {
        Some(head) => {
            if head.schema_version != NativeControlSchemaVersion::V1
                || head.node_type != "ConversationHead"
                || head.graph != graph
                || head.tenant != tenant
                || head.owner != owner
                || head.context_id != turn.context_id
            {
                return Err("ACCESS_DENIED: conversation owner is unavailable".into());
            }
            preceding_run(&head, turn, nodes, crypto)?;
        }
        None if turn.expected_revision != 0 => {
            return Err("CONVERSATION_CONFLICT: conversation head is missing".into())
        }
        None => {}
    }
    let revision = turn
        .expected_revision
        .checked_add(1)
        .ok_or("CONVERSATION_CONFLICT: conversation revision exhausted")?;
    let head = Head {
        schema_version: NativeControlSchemaVersion::V1,
        node_type: "ConversationHead".into(),
        graph: graph.into(),
        tenant: tenant.into(),
        owner: owner.into(),
        context_id: turn.context_id.clone(),
        revision,
        run_id: run.into(),
    };
    let encoded = rmp_serde::to_vec_named(&head).map_err(|e| e.to_string())?;
    let sealed = crypto.seal(&encoded);
    nodes.insert((graph, id.as_str()), sealed.as_ref())?;
    Ok(Some(id))
}

#[cfg(test)]
mod tests;
