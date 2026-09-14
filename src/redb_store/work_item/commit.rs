//! WorkItem commit.rs transitions.

use super::*;

/// The lease fence a `CommitWorkItemResult` must satisfy: the caller owns the
/// lease, the item is live, and the lease has not expired.
pub(crate) fn commit_work_item_lease_is_valid(
    props: &serde_json::Map<String, serde_json::Value>,
    worker_id: &str,
    lease_epoch: u64,
    fencing_token: u64,
    now_ms: u64,
) -> bool {
    property_string(props, "lease_owner") == worker_id
        && matches!(property_string(props, "status"), "leased" | "running")
        && property_u64(props, "lease_epoch") == lease_epoch
        && property_u64(props, "fencing_token") == fencing_token
        && property_f64(props, "lease_expires_at") >= now_ms as f64 / 1000.0
}

/// The three short-circuit responses of a commit, in their original order:
/// a tenant mismatch reads as `missing`, an already-terminal item as `noop`,
/// and a failed lease fence as `fenced`.  `Ok(None)` means the commit proceeds.
pub(crate) fn commit_work_item_result_precheck(
    props: &serde_json::Map<String, serde_json::Value>,
    work_item_id: &str,
    tenant: &str,
    worker_id: &str,
    lease_epoch: u64,
    fencing_token: u64,
    now_ms: u64,
) -> Option<
    eg_types::result_contract::coordination::WorkItemTransition<
        eg_types::result_contract::coordination::WorkItemCommitStatus,
    >,
> {
    if property_string(props, "tenant") != tenant {
        return Some(unchanged_transition(
            eg_types::result_contract::coordination::WorkItemCommitStatus::Missing,
            None,
        ));
    }
    if matches!(
        property_string(props, "status"),
        "succeeded" | "failed" | "cancelled" | "dead_letter"
    ) {
        return Some(unchanged_transition(
            eg_types::result_contract::coordination::WorkItemCommitStatus::Noop,
            Some(work_item_id),
        ));
    }
    if !commit_work_item_lease_is_valid(props, worker_id, lease_epoch, fencing_token, now_ms) {
        return Some(unchanged_transition(
            eg_types::result_contract::coordination::WorkItemCommitStatus::Fenced,
            Some(work_item_id),
        ));
    }
    None
}

/// Write the committed status into `props` and report it.  A retryable failure
/// below the attempt ceiling reschedules (`ready` + backoff + bumped fence) and
/// reports `retry_scheduled`; otherwise the item goes terminal (`dead_letter`
/// for an exhausted retryable failure, else the outcome verb itself).
pub(crate) fn commit_work_item_apply_status<'o>(
    props: &mut serde_json::Map<String, serde_json::Value>,
    outcome: &'o str,
    retryable: bool,
    lease_epoch: u64,
    fencing_token: u64,
    now_s: f64,
) -> &'o str {
    let attempts = property_u64(props, "attempt");
    let max_attempts = property_u64(props, "max_attempts").max(1);
    if outcome == "failed" && retryable && attempts < max_attempts {
        let backoff = property_f64(props, "backoff_base_s").max(1.0)
            * 2f64.powi(attempts.saturating_sub(1).min(31) as i32);
        props.insert("status".into(), serde_json::Value::String("ready".into()));
        props.insert(
            "next_retry_at".into(),
            serde_json::Value::from(now_s + backoff),
        );
        props.insert(
            "lease_epoch".into(),
            serde_json::Value::from((lease_epoch).saturating_add(1)),
        );
        props.insert(
            "fencing_token".into(),
            serde_json::Value::from((fencing_token).saturating_add(1)),
        );
        return "retry_scheduled";
    }
    let terminal = if outcome == "failed" && retryable {
        "dead_letter"
    } else {
        outcome
    };
    props.insert("status".into(), serde_json::Value::String(terminal.into()));
    props.insert("completed_at".into(), serde_json::Value::from(now_s));
    terminal
}

/// Record the commit's lease/result bookkeeping on the item.
pub(crate) fn commit_work_item_record_result_refs(
    props: &mut serde_json::Map<String, serde_json::Value>,
    worker_id: &str,
    result_ref: &Option<String>,
    error_ref: &Option<String>,
    now_s: f64,
) {
    props.insert(
        "result_ref".into(),
        result_ref
            .clone()
            .map(serde_json::Value::String)
            .unwrap_or(serde_json::Value::Null),
    );
    props.insert(
        "error_ref".into(),
        error_ref
            .clone()
            .map(serde_json::Value::String)
            .unwrap_or(serde_json::Value::Null),
    );
    props.insert("lease_owner".into(), serde_json::Value::Null);
    props.insert(
        "last_lease_owner".into(),
        serde_json::Value::String(worker_id.to_string()),
    );
    props.insert("lease_expires_at".into(), serde_json::Value::Null);
    props.insert("updated_at".into(), serde_json::Value::from(now_s));
}

/// Decrement each downstream child's dependency count after a successful
/// commit, releasing a child to `ready` once its last dependency clears.
/// Children that no longer exist are skipped, as before.
pub(crate) fn commit_work_item_release_downstream(
    graph: &str,
    props: &serde_json::Map<String, serde_json::Value>,
    now_s: f64,
    nodes: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
    changed: &mut Vec<String>,
) -> Result<(), String> {
    let downstream = props
        .get("downstream_ids")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default();
    for child in downstream.iter().filter_map(serde_json::Value::as_str) {
        let child_bytes = nodes
            .get((graph, child))?
            .map(|value| crypto.unseal(value.value()))
            .transpose()?;
        let Some(child_bytes) = child_bytes else {
            continue;
        };
        let mut child_props: serde_json::Map<String, serde_json::Value> =
            decode_durable(&child_bytes)?;
        let count = property_u64(&child_props, "dep_count").saturating_sub(1);
        child_props.insert("dep_count".into(), serde_json::Value::from(count));
        if count == 0 && property_string(&child_props, "status") == "submitted" {
            child_props.insert("status".into(), serde_json::Value::String("ready".into()));
        }
        child_props.insert("updated_at".into(), serde_json::Value::from(now_s));
        write_work_item_props(nodes, graph, child, &child_props, crypto)?;
        changed.push(child.to_string());
    }
    Ok(())
}

pub(crate) struct CommitWorkItemResultInput<'args, 'table, 'crypto> {
    pub(crate) graph: &'args str,
    pub(crate) tenant: &'args str,
    pub(crate) work_item_id: &'args str,
    pub(crate) worker_id: &'args str,
    pub(crate) lease_epoch: u64,
    pub(crate) fencing_token: u64,
    pub(crate) outcome: &'args str,
    pub(crate) result_ref: &'args Option<String>,
    pub(crate) error_ref: &'args Option<String>,
    pub(crate) retryable: bool,
    pub(crate) now_ms: u64,
    pub(crate) outcome_extension: Option<&'args eg_types::outcome_bundle::TerminalOutcomeExtension>,
    pub(crate) batch_id: &'args str,
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
    pub(crate) crypto: DurableCrypto<'crypto>,
}

pub(crate) fn apply_commit_work_item_result_row(
    input: CommitWorkItemResultInput<'_, '_, '_>,
) -> Result<Option<crate::protocol::ResultPayload>, String> {
    let current = input
        .nodes
        .get((input.graph, input.work_item_id))?
        .map(|value| input.crypto.unseal(value.value()))
        .transpose()?;
    let Some(bytes) = current else {
        return commit_transition_result(unchanged_transition(
            eg_types::result_contract::coordination::WorkItemCommitStatus::Missing,
            None,
        ))
        .map(Some);
    };
    let mut props: serde_json::Map<String, serde_json::Value> = decode_durable(&bytes)?;
    let pre_props = props.clone();
    if let Some(refusal) = commit_work_item_result_precheck(
        &props,
        input.work_item_id,
        input.tenant,
        input.worker_id,
        input.lease_epoch,
        input.fencing_token,
        input.now_ms,
    ) {
        return commit_transition_result(refusal).map(Some);
    }
    if !matches!(input.outcome, "succeeded" | "failed" | "cancelled") {
        return Err("CommitWorkItemResult outcome must be succeeded, failed, or cancelled".into());
    }
    if let Some(extension) = input.outcome_extension {
        validate_terminal_extension_binding(
            &props,
            extension,
            TerminalCommitClaim {
                worker_id: input.worker_id,
                work_item_id: input.work_item_id,
                fencing_token: input.fencing_token,
                outcome: input.outcome,
                result_ref: input.result_ref,
                batch_id: input.batch_id,
            },
        )?;
        ensure_receipt_rows_absent(input.graph, &extension.receipt_nodes, input.nodes)?;
    }
    let now_s = input.now_ms as f64 / 1000.0;
    // Phase-1 mirror inputs: pre-status (leased|running, validated above) + the
    // DLQ-threshold policy boolean (retryable && attempt < max_attempts) that the
    // chart reads as a pre-computed guard input.
    #[cfg(feature = "statechart")]
    let pre_status = property_string(&props, "status").to_string();
    #[cfg(feature = "statechart")]
    let commit_retry_eligible =
        property_u64(&props, "attempt") < property_u64(&props, "max_attempts").max(1);
    let committed_status = commit_work_item_apply_status(
        &mut props,
        input.outcome,
        input.retryable,
        input.lease_epoch,
        input.fencing_token,
        now_s,
    );
    commit_work_item_record_result_refs(
        &mut props,
        input.worker_id,
        input.result_ref,
        input.error_ref,
        now_s,
    );
    development_lane::transition_work_item_terminal_hold(
        input.graph,
        &pre_props,
        input.work_item_id,
        committed_status,
        false,
        property_u64(&props, "attempt"),
        property_u64(&props, "lease_epoch"),
        property_u64(&props, "fencing_token"),
        property_string(&props, "work_item_fence"),
        input.holds,
        input.work_item_index,
        input.counters,
        input.pressure_index,
        input.policies,
        input.crypto,
    )?;
    // Phase-1 mirror: the commit outcome maps to the chart's commit_* event; the
    // authoritative next state is whatever the handler persisted (ready on a
    // scheduled retry, else the terminal). The chart must independently agree.
    #[cfg(feature = "statechart")]
    {
        let event = match input.outcome {
            "succeeded" => crate::work_item_statechart::EV_COMMIT_SUCCEEDED,
            "cancelled" => crate::work_item_statechart::EV_COMMIT_CANCELLED,
            _ => crate::work_item_statechart::EV_COMMIT_FAILED,
        };
        let mirror_payload = serde_json::json!({
            "fence_valid": true,
            "retryable": input.retryable,
            "retry_eligible": commit_retry_eligible,
        });
        let authoritative_next = property_string(&props, "status").to_string();
        apply_work_item_mirror(
            &mut props,
            input.work_item_id,
            &pre_status,
            event,
            mirror_payload,
            Some(&authoritative_next),
        );
    }
    write_work_item_props(
        input.nodes,
        input.graph,
        input.work_item_id,
        &props,
        input.crypto,
    )?;

    let mut changed = vec![input.work_item_id.to_string()];
    if committed_status == "succeeded" {
        commit_work_item_release_downstream(
            input.graph,
            &props,
            now_s,
            input.nodes,
            input.crypto,
            &mut changed,
        )?;
    }
    if committed_status != "retry_scheduled" {
        if let Some(extension) = input.outcome_extension {
            apply_receipt_rows(
                input.graph,
                &extension.receipt_nodes,
                input.nodes,
                input.crypto,
            )?;
        }
    }
    commit_transition_result(
        eg_types::result_contract::coordination::WorkItemTransition {
            status: declared_commit_status(committed_status)?,
            work_item_id: Some(input.work_item_id.to_string()),
            lease_epoch: Some(input.lease_epoch),
            fencing_token: Some(input.fencing_token),
            changed_work_item_ids: changed,
        },
    )
    .map(Some)
}

/// The terminal commit an outcome bundle has to be bound to.
///
/// These are the admitted `Method::CommitWorkItemResult` facts plus the batch
/// the commit rides in. A bundle is only trustworthy if it agrees with ALL of
/// them at once -- a bundle that names the right work item but the wrong worker,
/// fence or batch is exactly the forgery this check exists to refuse -- so they
/// are carried as one claim rather than six positional facts a caller can
/// transpose.
struct TerminalCommitClaim<'a> {
    /// The worker the lease is held by, and which the bundle's
    /// `executor_lease_actor` must name.
    worker_id: &'a str,
    work_item_id: &'a str,
    fencing_token: u64,
    /// `succeeded` / `failed` / `cancelled`, already validated by the caller.
    outcome: &'a str,
    result_ref: &'a Option<String>,
    /// The mutation batch this commit rides in; the bundle's outbox id must
    /// match it, so a bundle cannot be replayed under a different batch.
    batch_id: &'a str,
}

fn validate_terminal_extension_binding(
    props: &serde_json::Map<String, serde_json::Value>,
    extension: &eg_types::outcome_bundle::TerminalOutcomeExtension,
    claim: TerminalCommitClaim<'_>,
) -> Result<(), String> {
    let TerminalCommitClaim {
        worker_id,
        work_item_id,
        fencing_token,
        outcome,
        result_ref,
        batch_id,
    } = claim;
    extension.validate()?;
    let bundle = &extension.outcome_bundle;
    if bundle.outcome != outcome {
        return Err(
            "terminal outcome bundle outcome does not match the terminal method".to_string(),
        );
    }
    if bundle.work_item_id != work_item_id
        || bundle.fence_token != fencing_token
        || bundle.result_ref != *result_ref
    {
        return Err("terminal outcome bundle does not match the WorkItem CAS".to_string());
    }
    if bundle.executor_lease_actor != worker_id {
        return Err("terminal outcome bundle executor does not own the lease".to_string());
    }
    if bundle.outbox_id != batch_id {
        return Err("terminal outcome bundle outbox does not match the mutation batch".to_string());
    }
    let metadata = props
        .get("metadata")
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| "WorkItem metadata is missing delegation bindings".to_string())?;
    let context = props
        .get("context")
        .and_then(serde_json::Value::as_object)
        .ok_or_else(|| "WorkItem context is missing delegation bindings".to_string())?;
    require_binding(
        "delegation_id",
        metadata.get("delegation_id"),
        &bundle.delegation_id,
    )?;
    require_binding("run_id", metadata.get("run_id"), &bundle.run_id)?;
    require_binding(
        "delegator_id",
        context.get("agent_id"),
        &bundle.delegator_id,
    )?;
    require_binding(
        "selected_agent_id",
        metadata.get("agent_id"),
        &bundle.selected_agent_id,
    )?;
    require_digest_binding(
        "capability_digest",
        metadata.get("capability_digest"),
        &bundle.capability_digest,
    )?;
    for (field, expected) in [
        ("catalog_digest", &bundle.catalog_digest),
        ("model_digest", &bundle.model_digest),
    ] {
        require_digest_binding(field, props.get(field), expected)?;
    }
    require_digest_binding(
        "policy_digest",
        props.get("policy_digest"),
        &bundle.policy_digest,
    )
}

fn require_binding(
    field: &str,
    actual: Option<&serde_json::Value>,
    expected: &str,
) -> Result<(), String> {
    let actual = actual
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| format!("WorkItem binding '{field}' is missing"))?;
    if actual != expected {
        return Err(format!(
            "WorkItem binding '{field}' does not match terminal outcome"
        ));
    }
    Ok(())
}

fn require_digest_binding(
    field: &str,
    actual: Option<&serde_json::Value>,
    expected: &str,
) -> Result<(), String> {
    let actual = actual
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| format!("WorkItem binding '{field}' is missing"))?;
    let normalized = actual.strip_prefix("sha256:").unwrap_or(actual);
    if normalized != expected {
        return Err(format!(
            "WorkItem binding '{field}' does not match terminal outcome"
        ));
    }
    Ok(())
}

fn ensure_receipt_rows_absent(
    graph: &str,
    receipt_nodes: &[eg_types::outcome_bundle::ReceiptNode],
    nodes: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
) -> Result<(), String> {
    for receipt in receipt_nodes {
        if nodes.get((graph, receipt.node_id.as_str()))?.is_some() {
            return Err(format!(
                "terminal receipt node '{}' already exists",
                receipt.node_id
            ));
        }
    }
    Ok(())
}

fn apply_receipt_rows(
    graph: &str,
    receipt_nodes: &[eg_types::outcome_bundle::ReceiptNode],
    nodes: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    for receipt in receipt_nodes {
        let sealed = crypto.seal(&receipt.properties_msgpack);
        nodes
            .insert((graph, receipt.node_id.as_str()), sealed.as_ref())
            .map_err(|error| error.to_string())?;
    }
    Ok(())
}
