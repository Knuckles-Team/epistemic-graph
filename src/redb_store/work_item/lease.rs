//! WorkItem lease.rs transitions.

use super::*;

pub(crate) struct RenewWorkItemLeaseInput<'args, 'table, 'crypto> {
    pub(crate) graph: &'args str,
    pub(crate) tenant: &'args String,
    pub(crate) work_item_id: &'args str,
    pub(crate) worker_id: &'args String,
    pub(crate) lease_epoch: u64,
    pub(crate) fencing_token: u64,
    pub(crate) now_ms: u64,
    pub(crate) lease_ms: u64,
    pub(crate) nodes:
        &'args mut ScopedOwnerTableMut<'table, (&'static str, &'static str), &'static [u8]>,
    pub(crate) crypto: DurableCrypto<'crypto>,
}

pub(crate) fn apply_renew_work_item_lease_row(
    input: RenewWorkItemLeaseInput<'_, '_, '_>,
) -> Result<Option<crate::protocol::ResultPayload>, String> {
    let RenewWorkItemLeaseInput {
        graph,
        tenant,
        work_item_id,
        worker_id,
        lease_epoch,
        fencing_token,
        now_ms,
        lease_ms,
        nodes,
        crypto,
    } = input;
    let decode = |bytes: &[u8]| -> Result<serde_json::Map<String, serde_json::Value>, String> {
        decode_durable(bytes)
    };
    if worker_id.trim().is_empty() || lease_ms == 0 {
        return Err("RenewWorkItemLease requires worker_id and non-zero lease_ms".into());
    }
    let current = nodes
        .get((graph, work_item_id))?
        .map(|value| crypto.unseal(value.value()))
        .transpose()?;
    // Every WorkItem result — including one that changed no row — MUST carry
    // `changed_work_item_ids`. The commit has already advanced the authoritative
    // graph version by the time `commit_work_item` reads this field, so a shape
    // missing it strands the serving projection one version behind and makes the
    // graph permanently read-only (INCIDENT-kg-readonly-2026-07-31).
    let Some(bytes) = current else {
        return lease_renewal_refused(
            eg_types::result_contract::coordination::LeaseRenewalRefusal::Missing,
        )
        .map(Some);
    };
    let mut props = decode(&bytes)?;
    let valid = property_string(&props, "tenant") == tenant
        && property_string(&props, "lease_owner") == worker_id
        && matches!(property_string(&props, "status"), "leased" | "running")
        && property_u64(&props, "lease_epoch") == lease_epoch
        && property_u64(&props, "fencing_token") == fencing_token
        && property_f64(&props, "lease_expires_at") >= now_ms as f64 / 1000.0;
    if !valid {
        return lease_renewal_refused(
            eg_types::result_contract::coordination::LeaseRenewalRefusal::Fenced,
        )
        .map(Some);
    }
    // Phase-1 mirror: the lease was validated (fence_valid), so leased|running →
    // running. Capture the pre-status before the authority overwrites it.
    #[cfg(feature = "statechart")]
    let pre_status = property_string(&props, "status").to_string();
    let now_s = now_ms as f64 / 1000.0;
    props.insert("status".into(), serde_json::Value::String("running".into()));
    props.insert("heartbeat_at".into(), serde_json::Value::from(now_s));
    props.insert("updated_at".into(), serde_json::Value::from(now_s));
    props.insert(
        "lease_expires_at".into(),
        serde_json::Value::from(now_s + lease_ms as f64 / 1000.0),
    );
    #[cfg(feature = "statechart")]
    apply_work_item_mirror(
        &mut props,
        work_item_id,
        &pre_status,
        crate::work_item_statechart::EV_RENEW,
        serde_json::json!({ "fence_valid": true }),
        Some("running"),
    );
    write_work_item_props(nodes, graph, work_item_id, &props, crypto)?;
    lease_renewed(
        work_item_id,
        lease_epoch,
        fencing_token,
        now_ms.saturating_add(lease_ms),
    )
    .map(Some)
}

/// A `RenewWorkItemLease` that extended the caller's live lease.
fn lease_renewed(
    work_item_id: &str,
    lease_epoch: u64,
    fencing_token: u64,
    lease_expires_at_ms: u64,
) -> Result<crate::protocol::ResultPayload, String> {
    crate::protocol::ResultPayload::of::<eg_types::result_contract::coordination::RenewWorkItemLease>(
        eg_types::result_contract::coordination::WorkItemLeaseRenewal {
            renewed: true,
            reason: None,
            work_item_id: Some(work_item_id.to_string()),
            lease_epoch: Some(lease_epoch),
            fencing_token: Some(fencing_token),
            lease_expires_at_ms: Some(lease_expires_at_ms),
            changed_work_item_ids: vec![work_item_id.to_string()],
        },
    )
}

/// A `RenewWorkItemLease` that renewed nothing and changed no row.
fn lease_renewal_refused(
    reason: eg_types::result_contract::coordination::LeaseRenewalRefusal,
) -> Result<crate::protocol::ResultPayload, String> {
    crate::protocol::ResultPayload::of::<eg_types::result_contract::coordination::RenewWorkItemLease>(
        eg_types::result_contract::coordination::WorkItemLeaseRenewal {
            renewed: false,
            reason: Some(reason),
            work_item_id: None,
            lease_epoch: None,
            fencing_token: None,
            lease_expires_at_ms: None,
            changed_work_item_ids: Vec::new(),
        },
    )
}

/// A WorkItem transition that changed no row.
fn unchanged_transition<Status>(
    status: Status,
    work_item_id: Option<&str>,
) -> eg_types::result_contract::coordination::WorkItemTransition<Status> {
    eg_types::result_contract::coordination::WorkItemTransition {
        status,
        work_item_id: work_item_id.map(str::to_string),
        lease_epoch: None,
        fencing_token: None,
        changed_work_item_ids: Vec::new(),
    }
}

fn commit_transition_result(
    transition: eg_types::result_contract::coordination::WorkItemTransition<
        eg_types::result_contract::coordination::WorkItemCommitStatus,
    >,
) -> Result<crate::protocol::ResultPayload, String> {
    crate::protocol::ResultPayload::of::<
        eg_types::result_contract::coordination::CommitWorkItemResult,
    >(transition)
}

fn cancel_transition_result(
    transition: eg_types::result_contract::coordination::WorkItemTransition<
        eg_types::result_contract::coordination::WorkItemCancelStatus,
    >,
) -> Result<crate::protocol::ResultPayload, String> {
    crate::protocol::ResultPayload::of::<eg_types::result_contract::coordination::CancelWorkItem>(
        transition,
    )
}

/// The declared status of an applied commit.
fn declared_commit_status(
    status: &str,
) -> Result<eg_types::result_contract::coordination::WorkItemCommitStatus, String> {
    match status {
        "succeeded" => Ok(eg_types::result_contract::coordination::WorkItemCommitStatus::Succeeded),
        "failed" => Ok(eg_types::result_contract::coordination::WorkItemCommitStatus::Failed),
        "cancelled" => Ok(eg_types::result_contract::coordination::WorkItemCommitStatus::Cancelled),
        "dead_letter" => {
            Ok(eg_types::result_contract::coordination::WorkItemCommitStatus::DeadLetter)
        }
        "retry_scheduled" => {
            Ok(eg_types::result_contract::coordination::WorkItemCommitStatus::RetryScheduled)
        }
        other => Err(format!(
            "CommitWorkItemResult produced an undeclared status '{other}'"
        )),
    }
}

/// Shape validation for a `CasWorkItemMetadata` request, in the original order:
/// exactly one settable field, a non-empty expected status set, and non-blank
/// tenant/work-item identifiers.
pub(crate) fn validate_cas_work_item_metadata_request(
    request: &crate::epistemic_operations_ext::CasWorkItemMetadataRequest,
) -> Result<(), String> {
    let field_pairs_set = [
        request.set_checkpoint_id.is_some(),
        request.set_metadata_msgpack.is_some(),
        request.set_prio_bucket.is_some(),
    ]
    .into_iter()
    .filter(|set| *set)
    .count();
    if field_pairs_set != 1 {
        return Err(
            "CasWorkItemMetadata requires exactly one of set_checkpoint_id / \
             set_metadata_msgpack / set_prio_bucket"
                .to_string(),
        );
    }
    if request.expected_status.is_empty() {
        return Err("CasWorkItemMetadata requires a non-empty expected_status".into());
    }
    if request.tenant_ref.trim().is_empty() || request.work_item_id.trim().is_empty() {
        return Err("CasWorkItemMetadata requires tenant_ref and work_item_id".into());
    }
    Ok(())
}

/// The status / tenant / lease-fence preconditions of a metadata CAS.  All three
/// are evaluated (as before) and the conjunction decides; a `false` here is a
/// `Conflict` outcome, not an error.
pub(crate) fn cas_work_item_metadata_preconditions_ok(
    request: &crate::epistemic_operations_ext::CasWorkItemMetadataRequest,
    props: &serde_json::Map<String, serde_json::Value>,
) -> bool {
    let tenant = &request.tenant_ref;
    let status_ok = request
        .expected_status
        .iter()
        .any(|status| status == property_string(props, "status"));
    let tenant_ok = property_string(props, "tenant") == tenant;
    let lease_ok = match &request.expected_lease {
        Some(fence) => {
            property_string(props, "lease_owner") == fence.worker_ref
                && property_u64(props, "lease_epoch") == fence.lease_epoch
                && property_u64(props, "fencing_token") == fence.fencing_token
        }
        None => true,
    };
    status_ok && tenant_ok && lease_ok
}

/// Apply the one settable field of a metadata CAS to `props`, after checking its
/// own expected pre-image.  Returns `Ok(false)` when that pre-image does not
/// match -- the caller turns that into a `Conflict` outcome, exactly as the
/// inline branches did.  `props` is only mutated on the matching path.
pub(crate) fn apply_cas_work_item_metadata_field(
    request: &crate::epistemic_operations_ext::CasWorkItemMetadataRequest,
    props: &mut serde_json::Map<String, serde_json::Value>,
) -> Result<bool, String> {
    if let Some(set_checkpoint_id) = &request.set_checkpoint_id {
        let current_checkpoint_id = props
            .get("checkpoint_id")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string);
        if current_checkpoint_id != request.expected_checkpoint_id {
            return Ok(false);
        }
        props.insert(
            "checkpoint_id".into(),
            serde_json::Value::String(set_checkpoint_id.clone()),
        );
    } else if let Some(set_metadata_bytes) = &request.set_metadata_msgpack {
        let current_metadata = props
            .get("metadata")
            .cloned()
            .unwrap_or(serde_json::Value::Object(Default::default()));
        let expected_metadata = match &request.expected_metadata_msgpack {
            Some(bytes) => decode_durable::<serde_json::Value>(bytes)
                .map_err(|_| "invalid expected_metadata_msgpack".to_string())?,
            None => serde_json::Value::Object(Default::default()),
        };
        if current_metadata != expected_metadata {
            return Ok(false);
        }
        let set_metadata = decode_durable::<serde_json::Value>(set_metadata_bytes)
            .map_err(|_| "invalid set_metadata_msgpack".to_string())?;
        props.insert("metadata".into(), set_metadata);
    } else if let Some(set_prio_bucket) = request.set_prio_bucket {
        let expected_prio_bucket = request.expected_prio_bucket.unwrap_or(0);
        let current_prio_bucket = props
            .get("prio_bucket")
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0);
        if current_prio_bucket != expected_prio_bucket {
            return Ok(false);
        }
        props.insert(
            "prio_bucket".into(),
            serde_json::Value::from(set_prio_bucket),
        );
    }
    Ok(true)
}

pub(crate) fn apply_cas_work_item_metadata_row(
    graph: &str,
    request: &crate::epistemic_operations_ext::CasWorkItemMetadataRequest,
    nodes: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<Option<crate::protocol::ResultPayload>, String> {
    use crate::epistemic_operations_ext::{
        CasWorkItemMetadataOutcome, CasWorkItemMetadataResult,
        CasWorkItemMetadataResultSchemaVersion,
    };

    let work_item_id = &request.work_item_id;
    let now_ms = request.now_ms;

    validate_cas_work_item_metadata_request(request)?;

    let respond = |outcome: CasWorkItemMetadataOutcome, changed: Vec<String>| {
        Ok(Some(crate::protocol::ResultPayload::of::<
            eg_types::result_contract::coordination::CasWorkItemMetadata,
        >(CasWorkItemMetadataResult {
            schema_version: CasWorkItemMetadataResultSchemaVersion::V1,
            outcome,
            work_item_id: work_item_id.clone(),
            changed_work_item_ids: changed,
        })?))
    };

    let current = nodes
        .get((graph, work_item_id.as_str()))?
        .map(|value| crypto.unseal(value.value()))
        .transpose()?;
    let Some(bytes) = current else {
        return respond(CasWorkItemMetadataOutcome::NotFound, vec![]);
    };
    let mut props: serde_json::Map<String, serde_json::Value> = decode_durable(&bytes)?;

    if !cas_work_item_metadata_preconditions_ok(request, &props) {
        return respond(CasWorkItemMetadataOutcome::Conflict, vec![]);
    }

    if !apply_cas_work_item_metadata_field(request, &mut props)? {
        return respond(CasWorkItemMetadataOutcome::Conflict, vec![]);
    }

    let now_s = now_ms as f64 / 1000.0;
    props.insert("updated_at".into(), serde_json::Value::from(now_s));
    write_work_item_props(nodes, graph, work_item_id, &props, crypto)?;
    respond(
        CasWorkItemMetadataOutcome::Applied,
        vec![work_item_id.clone()],
    )
}
