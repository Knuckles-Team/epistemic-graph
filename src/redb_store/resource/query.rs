use super::*;

pub(crate) fn resource_request_from_record(
    record: &ResourceReservationRecord,
    now_ms: u64,
) -> ResourceReservationRequest {
    ResourceReservationRequest {
        schema_version: crate::epistemic_operations::ResourceReservationRequestSchemaVersion::V1,
        tenant_ref: record.tenant_ref.clone(),
        work_item_id: record.work_item_id.clone(),
        owner_id: record.owner_id.clone(),
        fence: record.fence.clone(),
        lease_epoch: record.lease_epoch,
        fencing_token: record.fencing_token,
        attempt: record.attempt,
        reservation_id: record.reservation_id.clone(),
        input_fingerprint: record.input_fingerprint.clone(),
        profile_name: record.profile_name.clone(),
        profile_version: record.profile_version.clone(),
        host_ref: record.host_ref.clone(),
        requirement: record.requirement.clone(),
        target_kind: match record.target_kind {
            ResourceReservationRecordTargetKind::Local => {
                ResourceReservationRequestTargetKind::Local
            }
            ResourceReservationRecordTargetKind::InventoryAlias => {
                ResourceReservationRequestTargetKind::InventoryAlias
            }
        },
        target_alias: record.target_alias.clone(),
        repository_id: record.repository_id.clone(),
        branch: record.branch.clone(),
        concurrency_key: record.concurrency_key.clone(),
        concurrency_limit: record.concurrency_limit,
        repository_exclusive: record.repository_exclusive,
        branch_exclusive: record.branch_exclusive,
        required_labels: record.required_labels.clone(),
        anti_affinity: record.anti_affinity.clone(),
        fairness_group: record.fairness_group.clone(),
        fairness_cost: record.fairness_cost,
        disk_low_watermark_mib: record.disk_low_watermark_mib,
        disk_high_watermark_mib: record.disk_high_watermark_mib,
        disk_policy_key: record.disk_policy_key.clone(),
        reserved_at_ms: record.reserved_at_ms,
        expires_at_ms: record.expires_at_ms,
        idempotency_key: format!("query:{}", record.reservation_id),
        now_ms,
        expected_host_revision: record.expected_host_revision,
        expected_lifecycle_revision: record.expected_lifecycle_revision,
    }
}

pub(crate) fn resource_no_reservation_query_result(
    request: &ResourceReservationStatusRequest,
    decision: ResourceReservationResultDecision,
) -> Result<ResourceReservationResult, String> {
    let work_item_id = request.work_item_id.clone().unwrap_or_default();
    Ok(ResourceReservationResult {
        schema_version: ResourceReservationResultSchemaVersion::V1,
        decision,
        reservation_id: None,
        work_item_id,
        attempt: request.attempt.unwrap_or(1),
        lease_epoch: request.lease_epoch.unwrap_or(0),
        fencing_token: request.fencing_token.unwrap_or(0),
        lifecycle_revision: 0,
        host_ref: None,
        host_revision: 0,
        record: None,
        state: ResourceReservationResultState::Absent,
        held_cpu_weight: 0,
        held_memory_mib: 0,
        held_disk_mib: 0,
        held_process_slots: 0,
        fairness_debt: 0,
        tombstone: false,
        changed_work_item_ids: Vec::new(),
    })
}

/// Bounded-text validation of a status query's optional selectors, in the
/// original field order so the first offending field is still the one reported.
pub(crate) fn resource_validate_query_selectors(
    request: &ResourceReservationStatusRequest,
) -> Result<(), String> {
    if let Some(value) = request.work_item_id.as_deref() {
        resource_text(value, "resource query work_item_id")?;
    }
    if let Some(value) = request.reservation_id.as_deref() {
        resource_text(value, "resource query reservation_id")?;
    }
    if let Some(value) = request.host_ref.as_deref() {
        resource_text(value, "resource query host_ref")?;
    }
    if let Some(value) = request.owner_id.as_deref() {
        resource_text(value, "resource query owner_id")?;
    }
    Ok(())
}

/// Continuation of `resource_validate_query_selectors`: fence, fingerprint,
/// fairness group and cursor.
pub(crate) fn resource_validate_query_correlations(
    request: &ResourceReservationStatusRequest,
) -> Result<(), String> {
    if let Some(value) = request.fence.as_deref() {
        resource_text(value, "resource query fence")?;
    }
    if let Some(value) = request.input_fingerprint.as_deref() {
        resource_fingerprint(value, "resource query input_fingerprint")?;
    }
    if let Some(value) = request.fairness_group.as_deref() {
        resource_text(value, "resource query fairness_group")?;
    }
    if let Some(value) = request.cursor.as_deref() {
        resource_text(value, "resource query cursor")?;
    }
    Ok(())
}

pub(crate) fn resource_validate_query_request(
    request: &ResourceReservationStatusRequest,
    require_limit: bool,
) -> Result<(), String> {
    resource_text(&request.tenant_ref, "resource query tenant_ref")?;
    resource_validate_query_selectors(request)?;
    resource_validate_query_correlations(request)?;
    if request.attempt.is_some_and(|attempt| attempt == 0) {
        return Err("resource query attempt must be positive".into());
    }
    if require_limit {
        if request.limit == 0 || request.limit > MAX_RESOURCE_STATUS_LIMIT as u64 {
            return Err("resource status request violates bounds".into());
        }
    } else if request.limit > MAX_RESOURCE_STATUS_LIMIT as u64 {
        return Err("resource query violates bounds".into());
    }
    Ok(())
}

pub(crate) fn resource_record_work_item_live(
    props: &serde_json::Map<String, serde_json::Value>,
    record: &ResourceReservationRecord,
    now_ms: u64,
) -> bool {
    let status = property_string(props, "status");
    let owner = if matches!(status, "leased" | "running") {
        property_string(props, "lease_owner")
    } else {
        property_string(props, "last_lease_owner")
    };
    let lease_until = (property_f64(props, "lease_expires_at") * 1000.0).max(0.0) as u64;
    property_string(props, "node_type") == "WorkItem"
        && property_string(props, "tenant") == record.tenant_ref
        && property_u64(props, "attempt") == record.attempt
        && property_u64(props, "lease_epoch") == record.lease_epoch
        && property_u64(props, "fencing_token") == record.fencing_token
        && owner == record.owner_id
        && matches!(status, "leased" | "running")
        && lease_until > now_ms
        && resource_expected_fence(record.fencing_token) == record.fence
}

#[cfg(test)]
pub(crate) fn resource_decode_result_payload(
    payload: crate::protocol::ResultPayload,
) -> Result<ResourceReservationResult, String> {
    let bytes = match payload {
        crate::protocol::ResultPayload::Raw(bytes) => bytes,
        _ => return Err("resource query result encoding failed".into()),
    };
    eg_types::msgpack::decode_bounded(
        &bytes,
        eg_types::msgpack::MsgpackLimits::new(64 * 1024, 10_000, 32),
    )
    .map_err(|_| "resource query result encoding failed".into())
}

/// Exact query reads the native reservation row and all caller correlations
/// from one MVCC snapshot.  A null reservation id is the intentionally narrow
/// current-WorkItem precheck used for scheduler ranking; it never returns a
/// reservation ledger and Reserve revalidates the same fence transactionally.
pub(crate) fn resolve_current_work_item_query_identity_fields(
    request: &ResourceReservationStatusRequest,
) -> Result<(&str, &str, &str), String> {
    let work_item_id = request
        .work_item_id
        .as_deref()
        .ok_or_else(|| "current WorkItem query requires work_item_id".to_string())?;
    let owner = request
        .owner_id
        .as_deref()
        .ok_or_else(|| "current WorkItem query requires owner_id".to_string())?;
    let fence = request
        .fence
        .as_deref()
        .ok_or_else(|| "current WorkItem query requires fence".to_string())?;
    Ok((work_item_id, owner, fence))
}

pub(crate) fn resolve_current_work_item_query_fence_fields(
    request: &ResourceReservationStatusRequest,
) -> Result<(u64, u64, u64), String> {
    let attempt = request
        .attempt
        .ok_or_else(|| "current WorkItem query requires attempt".to_string())?;
    let lease_epoch = request
        .lease_epoch
        .ok_or_else(|| "current WorkItem query requires lease_epoch".to_string())?;
    let fencing_token = request
        .fencing_token
        .ok_or_else(|| "current WorkItem query requires fencing_token".to_string())?;
    Ok((attempt, lease_epoch, fencing_token))
}

pub(crate) fn resolve_current_work_item_query_fields(
    request: &ResourceReservationStatusRequest,
) -> Result<(&str, &str, &str, u64, u64, u64), String> {
    let (work_item_id, owner, fence) = resolve_current_work_item_query_identity_fields(request)?;
    let (attempt, lease_epoch, fencing_token) =
        resolve_current_work_item_query_fence_fields(request)?;
    Ok((
        work_item_id,
        owner,
        fence,
        attempt,
        lease_epoch,
        fencing_token,
    ))
}

pub(crate) fn current_work_item_query_matches(
    props: &serde_json::Map<String, serde_json::Value>,
    request: &ResourceReservationStatusRequest,
    owner: &str,
    fence: &str,
    attempt: u64,
    lease_epoch: u64,
    fencing_token: u64,
) -> bool {
    let current_attempt = property_u64(props, "attempt");
    let current_epoch = property_u64(props, "lease_epoch");
    let current_token = property_u64(props, "fencing_token");
    let status = property_string(props, "status");
    let current_owner = if matches!(status, "leased" | "running") {
        property_string(props, "lease_owner")
    } else {
        property_string(props, "last_lease_owner")
    };
    let live_until = (property_f64(props, "lease_expires_at") * 1000.0).max(0.0) as u64;
    property_string(props, "node_type") == "WorkItem"
        && property_string(props, "tenant") == request.tenant_ref
        && current_attempt == attempt
        && current_epoch == lease_epoch
        && current_token == fencing_token
        && fence == resource_expected_fence(fencing_token)
        && current_owner == owner
        && matches!(status, "leased" | "running")
        && live_until > request.now_ms
}

pub(crate) fn read_resource_reservation_current_work_item_query(
    nodes: &eg_storage::ScopedOwnerTable<(&str, &str), &[u8]>,
    graph: &str,
    request: &ResourceReservationStatusRequest,
    crypto: DurableCrypto<'_>,
) -> Result<ResourceReservationResult, String> {
    let (work_item_id, owner, fence, attempt, lease_epoch, fencing_token) =
        resolve_current_work_item_query_fields(request)?;
    let bytes = nodes
        .get((graph, work_item_id))?
        .map(|value| crypto.unseal(value.value()))
        .transpose()?;
    let Some(bytes) = bytes else {
        return resource_no_reservation_query_result(
            request,
            ResourceReservationResultDecision::NotFound,
        );
    };
    let props: serde_json::Map<String, serde_json::Value> = decode_durable(&bytes)?;
    let current = current_work_item_query_matches(
        &props,
        request,
        owner,
        fence,
        attempt,
        lease_epoch,
        fencing_token,
    );
    let decision = if current {
        ResourceReservationResultDecision::Accepted
    } else {
        ResourceReservationResultDecision::Stale
    };
    resource_no_reservation_query_result(request, decision)
}

// RM's mirrorless retry query intentionally omits the fingerprint: the
// native record is the source of truth and the adapter compares it
// after decoding.  If a mirror supplies one, it remains an exact
// correlation and a mismatch fails closed.
pub(crate) fn resource_reservation_query_correlates(
    request: &ResourceReservationStatusRequest,
    record: &ResourceReservationRecord,
) -> bool {
    request.work_item_id.as_deref() == Some(record.work_item_id.as_str())
        && request
            .host_ref
            .as_deref()
            .is_none_or(|host_ref| host_ref == record.host_ref)
        && request.owner_id.as_deref() == Some(record.owner_id.as_str())
        && request.fence.as_deref() == Some(record.fence.as_str())
        && request.attempt == Some(record.attempt)
        && request.lease_epoch == Some(record.lease_epoch)
        && request.fencing_token == Some(record.fencing_token)
        && request
            .input_fingerprint
            .as_deref()
            .is_none_or(|fingerprint| fingerprint == record.input_fingerprint.as_str())
}

pub(crate) fn resource_reservation_query_host(
    read: &ScopedRead<'_, GraphShardOwner>,
    graph: &str,
    record: &ResourceReservationRecord,
    crypto: DurableCrypto<'_>,
) -> Result<Option<DurableResourceHost>, String> {
    let hosts = read.scoped_owner_table(RESOURCE_HOSTS)?;
    hosts
        .get((graph, record.host_ref.as_str()))?
        .map(|row| resource_decode::<DurableResourceHost>(row.value(), crypto))
        .transpose()
}

pub(crate) fn resource_reservation_query_current_work_item(
    nodes: &eg_storage::ScopedOwnerTable<(&str, &str), &[u8]>,
    graph: &str,
    record: &ResourceReservationRecord,
    crypto: DurableCrypto<'_>,
) -> Result<Option<serde_json::Map<String, serde_json::Value>>, String> {
    let current_item = nodes
        .get((graph, record.work_item_id.as_str()))?
        .map(|value| crypto.unseal(value.value()))
        .transpose()?;
    current_item
        .as_deref()
        .map(decode_durable::<serde_json::Map<String, serde_json::Value>>)
        .transpose()
}

pub(crate) fn build_resource_reservation_query_result(
    read: &ScopedRead<'_, GraphShardOwner>,
    nodes: &eg_storage::ScopedOwnerTable<(&str, &str), &[u8]>,
    graph: &str,
    request: &ResourceReservationStatusRequest,
    stored: &DurableResourceReservation,
    crypto: DurableCrypto<'_>,
) -> Result<ResourceReservationResult, String> {
    let record = &stored.record;
    let host = resource_reservation_query_host(read, graph, record, crypto)?;
    let request_for_payload = resource_request_from_record(record, request.now_ms);
    let current = resource_reservation_query_current_work_item(nodes, graph, record, crypto)?;
    let current_valid = current
        .as_ref()
        .is_some_and(|props| resource_record_work_item_live(props, record, request.now_ms));
    let tombstone_replay = record.tombstone;
    let decision = if current_valid || tombstone_replay {
        ResourceReservationResultDecision::Idempotent
    } else {
        ResourceReservationResultDecision::Stale
    };
    resource_result_payload(
        decision,
        &request_for_payload,
        (current_valid || tombstone_replay).then(|| record.clone()),
        host.as_ref(),
        stored.fairness_debt,
        Vec::new(),
    )
}

pub(crate) fn read_resource_reservation_by_id(
    read: &ScopedRead<'_, GraphShardOwner>,
    nodes: &eg_storage::ScopedOwnerTable<(&str, &str), &[u8]>,
    graph: &str,
    request: &ResourceReservationStatusRequest,
    crypto: DurableCrypto<'_>,
) -> Result<ResourceReservationResult, String> {
    let reservation_id = request.reservation_id.as_deref().unwrap_or_default();
    resource_text(reservation_id, "resource reservation_id")?;
    let reservations = read.scoped_owner_table(RESOURCE_RESERVATIONS)?;
    // A mirrorless RM admission query is an expected pre-reserve read.  A
    // missing native row is a typed absence, not a transport failure; the
    // scheduler then submits Reserve and lets that transaction revalidate
    // the WorkItem/fence atomically.
    let Some(row) = reservations.get((graph, reservation_id))? else {
        return resource_no_reservation_query_result(
            request,
            ResourceReservationResultDecision::NotFound,
        );
    };
    let stored: DurableResourceReservation = resource_decode(row.value(), crypto)?;
    if stored.record.tenant_ref != request.tenant_ref {
        // Preserve tenant isolation while keeping the public query vocabulary
        // typed and bounded.  Do not reveal whether another tenant owns this
        // reservation id through a transport error.
        return resource_no_reservation_query_result(
            request,
            ResourceReservationResultDecision::NotFound,
        );
    }
    if !resource_reservation_query_correlates(request, &stored.record) {
        return Err("resource reservation correlation does not match".into());
    }
    build_resource_reservation_query_result(read, nodes, graph, request, &stored, crypto)
}

pub(crate) fn read_resource_reservation(
    shard: &Shard,
    graph: &str,
    request: &ResourceReservationStatusRequest,
    crypto: DurableCrypto<'_>,
) -> Result<ResourceReservationResult, String> {
    resource_validate_query_request(request, false)?;
    let handle = shard.graph(graph)?;
    let read = shard.read(&handle)?;
    let nodes = read.scoped_owner_table(NODES)?;
    if request.reservation_id.is_none() {
        return read_resource_reservation_current_work_item_query(&nodes, graph, request, crypto);
    }
    read_resource_reservation_by_id(&read, &nodes, graph, request, crypto)
}
