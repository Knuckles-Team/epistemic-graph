pub(crate) fn resource_encode<T: serde::Serialize>(
    value: &T,
    crypto: DurableCrypto<'_>,
) -> Result<Vec<u8>, String> {
    let bytes = rmp_serde::to_vec_named(value).map_err(|e| e.to_string())?;
    Ok(crypto.seal(&bytes).into_owned())
}

pub(crate) fn resource_decode<T: serde::de::DeserializeOwned>(
    value: &[u8],
    crypto: DurableCrypto<'_>,
) -> Result<T, String> {
    let bytes = crypto.unseal(value)?;
    decode_durable(&bytes)
}

pub(crate) fn resource_put_host(
    hosts: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    graph: &str,
    host: &DurableResourceHost,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let bytes = resource_encode(host, crypto)?;
    hosts.insert((graph, host.host_ref.as_str()), bytes.as_slice())?;
    Ok(())
}

pub(crate) fn resource_put_reservation(
    reservations: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    graph: &str,
    reservation: &DurableResourceReservation,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let bytes = resource_encode(reservation, crypto)?;
    reservations.insert(
        (graph, reservation.record.reservation_id.as_str()),
        bytes.as_slice(),
    )?;
    Ok(())
}

pub(crate) fn resource_load_fairness(
    fairness: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    graph: &str,
    tenant: &str,
    group: &str,
    crypto: DurableCrypto<'_>,
) -> Result<DurableResourceFairness, String> {
    let key = resource_fairness_scope_key(tenant, group);
    fairness
        .get((graph, key.as_str()))?
        .map(|row| resource_decode(row.value(), crypto))
        .transpose()
        .map(|value| value.unwrap_or_default())
}

pub(crate) fn resource_put_fairness(
    fairness: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    graph: &str,
    tenant: &str,
    group: &str,
    value: &DurableResourceFairness,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let key = resource_fairness_scope_key(tenant, group);
    let bytes = resource_encode(value, crypto)?;
    fairness.insert((graph, key.as_str()), bytes.as_slice())?;
    Ok(())
}

pub(crate) fn resource_load_host(
    hosts: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    graph: &str,
    host_ref: &str,
    crypto: DurableCrypto<'_>,
) -> Result<Option<DurableResourceHost>, String> {
    hosts
        .get((graph, host_ref))?
        .map(|row| resource_decode(row.value(), crypto))
        .transpose()
}

pub(crate) fn resource_load_reservation(
    reservations: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    graph: &str,
    reservation_id: &str,
    crypto: DurableCrypto<'_>,
) -> Result<Option<DurableResourceReservation>, String> {
    reservations
        .get((graph, reservation_id))?
        .map(|row| resource_decode(row.value(), crypto))
        .transpose()
}

pub(crate) fn resource_adjust_concurrency(
    concurrency: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str), u64>,
    graph: &str,
    key: &str,
    delta: i64,
) -> Result<u64, String> {
    let current = concurrency
        .get((graph, key))?
        .map(|value| value.value())
        .unwrap_or(0);
    let next = if delta >= 0 {
        current
            .checked_add(delta as u64)
            .ok_or_else(|| "resource concurrency counter overflow".to_string())?
    } else {
        current
            .checked_sub(delta.unsigned_abs())
            .ok_or_else(|| "resource concurrency counter underflow".to_string())?
    };
    if next == 0 {
        concurrency.remove((graph, key))?;
    } else {
        concurrency.insert((graph, key), next)?;
    }
    Ok(next)
}

pub(crate) fn resource_adjust_anti_affinity(
    anti_affinity: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str, &str), u64>,
    graph: &str,
    host_ref: &str,
    tag: &str,
    delta: i64,
) -> Result<u64, String> {
    let current = anti_affinity
        .get((graph, host_ref, tag))?
        .map(|value| value.value())
        .unwrap_or(0);
    let next = if delta >= 0 {
        current
            .checked_add(delta as u64)
            .ok_or_else(|| "resource anti-affinity counter overflow".to_string())?
    } else {
        current
            .checked_sub(delta.unsigned_abs())
            .ok_or_else(|| "resource anti-affinity counter underflow".to_string())?
    };
    if next == 0 {
        anti_affinity.remove((graph, host_ref, tag))?;
    } else {
        anti_affinity.insert((graph, host_ref, tag), next)?;
    }
    Ok(next)
}

pub(crate) fn resource_exclusivity_keys(request: &ResourceReservationRequest) -> Vec<String> {
    let mut keys = Vec::with_capacity(2);
    if request.repository_exclusive {
        keys.push(resource_scope_key(&[
            "tenant",
            &request.tenant_ref,
            "repository",
            &request.repository_id,
        ]));
    }
    if request.branch_exclusive {
        keys.push(resource_scope_key(&[
            "tenant",
            &request.tenant_ref,
            "branch",
            &request.repository_id,
            &request.branch,
        ]));
    }
    keys
}

/// Composite native index keys use a reserved NUL separator. Every component
/// has already passed `resource_text`, which rejects NUL/control characters,
/// making tenant/scope boundaries unambiguous instead of relying on a caller's
/// arbitrary spelling.
pub(crate) fn resource_scope_key(parts: &[&str]) -> String {
    parts.join("\0")
}

/// Concurrency keys are explicit global scheduler scope.  The prefix prevents
/// an untrusted caller from colliding with future tenant-scoped namespaces while
/// preserving one exact counter across tenants for a shared host profile.
pub(crate) fn resource_concurrency_scope_key(key: &str) -> String {
    resource_scope_key(&["global", key])
}

pub(crate) fn resource_fairness_scope_key(tenant: &str, group: &str) -> String {
    resource_scope_key(&[tenant, group])
}

pub(crate) fn resource_record_target_kind_from_request(
    kind: ResourceReservationRequestTargetKind,
) -> ResourceReservationRecordTargetKind {
    match kind {
        ResourceReservationRequestTargetKind::Local => ResourceReservationRecordTargetKind::Local,
        ResourceReservationRequestTargetKind::InventoryAlias => {
            ResourceReservationRecordTargetKind::InventoryAlias
        }
    }
}

pub(crate) fn resource_build_record(
    request: &ResourceReservationRequest,
    host: &DurableResourceHost,
    revision: u64,
    lifecycle_revision: u64,
) -> Result<ResourceReservationRecord, String> {
    let mut labels = host.labels.clone();
    labels.sort();
    Ok(ResourceReservationRecord {
        reservation_id: request.reservation_id.clone(),
        tenant_ref: request.tenant_ref.clone(),
        owner_id: request.owner_id.clone(),
        work_item_id: request.work_item_id.clone(),
        fence: request.fence.clone(),
        attempt: request.attempt,
        lease_epoch: request.lease_epoch,
        fencing_token: request.fencing_token,
        input_fingerprint: request.input_fingerprint.clone(),
        host_ref: request.host_ref.clone(),
        profile_name: request.profile_name.clone(),
        profile_version: request.profile_version.clone(),
        requirement: request.requirement.clone(),
        capacity_snapshot: ResourceCapacitySnapshot {
            cpu_weight: host.capacity.cpu_weight,
            memory_mib: host.capacity.memory_mib,
            disk_mib: host.capacity.disk_mib,
            process_slots: host.capacity.process_slots,
            host_revision: host.revision,
        },
        selected_target: ResourceTargetSnapshot {
            kind: resource_snapshot_kind(&host.target_kind)?,
            alias: host.target_alias.clone(),
            capability_labels: labels,
        },
        target_kind: resource_record_target_kind_from_request(request.target_kind),
        target_alias: request.target_alias.clone(),
        repository_id: request.repository_id.clone(),
        branch: request.branch.clone(),
        concurrency_key: request.concurrency_key.clone(),
        concurrency_limit: request.concurrency_limit,
        repository_exclusive: request.repository_exclusive,
        branch_exclusive: request.branch_exclusive,
        required_labels: request.required_labels.clone(),
        anti_affinity: request.anti_affinity.clone(),
        fairness_group: request.fairness_group.clone(),
        fairness_cost: request.fairness_cost,
        disk_low_watermark_mib: request.disk_low_watermark_mib,
        disk_high_watermark_mib: request.disk_high_watermark_mib,
        disk_policy_key: request.disk_policy_key.clone(),
        reserved_at_ms: request.reserved_at_ms,
        expires_at_ms: request.expires_at_ms,
        expected_host_revision: request.expected_host_revision,
        // Lifecycle CAS is an operation input, not immutable admission
        // identity. Reserve creation accepts only absent/zero; release and
        // reclaim record the successful precondition on their tombstone below
        // so an exact lifecycle retry can be distinguished from a changed one.
        expected_lifecycle_revision: None,
        state: ResourceReservationRecordState::Reserved,
        revision,
        lifecycle_revision,
        tombstone: false,
    })
}

pub(crate) fn resource_validate_host_freshness(
    host: &DurableResourceHost,
    now_ms: u64,
) -> ResourceReservationResultDecision {
    if host.heartbeat_at_ms > now_ms {
        return ResourceReservationResultDecision::StaleHost;
    }
    if now_ms.saturating_sub(host.heartbeat_at_ms) > host.heartbeat_ttl_ms {
        return ResourceReservationResultDecision::StaleHost;
    }
    if host.draining {
        return ResourceReservationResultDecision::Drained;
    }
    if host.quarantined {
        return ResourceReservationResultDecision::Quarantined;
    }
    ResourceReservationResultDecision::Accepted
}
