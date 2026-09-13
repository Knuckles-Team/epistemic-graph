/// Tenant / record / terminal-state prechecks of a release-or-reclaim commit.
/// A reserve that finds a live row is itself idempotent.  `Ok(Some(..))` decides
/// the request.
pub(crate) fn resource_release_row_precheck(
    graph: &str,
    request: &ResourceReservationRequest,
    stored: &DurableResourceReservation,
    is_reserve: bool,
    hosts: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<Option<ResourceReservationResult>, String> {
    if stored.record.tenant_ref != request.tenant_ref {
        return Ok(Some(resource_result_payload(
            ResourceReservationResultDecision::Conflict,
            request,
            None,
            None,
            0,
            vec![],
        )?));
    }
    if !resource_request_matches_record(request, &stored.record) {
        return Ok(Some(resource_result_payload(
            ResourceReservationResultDecision::InputConflict,
            request,
            None,
            None,
            stored.fairness_debt,
            vec![],
        )?));
    }
    if stored.record.state != ResourceReservationRecordState::Reserved || is_reserve {
        let host = resource_load_host(hosts, graph, &stored.record.host_ref, crypto)?;
        return Ok(Some(resource_result_payload(
            ResourceReservationResultDecision::Idempotent,
            request,
            Some(stored.record.clone()),
            host.as_ref(),
            stored.fairness_debt,
            vec![],
        )?));
    }
    Ok(None)
}

/// Reclaim-only policy gates: the reservation must have expired, and the linked
/// WorkItem must not still hold a live lease.
pub(crate) fn resource_reclaim_policy_precheck(
    request: &ResourceReservationRequest,
    stored: &DurableResourceReservation,
    props: &serde_json::Map<String, serde_json::Value>,
) -> Result<Option<ResourceReservationResult>, String> {
    let refuse = || {
        resource_result_payload(
            ResourceReservationResultDecision::Policy,
            request,
            Some(stored.record.clone()),
            None,
            stored.fairness_debt,
            vec![],
        )
        .map(Some)
    };
    if request.now_ms < stored.record.expires_at_ms {
        return refuse();
    }
    let status = property_string(props, "status");
    let lease_expires_at_ms = (property_f64(props, "lease_expires_at") * 1000.0).max(0.0) as u64;
    if matches!(status, "leased" | "running") && lease_expires_at_ms > request.now_ms {
        return refuse();
    }
    Ok(None)
}

/// Give the reservation's held capacity back to its host.  Any underflow is a
/// corrupt-accounting error, never a silent saturation.
pub(crate) fn resource_release_host_capacity(
    host: &mut DurableResourceHost,
    stored: &DurableResourceReservation,
) -> Result<(), String> {
    host.held_cpu_weight = host
        .held_cpu_weight
        .checked_sub(stored.held_cpu_weight)
        .ok_or_else(|| "resource host cpu accounting underflow".to_string())?;
    host.held_memory_mib = host
        .held_memory_mib
        .checked_sub(stored.held_memory_mib)
        .ok_or_else(|| "resource host memory accounting underflow".to_string())?;
    host.held_disk_mib = host
        .held_disk_mib
        .checked_sub(stored.held_disk_mib)
        .ok_or_else(|| "resource host disk accounting underflow".to_string())?;
    host.held_process_slots = host
        .held_process_slots
        .checked_sub(stored.held_process_slots)
        .ok_or_else(|| "resource host process accounting underflow".to_string())?;
    Ok(())
}

/// The tombstoned successor of a released/reclaimed reservation.
///
/// Fairness debt is historical service debt, not held capacity; releasing a
/// reservation must not erase the cost already charged to this tenant/group, so
/// the current `debt` is carried onto the tombstone.
pub(crate) fn resource_build_released_record(
    stored: &DurableResourceReservation,
    request: &ResourceReservationRequest,
    is_reclaim: bool,
    work_item_fence: &ResourceWorkItemFence,
    debt: u64,
) -> DurableResourceReservation {
    let mut next = stored.clone();
    next.record.state = if is_reclaim {
        if work_item_fence.superseded {
            ResourceReservationRecordState::Superseded
        } else {
            ResourceReservationRecordState::Reclaimed
        }
    } else {
        ResourceReservationRecordState::Released
    };
    next.record.revision = next.record.revision.saturating_add(1);
    next.record.lifecycle_revision = next.record.lifecycle_revision.saturating_add(1);
    next.record.expected_lifecycle_revision = request.expected_lifecycle_revision;
    next.record.tombstone = true;
    next.held_cpu_weight = 0;
    next.held_memory_mib = 0;
    next.held_disk_mib = 0;
    next.held_process_slots = 0;
    next.fairness_debt = debt;
    next
}

/// Drop the exclusivity keys this reservation owned and clear its disk-policy
/// block once the freed capacity is back under the low watermark.
pub(crate) fn resource_release_exclusivity_and_disk(
    graph: &str,
    request: &ResourceReservationRequest,
    host: &DurableResourceHost,
    exclusivity: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str), &str>,
    disk_policies: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    for key in resource_exclusivity_keys(request) {
        let owner = exclusivity
            .get((graph, key.as_str()))?
            .map(|value| value.value().to_string());
        if owner.as_deref() == Some(request.reservation_id.as_str()) {
            exclusivity.remove((graph, key.as_str()))?;
        }
    }
    let disk_key = format!("{}\0{}", request.host_ref, request.disk_policy_key);
    let existing_policy = disk_policies
        .get((graph, disk_key.as_str()))?
        .map(|value| value.value().to_vec());
    let Some(policy_bytes) = existing_policy else {
        return Ok(());
    };
    let mut policy: DurableResourceDiskPolicy = resource_decode(&policy_bytes, crypto)?;
    if policy.low_watermark_mib != request.disk_low_watermark_mib
        || policy.high_watermark_mib != request.disk_high_watermark_mib
    {
        return Ok(());
    }
    let used = host.disk_used_mib.saturating_add(host.held_disk_mib);
    if policy
        .low_watermark_mib
        .is_some_and(|watermark| used <= watermark)
    {
        policy.blocked = false;
        policy.revision = policy.revision.saturating_add(1);
        let bytes = resource_encode(&policy, crypto)?;
        disk_policies.insert((graph, disk_key.as_str()), bytes.as_slice())?;
    }
    Ok(())
}

/// Phase 4: the SECOND existing-reservation branch -- when a reservation row is
/// already on file, this is guaranteed to fully decide the request (idempotent
/// replay, reserve short-circuit, reclaim-not-yet-expired refusal, or the actual
/// release/reclaim commit that decrements the host and tombstones the record).
/// Returns `Ok(None)` only when `existing` is `None`, meaning: no decision made,
/// continue into the reserve-admission path. Literal relocation of the original
/// function's fourth block (`if let Some(stored) = existing.as_ref() { .. }`).
pub(crate) fn resource_commit_release_or_reclaim(
    graph: &str,
    request: &ResourceReservationRequest,
    existing: Option<&DurableResourceReservation>,
    mode: ResourceLifecycleMode,
    work_item_fence: &ResourceWorkItemFence,
    props: &serde_json::Map<String, serde_json::Value>,
    tables: &mut ResourceReservationTables<'_, '_, '_>,
) -> Result<Option<ResourceReservationResult>, String> {
    let Some(stored) = existing else {
        return Ok(None);
    };
    if let Some(payload) = resource_release_row_precheck(
        graph,
        request,
        stored,
        mode.is_reserve(),
        tables.hosts,
        tables.crypto,
    )? {
        return Ok(Some(payload));
    }
    if mode.is_reclaim() {
        if let Some(payload) = resource_reclaim_policy_precheck(request, stored, props)? {
            return Ok(Some(payload));
        }
    }
    let Some(mut host) =
        resource_load_host(tables.hosts, graph, &stored.record.host_ref, tables.crypto)?
    else {
        return Ok(Some(resource_result_payload(
            ResourceReservationResultDecision::Policy,
            request,
            Some(stored.record.clone()),
            None,
            stored.fairness_debt,
            vec![],
        )?));
    };
    resource_release_host_capacity(&mut host, stored)?;
    resource_put_host(tables.hosts, graph, &host, tables.crypto)?;
    let debt_row = resource_load_fairness(
        tables.fairness,
        graph,
        &request.tenant_ref,
        &request.fairness_group,
        tables.crypto,
    )?;
    let debt = debt_row.debt;
    let next =
        resource_build_released_record(stored, request, mode.is_reclaim(), work_item_fence, debt);
    resource_put_reservation(tables.reservations, graph, &next, tables.crypto)?;
    let concurrency_key = resource_concurrency_scope_key(&request.concurrency_key);
    resource_adjust_concurrency(tables.concurrency, graph, &concurrency_key, -1)?;
    for tag in &request.anti_affinity {
        resource_adjust_anti_affinity(tables.anti_affinity, graph, &request.host_ref, tag, -1)?;
    }
    resource_release_exclusivity_and_disk(
        graph,
        request,
        &host,
        tables.exclusivity,
        tables.disk_policies,
        tables.crypto,
    )?;
    Ok(Some(resource_result_payload(
        ResourceReservationResultDecision::Accepted,
        request,
        Some(next.record),
        Some(&host),
        debt,
        vec![request.work_item_id.clone()],
    )?))
}

/// Phase 5 (reserve-only path): the attempt-index winner check. Literal relocation.
pub(crate) fn resource_check_attempt_winner_conflict(
    attempts: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str, u64), &str>,
    reservations: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    graph: &str,
    request: &ResourceReservationRequest,
    crypto: DurableCrypto<'_>,
) -> Result<Option<ResourceReservationResult>, String> {
    let winner = attempts
        .get((graph, request.work_item_id.as_str(), request.attempt))?
        .map(|value| value.value().to_string());
    if let Some(winner) = winner {
        // The attempt index is a derived invariant, not an alternate
        // source of reservation truth.  Recharging an existing host
        // when the index points at a missing authoritative row would
        // turn partial/corrupt state into a second accepted hold.
        if resource_load_reservation(reservations, graph, &winner, crypto)?.is_none() {
            return Err("resource reservation attempt index references missing reservation".into());
        }
        if winner != request.reservation_id {
            return Ok(Some(resource_result_payload(
                ResourceReservationResultDecision::Conflict,
                request,
                None,
                None,
                0,
                vec![],
            )?));
        }
    }
    Ok(None)
}

pub(crate) fn resource_admission_refusal(
    decision: ResourceReservationResultDecision,
    request: &ResourceReservationRequest,
    host: &DurableResourceHost,
) -> Result<ResourceReservationResult, String> {
    resource_result_payload(decision, request, None, Some(host), 0, vec![])
}
