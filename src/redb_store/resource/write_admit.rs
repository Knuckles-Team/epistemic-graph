/// Host-eligibility gates: the caller's expected host revision, freshness,
/// required labels, and the two target-identity checks.
///
/// The request target is the scheduler's selected placement, while
/// preferred/required targets in the WorkItem extension describe eligibility and
/// ordering.  Once selected, the host's immutable target identity must still
/// equal the asserted local/alias pair; otherwise a local record could carry an
/// inventory host snapshot (or vice versa) and RM could reconstruct a
/// contradictory target.
pub(crate) fn resource_admit_check_host_eligibility(
    request: &ResourceReservationRequest,
    host: &DurableResourceHost,
    extension: &serde_json::Map<String, serde_json::Value>,
) -> Result<Option<ResourceReservationResult>, String> {
    if let Some(expected) = request.expected_host_revision {
        if expected != host.revision {
            return Ok(Some(resource_admission_refusal(
                ResourceReservationResultDecision::StaleHost,
                request,
                host,
            )?));
        }
    }
    let host_state = resource_validate_host_freshness(host, request.now_ms);
    if host_state != ResourceReservationResultDecision::Accepted {
        return Ok(Some(resource_admission_refusal(host_state, request, host)?));
    }
    if !request
        .required_labels
        .iter()
        .all(|label| host.labels.iter().any(|value| value == label))
    {
        return Ok(Some(resource_admission_refusal(
            ResourceReservationResultDecision::Labels,
            request,
            host,
        )?));
    }
    if !resource_target_selection_matches(extension, host)? {
        return Ok(Some(resource_admission_refusal(
            ResourceReservationResultDecision::Policy,
            request,
            host,
        )?));
    }
    if !resource_selected_target_matches_request(request, host) {
        return Ok(Some(resource_admission_refusal(
            ResourceReservationResultDecision::Policy,
            request,
            host,
        )?));
    }
    Ok(None)
}

/// Index-backed admission gates, in the original order: anti-affinity tags,
/// the concurrency scope limit, exclusivity keys, and the capacity vector.
pub(crate) fn resource_admit_check_index_gates(
    graph: &str,
    request: &ResourceReservationRequest,
    host: &DurableResourceHost,
    anti_affinity: &eg_storage::ScopedOwnerTableMut<'_, (&str, &str, &str), u64>,
    concurrency: &eg_storage::ScopedOwnerTableMut<'_, (&str, &str), u64>,
    exclusivity: &eg_storage::ScopedOwnerTableMut<'_, (&str, &str), &str>,
) -> Result<Option<ResourceReservationResult>, String> {
    for tag in &request.anti_affinity {
        let count = anti_affinity
            .get((graph, request.host_ref.as_str(), tag.as_str()))?
            .map(|value| value.value())
            .unwrap_or(0);
        if count != 0 {
            return Ok(Some(resource_admission_refusal(
                ResourceReservationResultDecision::AntiAffinity,
                request,
                host,
            )?));
        }
    }
    let concurrency_key = resource_concurrency_scope_key(&request.concurrency_key);
    let concurrency_count = concurrency
        .get((graph, concurrency_key.as_str()))?
        .map(|value| value.value())
        .unwrap_or(0);
    if request
        .concurrency_limit
        .is_some_and(|limit| concurrency_count >= limit)
    {
        return Ok(Some(resource_admission_refusal(
            ResourceReservationResultDecision::Concurrency,
            request,
            host,
        )?));
    }
    for key in resource_exclusivity_keys(request) {
        if exclusivity.get((graph, key.as_str()))?.is_some() {
            return Ok(Some(resource_admission_refusal(
                ResourceReservationResultDecision::Exclusivity,
                request,
                host,
            )?));
        }
    }
    if !resource_capacity_sum(host, &request.requirement) {
        return Ok(Some(resource_admission_refusal(
            ResourceReservationResultDecision::Capacity,
            request,
            host,
        )?));
    }
    Ok(None)
}

/// The disk gates that can refuse before anything is written: the per-host
/// policy-count bound, watermark agreement with the existing policy row, and
/// free space on the host.
pub(crate) fn resource_admit_check_disk(
    request: &ResourceReservationRequest,
    host: &DurableResourceHost,
    existing_policy: Option<&DurableResourceDiskPolicy>,
    policy_row_count: usize,
) -> Result<Option<ResourceReservationResult>, String> {
    if existing_policy.is_none() && policy_row_count >= MAX_RESOURCE_HOST_DISK_POLICIES {
        return Ok(Some(resource_admission_refusal(
            ResourceReservationResultDecision::Policy,
            request,
            host,
        )?));
    }
    if let Some(policy) = existing_policy {
        if policy.low_watermark_mib != request.disk_low_watermark_mib
            || policy.high_watermark_mib != request.disk_high_watermark_mib
        {
            return Ok(Some(resource_admission_refusal(
                ResourceReservationResultDecision::Policy,
                request,
                host,
            )?));
        }
    }
    let available_disk = host
        .disk_capacity_mib
        .checked_sub(host.disk_used_mib)
        .and_then(|value| value.checked_sub(host.held_disk_mib))
        .unwrap_or(0);
    if request.requirement.disk_mib > available_disk {
        return Ok(Some(resource_admission_refusal(
            ResourceReservationResultDecision::Disk,
            request,
            host,
        )?));
    }
    Ok(None)
}

/// Persist one disk-policy row for `disk_key`.
pub(crate) fn resource_put_disk_policy(
    disk_policies: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    graph: &str,
    disk_key: &str,
    policy: &DurableResourceDiskPolicy,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let bytes = resource_encode(policy, crypto)?;
    disk_policies.insert((graph, disk_key), bytes.as_slice())?;
    Ok(())
}

/// The disk-policy hysteresis step: compute whether this reservation would cross
/// the watermark, persist the resulting policy row, and refuse when blocked.
pub(crate) fn resource_admit_apply_disk_policy(
    graph: &str,
    request: &ResourceReservationRequest,
    host: &DurableResourceHost,
    disk_key: &str,
    existing_policy: Option<&DurableResourceDiskPolicy>,
    disk_policies: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<Option<ResourceReservationResult>, String> {
    let predicted_used = host
        .disk_used_mib
        .checked_add(host.held_disk_mib)
        .and_then(|value| value.checked_add(request.requirement.disk_mib))
        .ok_or_else(|| "resource disk accounting overflow".to_string())?;
    let blocked = resource_disk_policy_blocked(
        existing_policy.is_some_and(|policy| policy.blocked),
        predicted_used,
        request.disk_low_watermark_mib,
        request.disk_high_watermark_mib,
    );
    let bumped_policy = |blocked: bool| DurableResourceDiskPolicy {
        blocked,
        low_watermark_mib: request.disk_low_watermark_mib,
        high_watermark_mib: request.disk_high_watermark_mib,
        revision: existing_policy.map_or(1, |value| value.revision.saturating_add(1)),
    };
    if blocked {
        resource_put_disk_policy(
            disk_policies,
            graph,
            disk_key,
            &bumped_policy(blocked),
            crypto,
        )?;
        return Ok(Some(resource_admission_refusal(
            ResourceReservationResultDecision::Disk,
            request,
            host,
        )?));
    }
    if existing_policy.is_some_and(|policy| policy.blocked != blocked) {
        resource_put_disk_policy(
            disk_policies,
            graph,
            disk_key,
            &bumped_policy(blocked),
            crypto,
        )?;
    }
    if existing_policy.is_none() {
        let policy = DurableResourceDiskPolicy {
            blocked: false,
            low_watermark_mib: request.disk_low_watermark_mib,
            high_watermark_mib: request.disk_high_watermark_mib,
            revision: 1,
        };
        resource_put_disk_policy(disk_policies, graph, disk_key, &policy, crypto)?;
    }
    Ok(None)
}

/// Phase 6 (reserve-only path): every host-admission gate (freshness, labels, target
/// selection, anti-affinity, concurrency, exclusivity, capacity, disk-policy bound and
/// hysteresis), including the disk-policy table normalization writes that were part of
/// the same guard chain in the original function. Literal relocation; the ONLY
/// difference from the original is that the final "insert a fresh default policy when
/// none existed" step (previously the last few lines before the fairness/commit phase)
/// is included here rather than split across the phase boundary, because nothing after
/// it in the original function ever read `existing_policy`, `policy_rows`, or
/// `disk_key` again.
/// Every reserve-admission refusal reports the same shape: the decision, the
/// request, and the host snapshot it was evaluated against.
pub(crate) fn resource_admit_reserve_host(
    tables: &mut ResourceReservationTables<'_, '_, '_>,
    graph: &str,
    request: &ResourceReservationRequest,
    extension: &serde_json::Map<String, serde_json::Value>,
) -> Result<ReservationLifecycleStep<DurableResourceHost>, String> {
    let host = resource_load_host(tables.hosts, graph, &request.host_ref, tables.crypto)?;
    let Some(host) = host else {
        return Ok(ReservationLifecycleStep::Return(
            resource_result_payload(
                ResourceReservationResultDecision::NotFound,
                request,
                None,
                None,
                0,
                vec![],
            )?
            .into(),
        ));
    };
    // Admission and host snapshots share the schema's 128-policy
    // bound.  Enumerating this exact host prefix is part of the same
    // transaction, so a new policy key cannot race a concurrent
    // reservation into an undecodable/unbounded host projection.
    let policy_rows = resource_collect_disk_policy_rows(
        tables.disk_policies,
        graph,
        &request.host_ref,
        tables.crypto,
    )?;
    if let Some(payload) = resource_admit_check_host_eligibility(request, &host, extension)? {
        return Ok(ReservationLifecycleStep::Return(payload.into()));
    }
    if let Some(payload) = resource_admit_check_index_gates(
        graph,
        request,
        &host,
        tables.anti_affinity,
        tables.concurrency,
        tables.exclusivity,
    )? {
        return Ok(ReservationLifecycleStep::Return(payload.into()));
    }
    let disk_key = format!("{}\0{}", request.host_ref, request.disk_policy_key);
    let existing_policy = tables
        .disk_policies
        .get((graph, disk_key.as_str()))?
        .map(|value| resource_decode::<DurableResourceDiskPolicy>(value.value(), tables.crypto))
        .transpose()?;
    if let Some(payload) =
        resource_admit_check_disk(request, &host, existing_policy.as_ref(), policy_rows.len())?
    {
        return Ok(ReservationLifecycleStep::Return(payload.into()));
    }
    if let Some(payload) = resource_admit_apply_disk_policy(
        graph,
        request,
        &host,
        &disk_key,
        existing_policy.as_ref(),
        tables.disk_policies,
        tables.crypto,
    )? {
        return Ok(ReservationLifecycleStep::Return(payload.into()));
    }
    Ok(ReservationLifecycleStep::Continue(host))
}

/// Phase 7 (reserve-only path): fairness-debt update, host held-capacity increments,
/// and the final durable persist (reservation, tenant index, attempt index,
/// exclusivity, concurrency, anti-affinity) that produces the Accepted result.
/// Literal relocation of the original function's final block.
pub(crate) fn resource_commit_reserve_admission(
    graph: &str,
    request: &ResourceReservationRequest,
    mut host: DurableResourceHost,
    tables: &mut ResourceReservationTables<'_, '_, '_>,
) -> Result<Option<ResourceReservationResult>, String> {
    let mut debt = resource_load_fairness(
        tables.fairness,
        graph,
        &request.tenant_ref,
        &request.fairness_group,
        tables.crypto,
    )?
    .debt;
    debt = debt
        .checked_add(request.fairness_cost)
        .ok_or_else(|| "resource fairness debt overflow".to_string())?;
    let fairness_row = DurableResourceFairness { debt };
    resource_put_fairness(
        tables.fairness,
        graph,
        &request.tenant_ref,
        &request.fairness_group,
        &fairness_row,
        tables.crypto,
    )?;
    host.held_cpu_weight = host
        .held_cpu_weight
        .checked_add(request.requirement.cpu_weight)
        .ok_or_else(|| "resource host cpu accounting overflow".to_string())?;
    host.held_memory_mib = host
        .held_memory_mib
        .checked_add(request.requirement.memory_mib)
        .ok_or_else(|| "resource host memory accounting overflow".to_string())?;
    host.held_disk_mib = host
        .held_disk_mib
        .checked_add(request.requirement.disk_mib)
        .ok_or_else(|| "resource host disk accounting overflow".to_string())?;
    host.held_process_slots = host
        .held_process_slots
        .checked_add(request.requirement.process_slots)
        .ok_or_else(|| "resource host process accounting overflow".to_string())?;
    let record = resource_build_record(request, &host, 1, 1)?;
    let stored = DurableResourceReservation {
        record: record.clone(),
        held_cpu_weight: request.requirement.cpu_weight,
        held_memory_mib: request.requirement.memory_mib,
        held_disk_mib: request.requirement.disk_mib,
        held_process_slots: request.requirement.process_slots,
        fairness_debt: debt,
    };
    resource_put_host(tables.hosts, graph, &host, tables.crypto)?;
    resource_put_reservation(tables.reservations, graph, &stored, tables.crypto)?;
    tables.tenant_index.insert(
        (
            graph,
            request.tenant_ref.as_str(),
            request.reservation_id.as_str(),
        ),
        request.reservation_id.as_str(),
    )?;
    tables.attempts.insert(
        (graph, request.work_item_id.as_str(), request.attempt),
        request.reservation_id.as_str(),
    )?;
    for key in resource_exclusivity_keys(request) {
        tables
            .exclusivity
            .insert((graph, key.as_str()), request.reservation_id.as_str())?;
    }
    let concurrency_key = resource_concurrency_scope_key(&request.concurrency_key);
    resource_adjust_concurrency(tables.concurrency, graph, &concurrency_key, 1)?;
    for tag in &request.anti_affinity {
        resource_adjust_anti_affinity(tables.anti_affinity, graph, &request.host_ref, tag, 1)?;
    }
    Ok(Some(resource_result_payload(
        ResourceReservationResultDecision::Accepted,
        request,
        Some(record),
        Some(&host),
        debt,
        vec![request.work_item_id.clone()],
    )?))
}
