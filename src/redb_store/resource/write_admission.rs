pub(crate) struct ResourceReservationTables<'borrow, 'table, 'crypto> {
    pub(crate) nodes: &'borrow mut eg_storage::ScopedOwnerTableMut<
        'table,
        (&'static str, &'static str),
        &'static [u8],
    >,
    pub(crate) reservations: &'borrow mut eg_storage::ScopedOwnerTableMut<
        'table,
        (&'static str, &'static str),
        &'static [u8],
    >,
    pub(crate) tenant_index: &'borrow mut eg_storage::ScopedOwnerTableMut<
        'table,
        (&'static str, &'static str, &'static str),
        &'static str,
    >,
    pub(crate) attempts: &'borrow mut eg_storage::ScopedOwnerTableMut<
        'table,
        (&'static str, &'static str, u64),
        &'static str,
    >,
    pub(crate) hosts: &'borrow mut eg_storage::ScopedOwnerTableMut<
        'table,
        (&'static str, &'static str),
        &'static [u8],
    >,
    pub(crate) exclusivity: &'borrow mut eg_storage::ScopedOwnerTableMut<
        'table,
        (&'static str, &'static str),
        &'static str,
    >,
    pub(crate) fairness: &'borrow mut eg_storage::ScopedOwnerTableMut<
        'table,
        (&'static str, &'static str),
        &'static [u8],
    >,
    pub(crate) concurrency:
        &'borrow mut eg_storage::ScopedOwnerTableMut<'table, (&'static str, &'static str), u64>,
    pub(crate) anti_affinity: &'borrow mut eg_storage::ScopedOwnerTableMut<
        'table,
        (&'static str, &'static str, &'static str),
        u64,
    >,
    pub(crate) disk_policies: &'borrow mut eg_storage::ScopedOwnerTableMut<
        'table,
        (&'static str, &'static str),
        &'static [u8],
    >,
    pub(crate) crypto: DurableCrypto<'crypto>,
}

pub(crate) struct ResourceReservationApplyRequest<'borrow, 'table, 'crypto> {
    pub(crate) graph: &'borrow str,
    pub(crate) method: &'borrow Method,
    pub(crate) tables: ResourceReservationTables<'borrow, 'table, 'crypto>,
}

pub(crate) fn apply_resource_reservation_rows(
    request: ResourceReservationApplyRequest<'_, '_, '_>,
) -> Result<Option<crate::protocol::ResultPayload>, String> {
    let ResourceReservationApplyRequest {
        graph,
        method,
        mut tables,
    } = request;
    apply_resource_reservation_operation(graph, method, &mut tables)
}

fn apply_resource_reservation_operation(
    graph: &str,
    method: &Method,
    tables: &mut ResourceReservationTables<'_, '_, '_>,
) -> Result<Option<crate::protocol::ResultPayload>, String> {
    // CX-EG-05 (CCN 186 -> decomposed): the two match arms below are each a
    // literal, behaviour-preserving relocation of the original arm body into
    // its own named function (see immediately after this function).
    match method {
        Method::UpdateResourceHost { request } => apply_update_resource_host_rows(
            request,
            graph,
            tables.hosts,
            tables.disk_policies,
            tables.crypto,
        )?
        .map(
            crate::protocol::ResultPayload::of::<
                eg_types::result_contract::coordination::UpdateResourceHost,
            >,
        )
        .transpose(),
        Method::ReserveWorkItemResources { request }
        | Method::ReleaseWorkItemResources { request }
        | Method::ReclaimWorkItemResources { request } => {
            apply_resource_reservation_lifecycle_rows(graph, method, request, tables)?
                .map(|result| resource_reservation_payload(method, result))
                .transpose()
        }
        _ => Ok(None),
    }
}

/// Encode a reservation lifecycle result as the declared result of the method that
/// produced it.
fn resource_reservation_payload(
    method: &Method,
    result: ResourceReservationResult,
) -> Result<crate::protocol::ResultPayload, String> {
    match method {
        Method::ReserveWorkItemResources { .. } => crate::protocol::ResultPayload::of::<
            eg_types::result_contract::coordination::ReserveWorkItemResources,
        >(result),
        Method::ReleaseWorkItemResources { .. } => crate::protocol::ResultPayload::of::<
            eg_types::result_contract::coordination::ReleaseWorkItemResources,
        >(result),
        Method::ReclaimWorkItemResources { .. } => crate::protocol::ResultPayload::of::<
            eg_types::result_contract::coordination::ReclaimWorkItemResources,
        >(result),
        _ => Err("resource reservation result for a non-reservation method".to_string()),
    }
}

pub(crate) fn validate_resource_host_update_identity(
    request: &ResourceHostUpdateRequest,
) -> Result<(), String> {
    resource_text(&request.tenant_ref, "resource host tenant_ref")?;
    resource_text(&request.host_ref, "resource host_ref")?;
    resource_labels(&request.labels, "resource host labels")?;
    resource_text(
        request.target_alias.as_deref().unwrap_or("local"),
        "resource host target_alias",
    )?;
    Ok(())
}

pub(crate) fn resolve_resource_host_update_target_kind(
    request: &ResourceHostUpdateRequest,
) -> Result<&'static str, String> {
    let target_kind = resource_host_target_kind(request.target_kind);
    if (target_kind == "local") != request.target_alias.is_none() {
        return Err("resource host target_alias does not match target_kind".into());
    }
    Ok(target_kind)
}

pub(crate) fn resource_host_heartbeat_bounds_violated(request: &ResourceHostUpdateRequest) -> bool {
    request.heartbeat_ttl_ms < 1_000
        || request.heartbeat_ttl_ms > 86_400_000
        || request.heartbeat_at_ms > request.now_ms
        || request.now_ms.saturating_sub(request.heartbeat_at_ms) > request.heartbeat_ttl_ms
}

pub(crate) fn resource_host_capacity_bounds_violated(request: &ResourceHostUpdateRequest) -> bool {
    request.capacity.cpu_weight == 0
        || request.capacity.memory_mib == 0
        || request.capacity.disk_mib == 0
        || request.capacity.process_slots == 0
        || request.capacity.cpu_weight > MAX_RESOURCE_DIMENSION
        || request.capacity.memory_mib > MAX_RESOURCE_DIMENSION
        || request.capacity.disk_mib > MAX_RESOURCE_DIMENSION
        || request.capacity.process_slots > MAX_RESOURCE_DIMENSION
}

pub(crate) fn resource_host_disk_bounds_violated(request: &ResourceHostUpdateRequest) -> bool {
    request.disk_used_mib > request.disk_capacity_mib
        || request.disk_capacity_mib == 0
        || request.disk_capacity_mib > MAX_RESOURCE_DIMENSION
        || request.disk_used_mib > MAX_RESOURCE_DIMENSION
}

pub(crate) fn resource_host_observed_bounds_violated(request: &ResourceHostUpdateRequest) -> bool {
    request.observed.cpu_weight > request.capacity.cpu_weight
        || request.observed.memory_mib > request.capacity.memory_mib
        || request.observed.disk_mib > request.capacity.disk_mib
        || request.observed.process_slots > request.capacity.process_slots
        || request.observed.cpu_weight > MAX_RESOURCE_DIMENSION
        || request.observed.memory_mib > MAX_RESOURCE_DIMENSION
        || request.observed.disk_mib > MAX_RESOURCE_DIMENSION
        || request.observed.process_slots > MAX_RESOURCE_DIMENSION
}

pub(crate) fn validate_resource_host_update_telemetry_bounds(
    request: &ResourceHostUpdateRequest,
) -> Result<(), String> {
    if resource_host_heartbeat_bounds_violated(request)
        || resource_host_capacity_bounds_violated(request)
        || resource_host_disk_bounds_violated(request)
        || resource_host_observed_bounds_violated(request)
        || request.revision == 0
    {
        return Err("resource host update violates telemetry bounds".into());
    }
    Ok(())
}

pub(crate) fn validate_resource_host_update_request(
    request: &ResourceHostUpdateRequest,
) -> Result<&'static str, String> {
    validate_resource_host_update_identity(request)?;
    let target_kind = resolve_resource_host_update_target_kind(request)?;
    validate_resource_host_update_telemetry_bounds(request)?;
    Ok(target_kind)
}

pub(crate) fn resource_host_update_exceeds_capacity(
    request: &ResourceHostUpdateRequest,
    host: &DurableResourceHost,
) -> bool {
    request
        .observed
        .cpu_weight
        .checked_add(host.held_cpu_weight)
        .is_none_or(|value| value > request.capacity.cpu_weight)
        || request
            .observed
            .memory_mib
            .checked_add(host.held_memory_mib)
            .is_none_or(|value| value > request.capacity.memory_mib)
        || request
            .observed
            .disk_mib
            .checked_add(host.held_disk_mib)
            .is_none_or(|value| value > request.capacity.disk_mib)
        || request
            .disk_used_mib
            .checked_add(host.held_disk_mib)
            .is_none_or(|value| value > request.disk_capacity_mib)
        || request
            .observed
            .process_slots
            .checked_add(host.held_process_slots)
            .is_none_or(|value| value > request.capacity.process_slots)
}

pub(crate) fn check_resource_host_update_conflicts(
    request: &ResourceHostUpdateRequest,
    host: &DurableResourceHost,
    target_kind: &str,
    policy_rows: &[(String, DurableResourceDiskPolicy)],
) -> Result<Option<ResourceHostUpdateResult>, String> {
    if host.target_kind != target_kind || host.target_alias != request.target_alias {
        return Ok(Some(resource_host_result(
            request,
            Some(host),
            policy_rows,
            false,
            ResourceHostUpdateResultReason::Conflict,
        )?));
    }
    if request.revision <= host.revision {
        return Ok(Some(resource_host_result(
            request,
            Some(host),
            policy_rows,
            false,
            ResourceHostUpdateResultReason::StaleHost,
        )?));
    }
    if resource_host_update_exceeds_capacity(request, host) {
        return Ok(Some(resource_host_result(
            request,
            Some(host),
            policy_rows,
            false,
            ResourceHostUpdateResultReason::Conflict,
        )?));
    }
    Ok(None)
}

pub(crate) fn build_resource_host_from_update(
    request: &ResourceHostUpdateRequest,
    current: Option<&DurableResourceHost>,
    target_kind: &str,
) -> DurableResourceHost {
    DurableResourceHost {
        // Physical host accounting is graph-scoped and shared across
        // tenants.  Preserve the first controller's provenance label;
        // authz on UpdateResourceHost, not this label, controls who may
        // publish telemetry.
        tenant_ref: current.map_or_else(
            || request.tenant_ref.clone(),
            |host| host.tenant_ref.clone(),
        ),
        host_ref: request.host_ref.clone(),
        revision: request.revision,
        capacity: request.capacity.clone(),
        observed: request.observed.clone(),
        heartbeat_at_ms: request.heartbeat_at_ms,
        heartbeat_ttl_ms: request.heartbeat_ttl_ms,
        now_ms: request.now_ms,
        draining: request.draining,
        quarantined: request.quarantined,
        labels: request.labels.clone(),
        target_kind: target_kind.to_string(),
        target_alias: request.target_alias.clone(),
        disk_used_mib: request.disk_used_mib,
        disk_capacity_mib: request.disk_capacity_mib,
        held_cpu_weight: current.map_or(0, |h| h.held_cpu_weight),
        held_memory_mib: current.map_or(0, |h| h.held_memory_mib),
        held_disk_mib: current.map_or(0, |h| h.held_disk_mib),
        held_process_slots: current.map_or(0, |h| h.held_process_slots),
    }
}

pub(crate) fn apply_update_resource_host_rows(
    request: &ResourceHostUpdateRequest,
    graph: &str,
    hosts: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    disk_policies: &mut eg_storage::ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<Option<ResourceHostUpdateResult>, String> {
    let target_kind = validate_resource_host_update_request(request)?;
    let current = resource_load_host(hosts, graph, &request.host_ref, crypto)?;
    let policy_rows =
        resource_collect_disk_policy_rows(disk_policies, graph, &request.host_ref, crypto)?;
    if let Some(host) = current.as_ref() {
        if let Some(payload) =
            check_resource_host_update_conflicts(request, host, target_kind, &policy_rows)?
        {
            return Ok(Some(payload));
        }
    }
    let host = build_resource_host_from_update(request, current.as_ref(), target_kind);
    resource_put_host(hosts, graph, &host, crypto)?;
    Ok(Some(resource_host_result(
        request,
        Some(&host),
        &policy_rows,
        true,
        ResourceHostUpdateResultReason::Accepted,
    )?))
}

/// Early-return signal used while decomposing `apply_resource_reservation_lifecycle_rows`
/// into phase functions: `Continue(v)` carries the phase's output forward to the next
/// phase; `Return(payload)` means the phase already produced the function's final result
/// and every caller must stop and return it unchanged.
pub(crate) enum ReservationLifecycleStep<T> {
    Continue(T),
    Return(Box<ResourceReservationResult>),
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) enum ResourceLifecycleMode {
    Reserve,
    Release,
    Reclaim,
}

impl ResourceLifecycleMode {
    pub(crate) fn from_method(method: &Method) -> Self {
        match method {
            Method::ReserveWorkItemResources { .. } => Self::Reserve,
            Method::ReclaimWorkItemResources { .. } => Self::Reclaim,
            _ => Self::Release,
        }
    }

    pub(crate) const fn is_reserve(self) -> bool {
        matches!(self, Self::Reserve)
    }

    pub(crate) const fn is_reclaim(self) -> bool {
        matches!(self, Self::Reclaim)
    }
}

pub(crate) fn apply_resource_reservation_lifecycle_rows(
    graph: &str,
    method: &Method,
    request: &ResourceReservationRequest,
    tables: &mut ResourceReservationTables<'_, '_, '_>,
) -> Result<Option<ResourceReservationResult>, String> {
    let (mode, existing, props, work_item_fence) =
        match resource_lifecycle_precheck_and_load_work_item(method, request, tables, graph)? {
            ReservationLifecycleStep::Return(payload) => return Ok(Some(*payload)),
            ReservationLifecycleStep::Continue(value) => value,
        };
    let extension = match resource_validate_work_item_status_and_extension(
        request,
        mode,
        &work_item_fence,
        &props,
    )? {
        ReservationLifecycleStep::Return(payload) => return Ok(Some(*payload)),
        ReservationLifecycleStep::Continue(value) => value,
    };
    if let Some(payload) = resource_commit_release_or_reclaim_or_reserve_gate(
        graph,
        request,
        existing.as_ref(),
        mode,
        &work_item_fence,
        &props,
        tables,
    )? {
        return Ok(Some(payload));
    }
    let host =
        match resource_admit_reserve_host_with_winner_check(tables, graph, request, extension)? {
            ReservationLifecycleStep::Return(payload) => return Ok(Some(*payload)),
            ReservationLifecycleStep::Continue(value) => value,
        };
    resource_commit_reserve_admission(graph, request, host, tables)
}

/// Thin sequencing wrapper: runs Phase 1 (`resource_lifecycle_precheck`) then Phase 2
/// (`resource_load_and_validate_work_item`) back to back, so the orchestrator has a
/// single call/match site for "validate the request and load both the existing
/// reservation (if any) and the WorkItem row." No behaviour is added; this is pure
/// call-site consolidation (see CX-EG-05's finding that a `?` after a call counts as
/// a branch under this repo's complexity gate the same as an `if`, so flattening N
/// sequential fallible calls into fewer named steps is what brings the caller's own
/// CCN down, not simplifying any individual step).
/// What Phases 1+2 hand the reservation orchestrator, in order: lifecycle mode,
/// the existing reservation (if any), the WorkItem's properties, and its lease
/// fence.
pub(crate) type ResourceLifecyclePrelude = (
    ResourceLifecycleMode,
    Option<DurableResourceReservation>,
    serde_json::Map<String, serde_json::Value>,
    ResourceWorkItemFence,
);

pub(crate) fn resource_lifecycle_precheck_and_load_work_item(
    method: &Method,
    request: &ResourceReservationRequest,
    tables: &mut ResourceReservationTables<'_, '_, '_>,
    graph: &str,
) -> Result<ReservationLifecycleStep<ResourceLifecyclePrelude>, String> {
    let (mode, existing) = match resource_lifecycle_precheck(method, request, tables, graph)? {
        ReservationLifecycleStep::Return(payload) => {
            return Ok(ReservationLifecycleStep::Return(payload));
        }
        ReservationLifecycleStep::Continue(value) => value,
    };
    let (props, work_item_fence) =
        match resource_load_and_validate_work_item(tables, graph, request, mode)? {
            ReservationLifecycleStep::Return(payload) => {
                return Ok(ReservationLifecycleStep::Return(payload));
            }
            ReservationLifecycleStep::Continue(value) => value,
        };
    Ok(ReservationLifecycleStep::Continue((
        mode,
        existing,
        props,
        work_item_fence,
    )))
}

/// Thin sequencing wrapper: runs Phase 4 (`resource_commit_release_or_reclaim`) and,
/// only when it declined to decide (no existing reservation row), applies the
/// reserve-mode fallback that immediately followed it in the original
/// function. Pure call-site consolidation, no behaviour change.
pub(crate) fn resource_commit_release_or_reclaim_or_reserve_gate(
    graph: &str,
    request: &ResourceReservationRequest,
    existing: Option<&DurableResourceReservation>,
    mode: ResourceLifecycleMode,
    work_item_fence: &ResourceWorkItemFence,
    props: &serde_json::Map<String, serde_json::Value>,
    tables: &mut ResourceReservationTables<'_, '_, '_>,
) -> Result<Option<ResourceReservationResult>, String> {
    if let Some(payload) = resource_commit_release_or_reclaim(
        graph,
        request,
        existing,
        mode,
        work_item_fence,
        props,
        tables,
    )? {
        return Ok(Some(payload));
    }
    if !mode.is_reserve() {
        return Ok(Some(resource_result_payload(
            ResourceReservationResultDecision::NotFound,
            request,
            None,
            None,
            0,
            vec![],
        )?));
    }
    Ok(None)
}

/// Thin sequencing wrapper: runs Phase 5 (`resource_check_attempt_winner_conflict`)
/// then, only if it did not already decide the request, Phase 6
/// (`resource_admit_reserve_host`). Pure call-site consolidation, no behaviour change.
pub(crate) fn resource_admit_reserve_host_with_winner_check(
    tables: &mut ResourceReservationTables<'_, '_, '_>,
    graph: &str,
    request: &ResourceReservationRequest,
    extension: &serde_json::Map<String, serde_json::Value>,
) -> Result<ReservationLifecycleStep<DurableResourceHost>, String> {
    if let Some(payload) = resource_check_attempt_winner_conflict(
        tables.attempts,
        tables.reservations,
        graph,
        request,
        tables.crypto,
    )? {
        return Ok(ReservationLifecycleStep::Return(payload.into()));
    }
    resource_admit_reserve_host(tables, graph, request, extension)
}

/// The two reserve-only window preconditions.
///
/// A creation has no prior lifecycle revision to satisfy: accept only the
/// explicit zero/absent form, since a positive caller precondition must not be
/// silently persisted or bypassed by a reserve replay.  The reservation window
/// must also contain `now`.
pub(crate) fn resource_reserve_window_precheck(
    request: &ResourceReservationRequest,
) -> Result<Option<ResourceReservationResult>, String> {
    if request
        .expected_lifecycle_revision
        .is_some_and(|revision| revision != 0)
    {
        return Ok(Some(resource_result_payload(
            ResourceReservationResultDecision::InputConflict,
            request,
            None,
            None,
            0,
            vec![],
        )?));
    }
    if request.now_ms < request.reserved_at_ms || request.now_ms >= request.expires_at_ms {
        return Ok(Some(resource_result_payload(
            ResourceReservationResultDecision::Policy,
            request,
            None,
            None,
            0,
            vec![],
        )?));
    }
    Ok(None)
}

/// The `expected_lifecycle_revision` precondition of a release/reclaim.
///
/// A live row is matched against its current revision; a terminal replay carries
/// the precondition captured by the successful lifecycle mutation, which keeps an
/// exact retry idempotent while refusing a changed precondition after the row is
/// tombstoned.  The refusal decision differs accordingly (`Stale` vs
/// `InputConflict`).
pub(crate) fn resource_lifecycle_revision_precheck(
    request: &ResourceReservationRequest,
    stored: &DurableResourceReservation,
) -> Result<Option<ResourceReservationResult>, String> {
    let reserved = stored.record.state == ResourceReservationRecordState::Reserved;
    let lifecycle_matches = if reserved {
        request.expected_lifecycle_revision == Some(stored.record.lifecycle_revision)
    } else {
        request.expected_lifecycle_revision == stored.record.expected_lifecycle_revision
    };
    if lifecycle_matches {
        return Ok(None);
    }
    Ok(Some(resource_result_payload(
        if reserved {
            ResourceReservationResultDecision::Stale
        } else {
            ResourceReservationResultDecision::InputConflict
        },
        request,
        Some(stored.record.clone()),
        None,
        stored.fairness_debt,
        vec![],
    )?))
}

/// The idempotency-precondition pass over an existing reservation row: tenant
/// match, full immutable record match, the release/reclaim lifecycle-revision
/// precondition, and the terminal-replay short-circuit.  `Ok(Some(..))` decides
/// the request; `Ok(None)` lets the lifecycle continue.
pub(crate) fn resource_existing_reservation_precheck(
    request: &ResourceReservationRequest,
    stored: &DurableResourceReservation,
    mode: ResourceLifecycleMode,
    graph: &str,
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
    if !mode.is_reserve() {
        if let Some(payload) = resource_lifecycle_revision_precheck(request, stored)? {
            return Ok(Some(payload));
        }
    }
    if stored.record.state != ResourceReservationRecordState::Reserved {
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

/// Phase 1: TTL/precondition/window validation, plus the FIRST idempotency-precondition
/// pass over any existing reservation row (tenant match, request match, the
/// release/reclaim `expected_lifecycle_revision` precondition, and the terminal-replay
/// short-circuit). Literal relocation of the original function's first ~110 lines;
/// no branch was added, removed, or reordered.
pub(crate) fn resource_lifecycle_precheck(
    method: &Method,
    request: &ResourceReservationRequest,
    tables: &mut ResourceReservationTables<'_, '_, '_>,
    graph: &str,
) -> Result<
    ReservationLifecycleStep<(ResourceLifecycleMode, Option<DurableResourceReservation>)>,
    String,
> {
    resource_validate_request(request)?;
    if request.expires_at_ms <= request.reserved_at_ms
        || request.expires_at_ms.saturating_sub(request.reserved_at_ms) > MAX_RESOURCE_TTL_MS
    {
        return Err("resource TTL violates the native bound".into());
    }
    let mode = ResourceLifecycleMode::from_method(method);
    if mode.is_reserve() {
        if let Some(payload) = resource_reserve_window_precheck(request)? {
            return Ok(ReservationLifecycleStep::Return(payload.into()));
        }
    }
    // A terminal row is the durable idempotency tombstone.  Replay of
    // the exact release/reclaim (or an accepted reserve) must remain
    // answerable after the WorkItem has rotated to a newer attempt or
    // even been removed from the graph; requiring the old live lease
    // first would turn a safe replay into a misleading stale refusal.
    // The full immutable record comparison prevents a caller from
    // using a tombstone's reservation id as a substitute for current
    // WorkItem/fence authorization.
    let existing = resource_load_reservation(
        tables.reservations,
        graph,
        &request.reservation_id,
        tables.crypto,
    )?;
    if let Some(stored) = existing.as_ref() {
        if let Some(payload) = resource_existing_reservation_precheck(
            request,
            stored,
            mode,
            graph,
            tables.hosts,
            tables.crypto,
        )? {
            return Ok(ReservationLifecycleStep::Return(payload.into()));
        }
    }
    Ok(ReservationLifecycleStep::Continue((mode, existing)))
}

/// Phase 2: load the WorkItem row and validate its fence (attempt/lease_epoch/
/// fencing_token vs. the request), exactly as the original function's next block.
/// The validated WorkItem row: its properties map plus the lease fence read from
/// the same MVCC snapshot.
pub(crate) type ResourceWorkItemAdmission = (
    serde_json::Map<String, serde_json::Value>,
    ResourceWorkItemFence,
);

pub(crate) fn resource_load_and_validate_work_item(
    tables: &mut ResourceReservationTables<'_, '_, '_>,
    graph: &str,
    request: &ResourceReservationRequest,
    mode: ResourceLifecycleMode,
) -> Result<ReservationLifecycleStep<ResourceWorkItemAdmission>, String> {
    let item_bytes = tables
        .nodes
        .get((graph, request.work_item_id.as_str()))?
        .map(|value| tables.crypto.unseal(value.value()))
        .transpose()?;
    let Some(item_bytes) = item_bytes else {
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
    let props: serde_json::Map<String, serde_json::Value> = decode_durable(&item_bytes)?;
    let work_item_fence = match resource_validate_work_item(&props, request, mode.is_reclaim()) {
        Ok(fence) => fence,
        Err(decision) => {
            return Ok(ReservationLifecycleStep::Return(
                resource_result_payload(decision, request, None, None, 0, vec![])?.into(),
            ));
        }
    };
    Ok(ReservationLifecycleStep::Continue((props, work_item_fence)))
}

/// Phase 3: validate the WorkItem's live status is consistent with the requested
/// lifecycle transition, then parse (and return) its resource admission extension,
/// checking the reserve-only input fingerprint. Literal relocation of the original
/// function's third block.
pub(crate) fn resource_validate_work_item_status_and_extension<'p>(
    request: &ResourceReservationRequest,
    mode: ResourceLifecycleMode,
    work_item_fence: &ResourceWorkItemFence,
    props: &'p serde_json::Map<String, serde_json::Value>,
) -> Result<ReservationLifecycleStep<&'p serde_json::Map<String, serde_json::Value>>, String> {
    if mode.is_reserve() {
        let status = property_string(props, "status");
        let lease_expires_at_ms =
            (property_f64(props, "lease_expires_at") * 1000.0).max(0.0) as u64;
        if !matches!(status, "leased" | "running") || lease_expires_at_ms <= request.now_ms {
            return Ok(ReservationLifecycleStep::Return(
                resource_result_payload(
                    ResourceReservationResultDecision::Stale,
                    request,
                    None,
                    None,
                    0,
                    vec![],
                )?
                .into(),
            ));
        }
    } else if (!mode.is_reclaim() || !work_item_fence.superseded)
        && !matches!(
            property_string(props, "status"),
            "leased" | "running" | "succeeded" | "failed" | "cancelled" | "dead_letter"
        )
    {
        // Release: any non-terminal, non-live status is stale.
        // Reclaim: same, but a reclaim of an already-superseded
        // reservation is legitimate (that is precisely what reclaim
        // is for), so it is exempted -- `!is_reclaim ||
        // !superseded` is `true` for release and `!superseded` for
        // reclaim, matching the two branches this replaces.
        return Ok(ReservationLifecycleStep::Return(
            resource_result_payload(
                ResourceReservationResultDecision::Stale,
                request,
                None,
                None,
                0,
                vec![],
            )?
            .into(),
        ));
    }
    let (_repository, extension) = resource_metadata_maps(props)
        .map_err(|_| "WorkItem resource admission extension is invalid".to_string())?;
    if mode.is_reserve() {
        let expected = resource_recomputed_fingerprint(props, request)?;
        if expected != request.input_fingerprint {
            return Ok(ReservationLifecycleStep::Return(
                resource_result_payload(
                    ResourceReservationResultDecision::InputConflict,
                    request,
                    None,
                    None,
                    0,
                    vec![],
                )?
                .into(),
            ));
        }
    }
    Ok(ReservationLifecycleStep::Continue(extension))
}
