use super::*;

/// The read-only resource tables a reservation status query scans.
pub(crate) struct ResourceReservationStatusTables {
    pub(crate) reservations:
        eg_storage::ScopedOwnerTable<(&'static str, &'static str), &'static [u8]>,
    pub(crate) tenant_index:
        eg_storage::ScopedOwnerTable<(&'static str, &'static str, &'static str), &'static str>,
    pub(crate) hosts: eg_storage::ScopedOwnerTable<(&'static str, &'static str), &'static [u8]>,
    pub(crate) disk_policies:
        eg_storage::ScopedOwnerTable<(&'static str, &'static str), &'static [u8]>,
}

pub(crate) fn open_resource_reservation_status_tables(
    read: &ScopedRead<'_, GraphShardOwner>,
) -> Result<ResourceReservationStatusTables, String> {
    let reservations = read.scoped_owner_table(RESOURCE_RESERVATIONS)?;
    let tenant_index = read.scoped_owner_table(RESOURCE_RESERVATION_TENANT_INDEX)?;
    let hosts = read.scoped_owner_table(RESOURCE_HOSTS)?;
    let disk_policies = read.scoped_owner_table(RESOURCE_DISK_POLICIES)?;
    Ok(ResourceReservationStatusTables {
        reservations,
        tenant_index,
        hosts,
        disk_policies,
    })
}

pub(crate) enum ResourceReservationStatusRowOutcome {
    StopScan,
    SkipCursor,
    Orphan,
    Processed {
        superseded: bool,
        summary: Option<ResourceReservationSummary>,
    },
}

pub(crate) fn resource_reservation_status_row_is_filtered_out(
    request: &ResourceReservationStatusRequest,
    record: &ResourceReservationRecord,
) -> bool {
    crate::redb_store::any_optional_text_filter_mismatch([
        (request.host_ref.as_deref(), record.host_ref.as_str()),
        (
            request.work_item_id.as_deref(),
            record.work_item_id.as_str(),
        ),
        (
            request.fairness_group.as_deref(),
            record.fairness_group.as_str(),
        ),
        (request.owner_id.as_deref(), record.owner_id.as_str()),
        (request.fence.as_deref(), record.fence.as_str()),
        (
            request.input_fingerprint.as_deref(),
            record.input_fingerprint.as_str(),
        ),
    ])
}

pub(crate) fn build_resource_reservation_summary(
    record: &ResourceReservationRecord,
    stored: &DurableResourceReservation,
) -> ResourceReservationSummary {
    let is_reserved = record.state == ResourceReservationRecordState::Reserved;
    ResourceReservationSummary {
        reservation_id: record.reservation_id.clone(),
        work_item_id: record.work_item_id.clone(),
        attempt: record.attempt,
        host_ref: record.host_ref.clone(),
        profile_name: record.profile_name.clone(),
        fairness_group: record.fairness_group.clone(),
        state: resource_summary_state(record.state),
        revision: record.revision,
        expires_at_ms: record.expires_at_ms,
        held_cpu_weight: if is_reserved {
            stored.held_cpu_weight
        } else {
            0
        },
        held_memory_mib: if is_reserved {
            stored.held_memory_mib
        } else {
            0
        },
        held_disk_mib: if is_reserved { stored.held_disk_mib } else { 0 },
        held_process_slots: if is_reserved {
            stored.held_process_slots
        } else {
            0
        },
        tombstone: record.tombstone,
    }
}

pub(crate) fn resolve_resource_reservation_status_row(
    reservation_id: &str,
    graph: &str,
    request: &ResourceReservationStatusRequest,
    reservations: &eg_storage::ScopedOwnerTable<(&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<ResourceReservationStatusRowOutcome, String> {
    let Some(row) = reservations.get((graph, reservation_id))? else {
        return Ok(ResourceReservationStatusRowOutcome::Orphan);
    };
    let stored: DurableResourceReservation = resource_decode(row.value(), crypto)?;
    let record = &stored.record;
    if record.tenant_ref != request.tenant_ref {
        return Ok(ResourceReservationStatusRowOutcome::Orphan);
    }
    let superseded = record.state == ResourceReservationRecordState::Superseded;
    if resource_reservation_status_row_is_filtered_out(request, record) {
        return Ok(ResourceReservationStatusRowOutcome::Processed {
            superseded,
            summary: None,
        });
    }
    let summary = build_resource_reservation_summary(record, &stored);
    Ok(ResourceReservationStatusRowOutcome::Processed {
        superseded,
        summary: Some(summary),
    })
}

pub(crate) struct ResourceReservationStatusRowContext<'row, 'crypto> {
    pub(crate) row_graph: &'row str,
    pub(crate) tenant: &'row str,
    pub(crate) reservation_id: &'row str,
    pub(crate) index_value: &'row str,
    pub(crate) graph: &'row str,
    pub(crate) cursor: &'row str,
    pub(crate) request: &'row ResourceReservationStatusRequest,
    pub(crate) reservations:
        &'row eg_storage::ScopedOwnerTable<(&'static str, &'static str), &'static [u8]>,
    pub(crate) crypto: DurableCrypto<'crypto>,
}

pub(crate) fn resource_reservation_status_row_outcome(
    context: ResourceReservationStatusRowContext<'_, '_>,
) -> Result<ResourceReservationStatusRowOutcome, String> {
    // The scan is `scope_rows()`, which starts at the FIRST row this graph
    // owns rather than at the `(graph, tenant, cursor)` position a raw range
    // could start from -- a key whose non-leading components are `&str` has no
    // inclusive upper bound, so no bounded range can express this prefix. The
    // rows before that position are therefore skipped here instead of never
    // being read; the decision for every row at or after it is unchanged, and
    // the scan still stops as soon as it leaves the requested tenant.
    if context.row_graph != context.graph || context.tenant > context.request.tenant_ref.as_str() {
        return Ok(ResourceReservationStatusRowOutcome::StopScan);
    }
    if context.tenant < context.request.tenant_ref.as_str()
        || context.reservation_id <= context.cursor
    {
        return Ok(ResourceReservationStatusRowOutcome::SkipCursor);
    }
    if context.index_value != context.reservation_id {
        return Ok(ResourceReservationStatusRowOutcome::Orphan);
    }
    resolve_resource_reservation_status_row(
        context.reservation_id,
        context.graph,
        context.request,
        context.reservations,
        context.crypto,
    )
}

pub(crate) struct ResourceReservationStatusScan {
    values: Vec<ResourceReservationSummary>,
    has_more: bool,
    last_returned_cursor: Option<String>,
    orphan_count: u64,
    superseded_count: u64,
}

pub(crate) fn apply_resource_reservation_status_row_outcome(
    outcome: ResourceReservationStatusRowOutcome,
    scan: &mut ResourceReservationStatusScan,
    limit: usize,
) -> std::ops::ControlFlow<()> {
    match outcome {
        ResourceReservationStatusRowOutcome::StopScan => std::ops::ControlFlow::Break(()),
        ResourceReservationStatusRowOutcome::SkipCursor => std::ops::ControlFlow::Continue(()),
        ResourceReservationStatusRowOutcome::Orphan => {
            scan.orphan_count = scan.orphan_count.saturating_add(1);
            std::ops::ControlFlow::Continue(())
        }
        ResourceReservationStatusRowOutcome::Processed {
            superseded,
            summary,
        } => {
            if superseded {
                scan.superseded_count = scan.superseded_count.saturating_add(1);
            }
            let Some(summary) = summary else {
                return std::ops::ControlFlow::Continue(());
            };
            let reservation_cursor = summary.reservation_id.clone();
            scan.values.push(summary);
            if scan.values.len() > limit {
                scan.values.pop();
                scan.has_more = true;
                return std::ops::ControlFlow::Break(());
            }
            scan.last_returned_cursor = Some(reservation_cursor);
            std::ops::ControlFlow::Continue(())
        }
    }
}

pub(crate) fn scan_resource_reservation_status_rows(
    tenant_index: &eg_storage::ScopedOwnerTable<(&str, &str, &str), &str>,
    reservations: &eg_storage::ScopedOwnerTable<(&str, &str), &[u8]>,
    graph: &str,
    cursor: &str,
    request: &ResourceReservationStatusRequest,
    crypto: DurableCrypto<'_>,
) -> Result<ResourceReservationStatusScan, String> {
    let mut scan = ResourceReservationStatusScan {
        values: Vec::new(),
        has_more: false,
        last_returned_cursor: None,
        orphan_count: 0,
        superseded_count: 0,
    };
    let mut scanned = 0usize;
    for row in tenant_index.scope_rows()? {
        scanned = scanned.saturating_add(1);
        if scanned > MAX_RESOURCE_STATUS_SCAN {
            return Err("resource status scan exceeds native bound".into());
        }
        let (key, value) = row?;
        let (row_graph, tenant, reservation_id) = key.value();
        let outcome =
            resource_reservation_status_row_outcome(ResourceReservationStatusRowContext {
                row_graph,
                tenant,
                reservation_id,
                index_value: value.value(),
                graph,
                cursor,
                request,
                reservations,
                crypto,
            })?;
        if apply_resource_reservation_status_row_outcome(outcome, &mut scan, request.limit as usize)
            .is_break()
        {
            break;
        }
    }
    Ok(scan)
}

pub(crate) fn resource_reservation_status_host(
    hosts: &eg_storage::ScopedOwnerTable<(&str, &str), &[u8]>,
    graph: &str,
    request: &ResourceReservationStatusRequest,
    crypto: DurableCrypto<'_>,
) -> Result<Option<DurableResourceHost>, String> {
    let Some(host_ref) = request.host_ref.as_deref() else {
        return Ok(None);
    };
    hosts
        .get((graph, host_ref))?
        .map(|row| resource_decode::<DurableResourceHost>(row.value(), crypto))
        .transpose()
}

pub(crate) fn decode_resource_disk_policy_row(
    policy_key: &str,
    prefix: &str,
    value_bytes: &[u8],
    crypto: DurableCrypto<'_>,
) -> Result<(String, DurableResourceDiskPolicy), String> {
    let policy_key = policy_key
        .strip_prefix(prefix)
        .ok_or_else(|| "resource disk-policy key escaped host scope".to_string())?;
    resource_text(policy_key, "resource disk_policy_key")?;
    Ok((
        policy_key.to_string(),
        resource_decode::<DurableResourceDiskPolicy>(value_bytes, crypto)?,
    ))
}

pub(crate) fn resource_reservation_status_host_policies(
    disk_policies: &eg_storage::ScopedOwnerTable<(&str, &str), &[u8]>,
    graph: &str,
    host: Option<&DurableResourceHost>,
    crypto: DurableCrypto<'_>,
) -> Result<Vec<(String, DurableResourceDiskPolicy)>, String> {
    let Some(host) = host else {
        return Ok(Vec::new());
    };
    let prefix = format!("{}\0", host.host_ref);
    let mut rows = Vec::new();
    // `scope_rows()` is the whole of this graph's disk-policy rows: a key whose
    // second component is a `&str` has no maximum, so the host prefix cannot be
    // expressed as a bounded range. The rows sorting BEFORE the prefix are
    // skipped rather than treated as the end of the scan; the first row at or
    // after it that does not carry the prefix still ends the scan, exactly as
    // the prefix `break` did.
    //
    // Starting at the prefix used to bound the whole scan for free; starting at
    // the scope does not, so the rows this skips are counted against the same
    // native status-scan budget the tenant-index scan already uses. Without it
    // one host's policy read would be linear in every OTHER host's policy rows,
    // unbounded.
    let mut scanned = 0usize;
    for row in disk_policies.scope_rows()? {
        scanned = scanned.saturating_add(1);
        if scanned > MAX_RESOURCE_STATUS_SCAN || rows.len() >= MAX_RESOURCE_HOST_DISK_POLICIES {
            return Err("resource disk-policy scan exceeds native bound".to_string());
        }
        let (key, value) = row?;
        let (row_graph, policy_key) = key.value();
        if row_graph != graph {
            break;
        }
        if !policy_key.starts_with(&prefix) {
            if policy_key < prefix.as_str() {
                continue;
            }
            break;
        }
        rows.push(decode_resource_disk_policy_row(
            policy_key,
            &prefix,
            value.value(),
            crypto,
        )?);
    }
    Ok(rows)
}

pub(crate) fn resource_reservation_status_fairness_debt(
    read: &ScopedRead<'_, GraphShardOwner>,
    graph: &str,
    request: &ResourceReservationStatusRequest,
    crypto: DurableCrypto<'_>,
) -> Result<u64, String> {
    let Some(group) = request.fairness_group.as_deref() else {
        return Ok(0);
    };
    let fairness = read.scoped_owner_table(RESOURCE_FAIRNESS)?;
    let row = fairness
        .get((
            graph,
            resource_fairness_scope_key(&request.tenant_ref, group).as_str(),
        ))?
        .map(|row| resource_decode::<DurableResourceFairness>(row.value(), crypto))
        .transpose()?;
    Ok(row.map_or(0, |row| row.debt))
}

pub(crate) fn build_resource_reservation_status_result(
    scan: ResourceReservationStatusScan,
    host: Option<DurableResourceHost>,
    host_snapshot: Option<ResourceReservationHostSnapshot>,
    fairness_debt: u64,
) -> ResourceReservationStatusResult {
    let next_cursor = scan.has_more.then_some(scan.last_returned_cursor).flatten();
    ResourceReservationStatusResult {
        schema_version: ResourceReservationStatusResultSchemaVersion::V1,
        complete: !scan.has_more,
        next_cursor,
        host_snapshot,
        host_ref: host.as_ref().map(|value| value.host_ref.clone()),
        host_revision: host.as_ref().map_or(0, |value| value.revision),
        held_cpu_weight: host.as_ref().map_or(0, |value| value.held_cpu_weight),
        held_memory_mib: host.as_ref().map_or(0, |value| value.held_memory_mib),
        held_disk_mib: host.as_ref().map_or(0, |value| value.held_disk_mib),
        held_process_slots: host.as_ref().map_or(0, |value| value.held_process_slots),
        fairness_debt,
        reservations: scan.values,
        orphan_count: scan.orphan_count,
        superseded_count: scan.superseded_count,
    }
}

pub(crate) fn read_resource_reservation_status(
    shard: &Shard,
    graph: &str,
    request: &ResourceReservationStatusRequest,
    crypto: DurableCrypto<'_>,
) -> Result<ResourceReservationStatusResult, String> {
    resource_validate_query_request(request, true)?;
    let cursor = request.cursor.as_deref().unwrap_or("");
    let handle = shard.graph(graph)?;
    let read = shard.read(&handle)?;
    let ResourceReservationStatusTables {
        reservations,
        tenant_index,
        hosts,
        disk_policies,
    } = open_resource_reservation_status_tables(&read)?;

    let scan = scan_resource_reservation_status_rows(
        &tenant_index,
        &reservations,
        graph,
        cursor,
        request,
        crypto,
    )?;

    let host = resource_reservation_status_host(&hosts, graph, request, crypto)?;
    let host_policies =
        resource_reservation_status_host_policies(&disk_policies, graph, host.as_ref(), crypto)?;
    let host_snapshot = host
        .as_ref()
        .map(|value| resource_reservation_host_snapshot(value, &host_policies))
        .transpose()?;
    let fairness_debt = resource_reservation_status_fairness_debt(&read, graph, request, crypto)?;

    Ok(build_resource_reservation_status_result(
        scan,
        host,
        host_snapshot,
        fairness_debt,
    ))
}
