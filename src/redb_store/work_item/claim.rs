//! WorkItem claim.rs transitions.

use super::*;

/// One selectable row of a claim scan: `(prio_bucket, deadline, created_at_ms,
/// node_id, props)`.  The first four components are the sort key.
pub(crate) type ClaimCandidateRow = (
    u64,
    u64,
    u64,
    String,
    serde_json::Map<String, serde_json::Value>,
);

/// What the claim scan decided about one scanned node.
pub(crate) enum ClaimRowOutcome {
    /// Not a claimable row for this request; the scan moves on.
    Skip,
    /// A live lease held by someone else — counts against the tenant quota.
    InFlight,
    /// An expired lease past its attempt ceiling, retired to `dead_letter`.
    Exhausted(serde_json::Map<String, serde_json::Value>),
    /// A selectable candidate.
    Candidate(ClaimCandidateRow),
}

/// Everything one pass over the graph's nodes produced for a claim.
pub(crate) struct ClaimWorkItemScan {
    inflight: u32,
    candidates: Vec<ClaimCandidateRow>,
    // The redb range cursor immutably borrows the table, so expired
    // exhausted rows are collected here and written only after the
    // scan. They still commit in this same MutationBatch transaction.
    exhausted: Vec<(String, serde_json::Map<String, serde_json::Value>)>,
    changed_work_item_ids: Vec<String>,
}

/// Retire an expired lease that has reached its attempt ceiling.
fn retire_exhausted_lease(
    props: &mut serde_json::Map<String, serde_json::Value>,
    node_id: &str,
    status: &str,
    now_s: f64,
    next_epoch: u64,
) {
    #[cfg(not(feature = "statechart"))]
    let _ = (node_id, status);
    props.insert(
        "status".into(),
        serde_json::Value::String("dead_letter".into()),
    );
    props.insert("lease_epoch".into(), serde_json::Value::from(next_epoch));
    props.insert("fencing_token".into(), serde_json::Value::from(next_epoch));
    props.insert("lease_owner".into(), serde_json::Value::Null);
    props.insert("lease_expires_at".into(), serde_json::Value::Null);
    props.insert("completed_at".into(), serde_json::Value::from(now_s));
    props.insert("updated_at".into(), serde_json::Value::from(now_s));
    props.insert(
        "error_ref".into(),
        serde_json::Value::String("lease_exhausted".into()),
    );
    #[cfg(feature = "statechart")]
    apply_work_item_mirror(
        props,
        node_id,
        status,
        crate::work_item_statechart::EV_LEASE_EXHAUSTED,
        serde_json::json!({}),
        Some("dead_letter"),
    );
}

/// Return an expired lease to the ready queue with a fresh fence.
fn reclaim_expired_lease(
    props: &mut serde_json::Map<String, serde_json::Value>,
    node_id: &str,
    status: &str,
    next_epoch: u64,
) {
    #[cfg(not(feature = "statechart"))]
    let _ = (node_id, status);
    props.insert("status".into(), serde_json::Value::String("ready".into()));
    props.insert("lease_epoch".into(), serde_json::Value::from(next_epoch));
    props.insert("fencing_token".into(), serde_json::Value::from(next_epoch));
    props.insert("lease_owner".into(), serde_json::Value::Null);
    props.insert("lease_expires_at".into(), serde_json::Value::Null);
    #[cfg(feature = "statechart")]
    apply_work_item_mirror(
        props,
        node_id,
        status,
        crate::work_item_statechart::EV_LEASE_RECLAIM,
        serde_json::json!({}),
        Some("ready"),
    );
}

/// Fence out an expired lease owner before the item participates in selection.
/// Returns `true` when the attempt ceiling was reached and the item was retired
/// to `dead_letter`; `false` when it was reclaimed back to `ready`.  Either way
/// the update is still private to the held transaction.
pub(crate) fn claim_reclaim_expired_lease(
    props: &mut serde_json::Map<String, serde_json::Value>,
    node_id: &str,
    status: &str,
    now_s: f64,
) -> bool {
    let attempts = property_u64(props, "attempt");
    let max_attempts = property_u64(props, "max_attempts").max(1);
    let next_epoch = property_u64(props, "lease_epoch").saturating_add(1);
    if attempts >= max_attempts {
        retire_exhausted_lease(props, node_id, status, now_s, next_epoch);
        true
    } else {
        reclaim_expired_lease(props, node_id, status, next_epoch);
        false
    }
}

/// The selection filter, in its original order: the item must be `ready`, match
/// every supplied queue/class/fairness selector, be past its retry backoff, and
/// not have blown its deadline.
pub(crate) fn claim_candidate_is_excluded(
    props: &serde_json::Map<String, serde_json::Value>,
    request: &crate::epistemic_operations::ClaimWorkItemRequest,
    now_s: f64,
) -> bool {
    property_string(props, "status") != "ready"
        || request
            .queue_ref
            .as_deref()
            .is_some_and(|queue| property_string(props, "queue") != queue)
        || request
            .resource_class
            .as_deref()
            .is_some_and(|resource_class| {
                property_string(props, "resource_class") != resource_class
            })
        || request
            .fairness_group
            .as_deref()
            .is_some_and(|fairness_group| {
                property_string(props, "fairness_group") != fairness_group
            })
        || property_f64(props, "next_retry_at") > now_s
        || props
            .get("deadline_unix")
            .and_then(serde_json::Value::as_f64)
            .is_some_and(|deadline| deadline < now_s)
}

/// The sort-key deadline of a selectable candidate: an absent (or already
/// filtered-out) deadline sorts last.
pub(crate) fn claim_candidate_deadline(
    props: &serde_json::Map<String, serde_json::Value>,
    now_s: f64,
) -> u64 {
    props
        .get("deadline_unix")
        .and_then(serde_json::Value::as_f64)
        .filter(|deadline| *deadline >= now_s)
        .map(|deadline| (deadline * 1000.0) as u64)
        .unwrap_or(u64::MAX)
}

/// Classify one scanned node for a claim request.  The checks run in the
/// original order, which matters: admission is tenant-wide even for an exact-id
/// delivery, so live leases contribute to the quota before the exact-id filter,
/// and that filter runs before an expired unrelated row could be reclaimed.
pub(crate) fn classify_claim_row(
    request: &crate::epistemic_operations::ClaimWorkItemRequest,
    node_id: &str,
    mut props: serde_json::Map<String, serde_json::Value>,
    now_s: f64,
) -> ClaimRowOutcome {
    if property_string(&props, "node_type") != "WorkItem" {
        return ClaimRowOutcome::Skip;
    }
    if property_string(&props, "tenant") != request.tenant_ref.as_str() {
        return ClaimRowOutcome::Skip;
    }
    let status = property_string(&props, "status").to_string();
    if matches!(status.as_str(), "leased" | "running")
        && property_f64(&props, "lease_expires_at") > now_s
    {
        return ClaimRowOutcome::InFlight;
    }
    if request
        .work_item_id
        .as_deref()
        .is_some_and(|selected| selected != node_id)
    {
        return ClaimRowOutcome::Skip;
    }
    if matches!(status.as_str(), "leased" | "running")
        && claim_reclaim_expired_lease(&mut props, node_id, &status, now_s)
    {
        return ClaimRowOutcome::Exhausted(props);
    }
    if claim_candidate_is_excluded(&props, request, now_s) {
        return ClaimRowOutcome::Skip;
    }
    let deadline = claim_candidate_deadline(&props, now_s);
    ClaimRowOutcome::Candidate((
        property_u64(&props, "prio_bucket"),
        deadline,
        (property_f64(&props, "created_at") * 1000.0) as u64,
        node_id.to_string(),
        props,
    ))
}

/// One bounded pass over this graph's node rows, tallying the tenant's in-flight
/// leases, the expired rows to retire, and the claimable candidates.
pub(crate) fn scan_claim_work_item_candidates(
    graph: &str,
    request: &crate::epistemic_operations::ClaimWorkItemRequest,
    now_s: f64,
    nodes: &ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<ClaimWorkItemScan, String> {
    permit_scoped_scan(nodes.scope_key(), graph)?;
    let mut scan = ClaimWorkItemScan {
        inflight: 0,
        candidates: Vec::new(),
        exhausted: Vec::new(),
        changed_work_item_ids: Vec::new(),
    };
    for row in nodes.scope_rows()? {
        let (key, value) = row?;
        let (_, node_id) = key.value();
        let bytes = crypto.unseal(value.value())?;
        let Ok(props) = decode_durable::<serde_json::Map<String, serde_json::Value>>(&bytes) else {
            continue;
        };
        match classify_claim_row(request, node_id, props, now_s) {
            ClaimRowOutcome::Skip => {}
            ClaimRowOutcome::InFlight => scan.inflight = scan.inflight.saturating_add(1),
            ClaimRowOutcome::Exhausted(props) => {
                let node_id = node_id.to_string();
                scan.changed_work_item_ids.push(node_id.clone());
                scan.exhausted.push((node_id, props));
            }
            ClaimRowOutcome::Candidate(candidate) => scan.candidates.push(candidate),
        }
    }
    Ok(scan)
}

/// The `claimed: false` result shape, shared by the tenant-quota and empty-queue
/// refusals.
pub(crate) fn claim_not_claimed_payload(
    reason: ClaimWorkItemResultReason,
    inflight: u32,
    changed_work_item_ids: Vec<String>,
) -> Result<crate::protocol::ResultPayload, String> {
    crate::protocol::ResultPayload::of::<eg_types::result_contract::coordination::ClaimWorkItem>(
        ClaimWorkItemResult {
            schema_version: ClaimWorkItemResultSchemaVersion::V1,
            claimed: false,
            reason,
            work_item_id: None,
            kind: None,
            payload_ref: None,
            lease_holder_ref: None,
            lease_epoch: None,
            fencing_token: None,
            lease_expires_at_ms: None,
            attempt: None,
            max_attempts: None,
            tenant_in_flight: Some(u64::from(inflight)),
            changed_work_item_ids,
        },
    )
}

/// Stamp the granted lease onto the selected candidate and report its
/// `(lease_epoch, attempt)`.
///
/// The native claim authority owns the per-attempt WorkItem fence.  Ready
/// submissions cannot supply one through generic graph writes; deriving it from
/// the authoritative lease epoch keeps the Raft transition deterministic while
/// ensuring every capability-bound live lease has a non-empty fence that changes
/// on reclaim.
pub(crate) fn claim_grant_lease(
    props: &mut serde_json::Map<String, serde_json::Value>,
    worker_id: &str,
    now_s: f64,
    lease_until_s: f64,
) -> (u64, u64) {
    let epoch = property_u64(props, "lease_epoch").saturating_add(1);
    let attempt = property_u64(props, "attempt").saturating_add(1);
    props.insert("status".into(), serde_json::Value::String("leased".into()));
    props.insert(
        "lease_owner".into(),
        serde_json::Value::String(worker_id.to_string()),
    );
    props.insert(
        "last_lease_owner".into(),
        serde_json::Value::String(worker_id.to_string()),
    );
    props.insert("lease_epoch".into(), serde_json::Value::from(epoch));
    props.insert("fencing_token".into(), serde_json::Value::from(epoch));
    props.insert(
        "lease_expires_at".into(),
        serde_json::Value::from(lease_until_s),
    );
    props.insert(
        "work_item_fence".into(),
        serde_json::Value::String(format!("lease-fence-v1:{epoch}")),
    );
    props.insert("heartbeat_at".into(), serde_json::Value::from(now_s));
    props.insert("updated_at".into(), serde_json::Value::from(now_s));
    props.insert("attempt".into(), serde_json::Value::from(attempt));
    (epoch, attempt)
}

struct ClaimWorkItemApply<'args, 'table, 'crypto> {
    graph: &'args str,
    request: &'args crate::epistemic_operations::ClaimWorkItemRequest,
    now_ms: u64,
    lease_ms: u64,
    now_s: f64,
    lease_until_s: f64,
    inflight: u32,
    node_id: String,
    props: serde_json::Map<String, serde_json::Value>,
    changed_work_item_ids: Vec<String>,
    nodes: &'args mut ScopedOwnerTableMut<'table, (&'static str, &'static str), &'static [u8]>,
    native_work_items:
        &'args mut ScopedOwnerTableMut<'table, (&'static str, &'static str), &'static [u8]>,
    crypto: DurableCrypto<'crypto>,
}

fn write_exhausted_claim_rows(
    graph: &str,
    exhausted: Vec<(String, serde_json::Map<String, serde_json::Value>)>,
    nodes: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    for (node_id, mut props) in exhausted {
        write_work_item_props(nodes, graph, &node_id, &mut props, crypto)?;
    }
    Ok(())
}

fn select_claim_candidate(mut candidates: Vec<ClaimCandidateRow>) -> Option<ClaimCandidateRow> {
    candidates.sort_by(|left, right| {
        (&left.0, &left.1, &left.2, &left.3).cmp(&(&right.0, &right.1, &right.2, &right.3))
    });
    candidates.into_iter().next()
}

fn apply_claimed_work_item(
    input: ClaimWorkItemApply<'_, '_, '_>,
) -> Result<crate::protocol::ResultPayload, String> {
    let ClaimWorkItemApply {
        graph,
        request,
        now_ms,
        lease_ms,
        now_s,
        lease_until_s,
        inflight,
        node_id,
        mut props,
        mut changed_work_item_ids,
        nodes,
        native_work_items,
        crypto,
    } = input;
    let worker_id = &request.worker_ref;
    let (epoch, attempt) = claim_grant_lease(&mut props, worker_id, now_s, lease_until_s);
    let kind = property_string(&props, "kind").to_string();
    let payload_ref = property_string(&props, "payload_ref").to_string();
    let max_attempts = property_u64(&props, "max_attempts").max(1);
    // Phase-1 statechart mirror: the picked candidate was `ready` (it passed the
    // `status != "ready"` filter above); selection already happened outside the
    // chart, so its `ready --claim--> leased` edge is unconditional.
    #[cfg(feature = "statechart")]
    apply_work_item_mirror(
        &mut props,
        &node_id,
        "ready",
        crate::work_item_statechart::EV_CLAIM,
        serde_json::json!({}),
        Some("leased"),
    );
    write_work_item_props(nodes, graph, &node_id, &mut props, crypto)?;
    work_item_capability::record_native_claim_in_wtx(
        native_work_items,
        graph,
        &node_id,
        &props,
        now_ms,
        crypto,
    )?;
    changed_work_item_ids.push(node_id.clone());
    crate::protocol::ResultPayload::of::<eg_types::result_contract::coordination::ClaimWorkItem>(
        ClaimWorkItemResult {
            schema_version: ClaimWorkItemResultSchemaVersion::V1,
            claimed: true,
            reason: ClaimWorkItemResultReason::Claimed,
            work_item_id: Some(node_id),
            kind: (!kind.is_empty()).then_some(kind),
            payload_ref: (!payload_ref.is_empty()).then_some(payload_ref),
            lease_holder_ref: Some(worker_id.clone()),
            lease_epoch: Some(epoch),
            fencing_token: Some(epoch),
            lease_expires_at_ms: Some(now_ms.saturating_add(lease_ms)),
            attempt: Some(attempt),
            max_attempts: Some(max_attempts),
            tenant_in_flight: Some(u64::from(inflight.saturating_add(1))),
            changed_work_item_ids,
        },
    )
}

pub(crate) fn apply_claim_work_item_row<'txn, 'crypto>(
    graph: &str,
    request: &crate::epistemic_operations::ClaimWorkItemRequest,
    nodes: &mut ScopedOwnerTableMut<'txn, (&'static str, &'static str), &'static [u8]>,
    native_work_items: &mut ScopedOwnerTableMut<'txn, (&'static str, &'static str), &'static [u8]>,
    crypto: DurableCrypto<'crypto>,
) -> Result<Option<crate::protocol::ResultPayload>, String> {
    let tenant = &request.tenant_ref;
    let worker_id = &request.worker_ref;
    let now_ms = request.now_ms;
    let lease_ms = request.lease_ms;
    let max_tenant_in_flight = request.max_tenant_in_flight;
    if tenant.trim().is_empty()
        || worker_id.trim().is_empty()
        || lease_ms == 0
        || !(1..=4096).contains(&max_tenant_in_flight)
    {
        return Err("ClaimWorkItem request violates the current protocol contract".into());
    }
    let now_s = now_ms as f64 / 1000.0;
    let lease_until_s = now_s + (lease_ms as f64 / 1000.0);
    let tenant_in_flight_limit = max_tenant_in_flight as u32;
    let ClaimWorkItemScan {
        inflight,
        candidates,
        exhausted,
        changed_work_item_ids,
    } = scan_claim_work_item_candidates(graph, request, now_s, nodes, crypto)?;
    write_exhausted_claim_rows(graph, exhausted, nodes, crypto)?;
    if inflight >= tenant_in_flight_limit {
        return Ok(Some(claim_not_claimed_payload(
            ClaimWorkItemResultReason::TenantQuota,
            inflight,
            changed_work_item_ids,
        )?));
    }
    let Some((_, _, _, node_id, props)) = select_claim_candidate(candidates) else {
        return Ok(Some(claim_not_claimed_payload(
            ClaimWorkItemResultReason::Empty,
            inflight,
            changed_work_item_ids,
        )?));
    };
    Ok(Some(apply_claimed_work_item(ClaimWorkItemApply {
        graph,
        request,
        now_ms,
        lease_ms,
        now_s,
        lease_until_s,
        inflight,
        node_id,
        props,
        changed_work_item_ids,
        nodes: &mut *nodes,
        native_work_items: &mut *native_work_items,
        crypto,
    })?))
}
