//! WorkItem cancel.rs transitions.

use super::*;

pub(crate) struct CancelWorkItemInput<'args, 'table, 'crypto> {
    pub(crate) graph: &'args str,
    pub(crate) tenant: &'args String,
    pub(crate) work_item_id: &'args str,
    pub(crate) reason_ref: &'args Option<String>,
    pub(crate) now_ms: u64,
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

fn load_transition_work_item_props(
    graph: &str,
    work_item_id: &str,
    nodes: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<Option<serde_json::Map<String, serde_json::Value>>, String> {
    nodes
        .get((graph, work_item_id))?
        .map(|value| crypto.unseal(value.value()))
        .transpose()?
        .map(|bytes| decode_durable(&bytes))
        .transpose()
}

fn cancel_status_refusal(
    props: &serde_json::Map<String, serde_json::Value>,
    now_s: f64,
) -> Option<eg_types::result_contract::coordination::WorkItemCancelStatus> {
    if matches!(
        property_string(props, "status"),
        "succeeded" | "failed" | "cancelled" | "dead_letter"
    ) {
        return Some(eg_types::result_contract::coordination::WorkItemCancelStatus::Noop);
    }
    if matches!(property_string(props, "status"), "leased" | "running")
        && property_f64(props, "lease_expires_at") >= now_s
    {
        return Some(eg_types::result_contract::coordination::WorkItemCancelStatus::InFlight);
    }
    if !matches!(
        property_string(props, "status"),
        "submitted" | "ready" | "leased" | "running"
    ) {
        return Some(eg_types::result_contract::coordination::WorkItemCancelStatus::NotCancellable);
    }
    None
}

fn apply_cancelled_work_item_props(
    props: &mut serde_json::Map<String, serde_json::Value>,
    reason_ref: &Option<String>,
    now_s: f64,
) -> Result<(u64, u64), String> {
    let lease_owner = property_string(props, "lease_owner");
    let last_lease_owner = if lease_owner.is_empty() {
        property_string(props, "last_lease_owner")
    } else {
        lease_owner
    }
    .to_string();
    let next_epoch = property_u64(props, "lease_epoch")
        .checked_add(1)
        .ok_or_else(|| "CancelWorkItem lease epoch overflow".to_string())?;
    let next_fencing_token = property_u64(props, "fencing_token")
        .checked_add(1)
        .ok_or_else(|| "CancelWorkItem fencing token overflow".to_string())?;
    props.insert(
        "status".into(),
        serde_json::Value::String("cancelled".into()),
    );
    props.insert("completed_at".into(), serde_json::Value::from(now_s));
    props.insert("updated_at".into(), serde_json::Value::from(now_s));
    props.insert("lease_owner".into(), serde_json::Value::Null);
    props.insert(
        "last_lease_owner".into(),
        serde_json::Value::String(last_lease_owner),
    );
    props.insert("lease_expires_at".into(), serde_json::Value::Null);
    props.insert("lease_epoch".into(), serde_json::Value::from(next_epoch));
    props.insert(
        "fencing_token".into(),
        serde_json::Value::from(next_fencing_token),
    );
    props.insert(
        "cancel_reason_ref".into(),
        reason_ref
            .clone()
            .map(serde_json::Value::String)
            .unwrap_or(serde_json::Value::Null),
    );
    Ok((next_epoch, next_fencing_token))
}

pub(crate) fn apply_cancel_work_item_row(
    input: CancelWorkItemInput<'_, '_, '_>,
) -> Result<Option<crate::protocol::ResultPayload>, String> {
    let CancelWorkItemInput {
        graph,
        tenant,
        work_item_id,
        reason_ref,
        now_ms,
        nodes,
        holds,
        work_item_index,
        counters,
        pressure_index,
        policies,
        crypto,
    } = input;
    let Some(mut props) = load_transition_work_item_props(graph, work_item_id, nodes, crypto)?
    else {
        return cancel_transition_result(unchanged_transition(
            eg_types::result_contract::coordination::WorkItemCancelStatus::Missing,
            None,
        ))
        .map(Some);
    };
    let pre_props = props.clone();
    if property_string(&props, "tenant") != tenant {
        return cancel_transition_result(unchanged_transition(
            eg_types::result_contract::coordination::WorkItemCancelStatus::Missing,
            None,
        ))
        .map(Some);
    }
    let now_s = now_ms as f64 / 1000.0;
    if let Some(status) = cancel_status_refusal(&props, now_s) {
        return cancel_transition_result(unchanged_transition(status, Some(work_item_id)))
            .map(Some);
    }
    // Phase-1 mirror: capture the pre-status (a cancellable non-terminal state)
    // before the authority marks it cancelled.
    #[cfg(feature = "statechart")]
    let pre_status = property_string(&props, "status").to_string();
    let (next_epoch, next_fencing_token) =
        apply_cancelled_work_item_props(&mut props, reason_ref, now_s)?;
    development_lane::transition_work_item_terminal_hold(
        graph,
        &pre_props,
        work_item_id,
        "cancelled",
        true,
        property_u64(&props, "attempt"),
        property_u64(&props, "lease_epoch"),
        property_u64(&props, "fencing_token"),
        property_string(&props, "work_item_fence"),
        holds,
        work_item_index,
        counters,
        pressure_index,
        policies,
        crypto,
    )?;
    #[cfg(feature = "statechart")]
    apply_work_item_mirror(
        &mut props,
        work_item_id,
        &pre_status,
        crate::work_item_statechart::EV_CANCEL,
        serde_json::json!({ "cancellable": true }),
        Some("cancelled"),
    );
    write_work_item_props(nodes, graph, work_item_id, &props, crypto)?;
    cancel_transition_result(
        eg_types::result_contract::coordination::WorkItemTransition {
            status: eg_types::result_contract::coordination::WorkItemCancelStatus::Cancelled,
            work_item_id: Some(work_item_id.to_string()),
            lease_epoch: Some(next_epoch),
            fencing_token: Some(next_fencing_token),
            changed_work_item_ids: vec![work_item_id.to_string()],
        },
    )
    .map(Some)
}

pub(crate) struct DeferWorkItemInput<'args, 'table, 'crypto> {
    pub(crate) fence: WorkItemFenceKey<'args>,
    pub(crate) next_retry_at_ms: u64,
    pub(crate) reason_ref: &'args Option<String>,
    pub(crate) now_ms: u64,
    pub(crate) nodes:
        &'args mut ScopedOwnerTableMut<'table, (&'static str, &'static str), &'static [u8]>,
    pub(crate) crypto: DurableCrypto<'crypto>,
}

fn defer_lease_is_valid(
    props: &serde_json::Map<String, serde_json::Value>,
    tenant: &String,
    worker_id: &String,
    lease_epoch: u64,
    fencing_token: u64,
    now_s: f64,
) -> bool {
    property_string(props, "tenant") == tenant
        && property_string(props, "lease_owner") == worker_id
        && matches!(property_string(props, "status"), "leased" | "running")
        && property_u64(props, "lease_epoch") == lease_epoch
        && property_u64(props, "fencing_token") == fencing_token
        && property_f64(props, "lease_expires_at") >= now_s
}

fn apply_deferred_work_item_props(
    props: &mut serde_json::Map<String, serde_json::Value>,
    reason_ref: &Option<String>,
    next_retry_at_ms: u64,
    now_s: f64,
    lease_epoch: u64,
) -> (u64, u64, u64) {
    let next_epoch = lease_epoch.saturating_add(1);
    let attempts = property_u64(props, "attempt").saturating_sub(1);
    let defer_count = property_u64(props, "defer_count").saturating_add(1);
    props.insert("status".into(), serde_json::Value::String("ready".into()));
    props.insert(
        "next_retry_at".into(),
        serde_json::Value::from(next_retry_at_ms as f64 / 1000.0),
    );
    props.insert("attempt".into(), serde_json::Value::from(attempts));
    props.insert("defer_count".into(), serde_json::Value::from(defer_count));
    props.insert("lease_owner".into(), serde_json::Value::Null);
    props.insert("lease_expires_at".into(), serde_json::Value::Null);
    props.insert("lease_epoch".into(), serde_json::Value::from(next_epoch));
    props.insert("fencing_token".into(), serde_json::Value::from(next_epoch));
    props.insert("updated_at".into(), serde_json::Value::from(now_s));
    props.insert(
        "defer_reason_ref".into(),
        reason_ref
            .clone()
            .map(serde_json::Value::String)
            .unwrap_or(serde_json::Value::Null),
    );
    (next_epoch, attempts, defer_count)
}

pub(crate) fn apply_defer_work_item_row(
    input: DeferWorkItemInput<'_, '_, '_>,
) -> Result<Option<crate::protocol::ResultPayload>, String> {
    let DeferWorkItemInput {
        fence:
            WorkItemFenceKey {
                graph,
                tenant,
                work_item_id,
                worker_id,
                lease_epoch,
                fencing_token,
            },
        next_retry_at_ms,
        reason_ref,
        now_ms,
        nodes,
        crypto,
    } = input;
    if next_retry_at_ms < now_ms {
        return Err("DeferWorkItem next_retry_at_ms must not precede now_ms".into());
    }
    let Some(mut props) = load_transition_work_item_props(graph, work_item_id, nodes, crypto)?
    else {
        return deferral_result(deferral_refused(
            eg_types::result_contract::coordination::WorkItemDeferStatus::Missing,
            None,
        ))
        .map(Some);
    };
    let now_s = now_ms as f64 / 1000.0;
    if !defer_lease_is_valid(&props, tenant, worker_id, lease_epoch, fencing_token, now_s) {
        return deferral_result(deferral_refused(
            eg_types::result_contract::coordination::WorkItemDeferStatus::Fenced,
            Some(work_item_id),
        ))
        .map(Some);
    }
    // Phase-1 mirror: capture the leased|running pre-status before the fenced
    // lease is released back to `ready`.
    #[cfg(feature = "statechart")]
    let pre_status = property_string(&props, "status").to_string();
    let (next_epoch, attempts, defer_count) = apply_deferred_work_item_props(
        &mut props,
        reason_ref,
        next_retry_at_ms,
        now_s,
        lease_epoch,
    );
    #[cfg(feature = "statechart")]
    apply_work_item_mirror(
        &mut props,
        work_item_id,
        &pre_status,
        crate::work_item_statechart::EV_DEFER,
        serde_json::json!({ "fence_valid": true }),
        Some("ready"),
    );
    write_work_item_props(nodes, graph, work_item_id, &props, crypto)?;
    deferral_result(eg_types::result_contract::coordination::WorkItemDeferral {
        status: eg_types::result_contract::coordination::WorkItemDeferStatus::Deferred,
        work_item_id: Some(work_item_id.to_string()),
        lease_epoch: Some(next_epoch),
        fencing_token: Some(next_epoch),
        next_retry_at_ms: Some(next_retry_at_ms),
        attempt: Some(attempts),
        defer_count: Some(defer_count),
        changed_work_item_ids: vec![work_item_id.to_string()],
    })
    .map(Some)
}

/// A `DeferWorkItem` that changed no row.
fn deferral_refused(
    status: eg_types::result_contract::coordination::WorkItemDeferStatus,
    work_item_id: Option<&str>,
) -> eg_types::result_contract::coordination::WorkItemDeferral {
    eg_types::result_contract::coordination::WorkItemDeferral {
        status,
        work_item_id: work_item_id.map(str::to_string),
        lease_epoch: None,
        fencing_token: None,
        next_retry_at_ms: None,
        attempt: None,
        defer_count: None,
        changed_work_item_ids: Vec::new(),
    }
}

fn deferral_result(
    deferral: eg_types::result_contract::coordination::WorkItemDeferral,
) -> Result<crate::protocol::ResultPayload, String> {
    crate::protocol::ResultPayload::of::<eg_types::result_contract::coordination::DeferWorkItem>(
        deferral,
    )
}
