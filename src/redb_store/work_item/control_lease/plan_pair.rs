//! Optional immutable plan pairing; no human or execution authority.
use super::*;
use medulla_types::control_lease::{ControlLeaseStatus, PendingPlanPreimage};

const PLAN_BINDING: &str = "pending_call_plan_binding";

fn live_plan(
    graph: &str,
    tenant: &str,
    expected: &PendingPlanPreimage,
    nodes: &NativeNodeRows<'_>,
    now_ms: u64,
    crypto: DurableCrypto<'_>,
) -> Result<NodeRow, String> {
    expected.validate()?;
    let row = load_row(nodes, graph, &expected.lease_id, crypto)?
        .filter(|row| is_tenant_control_lease(row, tenant))
        .ok_or("paired plan unavailable")?;
    let view = ControlLeaseView::from_row(&expected.lease_id, &row)?;
    let exact = view.revision == expected.expected_revision
        && view.kind == expected.expected_kind
        && view.grant == expected.expected_grant;
    let live = view.status == ControlLeaseStatus::Active
        && view.issued_at_ms <= now_ms
        && now_ms < view.expires_at_ms
        && now_ms < view.hard_expires_at_ms;
    if !exact || !live {
        return Err("paired plan preimage or lifetime conflict".into());
    }
    Ok(row)
}

pub(super) fn register(
    graph: &str,
    request: &IssueControlLeaseRequest,
    lease: &mut NodeRow,
    nodes: &NativeNodeRows<'_>,
    now_ms: u64,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let Some(plan) = request
        .pending_call_registration
        .as_ref()
        .and_then(|p| p.plan.as_ref())
    else {
        return Ok(());
    };
    let row = live_plan(graph, &request.tenant, plan, nodes, now_ms, crypto)?;
    if request.issued_at_ms < property_u64(&row, "issued_at_ms")
        || request.expires_at_ms > property_u64(&row, "expires_at_ms")
        || request.hard_expires_at_ms > property_u64(&row, "hard_expires_at_ms")
    {
        return Err("pending lifetime exceeds paired plan".into());
    }
    lease.insert(
        PLAN_BINDING.into(),
        serde_json::to_value(plan).map_err(|e| e.to_string())?,
    );
    Ok(())
}

pub(super) fn consume(
    graph: &str,
    request: &TransitionControlLeaseRequest,
    lease: &NodeRow,
    nodes: &NativeNodeRows<'_>,
    now_ms: u64,
    crypto: DurableCrypto<'_>,
) -> Result<Option<(String, NodeRow)>, String> {
    if request.to != ControlLeaseTarget::Consumed {
        return Ok(None);
    }
    let presented = request
        .pending_call_consumption
        .as_ref()
        .and_then(|p| p.plan.as_ref());
    let stored = lease.get(PLAN_BINDING);
    let Some(plan) = presented else {
        return if stored.is_some() {
            Err("registered plan pair cannot be omitted".into())
        } else {
            Ok(None)
        };
    };
    let encoded = serde_json::to_value(plan).map_err(|e| e.to_string())?;
    if stored != Some(&encoded) || plan.lease_id == request.lease_id {
        return Err("paired plan does not match native registration".into());
    }
    let mut row = live_plan(graph, &request.tenant, plan, nodes, now_ms, crypto)?;
    row.insert(
        "status".into(),
        ControlLeaseStatus::Consumed.as_stored().into(),
    );
    Ok(Some((plan.lease_id.clone(), row)))
}
