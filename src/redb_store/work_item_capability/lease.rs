//! The authoritative live-lease read and the capability-record binding check.
//!
//! Both run against public control state or an already-decoded record, never
//! a private capability row read, so every caller can apply them before (or
//! instead of) touching private capability material.

use crate::redb_store::{decode_durable, property_f64, property_string, property_u64};

use super::{
    is_work_item, native_claim_exists, AuthenticatedAuthority, CapabilityRecord, CapabilityRows,
    DurableCrypto, LiveLease, Refusal,
};

type Properties = serde_json::Map<String, serde_json::Value>;

pub(super) fn read_live_lease(
    nodes: &CapabilityRows<'_>,
    native_work_items: &CapabilityRows<'_>,
    graph: &str,
    work_item_id: &str,
    authority: &AuthenticatedAuthority,
    crypto: DurableCrypto<'_>,
) -> Result<LiveLease, Refusal> {
    let props = read_work_item_properties(nodes, graph, work_item_id, crypto)?;
    if !native_claim_exists(native_work_items, graph, work_item_id, crypto)? {
        // A generic NODES row is not a native WorkItem authority, even when it
        // happens to contain a complete plausible lease tuple.
        return Err(Refusal::Stale);
    }
    ensure_lease_held_by(&props, authority)?;
    let lease = LiveLease {
        tenant: authority.tenant.clone(),
        agent_id: authority.agent_id.clone(),
        expires_at_ms: lease_expiry_ms(&props, authority.now_ms)?,
        attempt: property_u64(&props, "attempt"),
        lease_epoch: property_u64(&props, "lease_epoch"),
        fencing_token: property_u64(&props, "fencing_token"),
        work_item_fence: property_string(&props, "work_item_fence").to_string(),
    };
    if !is_fenced(&lease) {
        return Err(Refusal::Stale);
    }
    Ok(lease)
}

/// The decoded properties of the WorkItem control row, or why there are none.
fn read_work_item_properties(
    nodes: &CapabilityRows<'_>,
    graph: &str,
    work_item_id: &str,
    crypto: DurableCrypto<'_>,
) -> Result<Properties, Refusal> {
    let row = nodes
        .get((graph, work_item_id))
        .map_err(|_| Refusal::NotFound)?
        .ok_or(Refusal::NotFound)?;
    let bytes = crypto.unseal(row.value()).map_err(|_| Refusal::Malformed)?;
    let props: Properties = decode_durable(&bytes).map_err(|_| Refusal::Malformed)?;
    if !is_work_item(&props) {
        return Err(Refusal::NotFound);
    }
    Ok(props)
}

/// The caller's tenant owns a lease that is currently leased or running.
fn ensure_lease_held_by(
    props: &Properties,
    authority: &AuthenticatedAuthority,
) -> Result<(), Refusal> {
    if property_string(props, "tenant") != authority.tenant {
        return Err(Refusal::Unauthorized);
    }
    if !matches!(property_string(props, "status"), "leased" | "running") {
        return Err(Refusal::Stale);
    }
    if property_string(props, "lease_owner") != authority.agent_id {
        return Err(Refusal::Unauthorized);
    }
    Ok(())
}

/// The lease expiry in milliseconds, refused when malformed or already past.
fn lease_expiry_ms(props: &Properties, now_ms: u64) -> Result<u64, Refusal> {
    let expiry_s = property_f64(props, "lease_expires_at");
    if !expiry_s.is_finite() || expiry_s <= 0.0 {
        return Err(Refusal::Malformed);
    }
    let expires_at_ms = (expiry_s * 1000.0).floor() as u64;
    if expires_at_ms <= now_ms {
        return Err(Refusal::Expired);
    }
    Ok(expires_at_ms)
}

/// A claimed lease carries a non-zero attempt, epoch and fence token.
fn is_fenced(lease: &LiveLease) -> bool {
    lease.attempt != 0
        && lease.lease_epoch != 0
        && lease.fencing_token != 0
        && !lease.work_item_fence.is_empty()
}

pub(super) fn record_matches_live(
    record: &CapabilityRecord,
    graph: &str,
    work_item_id: &str,
    authority: &AuthenticatedAuthority,
    live: &LiveLease,
) -> bool {
    // The record was minted for exactly this graph item, authenticated caller
    // context and live lease tuple, and has not expired.
    let minted_for = (
        (record.graph.as_str(), record.work_item_id.as_str()),
        (
            record.tenant.as_str(),
            record.audience.as_str(),
            record.principal.as_str(),
            record.agent_id.as_str(),
            record.session.as_str(),
            record.authority_epoch,
            record.incarnation_id.as_str(),
        ),
        (
            record.attempt,
            record.lease_epoch,
            record.fencing_token,
            record.work_item_fence.as_str(),
            record.expires_at_ms,
        ),
    );
    let presented = (
        (graph, work_item_id),
        (
            authority.tenant.as_str(),
            authority.audience.as_str(),
            authority.principal.as_str(),
            authority.agent_id.as_str(),
            authority.session.as_str(),
            authority.authority_epoch,
            authority.incarnation_id.as_str(),
        ),
        (
            live.attempt,
            live.lease_epoch,
            live.fencing_token,
            live.work_item_fence.as_str(),
            live.expires_at_ms,
        ),
    );
    record.schema_version == 1 && minted_for == presented && record.expires_at_ms > authority.now_ms
}
