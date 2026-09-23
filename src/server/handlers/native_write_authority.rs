//! Authority checks for native writes whose body names its own scope.
//!
//! * **WorkItem-kernel writes bind their body tenant to the verified carrier
//!   tenant.** Every WorkItem transition (`ClaimWorkItem`, `RenewWorkItemLease`,
//!   `CommitWorkItemResult`, `CancelWorkItem`, `DeferWorkItem`,
//!   `CasWorkItemMetadata`), every admission (`SubmitWorkItem`,
//!   `SubmitWorkItems` incl. each child, `KgDelegate`), every control-lease
//!   write and every work-market write (`GapUpsert`, `GapTransition`,
//!   `GapSettle`, `WorkOfferPut`) names a tenant in its body. That name is a correlation, never an
//!   authority claim: a mismatch is `ACCESS_DENIED`, with no aggregate-reader or
//!   `kg:admin` exception -- the same rule the typed reads apply.
//! * **Fleet event streams are written only by the fleet event authority.** A
//!   write to a `fleet.`-prefixed broker stream (declare, publish, trim, commit
//!   offset) needs the exact `fleet:events` scope (or its `fleet:*` wildcard, or
//!   `kg:admin`); a coarse `kg:write` is not enough (graph-os EG-5).
//!
//! Replicated state-machine applies are exempt (the proposing request was
//! checked); the caller skips this module for them.

use crate::protocol::Method;

/// The scope that may write `fleet.` broker streams.
pub(crate) const FLEET_EVENT_ACTION: &str = "fleet:events";
/// Broker streams under this prefix belong to the fleet event authority.
const FLEET_STREAM_PREFIX: &str = "fleet.";

/// The request's tenant is a correlation, not an authority claim: it must be
/// the tenant the verified carrier names.
pub(crate) fn require_carrier_tenant(requested: &str, verified: &str) -> Result<(), String> {
    if verified.is_empty() || requested != verified {
        return Err("ACCESS_DENIED: request tenant must match verified request tenant".into());
    }
    Ok(())
}

/// Refuse a native write whose body names a tenant other than the verified
/// carrier's, or that writes a fleet stream without the fleet authority.
pub(crate) fn refuse_foreign_native_write(
    method: &Method,
    verified_tenant: &str,
    fleet_authority: FleetAuthority,
) -> Result<(), String> {
    work_item_body_tenants(method)
        .into_iter()
        .try_for_each(|tenant| require_carrier_tenant(tenant, verified_tenant))?;
    let fleet_write =
        fleet_stream_write(method).is_some_and(|s| s.starts_with(FLEET_STREAM_PREFIX));
    if fleet_write && fleet_authority == FleetAuthority::Absent {
        return Err(format!(
            "ACCESS_DENIED: fleet event streams require the '{FLEET_EVENT_ACTION}' scope"
        ));
    }
    Ok(())
}

/// Whether the verified caller holds the fleet event authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FleetAuthority {
    Held,
    Absent,
}

/// Every tenant a WorkItem-kernel write names in its body.
fn work_item_body_tenants(method: &Method) -> Vec<&str> {
    match method {
        Method::ClaimWorkItem { request } => vec![request.tenant_ref.as_str()],
        Method::CasWorkItemMetadata { request } => vec![request.tenant_ref.as_str()],
        Method::RenewWorkItemLease { tenant, .. }
        | Method::CommitWorkItemResult { tenant, .. }
        | Method::CancelWorkItem { tenant, .. }
        | Method::DeferWorkItem { tenant, .. } => vec![tenant.as_str()],
        Method::IssueControlLease { request } => vec![request.tenant.as_str()],
        Method::TransitionControlLease { request } => vec![request.tenant.as_str()],
        // EH-346: the engine-internal policy-record store binds the same way.
        Method::PolicyEvolutionStore { request } => vec![request.tenant_id.as_str()],
        Method::SubmitWorkItem { request } => vec![request.context.tenant_id.as_str()],
        Method::KgDelegate { request } => vec![request.context.tenant_id.as_str()],
        Method::SubmitWorkItems { request } => std::iter::once(&request.context)
            .chain(request.requests.iter().map(|child| &child.context))
            .map(|context| context.tenant_id.as_str())
            .collect(),
        // EH-348 work-market writes share the kernel and its tenant rule.
        other => eg_types::work_market::market_write_scope(other)
            .map(|(tenant, _)| vec![tenant])
            .unwrap_or_default(),
    }
}

/// The stream a broker-stream write targets.
fn fleet_stream_write(method: &Method) -> Option<&str> {
    match method {
        Method::StreamDeclare { stream, .. }
        | Method::StreamPublish { stream, .. }
        | Method::StreamTrim { stream, .. }
        | Method::StreamCommitOffset { stream, .. } => Some(stream.as_str()),
        _ => None,
    }
}

#[cfg(test)]
mod tests;
