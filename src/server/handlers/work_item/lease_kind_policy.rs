//! Enforce the deploy's per-principal control-lease kind allowlist
//! ([`ControlLeaseKindPolicy`], operator ruling 2026-09-24) on both native
//! lease writes, in their handler, before they reach the WorkItem kernel.
//! Dispatch has already checked the caller's `lease:write` scope and bound
//! the body tenant to the verified carrier; this only narrows.
//!
//! The policy comes from `EPISTEMIC_GRAPH_CONTROL_LEASE_KIND_POLICY_JSON`,
//! read once. Unset means no principal is restricted. A malformed value
//! refuses every control-lease write in this process (fail closed) rather
//! than dropping the restriction.
//!
//! An issue names its kind. A transition does not, so the stored lease is
//! read; its kind is immutable after issue, so the read cannot race a change
//! of kind; under raft it runs after the placement-leader check, so the read
//! is the leader's. A lease that is not visible falls through to the kernel,
//! which answers `not_found` itself.

use std::sync::OnceLock;

use eg_types::control_lease::ControlLeaseKindPolicy;

use super::HandleContext;
use crate::protocol::{Method, Response};

const POLICY_ENV: &str = "EPISTEMIC_GRAPH_CONTROL_LEASE_KIND_POLICY_JSON";

fn load_policy(raw: Option<String>) -> Result<ControlLeaseKindPolicy, String> {
    raw.map_or_else(
        || Ok(ControlLeaseKindPolicy::default()),
        |raw| ControlLeaseKindPolicy::from_json(&raw),
    )
}

fn policy() -> &'static Result<ControlLeaseKindPolicy, String> {
    static POLICY: OnceLock<Result<ControlLeaseKindPolicy, String>> = OnceLock::new();
    POLICY.get_or_init(|| load_policy(std::env::var(POLICY_ENV).ok()))
}

/// The lease a write targets: its kind when the request names it, else the
/// tenant and id to look it up by.
enum LeaseTarget<'a> {
    Kind(&'a str),
    Stored { tenant: &'a str, lease_id: &'a str },
}

fn lease_target(method: &Method) -> Option<LeaseTarget<'_>> {
    match method {
        Method::IssueControlLease { request } => Some(LeaseTarget::Kind(&request.kind)),
        Method::TransitionControlLease { request } => Some(LeaseTarget::Stored {
            tenant: &request.tenant,
            lease_id: &request.lease_id,
        }),
        _ => None,
    }
}

async fn stored_kind(
    ctx: &HandleContext<'_>,
    tenant: &str,
    lease_id: &str,
) -> Result<Option<String>, String> {
    let backend = ctx
        .persistence
        .as_ref()
        .and_then(|backend| backend.as_redb())
        .ok_or_else(|| "control lease writes require the redb persistence backend".to_string())?;
    let graph = crate::persist::sanitize(ctx.graph_name);
    Ok(backend
        .read_control_lease(&graph, tenant, lease_id)
        .await?
        .map(|lease| lease.kind))
}

async fn check(
    ctx: &HandleContext<'_>,
    policy: &ControlLeaseKindPolicy,
    target: LeaseTarget<'_>,
) -> Result<(), String> {
    let agent_id = ctx.verified_context.agent_id();
    if !policy.restricts(agent_id) {
        return Ok(());
    }
    match target {
        LeaseTarget::Kind(kind) => policy.require(agent_id, kind),
        LeaseTarget::Stored { tenant, lease_id } => {
            let stored = stored_kind(ctx, tenant, lease_id).await?;
            stored.map_or(Ok(()), |kind| policy.require(agent_id, &kind))
        }
    }
}

/// `Some(refusal)` when the verified principal may not write this lease.
pub(super) async fn refuse_unpermitted_lease_kind(
    ctx: &HandleContext<'_>,
    method: &Method,
) -> Option<Response> {
    let target = lease_target(method)?;
    let outcome = match policy() {
        Ok(policy) => check(ctx, policy, target).await,
        Err(error) => Err(format!("ACCESS_DENIED: {error}")),
    };
    outcome.err().map(|denied| {
        crate::metrics::access_denied();
        Response::err(ctx.req_id, denied)
    })
}

#[cfg(test)]
mod tests {
    use super::load_policy;

    #[test]
    fn an_unset_policy_restricts_nobody_and_a_malformed_one_fails_closed() {
        let unset = load_policy(None).expect("unset is the empty policy");
        assert!(!unset.restricts("service:graph-os"));
        assert!(load_policy(Some("{not json".to_string())).is_err());
        let set = load_policy(Some(
            r#"{"service:graph-os": ["finance.order-proposal"]}"#.into(),
        ))
        .expect("well-formed");
        assert!(set.permits("service:graph-os", "finance.order-proposal"));
        assert!(!set.permits("service:graph-os", "rbac.elevation"));
    }
}
