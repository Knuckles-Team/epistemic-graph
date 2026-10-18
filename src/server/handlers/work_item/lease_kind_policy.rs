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
//! is the leader's. A lease that is not visible refuses before dispatch: a
//! concurrent issue must not supply an unapproved kind after this lookup.

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
    let kind = match target {
        LeaseTarget::Kind(kind) => Some(kind.to_owned()),
        LeaseTarget::Stored { tenant, lease_id } => stored_kind(ctx, tenant, lease_id).await?,
    };
    check_resolved_kind(ctx.verified_context, policy, kind.as_deref())
}

fn check_resolved_kind(
    verified: &crate::server::auth::VerifiedRequestContext,
    policy: &ControlLeaseKindPolicy,
    kind: Option<&str>,
) -> Result<(), String> {
    // Never dispatch after an absent lookup: an issue could race that read and
    // create a kind for which this caller has not been authorized.
    let kind = kind.ok_or("ACCESS_DENIED: control lease target unavailable before transition")?;
    require_pending_work_authority(kind, verified)?;
    policy.require(verified.agent_id(), kind)
}

/// A pending-call lease also mutates a WorkItem; lease-write alone is insufficient.
fn require_pending_work_authority(
    kind: &str,
    verified: &crate::server::auth::VerifiedRequestContext,
) -> Result<(), String> {
    if kind == eg_types::control_lease::PENDING_CALL_KIND
        && !verified.allows_method("work:write", true)
    {
        return Err("ACCESS_DENIED: pending-call storage requires work:write authority".into());
    }
    Ok(())
}

/// The paired storage primitive has no served authority composition yet.
fn refuse_paired_plan(req_id: u64, method: &Method) -> Option<Response> {
    let paired = match method {
        Method::IssueControlLease { request } => request
            .pending_call_registration
            .as_ref()
            .is_some_and(|pending| pending.plan.is_some()),
        Method::TransitionControlLease { request } => request
            .pending_call_consumption
            .as_ref()
            .is_some_and(|pending| pending.plan.is_some()),
        _ => false,
    };
    paired.then(|| {
        crate::server::contract_wave::not_yet_served(req_id, "ControlLease.pending_call.plan")
    })
}

/// `Some(refusal)` when the verified principal may not write this lease.
pub(super) async fn refuse_unpermitted_lease_kind(
    ctx: &HandleContext<'_>,
    method: &Method,
) -> Option<Response> {
    if let Some(refusal) = refuse_paired_plan(ctx.req_id, method) {
        return Some(refusal);
    }
    let target = lease_target(method)?;
    let outcome = match policy() {
        Ok(policy) => check(ctx, policy, target).await,
        Err(error) => Err(format!("ACCESS_DENIED: {error}")),
    };
    refusal_response(ctx.req_id, outcome)
}

fn refusal_response(req_id: u64, outcome: Result<(), String>) -> Option<Response> {
    outcome.err().map(|denied| {
        crate::metrics::access_denied();
        Response::err(req_id, denied)
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

    // spec: EG-DURABLE-KERNEL-R039.3
    #[test]
    fn lease_write_alone_cannot_change_pending_work_item_state() {
        use crate::server::auth::VerifiedRequestContext;
        use eg_types::control_lease::PENDING_CALL_KIND;
        let lease_only = VerifiedRequestContext::verified_for_test_with_scopes(
            "worker-a",
            "tenant-a",
            &["lease:write"],
        );
        assert!(super::require_pending_work_authority(PENDING_CALL_KIND, &lease_only).is_err());
        assert!(super::require_pending_work_authority("browser.control", &lease_only).is_ok());
        let both = VerifiedRequestContext::verified_for_test_with_scopes(
            "worker-a",
            "tenant-a",
            &["lease:write", "work:write"],
        );
        assert!(super::require_pending_work_authority(PENDING_CALL_KIND, &both).is_ok());
    }

    // spec: EG-DURABLE-KERNEL-R039.3
    #[test]
    fn absent_lookup_returns_a_terminal_refusal_before_any_later_registration() {
        use crate::server::auth::VerifiedRequestContext;
        use eg_types::control_lease::{ControlLeaseKindPolicy, PENDING_CALL_KIND};
        let verified = VerifiedRequestContext::verified_for_test_with_scopes(
            "worker-a",
            "tenant-a",
            &["lease:write", "work:write"],
        );
        let policy = ControlLeaseKindPolicy::default();
        let absent = super::check_resolved_kind(&verified, &policy, None);
        let stop_dispatch = super::refusal_response(1, absent);
        // This is the handler's terminal-response boundary, not a concurrent
        // storage test: a later lookup cannot replace the already made refusal.
        assert!(stop_dispatch.is_some());
        assert!(super::check_resolved_kind(&verified, &policy, Some(PENDING_CALL_KIND)).is_ok());
        assert!(stop_dispatch.is_some());
    }
    // spec: EG-DURABLE-KERNEL-R039.4
    #[test]
    fn paired_storage_is_unavailable_on_both_served_writes() {
        use crate::protocol::Method;
        use serde_json::json;
        let plan = json!({"lease_id":"plan-a", "expected_revision":1,
            "expected_kind":"configured.plan", "expected_grant":{}});
        let work = json!({"work_item_id":"work-a", "expected_revision":1,
            "expected_metadata":{}});
        for paired in [false, true] {
            let mut registration = json!({"work_item":work,
                "expected_lease":{"worker_ref":"worker-a","lease_epoch":1,"fencing_token":1}});
            let mut consumption = json!({"work_item":work,"expected_grant":{}});
            if paired {
                registration["plan"] = plan.clone();
                consumption["plan"] = plan.clone();
            }
            let issue = Method::IssueControlLease {
                request: serde_json::from_value(json!({
                    "tenant":"tenant-a","lease_id":"pending-a","kind":"work_item.pending_call",
                    "grant":{},"issued_at_ms":1000,"expires_at_ms":2000,"hard_expires_at_ms":2000,
                    "idempotency_key":"issue-a","pending_call_registration":registration
                }))
                .unwrap(),
            };
            let consume = Method::TransitionControlLease {
                request: serde_json::from_value(json!({
                    "tenant":"tenant-a","lease_id":"pending-a","expected_revision":1,
                    "to":"consumed","idempotency_key":"consume-a",
                    "pending_call_consumption":consumption
                }))
                .unwrap(),
            };
            for method in [issue, consume] {
                let refusal = super::refuse_paired_plan(7, &method);
                assert_eq!(refusal.is_some(), paired);
                if let Some(response) = refusal {
                    assert_eq!(response.id, 7);
                    assert!(response
                        .refusal_text()
                        .unwrap()
                        .starts_with("METHOD_NOT_YET_SERVED:"));
                }
            }
        }
    }
}
