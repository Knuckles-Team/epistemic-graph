//! Explicit server owner for native WorkItem lifecycle transitions.
//!
//! The six transition methods are selected here rather than by dispatch's
//! `is_work_item_mutation_method` classifier. Their authoritative effect is
//! unchanged: `mutation_batch::commit_work_item` atomically persists the
//! transition/result/outbox before refreshing the in-memory graph projection.

use std::sync::Arc;

use crate::graph::GraphCore;
use crate::protocol::{Method, Response};
use crate::server::auth::VerifiedRequestContext;
use crate::server::persistence::PersistenceBackend;

/// Already-authorized graph and placement context for one WorkItem transition.
pub(crate) struct HandleContext<'a> {
    pub(crate) req_id: u64,
    pub(crate) graph_name: &'a str,
    pub(crate) caller: Option<&'a str>,
    pub(crate) verified_context: &'a VerifiedRequestContext,
    pub(crate) core: &'a Arc<GraphCore>,
    pub(crate) persistence: &'a Option<Arc<dyn PersistenceBackend>>,
    #[cfg(feature = "raft")]
    pub(crate) routed_raft: &'a Option<crate::raft::multi::RoutedRaftHandle>,
}

/// Handle exactly the six result-producing WorkItem lifecycle transitions and
/// the two native control-lease writes (graph-os EG-2), which share the same
/// durable WorkItem MutationBatch kernel. Dispatch has already bound a lease
/// write's tenant to the verified carrier (`route_control_lease_writes`).
///
/// Submission and resource-reservation methods intentionally return `Err` and
/// remain with their distinct admission/authority routes.
pub(crate) async fn try_handle(ctx: HandleContext<'_>, method: Method) -> Result<Response, Method> {
    let method = match method {
        // Enumerated, not `work_item_kernel_writes!()`: this is the dispatch
        // arm `check_contract_method_reachability.py` reads by name.
        method @ (Method::ClaimWorkItem { .. }
        | Method::RenewWorkItemLease { .. }
        | Method::CommitWorkItemResult { .. }
        | Method::CancelWorkItem { .. }
        | Method::DeferWorkItem { .. }
        | Method::RequestWorkItemInput { .. }
        | Method::AnswerWorkItemInput { .. }
        | Method::ExpireWorkItemInput { .. }
        | Method::CasWorkItemMetadata { .. }
        | Method::IssueControlLease { .. }
        | Method::TransitionControlLease { .. }) => method,
        other => return Err(other),
    };
    let method = match method {
        Method::IssueControlLease { mut request } => {
            request = match bind_control_lease_issuer(request, ctx.verified_context) {
                Ok(bound) => bound,
                Err(error) => return Ok(Response::err(ctx.req_id, error)),
            };
            Method::IssueControlLease { request }
        }
        other => other,
    };
    let native_tenant = match &method {
        Method::RequestWorkItemInput { request } => Some(request.tenant.as_str()),
        Method::AnswerWorkItemInput { request } => Some(request.tenant.as_str()),
        Method::ExpireWorkItemInput { request } => Some(request.tenant.as_str()),
        Method::IssueControlLease { request } => Some(request.tenant.as_str()),
        _ => None,
    };
    if native_tenant.is_some_and(|tenant| tenant != ctx.verified_context.tenant()) {
        return Ok(Response::err(
            ctx.req_id,
            "ACCESS_DENIED: native WorkItem tenant does not match verified carrier",
        ));
    }
    if matches!(&method, Method::ClaimWorkItem { request }
        if request.schema_version == crate::epistemic_operations::ClaimWorkItemRequestSchemaVersion::V2)
        && !ctx
            .verified_context
            .allows_exact_scope("workitem:input-answer:read")
    {
        return Ok(Response::err(
            ctx.req_id,
            "ACCESS_DENIED: V2 WorkItem claim requires exact workitem:input-answer:read scope",
        ));
    }
    if matches!(&method, Method::AnswerWorkItemInput { .. })
        && !ctx
            .verified_context
            .allows_exact_scope("workitem:input-answer")
    {
        return Ok(Response::err(
            ctx.req_id,
            "ACCESS_DENIED: pending-input answer requires exact workitem:input-answer scope",
        ));
    }
    if matches!(&method, Method::ExpireWorkItemInput { .. })
        && !ctx
            .verified_context
            .allows_exact_scope("workitem:input-expire")
    {
        return Ok(Response::err(
            ctx.req_id,
            "ACCESS_DENIED: pending-input expiry requires exact workitem:input-expire scope",
        ));
    }

    #[cfg(feature = "raft")]
    let (placement_epoch, placement_fence) = if let Some(routed) = ctx.routed_raft.as_ref() {
        let leader = routed.handle.current_leader().await;
        if leader != Some(routed.handle.node_id) {
            return Ok(Response::stale_route(
                ctx.req_id,
                ctx.graph_name,
                routed.group_id,
                routed.epoch,
                leader,
                "WorkItem transitions require the current placement leader",
            ));
        }
        (routed.epoch, Some(routed.group_id))
    } else {
        (0, None)
    };
    #[cfg(not(feature = "raft"))]
    let (placement_epoch, placement_fence) = (0, None);

    let response =
        match commit_verified_work_item(&ctx, placement_epoch, placement_fence, method).await {
            Ok(result) => Response::ok(ctx.req_id, result),
            Err(error) => Response::err(ctx.req_id, format!("WorkItem mutation failed: {error}")),
        };
    Ok(response)
}

fn bind_control_lease_issuer(
    mut request: eg_types::control_lease::IssueControlLeaseRequest,
    verified: &crate::server::auth::VerifiedRequestContext,
) -> Result<eg_types::control_lease::IssueControlLeaseRequest, String> {
    if request.issuer.is_some() {
        return Err("ACCESS_DENIED: control lease issuer evidence is server-owned".into());
    }
    if request.kind == eg_types::control_lease::HUMAN_WORKER_DELEGATION_KIND {
        if !verified.verified_human_issuer()
            || !verified.allows_exact_scope("workitem:human-issuer")
        {
            return Err("ACCESS_DENIED: human-worker delegation requires an OIDC-verified, undelegated human and exact issuer scope".into());
        }
        request.issuer = Some(eg_types::control_lease::ControlLeaseIssuer {
            principal_ref: verified.principal_persistence_id(),
            kind: eg_types::control_lease::ControlLeaseIssuerKind::Human,
        });
    }
    Ok(request)
}

#[cfg(test)]
mod issuer_tests {
    use super::*;
    use crate::acl::RequestContextClaims;
    use eg_types::control_lease::{
        ControlLeaseIssuer, ControlLeaseIssuerKind, IssueControlLeaseRequest,
        HUMAN_WORKER_DELEGATION_KIND,
    };

    fn issue() -> IssueControlLeaseRequest {
        IssueControlLeaseRequest {
            tenant: "tenant-a".into(),
            lease_id: "approval-1".into(),
            kind: HUMAN_WORKER_DELEGATION_KIND.into(),
            grant: serde_json::Map::from_iter([("origin_kind".into(), "human".into())]),
            issued_at_ms: 1,
            expires_at_ms: 2,
            hard_expires_at_ms: 3,
            idempotency_key: "approval-1".into(),
            issuer: None,
        }
    }

    fn context(kind: Option<&str>, scopes: &[&str], delegated: bool) -> VerifiedRequestContext {
        let principal = "human-a".to_string();
        VerifiedRequestContext::from_verified_claims(
            RequestContextClaims {
                principal: principal.clone(),
                tenant: "tenant-a".into(),
                agent_id: if delegated {
                    "worker-a".into()
                } else {
                    principal.clone()
                },
                delegation: if delegated {
                    vec![principal, "worker-a".into()]
                } else {
                    vec![]
                },
                scopes: scopes.iter().map(|scope| (*scope).into()).collect(),
                ..RequestContextClaims::default()
            },
            "approval-1".into(),
        )
        .with_verified_oidc_kind(kind.map(str::to_string))
    }

    #[test]
    fn human_issuer_requires_verified_oidc_kind_exact_scope_and_no_delegation() {
        let exact = ["workitem:human-issuer"];
        for carrier in [
            context(None, &exact, false),
            context(Some("service"), &exact, false),
            context(Some("human"), &["*"], false),
            context(Some("human"), &exact, true),
        ] {
            assert!(bind_control_lease_issuer(issue(), &carrier).is_err());
        }
        let bound =
            bind_control_lease_issuer(issue(), &context(Some("human"), &exact, false)).unwrap();
        assert_eq!(bound.issuer.unwrap().kind, ControlLeaseIssuerKind::Human);
    }

    #[test]
    fn caller_cannot_supply_issuer_even_when_it_names_the_same_subject() {
        let mut request = issue();
        request.issuer = Some(ControlLeaseIssuer {
            principal_ref:
                "principal:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    .into(),
            kind: ControlLeaseIssuerKind::Human,
        });
        assert!(bind_control_lease_issuer(
            request,
            &context(Some("human"), &["workitem:human-issuer"], false),
        )
        .is_err());
    }
}

/// Commit one native WorkItem method through the durable MutationBatch path under
/// the request's verified idempotency key, attempt nonce and placement fence.
/// Shared by the lifecycle transitions here and by kg-delegate admission.
pub(crate) async fn commit_verified_work_item(
    ctx: &HandleContext<'_>,
    placement_epoch: u64,
    placement_fence: Option<u64>,
    method: Method,
) -> Result<crate::protocol::ResultPayload, String> {
    crate::server::mutation_batch::commit_work_item(
        crate::server::mutation_batch::WorkItemCommitRequest::new(
            ctx.persistence.as_ref(),
            ctx.core,
            crate::server::mutation_batch::CommitOrigin {
                request_id: ctx.req_id,
                principal: ctx.caller,
            },
            Some(ctx.verified_context.idempotency_key()),
            ctx.graph_name,
            placement_epoch,
            method,
        )
        .with_attempt_nonce(ctx.verified_context.attempt_nonce())
        .with_placement_fencing_token(placement_fence),
    )
    .await
}
