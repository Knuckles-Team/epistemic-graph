//! Just-in-time RBAC elevation dispatch (EH-404).
//!
//! Who acted is never read from the request body. [`stamp_elevation_actor`]
//! overwrites `Method::RbacElevation.actor` from the verified request context
//! at the request boundary, before consensus routing, so the replicated
//! command carries the same actor the leader authorized; a replica applying
//! it (whose reconstructed context holds only one-way fingerprints) uses the
//! stamped actor and nothing else. The handler re-derives the actor from the
//! verified context on every non-replicated call, so a path that skipped the
//! stamp still cannot act as someone else.

#[cfg(feature = "security")]
use eg_types::rbac_elevation::RbacElevationOp;
use eg_types::rbac_elevation::{
    ElevationActor, ElevationDelegation, ElevationStanding, APPROVE_ELEVATION_SCOPE,
};
#[cfg(feature = "security")]
use eg_types::result_contract::security::{
    RbacElevationApprove, RbacElevationList, RbacElevationRequest, RbacElevationRevoke,
};

use super::VerifiedRequestContext;
use crate::protocol::Method;
#[cfg(feature = "security")]
use crate::protocol::{Response, ResultPayload};

/// Whose actor an elevation op carries: one re-derived from this request's
/// verified context, or the one a leader stamped into a replicated command.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ElevationStampAuthority {
    External,
    Replicated,
}

impl ElevationStampAuthority {
    pub(crate) fn of(state_machine_authorized: bool) -> Self {
        if state_machine_authorized {
            Self::Replicated
        } else {
            Self::External
        }
    }
}

/// Refusal for a replicated elevation that reached apply without its actor.
#[cfg(feature = "security")]
const UNSTAMPED: &str =
    "ELEVATION_ACTOR_UNSTAMPED: a replicated elevation must carry its stamped actor";

/// The actor of a verified external request. Approval standing needs the
/// EXACT approval scope: `*`, `rbac:*` and `kg:admin` do not stand in for it.
pub(crate) fn elevation_actor(context: &VerifiedRequestContext) -> ElevationActor {
    let claims = context.claims();
    let delegation = if claims.delegation.is_empty() {
        ElevationDelegation::Direct
    } else {
        ElevationDelegation::Delegated
    };
    let standing = if claims
        .scopes
        .iter()
        .any(|scope| scope == APPROVE_ELEVATION_SCOPE)
    {
        ElevationStanding::Approver
    } else {
        ElevationStanding::Requester
    };
    let identities = std::iter::once(claims.principal.as_str())
        .chain(claims.delegation.iter().map(String::as_str));
    ElevationActor::from_identities(&claims.agent_id, identities, delegation, standing)
}

/// Overwrite the actor of an external elevation request from its verified
/// context. A replicated apply keeps the actor its leader stamped.
pub(crate) fn stamp_elevation_actor(
    method: &mut Method,
    context: &VerifiedRequestContext,
    authority: ElevationStampAuthority,
) {
    if authority == ElevationStampAuthority::Replicated {
        return;
    }
    if let Method::RbacElevation { actor, .. } = method {
        *actor = Some(elevation_actor(context));
    }
}

/// Serve one elevation op against the RBAC policy image.
#[cfg(feature = "security")]
pub(crate) async fn dispatch_rbac_elevation(
    state: &std::sync::Arc<tokio::sync::RwLock<super::ServerState>>,
    req_id: u64,
    context: &VerifiedRequestContext,
    authority: ElevationStampAuthority,
    (op, stamped): (RbacElevationOp, Option<ElevationActor>),
) -> Response {
    let actor = if authority == ElevationStampAuthority::Replicated {
        match stamped {
            Some(actor) => actor,
            None => return Response::err(req_id, UNSTAMPED),
        }
    } else {
        elevation_actor(context)
    };
    let now_ms = super::authoritative_now_ms();
    let mut guard = super::timed_write(state).await;
    let isolation = &mut guard.isolation;
    let outcome = match &op {
        RbacElevationOp::Request { request } => isolation
            .try_request_elevation(&actor, request, now_ms)
            .map(ResultPayload::of::<RbacElevationRequest>),
        RbacElevationOp::Approve { request } => isolation
            .try_approve_elevation(&actor, request, now_ms)
            .map(ResultPayload::of::<RbacElevationApprove>),
        RbacElevationOp::Revoke { request } => isolation
            .try_revoke_elevation(&actor, request, now_ms)
            .map(ResultPayload::of::<RbacElevationRevoke>),
        RbacElevationOp::List => Ok(ResultPayload::of::<RbacElevationList>(
            isolation.elevations_visible_to(&actor, now_ms),
        )),
    };
    drop(guard);
    respond(req_id, &op, &actor, outcome)
}

#[cfg(feature = "security")]
fn respond(
    req_id: u64,
    op: &RbacElevationOp,
    actor: &ElevationActor,
    outcome: Result<Result<ResultPayload, String>, crate::isolation::ElevationError>,
) -> Response {
    let elevation_id = op.elevation_id().unwrap_or("");
    match outcome {
        Ok(Ok(payload)) => {
            if op.is_mutation() {
                tracing::info!(
                    target: "epistemic_graph::rbac::elevation::audit",
                    op = op.name(),
                    elevation_id,
                    actor = %actor.parties_digest(),
                    "rbac elevation transition committed"
                );
            }
            Response::ok(req_id, payload)
        }
        Ok(Err(error)) => Response::err(req_id, error),
        Err(error) => {
            tracing::warn!(
                target: "epistemic_graph::rbac::elevation::audit",
                op = op.name(),
                elevation_id,
                actor = %actor.parties_digest(),
                %error,
                "rbac elevation refused"
            );
            Response::err(req_id, error.to_string())
        }
    }
}

#[cfg(all(test, feature = "security"))]
mod tests;
