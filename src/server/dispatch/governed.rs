//! Governed-change dispatch (EH-560).
//!
//! As with elevations, who acted is never read from the body:
//! [`stamp_governed_actor`] overwrites `Method::GovernedChange.actor` from the
//! verified context at the request boundary, before consensus routing; a
//! replicated apply uses its leader's stamp and nothing else.

use eg_types::governed_change::GovernedActor;
#[cfg(feature = "security")]
use eg_types::governed_change::GovernedChangeOp;
#[cfg(feature = "security")]
use eg_types::result_contract::security::{
    GovernedApprove, GovernedGet, GovernedList, GovernedPropose, GovernedRevoke,
};

use super::elevation::ElevationStampAuthority;
use super::VerifiedRequestContext;
use crate::protocol::Method;
#[cfg(feature = "security")]
use crate::protocol::{Response, ResultPayload};

/// The actor of a verified request: its identities (hashed), whether it is
/// delegated, and its EXACT `governance:*` scopes.
pub(crate) fn governed_actor(context: &VerifiedRequestContext) -> GovernedActor {
    let claims = context.claims();
    let scopes = claims
        .scopes
        .iter()
        .filter(|scope| scope.starts_with("governance:") && !scope.ends_with(":*"))
        .cloned()
        .collect();
    let identities = std::iter::once(claims.principal.as_str())
        .chain(claims.delegation.iter().map(String::as_str));
    GovernedActor::from_identities(
        &claims.agent_id,
        identities,
        !claims.delegation.is_empty(),
        scopes,
    )
}

/// Overwrite an external governed op's actor from its verified context.
pub(crate) fn stamp_governed_actor(
    method: &mut Method,
    context: &VerifiedRequestContext,
    authority: ElevationStampAuthority,
) {
    if authority == ElevationStampAuthority::Replicated {
        return;
    }
    if let Method::GovernedChange { actor, .. } = method {
        *actor = Some(governed_actor(context));
    }
}

/// Serve one governed op against the RBAC policy image.
#[cfg(feature = "security")]
pub(crate) async fn dispatch_governed_change(
    state: &std::sync::Arc<tokio::sync::RwLock<super::ServerState>>,
    req_id: u64,
    context: &VerifiedRequestContext,
    authority: ElevationStampAuthority,
    (op, stamped): (GovernedChangeOp, Option<GovernedActor>),
) -> Response {
    let actor = match (authority, stamped) {
        (ElevationStampAuthority::External, _) => governed_actor(context),
        (ElevationStampAuthority::Replicated, Some(actor)) => actor,
        (ElevationStampAuthority::Replicated, None) => {
            return Response::err(
                req_id,
                "GOVERNED_ACTOR_UNSTAMPED: a replicated governed op must carry its actor",
            )
        }
    };
    let now_ms = super::authoritative_now_ms();
    let mut guard = super::timed_write(state).await;
    let isolation = &mut guard.isolation;
    let outcome = match &op {
        GovernedChangeOp::Propose { request } => isolation
            .try_propose_change(&actor, request, now_ms)
            .map(ResultPayload::of::<GovernedPropose>),
        GovernedChangeOp::Approve { request } => isolation
            .try_approve_change(&actor, request, now_ms)
            .map(ResultPayload::of::<GovernedApprove>),
        GovernedChangeOp::Revoke { change_id } => isolation
            .try_revoke_change(&actor, change_id, now_ms)
            .map(ResultPayload::of::<GovernedRevoke>),
        GovernedChangeOp::Get { change_id } => Ok(ResultPayload::of::<GovernedGet>(
            isolation.rbac().governed().get(change_id, now_ms),
        )),
        GovernedChangeOp::List => Ok(ResultPayload::of::<GovernedList>(
            isolation.rbac().governed().list(now_ms),
        )),
    };
    drop(guard);
    match outcome {
        Ok(Ok(payload)) => {
            if op.is_mutation() {
                tracing::info!(
                    target: "epistemic_graph::governance::audit",
                    op = op.name(),
                    actor = %actor.agent_id,
                    "governed change transition committed"
                );
            }
            Response::ok(req_id, payload)
        }
        Ok(Err(error)) => Response::err(req_id, error),
        Err(error) => Response::err(req_id, error.to_string()),
    }
}

#[cfg(all(test, feature = "security"))]
mod tests;
