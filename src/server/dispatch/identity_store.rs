//! `Method::Identity`: dispatch for the engine-owned identity store.
//!
//! Two steps, both on the request path:
//!
//! 1. [`stamp_identity`] runs at the request boundary, after the ledger's
//!    scope check and before consensus routing. It stamps the actor from the
//!    verified context (the body's stamp is overwritten), checks the op's
//!    EXACT `identity:*` authority before any expensive work, then derives
//!    every hash, verdict and sealed value and clears every plaintext secret.
//!    A replicated apply keeps the stamp its leader derived.
//! 2. [`dispatch_identity`] applies the stamped op to the store and its RBAC
//!    projection in one durable write.

use std::sync::Arc;

use eg_types::identity::{IdentityOp, IdentityStamp};
use eg_types::result_contract::security::Identity as IdentityResult;
use tokio::sync::RwLock;

use super::elevation::ElevationStampAuthority;
use super::{ServerState, VerifiedRequestContext};
use crate::protocol::{Method, Response, ResultPayload};
use crate::server::identity_view::identity_actor;

mod exposure;
#[cfg(test)]
mod pause;
mod secrets;
mod stamp;

/// Refusal for an identity op that reached apply without its stamp.
const UNSTAMPED: &str = "IDENTITY_UNSTAMPED: an identity op must carry its boundary-derived stamp";
/// Refusal for a stamp whose actor is not the verified caller's.
const FORGED: &str = "IDENTITY_FORGED_STAMP: the stamp does not name the verified caller";

/// Stamp an external identity op; a replicated one keeps its stamp.
pub(crate) async fn stamp_identity(
    state: &Arc<RwLock<ServerState>>,
    method: &mut Method,
    context: &VerifiedRequestContext,
    authority: ElevationStampAuthority,
) -> Result<(), String> {
    if authority == ElevationStampAuthority::Replicated {
        return Ok(());
    }
    let Method::Identity { op, stamp } = method else {
        return Ok(());
    };
    let mut derived = IdentityStamp::for_actor(identity_actor(context));
    let (store, service_secret) = {
        let guard = super::timed_read(state).await;
        (
            guard.isolation.rbac().identity_store().clone(),
            guard.auth_secret.clone(),
        )
    };
    store
        .authorize(op, &derived, &eg_capabilities::scopes::ScopeRegistry)
        .map_err(|refusal| refusal.to_string())?;
    let mut owned = op.clone();
    let now_ms = super::authoritative_now_ms();
    #[cfg(test)]
    let pause = pause::take(state);
    let derived = tokio::task::spawn_blocking(move || {
        let env = stamp::StampEnv {
            store: &store,
            service_secret: &service_secret,
            now_ms,
            engine_loopback: exposure::listeners_loopback(),
        };
        let outcome = stamp::derive(&mut owned, &mut derived, &env).map(|()| (owned, derived));
        // The derivation read `store`, a snapshot taken before this point and
        // outside the engine's write lock. A test holds it here to change the
        // live store before the verdict is applied.
        #[cfg(test)]
        pause::hold(pause);
        outcome
    })
    .await
    .map_err(|error| format!("IDENTITY_STAMP_FAILED: {error}"))?
    .map_err(|refusal| refusal.to_string())?;
    (*op, *stamp) = (derived.0, Some(derived.1));
    Ok(())
}

/// Apply one stamped identity op.
pub(crate) async fn dispatch_identity(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    context: &VerifiedRequestContext,
    authority: ElevationStampAuthority,
    (op, stamp): (IdentityOp, Option<IdentityStamp>),
) -> Response {
    let Some(stamp) = stamp else {
        return Response::err(req_id, UNSTAMPED);
    };
    if authority == ElevationStampAuthority::External && stamp.actor != identity_actor(context) {
        return Response::err(req_id, FORGED);
    }
    let now_ms = super::authoritative_now_ms();
    let mut guard = super::timed_write(state).await;
    let outcome = guard.isolation.try_apply_identity(
        &op,
        &stamp,
        now_ms,
        &eg_capabilities::scopes::ScopeRegistry,
    );
    guard.publish_identity_view();
    drop(guard);
    respond(req_id, &op, &stamp, outcome)
}

fn respond(
    req_id: u64,
    op: &IdentityOp,
    stamp: &IdentityStamp,
    outcome: Result<eg_types::identity::IdentityReply, crate::isolation::IdentityStoreError>,
) -> Response {
    match outcome {
        Ok(reply) => {
            if op.is_mutation() {
                tracing::info!(
                    target: "epistemic_graph::identity::audit",
                    op = op.name(),
                    actor = %stamp.actor.principal_id,
                    "identity op committed"
                );
            }
            match ResultPayload::of::<IdentityResult>(reply) {
                Ok(payload) => Response::ok(req_id, payload),
                Err(error) => Response::err(req_id, error),
            }
        }
        Err(error) => {
            tracing::warn!(
                target: "epistemic_graph::identity::audit",
                op = op.name(),
                actor = %stamp.actor.principal_id,
                %error,
                "identity op refused"
            );
            Response::err(req_id, error.to_string())
        }
    }
}

#[cfg(test)]
mod tests;
