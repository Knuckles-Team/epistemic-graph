//! Public resolution authority is composed at the live store boundary, never
//! from a broker's arbitrary principal lookup. No delegation authority is
//! implemented here: a chain or broker scope cannot establish a subject login.

use eg_types::acl::RequestContextClaims;
use eg_types::identity::{
    IdentityOp, IdentityRefusal, IdentityReply, IdentityStamp, IdentityStore, PrincipalResolution,
    SessionOp, TokenOp, UserOp,
};

use super::{ElevationStampAuthority, ServerState, VerifiedRequestContext};
use crate::server::auth::{request_context_policy, RequestContextPolicy};
use crate::server::identity_view::identity_actor;

// Use the existing closed error vocabulary: Response::err must preserve these
// as classified refusals, not collapse a new undeclared prefix to INTERNAL.
const NO_PROVENANCE: IdentityRefusal = IdentityRefusal::NotAuthorized;
const NO_AUTHORITY: IdentityRefusal = IdentityRefusal::NotAuthorized;

/// Called with the server write lock held. A successful credential operation
/// supplies its own subject and already-narrowed scopes; callers cannot supply
/// a successful reply or a transferable `credential_verified` flag.
pub(super) fn apply_and_compose(
    state: &mut ServerState,
    op: &IdentityOp,
    stamp: &IdentityStamp,
    context: &VerifiedRequestContext,
    authority: ElevationStampAuthority,
    now_ms: u64,
) -> Result<IdentityReply<RequestContextClaims>, String> {
    let issuing = is_credential_resolution(op);
    if matches!(op, IdentityOp::User(UserOp::Resolve { .. })) {
        return Err(NO_PROVENANCE.to_string());
    }
    let policy = if issuing {
        let policy = request_context_policy()
            .map_err(|_| IdentityRefusal::PreconditionFailed.to_string())?;
        authorize_resolution(
            state.isolation.rbac().identity_store(),
            op,
            stamp,
            context,
            authority,
            policy,
        )?;
        Some(policy)
    } else {
        None
    };
    let reply = state
        .isolation
        .try_apply_identity(op, stamp, now_ms, &eg_capabilities::scopes::ScopeRegistry)
        .map_err(|error| error.to_string())?;
    reply.try_with_request_context(|resolution| {
        // Only a SUCCESSFUL Session::Resolve / Token::VerifyApiKey above can
        // reach this producer. The lock remains held throughout composition.
        let policy = policy.ok_or_else(|| NO_PROVENANCE.to_string())?;
        if matches!(op, IdentityOp::Session(SessionOp::Resolve { .. })) {
            let hash = stamp.token_hash(0).map_err(|error| error.to_string())?;
            if state
                .isolation
                .rbac()
                .identity_store()
                .session_principal(hash, now_ms)
                != Some(resolution.principal_id.as_str())
            {
                return Err(NO_PROVENANCE.to_string());
            }
        }
        // resolve_session may already have durably touched a pending session.
        // Refusing issuance does not revoke it or consume its MFA ceremony.
        compose_resolution(resolution, policy)
    })
}

fn is_credential_resolution(op: &IdentityOp) -> bool {
    matches!(
        op,
        IdentityOp::Session(SessionOp::Resolve { .. })
            | IdentityOp::Token(TokenOp::VerifyApiKey { .. })
    )
}

fn authorize_resolution(
    store: &IdentityStore,
    op: &IdentityOp,
    stamp: &IdentityStamp,
    context: &VerifiedRequestContext,
    authority: ElevationStampAuthority,
    policy: &RequestContextPolicy,
) -> Result<(), String> {
    let claims = context.claims();
    // Replicated contexts retain fingerprints, not live credential provenance.
    // Unmanaged broker tokens have no independent current authority in this
    // store. Keep them unavailable rather than treating token claims as a grant.
    if !is_credential_resolution(op)
        || authority != ElevationStampAuthority::External
        || stamp.actor != identity_actor(context)
        || stamp.actor.delegated
        || claims.principal != claims.agent_id
        || !claims.delegation.is_empty()
        || !store.manages(&stamp.actor.principal_id)
    {
        return Err(NO_AUTHORITY.to_string());
    }
    validate_policy(claims, policy)?;
    store
        .authorize(op, stamp, &eg_capabilities::scopes::ScopeRegistry)
        .map_err(|error| error.to_string())
}

fn validate_policy(
    claims: &RequestContextClaims,
    policy: &RequestContextPolicy,
) -> Result<(), String> {
    for (actual, expected) in [
        (&claims.tenant, &policy.expected_tenant),
        (&claims.audience, &policy.expected_audience),
        (&claims.policy_version, &policy.expected_policy_version),
    ] {
        if expected.trim().is_empty() || actual != expected {
            return Err(IdentityRefusal::PreconditionFailed.to_string());
        }
    }
    Ok(())
}

fn compose_resolution(
    resolution: PrincipalResolution,
    policy: &RequestContextPolicy,
) -> Result<PrincipalResolution<RequestContextClaims>, String> {
    if !resolution.status.is_active()
        || resolution.session_mfa_pending
        || (resolution.mfa_required && !resolution.mfa_enrolled)
        || resolution.principal_id.trim().is_empty()
        || resolution
            .roles
            .iter()
            .chain(resolution.scopes.iter())
            .any(|v| v.trim().is_empty())
    {
        return Err(IdentityRefusal::NotAuthorized.to_string());
    }
    let claims = RequestContextClaims {
        principal: resolution.principal_id.clone(),
        tenant: policy.expected_tenant.clone(),
        audience: policy.expected_audience.clone(),
        agent_id: resolution.principal_id.clone(),
        roles: resolution.roles.iter().cloned().collect(),
        // Preserve the successful operation's scope intersection/class filter.
        scopes: resolution.scopes.iter().cloned().collect(),
        policy_version: policy.expected_policy_version.clone(),
        delegation: Vec::new(),
        node: None,
        priority: None,
    };
    validate_policy(&claims, policy)?;
    Ok(resolution.with_request_context(claims))
}

#[cfg(test)]
mod tests;
