//! Verified request-context claim checks: every signed envelope's claims must be
//! well-formed, carry a coherent delegation chain, be issued for this deployment,
//! and (when present) be bound to this node -- all before dispatch or replay lookup.

use std::collections::HashSet;

use super::{
    node_identity, require_node_binding_mode, warn_absent_node_claim_once, NodeBindingMode,
    Request, RequestContextClaims, RequestContextPolicy,
};

fn validate_unique_claims(label: &str, values: &[String]) -> Result<(), String> {
    let mut seen = HashSet::new();
    for value in values {
        if value.trim().is_empty() {
            return Err(format!("request context contains an empty {label}"));
        }
        if !seen.insert(value.as_str()) {
            return Err(format!(
                "request context contains duplicate {label} '{value}'"
            ));
        }
    }
    Ok(())
}

pub(super) fn validate_context_claims(
    req: &Request,
    claims: &RequestContextClaims,
    policy: &RequestContextPolicy,
) -> Result<(), String> {
    validate_claim_fields(claims)?;
    if req
        .agent_id
        .as_deref()
        .is_some_and(|asserted_agent| asserted_agent != claims.agent_id)
    {
        return Err("request agent_id does not match verified context".to_string());
    }
    validate_delegation_chain(claims)?;
    validate_deployment_binding(claims, policy)?;

    // ── ADR-3 / W1.9: node-bound envelopes ──────────────────────────────
    // A present claim is ALWAYS exact-matched against this node's own
    // identity, in every posture -- only an ABSENT claim's handling varies
    // by `EPISTEMIC_GRAPH_REQUIRE_NODE_BINDING`. Checked here (inside the
    // same pre-dispatch claims check `verify_envelope_v2_with` runs BEFORE
    // its nonce/replay lookup) so a captured envelope replayed against a
    // DIFFERENT node fails fast, at zero consensus/replication cost, before
    // ever touching the replay ledger.
    validate_node_claim(claims)
}

/// Every identity field is present, and the role/scope/delegation lists carry no
/// empty or duplicate entries.
fn validate_claim_fields(claims: &RequestContextClaims) -> Result<(), String> {
    for (name, value) in [
        ("principal", claims.principal.as_str()),
        ("tenant", claims.tenant.as_str()),
        ("audience", claims.audience.as_str()),
        ("agent_id", claims.agent_id.as_str()),
        ("policy_version", claims.policy_version.as_str()),
    ] {
        if value.trim().is_empty() {
            return Err(format!("request context {name} must not be empty"));
        }
    }
    validate_unique_claims("role", &claims.roles)?;
    validate_unique_claims("scope", &claims.scopes)?;
    validate_unique_claims("delegation subject", &claims.delegation)
}

/// A principal acting as itself carries no delegation chain; a principal acting
/// through another agent carries a chain of at least two hops running from the
/// principal to that effective agent.
fn validate_delegation_chain(claims: &RequestContextClaims) -> Result<(), String> {
    let chain = &claims.delegation;
    if claims.principal == claims.agent_id {
        if !chain.is_empty() {
            return Err("non-delegated context must have an empty delegation chain".to_string());
        }
        return Ok(());
    }
    let runs_principal_to_agent = chain.len() >= 2
        && chain.first() == Some(&claims.principal)
        && chain.last() == Some(&claims.agent_id);
    if !runs_principal_to_agent {
        return Err("delegation chain must run from principal to effective agent".to_string());
    }
    Ok(())
}

/// The context was issued for this deployment's audience, tenant and active policy.
fn validate_deployment_binding(
    claims: &RequestContextClaims,
    policy: &RequestContextPolicy,
) -> Result<(), String> {
    if claims.audience != policy.expected_audience {
        return Err("request context audience does not match deployment".to_string());
    }
    if claims.tenant != policy.expected_tenant {
        return Err("request context tenant does not match graph tenant".to_string());
    }
    if claims.policy_version != policy.expected_policy_version {
        return Err("request context policy version is not active".to_string());
    }
    Ok(())
}

/// A present node claim must name this node exactly; an absent one is handled per
/// [`require_node_binding_mode`].
fn validate_node_claim(claims: &RequestContextClaims) -> Result<(), String> {
    let Some(claimed) = claims.node.as_deref() else {
        return absent_node_claim(&claims.principal);
    };
    if claimed.trim().is_empty() {
        return Err("request context node claim must not be empty when present".to_string());
    }
    if claimed != node_identity() {
        return Err(format!(
            "NODE_MISMATCH: request context is bound to node '{claimed}', \
             which does not match this node's identity"
        ));
    }
    Ok(())
}

/// An envelope without a node claim: accepted silently (`off`), accepted with a
/// once-per-principal warning (`warn`), or refused (`on`).
fn absent_node_claim(principal: &str) -> Result<(), String> {
    match require_node_binding_mode() {
        NodeBindingMode::Off => Ok(()),
        NodeBindingMode::Warn => {
            warn_absent_node_claim_once(principal);
            Ok(())
        }
        NodeBindingMode::On => Err("NODE_MISMATCH: request context is missing the required \
                     node-binding claim (EPISTEMIC_GRAPH_REQUIRE_NODE_BINDING=on)"
            .to_string()),
    }
}
