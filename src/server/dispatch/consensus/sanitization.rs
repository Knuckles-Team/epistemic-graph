use super::*;

/// Transaction control, identity registration and multisig mutation: the three
/// proposals whose payload must be re-signed/re-anchored against the caller's
/// ORIGINAL request graph before routing rewrites `RaftRequest.graph_name`.
/// Anything else is handed back untouched.
#[cfg(feature = "raft")]
fn sanitize_authored_proposal(
    request_graph: &str,
    verified_context: &VerifiedRequestContext,
    method: Method,
) -> Result<Method, String> {
    match method {
        // Transaction control is ordered by the placement group, not by the
        // transaction's data graph. Freeze the caller's original request graph
        // into the method before routing changes `RaftRequest.graph_name`, or a
        // body-less BeginTxn would accidentally target the placement graph.
        Method::BeginTxn { graph, isolation } => Ok(Method::BeginTxn {
            graph: Some(graph.unwrap_or_else(|| request_graph.to_string())),
            isolation,
        }),
        Method::RegisterIdentity {
            agent_id,
            role,
            teams,
            signature,
            roles,
        } => {
            verify_register_identity_signature(
                verified_context,
                request_graph,
                &agent_id,
                &role,
                &teams,
                &roles,
                &signature,
            )?;
            Ok(Method::RegisterIdentity {
                agent_id,
                role,
                teams,
                signature,
                roles,
            })
        }
        Method::ApplyMultisigMutation {
            signatures,
            threshold,
            mutation_type,
            query,
        } => {
            verify_multisig_mutation_signatures(
                verified_context,
                request_graph,
                &signatures,
                threshold,
                &mutation_type,
                &query,
            )?;
            Ok(Method::ApplyMultisigMutation {
                signatures,
                threshold,
                mutation_type,
                query,
            })
        }
        other => Ok(other),
    }
}

/// The tenant in a replicated resource body is a correlation, not an authority
/// claim: it must be the verified tenant unless the caller is an admin. The
/// reservation and host surfaces report distinct refusals.
#[cfg(feature = "raft")]
fn resource_tenant_denial(
    method: &Method,
    verified_context: &VerifiedRequestContext,
    authority: &CarrierAuthority,
) -> Option<&'static str> {
    let (tenant_ref, denial) = match method {
        Method::ReserveWorkItemResources { request }
        | Method::ReleaseWorkItemResources { request }
        | Method::ReclaimWorkItemResources { request } => (
            request.tenant_ref.as_str(),
            "ACCESS_DENIED: replicated resource tenant is not the verified tenant",
        ),
        Method::UpdateResourceHost { request } => (
            request.tenant_ref.as_str(),
            "ACCESS_DENIED: replicated resource host tenant is not the verified tenant",
        ),
        _ => return None,
    };
    (tenant_ref != verified_context.tenant() && !authority.is_admin()).then_some(denial)
}

#[cfg(feature = "raft")]
fn sanitize_resource_proposal(
    verified_context: &VerifiedRequestContext,
    authority: &CarrierAuthority,
    method: Method,
) -> Result<Method, String> {
    match resource_tenant_denial(&method, verified_context, authority) {
        Some(denial) => Err(denial.to_string()),
        None => Ok(method),
    }
}

/// Channel/messaging proposals name their own actor. The caller's claimed actor
/// must be the caller, and the REPLICATED copy carries the actor scope rather
/// than the display agent id, so replay is stable across identity renames.
#[cfg(feature = "raft")]
fn sanitize_channel_proposal(
    authority: &CarrierAuthority,
    method: Method,
) -> Result<Method, String> {
    match method {
        Method::CreateChannel {
            channel_id,
            channel_type,
            creator,
            initial_members,
        } => {
            if creator != authority.agent_id() {
                return Err("ACCESS_DENIED: channel creator must be caller".to_string());
            }
            Ok(Method::CreateChannel {
                channel_id,
                channel_type,
                creator: authority.actor_scope().to_string(),
                initial_members: initial_members
                    .into_iter()
                    .map(|member| crate::server::mutation_batch::principal_fingerprint(&member))
                    .collect::<Result<Vec<_>, _>>()?,
            })
        }
        Method::JoinChannel {
            channel_id,
            agent_id,
        } => {
            if agent_id != authority.agent_id() {
                return Err("ACCESS_DENIED: channel join actor must be caller".to_string());
            }
            Ok(Method::JoinChannel {
                channel_id,
                agent_id: authority.actor_scope().to_string(),
            })
        }
        Method::LeaveChannel {
            channel_id,
            agent_id,
        } => {
            if agent_id != authority.agent_id() {
                return Err("ACCESS_DENIED: channel leave actor must be caller".to_string());
            }
            Ok(Method::LeaveChannel {
                channel_id,
                agent_id: authority.actor_scope().to_string(),
            })
        }
        Method::SendMessage {
            channel_id,
            sender,
            payload,
        } => {
            if sender != authority.agent_id() {
                return Err("ACCESS_DENIED: message sender must be caller".to_string());
            }
            Ok(Method::SendMessage {
                channel_id,
                sender: authority.actor_scope().to_string(),
                payload,
            })
        }
        other => Ok(other),
    }
}

#[cfg(feature = "raft")]
pub(super) fn sanitize_native_proposal(
    request_graph: &str,
    verified_context: &VerifiedRequestContext,
    authority: &CarrierAuthority,
    method: Method,
) -> Result<Method, String> {
    if capability_authority_unavailable(&method) {
        return Err(crate::redb_store::work_item_capability::AUTHORITY_UNAVAILABLE.to_string());
    }
    // The three groups own disjoint `Method` variants, so each hands an
    // unrecognised method straight through and the chain order is immaterial.
    let method = sanitize_authored_proposal(request_graph, verified_context, method)?;
    let method = sanitize_resource_proposal(verified_context, authority, method)?;
    sanitize_channel_proposal(authority, method)
}
