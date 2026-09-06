//! Shared served `KnowledgeBatch` result plane.
//!
//! `dispatch_graph_op` calls this handler only after graph ACL, lazy-materialization,
//! and authoritative placement checks have succeeded.  Each family then reuses its
//! existing RLS-aware execution path and is projected through exactly one of the
//! seven `eg_plan::result_stream` adapters.  The wire returns one bounded Arrow
//! batch per request; the cursor binds authority, placement, query, complete source
//! snapshot, schema, and batch size, so a changed result or policy fails closed.

use std::sync::Arc;

use eg_modality::{OpaqueRef, ProtocolError};
use eg_plan::{
    cross_modal_result_stream, graph_result_stream, job_result_stream, rdf_result_stream,
    sql_result_stream, time_series_result_stream, vector_result_stream, KnowledgeBatch,
    KnowledgeBatchRow, KnowledgeStreamContext, KnowledgeStreamCursor, ServedResultFamily,
};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use tokio::sync::RwLock;

use crate::graph::GraphCore;
#[cfg(test)]
use crate::knowledge_stream::KnowledgeStreamProjection;
use crate::knowledge_stream::{
    KnowledgeResultFamily, KnowledgeStreamBatchV1, KnowledgeStreamCursorV1, KnowledgeStreamQuery,
    KnowledgeStreamRequestV1, KNOWLEDGE_STREAM_SCHEMA_VERSION,
};
use crate::protocol::{Method, Response, ResultPayload};
use eg_types::acl::RequestContextClaims;

#[cfg(feature = "security")]
use eg_core::rbac_persist::RbacPolicyStore;

use super::super::access::{CarrierAuthority, GraphReadAuthority};
use super::super::state::ServerState;

const SCORE_COLUMN: &str = "score";
const MAX_KNOWLEDGE_RESULT_BYTES: usize = 64 * 1024 * 1024;
const MAX_KNOWLEDGE_RESULT_ITEMS: usize = 1_000_000;
type HmacSha256 = Hmac<Sha256>;

/// Privacy-safe authority derived only after the request-context MAC, tenant,
/// audience, policy version, scopes, roles, and delegation have been verified.
/// Raw claims never enter the stream cursor or a result row.
///
/// A KnowledgeStream authority is usable for serving only when it carries the
/// opaque [`crate::isolation::PolicyDecisionLease`] minted by the durable RBAC
/// authority. Claims-derived references are cursor material only and are never
/// accepted as authorization proof by [`try_handle`].
#[derive(Clone)]
pub(crate) struct KnowledgeStreamAuthority {
    tenant_ref: OpaqueRef,
    access_policy_ref: OpaqueRef,
    reference_key: [u8; 32],
    #[cfg(feature = "security")]
    policy_lease: Option<Arc<crate::isolation::PolicyDecisionLease>>,
    #[cfg(feature = "security")]
    policy_store: Option<Arc<dyn RbacPolicyStore>>,
    #[cfg(feature = "security")]
    originating_actor_scope: Option<String>,
}

impl KnowledgeStreamAuthority {
    pub(crate) fn from_verified(
        server_secret: &str,
        claims: &RequestContextClaims,
    ) -> Result<Self, String> {
        if server_secret.is_empty()
            || claims.tenant.is_empty()
            || claims.audience.is_empty()
            || claims.policy_version.is_empty()
            || claims.principal.is_empty()
            || claims.agent_id.is_empty()
        {
            return Err("verified KnowledgeStream authority is incomplete".to_string());
        }
        let tenant_ref = keyed_ref(
            server_secret,
            "tenant",
            &[("tenant", claims.tenant.as_str())],
        )?;

        let mut roles = claims.roles.iter().map(String::as_str).collect::<Vec<_>>();
        roles.sort_unstable();
        let mut scopes = claims.scopes.iter().map(String::as_str).collect::<Vec<_>>();
        scopes.sort_unstable();
        let mut policy_values = vec![
            ("tenant", claims.tenant.as_str()),
            ("audience", claims.audience.as_str()),
            ("policy", claims.policy_version.as_str()),
            ("principal", claims.principal.as_str()),
            ("agent", claims.agent_id.as_str()),
        ];
        policy_values.extend(roles.into_iter().map(|value| ("role", value)));
        policy_values.extend(scopes.into_iter().map(|value| ("scope", value)));
        policy_values.extend(
            claims
                .delegation
                .iter()
                .map(String::as_str)
                .map(|value| ("delegation", value)),
        );
        let access_policy_ref = keyed_ref(server_secret, "access-policy", &policy_values)?;
        let reference_key = keyed_bytes(server_secret, "stream-reference-key", &[])?;

        Ok(Self {
            tenant_ref,
            access_policy_ref,
            reference_key,
            #[cfg(feature = "security")]
            policy_lease: None,
            #[cfg(feature = "security")]
            policy_store: None,
            #[cfg(feature = "security")]
            originating_actor_scope: None,
        })
    }

    /// Bind the stream to one exact durable graph-policy image.
    ///
    /// `PolicyDecisionLease` is deliberately non-constructible and non-Clone;
    /// storing it behind an `Arc` keeps the existing authority routing shape
    /// cloneable without turning a wire cursor into a durable permission.  The
    /// policy reference and reference-key domain are re-derived from the lease,
    /// never from the signed `policy_version` claim.
    #[cfg(feature = "security")]
    pub(crate) fn from_verified_with_lease(
        server_secret: &str,
        claims: &RequestContextClaims,
        graph_name: &str,
        carrier: &CarrierAuthority,
        lease: Arc<crate::isolation::PolicyDecisionLease>,
        policy_store: Arc<dyn RbacPolicyStore>,
    ) -> Result<Self, String> {
        let mut authority = Self::from_verified(server_secret, claims)?;
        let actor_scope = carrier.actor_scope();
        if !lease_matches_verified_context(lease.as_ref(), claims, graph_name, carrier) {
            return Err("KnowledgeStream policy decision lease binding mismatch".to_string());
        }
        lease
            .policy_snapshot()
            .validate()
            .map_err(|_| "KnowledgeStream policy decision lease identity is invalid".to_string())?;
        lease
            .validate_before(policy_store.as_ref())
            .map_err(|_| "KnowledgeStream policy decision lease is stale".to_string())?;

        let originating_scope_ref = keyed_ref(
            server_secret,
            "policy-originating-principal",
            &[
                ("principal", lease.originating_principal()),
                ("actor-scope", actor_scope),
            ],
        )?;
        let effective_actor_ref = keyed_ref(
            server_secret,
            "policy-effective-actor",
            &[("agent", lease.effective_actor())],
        )?;
        let binding = policy_decision_binding(
            claims,
            lease.as_ref(),
            &originating_scope_ref,
            &effective_actor_ref,
        );
        let binding_refs = binding
            .iter()
            .map(|(field, value)| (field.as_str(), value.as_str()))
            .collect::<Vec<_>>();

        authority.tenant_ref = keyed_ref(server_secret, "tenant", &[("tenant", lease.tenant())])?;
        authority.access_policy_ref = keyed_ref(server_secret, "access-policy", &binding_refs)?;
        authority.reference_key =
            keyed_bytes(server_secret, "stream-reference-key", &binding_refs)?;
        authority.policy_lease = Some(lease);
        authority.policy_store = Some(policy_store);
        authority.originating_actor_scope = Some(actor_scope.to_string());
        Ok(authority)
    }

    /// Borrow the exact durable lease captured for this request. Callers pass
    /// this handle to policy-aware delegated readers; they must not mint,
    /// clone, or reconstruct a second graph capability.
    #[cfg(feature = "security")]
    pub(super) fn policy_lease(&self) -> Option<&Arc<crate::isolation::PolicyDecisionLease>> {
        self.policy_lease.as_ref()
    }

    /// Filter an owned graph snapshot with the same durable lease that binds
    /// this stream.  Graph/vector readers must not substitute a cloned
    /// `IsolationLayer` or a separately captured row-policy image.
    pub(super) fn filter_view(&self, view: &mut crate::graph::GraphView) -> Result<(), String> {
        #[cfg(feature = "security")]
        {
            let lease = self.policy_lease.as_ref().ok_or_else(|| {
                "KnowledgeStream requires a durable policy decision lease".to_string()
            })?;
            let store = self
                .policy_store
                .as_ref()
                .ok_or_else(|| "KnowledgeStream policy authority is unavailable".to_string())?;
            return lease
                .filter_view(store.as_ref(), view)
                .map_err(|_| "KnowledgeStream policy decision lease is stale".to_string());
        }
        #[cfg(not(feature = "security"))]
        {
            let _ = view;
            Ok(())
        }
    }

    /// Validate the durable lease's originating-principal, effective-actor,
    /// tenant, and resource binding.
    ///
    /// This is intentionally a closed check with a generic error.  Policy
    /// versions/digests are never included in a response error or log field
    /// sourced from a caller-controlled cursor.
    pub(super) fn validate_request_binding(
        &self,
        graph_name: &str,
        carrier: &CarrierAuthority,
    ) -> Result<(), String> {
        #[cfg(feature = "security")]
        {
            let lease = self.policy_lease.as_ref().ok_or_else(|| {
                "KnowledgeStream requires a durable policy decision lease".to_string()
            })?;
            if !lease_matches_request(
                lease,
                graph_name,
                carrier.agent_id(),
                self.originating_actor_scope.as_deref(),
                carrier.actor_scope(),
            ) {
                return Err("KnowledgeStream policy decision lease binding mismatch".to_string());
            }
            return Ok(());
        }
        #[cfg(not(feature = "security"))]
        {
            let _ = (graph_name, carrier);
            Err("KnowledgeStream requires the security policy lease feature".to_string())
        }
    }

    /// Revalidate the durable policy image immediately before using a page.
    pub(super) fn validate_before(&self) -> Result<(), String> {
        self.validate_lease(true)
    }

    /// Revalidate the durable policy image immediately after producing a page.
    pub(super) fn validate_after(&self) -> Result<(), String> {
        self.validate_lease(false)
    }

    fn validate_lease(&self, before: bool) -> Result<(), String> {
        #[cfg(feature = "security")]
        {
            let lease = self.policy_lease.as_ref().ok_or_else(|| {
                "KnowledgeStream requires a durable policy decision lease".to_string()
            })?;
            let store = self
                .policy_store
                .as_ref()
                .ok_or_else(|| "KnowledgeStream policy authority is unavailable".to_string())?;
            let result = if before {
                lease.validate_before(store.as_ref())
            } else {
                lease.validate_after(store.as_ref())
            };
            result.map_err(|_| "KnowledgeStream policy decision lease is stale".to_string())
        }
        #[cfg(not(feature = "security"))]
        {
            let _ = before;
            Err("KnowledgeStream requires the security policy lease feature".to_string())
        }
    }

    /// Internal stream helper used by pure adapter tests.  Production entry
    /// points call [`Self::validate_request_binding`] first, so an unbound
    /// test authority cannot reach the served handler.
    pub(super) fn validate_if_bound(&self, before: bool) -> Result<(), String> {
        #[cfg(feature = "security")]
        {
            return match (&self.policy_lease, &self.policy_store) {
                (None, None) => Ok(()),
                (Some(_), Some(_)) if before => self.validate_before(),
                (Some(_), Some(_)) => self.validate_after(),
                _ => Err("KnowledgeStream policy authority is unavailable".to_string()),
            };
        }
        #[cfg(not(feature = "security"))]
        {
            let _ = before;
            Ok(())
        }
    }
}

#[cfg(feature = "security")]
fn lease_matches_verified_context(
    lease: &crate::isolation::PolicyDecisionLease,
    claims: &RequestContextClaims,
    graph_name: &str,
    carrier: &CarrierAuthority,
) -> bool {
    !graph_name.is_empty()
        && !carrier.actor_scope().is_empty()
        && carrier.agent_id() == claims.agent_id.as_str()
        && lease.access() == crate::isolation::AccessLevel::Read
        && lease.originating_principal() == claims.principal.as_str()
        && lease.effective_actor() == claims.agent_id.as_str()
        && lease.tenant() == claims.tenant.as_str()
        && lease.resource() == graph_name
}

#[cfg(feature = "security")]
fn lease_matches_request(
    lease: &crate::isolation::PolicyDecisionLease,
    graph_name: &str,
    effective_actor: &str,
    bound_actor_scope: Option<&str>,
    originating_actor_scope: &str,
) -> bool {
    lease.effective_actor() == effective_actor
        && lease.resource() == graph_name
        && bound_actor_scope == Some(originating_actor_scope)
        && lease.access() == crate::isolation::AccessLevel::Read
}

#[cfg(feature = "security")]
fn policy_decision_binding(
    claims: &RequestContextClaims,
    lease: &crate::isolation::PolicyDecisionLease,
    originating_scope_ref: &OpaqueRef,
    effective_actor_ref: &OpaqueRef,
) -> Vec<(String, String)> {
    let decision_basis = match lease.decision_basis() {
        crate::isolation::PolicyDecisionBasis::System => "system",
        crate::isolation::PolicyDecisionBasis::RbacAllow => "rbac_allow",
    };
    let mut binding = vec![
        ("tenant".to_string(), lease.tenant().to_string()),
        (
            "originating-principal-scope".to_string(),
            originating_scope_ref.as_str().to_string(),
        ),
        (
            "effective-actor-scope".to_string(),
            effective_actor_ref.as_str().to_string(),
        ),
        ("resource".to_string(), lease.resource().to_string()),
        (
            "policy-version".to_string(),
            lease.policy_snapshot().version.to_string(),
        ),
        (
            "policy-digest".to_string(),
            lease.policy_snapshot().digest.clone(),
        ),
        (
            "row-policy-digest".to_string(),
            lease.row_policy_identity_digest().to_string(),
        ),
        ("decision-basis".to_string(), decision_basis.to_string()),
        ("audience".to_string(), claims.audience.clone()),
    ];
    let mut roles = claims.roles.clone();
    roles.sort_unstable();
    binding.extend(roles.into_iter().map(|value| ("role".to_string(), value)));
    let mut scopes = claims.scopes.clone();
    scopes.sort_unstable();
    binding.extend(scopes.into_iter().map(|value| ("scope".to_string(), value)));
    binding.extend(
        claims
            .delegation
            .iter()
            .cloned()
            .map(|value| ("delegation".to_string(), value)),
    );
    binding
}

fn keyed_bytes(secret: &str, domain: &str, values: &[(&str, &str)]) -> Result<[u8; 32], String> {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
        .map_err(|_| "failed to derive KnowledgeStream authority".to_string())?;
    mac.update(domain.as_bytes());
    for (field, value) in values {
        mac.update(&(field.len() as u64).to_be_bytes());
        mac.update(field.as_bytes());
        mac.update(&(value.len() as u64).to_be_bytes());
        mac.update(value.as_bytes());
    }
    Ok(mac.finalize().into_bytes().into())
}

fn keyed_ref(secret: &str, namespace: &str, values: &[(&str, &str)]) -> Result<OpaqueRef, String> {
    OpaqueRef::scoped(
        namespace,
        &hex::encode(keyed_bytes(secret, namespace, values)?),
    )
    .map_err(|_| "failed to derive KnowledgeStream authority".to_string())
}

fn keyed_opaque(
    authority: &KnowledgeStreamAuthority,
    namespace: &str,
    input: &[u8],
) -> Result<OpaqueRef, String> {
    let mut mac = HmacSha256::new_from_slice(&authority.reference_key)
        .map_err(|_| "failed to derive KnowledgeStream reference".to_string())?;
    mac.update(namespace.as_bytes());
    mac.update(&(input.len() as u64).to_be_bytes());
    mac.update(input);
    OpaqueRef::scoped(namespace, &hex::encode(mac.finalize().into_bytes()))
        .map_err(|error: ProtocolError| error.to_string())
}

pub(super) struct FamilyExecution {
    rows: Vec<KnowledgeBatchRow>,
    source_result: ResultPayload,
}

pub(crate) struct KnowledgeStreamHandlerCtx<'a> {
    pub(crate) state: &'a Arc<RwLock<ServerState>>,
    pub(crate) req_id: u64,
    pub(crate) graph_name: &'a str,
    pub(crate) core: Arc<GraphCore>,
    pub(crate) caller: &'a str,
    pub(crate) carrier: &'a CarrierAuthority,
    pub(crate) authority: &'a KnowledgeStreamAuthority,
    pub(crate) placement_epoch: u64,
    pub(crate) fencing_token: Option<u64>,
    pub(crate) read_authority: &'a GraphReadAuthority,
    #[cfg(feature = "security")]
    pub(crate) rls: &'a Arc<crate::isolation::IsolationLayer>,
}

fn validate_stream_preflight(
    authority: &KnowledgeStreamAuthority,
    caller: &str,
    graph_name: &str,
    carrier: &CarrierAuthority,
    request: &KnowledgeStreamRequestV1,
) -> Result<(), String> {
    if caller != carrier.agent_id() {
        crate::metrics::access_denied();
        return Err("KnowledgeStream policy decision lease binding mismatch".to_string());
    }
    if let Err(error) = authority.validate_request_binding(graph_name, carrier) {
        crate::metrics::access_denied();
        return Err(error);
    }
    if request.schema_version != KNOWLEDGE_STREAM_SCHEMA_VERSION {
        return Err("unsupported KnowledgeStream schema version".to_string());
    }
    if request.batch_size == 0 {
        return Err("KnowledgeStream batch_size must be non-zero".to_string());
    }
    if let Err(error) = authority.validate_before() {
        crate::metrics::access_denied();
        return Err(error);
    }
    Ok(())
}

/// Route the one native stream method. A non-stream method is returned unchanged.
pub(crate) async fn try_handle(
    ctx: KnowledgeStreamHandlerCtx<'_>,
    method: Method,
) -> Result<Response, Method> {
    let KnowledgeStreamHandlerCtx {
        state,
        req_id,
        graph_name,
        core,
        caller,
        carrier,
        authority,
        placement_epoch,
        fencing_token,
        read_authority,
        #[cfg(feature = "security")]
        rls,
    } = ctx;
    let Method::KnowledgeStream { request } = method else {
        return Err(method);
    };
    if let Err(error) = validate_stream_preflight(authority, caller, graph_name, carrier, &request)
    {
        return Ok(Response::err(req_id, error));
    }

    let stream_ctx = KnowledgeStreamExecCtx {
        caller,
        carrier,
        placement_epoch,
        fencing_token,
        authority,
        read_authority,
    };
    let execution = match families::execute_family(
        families::FamilyExecutionCtx {
            state,
            req_id,
            graph_name,
            core: &core,
            stream: &stream_ctx,
            #[cfg(feature = "security")]
            rls,
        },
        &request.query,
    )
    .await
    {
        Ok(value) => value,
        Err(error) => return Ok(Response::err(req_id, error)),
    };
    // The delegated family may have crossed graph/SQL/RDF/vector code while a
    // durable policy mutation raced it.  The source rows stay private until
    // this check succeeds.
    if let Err(error) = authority.validate_after() {
        crate::metrics::access_denied();
        return Ok(Response::err(req_id, error));
    }
    let response = stream::serve_execution(
        graph_name,
        authority,
        placement_epoch,
        fencing_token,
        request,
        execution,
    );
    Ok(match response {
        Ok(batch) => match authority.validate_after() {
            Ok(()) => Response::ok(req_id, ResultPayload::raw(&batch)),
            Err(error) => {
                crate::metrics::access_denied();
                Response::err(req_id, error)
            }
        },
        Err(error) => Response::err(req_id, error),
    })
}

/// The caller/authority fields `execute_family` needs alongside its per-query
/// `query` argument, bundled so the function stays under the clippy
/// argument-count ceiling.
pub(super) struct KnowledgeStreamExecCtx<'a> {
    caller: &'a str,
    carrier: &'a CarrierAuthority,
    placement_epoch: u64,
    fencing_token: Option<u64>,
    authority: &'a KnowledgeStreamAuthority,
    read_authority: &'a GraphReadAuthority,
}

mod families;
mod stream;
#[cfg(test)]
mod tests;

use stream::*;
