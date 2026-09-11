//! RF-020 `kg-delegate` admission adapter.
//!
//! This handler validates the authenticated outer context and the retained
//! Agent Library revision, then lowers the request to the existing native
//! `Method::SubmitWorkItem`.  The caller sends that method through the ordinary
//! WorkItem admission route and maps its committed `SubmitWorkItemResult` with
//! [`result_from_submit`].  The native command log and outbox remain the sole
//! admission/result authority.

use std::collections::BTreeMap;
#[cfg(feature = "redb")]
use std::sync::Arc;

use eg_types::agent_library::AgentLibraryEntry;
use eg_types::delegation::{
    AgentLibraryEntryRef, KgDelegateDecision, KgDelegateRequest, KgDelegateResult,
};
use eg_types::epistemic_operations::RequestContext;
#[cfg(feature = "redb")]
use eg_types::mutation_batch::MutationBatchStatus;
use eg_types::native_control::{
    NativeControlSchemaVersion, SubmitWorkItemRequest, SubmitWorkItemResult,
};
use eg_types::protocol::Method;
#[cfg(feature = "redb")]
use eg_types::protocol::{Response, ResultPayload};
use serde_json::json;

use crate::server::auth::VerifiedRequestContext;

#[cfg(feature = "redb")]
use crate::server::persistence::PersistenceBackend;

#[cfg(feature = "redb")]
use crate::graph::GraphCore;

#[cfg(feature = "redb")]
use crate::server::persistence::agent_library::AgentLibraryStore;

/// Already-authorized graph and placement context for one RF-020 admission.
/// The library handle is the EG-owned retained-definition authority; the
/// handler never accepts an entry supplied by a caller as the source of truth.
#[cfg(feature = "redb")]
pub(crate) struct HandleContext<'a> {
    pub(crate) state: &'a Arc<tokio::sync::RwLock<crate::server::state::ServerState>>,
    pub(crate) req_id: u64,
    pub(crate) graph_name: &'a str,
    pub(crate) caller: Option<&'a str>,
    pub(crate) verified_context: &'a VerifiedRequestContext,
    pub(crate) core: &'a Arc<GraphCore>,
    pub(crate) persistence: &'a Option<Arc<dyn PersistenceBackend>>,
    pub(crate) agent_library: &'a Option<Arc<AgentLibraryStore>>,
    #[cfg(feature = "raft")]
    pub(crate) routed_raft: &'a Option<crate::raft::multi::RoutedRaftHandle>,
}

/// The validated delegation plus its native WorkItem lowering.  The original
/// request is retained only for response correlation; durable admission uses
/// `work_item` and the existing command-log/outbox transaction.
#[derive(Clone, Debug)]
pub(crate) struct BoundDelegation {
    pub(crate) request: KgDelegateRequest,
    pub(crate) work_item: SubmitWorkItemRequest,
}

#[cfg(feature = "redb")]
enum AdmissionError {
    Response(Response),
    Message(String),
}

impl BoundDelegation {
    pub(crate) fn method(&self) -> Method {
        Method::SubmitWorkItem {
            request: self.work_item.clone(),
        }
    }
}

/// Admit one delegation through the existing native WorkItem transaction.
///
/// The route resolves the retained definition using the authenticated outer
/// tenant/agent, validates the caller's pinned reference, then commits the
/// lowered `SubmitWorkItem` through the existing native consensus path when
/// placement is active.  Replays therefore come from the native WorkItem
/// idempotency record rather than a delegation-specific replay ledger.
#[cfg(feature = "redb")]
pub(crate) async fn try_handle(ctx: HandleContext<'_>, method: Method) -> Result<Response, Method> {
    let Method::KgDelegate { request } = method else {
        return Err(method);
    };
    let req_id = ctx.req_id;

    match admit_request(ctx, *request).await {
        Ok(response) => Ok(response),
        Err(AdmissionError::Response(response)) => Ok(response),
        Err(AdmissionError::Message(error)) => Ok(Response::err(req_id, error)),
    }
}

#[cfg(feature = "redb")]
async fn admit_request(
    ctx: HandleContext<'_>,
    request: KgDelegateRequest,
) -> Result<Response, AdmissionError> {
    let (placement_epoch, placement_fence, clustered) = placement_authority(&ctx)
        .await
        .map_err(AdmissionError::Response)?;

    // The native stable-key record is the first domain decision. A committed
    // replay must remain serviceable after the Agent Library head is retired;
    // a fresh key falls through to the current lifecycle/eligibility checks.
    validate_request_binding(&request, ctx.verified_context, ctx.graph_name)
        .map_err(AdmissionError::Message)?;
    if let Some(response) =
        replay_before_library(&ctx, placement_epoch, placement_fence, clustered, &request).await?
    {
        return Ok(response);
    }

    let requested_target_id = request.target.target_id().to_string();
    let requested_revision = request.target.entry_revision();
    let requested_is_graph = request.target.is_graph();
    // Replays are resolved above without reopening the library. A fresh
    // request after process restart lazily restores the durable owner before
    // reading the retained revision.
    let store = match ctx.agent_library.as_ref() {
        Some(store) => Arc::clone(store),
        None => {
            let mut state = ctx.state.write().await;
            state
                .ensure_agent_library()
                .map_err(AdmissionError::Message)?
        }
    };
    let retained = if requested_is_graph {
        RetainedTarget::Graph(
            retained_graph(
                store.as_ref(),
                ctx.verified_context.tenant(),
                &requested_target_id,
                requested_revision,
            )
            .map_err(AdmissionError::Message)?,
        )
    } else {
        RetainedTarget::Agent(
            retained_agent(
                store.as_ref(),
                ctx.verified_context.tenant(),
                &requested_target_id,
                requested_revision,
            )
            .map_err(AdmissionError::Message)?,
        )
    };
    let bound = bind_request(request, ctx.verified_context, &retained, ctx.graph_name)
        .map_err(AdmissionError::Message)?;
    let native = submit_native(
        &ctx,
        placement_epoch,
        placement_fence,
        clustered,
        bound.method(),
    )
    .await?;
    let native = decode_submit_result(native).map_err(AdmissionError::Message)?;
    let result = result_from_submit(&bound, native).map_err(AdmissionError::Message)?;
    Ok(Response::ok(ctx.req_id, ResultPayload::raw(&result)))
}

#[cfg(feature = "redb")]
async fn replay_before_library(
    ctx: &HandleContext<'_>,
    placement_epoch: u64,
    placement_fence: Option<u64>,
    clustered: bool,
    request: &KgDelegateRequest,
) -> Result<Option<Response>, AdmissionError> {
    let Some(persistence) = ctx.persistence.as_ref() else {
        return Ok(None);
    };
    let probe = lower_work_item(request, BTreeMap::new(), Vec::new());
    let identity = crate::server::mutation_batch::work_item_batch_identity(
        ctx.graph_name,
        ctx.verified_context.tenant(),
        ctx.req_id,
        &Method::SubmitWorkItem { request: probe },
    )
    .map_err(AdmissionError::Message)?;
    let graph_fname = crate::persist::sanitize(ctx.graph_name);
    let Some(record) = persistence
        .read_mutation_batch(&graph_fname, &identity.batch_id)
        .await
        .map_err(AdmissionError::Message)?
    else {
        return Ok(None);
    };
    record
        .validate_identity()
        .map_err(AdmissionError::Message)?;
    if record.batch.batch_id != identity.batch_id {
        return Err(AdmissionError::Message(
            "kg-delegate replay receipt identity does not match its stable key".to_string(),
        ));
    }
    if record.status != MutationBatchStatus::Committed {
        return Ok(None);
    }
    if record.result_msgpack.is_none() {
        return Err(AdmissionError::Message(
            "kg-delegate replay receipt has no native result".to_string(),
        ));
    }
    let stored = record
        .batch
        .operations
        .iter()
        .find_map(|operation| match &operation.method {
            Method::SubmitWorkItem { request } => Some(request.clone()),
            _ => None,
        })
        .ok_or_else(|| {
            AdmissionError::Message(
                "kg-delegate replay receipt has no native SubmitWorkItem operation".to_string(),
            )
        })?;
    if !replay_request_matches(request, &stored) {
        return Ok(Some(Response::err(
            ctx.req_id,
            "IDEMPOTENCY_CONFLICT: kg-delegate stable key is bound to a different request",
        )));
    }
    let bound = BoundDelegation {
        request: request.clone(),
        work_item: stored,
    };
    let native = submit_native(
        ctx,
        placement_epoch,
        placement_fence,
        clustered,
        bound.method(),
    )
    .await?;
    let native = decode_submit_result(native).map_err(AdmissionError::Message)?;
    let result = result_from_submit(&bound, native).map_err(AdmissionError::Message)?;
    Ok(Some(Response::ok(ctx.req_id, ResultPayload::raw(&result))))
}

#[cfg(feature = "redb")]
async fn submit_native(
    ctx: &HandleContext<'_>,
    placement_epoch: u64,
    placement_fence: Option<u64>,
    clustered: bool,
    method: Method,
) -> Result<ResultPayload, AdmissionError> {
    #[cfg(feature = "raft")]
    if clustered && !crate::server::dispatch::is_replicated_apply() {
        let response = crate::server::dispatch::propose_native_mutation(
            ctx.state,
            ctx.graph_name,
            ctx.req_id,
            ctx.verified_context,
            false,
            method,
        )
        .await;
        if response.error.is_some() {
            return Err(AdmissionError::Response(response));
        }
        return response.result.ok_or_else(|| {
            AdmissionError::Message(
                "kg-delegate consensus admission returned no native result".to_string(),
            )
        });
    }
    crate::server::mutation_batch::commit_work_item(
        ctx.persistence.as_ref(),
        ctx.core,
        ctx.req_id,
        ctx.verified_context.attempt_nonce(),
        Some(ctx.verified_context.idempotency_key()),
        ctx.caller,
        ctx.graph_name,
        placement_epoch,
        placement_fence,
        method,
    )
    .await
    .map_err(|error| AdmissionError::Message(format!("kg-delegate admission failed: {error}")))
}

#[cfg(feature = "redb")]
/// The durable record a delegation names, resolved and proven retained.
///
/// Mirrors `DelegationTarget`. Everything downstream of resolution -- the work
/// item, the outbox headers, the receipt -- is the same shape for both, so the
/// branch lives here and nowhere else.
#[cfg(feature = "redb")]
pub(crate) enum RetainedTarget {
    Agent(AgentLibraryEntry),
    Graph(eg_types::agent_graph::AgentGraphEntry),
}

#[cfg(feature = "redb")]
impl RetainedTarget {
    fn actor_scope(&self) -> &str {
        match self {
            Self::Agent(entry) => &entry.actor_scope,
            Self::Graph(graph) => &graph.actor_scope,
        }
    }

    fn purpose_id(&self) -> &str {
        match self {
            Self::Agent(entry) => &entry.purpose_id,
            Self::Graph(graph) => &graph.purpose_id,
        }
    }

    fn policy_digest(&self) -> &str {
        match self {
            Self::Agent(entry) => &entry.policy_digest,
            Self::Graph(graph) => &graph.policy_digest,
        }
    }

    fn tenant_id(&self) -> &str {
        match self {
            Self::Agent(entry) => &entry.tenant_id,
            Self::Graph(graph) => &graph.tenant_id,
        }
    }

    pub(crate) fn as_agent(&self) -> Option<&AgentLibraryEntry> {
        match self {
            Self::Agent(entry) => Some(entry),
            Self::Graph(_) => None,
        }
    }
}

/// Resolve the retained graph revision a delegation names.
///
/// Same rules as [`retained_agent`]: the HEAD must not be retired (nothing new
/// may be started on a withdrawn graph), and the named revision must itself be
/// retained and not a tombstone.
#[cfg(feature = "redb")]
fn retained_graph(
    store: &AgentLibraryStore,
    tenant_id: &str,
    graph_id: &str,
    entry_revision: u64,
) -> Result<eg_types::agent_graph::AgentGraphEntry, String> {
    use eg_types::agent_library::AgentLibraryLifecycle;
    let revisions = store
        .graph_revisions(tenant_id, graph_id)
        .map_err(|error| error.to_string())?;
    if revisions
        .last()
        .is_some_and(|graph| graph.lifecycle == AgentLibraryLifecycle::Retired)
    {
        return Err("kg-delegate selected agent graph head is retired".to_string());
    }
    revisions
        .into_iter()
        .find(|graph| {
            graph.entry_revision == entry_revision
                && graph.lifecycle != AgentLibraryLifecycle::Retired
        })
        .ok_or_else(|| {
            "kg-delegate has no retained agent graph revision for the selected graph".to_string()
        })
}

fn retained_agent(
    store: &AgentLibraryStore,
    tenant_id: &str,
    agent_id: &str,
    entry_revision: u64,
) -> Result<AgentLibraryEntry, String> {
    let entries = store
        .revisions(tenant_id, agent_id)
        .map_err(|error| error.to_string())?;
    if entries.last().is_some_and(AgentLibraryEntry::is_retired) {
        return Err("kg-delegate selected Agent Library head is retired".to_string());
    }
    entries
        .into_iter()
        .find(|entry| entry.entry_revision == entry_revision && !entry.is_retired())
        .ok_or_else(|| {
            "kg-delegate has no retained Agent Library revision for the selected agent".to_string()
        })
}

#[cfg(feature = "redb")]
async fn placement_authority(
    ctx: &HandleContext<'_>,
) -> Result<(u64, Option<u64>, bool), Response> {
    #[cfg(feature = "raft")]
    {
        let authority = ctx.state.read().await.placement_authority();
        match authority {
            crate::server::state::PlacementAuthorityKind::Local => return Ok((0, None, false)),
            crate::server::state::PlacementAuthorityKind::Missing => {
                return Err(Response::err(
                    ctx.req_id,
                    crate::server::state::MISSING_PLACEMENT_AUTHORITY,
                ));
            }
            crate::server::state::PlacementAuthorityKind::MultiRaft => {}
        }
        let Some(routed) = ctx.routed_raft.as_ref() else {
            return Err(Response::err(
                ctx.req_id,
                crate::server::state::MISSING_PLACEMENT_AUTHORITY,
            ));
        };
        let leader = routed.handle.current_leader().await;
        if leader != Some(routed.handle.node_id) {
            return Err(Response::stale_route(
                ctx.req_id,
                ctx.graph_name,
                routed.group_id,
                routed.epoch,
                leader,
                "kg-delegate admission requires the current placement leader",
            ));
        }
        return Ok((routed.epoch, Some(routed.group_id), true));
    }
    #[cfg(not(feature = "raft"))]
    {
        Ok((0, None, false))
    }
}

#[cfg(feature = "redb")]
fn decode_submit_result(payload: ResultPayload) -> Result<SubmitWorkItemResult, String> {
    match payload {
        ResultPayload::Raw(bytes) => eg_types::msgpack::decode_bounded(
            &bytes,
            eg_types::msgpack::MsgpackLimits::new(4 * 1024 * 1024, 100_000, 64),
        )
        .map_err(|_| "native admission result is invalid or exceeds resource limits".to_string()),
        ResultPayload::Json(value) => serde_json::from_value(value)
            .map_err(|error| format!("native admission result is invalid: {error}")),
        _ => Err("native admission result has an unsupported payload shape".to_string()),
    }
}

/// Validate an RF-020 request against authenticated authority and lower it to
/// the existing native WorkItem admission DTO.
///
/// `request.context` is a wire assertion.  Every authority-bearing comparison
/// uses `verified_context`, and the retained Agent Library entry is supplied by
/// its owner.  A forged nested context therefore cannot become a trusted
/// `VerifiedRequestContext` through deserialization.
pub(crate) fn bind_request(
    request: KgDelegateRequest,
    verified_context: &VerifiedRequestContext,
    retained: &RetainedTarget,
    graph_name: &str,
) -> Result<BoundDelegation, String> {
    validate_request_binding(&request, verified_context, graph_name)?;
    validate_retained_target(&request, verified_context, retained)?;

    let provenance_refs = provenance_refs(&request);
    let metadata = delegation_metadata(&request, retained);
    let work_item = lower_work_item(&request, metadata, provenance_refs);

    Ok(BoundDelegation { request, work_item })
}

fn lower_work_item(
    request: &KgDelegateRequest,
    metadata: BTreeMap<String, serde_json::Value>,
    provenance_refs: Vec<String>,
) -> SubmitWorkItemRequest {
    SubmitWorkItemRequest {
        schema_version: NativeControlSchemaVersion::V1,
        context: request.context.clone(),
        work_item_id: request.work_item_id.clone(),
        idempotency_key: request.idempotency_key.clone(),
        command_digest: request.command_digest.clone(),
        kind: request.kind.clone(),
        priority: request.priority,
        depends_on: Vec::new(),
        input_ref: request.input_ref.clone(),
        policy_digest: request.policy_digest.clone(),
        catalog_digest: request.catalog_digest.clone(),
        // The native work item carries one model digest. An agent delegation
        // supplies it directly; a graph has none, so it supplies its shape
        // digest -- which is the honest answer rather than a placeholder,
        // because the shape pins every agent node and therefore every model the
        // run may use.
        model_digest: request.model_digest.clone().unwrap_or_else(|| {
            request
                .target
                .pinned_digest()
                .strip_prefix("sha256:")
                .unwrap_or_else(|| request.target.pinned_digest())
                .to_string()
        }),
        max_attempts: request.max_attempts,
        deadline_unix: request.deadline_unix,
        metadata,
        provenance_refs,
        max_tenant_in_flight: request.max_tenant_in_flight,
    }
}

#[cfg(feature = "redb")]
fn replay_request_matches(request: &KgDelegateRequest, stored: &SubmitWorkItemRequest) -> bool {
    if !wire_metadata_matches(request, &stored.metadata) {
        return false;
    }
    let mut proposed = lower_work_item(
        request,
        stored.metadata.clone(),
        stored.provenance_refs.clone(),
    );
    let mut retained = stored.clone();
    normalize_replay_context(&mut proposed.context);
    normalize_replay_context(&mut retained.context);
    proposed == retained
}

#[cfg(feature = "redb")]
fn wire_metadata_matches(
    request: &KgDelegateRequest,
    stored: &BTreeMap<String, serde_json::Value>,
) -> bool {
    delegation_wire_metadata(request)
        .into_iter()
        .all(|(key, value)| stored.get(&key) == Some(&value))
}

fn normalize_replay_context(context: &mut RequestContext) {
    context.request_id.clear();
    context.trace_id.clear();
    context.issued_at_ms = 0;
    context.expires_at_ms = 0;
    context.placement_epoch = None;
}

fn validate_request_binding(
    request: &KgDelegateRequest,
    verified_context: &VerifiedRequestContext,
    graph_name: &str,
) -> Result<(), String> {
    request.validate()?;
    validate_request_context(&request.context, verified_context, graph_name)?;
    if verified_context.allows_method("work:delegate", true) {
        Ok(())
    } else {
        Err("kg-delegate requires the authenticated work:delegate scope".to_string())
    }
}

#[cfg(feature = "redb")]
fn validate_retained_target(
    request: &KgDelegateRequest,
    verified_context: &VerifiedRequestContext,
    retained: &RetainedTarget,
) -> Result<(), String> {
    // Tenant, scope, purpose and policy are checked identically for both
    // targets -- they are properties of the delegation, not of what it runs.
    if retained.tenant_id() != verified_context.tenant() {
        return Err(
            "kg-delegate retained target is outside authenticated tenant".to_string(),
        );
    }
    if request.actor_scope != retained.actor_scope() || request.purpose != retained.purpose_id() {
        return Err(
            "kg-delegate actor scope or purpose is not the retained target's policy".to_string(),
        );
    }
    if request.policy_digest != retained.policy_digest() {
        return Err("kg-delegate policy digest is stale or unresolved".to_string());
    }
    match (&request.target, retained) {
        (eg_types::delegation::DelegationTarget::Agent { entry }, RetainedTarget::Agent(retained_agent)) => {
            retained_agent.validate()?;
            if retained_agent.is_retired() {
                return Err("kg-delegate retained Agent Library entry is retired".to_string());
            }
            if *entry != AgentLibraryEntryRef::from_entry(retained_agent) {
                return Err(
                    "kg-delegate agent entry is not the retained revision/digest".to_string()
                );
            }
            validate_execution_bindings(request, retained_agent)?;
        }
        (eg_types::delegation::DelegationTarget::Graph { graph }, RetainedTarget::Graph(retained_graph)) => {
            retained_graph.validate()?;
            if graph.shape_digest != retained_graph.shape_digest {
                return Err(
                    "kg-delegate agent graph is not the retained revision/shape digest".to_string(),
                );
            }
            // The composed ceiling must be the one this graph was ADMITTED
            // with. A caller that could raise it would escape the bound the
            // composition check enforced at publish -- which is the whole point
            // of carrying it rather than trusting the executor to recompute.
            let admitted = eg_types::agent_graph::validate_composition(
                &retained_graph.tenant_id,
                &retained_graph.shape,
                |_, _| {
                    Err("kg-delegate cannot resolve composed children at admission".to_string())
                },
            )
            .map(|facts| facts.total_work);
            // A graph with no child graphs resolves nothing and yields its own
            // ceiling; one WITH children cannot be re-derived here without the
            // store, so its recorded ceiling is accepted as validated at
            // publish and only range-checked.
            if let Ok(total_work) = admitted {
                if graph.composed_work_ceiling != total_work {
                    return Err(
                        "kg-delegate agent graph composed_work_ceiling is not the admitted ceiling"
                            .to_string(),
                    );
                }
            }
            // A graph pins no scalar model; its shape digest is the capability
            // binding, since it transitively covers every agent and component.
            let expected_capability =
                unprefixed_digest("shape_digest", &retained_graph.shape_digest)?;
            if request.capability_digest != expected_capability {
                return Err(
                    "kg-delegate capability digest does not match the retained graph shape"
                        .to_string(),
                );
            }
        }
        _ => {
            return Err(
                "kg-delegate resolved a different target kind than the request named".to_string(),
            )
        }
    }
    Ok(())
}

/// Bind the runtime currency in the delegation request to the exact retained
/// definition.  The wire request carries the unprefixed SHA-256 form used by
/// native WorkItem fields; Agent Library identity stores the same digests as
/// `sha256:<hex>`.  The generated contract catalog binds the request schema,
/// while the retained entry metadata binds B's tool, skill, ontology, and
/// model components.  This gives every immutable execution component a
/// checked home without treating caller strings as a second authority.
fn validate_execution_bindings(
    request: &KgDelegateRequest,
    retained_agent: &AgentLibraryEntry,
) -> Result<(), String> {
    let expected_model =
        unprefixed_digest("model_profile_digest", &retained_agent.model_profile_digest())?;
    if request.model_digest.as_deref() != Some(expected_model.as_str()) {
        return Err(
            "kg-delegate model digest does not match retained Agent Library model".to_string(),
        );
    }
    let expected_capability =
        unprefixed_digest("tool_set_digest", &retained_agent.tool_set_digest())?;
    if request.capability_digest != expected_capability {
        return Err(
            "kg-delegate capability digest does not match retained Agent Library tool set"
                .to_string(),
        );
    }
    if request.catalog_digest != eg_capabilities::CONTRACT_CATALOG_DIGEST {
        return Err(
            "kg-delegate catalog digest does not match the generated contract catalog".to_string(),
        );
    }
    Ok(())
}

fn unprefixed_digest(field: &str, digest: &str) -> Result<String, String> {
    let value = digest
        .strip_prefix("sha256:")
        .filter(|value| value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit()))
        .ok_or_else(|| format!("retained Agent Library {field} is not a SHA-256 digest"))?;
    Ok(value.to_ascii_lowercase())
}

/// Provenance headers for the admitted work item.
///
/// An agent contributes its components' digests; a graph contributes its shape
/// digest and composed ceiling. A graph deliberately does NOT restate per-agent
/// digests: its shape digest already pins every one of them, and copying them
/// out would create a second set that could disagree.
#[cfg(feature = "redb")]
fn delegation_metadata(
    request: &KgDelegateRequest,
    retained: &RetainedTarget,
) -> BTreeMap<String, serde_json::Value> {
    let mut metadata = delegation_wire_metadata(request);
    let retained_agent = match retained {
        RetainedTarget::Agent(entry) => entry,
        RetainedTarget::Graph(graph) => {
            metadata.extend(BTreeMap::from([
                ("target_kind".to_string(), json!("agent_graph")),
                ("agent_graph_id".to_string(), json!(graph.graph_id)),
                (
                    "agent_graph_revision".to_string(),
                    json!(graph.entry_revision),
                ),
                (
                    "agent_graph_shape_digest".to_string(),
                    json!(graph.shape_digest),
                ),
                ("agent_graph_version".to_string(), json!(graph.version)),
                (
                    "agent_graph_node_count".to_string(),
                    json!(graph.shape.nodes.len()),
                ),
                (
                    "agent_graph_max_iterations".to_string(),
                    json!(graph.shape.max_iterations),
                ),
            ]));
            return metadata;
        }
    };
    metadata.extend(BTreeMap::from([
        ("target_kind".to_string(), json!("agent")),
        (
            "agent_package_id".to_string(),
            json!(retained_agent.package_id),
        ),
        ("agent_version".to_string(), json!(retained_agent.version)),
        (
            "agent_role_digest".to_string(),
            json!(retained_agent.role_digest),
        ),
        (
            "agent_system_prompt_digest".to_string(),
            json!(retained_agent.system_prompt_digest()),
        ),
        (
            "agent_tool_set_digest".to_string(),
            json!(retained_agent.tool_set_digest()),
        ),
        (
            "agent_skill_set_digest".to_string(),
            json!(retained_agent.skill_set_digest()),
        ),
        (
            "agent_model_identity".to_string(),
            json!(retained_agent.model_identity),
        ),
        (
            "agent_model_profile_digest".to_string(),
            json!(retained_agent.model_profile_digest()),
        ),
        (
            "agent_ontology_set_digest".to_string(),
            json!(retained_agent.ontology_set_digest()),
        ),
    ]));
    metadata
}

fn delegation_wire_metadata(request: &KgDelegateRequest) -> BTreeMap<String, serde_json::Value> {
    BTreeMap::from([
        ("delegation_id".to_string(), json!(request.delegation_id)),
        ("run_id".to_string(), json!(request.run_id)),
        ("trace_id".to_string(), json!(request.trace_id)),
        ("actor_scope".to_string(), json!(request.actor_scope)),
        ("purpose".to_string(), json!(request.purpose)),
        // Target-shaped rather than agent-shaped: the names stay `agent_*` so
        // every existing consumer of these headers keeps working, and the
        // values come from whichever target was named. `target_id` is an agent
        // id or a graph id; `pinned_digest` is a definition digest or a shape
        // digest.
        (
            "agent_tenant_id".to_string(),
            json!(request.target.tenant_id()),
        ),
        ("agent_id".to_string(), json!(request.target.target_id())),
        (
            "agent_entry_revision".to_string(),
            json!(request.target.entry_revision()),
        ),
        (
            "agent_definition_digest".to_string(),
            json!(request.target.pinned_digest()),
        ),
        (
            "capability_digest".to_string(),
            json!(request.capability_digest),
        ),
    ])
}

/// Map the committed native admission result to the RF-020 result wire.
///
/// These checks prevent a native result for another idempotency key, command,
/// or provenance set from being exposed as this delegation's receipt.  Native
/// replay is preserved as `Replayed`; no delegation-specific replay ledger is
/// introduced.
pub(crate) fn result_from_submit(
    bound: &BoundDelegation,
    result: SubmitWorkItemResult,
) -> Result<KgDelegateResult, String> {
    let decision = validate_submit_result(bound, &result)?;
    Ok(KgDelegateResult {
        schema_version: eg_types::delegation::KgDelegateSchemaVersion::V1,
        decision,
        delegation_id: bound.request.delegation_id.clone(),
        run_id: bound.request.run_id.clone(),
        trace_id: bound.request.trace_id.clone(),
        work_item_id: result.work_item_id,
        outbox_id: result.outbox_id,
        idempotency_key: result.idempotency_key,
        target: bound.request.target.clone(),
        command_digest: result.command_digest,
        capability_digest: bound.request.capability_digest.clone(),
        catalog_digest: bound.request.catalog_digest.clone(),
        policy_digest: bound.request.policy_digest.clone(),
        model_digest: bound.request.model_digest.clone(),
    })
}

fn validate_submit_result(
    bound: &BoundDelegation,
    result: &SubmitWorkItemResult,
) -> Result<KgDelegateDecision, String> {
    validate_submit_identity(bound, result)?;
    let decision = match (result.created, result.replayed) {
        (true, false) => KgDelegateDecision::Accepted,
        (false, true) => KgDelegateDecision::Replayed,
        _ => return Err("native admission returned an invalid created/replayed pair".to_string()),
    };
    if result.work_item_id.trim().is_empty()
        || result.outbox_id.trim().is_empty()
        || result.status.trim().is_empty()
        || result.command_sequence == 0
    {
        return Err("native admission returned an incomplete command receipt".to_string());
    }
    Ok(decision)
}

fn validate_submit_identity(
    bound: &BoundDelegation,
    result: &SubmitWorkItemResult,
) -> Result<(), String> {
    if result.schema_version != NativeControlSchemaVersion::V1 {
        return Err("native admission returned an unsupported schema version".to_string());
    }
    for (matches, error) in [
        (
            result.idempotency_key == bound.request.idempotency_key,
            "native admission result idempotency key does not match delegation",
        ),
        (
            result.command_digest == bound.request.command_digest,
            "native admission result command digest does not match delegation",
        ),
        (
            result.provenance_refs == bound.work_item.provenance_refs,
            "native admission result provenance does not match delegation",
        ),
    ] {
        if !matches {
            return Err(error.to_string());
        }
    }
    if let Some(expected_work_item_id) = &bound.request.work_item_id {
        if &result.work_item_id != expected_work_item_id {
            return Err(
                "native admission result WorkItem id does not match delegation".to_string(),
            );
        }
    }
    Ok(())
}

pub(crate) fn provenance_refs(request: &KgDelegateRequest) -> Vec<String> {
    vec![
        format!("delegation:{}", request.delegation_id),
        format!("run:{}", request.run_id),
        format!("trace:{}", request.trace_id),
        request.target.provenance_ref(),
    ]
}

fn validate_request_context(
    context: &RequestContext,
    verified_context: &VerifiedRequestContext,
    graph_name: &str,
) -> Result<(), String> {
    if context.schema_version != eg_types::epistemic_operations::RequestContextSchemaVersion::V2 {
        return Err("kg-delegate context schema_version is unsupported".to_string());
    }
    if context.graph != graph_name {
        return Err("kg-delegate context graph does not match request graph".to_string());
    }
    if !context_matches_verified_authority(context, verified_context) {
        return Err("kg-delegate context does not match authenticated outer authority".to_string());
    }
    if !context_has_valid_window(context) {
        return Err("kg-delegate context is incomplete or expired".to_string());
    }
    if !context_scopes_within_authority(context, verified_context) {
        return Err(
            "kg-delegate context requests a scope outside the authenticated envelope".to_string(),
        );
    }
    Ok(())
}

/// Whether a `RequestContext`'s tenant/agent/audience/policy_version all match the
/// caller's already-verified authority. This is the one identity check every request
/// boundary shares -- both this `kg-delegate` context validation and the native
/// `SubmitWorkItem` command binding (`server::dispatch::request_boundary::
/// validate_submit_context`) run the exact same four-field comparison over the same
/// verified-authority carrier before going on to check their own request's time
/// window / scope bounds, which ARE surface-specific and stay local to each caller.
/// `pub(crate)` so the dispatch request boundary can call it too.
pub(crate) fn context_matches_verified_authority(
    context: &RequestContext,
    verified_context: &VerifiedRequestContext,
) -> bool {
    context.tenant_id == verified_context.tenant()
        && context.agent_id == verified_context.agent_id()
        && context.audience == verified_context.claims().audience
        && context.policy_version == verified_context.claims().policy_version
}

fn context_has_valid_window(context: &RequestContext) -> bool {
    !context.request_id.trim().is_empty()
        && !context.subject_id.trim().is_empty()
        && !context.trace_id.trim().is_empty()
        && context.expires_at_ms >= context.issued_at_ms
}

fn context_scopes_within_authority(
    context: &RequestContext,
    verified_context: &VerifiedRequestContext,
) -> bool {
    context
        .scopes
        .iter()
        .all(|scope| !scope.trim().is_empty() && verified_context.allows_action(scope))
}

#[cfg(test)]
mod tests {
    use super::*;
    use eg_types::agent_library::{AgentLibraryEntryDraft, AgentLibraryLifecycle};
    use eg_types::delegation::{KgDelegateSchemaVersion, MAX_DELEGATION_IN_FLIGHT};
    use eg_types::epistemic_operations::{
        RequestContext, RequestContextAuthenticationMethod, RequestContextSchemaVersion,
    };

    fn digest(byte: char) -> String {
        byte.to_string().repeat(64)
    }

    fn prefixed_digest(byte: char) -> String {
        format!("sha256:{}", digest(byte))
    }

    fn context(tenant: &str) -> RequestContext {
        RequestContext {
            schema_version: RequestContextSchemaVersion::V2,
            request_id: "request:1".into(),
            subject_id: "subject:1".into(),
            tenant_id: tenant.into(),
            agent_id: "agent:1".into(),
            scopes: vec!["work:delegate".into()],
            audience: "epistemic-graph".into(),
            authentication_method: RequestContextAuthenticationMethod::LocalProcess,
            policy_version: "policy-test".into(),
            graph: tenant.into(),
            placement_epoch: Some(1),
            trace_id: "trace:context".into(),
            issued_at_ms: 1,
            expires_at_ms: 2,
        }
    }

    fn agent_entry() -> AgentLibraryEntry {
        agent_entry_for("agent:1")
    }

    fn agent_entry_for(agent_id: &str) -> AgentLibraryEntry {
        AgentLibraryEntry::create(
            AgentLibraryEntryDraft {
                agent_id: agent_id.into(),
                package_id: "package:1".into(),
                version: "1.0.0".into(),
                role: "worker".into(),
                role_digest: prefixed_digest('a'),
                system_prompt: eg_types::agent_component::ComponentDependency {
                    component_id: "prompt:1".into(),
                    kind: eg_types::agent_component::AgentComponentKind::SystemPrompt,
                    definition_digest: prefixed_digest('b'),
                },
                tools: vec![
                    eg_types::agent_component::ComponentDependency {
                        component_id: "tool:1".into(),
                        kind: eg_types::agent_component::AgentComponentKind::Tool,
                        definition_digest: prefixed_digest('c'),
                    },
                ],
                skills: vec![
                    eg_types::agent_component::ComponentDependency {
                        component_id: "skill:1".into(),
                        kind: eg_types::agent_component::AgentComponentKind::Skill,
                        definition_digest: prefixed_digest('d'),
                    },
                ],
                model_profile: eg_types::agent_component::ComponentDependency {
                    component_id: "model-profile:1".into(),
                    kind: eg_types::agent_component::AgentComponentKind::ModelProfile,
                    definition_digest: prefixed_digest('e'),
                },
                model_identity: "model:1".into(),
                ontologies: vec![
                    eg_types::agent_component::ComponentDependency {
                        component_id: "ontology:1".into(),
                        kind: eg_types::agent_component::AgentComponentKind::Ontology,
                        definition_digest: prefixed_digest('f'),
                    },
                ],
                tenant_id: "tenant:1".into(),
                actor_scope: format!("tenant:1/{agent_id}"),
                purpose_id: "delegation.execute".into(),
                policy_digest: prefixed_digest('0'),
                source_revision: "library-source:7".into(),
                source_revision_digest: prefixed_digest('1'),
                runtime: Default::default(),
                instantiated_from: None,
            },
            7,
            AgentLibraryLifecycle::Published,
            1,
            1,
        )
        .unwrap()
    }

    fn request(tenant: &str, entry: &AgentLibraryEntry) -> KgDelegateRequest {
        KgDelegateRequest {
            schema_version: KgDelegateSchemaVersion::V1,
            context: context(tenant),
            delegation_id: "delegation:1".into(),
            run_id: "run:1".into(),
            trace_id: "trace:run:1".into(),
            target: eg_types::delegation::DelegationTarget::Agent {
                entry: AgentLibraryEntryRef::from_entry(entry),
            },
            input_ref: "cas:input:1".into(),
            command_digest: digest('2'),
            capability_digest: unprefixed_digest("tool_set_digest", &entry.tool_set_digest())
                .unwrap(),
            catalog_digest: eg_capabilities::CONTRACT_CATALOG_DIGEST.to_string(),
            policy_digest: entry.policy_digest.clone(),
            model_digest: Some(
                unprefixed_digest("model_profile_digest", &entry.model_profile_digest()).unwrap(),
            ),
            idempotency_key: "delegate-idempotency:1".into(),
            kind: "agent.execute".into(),
            actor_scope: entry.actor_scope.clone(),
            purpose: entry.purpose_id.clone(),
            work_item_id: Some("workitem:1".into()),
            priority: 10,
            max_attempts: 3,
            deadline_unix: Some(2_000.0),
            max_tenant_in_flight: 10,
        }
    }

    fn verified() -> VerifiedRequestContext {
        VerifiedRequestContext::verified_for_test_with_scopes(
            "agent:1",
            "tenant:1",
            &["work:delegate"],
        )
    }

    #[test]
    fn bind_uses_authenticated_outer_tenant_and_pinned_entry() {
        let entry = agent_entry();
        let bound =
            bind_request(request("tenant:1", &entry), &verified(), &RetainedTarget::Agent(entry.clone()), "tenant:1").unwrap();
        assert_eq!(bound.work_item.context.tenant_id, "tenant:1");
        assert_eq!(bound.work_item.provenance_refs.len(), 4);
        assert_eq!(
            bound.work_item.metadata["capability_digest"],
            json!(digest('3'))
        );
        assert_eq!(
            bound.work_item.metadata["agent_model_profile_digest"],
            json!(entry.model_profile_digest())
        );
        assert_eq!(
            bound.work_item.metadata["agent_skill_set_digest"],
            json!(entry.skill_set_digest())
        );
        assert!(matches!(bound.method(), Method::SubmitWorkItem { .. }));
    }

    #[test]
    fn execution_digest_mismatch_is_rejected_before_lowering() {
        let entry = agent_entry();
        for (field, value) in [
            ("model_digest", digest('9')),
            ("capability_digest", digest('9')),
            ("catalog_digest", digest('9')),
        ] {
            let mut request = request("tenant:1", &entry);
            match field {
                "model_digest" => request.model_digest = Some(value),
                "capability_digest" => request.capability_digest = value,
                "catalog_digest" => request.catalog_digest = value,
                _ => unreachable!(),
            }
            let error = bind_request(request, &verified(), &RetainedTarget::Agent(entry.clone()), "tenant:1").unwrap_err();
            assert!(error.contains("digest"), "{field}: {error}");
        }
    }

    #[test]
    fn caller_may_delegate_to_another_retained_agent_in_same_tenant() {
        let entry = agent_entry_for("agent:2");
        let bound =
            bind_request(request("tenant:1", &entry), &verified(), &RetainedTarget::Agent(entry.clone()), "tenant:1").unwrap();
        assert_eq!(bound.work_item.context.agent_id, "agent:1");
        assert_eq!(bound.work_item.metadata["agent_id"], json!("agent:2"));
    }

    #[test]
    fn missing_delegate_policy_is_rejected_before_library_lookup() {
        let entry = agent_entry();
        let caller =
            VerifiedRequestContext::verified_for_test_with_scopes("agent:1", "tenant:1", &[]);
        let error =
            bind_request(request("tenant:1", &entry), &caller, &RetainedTarget::Agent(entry.clone()), "tenant:1").unwrap_err();
        assert!(error.contains("work:delegate"));
    }

    #[test]
    fn forged_nested_tenant_is_rejected() {
        let entry = agent_entry();
        let error = bind_request(
            request("tenant:other", &entry),
            &verified(),
            &RetainedTarget::Agent(entry.clone()),
            "tenant:other",
        )
        .unwrap_err();
        assert!(error.contains("authenticated outer authority"));
    }

    #[test]
    fn stale_agent_revision_is_rejected_before_lowering() {
        let entry = agent_entry();
        let mut retained = entry.clone();
        retained.entry_revision += 1;
        let error = bind_request(
            request("tenant:1", &entry),
            &verified(),
            &RetainedTarget::Agent(retained.clone()),
            "tenant:1",
        )
        .unwrap_err();
        assert!(error.contains("retained revision/digest"));
    }

    #[test]
    fn stale_policy_digest_is_rejected_before_lowering() {
        let entry = agent_entry();
        let mut request = request("tenant:1", &entry);
        request.policy_digest = prefixed_digest('9');
        let error = bind_request(request, &verified(), &RetainedTarget::Agent(entry.clone()), "tenant:1").unwrap_err();
        assert!(error.contains("policy digest"));
    }

    #[test]
    fn retired_entry_is_rejected_before_lowering() {
        let entry = agent_entry();
        let retired = entry.retire(8, 2).unwrap();
        let request = request("tenant:1", &retired);
        let error = bind_request(request, &verified(), &RetainedTarget::Agent(retired.clone()), "tenant:1").unwrap_err();
        assert!(error.contains("retired"));
    }

    #[test]
    fn native_result_maps_only_matching_admission() {
        let entry = agent_entry();
        let bound =
            bind_request(request("tenant:1", &entry), &verified(), &RetainedTarget::Agent(entry.clone()), "tenant:1").unwrap();
        let result = SubmitWorkItemResult {
            schema_version: NativeControlSchemaVersion::V1,
            work_item_id: "workitem:1".into(),
            status: "ready".into(),
            created: true,
            replayed: false,
            command_sequence: 1,
            idempotency_key: "delegate-idempotency:1".into(),
            dependency_count: 0,
            admitted_count: 1,
            max_tenant_in_flight: MAX_DELEGATION_IN_FLIGHT,
            outbox_id: "outbox:1".into(),
            command_digest: digest('2'),
            provenance_refs: bound.work_item.provenance_refs.clone(),
            changed_work_item_ids: vec!["workitem:1".into()],
        };
        let mapped = result_from_submit(&bound, result).unwrap();
        assert_eq!(mapped.decision, KgDelegateDecision::Accepted);
        assert_eq!(mapped.outbox_id, "outbox:1");
    }

    #[test]
    fn native_replay_maps_to_replayed_result() {
        let entry = agent_entry();
        let bound =
            bind_request(request("tenant:1", &entry), &verified(), &RetainedTarget::Agent(entry.clone()), "tenant:1").unwrap();
        let result = SubmitWorkItemResult {
            schema_version: NativeControlSchemaVersion::V1,
            work_item_id: "workitem:1".into(),
            status: "ready".into(),
            created: false,
            replayed: true,
            command_sequence: 1,
            idempotency_key: "delegate-idempotency:1".into(),
            dependency_count: 0,
            admitted_count: 1,
            max_tenant_in_flight: 10,
            outbox_id: "outbox:1".into(),
            command_digest: digest('2'),
            provenance_refs: bound.work_item.provenance_refs.clone(),
            changed_work_item_ids: vec!["workitem:1".into()],
        };
        assert_eq!(
            result_from_submit(&bound, result).unwrap().decision,
            KgDelegateDecision::Replayed
        );
    }

    #[cfg(feature = "raft")]
    #[test]
    fn delegated_admission_uses_the_existing_work_item_native_command() {
        let entry = agent_entry();
        let bound =
            bind_request(request("tenant:1", &entry), &verified(), &RetainedTarget::Agent(entry.clone()), "tenant:1").unwrap();
        let command = crate::raft::NativeMutationCommand::from_public_method(
            bound.method(),
            "delegation-native-route-test",
        )
        .expect("lowered SubmitWorkItem has a bounded native command");

        assert!(matches!(
            command,
            crate::raft::NativeMutationCommand::WorkItem { .. }
        ));
        assert!(crate::raft::NATIVE_CONSENSUS_METHODS.contains(&"SubmitWorkItem"));
        let reopened = command
            .open_public_method("delegation-native-route-test")
            .expect("native command authenticates its sealed method")
            .expect("WorkItem command carries a public method");
        assert!(matches!(reopened, Method::SubmitWorkItem { .. }));
    }
}
