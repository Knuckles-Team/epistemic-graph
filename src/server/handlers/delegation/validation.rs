use super::*;

/// Resolve the retained graph revision a delegation names.
///
/// Same rules as [`retained_agent`]: the HEAD must not be retired (nothing new
/// may be started on a withdrawn graph), and the named revision must itself be
/// retained and not a tombstone.
#[cfg(feature = "redb")]
pub(crate) fn retained_graph(
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

pub(crate) fn retained_agent(
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
// Same as `submit_native` above: `ctx` is read only by the `raft` block, so it
// is unused -- not dead -- in a build without it.
#[cfg_attr(not(feature = "raft"), allow(unused_variables))]
pub(crate) async fn placement_authority(
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
pub(crate) fn decode_submit_result(payload: ResultPayload) -> Result<SubmitWorkItemResult, String> {
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

pub(crate) fn lower_work_item(
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
        // supplies it directly; a graph has none, so it supplies the digest it
        // pins -- its record digest, which covers the shape digest and so pins
        // every agent node and therefore every model the run may use. An honest
        // answer rather than a placeholder.
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
pub(crate) fn replay_request_matches(
    request: &KgDelegateRequest,
    stored: &SubmitWorkItemRequest,
) -> bool {
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
pub(crate) fn wire_metadata_matches(
    request: &KgDelegateRequest,
    stored: &BTreeMap<String, serde_json::Value>,
) -> bool {
    delegation_wire_metadata(request)
        .into_iter()
        .all(|(key, value)| stored.get(&key) == Some(&value))
}

pub(crate) fn normalize_replay_context(context: &mut RequestContext) {
    context.request_id.clear();
    context.trace_id.clear();
    context.issued_at_ms = 0;
    context.expires_at_ms = 0;
    context.placement_epoch = None;
}

pub(crate) fn validate_request_binding(
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
pub(crate) fn validate_retained_target(
    request: &KgDelegateRequest,
    verified_context: &VerifiedRequestContext,
    retained: &RetainedTarget,
) -> Result<(), String> {
    validate_retained_metadata(request, verified_context, retained)?;
    match (&request.target, retained) {
        (
            eg_types::delegation::DelegationTarget::Agent { entry },
            RetainedTarget::Agent(retained_agent),
        ) => validate_agent_target(entry, request, retained_agent),
        (
            eg_types::delegation::DelegationTarget::Graph { graph },
            RetainedTarget::Graph(retained_graph),
        ) => validate_graph_target(graph, request, retained_graph),
        _ => Err("kg-delegate resolved a different target kind than the request named".to_string()),
    }
}

#[cfg(feature = "redb")]
fn validate_retained_metadata(
    request: &KgDelegateRequest,
    verified_context: &VerifiedRequestContext,
    retained: &RetainedTarget,
) -> Result<(), String> {
    if retained.tenant_id() != verified_context.tenant() {
        return Err("kg-delegate retained target is outside authenticated tenant".to_string());
    }
    if request.actor_scope != retained.actor_scope() || request.purpose != retained.purpose_id() {
        return Err(
            "kg-delegate actor scope or purpose is not the retained target's policy".to_string(),
        );
    }
    if request.policy_digest != retained.policy_digest() {
        return Err("kg-delegate policy digest is stale or unresolved".to_string());
    }
    Ok(())
}

#[cfg(feature = "redb")]
fn validate_agent_target(
    entry: &AgentLibraryEntryRef,
    request: &KgDelegateRequest,
    retained_agent: &AgentLibraryEntry,
) -> Result<(), String> {
    retained_agent.validate()?;
    if retained_agent.is_retired() {
        return Err("kg-delegate retained Agent Library entry is retired".to_string());
    }
    if *entry != AgentLibraryEntryRef::from_entry(retained_agent) {
        return Err("kg-delegate agent entry is not the retained revision/digest".to_string());
    }
    validate_execution_bindings(request, retained_agent)
}

#[cfg(feature = "redb")]
fn validate_graph_target(
    graph: &eg_types::delegation::AgentGraphEntryRef,
    request: &KgDelegateRequest,
    retained_graph: &eg_types::agent_graph::AgentGraphEntry,
) -> Result<(), String> {
    retained_graph.validate()?;
    // The composed ceiling must be the one this graph was ADMITTED
    // with. A caller that could raise it would escape the bound the
    // composition check enforced at publish -- which is the whole point
    // of carrying it rather than trusting the executor to recompute.
    //
    // Compared against the PERSISTED value, not a re-derivation. The
    // re-derivation this once attempted could not resolve child graphs
    // from here (it had no store), so for the only case that matters --
    // a graph WITH children -- it always errored, the comparison was
    // skipped, and the caller's own number was accepted subject to
    // nothing but `1..=MAX_COMPOSITION_WORK`. A graph admitted at 2,000
    // could be delegated declaring 1,000,000.
    if graph.composed_work_ceiling != retained_graph.composed_work_ceiling {
        return Err(
            "kg-delegate agent graph composed_work_ceiling is not the admitted ceiling".to_string(),
        );
    }
    // The WHOLE reference, exactly as the agent arm compares the whole
    // `AgentLibraryEntryRef`. A delegation pins the record digest, not
    // the shape digest: it names a record IDENTITY, and
    // `synthesis_evidence` -- the artifact that makes a synthesized
    // graph auditable -- is inside `definition_digest` and outside
    // `shape_digest`. (A composing parent still pins the SHAPE; see
    // `AgentGraphEntryRef`'s type doc for why the two differ.)
    if *graph != eg_types::delegation::AgentGraphEntryRef::from_entry(retained_graph) {
        return Err(
            "kg-delegate agent graph is not the retained revision/definition digest".to_string(),
        );
    }
    // A graph pins no scalar model; its SHAPE digest is the capability
    // binding, since it transitively covers every agent and component.
    // Deliberately not the record digest the reference pins: a
    // capability proof is about what will RUN, and two records that do
    // the same thing must present the same capability.
    let expected_capability = unprefixed_digest("shape_digest", &retained_graph.shape_digest)?;
    if request.capability_digest != expected_capability {
        return Err(
            "kg-delegate capability digest does not match the retained graph shape".to_string(),
        );
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
pub(crate) fn validate_execution_bindings(
    request: &KgDelegateRequest,
    retained_agent: &AgentLibraryEntry,
) -> Result<(), String> {
    let expected_model = unprefixed_digest(
        "model_profile_digest",
        retained_agent.model_profile_digest(),
    )?;
    if request.model_digest.as_deref() != Some(expected_model.as_str()) {
        return Err(
            "kg-delegate model digest does not match retained Agent Library model".to_string(),
        );
    }
    let expected_capability =
        unprefixed_digest("tool_surface_digest", &retained_agent.tool_surface_digest())?;
    if request.capability_digest != expected_capability {
        return Err(
            "kg-delegate capability digest does not match retained Agent Library tool surface"
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

pub(crate) fn unprefixed_digest(field: &str, digest: &str) -> Result<String, String> {
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
pub(crate) fn delegation_metadata(
    request: &KgDelegateRequest,
    retained: &RetainedTarget,
) -> BTreeMap<String, serde_json::Value> {
    let mut metadata = delegation_wire_metadata(request);
    let retained_agent = match retained {
        RetainedTarget::Agent(entry) => entry.as_ref(),
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
            "agent_tool_surface_digest".to_string(),
            json!(retained_agent.tool_surface_digest()),
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

pub(crate) fn delegation_wire_metadata(
    request: &KgDelegateRequest,
) -> BTreeMap<String, serde_json::Value> {
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
        schema_version: eg_types::delegation::KgDelegateSchemaVersion::V2,
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

pub(crate) fn validate_submit_result(
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

pub(crate) fn validate_submit_identity(
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

pub(crate) fn validate_request_context(
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
    if !super::context_matches_verified_authority(context, verified_context) {
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

pub(crate) fn context_has_valid_window(context: &RequestContext) -> bool {
    !context.request_id.trim().is_empty()
        && !context.subject_id.trim().is_empty()
        && !context.trace_id.trim().is_empty()
        && context.expires_at_ms >= context.issued_at_ms
}

pub(crate) fn context_scopes_within_authority(
    context: &RequestContext,
    verified_context: &VerifiedRequestContext,
) -> bool {
    context
        .scopes
        .iter()
        .all(|scope| !scope.trim().is_empty() && verified_context.allows_action(scope))
}
