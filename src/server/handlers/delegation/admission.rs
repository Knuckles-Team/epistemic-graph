use super::*;

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
pub(crate) enum AdmissionError {
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
pub(crate) async fn admit_request(
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
        RetainedTarget::Graph(Box::new(
            retained_graph(
                store.as_ref(),
                ctx.verified_context.tenant(),
                &requested_target_id,
                requested_revision,
            )
            .map_err(AdmissionError::Message)?,
        ))
    } else {
        RetainedTarget::Agent(Box::new(
            retained_agent(
                store.as_ref(),
                ctx.verified_context.tenant(),
                &requested_target_id,
                requested_revision,
            )
            .map_err(AdmissionError::Message)?,
        ))
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
    Ok(Response::ok(
        ctx.req_id,
        ResultPayload::of::<eg_types::result_contract::coordination::KgDelegate>(result),
    ))
}

#[cfg(feature = "redb")]
pub(crate) async fn replay_before_library(
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
    Ok(Some(Response::ok(
        ctx.req_id,
        ResultPayload::of::<eg_types::result_contract::coordination::KgDelegate>(result),
    )))
}

#[cfg(feature = "redb")]
// `clustered` is read only by the `raft` clustered-dispatch branch below. In a
// build without that feature it is genuinely unused rather than dead: it is part
// of this handler's shared signature, and renaming it to `_clustered` would
// break the clustered build, which reads it by name.
#[cfg_attr(not(feature = "raft"), allow(unused_variables))]
pub(crate) async fn submit_native(
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
        crate::server::mutation_batch::WorkItemCommitRequest::new(
            ctx.persistence.as_ref(),
            ctx.core,
            ctx.req_id,
            Some(ctx.verified_context.idempotency_key()),
            ctx.caller,
            ctx.graph_name,
            placement_epoch,
            method,
        )
        .with_attempt_nonce(ctx.verified_context.attempt_nonce())
        .with_placement_fencing_token(placement_fence),
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
    /// Boxed because `AgentLibraryEntry` is ~900 bytes while the graph entry is
    /// a fraction of that, so an unboxed enum made every `RetainedTarget` --
    /// including every `Graph` one -- pay the agent's size. The indirection is
    /// free of consequence here: this type is a resolution result that lives
    /// only in process. It derives no `Serialize`, never reaches the wire, and
    /// never crosses a durable boundary, so boxing cannot change a stored or
    /// transmitted representation.
    Agent(Box<AgentLibraryEntry>),
    /// Boxed for the same reason and with the same safety argument as `Agent`
    /// above: ~370 bytes against a boxed sibling, and no wire or durable
    /// representation to disturb. Boxing only one arm just moves the cost to
    /// the other, so both are indirect and the enum is now two words.
    Graph(Box<eg_types::agent_graph::AgentGraphEntry>),
}

#[cfg(feature = "redb")]
impl RetainedTarget {
    pub(crate) fn actor_scope(&self) -> &str {
        match self {
            Self::Agent(entry) => &entry.actor_scope,
            Self::Graph(graph) => &graph.actor_scope,
        }
    }

    pub(crate) fn purpose_id(&self) -> &str {
        match self {
            Self::Agent(entry) => &entry.purpose_id,
            Self::Graph(graph) => &graph.purpose_id,
        }
    }

    pub(crate) fn policy_digest(&self) -> &str {
        match self {
            Self::Agent(entry) => &entry.policy_digest,
            Self::Graph(graph) => &graph.policy_digest,
        }
    }

    pub(crate) fn tenant_id(&self) -> &str {
        match self {
            Self::Agent(entry) => &entry.tenant_id,
            Self::Graph(graph) => &graph.tenant_id,
        }
    }

    pub(crate) fn as_agent(&self) -> Option<&AgentLibraryEntry> {
        match self {
            Self::Agent(entry) => Some(entry.as_ref()),
            Self::Graph(_) => None,
        }
    }
}
