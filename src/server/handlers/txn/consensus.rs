//! Private transaction consensus implementation.

use super::*;

pub(super) const CONSENSUS_TXN_SCHEMA_VERSION: u16 = 1;

/// Transient prepare result returned only to the control-group leader. The staged
/// plan is immediately sealed again for each participant command; it is never
/// written to a Raft log, receipt, trace, or diagnostic in plaintext.
#[cfg(feature = "raft")]
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ConsensusPreparedTransaction {
    schema_version: u16,
    coordinator_id: String,
    #[serde(with = "serde_bytes")]
    recovery_plan: Vec<u8>,
}

/// One engine-placed participant command built from a prepared transaction.
#[cfg(feature = "raft")]
pub(crate) struct ConsensusTransactionParticipant {
    pub(crate) coordinator_id: String,
    pub(crate) participant_id: u64,
    pub(crate) graph_name: String,
    pub(crate) graph_type: crate::protocol::GraphType,
    pub(crate) group_id: crate::raft::GroupId,
    pub(crate) placement_epoch: u64,
    pub(crate) fencing_token: Option<u64>,
    pub(crate) sealed_plan_source: Vec<u8>,
}

/// Fully resolved participant fanout. Placement comes only from the engine's
/// catalog and is revalidated by each participant state machine before prepare.
#[cfg(feature = "raft")]
pub(crate) struct ConsensusTransactionFanout {
    pub(crate) coordinator_id: String,
    pub(crate) participants: Vec<ConsensusTransactionParticipant>,
}

#[cfg(feature = "raft")]
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ConsensusParticipantPlan {
    schema_version: u16,
    coordinator_id: String,
    participant_id: u64,
    graph_name: String,
    graph_type: crate::protocol::GraphType,
    group_id: crate::raft::GroupId,
    placement_epoch: u64,
    fencing_token: Option<u64>,
    #[serde(with = "serde_bytes")]
    recovery_plan: Vec<u8>,
}

/// First phase of a clustered Commit. Every staging/control replica freezes the
/// same canonical encrypted recovery plan and returns its deterministic transient
/// body. Graph effects are deliberately absent: the request leader next drives
/// participant prepare, a control-group decision, participant commit, and final
/// parent terminalization without issuing Raft writes from state-machine apply.
#[cfg(feature = "raft")]
pub(crate) async fn prepare_consensus_commit(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    caller: Option<&str>,
    txn_id: &str,
) -> Response {
    let _coordinator_guard =
        crate::server::mutation_batch::lock_graph(&transaction_receipt_id(txn_id)).await;
    let (open, persistence, open_map) = {
        let s = state.read().await;
        (
            s.open_txns.remove(txn_id),
            s.persistence.clone(),
            s.open_txns.clone(),
        )
    };
    let (txn, receipt) = match open {
        Some((_id, txn_mutex)) => {
            match prepare_consensus_open_txn(
                state,
                req_id,
                caller,
                txn_id,
                txn_mutex,
                persistence,
                open_map,
            )
            .await
            {
                Ok(pair) => pair,
                Err(response) => return response,
            }
        }
        None => {
            match prepare_consensus_resume_txn(state, req_id, caller, txn_id, persistence).await {
                Ok(pair) => pair,
                Err(response) => return response,
            }
        }
    };

    let recovery_plan = match txn.encode_recovery_plan() {
        Ok(plan) => plan,
        Err(error) => return Response::err(req_id, error),
    };
    let prepared = ConsensusPreparedTransaction {
        schema_version: CONSENSUS_TXN_SCHEMA_VERSION,
        coordinator_id: receipt_coordinator_id(&receipt),
        recovery_plan,
    };
    match rmp_serde::to_vec_named(&prepared) {
        Ok(bytes) => Response::ok(req_id, ResultPayload::Raw(bytes)),
        Err(_) => Response::err(req_id, "consensus transaction prepare encode failed"),
    }
}

/// The `prepare_consensus_commit`-time path for a txn still open in RAM:
/// authorize the staged plan and atomically seal it as Prepared (or return
/// its replay). `Err(_)` carries the final `Response` for an early return.
pub(super) async fn prepare_consensus_open_txn(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    caller: Option<&str>,
    txn_id: &str,
    txn_mutex: parking_lot::Mutex<GraphTxnState>,
    persistence: Option<Arc<dyn crate::server::persistence::PersistenceBackend>>,
    open_map: Arc<dashmap::DashMap<String, parking_lot::Mutex<GraphTxnState>>>,
) -> Result<(GraphTxnState, TxnReceipt), Response> {
    let txn = txn_mutex.into_inner();
    let mut restore = TxnRestoreGuard::new(open_map, txn_id, txn.clone());
    if let Err(error) = authorize_txn_plan(state, caller, &txn).await {
        return Err(Response::err(req_id, error));
    }
    // B-9 note: the clustered/consensus prepare phase has no caller
    // idempotency key of its own (Raft's replicated log is the durability
    // mechanism here) -- always `None`, byte-identical to pre-B-9 behavior.
    let (receipt, replayed) = match begin_txn_receipt(
        persistence.clone(),
        req_id,
        caller,
        txn_id,
        &txn,
        None,
        None,
    ) {
        Ok(value) => value,
        Err(error) => return Err(Response::err(req_id, error)),
    };
    restore.complete();
    if let Some(result) = replayed {
        let parent_id = transaction_receipt_id(txn_id);
        if let Err(error) = cleanup_cross_shard_decision(state, &parent_id).await {
            return Err(Response::err(
                req_id,
                format!("transaction cleanup failed: {error}"),
            ));
        }
        return Err(Response::ok(req_id, result));
    }
    Ok((txn, receipt))
}

/// The `prepare_consensus_commit`-time path when no matching txn is open in
/// RAM: reconcile a crash-recovered commit, or resume a durably Prepared
/// parent. `Err(_)` carries the final `Response` for an early return.
pub(super) async fn prepare_consensus_resume_txn(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    caller: Option<&str>,
    txn_id: &str,
    persistence: Option<Arc<dyn crate::server::persistence::PersistenceBackend>>,
) -> Result<(GraphTxnState, TxnReceipt), Response> {
    match reconcile_committed_txn(state, req_id, caller, txn_id, None, None, None).await {
        Ok(Some(response)) => return Err(response),
        Ok(None) => {}
        Err(error) => {
            return Err(Response::err(
                req_id,
                format!("transaction receipt reconciliation failed: {error}"),
            ));
        }
    }
    let resumed = match resume_txn_receipt(persistence, req_id, caller, txn_id, None, None, None) {
        Ok(value) => value,
        Err(error) => return Err(Response::err(req_id, error)),
    };
    let Some((receipt, replayed, recovered)) = resumed else {
        return Err(Response::err(
            req_id,
            format!("unknown transaction '{}'", txn_id),
        ));
    };
    if let Some(result) = replayed {
        return Err(Response::ok(req_id, result));
    }
    let Some(txn) = recovered else {
        return Err(Response::err(
            req_id,
            "prepared transaction has no recovery plan",
        ));
    };
    Ok((txn, receipt))
}

#[cfg(feature = "raft")]
pub(super) fn decode_consensus_prepared(
    bytes: &[u8],
) -> Result<ConsensusPreparedTransaction, String> {
    let prepared: ConsensusPreparedTransaction =
        decode_txn_value(bytes, MAX_TXN_NESTED_BYTES, MAX_TXN_NESTED_ITEMS)?;
    if prepared.schema_version != CONSENSUS_TXN_SCHEMA_VERSION
        || prepared.recovery_plan.is_empty()
        || prepared.coordinator_id.is_empty()
    {
        return Err("consensus transaction prepare is invalid".to_string());
    }
    Ok(prepared)
}

#[cfg(feature = "raft")]
pub(super) fn consensus_participant_id(coordinator_id: &str, graph_name: &str) -> u64 {
    use sha2::{Digest, Sha256};
    let mut digest = Sha256::new();
    digest.update(b"epistemic-graph-consensus-participant-v1\0");
    digest.update((coordinator_id.len() as u64).to_be_bytes());
    digest.update(coordinator_id.as_bytes());
    digest.update((graph_name.len() as u64).to_be_bytes());
    digest.update(graph_name.as_bytes());
    let bytes = digest.finalize();
    u64::from_be_bytes(bytes[..8].try_into().expect("sha256 prefix is eight bytes"))
}

/// Resolve the complete transaction span through the engine-owned placement
/// catalog and build one sealed-command source per graph participant. No caller
/// placement hint or local hash is accepted.
#[cfg(feature = "raft")]
pub(crate) async fn build_consensus_transaction_fanout(
    state: &Arc<RwLock<ServerState>>,
    prepared_bytes: &[u8],
) -> Result<ConsensusTransactionFanout, String> {
    let prepared = decode_consensus_prepared(prepared_bytes)?;
    let txn = GraphTxnState::decode_recovery_plan(&prepared.recovery_plan, String::new())?;
    let (multi, graph_types) = {
        let current = state.read().await;
        let multi = current.multi_raft.clone().ok_or_else(|| {
            "consensus transaction requires MultiRaft placement authority".to_string()
        })?;
        let mut graph_types = std::collections::BTreeMap::new();
        for graph in txn.touched_graphs() {
            let entry = current
                .registry
                .get(&graph)
                .ok_or_else(|| format!("Graph '{}' not found", graph))?;
            graph_types.insert(graph, entry.graph_type);
        }
        (multi, graph_types)
    };

    let mut participants = Vec::with_capacity(graph_types.len());
    let mut participant_ids = std::collections::BTreeSet::new();
    for (graph_name, graph_type) in graph_types {
        let route = multi.route_graph(&graph_name).await;
        let participant_id = consensus_participant_id(&prepared.coordinator_id, &graph_name);
        if !participant_ids.insert(participant_id) {
            return Err("consensus transaction participant identity collision".to_string());
        }
        let participant_plan = ConsensusParticipantPlan {
            schema_version: CONSENSUS_TXN_SCHEMA_VERSION,
            coordinator_id: prepared.coordinator_id.clone(),
            participant_id,
            graph_name: graph_name.clone(),
            graph_type,
            group_id: route.group,
            placement_epoch: route.epoch,
            fencing_token: route.placed.then_some(route.fencing_token()),
            recovery_plan: prepared.recovery_plan.clone(),
        };
        let sealed_plan_source = rmp_serde::to_vec_named(&participant_plan)
            .map_err(|_| "consensus participant plan encode failed".to_string())?;
        participants.push(ConsensusTransactionParticipant {
            coordinator_id: prepared.coordinator_id.clone(),
            participant_id,
            graph_name,
            graph_type,
            group_id: route.group,
            placement_epoch: route.epoch,
            fencing_token: route.placed.then_some(route.fencing_token()),
            sealed_plan_source,
        });
    }
    if participants.is_empty() {
        return Err("consensus transaction has no participants".to_string());
    }
    Ok(ConsensusTransactionFanout {
        coordinator_id: prepared.coordinator_id,
        participants,
    })
}

#[cfg(feature = "raft")]
pub(super) fn decode_consensus_participant(
    bytes: &[u8],
    expected_coordinator: &str,
    expected_participant: u64,
) -> Result<(ConsensusParticipantPlan, GraphTxnState), String> {
    let plan: ConsensusParticipantPlan =
        decode_txn_value(bytes, MAX_TXN_NESTED_BYTES, MAX_TXN_NESTED_ITEMS)?;
    if plan.schema_version != CONSENSUS_TXN_SCHEMA_VERSION
        || plan.coordinator_id != expected_coordinator
        || plan.participant_id != expected_participant
        || plan.graph_name.is_empty()
        || plan.recovery_plan.is_empty()
        || (plan.placement_epoch > 0 && plan.fencing_token.is_none())
    {
        return Err("consensus transaction participant plan is invalid".to_string());
    }
    let txn = GraphTxnState::decode_recovery_plan(&plan.recovery_plan, String::new())?;
    if !txn
        .touched_graphs()
        .iter()
        .any(|graph| graph == &plan.graph_name)
    {
        return Err("consensus transaction participant is outside the prepared span".to_string());
    }
    Ok((plan, txn))
}

#[cfg(feature = "raft")]
pub(super) async fn validate_consensus_participant_placement(
    state: &Arc<RwLock<ServerState>>,
    plan: &ConsensusParticipantPlan,
    applying_group: crate::raft::GroupId,
    applying_epoch: u64,
    applying_fence: Option<u64>,
) -> Result<Arc<crate::graph::GraphCore>, String> {
    if plan.group_id != applying_group
        || plan.placement_epoch != applying_epoch
        || plan.fencing_token != applying_fence
    {
        return Err("consensus transaction participant reached the wrong group".to_string());
    }
    let multi = state
        .read()
        .await
        .multi_raft
        .clone()
        .ok_or_else(|| "consensus transaction lost placement authority".to_string())?;
    let route = multi.route_graph(&plan.graph_name).await;
    if route.group != plan.group_id
        || route.epoch != plan.placement_epoch
        || route.placed.then_some(route.fencing_token()) != plan.fencing_token
    {
        return Err("consensus transaction participant placement changed".to_string());
    }
    let current = state.read().await;
    let entry = current
        .registry
        .get(&plan.graph_name)
        .ok_or_else(|| format!("Graph '{}' not found", plan.graph_name))?;
    if entry.graph_type != plan.graph_type {
        return Err("consensus transaction participant graph type changed".to_string());
    }
    Ok(entry.core.clone())
}

#[cfg(feature = "raft")]
pub(super) fn extra_participant_is_valid(
    core: &crate::graph::GraphCore,
    methods: &[Method],
) -> bool {
    let inserts: std::collections::BTreeSet<&str> = methods
        .iter()
        .filter_map(|method| match method {
            Method::AddNode { node_id, .. } => Some(node_id.as_str()),
            _ => None,
        })
        .collect();
    methods.iter().all(|method| match method {
        Method::AddEdge {
            source_id,
            target_id,
            ..
        } => {
            (core.has_node(source_id) || inserts.contains(source_id.as_str()))
                && (core.has_node(target_id) || inserts.contains(target_id.as_str()))
        }
        _ => true,
    })
}

#[cfg(feature = "raft")]
pub(super) fn participant_methods<'a>(
    txn: &'a GraphTxnState,
    graph_name: &str,
) -> Option<&'a [Method]> {
    if txn.graph == graph_name {
        Some(&txn.write_set)
    } else {
        txn.extra_writes.get(graph_name).map(Vec::as_slice)
    }
}

#[cfg(feature = "raft")]
pub(super) fn consensus_participant_child_id(
    coordinator_id: &str,
    participant_id: u64,
    graph_name: &str,
) -> String {
    crate::server::mutation_batch::opaque_coordinator_key(
        "consensus-transaction-child",
        graph_name,
        &format!("{coordinator_id}:{participant_id}"),
    )
}

#[cfg(feature = "raft")]
pub(super) fn consensus_participant_receipt_id(
    txn: &GraphTxnState,
    coordinator_id: &str,
    participant_id: u64,
    graph_name: &str,
) -> String {
    let child = consensus_participant_child_id(coordinator_id, participant_id, graph_name);
    if txn.graph == graph_name && txn.is_cross_modal() {
        crate::server::mutation_batch::opaque_coordinator_key("crossmodal", graph_name, &child)
    } else {
        child
    }
}

/// Apply a participant PREPARE after its command is committed in the participant's
/// own Raft group. The encrypted durable intent is idempotent and byte-bound to the
/// parent plan; a conflicting retry fails closed.
#[cfg(feature = "raft")]
pub(crate) async fn apply_consensus_participant_prepare(
    state: &Arc<RwLock<ServerState>>,
    applying_group: crate::raft::GroupId,
    applying_epoch: u64,
    applying_fence: Option<u64>,
    coordinator_id: &str,
    participant_id: u64,
    plan_bytes: &[u8],
) -> Result<bool, String> {
    let _placement_guard = crate::server::txn::consensus_placement_fence_guard().await;
    let (plan, txn) = decode_consensus_participant(plan_bytes, coordinator_id, participant_id)?;
    let core = validate_consensus_participant_placement(
        state,
        &plan,
        applying_group,
        applying_epoch,
        applying_fence,
    )
    .await?;
    let backend = state
        .read()
        .await
        .persistence
        .clone()
        .ok_or_else(|| "consensus participant requires durable redb".to_string())?;
    let receipt_id =
        consensus_participant_receipt_id(&txn, coordinator_id, participant_id, &plan.graph_name);
    if backend
        .read_mutation_batch(&crate::persist::sanitize(&plan.graph_name), &receipt_id)
        .await?
        .is_some()
    {
        crate::server::txn::release_consensus_graph_fence(
            &plan.graph_name,
            coordinator_id,
            participant_id,
        );
        return Ok(true);
    }
    let methods = participant_methods(&txn, &plan.graph_name)
        .ok_or_else(|| "consensus participant has no graph slice".to_string())?;
    let valid = if txn.graph == plan.graph_name {
        txn.validate(&core)
    } else {
        extra_participant_is_valid(&core, methods)
    };
    if !valid {
        return Ok(false);
    }
    let acquired = crate::server::txn::acquire_consensus_graph_fence(
        &plan.graph_name,
        coordinator_id,
        participant_id,
    )?;
    let redb = backend
        .as_redb()
        .ok_or_else(|| "consensus participant requires durable redb".to_string())?;
    if let Some(existing_plan) = redb.xshard_prepare_get(coordinator_id, participant_id)? {
        if existing_plan != plan_bytes {
            if acquired {
                crate::server::txn::release_consensus_graph_fence(
                    &plan.graph_name,
                    coordinator_id,
                    participant_id,
                );
            }
            return Err("consensus participant prepare conflicts with durable intent".to_string());
        }
        return Ok(true);
    }
    if let Err(error) = redb
        .xshard_prepare_put(coordinator_id, participant_id, plan_bytes.to_vec())
        .await
    {
        if acquired {
            crate::server::txn::release_consensus_graph_fence(
                &plan.graph_name,
                coordinator_id,
                participant_id,
            );
        }
        return Err(error);
    }
    Ok(true)
}

#[cfg(feature = "raft")]
pub(super) fn isolate_participant_transaction(
    mut txn: GraphTxnState,
    graph_name: &str,
    core: &crate::graph::GraphCore,
) -> Result<GraphTxnState, String> {
    if txn.graph == graph_name {
        txn.extra_writes.clear();
        return Ok(txn);
    }
    let methods = txn
        .extra_writes
        .remove(graph_name)
        .ok_or_else(|| "consensus participant has no graph slice".to_string())?;
    txn.graph = graph_name.to_string();
    txn.begin_version = core.version();
    txn.write_set = methods;
    txn.read_set.clear();
    txn.predicate_reads.clear();
    txn.extra_writes.clear();
    txn.vectors.clear();
    txn.blob_refs.clear();
    txn.measurements.clear();
    txn.axioms.clear();
    txn.constructs.clear();
    txn.plan_writeback.clear();
    Ok(txn)
}

/// Apply a decided participant atomically in its owning graph/group. The child
/// batch id binds graph + participant + parent, making command and snapshot replay
/// idempotent. A missing/mismatched prepared intent never authorizes a first apply.
/// The identifying fields of a decided consensus transaction participant,
/// bundled so [`apply_consensus_participant_commit`] stays under the clippy
/// argument-count ceiling.
#[cfg(feature = "raft")]
pub(crate) struct ConsensusParticipantCommitRef<'a> {
    pub(crate) coordinator_id: &'a str,
    pub(crate) participant_id: u64,
    pub(crate) plan_bytes: &'a [u8],
}

#[cfg(feature = "raft")]
pub(crate) async fn apply_consensus_participant_commit(
    state: &Arc<RwLock<ServerState>>,
    request_id: u64,
    applying_group: crate::raft::GroupId,
    authority: &crate::raft::RaftMutationContext,
    participant: ConsensusParticipantCommitRef<'_>,
) -> Result<bool, String> {
    let ConsensusParticipantCommitRef {
        coordinator_id,
        participant_id,
        plan_bytes,
    } = participant;
    let principal =
        crate::server::mutation_batch::principal_fingerprint(&authority.principal_fingerprint)?;
    let (plan, txn) = decode_consensus_participant(plan_bytes, coordinator_id, participant_id)?;
    let core = validate_consensus_participant_placement(
        state,
        &plan,
        applying_group,
        authority.placement_epoch,
        authority.fencing_token,
    )
    .await?;
    let backend = state
        .read()
        .await
        .persistence
        .clone()
        .ok_or_else(|| "consensus participant requires durable persistence".to_string())?;
    let redb = backend
        .as_redb()
        .ok_or_else(|| "consensus participant requires durable redb".to_string())?;
    let prepared = redb.xshard_prepare_get(coordinator_id, participant_id)?;
    let child_id = consensus_participant_child_id(coordinator_id, participant_id, &plan.graph_name);
    let participant = isolate_participant_transaction(txn, &plan.graph_name, &core)?;
    let cross_modal = participant.is_cross_modal();
    let receipt_id = if cross_modal {
        crate::server::mutation_batch::opaque_coordinator_key(
            "crossmodal",
            &plan.graph_name,
            &child_id,
        )
    } else {
        child_id.clone()
    };
    let fname = crate::persist::sanitize(&plan.graph_name);
    let already_committed = backend
        .read_mutation_batch(&fname, &receipt_id)
        .await?
        .is_some();
    if !already_committed && !matches!(prepared.as_deref(), Some(bytes) if bytes == plan_bytes) {
        return Err("consensus participant commit has no matching prepared intent".to_string());
    }

    let committed = if cross_modal {
        commit_cross_modal_txn_with_nonce(
            state,
            request_id,
            Some(&principal),
            &child_id,
            participant,
            authority.attempt_nonce,
        )
        .await?
    } else if participant.write_set.is_empty() {
        true
    } else {
        crate::server::mutation_batch::commit_internal_graph_methods_with_nonce(
            crate::server::mutation_batch::InternalGraphCommitRequest::new(
                Some(&backend),
                &core,
                request_id,
                Some(&principal),
                &plan.graph_name,
                &child_id,
                participant.write_set,
                &ResultPayload::Bool(true),
            )
            .with_attempt_nonce(authority.attempt_nonce),
        )
        .await?;
        true
    };
    if committed {
        redb.xshard_prepare_clear(coordinator_id, participant_id)
            .await?;
        crate::server::txn::release_consensus_graph_fence(
            &plan.graph_name,
            coordinator_id,
            participant_id,
        );
    }
    Ok(committed)
}

#[cfg(feature = "raft")]
pub(crate) async fn apply_consensus_participant_abort(
    state: &Arc<RwLock<ServerState>>,
    coordinator_id: &str,
    participant_id: u64,
) -> Result<bool, String> {
    let backend = state
        .read()
        .await
        .persistence
        .clone()
        .ok_or_else(|| "consensus participant abort requires persistence".to_string())?;
    let redb = backend
        .as_redb()
        .ok_or_else(|| "consensus participant abort requires durable redb".to_string())?;
    let plan = redb.xshard_prepare_get(coordinator_id, participant_id)?;
    redb.xshard_prepare_clear(coordinator_id, participant_id)
        .await?;
    if let Some(plan) = plan {
        if let Ok((decoded, _)) =
            decode_consensus_participant(&plan, coordinator_id, participant_id)
        {
            crate::server::txn::release_consensus_graph_fence(
                &decoded.graph_name,
                coordinator_id,
                participant_id,
            );
        }
    }
    Ok(true)
}

#[cfg(feature = "raft")]
pub(crate) async fn apply_consensus_transaction_decision(
    state: &Arc<RwLock<ServerState>>,
    coordinator_id: &str,
    principal: &str,
    commit: bool,
) -> Result<bool, String> {
    let principal = crate::server::mutation_batch::principal_fingerprint(principal)?;
    let backend = state
        .read()
        .await
        .persistence
        .clone()
        .ok_or_else(|| "consensus transaction decision requires persistence".to_string())?;
    let redb = backend
        .as_redb()
        .ok_or_else(|| "consensus transaction decision requires durable redb".to_string())?;
    let parent = crate::server::handlers::admin::resume_named_admin_saga(
        redb,
        coordinator_id,
        Some(&principal),
    )?
    .ok_or_else(|| "consensus transaction decision has no prepared parent".to_string())?;
    if let Some(result) = parent.replayed {
        return match result {
            ResultPayload::Bool(value) if value == commit => Ok(value),
            ResultPayload::Bool(_) => {
                Err("consensus transaction decision conflicts with its parent".to_string())
            }
            _ => Err("consensus transaction parent has an invalid result".to_string()),
        };
    }
    if let Some(existing) = redb.xshard_decision_get(coordinator_id)? {
        if existing != commit {
            return Err(
                "consensus transaction decision conflicts with durable outcome".to_string(),
            );
        }
        return Ok(existing);
    }
    redb.xshard_recoverable_decision_put(coordinator_id, commit)
        .await?;
    Ok(commit)
}

#[cfg(feature = "raft")]
pub(crate) async fn apply_consensus_transaction_finalize(
    state: &Arc<RwLock<ServerState>>,
    coordinator_id: &str,
    principal: &str,
    commit: bool,
) -> Result<bool, String> {
    let principal = crate::server::mutation_batch::principal_fingerprint(principal)?;
    let backend = state
        .read()
        .await
        .persistence
        .clone()
        .ok_or_else(|| "consensus transaction finalize requires persistence".to_string())?;
    let redb = backend
        .as_redb()
        .ok_or_else(|| "consensus transaction finalize requires durable redb".to_string())?;
    let decision = redb
        .xshard_decision_get(coordinator_id)?
        .ok_or_else(|| "consensus transaction finalize has no durable decision".to_string())?;
    if decision != commit {
        return Err("consensus transaction finalize conflicts with durable decision".to_string());
    }
    let saga = crate::server::handlers::admin::resume_named_admin_saga(
        redb,
        coordinator_id,
        Some(&principal),
    )?
    .ok_or_else(|| "consensus transaction finalize has no prepared parent".to_string())?;
    let result = if let Some(result) = saga.replayed {
        result
    } else {
        crate::server::handlers::admin::finish_admin_saga(
            redb,
            saga.batch,
            saga.created_at_ms,
            ResultPayload::Bool(commit),
        )?
    };
    if !matches!(result, ResultPayload::Bool(value) if value == commit) {
        return Err("consensus transaction parent has a conflicting result".to_string());
    }
    redb.xshard_decision_clear(coordinator_id).await?;
    Ok(commit)
}
