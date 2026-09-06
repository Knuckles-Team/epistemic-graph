//! Durable commit, serialization, and serving-projection publication.

use std::sync::{Arc, OnceLock};

use tokio::sync::{Mutex, OwnedMutexGuard};

use crate::change_envelope::ChangeEnvelope;
use crate::graph::GraphCore;
use crate::mutation_batch::{LogicalName, MutationStateDescriptor, MutationSurface};
use crate::protocol::{Method, ResultPayload};
use crate::server::persistence::PersistenceBackend;

use super::compile::{authoritative_graph_version, compile_methods, CompileBatch};
use super::digest::{lifecycle_batch_id, principal_fingerprint, work_item_batch_identity};

/// One deterministic async serialization lane for each logical graph. Transaction
/// Commit and the ordinary mutation gateway both acquire it, so OCC validation
/// cannot race a gateway write while its durable-before-RAM batch is in flight. A
/// fixed number of deterministic stripes bounds memory across create/delete churn; a hash
/// collision only serializes two unrelated graphs and cannot weaken correctness.
pub(crate) async fn lock_graph(graph: &str) -> OwnedMutexGuard<()> {
    const STRIPES: usize = 1024;
    static LOCKS: OnceLock<Vec<Arc<Mutex<()>>>> = OnceLock::new();
    let locks = LOCKS.get_or_init(|| (0..STRIPES).map(|_| Arc::new(Mutex::new(()))).collect());
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in graph.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    let lock = Arc::clone(&locks[(hash as usize) % STRIPES]);
    lock.lock_owned().await
}

/// Commit an engine-internal graph write-set (for example an asynchronous job
/// result) through the same staged-state MutationBatch authority as public
/// runtime-result mutations.  Payload-bearing graph methods are represented by
/// opaque digests in coordinator metadata; their values exist only in the
/// authoritative graph image, avoiding a second PII-bearing copy in status/outbox
/// tables.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn commit_internal_graph_methods(
    persistence: Option<&Arc<dyn PersistenceBackend>>,
    core: &Arc<GraphCore>,
    request_id: u64,
    principal: Option<&str>,
    graph: &str,
    batch_id: &str,
    methods: Vec<Method>,
    result: &ResultPayload,
) -> Result<crate::mutation_batch::MutationBatchCommit, String> {
    let persistence = persistence.ok_or_else(|| {
        "internal graph write requires an authoritative MutationBatch backend".to_string()
    })?;
    let _guard = lock_graph(graph).await;
    let fname = crate::persist::sanitize(graph);
    let expected_principal = principal_fingerprint(
        principal
            .ok_or_else(|| "internal graph write requires a verified principal".to_string())?,
    )?;

    if let Some(record) = persistence.read_mutation_batch(&fname, batch_id).await? {
        use sha2::{Digest, Sha256};
        let operations_match = record.batch.operations.len() == methods.len()
            && record
                .batch
                .operations
                .iter()
                .zip(methods.iter())
                .enumerate()
                .all(|(ordinal, (operation, method))| {
                    let encoded = rmp_serde::to_vec_named(method).ok();
                    let expected = encoded
                        .map(|bytes| format!("sha256:{}", hex::encode(Sha256::digest(bytes))));
                    operation.ordinal == ordinal as u32
                        && matches!(
                            &operation.method,
                            Method::ApplyMutation { event_type, query }
                                if event_type == "authoritative_state_operation"
                                    && expected.as_deref() == Some(query.as_str())
                        )
                });
        if record.status != crate::mutation_batch::MutationBatchStatus::Committed
            || record.batch.batch_id != batch_id
            || record
                .batch
                .identity
                .scope()
                .graph_name()
                .map(LogicalName::as_str)
                != Some(graph)
            || record.batch.identity.tenant().as_str() != graph
            || record.batch.context.principal != expected_principal
            || !operations_match
        {
            return Err("internal child receipt does not match its parent scope".to_string());
        }
        let expected_result = rmp_serde::to_vec_named(result).map_err(|error| error.to_string())?;
        if record.result_msgpack.as_deref() != Some(expected_result.as_slice()) {
            return Err("internal child receipt has a conflicting terminal result".to_string());
        }
        let (snapshot, version) = persistence
            .read_authoritative_graph_snapshot(&fname)
            .await?
            .ok_or_else(|| "committed internal graph image is missing".to_string())?;
        core.install_committed_snapshot(snapshot, version)?;
        // `MutationBatchCommit.identity` is a new v1 field: a self-checking
        // envelope copy that must equal `record.identity` (see its doc comment
        // in `crates/eg-types/src/mutation_batch/model/records.rs`).
        return Ok(crate::mutation_batch::MutationBatchCommit {
            identity: record.identity.clone(),
            record,
            replayed: true,
        });
    }

    let (base_snapshot, source_version) = match persistence
        .read_authoritative_graph_snapshot(&fname)
        .await?
    {
        Some(value) => value,
        None => (
            core.snapshot(),
            authoritative_graph_version(persistence, &fname, core).await?,
        ),
    };
    let base_snapshot_for_delta = base_snapshot.clone();
    let staged = GraphCore::from_snapshot(base_snapshot, source_version)?;
    for method in &methods {
        apply_projectable_method(&staged, method)?;
    }
    let staged_snapshot = staged.snapshot();
    let row_delta =
        crate::graph_delta::GraphRowDelta::between(&base_snapshot_for_delta, &staged_snapshot)?;
    let state_msgpack = row_delta.to_msgpack()?;
    use sha2::{Digest, Sha256};
    let target_graph_version = source_version
        .checked_add(1)
        .ok_or_else(|| "authoritative graph version overflow".to_string())?;
    let descriptor = MutationStateDescriptor {
        algorithm: crate::graph_delta::ROW_DELTA_ALGORITHM.to_string(),
        digest: hex::encode(Sha256::digest(&state_msgpack)),
        source_graph_version: source_version,
        target_graph_version,
    };
    let created_at_ms = crate::server::dispatch::authoritative_now_ms();
    let batch = compile_methods(
        CompileBatch {
            batch_id,
            request_id,
            principal,
            tenant: graph,
            graph,
            placement_epoch: 0,
            idempotency_key: batch_id,
            expected_graph_version: Some(source_version),
            fencing_token: None,
            created_at_ms,
            default_surface: MutationSurface::Job,
            authoritative_state: Some(descriptor),
        },
        methods,
    )?;
    let result_msgpack = rmp_serde::to_vec_named(result).map_err(|error| error.to_string())?;
    let committed = persistence
        .commit_mutation_batch_state(
            &fname,
            &batch,
            state_msgpack,
            Some(&result_msgpack),
            created_at_ms,
            // Preserves this function's existing (pre-existing, out of scope here)
            // behavior exactly: every `methods` list this internal coordinator sees
            // today (Txn/2PC child write-sets, multi-graph commit slices, job-claim
            // provenance) is policy-audited == true. NOTE: `handlers::query.rs`'s
            // `RecomputeMaterialization` caller is a known exception (policy
            // `audited: false`) that this `true` does NOT correctly honor -- same
            // root cause as the TouchNodes fix elsewhere in this changeset, but
            // untested here and out of scope for this fix; left as a follow-up.
            true,
        )
        .await?;
    if committed.replayed {
        let (snapshot, version) = persistence
            .read_authoritative_graph_snapshot(&fname)
            .await?
            .ok_or_else(|| "committed internal graph image is missing".to_string())?;
        core.install_committed_snapshot(snapshot, version)?;
    } else {
        crate::server::mutation::publish_committed_row_delta(
            persistence,
            &fname,
            core,
            &row_delta,
            source_version,
        )
        .await?;
        if row_delta.preserves_node_derived_indexes() {
            core.mark_dirty_preserving_indexes();
        } else {
            core.mark_dirty();
        }
    }
    Ok(committed)
}

fn apply_projectable_method(core: &GraphCore, method: &Method) -> Result<(), String> {
    match method {
        Method::AddNode {
            node_id,
            properties_msgpack,
        } => {
            core.add_node(node_id.clone(), properties_msgpack.clone());
            Ok(())
        }
        Method::RemoveNode { node_id } => {
            core.remove_node(node_id.clone());
            Ok(())
        }
        Method::AddEdge {
            source_id,
            target_id,
            properties_msgpack,
        } => core.add_edge(
            source_id.clone(),
            target_id.clone(),
            properties_msgpack.clone(),
        ),
        Method::RemoveEdge {
            source_id,
            target_id,
        } => {
            core.remove_edge(source_id.clone(), target_id.clone());
            Ok(())
        }
        Method::CompareAndSetNodeFields {
            node_id,
            conditions_msgpack,
            updates_msgpack,
        } => {
            // Match the staged-transaction contract: malformed maps or a failed
            // predicate are a no-op CAS, not a partial child-batch failure.
            if let (Ok(conditions), Ok(updates)) = (
                eg_types::msgpack::decode_property_object(conditions_msgpack),
                eg_types::msgpack::decode_property_object(updates_msgpack),
            ) {
                let _ = core.compare_and_set_fields(node_id, &conditions, &updates);
            }
            Ok(())
        }
        Method::ClearGraph => {
            core.clear();
            Ok(())
        }
        Method::FromMsgpack { msgpack } => core.from_msgpack(msgpack),
        #[cfg(feature = "epistemic")]
        Method::RecomputeMaterialization { .. } => Ok(()),
        _ => Err("internal graph MutationBatch contains a non-projectable method".to_string()),
    }
}

/// Execute a WorkItem claim/renew/result transition inside the redb
/// MutationBatch transaction and then refresh every affected in-memory node from
/// the authoritative store. No selection or transition runs in RAM first.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn commit_work_item(
    persistence: Option<&Arc<dyn PersistenceBackend>>,
    core: &Arc<GraphCore>,
    request_id: u64,
    principal: Option<&str>,
    graph: &str,
    placement_epoch: u64,
    placement_fencing_token: Option<u64>,
    method: Method,
) -> Result<ResultPayload, String> {
    let persistence = persistence.ok_or_else(|| {
        "WorkItem mutation requires an authoritative persistence backend".to_string()
    })?;
    let tenant = match &method {
        Method::SubmitWorkItem { request } => request.context.tenant_id.clone(),
        Method::SubmitWorkItems { request } => request.context.tenant_id.clone(),
        Method::ClaimWorkItem { request } => request.tenant_ref.clone(),
        Method::CasWorkItemMetadata { request } => request.tenant_ref.clone(),
        Method::RenewWorkItemLease { tenant, .. }
        | Method::CommitWorkItemResult { tenant, .. }
        | Method::CancelWorkItem { tenant, .. }
        | Method::DeferWorkItem { tenant, .. } => tenant.clone(),
        Method::ReserveWorkItemResources { request }
        | Method::ReleaseWorkItemResources { request }
        | Method::ReclaimWorkItemResources { request } => request.tenant_ref.clone(),
        Method::UpdateResourceHost { request } => request.tenant_ref.clone(),
        _ => return Err("commit_work_item received a non-WorkItem operation".to_string()),
    };
    if tenant.trim().is_empty() {
        return Err("WorkItem mutation requires a non-empty tenant".to_string());
    }
    // WorkItem rows live in the same authoritative graph image and advance the
    // same graph version as every other MutationBatch. Keep version discovery,
    // durable commit, and RAM publication inside the shared per-graph lane so a
    // ChangeEnvelope cannot pass its version fence and then lose a redb race to
    // a background claim/renew/result transition (or vice versa).
    let _mutation_guard = lock_graph(graph).await;
    let identity = work_item_batch_identity(graph, &tenant, request_id, &method)?;
    let submit_batch = matches!(&method, Method::SubmitWorkItems { .. });
    let submit = submit_batch || matches!(&method, Method::SubmitWorkItem { .. });
    // Resource-host inventory is committed through the same native WorkItem
    // mutation lane so it receives the same durability, ordering, and audit
    // guarantees. Unlike claims and reservations, however, it has no graph-node
    // mirror to refresh after commit. Its typed result therefore intentionally
    // has no `changed_work_item_ids` field.
    let publishes_work_item_rows = !matches!(&method, Method::UpdateResourceHost { .. });
    let created_at_ms = crate::server::dispatch::authoritative_now_ms();
    let fname = crate::persist::sanitize(graph);
    // Terminal WorkItem methods carry their own lease epoch/fencing CAS -- the
    // WorkItem lease/fencing token is their real CAS guard, not this graph
    // version -- and `compute_native_terminal_work_item_cas`
    // (`src/redb_store.rs`) makes `check_occ_version_and_fence` skip comparing
    // it for them. v1's `VersionExpectation` has no "unversioned" arm available
    // to an ordinary tenant (`validate_version_expectation`), so unlike v2 this
    // can no longer be `None` for ANY WorkItem method, terminal or not: it must
    // always carry a well-formed, real version. Claim/renew retries recompute a
    // fresh one safely too, since neither the idempotency/replay identity
    // (`mutation_batch_replay_identity_keys`) nor a genuine idempotent replay
    // (short-circuited by `check_idempotency_replay` before OCC is even
    // evaluated) depend on this value matching the original commit's.
    let expected_graph_version = authoritative_graph_version(persistence, &fname, core).await?;
    let batch = compile_methods(
        CompileBatch {
            batch_id: &identity.batch_id,
            request_id: identity.durable_request_id,
            principal,
            tenant: &tenant,
            graph,
            placement_epoch,
            idempotency_key: &identity.idempotency_key,
            expected_graph_version: Some(expected_graph_version),
            fencing_token: placement_fencing_token,
            created_at_ms,
            default_surface: MutationSurface::Job,
            authoritative_state: None,
        },
        vec![method],
    )?;
    let committed = persistence
        .commit_mutation_batch(&fname, &batch, None, created_at_ms)
        .await?;
    // Durability has already advanced the authoritative graph version. Everything
    // below is RAM publication, and a failure in ANY of it must not strand the
    // serving projection one version behind the authority: `authoritative_graph_version`
    // then fails closed on every later write and the whole graph is permanently
    // read-only until it is re-materialized. Repair the projection from the same
    // authoritative image every replay path installs, then surface the original error
    // — never swallowed, and never by equalizing a version counter.
    let result = publish_committed_work_item(
        persistence,
        &fname,
        core,
        &committed,
        publishes_work_item_rows,
    )
    .await;
    match result {
        Ok(result) if committed.replayed && submit => mark_submit_replayed(result, submit_batch),
        Ok(result) => Ok(result),
        Err(error) => match reconcile_projection_from_authority(persistence, &fname, core).await {
            Ok(()) => Err(error),
            Err(repair) => Err(format!(
                "{error}; serving projection repair from authority also failed: {repair}"
            )),
        },
    }
}

/// The durable MutationBatch record retains the original successful submit
/// result so a replay can prove the exact command it deduplicated.  The wire
/// result, however, must tell the caller that this invocation replayed that
/// record rather than creating a second WorkItem.  Rewrite only this response
/// bit after the authoritative replay; never write the rewritten bytes back to
/// redb.
fn mark_submit_replayed(result: ResultPayload, batch: bool) -> Result<ResultPayload, String> {
    fn set_flags(value: &mut serde_json::Value, batch: bool) -> Result<(), String> {
        let object = value
            .as_object_mut()
            .ok_or_else(|| "replayed SubmitWorkItem result is not an object".to_string())?;
        if batch {
            object.insert("replayed".to_string(), serde_json::Value::Bool(true));
            let children = object
                .get_mut("results")
                .and_then(serde_json::Value::as_array_mut)
                .ok_or_else(|| "replayed SubmitWorkItems result has no results".to_string())?;
            for child in children {
                let child = child.as_object_mut().ok_or_else(|| {
                    "replayed SubmitWorkItems child result is not an object".to_string()
                })?;
                child.insert("created".to_string(), serde_json::Value::Bool(false));
                child.insert("replayed".to_string(), serde_json::Value::Bool(true));
            }
        } else {
            object.insert("created".to_string(), serde_json::Value::Bool(false));
            object.insert("replayed".to_string(), serde_json::Value::Bool(true));
        }
        Ok(())
    }

    match result {
        ResultPayload::Raw(bytes) => {
            let mut value: serde_json::Value = eg_types::msgpack::decode_bounded(
                &bytes,
                eg_types::msgpack::MsgpackLimits::new(4 * 1024 * 1024, 100_000, 64),
            )
            .map_err(|_| "replayed SubmitWorkItem result is corrupt".to_string())?;
            set_flags(&mut value, batch)?;
            let bytes = rmp_serde::to_vec_named(&value).map_err(|e| e.to_string())?;
            Ok(ResultPayload::Raw(bytes))
        }
        ResultPayload::PropertiesMsgpack(bytes) => {
            let mut value: serde_json::Value = eg_types::msgpack::decode_bounded(
                &bytes,
                eg_types::msgpack::MsgpackLimits::new(4 * 1024 * 1024, 100_000, 64),
            )
            .map_err(|_| "replayed SubmitWorkItem result is corrupt".to_string())?;
            set_flags(&mut value, batch)?;
            let bytes = rmp_serde::to_vec_named(&value).map_err(|e| e.to_string())?;
            Ok(ResultPayload::PropertiesMsgpack(bytes))
        }
        ResultPayload::Json(mut value) => {
            set_flags(&mut value, batch)?;
            Ok(ResultPayload::Json(value))
        }
        _ => Err("replayed SubmitWorkItem result has an invalid payload shape".to_string()),
    }
}

/// Decode a durably committed WorkItem batch's terminal result and publish its
/// changed rows into the serving projection, advancing the serving version exactly
/// once. Fallible only in the RAM-publication sense — the caller owns repairing the
/// projection from authority when this fails.
async fn publish_committed_work_item(
    persistence: &Arc<dyn PersistenceBackend>,
    graph_fname: &str,
    core: &Arc<GraphCore>,
    committed: &crate::mutation_batch::MutationBatchCommit,
    publishes_work_item_rows: bool,
) -> Result<ResultPayload, String> {
    let bytes = committed
        .record
        .result_msgpack
        .as_deref()
        .ok_or_else(|| "committed WorkItem batch has no durable result".to_string())?;
    let result: ResultPayload = eg_types::msgpack::decode_bounded(
        bytes,
        eg_types::msgpack::MsgpackLimits::new(64 * 1024 * 1024, 1_000_000, 64),
    )
    .map_err(|_| "committed WorkItem result is corrupt".to_string())?;

    if !committed.replayed {
        for node_id in changed_work_item_ids(&result, publishes_work_item_rows)? {
            let props = persistence
                .read_node(graph_fname, &node_id)
                .await?
                .ok_or_else(|| format!("committed WorkItem projection '{}' is missing", node_id))?;
            core.add_node(node_id, props);
        }
        core.mark_dirty();
    }
    Ok(result)
}

/// Re-materialize the serving projection from the authoritative durable image at the
/// authority's own version. This is the SAME primitive every idempotent-replay path
/// uses (`read_authoritative_graph_snapshot` -> `install_committed_snapshot`): it
/// installs the committed image and its committed version together, so it can never
/// silence the authority check by writing a version the durable rows do not back.
async fn reconcile_projection_from_authority(
    persistence: &Arc<dyn PersistenceBackend>,
    graph_fname: &str,
    core: &Arc<GraphCore>,
) -> Result<(), String> {
    let (snapshot, version) = persistence
        .read_authoritative_graph_snapshot(graph_fname)
        .await?
        .ok_or_else(|| "committed graph image is missing".to_string())?;
    core.install_committed_snapshot(snapshot, version)
}

pub(super) fn changed_work_item_ids(
    result: &ResultPayload,
    publishes_work_item_rows: bool,
) -> Result<Vec<String>, String> {
    fn from_json(
        value: &serde_json::Value,
        publishes_work_item_rows: bool,
    ) -> Result<Vec<String>, String> {
        if !publishes_work_item_rows {
            return if value.get("changed_work_item_ids").is_none() {
                Ok(Vec::new())
            } else {
                Err(
                    "committed resource-host result unexpectedly has changed_work_item_ids"
                        .to_string(),
                )
            };
        }
        let values = value
            .get("changed_work_item_ids")
            .ok_or_else(|| "committed WorkItem result has no changed_work_item_ids".to_string())?
            .as_array()
            .ok_or_else(|| {
                "committed WorkItem result has non-array changed_work_item_ids".to_string()
            })?;
        values
            .iter()
            .map(|value| {
                value.as_str().map(str::to_string).ok_or_else(|| {
                    "committed WorkItem result has a non-string changed id".to_string()
                })
            })
            .collect()
    }

    match result {
        ResultPayload::Json(value) => from_json(value, publishes_work_item_rows),
        // ``ResultPayload::raw`` is wire-identical to ``PropertiesMsgpack``.
        // Because ResultPayload is untagged, decoding the durable outer payload
        // can legitimately select either byte variant. Both carry the same
        // typed WorkItem result and must refresh the resident graph projection.
        ResultPayload::Raw(bytes) | ResultPayload::PropertiesMsgpack(bytes) => {
            let value: serde_json::Value = eg_types::msgpack::decode_bounded(
                bytes,
                eg_types::msgpack::MsgpackLimits::new(1024 * 1024, 10_000, 32),
            )
            .map_err(|_| "committed WorkItem inner result is corrupt".to_string())?;
            from_json(&value, publishes_work_item_rows)
        }
        _ => Err("committed WorkItem result has an invalid payload shape".to_string()),
    }
}

/// Publish the graph-row projection of a durably committed ChangeEnvelope.
/// Durable redb state remains authoritative; a failure here is repaired from the
/// transactional `engine.projection.rebuild` outbox rather than rolling back or
/// pretending the envelope did not commit.
pub(crate) fn publish_change_envelope_projection(
    core: &Arc<GraphCore>,
    envelope: &ChangeEnvelope,
) -> Result<(), String> {
    // Project into an isolated copy first. A late CAS failure or missing edge
    // endpoint must never leave the live cache with only the earlier operations
    // applied after the authoritative redb transaction committed atomically.
    // The final snapshot swap is the one publication point observed by readers.
    let source_version = core.version();
    let staged = Arc::new(GraphCore::new());
    staged.install_committed_snapshot(core.snapshot(), source_version)?;
    for operation in &envelope.mutation.operations {
        match &operation.method {
            Method::AddNode {
                node_id,
                properties_msgpack,
            } => staged.add_node(node_id.clone(), properties_msgpack.clone()),
            Method::RemoveNode { node_id } => staged.remove_node(node_id.clone()),
            Method::CompareAndSetNodeFields {
                node_id,
                conditions_msgpack,
                updates_msgpack,
            } => {
                let conditions = eg_types::msgpack::decode_property_object(conditions_msgpack)
                    .map_err(|_| "invalid committed CAS conditions".to_string())?;
                let updates = eg_types::msgpack::decode_property_object(updates_msgpack)
                    .map_err(|_| "invalid committed CAS updates".to_string())?;
                if !staged.compare_and_set_fields(node_id, &conditions, &updates) {
                    return Err(format!(
                        "committed CAS projection for '{}' no longer matches RAM",
                        node_id
                    ));
                }
            }
            Method::AddEdge {
                source_id,
                target_id,
                properties_msgpack,
            } => staged.add_edge(
                source_id.clone(),
                target_id.clone(),
                properties_msgpack.clone(),
            )?,
            Method::RemoveEdge {
                source_id,
                target_id,
            } => staged.remove_edge(source_id.clone(), target_id.clone()),
            Method::ClearGraph => staged.clear(),
            other => {
                return Err(format!(
                    "ChangeEnvelope contains a non-projectable operation in domain {:?}",
                    crate::server::mutation_batch::domain_for(other, operation.surface)
                ));
            }
        }
    }
    if core.version() != source_version {
        return Err(format!(
            "ChangeEnvelope projection raced another write: expected version {source_version}, current {}",
            core.version()
        ));
    }
    let target_graph_version = source_version
        .checked_add(1)
        .ok_or_else(|| "authoritative graph version overflow".to_string())?;
    core.install_committed_snapshot(staged.snapshot(), target_graph_version)
}

/// Commit one CreateGraph/DeleteGraph batch before the caller mutates the in-RAM
/// registry.  The redb kernel applies graph_meta/purge, status, idempotency and
/// outbox atomically; `replayed` lets the caller finish a post-commit RAM publish.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn commit_lifecycle(
    persistence: &Arc<dyn PersistenceBackend>,
    action: &str,
    request_id: u64,
    principal: Option<&str>,
    graph: &str,
    method: Method,
    result: &ResultPayload,
) -> Result<crate::mutation_batch::MutationBatchCommit, String> {
    let batch_id = lifecycle_batch_id(action, graph, request_id);
    let created_at_ms = crate::server::dispatch::authoritative_now_ms();
    let fname = crate::persist::sanitize(graph);
    // v1's `VersionExpectation` has no "unversioned" arm available to an ordinary
    // tenant, so this can no longer pass `None` for "don't care". A not-yet-created
    // graph has no MUTATION_GRAPH_VERSION row -- `read_current_mutation_graph_version`
    // (`src/redb_store.rs`) treats that as `INITIAL_GRAPH_VERSION` (0), which is
    // exactly the correct expectation for CreateGraph; DeleteGraph reads the
    // graph's real current version.
    let expected_graph_version = persistence
        .read_mutation_graph_version(&fname)
        .await?
        .unwrap_or(0);
    let batch = compile_methods(
        CompileBatch {
            batch_id: &batch_id,
            request_id,
            principal,
            tenant: graph,
            graph,
            placement_epoch: 0,
            idempotency_key: &batch_id,
            expected_graph_version: Some(expected_graph_version),
            fencing_token: None,
            created_at_ms,
            default_surface: MutationSurface::Lifecycle,
            authoritative_state: None,
        },
        vec![method],
    )?;
    let encoded_result = rmp_serde::to_vec_named(result).map_err(|e| e.to_string())?;
    persistence
        .commit_mutation_batch(&fname, &batch, Some(&encoded_result), created_at_ms)
        .await
}

/// Did this exact lifecycle request already reach its durable commit point? Used
/// when the registry already reflects Create (or no longer reflects Delete) so a
/// network retry returns the committed outcome instead of creating a second batch.
pub(crate) async fn lifecycle_was_committed(
    persistence: &Arc<dyn PersistenceBackend>,
    action: &str,
    graph: &str,
    request_id: u64,
) -> Result<bool, String> {
    let fname = crate::persist::sanitize(graph);
    let batch_id = lifecycle_batch_id(action, graph, request_id);
    let record_exists = persistence
        .read_mutation_batch(&fname, &batch_id)
        .await?
        .is_some();
    if !record_exists {
        return Ok(false);
    }
    Ok(persistence
        .read_mutation_lifecycle_head(&fname)
        .await?
        .as_deref()
        == Some(batch_id.as_str()))
}
