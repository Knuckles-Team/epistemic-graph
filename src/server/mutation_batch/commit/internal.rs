use std::sync::{Arc, OnceLock};

use sha2::{Digest, Sha256};
use tokio::sync::{Mutex, OwnedMutexGuard};

use crate::graph::GraphCore;
use crate::mutation_batch::{MutationStateDescriptor, MutationSurface};
use crate::protocol::{Method, ResultPayload};
use crate::server::persistence::PersistenceBackend;
use eg_types::contract::Nonce;

use super::super::compile::{authoritative_graph_version, compile_methods, CompileBatch};

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

/// Parameters for one internal graph write. Keeping the coordinator's options
/// in one object makes the strict promotion path share exactly the same commit
/// protocol as ordinary internal writes.
pub(crate) struct InternalGraphCommitRequest<'a> {
    pub(crate) persistence: Option<&'a Arc<dyn PersistenceBackend>>,
    pub(crate) core: &'a Arc<GraphCore>,
    pub(crate) request_id: u64,
    pub(crate) principal: Option<&'a str>,
    pub(crate) graph: &'a str,
    pub(crate) batch_id: &'a str,
    pub(crate) methods: Vec<Method>,
    pub(crate) result: &'a ResultPayload,
    attempt_nonce: Option<Nonce>,
    strict_promotion: bool,
}

impl<'a> InternalGraphCommitRequest<'a> {
    pub(crate) fn new(
        persistence: Option<&'a Arc<dyn PersistenceBackend>>,
        core: &'a Arc<GraphCore>,
        request_id: u64,
        principal: Option<&'a str>,
        graph: &'a str,
        batch_id: &'a str,
        methods: Vec<Method>,
        result: &'a ResultPayload,
    ) -> Self {
        Self {
            persistence,
            core,
            request_id,
            principal,
            graph,
            batch_id,
            methods,
            result,
            attempt_nonce: None,
            strict_promotion: false,
        }
    }

    pub(crate) fn with_attempt_nonce(mut self, attempt_nonce: Option<Nonce>) -> Self {
        self.attempt_nonce = attempt_nonce;
        self
    }

    pub(crate) fn with_strict_promotion(mut self) -> Self {
        self.strict_promotion = true;
        self
    }
}

struct InternalGraphCommit<'a> {
    persistence: &'a Arc<dyn PersistenceBackend>,
    core: &'a Arc<GraphCore>,
    request_id: u64,
    principal: &'a str,
    graph: &'a str,
    batch_id: &'a str,
    methods: Vec<Method>,
    result: &'a ResultPayload,
    attempt_nonce: Option<Nonce>,
    strict_promotion: bool,
    graph_fname: String,
}

/// Commit an engine-internal graph write-set (for example an asynchronous job
/// result) through the same staged-state MutationBatch authority as public
/// runtime-result mutations. Payload-bearing graph methods are represented by
/// opaque digests in coordinator metadata; their values exist only in the
/// authoritative graph image, avoiding a second PII-bearing copy in status/outbox
/// tables.
pub(crate) async fn commit_internal_graph_methods(
    request: InternalGraphCommitRequest<'_>,
) -> Result<crate::mutation_batch::MutationBatchCommit, String> {
    commit_internal_graph_methods_with_nonce_mode(request).await
}

pub(crate) async fn commit_internal_graph_methods_with_nonce(
    request: InternalGraphCommitRequest<'_>,
) -> Result<crate::mutation_batch::MutationBatchCommit, String> {
    commit_internal_graph_methods_with_nonce_mode(request).await
}

pub(super) async fn commit_internal_graph_methods_with_nonce_mode(
    request: InternalGraphCommitRequest<'_>,
) -> Result<crate::mutation_batch::MutationBatchCommit, String> {
    let persistence = request.persistence.ok_or_else(|| {
        "internal graph write requires an authoritative MutationBatch backend".to_string()
    })?;
    // Internal graph commits share the authoritative image with the ordinary gateway and
    // transaction commit paths. Hold the same per-graph lane from version discovery through
    // the durable commit and serving publication; otherwise two internal callers can both
    // validate one source version and one can overwrite the other's state transition.
    let _graph_guard = lock_graph(request.graph).await;
    let principal = request
        .principal
        .ok_or_else(|| "internal graph write requires a verified principal".to_string())?;
    let graph_fname = crate::persist::sanitize(request.graph);
    let record = persistence
        .read_mutation_batch(&graph_fname, request.batch_id)
        .await?;
    let input = InternalGraphCommit {
        persistence,
        core: request.core,
        request_id: request.request_id,
        principal,
        graph: request.graph,
        batch_id: request.batch_id,
        methods: request.methods,
        result: request.result,
        attempt_nonce: request.attempt_nonce,
        strict_promotion: request.strict_promotion,
        graph_fname,
    };
    match record {
        Some(record) => replay_internal_graph(input, record).await,
        None => commit_fresh_internal_graph(input).await,
    }
}

async fn replay_internal_graph(
    input: InternalGraphCommit<'_>,
    record: eg_types::MutationBatchRecord,
) -> Result<crate::mutation_batch::MutationBatchCommit, String> {
    let InternalGraphCommit {
        persistence,
        core,
        request_id,
        principal,
        graph,
        batch_id,
        methods,
        result,
        attempt_nonce,
        graph_fname,
        ..
    } = input;
    let mut descriptor = record
        .batch
        .authoritative_state
        .clone()
        .ok_or_else(|| "internal child receipt has no authoritative state".to_string())?;
    let (snapshot, version) = persistence
        .read_authoritative_graph_snapshot(&graph_fname)
        .await?
        .ok_or_else(|| "committed internal graph image is missing".to_string())?;
    descriptor.source_graph_version = version;
    descriptor.target_graph_version = version
        .checked_add(1)
        .ok_or_else(|| "authoritative graph version overflow".to_string())?;
    let created_at_ms = crate::server::dispatch::authoritative_now_ms();
    let probe = compile_methods(
        CompileBatch {
            batch_id,
            request_id,
            attempt_nonce,
            principal: Some(principal),
            tenant: graph,
            graph,
            placement_epoch: 0,
            idempotency_key: batch_id,
            expected_graph_version: Some(version),
            fencing_token: None,
            created_at_ms,
            default_surface: MutationSurface::Job,
            authoritative_state: Some(descriptor),
        },
        methods,
    )?;
    let committed = persistence
        .commit_mutation_batch_state(&graph_fname, &probe, Vec::new(), None, created_at_ms, true)
        .await?;
    if !committed.replayed {
        return Err("internal child replay probe unexpectedly committed fresh work".to_string());
    }
    let expected_result = rmp_serde::to_vec_named(result).map_err(|error| error.to_string())?;
    if committed.record.result_msgpack.as_deref() != Some(expected_result.as_slice()) {
        return Err("internal child receipt has a conflicting terminal result".to_string());
    }
    let committed_descriptor = committed
        .record
        .batch
        .authoritative_state
        .as_ref()
        .ok_or_else(|| "internal child receipt has no authoritative state".to_string())?;
    install_validated_internal_replay_snapshot(core, snapshot, version, committed_descriptor)?;
    Ok(committed)
}

fn stage_internal_graph(
    core: &GraphCore,
    base_snapshot: crate::graph::GraphSnapshot,
    source_version: u64,
    methods: Vec<Method>,
    strict_promotion: bool,
) -> Result<
    (
        crate::graph_delta::GraphRowDelta,
        MutationStateDescriptor,
        Vec<Method>,
        Vec<u8>,
    ),
    String,
> {
    let base_snapshot_for_delta = base_snapshot.clone();
    let staged = GraphCore::from_snapshot(base_snapshot, source_version)?;
    for method in &methods {
        if strict_promotion {
            #[cfg(feature = "program-optimization")]
            super::program::apply_promotion_projectable_method(&staged, method)?;
            #[cfg(not(feature = "program-optimization"))]
            return Err("strict promotion is unavailable in this build".to_string());
        } else {
            apply_projectable_method(&staged, method)?;
        }
    }
    let staged_snapshot = staged.snapshot();
    let row_delta =
        crate::graph_delta::GraphRowDelta::between(&base_snapshot_for_delta, &staged_snapshot)?;
    let state_msgpack = row_delta.to_msgpack()?;
    let target_graph_version = source_version
        .checked_add(1)
        .ok_or_else(|| "authoritative graph version overflow".to_string())?;
    let descriptor = MutationStateDescriptor {
        algorithm: crate::graph_delta::ROW_DELTA_ALGORITHM.to_string(),
        digest: hex::encode(Sha256::digest(&state_msgpack)),
        source_graph_version: source_version,
        target_graph_version,
    };
    Ok((row_delta, descriptor, methods, state_msgpack))
}

struct PreparedInternalGraphCommit<'a> {
    persistence: &'a Arc<dyn PersistenceBackend>,
    core: &'a Arc<GraphCore>,
    request_id: u64,
    principal: &'a str,
    graph: &'a str,
    batch_id: &'a str,
    methods: Vec<Method>,
    result: &'a ResultPayload,
    attempt_nonce: Option<Nonce>,
    graph_fname: String,
    source_version: u64,
    row_delta: crate::graph_delta::GraphRowDelta,
    state_msgpack: Vec<u8>,
    descriptor: MutationStateDescriptor,
}

async fn commit_fresh_internal_graph(
    input: InternalGraphCommit<'_>,
) -> Result<crate::mutation_batch::MutationBatchCommit, String> {
    let InternalGraphCommit {
        persistence,
        core,
        request_id,
        principal,
        graph,
        batch_id,
        methods,
        result,
        attempt_nonce,
        strict_promotion,
        graph_fname,
    } = input;
    let (base_snapshot, source_version) = match persistence
        .read_authoritative_graph_snapshot(&graph_fname)
        .await?
    {
        Some(value) => value,
        None => (
            core.snapshot(),
            authoritative_graph_version(persistence, &graph_fname, core).await?,
        ),
    };
    let (row_delta, descriptor, methods, state_msgpack) = stage_internal_graph(
        core,
        base_snapshot,
        source_version,
        methods,
        strict_promotion,
    )?;
    commit_prepared_internal_graph(PreparedInternalGraphCommit {
        persistence,
        core,
        request_id,
        principal,
        graph,
        batch_id,
        methods,
        result,
        attempt_nonce,
        graph_fname,
        source_version,
        row_delta,
        state_msgpack,
        descriptor,
    })
    .await
}

async fn commit_prepared_internal_graph(
    input: PreparedInternalGraphCommit<'_>,
) -> Result<crate::mutation_batch::MutationBatchCommit, String> {
    let PreparedInternalGraphCommit {
        persistence,
        core,
        request_id,
        principal,
        graph,
        batch_id,
        methods,
        result,
        attempt_nonce,
        graph_fname,
        source_version,
        row_delta,
        state_msgpack,
        descriptor,
    } = input;
    let created_at_ms = crate::server::dispatch::authoritative_now_ms();
    let batch = compile_methods(
        CompileBatch {
            batch_id,
            request_id,
            attempt_nonce,
            principal: Some(principal),
            tenant: graph,
            graph,
            placement_epoch: 0,
            idempotency_key: batch_id,
            expected_graph_version: Some(source_version),
            fencing_token: None,
            created_at_ms,
            default_surface: MutationSurface::Job,
            authoritative_state: Some(descriptor.clone()),
        },
        methods,
    )?;
    let result_msgpack = rmp_serde::to_vec_named(result).map_err(|error| error.to_string())?;
    let committed = persistence
        .commit_mutation_batch_state(
            &graph_fname,
            &batch,
            state_msgpack,
            Some(&result_msgpack),
            created_at_ms,
            // Preserves this function's existing behavior: internal coordinator
            // write-sets are policy-audited.
            true,
        )
        .await?;
    committed.validate()?;
    if committed.replayed {
        let (snapshot, version) = persistence
            .read_authoritative_graph_snapshot(&graph_fname)
            .await?
            .ok_or_else(|| "committed internal graph image is missing".to_string())?;
        install_validated_internal_replay_snapshot(core, snapshot, version, &descriptor)?;
    } else {
        crate::server::mutation::publish_committed_row_delta(
            persistence,
            &graph_fname,
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
/// A digest of everything a [`crate::graph::GraphSnapshot`] persists, in a
/// CANONICAL order.
fn canonical_graph_image_digest(snapshot: &crate::graph::GraphSnapshot) -> Result<String, String> {
    let mut nodes: Vec<(&str, &[u8])> = snapshot
        .nodes
        .iter()
        .map(|(node_id, properties)| (node_id.as_str(), properties.as_slice()))
        .collect();
    nodes.sort_unstable();
    let mut edges: Vec<(&str, &str, &[u8])> = snapshot
        .edges
        .iter()
        .map(|(source, target, properties)| {
            (source.as_str(), target.as_str(), properties.as_slice())
        })
        .collect();
    edges.sort_unstable();
    let mut embeddings = snapshot.semantic_store.embeddings_snapshot();
    embeddings.sort_by(|left, right| left.0.cmp(&right.0));
    let canonical = (
        snapshot.schema_version,
        &snapshot.integrity_policy,
        nodes,
        edges,
        // The ledger is an append-ordered log: its order IS its content.
        &snapshot.ledger,
        snapshot.semantic_store.space(),
        embeddings,
    );
    let bytes = rmp_serde::to_vec(&canonical)
        .map_err(|error| format!("canonical graph image encode failed: {error}"))?;
    Ok(hex::encode(Sha256::digest(bytes)))
}

fn install_validated_internal_replay_snapshot(
    core: &GraphCore,
    snapshot: crate::graph::GraphSnapshot,
    version: u64,
    descriptor: &MutationStateDescriptor,
) -> Result<(), String> {
    if descriptor.algorithm != crate::graph_delta::ROW_DELTA_ALGORITHM
        || version < descriptor.target_graph_version
    {
        return Err(
            "committed internal graph image does not match its state transition".to_string(),
        );
    }
    let current = core.version();
    if current == descriptor.source_graph_version && version == descriptor.target_graph_version {
        return install_replay_at_source(core, snapshot, version, descriptor);
    }
    if current == descriptor.target_graph_version && version == descriptor.target_graph_version {
        return verify_current_replay(core, snapshot);
    }
    if current > descriptor.target_graph_version && version >= current {
        return verify_newer_replay(core, snapshot);
    }
    Err("serving graph version cannot accept the committed replay".to_string())
}

fn install_replay_at_source(
    core: &GraphCore,
    snapshot: crate::graph::GraphSnapshot,
    version: u64,
    descriptor: &MutationStateDescriptor,
) -> Result<(), String> {
    let delta = crate::graph_delta::GraphRowDelta::between(&core.snapshot(), &snapshot)?;
    let bytes = delta.to_msgpack()?;
    if hex::encode(Sha256::digest(bytes)) != descriptor.digest {
        return Err("committed internal graph image does not match its state digest".to_string());
    }
    core.install_committed_snapshot(snapshot, version)
}

fn verify_current_replay(
    core: &GraphCore,
    snapshot: crate::graph::GraphSnapshot,
) -> Result<(), String> {
    if canonical_graph_image_digest(&core.snapshot())? != canonical_graph_image_digest(&snapshot)? {
        return Err("serving graph image differs from its committed replay".to_string());
    }
    Ok(())
}

fn verify_newer_replay(
    core: &GraphCore,
    snapshot: crate::graph::GraphSnapshot,
) -> Result<(), String> {
    if canonical_graph_image_digest(&core.snapshot())? != canonical_graph_image_digest(&snapshot)? {
        return Err("serving graph image differs from its newer authoritative state".to_string());
    }
    Ok(())
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
