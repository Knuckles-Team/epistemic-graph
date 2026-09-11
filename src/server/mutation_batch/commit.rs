//! Durable commit, serialization, and serving-projection publication.

use std::sync::{Arc, OnceLock};

use sha2::{Digest, Sha256};
use tokio::sync::{Mutex, OwnedMutexGuard};

use crate::change_envelope::ChangeEnvelope;
use crate::graph::GraphCore;
use crate::mutation_batch::{MutationStateDescriptor, MutationSurface};
use crate::protocol::{Method, ResultPayload};
use crate::server::persistence::PersistenceBackend;
use eg_types::contract::Nonce;

use super::compile::{authoritative_graph_version, compile_methods, CompileBatch};
use super::digest::{lifecycle_batch_id, work_item_batch_identity};

#[cfg(feature = "program-optimization")]
use eg_modality::OpaqueRef;

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
    commit_internal_graph_methods_with_nonce(
        persistence,
        core,
        request_id,
        principal,
        graph,
        batch_id,
        methods,
        result,
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn commit_internal_graph_methods_with_nonce(
    persistence: Option<&Arc<dyn PersistenceBackend>>,
    core: &Arc<GraphCore>,
    request_id: u64,
    principal: Option<&str>,
    graph: &str,
    batch_id: &str,
    methods: Vec<Method>,
    result: &ResultPayload,
    attempt_nonce: Option<Nonce>,
) -> Result<crate::mutation_batch::MutationBatchCommit, String> {
    commit_internal_graph_methods_with_nonce_mode(
        persistence,
        core,
        request_id,
        principal,
        graph,
        batch_id,
        methods,
        result,
        attempt_nonce,
        false,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn commit_internal_graph_methods_with_nonce_mode(
    persistence: Option<&Arc<dyn PersistenceBackend>>,
    core: &Arc<GraphCore>,
    request_id: u64,
    principal: Option<&str>,
    graph: &str,
    batch_id: &str,
    methods: Vec<Method>,
    result: &ResultPayload,
    attempt_nonce: Option<Nonce>,
    strict_promotion: bool,
) -> Result<crate::mutation_batch::MutationBatchCommit, String> {
    let persistence = persistence.ok_or_else(|| {
        "internal graph write requires an authoritative MutationBatch backend".to_string()
    })?;
    let _guard = lock_graph(graph).await;
    let fname = crate::persist::sanitize(graph);
    let principal = principal
        .ok_or_else(|| "internal graph write requires a verified principal".to_string())?;

    if let Some(record) = persistence.read_mutation_batch(&fname, batch_id).await? {
        let mut descriptor = record
            .batch
            .authoritative_state
            .clone()
            .ok_or_else(|| "internal child receipt has no authoritative state".to_string())?;
        let (snapshot, version) = persistence
            .read_authoritative_graph_snapshot(&fname)
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
            .commit_mutation_batch_state(&fname, &probe, Vec::new(), None, created_at_ms, true)
            .await?;
        if !committed.replayed {
            return Err(
                "internal child replay probe unexpectedly committed fresh work".to_string(),
            );
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
        return Ok(committed);
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
        if strict_promotion {
            #[cfg(feature = "program-optimization")]
            apply_promotion_projectable_method(&staged, method)?;
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
    committed.validate()?;
    if committed.replayed {
        let (snapshot, version) = persistence
            .read_authoritative_graph_snapshot(&fname)
            .await?
            .ok_or_else(|| "committed internal graph image is missing".to_string())?;
        install_validated_internal_replay_snapshot(core, snapshot, version, &descriptor)?;
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

/// Commit the selected native program revision and its result claim through the
/// same graph authority. The stable batch identity is checked before this
/// function performs any pointer/CAS validation, so a retry with a new nonce
/// returns its stored receipt even when the active pointer has since moved.
#[cfg(feature = "program-optimization")]
#[allow(clippy::too_many_arguments)]
pub(crate) async fn commit_program_promotion(
    persistence: Option<&Arc<dyn PersistenceBackend>>,
    core: &Arc<GraphCore>,
    request_id: u64,
    principal: Option<&str>,
    graph: &str,
    batch_id: &str,
    claim_methods: Vec<Method>,
    result: &ResultPayload,
    identity: &eg_program::ProgramRevisionIdentity,
    attempt_nonce: Option<Nonce>,
) -> Result<crate::mutation_batch::MutationBatchCommit, String> {
    identity
        .validate()
        .map_err(|error| format!("invalid program promotion identity: {error}"))?;
    let record = identity
        .candidate_record
        .as_ref()
        .ok_or_else(|| "program promotion identity has no durable candidate record".to_string())?;
    let claim_method = claim_methods.iter().find_map(|method| match method {
        Method::AddNode {
            node_id,
            properties_msgpack,
        } if node_id == &record.candidate_claim_ref => Some(properties_msgpack),
        _ => None,
    });
    let claim_properties = claim_method
        .ok_or_else(|| "program promotion claim has no selected candidate row".to_string())
        .and_then(|properties| {
            eg_types::msgpack::decode_property_value(properties)
                .map_err(|_| "program promotion candidate claim is not decodable".to_string())
        })?;
    let result_claim_ref = format!("jobclaim:{}", record.result_ref.as_str());
    let result_claim_properties = claim_methods.iter().find_map(|method| match method {
        Method::AddNode {
            node_id,
            properties_msgpack,
        } if node_id == &result_claim_ref => Some(properties_msgpack),
        _ => None,
    });
    let result_claim_properties = result_claim_properties
        .ok_or_else(|| "program promotion claim has no result lineage row".to_string())
        .and_then(|properties| {
            eg_types::msgpack::decode_property_value(properties)
                .map_err(|_| "program promotion result claim is not decodable".to_string())
        })?;
    validate_program_result_claim(identity, &result_claim_properties)?;
    identity
        .validate_candidate_claim(&claim_properties)
        .map_err(|error| format!("program promotion candidate claim is invalid: {error}"))?;
    if claim_methods
        .iter()
        .any(|method| !matches!(method, Method::AddNode { .. } | Method::AddEdge { .. }))
    {
        return Err("program promotion claim contains a non-claim graph method".to_string());
    }
    let mut methods = program_promotion_methods(identity)?;
    methods.extend(claim_methods);
    commit_internal_graph_methods_with_nonce_mode(
        persistence,
        core,
        request_id,
        principal,
        graph,
        batch_id,
        methods,
        result,
        attempt_nonce,
        true,
    )
    .await
}

/// A digest of everything a [`crate::graph::GraphSnapshot`] persists, in a
/// CANONICAL order.
///
/// `GraphSnapshot::to_msgpack()` is not usable as an image identity: `nodes`
/// and `edges` are `Vec`s materialized by iterating the in-memory maps, and
/// `SemanticStore` persists its `embeddings` as a `HashMap`. A snapshot built
/// fresh from the serving core and the same snapshot decoded back off the
/// ledger therefore hold the SAME rows in different orders and never share a
/// digest -- so comparing raw `to_msgpack()` bytes reported "differs" for two
/// identical graphs and made every internal replay of an already-applied
/// promotion fail closed.
///
/// Sorting the row collections (and the embeddings) first makes the comparison
/// depend on content alone. Every persisted component is still covered --
/// schema version, integrity policy, nodes, edges, the ordered ledger, the
/// embedding space and the embeddings themselves -- so this is strictly a
/// canonicalization of the existing check, not a narrower one.
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
    match core.version() {
        current
            if current == descriptor.source_graph_version
                && version == descriptor.target_graph_version =>
        {
            let delta = crate::graph_delta::GraphRowDelta::between(&core.snapshot(), &snapshot)?;
            let bytes = delta.to_msgpack()?;
            if hex::encode(Sha256::digest(bytes)) != descriptor.digest {
                return Err(
                    "committed internal graph image does not match its state digest".to_string(),
                );
            }
            core.install_committed_snapshot(snapshot, version)
        }
        current
            if current == descriptor.target_graph_version
                && version == descriptor.target_graph_version =>
        {
            if canonical_graph_image_digest(&core.snapshot())?
                != canonical_graph_image_digest(&snapshot)?
            {
                return Err("serving graph image differs from its committed replay".to_string());
            }
            Ok(())
        }
        current if current > descriptor.target_graph_version && version >= current => {
            // A later committed promotion may have advanced the graph after
            // this receipt was written.  A retry must return that receipt
            // without replacing the newer serving image with an older one.
            if canonical_graph_image_digest(&core.snapshot())?
                != canonical_graph_image_digest(&snapshot)?
            {
                return Err(
                    "serving graph image differs from its newer authoritative state".to_string(),
                );
            }
            Ok(())
        }
        _ => Err("serving graph version cannot accept the committed replay".to_string()),
    }
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

#[cfg(feature = "program-optimization")]
fn program_promotion_methods(
    identity: &eg_program::ProgramRevisionIdentity,
) -> Result<Vec<Method>, String> {
    let record = identity
        .candidate_record
        .as_ref()
        .ok_or_else(|| "program promotion identity has no durable candidate record".to_string())?;
    let mut candidate_properties =
        serde_json::to_value(record).map_err(|error| error.to_string())?;
    candidate_properties
        .as_object_mut()
        .ok_or_else(|| "program candidate record is not an object".to_string())?
        .insert(
            "type".to_string(),
            serde_json::Value::String("ProgramCandidate".to_string()),
        );
    let revision_properties = serde_json::json!({
        "type": "ProgramRevision",
        "schema_version": identity.schema_version,
        "program_ref": identity.program_ref.as_str(),
        "revision_ref": identity.revision_ref.as_str(),
        "base_revision": identity.base_revision,
        "revision": identity.revision,
        "parent_ref": identity.parent_ref.as_ref().map(|value| value.as_str()),
        "candidate_ref": identity.candidate_ref.as_str(),
        "content_digest": identity.content_digest,
        "policy": &identity.policy,
        "tool_policy_ref": identity.tool_policy_ref.as_ref().map(|value| value.as_str()),
        "model_profile_ref": identity.model_profile_ref.as_ref().map(|value| value.as_str()),
        "candidate_record": &identity.candidate_record,
    });
    let active_properties = serde_json::json!({
        "type": "ProgramActiveRevision",
        "schema_version": identity.schema_version,
        "program_ref": identity.program_ref.as_str(),
        "revision_ref": identity.revision_ref.as_str(),
        "base_revision": identity.base_revision,
        "revision": identity.revision,
        "parent_ref": identity.parent_ref.as_ref().map(|value| value.as_str()),
        "candidate_ref": identity.candidate_ref.as_str(),
        "content_digest": identity.content_digest,
        "policy": &identity.policy,
        "tool_policy_ref": identity.tool_policy_ref.as_ref().map(|value| value.as_str()),
        "model_profile_ref": identity.model_profile_ref.as_ref().map(|value| value.as_str()),
        "candidate_record": &identity.candidate_record,
    });
    let candidate_bytes =
        rmp_serde::to_vec_named(&candidate_properties).map_err(|error| error.to_string())?;
    let revision_bytes =
        rmp_serde::to_vec_named(&revision_properties).map_err(|error| error.to_string())?;
    let active_bytes =
        rmp_serde::to_vec_named(&active_properties).map_err(|error| error.to_string())?;
    let active_node = identity.active_pointer_ref();
    let mut methods = vec![
        Method::AddNode {
            node_id: identity.candidate_ref.as_str().to_string(),
            properties_msgpack: candidate_bytes,
        },
        Method::AddNode {
            node_id: identity.revision_ref.as_str().to_string(),
            properties_msgpack: revision_bytes,
        },
    ];
    for (reference, node_type) in [
        (identity.tool_policy_ref.as_ref(), "ToolPolicy"),
        (identity.model_profile_ref.as_ref(), "ModelProfile"),
    ] {
        if let Some(reference) = reference {
            let conditions = serde_json::json!({
                "type": node_type,
                "ref": reference.as_str(),
            });
            methods.push(Method::CompareAndSetNodeFields {
                node_id: reference.as_str().to_string(),
                conditions_msgpack: rmp_serde::to_vec_named(&conditions)
                    .map_err(|error| error.to_string())?,
                updates_msgpack: rmp_serde::to_vec_named(&serde_json::json!({}))
                    .map_err(|error| error.to_string())?,
            });
        }
    }
    if let Some(parent_ref) = &identity.parent_ref {
        let conditions = serde_json::json!({
            "program_ref": identity.program_ref.as_str(),
            "revision_ref": parent_ref.as_str(),
            "revision": identity.base_revision,
        });
        let conditions_msgpack =
            rmp_serde::to_vec_named(&conditions).map_err(|error| error.to_string())?;
        methods.push(Method::CompareAndSetNodeFields {
            node_id: active_node.as_str().to_string(),
            conditions_msgpack,
            updates_msgpack: active_bytes,
        });
    } else {
        methods.push(Method::AddNode {
            node_id: active_node.as_str().to_string(),
            properties_msgpack: active_bytes,
        });
    }
    Ok(methods)
}

#[cfg(feature = "program-optimization")]
fn resolve_program_binding_node(
    core: &GraphCore,
    reference: &OpaqueRef,
    expected_type: &str,
) -> Result<(), String> {
    let properties = core
        .get_node_properties(reference.as_str())
        .ok_or_else(|| {
            format!(
                "durable program binding '{}' is missing",
                reference.as_str()
            )
        })?;
    let value = eg_types::msgpack::decode_property_value(&properties)
        .map_err(|_| format!("durable program binding '{}' is not decodable", reference))?;
    let object = value
        .as_object()
        .ok_or_else(|| format!("durable program binding '{}' is not an object", reference))?;
    if object.get("type").and_then(serde_json::Value::as_str) != Some(expected_type)
        || object.get("ref").and_then(serde_json::Value::as_str) != Some(reference.as_str())
    {
        return Err(format!(
            "durable program binding '{}' has the wrong canonical type or ref",
            reference.as_str()
        ));
    }
    Ok(())
}

#[cfg(feature = "program-optimization")]
fn validate_program_result_claim(
    identity: &eg_program::ProgramRevisionIdentity,
    claim_properties: &serde_json::Value,
) -> Result<(), String> {
    let record = identity
        .candidate_record
        .as_ref()
        .ok_or_else(|| "program promotion identity has no candidate record".to_string())?;
    let object = claim_properties
        .as_object()
        .ok_or_else(|| "program result claim is not an object".to_string())?;
    if object.get("type").and_then(serde_json::Value::as_str) != Some("Claim")
        || object.get("family").and_then(serde_json::Value::as_str) != Some("program.optimization")
        || object.get("about").and_then(serde_json::Value::as_str)
            != Some(record.result_ref.as_str())
        || object.get("result_ref").and_then(serde_json::Value::as_str)
            != Some(record.result_ref.as_str())
    {
        return Err("program result claim is not bound to the promotion result".to_string());
    }
    let input_dataset_ref = object
        .get("input_dataset_ref")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| "program result claim has no input dataset binding".to_string())?;
    let input_dataset_ref = OpaqueRef::new(input_dataset_ref.to_string())
        .map_err(|_| "program result claim input dataset binding is invalid".to_string())?;
    if input_dataset_ref.namespace() != "job_input" {
        return Err(
            "program result claim input dataset binding has the wrong namespace".to_string(),
        );
    }
    let input_content_digest = object
        .get("input_content_digest")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| "program result claim has no input content digest".to_string())?;
    let input_snapshot_version = object
        .get("input_snapshot_version")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| "program result claim has no input snapshot binding".to_string())?;
    if input_content_digest.len() != 64
        || !input_content_digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
        || input_snapshot_version == 0
        || input_dataset_ref != record.result_input_dataset_ref
        || input_content_digest != record.result_input_content_digest
        || input_snapshot_version != record.result_input_snapshot_version
    {
        return Err("program result claim input snapshot binding is invalid".to_string());
    }
    let job_id = object
        .get("job_id")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| "program result claim has no job binding".to_string())?;
    let algo_family = object
        .get("algo_family")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| "program result claim has no algorithm family binding".to_string())?;
    let algo_algorithm = object
        .get("algo_algorithm")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| "program result claim has no algorithm binding".to_string())?;
    let has_complete_algo_lineage = [
        "algo_params_digest",
        "algo_code_version",
        "algo_env_version",
    ]
    .into_iter()
    .all(|field| {
        object
            .get(field)
            .and_then(serde_json::Value::as_str)
            .is_some_and(|value| !value.is_empty())
    });
    let confidence = object
        .get("confidence")
        .and_then(serde_json::Value::as_f64)
        .ok_or_else(|| "program result claim has no confidence binding".to_string())?;
    if job_id.is_empty()
        || algo_family != "program.optimization"
        || algo_algorithm.is_empty()
        || !has_complete_algo_lineage
        || !confidence.is_finite()
        || !(0.0..=1.0).contains(&confidence)
        || object
            .get("validation_state")
            .and_then(serde_json::Value::as_str)
            != Some("unvalidated")
    {
        return Err("program result claim lineage binding is invalid".to_string());
    }
    Ok(())
}

/// Resolve a promoted revision only through its durable revision, candidate,
/// and selected-result-claim chain. The analytics job row may be purged after
/// publication; these graph rows remain the immutable execution identity.
#[cfg(feature = "program-optimization")]
pub(crate) fn resolve_program_promotion_identity(
    core: &GraphCore,
    revision_ref: &OpaqueRef,
) -> Result<eg_program::ProgramRevisionIdentity, String> {
    let revision_properties = core
        .get_node_properties(revision_ref.as_str())
        .ok_or_else(|| "durable program revision is missing".to_string())?;
    let revision_value = eg_types::msgpack::decode_property_value(&revision_properties)
        .map_err(|_| "durable program revision is not decodable".to_string())?;
    let identity = eg_program::ProgramRevisionIdentity::from_durable_properties(&revision_value)
        .map_err(|error| format!("durable program revision is invalid: {error}"))?;
    if identity.revision_ref != *revision_ref {
        return Err("durable program revision id does not match its row".to_string());
    }
    let record = identity
        .candidate_record
        .as_ref()
        .ok_or_else(|| "durable program revision has no candidate record".to_string())?;
    let candidate_properties = core
        .get_node_properties(identity.candidate_ref.as_str())
        .ok_or_else(|| "durable program candidate record is missing".to_string())?;
    let candidate_value = eg_types::msgpack::decode_property_value(&candidate_properties)
        .map_err(|_| "durable program candidate record is not decodable".to_string())?;
    let mut candidate_object = candidate_value
        .as_object()
        .cloned()
        .ok_or_else(|| "durable program candidate record is not an object".to_string())?;
    if candidate_object.remove("type")
        != Some(serde_json::Value::String("ProgramCandidate".to_string()))
    {
        return Err("durable program candidate record has the wrong type".to_string());
    }
    let stored_record: eg_program::ProgramCandidateRecord =
        serde_json::from_value(serde_json::Value::Object(candidate_object))
            .map_err(|_| "durable program candidate record is invalid".to_string())?;
    stored_record
        .validate()
        .map_err(|error| format!("durable program candidate digest is invalid: {error}"))?;
    if stored_record != *record {
        return Err("durable program candidate record differs from the revision".to_string());
    }
    if let Some(reference) = record.tool_policy_ref.as_ref() {
        resolve_program_binding_node(core, reference, "ToolPolicy")?;
    }
    if let Some(reference) = record.model_profile_ref.as_ref() {
        resolve_program_binding_node(core, reference, "ModelProfile")?;
    }
    let claim_properties = core
        .get_node_properties(&record.candidate_claim_ref)
        .ok_or_else(|| "durable selected candidate claim is missing".to_string())?;
    let claim_value = eg_types::msgpack::decode_property_value(&claim_properties)
        .map_err(|_| "durable selected candidate claim is not decodable".to_string())?;
    identity
        .validate_candidate_claim(&claim_value)
        .map_err(|error| format!("durable selected candidate claim is invalid: {error}"))?;
    let result_claim_ref = format!("jobclaim:{}", record.result_ref.as_str());
    let result_claim_properties = core
        .get_node_properties(&result_claim_ref)
        .ok_or_else(|| "durable program result claim is missing".to_string())?;
    let result_claim_value = eg_types::msgpack::decode_property_value(&result_claim_properties)
        .map_err(|_| "durable program result claim is not decodable".to_string())?;
    validate_program_result_claim(&identity, &result_claim_value)?;
    Ok(identity)
}

/// Apply a promotion write-set to the isolated staging graph with fail-closed
/// AddNode and AddEdge checks. Generic internal graph writes intentionally keep
/// their historical upsert/no-op behavior; only the promotion coordinator uses
/// this strict path.
#[cfg(feature = "program-optimization")]
fn apply_promotion_projectable_method(core: &GraphCore, method: &Method) -> Result<(), String> {
    match method {
        Method::AddNode {
            node_id,
            properties_msgpack,
        } => {
            if core.has_node(node_id) {
                return if core.get_node_properties(node_id).as_deref()
                    == Some(properties_msgpack.as_slice())
                {
                    Ok(())
                } else {
                    Err(format!(
                        "promotion node '{}' already has different properties",
                        node_id
                    ))
                };
            }
            if core.create_node_if_absent(node_id.clone(), properties_msgpack.clone()) {
                Ok(())
            } else {
                Err(format!("promotion node '{}' could not be created", node_id))
            }
        }
        Method::AddEdge {
            source_id,
            target_id,
            properties_msgpack,
        } => {
            let snapshot = core.snapshot();
            let existing = snapshot.edges.iter().find_map(|(source, target, bytes)| {
                (source == source_id && target == target_id).then_some(bytes.as_ref())
            });
            match existing {
                Some(bytes) if bytes == properties_msgpack => Ok(()),
                Some(_) => Err(format!(
                    "promotion edge '{} -> {}' already has different properties",
                    source_id, target_id
                )),
                None => core.add_edge(
                    source_id.clone(),
                    target_id.clone(),
                    properties_msgpack.clone(),
                ),
            }
        }
        Method::CompareAndSetNodeFields {
            node_id,
            conditions_msgpack,
            updates_msgpack,
        } => {
            let conditions = eg_types::msgpack::decode_property_object(conditions_msgpack)
                .map_err(|_| "invalid promotion CAS conditions".to_string())?;
            let updates = eg_types::msgpack::decode_property_object(updates_msgpack)
                .map_err(|_| "invalid promotion CAS updates".to_string())?;
            if core.compare_and_set_fields(node_id, &conditions, &updates) {
                Ok(())
            } else {
                Err(format!(
                    "promotion active pointer CAS failed for '{}'",
                    node_id
                ))
            }
        }
        _ => Err("promotion write-set contains a non-projectable method".to_string()),
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
    attempt_nonce: Option<Nonce>,
    stable_idempotency_key: Option<&str>,
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
    if stable_idempotency_key.is_some_and(|key| key.trim().is_empty()) {
        return Err(
            "authenticated WorkItem mutation requires a non-empty idempotency key".to_string(),
        );
    }
    // WorkItem rows live in the same authoritative graph image and advance the
    // same graph version as every other MutationBatch. Keep version discovery,
    // durable commit, and RAM publication inside the shared per-graph lane so a
    // ChangeEnvelope cannot pass its version fence and then lose a redb race to
    // a background claim/renew/result transition (or vice versa).
    let _mutation_guard = lock_graph(graph).await;
    // Claim, renew, and metadata-CAS methods have no method-body idempotency key;
    // their old identity therefore depended on the transport request number.
    // Once authenticated, the envelope key is the retry identity and the
    // request number used by the existing privacy-safe digest must be stable
    // across a retry. Native callers keep the transport-derived identity below.
    let identity_request_id = if let Some(key) = stable_idempotency_key {
        if matches!(
            &method,
            Method::ClaimWorkItem { .. }
                | Method::RenewWorkItemLease { .. }
                | Method::CasWorkItemMetadata { .. }
        ) {
            let mut digest = Sha256::new();
            digest.update(b"epistemic-graph.authenticated-work-item.v1");
            for field in [graph.as_bytes(), tenant.as_bytes(), key.as_bytes()] {
                digest.update((field.len() as u64).to_be_bytes());
                digest.update(field);
            }
            let mut request_bytes = [0u8; 8];
            request_bytes.copy_from_slice(&digest.finalize()[..8]);
            u64::from_be_bytes(request_bytes).max(1)
        } else {
            request_id
        }
    } else {
        request_id
    };
    let identity = work_item_batch_identity(graph, &tenant, identity_request_id, &method)?;
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
            attempt_nonce,
            principal,
            tenant: &tenant,
            graph,
            placement_epoch,
            idempotency_key: stable_idempotency_key.unwrap_or(&identity.idempotency_key),
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
        // ``ResultPayload::raw`` is the one canonical binary result representation.
        // The durable outer payload decodes to it and carries the typed WorkItem
        // result that must refresh the resident graph projection.
        ResultPayload::Raw(bytes) => {
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
    attempt_nonce: Option<Nonce>,
    principal: Option<&str>,
    idempotency_key: &str,
    graph: &str,
    method: Method,
    result: &ResultPayload,
) -> Result<crate::mutation_batch::MutationBatchCommit, String> {
    let batch_id = lifecycle_batch_id(action, graph, principal, idempotency_key);
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
            attempt_nonce,
            principal,
            tenant: graph,
            graph,
            placement_epoch: 0,
            idempotency_key,
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
    attempt_nonce: Option<Nonce>,
    principal: Option<&str>,
    idempotency_key: &str,
    method: Method,
    result: &ResultPayload,
) -> Result<bool, String> {
    let fname = crate::persist::sanitize(graph);
    let batch_id = lifecycle_batch_id(action, graph, principal, idempotency_key);
    if persistence
        .read_mutation_batch(&fname, &batch_id)
        .await?
        .is_none()
    {
        return Ok(false);
    }
    let committed = commit_lifecycle(
        persistence,
        action,
        request_id,
        attempt_nonce,
        principal,
        idempotency_key,
        graph,
        method,
        result,
    )
    .await?;
    if !committed.replayed {
        return Err("lifecycle replay probe unexpectedly committed fresh work".to_string());
    }
    Ok(persistence
        .read_mutation_lifecycle_head(&fname)
        .await?
        .as_deref()
        == Some(committed.record.batch.batch_id.as_str()))
}

#[cfg(test)]
mod internal_replay_tests {
    use super::*;

    /// The opaque serving principal this test's job store is opened as.
    /// `eg_types::mutation_batch`'s `validate_serving_principal` requires the
    /// `principal:sha256:<64 hex>` shape for every durable mutation authority,
    /// exactly like `eg_jobs::dev_scope_grant::DEV_PRINCIPAL`; a human-readable
    /// label is refused by the jobs codec before the store ever opens.
    #[cfg(all(feature = "redb", feature = "program-optimization"))]
    const PROMOTION_TEST_PRINCIPAL: &str =
        "principal:sha256:9f2c1d0e4b7a836512cd94ef0a7b61d3428f5c9e0b13a6d748ff205ce9b374a1";

    #[cfg(all(feature = "redb", feature = "program-optimization"))]
    struct PromotionJobScopeVerifier;

    #[cfg(all(feature = "redb", feature = "program-optimization"))]
    impl eg_storage::ScopeGrantVerifier for PromotionJobScopeVerifier {
        fn verify(
            &self,
            _physical: &eg_storage::PhysicalStoreIdentity,
            layout: eg_storage::OwnerLayout,
            _identity: &eg_types::MutationScopeIdentity,
            principal: &str,
            proof: &[u8],
        ) -> Result<(), String> {
            if layout == eg_storage::OwnerLayout::Jobs
                && principal == PROMOTION_TEST_PRINCIPAL
                && proof == b"promotion-test-proof"
            {
                Ok(())
            } else {
                Err("promotion test scope authority rejected".to_string())
            }
        }
    }

    #[cfg(feature = "redb")]
    #[tokio::test(flavor = "multi_thread")]
    async fn internal_replay_rejects_a_conflicting_terminal_result_after_kernel_admission() {
        let dir = crate::test_support::temp_dir(
            "eg-internal-mutation-replay",
            "terminal-result-conflict",
        );
        let persistence: Arc<dyn PersistenceBackend> = Arc::new(
            crate::server::persistence::redb_backend::RedbBackend::open(
                dir.to_string_lossy().into_owned(),
                64,
            )
            .expect("open redb backend"),
        );
        let core = Arc::new(GraphCore::new());
        let method = Method::AddNode {
            node_id: "node-a".to_string(),
            properties_msgpack: Vec::new(),
        };
        let first = commit_internal_graph_methods_with_nonce(
            Some(&persistence),
            &core,
            1,
            Some("internal-caller"),
            "internal-result-graph",
            "internal-result-key",
            vec![method.clone()],
            &ResultPayload::String("first-result".to_string()),
            Some(Nonce::from_bytes([1; 32])),
        )
        .await
        .expect("first internal commit");
        assert!(!first.replayed);

        let error = commit_internal_graph_methods_with_nonce(
            Some(&persistence),
            &core,
            2,
            Some("internal-caller"),
            "internal-result-graph",
            "internal-result-key",
            vec![method],
            &ResultPayload::String("contradictory-result".to_string()),
            Some(Nonce::from_bytes([2; 32])),
        )
        .await
        .expect_err("a contradictory terminal result must not replay silently");
        assert!(error.contains("conflicting terminal result"), "{error}");
    }

    #[test]
    fn internal_replay_identity_excludes_attempt_fields_but_binds_key_actor_and_payload() {
        fn batch(
            request_id: u64,
            nonce: u8,
            principal: &str,
            key: &str,
            node: &str,
            source: u64,
        ) -> crate::mutation_batch::MutationBatch {
            compile_methods(
                CompileBatch {
                    batch_id: "internal-replay",
                    request_id,
                    attempt_nonce: Some(Nonce::from_bytes([nonce; 32])),
                    principal: Some(principal),
                    tenant: "graph-a",
                    graph: "graph-a",
                    placement_epoch: 0,
                    idempotency_key: key,
                    expected_graph_version: Some(source),
                    fencing_token: None,
                    created_at_ms: request_id,
                    default_surface: MutationSurface::Job,
                    authoritative_state: Some(MutationStateDescriptor {
                        algorithm: crate::graph_delta::ROW_DELTA_ALGORITHM.to_string(),
                        digest: "0".repeat(64),
                        source_graph_version: source,
                        target_graph_version: source + 1,
                    }),
                },
                vec![Method::RemoveNode {
                    node_id: node.to_string(),
                }],
            )
            .unwrap()
        }
        let identity = |batch: &crate::mutation_batch::MutationBatch| {
            batch
                .envelope
                .operation()
                .unwrap()
                .operation_identity()
                .unwrap()
                .digest()
                .unwrap()
        };
        let first = batch(7, 1, "caller-a", "internal-replay", "node-a", 3);
        let fresh_attempt = batch(19, 2, "caller-a", "internal-replay", "node-a", 9);
        assert_eq!(identity(&first), identity(&fresh_attempt));
        assert_ne!(
            identity(&first),
            identity(&batch(20, 3, "caller-a", "different", "node-a", 9))
        );
        assert_ne!(
            identity(&first),
            identity(&batch(20, 3, "caller-b", "internal-replay", "node-a", 9))
        );
        assert_ne!(
            identity(&first),
            identity(&batch(20, 3, "caller-a", "internal-replay", "other", 9))
        );
    }

    #[test]
    fn internal_replay_snapshot_is_digest_and_version_bound_before_install() {
        let empty = GraphCore::new().snapshot();
        let serving = GraphCore::from_snapshot(empty.clone(), 3).unwrap();
        let staged = GraphCore::from_snapshot(empty, 3).unwrap();
        apply_projectable_method(
            &staged,
            &Method::AddNode {
                node_id: "node-a".to_string(),
                properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({"value": 1}))
                    .unwrap(),
            },
        )
        .unwrap();
        let snapshot = staged.snapshot();
        let delta =
            crate::graph_delta::GraphRowDelta::between(&serving.snapshot(), &snapshot).unwrap();
        let descriptor = MutationStateDescriptor {
            algorithm: crate::graph_delta::ROW_DELTA_ALGORITHM.to_string(),
            digest: hex::encode(Sha256::digest(delta.to_msgpack().unwrap())),
            source_graph_version: 3,
            target_graph_version: 4,
        };

        let mut wrong_digest = descriptor.clone();
        wrong_digest.digest = "0".repeat(64);
        assert!(install_validated_internal_replay_snapshot(
            &serving,
            snapshot.clone(),
            4,
            &wrong_digest,
        )
        .is_err());
        assert_eq!(serving.version(), 3);
        assert!(install_validated_internal_replay_snapshot(
            &serving,
            snapshot.clone(),
            5,
            &descriptor,
        )
        .is_err());
        assert_eq!(serving.version(), 3);

        install_validated_internal_replay_snapshot(&serving, snapshot, 4, &descriptor).unwrap();
        assert_eq!(serving.version(), 4);
    }

    #[cfg(all(feature = "redb", feature = "program-optimization"))]
    #[tokio::test(flavor = "multi_thread")]
    async fn promotion_replay_and_failed_cas_preserve_authority_and_restart_identity() {
        fn identity(
            revision: u64,
            parent_ref: Option<eg_modality::OpaqueRef>,
            result_ref: eg_modality::OpaqueRef,
            result_input_dataset_ref: eg_modality::OpaqueRef,
            result_input_content_digest: &str,
            result_input_snapshot_version: u64,
            tool_policy_token: &str,
            model_profile_token: &str,
        ) -> eg_program::ProgramRevisionIdentity {
            let program_ref = eg_modality::OpaqueRef::scoped("program", &"1".repeat(64)).unwrap();
            let policy = eg_modality::PolicyEnvelope {
                tenant_ref: eg_modality::OpaqueRef::scoped("tenant", &"2".repeat(64)).unwrap(),
                access_policy_ref: eg_modality::OpaqueRef::scoped("policy", &"3".repeat(64))
                    .unwrap(),
                classification: eg_modality::Classification::Internal,
                retention_policy_ref: eg_modality::OpaqueRef::scoped("retention", &"4".repeat(64))
                    .unwrap(),
                deletion_policy_ref: eg_modality::OpaqueRef::scoped("deletion", &"5".repeat(64))
                    .unwrap(),
                legal_hold_ref: None,
                purpose_refs: vec![
                    eg_modality::OpaqueRef::scoped("purpose", &"6".repeat(64)).unwrap()
                ],
            };
            let tool_policy_ref =
                Some(eg_modality::OpaqueRef::scoped("tool_policy", tool_policy_token).unwrap());
            let model_profile_ref =
                Some(eg_modality::OpaqueRef::scoped("model_profile", model_profile_token).unwrap());
            let demonstration_refs =
                vec![eg_modality::OpaqueRef::scoped("example", &"9".repeat(64)).unwrap()];
            let corpus_ref = eg_modality::OpaqueRef::scoped("corpus", &"a".repeat(64)).unwrap();
            let seed: u64 = 17;
            let program = eg_program::ProgramRevision {
                schema_version: eg_program::PROGRAM_SCHEMA_VERSION,
                program_ref: program_ref.clone(),
                revision: revision - 1,
                parent_ref: parent_ref.clone(),
                signature: eg_program::SignatureSpec {
                    signature_ref: eg_modality::OpaqueRef::scoped("signature", &"9".repeat(64))
                        .unwrap(),
                    instruction_ref: eg_modality::OpaqueRef::scoped("instruction", &"a".repeat(64))
                        .unwrap(),
                    fields: vec![
                        eg_program::FieldSpec {
                            name: "input".to_string(),
                            role: eg_program::FieldRole::Input,
                            schema_ref: eg_modality::OpaqueRef::scoped("schema", &"b".repeat(64))
                                .unwrap(),
                            description_ref: None,
                            required: true,
                        },
                        eg_program::FieldSpec {
                            name: "output".to_string(),
                            role: eg_program::FieldRole::Output,
                            schema_ref: eg_modality::OpaqueRef::scoped("schema", &"c".repeat(64))
                                .unwrap(),
                            description_ref: None,
                            required: true,
                        },
                    ],
                },
                module: eg_program::ModuleKind::Predict,
                adapter: eg_program::AdapterKind::Chat,
                tool_refs: Vec::new(),
                policy: policy.clone(),
            };
            let mut digest = Sha256::new();
            digest.update(b"eg-program.candidate.v3\0");
            digest.update(program.program_ref.as_str().as_bytes());
            digest.update(program.revision.to_le_bytes());
            digest.update(program.signature.signature_ref.as_str().as_bytes());
            digest.update(program.signature.instruction_ref.as_str().as_bytes());
            digest.update(program.module.as_str().as_bytes());
            digest.update(program.adapter.as_str().as_bytes());
            digest.update(program.policy.tenant_ref.as_str().as_bytes());
            digest.update(program.policy.access_policy_ref.as_str().as_bytes());
            digest.update(corpus_ref.as_str().as_bytes());
            digest.update(result_input_snapshot_version.to_le_bytes());
            digest.update(
                eg_program::OptimizerKind::LabeledFewShot
                    .as_str()
                    .as_bytes(),
            );
            digest.update(seed.to_le_bytes());
            digest.update(eg_program::CandidateRole::Proposal.as_str().as_bytes());
            digest.update(b"demonstrations".len().to_le_bytes());
            digest.update(b"demonstrations");
            digest.update((demonstration_refs.len() as u64).to_le_bytes());
            for reference in &demonstration_refs {
                digest.update((reference.as_str().len() as u64).to_le_bytes());
                digest.update(reference.as_str().as_bytes());
            }
            for label in [b"artifacts".as_slice(), b"composition".as_slice()] {
                digest.update((label.len() as u64).to_le_bytes());
                digest.update(label);
                digest.update(0_u64.to_le_bytes());
            }
            for (label, reference) in [
                (b"instruction".as_slice(), None),
                (b"tool_policy".as_slice(), tool_policy_ref.as_ref()),
                (b"model_profile".as_slice(), model_profile_ref.as_ref()),
            ] {
                digest.update((label.len() as u64).to_le_bytes());
                digest.update(label);
                digest.update([u8::from(reference.is_some())]);
                if let Some(reference) = reference {
                    digest.update((reference.as_str().len() as u64).to_le_bytes());
                    digest.update(reference.as_str().as_bytes());
                }
            }
            digest.update((b"modalities".len() as u64).to_le_bytes());
            digest.update(b"modalities");
            digest.update(1_u64.to_le_bytes());
            digest.update((b"text".len() as u64).to_le_bytes());
            digest.update(b"text");
            let content_digest = hex::encode(digest.finalize());
            let candidate_ref =
                eg_modality::OpaqueRef::scoped("program_candidate", &content_digest).unwrap();
            let candidate = eg_program::ProgramCandidate {
                candidate_ref,
                program_ref,
                optimizer: eg_program::OptimizerKind::LabeledFewShot,
                role: eg_program::CandidateRole::Proposal,
                demonstration_refs,
                artifact_refs: Vec::new(),
                composition_refs: Vec::new(),
                instruction_ref: None,
                tool_policy_ref,
                model_profile_ref,
                modalities: std::collections::BTreeSet::from([eg_program::ProgramModality::Text]),
                policy,
                content_digest,
                evaluation: None,
            };
            eg_program::ProgramRevisionIdentity::from_candidate_with_binding(
                &program,
                &candidate,
                parent_ref,
                eg_program::ProgramResultInput {
                    result_ref,
                    dataset_ref: result_input_dataset_ref,
                    content_digest: result_input_content_digest.to_string(),
                    snapshot_version: result_input_snapshot_version,
                },
                eg_program::ProgramCorpusBinding {
                    corpus_ref,
                    snapshot_version: result_input_snapshot_version,
                },
                seed,
            )
            .expect("construct promotion identity from the public constructor")
        }

        fn candidate_knowledge(
            identity: &eg_program::ProgramRevisionIdentity,
            evidence_ref: &str,
            promotion_identity: serde_json::Value,
        ) -> serde_json::Value {
            let record = identity
                .candidate_record
                .as_ref()
                .expect("promotion test identity has a candidate record");
            let references = |values: &[eg_modality::OpaqueRef]| {
                serde_json::Value::Array(
                    values
                        .iter()
                        .map(|reference| serde_json::json!(reference.as_str()))
                        .collect(),
                )
            };
            let optional_reference = |reference: Option<&eg_modality::OpaqueRef>| {
                reference.map_or(serde_json::Value::Null, |value| {
                    serde_json::json!(value.as_str())
                })
            };
            serde_json::json!({
                "id": record.candidate_ref.as_str(),
                "kind": "program_candidate",
                "confidence": 0.0,
                "evidence_refs": [evidence_ref],
                "source_refs": [evidence_ref],
                "proof_ids": [],
                "contradiction_ids": [],
                "program_ref": record.program_ref.as_str(),
                "optimizer": record.optimizer.as_str(),
                "execution": record.optimizer.execution().as_str(),
                "candidate_role": record.role.as_str(),
                "demonstration_refs": references(&record.demonstration_refs),
                "artifact_refs": references(&record.artifact_refs),
                "composition_refs": references(&record.composition_refs),
                "instruction_ref": optional_reference(record.candidate_instruction_ref.as_ref()),
                "tool_policy_ref": optional_reference(record.tool_policy_ref.as_ref()),
                "model_profile_ref": optional_reference(record.model_profile_ref.as_ref()),
                "policy": &identity.policy,
                "modalities": &record.modalities,
                "plan_ref": serde_json::Value::Null,
                "plan_step_kinds": [],
                "plan_executors": [],
                "plan_input_refs": [],
                "plan_output_refs": [],
                "plan_depends_on": [],
                "max_operations": serde_json::Value::Null,
                "selected": true,
                "promotion_identity": promotion_identity,
            })
        }

        fn candidate_claim_method(identity: &eg_program::ProgramRevisionIdentity) -> Method {
            let record = identity
                .candidate_record
                .as_ref()
                .expect("promotion test identity has a candidate record");
            let knowledge = candidate_knowledge(
                identity,
                "eg:dataset:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                serde_json::to_value(identity).expect("promotion identity serializes"),
            );
            let properties = serde_json::json!({
                "type": "Claim",
                "family": "program.optimization",
                "about": record.candidate_ref.as_str(),
                "result_ref": record.result_ref.as_str(),
                "knowledge": knowledge,
            });
            Method::AddNode {
                node_id: record.candidate_claim_ref.clone(),
                properties_msgpack: rmp_serde::to_vec_named(&properties).unwrap(),
            }
        }

        fn actual_job_result(
            identity: &eg_program::ProgramRevisionIdentity,
            job: &eg_jobs::AnalyticsJob,
        ) -> eg_jobs::TypedJobResult {
            let knowledge = candidate_knowledge(
                identity,
                job.input_snapshot.dataset_ref.as_str(),
                serde_json::to_value(identity).expect("promotion identity serializes"),
            );
            let row = knowledge
                .as_object()
                .expect("candidate knowledge is an object")
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect();
            let schema = [
                ("id", "string", false),
                ("kind", "string", false),
                ("confidence", "float64", false),
                ("evidence_refs", "list<string>", false),
                ("source_refs", "list<string>", false),
                ("proof_ids", "list<string>", false),
                ("contradiction_ids", "list<string>", false),
                ("program_ref", "string", false),
                ("optimizer", "string", false),
                ("execution", "string", false),
                ("candidate_role", "string", true),
                ("demonstration_refs", "list<string>", false),
                ("artifact_refs", "list<string>", false),
                ("composition_refs", "list<string>", false),
                ("instruction_ref", "string", true),
                ("tool_policy_ref", "string", true),
                ("model_profile_ref", "string", true),
                ("policy", "object", true),
                ("modalities", "list<string>", false),
                ("plan_ref", "string", true),
                ("plan_step_kinds", "list<string>", false),
                ("plan_executors", "list<string>", false),
                ("plan_input_refs", "list<string>", false),
                ("plan_output_refs", "list<string>", false),
                ("plan_depends_on", "list<string>", false),
                ("max_operations", "uint64", true),
                ("selected", "bool", false),
                ("promotion_identity", "object", true),
            ]
            .into_iter()
            .map(|(name, logical_type, nullable)| eg_jobs::ResultColumn {
                name: name.to_string(),
                logical_type: logical_type.to_string(),
                nullable,
            })
            .collect();
            eg_jobs::TypedJobResult::new(
                schema,
                vec![row],
                vec![job.input_snapshot.dataset_ref.clone()],
                Vec::new(),
                None,
                None,
                eg_jobs::ReproducibilityManifest {
                    input_dataset_ref: job.input_snapshot.dataset_ref.clone(),
                    input_content_digest: job.input_snapshot.content_digest.clone(),
                    input_snapshot_version: job.input_snapshot.version,
                    algorithm_ref: format!("{}:{}", job.algo.family, job.algo.algorithm),
                    params_digest: job.algo.params_digest.clone(),
                    implementation_version: job.algo.code_version.clone(),
                    environment_version: job.algo.env_version.clone(),
                    policy_fingerprint: job.policy.policy_fingerprint.clone(),
                },
            )
            .expect("construct actual typed program result")
        }

        fn result_claim_method(identity: &eg_program::ProgramRevisionIdentity) -> Method {
            let record = identity
                .candidate_record
                .as_ref()
                .expect("promotion test identity has a candidate record");
            let result_claim_ref = format!("jobclaim:{}", record.result_ref.as_str());
            let properties = serde_json::json!({
                "type": "Claim",
                "family": "program.optimization",
                "about": record.result_ref.as_str(),
                "result_ref": record.result_ref.as_str(),
                "confidence": 0.5,
                "validation_state": "unvalidated",
                "job_id": "job-promotion-test",
                "input_dataset_ref": record.result_input_dataset_ref.as_str(),
                "input_content_digest": record.result_input_content_digest,
                "input_snapshot_version": record.result_input_snapshot_version,
                "algo_family": "program.optimization",
                "algo_algorithm": "labeled_few_shot",
                "algo_params_digest": "promotion-params",
                "algo_code_version": "promotion-test",
                "algo_env_version": "promotion-test",
            });
            Method::AddNode {
                node_id: result_claim_ref,
                properties_msgpack: rmp_serde::to_vec_named(&properties).unwrap(),
            }
        }

        fn claim_methods(identity: &eg_program::ProgramRevisionIdentity) -> Vec<Method> {
            vec![
                result_claim_method(identity),
                candidate_claim_method(identity),
            ]
        }

        let dir = crate::test_support::temp_dir("eg-program-promotion", "cas-replay");
        let persistence: Arc<dyn PersistenceBackend> = Arc::new(
            crate::server::persistence::redb_backend::RedbBackend::open(
                dir.to_string_lossy().into_owned(),
                64,
            )
            .expect("open redb backend"),
        );
        let core = Arc::new(GraphCore::new());
        let seeded_tool_policy_ref =
            eg_modality::OpaqueRef::scoped("tool_policy", &"7".repeat(64)).unwrap();
        let seeded_model_profile_ref =
            eg_modality::OpaqueRef::scoped("model_profile", &"8".repeat(64)).unwrap();
        let binding_seed_result = ResultPayload::Json(serde_json::json!({
            "seed": "governed-program-bindings"
        }));
        commit_internal_graph_methods(
            Some(&persistence),
            &core,
            900,
            Some("promotion-binding-seeder"),
            "program-graph",
            "program-binding-seed",
            vec![
                Method::AddNode {
                    node_id: seeded_tool_policy_ref.as_str().to_string(),
                    properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({
                        "type": "ToolPolicy",
                        "ref": seeded_tool_policy_ref.as_str(),
                        "policy_version": "tool-policy-v1",
                    }))
                    .unwrap(),
                },
                Method::AddNode {
                    node_id: seeded_model_profile_ref.as_str().to_string(),
                    properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({
                        "type": "ModelProfile",
                        "ref": seeded_model_profile_ref.as_str(),
                        "profile_version": "model-profile-v1",
                    }))
                    .unwrap(),
                },
            ],
            &binding_seed_result,
        )
        .await
        .expect("seed governed program bindings through an earlier durable commit");
        let job_dir = crate::test_support::temp_dir("eg-program-promotion", "job-store");
        std::fs::create_dir_all(&job_dir).expect("create separate job-store directory");
        let job_path = job_dir.join("jobs.redb");
        assert_ne!(job_path, dir.join("graph-0.redb"));
        let job_store = eg_jobs::JobStore::open(
            &job_path,
            &PromotionJobScopeVerifier,
            PROMOTION_TEST_PRINCIPAL,
            b"promotion-test-proof",
        )
        .expect("open physically separate job store");
        let dataset_ref =
            "eg:job_input:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let input_snapshot = eg_jobs::InputSnapshotHandle::new("program-graph", 7)
            .with_dataset(dataset_ref, "b".repeat(64));
        let submitted = job_store
            .submit(eg_jobs::SubmitSpec {
                input_snapshot,
                policy: eg_jobs::JobPolicy {
                    tenant: "promotion-test-tenant".to_string(),
                    actor: "promotion-test-actor".to_string(),
                    purpose: "program-promotion-test".to_string(),
                    policy_fingerprint: "promotion-policy".to_string(),
                    ..Default::default()
                },
                algo: eg_jobs::AlgoVersion {
                    family: "program.optimization".to_string(),
                    algorithm: "labeled_few_shot".to_string(),
                    params_digest: "promotion-params".to_string(),
                    code_version: "promotion-test".to_string(),
                    env_version: "promotion-test".to_string(),
                },
                input_payload: None,
                max_attempts: 1,
                backoff_ms: 0,
            })
            .expect("submit actual optimization job");
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock after epoch")
            .as_millis() as i64;
        let worker_claim = job_store
            .claim_next(
                "promotion-test-worker",
                &[],
                now_ms,
                60_000,
                eg_jobs::TenantJobQuota::default(),
            )
            .expect("claim actual optimization job")
            .expect("actual optimization job is ready");
        let actual_result_ref = eg_modality::OpaqueRef::new(worker_claim.job.result_ref())
            .expect("job store issues an opaque result reference");
        let result_input_dataset_ref = eg_modality::OpaqueRef::new(dataset_ref.to_string())
            .expect("job input dataset is an opaque reference");
        let first_identity = identity(
            2,
            None,
            actual_result_ref,
            result_input_dataset_ref.clone(),
            &"b".repeat(64),
            7,
            &"7".repeat(64),
            &"8".repeat(64),
        );
        let staged = job_store
            .stage_result_fenced(
                &submitted.job_id,
                &worker_claim.lease.worker_ref,
                worker_claim.lease.epoch,
                actual_job_result(&first_identity, &worker_claim.job),
                now_ms + 1,
            )
            .expect("stage actual typed optimization result");
        let actual_claim_plan = eg_jobs::plan_result_claim(&staged, 0.5, None)
            .expect("publish actual job result as claim methods");
        let actual_claim_methods = actual_claim_plan.methods;
        let first_result = ResultPayload::Json(serde_json::json!({"receipt": "first"}));
        let first = commit_program_promotion(
            Some(&persistence),
            &core,
            1,
            Some("program-worker"),
            "program-graph",
            "program-promotion-one",
            actual_claim_methods.clone(),
            &first_result,
            &first_identity,
            Some(Nonce::from_bytes([11; 32])),
        )
        .await
        .expect("first promotion");
        assert!(!first.replayed);

        let replay = commit_program_promotion(
            Some(&persistence),
            &core,
            2,
            Some("program-worker"),
            "program-graph",
            "program-promotion-one",
            actual_claim_methods.clone(),
            &first_result,
            &first_identity,
            Some(Nonce::from_bytes([12; 32])),
        )
        .await
        .expect("fresh nonce retries the stored promotion receipt");
        assert!(replay.replayed);
        let published = job_store
            .complete_publication_fenced(
                &submitted.job_id,
                &worker_claim.lease.worker_ref,
                worker_claim.lease.epoch,
                now_ms + 2,
            )
            .expect("complete publication after graph commit");
        assert!(matches!(
            published.state,
            eg_jobs::JobState::Succeeded { .. }
        ));

        let second_dataset_ref =
            "eg:job_input:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc";
        let second_submitted = job_store
            .submit(eg_jobs::SubmitSpec {
                input_snapshot: eg_jobs::InputSnapshotHandle::new("program-graph", 8)
                    .with_dataset(second_dataset_ref, "c".repeat(64)),
                policy: eg_jobs::JobPolicy {
                    tenant: "promotion-test-tenant".to_string(),
                    actor: "promotion-test-actor".to_string(),
                    purpose: "program-promotion-test-second".to_string(),
                    policy_fingerprint: "promotion-policy".to_string(),
                    ..Default::default()
                },
                algo: eg_jobs::AlgoVersion {
                    family: "program.optimization".to_string(),
                    algorithm: "labeled_few_shot".to_string(),
                    params_digest: "promotion-params-second".to_string(),
                    code_version: "promotion-test".to_string(),
                    env_version: "promotion-test".to_string(),
                },
                input_payload: None,
                max_attempts: 1,
                backoff_ms: 0,
            })
            .expect("submit second actual optimization job");
        let second_worker_claim = job_store
            .claim_next(
                "promotion-test-worker-2",
                &[],
                now_ms + 3,
                60_000,
                eg_jobs::TenantJobQuota::default(),
            )
            .expect("claim second actual optimization job")
            .expect("second actual optimization job is ready");
        let second_result_ref = eg_modality::OpaqueRef::new(second_worker_claim.job.result_ref())
            .expect("second job store result reference is opaque");
        let second_input_dataset_ref = eg_modality::OpaqueRef::new(second_dataset_ref.to_string())
            .expect("second job input dataset is opaque");
        let second_identity_seed = identity(
            3,
            Some(first_identity.revision_ref.clone()),
            second_result_ref,
            second_input_dataset_ref,
            &"c".repeat(64),
            8,
            &"7".repeat(64),
            &"8".repeat(64),
        );
        let second_staged = job_store
            .stage_result_fenced(
                &second_submitted.job_id,
                &second_worker_claim.lease.worker_ref,
                second_worker_claim.lease.epoch,
                actual_job_result(&second_identity_seed, &second_worker_claim.job),
                now_ms + 4,
            )
            .expect("stage second actual typed optimization result");
        let second_identity: eg_program::ProgramRevisionIdentity = second_staged
            .output
            .as_ref()
            .and_then(|output| output.rows.first())
            .and_then(|row| row.get("promotion_identity"))
            .cloned()
            .and_then(|value| serde_json::from_value(value).ok())
            .expect("derive second promotion identity from staged JobStore output");
        second_identity
            .validate()
            .expect("staged second promotion identity remains valid");
        let second_claim_plan = eg_jobs::plan_result_claim(&second_staged, 0.5, None)
            .expect("publish second actual job result as claim methods");
        let second_result = ResultPayload::Json(serde_json::json!({"receipt": "second"}));
        let second = commit_program_promotion(
            Some(&persistence),
            &core,
            3,
            Some("program-worker"),
            "program-graph",
            "program-promotion-two",
            second_claim_plan.methods,
            &second_result,
            &second_identity,
            Some(Nonce::from_bytes([14; 32])),
        )
        .await
        .expect("second promotion advances the active pointer");
        assert!(!second.replayed);
        let published_second = job_store
            .complete_publication_fenced(
                &second_submitted.job_id,
                &second_worker_claim.lease.worker_ref,
                second_worker_claim.lease.epoch,
                now_ms + 5,
            )
            .expect("complete second publication after graph commit");
        assert!(matches!(
            published_second.state,
            eg_jobs::JobState::Succeeded { .. }
        ));

        let missing_binding_identity = identity(
            3,
            Some(first_identity.revision_ref.clone()),
            second_identity
                .candidate_record
                .as_ref()
                .expect("second identity candidate record")
                .result_ref
                .clone(),
            second_identity
                .candidate_record
                .as_ref()
                .expect("second identity input binding")
                .result_input_dataset_ref
                .clone(),
            &"c".repeat(64),
            8,
            &"d".repeat(64),
            &"8".repeat(64),
        );
        let before_missing_binding = persistence
            .read_authoritative_graph_snapshot(&crate::persist::sanitize("program-graph"))
            .await
            .expect("read pre-missing-binding snapshot")
            .expect("promotion snapshot")
            .0
            .to_msgpack()
            .unwrap();
        let missing_binding = commit_program_promotion(
            Some(&persistence),
            &core,
            31,
            Some("program-worker"),
            "program-graph",
            "program-promotion-missing-binding",
            claim_methods(&missing_binding_identity),
            &ResultPayload::Json(serde_json::json!({"receipt": "missing-binding"})),
            &missing_binding_identity,
            Some(Nonce::from_bytes([31; 32])),
        )
        .await;
        assert!(
            missing_binding.is_err(),
            "missing binding must abort staging"
        );
        let after_missing_binding = persistence
            .read_authoritative_graph_snapshot(&crate::persist::sanitize("program-graph"))
            .await
            .expect("read post-missing-binding snapshot")
            .expect("promotion snapshot")
            .0
            .to_msgpack()
            .unwrap();
        assert_eq!(before_missing_binding, after_missing_binding);
        assert!(persistence
            .read_mutation_batch(
                &crate::persist::sanitize("program-graph"),
                "program-promotion-missing-binding"
            )
            .await
            .expect("read missing-binding receipt")
            .is_none());

        let wrong_tool_policy_ref =
            eg_modality::OpaqueRef::scoped("tool_policy", &"9".repeat(64)).unwrap();
        let wrong_binding_seed_result = ResultPayload::Json(serde_json::json!({
            "seed": "wrong-type-program-binding"
        }));
        commit_internal_graph_methods(
            Some(&persistence),
            &core,
            901,
            Some("promotion-binding-seeder"),
            "program-graph",
            "program-binding-wrong-type",
            vec![Method::AddNode {
                node_id: wrong_tool_policy_ref.as_str().to_string(),
                properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({
                    "type": "ModelProfile",
                    "ref": wrong_tool_policy_ref.as_str(),
                }))
                .unwrap(),
            }],
            &wrong_binding_seed_result,
        )
        .await
        .expect("seed wrong-type binding fixture through a durable commit");
        let wrong_binding_identity = identity(
            3,
            Some(first_identity.revision_ref.clone()),
            second_identity
                .candidate_record
                .as_ref()
                .expect("second identity candidate record")
                .result_ref
                .clone(),
            second_identity
                .candidate_record
                .as_ref()
                .expect("second identity input binding")
                .result_input_dataset_ref
                .clone(),
            &"c".repeat(64),
            8,
            &"9".repeat(64),
            &"8".repeat(64),
        );
        let before_wrong_binding = persistence
            .read_authoritative_graph_snapshot(&crate::persist::sanitize("program-graph"))
            .await
            .expect("read pre-wrong-binding snapshot")
            .expect("promotion snapshot")
            .0
            .to_msgpack()
            .unwrap();
        let wrong_binding = commit_program_promotion(
            Some(&persistence),
            &core,
            32,
            Some("program-worker"),
            "program-graph",
            "program-promotion-wrong-binding",
            claim_methods(&wrong_binding_identity),
            &ResultPayload::Json(serde_json::json!({"receipt": "wrong-binding"})),
            &wrong_binding_identity,
            Some(Nonce::from_bytes([32; 32])),
        )
        .await;
        assert!(
            wrong_binding.is_err(),
            "wrong binding type must abort staging"
        );
        let after_wrong_binding = persistence
            .read_authoritative_graph_snapshot(&crate::persist::sanitize("program-graph"))
            .await
            .expect("read post-wrong-binding snapshot")
            .expect("promotion snapshot")
            .0
            .to_msgpack()
            .unwrap();
        assert_eq!(before_wrong_binding, after_wrong_binding);
        assert!(persistence
            .read_mutation_batch(
                &crate::persist::sanitize("program-graph"),
                "program-promotion-wrong-binding"
            )
            .await
            .expect("read wrong-binding receipt")
            .is_none());

        let replay_after_pointer_move = commit_program_promotion(
            Some(&persistence),
            &core,
            4,
            Some("program-worker"),
            "program-graph",
            "program-promotion-one",
            // The SAME operation the key was committed with above -- the real
            // `eg_jobs::plan_result_claim` output, not the hand-built
            // `claim_methods` pair. A replay is identified by its operation, so
            // presenting different methods under the same idempotency key is an
            // IDEMPOTENCY_CONFLICT by contract (which is what the other
            // `claim_methods(..)` call sites here deliberately provoke, each
            // with its own distinct identity).
            actual_claim_methods.clone(),
            &first_result,
            &first_identity,
            Some(Nonce::from_bytes([15; 32])),
        )
        .await
        .expect("same-key retry returns the first receipt after pointer movement");
        assert!(replay_after_pointer_move.replayed);

        let fname = crate::persist::sanitize("program-graph");
        let before_failed = persistence
            .read_authoritative_graph_snapshot(&fname)
            .await
            .expect("read pre-CAS-failure snapshot")
            .expect("promotion snapshot");
        let before_failed_bytes = before_failed.0.to_msgpack().unwrap();
        let failed_identity = identity(
            3,
            Some(eg_modality::OpaqueRef::scoped("program_revision", &"4".repeat(64)).unwrap()),
            eg_modality::OpaqueRef::scoped("job_result", &format!("{}3", "b".repeat(63))).unwrap(),
            result_input_dataset_ref,
            &"b".repeat(64),
            7,
            &"7".repeat(64),
            &"8".repeat(64),
        );
        let failed = commit_program_promotion(
            Some(&persistence),
            &core,
            5,
            Some("program-worker"),
            "program-graph",
            "program-promotion-three",
            claim_methods(&failed_identity),
            &ResultPayload::Json(serde_json::json!({"receipt": "failed"})),
            &failed_identity,
            Some(Nonce::from_bytes([13; 32])),
        )
        .await;
        assert!(failed.is_err());
        let after_failed = persistence
            .read_authoritative_graph_snapshot(&fname)
            .await
            .expect("read post-CAS-failure snapshot")
            .expect("promotion snapshot");
        assert_eq!(before_failed_bytes, after_failed.0.to_msgpack().unwrap());
        assert!(persistence
            .read_mutation_batch(&fname, "program-promotion-three")
            .await
            .expect("read failed promotion receipt")
            .is_none());

        let graph_before_job_store_removal = after_failed.0.to_msgpack().unwrap();
        drop(job_store);
        assert!(
            job_path.exists(),
            "published job file exists before removal"
        );
        std::fs::remove_file(&job_path).expect("remove only the temporary job-store file");
        assert!(!job_path.exists(), "temporary job-store file was removed");
        let fresh_job_store = eg_jobs::JobStore::open(
            &job_path,
            &PromotionJobScopeVerifier,
            PROMOTION_TEST_PRINCIPAL,
            b"promotion-test-proof",
        )
        .expect("reopen a fresh job store after file removal");
        assert!(
            fresh_job_store
                .list_ids()
                .expect("list fresh job-store ids")
                .is_empty(),
            "the reopened job store has no retained job rows"
        );
        drop(fresh_job_store);
        std::fs::remove_file(&job_path).expect("remove reopened temporary job-store file");
        // Close every graph handle before reopening the authoritative graph. The
        // separate job-store deletion must not be masked by a live graph backend
        // or an in-memory GraphCore snapshot.
        drop(core);
        persistence.shutdown();
        drop(persistence);
        let reopened_persistence: Arc<dyn PersistenceBackend> = Arc::new(
            crate::server::persistence::redb_backend::RedbBackend::open(
                dir.to_string_lossy().into_owned(),
                64,
            )
            .expect("reopen authoritative graph backend after job-store removal"),
        );
        let graph_after_job_store_reopen = reopened_persistence
            .read_authoritative_graph_snapshot(&fname)
            .await
            .expect("read graph after independent job-store removal")
            .expect("promotion snapshot after independent job-store removal");
        assert_eq!(
            graph_before_job_store_removal,
            graph_after_job_store_reopen.0.to_msgpack().unwrap(),
            "job-store close/removal/reopen leaves retained graph bytes unchanged"
        );
        let restarted = GraphCore::from_snapshot(
            graph_after_job_store_reopen.0.clone(),
            graph_after_job_store_reopen.1,
        )
        .expect("restore promoted graph");
        let pointer = first_identity.active_pointer_ref();
        let pointer_bytes = restarted
            .get_node_properties(pointer.as_str())
            .expect("active pointer survives restart");
        let pointer_value = eg_types::msgpack::decode_property_value(&pointer_bytes)
            .expect("decode active pointer");
        assert_eq!(
            pointer_value
                .get("base_revision")
                .and_then(serde_json::Value::as_u64),
            Some(second_identity.base_revision)
        );
        assert_eq!(
            pointer_value
                .get("revision_ref")
                .and_then(serde_json::Value::as_str),
            Some(second_identity.revision_ref.as_str())
        );
        assert_eq!(
            pointer_value.get("policy"),
            Some(&serde_json::to_value(&second_identity.policy).unwrap()),
        );
        assert_eq!(
            pointer_value
                .get("tool_policy_ref")
                .and_then(serde_json::Value::as_str),
            second_identity
                .tool_policy_ref
                .as_ref()
                .map(eg_modality::OpaqueRef::as_str),
        );
        assert_eq!(
            pointer_value
                .get("model_profile_ref")
                .and_then(serde_json::Value::as_str),
            second_identity
                .model_profile_ref
                .as_ref()
                .map(eg_modality::OpaqueRef::as_str),
        );
        let revision_properties = restarted
            .get_node_properties(second_identity.revision_ref.as_str())
            .expect("durable revision survives restart after job retention");
        let revision_value = eg_types::msgpack::decode_property_value(&revision_properties)
            .expect("decode durable revision");
        let resolved =
            eg_program::ProgramRevisionIdentity::from_durable_properties(&revision_value)
                .expect("resolve durable revision identity after job purge");
        assert_eq!(resolved, second_identity);
        assert_eq!(resolved.policy, second_identity.policy);
        let resolved_chain =
            resolve_program_promotion_identity(&restarted, &second_identity.revision_ref)
                .expect("resolve candidate and claim chain after job purge");
        assert_eq!(resolved_chain, second_identity);
        reopened_persistence.shutdown();
        drop(reopened_persistence);
        let candidate_record = second_identity.candidate_record.as_ref().unwrap();
        assert!(restarted.has_node(second_identity.candidate_ref.as_str()));
        assert!(restarted.has_node(&candidate_record.candidate_claim_ref));
        assert!(restarted.has_node(first_identity.revision_ref.as_str()));
        assert!(restarted.has_node(second_identity.revision_ref.as_str()));

        fn assert_candidate_claim_tamper(
            snapshot: &crate::graph::GraphSnapshot,
            version: u64,
            claim_ref: &str,
            revision_ref: &eg_modality::OpaqueRef,
            mutate: impl FnOnce(&mut serde_json::Value),
        ) {
            let fixture = GraphCore::from_snapshot(snapshot.clone(), version)
                .expect("restore selected-claim tamper fixture");
            let properties = fixture
                .get_node_properties(claim_ref)
                .expect("selected claim exists in tamper fixture");
            let mut value = eg_types::msgpack::decode_property_value(&properties)
                .expect("decode selected claim tamper fixture");
            mutate(&mut value);
            fixture.add_node(
                claim_ref.to_string(),
                rmp_serde::to_vec_named(&value).expect("encode selected claim tamper fixture"),
            );
            assert!(
                resolve_program_promotion_identity(&fixture, revision_ref).is_err(),
                "selected-claim tamper must fail closed"
            );
        }

        assert_candidate_claim_tamper(
            &graph_after_job_store_reopen.0,
            graph_after_job_store_reopen.1,
            &candidate_record.candidate_claim_ref,
            &second_identity.revision_ref,
            |value| {
                value
                    .as_object_mut()
                    .expect("claim is an object")
                    .insert("family".to_string(), serde_json::json!("other.family"));
            },
        );
        assert_candidate_claim_tamper(
            &graph_after_job_store_reopen.0,
            graph_after_job_store_reopen.1,
            &candidate_record.candidate_claim_ref,
            &second_identity.revision_ref,
            |value| {
                value["knowledge"]["selected"] = serde_json::Value::Bool(false);
            },
        );
        assert_candidate_claim_tamper(
            &graph_after_job_store_reopen.0,
            graph_after_job_store_reopen.1,
            &candidate_record.candidate_claim_ref,
            &second_identity.revision_ref,
            |value| {
                value["knowledge"]["promotion_identity"] = serde_json::Value::Null;
            },
        );

        let result_claim_ref = format!("jobclaim:{}", candidate_record.result_ref.as_str());
        let result_digest_tamper = GraphCore::from_snapshot(
            graph_after_job_store_reopen.0.clone(),
            graph_after_job_store_reopen.1,
        )
        .expect("restore result-claim digest tamper fixture");
        let result_properties = result_digest_tamper
            .get_node_properties(&result_claim_ref)
            .expect("result claim exists in tamper fixture");
        let mut result_value = eg_types::msgpack::decode_property_value(&result_properties)
            .expect("decode result claim tamper fixture");
        // A digest that differs from the claim's real `input_content_digest` BY
        // CONSTRUCTION. The hardcoded `"c".repeat(64)` that stood here WAS
        // `second_identity`'s genuine input content digest, so the "tamper"
        // wrote the same bytes back: the claim stayed valid, resolution
        // correctly succeeded, and the assertion below proved nothing about
        // digest binding. Deriving the tampered value from the real one keeps
        // that collision impossible if the fixture's digests are ever changed.
        let genuine_input_digest = result_value["input_content_digest"]
            .as_str()
            .expect("result claim carries its input content digest")
            .to_string();
        let tampered_input_digest = if genuine_input_digest.starts_with('d') {
            "e".repeat(64)
        } else {
            "d".repeat(64)
        };
        assert_ne!(
            tampered_input_digest, genuine_input_digest,
            "the tampered input digest must actually differ from the real one"
        );
        result_value["input_content_digest"] = serde_json::json!(tampered_input_digest);
        result_digest_tamper.add_node(
            result_claim_ref.clone(),
            rmp_serde::to_vec_named(&result_value).expect("encode result claim tamper fixture"),
        );
        assert!(
            resolve_program_promotion_identity(
                &result_digest_tamper,
                &second_identity.revision_ref
            )
            .is_err(),
            "result-claim digest tamper must fail closed"
        );

        let modality_tamper = GraphCore::from_snapshot(
            graph_after_job_store_reopen.0.clone(),
            graph_after_job_store_reopen.1,
        )
        .expect("restore modality tamper fixture");
        let revision_properties = modality_tamper
            .get_node_properties(second_identity.revision_ref.as_str())
            .expect("revision exists in modality tamper fixture");
        let mut modality_value = eg_types::msgpack::decode_property_value(&revision_properties)
            .expect("decode modality tamper fixture");
        modality_value["candidate_record"]["modalities"] = serde_json::json!(["image"]);
        modality_tamper.add_node(
            second_identity.revision_ref.as_str().to_string(),
            rmp_serde::to_vec_named(&modality_value).expect("encode modality tamper fixture"),
        );
        assert!(
            eg_program::ProgramRevisionIdentity::from_durable_properties(&modality_value).is_err(),
            "candidate modality tamper must invalidate the canonical candidate digest"
        );
        assert!(
            resolve_program_promotion_identity(&modality_tamper, &second_identity.revision_ref)
                .is_err(),
            "candidate modality tamper must invalidate restart resolution"
        );

        let binding_tamper = GraphCore::from_snapshot(
            graph_after_job_store_reopen.0.clone(),
            graph_after_job_store_reopen.1,
        )
        .expect("restore binding type tamper fixture");
        let tool_policy_ref = second_identity
            .tool_policy_ref
            .as_ref()
            .expect("tool policy binding");
        binding_tamper.add_node(
            tool_policy_ref.as_str().to_string(),
            rmp_serde::to_vec_named(&serde_json::json!({
                "type": "ModelProfile",
                "ref": tool_policy_ref.as_str(),
            }))
            .expect("encode binding type tamper fixture"),
        );
        assert!(
            resolve_program_promotion_identity(&binding_tamper, &second_identity.revision_ref)
                .is_err(),
            "binding canonical type tamper must fail closed"
        );

        let authority_digest_tamper = GraphCore::from_snapshot(
            graph_after_job_store_reopen.0.clone(),
            graph_after_job_store_reopen.1,
        )
        .expect("restore authority digest tamper fixture");
        let revision_properties = authority_digest_tamper
            .get_node_properties(second_identity.revision_ref.as_str())
            .expect("revision exists in authority digest tamper fixture");
        let mut authority_value = eg_types::msgpack::decode_property_value(&revision_properties)
            .expect("decode authority digest tamper fixture");
        let coordinated_tamper_identity = identity(
            3,
            Some(first_identity.revision_ref.clone()),
            eg_modality::OpaqueRef::scoped("job_result", &"e".repeat(64)).unwrap(),
            second_identity
                .candidate_record
                .as_ref()
                .expect("second identity input binding")
                .result_input_dataset_ref
                .clone(),
            &"c".repeat(64),
            8,
            &"7".repeat(64),
            &"8".repeat(64),
        );
        let coordinated_record = coordinated_tamper_identity
            .candidate_record
            .as_ref()
            .expect("coordinated tamper candidate record");
        coordinated_record
            .validate()
            .expect("coordinated result/claim tamper keeps the record digest valid");
        authority_value["candidate_record"] =
            serde_json::to_value(coordinated_record).expect("encode coordinated tamper record");
        authority_digest_tamper.add_node(
            second_identity.revision_ref.as_str().to_string(),
            rmp_serde::to_vec_named(&authority_value)
                .expect("encode authority digest tamper fixture"),
        );
        assert!(
            resolve_program_promotion_identity(
                &authority_digest_tamper,
                &second_identity.revision_ref
            )
            .is_err(),
            "complete candidate result/claim tamper must fail revision-token validation"
        );

        let alias_tamper = GraphCore::from_snapshot(
            graph_after_job_store_reopen.0.clone(),
            graph_after_job_store_reopen.1,
        )
        .expect("restore opaque reference alias fixture");
        let revision_properties = alias_tamper
            .get_node_properties(second_identity.revision_ref.as_str())
            .expect("revision exists in opaque reference alias fixture");
        let mut alias_value = eg_types::msgpack::decode_property_value(&revision_properties)
            .expect("decode opaque reference alias fixture");
        alias_value["candidate_record"]["result_input_dataset_ref"] =
            serde_json::json!(format!("eg:job_input:alias:{}", "c".repeat(64)));
        assert!(
            eg_program::ProgramRevisionIdentity::from_durable_properties(&alias_value).is_err(),
            "result-input namespace aliases must not validate as canonical refs"
        );

        let mut candidate_alias_value = eg_types::msgpack::decode_property_value(
            &alias_tamper
                .get_node_properties(second_identity.revision_ref.as_str())
                .expect("revision remains available in candidate alias fixture"),
        )
        .expect("decode candidate alias fixture");
        candidate_alias_value["candidate_ref"] = serde_json::json!(format!(
            "eg:program_candidate:alias:{}",
            second_identity.content_digest
        ));
        assert!(
            eg_program::ProgramRevisionIdentity::from_durable_properties(&candidate_alias_value)
                .is_err(),
            "candidate namespace aliases must not validate as canonical refs"
        );

        let corrupted = GraphCore::from_snapshot(after_failed.0.clone(), after_failed.1)
            .expect("restore candidate corruption fixture");
        corrupted.add_node(
            second_identity.candidate_ref.as_str().to_string(),
            rmp_serde::to_vec_named(&serde_json::json!({
                "type": "ProgramCandidate",
                "content_digest": "f".repeat(64),
            }))
            .unwrap(),
        );
        assert!(
            resolve_program_promotion_identity(&corrupted, &second_identity.revision_ref).is_err()
        );

        let dangling_claim = GraphCore::from_snapshot(after_failed.0, after_failed.1)
            .expect("restore dangling claim fixture");
        dangling_claim.remove_node(candidate_record.candidate_claim_ref.clone());
        assert!(
            resolve_program_promotion_identity(&dangling_claim, &second_identity.revision_ref)
                .is_err()
        );
    }
}
