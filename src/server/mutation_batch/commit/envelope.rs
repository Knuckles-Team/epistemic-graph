use std::sync::Arc;

use crate::change_envelope::ChangeEnvelope;
use crate::graph::GraphCore;
use crate::protocol::Method;

/// Vectors and node removals an envelope carries. Vectors live in the
/// semantic store beside the snapshot, so they are published after the
/// snapshot swap (EG-UNIFIED-DATA-PLANE-R651.7.3).
#[derive(Default)]
struct StagedVectors<'a> {
    added: Vec<(&'a str, &'a [f32])>,
    removed: Vec<&'a str>,
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
    let mut vectors = StagedVectors::default();
    for operation in &envelope.mutation.operations {
        stage_operation(&staged, operation, &mut vectors)?;
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
    core.install_committed_snapshot(staged.snapshot(), target_graph_version)?;
    publish_vectors(core, vectors)
}

/// Apply one committed operation to the staged copy.
fn stage_operation<'a>(
    staged: &GraphCore,
    operation: &'a crate::mutation_batch::MutationOperation,
    vectors: &mut StagedVectors<'a>,
) -> Result<(), String> {
    match &operation.method {
        Method::AddNode {
            node_id,
            properties_msgpack,
        } => staged.add_node(node_id.clone(), properties_msgpack.clone()),
        Method::RemoveNode { node_id } => {
            staged.remove_node(node_id.clone());
            vectors.removed.push(node_id);
        }
        Method::AddEmbedding { node_id, embedding } => stage_vector(node_id, embedding, vectors)?,
        Method::CompareAndSetNodeFields {
            node_id,
            conditions_msgpack,
            updates_msgpack,
        } => stage_cas(staged, node_id, conditions_msgpack, updates_msgpack)?,
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
    Ok(())
}

fn stage_vector<'a>(
    node_id: &'a str,
    embedding: &'a [f32],
    vectors: &mut StagedVectors<'a>,
) -> Result<(), String> {
    if embedding.is_empty() || embedding.iter().any(|x| !x.is_finite()) {
        return Err(format!(
            "committed embedding for '{node_id}' is not a finite vector"
        ));
    }
    vectors.added.push((node_id, embedding));
    Ok(())
}

fn stage_cas(
    staged: &GraphCore,
    node_id: &str,
    conditions_msgpack: &[u8],
    updates_msgpack: &[u8],
) -> Result<(), String> {
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
    Ok(())
}

/// Publish staged vectors after the snapshot swap; a removed node loses its vector.
fn publish_vectors(core: &GraphCore, vectors: StagedVectors<'_>) -> Result<(), String> {
    if vectors.added.is_empty() && vectors.removed.is_empty() {
        return Ok(());
    }
    let mut semantic = core.semantic_store.write();
    for node_id in vectors.removed {
        semantic.remove_embedding(node_id);
    }
    for (node_id, embedding) in vectors.added {
        semantic
            .add_embedding(node_id.to_string(), embedding.to_vec())
            .map_err(|error| format!("committed embedding for '{node_id}': {error}"))?;
    }
    Ok(())
}
