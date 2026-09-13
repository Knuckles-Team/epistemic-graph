use std::sync::Arc;

use crate::change_envelope::ChangeEnvelope;
use crate::graph::GraphCore;
use crate::protocol::Method;

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
