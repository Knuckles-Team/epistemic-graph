//! Bounded batch decoding, validation, preview, and application.

use eg_core::compute::semantic::MAX_EMBEDDING_DIMENSION;
use std::collections::{BTreeMap, BTreeSet, HashMap};

use crate::graph::GraphCore;

/// One validated operation in the public `BatchUpdate` wire contract.
///
/// This type is shared with the redb row adapter so RAM execution, WAL replay,
/// embedded mode, and authoritative persistence cannot drift onto different field
/// names. The public keys are deliberately `id`, `source`, and `target`.
#[derive(Debug, Clone, PartialEq)]
pub enum BatchOperation {
    AddNode {
        id: String,
        properties_msgpack: Vec<u8>,
        upsert: bool,
    },
    RemoveNode {
        id: String,
    },
    AddEdge {
        source: String,
        target: String,
        properties_msgpack: Vec<u8>,
        upsert: bool,
    },
    RemoveEdge {
        source: String,
        target: String,
    },
    AddEmbedding {
        id: String,
        embedding: Vec<f32>,
    },
}

const MAX_BATCH_UPDATE_BYTES: usize = 32 * 1024 * 1024;
const MAX_BATCH_UPDATE_ITEMS: usize = 500_000;
const MAX_BATCH_OPERATIONS: usize = 50_000;
pub(crate) const MAX_BATCH_ID_BYTES: usize = 4_096;
const MAX_BATCH_PROPERTIES_BYTES: usize = 4 * 1024 * 1024;

#[derive(Debug, Clone, Default, serde::Serialize)]
struct BatchUpdateSummary {
    added_nodes: u32,
    upserted_nodes: u32,
    removed_nodes: u32,
    added_edges: u32,
    upserted_edges: u32,
    removed_edges: u32,
    added_embeddings: u32,
    errors: Vec<String>,
}

fn required_batch_id(
    operation: &serde_json::Value,
    index: usize,
    key: &str,
) -> Result<String, String> {
    let value = operation
        .get(key)
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| format!("BatchUpdate op[{index}] requires a non-empty string '{key}'"))?;
    if value.len() > MAX_BATCH_ID_BYTES || value.chars().any(char::is_control) {
        return Err(format!(
            "BatchUpdate op[{index}] '{key}' exceeds the identifier policy"
        ));
    }
    Ok(value.to_owned())
}

fn batch_properties(operation: &serde_json::Value, index: usize) -> Result<Vec<u8>, String> {
    let value = operation
        .get("properties")
        .cloned()
        .unwrap_or_else(|| serde_json::Value::Object(serde_json::Map::new()));
    if !value.is_object() {
        return Err(format!(
            "BatchUpdate op[{index}] 'properties' must be an object"
        ));
    }
    let encoded = rmp_serde::to_vec_named(&value)
        .map_err(|_| format!("BatchUpdate op[{index}] properties encode failed"))?;
    if encoded.len() > MAX_BATCH_PROPERTIES_BYTES {
        return Err(format!(
            "BatchUpdate op[{index}] properties exceed the resource limit"
        ));
    }
    Ok(encoded)
}

/// Merge the supplied top-level fields into one existing node property object.
///
/// Both the RAM executor and the authoritative redb row adapter call this exact
/// routine so `upsert_node` cannot drift between resident and durable state.
/// Nested values are replaced as complete top-level fields; this is deliberately
/// not a recursive JSON merge.
pub fn merge_batch_node_properties(current: &[u8], updates: &[u8]) -> Result<Vec<u8>, String> {
    let current = eg_types::msgpack::decode_property_value(current)
        .map_err(|_| "existing node properties are not a valid object".to_string())?;
    let updates = eg_types::msgpack::decode_property_value(updates)
        .map_err(|_| "upsert properties are not a valid object".to_string())?;
    let serde_json::Value::Object(mut current) = current else {
        return Err("existing node properties are not a valid object".to_string());
    };
    let serde_json::Value::Object(updates) = updates else {
        return Err("upsert properties are not a valid object".to_string());
    };
    current.extend(updates);
    rmp_serde::to_vec_named(&serde_json::Value::Object(current)).map_err(|error| error.to_string())
}

/// Decode and validate the public `BatchUpdate` schema without mutating state.
///
/// Malformed MessagePack, missing fields, unknown operations, non-object properties,
/// and invalid embeddings are terminal errors. Callers must never reinterpret an
/// opaque or partially decoded payload as an empty successful batch.
/// Decode one `add_embedding` batch operation. Split out of
/// `decode_batch_operations` (extract-method, cx/wD8) — same terms, same
/// order as before.
fn decode_batch_embedding_op(
    operation: &serde_json::Value,
    index: usize,
) -> Result<BatchOperation, String> {
    let id = required_batch_id(operation, index, "id")?;
    let values = operation
        .get("embedding")
        .and_then(serde_json::Value::as_array)
        .filter(|values| !values.is_empty())
        .ok_or_else(|| {
            format!("BatchUpdate op[{index}] 'embedding' must be a non-empty number array")
        })?;
    if values.len() > MAX_EMBEDDING_DIMENSION {
        return Err(format!(
            "BatchUpdate op[{index}] embedding exceeds the dimension limit"
        ));
    }
    let mut embedding = Vec::with_capacity(values.len());
    for value in values {
        let number = value
            .as_f64()
            .ok_or_else(|| format!("BatchUpdate op[{index}] embedding contains a non-number"))?;
        let component = number as f32;
        if !number.is_finite() || !component.is_finite() {
            return Err(format!(
                "BatchUpdate op[{index}] embedding contains a non-finite component"
            ));
        }
        embedding.push(component);
    }
    Ok(BatchOperation::AddEmbedding { id, embedding })
}

pub fn decode_batch_operations(operations_msgpack: &[u8]) -> Result<Vec<BatchOperation>, String> {
    let operations: Vec<serde_json::Value> = eg_types::msgpack::decode_bounded(
        operations_msgpack,
        eg_types::msgpack::MsgpackLimits::new(MAX_BATCH_UPDATE_BYTES, MAX_BATCH_UPDATE_ITEMS, 64),
    )
    .map_err(|_| "[EpistemicGraph::batch_update] invalid or over-complex MsgPack".to_string())?;
    if operations.len() > MAX_BATCH_OPERATIONS {
        return Err("BatchUpdate operation count exceeds the resource limit".to_string());
    }
    let mut decoded = Vec::with_capacity(operations.len());
    for (index, operation) in operations.iter().enumerate() {
        let kind = operation
            .get("op")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| format!("BatchUpdate op[{index}] requires string 'op'"))?;
        let decoded_operation = match kind {
            "add_node" | "upsert_node" => BatchOperation::AddNode {
                id: required_batch_id(operation, index, "id")?,
                properties_msgpack: batch_properties(operation, index)?,
                upsert: kind == "upsert_node",
            },
            "remove_node" => BatchOperation::RemoveNode {
                id: required_batch_id(operation, index, "id")?,
            },
            "add_edge" | "upsert_edge" => BatchOperation::AddEdge {
                source: required_batch_id(operation, index, "source")?,
                target: required_batch_id(operation, index, "target")?,
                properties_msgpack: batch_properties(operation, index)?,
                upsert: kind == "upsert_edge",
            },
            "remove_edge" => BatchOperation::RemoveEdge {
                source: required_batch_id(operation, index, "source")?,
                target: required_batch_id(operation, index, "target")?,
            },
            "add_embedding" => decode_batch_embedding_op(operation, index)?,
            _ => return Err(format!("BatchUpdate op[{index}] has an unknown operation")),
        };
        decoded.push(decoded_operation);
    }
    Ok(decoded)
}

/// Prepare one `AddNode`/`upsert_node` op — merging cumulative properties for
/// a repeated id within the batch. Split out of `prepare_batch_operations_with`
/// (extract-method, cx/wD8) — same terms, same order as before.
fn prepare_add_node_op(
    index: usize,
    id: &str,
    properties_msgpack: &mut Vec<u8>,
    upsert: bool,
    node_state: &mut HashMap<String, Option<Vec<u8>>>,
    node_exists: &mut impl FnMut(&str) -> bool,
    node_properties: &mut impl FnMut(&str) -> Option<Vec<u8>>,
) -> Result<(), String> {
    if upsert {
        let current = match node_state.get(id) {
            Some(properties) => properties.clone(),
            None => match node_properties(id) {
                Some(properties) => Some(properties),
                None if node_exists(id) => {
                    return Err(format!(
                        "BatchUpdate op[{index}] node '{id}' has no property document"
                    ));
                }
                None => None,
            },
        };
        if let Some(current) = current {
            *properties_msgpack = merge_batch_node_properties(&current, properties_msgpack)
                .map_err(|reason| {
                    format!("BatchUpdate op[{index}] cannot upsert node '{id}': {reason}")
                })?;
        }
    }
    node_state.insert(id.to_string(), Some(properties_msgpack.clone()));
    Ok(())
}

/// Prepare one `AddEdge` op — endpoints must exist at that point in the
/// batch. Split out of `prepare_batch_operations_with` (extract-method,
/// cx/wD8) — same terms, same order as before.
fn prepare_add_edge_op(
    index: usize,
    source: &str,
    target: &str,
    node_state: &HashMap<String, Option<Vec<u8>>>,
    node_exists: &mut impl FnMut(&str) -> bool,
) -> Result<(), String> {
    let source_exists = node_state
        .get(source)
        .map(Option::is_some)
        .unwrap_or_else(|| node_exists(source));
    let target_exists = node_state
        .get(target)
        .map(Option::is_some)
        .unwrap_or_else(|| node_exists(target));
    if !source_exists || !target_exists {
        return Err(format!(
            "BatchUpdate op[{index}] edge endpoints must exist at that point in the batch"
        ));
    }
    Ok(())
}

/// Prepare one `AddEmbedding` op — validates the node exists and the
/// embedding's dimension against `expected_embedding_dim`. Split out of
/// `prepare_batch_operations_with` (extract-method, cx/wD8) — same terms,
/// same order as before. Returns the (possibly newly-established) expected
/// dimension.
fn prepare_add_embedding_op(
    index: usize,
    id: &str,
    embedding: &[f32],
    expected_embedding_dim: usize,
    node_state: &HashMap<String, Option<Vec<u8>>>,
    node_exists: &mut impl FnMut(&str) -> bool,
) -> Result<usize, String> {
    let exists = node_state
        .get(id)
        .map(Option::is_some)
        .unwrap_or_else(|| node_exists(id));
    if !exists {
        return Err(format!(
            "BatchUpdate op[{index}] embedding node '{id}' does not exist"
        ));
    }
    // Same guard as the arena/map chokepoint (`SemanticStore::add_embedding`),
    // run for EVERY embedding op in the batch up front so a
    // mixed-dimension OR non-finite-component batch (e.g. op[2] matches
    // the store, op[5] doesn't, or op[5] contains a NaN) is rejected as a
    // whole, before `batch_update` applies op[0]/op[1] to the live store.
    eg_core::compute::semantic::check_embedding_dimension(embedding, expected_embedding_dim)
        .map_err(|error| format!("BatchUpdate op[{index}] {error}"))
}

fn prepare_batch_operations_with(
    operations: &mut [BatchOperation],
    mut node_exists: impl FnMut(&str) -> bool,
    mut node_properties: impl FnMut(&str) -> Option<Vec<u8>>,
    // CONCEPT:EG-KG.compute.rank-dim-mismatch-guard (BUG-007). The store's CURRENT established
    // embedding dimension (`0` = store empty / unset). Every `AddEmbedding` op in
    // this batch is validated against it (and against each other, once the batch
    // itself establishes a dimension for an empty store) BEFORE `batch_update`
    // applies a single one to the LIVE `semantic_store` — that apply loop mutates
    // the resident store directly (no rollback), so a mixed-dimension batch MUST be
    // rejected here, whole, or it would partially apply.
    store_dim: usize,
) -> Result<(), String> {
    // Track only ids touched by this batch. The property image is needed so two
    // ordered upserts merge cumulatively before the first RAM mutation occurs.
    let mut node_state = HashMap::<String, Option<Vec<u8>>>::new();
    let mut expected_embedding_dim = store_dim;
    for (index, operation) in operations.iter_mut().enumerate() {
        match operation {
            BatchOperation::AddNode {
                id,
                properties_msgpack,
                upsert,
            } => {
                prepare_add_node_op(
                    index,
                    id,
                    properties_msgpack,
                    *upsert,
                    &mut node_state,
                    &mut node_exists,
                    &mut node_properties,
                )?;
            }
            BatchOperation::RemoveNode { id } => {
                node_state.insert(id.clone(), None);
            }
            BatchOperation::AddEdge { source, target, .. } => {
                prepare_add_edge_op(index, source, target, &node_state, &mut node_exists)?;
            }
            BatchOperation::RemoveEdge { .. } => {}
            BatchOperation::AddEmbedding { id, embedding } => {
                expected_embedding_dim = prepare_add_embedding_op(
                    index,
                    id,
                    embedding,
                    expected_embedding_dim,
                    &node_state,
                    &mut node_exists,
                )?;
            }
        }
    }
    Ok(())
}

fn prepare_batch_operations(
    core: &GraphCore,
    operations: &mut [BatchOperation],
) -> Result<(), String> {
    prepare_batch_operations_with(
        operations,
        |id| core.has_node(id),
        |id| core.get_node_properties(id),
        core.semantic_store.read().dim(),
    )
}

fn batch_summary(operations: &[BatchOperation]) -> BatchUpdateSummary {
    let mut summary = BatchUpdateSummary::default();
    for operation in operations {
        match operation {
            BatchOperation::AddNode { upsert, .. } => {
                summary.added_nodes += 1;
                summary.upserted_nodes += u32::from(*upsert);
            }
            BatchOperation::RemoveNode { .. } => summary.removed_nodes += 1,
            BatchOperation::AddEdge { upsert, .. } => {
                summary.added_edges += 1;
                summary.upserted_edges += u32::from(*upsert);
            }
            BatchOperation::RemoveEdge { .. } => summary.removed_edges += 1,
            BatchOperation::AddEmbedding { .. } => summary.added_embeddings += 1,
        }
    }
    summary
}

fn encode_batch_summary(summary: &BatchUpdateSummary) -> Result<Vec<u8>, String> {
    rmp_serde::to_vec_named(summary).map_err(|error| error.to_string())
}

/// Validate a batch against the current graph and return its deterministic success
/// payload without applying it. The authoritative mutation gateway uses this to put
/// valid batches on the durable-before-RAM row path.
pub fn batch_update_preview(
    core: &GraphCore,
    operations_msgpack: &[u8],
) -> Result<Vec<u8>, String> {
    let mut operations = decode_batch_operations(operations_msgpack)?;
    prepare_batch_operations(core, &mut operations)?;
    encode_batch_summary(&batch_summary(&operations))
}

/// Apply a validated collection of graph/vector operations atomically from the
/// caller's perspective. Structural rows share one topology transaction; semantic
/// actions execute in wire order while that topology guard is still held. The same
/// decoded operations are used by redb, so replay and restart preserve RAM behavior.
///
/// Supported operations are `add_node`, `upsert_node`, `remove_node`, `add_edge`,
/// `upsert_edge`, `remove_edge`, and `add_embedding`. Nodes use `id`; edges use
/// `source` and `target`; embeddings use `id` plus a non-empty `embedding` array.
/// One pending change to `core.semantic_store`, applied after the topology
/// `txn` commits. Split out of `batch_update` (extract-method, cx/wD8) — was
/// a local `enum` there, unchanged.
enum SemanticAction {
    Upsert(String, Vec<f32>),
    Remove(String),
}

/// Apply one decoded `BatchOperation` to the in-flight `txn`/changeset state.
/// Split out of `batch_update`'s operation loop (extract-method, cx/wD8) —
/// same terms, same order as before.
type BatchApplyContext<'state, 'graph> = (
    bool,
    &'state mut BTreeMap<String, Vec<u8>>,
    &'state mut BTreeSet<String>,
    &'state mut eg_core::index::ChangeSet,
    &'state mut Vec<SemanticAction>,
    &'state mut eg_core::graph::GraphTxn<'graph>,
);

fn apply_batch_operation(
    operation: BatchOperation,
    context: BatchApplyContext<'_, '_>,
) -> Result<(), String> {
    let (capture_content, node_upserts, node_removals, change, semantic_actions, txn) = context;
    match operation {
        BatchOperation::AddNode {
            id,
            properties_msgpack,
            ..
        } => {
            if capture_content {
                node_removals.remove(&id);
                node_upserts.insert(id.clone(), properties_msgpack.clone());
            } else {
                node_removals.remove(&id);
                node_upserts.insert(id.clone(), Vec::new());
            }
            txn.add_node(id, properties_msgpack);
        }
        BatchOperation::RemoveNode { id } => {
            node_upserts.remove(&id);
            node_removals.insert(id.clone());
            txn.remove_node(id.clone());
            semantic_actions.push(SemanticAction::Remove(id));
        }
        BatchOperation::AddEdge {
            source,
            target,
            properties_msgpack,
            upsert,
        } => {
            if upsert {
                txn.remove_edge(source.clone(), target.clone());
                change.record_remove_edge(source.clone(), target.clone());
            }
            txn.add_edge(source.clone(), target.clone(), properties_msgpack)?;
            change.record_add_edge(source, target);
        }
        BatchOperation::RemoveEdge { source, target } => {
            txn.remove_edge(source.clone(), target.clone());
            change.record_remove_edge(source, target);
        }
        BatchOperation::AddEmbedding { id, embedding } => {
            semantic_actions.push(SemanticAction::Upsert(id, embedding));
        }
    }
    Ok(())
}

pub fn batch_update(core: &GraphCore, operations_msgpack: &[u8]) -> Result<Vec<u8>, String> {
    let mut operations = decode_batch_operations(operations_msgpack)?;
    let summary = batch_summary(&operations);
    let capture_content = core.wants_change_content();
    let mut node_upserts = BTreeMap::<String, Vec<u8>>::new();
    let mut node_removals = BTreeSet::<String>::new();
    let mut change = eg_core::index::ChangeSet::new();
    let mut semantic_actions = Vec::new();

    // ONE topology guard makes every structural operation atomic to graph readers.
    let mut txn = core.txn();
    // Revalidate against the state protected by this exact guard. Validation must
    // finish before the first mutation so a concurrent removal between preview and
    // execution cannot turn an otherwise valid batch into a partial write.
    prepare_batch_operations_with(
        &mut operations,
        |id| txn.has_node(id),
        |id| txn.get_node_properties(id),
        // Embeddings are NEVER staged through `txn` (that guard covers node/edge
        // topology only) — the apply loop below mutates `core.semantic_store`
        // directly, live, regardless of txn state. So the dimension to validate
        // against here is the SAME live store `prepare_batch_operations` reads,
        // not anything txn-scoped; there is no separate "staged" dimension to be
        // consistent with (CONCEPT:EG-KG.compute.rank-dim-mismatch-guard, BUG-007).
        core.semantic_store.read().dim(),
    )?;
    let source_version = core.version();
    for operation in operations {
        apply_batch_operation(
            operation,
            (
                capture_content,
                &mut node_upserts,
                &mut node_removals,
                &mut change,
                &mut semantic_actions,
                &mut txn,
            ),
        )?;
    }

    // Preserve operation order: remove→re-add→embedding and embedding→remove
    // reach the same final semantic state after RAM execution and durable replay.
    if !semantic_actions.is_empty() {
        let mut semantic = core.semantic_store.write();
        for action in semantic_actions {
            match action {
                // `prepare_batch_operations` already validated every embedding's
                // dimension against the store and against the rest of this batch
                // (CONCEPT:EG-KG.compute.rank-dim-mismatch-guard, BUG-007), so this should never
                // observe a mismatch — propagated rather than `.expect()`-panicked
                // so a genuine surprise (e.g. a concurrent mutation between
                // validation and apply) fails closed instead of crashing the
                // dispatcher.
                SemanticAction::Upsert(id, embedding) => semantic
                    .add_embedding(id, embedding)
                    .map_err(|error| error.to_string())?,
                SemanticAction::Remove(id) => {
                    semantic.remove_embedding(&id);
                }
            }
        }
    }

    for (id, properties) in node_upserts {
        if capture_content {
            change
                .added_nodes
                .push(eg_core::index::NodeChange::with_properties(id, properties));
        } else {
            change.record_add_node(id);
        }
    }
    for id in node_removals {
        change.record_remove_node(id);
    }
    let node_count = txn.node_count();
    let edge_count = txn.edge_count();
    core.maintain_indexes_at(
        &change,
        source_version.saturating_add(1),
        node_count,
        edge_count,
    );
    drop(txn);

    encode_batch_summary(&summary)
}
