use super::store_prelude::*;
use super::*;
use crate::protocol::MethodWriteFamily;

/// Methods whose complete authoritative effect is represented by the NODES/EDGES/
/// LEDGER row transaction below.  Anything else must first be lowered by its
/// surface adapter; accepting it and letting `apply_method_rows`'s non-applicable arm
/// run would create a committed status with missing state.
pub(crate) fn supports_atomic_batch_rows(method: &Method) -> bool {
    matches!(
        (method.write_family(), method),
        (
            Some(MethodWriteFamily::WorkItemLease | MethodWriteFamily::WorkItemResource),
            _
        ) | (
            _,
            Method::AddNode { .. }
                | Method::RemoveNode { .. }
                | Method::CompareAndSetNodeFields { .. }
                | Method::AddEdge { .. }
                | Method::RemoveEdge { .. }
                | Method::BatchUpdate { .. }
                | Method::AddEmbedding { .. }
                | Method::ClearGraph
                | Method::ClearLedger
                | Method::CreateGraph { .. }
                | Method::DeleteGraph { .. }
                | Method::SubmitWorkItem { .. }
                | Method::SubmitWorkItems { .. }
        )
    )
}

pub(crate) fn property_f64(props: &serde_json::Map<String, serde_json::Value>, key: &str) -> f64 {
    props
        .get(key)
        .and_then(serde_json::Value::as_f64)
        .unwrap_or(0.0)
}

pub(crate) fn property_u64(props: &serde_json::Map<String, serde_json::Value>, key: &str) -> u64 {
    props
        .get(key)
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0)
}

pub(crate) fn property_string<'a>(
    props: &'a serde_json::Map<String, serde_json::Value>,
    key: &str,
) -> &'a str {
    props
        .get(key)
        .and_then(serde_json::Value::as_str)
        .unwrap_or("")
}

/// Persist one WorkItem row, as its next row revision.
///
/// Every native write of a WorkItem row goes through here, so this is the one
/// place the row's revision advances: `props` leaves carrying
/// `eg_types::work_item_read::WORK_ITEM_ROW_REVISION` one past the revision it
/// was read at (1 for a new row). `GetWorkItem`/`ListWorkItems` project it as
/// the caller-visible `version`.
pub(crate) fn write_work_item_props(
    nodes: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    graph: &str,
    node_id: &str,
    props: &mut serde_json::Map<String, serde_json::Value>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    use eg_types::work_item_read::WORK_ITEM_ROW_REVISION;
    let revision = property_u64(props, WORK_ITEM_ROW_REVISION).saturating_add(1);
    props.insert(
        WORK_ITEM_ROW_REVISION.into(),
        serde_json::Value::from(revision),
    );
    let bytes = rmp_serde::to_vec_named(props).map_err(|e| e.to_string())?;
    let sealed = crypto.seal(&bytes);
    nodes
        .insert((graph, node_id), sealed.as_ref())
        .map_err(|e| e.to_string())?;
    Ok(())
}

/// Drive the phase-1 statechart MIRROR for one WorkItem transition (ADR-5 / W2.2) and
/// fold its durable `MachineInstance` projection back INTO the same `props` map — so the
/// mirror commits in the SAME redb write transaction as the authoritative lifecycle
/// `status` on the SAME shard. That co-location is what makes a `kill -9` mid-transition
/// unable to split the row from its mirror (both land, or neither does), and it keeps the
/// mirror state Cypher-queryable as ordinary node properties (`machine_state`).
///
/// The redb row's `status` REMAINS the authority in phase 1; this only compares the
/// chart's independently-computed next state against it and raises the divergence alarm on
/// a mismatch (a chart bug caught before the phase-2 authority flip). `pre_status` is the
/// row status BEFORE the authority mutated it; `authoritative_next` is `Some(status)` the
/// authority persisted (the firing handlers always transition, so it is always `Some`).
#[cfg(feature = "statechart")]
pub(crate) fn apply_work_item_mirror(
    props: &mut serde_json::Map<String, serde_json::Value>,
    work_item_id: &str,
    pre_status: &str,
    event: &str,
    payload: serde_json::Value,
    authoritative_next: Option<&str>,
) {
    let outcome =
        crate::work_item_statechart::mirror_outcome(pre_status, event, payload, authoritative_next);
    if outcome.diverged {
        crate::work_item_statechart::emit_divergence(
            work_item_id,
            pre_status,
            event,
            authoritative_next,
            &outcome.next_state,
        );
    }
    let prior_version = property_u64(props, "machine_version");
    let next_version = if outcome.fired {
        prior_version.saturating_add(1)
    } else {
        prior_version
    };
    props.insert(
        "machine_state".into(),
        serde_json::Value::String(outcome.next_state),
    );
    props.insert(
        "machine_version".into(),
        serde_json::Value::from(next_version),
    );
    props.insert(
        "machine_def_id".into(),
        serde_json::Value::String(crate::work_item_statechart::WORK_ITEM_DEF_ID.clone()),
    );
}

/// Read and decode one encrypted durable row while preserving each table's
/// typed key and decoder.  A missing table is the same as a missing row: older
/// stores may not have introduced every table yet, so callers must see a
/// typed absence rather than a schema error.
/// Read one scope-prefixed owner row of one graph, unsealed and decoded.
///
/// The read half of the shard's own rows. Every table it serves leads its key
/// with the graph name, so the bound is the capability's scope rather than the
/// key the caller passes: a reader for one graph cannot address another's rows
/// in the file they share.
pub(crate) fn read_typed_graph_row<K, T, Decode>(
    shard: &Shard,
    graph_fname: &str,
    table_definition: TableDefinition<'static, K, &[u8]>,
    key: K::SelfType<'_>,
    crypto: DurableCrypto<'_>,
    decode: Decode,
) -> Result<Option<T>, String>
where
    K: redb::Key + 'static,
    for<'k> K::SelfType<'k>: eg_storage::OwnerRowScope,
    Decode: FnOnce(&[u8]) -> Result<T, String>,
{
    let handle = shard.graph(graph_fname)?;
    let read = shard.read(&handle)?;
    read.scoped_owner_table(table_definition)?
        .get(key)?
        .map(|value| {
            let bytes = crypto.unseal(value.value())?;
            decode(&bytes)
        })
        .transpose()
}

/// Read one durable batch receipt of one graph.
///
/// The receipt is the kernel's `ledger_batches` row now, not a shard table, so
/// the route binding this used to re-check by decoding is enforced before the
/// read: `Shard::graph` binds the scope derived from `graph_fname`, and a
/// `ScopedRead` on that scope can only see that scope's receipts.
pub(crate) fn read_mutation_batch_for_graph(
    shard: &Shard,
    graph_fname: &str,
    batch_id: &str,
) -> Result<Option<MutationBatchRecord>, String> {
    let handle = shard.graph(graph_fname)?;
    eg_transaction::read_ledger(&shard.read(&handle)?, batch_id)
}

pub(crate) fn read_change_envelope(
    shard: &Shard,
    graph_fname: &str,
    envelope_id: &str,
    crypto: DurableCrypto<'_>,
) -> Result<Option<ChangeEnvelopeRecord>, String> {
    read_typed_graph_row(
        shard,
        graph_fname,
        CHANGE_ENVELOPES,
        (graph_fname, envelope_id),
        crypto,
        decode_durable,
    )
}

pub(crate) fn read_content_version(
    shard: &Shard,
    tenant: &str,
    graph_fname: &str,
    object_id: &str,
    crypto: DurableCrypto<'_>,
) -> Result<Option<ContentVersion>, String> {
    read_typed_graph_row(
        shard,
        graph_fname,
        CONTENT_VERSIONS,
        (graph_fname, tenant, object_id),
        crypto,
        decode_durable,
    )
}

pub(crate) fn read_change_cursor(
    shard: &Shard,
    tenant: &str,
    graph_fname: &str,
    source: &str,
    partition: &str,
    crypto: DurableCrypto<'_>,
) -> Result<Option<ChangeCursor>, String> {
    read_typed_graph_row(
        shard,
        graph_fname,
        CHANGE_CURSORS,
        (graph_fname, tenant, source, partition),
        crypto,
        decode_durable,
    )
}

/// Every immutable outbox row of one batch, in ordinal order.
pub(crate) fn read_mutation_outbox(
    shard: &Shard,
    graph_fname: &str,
    batch_id: &str,
) -> Result<Vec<MutationOutboxRecord>, String> {
    let handle = shard.graph(graph_fname)?;
    eg_transaction::read_outbox(&shard.read(&handle)?, batch_id)
}

/// The authoritative version of one graph.
///
/// One counter, one owner. The shard's `mutation_graph_version` table is
/// retired: the kernel's `ledger_versions` row for this graph's bound scope IS
/// the authoritative version, it is what `admit_batch` resolves inside the write
/// transaction, and it is what every admitted batch advances by exactly one.
/// Two counters over one file in one transaction is the dual authority
/// RF-RULING-004 forbids, which is why this is a rename of the reader and a
/// deletion of the table rather than a migration.
pub(crate) fn read_mutation_graph_version(shard: &Shard, graph_fname: &str) -> Result<u64, String> {
    let handle = shard.graph(graph_fname)?;
    eg_transaction::version(&shard.read(&handle)?)
}

/// Durably write/overwrite a graph_meta identity row in its OWN transaction.
pub(crate) fn write_graph_meta(
    shard: &Shard,
    graph: &str,
    name: &str,
    graph_type: GraphType,
) -> Result<(), String> {
    {
        let read = shard.control_read()?;
        let meta = read
            .open_owner_table(GRAPH_META)
            .map_err(|e| e.to_string())?;
        if let Some(existing) = meta.get(graph).map_err(|e| e.to_string())? {
            let record = decode_meta_record(graph, existing.value())?;
            return if record.name == name && record.graph_type == graph_type {
                Ok(())
            } else {
                Err("graph metadata conflict".to_string())
            };
        }
    }
    let incarnation_id = new_incarnation_id(graph);
    write_graph_meta_with_incarnation(shard, graph, name, graph_type, &incarnation_id)
}

/// Durably register an exact lifecycle incarnation. Repeating the same identity
/// is idempotent; attempting to overwrite a live same-name incarnation fails
/// closed so stale work cannot silently retarget itself.
pub(crate) fn write_graph_meta_with_incarnation(
    shard: &Shard,
    graph: &str,
    name: &str,
    graph_type: GraphType,
    incarnation_id: &str,
) -> Result<(), String> {
    reject_reserved_graph(graph)?;
    if incarnation_id.trim().is_empty() {
        return Err("graph incarnation id must not be empty".to_string());
    }
    let op_id = format!("graph_meta/{graph}/{incarnation_id}");
    let (group, batches) = shard.admit_maintenance(&[], &op_id)?;
    let write = ShardWrite::open(shard, &group, &[], &batches)?;
    let result = (|| {
        let mut meta = write
            .control()
            .open_table(GRAPH_META)
            .map_err(|e| e.to_string())?;
        let existing = meta
            .get(graph)
            .map_err(|e| e.to_string())?
            .map(|value| value.value().to_vec());
        if let Some(existing) = existing {
            let record = decode_meta_record(graph, &existing)?;
            if record.incarnation_id != incarnation_id {
                return Err("graph incarnation conflict".to_string());
            }
        } else {
            let encoded = encode_meta_with_incarnation(name, graph_type, incarnation_id)?;
            meta.insert(graph, encoded.as_slice())
                .map_err(|e| e.to_string())?;
        }
        Ok(())
    })();
    let finished = write.finish();
    match (result, finished) {
        (Ok(()), Ok(())) => shard.commit_drain(group, &batches, 0),
        (Err(error), _) | (Ok(()), Err(error)) => {
            shard.mutations().abort_group(group)?;
            Err(error)
        }
    }
}

/// Point-read a single node's stored properties (read-through path).
pub(crate) fn read_one_node(
    shard: &Shard,
    graph: &str,
    node_id: &str,
    crypto: DurableCrypto<'_>,
) -> Result<Option<Vec<u8>>, String> {
    let handle = shard.graph(graph)?;
    let read = shard.read(&handle)?;
    let nodes = read.scoped_owner_table(NODES).map_err(|e| e.to_string())?;
    let v = nodes
        .get((graph, node_id))
        .map_err(|e| e.to_string())?
        .map(|g| crypto.unseal(g.value()))
        .transpose()?;
    Ok(v)
}

/// Test a batch of node ids against one MVCC snapshot. Eviction needs presence,
/// not decrypted properties, so this avoids N transactions and N payload copies.
/// The returned vector is positionally aligned with `node_ids`.
pub(crate) fn durable_node_presence(
    shard: &Shard,
    graph: &str,
    node_ids: &[String],
) -> Result<Vec<bool>, String> {
    let handle = shard.graph(graph)?;
    let read = shard.read(&handle)?;
    let nodes = read
        .scoped_owner_table(NODES)
        .map_err(|error| error.to_string())?;
    let mut present = Vec::with_capacity(node_ids.len());
    for node_id in node_ids {
        present.push(
            nodes
                .get((graph, node_id.as_str()))
                .map_err(|error| error.to_string())?
                .is_some(),
        );
    }
    Ok(present)
}

pub(crate) fn read_semantic_store(
    semantic: &ScopedOwnerTableMut<'_, &str, &[u8]>,
    graph: &str,
    crypto: DurableCrypto<'_>,
) -> Result<Option<crate::compute::semantic::SemanticStore>, String> {
    semantic
        .get(graph)
        .map_err(|error| error.to_string())?
        .map(|value| {
            let bytes = crypto.unseal(value.value())?;
            decode_durable(&bytes)
        })
        .transpose()
}

pub(crate) fn write_semantic_store(
    semantic: &mut ScopedOwnerTableMut<'_, &str, &[u8]>,
    graph: &str,
    store: &crate::compute::semantic::SemanticStore,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let bytes = rmp_serde::to_vec_named(store).map_err(|error| error.to_string())?;
    let sealed = crypto.seal(&bytes);
    semantic
        .insert(graph, sealed.as_ref())
        .map_err(|error| error.to_string())?;
    Ok(())
}

pub(crate) fn upsert_durable_embedding(
    semantic: &mut ScopedOwnerTableMut<'_, &str, &[u8]>,
    graph: &str,
    node_id: &str,
    embedding: &[f32],
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let mut store = read_semantic_store(semantic, graph, crypto)?.unwrap_or_default();
    store
        .add_embedding(node_id.to_string(), embedding.to_vec())
        .map_err(|error| error.to_string())?;
    write_semantic_store(semantic, graph, &store, crypto)
}

pub(crate) fn remove_durable_embedding(
    semantic: &mut ScopedOwnerTableMut<'_, &str, &[u8]>,
    graph: &str,
    node_id: &str,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let Some(mut store) = read_semantic_store(semantic, graph, crypto)? else {
        return Ok(());
    };
    if store.remove_embedding(node_id) {
        write_semantic_store(semantic, graph, &store, crypto)?;
    }
    Ok(())
}

pub(crate) fn remove_durable_edge_pair(
    graph: &str,
    source: &str,
    target: &str,
    edges: &mut ScopedOwnerTableMut<'_, (&str, &str, &str, u32), &[u8]>,
) -> Result<(), String> {
    let ordinals: Vec<u32> = edges
        .range_inclusive(
            (graph, source, target, 0u32),
            (graph, source, target, u32::MAX),
        )?
        .map(|row| {
            let (key, _) = row.map_err(|error| error.to_string())?;
            Ok(key.value().3)
        })
        .collect::<Result<_, String>>()?;
    for ordinal in ordinals {
        edges
            .remove((graph, source, target, ordinal))
            .map_err(|error| error.to_string())?;
    }
    invalidate_edge_ord(graph, source, target);
    Ok(())
}

pub(crate) fn remove_durable_node(
    graph: &str,
    node_id: &str,
    nodes: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    edges: &mut ScopedOwnerTableMut<'_, (&str, &str, &str, u32), &[u8]>,
    semantic: &mut ScopedOwnerTableMut<'_, &str, &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    remove_durable_node_rows(graph, node_id, nodes, edges)?;
    remove_durable_embedding(semantic, graph, node_id, crypto)
}

pub(crate) fn remove_durable_node_rows(
    graph: &str,
    node_id: &str,
    nodes: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    edges: &mut ScopedOwnerTableMut<'_, (&str, &str, &str, u32), &[u8]>,
) -> Result<(), String> {
    nodes
        .remove((graph, node_id))
        .map_err(|error| error.to_string())?;
    // The edge key is `(graph, source, target, ordinal)`: outgoing edges form a
    // prefix, while incoming edges require one bounded scan of this graph.
    let incident: Vec<(String, String, u32)> = edges
        .scope_rows()
        .map_err(|error| error.to_string())?
        .filter_map(|row| match row {
            Ok((key, _)) => {
                let (row_graph, source, target, ordinal) = key.value();
                if row_graph != graph {
                    return Some(Err("graph edge row escaped its scope".to_string()));
                }
                (source == node_id || target == node_id)
                    .then(|| Ok((source.to_string(), target.to_string(), ordinal)))
            }
            Err(error) => Some(Err(error.to_string())),
        })
        .collect::<Result<_, String>>()?;
    for (source, target, ordinal) in incident {
        edges
            .remove((graph, source.as_str(), target.as_str(), ordinal))
            .map_err(|error| error.to_string())?;
        invalidate_edge_ord(graph, &source, &target);
    }
    invalidate_node_edge_ords(graph, node_id);
    Ok(())
}
