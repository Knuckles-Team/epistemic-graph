use super::store_prelude::*;
use super::*;

/// `Method::CompareAndSetNodeFields` row effect.  Evaluates and merges the CAS
/// against the durable pre-image inside the held transaction; persisting
/// `updates_msgpack` by itself discarded every untouched property and could
/// diverge from GraphCore.  A missing node is a silent no-op, as before.
pub(crate) fn apply_cas_node_fields_row(
    graph: &str,
    node_id: &str,
    conditions_msgpack: &[u8],
    updates_msgpack: &[u8],
    nodes: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let Some(current) = nodes
        .get((graph, node_id))
        .map_err(|e| e.to_string())?
        .map(|v| crypto.unseal(v.value()))
        .transpose()?
    else {
        return Ok(());
    };
    let mut props: serde_json::Map<String, serde_json::Value> = decode_durable(&current)?;
    let conditions: serde_json::Map<String, serde_json::Value> =
        decode_durable(conditions_msgpack)?;
    let updates: serde_json::Map<String, serde_json::Value> = decode_durable(updates_msgpack)?;
    let matches = conditions.iter().all(|(key, expected)| {
        props.get(key).cloned().unwrap_or(serde_json::Value::Null) == *expected
    });
    if matches {
        props.extend(updates);
        let bytes = rmp_serde::to_vec_named(&props).map_err(|e| e.to_string())?;
        let blob = crypto.seal(&bytes);
        nodes
            .insert((graph, node_id), blob.as_ref())
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

/// `Method::AddEdge` row effect: both endpoints must already be durable, then the
/// edge is appended at the next ordinal for the pair.
pub(crate) fn apply_add_edge_row(
    graph: &str,
    source_id: &str,
    target_id: &str,
    properties_msgpack: &[u8],
    nodes: &ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    edges: &mut ScopedOwnerTableMut<'_, (&str, &str, &str, u32), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let source_exists = nodes
        .get((graph, source_id))
        .map_err(|e| e.to_string())?
        .is_some();
    let target_exists = nodes
        .get((graph, target_id))
        .map_err(|e| e.to_string())?
        .is_some();
    if !source_exists || !target_exists {
        return Err(format!(
            "AddEdge requires durable endpoints: source '{}' present={}, target '{}' present={}",
            source_id, source_exists, target_id, target_exists
        ));
    }
    let ord = next_edge_ordinal(edges, graph, source_id, target_id)?;
    let blob = crypto.seal(properties_msgpack);
    edges
        .insert((graph, source_id, target_id, ord), blob.as_ref())
        .map_err(|e| e.to_string())?;
    Ok(())
}

/// Translate ONE applied method into redb row writes inside an open transaction.
/// Mirrors `crate::mutation_apply::apply`'s method set: the durable DATA mutations only.
/// The row handles every graph-row writer of one member needs, opened once.
///
/// `redb` refuses a second open of a table whose first handle is still alive,
/// and every graph member of a group writes the same `nodes`, so these are
/// opened per member and dropped before the next member's. Bundling them is
/// what keeps the writers below inside the argument cap without threading six
/// same-shaped handles positionally through every one of them.
pub(crate) struct GraphRowTables<'g> {
    pub(crate) nodes: ScopedOwnerTableMut<'g, (&'static str, &'static str), &'static [u8]>,
    pub(crate) edges:
        ScopedOwnerTableMut<'g, (&'static str, &'static str, &'static str, u32), &'static [u8]>,
    pub(crate) ledger: ScopedOwnerTableMut<'g, (&'static str, u64), &'static str>,
    pub(crate) semantic: ScopedOwnerTableMut<'g, &'static str, &'static [u8]>,
    pub(crate) command_sequences: ScopedOwnerTableMut<'g, &'static str, u64>,
    pub(crate) native_work_items:
        ScopedOwnerTableMut<'g, (&'static str, &'static str), &'static [u8]>,
    #[cfg(feature = "security")]
    pub(crate) audit: ScopedOwnerTableMut<'g, (&'static str, u64), &'static [u8]>,
}

/// Borrowed form used by the native operation loop, which already has several
/// table guards open for resource and lane operations. It lets the shared graph
/// row applier enforce the same scope boundary without opening a second handle
/// to any table that the caller owns.
pub(crate) struct GraphRowTablesRef<'a, 'n, 'e, 'l, 's, 'w>
where
    'n: 'a,
    'e: 'a,
    'l: 'a,
    's: 'a,
    'w: 'a,
{
    pub(crate) nodes: &'a mut ScopedOwnerTableMut<'n, (&'static str, &'static str), &'static [u8]>,
    pub(crate) edges: &'a mut ScopedOwnerTableMut<
        'e,
        (&'static str, &'static str, &'static str, u32),
        &'static [u8],
    >,
    pub(crate) ledger: &'a mut ScopedOwnerTableMut<'l, (&'static str, u64), &'static str>,
    pub(crate) semantic: &'a mut ScopedOwnerTableMut<'s, &'static str, &'static [u8]>,
    pub(crate) native_work_items:
        &'a mut ScopedOwnerTableMut<'w, (&'static str, &'static str), &'static [u8]>,
}

impl<'g> GraphRowTables<'g> {
    /// Open one member's graph-row tables, bounded to that member's own scope.
    pub(crate) fn open<'txn>(
        member: &'g AdmittedOwnerWrite<'txn, GraphShardOwner>,
    ) -> Result<Self, String> {
        Ok(Self {
            nodes: member.open_scoped_table(NODES)?,
            edges: member.open_scoped_table(EDGES)?,
            ledger: member.open_scoped_table(LEDGER)?,
            semantic: member.open_scoped_table(SEMANTIC)?,
            command_sequences: member.open_scoped_table(WORK_ITEM_COMMAND_SEQUENCE)?,
            native_work_items: member.open_scoped_table(work_item_capability::NATIVE_WORK_ITEMS)?,
            #[cfg(feature = "security")]
            audit: member.open_scoped_table(AUDIT)?,
        })
    }
}

pub(crate) fn apply_method_rows(
    graph: &str,
    method: &Method,
    tables: &mut GraphRowTables<'_>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let mut tables = GraphRowTablesRef {
        nodes: &mut tables.nodes,
        edges: &mut tables.edges,
        ledger: &mut tables.ledger,
        semantic: &mut tables.semantic,
        native_work_items: &mut tables.native_work_items,
    };
    apply_method_rows_ref(graph, method, &mut tables, crypto)
}

pub(crate) fn apply_method_rows_ref(
    graph: &str,
    method: &Method,
    tables: &mut GraphRowTablesRef<'_, '_, '_, '_, '_, '_>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let GraphRowTablesRef {
        nodes,
        edges,
        ledger,
        semantic,
        native_work_items,
        ..
    } = tables;
    work_item_capability::validate_generic_method(graph, method, nodes, native_work_items, crypto)?;
    work_item::refuse_generic_native_row_write(graph, method, nodes, crypto)?;
    match method {
        Method::AddNode {
            node_id,
            properties_msgpack,
        } => {
            let blob = crypto.seal(properties_msgpack);
            nodes
                .insert((graph, node_id.as_str()), blob.as_ref())
                .map_err(|e| e.to_string())?;
        }
        Method::RemoveNode { node_id } => {
            remove_durable_node(graph, node_id, nodes, edges, semantic, crypto)?;
        }
        Method::CompareAndSetNodeFields {
            node_id,
            conditions_msgpack,
            updates_msgpack,
        } => {
            apply_cas_node_fields_row(
                graph,
                node_id,
                conditions_msgpack,
                updates_msgpack,
                nodes,
                crypto,
            )?;
        }
        Method::AddEdge {
            source_id,
            target_id,
            properties_msgpack,
        } => {
            apply_add_edge_row(
                graph,
                source_id,
                target_id,
                properties_msgpack,
                nodes,
                edges,
                crypto,
            )?;
        }
        Method::RemoveEdge {
            source_id,
            target_id,
        } => {
            remove_durable_edge_pair(graph, source_id, target_id, edges)?;
        }
        Method::BatchUpdate { operations_msgpack } => {
            apply_batch_rows(graph, operations_msgpack, nodes, edges, semantic, crypto)?;
        }
        // ClearGraph and DeleteGraph share one arm because their row effect is
        // byte-identical.  For DeleteGraph the lifecycle caller performs the
        // native/resource drain guard before entering this row applier; keeping
        // its ordinary graph effect here as well means no low-level path
        // (including cross-modal and checkpoint pending methods) can silently
        // commit a no-op graph delete.
        Method::ClearGraph | Method::DeleteGraph { .. } => {
            clear_graph_rows(graph, nodes, edges, ledger)?;
            semantic.remove(graph).map_err(|error| error.to_string())?;
        }
        Method::AddEmbedding { node_id, embedding } => {
            upsert_durable_embedding(semantic, graph, node_id, embedding, crypto)?;
        }
        Method::MintWorkItemClaimCapability { .. }
        | Method::VerifyWorkItemClaimCapability { .. } => {
            return Err(
                "WorkItem claim capabilities require their native authority operation".to_string(),
            );
        }
        _ => {}
    }
    Ok(())
}

// ── O(1) edge-ordinal counter (CONCEPT:EG-KG.storage.redb-store #3) ────────────────────────────
//
// **Why this exists (profiling rationale).** Assigning an edge's ordinal used to
// RANGE-SCAN that (graph,src,tgt)'s existing edge rows on EVERY `AddEdge` to find
// `max+1` — O(degree) B-tree walks inside the held `WriteTransaction`. On a
// high-degree node every insert got slower as the node fan-out grew, burning the
// now-CPU-bound writer (post EG-024). This mirrors the EG-025 audit-tail fix: keep
// an in-memory per-(graph,src,tgt) next-ordinal counter and chain off it with NO
// per-op scan.
//
// **Why an in-memory counter is authoritative.** EG-026 gives each shard a single
// dedicated writer thread (`eg-redb-writer*`), and a graph routes deterministically
// to exactly one shard — so that thread is the ONLY mutator of its EDGES rows.
// Nothing can advance an ordinal behind our back, so a counter living in that
// thread's storage is correct. We hold it in a `thread_local` rather than a threaded
// parameter because the shared `commit_ops`/`commit_crossmodal` signatures are fixed
// by an out-of-scope caller (`redb_backend`); a thread-local is naturally scoped to
// the one writer thread and lives for its whole lifetime (= the `Pending` lifetime
// that holds the EG-025 audit cache). On any OTHER thread (the embedded one-op-per-
// txn path, tests, tooling) the counter is NOT authoritative — another thread could
// be the real writer — so those contexts fall back to an exact bounded B-tree tail
// seek.
//
// **Restart / correctness.** A fresh process ⇒ fresh writer thread ⇒ empty cache ⇒
// the first touch of each (graph,src,tgt) re-seeds from one bounded tail seek (max+1,
// or 0 when none), then advances in RAM. Edge removals on the writer thread
// (RemoveEdge/RemoveNode/ClearGraph/checkpoint-clear) INVALIDATE the relevant cache
// entries so a later AddEdge re-seeds from the post-removal state — preserving the
// exact "reset to 0 once all edges of a pair are gone" behavior of the old scan.
// Because the counter is seeded at the true `max+1` and only ever increments within
// the sole writer, an assigned ordinal can never collide with an existing row and is
// strictly monotonic per (graph,src,tgt).
/// `graph -> source -> target -> next ordinal to assign`, the shape of
/// [`EDGE_ORD_CACHE`]'s thread-local map.
pub(crate) type EdgeOrdCache = HashMap<String, HashMap<String, HashMap<String, u64>>>;

thread_local! {
    /// True iff this thread is a dedicated redb group-commit writer (`eg-redb-writer`
    /// / `eg-redb-writer-<i>`, CONCEPT:EG-KG.backend.sharded-k-way-durable). Computed once per thread; gates whether
    /// the in-RAM edge-ordinal counter below is authoritative.
    static IS_REDB_WRITER: bool = std::thread::current()
        .name()
        .map(|n| n.starts_with("eg-redb-writer"))
        .unwrap_or(false);

    /// `graph -> source -> target -> next ordinal to assign`. The u64 counter can
    /// represent `u32::MAX + 1` as an explicit exhausted sentinel after assigning
    /// the final valid durable ordinal; it is never serialized.
    /// The hierarchy keeps
    /// hot pair lookup expected O(1) while making whole-graph and whole-source
    /// invalidation O(1), rather than retaining over every cached pair.
    pub(super) static EDGE_ORD_CACHE: RefCell<EdgeOrdCache> = RefCell::new(HashMap::new());
}

/// Next free edge ordinal for a (src,tgt) pair in this graph.
///
/// O(1) on the dedicated writer thread (EG-026/EG-029): the in-RAM counter, seeded once
/// per (graph,src,tgt) from one bounded tail seek. Off the writer thread it is NOT
/// authoritative, so it performs the same exact O(log E) seek on every call.
pub(crate) fn next_edge_ordinal(
    edges: &ScopedOwnerTableMut<'_, (&str, &str, &str, u32), &[u8]>,
    graph: &str,
    src: &str,
    tgt: &str,
) -> Result<u32, String> {
    if !IS_REDB_WRITER.with(|w| *w) {
        return scan_next_edge_ordinal(edges, graph, src, tgt);
    }
    EDGE_ORD_CACHE.with(|c| -> Result<u32, String> {
        let mut cache = c.borrow_mut();
        let targets = cache
            .entry(graph.to_string())
            .or_default()
            .entry(src.to_string())
            .or_default();
        let next = match targets.get(tgt) {
            // Hot path: the cached counter — NO scan inside the held write txn.
            Some(&n) => n,
            // Cold path: first touch since open / restart — seed from one scan.
            None => u64::from(scan_next_edge_ordinal(edges, graph, src, tgt)?),
        };
        let ordinal =
            u32::try_from(next).map_err(|_| "edge ordinal space exhausted".to_string())?;
        targets.insert(tgt.to_string(), next + 1);
        Ok(ordinal)
    })
}

/// Seek the highest existing ordinal for `(graph, src, tgt)` and add one. The
/// composite key range is ordered by ordinal, so `next_back` makes this O(log E)
/// instead of walking all parallel rows for the pair. Used to seed the writer cache
/// and as the off-writer-thread fallback.
pub(crate) fn scan_next_edge_ordinal(
    edges: &ScopedOwnerTableMut<'_, (&str, &str, &str, u32), &[u8]>,
    graph: &str,
    src: &str,
    tgt: &str,
) -> Result<u32, String> {
    let max = edges
        .range_inclusive((graph, src, tgt, 0u32), (graph, src, tgt, u32::MAX))?
        .next_back()
        .transpose()
        .map_err(|e| e.to_string())?
        .map(|(key, _)| key.value().3);
    max.map_or(Ok(0), |ordinal| {
        ordinal
            .checked_add(1)
            .ok_or_else(|| "edge ordinal space exhausted".to_string())
    })
}

/// Drop the cached next-ordinal for ONE (graph,src,tgt) (RemoveEdge). No-op off the
/// writer thread / when the key was never cached.
pub(crate) fn invalidate_edge_ord(graph: &str, src: &str, tgt: &str) {
    EDGE_ORD_CACHE.with(|c| {
        let mut cache = c.borrow_mut();
        let mut remove_graph = false;
        if let Some(sources) = cache.get_mut(graph) {
            let remove_source = sources.get_mut(src).is_some_and(|targets| {
                targets.remove(tgt);
                targets.is_empty()
            });
            if remove_source {
                sources.remove(src);
            }
            remove_graph = sources.is_empty();
        }
        if remove_graph {
            cache.remove(graph);
        }
    });
}

/// Drop every cached next-ordinal whose SOURCE is `node` in `graph` (RemoveNode sweeps
/// exactly that node's outgoing edges).
pub(crate) fn invalidate_node_edge_ords(graph: &str, node: &str) {
    EDGE_ORD_CACHE.with(|c| {
        let mut cache = c.borrow_mut();
        let remove_graph = cache.get_mut(graph).is_some_and(|sources| {
            sources.remove(node);
            sources.is_empty()
        });
        if remove_graph {
            cache.remove(graph);
        }
    });
}

/// Drop every cached next-ordinal for `graph` (ClearGraph / purge / checkpoint re-seed).
pub(crate) fn invalidate_graph_edge_ords(graph: &str) {
    EDGE_ORD_CACHE.with(|c| {
        c.borrow_mut().remove(graph);
    });
}

/// Bounded exact-binary proof seam for the cold edge-ordinal seed and the
/// writer-local invalidation path. It writes only opaque synthetic rows into the
/// caller-owned private probe database and returns raw semantic outcomes; release
/// evidence never includes the supplied path.
#[doc(hidden)]
/// How long the edge-ordinal probe worker may run. Its scale is capped at
/// 100_000 synthetic rows into a caller-owned private database, so this is
/// generous by more than an order of magnitude.
const EDGE_ORDINAL_PROBE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(300);

pub fn exact_performance_probe_edge_ordinal(
    database_path: &std::path::Path,
    parallel_rows: usize,
) -> Result<(u32, u32, u32), String> {
    if parallel_rows == 0 || parallel_rows > 100_000 || parallel_rows > u32::MAX as usize {
        return Err("edge-ordinal probe scale is outside its bound".to_string());
    }
    let path = database_path.to_path_buf();
    let worker = std::thread::Builder::new()
        .name("eg-redb-writer-g37".to_string())
        .spawn(move || -> Result<(u32, u32, u32), String> {
            let shard = Shard::open(&path)?;
            let graph = "g37".to_string();
            let members = shard.graph_members(std::slice::from_ref(&graph))?;
            let (group, batches) = shard.admit_maintenance(&members, "probe/g37")?;
            let write = ShardWrite::open(&shard, &group, &members, &batches)?;
            let result = run_edge_ordinal_probe_rows(&write, &graph, parallel_rows);
            let finished = write.finish();
            let result = match (result, finished) {
                (Ok(value), Ok(())) => Ok(value),
                (Err(error), _) | (Ok(_), Err(error)) => Err(error),
            };
            shard.mutations().abort_group(group)?;
            result
        })
        .map_err(|error| error.to_string())?;
    // The probe's scale is capped at 100_000 rows above, so a worker still
    // running after this deadline is wedged rather than slow -- and a probe
    // that hangs reports nothing at all, which is the failure mode this bound
    // exists to prevent.
    crate::bounded_join::join_within(
        worker,
        "the edge-ordinal probe worker",
        EDGE_ORDINAL_PROBE_TIMEOUT,
    )?
}

fn run_edge_ordinal_probe_rows(
    write: &ShardWrite<'_>,
    graph: &str,
    parallel_rows: usize,
) -> Result<(u32, u32, u32), String> {
    let mut edges = write.graph(graph)?.open_scoped_table(EDGES)?;
    let value = [0u8];
    for ordinal in 0..parallel_rows as u32 {
        edges
            .insert((graph, "source", "target", ordinal), value.as_slice())
            .map_err(|error| error.to_string())?;
    }
    let cold = next_edge_ordinal(&edges, graph, "source", "target")?;
    let hot = next_edge_ordinal(&edges, graph, "source", "target")?;
    invalidate_edge_ord(graph, "source", "target");
    let reseeded = next_edge_ordinal(&edges, graph, "source", "target")?;
    Ok((cold, hot, reseeded))
}

pub(crate) fn apply_batch_add_node_row(
    graph: &str,
    index: usize,
    id: &str,
    mut properties_msgpack: Vec<u8>,
    upsert: bool,
    nodes: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    if upsert {
        let current = nodes
            .get((graph, id))
            .map_err(|error| error.to_string())?
            .map(|stored| crypto.unseal(stored.value()))
            .transpose()?;
        if let Some(current) = current {
            properties_msgpack =
                crate::algorithms::merge_batch_node_properties(&current, &properties_msgpack)
                    .map_err(|reason| {
                        format!("BatchUpdate op[{index}] cannot upsert node '{id}': {reason}")
                    })?;
        }
    }
    let sealed = crypto.seal(&properties_msgpack);
    nodes
        .insert((graph, id), sealed.as_ref())
        .map_err(|error| error.to_string())?;
    Ok(())
}

/// `BatchOperation::AddEdge`.  Both endpoints must exist at this point in the
/// batch; an upsert first drops the existing pair.
pub(crate) struct BatchEdgeRow<'a> {
    graph: &'a str,
    index: usize,
    source: &'a str,
    target: &'a str,
    properties_msgpack: &'a [u8],
    upsert: bool,
}

pub(crate) fn apply_batch_add_edge_row(
    input: BatchEdgeRow<'_>,
    nodes: &ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    edges: &mut ScopedOwnerTableMut<'_, (&str, &str, &str, u32), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let BatchEdgeRow {
        graph,
        index,
        source,
        target,
        properties_msgpack,
        upsert,
    } = input;
    let source_exists = nodes
        .get((graph, source))
        .map_err(|error| error.to_string())?
        .is_some();
    let target_exists = nodes
        .get((graph, target))
        .map_err(|error| error.to_string())?
        .is_some();
    if !source_exists || !target_exists {
        return Err(format!(
            "BatchUpdate op[{index}] edge endpoints must exist at that point in the batch"
        ));
    }
    if upsert {
        remove_durable_edge_pair(graph, source, target, edges)?;
    }
    let ordinal = next_edge_ordinal(edges, graph, source, target)?;
    let sealed = crypto.seal(properties_msgpack);
    edges
        .insert((graph, source, target, ordinal), sealed.as_ref())
        .map_err(|error| error.to_string())?;
    Ok(())
}

/// `BatchOperation::AddEmbedding`.
///
/// CONCEPT:EG-KG.compute.rank-dim-mismatch-guard (BUG-007): `semantic_store` is
/// a scratch decode written back durably ONLY after the whole batch loop returns
/// `Ok` (`if semantic_dirty { write_semantic_store(...) }` in the caller), and
/// that caller's caller drops the enclosing `WriteTransaction` without
/// committing on any `Err` -- so, exactly like the "node does not exist" check
/// here, a rejected write never partially lands durably.
pub(crate) fn apply_batch_add_embedding_row(
    graph: &str,
    index: usize,
    id: String,
    embedding: Vec<f32>,
    nodes: &ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    semantic_store: &mut crate::compute::semantic::SemanticStore,
) -> Result<(), String> {
    if nodes
        .get((graph, id.as_str()))
        .map_err(|error| error.to_string())?
        .is_none()
    {
        return Err(format!(
            "BatchUpdate op[{index}] embedding node '{id}' does not exist"
        ));
    }
    semantic_store
        .add_embedding(id, embedding)
        .map_err(|error| format!("BatchUpdate op[{index}] {error}"))?;
    Ok(())
}

/// Apply a decoded `BatchUpdate` op-list as row writes.
/// `BatchOperation::AddNode`.  An upsert merges over the durable pre-image
/// first; a plain add replaces the row outright.
pub(crate) fn apply_batch_rows(
    graph: &str,
    operations_msgpack: &[u8],
    nodes: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    edges: &mut ScopedOwnerTableMut<'_, (&str, &str, &str, u32), &[u8]>,
    semantic: &mut ScopedOwnerTableMut<'_, &str, &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    use crate::algorithms::BatchOperation;

    // The compute crate owns the public schema. Decode it here too instead of
    // maintaining a second set of field aliases at the durability boundary.
    // Any error aborts the enclosing redb transaction: never acknowledge an
    // opaque or partially applied batch.
    let operations = crate::algorithms::decode_batch_operations(operations_msgpack)?;
    let has_semantic_operations = operations.iter().any(|operation| {
        matches!(
            operation,
            BatchOperation::RemoveNode { .. } | BatchOperation::AddEmbedding { .. }
        )
    });
    // Load the graph's vector store at most once. A large embedding batch must not
    // repeatedly deserialize and reserialize the whole semantic blob per element.
    let mut semantic_store = has_semantic_operations
        .then(|| read_semantic_store(semantic, graph, crypto))
        .transpose()?
        .flatten()
        .unwrap_or_default();
    let mut semantic_dirty = false;
    for (index, operation) in operations.into_iter().enumerate() {
        match operation {
            BatchOperation::AddNode {
                id,
                properties_msgpack,
                upsert,
            } => {
                apply_batch_add_node_row(
                    graph,
                    index,
                    id.as_str(),
                    properties_msgpack,
                    upsert,
                    nodes,
                    crypto,
                )?;
            }
            BatchOperation::RemoveNode { id } => {
                remove_durable_node_rows(graph, &id, nodes, edges)?;
                semantic_dirty |= semantic_store.remove_embedding(&id);
            }
            BatchOperation::AddEdge {
                source,
                target,
                properties_msgpack,
                upsert,
            } => {
                apply_batch_add_edge_row(
                    BatchEdgeRow {
                        graph,
                        index,
                        source: source.as_str(),
                        target: target.as_str(),
                        properties_msgpack: &properties_msgpack,
                        upsert,
                    },
                    nodes,
                    edges,
                    crypto,
                )?;
            }
            BatchOperation::RemoveEdge { source, target } => {
                remove_durable_edge_pair(graph, &source, &target, edges)?;
            }
            BatchOperation::AddEmbedding { id, embedding } => {
                apply_batch_add_embedding_row(
                    graph,
                    index,
                    id,
                    embedding,
                    nodes,
                    &mut semantic_store,
                )?;
                semantic_dirty = true;
            }
        }
    }
    if semantic_dirty {
        write_semantic_store(semantic, graph, &semantic_store, crypto)?;
    }
    Ok(())
}
