//! Reading a graph back out: whole-graph dumps, keyset-paged dumps, the
//! boot-time catalog scan and the `graph_meta` record codec.
//!
//! Two reader classes, and the split is the row class: a per-graph dump reads
//! through that graph's `ScopedRead` and enumerates its rows with `scope_rows`,
//! while the catalog scan reads `graph_meta` -- a FILE-WIDE table -- through the
//! control scope, because the boot path has to learn the graph names before any
//! graph scope can be bound (G2-B2).
//!
//! [`read_all_dumps`] is the composition of the two, and it is the one place the
//! circularity that made `graph_meta` file-wide is visible: it FIRST enumerates
//! the catalog on the control scope to learn which graphs the file hosts, and
//! only THEN binds each of them in turn and reads its rows through that graph's
//! own scoped read. It can no longer be one whole-table `iter()` bucketed by key
//! prefix, because a scoped read cannot see another graph's rows -- which is the
//! point of the scoping, not an obstacle to it.

use eg_storage::{GraphShardOwner, ScopeRow, ScopedRead};
use redb::ReadableTable;

use super::shard::{Shard, ShardWrite};
use super::*;

/// One graph's rows in a table whose key is `(graph, key)`.
type OnePartRows<V> = Vec<(String, V)>;

/// One graph's rows in a table whose key is `(graph, first, second)`.
type TwoPartRows<K2, V> = Vec<((String, K2), V)>;

/// One page of node rows, whether the node scan is drained, and the durable
/// node cursor this page leaves behind.
type NodePageScan = (OnePartRows<Vec<u8>>, bool, Option<String>);

/// One page of edge rows, whether the edge scan is drained, and the durable
/// edge cursor this page leaves behind.
type EdgePageScan = (
    Vec<(String, String, Vec<u8>)>,
    bool,
    Option<(String, String, u32)>,
);

/// Whether a scope-bounded scan has no further row of its own scope.
///
/// `scope_rows` already stops on the first key that leaves the scope, so "the
/// next row belongs to another graph" is no longer expressible and "no next
/// row" is the whole condition. A storage error is propagated rather than read
/// as the end of the scan, so a failing peek cannot look like an exhausted one.
fn scan_exhausted<K, V>(
    rows: &mut impl Iterator<Item = ScopeRow<'static, K, V>>,
) -> Result<bool, String>
where
    K: redb::Value + 'static,
    V: redb::Value + 'static,
{
    match rows.next() {
        Some(row) => row.map(|_| false),
        None => Ok(true),
    }
}

pub(crate) fn dump_graph_2str_bytes(
    read: &ScopedRead<'_, GraphShardOwner>,
    definition: redb::TableDefinition<'static, (&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<OnePartRows<Vec<u8>>, String> {
    let table = read.scoped_owner_table(definition)?;
    let mut out = Vec::new();
    for row in table.scope_rows()? {
        let (k, v) = row?;
        let (_, second) = k.value();
        out.push((second.to_string(), crypto.unseal(v.value())?));
    }
    Ok(out)
}

pub(crate) fn dump_graph_2str_text(
    read: &ScopedRead<'_, GraphShardOwner>,
    definition: redb::TableDefinition<'static, (&str, &str), &str>,
) -> Result<OnePartRows<String>, String> {
    let table = read.scoped_owner_table(definition)?;
    let mut out = Vec::new();
    for row in table.scope_rows()? {
        let (k, v) = row?;
        let (_, second) = k.value();
        out.push((second.to_string(), v.value().to_string()));
    }
    Ok(out)
}

pub(crate) fn dump_graph_2str_u64(
    read: &ScopedRead<'_, GraphShardOwner>,
    definition: redb::TableDefinition<'static, (&str, &str), u64>,
) -> Result<OnePartRows<u64>, String> {
    let table = read.scoped_owner_table(definition)?;
    let mut out = Vec::new();
    for row in table.scope_rows()? {
        let (k, v) = row?;
        let (_, second) = k.value();
        out.push((second.to_string(), v.value()));
    }
    Ok(out)
}

pub(crate) fn dump_graph_3str_text(
    read: &ScopedRead<'_, GraphShardOwner>,
    definition: redb::TableDefinition<'static, (&str, &str, &str), &str>,
) -> Result<TwoPartRows<String, String>, String> {
    let table = read.scoped_owner_table(definition)?;
    let mut out = Vec::new();
    for row in table.scope_rows()? {
        let (k, v) = row?;
        let (_, second, third) = k.value();
        out.push((
            (second.to_string(), third.to_string()),
            v.value().to_string(),
        ));
    }
    Ok(out)
}

pub(crate) fn dump_graph_3str_bytes(
    read: &ScopedRead<'_, GraphShardOwner>,
    definition: redb::TableDefinition<'static, (&str, &str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<TwoPartRows<String, Vec<u8>>, String> {
    let table = read.scoped_owner_table(definition)?;
    let mut out = Vec::new();
    for row in table.scope_rows()? {
        let (k, v) = row?;
        let (_, second, third) = k.value();
        out.push((
            (second.to_string(), third.to_string()),
            crypto.unseal(v.value())?,
        ));
    }
    Ok(out)
}

pub(crate) fn dump_graph_str_u64_text(
    read: &ScopedRead<'_, GraphShardOwner>,
    definition: redb::TableDefinition<'static, (&str, &str, u64), &str>,
) -> Result<TwoPartRows<u64, String>, String> {
    let table = read.scoped_owner_table(definition)?;
    let mut out = Vec::new();
    for row in table.scope_rows()? {
        let (k, v) = row?;
        let (_, second, third) = k.value();
        out.push(((second.to_string(), third), v.value().to_string()));
    }
    Ok(out)
}

pub(crate) fn dump_graph_3str_u64(
    read: &ScopedRead<'_, GraphShardOwner>,
    definition: redb::TableDefinition<'static, (&str, &str, &str), u64>,
) -> Result<TwoPartRows<String, u64>, String> {
    let table = read.scoped_owner_table(definition)?;
    let mut out = Vec::new();
    for row in table.scope_rows()? {
        let (k, v) = row?;
        let (_, second, third) = k.value();
        out.push(((second.to_string(), third.to_string()), v.value()));
    }
    Ok(out)
}

/// Read every `development_lane_*`/`resource_*` row of the read's OWN graph
/// (BUG-CX-096). Called once from [`read_graph_dump`]; split out purely to keep
/// that function's own complexity from absorbing all 19 table scans.
///
/// Every table here is scope-prefixed, so the graph is no longer an argument:
/// the bound comes from the capability that issued `read`, and a scan cannot
/// reach past its own scope even if a caller wanted it to.
pub(crate) fn read_native_operation_dump_rows(
    read: &ScopedRead<'_, GraphShardOwner>,
    crypto: DurableCrypto<'_>,
) -> Result<NativeOperationDumpRows, String> {
    let pressure_index = {
        let table = read.scoped_owner_table(development_lane::PRESSURE_INDEX)?;
        let mut out = Vec::new();
        for row in table.scope_rows()? {
            let (k, v) = row?;
            let (_, tenant, scope, metric, value, counter_key) = k.value();
            out.push((
                (
                    tenant.to_string(),
                    scope.to_string(),
                    metric.to_string(),
                    value,
                    counter_key.to_string(),
                ),
                v.value(),
            ));
        }
        out
    };
    Ok(NativeOperationDumpRows {
        development_lane_holds: dump_graph_2str_bytes(read, development_lane::HOLDS, crypto)?,
        development_lane_tenant_index: dump_graph_3str_text(read, development_lane::TENANT_INDEX)?,
        development_lane_lane_index: dump_graph_3str_text(read, development_lane::LANE_INDEX)?,
        development_lane_repository_branch_index: dump_graph_3str_text(
            read,
            development_lane::REPOSITORY_BRANCH_INDEX,
        )?,
        development_lane_worktree_index: dump_graph_2str_text(
            read,
            development_lane::WORKTREE_INDEX,
        )?,
        development_lane_work_item_index: dump_graph_str_u64_text(
            read,
            development_lane::WORK_ITEM_INDEX,
        )?,
        development_lane_counters: dump_graph_2str_bytes(read, development_lane::COUNTERS, crypto)?,
        development_lane_pressure_index: pressure_index,
        development_lane_policies: dump_graph_2str_bytes(read, development_lane::POLICIES, crypto)?,
        development_lane_invocations: dump_graph_3str_bytes(
            read,
            development_lane::INVOCATIONS,
            crypto,
        )?,
        resource_reservations: dump_graph_2str_bytes(read, RESOURCE_RESERVATIONS, crypto)?,
        resource_reservation_tenant_index: dump_graph_3str_text(
            read,
            RESOURCE_RESERVATION_TENANT_INDEX,
        )?,
        resource_reservation_attempts: dump_graph_str_u64_text(
            read,
            RESOURCE_RESERVATION_ATTEMPTS,
        )?,
        resource_hosts: dump_graph_2str_bytes(read, RESOURCE_HOSTS, crypto)?,
        resource_exclusivity: dump_graph_2str_text(read, RESOURCE_EXCLUSIVITY)?,
        resource_fairness: dump_graph_2str_bytes(read, RESOURCE_FAIRNESS, crypto)?,
        resource_concurrency: dump_graph_2str_u64(read, RESOURCE_CONCURRENCY)?,
        resource_anti_affinity: dump_graph_3str_u64(read, RESOURCE_ANTI_AFFINITY)?,
        resource_disk_policies: dump_graph_2str_bytes(read, RESOURCE_DISK_POLICIES, crypto)?,
    })
}

/// One graph's four core row families, read through that graph's own scope.
///
/// Shared by [`read_graph_dump`] and [`read_all_dumps`] because they read
/// exactly the same rows and differ only in what surrounds them: the one-graph
/// path adds the native diagnostic tables, the whole-store path repeats this
/// per catalog entry.
struct GraphCoreRows {
    nodes: OnePartRows<Vec<u8>>,
    edges: Vec<(String, String, Vec<u8>)>,
    ledger: Vec<String>,
    semantic: Vec<u8>,
}

fn read_graph_core_rows(
    read: &ScopedRead<'_, GraphShardOwner>,
    graph: &str,
    crypto: DurableCrypto<'_>,
) -> Result<GraphCoreRows, String> {
    let nodes_table = read.scoped_owner_table(NODES)?;
    let mut nodes = Vec::new();
    for row in nodes_table.scope_rows()? {
        let (k, v) = row?;
        let (_, id) = k.value();
        nodes.push((id.to_string(), crypto.unseal(v.value())?));
    }
    let edges_table = read.scoped_owner_table(EDGES)?;
    let mut edges = Vec::new();
    for row in edges_table.scope_rows()? {
        let (k, v) = row?;
        let (_, source, target, _) = k.value();
        edges.push((
            source.to_string(),
            target.to_string(),
            crypto.unseal(v.value())?,
        ));
    }
    let ledger_table = read.scoped_owner_table(LEDGER)?;
    let mut ledger = Vec::new();
    for row in ledger_table.scope_rows()? {
        let (_, v) = row?;
        ledger.push(v.value().to_string());
    }
    // `semantic_store` is one row per graph keyed by the graph name itself, so
    // the scope bound makes the key a lookup rather than a scan.
    let semantic = read
        .scoped_owner_table(SEMANTIC)?
        .get(graph)?
        .map(|v| crypto.unseal(v.value()))
        .transpose()?
        .unwrap_or_default();
    Ok(GraphCoreRows {
        nodes,
        edges,
        ledger,
        semantic,
    })
}

/// One graph's catalog row, read on the scope that owns the catalog.
///
/// `graph_meta` is FILE-WIDE, so this is a control-scope read and never a
/// graph-scope one: [`Shard::control_read`] exists before any graph is bound,
/// which is exactly what lets a caller ask "does this graph exist here?"
/// WITHOUT binding it first.
pub(crate) fn read_catalog_record(
    shard: &Shard,
    graph: &str,
) -> Result<Option<GraphMetaRecord>, String> {
    let control = shard.control_read()?;
    let catalog = control.open_owner_table(GRAPH_META)?;
    let Some(value) = catalog.get(graph).map_err(|e| e.to_string())? else {
        return Ok(None);
    };
    decode_meta_record(graph, value.value()).map(Some)
}

/// Every catalog row of the file, in key order.
///
/// The boot scan's primitive, and the first half of [`read_all_dumps`]: a graph
/// scope cannot be bound until its name is known, and the name is only knowable
/// from here.
fn read_graph_catalog(shard: &Shard) -> Result<Vec<(String, GraphMetaRecord)>, String> {
    let control = shard.control_read()?;
    let catalog = control.open_owner_table(GRAPH_META)?;
    let mut out = Vec::new();
    for row in catalog.iter().map_err(|e| e.to_string())? {
        let (k, v) = row.map_err(|e| e.to_string())?;
        let graph = k.value().to_string();
        let record = decode_meta_record(&graph, v.value())?;
        out.push((graph, record));
    }
    Ok(out)
}

/// Read ONE graph's durable rows into a read-only [`GraphDump`] (CONCEPT:EG-KG.storage.100m-tenant —
/// tenant rehydration). Every scan is bounded to that graph's own scope, so a cold
/// tenant rehydrates from redb without reading the whole store — and, unlike the
/// key-prefix scans this replaces, without the file's other graphs even being
/// reachable. This materialization view is intentionally rejected by
/// [`apply_checkpoint`]; cross-store moves use the complete fenced
/// `RawGraphRows`/`RedbBackend::reshard_graph` protocol. `None` means the graph has
/// no durable identity (`graph_meta`) row.
pub(crate) fn read_graph_dump(
    shard: &Shard,
    graph: &str,
    crypto: DurableCrypto<'_>,
) -> Result<Option<GraphDump>, String> {
    // The catalog answers "is this a graph of this file?" on the control scope,
    // BEFORE a graph scope is bound: binding an unknown graph would create one.
    let Some(meta_record) = read_catalog_record(shard, graph)? else {
        return Ok(None);
    };
    let handle = shard.graph(graph)?;
    let read = shard.read(&handle)?;
    let source_snapshot_version = eg_transaction::version(&read)?;
    let core = read_graph_core_rows(&read, graph, crypto)?;
    // BUG-CX-096: expose native lane/resource authority rows for diagnostics.
    // The private read-only origin marker prevents this incomplete view from
    // being replayed as a checkpoint or transfer image.
    let native = read_native_operation_dump_rows(&read, crypto)?;

    Ok(Some(GraphDump {
        kind: GraphDumpKind::DurableReadOnlyMaterialization,
        graph: graph.to_string(),
        name: meta_record.name,
        graph_type: meta_record.graph_type,
        incarnation_id: meta_record.incarnation_id,
        source_snapshot_version,
        schema_sources: meta_record.schema_sources,
        nodes: core.nodes,
        edges: core.edges,
        ledger: core.ledger,
        semantic: core.semantic,
        native,
    }))
}

/// One bounded, SOURCE-level page of ONE graph's durable rows (CONCEPT:EG-KG.memory.graph-guided-paging,
/// CONCEPT:EG-KG.sharding.paged-lazy-open, L38 "paged adjacency"). The paged sibling of
/// [`read_graph_dump`]: instead of collecting the WHOLE graph's node/edge rows into one
/// `Vec` before returning (the thing that makes a lazy first-open of a 10M+-node/token
/// graph spike RAM), this walks the SAME per-graph scope scan but stops after `page_size`
/// combined rows and reports whether more remain — so the caller (`RedbBackend`'s
/// `GraphMaterializer::materialize_page` override) never holds more than one page's worth
/// of rows in memory at a time, at the SOURCE, not just when replaying into `GraphCore`.
pub(crate) struct GraphDumpPage {
    pub nodes: Vec<(String, Vec<u8>)>,
    pub edges: Vec<(String, String, Vec<u8>)>,
    /// Only populated on the first page (no keyset cursor) — mirrors
    /// [`eg_core::registry::GraphMaterializer::materialize_page`]'s single-blob
    /// convention so a paged replay attaches the semantic store exactly once.
    pub semantic: Vec<u8>,
    /// Authoritative graph-control state, populated on the first page only.
    pub schema_sources: Option<std::sync::Arc<crate::graph::GraphSchemaSources>>,
    pub nodes_exhausted: bool,
    pub edges_exhausted: bool,
    /// Effective durable keyset positions after this page. They preserve the
    /// prior value when a page advances only the other row family.
    pub node_after: Option<String>,
    pub edge_after: Option<(String, String, u32)>,
    pub incarnation_id: String,
    pub source_snapshot_version: u64,
}

/// A bounded page cursor for [`read_graph_dump_page`] — every argument except
/// the routing `shard`/`graph`/`crypto`, borrowed so the caller's owned
/// [`crate::server::persistence::redb_backend::PageQuery`] (or a test literal)
/// need not be cloned just to make this call.
pub(crate) struct PageCursorRef<'a> {
    pub node_offset: usize,
    pub edge_offset: usize,
    pub node_after: Option<&'a str>,
    pub edge_after: Option<(&'a str, &'a str, u32)>,
    pub page_size: usize,
}

/// What one row of a paged node scan contributes to the page.
pub(crate) enum GraphDumpPageNodeRowStep {
    /// At or before the caller's cursor: an earlier page already returned it.
    BeforeCursor,
    Pushed(String, Vec<u8>),
}

/// One `(graph, id) -> value` NODES row as the scope-bounded scan yields it.
pub(crate) type GraphDumpNodeRow = ScopeRow<'static, (&'static str, &'static str), &'static [u8]>;

pub(crate) fn graph_dump_page_node_row_step(
    row: GraphDumpNodeRow,
    resume_after: Option<&str>,
    crypto: DurableCrypto<'_>,
) -> Result<GraphDumpPageNodeRowStep, String> {
    let (k, v) = row?;
    let (_, id) = k.value();
    if resume_after.is_some_and(|cursor| id <= cursor) {
        return Ok(GraphDumpPageNodeRowStep::BeforeCursor);
    }
    Ok(GraphDumpPageNodeRowStep::Pushed(
        id.to_string(),
        crypto.unseal(v.value())?,
    ))
}

// Nodes: walk this graph's scope from its first row, drop everything at or
// before the caller's cursor, then take at most `page_size` more. One extra
// `.next()` after filling the page (NOT collected) tells us whether more nodes
// remain.
//
// **Why the resume is a forward skip and not a seek.** The keyed resume this
// replaces was `range((graph, node_after)..)`, which no scope-bounded accessor
// can express: `range_inclusive` needs BOTH bounds inside the scope and the
// trailing key component here is a `&str`, which has no maximum -- any string
// having a candidate sentinel as a proper prefix sorts after it. Inventing an
// end key would be an assumption this layer cannot enforce, because node ids
// are caller bytes. The cursor is therefore re-found by skipping, which costs a
// scan of the already-returned prefix per page; the page's own memory bound --
// the property this function exists for -- is unaffected, and the seek returns
// unchanged the day a scope-bounded `range_from` lands beside `scope_rows`.
pub(crate) fn read_graph_dump_page_nodes(
    read: &ScopedRead<'_, GraphShardOwner>,
    node_after: Option<&str>,
    page_size: usize,
    crypto: DurableCrypto<'_>,
) -> Result<NodePageScan, String> {
    let table = read.scoped_owner_table(NODES)?;
    let mut nodes = Vec::with_capacity(page_size.min(1024));
    let mut nodes_exhausted = true;
    let mut next_node_after = node_after.map(str::to_string);
    let mut rows = table.scope_rows()?;
    while nodes.len() < page_size {
        let Some(row) = rows.next() else { break };
        match graph_dump_page_node_row_step(row, node_after, crypto)? {
            GraphDumpPageNodeRowStep::BeforeCursor => continue,
            GraphDumpPageNodeRowStep::Pushed(id, bytes) => {
                next_node_after = Some(id.clone());
                nodes.push((id, bytes));
                nodes_exhausted = false; // provisional; corrected by the peek below
            }
        }
    }
    if !nodes_exhausted {
        nodes_exhausted = scan_exhausted(&mut rows)?;
    }
    Ok((nodes, nodes_exhausted, next_node_after))
}

/// What one row of a paged edge scan contributes to the page.
pub(crate) enum GraphDumpPageEdgeRowStep {
    /// At or before the caller's cursor: an earlier page already returned it.
    BeforeCursor,
    Pushed(String, String, u32, Vec<u8>),
}

/// One `(graph, source, target, ordinal) -> value` EDGES row as the
/// scope-bounded scan yields it.
pub(crate) type GraphDumpEdgeRow =
    ScopeRow<'static, (&'static str, &'static str, &'static str, u32), &'static [u8]>;

pub(crate) fn graph_dump_page_edge_row_step(
    row: GraphDumpEdgeRow,
    resume_after: Option<(&str, &str, u32)>,
    crypto: DurableCrypto<'_>,
) -> Result<GraphDumpPageEdgeRowStep, String> {
    let (k, v) = row?;
    let (_, source, target, ordinal) = k.value();
    if resume_after.is_some_and(|cursor| (source, target, ordinal) <= cursor) {
        return Ok(GraphDumpPageEdgeRowStep::BeforeCursor);
    }
    Ok(GraphDumpPageEdgeRowStep::Pushed(
        source.to_string(),
        target.to_string(),
        ordinal,
        crypto.unseal(v.value())?,
    ))
}

// Edges: only once every node has been paged in (mirrors `apply_material_page`'s
// nodes-before-edges ordering, so a partially-opened graph never has an edge
// dangling on a not-yet-added node), spend the page's remaining budget on edges.
// `should_scan` is exactly "no more node rows remain for this graph AND the page
// still has budget left" (the same nodes-first gate `apply_material_page` uses).
// The returned `bool` tracks whether the EDGE scan itself is drained (only
// meaningful once nodes are exhausted); the caller always ANDs it with
// `nodes_exhausted`, so a page that is still working through nodes never
// falsely reports edges done. The cursor is re-found by skipping for the reason
// given on [`read_graph_dump_page_nodes`], and even more strictly here: an edge
// key's trailing components are two caller `&str`s and an ordinal.
pub(crate) fn read_graph_dump_page_edges(
    read: &ScopedRead<'_, GraphShardOwner>,
    edge_after: Option<(&str, &str, u32)>,
    edge_budget: usize,
    should_scan: bool,
    crypto: DurableCrypto<'_>,
) -> Result<EdgePageScan, String> {
    let mut edges = Vec::new();
    let mut edges_done_this_call = false;
    let mut next_edge_after = edge_after
        .map(|(source, target, ordinal)| (source.to_string(), target.to_string(), ordinal));
    if !should_scan {
        return Ok((edges, edges_done_this_call, next_edge_after));
    }
    let table = read.scoped_owner_table(EDGES)?;
    let mut rows = table.scope_rows()?;
    edges_done_this_call = true;
    while edges.len() < edge_budget {
        let Some(row) = rows.next() else { break };
        match graph_dump_page_edge_row_step(row, edge_after, crypto)? {
            GraphDumpPageEdgeRowStep::BeforeCursor => continue,
            GraphDumpPageEdgeRowStep::Pushed(source, target, ordinal, bytes) => {
                next_edge_after = Some((source.clone(), target.clone(), ordinal));
                edges.push((source, target, bytes));
                edges_done_this_call = false;
            }
        }
    }
    if !edges_done_this_call {
        edges_done_this_call = scan_exhausted(&mut rows)?;
    }
    Ok((edges, edges_done_this_call, next_edge_after))
}

pub(crate) fn read_graph_dump_page_semantic(
    read: &ScopedRead<'_, GraphShardOwner>,
    graph: &str,
    node_after: Option<&str>,
    edge_after: Option<(&str, &str, u32)>,
    crypto: DurableCrypto<'_>,
) -> Result<Vec<u8>, String> {
    if node_after.is_some() || edge_after.is_some() {
        return Ok(Vec::new());
    }
    Ok(read
        .scoped_owner_table(SEMANTIC)?
        .get(graph)?
        .map(|v| crypto.unseal(v.value()))
        .transpose()?
        .unwrap_or_default())
}

pub(crate) fn read_graph_dump_page(
    shard: &Shard,
    graph: &str,
    crypto: DurableCrypto<'_>,
    cursor: PageCursorRef<'_>,
) -> Result<Option<GraphDumpPage>, String> {
    let PageCursorRef {
        node_offset: _node_offset,
        edge_offset: _edge_offset,
        node_after,
        edge_after,
        page_size,
    } = cursor;
    let page_size = page_size.max(1);
    let Some(meta_record) = read_catalog_record(shard, graph)? else {
        return Ok(None);
    };
    let handle = shard.graph(graph)?;
    let read = shard.read(&handle)?;
    // The graph's authoritative version is the kernel's ledger row for this
    // scope, resolved on the same read the rows come from.
    let source_snapshot_version = eg_transaction::version(&read)?;

    let (nodes, nodes_exhausted, next_node_after) =
        read_graph_dump_page_nodes(&read, node_after, page_size, crypto)?;

    let edge_budget = page_size.saturating_sub(nodes.len());
    let should_scan_edges = nodes_exhausted && edge_budget > 0;
    let (edges, edges_done_this_call, next_edge_after) =
        read_graph_dump_page_edges(&read, edge_after, edge_budget, should_scan_edges, crypto)?;
    let edges_exhausted = nodes_exhausted && edges_done_this_call;

    let semantic = read_graph_dump_page_semantic(&read, graph, node_after, edge_after, crypto)?;

    let first_page = node_after.is_none() && edge_after.is_none();
    Ok(Some(GraphDumpPage {
        nodes,
        edges,
        semantic,
        schema_sources: first_page.then_some(meta_record.schema_sources),
        nodes_exhausted,
        edges_exhausted,
        node_after: next_node_after,
        edge_after: next_edge_after,
        incarnation_id: meta_record.incarnation_id,
        source_snapshot_version,
    }))
}

#[cfg(test)]
mod keyset_page_tests {
    use super::*;

    #[test]
    fn durable_decode_rejects_declared_allocation_bomb() {
        let allocation_bomb = [0xdd, 0xff, 0xff, 0xff, 0xff];
        assert!(decode_durable::<Vec<serde_json::Value>>(&allocation_bomb).is_err());
    }

    #[test]
    fn graph_metadata_requires_the_current_version_and_complete_identity() {
        let sources = crate::graph::GraphSchemaSources::default();
        let encoded = encode_meta_record(
            "graph",
            GraphType::Global,
            "incarnation:test:current",
            &sources,
        )
        .unwrap();
        let decoded = decode_meta_record("graph", &encoded).unwrap();
        assert_eq!(decoded.name, "graph");
        assert_eq!(decoded.incarnation_id, "incarnation:test:current");
        assert_eq!(decoded.schema_sources.as_ref(), &sources);

        let unversioned = rmp_serde::to_vec_named(&serde_json::json!({
            "name": "graph",
            "graph_type": GraphType::Global,
            "incarnation_id": "incarnation:test:retired"
        }))
        .unwrap();
        assert!(decode_meta_record("graph", &unversioned).is_err());

        let missing_incarnation = rmp_serde::to_vec_named(&serde_json::json!({
            "schema_version": GRAPH_META_SCHEMA_VERSION,
            "name": "graph",
            "graph_type": GraphType::Global,
            "schema_sources": null
        }))
        .unwrap();
        assert!(decode_meta_record("graph", &missing_incarnation).is_err());

        let missing_sources = rmp_serde::to_vec_named(&serde_json::json!({
            "schema_version": GRAPH_META_SCHEMA_VERSION,
            "name": "graph",
            "graph_type": GraphType::Global,
            "incarnation_id": "incarnation:test:missing-policy"
        }))
        .unwrap();
        assert!(decode_meta_record("graph", &missing_sources).is_err());
    }

    fn temp_path() -> std::path::PathBuf {
        crate::redb_store::temp_path("eg-keyset-page", "")
    }

    #[test]
    fn keyset_pages_recover_every_node_and_parallel_edge_without_prefix_skips() {
        let path = temp_path();
        // `create_owner` materializes the whole declared census, so the
        // hand-rolled table bootstrap the raw path needed is gone.
        let shard = Shard::open(&path).unwrap();

        let nodes: Vec<_> = ["a", "b", "c", "n00", "n01", "n02", "n03"]
            .into_iter()
            .map(|id| (id.to_string(), id.as_bytes().to_vec()))
            .collect();
        let mut edges = Vec::new();
        for ordinal in 0..5u8 {
            edges.push(("a".to_string(), "b".to_string(), vec![ordinal]));
        }
        edges.push(("b".to_string(), "c".to_string(), vec![5]));
        edges.push(("b".to_string(), "c".to_string(), vec![6]));
        apply_checkpoint(
            &shard,
            &mut Vec::new(),
            vec![GraphDump::in_place_core_checkpoint(InPlaceCoreCheckpoint {
                graph: "graph".to_string(),
                name: "graph".to_string(),
                graph_type: GraphType::Global,
                incarnation_id: "incarnation:test:keyset".to_string(),
                source_snapshot_version: 7,
                schema_sources: std::sync::Arc::new(crate::graph::GraphSchemaSources::default()),
                nodes: nodes.clone(),
                edges: edges.clone(),
                ledger: Vec::new(),
                semantic: Vec::new(),
            })],
            DurableCrypto::none(),
        )
        .unwrap();

        let mut node_after: Option<String> = None;
        let mut edge_after: Option<(String, String, u32)> = None;
        let mut got_nodes = Vec::new();
        let mut got_edges = Vec::new();
        let mut first = true;
        loop {
            let edge_cursor = edge_after
                .as_ref()
                .map(|(source, target, ordinal)| (source.as_str(), target.as_str(), *ordinal));
            // Offsets are deliberately nonsense after page one. Correct recovery
            // must be driven solely by the durable keyset positions.
            let offset = if first { 0 } else { 1_000_000 };
            let page = read_graph_dump_page(
                &shard,
                "graph",
                DurableCrypto::none(),
                PageCursorRef {
                    node_offset: offset,
                    edge_offset: offset,
                    node_after: node_after.as_deref(),
                    edge_after: edge_cursor,
                    page_size: 3,
                },
            )
            .unwrap()
            .unwrap();
            assert!(page.nodes.len() + page.edges.len() <= 3);
            got_nodes.extend(page.nodes.iter().map(|(id, _)| id.clone()));
            got_edges.extend(page.edges.iter().cloned());
            node_after = page.node_after;
            edge_after = page.edge_after;
            first = false;
            if page.nodes_exhausted && page.edges_exhausted {
                break;
            }
        }

        assert_eq!(
            got_nodes,
            nodes.iter().map(|(id, _)| id.clone()).collect::<Vec<_>>()
        );
        assert_eq!(got_edges, edges);
        drop(shard);
        let _ = std::fs::remove_file(path);
    }
}

/// Read the entire store into read-only per-graph materialization views.
///
/// The catalog scan FIRST, on the control scope, then one bound scope per graph
/// it named: the file-wide `graph_meta` table is the only thing that knows which
/// graphs this file hosts, and a graph's rows are only reachable through its own
/// scope once it is bound. It is deliberately no longer one whole-table `iter()`
/// bucketed by key prefix — a scoped read cannot see another graph's rows, which
/// is the confinement this cut exists for.
///
/// These views are not cross-store transfer images and are rejected by
/// [`apply_checkpoint`].
pub(crate) fn read_all_dumps(
    shard: &Shard,
    crypto: DurableCrypto<'_>,
) -> Result<Vec<GraphDump>, String> {
    let catalog = read_graph_catalog(shard)?;
    let mut dumps = Vec::with_capacity(catalog.len());
    for (graph, record) in catalog {
        let handle = shard.graph(&graph)?;
        let read = shard.read(&handle)?;
        let source_snapshot_version = eg_transaction::version(&read)?;
        let core = read_graph_core_rows(&read, &graph, crypto)?;
        dumps.push(GraphDump {
            kind: GraphDumpKind::DurableReadOnlyMaterialization,
            graph,
            name: record.name,
            graph_type: record.graph_type,
            incarnation_id: record.incarnation_id,
            source_snapshot_version,
            schema_sources: record.schema_sources,
            nodes: core.nodes,
            edges: core.edges,
            ledger: core.ledger,
            semantic: core.semantic,
            native: Default::default(),
        });
    }
    Ok(dumps)
}

/// Cheap CATALOG-ONLY scan: every graph's identity row `(fname, name, graph_type)`
/// (CONCEPT:EG-KG.sharding.lazy-graph-catalog, DIST-P2-3) — NO node/edge/ledger/semantic table is
/// touched, and no graph scope is bound. Booting with millions of persisted graphs costs
/// one sequential scan of small `{name, graph_type}` rows on the control scope, not
/// `read_all_dumps`'s full per-graph rehydrate. Each returned graph materializes its
/// `GraphCore` lazily on first access via the registry's `GraphMaterializer` seam (which
/// reuses [`read_graph_dump`] to fetch the SAME durable rows this scan skipped).
pub(crate) fn read_all_graph_meta(
    shard: &Shard,
) -> Result<Vec<(String, String, GraphType, String)>, String> {
    Ok(read_graph_catalog(shard)?
        .into_iter()
        .map(|(fname, record)| (fname, record.name, record.graph_type, record.incarnation_id))
        .collect())
}

pub(crate) fn encode_meta_with_incarnation(
    name: &str,
    gtype: GraphType,
    incarnation_id: &str,
) -> Result<Vec<u8>, String> {
    encode_meta_record(
        name,
        gtype,
        incarnation_id,
        &crate::graph::GraphSchemaSources::default(),
    )
}

pub(crate) fn encode_meta_record(
    name: &str,
    graph_type: GraphType,
    incarnation_id: &str,
    schema_sources: &crate::graph::GraphSchemaSources,
) -> Result<Vec<u8>, String> {
    if name.trim().is_empty() || incarnation_id.trim().is_empty() {
        return Err("graph metadata identity fields must not be empty".to_string());
    }
    schema_sources.validate()?;
    rmp_serde::to_vec_named(&GraphMetaRecord {
        schema_version: GRAPH_META_SCHEMA_VERSION,
        name: name.to_string(),
        graph_type,
        incarnation_id: incarnation_id.to_string(),
        schema_sources: std::sync::Arc::new(schema_sources.clone()),
    })
    .map_err(|error| format!("encode graph metadata: {error}"))
}

/// The durable identity row of one graph, as the catalog stores it.
///
/// The fields are `pub(crate)` because the reader of a decoded record is not
/// only this module: the shard's own create/rename/rebind paths compare and
/// re-encode `name`/`graph_type`/`incarnation_id`, and they live in the PARENT
/// module, which a private field of a child would hide from.
#[derive(serde::Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GraphMetaRecord {
    pub(crate) schema_version: u16,
    pub(crate) name: String,
    pub(crate) graph_type: GraphType,
    pub(crate) incarnation_id: String,
    /// Required current graph schema authority.
    pub(crate) schema_sources: std::sync::Arc<crate::graph::GraphSchemaSources>,
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct GraphMetaRecordCurrent {
    schema_version: u16,
    name: String,
    graph_type: GraphType,
    incarnation_id: String,
    schema_sources: std::sync::Arc<crate::graph::GraphSchemaSources>,
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct GraphMetaRecordV2 {
    schema_version: u16,
    name: String,
    graph_type: GraphType,
    incarnation_id: String,
    #[serde(deserialize_with = "deserialize_required_option")]
    integrity_policy: Option<crate::graph::IntegrityPolicyV2>,
}

#[derive(serde::Deserialize)]
#[serde(untagged)]
enum GraphMetaRecordWire {
    Current(GraphMetaRecordCurrent),
    V2(GraphMetaRecordV2),
}

impl<'de> serde::Deserialize<'de> for GraphMetaRecord {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        use serde::de::Error as _;
        match GraphMetaRecordWire::deserialize(deserializer)? {
            GraphMetaRecordWire::Current(value)
                if value.schema_version == GRAPH_META_SCHEMA_VERSION =>
            {
                let schema_sources = std::sync::Arc::new(
                    std::sync::Arc::unwrap_or_clone(value.schema_sources)
                        .reconciled_current_core()
                        .map_err(D::Error::custom)?,
                );
                Ok(Self {
                    schema_version: value.schema_version,
                    name: value.name,
                    graph_type: value.graph_type,
                    incarnation_id: value.incarnation_id,
                    schema_sources,
                })
            }
            GraphMetaRecordWire::V2(value) if value.schema_version == 2 => Ok(Self {
                schema_version: GRAPH_META_SCHEMA_VERSION,
                name: value.name,
                graph_type: value.graph_type,
                incarnation_id: value.incarnation_id,
                schema_sources: crate::graph::lift_v2_integrity_policy(value.integrity_policy)
                    .map_err(D::Error::custom)?,
            }),
            GraphMetaRecordWire::Current(value) => Err(D::Error::custom(format!(
                "unsupported graph metadata schema version {}",
                value.schema_version
            ))),
            GraphMetaRecordWire::V2(value) => Err(D::Error::custom(format!(
                "unsupported legacy graph metadata schema version {}",
                value.schema_version
            ))),
        }
    }
}

pub(crate) fn deserialize_required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::Deserialize<'de>,
{
    <Option<T> as serde::Deserialize>::deserialize(deserializer)
}

pub(crate) const GRAPH_META_SCHEMA_VERSION: u16 = 3;

/// The durable `graph_meta` schema version this build writes.
pub(crate) fn graph_meta_schema_version() -> u16 {
    GRAPH_META_SCHEMA_VERSION
}

/// The pre-schema_version `graph_meta` value: a bare msgpack map of exactly
/// `{"name", "graph_type"}` written by every engine build before the versioned
/// record landed.
///
/// Retaining this reader is the ON-DISK MIGRATION exception to No-Legacy — the
/// one case the architecture doc carves out, because a durable store cannot be
/// updated by editing code. Without it a store written by any prior build is
/// permanently unopenable: `GraphMetaRecord` is `deny_unknown_fields` and its
/// authoritative schema-source field has no serde default, so a legacy row cannot decode, the
/// catalog load fails, and the engine refuses to start with
/// "durable recovery failed; refusing availability". That is exactly what a 9.9G
/// production store did.
///
/// This is read-old → write-new, not a permanent dual-format reader: every
/// decoded legacy row is rewritten in the current format by
/// [`upgrade_legacy_graph_meta`], so a converted store never takes this path
/// again and this shape can be deleted once no legacy store remains.
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct LegacyGraphMetaRecord {
    name: String,
    graph_type: GraphType,
}

/// Derive a STABLE incarnation id for a graph recovered from a legacy record.
///
/// Deliberately not [`new_incarnation_id`]: that mixes in the wall clock, so a
/// legacy store would mint a different incarnation on every open. Incarnation
/// identity is what fencing and raft replica agreement are keyed on, so a
/// per-restart value would make a migrated graph look like a new incarnation on
/// each boot and would disagree across replicas converting the same store.
/// Hashing only a fixed domain tag and the graph name makes the upgrade
/// deterministic, idempotent, and identical on every node.
pub(crate) fn legacy_incarnation_id(graph: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut digest = Sha256::new();
    digest.update(b"epistemic-graph-incarnation-legacy-v1\0");
    digest.update(graph.as_bytes());
    format!("{:x}", digest.finalize())[..32].to_string()
}

pub(crate) fn new_incarnation_id(graph: &str) -> String {
    static NEXT_INCARNATION: AtomicU64 = AtomicU64::new(1);
    use sha2::{Digest, Sha256};
    let mut digest = Sha256::new();
    digest.update(b"epistemic-graph-incarnation-v1\0");
    digest.update(graph.as_bytes());
    digest.update(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
            .to_le_bytes(),
    );
    digest.update(
        NEXT_INCARNATION
            .fetch_add(1, Ordering::Relaxed)
            .to_le_bytes(),
    );
    format!("incarnation:durable:{}", hex::encode(digest.finalize()))
}

pub(crate) fn decode_meta_record(graph: &str, blob: &[u8]) -> Result<GraphMetaRecord, String> {
    let record: GraphMetaRecord = match decode_durable::<GraphMetaRecord>(blob) {
        Ok(record) => record,
        // A legacy row cannot decode into the versioned record at all (it has no
        // schema_version/incarnation_id and the struct denies unknown fields), so
        // the fallback is keyed on the decode failing, not on a version compare.
        Err(current_error) => decode_legacy_meta_record(graph, blob)
            .ok_or_else(|| format!("decode graph metadata for {graph}: {current_error}"))?,
    };
    if record.schema_version != GRAPH_META_SCHEMA_VERSION {
        return Err(format!(
            "graph metadata for {graph} has unsupported schema version {}",
            record.schema_version
        ));
    }
    if record.name.trim().is_empty() || record.incarnation_id.trim().is_empty() {
        return Err(format!(
            "graph metadata for {graph} has incomplete identity"
        ));
    }
    Ok(record)
}

/// Lift a legacy `{"name", "graph_type"}` row into the current record shape.
///
/// Returns `None` when the blob is not a legacy record either, so the caller can
/// report the CURRENT format's decode error rather than masking genuine
/// corruption as "not legacy".
///
/// The pre-versioned row gains this binary's immutable core catalog and no
/// dynamic sources.
pub(crate) fn decode_legacy_meta_record(graph: &str, blob: &[u8]) -> Option<GraphMetaRecord> {
    let legacy: LegacyGraphMetaRecord = decode_durable(blob).ok()?;
    if legacy.name.trim().is_empty() {
        return None;
    }
    Some(GraphMetaRecord {
        schema_version: GRAPH_META_SCHEMA_VERSION,
        incarnation_id: legacy_incarnation_id(&legacy.name),
        name: legacy.name,
        graph_type: legacy.graph_type,
        schema_sources: std::sync::Arc::new(crate::graph::GraphSchemaSources::default()),
    })
    .inspect(|_| {
        tracing::info!(
            "graph metadata for {graph} upgraded from the pre-versioned format \
             (schema sources initialized); it is rewritten in the current format"
        )
    })
}

/// A per-ATTEMPT operation id for one control-only catalog write.
///
/// Unique per attempt, deliberately, exactly as `Shard`'s own `drain_batch`
/// requires: the kernel resolves a batch id that already carries a durable
/// receipt to `Begin::Replay` and SKIPS it, so a reused id would silently drop a
/// retried migration. The wall-clock component keeps two runs of the same
/// counter value apart across a restart.
fn catalog_write_attempt_id(label: &str) -> String {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|since| since.as_nanos())
        .unwrap_or_default();
    format!(
        "{label}/{}-{nanos}:{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}

/// Write `rows` into the file-wide catalog as ONE control-only maintenance
/// commit.
///
/// `graph_meta` belongs to the file, not to any graph it hosts, so the admitted
/// group carries the control member and NO graph members — the empty-member
/// write the seam documents. Every row lands in one physical transaction, so a
/// crash mid-write leaves the catalog wholly on its previous contents.
fn commit_catalog_rows(
    shard: &Shard,
    label: &str,
    rows: &[(String, Vec<u8>)],
) -> Result<(), String> {
    let op_id = catalog_write_attempt_id(label);
    let (group, batches) = shard.admit_maintenance(&[], &op_id)?;
    let write = ShardWrite::open(shard, &group, &[], &batches)?;
    let inserted = insert_catalog_rows(&write, rows);
    // The row gate closes whether or not the rows landed: dropping a member's
    // owner-row admission unfinished poisons the shared transaction.
    let finished = write.finish();
    match (inserted, finished) {
        // The shard's own bookkeeping carries no caller instant, so it commits
        // at 0 rather than reading a second, unsynchronized clock inside the
        // durable tier -- the same choice `remove_graph_catalog_row` makes.
        (Ok(()), Ok(())) => shard.commit_drain(group, &batches, 0),
        (Err(error), _) | (Ok(()), Err(error)) => {
            shard.mutations().abort_group(group)?;
            Err(error)
        }
    }
}

fn insert_catalog_rows(write: &ShardWrite<'_>, rows: &[(String, Vec<u8>)]) -> Result<(), String> {
    let mut catalog = write.control().open_table(GRAPH_META)?;
    for (graph, encoded) in rows {
        catalog
            .insert(graph.as_str(), encoded.as_slice())
            .map_err(|e| e.to_string())?;
    }
    Ok(())
}

/// Rewrite every legacy `graph_meta` row of `shard` in the current format.
///
/// The write-new half of the one-time migration. The scan is a control-scope
/// read of the file-wide catalog and the rewrite is one control-only admitted
/// commit, so a crash mid-upgrade leaves the store wholly on the old format
/// (still readable by the fallback above) rather than half-converted. Idempotent:
/// rows already in the current format decode on the first attempt and are left
/// untouched, so a second run is a no-op and rewrites nothing.
///
/// Returns the number of rows upgraded.
pub(crate) fn upgrade_legacy_graph_meta(shard: &Shard) -> Result<usize, String> {
    let stale: Vec<(String, Vec<u8>)> = {
        let control = shard.control_read()?;
        let catalog = control.open_owner_table(GRAPH_META)?;
        let mut stale = Vec::new();
        for row in catalog.iter().map_err(|e| e.to_string())? {
            let (k, v) = row.map_err(|e| e.to_string())?;
            let key = k.value().to_string();
            if graph_meta_record_is_current(v.value()) {
                continue; // already physically current, including the core catalog
            }
            let Ok(record) = decode_meta_record(&key, v.value()) else {
                // Neither supported old format: leave it for catalog load to report.
                continue;
            };
            let encoded = encode_meta_record(
                &record.name,
                record.graph_type,
                &record.incarnation_id,
                record.schema_sources.as_ref(),
            )?;
            stale.push((key, encoded));
        }
        stale
    };
    if stale.is_empty() {
        return Ok(0);
    }
    let count = stale.len();
    commit_catalog_rows(shard, "graph_meta_migration", &stale)?;
    Ok(count)
}

/// Decode the logical identity carried by a raw `graph_meta` row.
///
/// Raft snapshots and online resharding copy this row verbatim.  Consumers must
/// derive the logical name/type from that sole durable authority rather than
/// trusting a second, independently serialized copy that could disagree with it.
pub(crate) fn decode_graph_meta_identity(
    graph: &str,
    blob: &[u8],
) -> Result<(String, GraphType, String), String> {
    let record = decode_meta_record(graph, blob)?;
    Ok((record.name, record.graph_type, record.incarnation_id))
}

fn graph_meta_record_is_current(blob: &[u8]) -> bool {
    let Ok(record) = decode_durable::<GraphMetaRecordCurrent>(blob) else {
        return false;
    };
    record.schema_version == GRAPH_META_SCHEMA_VERSION && record.schema_sources.validate().is_ok()
}

#[cfg(test)]
mod graph_meta_migration_tests {
    //! The pre-versioned `graph_meta` format must stay openable, because a
    //! durable store cannot be migrated by shipping new code alone. A production
    //! store written by an earlier build was permanently unopenable without this
    //! path: the engine died with "durable recovery failed; refusing availability".
    use super::*;

    /// The exact bytes a pre-versioned build wrote: `{"name", "graph_type"}`.
    fn legacy_blob(name: &str, gtype: GraphType) -> Vec<u8> {
        rmp_serde::to_vec_named(&serde_json::json!({"name": name, "graph_type": gtype})).unwrap()
    }

    fn v2_blob(name: &str, gtype: GraphType, incarnation_id: &str) -> Vec<u8> {
        rmp_serde::to_vec_named(&GraphMetaRecordV2 {
            schema_version: 2,
            name: name.to_string(),
            graph_type: gtype,
            incarnation_id: incarnation_id.to_string(),
            integrity_policy: Some(crate::graph::IntegrityPolicyV2 {
                shapes_ttl: "@prefix sh: <http://www.w3.org/ns/shacl#> .".to_string(),
            }),
        })
        .unwrap()
    }

    fn prior_core_blob(name: &str, gtype: GraphType, incarnation_id: &str) -> Vec<u8> {
        let mut sources = crate::graph::GraphSchemaSources::default();
        sources.core.pop_first();
        rmp_serde::to_vec_named(&GraphMetaRecordCurrent {
            schema_version: GRAPH_META_SCHEMA_VERSION,
            name: name.to_string(),
            graph_type: gtype,
            incarnation_id: incarnation_id.to_string(),
            schema_sources: std::sync::Arc::new(sources),
        })
        .unwrap()
    }

    fn open(dir: &std::path::Path) -> Shard {
        Shard::open(&dir.join("graph-0.redb")).unwrap()
    }

    /// Plant one catalog row through the control member.
    ///
    /// The known-bad VALUE is the point of the fixture, so it goes in through
    /// the same admitted control-only write the migration itself uses: a raw
    /// `redb::Database` could not produce a file the kernel will open at all.
    fn put(shard: &Shard, key: &str, blob: &[u8]) {
        commit_catalog_rows(
            shard,
            "graph_meta_fixture",
            &[(key.to_string(), blob.to_vec())],
        )
        .unwrap();
    }

    #[test]
    fn a_legacy_row_decodes_instead_of_failing_recovery() {
        let record = decode_meta_record("g", &legacy_blob("mygraph", GraphType::Global)).unwrap();
        assert_eq!(record.name, "mygraph");
        assert_eq!(record.schema_version, GRAPH_META_SCHEMA_VERSION);
        assert!(record.schema_sources.dynamic.is_empty());
        assert!(!record.schema_sources.core.is_empty());
        assert!(!record.incarnation_id.trim().is_empty());
    }

    #[test]
    fn a_legacy_incarnation_is_stable_across_calls() {
        // Fencing and raft replica agreement key on incarnation identity, so a
        // per-open value would make a migrated graph look new on every boot and
        // would disagree between replicas converting the same store.
        assert_eq!(legacy_incarnation_id("g"), legacy_incarnation_id("g"));
        assert_ne!(legacy_incarnation_id("g"), legacy_incarnation_id("h"));
        assert_eq!(
            decode_meta_record("g", &legacy_blob("g", GraphType::Global))
                .unwrap()
                .incarnation_id,
            decode_meta_record("g", &legacy_blob("g", GraphType::Global))
                .unwrap()
                .incarnation_id
        );
    }

    #[test]
    fn a_current_row_still_round_trips_unchanged() {
        let encoded = encode_meta_with_incarnation("g", GraphType::Global, "inc-1").unwrap();
        let record = decode_meta_record("g", &encoded).unwrap();
        assert_eq!(record.incarnation_id, "inc-1");
    }

    #[test]
    fn a_v2_row_lifts_its_integrity_policy_into_the_operator_source() {
        const V2_META_GOLDEN: &str = "85ae736368656d615f76657273696f6e02a46e616d65a167aa67726170685f74797065a6476c6f62616cae696e6361726e6174696f6e5f6964a6696e632d7632b0696e746567726974795f706f6c69637981aa7368617065735f74746cd92b407072656669782073683a203c687474703a2f2f7777772e77332e6f72672f6e732f736861636c233e202e";
        let legacy = GraphMetaRecordV2 {
            schema_version: 2,
            name: "g".to_string(),
            graph_type: GraphType::Global,
            incarnation_id: "inc-v2".to_string(),
            integrity_policy: Some(crate::graph::IntegrityPolicyV2 {
                shapes_ttl: "@prefix sh: <http://www.w3.org/ns/shacl#> .".to_string(),
            }),
        };
        let bytes = hex::decode(V2_META_GOLDEN).unwrap();
        assert_eq!(rmp_serde::to_vec_named(&legacy).unwrap(), bytes);
        let record = decode_meta_record("g", &bytes).unwrap();
        assert_eq!(record.schema_version, GRAPH_META_SCHEMA_VERSION);
        assert!(record
            .schema_sources
            .dynamic
            .contains_key(crate::graph::OPERATOR_SOURCE_ID));
        assert_eq!(record.incarnation_id, "inc-v2");
    }

    #[test]
    fn genuine_corruption_still_reports_the_current_format_error() {
        // The fallback must not mask real corruption as "not legacy".
        let error = match decode_meta_record("g", b"\xc1\xc1not-msgpack") {
            Ok(_) => panic!("corrupt graph metadata must not decode"),
            Err(error) => error,
        };
        assert!(error.contains("decode graph metadata for g"), "{error}");
    }

    #[test]
    fn upgrade_rewrites_legacy_rows_and_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let shard = open(dir.path());
        put(
            &shard,
            "graph-a",
            &legacy_blob("graph-a", GraphType::Global),
        );
        put(
            &shard,
            "graph-b",
            &encode_meta_with_incarnation("graph-b", GraphType::Global, "inc-b").unwrap(),
        );
        put(
            &shard,
            "graph-v2",
            &v2_blob("graph-v2", GraphType::Global, "inc-v2"),
        );
        put(
            &shard,
            "graph-prior-core",
            &prior_core_blob("graph-prior-core", GraphType::Global, "inc-prior-core"),
        );

        assert_eq!(
            upgrade_legacy_graph_meta(&shard).unwrap(),
            3,
            "pre-versioned, v2, and prior-core rows need one atomic physical rewrite"
        );
        // Second run rewrites nothing — the migration is genuinely one-time.
        assert_eq!(upgrade_legacy_graph_meta(&shard).unwrap(), 0);

        let rows = read_all_graph_meta(&shard).unwrap();
        assert_eq!(rows.len(), 4);
        // The converted row now decodes as current WITHOUT the legacy fallback.
        let control = shard.control_read().unwrap();
        let catalog = control.open_owner_table(GRAPH_META).unwrap();
        let raw = catalog.get("graph-a").unwrap().unwrap();
        assert!(decode_durable::<GraphMetaRecord>(raw.value()).is_ok());
        let raw = catalog.get("graph-v2").unwrap().unwrap();
        let migrated: GraphMetaRecord = decode_durable(raw.value()).unwrap();
        assert!(migrated
            .schema_sources
            .dynamic
            .contains_key(crate::graph::OPERATOR_SOURCE_ID));
        let raw = catalog.get("graph-prior-core").unwrap().unwrap();
        let migrated: GraphMetaRecord = decode_durable(raw.value()).unwrap();
        migrated.schema_sources.validate().unwrap();
    }

    #[test]
    fn upgrade_is_a_no_op_on_a_store_that_has_never_held_a_graph() {
        // A fresh install has never written a graph. `create_owner` materializes
        // the whole census, so the catalog table now always exists and is merely
        // EMPTY; the migration must still be a no-op rather than a startup
        // failure for a brand-new deployment.
        let dir = tempfile::tempdir().unwrap();
        let shard = open(dir.path());
        assert_eq!(upgrade_legacy_graph_meta(&shard).unwrap(), 0);
    }

    #[test]
    fn a_whole_legacy_store_recovers_its_catalog() {
        // The end-to-end shape of the production failure: several legacy graphs,
        // none of which the current record can decode.
        let dir = tempfile::tempdir().unwrap();
        let shard = open(dir.path());
        for name in ["alpha", "beta", "gamma"] {
            put(&shard, name, &legacy_blob(name, GraphType::Global));
        }
        let rows = read_all_graph_meta(&shard).unwrap();
        assert_eq!(rows.len(), 3);
        let mut names: Vec<_> = rows.into_iter().map(|(_, n, _, _)| n).collect();
        names.sort();
        assert_eq!(names, vec!["alpha", "beta", "gamma"]);
    }
}
