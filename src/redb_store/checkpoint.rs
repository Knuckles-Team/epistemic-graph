//! Applying a checkpoint: validating an incoming dump against the live resource
//! domain, then replacing a graph's rows wholesale.
//!
//! A checkpoint spans many graphs, so it is one admitted scope group: each
//! graph is its own member confined to its own rows, and catalog entries it
//! rewrites ride the control member. The mutation kernel owns the graph version
//! and ledger; this module only replaces the graph payload and catalog image.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};

use eg_storage::ScopedOwnerTableMut;

use super::shard::{Shard, ShardWrite};
use super::*;

pub(crate) const CHECKPOINT_RESOURCE_REFUSAL: &str = "checkpoint resource domain validation failed";

/// Every checkpoint resource-link failure reports the same opaque refusal, so
/// that a dump cannot probe the durable resource domain through error text.
pub(crate) fn checkpoint_resource_refusal() -> String {
    CHECKPOINT_RESOURCE_REFUSAL.to_string()
}

/// Scan this graph's reservation rows, bounded by `MAX_RESOURCE_CLEAR_SCAN`, and
/// return the ones still holding capacity. The scope capability bounds the scan
/// to this graph; no caller-provided graph prefix is trusted for row ownership.
fn collect_checkpoint_active_reservations(
    graph: &str,
    reservations: &ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<Vec<DurableResourceReservation>, String> {
    let mut scanned = 0usize;
    let mut active_rows = Vec::new();
    for row in reservations
        .scope_rows()
        .map_err(|_| checkpoint_resource_refusal())?
    {
        let (key, value) = row.map_err(|_| checkpoint_resource_refusal())?;
        let (row_graph, reservation_id) = key.value();
        if row_graph != graph {
            return Err(checkpoint_resource_refusal());
        }
        scanned = scanned.saturating_add(1);
        if scanned > MAX_RESOURCE_CLEAR_SCAN {
            return Err(checkpoint_resource_refusal());
        }
        let stored: DurableResourceReservation =
            resource_decode(value.value(), crypto).map_err(|_| checkpoint_resource_refusal())?;
        if stored.record.reservation_id != reservation_id {
            return Err(checkpoint_resource_refusal());
        }
        if resource_reservation_row_is_active(&stored) {
            active_rows.push(stored);
        }
    }
    Ok(active_rows)
}

/// Index only the bounded set of WorkItems linked by active holds. This is one
/// O(nodes + active-holds) pass over the incoming image instead of an
/// O(nodes * reservations) search. A duplicate id in the incoming image is a
/// refusal.
fn index_checkpoint_incoming_active<'n>(
    incoming_nodes: &'n [(String, Vec<u8>)],
    active_rows: &[DurableResourceReservation],
) -> Result<HashMap<&'n str, &'n [u8]>, String> {
    let active_ids: HashSet<String> = active_rows
        .iter()
        .map(|stored| stored.record.work_item_id.clone())
        .collect();
    let mut incoming_active: HashMap<&str, &[u8]> = HashMap::with_capacity(active_ids.len());
    for (id, bytes) in incoming_nodes {
        if active_ids.contains(id)
            && incoming_active
                .insert(id.as_str(), bytes.as_slice())
                .is_some()
        {
            return Err(checkpoint_resource_refusal());
        }
    }
    Ok(incoming_active)
}

/// Validate one active hold against the incoming replacement image, not the
/// rows currently in redb. The graph and host tables are already capability
/// bounded to the graph being restored.
fn validate_checkpoint_active_reservation(
    graph: &str,
    stored: &DurableResourceReservation,
    incoming_active: &HashMap<&str, &[u8]>,
    hosts: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let Some(item_bytes) = incoming_active
        .get(stored.record.work_item_id.as_str())
        .copied()
    else {
        return Err(checkpoint_resource_refusal());
    };
    let props: serde_json::Map<String, serde_json::Value> =
        decode_durable(item_bytes).map_err(|_| checkpoint_resource_refusal())?;
    let request = resource_request_from_record(&stored.record, stored.record.reserved_at_ms);
    resource_validate_work_item(&props, &request, false)
        .map_err(|_| checkpoint_resource_refusal())?;
    if !resource_record_work_item_live(&props, &stored.record, stored.record.reserved_at_ms) {
        return Err(checkpoint_resource_refusal());
    }

    let (_, extension) =
        resource_metadata_maps(&props).map_err(|_| checkpoint_resource_refusal())?;
    let host = resource_load_host(hosts, graph, &stored.record.host_ref, crypto)
        .map_err(|_| checkpoint_resource_refusal())?
        .ok_or_else(checkpoint_resource_refusal)?;
    if host.host_ref != stored.record.host_ref
        || host.target_kind != resource_record_target_kind(stored.record.target_kind)
        || host.target_alias != stored.record.target_alias
    {
        return Err(checkpoint_resource_refusal());
    }
    if !resource_target_selection_matches(extension, &host)
        .map_err(|_| checkpoint_resource_refusal())?
    {
        return Err(checkpoint_resource_refusal());
    }
    Ok(())
}

/// Validate the WorkItem side of every active native reservation before replacing
/// a graph image from a checkpoint. Resource rows are deliberately preserved by
/// ordinary GraphDump restore, so a dump omitting a linked WorkItem is refused
/// before any graph row is cleared.
pub(crate) fn validate_checkpoint_resource_links(
    graph: &str,
    incoming_nodes: &[(String, Vec<u8>)],
    reservations: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    hosts: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let active_rows = collect_checkpoint_active_reservations(graph, reservations, crypto)?;
    let incoming_active = index_checkpoint_incoming_active(incoming_nodes, &active_rows)?;
    for stored in &active_rows {
        validate_checkpoint_active_reservation(graph, stored, &incoming_active, hosts, crypto)?;
    }
    Ok(())
}

/// Validate and replace one graph's payload inside the already admitted group.
fn apply_checkpoint_dump(
    write: &ShardWrite<'_>,
    dump: &GraphDump,
    current_version: u64,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    if dump.source_snapshot_version < current_version {
        return Err("checkpoint graph image is stale".to_string());
    }

    let graph = dump.graph.as_str();
    let member = write.graph(graph)?;
    let incoming_nodes = dump
        .nodes
        .iter()
        .map(|(node_id, properties)| (node_id.clone(), properties.clone()))
        .collect::<Vec<_>>();
    work_item_capability::validate_snapshot_nodes(&incoming_nodes)?;

    // Validate every retained native authority before touching graph rows. The
    // handles are dropped before private capability cleanup because redb rejects
    // opening one table twice in a write transaction.
    {
        let mut reservations = member.open_scoped_table(RESOURCE_RESERVATIONS)?;
        let mut hosts = member.open_scoped_table(RESOURCE_HOSTS)?;
        validate_checkpoint_resource_links(
            graph,
            &incoming_nodes,
            &mut reservations,
            &mut hosts,
            crypto,
        )?;
        let holds = member.open_scoped_table(development_lane::HOLDS)?;
        development_lane::validate_checkpoint_lane_links(graph, &dump.nodes, &holds, crypto)
            .map_err(|_| checkpoint_resource_refusal())?;
    }
    work_item_capability::clear_graph_rows(write, graph)?;

    let mut tables = GraphRowTables::open(member)?;
    clear_graph_rows(
        graph,
        &mut tables.nodes,
        &mut tables.edges,
        &mut tables.ledger,
    )?;
    for (id, props) in &dump.nodes {
        let blob = crypto.seal(props);
        tables
            .nodes
            .insert((graph, id.as_str()), blob.as_ref())
            .map_err(|e| e.to_string())?;
    }
    for (src, tgt, props) in &dump.edges {
        let ordinal = next_edge_ordinal(&tables.edges, graph, src, tgt)?;
        let blob = crypto.seal(props);
        tables
            .edges
            .insert((graph, src.as_str(), tgt.as_str(), ordinal), blob.as_ref())
            .map_err(|e| e.to_string())?;
    }
    for (seq, line) in dump.ledger.iter().enumerate() {
        tables
            .ledger
            .insert((graph, seq as u64), line.as_str())
            .map_err(|e| e.to_string())?;
    }
    let semantic = crypto.seal(&dump.semantic);
    tables
        .semantic
        .insert(graph, semantic.as_ref())
        .map_err(|e| e.to_string())?;
    drop(tables);

    let encoded = encode_meta_record(
        &dump.name,
        dump.graph_type,
        &dump.incarnation_id,
        dump.integrity_policy.as_ref(),
    )?;
    write
        .control()
        .open_table(GRAPH_META)?
        .insert(graph, encoded.as_slice())
        .map_err(|e| e.to_string())?;
    Ok(())
}

fn checkpoint_attempt_id() -> String {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    format!(
        "checkpoint/{}/{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}

/// Apply pending graph methods and replacement dumps in one admitted group.
#[allow(clippy::too_many_arguments)]
fn apply_checkpoint_group(
    write: &ShardWrite<'_>,
    graph_names: &[String],
    members: &[(
        String,
        std::sync::Arc<eg_storage::OwnedStoreHandle<eg_storage::GraphShardOwner>>,
    )],
    batches: &[MutationBatch],
    pending: &[(String, Method)],
    dumps: &BTreeMap<String, GraphDump>,
    crypto: DurableCrypto<'_>,
    #[cfg(feature = "security")] audit_tail: &mut AuditTailCache,
) -> Result<usize, String> {
    let mut pending_by_graph: BTreeMap<String, Vec<Method>> = BTreeMap::new();
    for (graph, method) in pending {
        reject_reserved_graph(graph)?;
        pending_by_graph
            .entry(graph.clone())
            .or_default()
            .push(method.clone());
    }
    for (index, graph) in graph_names.iter().enumerate() {
        let _ = members
            .get(index)
            .ok_or_else(|| "checkpoint group member list is incomplete".to_string())?;
        if let Some(methods) = pending_by_graph.get(graph) {
            super::apply_graph_methods(
                write,
                graph,
                methods,
                crypto,
                #[cfg(feature = "security")]
                audit_tail,
            )?;
            super::backfill_graph_meta_row(write, graph)?;
        }
        if let Some(dump) = dumps.get(graph) {
            let batch = batches
                .get(index + 1)
                .ok_or_else(|| "checkpoint graph batch list is incomplete".to_string())?;
            let current_version = match batch.version_expectation {
                VersionExpectation::Graph(version) => version,
                other => return Err(format!("checkpoint admitted non-graph version {other:?}")),
            };
            apply_checkpoint_dump(write, dump, current_version, crypto)?;
        }
    }
    Ok(dumps.len())
}

pub(crate) fn validate_checkpoint_dumps(graphs: &[GraphDump]) -> Result<(), String> {
    let mut graph_ids = HashSet::with_capacity(graphs.len());
    for dump in graphs {
        reject_reserved_graph(&dump.graph)?;
        if !graph_ids.insert(dump.graph.as_str()) {
            return Err("checkpoint contains duplicate graph id".to_string());
        }
        dump.validate_in_place_checkpoint()?;
    }
    Ok(())
}

/// Snapshot the full registry dump into the kernel-owned shard, overwriting each
/// graph's rows and committing the pending mutations and image as one group.
pub(crate) fn apply_checkpoint(
    shard: &Shard,
    pending: &mut Vec<(String, Method)>,
    graphs: Vec<GraphDump>,
    crypto: DurableCrypto<'_>,
) -> Result<usize, String> {
    // Validate origin/completeness before opening a write group or replaying any
    // pending mutation. A durable read is intentionally not a transferable
    // checkpoint, even when its diagnostic native row vectors happen to be empty.
    validate_checkpoint_dumps(&graphs)?;
    let mut dumps = BTreeMap::new();
    for dump in graphs {
        dumps.insert(dump.graph.clone(), dump);
    }

    let mut names = BTreeSet::new();
    for graph in pending.iter().map(|(graph, _)| graph) {
        reject_reserved_graph(graph)?;
        names.insert(graph.clone());
    }
    names.extend(dumps.keys().cloned());
    let graph_names: Vec<String> = names.into_iter().collect();
    let members = shard.graph_members(&graph_names)?;
    let op_id = checkpoint_attempt_id();
    let (group, batches) = shard.admit_maintenance(&members, &op_id)?;
    let write = ShardWrite::open(shard, &group, &members, &batches)?;
    #[cfg(feature = "security")]
    let mut audit_tail = AuditTailCache::new();
    let applied = apply_checkpoint_group(
        &write,
        &graph_names,
        &members,
        &batches,
        pending,
        &dumps,
        crypto,
        #[cfg(feature = "security")]
        &mut audit_tail,
    );
    let finished = write.finish();
    match (applied, finished) {
        (Ok(count), Ok(())) => {
            shard.commit_drain(group, &batches, 0)?;
            pending.clear();
            Ok(count)
        }
        (Err(error), _) | (Ok(_), Err(error)) => {
            shard.mutations().abort_group(group)?;
            Err(error)
        }
    }
}
