use super::store_prelude::*;
use super::*;

pub fn reject_reserved_graph(graph: &str) -> Result<(), String> {
    if graph == SHARD_CONTROL_GRAPH || sanitize(graph) == SHARD_CONTROL_GRAPH {
        return Err(format!(
            "'{SHARD_CONTROL_GRAPH}' is the shard's reserved control scope and cannot be used as a graph"
        ));
    }
    Ok(())
}

/// Map a logical graph name to the bounded durable key used by the served and
/// embedded paths. Escaping avoids collisions caused by lossy path replacement.
pub fn sanitize(name: &str) -> String {
    use sha2::{Digest, Sha256};

    let mut key = String::with_capacity(name.len());
    for &byte in name.as_bytes() {
        use std::fmt::Write as _;
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.') {
            key.push(char::from(byte));
        } else {
            write!(&mut key, "~{byte:02x}").expect("writing to String cannot fail");
        }
    }
    if key.len() <= 200 {
        return key;
    }
    let digest = Sha256::digest(name.as_bytes());
    let mut bounded = String::with_capacity(66);
    bounded.push_str("~h");
    for byte in digest {
        use std::fmt::Write as _;
        write!(&mut bounded, "{byte:02x}").expect("writing to String cannot fail");
    }
    bounded
}

/// Recover the logical graph name from a durable key produced by [`sanitize`].
///
/// `sanitize` is a byte escaping, so it is EXACTLY invertible for every ordinary
/// name -- that reversibility is the property its own doc claims and the reason
/// it replaced the older lossy character-replacement scheme. The one form that
/// cannot be inverted is the bounded `~h<sha256>` key a name longer than 200
/// escaped bytes falls back to; a digest has no preimage, so this returns `None`
/// for it rather than inventing one.
pub(crate) fn unsanitize(key: &str) -> Option<String> {
    if key.starts_with("~h") {
        return None;
    }
    let bytes = key.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'~' => {
                let hex = key.get(index + 1..index + 3)?;
                out.push(u8::from_str_radix(hex, 16).ok()?);
                index += 3;
            }
            byte => {
                out.push(byte);
                index += 1;
            }
        }
    }
    let name = String::from_utf8(out).ok()?;
    // Round-trip or refuse: a key this does not re-encode to is not a key this
    // function produced, and guessing at it would put a wrong name in the
    // durable catalog.
    (sanitize(&name) == key).then_some(name)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GraphDumpKind {
    InPlaceCoreCheckpoint,
    DurableReadOnlyMaterialization,
}

/// The complete in-memory `GraphCore` image accepted by an in-place checkpoint.
///
/// Keeping this input distinct from [`GraphDump`] makes the authority boundary
/// structural: callers cannot accidentally attach durable native rows or reuse a
/// read-only materialization while constructing an ordinary checkpoint.
pub(crate) struct InPlaceCoreCheckpoint {
    pub graph: String,
    pub name: String,
    pub graph_type: GraphType,
    pub incarnation_id: String,
    pub source_snapshot_version: u64,
    pub integrity_policy: Option<crate::graph::IntegrityPolicy>,
    pub nodes: Vec<(String, Vec<u8>)>,
    pub edges: Vec<(String, String, Vec<u8>)>,
    pub ledger: Vec<String>,
    pub semantic: Vec<u8>,
}

/// An owned, off-lock view of one graph used by checkpoint and materialization paths.
///
/// This is deliberately not a cross-store transfer image. Durable reads include
/// diagnostic native rows but omit other graph-scoped authorities. Complete moves
/// must use the fenced `RedbBackend::reshard_graph` / `RawGraphRows` protocol.
pub struct GraphDump {
    pub(crate) kind: GraphDumpKind,
    pub graph: String,
    pub name: String,
    pub graph_type: GraphType,
    pub incarnation_id: String,
    pub source_snapshot_version: u64,
    pub integrity_policy: Option<crate::graph::IntegrityPolicy>,
    pub nodes: Vec<(String, Vec<u8>)>,
    pub edges: Vec<(String, String, Vec<u8>)>,
    pub ledger: Vec<String>,
    pub semantic: Vec<u8>,
    /// Diagnostic/read-only native `development_lane_*`/`resource_*` rows for
    /// this graph (BUG-CX-096). They make omissions observable to readers; they
    /// are not a complete authority-transfer contract and are never applied by
    /// an ordinary in-place checkpoint.
    pub native: NativeOperationDumpRows,
}

impl GraphDump {
    pub(crate) fn in_place_core_checkpoint(checkpoint: InPlaceCoreCheckpoint) -> Self {
        Self {
            kind: GraphDumpKind::InPlaceCoreCheckpoint,
            graph: checkpoint.graph,
            name: checkpoint.name,
            graph_type: checkpoint.graph_type,
            incarnation_id: checkpoint.incarnation_id,
            source_snapshot_version: checkpoint.source_snapshot_version,
            integrity_policy: checkpoint.integrity_policy,
            nodes: checkpoint.nodes,
            edges: checkpoint.edges,
            ledger: checkpoint.ledger,
            semantic: checkpoint.semantic,
            native: NativeOperationDumpRows::default(),
        }
    }

    pub(crate) fn validate_in_place_checkpoint(&self) -> Result<(), String> {
        if self.kind != GraphDumpKind::InPlaceCoreCheckpoint {
            return Err(
                "checkpoint refused a durable read-only dump; use RedbBackend::reshard_graph for cross-store transfer"
                    .to_string(),
            );
        }
        if !self.native.is_empty() {
            return Err(
                "checkpoint refused native authority rows; use RedbBackend::reshard_graph for cross-store transfer"
                    .to_string(),
            );
        }
        Ok(())
    }
}

/// The graph-scoped row set of every `development_lane_*` (10 tables) and
/// `resource_*` (9 tables) redb table, keyed by the NON-graph part of each
/// table's real key (the dump's own `graph` field already carries the graph
/// component). Every value blob that is sealed at rest in redb (an `&[u8]`
/// column, decoded via `resource_decode`/`DurableLaneHold`/
/// `DurableResourceReservation`-style wrappers elsewhere in this module) is
/// stored here UNSEALED — the plaintext form, the same convention
/// [`read_graph_dump`] already uses for `nodes`/`edges`/`semantic`. A plain
/// text or numeric index column is stored as-is (never sealed on disk).
#[derive(Default)]
pub struct NativeOperationDumpRows {
    /// `development_lane_holds`: `(graph, hold_id) -> sealed DurableLaneHold`.
    pub development_lane_holds: Vec<(String, Vec<u8>)>,
    /// `development_lane_tenant_index`: `(graph, tenant, hold_id) -> hold_id`.
    pub development_lane_tenant_index: Vec<((String, String), String)>,
    /// `development_lane_lane_index`: `(graph, tenant, lane_id) -> hold_id`.
    pub development_lane_lane_index: Vec<((String, String), String)>,
    /// `development_lane_repository_branch_index`: `(graph, tenant, branch) -> hold_id`.
    pub development_lane_repository_branch_index: Vec<((String, String), String)>,
    /// `development_lane_worktree_index`: `(graph, worktree) -> hold_id`.
    pub development_lane_worktree_index: Vec<(String, String)>,
    /// `development_lane_work_item_index`: `(graph, tenant, attempt) -> hold_id`.
    pub development_lane_work_item_index: Vec<((String, u64), String)>,
    /// `development_lane_counters`: `(graph, scope) -> sealed counters blob`.
    pub development_lane_counters: Vec<(String, Vec<u8>)>,
    /// `development_lane_pressure_index`: `(graph, tenant, scope, metric, value, counter_key) -> marker`.
    #[allow(clippy::type_complexity)]
    pub development_lane_pressure_index: Vec<((String, String, String, u64, String), u8)>,
    /// `development_lane_policies`: `(graph, tenant) -> sealed policy blob`.
    pub development_lane_policies: Vec<(String, Vec<u8>)>,
    /// `development_lane_invocations`: `(graph, tenant, key) -> sealed replay blob`.
    pub development_lane_invocations: Vec<((String, String), Vec<u8>)>,
    /// `resource_reservations`: `(graph, reservation_id) -> sealed DurableResourceReservation`.
    pub resource_reservations: Vec<(String, Vec<u8>)>,
    /// `resource_reservation_tenant_index`: `(graph, tenant, reservation_id) -> reservation_id`.
    pub resource_reservation_tenant_index: Vec<((String, String), String)>,
    /// `resource_reservation_attempts`: `(graph, work_item, attempt) -> reservation_id`.
    pub resource_reservation_attempts: Vec<((String, u64), String)>,
    /// `resource_hosts`: `(graph, host_ref) -> sealed DurableResourceHost`.
    pub resource_hosts: Vec<(String, Vec<u8>)>,
    /// `resource_exclusivity`: `(graph, key) -> reservation_id`.
    pub resource_exclusivity: Vec<(String, String)>,
    /// `resource_fairness`: `(graph, group) -> sealed fairness blob`.
    pub resource_fairness: Vec<(String, Vec<u8>)>,
    /// `resource_concurrency`: `(graph, key) -> active count`.
    pub resource_concurrency: Vec<(String, u64)>,
    /// `resource_anti_affinity`: `(graph, host, tag) -> count`.
    pub resource_anti_affinity: Vec<((String, String), u64)>,
    /// `resource_disk_policies`: `(graph, key) -> sealed disk-policy blob`.
    pub resource_disk_policies: Vec<(String, Vec<u8>)>,
}

impl NativeOperationDumpRows {
    pub(super) fn is_empty(&self) -> bool {
        [
            self.development_lane_holds.len(),
            self.development_lane_tenant_index.len(),
            self.development_lane_lane_index.len(),
            self.development_lane_repository_branch_index.len(),
            self.development_lane_worktree_index.len(),
            self.development_lane_work_item_index.len(),
            self.development_lane_counters.len(),
            self.development_lane_pressure_index.len(),
            self.development_lane_policies.len(),
            self.development_lane_invocations.len(),
            self.resource_reservations.len(),
            self.resource_reservation_tenant_index.len(),
            self.resource_reservation_attempts.len(),
            self.resource_hosts.len(),
            self.resource_exclusivity.len(),
            self.resource_fairness.len(),
            self.resource_concurrency.len(),
            self.resource_anti_affinity.len(),
            self.resource_disk_policies.len(),
        ]
        .into_iter()
        .sum::<usize>()
            == 0
    }
}

/// Commit one drained burst -- every buffered mutation plus any Raft log
/// appends -- as ONE admitted scope group (CONCEPT:EG-KG.storage.one-fsync-covers-raft,
/// RF-RULING-008).
///
/// One group is one physical write transaction and one fsync, so a graph
/// mutation and the Raft log entry that replicated it are durable together and
/// N buffered writers are notified off one flush. Each touched graph is its own
/// member, confined to its own rows by the capability rather than by an
/// argument; the Raft log, and the catalog row a first-touch graph needs, ride
/// the control member. The embedded path passes an empty `raft_log_ops`.
///
/// A burst touching more than `MAX_SHARD_GROUP_GRAPHS` distinct graphs flushes
/// in chunks, deterministically and in order, so replicas draining the same
/// burst apply the same chunks in the same sequence.
///
/// `drain_id` must be unique per ATTEMPT. This path has no replay requirement --
/// before the cutover it carried no batch identity, no idempotency row and no
/// receipt -- and exactly-once for a replicated entry is already carried by the
/// Raft applied index, which is persisted after the effect lands and never
/// regresses. Deriving the id from `(raft_group, index)` instead would
/// manufacture a conflicting replay out of a path that never needed one.
pub(crate) fn commit_ops(
    shard: &Shard,
    ops: &mut Vec<(String, Method)>,
    raft_log_ops: &mut Vec<(u64, u64, Vec<u8>)>,
    drain_id: &str,
    committed_at_ms: u64,
    crypto: DurableCrypto<'_>,
    // O(1) audit-chain tail cache (CONCEPT:EG-KG.storage.embedded-store), owned by the caller across batches.
    #[cfg(feature = "security")] audit_tail: &mut AuditTailCache,
) -> Result<(), String> {
    if ops.is_empty() && raft_log_ops.is_empty() {
        return Ok(());
    }
    // Group by graph BEFORE anything else: a `BTreeMap` keeps the member order
    // deterministic across replicas, which is what makes the chunking below
    // reproducible, and it is also the order `ShardWrite` hands the members
    // back in.
    let mut by_graph: BTreeMap<String, Vec<Method>> = BTreeMap::new();
    for (graph, method) in ops.drain(..) {
        reject_reserved_graph(&graph)?;
        by_graph.entry(graph).or_default().push(method);
    }
    let graphs: Vec<String> = by_graph.keys().cloned().collect();
    let chunks = shard::chunk_graphs(&graphs);
    // Raft entries ride the FIRST chunk's control member: they are one member's
    // rows, not per-graph rows, and splitting them across chunks would break the
    // "one fsync covers Raft" property for every chunk but one.
    let mut raft_pending = std::mem::take(raft_log_ops);

    if chunks.is_empty() {
        return commit_drained_chunk(
            shard,
            &[],
            &mut by_graph,
            &mut raft_pending,
            drain_id,
            committed_at_ms,
            crypto,
            #[cfg(feature = "security")]
            audit_tail,
        );
    }
    for (index, chunk) in chunks.iter().enumerate() {
        let chunk_id = format!("{drain_id}/{index}");
        commit_drained_chunk(
            shard,
            chunk,
            &mut by_graph,
            &mut raft_pending,
            &chunk_id,
            committed_at_ms,
            crypto,
            #[cfg(feature = "security")]
            audit_tail,
        )?;
    }
    Ok(())
}

/// One chunk of a drained burst: one admitted group, one commit, one fsync.
#[allow(clippy::too_many_arguments)]
pub(crate) fn commit_drained_chunk(
    shard: &Shard,
    graphs: &[String],
    by_graph: &mut BTreeMap<String, Vec<Method>>,
    raft_log_ops: &mut Vec<(u64, u64, Vec<u8>)>,
    drain_id: &str,
    committed_at_ms: u64,
    crypto: DurableCrypto<'_>,
    #[cfg(feature = "security")] audit_tail: &mut AuditTailCache,
) -> Result<(), String> {
    // Cold graphs bind FIRST, in their own transactions: binding opens and
    // commits its own write and redb admits one writer, so it cannot happen
    // inside the group.
    let members = shard.graph_members(graphs)?;
    let (group, batches) = shard.admit_drain(&members, drain_id)?;
    let write = ShardWrite::open(shard, &group, &members, &batches)?;
    let applied = apply_drained_chunk(
        &write,
        graphs,
        by_graph,
        raft_log_ops,
        crypto,
        #[cfg(feature = "security")]
        audit_tail,
    );
    // The row gate closes whether or not the rows landed: dropping a member's
    // owner-row admission unfinished poisons the shared transaction, so the
    // failure must not skip it.
    let finished = write.finish();
    match (applied, finished) {
        (Ok(()), Ok(())) => shard.commit_drain(group, &batches, committed_at_ms),
        (Err(error), _) | (Ok(()), Err(error)) => {
            shard.mutations().abort_group(group)?;
            Err(error)
        }
    }
}

/// Every row one chunk writes, member by member.
///
/// One graph's tables at a time: `redb` refuses a second open of a table whose
/// first handle is still alive, and every graph member writes the same `nodes`.
/// The per-graph loop is what keeps that legal, and it is also the natural
/// shape -- a member's rows are exactly one graph's.
pub(crate) fn apply_drained_chunk(
    write: &ShardWrite<'_>,
    graphs: &[String],
    by_graph: &mut BTreeMap<String, Vec<Method>>,
    raft_log_ops: &mut Vec<(u64, u64, Vec<u8>)>,
    crypto: DurableCrypto<'_>,
    #[cfg(feature = "security")] audit_tail: &mut AuditTailCache,
) -> Result<(), String> {
    for graph in graphs {
        let Some(methods) = by_graph.remove(graph) else {
            continue;
        };
        apply_graph_methods(
            write,
            graph,
            &methods,
            crypto,
            #[cfg(feature = "security")]
            audit_tail,
        )?;
        backfill_graph_meta_row(write, graph)?;
    }
    if !raft_log_ops.is_empty() {
        let mut log = write.control().open_table(RAFT_LOG)?;
        for (gid, idx, blob) in raft_log_ops.drain(..) {
            // Consensus entries carry Method payloads. When the deployment data
            // key is active, seal them just like authoritative value rows so
            // source properties are not exposed by the local Raft log.
            let sealed = crypto.seal(&blob);
            log.insert((gid, idx), sealed.as_ref())
                .map_err(|error| error.to_string())?;
        }
    }
    Ok(())
}

/// One graph member's rows for one drained chunk.
pub(crate) fn apply_graph_methods(
    write: &ShardWrite<'_>,
    graph: &str,
    methods: &[Method],
    crypto: DurableCrypto<'_>,
    #[cfg(feature = "security")] audit_tail: &mut AuditTailCache,
) -> Result<(), String> {
    let member = write.graph(graph)?;
    let mut tables = GraphRowTables::open(member)?;
    for method in methods {
        if matches!(method, Method::ClearGraph | Method::DeleteGraph { .. }) {
            tables.command_sequences.remove(graph)?;
            clear_resource_rows_in_wtx(write, graph, crypto)?;
            development_lane::clear_native_graph_rows_in_wtx(write, graph, crypto)?;
            capacity_lease::clear_graph_rows(write, graph)?;
            work_item_capability::clear_graph_rows_with_native(
                write,
                graph,
                &mut tables.native_work_items,
            )?;
        }
        apply_method_rows(graph, method, &mut tables, crypto)?;
        #[cfg(feature = "security")]
        append_audit_entry(&mut tables.audit, audit_tail, graph, method)?;
    }
    drop(tables);
    development_lane::validate_current_lane_links_in_wtx(write, graph, crypto)
}

/// Backfill the catalog row of a graph that received writes but was never
/// explicitly registered (the pre-created `__commons__`, for instance), so
/// authoritative `load_all` recovers it with no checkpoint.
///
/// The catalog is FILE-WIDE (RF-ADR-006 row-class correction, G2): it is the
/// file's list of which graphs it hosts, and the boot scan has to enumerate it
/// to learn those names before any graph scope can be bound. So the row is the
/// control member's to write, in the same admitted group as the graph's own.
/// The LOGICAL name a catalog row created from a durable key must carry.
///
/// The catalog row's name field is what `RedbBackend::load_into` registers a
/// recovered graph under, and what a client knows the graph by. Both writers
/// that create a row without being handed a separate logical name have only the
/// durable KEY -- the `sanitize`d spelling -- and writing that straight into the
/// name field silently renames every graph whose name needed escaping: a graph
/// called `"tenant:acme"` came back from a restart as `"tenant~3aacme"`, so a
/// lookup by its real name missed and its rows looked lost. It stayed invisible
/// because the common case is an already-alphanumeric name, where key and name
/// are the same string.
///
/// `sanitize` is a byte escaping and exactly invertible for such a name, so the
/// logical name is RECOVERED here, not guessed. The one key it cannot invert is
/// the bounded `~h<sha256>` form a very long name falls back to; there is
/// genuinely no preimage for that, so the key stands as the name exactly as
/// before, and such a graph must be registered explicitly (through
/// `write_graph_meta_with_incarnation`, which is given both) to carry its real
/// name across a restart.
pub(crate) fn catalog_display_name(graph_fname: &str) -> String {
    unsanitize(graph_fname).unwrap_or_else(|| graph_fname.to_string())
}

pub(crate) fn backfill_graph_meta_row(write: &ShardWrite<'_>, graph: &str) -> Result<(), String> {
    let mut meta = write.control().open_table(GRAPH_META)?;
    if meta
        .get(graph)
        .map_err(|error| error.to_string())?
        .is_some()
    {
        return Ok(());
    }
    let incarnation_id = new_incarnation_id(graph);
    let name = catalog_display_name(graph);
    let encoded = encode_meta_with_incarnation(&name, GraphType::Global, &incarnation_id)?;
    meta.insert(graph, encoded.as_slice())
        .map_err(|error| error.to_string())?;
    Ok(())
}
