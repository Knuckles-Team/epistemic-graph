//! Cross-modal atomic commit: blob refs, vectors, measurements and projection
//! rows written into the SAME shard transaction as the graph rows
//! (CONCEPT:EG-KG.backend.cross-modal-atomic-commit).
//!
//! # A cross-modal commit is a scope GROUP
//!
//! Where this path used to say "the same `WriteTransaction`" it now says "the
//! same admitted scope group" (RF-RULING-008): ONE physical write transaction,
//! N members, each confined to its own rows by the capability it holds rather
//! than by an argument.
//!
//! * The graph rows -- `nodes`, `edges`, `ledger`, `semantic_store`, the audit
//!   chain, the native WorkItem provenance rows -- are ONE graph member's, and
//!   every key of theirs leads with that graph's name
//!   ([`ShardWrite::graph`] -> `open_scoped_table`).
//! * The three `series_*` tables are FILE-WIDE: their keys carry no graph
//!   component at all, so they belong to the file rather than to any graph and
//!   ride the control member ([`ShardWrite::control`] -> `open_table`, which
//!   yields a raw `redb::Table`). The catalog row `backfill_graph_meta_row`
//!   backfills is file-wide for the same reason.
//!
//! That split is exactly why the commit is a group and not a single scope: the
//! measurement and the node it annotates cannot be one member's rows, but they
//! must be one fsync.
//!
//! # The eg-tsdb seam
//!
//! Measurement rows reach `eg-tsdb` through its `SeriesTableWriter` trait,
//! which this path satisfies with `AdmittedOwnerWrite<'_, GraphShardOwner>`
//! (the control member) -- eg-tsdb never opens a database, it only turns an
//! already-authorized admission into a table handle.
//!
//! # Table-handle discipline
//!
//! `redb` refuses to open one table twice in a transaction while the first
//! handle is alive, so each phase below opens the graph tables it needs, does
//! its row work, and lets them fall out of scope before the next phase opens
//! them again. Reopening within the group's transaction observes everything the
//! previous phase staged, which is why the phases can be sequenced this way at
//! all -- and why there is ONE blob-ref writer here rather than a
//! "reopened"/"original" pair.

use super::*;

use eg_storage::ScopedOwnerTableMut;

use super::shard::{Shard, ShardWrite};

/// One node's vector upsert for a cross-modal commit (CONCEPT:EG-KG.txn.reader-never-sees-node).
pub type VectorUpsert = (String, Vec<f32>);

/// A blob-reference for a cross-modal commit (CONCEPT:EG-KG.txn.reader-never-sees-node): a `(node_id, digest)`
/// pair recorded as a durable graph-side link to an already-stored blob. The blob
/// BYTES live in the content-addressed `blob.redb` (pre-uploaded); THIS is the durable
/// graph pointer that must land atomically with the node/vector/property.
pub type BlobRefRow = (String, String);

/// One graph's `nodes` (or `native_work_items`) rows opened for writing on that
/// graph's own scope.
type NodeRows<'a> = ScopedOwnerTableMut<'a, (&'static str, &'static str), &'static [u8]>;

/// One graph's `semantic_store` row opened for writing on that graph's own
/// scope. Its key IS the graph name, so the scope bound and the key coincide.
type SemanticRows<'a> = ScopedOwnerTableMut<'a, &'static str, &'static [u8]>;

/// Blob-ref half of the cross-modal projection, for ONE node: a `__blob__`
/// reserved property carrying the digest, merged into the node row.
///
/// Read-modify-write, so the ref rides the node row: unseal the current
/// property blob, merge, re-seal. A node under native WorkItem authority -- or
/// one whose properties cannot be decoded, which is indistinguishable from one
/// that is -- is refused, because a generic blob update is not an authority
/// over a WorkItem.
///
/// The single writer. Before the cut this existed twice, as `_reopened` and
/// `_original`, differing only in whether the two halves of the authority guard
/// were sequenced or combined -- an artifact of whether the caller had already
/// dropped and reopened `nodes`. Under the group path every phase opens its own
/// handles, so there is one shape and one copy: read `current` once and combine
/// both refusals with `||`, which is also strictly the fewer reads.
pub(crate) fn apply_crossmodal_blob_ref(
    graph: &str,
    node_id: &str,
    digest: &str,
    nodes: &mut NodeRows<'_>,
    native_work_items: &NodeRows<'_>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let current = nodes
        .get((graph, node_id))?
        .map(|value| crypto.unseal(value.value()))
        .transpose()?;
    if native_work_items.get((graph, node_id))?.is_some()
        || current.as_ref().is_some_and(|bytes| {
            decode_durable::<serde_json::Map<String, serde_json::Value>>(bytes)
                .map(|props| property_string(&props, "node_type") == "WorkItem")
                .unwrap_or(true)
        })
    {
        return Err("native WorkItem authority required for generic blob update".to_string());
    }
    let mut props: serde_json::Map<String, serde_json::Value> = match current {
        Some(bytes) => decode_durable(&bytes)?,
        None => serde_json::Map::new(),
    };
    props.insert(
        "__blob__".to_string(),
        serde_json::Value::String(digest.to_string()),
    );
    let bytes = rmp_serde::to_vec_named(&props).map_err(|e| e.to_string())?;
    let sealed = crypto.seal(&bytes);
    nodes.insert((graph, node_id), sealed.as_ref())
}

/// Every staged blob ref of one graph, through the one writer above.
pub(crate) fn apply_crossmodal_blob_refs(
    graph: &str,
    blob_refs: &[BlobRefRow],
    nodes: &mut NodeRows<'_>,
    native_work_items: &NodeRows<'_>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    for (node_id, digest) in blob_refs {
        apply_crossmodal_blob_ref(graph, node_id, digest, nodes, native_work_items, crypto)?;
    }
    Ok(())
}

/// Vector half of the cross-modal projection: the graph's `semantic_store` blob
/// is read-modify-written inside the group's transaction, so a node and its
/// embedding are durable together -- never a node without its vector or
/// vice-versa.
pub(crate) fn apply_crossmodal_vectors(
    graph: &str,
    vectors: &[VectorUpsert],
    semantic: &mut SemanticRows<'_>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    if vectors.is_empty() {
        return Ok(());
    }
    let current = semantic
        .get(graph)?
        .map(|value| crypto.unseal(value.value()))
        .transpose()?;
    let mut store = match current {
        Some(bytes) => decode_durable::<crate::compute::semantic::SemanticStore>(&bytes)?,
        None => crate::compute::semantic::SemanticStore::default(),
    };
    for (node_id, embedding) in vectors {
        // CONCEPT:EG-KG.compute.rank-dim-mismatch-guard (BUG-007): a rejected write bails via `?`
        // BEFORE `store` is reserialized/inserted below, so a mid-batch mismatch
        // never reaches durable storage -- `store` here is a scratch decode, not
        // the live in-RAM store, discarded on this early return. Any error here
        // also fails the whole admitted group (see [`commit_crossmodal`]), so a
        // rejected write can never partially land.
        store
            .add_embedding(node_id.clone(), embedding.clone())
            .map_err(|error| error.to_string())?;
    }
    let bytes = rmp_serde::to_vec_named(&store).map_err(|e| e.to_string())?;
    let sealed = crypto.seal(&bytes);
    semantic.insert(graph, sealed.as_ref())
}

/// Time-series half of the cross-modal projection: each batch is appended into
/// `series_chunks`/`series_meta` on the CONTROL member of this group.
///
/// Those tables are file-wide -- their keys carry no graph component -- so they
/// are the file's rows, not the graph's, and eg-tsdb reaches them through
/// `SeriesTableWriter for AdmittedOwnerWrite<'_, GraphShardOwner>` using the
/// shared chunk encoding. That is what makes a measurement atomic WITH the
/// graph modalities: the same admitted group, and therefore the same fsync.
#[cfg(feature = "tsdb")]
pub(crate) fn apply_crossmodal_measurements(
    write: &ShardWrite<'_>,
    measurements: &[crate::MeasurementBatch],
) -> Result<(), String> {
    for (series, n_fields, bucket_ns, field_names, points) in measurements {
        // Persistence accepts only the authority-scoped key produced at the
        // verified carrier boundary; it never derives or guesses a tenant.
        if eg_tsdb::store::SeriesKey::decode(series).is_none() {
            return Err("time-series key is not canonically scoped".to_string());
        }
        let points: Vec<eg_tsdb::point::Point> = points
            .iter()
            .map(|(ts, values)| eg_tsdb::point::Point {
                ts: *ts,
                values: values.clone(),
            })
            .collect();
        eg_tsdb::store::append_batch_in_wtx(
            write.control(),
            series,
            *n_fields,
            *bucket_ns,
            field_names,
            &points,
        )
        .map_err(|e| e.to_string())?;
    }
    Ok(())
}

/// A build without the `tsdb` feature has no series tables and no eg-tsdb
/// dependency, so a measurement here has no durable home -- error rather than
/// silently drop it. The staging handler is `tsdb`-gated, so in practice this is
/// never non-empty.
#[cfg(not(feature = "tsdb"))]
pub(crate) fn apply_crossmodal_measurements(
    _write: &ShardWrite<'_>,
    measurements: &[crate::MeasurementBatch],
) -> Result<(), String> {
    if !measurements.is_empty() {
        return Err("time-series cross-modal commit requires the `tsdb` feature".to_string());
    }
    Ok(())
}

/// The blob-ref and vector projections of one graph member.
///
/// Each modality opens the graph tables it needs and lets them fall out of
/// scope again, so the phase that follows -- the lane-link validation, which
/// reopens `nodes` -- is legal. Nothing is opened for an empty write-set, so a
/// measurement-only commit never touches `nodes` or `semantic_store` at all.
pub(crate) fn apply_crossmodal_blob_and_vector_rows(
    write: &ShardWrite<'_>,
    graph: &str,
    vectors: &[VectorUpsert],
    blob_refs: &[BlobRefRow],
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let member = write.graph(graph)?;
    if !blob_refs.is_empty() {
        let mut nodes = member.open_scoped_table(NODES)?;
        let native_work_items =
            member.open_scoped_table(work_item_capability::NATIVE_WORK_ITEMS)?;
        apply_crossmodal_blob_refs(graph, blob_refs, &mut nodes, &native_work_items, crypto)?;
    }
    if !vectors.is_empty() {
        let mut semantic = member.open_scoped_table(SEMANTIC)?;
        apply_crossmodal_vectors(graph, vectors, &mut semantic, crypto)?;
    }
    Ok(())
}

/// Apply only the non-topology projections of a cross-modal write-set inside an
/// already-admitted scope group.
///
/// The universal MutationBatch kernel calls this after graph rows and before
/// status/outbox; the low-level cross-modal primitive reaches the same rows
/// through [`apply_crossmodal_body`]. No commit occurs here.
pub(crate) fn apply_crossmodal_projection_rows(
    write: &ShardWrite<'_>,
    graph: &str,
    vectors: &[VectorUpsert],
    blob_refs: &[BlobRefRow],
    measurements: &[crate::MeasurementBatch],
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    apply_crossmodal_blob_and_vector_rows(write, graph, vectors, blob_refs, crypto)?;
    apply_crossmodal_measurements(write, measurements)
}

pub(crate) fn crossmodal_has_clear_or_delete(methods: &[Method]) -> bool {
    methods
        .iter()
        .any(|method| matches!(method, Method::ClearGraph | Method::DeleteGraph { .. }))
}

pub(crate) fn crossmodal_has_node_topology_methods(methods: &[Method]) -> bool {
    methods.iter().any(|method| {
        matches!(
            method,
            Method::AddNode { .. }
                | Method::RemoveNode { .. }
                | Method::CompareAndSetNodeFields { .. }
                | Method::BatchUpdate { .. }
                | Method::ClearGraph
                | Method::DeleteGraph { .. }
        )
    })
}

/// The two graph-clear sweeps that open their own tables, run before this
/// member's own graph tables are opened.
///
/// The third sweep -- private WorkItem capability state -- cannot run here: it
/// needs the `native_work_items` handle [`apply_crossmodal_methods`] already
/// holds, and `redb` refuses a second open of a live table.
pub(crate) fn apply_crossmodal_clear_native_graph_rows(
    write: &ShardWrite<'_>,
    graph: &str,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    development_lane::clear_native_graph_rows_in_wtx(write, graph, crypto)?;
    capacity_lease::clear_graph_rows(write, graph)
}

/// Graph mutations (nodes/edges/properties) -- the SAME row apply the
/// single-modal path uses -- on one graph member of this group.
///
/// The member's graph tables are opened here and released when this function
/// returns, which is the drop point every later phase depends on: the borrow
/// ends with the scope rather than with a hand-written `drop`.
pub(crate) fn apply_crossmodal_methods(
    write: &ShardWrite<'_>,
    graph: &str,
    methods: &[Method],
    crypto: DurableCrypto<'_>,
    #[cfg(feature = "security")] audit_tail: &mut AuditTailCache,
) -> Result<(), String> {
    let mut tables = GraphRowTables::open(write.graph(graph)?)?;
    if crossmodal_has_clear_or_delete(methods) {
        work_item_capability::clear_graph_rows_with_native(
            write,
            graph,
            &mut tables.native_work_items,
        )?;
    }
    for method in methods {
        apply_method_rows(graph, method, &mut tables, crypto)?;
        #[cfg(feature = "security")]
        append_audit_entry(&mut tables.audit, audit_tail, graph, method)?;
    }
    Ok(())
}

/// Close one cross-modal write-set: re-validate the lane policy against the
/// FINAL image, land the measurements on the control member, and backfill the
/// graph's catalog row.
///
/// The lane check runs against the post-projection image, not only the
/// pre-projection graph rows, because a blob ref and a vector upsert are both
/// in-transaction node/semantic replacement surfaces.
pub(crate) fn finalize_crossmodal_commit(
    write: &ShardWrite<'_>,
    graph: &str,
    measurements: &[crate::MeasurementBatch],
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    development_lane::validate_current_lane_links_in_wtx(write, graph, crypto)?;
    apply_crossmodal_measurements(write, measurements)?;
    backfill_graph_meta_row(write, graph)
}

/// Every row one cross-modal write-set stages, in order, on one already-admitted
/// scope group.
pub(crate) fn apply_crossmodal_body(
    write: &ShardWrite<'_>,
    graph: &str,
    staged: CrossModalStaged<'_>,
    crypto: DurableCrypto<'_>,
    #[cfg(feature = "security")] audit_tail: &mut AuditTailCache,
) -> Result<(), String> {
    if crossmodal_has_clear_or_delete(staged.methods) {
        apply_crossmodal_clear_native_graph_rows(write, graph, crypto)?;
    }
    apply_crossmodal_methods(
        write,
        graph,
        staged.methods,
        crypto,
        #[cfg(feature = "security")]
        audit_tail,
    )?;
    // Blob refs are read-modify-write node projections, so a topology change
    // has to clear the lane policy BEFORE they are merged as well as after --
    // the projection can only be validated against a graph that was already
    // valid without it.
    if crossmodal_has_node_topology_methods(staged.methods) {
        development_lane::validate_current_lane_links_in_wtx(write, graph, crypto)?;
    }
    apply_crossmodal_blob_and_vector_rows(write, graph, staged.vectors, staged.blob_refs, crypto)?;
    finalize_crossmodal_commit(write, graph, staged.measurements, crypto)
}

/// Everything one cross-modal commit stages, bundled so [`commit_crossmodal`]
/// stays inside clippy's parameter cap. Each slice is exactly the borrow callers
/// used to pass positionally, in the same order.
#[derive(Clone, Copy, Default)]
pub(crate) struct CrossModalStaged<'a> {
    pub methods: &'a [Method],
    pub vectors: &'a [VectorUpsert],
    pub blob_refs: &'a [BlobRefRow],
    /// Staged time-series measurement batches (CONCEPT:EG-KG.backend.cross-modal-atomic-commit). Each lands in the
    /// SAME admitted group as the graph/vector/blob writes, into
    /// `series_chunks`/`series_meta` in THIS shard (not a separate
    /// `series.redb`), so a measurement and the node it annotates are durable
    /// together -- never one without the other.
    pub measurements: &'a [crate::MeasurementBatch],
}

/// **Cross-modal ACID commit (CONCEPT:EG-KG.txn.reader-never-sees-node)** -- land a graph + vector + blob-ref +
/// property + measurement write-set for ONE graph in ONE admitted scope group,
/// all-or-nothing.
///
/// This is the durable barrier the single-graph cross-modal txn commits through.
/// Every modality writes into the SAME group, so the commit is atomic:
///   * **graph** ops (`AddNode`/`AddEdge`/`CompareAndSetNodeFields`/…) →
///     `nodes`/`edges`, via the shared [`apply_method_rows`] (the SAME rows the
///     single-modal path writes), on the graph member;
///   * **vectors** → the graph's `semantic_store` blob is read-modify-written
///     inside the group (deserialize → `add_embedding` each upsert →
///     reserialize), on the graph member;
///   * **blob refs** → a `__blob__` reserved property on the node carrying the
///     digest, written into `nodes`, on the graph member;
///   * **measurements** (CONCEPT:EG-KG.backend.cross-modal-atomic-commit) → each time-series batch is appended into
///     `series_chunks`/`series_meta` via the shared eg-tsdb chunk encoding, on
///     the CONTROL member, because those tables are file-wide. `tsdb`-gated; a
///     slim redb-only build errors on a non-empty batch. This shard copy is the
///     atomic/authoritative one; the caller
///     (`handlers::txn::commit_cross_modal_txn`) additionally replays the same
///     batch into the SERVED `series.redb` right after this call returns `Ok`, so
///     it is actually reachable through the public `Ts*`/`Op::TsScan` read path
///     (CONCEPT:EG-KG.backend.ts-served-materialize, EG-P0-4) -- see that
///     function's doc comment for the exact guarantee and the one remaining
///     non-atomic boundary.
///
/// If ANY step errors the group is ABORTED rather than committed, so none of the
/// modalities land (a true rollback, no partial). On success the kernel commits
/// the one physical transaction at `Durability::Immediate` -- commit-before-ack:
/// the cross-modal write is on disk before the client is told it succeeded.
///
/// The class is `Operation`, not `Maintenance`: these are caller methods, which
/// is exactly what [`Shard::admit_drain`] labels. It is not
/// [`Shard::admit_batch`] either -- that admits a caller's own `MutationBatch`
/// verbatim, and this primitive receives none; the batch-carrying cross-modal
/// path is `commit_mutation_batch_crossmodal`.
///
/// `op_id` must be unique per ATTEMPT, exactly as [`commit_ops`]'s `drain_id`
/// must: the kernel resolves a batch id that already carries a durable receipt
/// to `Begin::Replay` and SKIPS it, so a reused id would silently drop this
/// commit after a restart.
pub(crate) fn commit_crossmodal(
    shard: &Shard,
    graph: &str,
    staged: CrossModalStaged<'_>,
    op_id: &str,
    committed_at_ms: u64,
    crypto: DurableCrypto<'_>,
    // O(1) audit-chain tail cache (CONCEPT:EG-KG.storage.embedded-store), shared with the group-commit path.
    #[cfg(feature = "security")] audit_tail: &mut AuditTailCache,
) -> Result<(), String> {
    // A cold graph binds FIRST, in its own transaction: binding opens and
    // commits its own write and redb admits one writer, so it cannot happen
    // inside the group.
    let members = shard.graph_members(&[graph])?;
    let (group, batches) = shard.admit_drain(&members, op_id)?;
    let write = ShardWrite::open(shard, &group, &members, &batches)?;
    let applied = apply_crossmodal_body(
        &write,
        graph,
        staged,
        crypto,
        #[cfg(feature = "security")]
        audit_tail,
    );
    // The row gate closes whether or not the rows landed: dropping a member's
    // owner-row admission unfinished poisons the shared transaction, so the
    // failure path must not skip it.
    let finished = write.finish();
    match (applied, finished) {
        // The atomic commit point: every modality lands here.
        (Ok(()), Ok(())) => shard.commit_drain(group, &batches, committed_at_ms),
        (Err(error), _) | (Ok(()), Err(error)) => {
            shard.mutations().abort_group(group)?;
            Err(error)
        }
    }
}
