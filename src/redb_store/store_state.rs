use super::store_prelude::*;
use super::*;

pub(crate) struct StagedMutationRows {
    pub(crate) generated_result: Option<Vec<u8>>,
}

/// The row-phase inputs, bundled out of [`stage_mutation_batch_rows`]'s
/// parameter list.
pub(crate) struct StagedRowInput<'a> {
    pub(crate) graph_fname: &'a str,
    pub(crate) batch: &'a MutationBatch,
    pub(crate) change: Option<&'a ChangeEnvelope>,
    pub(crate) authoritative_state_msgpack: Option<&'a [u8]>,
    pub(crate) crossmodal: Option<&'a CrossModalBatchRows<'a>>,
    pub(crate) committed_at_ms: u64,
    pub(crate) audited: bool,
    pub(crate) crashpoint: Option<MutationBatchCrashpoint>,
}

/// Every OWNER row one batch writes, inside the admitted group.
///
/// The row gate closes whether or not the rows landed: dropping a member's
/// owner-row admission unfinished poisons the shared transaction, so a failure
/// here must not skip it.
pub(crate) fn stage_mutation_batch_rows(
    shard: &Shard,
    group: &AdmittedGroup<'_, GraphShardOwner>,
    members: &[(String, Arc<OwnedStoreHandle<GraphShardOwner>>)],
    batches: &[MutationBatch],
    input: StagedRowInput<'_>,
    crypto: DurableCrypto<'_>,
    #[cfg(feature = "security")] staged_audit_tail: &mut AuditTailCache,
) -> Result<StagedMutationRows, String> {
    let write = ShardWrite::open(shard, group, members, batches)?;
    let staged = stage_rows_in(
        &write,
        input,
        crypto,
        #[cfg(feature = "security")]
        staged_audit_tail,
    );
    let finished = write.finish();
    match (staged, finished) {
        (Ok(staged), Ok(())) => Ok(staged),
        (Err(error), _) | (Ok(_), Err(error)) => Err(error),
    }
}

/// The row phases themselves, in order, with the row gate already open.
pub(crate) fn stage_rows_in(
    write: &ShardWrite<'_>,
    input: StagedRowInput<'_>,
    crypto: DurableCrypto<'_>,
    #[cfg(feature = "security")] staged_audit_tail: &mut AuditTailCache,
) -> Result<StagedMutationRows, String> {
    let StagedRowInput {
        graph_fname,
        batch,
        change,
        authoritative_state_msgpack,
        crossmodal,
        committed_at_ms,
        audited,
        crashpoint,
    } = input;

    // Domain preconditions the kernel cannot know: an envelope must not already
    // be committed, and a content version / cursor precondition must hold. These
    // are the ONLY checks that survived the cut -- the idempotency, OCC and
    // fence checks beside them are the kernel's now.
    let plan = prepare_and_validate_mutation_batch(
        &MutationRowCtx {
            write,
            graph_fname,
            batch,
            crypto,
        },
        change,
        authoritative_state_msgpack,
        crossmodal,
    )?;

    run_mutation_batch_crashpoint(batch, crashpoint, MutationBatchCrashpoint::BeforeRows)?;

    let generated_result = apply_state_dispatch_rows(StateDispatchInput {
        write,
        graph_fname,
        staged_state: plan.staged_state.as_ref(),
        batch,
        committed_at_ms,
        crypto,
        #[cfg(feature = "security")]
        staged_audit_tail,
        audited,
        crossmodal_present: crossmodal.is_some(),
    })?;

    apply_crossmodal_rows_phase(write, graph_fname, crossmodal, crypto)?;

    apply_post_row_cleanup(
        write,
        graph_fname,
        batch,
        plan.lifecycle.as_ref(),
        authoritative_state_msgpack.is_none(),
    )?;

    run_mutation_batch_crashpoint(
        batch,
        crashpoint,
        MutationBatchCrashpoint::AfterRowsBeforeMetadata,
    )?;

    // The governance rows and the catalog entry. The receipt, idempotency row,
    // class row, version bump and outbox rows that used to sit beside them are
    // written by `commit::finish` from the batch itself.
    if let Some(change) = change {
        apply_change_envelope_commit_rows(write, graph_fname, change, committed_at_ms, crypto)?;
    }
    write_mutation_batch_graph_meta_row(
        write,
        graph_fname,
        batch,
        plan.lifecycle,
        plan.schema_sources_update,
    )?;

    Ok(StagedMutationRows { generated_result })
}

pub(crate) fn compute_native_terminal_work_item_cas(batch: &MutationBatch) -> bool {
    batch.authoritative_state.is_none()
        && match batch.operations.first() {
            Some(first)
                if first.domain == DurabilityDomain::ControlPlane
                    && first.surface == MutationSurface::Job
                    && matches!(&first.method, Method::CommitWorkItemResult { .. }) =>
            {
                batch.operations[1..]
                    .iter()
                    .all(|op| matches!(&op.method, Method::AddNode { .. }))
            }
            Some(first) => {
                batch.operations.len() == 1
                    && first.domain == DurabilityDomain::ControlPlane
                    && first.surface == MutationSurface::Job
                    && matches!(
                        &first.method,
                        Method::CancelWorkItem { .. }
                            | Method::DeferWorkItem { .. }
                            | Method::IssueControlLease { .. }
                            | Method::TransitionControlLease { .. }
                            | work_item_resource_writes!()
                            | Method::SubmitWorkItem { .. }
                            | Method::SubmitWorkItems { .. }
                    )
            }
            None => false,
        }
}

pub(crate) fn resolve_mutation_authoritative_state(
    batch: &MutationBatch,
    authoritative_state_msgpack: Option<&[u8]>,
) -> Result<Option<AuthoritativeGraphState>, String> {
    Ok(
        match (&batch.authoritative_state, authoritative_state_msgpack) {
            (Some(descriptor), Some(bytes)) => {
                use sha2::{Digest, Sha256};
                let digest = hex::encode(Sha256::digest(bytes));
                if digest != descriptor.digest {
                    return Err(
                        "authoritative state digest does not match MutationBatch".to_string()
                    );
                }
                let state = match descriptor.algorithm.as_str() {
                    "sha256" => AuthoritativeGraphState::Snapshot(Box::new(
                        decode_durable::<crate::graph::GraphSnapshot>(bytes).map_err(|_| {
                            "authoritative graph state is invalid or exceeds resource limits"
                                .to_string()
                        })?,
                    )),
                    crate::graph_delta::ROW_DELTA_ALGORITHM
                    | crate::graph_delta::LEGACY_ROW_DELTA_ALGORITHM => {
                        AuthoritativeGraphState::RowDelta(decode_authoritative_row_delta(
                            bytes,
                            &descriptor.algorithm,
                        )?)
                    }
                    _ => return Err("unsupported authoritative state algorithm".to_string()),
                };
                Some(state)
            }
            (None, None) => None,
            (Some(_), None) => {
                return Err("MutationBatch state descriptor has no authoritative bytes".to_string());
            }
            (None, Some(_)) => {
                return Err(
                    "authoritative bytes require a MutationBatch state descriptor".to_string(),
                );
            }
        },
    )
}

fn decode_authoritative_row_delta(
    bytes: &[u8],
    algorithm: &str,
) -> Result<crate::graph_delta::GraphRowDelta, String> {
    let delta = decode_durable::<crate::graph_delta::GraphRowDelta>(bytes).map_err(|_| {
        "authoritative graph row delta is invalid or exceeds resource limits".to_string()
    })?;
    delta.validate()?;
    if !delta.matches_algorithm(algorithm) {
        return Err(
            "authoritative graph row delta algorithm does not match its wire version".to_string(),
        );
    }
    Ok(delta)
}

// `None` means this mutation does not change graph control state. A present
// source set may have no dynamic entries, which explicitly detaches the final
// graph-local source without confusing "unchanged" with "core-only".
pub(crate) fn resolve_schema_sources_update(
    staged_state: Option<&AuthoritativeGraphState>,
) -> Option<std::sync::Arc<crate::graph::GraphSchemaSources>> {
    match staged_state {
        Some(AuthoritativeGraphState::Snapshot(snapshot)) => {
            Some(std::sync::Arc::clone(&snapshot.schema_sources))
        }
        Some(AuthoritativeGraphState::RowDelta(delta)) => delta.schema_sources_update().cloned(),
        None => None,
    }
}

/// The graph name of a graph-scoped `MutationBatch`. Every commit path in this
/// module is indexed by `graph_fname` and only ever handles
/// `MutationScope::Graph` batches -- `batch.validate()` (run before any of
/// these call this) structurally requires `MutationScope::Graph` to pair with
/// `VersionExpectation::Graph(_)`, so a native scope can never reach here in
/// practice. A native scope reports no graph name at all; fail closed instead
/// of ever substituting "" for it.
pub(crate) fn mutation_batch_graph_name(batch: &MutationBatch) -> Result<&str, String> {
    batch
        .identity
        .scope()
        .graph_name()
        .map(LogicalName::as_str)
        .ok_or_else(|| "mutation batch is not graph-scoped".to_string())
}

pub(crate) fn validate_mutation_batch_route_and_lowering(
    batch: &MutationBatch,
    graph_fname: &str,
    crossmodal: Option<&CrossModalBatchRows<'_>>,
    staged_state_is_none: bool,
) -> Result<(), String> {
    let batch_graph_name = mutation_batch_graph_name(batch)?;
    // `Shard::bind_caller_batch` has already proved the caller's logical graph
    // against `sanitize(...)` and rebound this batch to the physical shard
    // scope.  This validator therefore sees the bound physical spelling; a
    // second sanitization would turn `acme~3aa` into `acme~7e3aa` and reject a
    // valid escaped route.  Keep the comparison exact here so the one
    // logical-to-physical conversion remains at the caller admission seam.
    if graph_fname != batch_graph_name {
        return Err(format!(
            "mutation batch graph route mismatch: bound batch '{}' does not match '{}'",
            batch_graph_name, graph_fname
        ));
    }
    for operation in &batch.operations {
        let crossmodal_sentinel = crossmodal.is_some()
            && matches!(
                &operation.method,
                Method::ApplyMutation { event_type, .. }
                    if event_type == "crossmodal_operation"
            );
        if staged_state_is_none
            && !supports_atomic_batch_rows(&operation.method)
            && !crossmodal_sentinel
        {
            return Err(format!(
                "MutationBatch operation {} is not lowered to the atomic graph-row kernel",
                operation.ordinal
            ));
        }
    }
    if let Some(rows) = crossmodal {
        for method in rows.methods {
            if !supports_atomic_batch_rows(method) {
                return Err(
                    "cross-modal graph method is not lowered to the atomic row kernel".to_string(),
                );
            }
        }
    }
    Ok(())
}

pub(crate) fn detect_and_validate_lifecycle(
    batch: &MutationBatch,
    graph_fname: &str,
) -> Result<Option<(bool, String, Option<GraphType>)>, String> {
    let lifecycle = batch
        .operations
        .iter()
        .find_map(|operation| match &operation.method {
            Method::CreateGraph {
                graph_name,
                graph_type,
            } => Some((true, graph_name.clone(), Some(*graph_type))),
            Method::DeleteGraph { graph_name } => Some((false, graph_name.clone(), None)),
            _ => None,
        });
    if let Some((_, ref graph_name, _)) = lifecycle {
        // Lifecycle methods retain their logical target in the operation while
        // the surrounding bound batch carries the physical shard key.
        if batch.operations.len() != 1 || sanitize(graph_name) != graph_fname {
            return Err(
                "lifecycle MutationBatch must contain exactly one operation for its target graph"
                    .to_string(),
            );
        }
    }
    Ok(lifecycle)
}

/// An envelope replay must reproduce the committed envelope byte-for-byte.
pub(crate) fn check_replay_envelope_matches(
    write: &ShardWrite<'_>,
    graph_fname: &str,
    change: &ChangeEnvelope,
    graph_name: &str,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let envelopes = write
        .graph(graph_fname)?
        .open_scoped_table(CHANGE_ENVELOPES)
        .map_err(|e| e.to_string())?;
    let stored = envelopes
        .get((graph_fname, change.envelope_id.as_str()))
        .map_err(|e| e.to_string())?
        .ok_or_else(|| {
            format!(
                "STALE_FENCE: committed envelope '{}' is no longer current for graph '{}'",
                change.envelope_id, graph_name
            )
        })?;
    let bytes = crypto.unseal(stored.value())?;
    let record: ChangeEnvelopeRecord = decode_durable(&bytes)?;
    let stored_bytes = rmp_serde::to_vec_named(&record.envelope).map_err(|e| e.to_string())?;
    let proposed_bytes = rmp_serde::to_vec_named(change).map_err(|e| e.to_string())?;
    if stored_bytes != proposed_bytes {
        return Err(format!(
            "IDEMPOTENCY_CONFLICT: envelope '{}' does not match its committed batch",
            change.envelope_id
        ));
    }
    Ok(())
}

/// Precondition 1: the envelope id must not already be committed for this graph.
pub(crate) fn check_change_envelope_not_committed(
    write: &ShardWrite<'_>,
    graph_fname: &str,
    change: &ChangeEnvelope,
) -> Result<(), String> {
    let envelopes = write
        .graph(graph_fname)?
        .open_scoped_table(CHANGE_ENVELOPES)
        .map_err(|e| e.to_string())?;
    if envelopes
        .get((graph_fname, change.envelope_id.as_str()))
        .map_err(|e| e.to_string())?
        .is_some()
    {
        return Err(format!(
            "IDEMPOTENCY_CONFLICT: envelope_id '{}' is already committed",
            change.envelope_id
        ));
    }
    Ok(())
}

/// Precondition 2: the envelope's content version must chain off the durable
/// one -- matching previous digest AND a strictly advancing source version --
/// or, with no durable row, must not claim a previous digest.
pub(crate) fn check_change_content_version_precondition(
    write: &ShardWrite<'_>,
    graph_fname: &str,
    tenant: &str,
    change: &ChangeEnvelope,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let versions = write
        .graph(graph_fname)?
        .open_scoped_table(CONTENT_VERSIONS)
        .map_err(|e| e.to_string())?;
    let version_key = (
        graph_fname,
        tenant,
        change.content_version.object_id.as_str(),
    );
    let current = versions
        .get(version_key)
        .map_err(|e| e.to_string())?
        .map(|row| {
            let bytes = crypto.unseal(row.value())?;
            decode_durable::<ContentVersion>(&bytes)
        })
        .transpose()?;
    match current {
        Some(current) => {
            if change.content_version.previous_digest.as_deref() != Some(current.digest.as_str()) {
                return Err(format!(
                    "STALE_CONTENT_VERSION: object '{}' expected previous digest does not match",
                    change.content_version.object_id
                ));
            }
            if !change
                .content_version
                .source_version
                .advances(&current.source_version)
            {
                return Err(format!(
                    "STALE_CONTENT_VERSION: object '{}' source version did not advance",
                    change.content_version.object_id
                ));
            }
        }
        None if change.content_version.previous_digest.is_some() => {
            return Err(format!(
                "STALE_CONTENT_VERSION: object '{}' has no prior version",
                change.content_version.object_id
            ));
        }
        None => {}
    }
    Ok(())
}

/// Precondition 3: the same chaining rule for the envelope's source cursor.
pub(crate) fn check_change_cursor_precondition(
    write: &ShardWrite<'_>,
    graph_fname: &str,
    tenant: &str,
    cursor: &ChangeCursor,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let cursors = write
        .graph(graph_fname)?
        .open_scoped_table(CHANGE_CURSORS)
        .map_err(|e| e.to_string())?;
    let cursor_key = (
        graph_fname,
        tenant,
        cursor.source.as_str(),
        cursor.partition.as_str(),
    );
    let current = cursors
        .get(cursor_key)
        .map_err(|e| e.to_string())?
        .map(|row| {
            let bytes = crypto.unseal(row.value())?;
            decode_durable::<ChangeCursor>(&bytes)
        })
        .transpose()?;
    match current {
        Some(current) => {
            if cursor.expected_previous.as_ref() != Some(&current.position) {
                return Err(format!(
                    "STALE_CURSOR: source '{}' partition '{}' expected position does not match",
                    cursor.source, cursor.partition
                ));
            }
            if !cursor.position.advances(&current.position) {
                return Err(format!(
                    "STALE_CURSOR: source '{}' partition '{}' did not advance",
                    cursor.source, cursor.partition
                ));
            }
        }
        None if cursor.expected_previous.is_some() => {
            return Err(format!(
                "STALE_CURSOR: source '{}' partition '{}' has no prior position",
                cursor.source, cursor.partition
            ));
        }
        None => {}
    }
    Ok(())
}

/// The three preconditions run in the original order -- envelope idempotency,
/// then content version, then cursor -- so a change that violates more than one
/// still reports the same first violation it did before the split.
pub(crate) fn validate_change_envelope_preconditions(
    write: &ShardWrite<'_>,
    graph_fname: &str,
    tenant: &str,
    change: Option<&ChangeEnvelope>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let Some(change) = change else {
        return Ok(());
    };
    check_change_envelope_not_committed(write, graph_fname, change)?;
    check_change_content_version_precondition(write, graph_fname, tenant, change, crypto)?;
    if let Some(cursor) = &change.cursor {
        check_change_cursor_precondition(write, graph_fname, tenant, cursor, crypto)?;
    }
    Ok(())
}

pub(crate) fn apply_snapshot_state(
    write: &ShardWrite<'_>,
    graph_fname: &str,
    snapshot: &crate::graph::GraphSnapshot,
    batch: &MutationBatch,
    crypto: DurableCrypto<'_>,
    #[cfg(feature = "security")] staged_audit_tail: &mut AuditTailCache,
    audited: bool,
) -> Result<(), String> {
    // Non-`security` builds have no audit rows, so the audit-only inputs are
    // intentionally unused while the shared call shape remains feature-stable.
    #[cfg(not(feature = "security"))]
    let _ = (batch, audited);
    let incoming_nodes = snapshot
        .nodes
        .iter()
        .map(|(node_id, properties)| (node_id.clone(), properties.as_ref().clone()))
        .collect::<Vec<_>>();
    // A snapshot replaces the native WorkItem image.  Validate that a
    // generic restore cannot manufacture an active lease, then purge all
    // private claim state atomically before installing the replacement.
    work_item_capability::validate_snapshot_nodes(&incoming_nodes)?;
    work_item_capability::clear_graph_rows(write, graph_fname)?;
    development_lane::validate_lane_links_in_wtx(write, graph_fname, &incoming_nodes, crypto)?;
    apply_snapshot_graph_rows(write, graph_fname, snapshot, crypto)?;
    apply_snapshot_semantic_row(write, graph_fname, snapshot, crypto)?;
    #[cfg(feature = "security")]
    append_snapshot_audit(write, graph_fname, batch, staged_audit_tail, audited)?;
    Ok(())
}

fn apply_snapshot_graph_rows(
    write: &ShardWrite<'_>,
    graph_fname: &str,
    snapshot: &crate::graph::GraphSnapshot,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let mut nodes = write
        .graph(graph_fname)?
        .open_scoped_table(NODES)
        .map_err(|e| e.to_string())?;
    let mut edges = write
        .graph(graph_fname)?
        .open_scoped_table(EDGES)
        .map_err(|e| e.to_string())?;
    let mut ledger = write
        .graph(graph_fname)?
        .open_scoped_table(LEDGER)
        .map_err(|e| e.to_string())?;
    clear_graph_rows(graph_fname, &mut nodes, &mut edges, &mut ledger)?;
    for (node_id, properties) in &snapshot.nodes {
        insert_snapshot_node(
            &mut nodes,
            graph_fname,
            node_id,
            properties.as_ref(),
            crypto,
        )?;
    }
    for (source, target, properties) in &snapshot.edges {
        insert_snapshot_edge(
            &mut edges,
            graph_fname,
            source,
            target,
            properties.as_ref(),
            crypto,
        )?;
    }
    for (sequence, line) in snapshot.ledger.iter().enumerate() {
        insert_snapshot_ledger(&mut ledger, graph_fname, sequence as u64, line)?;
    }
    drop(nodes);
    drop(edges);
    drop(ledger);
    Ok(())
}

fn insert_snapshot_node(
    nodes: &mut ScopedOwnerTableMut<'_, (&str, &str), &[u8]>,
    graph_fname: &str,
    node_id: &str,
    properties: &[u8],
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let sealed = crypto.seal(properties);
    nodes
        .insert((graph_fname, node_id), sealed.as_ref())
        .map_err(|e| e.to_string())
}

fn insert_snapshot_edge(
    edges: &mut ScopedOwnerTableMut<'_, (&str, &str, &str, u32), &[u8]>,
    graph_fname: &str,
    source: &str,
    target: &str,
    properties: &[u8],
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let ordinal = next_edge_ordinal(edges, graph_fname, source, target)?;
    let sealed = crypto.seal(properties);
    edges
        .insert((graph_fname, source, target, ordinal), sealed.as_ref())
        .map_err(|e| e.to_string())
}

fn insert_snapshot_ledger(
    ledger: &mut ScopedOwnerTableMut<'_, (&str, u64), &str>,
    graph_fname: &str,
    sequence: u64,
    line: &str,
) -> Result<(), String> {
    ledger
        .insert((graph_fname, sequence), line)
        .map_err(|e| e.to_string())
}

fn apply_snapshot_semantic_row(
    write: &ShardWrite<'_>,
    graph_fname: &str,
    snapshot: &crate::graph::GraphSnapshot,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let semantic_bytes =
        rmp_serde::to_vec_named(&snapshot.semantic_store).map_err(|e| e.to_string())?;
    let sealed_semantic = crypto.seal(&semantic_bytes);
    let mut semantic = write
        .graph(graph_fname)?
        .open_scoped_table(SEMANTIC)
        .map_err(|e| e.to_string())?;
    semantic
        .insert(graph_fname, sealed_semantic.as_ref())
        .map_err(|e| e.to_string())
}

#[cfg(feature = "security")]
fn append_snapshot_audit(
    write: &ShardWrite<'_>,
    graph_fname: &str,
    batch: &MutationBatch,
    staged_audit_tail: &mut AuditTailCache,
    audited: bool,
) -> Result<(), String> {
    if !audited {
        return Ok(());
    }
    let mut audit = write
        .graph(graph_fname)?
        .open_scoped_table(AUDIT)
        .map_err(|e| e.to_string())?;
    for operation in &batch.operations {
        append_audit_entry(
            &mut audit,
            staged_audit_tail,
            graph_fname,
            &operation.method,
        )?;
    }
    Ok(())
}

pub(crate) fn apply_row_delta_state(
    write: &ShardWrite<'_>,
    graph_fname: &str,
    delta: &crate::graph_delta::GraphRowDelta,
    batch: &MutationBatch,
    crypto: DurableCrypto<'_>,
    #[cfg(feature = "security")] staged_audit_tail: &mut AuditTailCache,
    audited: bool,
) -> Result<(), String> {
    // Non-`security` builds have no audit rows, so the audit-only inputs are
    // intentionally unused while the shared call shape remains feature-stable.
    #[cfg(not(feature = "security"))]
    let _ = (batch, audited);
    let mut tables = GraphRowTables::open(write.graph(graph_fname)?)?;
    for method in delta.operations() {
        apply_method_rows(graph_fname, method, &mut tables, crypto)?;
    }
    if let Some((_, retain, append)) = delta.ledger_patch() {
        let suffix_keys: Vec<u64> = tables
            .ledger
            .scope_rows()
            .map_err(|error| error.to_string())?
            .map(|row| {
                let (key, _) = row.map_err(|error| error.to_string())?;
                let (row_graph, sequence) = key.value();
                if row_graph != graph_fname {
                    return Err("graph row delta ledger escaped its scope".to_string());
                }
                Ok(sequence)
            })
            .filter_map(|row| match row {
                Ok(sequence) if sequence >= retain => Some(Ok(sequence)),
                Ok(_) => None,
                Err(error) => Some(Err(error)),
            })
            .collect::<Result<_, _>>()?;
        for sequence in suffix_keys {
            tables
                .ledger
                .remove((graph_fname, sequence))
                .map_err(|error| error.to_string())?;
        }
        for (offset, line) in append.iter().enumerate() {
            let sequence = retain
                .checked_add(offset as u64)
                .ok_or_else(|| "graph row delta ledger sequence overflow".to_string())?;
            tables
                .ledger
                .insert((graph_fname, sequence), line.as_str())
                .map_err(|error| error.to_string())?;
        }
    }
    drop(tables);
    development_lane::validate_current_lane_links_in_wtx(write, graph_fname, crypto)?;

    // The delta is an authenticated projection detail. Audit the original
    // opaque operation receipt so sensitive row properties are not copied
    // into audit/status/outbox surfaces -- but only when the ORIGINAL,
    // pre-opaque-wrapped method's policy actually calls for it (`audited`,
    // resolved by the caller before the operation was compiled; see this
    // function's doc comment). `TouchNodes` is the standing example of a
    // state-backed method that is durable but intentionally unaudited: the
    // opaque receipt shape here is identical to an audited method's, so this
    // flag -- not the operation's (rewritten) method -- is what tells the two
    // apart.
    #[cfg(feature = "security")]
    if audited {
        let mut audit = write
            .graph(graph_fname)?
            .open_scoped_table(AUDIT)
            .map_err(|e| e.to_string())?;
        for operation in &batch.operations {
            append_audit_entry(
                &mut audit,
                staged_audit_tail,
                graph_fname,
                &operation.method,
            )?;
        }
    }
    Ok(())
}
