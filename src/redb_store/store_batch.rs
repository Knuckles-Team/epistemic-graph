use super::store_prelude::*;
use super::*;

/// The compact graph/control path can replace or remove a linked WorkItem
/// just as a snapshot/row-delta can (`clears_semantic`); a lifecycle DeleteGraph
/// additionally purges the PRIOR incarnation's mutation-authority rows before
/// this delete writes its own fresh tombstone record.
pub(crate) fn apply_post_row_cleanup(
    write: &ShardWrite<'_>,
    graph_fname: &str,
    batch: &MutationBatch,
    lifecycle: Option<&(bool, String, Option<GraphType>)>,
    authoritative_state_msgpack_is_none: bool,
) -> Result<(), String> {
    let clears_semantic = authoritative_state_msgpack_is_none
        && (matches!(lifecycle, Some((false, _, _)))
            || batch
                .operations
                .iter()
                .any(|operation| matches!(&operation.method, Method::ClearGraph)));
    if clears_semantic {
        let mut semantic = write
            .graph(graph_fname)?
            .open_scoped_table(SEMANTIC)
            .map_err(|e| e.to_string())?;
        semantic.remove(graph_fname).map_err(|e| e.to_string())?;
    }
    if matches!(lifecycle, Some((false, _, _))) {
        clear_change_material_rows(write, graph_fname)?;
        // D-P0-U04, restored: the deleted generation's mutation authority goes
        // with it, atomically, in this same transaction and BEFORE the delete
        // writes its own receipt.
        //
        // A graph shard's scope identity is derived from the durable graph name
        // alone, so a same-name recreate rebinds the SAME ledger scope key and
        // inherits every replay key, attempt nonce, receipt, outbox row,
        // delivery lease and projection cursor the deleted generation left --
        // the exact collision D-P0-U04 closed. `purge_graph_rows`
        // (`MutationKernel::purge_scope_with`) covers the embedded engine's
        // whole-store teardown, but it opens its OWN transaction and has no
        // production caller on the served path, so a `Method::DeleteGraph`
        // committed as an ordinary batch is not covered by it.
        //
        // The sweep is the KERNEL's, not this payload transaction's: the call
        // below is a kernel entry point that performs it inside the admitted
        // write, so there is one authority rather than two and nothing races a
        // retirement proof. The scope version is deliberately left monotonic --
        // see `purge_scope_ledger_generation`.
        write
            .graph(graph_fname)?
            .purge_replaced_generation_ledger()?;
    }
    Ok(())
}

/// Everything one batch's row phase needs that was resolved before the rows.
pub(crate) struct MutationBatchPlan {
    pub(crate) staged_state: Option<AuthoritativeGraphState>,
    pub(crate) integrity_policy_update: Option<Option<crate::graph::IntegrityPolicy>>,
    pub(crate) lifecycle: Option<(bool, String, Option<GraphType>)>,
}

/// The DOMAIN validation a batch must pass before a single row is touched.
///
/// This is what is left of the old prelude. The idempotency, batch-id
/// uniqueness, OCC and fence checks that used to stand beside these are the
/// kernel's, run inside the transaction by `commit::begin`; a `Replayed`
/// outcome is likewise the kernel's answer, resolved at admission, so this no
/// longer returns one. What remains is what only the domain knows: whether the
/// batch's route and lowering are coherent, whether it is a lifecycle
/// operation, and whether the change envelope's preconditions -- not already
/// committed, content version and cursor as expected -- hold.
pub(crate) fn prepare_and_validate_mutation_batch(
    ctx: &MutationRowCtx<'_>,
    change: Option<&ChangeEnvelope>,
    authoritative_state_msgpack: Option<&[u8]>,
    crossmodal: Option<&CrossModalBatchRows<'_>>,
) -> Result<MutationBatchPlan, String> {
    let MutationRowCtx {
        write,
        graph_fname,
        batch,
        crypto,
    } = *ctx;
    batch.validate_write_budget()?;
    let staged_state = resolve_mutation_authoritative_state(batch, authoritative_state_msgpack)?;
    let integrity_policy_update = resolve_integrity_policy_update(staged_state.as_ref());
    validate_mutation_batch_route_and_lowering(
        batch,
        graph_fname,
        crossmodal,
        staged_state.is_none(),
    )?;
    let lifecycle = detect_and_validate_lifecycle(batch, graph_fname)?;
    // Governance material remains caller-scoped even though the physical
    // mutation ledger is rebound to the shard scope. Preserve the validated
    // ChangeEnvelope mutation tenant for its content/cursor/material keys.
    let change_tenant = change.map_or(batch.identity.tenant().as_str(), |change| {
        change.mutation.identity.tenant().as_str()
    });
    validate_change_envelope_preconditions(write, graph_fname, change_tenant, change, crypto)?;
    Ok(MutationBatchPlan {
        staged_state,
        integrity_policy_update,
        lifecycle,
    })
}

/// A single injected-crashpoint check + the matching certification-fault hook,
/// for the two crashpoints `apply_mutation_batch_in_wtx` itself observes
/// (`BeforeCommit`/`AfterCommitBeforeAck` are the CALLER's, not this
/// function's -- see `commit_mutation_batch_inner`).
pub(crate) fn run_mutation_batch_crashpoint(
    batch: &MutationBatch,
    crashpoint: Option<MutationBatchCrashpoint>,
    at: MutationBatchCrashpoint,
) -> Result<(), String> {
    if crashpoint == Some(at) {
        let message = match at {
            MutationBatchCrashpoint::BeforeRows => "injected crash before mutation rows",
            MutationBatchCrashpoint::AfterRowsBeforeMetadata => {
                "injected crash after mutation rows"
            }
            _ => "injected crash",
        };
        return Err(message.to_string());
    }
    let phase = match at {
        MutationBatchCrashpoint::BeforeRows => {
            crate::mutation_batch::MutationCommitPhase::BeforeRows
        }
        MutationBatchCrashpoint::AfterRowsBeforeMetadata => {
            crate::mutation_batch::MutationCommitPhase::AfterRowsBeforeMetadata
        }
        _ => return Ok(()),
    };
    crate::mutation_batch::apply_certification_fault(batch, phase)
}

/// Dispatch to whichever of the three row-application shapes this batch uses
/// (authoritative Snapshot / authoritative RowDelta / native per-Method rows),
/// returning the native path's `generated_result` (`None` for the other two
/// shapes, exactly as the pre-decomposition code left `generated_result`
/// untouched outside the native/`else` branch).
pub(crate) struct StateDispatchInput<'a, 'txn> {
    pub(crate) write: &'a ShardWrite<'txn>,
    pub(crate) graph_fname: &'a str,
    pub(crate) staged_state: Option<&'a AuthoritativeGraphState>,
    pub(crate) batch: &'a MutationBatch,
    pub(crate) committed_at_ms: u64,
    pub(crate) crypto: DurableCrypto<'txn>,
    #[cfg(feature = "security")]
    pub(crate) staged_audit_tail: &'a mut AuditTailCache,
    pub(crate) audited: bool,
    pub(crate) crossmodal_present: bool,
}

pub(crate) fn apply_state_dispatch_rows(
    input: StateDispatchInput<'_, '_>,
) -> Result<Option<Vec<u8>>, String> {
    let StateDispatchInput {
        write,
        graph_fname,
        staged_state,
        batch,
        committed_at_ms,
        crypto,
        #[cfg(feature = "security")]
        staged_audit_tail,
        audited,
        crossmodal_present,
    } = input;
    let mut generated_result: Option<Vec<u8>> = None;
    match staged_state {
        Some(AuthoritativeGraphState::Snapshot(snapshot)) => {
            apply_snapshot_state(
                write,
                graph_fname,
                snapshot,
                batch,
                crypto,
                #[cfg(feature = "security")]
                staged_audit_tail,
                audited,
            )?;
        }
        Some(AuthoritativeGraphState::RowDelta(delta)) => {
            apply_row_delta_state(
                write,
                graph_fname,
                delta,
                batch,
                crypto,
                #[cfg(feature = "security")]
                staged_audit_tail,
                audited,
            )?;
        }
        None => {
            apply_native_operations(
                &MutationRowCtx {
                    write,
                    graph_fname,
                    batch,
                    crypto,
                },
                NativeOperationOptions {
                    committed_at_ms,
                    #[cfg(feature = "security")]
                    staged_audit_tail,
                    generated_result: &mut generated_result,
                    crossmodal_present,
                },
            )?;
        }
    }
    Ok(generated_result)
}

pub(crate) fn apply_crossmodal_rows_phase(
    write: &ShardWrite<'_>,
    graph_fname: &str,
    crossmodal: Option<&CrossModalBatchRows<'_>>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let Some(rows) = crossmodal else {
        return Ok(());
    };
    if !rows.methods.is_empty() {
        apply_crossmodal_graph_rows(write, graph_fname, rows, crypto)?;
    }
    if rows
        .methods
        .iter()
        .any(|method| matches!(method, Method::ClearGraph))
    {
        clear_crossmodal_semantic_row(write, graph_fname)?;
    }
    apply_crossmodal_projection_rows(
        write,
        graph_fname,
        rows.vectors,
        rows.blob_refs,
        rows.measurements,
        crypto,
    )?;
    // Blob/vector projection is also an in-transaction node/semantic
    // replacement surface.  Re-run the lane policy after it so the final
    // image, not only the pre-projection graph rows, is what can commit.
    development_lane::validate_current_lane_links_in_wtx(write, graph_fname, crypto)
}

fn apply_crossmodal_graph_rows(
    write: &ShardWrite<'_>,
    graph_fname: &str,
    rows: &CrossModalBatchRows<'_>,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let mut tables = GraphRowTables::open(write.graph(graph_fname)?)?;
    let mut resource_tables = ResourceRowTables::open(write, graph_fname)?;
    if rows
        .methods
        .iter()
        .any(|method| matches!(method, Method::ClearGraph | Method::DeleteGraph { .. }))
    {
        clear_resource_rows_with_tables(graph_fname, &mut resource_tables, crypto)?;
        development_lane::clear_native_graph_rows_in_wtx(write, graph_fname, crypto)?;
        capacity_lease::clear_graph_rows(write, graph_fname)?;
        work_item_capability::clear_graph_rows_with_native(
            write,
            graph_fname,
            &mut tables.native_work_items,
        )?;
    }
    for method in rows.methods {
        apply_method_rows(graph_fname, method, &mut tables, crypto)?;
    }
    drop(resource_tables);
    drop(tables);
    development_lane::validate_current_lane_links_in_wtx(write, graph_fname, crypto)
}

fn clear_crossmodal_semantic_row(write: &ShardWrite<'_>, graph_fname: &str) -> Result<(), String> {
    let mut semantic = write
        .graph(graph_fname)?
        .open_scoped_table(SEMANTIC)
        .map_err(|e| e.to_string())?;
    semantic.remove(graph_fname).map_err(|e| e.to_string())?;
    Ok(())
}

pub(crate) fn write_change_envelope_and_content_version_rows(
    write: &ShardWrite<'_>,
    graph_fname: &str,
    tenant: &str,
    change: &ChangeEnvelope,
    committed_at_ms: u64,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let envelope_record = ChangeEnvelopeRecord {
        envelope: change.clone(),
        committed_at_ms,
    };
    let bytes = rmp_serde::to_vec_named(&envelope_record).map_err(|e| e.to_string())?;
    let sealed = crypto.seal(&bytes);
    let mut envelopes = write
        .graph(graph_fname)?
        .open_scoped_table(CHANGE_ENVELOPES)
        .map_err(|e| e.to_string())?;
    envelopes
        .insert((graph_fname, change.envelope_id.as_str()), sealed.as_ref())
        .map_err(|e| e.to_string())?;

    let bytes = rmp_serde::to_vec_named(&change.content_version).map_err(|e| e.to_string())?;
    let sealed = crypto.seal(&bytes);
    let mut versions = write
        .graph(graph_fname)?
        .open_scoped_table(CONTENT_VERSIONS)
        .map_err(|e| e.to_string())?;
    versions
        .insert(
            (
                graph_fname,
                tenant,
                change.content_version.object_id.as_str(),
            ),
            sealed.as_ref(),
        )
        .map_err(|e| e.to_string())?;

    Ok(())
}

pub(crate) fn write_change_cursor_row(
    write: &ShardWrite<'_>,
    graph_fname: &str,
    tenant: &str,
    cursor: &ChangeCursor,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let bytes = rmp_serde::to_vec_named(cursor).map_err(|e| e.to_string())?;
    let sealed = crypto.seal(&bytes);
    let mut cursors = write
        .graph(graph_fname)?
        .open_scoped_table(CHANGE_CURSORS)
        .map_err(|e| e.to_string())?;
    cursors
        .insert(
            (
                graph_fname,
                tenant,
                cursor.source.as_str(),
                cursor.partition.as_str(),
            ),
            sealed.as_ref(),
        )
        .map_err(|e| e.to_string())?;
    Ok(())
}

pub(crate) fn write_change_material_blobs(
    write: &ShardWrite<'_>,
    graph_fname: &str,
    tenant: &str,
    change: &ChangeEnvelope,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let mut blobs = write
        .graph(graph_fname)?
        .open_scoped_table(CHANGE_BLOBS)
        .map_err(|e| e.to_string())?;
    for blob in &change.blobs {
        let key = (graph_fname, tenant, blob.blob_id.as_str());
        match blob.operation {
            MaterialOperation::Upsert => {
                let bytes = rmp_serde::to_vec_named(blob).map_err(|e| e.to_string())?;
                let sealed = crypto.seal(&bytes);
                blobs
                    .insert(key, sealed.as_ref())
                    .map_err(|e| e.to_string())?;
            }
            MaterialOperation::Delete => {
                blobs.remove(key).map_err(|e| e.to_string())?;
            }
        }
    }
    Ok(())
}

pub(crate) fn write_change_material_features(
    write: &ShardWrite<'_>,
    graph_fname: &str,
    tenant: &str,
    change: &ChangeEnvelope,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let mut features = write
        .graph(graph_fname)?
        .open_scoped_table(CHANGE_FEATURES)
        .map_err(|e| e.to_string())?;
    for feature in &change.features {
        let key = (graph_fname, tenant, feature.feature_id.as_str());
        match feature.operation {
            MaterialOperation::Upsert => {
                let bytes = rmp_serde::to_vec_named(feature).map_err(|e| e.to_string())?;
                let sealed = crypto.seal(&bytes);
                features
                    .insert(key, sealed.as_ref())
                    .map_err(|e| e.to_string())?;
            }
            MaterialOperation::Delete => {
                features.remove(key).map_err(|e| e.to_string())?;
            }
        }
    }
    Ok(())
}

pub(crate) fn write_change_material_evidence(
    write: &ShardWrite<'_>,
    graph_fname: &str,
    tenant: &str,
    change: &ChangeEnvelope,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let mut evidence = write
        .graph(graph_fname)?
        .open_scoped_table(CHANGE_EVIDENCE)
        .map_err(|e| e.to_string())?;
    for item in &change.evidence {
        let key = (graph_fname, tenant, item.evidence_id.as_str());
        match item.operation {
            MaterialOperation::Upsert => {
                let bytes = rmp_serde::to_vec_named(item).map_err(|e| e.to_string())?;
                let sealed = crypto.seal(&bytes);
                evidence
                    .insert(key, sealed.as_ref())
                    .map_err(|e| e.to_string())?;
            }
            MaterialOperation::Delete => {
                evidence.remove(key).map_err(|e| e.to_string())?;
            }
        }
    }
    Ok(())
}

pub(crate) fn write_change_material_policies(
    write: &ShardWrite<'_>,
    graph_fname: &str,
    tenant: &str,
    change: &ChangeEnvelope,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let mut policies = write
        .graph(graph_fname)?
        .open_scoped_table(CHANGE_POLICIES)
        .map_err(|e| e.to_string())?;
    for policy in &change.policies {
        let key = (graph_fname, tenant, policy.policy_id.as_str());
        match policy.operation {
            MaterialOperation::Upsert => {
                let bytes = rmp_serde::to_vec_named(policy).map_err(|e| e.to_string())?;
                let sealed = crypto.seal(&bytes);
                policies
                    .insert(key, sealed.as_ref())
                    .map_err(|e| e.to_string())?;
            }
            MaterialOperation::Delete => {
                policies.remove(key).map_err(|e| e.to_string())?;
            }
        }
    }
    Ok(())
}

pub(crate) fn write_change_material_lineage(
    write: &ShardWrite<'_>,
    graph_fname: &str,
    tenant: &str,
    change: &ChangeEnvelope,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let mut lineage = write
        .graph(graph_fname)?
        .open_scoped_table(CHANGE_LINEAGE)
        .map_err(|e| e.to_string())?;
    for item in &change.lineage {
        let key = (graph_fname, tenant, item.lineage_id.as_str());
        match item.operation {
            MaterialOperation::Upsert => {
                let bytes = rmp_serde::to_vec_named(item).map_err(|e| e.to_string())?;
                let sealed = crypto.seal(&bytes);
                lineage
                    .insert(key, sealed.as_ref())
                    .map_err(|e| e.to_string())?;
            }
            MaterialOperation::Delete => {
                lineage.remove(key).map_err(|e| e.to_string())?;
            }
        }
    }
    Ok(())
}

/// The governed change envelope's own rows: the retained envelope, the content
/// version, the source cursor and the five material families.
///
/// The `epistemic.change.committed.v1` OUTBOX event that used to be written
/// here by hand is gone from this function, not from the system: outbox rows
/// are written by `commit::finish` from `batch.outbox`, so the event is
/// compiled onto the batch as an intent (see
/// `server::mutation_batch::compile`). One writer, one ordinal space, one
/// order -- where before an ordinal counter was threaded through four writers
/// and asserted at the end.
pub(crate) fn apply_change_envelope_commit_rows(
    write: &ShardWrite<'_>,
    graph_fname: &str,
    change: &ChangeEnvelope,
    committed_at_ms: u64,
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let tenant = change.mutation.identity.tenant().as_str();
    write_change_envelope_and_content_version_rows(
        write,
        graph_fname,
        tenant,
        change,
        committed_at_ms,
        crypto,
    )?;

    if let Some(cursor) = &change.cursor {
        write_change_cursor_row(write, graph_fname, tenant, cursor, crypto)?;
    }

    write_change_material_blobs(write, graph_fname, tenant, change, crypto)?;
    write_change_material_features(write, graph_fname, tenant, change, crypto)?;
    write_change_material_evidence(write, graph_fname, tenant, change, crypto)?;
    write_change_material_policies(write, graph_fname, tenant, change, crypto)?;
    write_change_material_lineage(write, graph_fname, tenant, change, crypto)
}

pub(crate) fn resolve_default_graph_meta_update(
    meta: &redb::Table<'_, &str, &[u8]>,
    graph_fname: &str,
    batch: &MutationBatch,
    integrity_policy_update: Option<&Option<crate::graph::IntegrityPolicy>>,
) -> Result<Option<Vec<u8>>, String> {
    let existing = meta
        .get(graph_fname)
        .map_err(|e| e.to_string())?
        .map(|value| value.value().to_vec());
    let encoded = match (existing, integrity_policy_update) {
        (Some(existing), Some(policy)) => {
            let record = decode_meta_record(graph_fname, &existing)?;
            Some(encode_meta_record(
                &record.name,
                record.graph_type,
                &record.incarnation_id,
                policy.as_ref(),
            )?)
        }
        (Some(_), None) => None,
        // `mutation_batch_graph_name` answers the batch's SCOPE resource, and by
        // this point `shard::bind_caller_batch` has already rebound the batch to
        // `graph_scope_identity(graph_fname)` -- so that name is the durable KEY,
        // not the caller's logical name. See `catalog_display_name`.
        (None, policy) => Some(encode_meta_record(
            &catalog_display_name(mutation_batch_graph_name(batch)?),
            GraphType::Global,
            &batch.batch_id,
            policy.and_then(Option::as_ref),
        )?),
    };
    Ok(encoded)
}

pub(crate) fn write_mutation_batch_graph_meta_row(
    write: &ShardWrite<'_>,
    graph_fname: &str,
    batch: &MutationBatch,
    lifecycle: Option<(bool, String, Option<GraphType>)>,
    integrity_policy_update: Option<Option<crate::graph::IntegrityPolicy>>,
) -> Result<(), String> {
    let mut meta = write
        .control()
        .open_table(GRAPH_META)
        .map_err(|e| e.to_string())?;
    match lifecycle {
        Some((true, graph_name, Some(graph_type))) => {
            let encoded = encode_meta_record(
                &graph_name,
                graph_type,
                &batch.batch_id,
                integrity_policy_update.as_ref().and_then(Option::as_ref),
            )?;
            meta.insert(graph_fname, encoded.as_slice())
                .map_err(|e| e.to_string())?;
        }
        Some((false, _, _)) => {
            meta.remove(graph_fname).map_err(|e| e.to_string())?;
        }
        _ => {
            let encoded = resolve_default_graph_meta_update(
                &meta,
                graph_fname,
                batch,
                integrity_policy_update.as_ref(),
            )?;
            if let Some(encoded) = encoded {
                meta.insert(graph_fname, encoded.as_slice())
                    .map_err(|e| e.to_string())?;
            }
        }
    }
    Ok(())
}
