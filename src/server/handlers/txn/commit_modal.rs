//! Private transaction commit_modal implementation.

use super::*;

pub(super) async fn commit_cross_modal(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    caller: Option<&str>,
    coordinator_id: &str,
    txn: GraphTxnState,
    attempt_nonce: Option<Nonce>,
) -> Response {
    match commit_cross_modal_txn_with_nonce(
        state,
        req_id,
        caller,
        coordinator_id,
        txn,
        attempt_nonce,
    )
    .await
    {
        Ok(committed) => Response::ok(req_id, ResultPayload::Bool(committed)),
        Err(e) => Response::err(req_id, e),
    }
}

/// Durable GraphQL cross-modal commit (CONCEPT:EG-KG.query.facade-reconcile-hook).
///
/// GraphQL stages its owner-bound transaction in the process registry, but the
/// commit authority is the same durable parent/child protocol used by native
/// cross-modal transactions. The parent receipt seals the complete staged plan
/// under the verified tenant and idempotency key before the child kernel commit;
/// retries recover that encrypted plan and re-enter the kernel instead of
/// reading a finished row and installing a snapshot outside the kernel.
#[cfg(feature = "graphql")]
enum GraphQlCrossModalPreparation {
    Replayed(bool),
    /// Boxed: the receipt plus staged transaction dwarf a replayed bool, and this
    /// value is built once per GraphQL cross-modal commit, not per row.
    Execute(Box<GraphQlCrossModalExecution>),
}

#[cfg(feature = "graphql")]
struct GraphQlCrossModalExecution {
    receipt: TxnReceipt,
    txn: GraphTxnState,
}

/// Identity for a GraphQL durable cross-modal commit: the request's target
/// graph/core, the in-memory staging registry, the transaction id, and the
/// verified carrier authority. `prepare_graphql_cross_modal` and
/// `commit_graphql_cross_modal` are two phases of the SAME commit and take
/// this exact tuple — bundled once so the two phases can't drift on which
/// fields identify a commit.
#[cfg(feature = "graphql")]
#[derive(Clone, Copy)]
pub(crate) struct GraphQlCrossModalCommit<'a> {
    pub(crate) state: &'a Arc<RwLock<ServerState>>,
    pub(crate) request_id: u64,
    pub(crate) graph_name: &'a str,
    pub(crate) core: &'a crate::graph::GraphCore,
    pub(crate) registry: &'a eg_graphql::CrossModalTxnRegistry,
    pub(crate) txn_id: &'a str,
    pub(crate) authority: &'a CarrierAuthority,
}

#[cfg(feature = "graphql")]
async fn prepare_graphql_cross_modal(
    ctx: &GraphQlCrossModalCommit<'_>,
) -> Result<GraphQlCrossModalPreparation, String> {
    let GraphQlCrossModalCommit {
        state,
        request_id,
        graph_name,
        core,
        registry,
        txn_id,
        authority,
    } = *ctx;
    let persistence = state.read().await.persistence.clone();
    let caller = Some(authority.agent_id());
    let idempotency_key = Some(authority.idempotency_key());
    let expected_tenant = Some(authority.tenant_scope());
    let resumed = resume_txn_receipt(
        persistence.clone(),
        request_id,
        caller,
        txn_id,
        idempotency_key,
        expected_tenant,
        authority.attempt_nonce(),
    )?;
    let (receipt, txn) = match resumed {
        Some((_receipt, Some(result), None)) => {
            return match result {
                ResultPayload::Bool(value) => Ok(GraphQlCrossModalPreparation::Replayed(value)),
                _ => Err("GraphQL cross-modal parent result has the wrong type".to_string()),
            };
        }
        Some((receipt, None, Some(txn))) => (receipt, txn),
        Some((_receipt, Some(_), Some(_))) | Some((_receipt, None, None)) => {
            return Err("GraphQL cross-modal parent receipt is inconsistent".to_string());
        }
        None => {
            let staged = registry
                .take(authority.owner_scope(), txn_id)
                .ok_or_else(|| format!("unknown transaction '{txn_id}'"))?;
            let txn = build_graphql_txn(core, graph_name, authority, staged);

            let (receipt, replayed) = begin_txn_receipt(
                persistence,
                request_id,
                caller,
                txn_id,
                &txn,
                idempotency_key,
                authority.attempt_nonce(),
            )?;
            if let Some(result) = replayed {
                return match result {
                    ResultPayload::Bool(value) => Ok(GraphQlCrossModalPreparation::Replayed(value)),
                    _ => Err("GraphQL cross-modal parent result has the wrong type".to_string()),
                };
            }
            (receipt, txn)
        }
    };
    Ok(GraphQlCrossModalPreparation::Execute(Box::new(
        GraphQlCrossModalExecution { receipt, txn },
    )))
}

#[cfg(feature = "graphql")]
pub(crate) async fn commit_graphql_cross_modal(
    ctx: GraphQlCrossModalCommit<'_>,
) -> Result<bool, String> {
    let GraphQlCrossModalCommit {
        state,
        request_id,
        authority,
        ..
    } = ctx;
    let preparation = prepare_graphql_cross_modal(&ctx).await?;
    let (receipt, txn) = match preparation {
        GraphQlCrossModalPreparation::Replayed(value) => return Ok(value),
        GraphQlCrossModalPreparation::Execute(execution) => (execution.receipt, execution.txn),
    };
    let coordinator_id = receipt_coordinator_id(&receipt);
    let committed = commit_cross_modal_txn_with_nonce(
        state,
        request_id,
        Some(authority.agent_id()),
        &coordinator_id,
        txn,
        authority.attempt_nonce(),
    )
    .await?;
    match finish_txn_receipt(receipt, ResultPayload::Bool(committed))? {
        ResultPayload::Bool(value) => Ok(value),
        _ => Err("GraphQL cross-modal parent result has the wrong type".to_string()),
    }
}

/// Mirror durable blob-ref properties onto the in-memory node for every
/// staged `(node_id, digest)` pair, once the durable cross-modal commit has
/// already landed the `__blob__` property on disk. Best-effort: any failure
/// to decode/re-encode a node's properties just leaves RAM momentarily behind
/// the durable row rather than failing the whole commit that already succeeded.
pub(super) fn mirror_blob_refs_into_ram(
    core: &crate::graph::GraphCore,
    blob_refs: &[(String, String)],
) {
    for (node_id, digest) in blob_refs {
        if let Some(blob) = core.get_node_properties(node_id) {
            if let Ok(mut props) = decode_txn_object(&blob) {
                props.insert(
                    "__blob__".to_string(),
                    serde_json::Value::String(digest.clone()),
                );
                if let Ok(updated) = rmp_serde::to_vec_named(&props) {
                    core.add_node(node_id.clone(), updated);
                }
            }
        }
    }
}

/// The reusable core of the cross-modal commit (CONCEPT:EG-KG.txn.reader-never-sees-node + EG-360/361/362),
/// factored out of [`commit_cross_modal`] so BOTH the RPC `Method::Commit` handler AND
/// the pgwire cross-modal txn seam (CONCEPT:EG-KG.txn.isolation-ryow-begin-set) drive the IDENTICAL commit — no
/// logic duplicated across the RPC + wire surfaces. Returns `Ok(true)` on commit,
/// `Ok(false)` on an OCC conflict (true rollback), `Err(msg)` on an ACL denial or a
/// durable-commit failure.
///
/// All modalities, MutationBatch status/result, OCC/fence, idempotency, and outbox
/// land in one durable transaction before the in-memory projection is published. A
/// missing backend or commit failure applies nothing.
pub(crate) async fn commit_cross_modal_txn(
    state: &Arc<RwLock<ServerState>>,
    request_id: u64,
    caller: Option<&str>,
    coordinator_id: &str,
    txn: GraphTxnState,
) -> Result<bool, String> {
    commit_cross_modal_txn_with_nonce(state, request_id, caller, coordinator_id, txn, None).await
}

async fn resolve_cross_modal_target(
    state: &Arc<RwLock<ServerState>>,
    caller: Option<&str>,
    txn: &GraphTxnState,
) -> Result<
    (
        Arc<crate::graph::GraphCore>,
        Option<Arc<dyn crate::server::persistence::PersistenceBackend>>,
    ),
    String,
> {
    let (core, persistence, graph_type, owner) = {
        let s = state.read().await;
        let entry = s
            .registry
            .get(&txn.graph)
            .ok_or_else(|| format!("Graph '{}' not found", txn.graph))?;
        (
            entry.core.clone(),
            s.persistence.clone(),
            entry.graph_type,
            entry.owner.clone(),
        )
    };
    if !consensus_apply_is_authorized() {
        let s = state.read().await;
        check_graph_access(
            &s.isolation,
            caller,
            &txn.graph,
            graph_type,
            owner.as_deref(),
            AccessLevel::Write,
        )?;
    }
    Ok((core, persistence))
}

pub(crate) async fn commit_cross_modal_txn_with_nonce(
    state: &Arc<RwLock<ServerState>>,
    request_id: u64,
    caller: Option<&str>,
    coordinator_id: &str,
    txn: GraphTxnState,
    attempt_nonce: Option<Nonce>,
) -> Result<bool, String> {
    let (core, persistence) = resolve_cross_modal_target(state, caller, &txn).await?;

    let _mutation_guard = crate::server::mutation_batch::lock_graph(&txn.graph).await;
    {
        let gtxn = core.txn();
        let valid = txn.validate(&core);
        drop(gtxn);
        if !valid {
            return Ok(false);
        }
    }

    let fname = crate::persist::sanitize(&txn.graph);
    let (methods, measurements) = build_cross_modal_plan(&txn)?;

    let Some(authority) = persistence.as_ref() else {
        return Err("cross-modal mutation requires durable persistence".to_string());
    };
    if let Some(committed) = commit_cross_modal_durable(CrossModalDurableArgs {
        state,
        core: &core,
        authority: authority.as_ref(),
        fname: &fname,
        coordinator_id,
        request_id,
        caller,
        attempt_nonce,
        txn: &txn,
        methods: &methods,
        measurements: &measurements,
    })
    .await?
    {
        return Ok(committed);
    }

    {
        let mut gtxn = core.txn();
        for method in &methods {
            apply_staged(&mut gtxn, method);
        }
    }
    mirror_blob_refs_into_ram(&core, &txn.blob_refs);
    {
        let mut store = core.semantic_store.write();
        for (node_id, embedding) in &txn.vectors {
            store
                .add_embedding(node_id.clone(), embedding.clone())
                .map_err(|error| error.to_string())?;
        }
    }
    #[cfg(feature = "tsdb")]
    project_committed_measurements(state, &measurements).await?;
    core.mark_dirty();
    Ok(true)
}

struct CrossModalDurableArgs<'a> {
    state: &'a Arc<RwLock<ServerState>>,
    core: &'a Arc<crate::graph::GraphCore>,
    authority: &'a dyn crate::server::persistence::PersistenceBackend,
    fname: &'a str,
    coordinator_id: &'a str,
    request_id: u64,
    caller: Option<&'a str>,
    attempt_nonce: Option<Nonce>,
    txn: &'a GraphTxnState,
    methods: &'a [Method],
    measurements: &'a [crate::MeasurementBatch],
}

async fn compile_cross_modal_batch(
    args: &CrossModalDurableArgs<'_>,
) -> Result<(crate::mutation_batch::MutationBatch, Vec<u8>), String> {
    let batch_id = crate::server::mutation_batch::opaque_coordinator_key(
        "crossmodal",
        &args.txn.graph,
        args.coordinator_id,
    );
    let authoritative_version = args
        .authority
        .read_mutation_graph_version(args.fname)
        .await?
        .unwrap_or(args.txn.begin_version);
    let modality_payload = rmp_serde::to_vec_named(&(
        args.methods,
        &args.txn.vectors,
        &args.txn.blob_refs,
        args.measurements,
    ))
    .map_err(|error| format!("cross-modal manifest encode failed: {error}"))?;
    let principal = txn_receipt_principal(args.caller)?;
    let batch = crate::server::mutation_batch::compile_crossmodal(
        crate::server::mutation_batch::CompileBatch {
            batch_id: &batch_id,
            request_id: args.request_id,
            attempt_nonce: args.attempt_nonce,
            principal: Some(&principal),
            tenant: &args.txn.tenant_scope,
            graph: &args.txn.graph,
            placement_epoch: 0,
            idempotency_key: &batch_id,
            expected_graph_version: Some(authoritative_version),
            fencing_token: None,
            created_at_ms: now_ms(),
            default_surface: crate::mutation_batch::MutationSurface::Transaction,
            authoritative_state: None,
        },
        &modality_payload,
        args.methods.len(),
        args.txn.vectors.len(),
        args.txn.blob_refs.len(),
        args.measurements.len(),
    )?;
    let result = rmp_serde::to_vec_named(&ResultPayload::Bool(true))
        .map_err(|error| format!("cross-modal result encode failed: {error}"))?;
    Ok((batch, result))
}

async fn commit_cross_modal_durable(
    args: CrossModalDurableArgs<'_>,
) -> Result<Option<bool>, String> {
    let (batch, result) = compile_cross_modal_batch(&args).await?;
    let committed = args
        .authority
        .commit_mutation_batch_crossmodal(crate::server::persistence::CrossModalCommitArgs {
            graph_fname: args.fname,
            batch: &batch,
            methods: args.methods,
            vectors: &args.txn.vectors,
            blob_refs: &args.txn.blob_refs,
            measurements: args.measurements,
            result_msgpack: Some(&result),
            committed_at_ms: now_ms(),
        })
        .await
        .map_err(|error| format!("cross-modal MutationBatch commit failed: {error}"))?;
    if !committed.replayed {
        return Ok(None);
    }
    let bytes = committed
        .record
        .result_msgpack
        .as_deref()
        .ok_or_else(|| "committed cross-modal batch has no durable result".to_string())?;
    let stored: ResultPayload = eg_types::msgpack::decode_bounded(
        bytes,
        eg_types::msgpack::MsgpackLimits::new(
            1024 * 1024,
            1_024,
            eg_types::msgpack::DEFAULT_MAX_DEPTH,
        ),
    )
    .map_err(|_| {
        "committed cross-modal result is invalid or exceeds resource limits".to_string()
    })?;
    let committed_bool = match stored {
        ResultPayload::Bool(value) => value,
        _ => return Err("committed cross-modal result has the wrong type".to_string()),
    };
    let (snapshot, version) = args
        .authority
        .read_authoritative_graph_snapshot(args.fname)
        .await?
        .ok_or_else(|| "committed cross-modal graph image is missing".to_string())?;
    args.core.install_committed_snapshot(snapshot, version)?;
    #[cfg(feature = "tsdb")]
    project_committed_measurements(args.state, args.measurements).await?;
    Ok(Some(committed_bool))
}

#[cfg(feature = "tsdb")]
pub(super) async fn project_committed_measurements(
    state: &Arc<RwLock<ServerState>>,
    measurements: &[crate::MeasurementBatch],
) -> Result<(), String> {
    if measurements.is_empty() {
        return Ok(());
    }
    let store =
        state.read().await.tsdb_store.clone().ok_or_else(|| {
            "committed measurements require the served time-series store".to_string()
        })?;
    for (series, n_fields, bucket_ns, field_names, points) in measurements {
        let points = points
            .iter()
            .map(|(ts, values)| eg_tsdb::point::Point {
                ts: *ts,
                values: values.clone(),
            })
            .collect::<Vec<_>>();
        if let Err(error) = store.append_batch(series, *n_fields, *bucket_ns, field_names, &points)
        {
            let _ = store.mark_projection_degraded(series, &error.to_string());
            return Err(format!(
                "committed time-series projection failed for governed series: {error}"
            ));
        }
        let metadata = store
            .meta(series)
            .map_err(|error| format!("committed time-series projection metadata failed: {error}"))?
            .ok_or_else(|| "committed time-series projection metadata is missing".to_string())?;
        store
            .mark_projection_ready(series, &metadata)
            .map_err(|error| format!("committed time-series cursor update failed: {error}"))?;
    }
    Ok(())
}
#[cfg(feature = "graphql")]
fn build_graphql_txn(
    core: &crate::graph::GraphCore,
    graph_name: &str,
    authority: &CarrierAuthority,
    staged: eg_graphql::CrossModalTxn,
) -> GraphTxnState {
    let mut txn = GraphTxnState::new(
        core,
        NewTxnArgs {
            graph: graph_name.to_string(),
            tenant_scope: authority.tenant_scope().to_string(),
            begin_version: core.version(),
            isolation: IsolationLevel::Snapshot,
            predicate: None,
            agent: authority.owner_scope().to_string(),
            now_ms: now_ms(),
        },
    );
    for write in staged.graph_writes() {
        match write {
            eg_graphql::GraphWrite::Node { id, blob } => txn.write_set.push(Method::AddNode {
                node_id: id.clone(),
                properties_msgpack: blob.clone(),
            }),
            eg_graphql::GraphWrite::Edge { from, to, blob } => {
                txn.write_set.push(Method::AddEdge {
                    source_id: from.clone(),
                    target_id: to.clone(),
                    properties_msgpack: blob.clone(),
                })
            }
        }
    }
    for (node_id, embedding) in staged.vectors() {
        txn.stage_vector(core, node_id.clone(), embedding.clone(), now_ms());
    }
    #[cfg(feature = "tsdb")]
    for (series, points) in staged.measurements() {
        let n_fields = points.first().map(|(_, values)| values.len()).unwrap_or(0);
        let field_names = (0..n_fields).map(|i| format!("f{i}")).collect();
        txn.stage_measurement(
            StagedMeasurement {
                series,
                n_fields,
                bucket_ns: DEFAULT_MEASUREMENT_BUCKET_NS,
                field_names,
                points,
            },
            now_ms(),
        );
    }
    txn
}
fn build_cross_modal_plan(
    txn: &GraphTxnState,
) -> Result<(Vec<Method>, Vec<crate::MeasurementBatch>), String> {
    let mut methods: Vec<Method> = txn.write_set.clone();
    methods.extend(txn.axioms.iter().cloned());
    methods.extend(txn.constructs.iter().cloned());
    methods.extend(txn.plan_writeback.iter().cloned());
    let measurements = txn
        .measurements
        .iter()
        .map(|measurement| {
            let batch = measurement.to_batch();
            #[cfg(feature = "tsdb")]
            if eg_tsdb::store::SeriesKey::decode(&batch.0).is_none() {
                return Err("staged time-series key is not canonically scoped".to_string());
            }
            Ok(batch)
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok((methods, measurements))
}
