//! Self-routing data-plane dispatch groups.

use super::*;

#[cfg(feature = "tsdb")]
use super::data_plane_arms::dispatch_transaction_methods_arm_1;
#[cfg(feature = "owl")]
use super::data_plane_arms::dispatch_transaction_methods_arm_2;
#[cfg(feature = "sparql")]
use super::data_plane_arms::dispatch_transaction_methods_arm_3;
#[cfg(feature = "query")]
use super::data_plane_arms::dispatch_transaction_methods_arm_4;
#[cfg(feature = "epistemic")]
use super::data_plane_arms::dispatch_transaction_methods_arm_5;
use super::data_plane_arms::{
    dispatch_change_envelope_methods_arm_0, dispatch_change_envelope_methods_arm_1,
    dispatch_change_envelope_methods_arm_2, dispatch_change_envelope_methods_arm_3,
    dispatch_change_envelope_methods_arm_4, dispatch_transaction_methods_arm_0,
};

/// Multi-op OCC transactions and their typed sub-operations.
///
/// Hands a method it does not own back as `ControlFlow::Continue`.
pub(super) async fn dispatch_transaction_methods(
    ctx: DispatchCtx<'_>,
    method: Method,
) -> ControlFlow<Response, Method> {
    #[allow(unused_variables)]
    let DispatchCtx {
        state,
        req,
        verified_context,
        ..
    } = ctx;
    ControlFlow::Break(match method {
        // ── Transactions (CONCEPT:EG-KG.txn.multi-op-occ-acid — multi-op OCC ACID) ──────
        // Stateful + self-routing: a Txn* op targets the graph the txn was opened
        // against (resolved from `open_txns`), NOT necessarily `req.graph`, and
        // BeginTxn carries its own graph. So they are handled here (with `state`)
        // BEFORE the graph-op path — never through `dispatch_graph_op`, whose
        // coalescer/registry-lookup assumes a single `req.graph` target. For
        // BeginTxn the request envelope's `graph` is the default target when the
        // body omits one.
        method @ (Method::BeginTxn { .. }
        | Method::TxnAddNode { .. }
        | Method::TxnRemoveNode { .. }
        | Method::TxnAddEdge { .. }
        | Method::TxnRemoveEdge { .. }
        | Method::TxnCas { .. }
        | Method::TxnAddEmbedding { .. }
        | Method::TxnBlobRef { .. }
        | Method::Commit { .. }
        | Method::Rollback { .. }) => dispatch_transaction_methods_arm_0(ctx, method).await,

        // Extended cross-modal STAGING (CONCEPT:EG-KG.compute.eg-187, closing EG-360/361/362 at RPC) — the tsdb-measurement,
        // OWL-axiom and SPARQL-CONSTRUCT stage methods. `handlers::txn::try_handle` handles
        // them (feature-gated), but they carry their OWN `graph` (like `TxnAddEmbedding`),
        // so they route straight there — NO `BeginTxn` graph-default rewrite. Without these
        // arms the variants fell through to the graph-op "not available" catch-all, so an
        // in-txn measurement/axiom/CONSTRUCT staged fine over pgwire (EG-372, which calls the
        // stage fns directly) but ERRORED over the native RPC surface — a "seamless" leak
        // (docs/north_star.md). Each is `cfg`-gated to match its protocol variant, so a slim
        // build without the feature keeps the prior catch-all behavior.
        #[cfg(feature = "tsdb")]
        method @ Method::TxnAddMeasurement { .. } => {
            dispatch_transaction_methods_arm_1(ctx, method).await
        }
        #[cfg(feature = "owl")]
        method @ Method::TxnAxiom { .. } => dispatch_transaction_methods_arm_2(ctx, method).await,
        #[cfg(feature = "sparql")]
        method @ Method::TxnConstruct { .. } => {
            dispatch_transaction_methods_arm_3(ctx, method).await
        }
        // Planner-writeback staging (CONCEPT:EG-KG.query.plan-dag, D7) — carries its OWN
        // `graph` (like `TxnConstruct`), so it routes straight to the txn handler with NO
        // BeginTxn graph-default rewrite. `query`-gated to match its protocol variant.
        #[cfg(feature = "query")]
        method @ Method::TxnPlanWriteback { .. } => {
            dispatch_transaction_methods_arm_4(ctx, method).await
        }
        // Materialize-belief staging (CONCEPT:EG-KG.epistemic.epistemic-substrate, D5) —
        // carries its OWN `graph` (like `TxnPlanWriteback`), so it routes straight to the
        // txn handler with NO BeginTxn graph-default rewrite. `epistemic`-gated to match
        // its protocol variant.
        #[cfg(feature = "epistemic")]
        method @ Method::TxnMaterializeBelief { .. } => {
            dispatch_transaction_methods_arm_5(ctx, method).await
        }
        other => return ControlFlow::Continue(other),
    })
}

/// The non-graph durable stores that ride the same connection: the chunked blob
/// store, the KV namespace and SQLite file import/export.
///
/// Hands a method it does not own back as `ControlFlow::Continue`.
pub(super) async fn dispatch_store_methods(
    ctx: DispatchCtx<'_>,
    method: Method,
) -> ControlFlow<Response, Method> {
    // Every arm of this domain is feature-gated: with none of them compiled
    // in the group owns no method and passes everything through.
    #[cfg(not(any(feature = "blob", feature = "kv", feature = "sqlite-file")))]
    {
        let _ = ctx;
        ControlFlow::Continue(method)
    }
    #[cfg(any(feature = "blob", feature = "kv", feature = "sqlite-file"))]
    {
        #[allow(unused_variables)]
        let DispatchCtx {
            state,
            req,
            verified_context,
            ..
        } = ctx;
        ControlFlow::Break(match method {
            // ── Blob (CONCEPT:EG-KG.storage.blob-namespace) ──────────────────────────────────
            // Content-addressed, NOT graph-scoped: a blob is keyed by digest and may be
            // referenced across graphs, so route at the top level (like txn) before the
            // per-graph chain. The variants only exist with the `blob` feature; without
            // it they aren't in the enum and a slim build can't reach this arm.
            #[cfg(feature = "blob")]
            method @ (Method::BlobBegin { .. }
            | Method::BlobChunkPut { .. }
            | Method::BlobCommit { .. }
            | Method::BlobFetchBegin { .. }
            | Method::BlobChunkGet { .. }
            | Method::BlobFetchEnd { .. }
            | Method::BlobRef { .. }
            | Method::BlobUnref { .. }
            | Method::BlobGc) => dispatch_store_methods_arm_0(ctx, method).await,

            // ── Key→Value (CONCEPT:EG-KG.storage.namespaced-kv-surface) ───────────────────────────────
            // Namespaced KV, NOT graph-scoped: a pair is keyed by (namespace, key) and
            // lives off the node/edge graph, so route at the top level (like blob/txn)
            // before the per-graph chain. The variants only exist with the `kv` feature;
            // without it they aren't in the enum and a slim build can't reach this arm.
            #[cfg(feature = "kv")]
            method @ (Method::KvGet { .. }
            | Method::KvPut { .. }
            | Method::KvDelete { .. }
            | Method::KvScan { .. }
            | Method::KvCas { .. }) => dispatch_store_methods_arm_1(ctx, method).await,

            // ── SQLite `.db` file import/export (CONCEPT:EG-KG.query.eg-feature/EG-332) ──
            // File-scoped, NOT graph-scoped: both ops target a filesystem `path` and move
            // rows through the verified caller's owner-scoped user-table store (behind `query`), so they
            // self-route here (like the Blob*/Kv* ops) BEFORE the per-graph chain. Gated
            // `sqlite-file` (which pulls the bundled C sqlite kept OUT of pi); a build
            // without it never has the variants in the enum, so this arm can't be reached.
            #[cfg(feature = "sqlite-file")]
            method @ (Method::ImportSqliteFile { .. } | Method::ExportSqliteFile { .. }) => {
                dispatch_store_methods_arm_2(ctx, method).await
            }
            other => return ControlFlow::Continue(other),
        })
    }
}

/// Typed SQL source batches self-route before the graph chain: they append
/// exact typed rows through the tenant's SQL owner, not a graph.
#[cfg(feature = "query")]
pub(super) async fn dispatch_sql_source_methods(
    ctx: DispatchCtx<'_>,
    method: Method,
) -> ControlFlow<Response, Method> {
    ControlFlow::Break(match method {
        Method::SqlSourceBatch { batch } => {
            dispatch_boxed(handlers::source_batch::handle(
                ctx.state,
                ctx.req.id,
                ctx.verified_context,
                batch,
            ))
            .await
        }
        other => return ControlFlow::Continue(other),
    })
}

#[cfg(feature = "blob")]
async fn dispatch_store_methods_arm_0(ctx: DispatchCtx<'_>, method: Method) -> Response {
    #[allow(unused_variables)]
    let DispatchCtx {
        state,
        req,
        verified_context,
        state_machine_authorized,
        identity_bootstrap,
    } = ctx;
    match method {
        method @ (Method::BlobBegin { .. }
        | Method::BlobChunkPut { .. }
        | Method::BlobCommit { .. }
        | Method::BlobFetchBegin { .. }
        | Method::BlobChunkGet { .. }
        | Method::BlobFetchEnd { .. }
        | Method::BlobRef { .. }
        | Method::BlobUnref { .. }
        | Method::BlobGc) => {
            dispatch_boxed(async {
                let req_id = req.id;
                {
                    let carrier = match CarrierAuthority::from_verified(verified_context) {
                        Ok(authority) => authority,
                        Err(denied) => return Response::err(req_id, denied),
                    };
                    match handlers::blob::try_handle(
                        state,
                        req_id,
                        &carrier,
                        verified_context.attempt_nonce(),
                        method,
                    )
                    .await
                    {
                        Ok(resp) => resp,
                        // Unreachable: every variant matched above is a blob method.
                        Err(_) => Response::err(req_id, "blob dispatch routing error"),
                    }
                }
            })
            .await
        }
        _ => Response::err(req.id, "router dispatch helper routing mismatch"),
    }
}

#[cfg(feature = "kv")]
async fn dispatch_store_methods_arm_1(ctx: DispatchCtx<'_>, method: Method) -> Response {
    #[allow(unused_variables)]
    let DispatchCtx {
        state,
        req,
        verified_context,
        state_machine_authorized,
        identity_bootstrap,
    } = ctx;
    match method {
        method @ (Method::KvGet { .. }
        | Method::KvPut { .. }
        | Method::KvDelete { .. }
        | Method::KvScan { .. }
        | Method::KvCas { .. }) => {
            dispatch_boxed(async {
                let req_id = req.id;
                {
                    let carrier = match CarrierAuthority::from_verified(verified_context) {
                        Ok(authority) => authority,
                        Err(denied) => return Response::err(req_id, denied),
                    };
                    match crate::server::kv::try_handle(state, req_id, &carrier, method).await {
                        Ok(resp) => resp,
                        // Unreachable: every variant matched above is a kv method.
                        Err(_) => Response::err(req_id, "kv dispatch routing error"),
                    }
                }
            })
            .await
        }
        _ => Response::err(req.id, "router dispatch helper routing mismatch"),
    }
}

#[cfg(feature = "sqlite-file")]
async fn dispatch_store_methods_arm_2(ctx: DispatchCtx<'_>, method: Method) -> Response {
    #[allow(unused_variables)]
    let DispatchCtx {
        state,
        req,
        verified_context,
        state_machine_authorized,
        identity_bootstrap,
    } = ctx;
    match method {
        method @ (Method::ImportSqliteFile { .. } | Method::ExportSqliteFile { .. }) => {
            dispatch_boxed(async {
                let req_id = req.id;
                {
                    let carrier = match CarrierAuthority::from_verified(verified_context) {
                        Ok(authority) => authority,
                        Err(denied) => return Response::err(req_id, denied),
                    };
                    match handlers::sqlite_file::try_handle(
                        state,
                        req_id,
                        &carrier,
                        verified_context.attempt_nonce(),
                        method,
                    )
                    .await
                    {
                        Ok(resp) => resp,
                        // Unreachable: both variants matched above are sqlite-file methods.
                        Err(_) => Response::err(req_id, "sqlite-file dispatch routing error"),
                    }
                }
            })
            .await
        }
        _ => Response::err(req.id, "router dispatch helper routing mismatch"),
    }
}

/// The reactive subscription plane: CDC tailing, continuous queries, watches,
/// triggers and live CEP standing queries.
///
/// Hands a method it does not own back as `ControlFlow::Continue`.
pub(super) async fn dispatch_streaming_methods(
    ctx: DispatchCtx<'_>,
    method: Method,
) -> ControlFlow<Response, Method> {
    // Every arm of this domain is feature-gated: with none of them compiled
    // in the group owns no method and passes everything through.
    #[cfg(not(feature = "streaming"))]
    {
        let _ = ctx;
        ControlFlow::Continue(method)
    }
    #[cfg(feature = "streaming")]
    {
        #[allow(unused_variables)]
        let DispatchCtx {
            state,
            req,
            verified_context,
            ..
        } = ctx;
        ControlFlow::Break(match method {
            // ── Streaming / CDC / subscriptions (CONCEPT:EG-KG.query.streaming-cdc-subscriptions/230) ───
            // The reactive READ + REGISTER surface over the CDC hub on `state` (the WRITE
            // side — emitting changes — lives in the dispatch_graph_op write-side-effect
            // block). These are NOT graph-mutating (CdcRead/Watch/FiredTriggers tail a
            // cursor; Register*/Drop* manage hub registrations), so they self-route here
            // BEFORE the per-graph chain, like tsdb/blob. Gated `streaming`: in a slim
            // build the arm is absent and the variants fall to the graph_ops not-built
            // catch-all (never a panic, never a mis-route).
            #[cfg(feature = "streaming")]
            method @ (Method::CdcRead { .. }
            | Method::RegisterContinuousQuery { .. }
            | Method::ReadContinuousQuery { .. }
            | Method::DropContinuousQuery { .. }
            | Method::Watch { .. }
            | Method::RegisterTrigger { .. }
            | Method::DropTrigger { .. }
            | Method::ListTriggers { .. }
            | Method::FiredTriggers { .. }) => dispatch_streaming_methods_arm_0(ctx, method).await,

            // ── Live CEP standing queries (CONCEPT:EG-KG.query.protocol-types) ───────────────
            // The PUSH half of the event-stream + CEP modality: register a CEP pattern once
            // (CepSubscribe), then long-poll the matches it detects as CDC changes flow
            // (CepPoll). The engine is fed by the CDC hub (the write side lives in the
            // dispatch write-side-effect block via `CepSurface::feed_change`); this is the
            // register + poll surface over it. NOT graph-mutating, so it self-routes here
            // BEFORE the per-graph chain (like the streaming/tsdb/blob surfaces). Gated
            // `all(streaming, stream)`: the CDC feed AND the live NFA engine. A build missing
            // either (e.g. `pi` — streaming, no stream) omits this arm; the `Cep*` variants
            // (gated `streaming`) then fall to the graph_ops not-available catch-all.
            #[cfg(all(feature = "streaming", feature = "stream"))]
            method @ (Method::CepSubscribe { .. }
            | Method::CepPoll { .. }
            | Method::CepUnsubscribe { .. }) => dispatch_streaming_methods_arm_1(ctx, method).await,
            other => return ControlFlow::Continue(other),
        })
    }
}

#[cfg(feature = "streaming")]
async fn dispatch_streaming_methods_arm_0(ctx: DispatchCtx<'_>, method: Method) -> Response {
    #[allow(unused_variables)]
    let DispatchCtx {
        state,
        req,
        verified_context,
        state_machine_authorized,
        identity_bootstrap,
    } = ctx;
    match method {
        method @ (Method::CdcRead { .. }
        | Method::RegisterContinuousQuery { .. }
        | Method::ReadContinuousQuery { .. }
        | Method::DropContinuousQuery { .. }
        | Method::Watch { .. }
        | Method::RegisterTrigger { .. }
        | Method::DropTrigger { .. }
        | Method::ListTriggers { .. }
        | Method::FiredTriggers { .. }) => {
            dispatch_boxed(async {
                let req_id = req.id;
                {
                    let carrier = match CarrierAuthority::from_verified(verified_context) {
                        Ok(authority) => authority,
                        Err(denied) => return Response::err(req_id, denied),
                    };
                    let read_authority = {
                        let s = timed_read(state).await;
                        match GraphReadAuthority::from_verified(verified_context, &s.isolation) {
                            Ok(authority) => authority,
                            Err(denied) => return Response::err(req_id, denied),
                        }
                    };
                    match handlers::streaming::try_handle(
                        state,
                        req_id,
                        &carrier,
                        &read_authority,
                        method,
                    )
                    .await
                    {
                        Ok(resp) => resp,
                        // Unreachable: every variant matched above is a streaming method.
                        Err(_) => Response::err(req_id, "streaming dispatch routing error"),
                    }
                }
            })
            .await
        }
        _ => Response::err(req.id, "router dispatch helper routing mismatch"),
    }
}

#[cfg(all(feature = "streaming", feature = "stream"))]
async fn dispatch_streaming_methods_arm_1(ctx: DispatchCtx<'_>, method: Method) -> Response {
    #[allow(unused_variables)]
    let DispatchCtx {
        state,
        req,
        verified_context,
        state_machine_authorized,
        identity_bootstrap,
    } = ctx;
    match method {
        method @ (Method::CepSubscribe { .. }
        | Method::CepPoll { .. }
        | Method::CepUnsubscribe { .. }) => {
            dispatch_boxed(async {
                let req_id = req.id;
                {
                    let carrier = match CarrierAuthority::from_verified(verified_context) {
                        Ok(authority) => authority,
                        Err(denied) => return Response::err(req_id, denied),
                    };
                    match crate::server::cep::try_handle(state, req_id, &carrier, method).await {
                        Ok(resp) => resp,
                        // Unreachable: every variant matched above is a CEP method.
                        Err(_) => Response::err(req_id, "cep dispatch routing error"),
                    }
                }
            })
            .await
        }
        _ => Response::err(req.id, "router dispatch helper routing mismatch"),
    }
}

/// The two governed stream WRITE surfaces — served-modality results and the
/// knowledge batch stream. Both go through an `authorize_and_route_*` admission
/// step before reaching the target graph, which is what separates them from the
/// read-side subscription plane above.
///
/// Hands a method it does not own back as `ControlFlow::Continue`.
pub(super) async fn dispatch_governed_stream_write_methods(
    ctx: DispatchCtx<'_>,
    method: Method,
) -> ControlFlow<Response, Method> {
    // Every arm of this domain is feature-gated: with none of them compiled
    // in the group owns no method and passes everything through.
    #[cfg(not(any(feature = "modality-serving", feature = "knowledge-batch")))]
    {
        let _ = ctx;
        ControlFlow::Continue(method)
    }
    #[cfg(any(feature = "modality-serving", feature = "knowledge-batch"))]
    {
        ControlFlow::Break(match method {
            #[cfg(feature = "modality-serving")]
            method @ Method::ServedModality { .. } => {
                dispatch_served_modality_method(ctx, method).await
            }
            #[cfg(feature = "knowledge-batch")]
            method @ Method::KnowledgeStream { .. } => {
                dispatch_knowledge_stream_method(ctx, method).await
            }
            other => return ControlFlow::Continue(other),
        })
    }
}

#[cfg(feature = "modality-serving")]
async fn dispatch_served_modality_method(ctx: DispatchCtx<'_>, method: Method) -> Response {
    #[allow(unused_variables)]
    let DispatchCtx {
        state,
        req,
        verified_context,
        state_machine_authorized,
        identity_bootstrap,
    } = ctx;
    match method {
        Method::ServedModality { op } => {
            dispatch_boxed(async {
                let req_id = req.id;
                let req_agent_id = req.agent_id.clone();
                let req_graph = req.graph.clone();
                {
                    let auth_secret = timed_read(state).await.auth_secret.clone();
                    let authority = match handlers::modality::ModalityAuthority::from_verified(
                        &auth_secret,
                        verified_context.claims(),
                    ) {
                        Ok(authority) => authority,
                        Err(error) => return Response::err(req_id, error),
                    };
                    dispatch_served_modality(
                        state,
                        &req_graph,
                        req_id,
                        req_agent_id.as_deref(),
                        verified_context,
                        op,
                        authority,
                    )
                    .await
                }
            })
            .await
        }
        _ => Response::err(req.id, "router dispatch helper routing mismatch"),
    }
}

#[cfg(feature = "knowledge-batch")]
async fn dispatch_knowledge_stream_method(ctx: DispatchCtx<'_>, method: Method) -> Response {
    #[allow(unused_variables)]
    let DispatchCtx {
        state,
        req,
        verified_context,
        state_machine_authorized,
        identity_bootstrap,
    } = ctx;
    match method {
        Method::KnowledgeStream { request } => {
            dispatch_boxed(async {
    let req_id = req.id;
    let req_agent_id = req.agent_id.clone();
    let req_graph = req.graph.clone();
    {
            let (auth_secret, isolation) = {
                let s = timed_read(state).await;
                (s.auth_secret.clone(), s.isolation.clone())
            };
            // §3's critical finding: as shipped, this was the ONLY production
            // constructor site for a `KnowledgeStreamAuthority`, and it called
            // the lease-less `from_verified` — meaning `authority.policy_lease`
            // was always `None` and every request was unconditionally denied
            // by `validate_request_binding` (mod.rs). Mint the durable lease
            // here and bind it, per GRAPH-POLICY-LEASE-CONTRACT.md §3/§7.
            #[cfg(feature = "security")]
            let authority = match (|| {
                let mint_auth = crate::isolation::MintAuthorization::compute_mac(
                    &auth_secret,
                    verified_context.claims(),
                )
                .and_then(|mac| {
                    crate::isolation::MintAuthorization::new(
                        &auth_secret,
                        verified_context.claims(),
                        &mac,
                    )
                })
                .map_err(|_| {
                    Response::err(req_id, "KnowledgeStream policy authority is unavailable")
                })?;
                let carrier = CarrierAuthority::from_verified(verified_context)
                    .map_err(|denied| Response::err(req_id, denied))?;
                let lease = isolation
                    .mint_policy_decision_lease(
                        &mint_auth,
                        &req_graph,
                        crate::isolation::AccessLevel::Read,
                    )
                    .map(std::sync::Arc::new)
                    .map_err(|error| match error {
                        crate::isolation::MintLeaseError::StoreUnavailable
                        | crate::isolation::MintLeaseError::StoreUnreadable => {
                            Response::err(req_id, "KnowledgeStream policy authority is unavailable")
                        }
                        crate::isolation::MintLeaseError::UnknownActor
                        | crate::isolation::MintLeaseError::AccessDenied => {
                            crate::metrics::access_denied();
                            Response::err(req_id, "ACCESS_DENIED")
                        }
                    })?;
                handlers::knowledge_stream::KnowledgeStreamAuthority::from_verified_with_lease(
                    &auth_secret,
                    verified_context.claims(),
                    &req_graph,
                    &carrier,
                    lease,
                    isolation.policy_store().ok_or_else(|| {
                        Response::err(req_id, "KnowledgeStream policy authority is unavailable")
                    })?,
                )
                .map_err(|error| Response::err(req_id, error))
            })() {
                Ok(authority) => authority,
                Err(response) => return response,
            };
            #[cfg(not(feature = "security"))]
            let authority = {
                let _ = &isolation;
                match handlers::knowledge_stream::KnowledgeStreamAuthority::from_verified(
                    &auth_secret,
                    verified_context.claims(),
                ) {
                    Ok(authority) => authority,
                    Err(error) => return Response::err(req_id, error),
                }
            };
            dispatch_knowledge_stream(
                state,
                &req_graph,
                req_id,
                req_agent_id.as_deref(),
                verified_context,
                request,
                authority,
            )
            .await
        }
})
                .await
        }
        _ => Response::err(req.id, "router dispatch helper routing mismatch"),
    }
}

/// Change-envelope replication and content versioning: apply one or many
/// envelopes, read one back, and read the content version / change cursor.
/// `GetChangeCursor` belongs HERE — the pre-domain cut had it alone in a
/// "query and batch" group with two unrelated methods.
///
/// Hands a method it does not own back as `ControlFlow::Continue`.
pub(super) async fn dispatch_change_envelope_methods(
    ctx: DispatchCtx<'_>,
    method: Method,
) -> ControlFlow<Response, Method> {
    #[allow(unused_variables)]
    let DispatchCtx {
        state,
        req,
        verified_context,
        ..
    } = ctx;
    ControlFlow::Break(match method {
        // ── Graph operations (dispatch to target graph) ──────────────
        method @ Method::ApplyChangeEnvelope { .. } => {
            dispatch_change_envelope_methods_arm_0(ctx, method).await
        }
        method @ Method::ApplyChangeEnvelopes { .. } => {
            dispatch_change_envelope_methods_arm_1(ctx, method).await
        }
        method @ Method::GetChangeEnvelope { .. } => {
            dispatch_change_envelope_methods_arm_2(ctx, method).await
        }
        method @ Method::GetContentVersion { .. } => {
            dispatch_change_envelope_methods_arm_3(ctx, method).await
        }
        method @ Method::GetChangeCursor { .. } => {
            dispatch_change_envelope_methods_arm_4(ctx, method).await
        }
        other => return ControlFlow::Continue(other),
    })
}

/// Methods whose graph target rides the METHOD BODY rather than the request
/// envelope: distributed OWL reasoning over a union of graphs, the
/// natural-language query facade (the `/nl` HTTP path has no envelope), and the
/// cross-graph batch write. They self-route here because `dispatch_graph_op`
/// assumes a single `req.graph`.
///
/// Hands a method it does not own back as `ControlFlow::Continue`.
pub(super) async fn dispatch_method_scoped_graph_methods(
    ctx: DispatchCtx<'_>,
    method: Method,
) -> ControlFlow<Response, Method> {
    #[allow(unused_variables)]
    let DispatchCtx {
        state,
        req,
        verified_context,
        ..
    } = ctx;
    ControlFlow::Break(match method {
        // ── Distributed OWL reasoning (CONCEPT:EG-KG.ontology.concept-13) ─────────────
        // Cross-shard: reasons over the UNION of several graphs, so it self-routes
        // here (with `state` to gather each shard's snapshot) BEFORE the per-graph
        // chain — never through `dispatch_graph_op`, which targets a single `req.graph`.
        // Gated `owl`: in a build without it the variant isn't in the enum.
        #[cfg(feature = "owl")]
        method @ Method::OwlReasonDistributed { .. } => {
            dispatch_boxed(async {
                let req_id = req.id;
                let verified_context = // `verified_context` is a `&VerifiedRequestContext` here; spell the
                // clone out so it cannot be read as cloning the reference.
                VerifiedRequestContext::clone(verified_context);
                {
                    let read_authority = {
                        let s = timed_read(state).await;
                        match GraphReadAuthority::from_verified(&verified_context, &s.isolation) {
                            Ok(authority) => authority,
                            Err(denied) => return Response::err(req_id, denied),
                        }
                    };
                    match handlers::rdf::try_handle_distributed(
                        state,
                        req_id,
                        &read_authority,
                        method,
                    )
                    .await
                    {
                        Ok(resp) => resp,
                        // Unreachable: the only variant routed here is OwlReasonDistributed.
                        Err(_) => Response::err(req_id, "owl distributed dispatch routing error"),
                    }
                }
            })
            .await
        }
        // Natural-language query (CONCEPT:EG-KG.query.core-query-input/EG-080): the graph rides the METHOD
        // (the `/nl` HTTP facade path has no request envelope), so route to the method's
        // `graph`, falling back to the request envelope's graph when it is empty. The
        // handler (behind `nl-query`) turns NL→UQL and runs the deterministic
        // `UnifiedQueryText` pipeline; a build without `nl-query` reaches the graph_ops
        // "not available" catch-all like any other feature-off method.
        Method::NlQuery { text, graph } => {
            dispatch_boxed(async {
                let req_id = req.id;
                let req_agent_id = req.agent_id.clone();
                let req_graph = req.graph.clone();
                {
                    let target = if graph.is_empty() {
                        req_graph.clone()
                    } else {
                        graph.clone()
                    };
                    dispatch_graph_op(
                        state,
                        &target,
                        req_id,
                        req_agent_id.as_deref(),
                        verified_context,
                        Method::NlQuery { text, graph },
                    )
                    .await
                }
            })
            .await
        }
        // Batched CROSS-GRAPH write (CONCEPT:EG-KG.storage.multi-graph-batch-write) — the
        // graphs ride the METHOD (one round-trip, many graphs), so like the txn/ts
        // self-routing ops it is handled HERE, BEFORE the single-`req.graph`
        // graph-op path. Each sub-batch fans through the normal per-graph write
        // path CONCURRENTLY, so N distinct graphs commit across N of the K shard
        // writers in parallel.
        Method::MultiGraphBatchUpdate { batches_msgpack } => {
            dispatch_boxed(async {
                let req_id = req.id;
                let req_agent_id = req.agent_id.clone();
                {
                    multi_graph_batch_update(
                        state,
                        req_id,
                        req_agent_id.as_deref(),
                        verified_context,
                        &batches_msgpack,
                    )
                    .await
                }
            })
            .await
        }
        other => return ControlFlow::Continue(other),
    })
}
