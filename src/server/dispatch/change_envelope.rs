use super::consensus::authoritative_now_ms;
#[cfg(feature = "redb")]
use super::graph_pipeline::dispatch_graph_op;
use super::graph_pipeline::GraphOpRouting;
use super::*;
use eg_types::result_contract::transactions as txn_results;

/// Batch envelope coordinator (CONCEPT:EG-KG.ingest.batched-change-envelopes). Validates
/// each envelope's context against the verified request authority, groups envelopes
/// by their `mutation.graph`, and routes each graph's envelopes to `dispatch_graph_op`
/// (which resolves that graph's Write ACL + placement and commits the group in ONE
/// coalesced transaction). Per-graph groups are independent (partial success across
/// graphs); within a graph the commit is atomic. Per-envelope results are reassembled
/// into REQUEST order under `{"results": [...]}` so a caller can advance a watermark
/// through the contiguous success prefix.
#[cfg(not(feature = "redb"))]
pub(super) async fn dispatch_change_envelopes(
    _state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    _caller: Option<&str>,
    _verified_context: &VerifiedRequestContext,
    _envelopes: Vec<crate::change_envelope::ChangeEnvelope>,
) -> Response {
    Response::err(
        req_id,
        "batch change-envelope commit requires a build with durable redb support",
    )
}

#[cfg(feature = "redb")]
pub(super) async fn dispatch_change_envelopes(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    caller: Option<&str>,
    verified_context: &VerifiedRequestContext,
    envelopes: Vec<crate::change_envelope::ChangeEnvelope>,
) -> Response {
    let total = envelopes.len();
    if total == 0 {
        return Response::ok(
            req_id,
            ResultPayload::of::<txn_results::ApplyChangeEnvelopes>(
                txn_results::ChangeEnvelopeBatch::default(),
            ),
        );
    }
    if total > crate::change_envelope::MAX_ENVELOPES_PER_BATCH {
        return Response::err(
            req_id,
            format!(
                "CHANGE_BATCH_TOO_LARGE: {total} envelopes exceed the {} cap",
                crate::change_envelope::MAX_ENVELOPES_PER_BATCH
            ),
        );
    }
    if let Some(response) =
        change_envelope_batch_authority_error(req_id, verified_context, &envelopes)
    {
        return response;
    }

    let mut per_index: Vec<Option<txn_results::ChangeEnvelopeOutcome>> = vec![None; total];
    let (groups, ungrouped) = group_change_envelopes_by_graph(envelopes);
    for index in ungrouped {
        // Unreachable in practice: `change_envelope_batch_authority_error` above
        // already rejects the whole request if any envelope's mutation scope has
        // no graph name. Kept as an explicit conflict entry, not a silent gap,
        // in case that invariant ever changes.
        per_index[index] = Some(change_envelope_conflict(
            None,
            "ApplyChangeEnvelopes requires a graph-scoped mutation",
        ));
    }
    for (graph, group) in groups {
        let indices: Vec<usize> = group.iter().map(|(index, _)| *index).collect();
        let group_envelopes: Vec<crate::change_envelope::ChangeEnvelope> =
            group.into_iter().map(|(_, envelope)| envelope).collect();
        let resp = dispatch_graph_op(
            state,
            &graph,
            req_id,
            caller,
            verified_context,
            Method::ApplyChangeEnvelopes {
                envelopes: group_envelopes,
            },
        )
        .await;
        scatter_change_envelope_group_results(&mut per_index, &indices, resp);
    }

    let results = per_index
        .into_iter()
        .map(|entry| {
            entry.unwrap_or_else(|| {
                change_envelope_conflict(None, "missing per-envelope result in batch response")
            })
        })
        .collect();
    Response::ok(
        req_id,
        ResultPayload::of::<txn_results::ApplyChangeEnvelopes>(txn_results::ChangeEnvelopeBatch {
            results,
        }),
    )
}

/// Per-envelope authority binding — mirrors the single `ApplyChangeEnvelope`
/// arm, minus the two batch-varying fields: the idempotency_key (per envelope,
/// enforced by the mutation-store idempotency table) and the graph (per
/// envelope, ACL-checked per group by `dispatch_graph_op`).
#[cfg(feature = "redb")]
fn change_envelope_batch_authority_error(
    req_id: u64,
    verified_context: &VerifiedRequestContext,
    envelopes: &[crate::change_envelope::ChangeEnvelope],
) -> Option<Response> {
    let claims = verified_context.claims();
    let principal = verified_context.principal_persistence_id();
    for envelope in envelopes {
        // `ApplyChangeEnvelopes` groups and routes every envelope by graph name
        // (`group_change_envelopes_by_graph` below, then `dispatch_graph_op`), so
        // a native (non-graph) mutation scope — which reports no graph name at
        // all — can never be routed and must fail closed here rather than being
        // silently dropped or grouped under a sentinel.
        if envelope.mutation.identity.scope().graph_name().is_none()
            || envelope.mutation.identity.tenant().as_str() != claims.tenant
            || eg_types::mutation_batch::batch_request_number(&envelope.mutation)
                != Some(req_id)
            // The CALLER is the outbox `actor` header, never the batch's serving
            // principal: RF-RULING-004's application note makes the latter this
            // engine's own on every domain, so comparing it would compare the
            // engine against itself and pass for any caller. The policy
            // comparison is gone with `policy_fingerprint`, an always-`None`
            // `Option<String>` that could only ever have refused every
            // caller-supplied envelope; the real policy revision is inside the
            // stable replay identity, where a change conflicts.
            || crate::server::mutation_batch::batch_actor(&envelope.mutation)
                != Some(principal.as_str())
        {
            return Some(Response::err(
                req_id,
                "ApplyChangeEnvelopes context does not match the verified request authority",
            ));
        }
    }
    None
}

/// Per-graph envelope groups, each entry carrying its graph name and the
/// `(request_index, envelope)` pairs staged for that graph in request order,
/// paired with the request indices of envelopes that had no graph to group by.
#[cfg(feature = "redb")]
type GroupedChangeEnvelopes = (
    Vec<(String, Vec<(usize, crate::change_envelope::ChangeEnvelope)>)>,
    Vec<usize>,
);

/// Group envelopes by graph, preserving first-seen graph order and the
/// per-graph envelope order, and carrying each envelope's REQUEST index so the
/// per-graph results can be scattered back into request order.
///
/// Every envelope reaching this function already passed
/// `change_envelope_batch_authority_error`, which rejects the whole batch if
/// any envelope's mutation scope has no graph name — so in practice a native
/// (non-graph) scope never appears here. It is still handled explicitly
/// (returned as `ungrouped`, never silently coerced into a "" / sentinel
/// group) so a future change to the authority check cannot turn this into a
/// silent misroute.
#[cfg(feature = "redb")]
fn group_change_envelopes_by_graph(
    envelopes: Vec<crate::change_envelope::ChangeEnvelope>,
) -> GroupedChangeEnvelopes {
    let mut graph_order: Vec<String> = Vec::new();
    let mut groups: std::collections::HashMap<
        String,
        Vec<(usize, crate::change_envelope::ChangeEnvelope)>,
    > = std::collections::HashMap::new();
    let mut ungrouped: Vec<usize> = Vec::new();
    for (index, envelope) in envelopes.into_iter().enumerate() {
        let Some(graph) = envelope
            .mutation
            .identity
            .scope()
            .graph_name()
            .map(|name| name.as_str().to_string())
        else {
            ungrouped.push(index);
            continue;
        };
        groups
            .entry(graph.clone())
            .or_insert_with(|| {
                graph_order.push(graph.clone());
                Vec::new()
            })
            .push((index, envelope));
    }
    let grouped = graph_order
        .into_iter()
        .map(|graph| {
            let group = groups.remove(&graph).expect("grouped graph is present");
            (graph, group)
        })
        .collect();
    (grouped, ungrouped)
}

/// Scatter one graph group's response back into request-ordered slots.
#[cfg(feature = "redb")]
fn scatter_change_envelope_group_results(
    per_index: &mut [Option<txn_results::ChangeEnvelopeOutcome>],
    indices: &[usize],
    response: Response,
) {
    // A transport/ACL/placement failure for the whole group (distinct from the
    // per-envelope atomic-batch abort, which returns Ok with conflict entries).
    let mut group_results = match declared_json_response::<txn_results::ChangeEnvelopeBatch>(
        response,
        "invalid batch response",
        "empty batch response",
    ) {
        Ok(batch) => batch.results.into_iter(),
        Err(error) => {
            for index in indices {
                per_index[*index] = Some(change_envelope_conflict(None, error.clone()));
            }
            return;
        }
    };
    for index in indices {
        per_index[*index] = Some(group_results.next().unwrap_or_else(|| {
            change_envelope_conflict(None, "missing per-envelope result in batch response")
        }));
    }
}

/// A `conflict` outcome for one envelope of an `ApplyChangeEnvelopes` batch.
fn change_envelope_conflict(
    envelope_id: Option<String>,
    error: impl Into<String>,
) -> txn_results::ChangeEnvelopeOutcome {
    txn_results::ChangeEnvelopeOutcome::Conflict(txn_results::ChangeEnvelopeConflict {
        envelope_id,
        error: error.into(),
    })
}

mod multi_graph;
pub(super) use multi_graph::{decode_multi_graph_batches, multi_graph_batch_update};

fn change_envelope_result(
    committed: &eg_types::ChangeEnvelopeCommit,
    projection_pending: bool,
) -> txn_results::ChangeEnvelopeApplied {
    txn_results::ChangeEnvelopeApplied {
        commit: committed.clone(),
        projection_pending,
        replication: None,
    }
}

/// Derive the native authority epoch from the registry-published graph
/// incarnation.  The caller cannot provide either value; a delete/recreate
/// therefore fences every capability from the retired incarnation.
pub(super) fn work_item_capability_authority_epoch(incarnation_id: &str) -> u64 {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(incarnation_id.as_bytes());
    u64::from_be_bytes(digest[..8].try_into().expect("fixed digest width")).max(1)
}

async fn check_change_envelope_placement_fence(
    req_id: u64,
    #[cfg(feature = "raft")] graph_name: &str,
    envelope: &eg_types::change_envelope::ChangeEnvelope,
    #[cfg(feature = "raft")] routed_raft: Option<&crate::raft::multi::RoutedRaftHandle>,
) -> Result<(), Response> {
    #[cfg(feature = "raft")]
    if let Some(routed) = routed_raft {
        let leader = routed.handle.current_leader().await;
        if leader != Some(routed.handle.node_id) {
            return Err(Response::stale_route(
                req_id,
                graph_name,
                routed.group_id,
                routed.epoch,
                leader,
                "ChangeEnvelope commits require the current placement leader",
            ));
        }
        if envelope.mutation.placement_epoch != routed.epoch
            || envelope.mutation.fencing_token != Some(routed.group_id)
        {
            return Err(Response::stale_route(
                req_id,
                graph_name,
                routed.group_id,
                routed.epoch,
                leader,
                "ChangeEnvelope placement epoch or fencing token is stale",
            ));
        }
    } else {
        if envelope.mutation.placement_epoch != 0 || envelope.mutation.fencing_token.is_some() {
            return Err(Response::err(
                req_id,
                "ChangeEnvelope carries a placement fence but no routed placement is active",
            ));
        }
    }
    #[cfg(not(feature = "raft"))]
    if envelope.mutation.placement_epoch != 0 || envelope.mutation.fencing_token.is_some() {
        return Err(Response::err(
            req_id,
            "ChangeEnvelope carries a placement fence in a single-node build",
        ));
    }
    Ok(())
}

/// The already-resolved graph/placement identity a replicated ChangeEnvelope
/// commit needs; the envelope and its leader-selected timestamp stay explicit
/// because they are what is actually being replicated.
#[cfg(feature = "raft")]
struct ChangeEnvelopeReplicaCtx<'a> {
    state: &'a Arc<RwLock<ServerState>>,
    req_id: u64,
    graph_name: &'a str,
    graph_type: crate::protocol::GraphType,
    tenant_scope: &'a str,
    fname: &'a str,
    routed_raft: Option<&'a crate::raft::multi::RoutedRaftHandle>,
}

/// Attempt the clustered replication path for `ApplyChangeEnvelope`: one log
/// entry, one native transaction on every replica, leader-selected timestamp
/// for byte-stable replay/follower records. `Some(resp)` means the request
/// was fully handled (locally or via a stale-route redirect) by this path;
/// `None` means no routed placement is active and the caller should fall
/// through to the local `commit_change_envelope` path.
#[cfg(feature = "raft")]
async fn try_replicate_change_envelope(
    ctx: ChangeEnvelopeReplicaCtx<'_>,
    envelope: &eg_types::change_envelope::ChangeEnvelope,
    committed_at_ms: u64,
) -> Option<Response> {
    let ChangeEnvelopeReplicaCtx {
        state,
        req_id,
        graph_name,
        graph_type,
        tenant_scope,
        fname,
        routed_raft,
    } = ctx;
    let routed = routed_raft?;
    let attempt_nonce = envelope
        .mutation
        .envelope
        .operation()
        .and_then(|operation| operation.nonce_replay_key().ok())
        .map(|key| key.nonce);
    let mutation = match crate::raft::RaftMutationContext::from_verified_request(
        envelope.mutation.batch_id.clone(),
        // The dispatch request number the batch was compiled for, read back
        // through the one encoder rather than from a second field.
        eg_types::mutation_batch::batch_request_number(&envelope.mutation).unwrap_or(req_id),
        attempt_nonce,
        tenant_scope,
        // The CALLER, which under RF-RULING-004's application note is the outbox
        // `actor` header -- never the serving principal, which is this engine's
        // own and would compare equal for every caller.
        crate::server::mutation_batch::batch_actor(&envelope.mutation)
            .unwrap_or_default()
            .to_string(),
        false,
        envelope.mutation.placement_epoch,
        envelope.mutation.fencing_token,
        envelope.mutation.created_at_ms,
    ) {
        Ok(context) => context,
        Err(error) => return Some(Response::err(req_id, error)),
    };
    let server_secret = timed_read(state).await.auth_secret.clone();
    let command = match crate::raft::ReplicatedMutation::change_envelope(envelope, &server_secret) {
        Ok(command) => command,
        Err(error) => return Some(Response::err(req_id, error)),
    };
    let request = crate::raft::RaftRequest {
        graph_fname: fname.to_string(),
        graph_name: graph_name.to_string(),
        graph_type,
        command,
        committed_at_ms,
        mutation,
    };
    Some(match routed.handle.client_write(request).await {
        Ok(response) if response.native_error.is_some() => {
            Response::err(req_id, response.native_error.unwrap_or_default())
        }
        Ok(response) => match response.change_envelope_commit {
            Some(committed) => {
                let mut result = change_envelope_result(&committed, response.projection_pending);
                result.replication = Some(txn_results::ChangeEnvelopeReplication {
                    replicated: true,
                    group: routed.group_id,
                    epoch: routed.epoch,
                    fencing_token: routed.fencing_token(),
                });
                Response::ok(
                    req_id,
                    ResultPayload::of::<txn_results::ApplyChangeEnvelope>(result),
                )
            }
            None => Response::err(
                req_id,
                "replicated ChangeEnvelope returned no commit receipt",
            ),
        },
        Err(error) => {
            let leader = routed.handle.current_leader().await;
            Response::stale_route(
                req_id,
                graph_name,
                routed.group_id,
                routed.epoch,
                leader,
                error,
            )
        }
    })
}

/// The resolved graph context a single-envelope `ApplyChangeEnvelope` commit
/// runs against: the live core, its durability backend, the placement route and
/// the tenant authority. Bundled to keep the dispatcher at the documented
/// parameter cap.
/// Commit a `ApplyChangeEnvelopes` batch and translate the durable result
/// (all-or-nothing per `crate::persist::sanitize`d graph) into the per-envelope
/// JSON result array the caller returns as `{"results": [...]}` -- success
/// entries and the atomic-abort's per-envelope conflict entries are the same
/// shape either way, so callers never have to branch on which happened.
async fn commit_change_envelope_batch_results(
    backend: &Arc<dyn crate::server::persistence::PersistenceBackend>,
    core: &Arc<crate::graph::GraphCore>,
    fname: &str,
    envelopes: &[eg_types::change_envelope::ChangeEnvelope],
    committed_at_ms: u64,
) -> Vec<txn_results::ChangeEnvelopeOutcome> {
    match backend
        .commit_change_envelopes(fname, envelopes, committed_at_ms)
        .await
    {
        Ok(commits) => envelopes
            .iter()
            .zip(commits.iter())
            .map(|(envelope, committed)| change_envelope_applied_entry(core, envelope, committed))
            .collect(),
        Err((failing_index, error)) => {
            change_envelope_abort_entries(envelopes, failing_index, &error)
        }
    }
}

/// One committed envelope's result entry. A replayed envelope is an idempotent
/// skip and does NOT republish its projection; a freshly applied one does, and
/// a projection failure is surfaced as `projection_pending` rather than
/// pretending the durable commit did not happen.
fn change_envelope_applied_entry(
    core: &Arc<crate::graph::GraphCore>,
    envelope: &eg_types::change_envelope::ChangeEnvelope,
    committed: &eg_types::ChangeEnvelopeCommit,
) -> txn_results::ChangeEnvelopeOutcome {
    let projection_error = if committed.replayed {
        None
    } else {
        crate::server::mutation_batch::publish_change_envelope_projection(core, envelope).err()
    };
    let entry = change_envelope_result(committed, projection_error.is_some());
    if committed.replayed {
        txn_results::ChangeEnvelopeOutcome::IdempotentSkip(entry)
    } else {
        txn_results::ChangeEnvelopeOutcome::Applied(entry)
    }
}

/// The whole graph-batch aborted atomically — nothing committed. Report the
/// batch outcome per envelope honestly: the offender carries its own error; the
/// siblings carry the abort cause.
fn change_envelope_abort_entries(
    envelopes: &[eg_types::change_envelope::ChangeEnvelope],
    failing_index: usize,
    error: &str,
) -> Vec<txn_results::ChangeEnvelopeOutcome> {
    envelopes
        .iter()
        .enumerate()
        .map(|(index, envelope)| {
            let this_error = if index == failing_index {
                error.to_string()
            } else {
                format!(
                    "ABORTED_ATOMIC_GRAPH_BATCH: sibling envelope {failing_index} failed ({error})"
                )
            };
            change_envelope_conflict(Some(envelope.envelope_id.clone()), this_error)
        })
        .collect()
}

pub(super) async fn route_change_envelope_ops(
    ctx: GraphOpRouting<'_>,
    method: Method,
) -> Result<Response, Method> {
    match method {
        Method::ApplyChangeEnvelope { envelope } => {
            Ok(apply_one_change_envelope(ctx, envelope).await)
        }
        Method::ApplyChangeEnvelopes { envelopes } => {
            Ok(apply_change_envelope_batch(ctx, envelopes).await)
        }
        Method::GetChangeEnvelope {
            envelope_id,
            tenant,
        } => Ok(read_change_envelope(ctx, envelope_id, tenant).await),
        Method::GetContentVersion { object_id, tenant } => {
            Ok(read_content_version(ctx, object_id, tenant).await)
        }
        Method::GetChangeCursor {
            source,
            partition,
            tenant,
        } => Ok(read_change_cursor(ctx, source, partition, tenant).await),
        other => Err(other),
    }
}

async fn apply_one_change_envelope(
    ctx: GraphOpRouting<'_>,
    envelope: eg_types::change_envelope::ChangeEnvelope,
) -> Response {
    let req_id = ctx.req_id;
    let graph_name = ctx.graph_name;
    let core = ctx.core;
    let persistence = ctx.persistence;
    #[cfg(feature = "raft")]
    let state = ctx.state;
    #[cfg(feature = "raft")]
    let tenant_scope = ctx.tenant_scope;
    #[cfg(feature = "raft")]
    let routed_raft = ctx.routed_raft;
    #[cfg(feature = "raft")]
    let graph_type = ctx.graph_type;

    if let Err(resp) = check_change_envelope_placement_fence(
        req_id,
        #[cfg(feature = "raft")]
        graph_name,
        &envelope,
        #[cfg(feature = "raft")]
        routed_raft.as_ref(),
    )
    .await
    {
        return resp;
    }

    let _mutation_guard = crate::server::mutation_batch::lock_graph(graph_name).await;
    match envelope.mutation.version_expectation {
        crate::mutation_batch::VersionExpectation::Graph(expected) => {
            if expected != core.version() {
                return Response::err(
                    req_id,
                    format!(
                        "STALE_GRAPH_VERSION: expected {expected}, current {}",
                        core.version()
                    ),
                );
            }
        }
        _ => {
            return Response::err(
                req_id,
                "ApplyChangeEnvelope requires a graph version expectation",
            );
        }
    }
    let committed_at_ms = authoritative_now_ms().max(envelope.mutation.created_at_ms);
    let Some(backend) = persistence.as_ref() else {
        return Response::err(
            req_id,
            "ApplyChangeEnvelope requires a configured persistence backend",
        );
    };
    let fname = crate::persist::sanitize(graph_name);

    #[cfg(feature = "raft")]
    if let Some(resp) = try_replicate_change_envelope(
        ChangeEnvelopeReplicaCtx {
            state,
            req_id,
            graph_name,
            graph_type,
            tenant_scope,
            fname: &fname,
            routed_raft: routed_raft.as_ref(),
        },
        &envelope,
        committed_at_ms,
    )
    .await
    {
        return resp;
    }

    let committed = match backend
        .commit_change_envelope(&fname, &envelope, committed_at_ms)
        .await
    {
        Ok(committed) => committed,
        Err(error) => {
            return Response::err(
                req_id,
                format!("ApplyChangeEnvelope atomic commit failed: {error}"),
            );
        }
    };
    let projection_error = if committed.replayed {
        None
    } else {
        crate::server::mutation_batch::publish_change_envelope_projection(core, &envelope).err()
    };
    let result = change_envelope_result(&committed, projection_error.is_some());
    Response::ok(
        req_id,
        ResultPayload::of::<txn_results::ApplyChangeEnvelope>(result),
    )
}

async fn apply_change_envelope_batch(
    ctx: GraphOpRouting<'_>,
    envelopes: Vec<eg_types::change_envelope::ChangeEnvelope>,
) -> Response {
    let req_id = ctx.req_id;
    let graph_name = ctx.graph_name;
    let core = ctx.core.clone();
    let persistence = ctx.persistence.clone();
    #[cfg(feature = "raft")]
    let routed_raft = ctx.routed_raft.clone();

    #[cfg(feature = "raft")]
    if routed_raft.is_some() {
        return Response::err(
            req_id,
            "CHANGE_BATCH_UNAVAILABLE_UNDER_PLACEMENT: use per-envelope ApplyChangeEnvelope",
        );
    }
    let _mutation_guard = crate::server::mutation_batch::lock_graph(graph_name).await;
    for envelope in &envelopes {
        if envelope.mutation.placement_epoch != 0 || envelope.mutation.fencing_token.is_some() {
            return Response::err(
                req_id,
                "ChangeEnvelope carries a placement fence in a single-node build",
            );
        }
    }
    let committed_at_ms = envelopes.iter().fold(authoritative_now_ms(), |acc, e| {
        acc.max(e.mutation.created_at_ms)
    });
    let Some(backend) = persistence.as_ref() else {
        return Response::err(
            req_id,
            "ApplyChangeEnvelopes requires a configured persistence backend",
        );
    };
    let fname = crate::persist::sanitize(graph_name);
    let results =
        commit_change_envelope_batch_results(backend, &core, &fname, &envelopes, committed_at_ms)
            .await;
    Response::ok(
        req_id,
        ResultPayload::of::<txn_results::ApplyChangeEnvelopes>(txn_results::ChangeEnvelopeBatch {
            results,
        }),
    )
}

async fn read_change_envelope(
    ctx: GraphOpRouting<'_>,
    envelope_id: String,
    tenant: String,
) -> Response {
    let req_id = ctx.req_id;
    let graph_name = ctx.graph_name;
    let Some(backend) = ctx.persistence.as_ref() else {
        return Response::err(req_id, "ChangeEnvelope persistence is unavailable");
    };
    let fname = crate::persist::sanitize(graph_name);
    match backend.read_change_envelope(&fname, &envelope_id).await {
        Ok(Some(record))
            if record.envelope.mutation.identity.tenant().as_str() == tenant
                && record
                    .envelope
                    .mutation
                    .identity
                    .scope()
                    .graph_name()
                    .map(crate::mutation_batch::LogicalName::as_str)
                    == Some(graph_name) =>
        {
            Response::ok(req_id, ResultPayload::raw(&record))
        }
        Ok(Some(_)) => Response::err(req_id, "ACCESS_DENIED: envelope tenant mismatch"),
        Ok(None) => Response::ok(
            req_id,
            ResultPayload::raw(&Option::<crate::change_envelope::ChangeEnvelopeRecord>::None),
        ),
        Err(error) => Response::err(req_id, format!("ChangeEnvelope read failed: {error}")),
    }
}

async fn read_content_version(
    ctx: GraphOpRouting<'_>,
    object_id: String,
    tenant: String,
) -> Response {
    let req_id = ctx.req_id;
    let graph_name = ctx.graph_name;
    let Some(backend) = ctx.persistence.as_ref() else {
        return Response::err(req_id, "content-version persistence is unavailable");
    };
    let fname = crate::persist::sanitize(graph_name);
    match backend
        .read_content_version(&fname, &tenant, &object_id)
        .await
    {
        Ok(version) => Response::ok(req_id, ResultPayload::raw(&version)),
        Err(error) => Response::err(req_id, format!("content-version read failed: {error}")),
    }
}

async fn read_change_cursor(
    ctx: GraphOpRouting<'_>,
    source: String,
    partition: String,
    tenant: String,
) -> Response {
    let req_id = ctx.req_id;
    let graph_name = ctx.graph_name;
    let Some(backend) = ctx.persistence.as_ref() else {
        return Response::err(req_id, "change-cursor persistence is unavailable");
    };
    let fname = crate::persist::sanitize(graph_name);
    match backend
        .read_change_cursor(&fname, &tenant, &source, &partition)
        .await
    {
        Ok(cursor) => Response::ok(req_id, ResultPayload::raw(&cursor)),
        Err(error) => Response::err(req_id, format!("change-cursor read failed: {error}")),
    }
}
