//! Private transaction reconcile implementation.

use super::*;

pub(super) const RECONCILE_LOOKUP_CONCURRENCY: usize = 32;

type ReconcileLookup = (String, Arc<crate::graph::GraphCore>, String, String);
type ReconcileLookupResult =
    Option<Result<Option<crate::mutation_batch::MutationBatchRecord>, String>>;

struct ReconcileContext {
    persistence: Arc<dyn crate::server::persistence::PersistenceBackend>,
    expected_principal: String,
    lookups: Vec<ReconcileLookup>,
}

async fn collect_reconcile_context(
    state: &Arc<RwLock<ServerState>>,
    caller: Option<&str>,
    txn_id: &str,
    idempotency_key: Option<&str>,
    expected_tenant: Option<&str>,
) -> Result<Option<ReconcileContext>, String> {
    let (persistence, graphs) = {
        let s = state.read().await;
        let graphs = s
            .registry
            .all_entries()
            .into_iter()
            .map(|entry| (entry.name.clone(), entry.core.clone()))
            .collect::<Vec<_>>();
        (s.persistence.clone(), graphs)
    };
    let Some(persistence) = persistence else {
        return Ok(None);
    };
    let caller =
        caller.ok_or_else(|| "transaction recovery requires a verified principal".to_string())?;
    let expected_principal = crate::server::mutation_batch::principal_fingerprint(caller)?;
    let parent_id = commit_receipt_id(txn_id, idempotency_key, expected_tenant);
    let mut lookups = Vec::with_capacity(graphs.len() * 2);
    for (graph, core) in graphs {
        let fname = crate::persist::sanitize(&graph);
        for namespace in ["txn", "crossmodal"] {
            let batch_id = crate::server::mutation_batch::opaque_coordinator_key(
                namespace, &graph, &parent_id,
            );
            lookups.push((graph.clone(), core.clone(), fname.clone(), batch_id));
        }
    }
    Ok(Some(ReconcileContext {
        persistence,
        expected_principal,
        lookups,
    }))
}

async fn fanout_reconcile_lookups(
    persistence: &Arc<dyn crate::server::persistence::PersistenceBackend>,
    lookups: &[ReconcileLookup],
) -> Result<Vec<ReconcileLookupResult>, String> {
    let semaphore = Arc::new(tokio::sync::Semaphore::new(RECONCILE_LOOKUP_CONCURRENCY));
    let mut set = tokio::task::JoinSet::new();
    for (idx, (_graph, _core, fname, batch_id)) in lookups.iter().enumerate() {
        let persistence = persistence.clone();
        let semaphore = semaphore.clone();
        let fname = fname.clone();
        let batch_id = batch_id.clone();
        set.spawn(async move {
            let _permit = semaphore
                .acquire_owned()
                .await
                .expect("reconcile lookup semaphore is never closed");
            (
                idx,
                persistence.read_mutation_batch(&fname, &batch_id).await,
            )
        });
    }
    let mut read_results: Vec<ReconcileLookupResult> = (0..lookups.len()).map(|_| None).collect();
    while let Some(joined) = set.join_next().await {
        let (idx, result) =
            joined.map_err(|error| format!("txn reconcile lookup task failed: {error}"))?;
        read_results[idx] = Some(result);
    }
    Ok(read_results)
}

/// Resolve an acknowledgement-lost retry before consulting ephemeral staging.
/// Single/cross-modal children are discovered across the durable graph catalog;
/// multi-graph commits use their named parent receipt. Successful child discovery
/// repairs both the serving projection and any still-Prepared parent receipt.
///
/// Independent graph/namespace reads are bounded and are replayed in their
/// original order so receipt semantics remain unchanged.
pub(super) async fn reconcile_committed_txn(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    caller: Option<&str>,
    txn_id: &str,
    idempotency_key: Option<&str>,
    expected_tenant: Option<&str>,
    attempt_nonce: Option<Nonce>,
) -> Result<Option<Response>, String> {
    let Some(context) =
        collect_reconcile_context(state, caller, txn_id, idempotency_key, expected_tenant).await?
    else {
        return Ok(None);
    };
    let ReconcileContext {
        persistence,
        expected_principal,
        lookups,
    } = context;
    let mut read_results = fanout_reconcile_lookups(&persistence, &lookups).await?;
    for ((graph, core, fname, batch_id), lookup_slot) in
        lookups.into_iter().zip(read_results.iter_mut())
    {
        let lookup = lookup_slot.take();
        if let Some(response) = reconcile_txn_candidate(ReconcileTxnCandidate {
            req_id,
            caller,
            txn_id,
            idempotency_key,
            expected_tenant,
            attempt_nonce,
            persistence: &persistence,
            expected_principal: &expected_principal,
            graph,
            core,
            fname,
            batch_id,
            lookup,
        })
        .await?
        {
            return Ok(Some(response));
        }
    }
    Ok(None)
}

/// One (graph, namespace) lookup candidate for [`reconcile_committed_txn`], plus
/// the context needed to validate and terminalize it. Grouped so the split-out
/// helper keeps a readable arity (clippy::too_many_arguments).
pub(super) struct ReconcileTxnCandidate<'a> {
    req_id: u64,
    caller: Option<&'a str>,
    txn_id: &'a str,
    idempotency_key: Option<&'a str>,
    expected_tenant: Option<&'a str>,
    attempt_nonce: Option<Nonce>,
    persistence: &'a Arc<dyn crate::server::persistence::PersistenceBackend>,
    expected_principal: &'a str,
    graph: String,
    core: Arc<crate::graph::GraphCore>,
    fname: String,
    batch_id: String,
    lookup: Option<Result<Option<crate::mutation_batch::MutationBatchRecord>, String>>,
}

/// Validate and, if it matches, terminalize one durable batch-record lookup
/// from the fan-out. `Ok(None)` means this candidate is not the committed
/// receipt; `Ok(Some(_))` is the final reconciled response.
pub(super) async fn reconcile_txn_candidate(
    args: ReconcileTxnCandidate<'_>,
) -> Result<Option<Response>, String> {
    let lookup = args
        .lookup
        .expect("every lookup index is populated before results are read");
    let record = match lookup {
        Ok(Some(record)) => record,
        Ok(None) => return Ok(None),
        Err(error) => return Err(error),
    };
    match reconcile_candidate_mismatch(
        &record,
        &args.batch_id,
        &args.graph,
        args.expected_tenant,
        args.expected_principal,
    ) {
        ReconcileCandidateMatch::Match => {}
        ReconcileCandidateMatch::OtherCandidate => return Ok(None),
        ReconcileCandidateMatch::ForeignAuthority(field) => {
            return Err(format!(
                "committed transaction receipt does not match caller scope ({field})"
            ));
        }
    }
    let bytes = record
        .result_msgpack
        .as_deref()
        .ok_or_else(|| "committed transaction has no durable result".to_string())?;
    let result = decode_txn_result(bytes)?;
    if !matches!(&result, ResultPayload::Bool(_)) {
        return Err("committed transaction child has the wrong result type".to_string());
    }
    let (snapshot, version) = args
        .persistence
        .read_authoritative_graph_snapshot(&args.fname)
        .await?
        .ok_or_else(|| "committed transaction graph image is missing".to_string())?;
    args.core.install_committed_snapshot(snapshot, version)?;

    let Some((receipt, replayed, _)) = resume_txn_receipt(
        Some(args.persistence.clone()),
        args.req_id,
        args.caller,
        args.txn_id,
        args.idempotency_key,
        args.expected_tenant,
        args.attempt_nonce,
    )?
    else {
        return Err("committed child has no durable transaction parent".to_string());
    };
    let reconciled = match replayed {
        Some(stored) => stored,
        None => finish_txn_receipt(receipt, result)?,
    };
    Ok(Some(Response::ok(args.req_id, reconciled)))
}

/// How one durable batch record relates to the committed child this reconcile
/// candidate is looking for.
pub(super) enum ReconcileCandidateMatch {
    /// Status, batch id, graph, tenant and principal all match the caller's
    /// scope: this IS the committed receipt.
    Match,
    /// A durable row that is not this fan-out's target at all -- a
    /// non-Committed row, a different batch id, or a differently scoped
    /// (for example native/ControlPlane) batch. The caller keeps looking.
    OtherCandidate,
    /// A committed, correctly scoped receipt that belongs to a DIFFERENT
    /// authority. Carries the name of the axis that disagreed.
    ForeignAuthority(&'static str),
}

/// Classify a durable batch record against what this reconcile candidate is
/// looking for.
///
/// The split matters: `reconcile_committed_txn` fans out over every
/// (resident graph x namespace) pair, so most candidates are legitimately
/// "some other row" and must not abort the search, while a foreign-authority
/// row at the caller's own coordinator key must.
pub(super) fn reconcile_candidate_mismatch(
    record: &crate::mutation_batch::MutationBatchRecord,
    batch_id: &str,
    graph: &str,
    expected_tenant: Option<&str>,
    expected_principal: &str,
) -> ReconcileCandidateMatch {
    if record.status != crate::mutation_batch::MutationBatchStatus::Committed
        || record.batch.batch_id != batch_id
        || record
            .batch
            .identity
            .scope()
            .graph_name()
            .map(|name| name.as_str())
            != Some(graph)
    {
        return ReconcileCandidateMatch::OtherCandidate;
    }
    // The graph and tenant are independent verified scopes.  A production
    // carrier may legitimately write graph `g` under tenant `t`, so matching
    // the tenant to `graph` would reject a valid crash-recovery receipt and
    // could fall through to a duplicate commit.  Reconcile against the
    // verified tenant carried by the retry instead.
    //
    // NOT via `record.batch.identity.tenant()`: a graph-scoped batch is
    // re-stamped onto the shard's own scope when the kernel-owned graph shard
    // admits it (`redb_store::shard`'s `bound.identity = graph_scope_identity`),
    // so the persisted identity tenant is the reserved `__shard__` for EVERY
    // caller -- deliberately, because the caller's tenant "is request-boundary
    // authorization and outbox attribution; it is deliberately NOT part of the
    // scope identity" (`eg_storage::owner::row_key::GRAPH_SHARD_TENANT`).
    // Comparing it to a caller tenant can therefore never match, which left
    // crash-recovery reconciliation permanently unable to adopt its own
    // committed cross-modal child.  The caller's scope survives on the batch's
    // outbox attribution, exactly like the principal `committing_actor()`
    // reads, so reconcile against that.
    if let Some(tenant) = expected_tenant {
        if committed_scope_digest(record) != Some(caller_scope_digest(tenant, graph)) {
            return ReconcileCandidateMatch::ForeignAuthority("tenant");
        }
    }
    if !matches!(record.committing_actor(), Ok(actor) if actor == expected_principal) {
        return ReconcileCandidateMatch::ForeignAuthority("principal");
    }
    ReconcileCandidateMatch::Match
}

/// The caller mutation scope a batch was compiled under, as
/// `server::mutation_batch::compile` stamps it onto every batch's outbox
/// attribution: `sha256(tenant || 0x00 || graph)`.
pub(super) fn caller_scope_digest(tenant: &str, graph: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut digest = Sha256::new();
    digest.update(tenant.as_bytes());
    digest.update([0]);
    digest.update(graph.as_bytes());
    hex::encode(digest.finalize())
}

/// The caller mutation scope recorded on a committed batch, or `None` when the
/// batch carries no scope attribution at all. The sibling of
/// `MutationBatchRecord::committing_actor`, which reads the actor header of the
/// same attribution row.
pub(super) fn committed_scope_digest(
    record: &crate::mutation_batch::MutationBatchRecord,
) -> Option<String> {
    record
        .batch
        .outbox
        .iter()
        .find_map(|intent| intent.headers.get("scope_sha256"))
        .filter(|digest| !digest.is_empty())
        .cloned()
}
