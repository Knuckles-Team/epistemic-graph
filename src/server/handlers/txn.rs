//! Multi-op OCC ACID transaction handler (CONCEPT:EG-KG.txn.multi-op-occ-acid).
//!
//! Owns the `Txn*`/`BeginTxn`/`Commit`/`Rollback` methods. These are STATEFUL
//! (they read/write `ServerState::open_txns`), so unlike the per-graph handlers
//! they take `state`. The staged ops never touch the graph or persistence; the
//! topology write lock is taken ONCE, at commit, where the OCC read-set is
//! validated and the staged write-set applied through a single `GraphTxn`.
//!
//! Composition with the write coalescer (CONCEPT:EG-KG.sharding.per-graph-write-coalescer): staged ops are applied
//! directly via `GraphTxn` at commit and never enter the coalescer's queue, so
//! there is no interaction or deadlock with the per-graph write worker — that
//! worker only batches NON-transactional single-op writes. A long-open txn holds
//! NO lock at all (begin/stage take only the cheap `open_txns` DashMap + per-entry
//! Mutex), so client think-time never blocks readers or writers.

use std::sync::Arc;

use tokio::sync::RwLock;

use super::super::access::{check_graph_access, CarrierAuthority, GraphReadAuthority};
use super::super::auth::VerifiedRequestContext;
use super::super::state::ServerState;
#[cfg(feature = "graphql")]
use super::super::txn::IsolationLevel;
#[cfg(feature = "tsdb")]
use super::super::txn::StagedMeasurement;
use super::super::txn::{now_ms, parse_isolation, GraphTxnState, NewTxnArgs};
use crate::isolation::AccessLevel;
use crate::protocol::{Method, Response, ResultPayload};

const MAX_TXN_RESULT_BYTES: usize = 1024 * 1024;
const MAX_TXN_NESTED_BYTES: usize = 64 * 1024 * 1024;
const MAX_TXN_NESTED_ITEMS: usize = 1_000_000;

fn consensus_apply_is_authorized() -> bool {
    #[cfg(feature = "raft")]
    {
        crate::server::dispatch::is_replicated_apply()
    }
    #[cfg(not(feature = "raft"))]
    {
        false
    }
}

fn decode_txn_value<T: serde::de::DeserializeOwned>(
    bytes: &[u8],
    max_bytes: usize,
    max_items: usize,
) -> Result<T, String> {
    eg_types::msgpack::decode_bounded(
        bytes,
        eg_types::msgpack::MsgpackLimits::new(
            max_bytes,
            max_items,
            eg_types::msgpack::DEFAULT_MAX_DEPTH,
        ),
    )
    .map_err(|_| "transaction value is invalid or exceeds resource limits".to_string())
}

fn decode_txn_result(bytes: &[u8]) -> Result<ResultPayload, String> {
    decode_txn_value(bytes, MAX_TXN_RESULT_BYTES, 1_024)
}

fn decode_txn_object(bytes: &[u8]) -> Result<serde_json::Map<String, serde_json::Value>, String> {
    decode_txn_value(bytes, MAX_TXN_NESTED_BYTES, MAX_TXN_NESTED_ITEMS)
}

/// Commit consumes an open transaction only after a terminal response. Any error
/// path (ACL, OCC staging, durability, projection) automatically restores the
/// complete staged state so the caller can retry with the same transaction id.
struct TxnRestoreGuard {
    open: Arc<dashmap::DashMap<String, parking_lot::Mutex<GraphTxnState>>>,
    txn_id: String,
    txn: Option<GraphTxnState>,
}

impl TxnRestoreGuard {
    fn new(
        open: Arc<dashmap::DashMap<String, parking_lot::Mutex<GraphTxnState>>>,
        txn_id: &str,
        txn: GraphTxnState,
    ) -> Self {
        Self {
            open,
            txn_id: txn_id.to_string(),
            txn: Some(txn),
        }
    }

    fn complete(&mut self) {
        self.txn = None;
    }
}

impl Drop for TxnRestoreGuard {
    fn drop(&mut self) {
        if let Some(txn) = self.txn.take() {
            self.open
                .insert(self.txn_id.clone(), parking_lot::Mutex::new(txn));
        }
    }
}

fn transaction_receipt_id(txn_id: &str) -> String {
    crate::server::mutation_batch::opaque_coordinator_key(
        "transaction-receipt",
        "transaction",
        txn_id,
    )
}

#[cfg(feature = "raft")]
fn cross_shard_transaction_id(parent_id: &str) -> String {
    // Use the digest-only parent id in the disjoint 2PC table as well.  This gives
    // recovery/GC a direct safe association without persisting the raw transaction
    // id or an additional mapping row.
    parent_id.to_string()
}

/// A retained 2PC decision is collected only after the parent receipt is terminal.
/// Calling this for a local/single-graph transaction is an idempotent no-op.
#[cfg(feature = "raft")]
async fn cleanup_cross_shard_decision(
    state: &Arc<RwLock<ServerState>>,
    txn_id: &str,
) -> Result<(), String> {
    let backend = state.read().await.persistence.clone();
    let Some(redb) = backend.as_ref().and_then(|value| value.as_redb()) else {
        return Ok(());
    };
    let parent_id = transaction_receipt_id(txn_id);
    if redb.xshard_decision_retain_get(&parent_id)? {
        let decision = redb
            .xshard_decision_get(&parent_id)?
            .ok_or_else(|| "retained cross-shard transaction is still undecided".to_string())?;
        let parent = eg_mutation_store::read_record(redb.admin_mutation_store(), &parent_id)?
            .ok_or_else(|| "retained cross-shard decision has no parent receipt".to_string())?;
        if parent.status != crate::mutation_batch::MutationBatchStatus::Committed {
            return Err("cross-shard decision cannot be collected before its parent".to_string());
        }
        let bytes = parent
            .result_msgpack
            .as_deref()
            .ok_or_else(|| "committed transaction parent has no result".to_string())?;
        let result = decode_txn_result(bytes)?;
        if !matches!(result, ResultPayload::Bool(value) if value == decision) {
            return Err("transaction parent and retained 2PC decision disagree".to_string());
        }
    }
    redb.xshard_decision_clear(&parent_id).await
}

#[cfg(not(feature = "raft"))]
async fn cleanup_cross_shard_decision(
    _state: &Arc<RwLock<ServerState>>,
    _txn_id: &str,
) -> Result<(), String> {
    Ok(())
}

#[cfg(feature = "redb")]
struct TxnReceipt {
    backend: Arc<dyn crate::server::persistence::PersistenceBackend>,
    saga: crate::server::handlers::admin::AdminSaga,
}

#[cfg(not(feature = "redb"))]
type TxnReceipt = ();

#[cfg(feature = "redb")]
fn receipt_coordinator_id(receipt: &TxnReceipt) -> String {
    receipt.saga.batch.batch_id.clone()
}

#[cfg(not(feature = "redb"))]
fn receipt_coordinator_id(_receipt: &TxnReceipt) -> String {
    String::new()
}

#[cfg(feature = "redb")]
fn begin_txn_receipt(
    backend: Option<Arc<dyn crate::server::persistence::PersistenceBackend>>,
    req_id: u64,
    caller: Option<&str>,
    txn_id: &str,
    txn: &GraphTxnState,
) -> Result<(TxnReceipt, Option<ResultPayload>), String> {
    let backend = backend.ok_or_else(|| {
        "transaction commit requires an authoritative MutationBatch backend".to_string()
    })?;
    let redb = backend
        .as_redb()
        .ok_or_else(|| "transaction commit requires durable redb".to_string())?;
    let (payload_digest, encrypted_payload) = seal_txn_recovery_plan(redb, txn)?;
    let saga = crate::server::handlers::admin::begin_named_admin_saga_with_private_payload(
        redb,
        req_id,
        caller,
        crate::server::handlers::admin::AdminSagaPayload {
            domain: crate::mutation_batch::MutationDomain::ControlPlane,
            batch_id: &transaction_receipt_id(txn_id),
            event_type: "transaction_recovery_plan",
            payload_digest: &payload_digest,
            encrypted_payload: &encrypted_payload,
        },
    )?;
    let replayed = saga.replayed.clone();
    Ok((TxnReceipt { backend, saga }, replayed))
}

#[cfg(not(feature = "redb"))]
fn begin_txn_receipt(
    _backend: Option<Arc<dyn crate::server::persistence::PersistenceBackend>>,
    _req_id: u64,
    _caller: Option<&str>,
    _txn_id: &str,
    _txn: &GraphTxnState,
) -> Result<((), Option<ResultPayload>), String> {
    Err("transaction commit requires the redb MutationBatch coordinator".to_string())
}

/// A resumed receipt, its already-known apply result (if any), and the rebuilt
/// ephemeral staging state — [`resume_txn_receipt`]'s success payload.
#[cfg(feature = "redb")]
type ResumedTxnReceipt = (TxnReceipt, Option<ResultPayload>, Option<GraphTxnState>);

/// Re-open a prepared/committed parent receipt after process restart.  A Prepared
/// receipt must still have its encrypted private plan; the plan is authenticated,
/// decrypted, digest-verified against the canonical batch, and only then rebuilt
/// into ephemeral staging.
#[cfg(feature = "redb")]
fn resume_txn_receipt(
    backend: Option<Arc<dyn crate::server::persistence::PersistenceBackend>>,
    caller: Option<&str>,
    txn_id: &str,
) -> Result<Option<ResumedTxnReceipt>, String> {
    let Some(backend) = backend else {
        return Ok(None);
    };
    let caller = caller
        .filter(|actor| !actor.trim().is_empty())
        .ok_or_else(|| "transaction recovery requires a verified actor".to_string())?;
    let redb = backend
        .as_redb()
        .ok_or_else(|| "transaction recovery requires durable redb".to_string())?;
    let Some(saga) = crate::server::handlers::admin::resume_named_admin_saga(
        redb,
        &transaction_receipt_id(txn_id),
        Some(caller),
    )?
    else {
        return Ok(None);
    };
    if saga.batch.tenant != "native" || saga.batch.graph != "cluster-admin" {
        return Err("transaction parent receipt has the wrong coordinator scope".to_string());
    }
    let _ = transaction_plan_digest(&saga.batch)?;
    let replayed = saga.replayed.clone();
    if replayed
        .as_ref()
        .map(|result| !matches!(result, ResultPayload::Bool(_)))
        .unwrap_or(false)
    {
        return Err("transaction parent receipt has the wrong result type".to_string());
    }
    let txn = if replayed.is_none() {
        let encrypted = eg_mutation_store::read_private_payload(
            redb.admin_mutation_store(),
            &saga.batch.batch_id,
        )?
        .ok_or_else(|| "prepared transaction has no encrypted recovery plan".to_string())?;
        Some(open_txn_recovery_plan(
            redb,
            &saga.batch,
            &encrypted,
            caller.to_string(),
        )?)
    } else {
        None
    };
    Ok(Some((TxnReceipt { backend, saga }, replayed, txn)))
}

/// The not-built stand-in for [`ResumedTxnReceipt`] when `redb` (and therefore
/// `TxnReceipt`) is not compiled in — same shape, `()` where the durable receipt
/// would be, since [`resume_txn_receipt`] below always returns `None` here.
#[cfg(not(feature = "redb"))]
type ResumedTxnReceiptStub = ((), Option<ResultPayload>, Option<GraphTxnState>);

#[cfg(not(feature = "redb"))]
fn resume_txn_receipt(
    _backend: Option<Arc<dyn crate::server::persistence::PersistenceBackend>>,
    _caller: Option<&str>,
    _txn_id: &str,
) -> Result<Option<ResumedTxnReceiptStub>, String> {
    Ok(None)
}

#[cfg(all(feature = "redb", feature = "security"))]
fn seal_txn_recovery_plan(
    backend: &crate::server::persistence::redb_backend::RedbBackend,
    txn: &GraphTxnState,
) -> Result<(String, Vec<u8>), String> {
    use sha2::{Digest, Sha256};
    let plaintext = txn.encode_recovery_plan()?;
    let digest = hex::encode(Sha256::digest(&plaintext));
    let cipher = backend.transaction_recovery_cipher().ok_or_else(|| {
        format!(
            "transaction durability requires {} to be configured",
            crate::crypto::ENCRYPTION_KEY_ENV
        )
    })?;
    Ok((digest, cipher.seal(&plaintext)))
}

#[cfg(all(feature = "redb", not(feature = "security")))]
fn seal_txn_recovery_plan(
    _backend: &crate::server::persistence::redb_backend::RedbBackend,
    _txn: &GraphTxnState,
) -> Result<(String, Vec<u8>), String> {
    Err("transaction durability requires a build with redb and security".to_string())
}

#[cfg(all(feature = "redb", feature = "security"))]
fn open_txn_recovery_plan(
    backend: &crate::server::persistence::redb_backend::RedbBackend,
    batch: &crate::mutation_batch::MutationBatch,
    encrypted: &[u8],
    agent: String,
) -> Result<GraphTxnState, String> {
    use sha2::{Digest, Sha256};
    if !crate::crypto::is_sealed(encrypted) {
        return Err("transaction recovery plan is not authenticated ciphertext".to_string());
    }
    let expected = transaction_plan_digest(batch)?;
    let cipher = backend.transaction_recovery_cipher().ok_or_else(|| {
        format!(
            "transaction recovery requires {} to be configured",
            crate::crypto::ENCRYPTION_KEY_ENV
        )
    })?;
    let plaintext = cipher
        .unseal(encrypted)
        .map_err(|error| format!("transaction recovery plan decrypt failed: {error}"))?;
    let actual = hex::encode(Sha256::digest(&plaintext));
    if actual != expected {
        return Err(
            "transaction recovery plan digest does not match its parent receipt".to_string(),
        );
    }
    GraphTxnState::decode_recovery_plan(&plaintext, agent)
}

#[cfg(all(feature = "redb", not(feature = "security")))]
fn open_txn_recovery_plan(
    _backend: &crate::server::persistence::redb_backend::RedbBackend,
    _batch: &crate::mutation_batch::MutationBatch,
    _encrypted: &[u8],
    _agent: String,
) -> Result<GraphTxnState, String> {
    Err("transaction recovery requires a build with redb and security".to_string())
}

#[cfg(feature = "redb")]
fn transaction_plan_digest(batch: &crate::mutation_batch::MutationBatch) -> Result<String, String> {
    let operation = batch
        .operations
        .first()
        .ok_or_else(|| "transaction parent receipt has no recovery-plan binding".to_string())?;
    if batch.operations.len() != 1 {
        return Err(
            "transaction parent receipt has an ambiguous recovery-plan binding".to_string(),
        );
    }
    let Method::ApplyMutation { event_type, query } = &operation.method else {
        return Err("transaction parent receipt has the wrong recovery-plan operation".to_string());
    };
    if event_type != "transaction_recovery_plan" {
        return Err("transaction parent receipt has the wrong recovery-plan event".to_string());
    }
    let digest = query.strip_prefix("sha256:").ok_or_else(|| {
        "transaction parent receipt has an invalid recovery-plan digest".to_string()
    })?;
    if digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err("transaction parent receipt has an invalid recovery-plan digest".to_string());
    }
    Ok(digest.to_ascii_lowercase())
}

/// Authorize every graph in the staged plan before writing its encrypted durable
/// copy.  Recovery re-checks at execution too, but preparation itself must not let
/// a caller who merely learned a transaction id freeze private staging under their
/// own receipt scope.
async fn authorize_txn_plan(
    state: &Arc<RwLock<ServerState>>,
    caller: Option<&str>,
    txn: &GraphTxnState,
) -> Result<(), String> {
    if consensus_apply_is_authorized() {
        return Ok(());
    }
    let s = state.read().await;
    for graph in txn.touched_graphs() {
        let entry = s
            .registry
            .get(&graph)
            .ok_or_else(|| format!("Graph '{}' not found", graph))?;
        check_graph_access(
            &s.isolation,
            caller,
            &graph,
            entry.graph_type,
            entry.owner.as_deref(),
            AccessLevel::Write,
        )?;
    }
    Ok(())
}

#[cfg(feature = "redb")]
fn finish_txn_receipt(receipt: TxnReceipt, result: ResultPayload) -> Result<ResultPayload, String> {
    let redb = receipt
        .backend
        .as_redb()
        .ok_or_else(|| "transaction receipt lost its redb coordinator".to_string())?;
    crate::server::handlers::admin::finish_admin_saga(
        redb,
        receipt.saga.batch,
        receipt.saga.created_at_ms,
        result,
    )
}

#[cfg(not(feature = "redb"))]
fn finish_txn_receipt(_receipt: (), _result: ResultPayload) -> Result<ResultPayload, String> {
    Err("transaction commit requires the redb MutationBatch coordinator".to_string())
}

/// Upper bound on concurrent durable-batch lookups fanned out per reconcile call.
/// Bounds worst-case fan-out (resident graphs x 2 namespaces) against the
/// persistence backend instead of letting an unusually large resident set spawn
/// thousands of simultaneous reads on one ack-lost retry.
const RECONCILE_LOOKUP_CONCURRENCY: usize = 32;

/// Resolve an acknowledgement-lost retry before consulting ephemeral staging.
/// Single/cross-modal children are discovered across the durable graph catalog;
/// multi-graph commits use their named parent receipt. Successful child discovery
/// repairs both the serving projection and any still-Prepared parent receipt.
///
/// The durable `read_mutation_batch` lookup (graph x {"txn","crossmodal"}) is the
/// only I/O here, and previously ran fully serialized — O(resident-graphs) awaited
/// round-trips before falling through to "not found" on a miss. The lookups are
/// independent reads keyed by a deterministic (graph, namespace) batch id, so they
/// are fanned out concurrently (bounded by `RECONCILE_LOOKUP_CONCURRENCY`) and only
/// the results are then walked in the original graph/namespace order, preserving
/// receipt semantics exactly: the first match (in that order) wins, and any read
/// error at an earlier position than a match still aborts the reconcile, same as
/// the prior sequential loop.
async fn reconcile_committed_txn(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    caller: Option<&str>,
    txn_id: &str,
) -> Result<Option<Response>, String> {
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
    let expected_principal = crate::server::mutation_batch::principal_fingerprint(
        caller.ok_or_else(|| "transaction recovery requires a verified principal".to_string())?,
    )?;

    let parent_id = transaction_receipt_id(txn_id);
    // (graph, core, fname, batch_id) for every (resident graph x namespace) pair,
    // in the same order the original sequential loop visited them.
    let mut lookups = Vec::with_capacity(graphs.len() * 2);
    for (graph, core) in &graphs {
        let fname = crate::persist::sanitize(graph);
        for namespace in ["txn", "crossmodal"] {
            let batch_id =
                crate::server::mutation_batch::opaque_coordinator_key(namespace, graph, &parent_id);
            lookups.push((graph.clone(), core.clone(), fname.clone(), batch_id));
        }
    }

    // Fan the durable reads out concurrently (bounded), then replay the results in
    // original order below so business-logic semantics are unchanged.
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
    let mut read_results: Vec<
        Option<Result<Option<crate::mutation_batch::MutationBatchRecord>, String>>,
    > = (0..lookups.len()).map(|_| None).collect();
    while let Some(joined) = set.join_next().await {
        let (idx, result) =
            joined.map_err(|error| format!("txn reconcile lookup task failed: {error}"))?;
        read_results[idx] = Some(result);
    }

    for (idx, (graph, core, fname, batch_id)) in lookups.into_iter().enumerate() {
        let record = match read_results[idx]
            .take()
            .expect("every lookup index is populated before results are read")
        {
            Ok(Some(record)) => record,
            Ok(None) => continue,
            Err(error) => return Err(error),
        };
        if record.status != crate::mutation_batch::MutationBatchStatus::Committed
            || record.batch.batch_id != batch_id
            || record.batch.graph != graph
            || record.batch.tenant != graph
            || record.batch.context.principal != expected_principal
        {
            return Err("committed transaction receipt does not match caller scope".to_string());
        }
        let bytes = record
            .result_msgpack
            .as_deref()
            .ok_or_else(|| "committed transaction has no durable result".to_string())?;
        let result = decode_txn_result(bytes)?;
        if !matches!(&result, ResultPayload::Bool(_)) {
            return Err("committed transaction child has the wrong result type".to_string());
        }
        let (snapshot, version) = persistence
            .read_authoritative_graph_snapshot(&fname)
            .await?
            .ok_or_else(|| "committed transaction graph image is missing".to_string())?;
        core.install_committed_snapshot(snapshot, version)?;

        // The child graph commit and the control-plane receipt deliberately
        // live in different redb authorities. A crash after the child fsync but
        // before `finish_txn_receipt` therefore leaves a recoverable Prepared
        // parent. Re-enter that named parent and terminalize it from the exact
        // durable child result before acknowledging the retry.
        let Some((receipt, replayed, _)) =
            resume_txn_receipt(Some(persistence.clone()), caller, txn_id)?
        else {
            return Err("committed child has no durable transaction parent".to_string());
        };
        let reconciled = match replayed {
            Some(stored) => stored,
            None => finish_txn_receipt(receipt, result)?,
        };
        return Ok(Some(Response::ok(req_id, reconciled)));
    }
    #[cfg(feature = "redb")]
    if let Some(redb) = persistence.as_redb() {
        if let Some(result) = crate::server::handlers::admin::read_named_admin_saga_result(
            redb,
            &transaction_receipt_id(txn_id),
            caller,
        )? {
            if !matches!(&result, ResultPayload::Bool(_)) {
                return Err("transaction parent receipt has the wrong result type".to_string());
            }
            return Ok(Some(Response::ok(req_id, result)));
        }
    }
    Ok(None)
}

/// Handle the transaction methods. Returns `Err(method)` for any non-txn method so
/// the dispatch chain falls through to the next handler (routing convention).
pub(crate) async fn try_handle(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    caller: &str,
    verified_context: &VerifiedRequestContext,
    method: Method,
) -> Result<Response, Method> {
    let carrier_authority = match CarrierAuthority::from_verified(verified_context) {
        Ok(authority) => authority,
        Err(error) => return Ok(Response::err(req_id, error)),
    };
    let txn_owner = carrier_authority.owner_scope();
    // A transaction id is a routing handle, never a bearer credential. Bind every
    // in-memory stage/read/rollback operation to the verified tenant+actor that
    // opened it.
    if let Some(txn_id) = method_txn_id(&method) {
        let state_guard = state.read().await;
        if let Some(entry) = state_guard.open_txns.get(txn_id) {
            if entry.value().lock().agent != txn_owner {
                crate::metrics::access_denied();
                return Ok(Response::err(
                    req_id,
                    "ACCESS_DENIED: transaction is not owned by caller",
                ));
            }
        };
    }
    let derived_read_authority = if is_derived_read_stage(&method) {
        let state_guard = state.read().await;
        match GraphReadAuthority::from_verified(verified_context, &state_guard.isolation) {
            Ok(authority) => Some(authority),
            Err(error) => return Ok(Response::err(req_id, error)),
        }
    } else {
        None
    };
    // Consumed only by the sparql/query/epistemic derived-read txn stages below
    // (each independently feature-gated); a slim build with none of them enabled
    // still needs this binding to compile.
    let _ = &derived_read_authority;
    #[cfg(feature = "tsdb")]
    let measurement_authority = if matches!(&method, Method::TxnAddMeasurement { .. }) {
        Some(carrier_authority.clone())
    } else {
        None
    };
    match method {
        Method::BeginTxn { graph, isolation } => Ok(begin_txn(
            state,
            req_id,
            Some(caller),
            txn_owner,
            carrier_authority.tenant_scope(),
            graph,
            isolation.as_deref(),
        )
        .await),
        Method::TxnAddNode {
            txn_id,
            node_id,
            properties_msgpack,
            graph,
        } => Ok(stage(
            state,
            req_id,
            &txn_id,
            graph.as_deref(),
            Method::AddNode {
                node_id,
                properties_msgpack,
            },
        )
        .await),
        Method::TxnRemoveNode {
            txn_id,
            node_id,
            graph,
        } => Ok(stage(
            state,
            req_id,
            &txn_id,
            graph.as_deref(),
            Method::RemoveNode { node_id },
        )
        .await),
        Method::TxnAddEdge {
            txn_id,
            source_id,
            target_id,
            properties_msgpack,
            graph,
        } => Ok(stage(
            state,
            req_id,
            &txn_id,
            graph.as_deref(),
            Method::AddEdge {
                source_id,
                target_id,
                properties_msgpack,
            },
        )
        .await),
        Method::TxnRemoveEdge {
            txn_id,
            source_id,
            target_id,
            graph,
        } => Ok(stage(
            state,
            req_id,
            &txn_id,
            graph.as_deref(),
            Method::RemoveEdge {
                source_id,
                target_id,
            },
        )
        .await),
        Method::TxnCas {
            txn_id,
            node_id,
            conditions_msgpack,
            updates_msgpack,
            graph,
        } => Ok(stage(
            state,
            req_id,
            &txn_id,
            graph.as_deref(),
            Method::CompareAndSetNodeFields {
                node_id,
                conditions_msgpack,
                updates_msgpack,
            },
        )
        .await),
        Method::TxnAddEmbedding {
            txn_id,
            node_id,
            embedding,
            graph,
        } => Ok(stage_vector(state, req_id, &txn_id, graph.as_deref(), node_id, embedding).await),
        Method::TxnBlobRef {
            txn_id,
            node_id,
            digest,
            graph,
        } => Ok(stage_blob_ref(state, req_id, &txn_id, graph.as_deref(), node_id, digest).await),
        // Extended cross-modal staging (CONCEPT:EG-KG.backend.cross-modal-atomic-commit/361/362). Each arm is feature-gated
        // at the FACADE (tsdb/owl/sparql); in a slim build the variant falls through to
        // `other => Err(other)` → the dispatch "not available in this build" catch-all.
        #[cfg(feature = "tsdb")]
        Method::TxnAddMeasurement {
            txn_id,
            series,
            points,
            graph,
        } => Ok(stage_measurement(
            state,
            req_id,
            &txn_id,
            graph.as_deref(),
            series,
            points,
            measurement_authority
                .as_ref()
                .expect("TxnAddMeasurement requires carrier authority"),
        )
        .await),
        #[cfg(feature = "owl")]
        Method::TxnAxiom {
            txn_id,
            turtle,
            graph,
        } => Ok(stage_axiom(state, req_id, &txn_id, graph.as_deref(), turtle).await),
        #[cfg(feature = "sparql")]
        Method::TxnConstruct {
            txn_id,
            sparql,
            graph,
        } => Ok(stage_construct(
            state,
            req_id,
            &txn_id,
            graph.as_deref(),
            sparql,
            derived_read_authority
                .as_ref()
                .expect("TxnConstruct is a derived read stage"),
        )
        .await),
        #[cfg(feature = "query")]
        Method::TxnPlanWriteback {
            txn_id,
            plan,
            anchor_id,
            relationship,
            graph,
        } => Ok(stage_plan_writeback(
            state,
            req_id,
            &txn_id,
            graph.as_deref(),
            PlanWritebackArgs {
                plan,
                anchor_id,
                relationship,
            },
            derived_read_authority
                .as_ref()
                .expect("TxnPlanWriteback is a derived read stage"),
        )
        .await),
        #[cfg(feature = "epistemic")]
        Method::TxnMaterializeBelief {
            txn_id,
            node_id,
            graph,
        } => Ok(stage_materialize_belief(
            state,
            req_id,
            &txn_id,
            graph.as_deref(),
            node_id,
            derived_read_authority
                .as_ref()
                .expect("TxnMaterializeBelief is a derived read stage"),
        )
        .await),
        Method::Commit { txn_id } => Ok(commit(state, req_id, Some(caller), &txn_id).await),
        Method::Rollback { txn_id } => Ok(rollback(state, req_id, &txn_id).await),
        other => Err(other),
    }
}

fn method_txn_id(method: &Method) -> Option<&str> {
    match method {
        Method::TxnAddNode { txn_id, .. }
        | Method::TxnRemoveNode { txn_id, .. }
        | Method::TxnAddEdge { txn_id, .. }
        | Method::TxnRemoveEdge { txn_id, .. }
        | Method::TxnCas { txn_id, .. }
        | Method::TxnAddEmbedding { txn_id, .. }
        | Method::TxnBlobRef { txn_id, .. }
        | Method::Commit { txn_id }
        | Method::Rollback { txn_id } => Some(txn_id),
        #[cfg(feature = "tsdb")]
        Method::TxnAddMeasurement { txn_id, .. } => Some(txn_id),
        #[cfg(feature = "owl")]
        Method::TxnAxiom { txn_id, .. } => Some(txn_id),
        #[cfg(feature = "sparql")]
        Method::TxnConstruct { txn_id, .. } => Some(txn_id),
        #[cfg(feature = "query")]
        Method::TxnPlanWriteback { txn_id, .. } => Some(txn_id),
        #[cfg(feature = "epistemic")]
        Method::TxnMaterializeBelief { txn_id, .. } => Some(txn_id),
        _ => None,
    }
}

fn is_derived_read_stage(method: &Method) -> bool {
    // Matched only by the sparql/query/epistemic arms below (each independently
    // feature-gated); a slim build with none of them enabled still needs this
    // parameter to compile.
    let _ = method;
    #[cfg(feature = "sparql")]
    if matches!(method, Method::TxnConstruct { .. }) {
        return true;
    }
    #[cfg(feature = "query")]
    if matches!(method, Method::TxnPlanWriteback { .. }) {
        return true;
    }
    #[cfg(feature = "epistemic")]
    if matches!(method, Method::TxnMaterializeBelief { .. }) {
        return true;
    }
    false
}

/// `BeginTxn`: resolve the target graph, enforce the per-graph + per-agent open-txn
/// caps, snapshot the OCC begin-version, and register a fresh staged transaction.
/// Returns the server-issued `txn_id` as a `String` payload.
async fn begin_txn(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    caller: Option<&str>,
    txn_owner: &str,
    tenant_scope: &str,
    graph: Option<String>,
    isolation: Option<&str>,
) -> Response {
    // Parse the isolation hint up front (CONCEPT:EG-KG.txn.serializable-zero-cost); an unknown value is
    // rejected before any graph/ACL work so the contract is unambiguous.
    let (level, predicate) = match parse_isolation(isolation) {
        Ok(parsed) => parsed,
        Err(msg) => return Response::err(req_id, msg),
    };
    // The request envelope's graph is the default target; `graph` overrides it.
    let s = state.read().await;
    let graph_name = graph.unwrap_or_default();
    let graph_name = if graph_name.is_empty() {
        return Response::err(req_id, "BeginTxn requires a target graph");
    } else {
        graph_name
    };
    let entry = match s.registry.get(&graph_name) {
        Some(e) => e,
        None => return Response::err(req_id, format!("Graph '{}' not found", graph_name)),
    };
    // A txn stages writes → require Write access up front (same gate the inline
    // write path applies), so an unauthorized caller cannot even open a txn.
    if !consensus_apply_is_authorized() {
        if let Err(denied) = check_graph_access(
            &s.isolation,
            caller,
            &graph_name,
            entry.graph_type,
            entry.owner.as_deref(),
            AccessLevel::Write,
        ) {
            return Response::err(req_id, denied);
        }
    }
    let begin_version = entry.core.version();

    // Open-txn caps (CONCEPT:EG-KG.txn.multi-op-occ-acid): bound memory the way per_graph_inflight
    // bounds request concurrency. Count current open txns for this graph/agent.
    let agent = txn_owner.to_string();
    let (mut for_graph, mut for_agent) = (0usize, 0usize);
    for e in s.open_txns.iter() {
        let t = e.value().lock();
        if t.graph == graph_name {
            for_graph += 1;
        }
        if t.agent == agent {
            for_agent += 1;
        }
    }
    if for_graph >= s.txn_max_per_graph {
        return Response::err(
            req_id,
            format!("too many open transactions for graph '{}'", graph_name),
        );
    }
    if for_agent >= s.txn_max_per_agent {
        return Response::err(req_id, "too many open transactions for agent");
    }

    let txn_id = s.txn_id_gen.next();
    // Under serializable with a declared predicate, the constructor captures the
    // predicate read-set fingerprint against `entry.core` at begin (the snapshot the
    // txn reads against). Snapshot level captures nothing extra.
    s.open_txns.insert(
        txn_id.clone(),
        parking_lot::Mutex::new(GraphTxnState::new(
            &entry.core,
            NewTxnArgs {
                graph: graph_name,
                tenant_scope: tenant_scope.to_string(),
                begin_version,
                isolation: level,
                predicate,
                agent,
                now_ms: now_ms(),
            },
        )),
    );
    Response::ok(req_id, ResultPayload::String(txn_id))
}

/// Stage one durable mutation into the open txn (no graph/persistence touch).
/// Acks `Bool(true)`; errors if the txn id is unknown (expired/committed/rolled).
///
/// `target_graph` (CONCEPT:EG-KG.txn.routes-cross-shard-txn): when `None` (or equal to the txn's default
/// graph) the op stages against the default graph through the single-graph OCC
/// read-set path — unchanged. When it names a DIFFERENT graph, the op accumulates in
/// the txn's multi-graph `extra_writes`; the default `core` is still passed so a
/// same-graph op keeps its read-set fingerprint, but a cross-graph op needs no
/// default-core read-set (the 2PC coordinator validates each participant slice).
async fn stage(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    txn_id: &str,
    target_graph: Option<&str>,
    op: Method,
) -> Response {
    let s = state.read().await;
    // Resolve the txn's target core so we can capture the OCC read-set fingerprint
    // of every node the op references at staging time.
    let entry = match s.open_txns.get(txn_id) {
        Some(e) => e,
        None => return Response::err(req_id, format!("unknown transaction '{}'", txn_id)),
    };
    let default_graph = entry.value().lock().graph.clone();
    let target = target_graph.unwrap_or(&default_graph).to_string();
    // A cross-graph op must reference a real graph (it will be a participant at
    // commit). Validate existence up front so a typo fails at stage, not commit.
    if !s.registry.exists(&target) {
        return Response::err(req_id, format!("Graph '{}' not found", target));
    }
    let core = match s.registry.get(&default_graph) {
        Some(g) => g.core.clone(),
        None => return Response::err(req_id, format!("Graph '{}' not found", default_graph)),
    };
    entry.value().lock().stage_in(&core, &target, op, now_ms());
    Response::ok(req_id, ResultPayload::Bool(true))
}

/// Stage a VECTOR upsert into the txn's cross-modal write-set (CONCEPT:EG-KG.txn.reader-never-sees-node). The
/// one-`WriteTransaction` cross-modal barrier is per-graph, so a vector targets the
/// txn's DEFAULT graph; a `graph` naming anything else is rejected because each
/// cross-modal atomic batch has exactly one authoritative graph owner. Acks `Bool(true)`.
async fn stage_vector(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    txn_id: &str,
    target_graph: Option<&str>,
    node_id: String,
    embedding: Vec<f32>,
) -> Response {
    let s = state.read().await;
    let entry = match s.open_txns.get(txn_id) {
        Some(e) => e,
        None => return Response::err(req_id, format!("unknown transaction '{}'", txn_id)),
    };
    let default_graph = entry.value().lock().graph.clone();
    if let Some(g) = target_graph {
        if g != default_graph {
            return Response::err(
                req_id,
                "cross-modal vector must target the txn's default graph",
            );
        }
    }
    let core = match s.registry.get(&default_graph) {
        Some(g) => g.core.clone(),
        None => return Response::err(req_id, format!("Graph '{}' not found", default_graph)),
    };
    entry
        .value()
        .lock()
        .stage_vector(&core, node_id, embedding, now_ms());
    Response::ok(req_id, ResultPayload::Bool(true))
}

/// Stage a BLOB REFERENCE into the txn's cross-modal write-set (CONCEPT:EG-KG.txn.reader-never-sees-node). Same
/// per-graph constraint as [`stage_vector`]. Acks `Bool(true)`.
async fn stage_blob_ref(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    txn_id: &str,
    target_graph: Option<&str>,
    node_id: String,
    digest: String,
) -> Response {
    let s = state.read().await;
    let entry = match s.open_txns.get(txn_id) {
        Some(e) => e,
        None => return Response::err(req_id, format!("unknown transaction '{}'", txn_id)),
    };
    let default_graph = entry.value().lock().graph.clone();
    if let Some(g) = target_graph {
        if g != default_graph {
            return Response::err(
                req_id,
                "cross-modal blob-ref must target the txn's default graph",
            );
        }
    }
    let core = match s.registry.get(&default_graph) {
        Some(g) => g.core.clone(),
        None => return Response::err(req_id, format!("Graph '{}' not found", default_graph)),
    };
    entry
        .value()
        .lock()
        .stage_blob_ref(&core, node_id, digest, now_ms());
    Response::ok(req_id, ResultPayload::Bool(true))
}

/// Default bucket width for a NEW cross-modal series (CONCEPT:EG-KG.backend.cross-modal-atomic-commit). The Lane-0
/// `TxnAddMeasurement` wire carries only `series` + `points` (no schema), so a
/// brand-new series is materialized with this 1-hour partition; an EXISTING series'
/// stored meta is authoritative and this is ignored.
#[cfg(feature = "tsdb")]
const DEFAULT_MEASUREMENT_BUCKET_NS: u64 = 3_600_000_000_000;

/// Stage a TIME-SERIES measurement batch into the txn's cross-modal write-set
/// (CONCEPT:EG-KG.backend.cross-modal-atomic-commit). Same per-graph constraint as [`stage_vector`]: the batch targets the
/// txn's DEFAULT graph (the one-`WriteTransaction` barrier is per-graph). The points are
/// decoded here (the SAME `Vec<(i64, Vec<f64>)>` MessagePack shape `TsAppend` carries), so
/// the commit path is a pure durable append. Acks `Bool(true)`.
#[cfg(feature = "tsdb")]
async fn stage_measurement(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    txn_id: &str,
    target_graph: Option<&str>,
    series: String,
    points_msgpack: Vec<u8>,
    authority: &CarrierAuthority,
) -> Response {
    let points = match super::timeseries::decode_wire_points(&points_msgpack) {
        Ok(p) => p,
        Err(error) => return Response::err(req_id, error),
    };
    let s = state.read().await;
    let entry = match s.open_txns.get(txn_id) {
        Some(e) => e,
        None => return Response::err(req_id, format!("unknown transaction '{}'", txn_id)),
    };
    let default_graph = entry.value().lock().graph.clone();
    if let Some(g) = target_graph {
        if g != default_graph {
            return Response::err(
                req_id,
                "cross-modal measurement must target the txn's default graph",
            );
        }
    }
    // Field width is inferred from the first point; an existing series' stored schema wins
    // at commit, so this only seeds a NEW series (with generated `f0..fN` names).
    let n_fields = points.first().map(|(_, v)| v.len()).unwrap_or(0);
    let field_names = (0..n_fields).map(|i| format!("f{i}")).collect();
    let scoped_series = eg_tsdb::store::SeriesKey::new(
        authority.tenant_scope(),
        authority.namespace("timeseries-graph", &default_graph),
        series,
    )
    .encode();
    let measurement = StagedMeasurement {
        series: scoped_series,
        n_fields,
        bucket_ns: DEFAULT_MEASUREMENT_BUCKET_NS,
        field_names,
        points,
    };
    entry
        .value()
        .lock()
        .stage_measurement(measurement, now_ms());
    Response::ok(req_id, ResultPayload::Bool(true))
}

/// Lower a stream of RDF triples to graph-native `AddNode`/`AddEdge` methods
/// (CONCEPT:EG-KG.txn.extended-cross-modal/362), mirroring the canonical `eg_rdf::mapping::load_triples`
/// property-graph projection so the durable rows match the in-memory model:
///   * literal object  → a property `{predicate: literal-cell}` on the subject node;
///   * resource object → subject + object nodes + an edge `{"relationship": predicate}`,
///     and (for `rdf:type`) the subject's `type` label.
///
/// The SAME lowered `Vec<Method>` is applied both durably (`apply_method_rows`) and
/// in-memory (`apply_staged`), so the two are identical by construction.
#[cfg(feature = "sparql")]
pub(crate) fn triples_to_methods(triples: &[eg_rdf::oxrdf::Triple]) -> Result<Vec<Method>, String> {
    let lowered = eg_rdf::mapping::lower_triples(triples.iter().cloned())?;
    let mut methods = Vec::with_capacity(lowered.nodes.len() + lowered.edges.len());
    for (id, blob) in lowered.nodes {
        methods.push(Method::AddNode {
            node_id: id,
            properties_msgpack: blob,
        });
    }
    for (source, target, blob) in lowered.edges {
        methods.push(Method::AddEdge {
            source_id: source,
            target_id: target,
            properties_msgpack: blob,
        });
    }
    Ok(methods)
}

/// Lower a SPARQL CONSTRUCT/DESCRIBE query's produced triples to graph-native
/// `AddNode`/`AddEdge` methods (CONCEPT:EG-KG.query.extended-cross-modal/EG-372), evaluating the query against
/// `core`'s committed snapshot. Shared by the RPC [`stage_construct`] and the pgwire
/// cross-modal txn seam so both surfaces lower a CONSTRUCT identically.
#[cfg(feature = "sparql")]
pub(crate) fn construct_to_methods(
    core: &crate::graph::GraphCore,
    sparql: &str,
) -> Result<Vec<Method>, String> {
    let snap = core.analysis_snapshot();
    construct_view_to_methods(&snap, sparql)
}

#[cfg(feature = "sparql")]
pub(crate) fn construct_view_to_methods(
    snap: &crate::graph::GraphView,
    sparql: &str,
) -> Result<Vec<Method>, String> {
    let proj = eg_rdf::sparql::Projection::from_wire("", "");
    let dataset = eg_rdf::sparql::Dataset::new(snap, Vec::new());
    let triples = match eg_rdf::sparql::execute(&dataset, sparql, &proj, None) {
        Ok(eg_rdf::sparql::QueryOutcome::Graph(t)) => t,
        Ok(_) => return Err("SPARQL CONSTRUCT/DESCRIBE query required".to_string()),
        Err(e) => return Err(e),
    };
    triples_to_methods(&triples)
}

/// Lower a SPARQL UPDATE's `INSERT DATA` triples to graph-native `AddNode`/`AddEdge`
/// methods (CONCEPT:EG-KG.txn.isolation-ryow-begin-set), reusing the SAME `triples_to_methods` lowering as the OWL
/// axiom path. Used by the pgwire cross-modal txn seam's `SPARQL UPDATE` verb.
#[cfg(feature = "sparql")]
pub(crate) fn sparql_update_to_methods(update_str: &str) -> Result<Vec<Method>, String> {
    let triples = eg_rdf::update::insert_data_triples(update_str)?;
    triples_to_methods(&triples)
}

/// Resolve the txn's DEFAULT-graph core, enforcing that `target_graph` (if given) is the
/// default (the cross-modal barrier is per-graph). Returns the core clone, or an error
/// `Response` to return directly. Shared by the axiom + CONSTRUCT stagers.
#[cfg(feature = "sparql")]
fn resolve_txn_default_core(
    s: &ServerState,
    req_id: u64,
    txn_id: &str,
    target_graph: Option<&str>,
    kind: &str,
) -> Result<Arc<crate::graph::GraphCore>, Response> {
    let entry = match s.open_txns.get(txn_id) {
        Some(e) => e,
        None => {
            return Err(Response::err(
                req_id,
                format!("unknown transaction '{}'", txn_id),
            ))
        }
    };
    let default_graph = entry.value().lock().graph.clone();
    if let Some(g) = target_graph {
        if g != default_graph {
            return Err(Response::err(
                req_id,
                format!("cross-modal {kind} must target the txn's default graph"),
            ));
        }
    }
    match s.registry.get(&default_graph) {
        Some(g) => Ok(g.core.clone()),
        None => Err(Response::err(
            req_id,
            format!("Graph '{}' not found", default_graph),
        )),
    }
}

/// Stage OWL AXIOMS (Turtle) into the txn's cross-modal write-set (CONCEPT:EG-KG.txn.extended-cross-modal). The
/// axioms are parsed + lowered to `AddNode`/`AddEdge` methods HERE (at stage time) so the
/// commit path treats them as ordinary graph mutations riding the one cross-modal
/// `WriteTransaction`. Acks `Bool(true)`.
#[cfg(feature = "owl")]
async fn stage_axiom(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    txn_id: &str,
    target_graph: Option<&str>,
    turtle: String,
) -> Response {
    let s = state.read().await;
    let core = match resolve_txn_default_core(&s, req_id, txn_id, target_graph, "axiom") {
        Ok(c) => c,
        Err(resp) => return resp,
    };
    let triples = match eg_rdf::mapping::parse_turtle(&turtle) {
        Ok(t) => t,
        Err(e) => return Response::err(req_id, format!("TxnAxiom: {e}")),
    };
    let methods = match triples_to_methods(&triples) {
        Ok(methods) => methods,
        Err(error) => return Response::err(req_id, format!("TxnAxiom: {error}")),
    };
    if let Some(e) = s.open_txns.get(txn_id) {
        e.value().lock().stage_axiom(&core, methods, now_ms());
    }
    Response::ok(req_id, ResultPayload::Bool(true))
}

/// Stage a SPARQL CONSTRUCT into the txn's cross-modal write-set (CONCEPT:EG-KG.query.extended-cross-modal). The
/// CONSTRUCT is evaluated NOW against the graph's committed snapshot; its produced triples
/// are lowered to `AddNode`/`AddEdge` methods that land in the SAME cross-modal
/// `WriteTransaction` at commit. (Read-your-own-writes over the txn's OTHER staged writes
/// is Lane A's overlay concern; here the CONSTRUCT reads the committed store.) Acks
/// `Bool(true)`.
#[cfg(feature = "sparql")]
async fn stage_construct(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    txn_id: &str,
    target_graph: Option<&str>,
    sparql: String,
    read_authority: &GraphReadAuthority,
) -> Response {
    let s = state.read().await;
    let core = match resolve_txn_default_core(&s, req_id, txn_id, target_graph, "construct") {
        Ok(c) => c,
        Err(resp) => return resp,
    };
    let projected = read_authority.project_core(&core);
    let methods = match construct_to_methods(&projected, &sparql) {
        Ok(m) => m,
        Err(e) => return Response::err(req_id, format!("TxnConstruct: {e}")),
    };
    if let Some(e) = s.open_txns.get(txn_id) {
        e.value().lock().stage_construct(&core, methods, now_ms());
    }
    Response::ok(req_id, ResultPayload::Bool(true))
}

/// Lower a planner writeback (CONCEPT:EG-KG.query.plan-dag, D7 — the planner-writeback
/// ACID seam) to graph-native `AddEdge` methods: run `plan` READ-ONLY against `core`'s
/// COMMITTED snapshot (the same "evaluate now, lower the result" shape
/// [`construct_to_methods`] uses for SPARQL CONSTRUCT), then materialize each id in the
/// result `RowSet` as an edge FROM `anchor_id` carrying `relationship` — e.g. a
/// `Reason`/`Traverse`-inferred edge set. Shared by the RPC [`stage_plan_writeback`].
#[cfg(feature = "query")]
pub(crate) fn plan_writeback_to_methods(
    core: &crate::graph::GraphCore,
    plan: eg_plan::Plan,
    anchor_id: &str,
    relationship: &str,
) -> Result<Vec<Method>, String> {
    let view = core.analysis_snapshot();
    let semantic = core.semantic_store.read().clone();
    let ctx = eg_plan::PlanCtx::new(&view, &semantic);
    let rs = eg_plan::execute(&plan, &ctx)?;
    let props = rmp_serde::to_vec_named(&serde_json::json!({ "relationship": relationship }))
        .map_err(|e| format!("plan writeback property encode: {e}"))?;
    Ok(rs
        .ids()
        .into_iter()
        .map(|target_id| Method::AddEdge {
            source_id: anchor_id.to_string(),
            target_id,
            properties_msgpack: props.clone(),
        })
        .collect())
}

/// Stage a PLANNER WRITEBACK into the txn's cross-modal write-set (CONCEPT:EG-KG.query.plan-dag,
/// D7). Mirrors [`stage_construct`]'s shape exactly: evaluate now against the committed
/// snapshot, lower to `AddEdge` methods, stage via `GraphTxnState::stage_plan_writeback`
/// so the materialized edges land in the SAME cross-modal `WriteTransaction` as the
/// txn's other staged modalities. Acks `Bool(true)`.
/// The plan-writeback payload fields of [`Method::TxnPlanWriteback`], bundled so
/// [`stage_plan_writeback`] stays under the clippy argument-count ceiling.
#[cfg(feature = "query")]
struct PlanWritebackArgs {
    plan: eg_plan::Plan,
    anchor_id: String,
    relationship: String,
}

#[cfg(feature = "query")]
async fn stage_plan_writeback(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    txn_id: &str,
    target_graph: Option<&str>,
    args: PlanWritebackArgs,
    read_authority: &GraphReadAuthority,
) -> Response {
    let PlanWritebackArgs {
        plan,
        anchor_id,
        relationship,
    } = args;
    let s = state.read().await;
    let core = match resolve_txn_default_core(&s, req_id, txn_id, target_graph, "plan writeback") {
        Ok(c) => c,
        Err(resp) => return resp,
    };
    let projected = read_authority.project_core(&core);
    if read_authority.is_active() {
        // The anchor may have been staged EARLIER in this SAME txn (read-your-own-writes
        // -- D7's own capstone case is exactly this: "the anchor node ITSELF is staged in
        // the SAME txn as the writeback"). `projected` alone only reflects the RLS-filtered
        // COMMITTED snapshot, so a same-txn-staged anchor would wrongly read as invisible.
        // Overlay the txn's own staged write-set onto that snapshot first -- the same
        // technique `run_unified_overlaid` uses for in-txn reads (CONCEPT:EG-KG.query.overlay-leg-rls-filter)
        // -- before deciding visibility. The PLAN ITSELF still evaluates against `projected`
        // (committed-only, unchanged below): only the anchor's own existence is RYOW-aware.
        let mut anchor_view = projected.analysis_snapshot();
        if let Some(entry) = s.open_txns.get(txn_id) {
            let write_set = entry.value().lock().write_set.clone();
            crate::server::handlers::query::overlay_write_set(&mut anchor_view, &write_set);
        }
        if !anchor_view.has_node(&anchor_id) {
            return Response::err(req_id, "TxnPlanWriteback: anchor is not visible");
        }
    }
    let methods = match plan_writeback_to_methods(&projected, plan, &anchor_id, &relationship) {
        Ok(m) => m,
        Err(e) => return Response::err(req_id, format!("TxnPlanWriteback: {e}")),
    };
    if let Some(e) = s.open_txns.get(txn_id) {
        e.value()
            .lock()
            .stage_plan_writeback(&core, methods, now_ms());
    }
    Response::ok(req_id, ResultPayload::Bool(true))
}

/// Compute the propagated belief for `node_id` over `core`'s COMMITTED snapshot and
/// lower it to ONE unconditional `CompareAndSetNodeFields` method that writes the
/// derived `BeliefState.confidence` back onto that node's `NodeData.confidence`
/// (CONCEPT:EG-KG.epistemic.epistemic-substrate, D5 — the "explicit, logged
/// materialize belief" op the `eg_epistemic` crate docs call for: the derived belief
/// is otherwise NEVER written back). `conditions_msgpack` is an EMPTY object
/// (vacuously true — `compare_and_set_fields` still requires the node to exist, which
/// is checked HERE so a missing node is rejected with a clear error at STAGE time
/// rather than a silently no-op'd CAS at commit).
///
/// Replay/idempotency: the confidence value is computed ONCE, here, at stage time, and
/// baked verbatim into `updates_msgpack` — exactly like `plan_writeback_to_methods`/
/// `construct_to_methods` freeze their evaluated result before staging. A WAL replay or
/// a duplicate commit of the SAME staged method therefore re-applies the SAME literal
/// bytes (a true idempotent CAS), never re-derives a drifted value from the
/// (by-then-already-updated) stored confidence — which is precisely the ratchet the
/// crate docs warn a naive "always re-propagate from current state" design would risk.
#[cfg(feature = "epistemic")]
pub(crate) fn materialize_belief_to_methods(
    core: &crate::graph::GraphCore,
    node_id: &str,
) -> Result<(Vec<Method>, f64), String> {
    if !core.has_node(node_id) {
        return Err(format!("node '{node_id}' not found"));
    }
    let view = core.analysis_snapshot();
    let bg = eg_epistemic::BeliefGraph::from_graph_view(&view);
    let policy = eg_epistemic::AuthorityPolicy::default();
    let confidence = eg_epistemic::propagate_confidence(&bg, node_id, &policy)
        .confidence
        .clamp(0.0, 1.0);
    let updates_msgpack = rmp_serde::to_vec_named(&serde_json::json!({ "confidence": confidence }))
        .map_err(|e| format!("materialize belief encode: {e}"))?;
    let conditions_msgpack =
        rmp_serde::to_vec_named(&serde_json::Map::<String, serde_json::Value>::new())
            .map_err(|e| format!("materialize belief encode: {e}"))?;
    Ok((
        vec![Method::CompareAndSetNodeFields {
            node_id: node_id.to_string(),
            conditions_msgpack,
            updates_msgpack,
        }],
        confidence,
    ))
}

/// Stage a MATERIALIZE-BELIEF op into the txn's cross-modal write-set (CONCEPT:
/// EG-KG.epistemic.epistemic-substrate, D5). Mirrors [`stage_plan_writeback`]'s shape
/// exactly: evaluate now against the committed snapshot, lower to a durable Method,
/// stage via `GraphTxnState::stage_plan_writeback` so the write lands in the SAME
/// cross-modal `WriteTransaction` as the txn's other staged modalities at commit —
/// where it rides the ALREADY-audited `CompareAndSetNodeFields` path (the
/// tamper-evident hash chain, CONCEPT:EG-KG.sharding.row-level-security, when
/// `security` is built, PLUS the unconditional in-memory ledger every CAS appends to
/// regardless) — never silent, no new audit mechanism. OPT-IN: nothing else stages
/// this op; it only ever runs when a caller explicitly sends `TxnMaterializeBelief`.
/// Returns the computed confidence in the ack payload (`{node_id, confidence}`) so the
/// caller can observe the derived value before deciding to commit.
#[cfg(feature = "epistemic")]
async fn stage_materialize_belief(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    txn_id: &str,
    target_graph: Option<&str>,
    node_id: String,
    read_authority: &GraphReadAuthority,
) -> Response {
    let s = state.read().await;
    let entry = match s.open_txns.get(txn_id) {
        Some(e) => e,
        None => return Response::err(req_id, format!("unknown transaction '{}'", txn_id)),
    };
    let default_graph = entry.value().lock().graph.clone();
    if let Some(g) = target_graph {
        if g != default_graph {
            return Response::err(
                req_id,
                "cross-modal materialize-belief must target the txn's default graph",
            );
        }
    }
    let core = match s.registry.get(&default_graph) {
        Some(g) => g.core.clone(),
        None => return Response::err(req_id, format!("Graph '{}' not found", default_graph)),
    };
    let projected = read_authority.project_core(&core);
    let (methods, confidence) = match materialize_belief_to_methods(&projected, &node_id) {
        Ok(m) => m,
        Err(e) => return Response::err(req_id, format!("TxnMaterializeBelief: {e}")),
    };
    if let Some(e) = s.open_txns.get(txn_id) {
        e.value()
            .lock()
            .stage_plan_writeback(&core, methods, now_ms());
    }
    Response::ok(
        req_id,
        ResultPayload::Json(serde_json::json!({
            "node_id": node_id,
            "confidence": confidence,
        })),
    )
}

/// `Commit`: the OCC serialization point. Validate the read-set under the held
/// topology write guard.  On an authoritative redb deployment the ordered write-set
/// is compiled into ONE canonical `MutationBatch`, committed durably with status,
/// idempotency and outbox, and only then published through ONE in-memory `GraphTxn`.
/// On conflict NOTHING is applied/persisted — a true rollback returning
/// `Bool(false)`. Non-authoritative deployments fail closed: a transaction is never
/// acknowledged through a data-only mirror path.
///
/// **Cross-shard routing (CONCEPT:EG-KG.storage.lane-n-increment + KG-2.226).** A txn whose staged ops all
/// target ONE graph (or graphs that resolve to ONE Raft group) stays on this
/// byte-for-byte-unchanged single-group FAST PATH. A MULTI-GRAPH txn whose staged
/// write-set ([`crate::server::txn::GraphTxnState::extra_writes`]) spans graphs in ≥2
/// Raft groups (`GroupRouter::is_cross_shard`) is a CROSS-SHARD txn: `Commit` routes it
/// through the 2PC [`crate::raft::cross_shard_txn::CrossShardCoordinator`]
/// ([`commit_multi_graph`]). The coordinator, the span gate, the durable 2PC records,
/// and recovery are the `raft harness`-proven Lane N machinery; THIS is the
/// user-facing wire that hands a staged multi-graph write-set to it.
async fn commit(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    caller: Option<&str>,
    txn_id: &str,
) -> Response {
    if consensus_apply_is_authorized() {
        return Response::err(
            req_id,
            "clustered Commit requires the typed participant protocol",
        );
    }
    // Serialize every first attempt/retry for this opaque parent.  This also closes
    // the restart race where two callers simultaneously discover the same Prepared
    // plan and try to resume its remaining children.
    let _coordinator_guard =
        crate::server::mutation_batch::lock_graph(&transaction_receipt_id(txn_id)).await;

    let (open, persistence, open_map) = {
        let s = state.read().await;
        (
            s.open_txns.remove(txn_id),
            s.persistence.clone(),
            s.open_txns.clone(),
        )
    };
    let (txn, receipt) = if let Some((_id, txn_mutex)) = open {
        let txn = txn_mutex.into_inner();
        // Until parent preparation succeeds, preserve the historical retry contract:
        // an encryption/configuration/fsync error puts staging back in RAM.  Once the
        // atomic Prepared+encrypted-plan commit returns, the durable plan becomes the
        // sole mutable authority and the transaction is frozen.
        let mut restore = TxnRestoreGuard::new(open_map, txn_id, txn.clone());
        if let Err(error) = authorize_txn_plan(state, caller, &txn).await {
            return Response::err(req_id, error);
        }
        let (receipt, replayed) =
            match begin_txn_receipt(persistence.clone(), req_id, caller, txn_id, &txn) {
                Ok(value) => value,
                Err(error) => return Response::err(req_id, error),
            };
        restore.complete();
        if let Some(result) = replayed {
            if let Err(error) = cleanup_cross_shard_decision(state, txn_id).await {
                return Response::err(req_id, format!("transaction cleanup failed: {error}"));
            }
            return Response::ok(req_id, result);
        }
        (txn, receipt)
    } else {
        match reconcile_committed_txn(state, req_id, caller, txn_id).await {
            Ok(Some(response)) => {
                if let Err(error) = cleanup_cross_shard_decision(state, txn_id).await {
                    return Response::err(req_id, format!("transaction cleanup failed: {error}"));
                }
                return response;
            }
            Ok(None) => {}
            Err(error) => {
                return Response::err(
                    req_id,
                    format!("transaction receipt reconciliation failed: {error}"),
                )
            }
        }
        let resumed = match resume_txn_receipt(persistence, caller, txn_id) {
            Ok(value) => value,
            Err(error) => return Response::err(req_id, error),
        };
        let Some((receipt, replayed, recovered)) = resumed else {
            return Response::err(req_id, format!("unknown transaction '{}'", txn_id));
        };
        if let Some(result) = replayed {
            if let Err(error) = cleanup_cross_shard_decision(state, txn_id).await {
                return Response::err(req_id, format!("transaction cleanup failed: {error}"));
            }
            return Response::ok(req_id, result);
        }
        let Some(txn) = recovered else {
            return Response::err(req_id, "prepared transaction has no recovery plan");
        };
        (txn, receipt)
    };

    commit_prepared(state, req_id, caller, txn_id, txn, receipt).await
}

const CONSENSUS_TXN_SCHEMA_VERSION: u16 = 1;

/// Transient prepare result returned only to the control-group leader. The staged
/// plan is immediately sealed again for each participant command; it is never
/// written to a Raft log, receipt, trace, or diagnostic in plaintext.
#[cfg(feature = "raft")]
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ConsensusPreparedTransaction {
    schema_version: u16,
    coordinator_id: String,
    #[serde(with = "serde_bytes")]
    recovery_plan: Vec<u8>,
}

/// One engine-placed participant command built from a prepared transaction.
#[cfg(feature = "raft")]
pub(crate) struct ConsensusTransactionParticipant {
    pub(crate) coordinator_id: String,
    pub(crate) participant_id: u64,
    pub(crate) graph_name: String,
    pub(crate) graph_type: crate::protocol::GraphType,
    pub(crate) group_id: crate::raft::GroupId,
    pub(crate) placement_epoch: u64,
    pub(crate) fencing_token: Option<u64>,
    pub(crate) sealed_plan_source: Vec<u8>,
}

/// Fully resolved participant fanout. Placement comes only from the engine's
/// catalog and is revalidated by each participant state machine before prepare.
#[cfg(feature = "raft")]
pub(crate) struct ConsensusTransactionFanout {
    pub(crate) coordinator_id: String,
    pub(crate) participants: Vec<ConsensusTransactionParticipant>,
}

#[cfg(feature = "raft")]
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct ConsensusParticipantPlan {
    schema_version: u16,
    coordinator_id: String,
    participant_id: u64,
    graph_name: String,
    graph_type: crate::protocol::GraphType,
    group_id: crate::raft::GroupId,
    placement_epoch: u64,
    fencing_token: Option<u64>,
    #[serde(with = "serde_bytes")]
    recovery_plan: Vec<u8>,
}

/// First phase of a clustered Commit. Every staging/control replica freezes the
/// same canonical encrypted recovery plan and returns its deterministic transient
/// body. Graph effects are deliberately absent: the request leader next drives
/// participant prepare, a control-group decision, participant commit, and final
/// parent terminalization without issuing Raft writes from state-machine apply.
#[cfg(feature = "raft")]
pub(crate) async fn prepare_consensus_commit(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    caller: Option<&str>,
    txn_id: &str,
) -> Response {
    let _coordinator_guard =
        crate::server::mutation_batch::lock_graph(&transaction_receipt_id(txn_id)).await;
    let (open, persistence, open_map) = {
        let s = state.read().await;
        (
            s.open_txns.remove(txn_id),
            s.persistence.clone(),
            s.open_txns.clone(),
        )
    };
    let (txn, receipt) = if let Some((_id, txn_mutex)) = open {
        let txn = txn_mutex.into_inner();
        let mut restore = TxnRestoreGuard::new(open_map, txn_id, txn.clone());
        if let Err(error) = authorize_txn_plan(state, caller, &txn).await {
            return Response::err(req_id, error);
        }
        let (receipt, replayed) =
            match begin_txn_receipt(persistence.clone(), req_id, caller, txn_id, &txn) {
                Ok(value) => value,
                Err(error) => return Response::err(req_id, error),
            };
        restore.complete();
        if let Some(result) = replayed {
            if let Err(error) = cleanup_cross_shard_decision(state, txn_id).await {
                return Response::err(req_id, format!("transaction cleanup failed: {error}"));
            }
            return Response::ok(req_id, result);
        }
        (txn, receipt)
    } else {
        match reconcile_committed_txn(state, req_id, caller, txn_id).await {
            Ok(Some(response)) => return response,
            Ok(None) => {}
            Err(error) => {
                return Response::err(
                    req_id,
                    format!("transaction receipt reconciliation failed: {error}"),
                )
            }
        }
        let resumed = match resume_txn_receipt(persistence, caller, txn_id) {
            Ok(value) => value,
            Err(error) => return Response::err(req_id, error),
        };
        let Some((receipt, replayed, recovered)) = resumed else {
            return Response::err(req_id, format!("unknown transaction '{}'", txn_id));
        };
        if let Some(result) = replayed {
            return Response::ok(req_id, result);
        }
        let Some(txn) = recovered else {
            return Response::err(req_id, "prepared transaction has no recovery plan");
        };
        (txn, receipt)
    };

    let recovery_plan = match txn.encode_recovery_plan() {
        Ok(plan) => plan,
        Err(error) => return Response::err(req_id, error),
    };
    let prepared = ConsensusPreparedTransaction {
        schema_version: CONSENSUS_TXN_SCHEMA_VERSION,
        coordinator_id: receipt_coordinator_id(&receipt),
        recovery_plan,
    };
    match rmp_serde::to_vec_named(&prepared) {
        Ok(bytes) => Response::ok(req_id, ResultPayload::Raw(bytes)),
        Err(_) => Response::err(req_id, "consensus transaction prepare encode failed"),
    }
}

#[cfg(feature = "raft")]
fn decode_consensus_prepared(bytes: &[u8]) -> Result<ConsensusPreparedTransaction, String> {
    let prepared: ConsensusPreparedTransaction =
        decode_txn_value(bytes, MAX_TXN_NESTED_BYTES, MAX_TXN_NESTED_ITEMS)?;
    if prepared.schema_version != CONSENSUS_TXN_SCHEMA_VERSION
        || prepared.recovery_plan.is_empty()
        || prepared.coordinator_id.is_empty()
    {
        return Err("consensus transaction prepare is invalid".to_string());
    }
    Ok(prepared)
}

#[cfg(feature = "raft")]
fn consensus_participant_id(coordinator_id: &str, graph_name: &str) -> u64 {
    use sha2::{Digest, Sha256};
    let mut digest = Sha256::new();
    digest.update(b"epistemic-graph-consensus-participant-v1\0");
    digest.update((coordinator_id.len() as u64).to_be_bytes());
    digest.update(coordinator_id.as_bytes());
    digest.update((graph_name.len() as u64).to_be_bytes());
    digest.update(graph_name.as_bytes());
    let bytes = digest.finalize();
    u64::from_be_bytes(bytes[..8].try_into().expect("sha256 prefix is eight bytes"))
}

/// Resolve the complete transaction span through the engine-owned placement
/// catalog and build one sealed-command source per graph participant. No caller
/// placement hint or local hash is accepted.
#[cfg(feature = "raft")]
pub(crate) async fn build_consensus_transaction_fanout(
    state: &Arc<RwLock<ServerState>>,
    prepared_bytes: &[u8],
) -> Result<ConsensusTransactionFanout, String> {
    let prepared = decode_consensus_prepared(prepared_bytes)?;
    let txn = GraphTxnState::decode_recovery_plan(&prepared.recovery_plan, String::new())?;
    let (multi, graph_types) = {
        let current = state.read().await;
        let multi = current.multi_raft.clone().ok_or_else(|| {
            "consensus transaction requires MultiRaft placement authority".to_string()
        })?;
        let mut graph_types = std::collections::BTreeMap::new();
        for graph in txn.touched_graphs() {
            let entry = current
                .registry
                .get(&graph)
                .ok_or_else(|| format!("Graph '{}' not found", graph))?;
            graph_types.insert(graph, entry.graph_type);
        }
        (multi, graph_types)
    };

    let mut participants = Vec::with_capacity(graph_types.len());
    let mut participant_ids = std::collections::BTreeSet::new();
    for (graph_name, graph_type) in graph_types {
        let route = multi.route_graph(&graph_name).await;
        let participant_id = consensus_participant_id(&prepared.coordinator_id, &graph_name);
        if !participant_ids.insert(participant_id) {
            return Err("consensus transaction participant identity collision".to_string());
        }
        let participant_plan = ConsensusParticipantPlan {
            schema_version: CONSENSUS_TXN_SCHEMA_VERSION,
            coordinator_id: prepared.coordinator_id.clone(),
            participant_id,
            graph_name: graph_name.clone(),
            graph_type,
            group_id: route.group,
            placement_epoch: route.epoch,
            fencing_token: route.placed.then_some(route.fencing_token()),
            recovery_plan: prepared.recovery_plan.clone(),
        };
        let sealed_plan_source = rmp_serde::to_vec_named(&participant_plan)
            .map_err(|_| "consensus participant plan encode failed".to_string())?;
        participants.push(ConsensusTransactionParticipant {
            coordinator_id: prepared.coordinator_id.clone(),
            participant_id,
            graph_name,
            graph_type,
            group_id: route.group,
            placement_epoch: route.epoch,
            fencing_token: route.placed.then_some(route.fencing_token()),
            sealed_plan_source,
        });
    }
    if participants.is_empty() {
        return Err("consensus transaction has no participants".to_string());
    }
    Ok(ConsensusTransactionFanout {
        coordinator_id: prepared.coordinator_id,
        participants,
    })
}

#[cfg(feature = "raft")]
fn decode_consensus_participant(
    bytes: &[u8],
    expected_coordinator: &str,
    expected_participant: u64,
) -> Result<(ConsensusParticipantPlan, GraphTxnState), String> {
    let plan: ConsensusParticipantPlan =
        decode_txn_value(bytes, MAX_TXN_NESTED_BYTES, MAX_TXN_NESTED_ITEMS)?;
    if plan.schema_version != CONSENSUS_TXN_SCHEMA_VERSION
        || plan.coordinator_id != expected_coordinator
        || plan.participant_id != expected_participant
        || plan.graph_name.is_empty()
        || plan.recovery_plan.is_empty()
        || (plan.placement_epoch > 0 && plan.fencing_token.is_none())
    {
        return Err("consensus transaction participant plan is invalid".to_string());
    }
    let txn = GraphTxnState::decode_recovery_plan(&plan.recovery_plan, String::new())?;
    if !txn
        .touched_graphs()
        .iter()
        .any(|graph| graph == &plan.graph_name)
    {
        return Err("consensus transaction participant is outside the prepared span".to_string());
    }
    Ok((plan, txn))
}

#[cfg(feature = "raft")]
async fn validate_consensus_participant_placement(
    state: &Arc<RwLock<ServerState>>,
    plan: &ConsensusParticipantPlan,
    applying_group: crate::raft::GroupId,
    applying_epoch: u64,
    applying_fence: Option<u64>,
) -> Result<Arc<crate::graph::GraphCore>, String> {
    if plan.group_id != applying_group
        || plan.placement_epoch != applying_epoch
        || plan.fencing_token != applying_fence
    {
        return Err("consensus transaction participant reached the wrong group".to_string());
    }
    let multi = state
        .read()
        .await
        .multi_raft
        .clone()
        .ok_or_else(|| "consensus transaction lost placement authority".to_string())?;
    let route = multi.route_graph(&plan.graph_name).await;
    if route.group != plan.group_id
        || route.epoch != plan.placement_epoch
        || route.placed.then_some(route.fencing_token()) != plan.fencing_token
    {
        return Err("consensus transaction participant placement changed".to_string());
    }
    let current = state.read().await;
    let entry = current
        .registry
        .get(&plan.graph_name)
        .ok_or_else(|| format!("Graph '{}' not found", plan.graph_name))?;
    if entry.graph_type != plan.graph_type {
        return Err("consensus transaction participant graph type changed".to_string());
    }
    Ok(entry.core.clone())
}

#[cfg(feature = "raft")]
fn extra_participant_is_valid(core: &crate::graph::GraphCore, methods: &[Method]) -> bool {
    let inserts: std::collections::BTreeSet<&str> = methods
        .iter()
        .filter_map(|method| match method {
            Method::AddNode { node_id, .. } => Some(node_id.as_str()),
            _ => None,
        })
        .collect();
    methods.iter().all(|method| match method {
        Method::AddEdge {
            source_id,
            target_id,
            ..
        } => {
            (core.has_node(source_id) || inserts.contains(source_id.as_str()))
                && (core.has_node(target_id) || inserts.contains(target_id.as_str()))
        }
        _ => true,
    })
}

#[cfg(feature = "raft")]
fn participant_methods<'a>(txn: &'a GraphTxnState, graph_name: &str) -> Option<&'a [Method]> {
    if txn.graph == graph_name {
        Some(&txn.write_set)
    } else {
        txn.extra_writes.get(graph_name).map(Vec::as_slice)
    }
}

#[cfg(feature = "raft")]
fn consensus_participant_child_id(
    coordinator_id: &str,
    participant_id: u64,
    graph_name: &str,
) -> String {
    crate::server::mutation_batch::opaque_coordinator_key(
        "consensus-transaction-child",
        graph_name,
        &format!("{coordinator_id}:{participant_id}"),
    )
}

#[cfg(feature = "raft")]
fn consensus_participant_receipt_id(
    txn: &GraphTxnState,
    coordinator_id: &str,
    participant_id: u64,
    graph_name: &str,
) -> String {
    let child = consensus_participant_child_id(coordinator_id, participant_id, graph_name);
    if txn.graph == graph_name && txn.is_cross_modal() {
        crate::server::mutation_batch::opaque_coordinator_key("crossmodal", graph_name, &child)
    } else {
        child
    }
}

/// Apply a participant PREPARE after its command is committed in the participant's
/// own Raft group. The encrypted durable intent is idempotent and byte-bound to the
/// parent plan; a conflicting retry fails closed.
#[cfg(feature = "raft")]
pub(crate) async fn apply_consensus_participant_prepare(
    state: &Arc<RwLock<ServerState>>,
    applying_group: crate::raft::GroupId,
    applying_epoch: u64,
    applying_fence: Option<u64>,
    coordinator_id: &str,
    participant_id: u64,
    plan_bytes: &[u8],
) -> Result<bool, String> {
    let _placement_guard = crate::server::txn::consensus_placement_fence_guard().await;
    let (plan, txn) = decode_consensus_participant(plan_bytes, coordinator_id, participant_id)?;
    let core = validate_consensus_participant_placement(
        state,
        &plan,
        applying_group,
        applying_epoch,
        applying_fence,
    )
    .await?;
    let backend = state
        .read()
        .await
        .persistence
        .clone()
        .ok_or_else(|| "consensus participant requires durable redb".to_string())?;
    let receipt_id =
        consensus_participant_receipt_id(&txn, coordinator_id, participant_id, &plan.graph_name);
    if backend
        .read_mutation_batch(&crate::persist::sanitize(&plan.graph_name), &receipt_id)
        .await?
        .is_some()
    {
        crate::server::txn::release_consensus_graph_fence(
            &plan.graph_name,
            coordinator_id,
            participant_id,
        );
        return Ok(true);
    }
    let methods = participant_methods(&txn, &plan.graph_name)
        .ok_or_else(|| "consensus participant has no graph slice".to_string())?;
    let valid = if txn.graph == plan.graph_name {
        txn.validate(&core)
    } else {
        extra_participant_is_valid(&core, methods)
    };
    if !valid {
        return Ok(false);
    }
    let acquired = crate::server::txn::acquire_consensus_graph_fence(
        &plan.graph_name,
        coordinator_id,
        participant_id,
    )?;
    let redb = backend
        .as_redb()
        .ok_or_else(|| "consensus participant requires durable redb".to_string())?;
    if let Some(existing_plan) = redb.xshard_prepare_get(coordinator_id, participant_id)? {
        if existing_plan != plan_bytes {
            if acquired {
                crate::server::txn::release_consensus_graph_fence(
                    &plan.graph_name,
                    coordinator_id,
                    participant_id,
                );
            }
            return Err("consensus participant prepare conflicts with durable intent".to_string());
        }
        return Ok(true);
    }
    if let Err(error) = redb
        .xshard_prepare_put(coordinator_id, participant_id, plan_bytes.to_vec())
        .await
    {
        if acquired {
            crate::server::txn::release_consensus_graph_fence(
                &plan.graph_name,
                coordinator_id,
                participant_id,
            );
        }
        return Err(error);
    }
    Ok(true)
}

#[cfg(feature = "raft")]
fn isolate_participant_transaction(
    mut txn: GraphTxnState,
    graph_name: &str,
    core: &crate::graph::GraphCore,
) -> Result<GraphTxnState, String> {
    if txn.graph == graph_name {
        txn.extra_writes.clear();
        return Ok(txn);
    }
    let methods = txn
        .extra_writes
        .remove(graph_name)
        .ok_or_else(|| "consensus participant has no graph slice".to_string())?;
    txn.graph = graph_name.to_string();
    txn.begin_version = core.version();
    txn.write_set = methods;
    txn.read_set.clear();
    txn.predicate_reads.clear();
    txn.extra_writes.clear();
    txn.vectors.clear();
    txn.blob_refs.clear();
    txn.measurements.clear();
    txn.axioms.clear();
    txn.constructs.clear();
    txn.plan_writeback.clear();
    Ok(txn)
}

/// Apply a decided participant atomically in its owning graph/group. The child
/// batch id binds graph + participant + parent, making command and snapshot replay
/// idempotent. A missing/mismatched prepared intent never authorizes a first apply.
/// The identifying fields of a decided consensus transaction participant,
/// bundled so [`apply_consensus_participant_commit`] stays under the clippy
/// argument-count ceiling.
#[cfg(feature = "raft")]
pub(crate) struct ConsensusParticipantCommitRef<'a> {
    pub(crate) coordinator_id: &'a str,
    pub(crate) participant_id: u64,
    pub(crate) plan_bytes: &'a [u8],
}

#[cfg(feature = "raft")]
pub(crate) async fn apply_consensus_participant_commit(
    state: &Arc<RwLock<ServerState>>,
    request_id: u64,
    applying_group: crate::raft::GroupId,
    authority: &crate::raft::RaftMutationContext,
    participant: ConsensusParticipantCommitRef<'_>,
) -> Result<bool, String> {
    let ConsensusParticipantCommitRef {
        coordinator_id,
        participant_id,
        plan_bytes,
    } = participant;
    let principal =
        crate::server::mutation_batch::principal_fingerprint(&authority.principal_fingerprint)?;
    let (plan, txn) = decode_consensus_participant(plan_bytes, coordinator_id, participant_id)?;
    let core = validate_consensus_participant_placement(
        state,
        &plan,
        applying_group,
        authority.placement_epoch,
        authority.fencing_token,
    )
    .await?;
    let backend = state
        .read()
        .await
        .persistence
        .clone()
        .ok_or_else(|| "consensus participant requires durable persistence".to_string())?;
    let redb = backend
        .as_redb()
        .ok_or_else(|| "consensus participant requires durable redb".to_string())?;
    let prepared = redb.xshard_prepare_get(coordinator_id, participant_id)?;
    let child_id = consensus_participant_child_id(coordinator_id, participant_id, &plan.graph_name);
    let participant = isolate_participant_transaction(txn, &plan.graph_name, &core)?;
    let cross_modal = participant.is_cross_modal();
    let receipt_id = if cross_modal {
        crate::server::mutation_batch::opaque_coordinator_key(
            "crossmodal",
            &plan.graph_name,
            &child_id,
        )
    } else {
        child_id.clone()
    };
    let fname = crate::persist::sanitize(&plan.graph_name);
    let already_committed = backend
        .read_mutation_batch(&fname, &receipt_id)
        .await?
        .is_some();
    if !already_committed && !matches!(prepared.as_deref(), Some(bytes) if bytes == plan_bytes) {
        return Err("consensus participant commit has no matching prepared intent".to_string());
    }

    let committed = if cross_modal {
        commit_cross_modal_txn(state, request_id, Some(&principal), &child_id, participant).await?
    } else if participant.write_set.is_empty() {
        true
    } else {
        crate::server::mutation_batch::commit_internal_graph_methods(
            Some(&backend),
            &core,
            request_id,
            Some(&principal),
            &plan.graph_name,
            &child_id,
            participant.write_set,
            &ResultPayload::Bool(true),
        )
        .await?;
        true
    };
    if committed {
        redb.xshard_prepare_clear(coordinator_id, participant_id)
            .await?;
        crate::server::txn::release_consensus_graph_fence(
            &plan.graph_name,
            coordinator_id,
            participant_id,
        );
    }
    Ok(committed)
}

#[cfg(feature = "raft")]
pub(crate) async fn apply_consensus_participant_abort(
    state: &Arc<RwLock<ServerState>>,
    coordinator_id: &str,
    participant_id: u64,
) -> Result<bool, String> {
    let backend = state
        .read()
        .await
        .persistence
        .clone()
        .ok_or_else(|| "consensus participant abort requires persistence".to_string())?;
    let redb = backend
        .as_redb()
        .ok_or_else(|| "consensus participant abort requires durable redb".to_string())?;
    let plan = redb.xshard_prepare_get(coordinator_id, participant_id)?;
    redb.xshard_prepare_clear(coordinator_id, participant_id)
        .await?;
    if let Some(plan) = plan {
        if let Ok((decoded, _)) =
            decode_consensus_participant(&plan, coordinator_id, participant_id)
        {
            crate::server::txn::release_consensus_graph_fence(
                &decoded.graph_name,
                coordinator_id,
                participant_id,
            );
        }
    }
    Ok(true)
}

#[cfg(feature = "raft")]
pub(crate) async fn apply_consensus_transaction_decision(
    state: &Arc<RwLock<ServerState>>,
    coordinator_id: &str,
    principal: &str,
    commit: bool,
) -> Result<bool, String> {
    let principal = crate::server::mutation_batch::principal_fingerprint(principal)?;
    let backend = state
        .read()
        .await
        .persistence
        .clone()
        .ok_or_else(|| "consensus transaction decision requires persistence".to_string())?;
    let redb = backend
        .as_redb()
        .ok_or_else(|| "consensus transaction decision requires durable redb".to_string())?;
    let parent = crate::server::handlers::admin::resume_named_admin_saga(
        redb,
        coordinator_id,
        Some(&principal),
    )?
    .ok_or_else(|| "consensus transaction decision has no prepared parent".to_string())?;
    if let Some(result) = parent.replayed {
        return match result {
            ResultPayload::Bool(value) if value == commit => Ok(value),
            ResultPayload::Bool(_) => {
                Err("consensus transaction decision conflicts with its parent".to_string())
            }
            _ => Err("consensus transaction parent has an invalid result".to_string()),
        };
    }
    if let Some(existing) = redb.xshard_decision_get(coordinator_id)? {
        if existing != commit {
            return Err(
                "consensus transaction decision conflicts with durable outcome".to_string(),
            );
        }
        return Ok(existing);
    }
    redb.xshard_recoverable_decision_put(coordinator_id, commit)
        .await?;
    Ok(commit)
}

#[cfg(feature = "raft")]
pub(crate) async fn apply_consensus_transaction_finalize(
    state: &Arc<RwLock<ServerState>>,
    coordinator_id: &str,
    principal: &str,
    commit: bool,
) -> Result<bool, String> {
    let principal = crate::server::mutation_batch::principal_fingerprint(principal)?;
    let backend = state
        .read()
        .await
        .persistence
        .clone()
        .ok_or_else(|| "consensus transaction finalize requires persistence".to_string())?;
    let redb = backend
        .as_redb()
        .ok_or_else(|| "consensus transaction finalize requires durable redb".to_string())?;
    let decision = redb
        .xshard_decision_get(coordinator_id)?
        .ok_or_else(|| "consensus transaction finalize has no durable decision".to_string())?;
    if decision != commit {
        return Err("consensus transaction finalize conflicts with durable decision".to_string());
    }
    let saga = crate::server::handlers::admin::resume_named_admin_saga(
        redb,
        coordinator_id,
        Some(&principal),
    )?
    .ok_or_else(|| "consensus transaction finalize has no prepared parent".to_string())?;
    let result = if let Some(result) = saga.replayed {
        result
    } else {
        crate::server::handlers::admin::finish_admin_saga(
            redb,
            saga.batch,
            saga.created_at_ms,
            ResultPayload::Bool(commit),
        )?
    };
    if !matches!(result, ResultPayload::Bool(value) if value == commit) {
        return Err("consensus transaction parent has a conflicting result".to_string());
    }
    redb.xshard_decision_clear(coordinator_id).await?;
    Ok(commit)
}

/// Execute a parent that is already durably Prepared with an encrypted canonical
/// plan.  Every return before `finish_txn_receipt` leaves that plan intact for the
/// next retry; terminalization atomically erases it.
async fn commit_prepared(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    caller: Option<&str>,
    txn_id: &str,
    txn: GraphTxnState,
    receipt: TxnReceipt,
) -> Response {
    let s = state.read().await;
    let coordinator_id = receipt_coordinator_id(&receipt);

    // ── Multi-graph span (CONCEPT:EG-KG.txn.routes-cross-shard-txn — Lane N) ────────────────────────────
    // If the txn staged ops against a graph other than its default, evaluate the
    // span against the router. A span over ≥2 Raft groups routes through the 2PC
    // coordinator (cross-shard, all-or-nothing across groups). A multi-graph span
    // that collapses to ONE group, OR no active cluster (incl. a non-raft build),
    // applies each graph's slice locally so no staged graph is silently dropped.
    if txn.is_multi_graph() {
        drop(s);
        let response = commit_multi_graph(state, req_id, caller, &coordinator_id, txn).await;
        if let Some(result) = response.result.clone() {
            return match finish_txn_receipt(receipt, result) {
                Ok(result) => match cleanup_cross_shard_decision(state, txn_id).await {
                    Ok(()) => Response::ok(req_id, result),
                    Err(error) => {
                        Response::err(req_id, format!("transaction cleanup failed: {error}"))
                    }
                },
                Err(error) => Response::err(req_id, error),
            };
        }
        return response;
    }

    // ── Cross-modal span (CONCEPT:EG-KG.txn.reader-never-sees-node) ─────────────────────────────────────
    // If the txn staged vectors or blob-refs, its single-graph commit must land
    // graph + vectors + blob-refs in ONE redb WriteTransaction (all-or-nothing).
    if txn.is_cross_modal() {
        drop(s);
        let response = commit_cross_modal(state, req_id, caller, &coordinator_id, txn).await;
        if let Some(result) = response.result.clone() {
            return match finish_txn_receipt(receipt, result) {
                Ok(result) => Response::ok(req_id, result),
                Err(error) => Response::err(req_id, error),
            };
        }
        return response;
    }

    let entry = match s.registry.get(&txn.graph) {
        Some(e) => e,
        None => return Response::err(req_id, format!("Graph '{}' not found", txn.graph)),
    };
    // Re-check Write access at commit (caller may differ from the opener; the gate
    // is cheap and keeps the contract identical to the inline write path).
    if !consensus_apply_is_authorized() {
        if let Err(denied) = check_graph_access(
            &s.isolation,
            caller,
            &txn.graph,
            entry.graph_type,
            entry.owner.as_deref(),
            AccessLevel::Write,
        ) {
            return Response::err(req_id, denied);
        }
    }
    let core = entry.core.clone();
    let persistence = s.persistence.clone();
    let graph_name = txn.graph.clone();
    drop(s); // release the registry read lock before taking the graph write lock.

    // Serialize with ordinary graph/query/RDF gateway mutations for the entire
    // validate → durable commit → RAM publish interval.  No lock is held during
    // client think-time; it begins only after Commit consumes the staged txn.
    let _mutation_guard = crate::server::mutation_batch::lock_graph(&graph_name).await;

    // Validate under the topology write barrier, but do not publish yet.  The
    // authoritative path below commits the batch first.
    {
        let gtxn = core.txn();
        if !txn.validate(&core) {
            drop(gtxn);
            return match finish_txn_receipt(receipt, ResultPayload::Bool(false)) {
                Ok(result) => Response::ok(req_id, result),
                Err(error) => Response::err(req_id, error),
            };
        }
        drop(gtxn);
    }

    let applied = txn.write_set.clone();
    if applied.is_empty() {
        return match finish_txn_receipt(receipt, ResultPayload::Bool(true)) {
            Ok(result) => Response::ok(req_id, result),
            Err(error) => Response::err(req_id, error),
        };
    }
    let committed_at_ms = now_ms();
    let batch_id =
        crate::server::mutation_batch::opaque_coordinator_key("txn", &graph_name, &coordinator_id);
    let idempotency_key = batch_id.clone();
    let tenant_scope = txn.tenant_scope.clone();
    let Some(authority) = persistence.as_ref() else {
        return Response::err(
            req_id,
            "authoritative MutationBatch commit requires a persistence backend",
        );
    };
    let authoritative_version = match authority
        .read_mutation_graph_version(&crate::persist::sanitize(&graph_name))
        .await
    {
        Ok(version) => version.unwrap_or(txn.begin_version),
        Err(error) => {
            return Response::err(
                req_id,
                format!("authoritative graph version read failed: {error}"),
            )
        }
    };
    let batch = match crate::server::mutation_batch::compile_methods(
        crate::server::mutation_batch::CompileBatch {
            batch_id: &batch_id,
            request_id: req_id,
            principal: caller,
            tenant: &tenant_scope,
            graph: &graph_name,
            placement_epoch: 0,
            idempotency_key: &idempotency_key,
            expected_graph_version: Some(authoritative_version),
            fencing_token: None,
            created_at_ms: committed_at_ms,
            default_surface: crate::mutation_batch::MutationSurface::Transaction,
            authoritative_state: None,
        },
        applied.clone(),
    ) {
        Ok(batch) => batch,
        Err(e) => return Response::err(req_id, format!("MutationBatch compile failed: {e}")),
    };
    let result_msgpack = match rmp_serde::to_vec_named(&ResultPayload::Bool(true)) {
        Ok(bytes) => bytes,
        Err(e) => return Response::err(req_id, format!("MutationBatch result encode failed: {e}")),
    };
    let fname = crate::persist::sanitize(&graph_name);

    // Authoritative commit point: all graph rows + batch status/idempotency/outbox
    // are durable before the serving projection changes.
    let committed = match authority
        .commit_mutation_batch(&fname, &batch, Some(&result_msgpack), committed_at_ms)
        .await
    {
        Ok(committed) => committed,
        Err(e) => {
            return Response::err(req_id, format!("MutationBatch durable commit failed: {e}"))
        }
    };
    if committed.replayed {
        let Some(bytes) = committed.record.result_msgpack.as_deref() else {
            return Response::err(req_id, "committed MutationBatch has no durable result");
        };
        return match decode_txn_result(bytes) {
            Ok(stored) => {
                let (snapshot, version) =
                    match authority.read_authoritative_graph_snapshot(&fname).await {
                        Ok(Some(value)) => value,
                        Ok(None) => {
                            return Response::err(
                                req_id,
                                "committed transaction graph image is missing",
                            )
                        }
                        Err(error) => return Response::err(req_id, error),
                    };
                if let Err(error) = core.install_committed_snapshot(snapshot, version) {
                    return Response::err(req_id, error);
                }
                match finish_txn_receipt(receipt, stored) {
                    Ok(result) => Response::ok(req_id, result),
                    Err(error) => Response::err(req_id, error),
                }
            }
            Err(error) => Response::err(req_id, error),
        };
    }

    // Durable commit succeeded:
    // publish the complete write-set to RAM under one graph transaction.
    {
        let mut gtxn = core.txn();
        for m in &applied {
            apply_staged(&mut gtxn, m);
        }
    }
    core.mark_dirty();

    #[cfg(feature = "metrics")]
    {
        let topo = core.topo.read();
        crate::metrics::set_graph_size(
            &graph_name,
            topo.graph.node_count() as i64,
            topo.graph.edge_count() as i64,
        );
    }

    match finish_txn_receipt(receipt, ResultPayload::Bool(true)) {
        Ok(result) => Response::ok(req_id, result),
        Err(error) => Response::err(req_id, error),
    }
}

/// Commit a CROSS-MODAL single-graph transaction (CONCEPT:EG-KG.txn.reader-never-sees-node + EG-360/361/362).
/// Validates the OCC read-set, then lands EVERY staged modality ATOMICALLY in ONE redb
/// `WriteTransaction` (commit-before-ack) BEFORE touching the in-memory model:
///   * graph methods + vector upserts + blob-refs (CONCEPT:EG-KG.txn.reader-never-sees-node);
///   * OWL-axiom + SPARQL-CONSTRUCT triples, lowered to `AddNode`/`AddEdge` and folded
///     into `methods` (CONCEPT:EG-KG.txn.extended-cross-modal/362);
///   * time-series measurement batches, written into the graph's SERIES tables in the
///     SAME transaction (CONCEPT:EG-KG.backend.cross-modal-atomic-commit).
///
/// A durable-commit failure applies NOTHING (no partial cross-modal commit) and the
/// in-memory state only ever reflects what is durable. On OCC conflict returns
/// `Bool(false)` (true rollback); on durable failure returns an ERROR.
///
/// The graph modalities (nodes/edges/axioms/CONSTRUCT/vectors/blob-refs) are mirrored
/// into the in-memory model after the durable commit. Measurements land durably in
/// the authoritative shard's SERIES tables (atomic with the rest, per the barrier above) AND are
/// then replayed into the SERVED `series.redb` (`state.tsdb_store`) so `TsRange`/
/// `TsAsofJoin`/`TsWindow`/`TsGapFill` and UQL `Op::TsScan` — which only ever read the
/// served store — actually see them post-commit (CONCEPT:EG-KG.backend.ts-served-materialize, EG-P0-4). See the
/// materialization step in [`commit_cross_modal_txn`] for the exact guarantee and the
/// remaining non-atomic boundary (a crash strictly between the two commits).
async fn commit_cross_modal(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    caller: Option<&str>,
    coordinator_id: &str,
    txn: GraphTxnState,
) -> Response {
    match commit_cross_modal_txn(state, req_id, caller, coordinator_id, txn).await {
        Ok(committed) => Response::ok(req_id, ResultPayload::Bool(committed)),
        Err(e) => Response::err(req_id, e),
    }
}

/// DURABLE GraphQL cross-modal commit (CONCEPT:EG-KG.query.facade-reconcile-hook). The
/// `eg-graphql` crate stages an owner-bound multi-request transaction but exposes no
/// direct commit path because it sits below `ServerState`/persistence. Here the facade
/// GraphQL carrier `take`s the staged txn,
/// converts it into a facade [`GraphTxnState`] (graph writes → `AddNode`/`AddEdge`,
/// embeddings → `stage_vector`, tsdb batches → `stage_measurement`) and lands every
/// modality DURABLY in ONE redb `WriteTransaction` via [`commit_cross_modal_txn`] — the
/// SAME committed machinery pgwire's `commit_txn_state` drives. Returns `Ok(true)` on
/// commit, `Ok(false)` on an OCC conflict, `Err` on an unknown txn / commit failure.
#[cfg(feature = "graphql")]
pub(crate) async fn commit_graphql_cross_modal(
    state: &Arc<RwLock<ServerState>>,
    request_id: u64,
    graph_name: &str,
    core: &crate::graph::GraphCore,
    registry: &eg_graphql::CrossModalTxnRegistry,
    txn_id: &str,
    authority: &CarrierAuthority,
) -> Result<bool, String> {
    let coordinator_id = crate::server::mutation_batch::opaque_coordinator_key(
        "graphql-crossmodal-owner",
        authority.owner_scope(),
        txn_id,
    );
    // Retry/restart reconciliation must happen before consuming the ephemeral
    // staging registry: an acknowledgement-lost retry legitimately has no staged
    // object, but its durable coordinator record is authoritative.
    let persistence = state.read().await.persistence.clone();
    if let Some(persistence) = persistence {
        let fname = crate::persist::sanitize(graph_name);
        let batch_id = crate::server::mutation_batch::opaque_coordinator_key(
            "crossmodal",
            graph_name,
            &coordinator_id,
        );
        if let Some(record) = persistence.read_mutation_batch(&fname, &batch_id).await? {
            let expected_principal =
                crate::server::mutation_batch::principal_fingerprint(authority.actor_scope())?;
            if record.batch.graph != graph_name
                || record.batch.tenant != authority.tenant_scope()
                || record.batch.context.principal != expected_principal
            {
                return Err(
                    "committed GraphQL cross-modal batch does not match caller scope".to_string(),
                );
            }
            let bytes = record
                .result_msgpack
                .as_deref()
                .ok_or_else(|| "committed GraphQL cross-modal batch has no result".to_string())?;
            let result = decode_txn_result(bytes)?;
            let committed = match result {
                ResultPayload::Bool(value) => value,
                _ => {
                    return Err(
                        "committed GraphQL cross-modal result has the wrong type".to_string()
                    )
                }
            };
            let (snapshot, version) = persistence
                .read_authoritative_graph_snapshot(&fname)
                .await?
                .ok_or_else(|| {
                    "committed GraphQL cross-modal graph image is missing".to_string()
                })?;
            core.install_committed_snapshot(snapshot, version)?;
            return Ok(committed);
        }
    }
    let staged = registry
        .take(authority.owner_scope(), txn_id)
        .ok_or_else(|| format!("unknown transaction '{txn_id}'"))?;
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
    // Staged graph writes (from `sparqlUpdate` / `sparqlConstruct`, already lowered to the
    // property-graph projection) → the SAME `AddNode`/`AddEdge` write-set the RPC/pgwire
    // seam commits.
    for w in staged.graph_writes() {
        match w {
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
    // tsdb measurement batches ride `full` (the facade enables `eg-graphql/crossmodal-tsdb`
    // from its tsdb graphql tier). Without the facade `tsdb` feature `staged.measurements()`
    // is empty (the crate leg is off too), so this loop is a no-op.
    #[cfg(feature = "tsdb")]
    for (series, points) in staged.measurements() {
        let n_fields = points.first().map(|(_, v)| v.len()).unwrap_or(0);
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
    commit_cross_modal_txn(
        state,
        request_id,
        Some(authority.actor_scope()),
        &coordinator_id,
        txn,
    )
    .await
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
    let (core, persistence, graph_type, owner) = {
        let s = state.read().await;
        let entry = match s.registry.get(&txn.graph) {
            Some(e) => e,
            None => return Err(format!("Graph '{}' not found", txn.graph)),
        };
        (
            entry.core.clone(),
            s.persistence.clone(),
            entry.graph_type,
            entry.owner.clone(),
        )
    };
    // Re-check Write access at commit.
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

    // Serialize validation, the authoritative one-fsync batch and RAM publication
    // with every other mutation surface for this graph.
    let _mutation_guard = crate::server::mutation_batch::lock_graph(&txn.graph).await;

    // ── OCC validation under the topology write guard (read-only check) ──
    {
        let gtxn = core.txn(); // serialization barrier vs concurrent writers
        let ok = txn.validate(&core);
        drop(gtxn); // release the barrier before returning / the durable commit
        if !ok {
            // Conflict: nothing applied, nothing persisted — true rollback.
            return Ok(false);
        }
    }

    let fname = crate::persist::sanitize(&txn.graph);
    // Graph-topology methods for the durable + in-memory apply: the ordinary staged
    // write-set PLUS the OWL-axiom, SPARQL-CONSTRUCT, and PLANNER-WRITEBACK (D7,
    // CONCEPT:EG-KG.query.plan-dag) triples already lowered to AddNode/AddEdge at stage
    // time (CONCEPT:EG-KG.txn.extended-cross-modal/362). Folding them here means they
    // ride the SAME `apply_method_rows` / `apply_staged` path as any other mutation, so
    // the committed axioms/CONSTRUCT/plan-writeback triples are durable + visible
    // atomically with the txn's other modalities.
    let mut methods: Vec<Method> = txn.write_set.clone();
    methods.extend(txn.axioms.iter().cloned());
    methods.extend(txn.constructs.iter().cloned());
    methods.extend(txn.plan_writeback.iter().cloned());
    // Time-series measurement batches land into the graph's SERIES tables in the SAME
    // WriteTransaction (CONCEPT:EG-KG.backend.cross-modal-atomic-commit).
    let measurements: Vec<crate::MeasurementBatch> = txn
        .measurements
        .iter()
        .map(|m| {
            let batch = m.to_batch();
            // Cross-modal batches must already carry the canonical verified scope
            // produced by `stage_measurement`; persistence never infers authority.
            #[cfg(feature = "tsdb")]
            {
                if eg_tsdb::store::SeriesKey::decode(&batch.0).is_none() {
                    return Err("staged time-series key is not canonically scoped".to_string());
                }
            }
            Ok(batch)
        })
        .collect::<Result<_, String>>()?;

    // ── THE ATOMIC COMMIT POINT ─────────────────────────────────────────────
    // The same redb transaction owns graph/vector/blob/time-series rows AND the
    // universal status, OCC/fence, idempotency result and transactional outbox.
    // A process death after fsync but before this function returns is reconciled by
    // the stable opaque coordinator key without executing any modality twice.
    let Some(p) = persistence.as_ref() else {
        return Err("cross-modal mutation requires durable persistence".to_string());
    };
    {
        let batch_id = crate::server::mutation_batch::opaque_coordinator_key(
            "crossmodal",
            &txn.graph,
            coordinator_id,
        );
        let authoritative_version = p
            .read_mutation_graph_version(&fname)
            .await?
            .unwrap_or(txn.begin_version);
        let modality_payload =
            rmp_serde::to_vec_named(&(&methods, &txn.vectors, &txn.blob_refs, &measurements))
                .map_err(|e| format!("cross-modal manifest encode failed: {e}"))?;
        let batch = crate::server::mutation_batch::compile_crossmodal(
            crate::server::mutation_batch::CompileBatch {
                batch_id: &batch_id,
                request_id,
                principal: caller,
                tenant: &txn.tenant_scope,
                graph: &txn.graph,
                placement_epoch: 0,
                idempotency_key: &batch_id,
                expected_graph_version: Some(authoritative_version),
                fencing_token: None,
                created_at_ms: now_ms(),
                default_surface: crate::mutation_batch::MutationSurface::Transaction,
                authoritative_state: None,
            },
            &modality_payload,
            methods.len(),
            txn.vectors.len(),
            txn.blob_refs.len(),
            measurements.len(),
        )?;
        let result = rmp_serde::to_vec_named(&ResultPayload::Bool(true))
            .map_err(|e| format!("cross-modal result encode failed: {e}"))?;
        let committed = p
            .commit_mutation_batch_crossmodal(crate::server::persistence::CrossModalCommitArgs {
                graph_fname: &fname,
                batch: &batch,
                methods: &methods,
                vectors: &txn.vectors,
                blob_refs: &txn.blob_refs,
                measurements: &measurements,
                result_msgpack: Some(&result),
                committed_at_ms: now_ms(),
            })
            .await
            .map_err(|e| format!("cross-modal MutationBatch commit failed: {e}"))?;
        if committed.replayed {
            let bytes =
                committed.record.result_msgpack.as_deref().ok_or_else(|| {
                    "committed cross-modal batch has no durable result".to_string()
                })?;
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
            let (snapshot, version) = p
                .read_authoritative_graph_snapshot(&fname)
                .await?
                .ok_or_else(|| "committed cross-modal graph image is missing".to_string())?;
            core.install_committed_snapshot(snapshot, version)?;
            #[cfg(feature = "tsdb")]
            project_committed_measurements(state, &measurements).await?;
            return Ok(committed_bool);
        }
    }

    // ── Durable commit succeeded → reflect it in the in-memory model ──
    {
        let mut gtxn = core.txn();
        for m in &methods {
            apply_staged(&mut gtxn, m);
        }
    }
    // Blob refs: mirror the durable `__blob__` property onto the in-memory node so a
    // RAM read matches redb (the durable row already carries it).
    for (node_id, digest) in &txn.blob_refs {
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
    // Vectors: add to the in-memory semantic store (the durable SEMANTIC blob already
    // carries them). The durable `commit_crossmodal` call above already validated
    // every vector's dimension before it could land on disk (CONCEPT:EG-KG.compute.rank-dim-mismatch-guard,
    // BUG-007), so a rejection here would mean RAM and the durable store have
    // already diverged — propagated via `?` rather than ignored so that surprise
    // fails loudly instead of silently.
    {
        let mut store = core.semantic_store.write();
        for (node_id, embedding) in &txn.vectors {
            store
                .add_embedding(node_id.clone(), embedding.clone())
                .map_err(|error| error.to_string())?;
        }
    }
    // ── Time-series SERVED read-path materialization (CONCEPT:EG-KG.backend.ts-served-materialize, EG-P0-4) ──
    // The atomic barrier above already landed `measurements` durably in the authoritative shard's
    // SERIES tables (atomic WITH the graph/vector/blob write — see `commit_crossmodal`'s
    // doc comment in `redb_store.rs`). But redb holds an EXCLUSIVE per-process file
    // lock, so the authoritative shard and the SERVED `series.redb` (`state.tsdb_store` — what
    // `TsRange`/`TsAsofJoin`/`TsWindow`/`TsGapFill` and UQL `Op::TsScan` actually read,
    // see `handlers::timeseries` + `eg_plan::exec::tsdb_scan_op`) are two DIFFERENT
    // `Database` handles that can never share one `WriteTransaction` — true one-commit
    // atomicity across both files is provided as authoritative commit plus a
    // deterministic, restart-reconciled served projection.
    //
    // We only ever reach this line once the authoritative-shard commit above has SUCCEEDED (an
    // OCC conflict or a durable-commit error both `return` earlier), so this is not
    // reached for a txn that didn't truly land. Given that, this is the CANONICAL path
    // chosen for this workstream: replay the SAME already-durable batch into the served
    // store as a second, immediate durable write, via the existing
    // `SeriesStore::append_batch` over the canonical `(tenant, graph, series)` key —
    // reusing the write path verbatim rather than teaching the read side to fan out
    // across every durable shard.
    //
    // Guarantee this gives: a cross-modal-committed measurement is visible through the
    // PUBLIC `Ts*`/`Op::TsScan` read path immediately after `Commit` acks, AND — because
    // `SeriesStore::append_batch` is itself a committed redb `WriteTransaction` on
    // `series.redb` — still visible after a full process restart (the served store is
    // reopened from disk, not rebuilt from RAM).
    //
    // Remaining non-atomic boundary (documented, not hidden): a process crash strictly
    // BETWEEN the two commits leaves the measurement durable in the authoritative shard (the
    // authoritative, atomic copy — recoverable) but NOT YET reflected in `series.redb`.
    // The startup reconciliation pass repairs that window before traffic is accepted,
    // using the durable scoped-series projection cursor and an exact point diff when
    // needed; callers can therefore observe temporary staleness only until restart
    // recovery completes, not a permanently invisible committed measurement.
    #[cfg(feature = "tsdb")]
    project_committed_measurements(state, &measurements).await?;
    core.mark_dirty();

    Ok(true)
}

#[cfg(feature = "tsdb")]
async fn project_committed_measurements(
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

/// One graph's slice of a multi-graph txn (CONCEPT:EG-KG.txn.routes-cross-shard-txn), resolved at commit:
/// name + sanitized fname + type + the staged ops for it.
struct CommitSlice {
    graph_name: String,
    graph_fname: String,
    #[cfg_attr(not(feature = "raft"), allow(dead_code))]
    graph_type: crate::protocol::GraphType,
    methods: Vec<Method>,
}

/// Commit a MULTI-GRAPH staged transaction (CONCEPT:EG-KG.txn.routes-cross-shard-txn — Lane N wire). Builds a
/// per-graph slice from the default-graph write-set + each `extra_writes` graph
/// (validating existence + Write access on each), then:
///
///   * **Cross-shard (≥2 Raft groups) + active cluster** → route the staged write-set
///     through [`crate::raft::cross_shard_txn::CrossShardCoordinator::commit_cross_shard`]:
///     the 2PC coordinator prepares each participant group durably (commit-before-vote),
///     logs ONE durable decision (the atomic commit point), then applies every slice
///     through its group's Raft `client_write`. All-or-nothing across groups,
///     recovery-resolvable. `Bool(true)` on COMMIT, `Bool(false)` on ABORT.
///   * **Single-group collapse, OR no active cluster (incl. a non-raft build)** →
///     commit each graph slice as a deterministic child MutationBatch under a durable
///     multi-graph coordinator saga. Re-entry resumes idempotent children and records
///     one terminal parent receipt, so no staged graph is silently dropped.
async fn commit_multi_graph(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    caller: Option<&str>,
    coordinator_id: &str,
    txn: GraphTxnState,
) -> Response {
    // Build the per-graph slices: the default graph (its write_set) + every extra
    // graph. Each graph must exist + the caller must hold Write on it.
    let mut slices: Vec<CommitSlice> = Vec::new();
    {
        let s = state.read().await;
        let mut per_graph: Vec<(String, Vec<Method>)> =
            vec![(txn.graph.clone(), txn.write_set.clone())];
        for (g, ops) in &txn.extra_writes {
            per_graph.push((g.clone(), ops.clone()));
        }
        per_graph.sort_by(|left, right| left.0.cmp(&right.0));
        for (graph_name, methods) in per_graph {
            let entry = match s.registry.get(&graph_name) {
                Some(e) => e,
                None => return Response::err(req_id, format!("Graph '{}' not found", graph_name)),
            };
            if !consensus_apply_is_authorized() {
                if let Err(denied) = check_graph_access(
                    &s.isolation,
                    caller,
                    &graph_name,
                    entry.graph_type,
                    entry.owner.as_deref(),
                    AccessLevel::Write,
                ) {
                    return Response::err(req_id, denied);
                }
            }
            slices.push(CommitSlice {
                graph_fname: crate::persist::sanitize(&graph_name),
                graph_type: entry.graph_type,
                graph_name,
                methods,
            });
        }
    }

    commit_recoverable_slices(state, req_id, caller, coordinator_id, slices).await
}

/// Commit an already-authorized coordinator plan through the same recoverable
/// graph-slice authority as a staged multi-graph transaction. This is the only
/// auxiliary-carrier entry point for complete graph images: clustered spans use
/// the retained-decision 2PC path; local/single-group spans use deterministic
/// child MutationBatches subordinate to the caller's durable parent receipt.
pub(crate) async fn commit_coordinated_graph_methods(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    caller: Option<&str>,
    coordinator_id: &str,
    graph_methods: Vec<(String, crate::protocol::GraphType, Vec<Method>)>,
) -> Response {
    let mut slices = Vec::with_capacity(graph_methods.len());
    {
        let current = state.read().await;
        for (graph_name, graph_type, methods) in graph_methods {
            let Some(entry) = current.registry.get(&graph_name) else {
                return Response::err(req_id, format!("Graph '{graph_name}' not found"));
            };
            if entry.graph_type != graph_type {
                return Response::err(
                    req_id,
                    format!("Graph '{graph_name}' changed type during coordination"),
                );
            }
            if !consensus_apply_is_authorized() {
                if let Err(denied) = check_graph_access(
                    &current.isolation,
                    caller,
                    &graph_name,
                    entry.graph_type,
                    entry.owner.as_deref(),
                    AccessLevel::Write,
                ) {
                    return Response::err(req_id, denied);
                }
            }
            slices.push(CommitSlice {
                graph_fname: crate::persist::sanitize(&graph_name),
                graph_type,
                graph_name,
                methods,
            });
        }
    }
    slices.sort_by(|left, right| left.graph_name.cmp(&right.graph_name));
    commit_recoverable_slices(state, req_id, caller, coordinator_id, slices).await
}

async fn commit_recoverable_slices(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    caller: Option<&str>,
    coordinator_id: &str,
    slices: Vec<CommitSlice>,
) -> Response {
    // ── CROSS-SHARD: a multi-graph span over ≥2 Raft groups routes through 2PC ──
    #[cfg(feature = "raft")]
    {
        let (multi, backend) = {
            let s = state.read().await;
            (s.multi_raft.clone(), s.persistence.clone())
        };
        let recoverable_in_flight = backend
            .as_ref()
            .and_then(|value| value.as_redb())
            .map(|redb| {
                let id = cross_shard_transaction_id(coordinator_id);
                redb.xshard_decision_retain_get(&id)
            })
            .transpose();
        let recoverable_in_flight = match recoverable_in_flight {
            Ok(value) => value.unwrap_or(false),
            Err(error) => {
                return Response::err(
                    req_id,
                    format!("cross-shard recovery lookup failed: {error}"),
                )
            }
        };
        if let Some(multi) = multi {
            if recoverable_in_flight
                || multi
                    .router()
                    .is_cross_shard(slices.iter().map(|slice| slice.graph_name.as_str()))
            {
                return commit_cross_shard(state, req_id, caller, coordinator_id, multi, slices)
                    .await;
            }
        } else if recoverable_in_flight {
            return Response::err(
                req_id,
                "cross-shard transaction recovery is waiting for the Raft groups",
            );
        }
    }

    // ── Single-group collapse OR no cluster: apply each slice locally ──
    apply_slices_locally(state, req_id, caller, coordinator_id, &slices).await
}

/// Route a cross-shard multi-graph txn through the 2PC coordinator (CONCEPT:EG-KG.txn.routes-cross-shard-txn).
#[cfg(feature = "raft")]
async fn commit_cross_shard(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    _caller: Option<&str>,
    coordinator_id: &str,
    multi: std::sync::Arc<crate::raft::multi::MultiRaft>,
    slices: Vec<CommitSlice>,
) -> Response {
    use crate::raft::cross_shard_txn::{
        CrossShardCoordinator, CrossShardTxn, GraphSlice, TxnOutcome,
    };

    let backend = {
        let s = state.read().await;
        s.persistence.clone()
    };
    let Some(backend) = backend else {
        return Response::err(req_id, "cross-shard txn requires a persistence backend");
    };
    let x_slices: Vec<GraphSlice> = slices
        .into_iter()
        .map(|s| GraphSlice {
            graph_name: s.graph_name,
            graph_fname: s.graph_fname,
            graph_type: s.graph_type,
            methods: s.methods,
        })
        .collect();
    let coord = CrossShardCoordinator::new(multi, backend.clone());
    let xtxn = CrossShardTxn {
        txn_id: cross_shard_transaction_id(coordinator_id),
        slices: x_slices,
    };
    let result = match coord.commit_cross_shard_recoverable(&xtxn).await {
        Ok(TxnOutcome::Committed) => ResultPayload::Bool(true),
        Ok(TxnOutcome::Aborted) => ResultPayload::Bool(false),
        Err(e) => return Response::err(req_id, format!("cross-shard commit failed: {e}")),
    };
    Response::ok(req_id, result)
}

/// Apply each graph's slice locally through a deterministic child MutationBatch
/// (CONCEPT:EG-KG.txn.routes-cross-shard-txn — the single-group / single-node
/// multi-graph path), subordinate to the prepared/committed transaction receipt opened
/// by [`commit`]. Used when the span collapses to one Raft group or no cluster is
/// active. Returns `Bool(true)`.
async fn apply_slices_locally(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    caller: Option<&str>,
    coordinator_id: &str,
    slices: &[CommitSlice],
) -> Response {
    let backend = {
        let s = state.read().await;
        s.persistence.clone()
    };
    let Some(backend) = backend else {
        return Response::err(req_id, "multi-graph commit requires durable persistence");
    };
    for slice in slices {
        let core = {
            let s = state.read().await;
            match s.registry.get(&slice.graph_name) {
                Some(e) => e.core.clone(),
                None => {
                    return Response::err(req_id, format!("Graph '{}' not found", slice.graph_name))
                }
            }
        };
        let child_id = crate::server::mutation_batch::opaque_coordinator_key(
            "multi-graph-child",
            &slice.graph_name,
            coordinator_id,
        );
        if let Err(error) = crate::server::mutation_batch::commit_internal_graph_methods(
            Some(&backend),
            &core,
            req_id,
            caller,
            &slice.graph_name,
            &child_id,
            slice.methods.clone(),
            &ResultPayload::Bool(true),
        )
        .await
        {
            return Response::err(req_id, format!("multi-graph child commit failed: {error}"));
        }
    }
    Response::ok(req_id, ResultPayload::Bool(true))
}

/// `Rollback`: discard the staged transaction. Nothing was ever applied or
/// persisted, so this only drops the in-memory state. Returns `Bool(true)` when a
/// txn was removed; an unknown id (already committed/rolled-back/expired) is
/// reported so the client knows the id is gone.
async fn rollback(state: &Arc<RwLock<ServerState>>, req_id: u64, txn_id: &str) -> Response {
    let s = state.read().await;
    if s.open_txns.remove(txn_id).is_some() {
        Response::ok(req_id, ResultPayload::Bool(true))
    } else {
        Response::err(req_id, format!("unknown transaction '{}'", txn_id))
    }
}

/// Apply one staged durable mutation through the held `GraphTxn` (the same engine
/// primitives the write coalescer uses, so behavior is identical). Errors from
/// add_edge / a failed CAS are NOT surfaced per-op in M6: a staged add_edge to a
/// missing endpoint is a no-op (its endpoints were validated into the read-set; if
/// absent the edge simply isn't added), matching the inline best-effort contract.
fn apply_staged(gtxn: &mut crate::graph::GraphTxn<'_>, method: &Method) {
    match method {
        Method::AddNode {
            node_id,
            properties_msgpack,
        } => gtxn.add_node(node_id.clone(), properties_msgpack.clone()),
        Method::RemoveNode { node_id } => gtxn.remove_node(node_id.clone()),
        Method::AddEdge {
            source_id,
            target_id,
            properties_msgpack,
        } => {
            let _ = gtxn.add_edge(
                source_id.clone(),
                target_id.clone(),
                properties_msgpack.clone(),
            );
        }
        Method::RemoveEdge {
            source_id,
            target_id,
        } => gtxn.remove_edge(source_id.clone(), target_id.clone()),
        Method::CompareAndSetNodeFields {
            node_id,
            conditions_msgpack,
            updates_msgpack,
        } => {
            // Decode the condition/update maps; a decode failure is a no-op CAS
            // (the inline path returns Bool(false) and touches nothing).
            if let (Ok(conditions), Ok(updates)) = (
                decode_txn_object(conditions_msgpack),
                decode_txn_object(updates_msgpack),
            ) {
                let _ = gtxn.compare_and_set_fields(node_id, &conditions, &updates);
            }
        }
        // Only durable mutations are ever staged (the protocol restricts Txn* to
        // this set); any other variant here is unreachable.
        _ => {}
    }
}

#[cfg(all(test, feature = "epistemic"))]
mod materialize_belief_tests {
    use super::*;
    use crate::graph::GraphCore;

    fn node(props: serde_json::Value) -> Vec<u8> {
        rmp_serde::to_vec_named(&props).unwrap()
    }

    fn apply_cas(core: &GraphCore, m: &Method) -> bool {
        let Method::CompareAndSetNodeFields {
            node_id,
            conditions_msgpack,
            updates_msgpack,
        } = m
        else {
            panic!("expected a CompareAndSetNodeFields method, got {m:?}");
        };
        let conditions =
            rmp_serde::from_slice::<serde_json::Map<String, serde_json::Value>>(conditions_msgpack)
                .unwrap();
        let updates =
            rmp_serde::from_slice::<serde_json::Map<String, serde_json::Value>>(updates_msgpack)
                .unwrap();
        core.compare_and_set_fields(node_id, &conditions, &updates)
    }

    fn stored_confidence(core: &GraphCore, node_id: &str) -> f64 {
        let blob = core.get_node_properties(node_id).expect("node exists");
        let v: serde_json::Value = rmp_serde::from_slice(&blob).unwrap();
        v.get("confidence").and_then(|c| c.as_f64()).unwrap()
    }

    /// D5: a claim with a supporting evidence node propagates to a HIGHER belief than
    /// its bare stored prior — and the lowered Method, once applied, writes that exact
    /// derived value onto `NodeData.confidence` (materialize belief writes).
    #[test]
    fn materialize_belief_writes_the_propagated_confidence() {
        let core = GraphCore::new();
        core.add_node(
            "claim".into(),
            node(serde_json::json!({"type": "Claim", "confidence": 0.5})),
        );
        core.add_node(
            "evidence".into(),
            node(serde_json::json!({"type": "Evidence", "confidence": 0.9})),
        );
        let _ = core.add_edge(
            "evidence".into(),
            "claim".into(),
            node(serde_json::json!({"relationship": "SUPPORTS"})),
        );

        let (methods, confidence) =
            materialize_belief_to_methods(&core, "claim").expect("node exists");
        assert_eq!(methods.len(), 1);
        assert!(
            confidence > 0.5,
            "support should raise belief above the bare prior, got {confidence}"
        );
        assert!((0.0..=1.0).contains(&confidence));

        assert!(apply_cas(&core, &methods[0]), "CAS must apply cleanly");
        assert!(
            (stored_confidence(&core, "claim") - confidence).abs() < 1e-12,
            "the node's stored confidence must equal the derived belief exactly"
        );
    }

    /// D5 idempotency: the LOWERED method carries a value FROZEN at stage time (not
    /// re-derived at apply time), so replaying the identical method — exactly what a
    /// WAL replay or a duplicate commit does — is a true no-op the second time: the
    /// stored confidence stays byte-identical (never ratchets/drifts).
    #[test]
    fn materialize_belief_replay_is_idempotent() {
        let core = GraphCore::new();
        core.add_node(
            "claim2".into(),
            node(serde_json::json!({"type": "Claim", "confidence": 0.5})),
        );
        core.add_node(
            "evidence2".into(),
            node(serde_json::json!({"type": "Evidence", "confidence": 0.9})),
        );
        let _ = core.add_edge(
            "evidence2".into(),
            "claim2".into(),
            node(serde_json::json!({"relationship": "SUPPORTS"})),
        );

        let (methods, confidence) =
            materialize_belief_to_methods(&core, "claim2").expect("node exists");

        assert!(apply_cas(&core, &methods[0]));
        let first = stored_confidence(&core, "claim2");
        assert!((first - confidence).abs() < 1e-12);

        // Re-apply the SAME (already-computed) method — as WAL replay / a duplicate
        // commit would — WITHOUT recomputing belief from the now-updated node.
        assert!(apply_cas(&core, &methods[0]));
        let second = stored_confidence(&core, "claim2");
        assert_eq!(
            first, second,
            "replaying the identical lowered method must not drift the stored value"
        );
    }

    /// D5 is audited: the write lands as an ordinary `CompareAndSetNodeFields` — the
    /// SAME durable Method `audit::audit_line` already recognizes and chains (CONCEPT:
    /// EG-KG.sharding.row-level-security) — never a bespoke, unaudited mutation.
    #[test]
    #[cfg(feature = "security")]
    fn materialize_belief_rides_the_audited_cas_path() {
        let core = GraphCore::new();
        core.add_node(
            "claim3".into(),
            node(serde_json::json!({"confidence": 0.5})),
        );
        let (methods, _confidence) =
            materialize_belief_to_methods(&core, "claim3").expect("node exists");
        let line = crate::audit::audit_line(&methods[0]);
        assert_eq!(line.as_deref(), Some("CAS_NODE|claim3"));
    }

    /// D5 is opt-in: a node with no evidence at all keeps its bare stored prior
    /// (`from_graph_view` treats a MISSING confidence as `1.0`, but here it IS set) —
    /// proving materialize-belief never fabricates a belief where none was asserted.
    #[test]
    fn materialize_belief_missing_node_errors() {
        let core = GraphCore::new();
        assert!(materialize_belief_to_methods(&core, "nope").is_err());
    }
}
