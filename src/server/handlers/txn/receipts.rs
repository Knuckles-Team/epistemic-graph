//! Private transaction receipts implementation.

use super::*;

pub(super) struct TxnRestoreGuard {
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

pub(super) fn transaction_receipt_id(txn_id: &str) -> String {
    crate::server::mutation_batch::opaque_coordinator_key(
        "transaction-receipt",
        "transaction",
        txn_id,
    )
}

/// B-9 (2026-08-13): the durable receipt/idempotency identity a `Commit` attempt
/// uses. Mirrors `ApplyChangeEnvelope`'s `(tenant, graph, idempotency_key)`
/// dedup scope onto the txn commit's own `AdminSaga` receipt mechanism, rather
/// than inventing a second one -- see `begin_txn_receipt`'s doc.
///
/// Without a caller key (`idempotency_key: None`), this is BYTE-IDENTICAL to
/// `transaction_receipt_id(txn_id)` -- today's behavior, unchanged: the receipt
/// is keyed purely by the server-issued `txn_id`, so a retry is only provably
/// safe when the caller still has that exact `txn_id` (a dropped-response retry
/// with the same handle). That already works today via `AdminSaga` replay.
///
/// With a caller key, the receipt is keyed by the verified tenant plus THAT key
/// -- a disjoint id space (`"idempotency"` vs `"transaction"` as the hashed
/// middle field, so a key-derived id can never collide with a txn_id-derived
/// one) -- so a retry that necessarily re-stages under a FRESH `txn_id` (the
/// caller lost track of the original and had to re-`BeginTxn`) still lands on
/// the SAME durable receipt row and replay-skips, closing the gap
/// `transaction_receipt_id` alone cannot: proving "committed, response lost"
/// apart from "never committed" even when the caller no longer has the original
/// `txn_id` to retry with.
pub(super) fn commit_receipt_id(
    txn_id: &str,
    idempotency_key: Option<&str>,
    tenant_scope: Option<&str>,
) -> String {
    match idempotency_key {
        Some(key) => {
            // The request key is stable across a lost-response re-stage, while the
            // verified tenant is part of the canonical replay scope.  Bind both
            // before hashing so one caller's key cannot select another tenant's
            // transaction receipt.
            let tenant = tenant_scope
                .filter(|value| !value.trim().is_empty())
                .unwrap_or("unknown");
            let scoped_key = crate::server::mutation_batch::opaque_coordinator_key(
                "transaction-receipt-tenant",
                tenant,
                key,
            );
            crate::server::mutation_batch::opaque_coordinator_key(
                "transaction-receipt",
                "idempotency",
                &scoped_key,
            )
        }
        None => transaction_receipt_id(txn_id),
    }
}

/// The three values that together select ONE durable commit receipt, and the
/// only three [`commit_receipt_id`] hashes.  They are never meaningful apart:
/// the opaque parent id alone is not a receipt key once a request key is in
/// play, and the request key alone would let one caller's key select another
/// tenant's transaction (see [`commit_receipt_id`]).  Threaded as one value so
/// a resume path cannot pass two of the three.
pub(super) struct CommitReceiptKey<'a> {
    pub(super) txn_id: &'a str,
    pub(super) idempotency_key: Option<&'a str>,
    /// The tenant verified for this request, when the caller is tenant-scoped.
    pub(super) expected_tenant: Option<&'a str>,
}

#[cfg(feature = "raft")]
pub(super) fn cross_shard_transaction_id(parent_id: &str) -> String {
    // Use the digest-only parent id in the disjoint 2PC table as well.  This gives
    // recovery/GC a direct safe association without persisting the raw transaction
    // id or an additional mapping row.
    parent_id.to_string()
}

/// A retained 2PC decision is collected only after the parent receipt is terminal.
/// Calling this for a local/single-graph transaction is an idempotent no-op.
#[cfg(feature = "raft")]
pub(super) async fn cleanup_cross_shard_decision(
    state: &Arc<RwLock<ServerState>>,
    parent_id: &str,
) -> Result<(), String> {
    let backend = state.read().await.persistence.clone();
    let Some(redb) = backend.as_ref().and_then(|value| value.as_redb()) else {
        return Ok(());
    };
    if redb.xshard_decision_retain_get(parent_id)? {
        let decision = redb
            .xshard_decision_get(parent_id)?
            .ok_or_else(|| "retained cross-shard transaction is still undecided".to_string())?;
        let parent = eg_transaction::read_ledger(&redb.admin_mutations_read()?, parent_id)?
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
    redb.xshard_decision_clear(parent_id).await
}

#[cfg(not(feature = "raft"))]
pub(super) async fn cleanup_cross_shard_decision(
    _state: &Arc<RwLock<ServerState>>,
    _parent_id: &str,
) -> Result<(), String> {
    Ok(())
}

#[cfg(feature = "redb")]
pub(super) struct TxnReceipt {
    pub(super) backend: Arc<dyn crate::server::persistence::PersistenceBackend>,
    pub(super) saga: crate::server::handlers::admin::AdminSaga,
}

#[cfg(not(feature = "redb"))]
pub(super) type TxnReceipt = ();

#[cfg(feature = "redb")]
pub(super) fn receipt_coordinator_id(receipt: &TxnReceipt) -> String {
    receipt.saga.batch.batch_id.clone()
}

#[cfg(not(feature = "redb"))]
pub(super) fn receipt_coordinator_id(_receipt: &TxnReceipt) -> String {
    String::new()
}

/// Begin (or replay-detect) the durable commit receipt for a staged transaction
/// (B-9, 2026-08-13). `idempotency_key` is the caller-supplied dedup key from
/// `Method::Commit` -- see `commit_receipt_id`'s doc for exactly how it changes
/// (or, when absent, does NOT change) the receipt's identity.
#[cfg(feature = "redb")]
pub(super) fn begin_txn_receipt(
    backend: Option<Arc<dyn crate::server::persistence::PersistenceBackend>>,
    req_id: u64,
    caller: Option<&str>,
    txn_id: &str,
    txn: &GraphTxnState,
    idempotency_key: Option<&str>,
    attempt_nonce: Option<Nonce>,
) -> Result<(TxnReceipt, Option<ResultPayload>), String> {
    let backend = backend.ok_or_else(|| {
        "transaction commit requires an authoritative MutationBatch backend".to_string()
    })?;
    let redb = backend
        .as_redb()
        .ok_or_else(|| "transaction commit requires durable redb".to_string())?;
    let (payload_digest, encrypted_payload) = seal_txn_recovery_plan(redb, txn)?;
    let saga =
        crate::server::handlers::admin::begin_named_admin_saga_with_private_payload_and_nonce(
            redb,
            req_id,
            caller,
            attempt_nonce,
            crate::server::handlers::admin::AdminSagaPayload {
                domain: crate::mutation_batch::DurabilityDomain::ControlPlane,
                batch_id: &commit_receipt_id(
                    txn_id,
                    idempotency_key,
                    Some(txn.tenant_scope.as_str()),
                ),
                event_type: "transaction_recovery_plan",
                payload_digest: &payload_digest,
                encrypted_payload: &encrypted_payload,
            },
        )?;
    let replayed = saga.replayed.clone();
    Ok((TxnReceipt { backend, saga }, replayed))
}

#[cfg(not(feature = "redb"))]
pub(super) fn begin_txn_receipt(
    _backend: Option<Arc<dyn crate::server::persistence::PersistenceBackend>>,
    _req_id: u64,
    _caller: Option<&str>,
    _txn_id: &str,
    _txn: &GraphTxnState,
    _idempotency_key: Option<&str>,
    _attempt_nonce: Option<Nonce>,
) -> Result<((), Option<ResultPayload>), String> {
    Err("transaction commit requires the redb MutationBatch coordinator".to_string())
}

/// A resumed receipt, its already-known apply result (if any), and the rebuilt
/// ephemeral staging state — [`resume_txn_receipt`]'s success payload.
#[cfg(feature = "redb")]
pub(super) type ResumedTxnReceipt = (TxnReceipt, Option<ResultPayload>, Option<GraphTxnState>);

#[cfg(feature = "redb")]
fn retry_txn_receipt(
    redb: &crate::server::persistence::redb_backend::RedbBackend,
    req_id: u64,
    caller: &str,
    saga: crate::server::handlers::admin::AdminSaga,
    attempt_nonce: Option<Nonce>,
) -> Result<crate::server::handlers::admin::AdminSaga, String> {
    let Some(attempt_nonce) = attempt_nonce else {
        return Ok(saga);
    };
    let payload_digest = transaction_plan_digest(&saga.batch)?;
    let prepared = saga.replayed.is_none();
    let encrypted_payload = txn_receipt_retry_payload(redb, &saga, prepared)?;
    let admitted =
        crate::server::handlers::admin::begin_named_admin_saga_with_private_payload_and_nonce(
            redb,
            req_id,
            Some(caller),
            Some(attempt_nonce),
            crate::server::handlers::admin::AdminSagaPayload {
                domain: crate::mutation_batch::DurabilityDomain::ControlPlane,
                batch_id: &saga.batch.batch_id,
                event_type: "transaction_recovery_plan",
                payload_digest: &payload_digest,
                encrypted_payload: &encrypted_payload,
            },
        )?;
    if prepared {
        // A Prepared saga's operation row was claimed by the original
        // prepare nonce.  The retry admission above re-enters the same
        // kernel and rejects an exact reuse, but the stored Prepared batch
        // must remain the batch that `finish_admin_saga` terminalizes;
        // handing it the freshly built batch would ask `record_operation_in`
        // to claim the same operation with a second nonce.
        Ok(saga)
    } else {
        Ok(admitted)
    }
}

#[cfg(feature = "redb")]
fn txn_receipt_retry_payload(
    redb: &crate::server::persistence::redb_backend::RedbBackend,
    saga: &crate::server::handlers::admin::AdminSaga,
    prepared: bool,
) -> Result<Vec<u8>, String> {
    if prepared {
        eg_transaction::read_private_payload(&redb.admin_mutations_read()?, &saga.batch.batch_id)?
            .ok_or_else(|| "prepared transaction has no encrypted recovery plan".to_string())
    } else {
        Ok(Vec::new())
    }
}

#[cfg(feature = "redb")]
fn resume_txn_staging(
    redb: &crate::server::persistence::redb_backend::RedbBackend,
    saga: &crate::server::handlers::admin::AdminSaga,
    caller: &str,
    expected_tenant: Option<&str>,
) -> Result<(Option<ResultPayload>, Option<GraphTxnState>), String> {
    let replayed = saga.replayed.clone();
    if replayed
        .as_ref()
        .map(|result| !matches!(result, ResultPayload::Bool(_)))
        .unwrap_or(false)
    {
        return Err("transaction parent receipt has the wrong result type".to_string());
    }
    let txn = if replayed.is_none() {
        let encrypted = eg_transaction::read_private_payload(
            &redb.admin_mutations_read()?,
            &saga.batch.batch_id,
        )?
        .ok_or_else(|| "prepared transaction has no encrypted recovery plan".to_string())?;
        let txn = open_txn_recovery_plan(redb, &saga.batch, &encrypted, caller.to_string())?;
        if let Some(expected_tenant) = expected_tenant {
            if txn.tenant_scope != expected_tenant {
                return Err("prepared transaction does not match caller tenant scope".to_string());
            }
        }
        Some(txn)
    } else {
        None
    };
    Ok((replayed, txn))
}

/// Re-open a prepared/committed parent receipt after process restart.  A Prepared
/// receipt must still have its encrypted private plan; the plan is authenticated,
/// decrypted, digest-verified against the canonical batch, and only then rebuilt
/// into ephemeral staging.
#[cfg(feature = "redb")]
pub(super) fn resume_txn_receipt(
    backend: Option<Arc<dyn crate::server::persistence::PersistenceBackend>>,
    req_id: u64,
    caller: Option<&str>,
    txn_id: &str,
    idempotency_key: Option<&str>,
    expected_tenant: Option<&str>,
    attempt_nonce: Option<Nonce>,
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
        &commit_receipt_id(txn_id, idempotency_key, expected_tenant),
        Some(caller),
    )?
    else {
        return Ok(None);
    };
    if saga.batch.identity
        != crate::server::persistence::redb_backend::cluster_admin_scope_identity()?
    {
        return Err("transaction parent receipt has the wrong coordinator scope".to_string());
    }
    let _ = transaction_plan_digest(&saga.batch)?;
    // A commit bypasses the transport replay ledger because its effect belongs
    // to the durable parent/child kernel.  Re-admit every verified retry
    // nonce through that SAME parent saga, including a terminal parent.  This
    // preserves the one authority's exact-nonce rejection while a fresh nonce
    // still receives the stored terminal result.  The private plan is reused
    // only while the parent is Prepared; terminal receipts have already erased
    // it and need only the digest-only operation identity.
    let saga = retry_txn_receipt(redb, req_id, caller, saga, attempt_nonce)?;
    let (replayed, txn) = resume_txn_staging(redb, &saga, caller, expected_tenant)?;
    Ok(Some((TxnReceipt { backend, saga }, replayed, txn)))
}

/// The not-built stand-in for [`ResumedTxnReceipt`] when `redb` (and therefore
/// `TxnReceipt`) is not compiled in — same shape, `()` where the durable receipt
/// would be, since [`resume_txn_receipt`] below always returns `None` here.
#[cfg(not(feature = "redb"))]
pub(super) type ResumedTxnReceiptStub = ((), Option<ResultPayload>, Option<GraphTxnState>);

#[cfg(not(feature = "redb"))]
pub(super) fn resume_txn_receipt(
    _backend: Option<Arc<dyn crate::server::persistence::PersistenceBackend>>,
    _req_id: u64,
    _caller: Option<&str>,
    _txn_id: &str,
    _idempotency_key: Option<&str>,
    _expected_tenant: Option<&str>,
    _attempt_nonce: Option<Nonce>,
) -> Result<Option<ResumedTxnReceiptStub>, String> {
    Ok(None)
}

#[cfg(all(feature = "redb", feature = "security"))]
pub(super) fn seal_txn_recovery_plan(
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
pub(super) fn seal_txn_recovery_plan(
    _backend: &crate::server::persistence::redb_backend::RedbBackend,
    _txn: &GraphTxnState,
) -> Result<(String, Vec<u8>), String> {
    Err("transaction durability requires a build with redb and security".to_string())
}

#[cfg(all(feature = "redb", feature = "security"))]
pub(super) fn open_txn_recovery_plan(
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
pub(super) fn open_txn_recovery_plan(
    _backend: &crate::server::persistence::redb_backend::RedbBackend,
    _batch: &crate::mutation_batch::MutationBatch,
    _encrypted: &[u8],
    _agent: String,
) -> Result<GraphTxnState, String> {
    Err("transaction recovery requires a build with redb and security".to_string())
}

#[cfg(feature = "redb")]
pub(super) fn transaction_plan_digest(
    batch: &crate::mutation_batch::MutationBatch,
) -> Result<String, String> {
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
pub(super) async fn authorize_txn_plan(
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

/// Finish an admin saga on behalf of a `{backend, saga}` receipt/control
/// wrapper — the txn-commit receipt (`TxnReceipt`), the transaction-lifecycle
/// receipt (`TxnLifecycleReceipt`), and the materialized-view control-plane
/// saga (`dist_compute::ControlSaga`) all carry that identical pair. Unwrap the
/// backend's redb coordinator and hand the batch to the one saga-completion
/// authority, `admin::finish_admin_saga`; the caller-specific "lost its redb
/// coordinator" wording is the only thing that varies between the three sites.
#[cfg(feature = "redb")]
pub(crate) fn finish_saga_via_redb(
    backend: &Arc<dyn crate::server::persistence::PersistenceBackend>,
    saga: crate::server::handlers::admin::AdminSaga,
    result: ResultPayload,
    lost_backend_message: &str,
) -> Result<ResultPayload, String> {
    let redb = backend
        .as_redb()
        .ok_or_else(|| lost_backend_message.to_string())?;
    crate::server::handlers::admin::finish_admin_saga(redb, saga.batch, saga.created_at_ms, result)
}

#[cfg(feature = "redb")]
pub(super) fn finish_txn_receipt(
    receipt: TxnReceipt,
    result: ResultPayload,
) -> Result<ResultPayload, String> {
    finish_saga_via_redb(
        &receipt.backend,
        receipt.saga,
        result,
        "transaction receipt lost its redb coordinator",
    )
}

#[cfg(not(feature = "redb"))]
pub(super) fn finish_txn_receipt(
    _receipt: (),
    _result: ResultPayload,
) -> Result<ResultPayload, String> {
    Err("transaction commit requires the redb MutationBatch coordinator".to_string())
}

/// Every mutating transaction-family method bypasses the transport replay
/// ledger because its effect belongs to the durable kernel.  Begin/stage/
/// rollback used to be the hole in that rule: they changed in-memory
/// `open_txns` state without ever presenting the verified nonce to the kernel.
/// Use the existing named admin saga as the one admission/receipt authority for
/// those lifecycle steps; this helper does not introduce a second replay table.
pub(super) fn is_txn_lifecycle_method(method: &Method) -> bool {
    matches!(method, Method::BeginTxn { .. })
        || (method_txn_id(method).is_some() && !matches!(method, Method::Commit { .. }))
}

/// A terminal lifecycle receipt is useful only while the volatile transaction
/// handle it describes is still present.  The durable saga may outlive the
/// process, but it cannot recreate `open_txns`; returning its old success after
/// restart would hand the caller a dead Begin handle or claim a Stage/Rollback
/// succeeded before the next request fails with `unknown transaction`.
pub(super) async fn validate_txn_lifecycle_replay(
    state: &Arc<RwLock<ServerState>>,
    method: &Method,
    owner: &str,
    result: &ResultPayload,
) -> Result<(), String> {
    let txn_id = if matches!(method, Method::BeginTxn { .. }) {
        match result {
            ResultPayload::String(txn_id) => txn_id.as_str(),
            _ => {
                return Err(
                    "transaction lifecycle receipt has the wrong BeginTxn result".to_string(),
                );
            }
        }
    } else {
        method_txn_id(method).ok_or_else(|| {
            "transaction lifecycle receipt has no volatile transaction handle".to_string()
        })?
    };
    let s = state.read().await;
    let Some(entry) = s.open_txns.get(txn_id) else {
        return Err(
            "transaction lifecycle receipt is terminal but volatile staging state is unavailable; \
             refusing to return a stale success"
                .to_string(),
        );
    };
    if entry.value().lock().agent != owner {
        return Err("transaction lifecycle receipt does not match caller scope".to_string());
    }
    Ok(())
}

pub(super) fn txn_lifecycle_batch_id(authority: &CarrierAuthority, method: &Method) -> String {
    // Keep one envelope idempotency key reusable across different transaction
    // operations by including the operation family in the opaque coordinator
    // input.  The full method body remains in the kernel operation digest, so a
    // changed txn id, graph, or payload still conflicts under the same key.
    let operation_key = format!("{}:{}", method.tag_name(), authority.idempotency_key());
    crate::server::mutation_batch::opaque_coordinator_key(
        "transaction-lifecycle",
        authority.owner_scope(),
        &operation_key,
    )
}

#[cfg(feature = "redb")]
pub(crate) struct TxnLifecycleReceipt {
    pub(super) backend: Arc<dyn crate::server::persistence::PersistenceBackend>,
    pub(super) saga: crate::server::handlers::admin::AdminSaga,
}

#[cfg(not(feature = "redb"))]
pub(crate) struct TxnLifecycleReceipt;

#[cfg(feature = "redb")]
pub(super) async fn begin_txn_lifecycle_receipt(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    caller: &str,
    authority: &CarrierAuthority,
    method: &Method,
) -> Result<TxnLifecycleReceipt, String> {
    let backend = state.read().await.persistence.clone().ok_or_else(|| {
        "transaction lifecycle requires an authoritative MutationBatch backend".to_string()
    })?;
    let redb = backend
        .as_redb()
        .ok_or_else(|| "transaction lifecycle requires durable redb".to_string())?;
    let batch_id = txn_lifecycle_batch_id(authority, method);
    // A lifecycle saga is only the replay authority; its effect still lives in
    // the volatile transaction registry.  If a prior attempt prepared that
    // saga and then died before terminalization, executing the method again
    // would duplicate Begin/Stage/Rollback (or silently operate on a different
    // handle after restart).  Probe the same durable coordinator before
    // admission so a Prepared receipt becomes an explicit lost-staging
    // refusal.  The real admission below still consumes the exact nonce and
    // therefore preserves the kernel's REPLAY_NONCE_CONSUMED / conflict
    // decisions for retries.
    let prepared =
        crate::server::handlers::admin::resume_named_admin_saga(redb, &batch_id, Some(caller))?
            .is_some_and(|saga| saga.replayed.is_none());
    let saga = crate::server::handlers::admin::begin_named_admin_saga_with_nonce(
        redb,
        req_id,
        Some(caller),
        method,
        crate::mutation_batch::DurabilityDomain::ControlPlane,
        &batch_id,
        authority.attempt_nonce(),
    )?;
    if prepared && saga.replayed.is_none() {
        return Err(
            "transaction lifecycle receipt is Prepared but volatile staging state is unavailable; \
             refusing to re-execute an ambiguous lifecycle operation"
                .to_string(),
        );
    }
    Ok(TxnLifecycleReceipt { backend, saga })
}

#[cfg(not(feature = "redb"))]
pub(super) async fn begin_txn_lifecycle_receipt(
    _state: &Arc<RwLock<ServerState>>,
    _req_id: u64,
    _caller: &str,
    _authority: &CarrierAuthority,
    _method: &Method,
) -> Result<TxnLifecycleReceipt, String> {
    Err("transaction lifecycle requires the redb MutationBatch coordinator".to_string())
}

#[cfg(feature = "redb")]
pub(super) fn finish_txn_lifecycle_receipt(
    receipt: TxnLifecycleReceipt,
    result: ResultPayload,
) -> Result<ResultPayload, String> {
    finish_saga_via_redb(
        &receipt.backend,
        receipt.saga,
        result,
        "transaction lifecycle lost its redb coordinator",
    )
}

#[cfg(not(feature = "redb"))]
pub(super) fn finish_txn_lifecycle_receipt(
    _receipt: TxnLifecycleReceipt,
    _result: ResultPayload,
) -> Result<ResultPayload, String> {
    Err("transaction lifecycle requires the redb MutationBatch coordinator".to_string())
}

/// Abort after a volatile lifecycle effect and before its durable terminal
/// receipt is written.  This is an explicit fault-window hook for restart
/// testing; it is inert unless a request id is armed in the environment.
/// Keeping the hook at this boundary exercises the real signed dispatch path
/// without creating another replay or idempotency authority.
pub(crate) fn fault_after_txn_lifecycle_effect(req_id: u64) {
    let Ok(armed) = std::env::var("EPISTEMIC_GRAPH_LIFECYCLE_EFFECT_FAULT_REQUEST_ID") else {
        return;
    };
    if armed.parse::<u64>().ok() == Some(req_id) {
        eprintln!("EPISTEMIC_GRAPH_LIFECYCLE_EFFECT_FAULT_REQUEST_ID armed for request {req_id}");
        std::process::abort();
    }
}

/// Admission result for a GraphQL-native begin/stage/read/rollback operation.
///
/// These operations mutate the process registry, but their replay identity and
/// terminal result belong to the same named admin saga used by the native
/// `BeginTxn`/`Txn*`/`Rollback` lifecycle.  Keeping the receipt behind this
/// facade lets the GraphQL handler execute its existing registry primitive after
/// admission without creating a second replay ledger or coordinator.
#[cfg(feature = "graphql")]
pub(crate) enum GraphQlLifecycleAdmission {
    Replayed(ResultPayload),
    /// Boxed because the receipt is ~450 bytes against the replayed payload's
    /// much smaller one, so unboxed both arms paid the receipt's size. Safe
    /// here for the same reason as elsewhere in this module: this enum is the
    /// in-process return of one admission call, derives no `Serialize`, and
    /// never crosses the wire or a durable boundary -- the receipt it carries
    /// has its own durable representation, which boxing does not touch.
    Execute(Box<TxnLifecycleReceipt>),
}

/// Consume the verified carrier's nonce and stable key for one native GraphQL
/// staging operation.  A fresh nonce with the same operation identity returns
/// `Replayed`; a reused nonce or changed method body is rejected by the kernel.
#[cfg(feature = "graphql")]
pub(crate) async fn begin_graphql_lifecycle(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    caller: &str,
    authority: &CarrierAuthority,
    method: &Method,
) -> Result<GraphQlLifecycleAdmission, String> {
    let receipt = begin_txn_lifecycle_receipt(state, req_id, caller, authority, method).await?;
    #[cfg(feature = "redb")]
    if let Some(result) = receipt.saga.replayed.clone() {
        return Ok(GraphQlLifecycleAdmission::Replayed(result));
    }
    Ok(GraphQlLifecycleAdmission::Execute(Box::new(receipt)))
}

/// Terminalize a native GraphQL staging operation through the same durable
/// lifecycle receipt used by the ordinary transaction-family methods.
#[cfg(feature = "graphql")]
pub(crate) fn finish_graphql_lifecycle(
    receipt: TxnLifecycleReceipt,
    result: ResultPayload,
) -> Result<ResultPayload, String> {
    finish_txn_lifecycle_receipt(receipt, result)
}
