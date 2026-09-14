//! Private transaction receipts implementation.

use super::*;

#[path = "receipts/lifecycle.rs"]
mod lifecycle;
pub(crate) use lifecycle::*;

pub(super) struct TxnRestoreGuard {
    open: Arc<dashmap::DashMap<String, parking_lot::Mutex<GraphTxnState>>>,
    txn_id: String,
    txn: Option<GraphTxnState>,
}

impl TxnRestoreGuard {
    pub(super) fn new(
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

    pub(super) fn complete(&mut self) {
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
/// Both keyed and unkeyed receipts retain a verifiable, privacy-safe tenant
/// binding in their durable identity. Parent-only consensus phases can therefore
/// verify the tenant even after terminalization erases the private recovery plan.
pub(super) fn commit_receipt_id(
    txn_id: &str,
    idempotency_key: Option<&str>,
    tenant_scope: Option<&str>,
) -> String {
    let operation_id = match idempotency_key {
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
        None => crate::server::mutation_batch::opaque_coordinator_key(
            "transaction-receipt-tenant",
            tenant_scope.unwrap_or("unknown"),
            &transaction_receipt_id(txn_id),
        ),
    };
    tenant_bound_receipt_id(tenant_scope.unwrap_or("unknown"), &operation_id)
}

fn tenant_bound_receipt_id(tenant_scope: &str, operation_id: &str) -> String {
    let binding = crate::server::mutation_batch::opaque_coordinator_key(
        "transaction-receipt-scope",
        tenant_scope,
        operation_id,
    );
    format!("{binding}:{operation_id}")
}

/// Validate the id read from the authenticated durable parent, never an
/// unverified routing hint. Reconstructing the whole id binds both the tenant
/// and operation; merely checking a caller-provided tenant prefix would not.
pub(super) fn validate_transaction_receipt_tenant(
    durable_receipt_id: &str,
    tenant_scope: &str,
) -> Result<(), String> {
    let operation_id = durable_receipt_id
        .strip_prefix("transaction-receipt-scope:")
        .and_then(|scoped| scoped.split_once(':'))
        .map(|(_, operation)| operation)
        .ok_or_else(|| "transaction parent has no verifiable tenant binding".to_string())?;
    if tenant_scope.trim().is_empty()
        || !valid_receipt_operation_id(operation_id)
        || tenant_bound_receipt_id(tenant_scope, operation_id) != durable_receipt_id
    {
        return Err("transaction parent does not match caller tenant scope".to_string());
    }
    Ok(())
}

fn valid_receipt_operation_id(operation_id: &str) -> bool {
    let Some((namespace, digest)) = operation_id.split_once(':') else {
        return false;
    };
    matches!(
        namespace,
        "transaction-receipt" | "transaction-receipt-tenant"
    ) && digest.len() == 64
        && digest
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

/// Check a specific open transaction's ownership without consulting durable
/// receipt state. Durable keyed replay must remain in `begin_txn_receipt`, after
/// the commit handler has removed the transaction under its restore guard.
pub(crate) async fn preflight_open_txn_authority(
    state: &Arc<RwLock<ServerState>>,
    req_id: u64,
    verified_context: &VerifiedRequestContext,
    txn_id: &str,
) -> Result<(), Response> {
    let authority = CarrierAuthority::from_verified(verified_context)
        .map_err(|error| Response::err(req_id, error))?;
    let state_guard = state.read().await;
    if let Some(entry) = state_guard.open_txns.get(txn_id) {
        let txn = entry.value().lock();
        if txn.tenant_scope != authority.tenant_scope() || txn.agent != authority.owner_scope() {
            crate::metrics::access_denied();
            return Err(Response::err(
                req_id,
                "ACCESS_DENIED: transaction is not owned by caller",
            ));
        }
    }
    Ok(())
}

/// The owner scope stored on an ephemeral or recovered transaction, derived
/// from the same tenant/actor pair as [`CarrierAuthority`].  Replicated apply
/// only carries the privacy-safe actor fingerprint, so this helper is the
/// state-machine-side equivalent of `CarrierAuthority::owner_scope`.
/// Native dispatch supplies the verified principal independently of effective
/// agent ACL attribution. Replicated prepare already carries its fingerprint.
pub(super) fn txn_receipt_principal(caller: Option<&str>) -> Result<String, String> {
    #[cfg(feature = "redb")]
    if let Ok(authority) = crate::server::handlers::admin::current_admin_saga_authority() {
        return Ok(authority.actor_scope().to_string());
    }
    crate::server::mutation_batch::principal_fingerprint(
        caller.ok_or_else(|| "transaction receipt requires a verified principal".to_string())?,
    )
}

pub(super) fn txn_owner_scope(tenant_scope: &str, principal: &str) -> Result<String, String> {
    let actor_scope = txn_receipt_principal(Some(principal))?;
    Ok(crate::server::mutation_batch::opaque_coordinator_key(
        "carrier-owner",
        tenant_scope,
        &actor_scope,
    ))
}

pub(super) fn validate_txn_owner(
    txn: &GraphTxnState,
    expected_tenant: Option<&str>,
    principal: &str,
) -> Result<(), String> {
    let expected_tenant = expected_tenant
        .filter(|tenant| !tenant.trim().is_empty())
        .ok_or_else(|| "transaction commit requires a verified tenant scope".to_string())?;
    if txn.tenant_scope != expected_tenant {
        return Err("transaction commit does not match caller tenant scope".to_string());
    }
    let expected_owner = txn_owner_scope(expected_tenant, principal)?;
    if txn.agent != expected_owner {
        return Err("transaction commit does not match caller principal scope".to_string());
    }
    Ok(())
}

pub(super) fn validate_txn_owner_scope(
    txn: &GraphTxnState,
    expected_tenant: Option<&str>,
    expected_owner: &str,
) -> Result<(), String> {
    let expected_tenant = expected_tenant
        .filter(|tenant| !tenant.trim().is_empty())
        .ok_or_else(|| "transaction commit requires a verified tenant scope".to_string())?;
    if txn.tenant_scope != expected_tenant {
        return Err("transaction commit does not match caller tenant scope".to_string());
    }
    if txn.agent != expected_owner {
        return Err("transaction commit does not match caller principal scope".to_string());
    }
    Ok(())
}

/// The three values that together select ONE durable commit receipt, and the
/// only three [`commit_receipt_id`] hashes. They are never meaningful apart:
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
        validate_txn_commit_result(&result)?;
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
    let principal = txn_receipt_principal(caller)?;
    let caller = Some(principal.as_str());
    let backend = backend.ok_or_else(|| {
        "transaction commit requires an authoritative MutationBatch backend".to_string()
    })?;
    let redb = backend
        .as_redb()
        .ok_or_else(|| "transaction commit requires durable redb".to_string())?;
    let saga = admit_txn_saga(
        redb,
        req_id,
        caller,
        txn_id,
        txn,
        idempotency_key,
        attempt_nonce,
    )?;
    let replayed = saga.replayed.clone();
    Ok((TxnReceipt { backend, saga }, replayed))
}

#[cfg(feature = "redb")]
fn admit_txn_saga(
    redb: &crate::server::persistence::redb_backend::RedbBackend,
    req_id: u64,
    caller: Option<&str>,
    txn_id: &str,
    txn: &GraphTxnState,
    idempotency_key: Option<&str>,
    attempt_nonce: Option<Nonce>,
) -> Result<crate::server::handlers::admin::AdminSaga, String> {
    let intent_digest = txn.replay_intent_digest()?;
    let batch_id = commit_receipt_id(txn_id, idempotency_key, Some(txn.tenant_scope.as_str()));
    // This function is called only after the open transaction has been removed
    // under `TxnRestoreGuard`. A keyed fresh re-stage may therefore inspect the
    // existing receipt without leaving a live duplicate handle behind. Compare
    // the stable semantic intent before nonce admission: changed writes under a
    // reused key are a conflict, while a changed OCC snapshot is replay-safe.
    if idempotency_key.is_some() {
        if let Some(existing) =
            crate::server::handlers::admin::resume_named_admin_saga(redb, &batch_id, caller)?
        {
            let existing_digest = transaction_plan_digest(&existing.batch)?;
            if existing_digest != intent_digest {
                return Err(
                    "transaction idempotency key conflicts with a different transaction intent"
                        .to_string(),
                );
            }
            if existing.prepared {
                return Err(
                    "transaction commit receipt is Prepared; refusing to execute a fresh transaction under the existing key"
                        .to_string(),
                );
            }
            let admitted = retry_txn_receipt(
                redb,
                req_id,
                caller
                    .ok_or_else(|| "transaction recovery requires a verified actor".to_string())?,
                existing,
                attempt_nonce,
            )?;
            let result = admitted
                .replayed
                .clone()
                .ok_or_else(|| "terminal transaction receipt has no replay result".to_string())?;
            validate_txn_commit_result(&result)?;
            return Ok(admitted);
        }
    }
    let (payload_digest, encrypted_payload) = seal_txn_recovery_plan(redb, txn)?;
    let saga =
        crate::server::handlers::admin::begin_named_admin_saga_with_private_payload_and_nonce(
            redb,
            req_id,
            caller,
            attempt_nonce,
            crate::server::handlers::admin::AdminSagaPayload {
                domain: crate::mutation_batch::DurabilityDomain::ControlPlane,
                batch_id: &batch_id,
                event_type: "transaction_recovery_plan",
                payload_digest: &payload_digest,
                encrypted_payload: &encrypted_payload,
            },
        )?;
    Ok(saga)
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
    let prepared = saga.prepared;
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
    if let Some(result) = replayed.as_ref() {
        validate_txn_commit_result(result)?;
    }
    let txn = if replayed.is_none() {
        let encrypted = eg_transaction::read_private_payload(
            &redb.admin_mutations_read()?,
            &saga.batch.batch_id,
        )?
        .ok_or_else(|| "prepared transaction has no encrypted recovery plan".to_string())?;
        let owner = txn_owner_scope(
            expected_tenant
                .ok_or_else(|| "prepared transaction has no verified tenant scope".to_string())?,
            caller,
        )?;
        let txn = open_txn_recovery_plan(redb, &saga.batch, &encrypted, owner)?;
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
    let principal = txn_receipt_principal(caller)?;
    let caller = principal.as_str();
    let expected_tenant = Some(
        expected_tenant
            .filter(|tenant| !tenant.is_empty())
            .ok_or_else(|| "transaction recovery requires a verified tenant".to_string())?,
    );
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
    let plaintext = txn.encode_recovery_plan()?;
    let digest = txn.replay_intent_digest()?;
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
    let txn = GraphTxnState::decode_recovery_plan(&plaintext, agent)?;
    if txn.replay_intent_digest()? != expected {
        return Err(
            "transaction recovery plan digest does not match its parent receipt".to_string(),
        );
    }
    Ok(txn)
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
    // The query stores the stable semantic intent digest, not the encrypted
    // recovery plaintext digest. The latter includes OCC observations and would
    // incorrectly turn a fresh lost-response re-stage into a conflict.
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
    validate_txn_commit_result(&result)?;
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

/// Transaction commit receipts are terminal boolean outcomes.  The parent
/// coordinator stores the outer `ResultPayload`, so replay and closure must
/// validate that envelope before returning it; decoding an outer receipt as a
/// method body would silently accept a different transaction result family.
pub(super) fn validate_txn_commit_result(result: &ResultPayload) -> Result<(), String> {
    if matches!(result, ResultPayload::Bool(_)) {
        Ok(())
    } else {
        Err("transaction parent receipt has the wrong result type".to_string())
    }
}
