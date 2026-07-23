//! Atomic subordinate-domain persistence for [`eg_types::MutationBatch`].
//!
//! Graph rows have a graph-redb commit kernel, while blob, KV, time-series and
//! job rows deliberately live in separate redb files.  This module gives every
//! such database the same terminal batch/status, OCC version, route fence,
//! idempotency and outbox tables.  A native store calls [`begin`] and [`finish`]
//! on the *same* `redb::WriteTransaction` that changes its authoritative rows;
//! there is therefore no coordinator/native-row split-brain window.

use eg_types::{
    mutation_batch::MutationCommitPhase, MutationBatch, MutationBatchRecord, MutationBatchStatus,
    MutationOutboxRecord, MutationVersionScope, MUTATION_BATCH_VERSION, NON_GRAPH_SOURCE_VERSION,
};
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition, WriteTransaction};
use sha2::{Digest, Sha256};

const MAX_MUTATION_RECORD_BYTES: usize = 64 * 1024 * 1024;
const MAX_MUTATION_RECORD_ITEMS: usize = 1_000_000;
const MAX_MUTATION_COLLECTION_ROWS: usize = 100_000;
const MAX_MUTATION_COLLECTION_BYTES: usize = 512 * 1024 * 1024;
const INITIAL_DOMAIN_VERSION: u64 = 0;

fn decode_record<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, String> {
    eg_types::msgpack::decode_bounded(
        bytes,
        eg_types::msgpack::MsgpackLimits::new(
            MAX_MUTATION_RECORD_BYTES,
            MAX_MUTATION_RECORD_ITEMS,
            eg_types::msgpack::DEFAULT_MAX_DEPTH,
        ),
    )
    .map_err(|_| "stored mutation record is invalid or exceeds resource limits".to_string())
}

fn decode_batch_record(bytes: &[u8]) -> Result<MutationBatchRecord, String> {
    let record: MutationBatchRecord = decode_record(bytes)?;
    record.batch.validate()?;
    Ok(record)
}

fn decode_outbox_record(bytes: &[u8]) -> Result<MutationOutboxRecord, String> {
    let record: MutationOutboxRecord = decode_record(bytes)?;
    record.validate()?;
    if record.version_scope != MutationVersionScope::NonGraph
        || record.source_graph_version != NON_GRAPH_SOURCE_VERSION
    {
        return Err("native mutation store contains a graph-scoped outbox record".to_string());
    }
    Ok(record)
}

fn account_row(rows: &mut usize, bytes: &mut usize, added: usize) -> Result<(), String> {
    *rows = (*rows)
        .checked_add(1)
        .filter(|rows| *rows <= MAX_MUTATION_COLLECTION_ROWS)
        .ok_or_else(|| "mutation record collection exceeds resource limits".to_string())?;
    *bytes = (*bytes)
        .checked_add(added)
        .filter(|bytes| *bytes <= MAX_MUTATION_COLLECTION_BYTES)
        .ok_or_else(|| "mutation record collection exceeds resource limits".to_string())?;
    Ok(())
}

pub const BATCHES: TableDefinition<'static, &str, &[u8]> = TableDefinition::new("mutation_batches");
pub const IDEMPOTENCY: TableDefinition<'static, (&str, &str, &str), &str> =
    TableDefinition::new("mutation_idempotency");
pub const VERSIONS: TableDefinition<'static, (&str, &str), u64> =
    TableDefinition::new("mutation_versions");
pub const FENCES: TableDefinition<'static, (&str, &str), &[u8]> =
    TableDefinition::new("mutation_fences");
pub const OUTBOX: TableDefinition<'static, (&str, u32), &[u8]> =
    TableDefinition::new("mutation_outbox");
/// Opaque, caller-encrypted recovery material for a prepared saga.  This table is
/// intentionally separate from [`BATCHES`] and [`OUTBOX`]: coordinator metadata
/// remains digest-only, while a crash-recovery owner may retain the minimum private
/// state needed to finish its idempotent children.  The store never decrypts or
/// interprets these bytes.
pub const PRIVATE_PAYLOADS: TableDefinition<'static, &str, &[u8]> =
    TableDefinition::new("mutation_private_payloads");

/// Privacy-safe integrity totals for a coordinator store backup/restore.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecoveryStoreCounts {
    pub batches: u64,
    pub prepared: u64,
    pub committed: u64,
    pub aborted: u64,
    pub idempotency: u64,
    pub versions: u64,
    pub fences: u64,
    pub outbox: u64,
    pub encrypted_private_payloads: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct Fence {
    placement_epoch: u64,
    fencing_token: u64,
}

/// Result of validating a proposed batch within its native write transaction.
#[derive(Debug, Clone)]
pub enum Begin {
    Apply { source_version: u64 },
    Replay(MutationBatchRecord),
}

#[derive(Debug, Clone)]
pub enum SagaBegin {
    Execute,
    Resume(MutationBatchRecord),
    Committed(MutationBatchRecord),
}

/// Materialize the shared tables in a native store's existing database.
pub fn initialize(db: &Database) -> Result<(), String> {
    let wtx = db.begin_write().map_err(|error| error.to_string())?;
    wtx.open_table(BATCHES).map_err(|error| error.to_string())?;
    wtx.open_table(IDEMPOTENCY)
        .map_err(|error| error.to_string())?;
    wtx.open_table(VERSIONS)
        .map_err(|error| error.to_string())?;
    wtx.open_table(FENCES).map_err(|error| error.to_string())?;
    wtx.open_table(OUTBOX).map_err(|error| error.to_string())?;
    wtx.open_table(PRIVATE_PAYLOADS)
        .map_err(|error| error.to_string())?;
    wtx.commit().map_err(|error| error.to_string())
}

/// Validate every coordinator receipt/link and return aggregate-only totals.
///
/// Private recovery plans are never decrypted here.  They must retain the
/// engine's authenticated-ciphertext envelope and may exist only for a prepared
/// transaction-recovery parent receipt.
pub fn validate_recovery_store(db: &Database) -> Result<RecoveryStoreCounts, String> {
    use eg_types::protocol::Method;
    let rtx = db.begin_read().map_err(|error| error.to_string())?;
    let batches = rtx.open_table(BATCHES).map_err(|error| error.to_string())?;
    let mut records = std::collections::BTreeMap::new();
    let mut counts = RecoveryStoreCounts::default();
    let mut collection_rows = 0usize;
    let mut collection_bytes = 0usize;
    for row in batches.iter().map_err(|error| error.to_string())? {
        let (key, value) = row.map_err(|error| error.to_string())?;
        account_row(
            &mut collection_rows,
            &mut collection_bytes,
            key.value()
                .len()
                .checked_add(value.value().len())
                .ok_or_else(|| "mutation record collection exceeds resource limits".to_string())?,
        )?;
        let record = decode_batch_record(value.value())?;
        if record.batch.batch_id != key.value() {
            return Err("coordinator batch key does not match its receipt".to_string());
        }
        counts.batches = counts.batches.saturating_add(1);
        match record.status {
            MutationBatchStatus::Prepared => counts.prepared = counts.prepared.saturating_add(1),
            MutationBatchStatus::Committed => counts.committed = counts.committed.saturating_add(1),
            MutationBatchStatus::Aborted => counts.aborted = counts.aborted.saturating_add(1),
        }
        records.insert(record.batch.batch_id.clone(), record);
    }

    let idempotency = rtx
        .open_table(IDEMPOTENCY)
        .map_err(|error| error.to_string())?;
    let mut idempotent_batches = std::collections::BTreeSet::new();
    for row in idempotency.iter().map_err(|error| error.to_string())? {
        let (key, value) = row.map_err(|error| error.to_string())?;
        let (tenant, graph, idempotency_key) = key.value();
        let added = tenant
            .len()
            .checked_add(graph.len())
            .and_then(|bytes| bytes.checked_add(idempotency_key.len()))
            .and_then(|bytes| bytes.checked_add(value.value().len()))
            .ok_or_else(|| "mutation record collection exceeds resource limits".to_string())?;
        account_row(&mut collection_rows, &mut collection_bytes, added)?;
        let record = records
            .get(value.value())
            .ok_or_else(|| "coordinator idempotency row points to a missing receipt".to_string())?;
        if record.batch.tenant != tenant
            || record.batch.graph != graph
            || record.batch.idempotency_key != idempotency_key
        {
            return Err("coordinator idempotency row does not bind its receipt".to_string());
        }
        if !idempotent_batches.insert(record.batch.batch_id.clone()) {
            return Err("coordinator receipt has multiple idempotency rows".to_string());
        }
        counts.idempotency = counts.idempotency.saturating_add(1);
    }
    if idempotent_batches
        != records
            .keys()
            .cloned()
            .collect::<std::collections::BTreeSet<_>>()
    {
        return Err("coordinator receipt is missing its idempotency row".to_string());
    }

    let versions = rtx
        .open_table(VERSIONS)
        .map_err(|error| error.to_string())?;
    let version_count = versions
        .iter()
        .map_err(|error| error.to_string())?
        .take(MAX_MUTATION_COLLECTION_ROWS + 1)
        .count();
    if version_count > MAX_MUTATION_COLLECTION_ROWS {
        return Err("mutation version collection exceeds resource limits".to_string());
    }
    counts.versions = version_count as u64;
    let fences = rtx.open_table(FENCES).map_err(|error| error.to_string())?;
    let fence_count = fences
        .iter()
        .map_err(|error| error.to_string())?
        .take(MAX_MUTATION_COLLECTION_ROWS + 1)
        .count();
    if fence_count > MAX_MUTATION_COLLECTION_ROWS {
        return Err("mutation fence collection exceeds resource limits".to_string());
    }
    counts.fences = fence_count as u64;

    let outbox = rtx.open_table(OUTBOX).map_err(|error| error.to_string())?;
    for row in outbox.iter().map_err(|error| error.to_string())? {
        let (key, value) = row.map_err(|error| error.to_string())?;
        account_row(
            &mut collection_rows,
            &mut collection_bytes,
            key.value()
                .0
                .len()
                .checked_add(value.value().len())
                .ok_or_else(|| "mutation record collection exceeds resource limits".to_string())?,
        )?;
        let receipt = decode_outbox_record(value.value())?;
        let (batch_id, ordinal) = key.value();
        let parent = records.get(batch_id);
        if receipt.batch_id != batch_id
            || receipt.ordinal != ordinal
            || parent.is_none()
            || parent.is_some_and(|record| record.status != MutationBatchStatus::Committed)
            || parent.and_then(|record| record.batch.outbox.get(ordinal as usize))
                != Some(&receipt.intent)
        {
            return Err("coordinator child/outbox receipt is not bound to its parent".to_string());
        }
        counts.outbox = counts.outbox.saturating_add(1);
    }

    let private = rtx
        .open_table(PRIVATE_PAYLOADS)
        .map_err(|error| error.to_string())?;
    let mut private_ids = std::collections::BTreeSet::new();
    for row in private.iter().map_err(|error| error.to_string())? {
        let (key, value) = row.map_err(|error| error.to_string())?;
        account_row(
            &mut collection_rows,
            &mut collection_bytes,
            key.value()
                .len()
                .checked_add(value.value().len())
                .ok_or_else(|| "mutation record collection exceeds resource limits".to_string())?,
        )?;
        let record = records
            .get(key.value())
            .ok_or_else(|| "private recovery plan has no coordinator parent receipt".to_string())?;
        let bytes = value.value();
        let is_recovery_plan = record.batch.operations.len() == 1
            && record.batch.operations.iter().any(|operation| {
                matches!(
                    &operation.method,
                    Method::ApplyMutation { event_type, query }
                        if event_type == "transaction_recovery_plan"
                            && query.len() == 71
                            && query.starts_with("sha256:")
                            && query[7..].bytes().all(|byte| {
                                byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)
                            })
                )
            });
        // ValueCipher format: 0xE6 + 12-byte nonce + ciphertext and 16-byte tag.
        if record.status != MutationBatchStatus::Prepared
            || !is_recovery_plan
            || bytes.len() <= 29
            || bytes[0] != 0xE6
        {
            return Err("private recovery plan is not authenticated staged ciphertext".to_string());
        }
        private_ids.insert(key.value().to_string());
        counts.encrypted_private_payloads = counts.encrypted_private_payloads.saturating_add(1);
    }
    for record in records.values() {
        let needs_plan = record.status == MutationBatchStatus::Prepared
            && record.batch.operations.iter().any(|operation| {
                matches!(
                    &operation.method,
                    Method::ApplyMutation { event_type, .. }
                        if event_type == "transaction_recovery_plan"
                )
            });
        if needs_plan && !private_ids.contains(&record.batch.batch_id) {
            return Err(
                "prepared transaction parent is missing encrypted recovery state".to_string(),
            );
        }
    }
    Ok(counts)
}

/// Return a cryptographic change token for the complete coordinator ledger.
///
/// The token is used only inside the online-backup boundary: if it changes while
/// graph shards are being snapshotted, the bundle is rejected before a manifest
/// is published. It contains no row keys, identities or private recovery bytes.
pub fn recovery_store_fingerprint(db: &Database) -> Result<[u8; 32], String> {
    fn field(hasher: &mut Sha256, value: &[u8]) {
        hasher.update((value.len() as u64).to_be_bytes());
        hasher.update(value);
    }

    let rtx = db.begin_read().map_err(|error| error.to_string())?;
    let mut hasher = Sha256::new();
    macro_rules! bytes_table {
        ($tag:literal, $definition:expr) => {{
            field(&mut hasher, $tag);
            let table = rtx
                .open_table($definition)
                .map_err(|error| error.to_string())?;
            for row in table.iter().map_err(|error| error.to_string())? {
                let (key, value) = row.map_err(|error| error.to_string())?;
                field(&mut hasher, key.value().as_bytes());
                field(&mut hasher, value.value());
            }
        }};
    }
    bytes_table!(b"batches", BATCHES);
    {
        field(&mut hasher, b"idempotency");
        let table = rtx
            .open_table(IDEMPOTENCY)
            .map_err(|error| error.to_string())?;
        for row in table.iter().map_err(|error| error.to_string())? {
            let (key, value) = row.map_err(|error| error.to_string())?;
            let (tenant, graph, idempotency_key) = key.value();
            field(&mut hasher, tenant.as_bytes());
            field(&mut hasher, graph.as_bytes());
            field(&mut hasher, idempotency_key.as_bytes());
            field(&mut hasher, value.value().as_bytes());
        }
    }
    {
        field(&mut hasher, b"versions");
        let table = rtx
            .open_table(VERSIONS)
            .map_err(|error| error.to_string())?;
        for row in table.iter().map_err(|error| error.to_string())? {
            let (key, value) = row.map_err(|error| error.to_string())?;
            let (tenant, graph) = key.value();
            field(&mut hasher, tenant.as_bytes());
            field(&mut hasher, graph.as_bytes());
            field(&mut hasher, &value.value().to_be_bytes());
        }
    }
    {
        field(&mut hasher, b"fences");
        let table = rtx.open_table(FENCES).map_err(|error| error.to_string())?;
        for row in table.iter().map_err(|error| error.to_string())? {
            let (key, value) = row.map_err(|error| error.to_string())?;
            let (tenant, graph) = key.value();
            field(&mut hasher, tenant.as_bytes());
            field(&mut hasher, graph.as_bytes());
            field(&mut hasher, value.value());
        }
    }
    {
        field(&mut hasher, b"outbox");
        let table = rtx.open_table(OUTBOX).map_err(|error| error.to_string())?;
        for row in table.iter().map_err(|error| error.to_string())? {
            let (key, value) = row.map_err(|error| error.to_string())?;
            let (batch_id, ordinal) = key.value();
            field(&mut hasher, batch_id.as_bytes());
            field(&mut hasher, &ordinal.to_be_bytes());
            field(&mut hasher, value.value());
        }
    }
    bytes_table!(b"private", PRIVATE_PAYLOADS);
    Ok(hasher.finalize().into())
}

/// Create a transactionally consistent, table-verbatim coordinator snapshot.
pub fn backup_recovery_store(
    source: &Database,
    destination: &std::path::Path,
) -> Result<RecoveryStoreCounts, String> {
    if destination.exists() {
        return Err("coordinator backup destination already exists".to_string());
    }
    validate_recovery_store(source)?;
    let rtx = source.begin_read().map_err(|error| error.to_string())?;
    let target = Database::create(destination).map_err(|error| error.to_string())?;
    let mut wtx = target.begin_write().map_err(|error| error.to_string())?;
    wtx.set_durability(redb::Durability::Immediate)
        .map_err(|error| error.to_string())?;

    macro_rules! copy_table {
        ($definition:expr) => {{
            let source_table = rtx
                .open_table($definition)
                .map_err(|error| error.to_string())?;
            let mut target_table = wtx
                .open_table($definition)
                .map_err(|error| error.to_string())?;
            for row in source_table.iter().map_err(|error| error.to_string())? {
                let (key, value) = row.map_err(|error| error.to_string())?;
                target_table
                    .insert(key.value(), value.value())
                    .map_err(|error| error.to_string())?;
            }
        }};
    }
    copy_table!(BATCHES);
    copy_table!(IDEMPOTENCY);
    copy_table!(VERSIONS);
    copy_table!(FENCES);
    copy_table!(OUTBOX);
    copy_table!(PRIVATE_PAYLOADS);
    wtx.commit().map_err(|error| error.to_string())?;
    validate_recovery_store(&target)
}

/// Validate idempotency, OCC and route fencing before native rows are changed.
/// The caller must retain `wtx` and either call [`finish`] or abort it.
pub fn begin(wtx: &WriteTransaction, batch: &MutationBatch) -> Result<Begin, String> {
    batch.validate()?;
    let idempotency_key = (
        batch.tenant.as_str(),
        batch.graph.as_str(),
        batch.idempotency_key.as_str(),
    );
    let replay_batch_id = {
        let table = wtx
            .open_table(IDEMPOTENCY)
            .map_err(|error| error.to_string())?;
        let value = table
            .get(idempotency_key)
            .map_err(|error| error.to_string())?
            .map(|value| value.value().to_string());
        value
    };
    if let Some(batch_id) = replay_batch_id {
        let record = read_record_in_wtx(wtx, &batch_id)?.ok_or_else(|| {
            format!("CORRUPT_MUTATION_LEDGER: idempotency key points to missing batch '{batch_id}'")
        })?;
        verify_replay_identity(batch, &record.batch)?;
        if record.status != MutationBatchStatus::Committed {
            return Err(format!(
                "mutation batch '{}' is not terminally committed",
                record.batch.batch_id
            ));
        }
        return Ok(Begin::Replay(record));
    }
    if let Some(record) = read_record_in_wtx(wtx, &batch.batch_id)? {
        verify_replay_identity(batch, &record.batch)?;
        return Err(format!(
            "CORRUPT_MUTATION_LEDGER: batch '{}' exists without its idempotency row",
            batch.batch_id
        ));
    }

    let scope = (batch.tenant.as_str(), batch.graph.as_str());
    let source_version = {
        let table = wtx
            .open_table(VERSIONS)
            .map_err(|error| error.to_string())?;
        let version = match table.get(scope).map_err(|error| error.to_string())? {
            Some(value) => value.value(),
            None => INITIAL_DOMAIN_VERSION,
        };
        version
    };
    if let Some(expected) = batch.expected_graph_version {
        if expected != source_version {
            return Err(format!(
                "STALE_VERSION: scope '{}/{}' expected version {} but authoritative version is {}",
                batch.tenant, batch.graph, expected, source_version
            ));
        }
    }

    let current_fence = {
        let table = wtx.open_table(FENCES).map_err(|error| error.to_string())?;
        let value = table
            .get(scope)
            .map_err(|error| error.to_string())?
            .map(|value| decode_record::<Fence>(value.value()))
            .transpose()?
            .unwrap_or(Fence {
                placement_epoch: 0,
                fencing_token: 0,
            });
        value
    };
    let proposed = Fence {
        placement_epoch: batch.placement_epoch,
        fencing_token: batch.fencing_token.unwrap_or(0),
    };
    if proposed.placement_epoch < current_fence.placement_epoch
        || (proposed.placement_epoch == current_fence.placement_epoch
            && proposed.fencing_token < current_fence.fencing_token)
    {
        return Err(format!(
            "STALE_FENCE: scope '{}/{}' route ({},{}) is older than ({},{})",
            batch.tenant,
            batch.graph,
            proposed.placement_epoch,
            proposed.fencing_token,
            current_fence.placement_epoch,
            current_fence.fencing_token
        ));
    }
    eg_types::mutation_batch::apply_certification_fault(batch, MutationCommitPhase::BeforeRows)?;
    Ok(Begin::Apply { source_version })
}

/// Persist the terminal result and coordinator metadata after native rows have
/// been changed in `wtx`.  The caller commits `wtx` exactly once afterward.
pub fn finish(
    wtx: &WriteTransaction,
    batch: &MutationBatch,
    result_msgpack: Option<Vec<u8>>,
    committed_at_ms: u64,
    source_version: u64,
) -> Result<MutationBatchRecord, String> {
    eg_types::mutation_batch::apply_certification_fault(
        batch,
        MutationCommitPhase::AfterRowsBeforeMetadata,
    )?;
    let target_version = source_version
        .checked_add(1)
        .ok_or_else(|| "mutation domain version overflow".to_string())?;
    let record = MutationBatchRecord {
        batch: batch.clone(),
        status: MutationBatchStatus::Committed,
        result_msgpack,
        committed_at_ms,
    };
    let record_bytes = rmp_serde::to_vec_named(&record).map_err(|error| error.to_string())?;
    {
        let mut table = wtx.open_table(BATCHES).map_err(|error| error.to_string())?;
        table
            .insert(batch.batch_id.as_str(), record_bytes.as_slice())
            .map_err(|error| error.to_string())?;
    }
    {
        let mut table = wtx
            .open_table(IDEMPOTENCY)
            .map_err(|error| error.to_string())?;
        table
            .insert(
                (
                    batch.tenant.as_str(),
                    batch.graph.as_str(),
                    batch.idempotency_key.as_str(),
                ),
                batch.batch_id.as_str(),
            )
            .map_err(|error| error.to_string())?;
    }
    {
        let mut table = wtx
            .open_table(VERSIONS)
            .map_err(|error| error.to_string())?;
        table
            .insert(
                (batch.tenant.as_str(), batch.graph.as_str()),
                target_version,
            )
            .map_err(|error| error.to_string())?;
    }
    {
        let fence = Fence {
            placement_epoch: batch.placement_epoch,
            fencing_token: batch.fencing_token.unwrap_or(0),
        };
        let bytes = rmp_serde::to_vec_named(&fence).map_err(|error| error.to_string())?;
        let mut table = wtx.open_table(FENCES).map_err(|error| error.to_string())?;
        table
            .insert(
                (batch.tenant.as_str(), batch.graph.as_str()),
                bytes.as_slice(),
            )
            .map_err(|error| error.to_string())?;
    }
    {
        let mut table = wtx.open_table(OUTBOX).map_err(|error| error.to_string())?;
        for (ordinal, intent) in batch.outbox.iter().enumerate() {
            let outbox = MutationOutboxRecord {
                schema_version: MUTATION_BATCH_VERSION,
                batch_id: batch.batch_id.clone(),
                ordinal: ordinal as u32,
                tenant: batch.tenant.clone(),
                graph: batch.graph.clone(),
                version_scope: MutationVersionScope::NonGraph,
                source_graph_version: NON_GRAPH_SOURCE_VERSION,
                intent: intent.clone(),
                created_at_ms: batch.created_at_ms,
            };
            outbox.validate()?;
            let bytes = rmp_serde::to_vec_named(&outbox).map_err(|error| error.to_string())?;
            table
                .insert((batch.batch_id.as_str(), ordinal as u32), bytes.as_slice())
                .map_err(|error| error.to_string())?;
        }
    }
    Ok(record)
}

/// Commit a native-domain write transaction and expose the same deterministic
/// pre-commit / acknowledgement-lost boundaries as the graph-row kernel.
/// Callers must use this after [`finish`] so rows, metadata, result and outbox
/// remain one all-or-nothing transaction under exact-binary certification.
pub fn commit(wtx: WriteTransaction, batch: &MutationBatch) -> Result<(), String> {
    eg_types::mutation_batch::apply_certification_fault(batch, MutationCommitPhase::BeforeCommit)?;
    wtx.commit().map_err(|error| error.to_string())?;
    eg_types::mutation_batch::apply_certification_fault(
        batch,
        MutationCommitPhase::AfterCommitBeforeAck,
    )
}

pub fn version(db: &Database, tenant: &str, graph: &str) -> Result<u64, String> {
    let rtx = db.begin_read().map_err(|error| error.to_string())?;
    let table = rtx
        .open_table(VERSIONS)
        .map_err(|error| error.to_string())?;
    let version = match table
        .get((tenant, graph))
        .map_err(|error| error.to_string())?
    {
        Some(value) => value.value(),
        None => INITIAL_DOMAIN_VERSION,
    };
    Ok(version)
}

pub fn read_record(db: &Database, batch_id: &str) -> Result<Option<MutationBatchRecord>, String> {
    let rtx = db.begin_read().map_err(|error| error.to_string())?;
    let table = rtx.open_table(BATCHES).map_err(|error| error.to_string())?;
    let record = table
        .get(batch_id)
        .map_err(|error| error.to_string())?
        .map(|value| decode_batch_record(value.value()))
        .transpose()?;
    Ok(record)
}

pub fn read_outbox(db: &Database, batch_id: &str) -> Result<Vec<MutationOutboxRecord>, String> {
    let rtx = db.begin_read().map_err(|error| error.to_string())?;
    let table = rtx.open_table(OUTBOX).map_err(|error| error.to_string())?;
    let mut rows = Vec::new();
    let mut row_count = 0usize;
    let mut row_bytes = 0usize;
    for row in table
        .range((batch_id, 0)..=(batch_id, u32::MAX))
        .map_err(|error| error.to_string())?
    {
        let (_, value) = row.map_err(|error| error.to_string())?;
        account_row(&mut row_count, &mut row_bytes, value.value().len())?;
        rows.push(decode_outbox_record(value.value())?);
    }
    Ok(rows)
}

/// Durably register a multi-store/saga coordinator before executing its
/// idempotent steps. A crash leaves `Prepared`, which a retry resumes; it can never
/// be mistaken for success.
pub fn prepare_saga(
    db: &Database,
    batch: &MutationBatch,
    prepared_at_ms: u64,
) -> Result<SagaBegin, String> {
    prepare_saga_with_private_payload(db, batch, prepared_at_ms, None)
}

/// Prepare a saga and durably attach opaque encrypted recovery material in the
/// SAME immediate-durability write transaction.  `private_payload` must already be
/// authenticated ciphertext; the native store is deliberately crypto-agnostic.
/// Re-entry never replaces the original bytes (AEAD nonces make byte comparison
/// meaningless); replay identity is instead bound by the digest-only operation in
/// `batch`, and the caller verifies that binding after decrypting.
pub fn prepare_saga_with_private_payload(
    db: &Database,
    batch: &MutationBatch,
    prepared_at_ms: u64,
    private_payload: Option<&[u8]>,
) -> Result<SagaBegin, String> {
    batch.validate()?;
    let mut wtx = db.begin_write().map_err(|error| error.to_string())?;
    wtx.set_durability(redb::Durability::Immediate)
        .map_err(|error| error.to_string())?;
    let existing_id = {
        let idem = wtx
            .open_table(IDEMPOTENCY)
            .map_err(|error| error.to_string())?;
        let value = idem
            .get((
                batch.tenant.as_str(),
                batch.graph.as_str(),
                batch.idempotency_key.as_str(),
            ))
            .map_err(|error| error.to_string())?
            .map(|value| value.value().to_string());
        value
    };
    if let Some(existing_id) = existing_id {
        let record = read_record_in_wtx(&wtx, &existing_id)?.ok_or_else(|| {
            format!(
                "CORRUPT_MUTATION_LEDGER: saga idempotency key points to missing batch '{existing_id}'"
            )
        })?;
        verify_replay_identity(batch, &record.batch)?;
        if private_payload.is_some()
            && record.status == MutationBatchStatus::Prepared
            && read_private_payload_in_wtx(&wtx, &record.batch.batch_id)?.is_none()
        {
            return Err(format!(
                "CORRUPT_MUTATION_LEDGER: prepared saga '{}' has no private recovery payload",
                record.batch.batch_id
            ));
        }
        wtx.abort().map_err(|error| error.to_string())?;
        return match record.status {
            MutationBatchStatus::Prepared => Ok(SagaBegin::Resume(record)),
            MutationBatchStatus::Committed => Ok(SagaBegin::Committed(record)),
            MutationBatchStatus::Aborted => {
                Err(format!("mutation saga '{}' was aborted", batch.batch_id))
            }
        };
    }
    match begin(&wtx, batch)? {
        Begin::Replay(record) => {
            wtx.abort().map_err(|error| error.to_string())?;
            return Ok(SagaBegin::Committed(record));
        }
        Begin::Apply { .. } => {}
    }
    let record = MutationBatchRecord {
        batch: batch.clone(),
        status: MutationBatchStatus::Prepared,
        result_msgpack: None,
        committed_at_ms: prepared_at_ms,
    };
    let bytes = rmp_serde::to_vec_named(&record).map_err(|error| error.to_string())?;
    {
        let mut records = wtx.open_table(BATCHES).map_err(|error| error.to_string())?;
        records
            .insert(batch.batch_id.as_str(), bytes.as_slice())
            .map_err(|error| error.to_string())?;
        let mut idem = wtx
            .open_table(IDEMPOTENCY)
            .map_err(|error| error.to_string())?;
        idem.insert(
            (
                batch.tenant.as_str(),
                batch.graph.as_str(),
                batch.idempotency_key.as_str(),
            ),
            batch.batch_id.as_str(),
        )
        .map_err(|error| error.to_string())?;
    }
    if let Some(payload) = private_payload {
        let mut private = wtx
            .open_table(PRIVATE_PAYLOADS)
            .map_err(|error| error.to_string())?;
        private
            .insert(batch.batch_id.as_str(), payload)
            .map_err(|error| error.to_string())?;
    }
    wtx.commit().map_err(|error| error.to_string())?;
    Ok(SagaBegin::Execute)
}

/// Read opaque recovery material for a still-prepared saga.  Consumers must
/// authenticate/decrypt it and verify its digest against the parent batch before
/// using it.
pub fn read_private_payload(db: &Database, batch_id: &str) -> Result<Option<Vec<u8>>, String> {
    let rtx = db.begin_read().map_err(|error| error.to_string())?;
    let table = rtx
        .open_table(PRIVATE_PAYLOADS)
        .map_err(|error| error.to_string())?;
    let payload = table
        .get(batch_id)
        .map_err(|error| error.to_string())?
        .map(|value| value.value().to_vec());
    Ok(payload)
}

/// Mark a prepared saga committed and materialize its result/version/fence/outbox.
pub fn commit_saga(
    db: &Database,
    batch: &MutationBatch,
    result_msgpack: Vec<u8>,
    committed_at_ms: u64,
) -> Result<(MutationBatchRecord, bool), String> {
    batch.validate()?;
    let mut wtx = db.begin_write().map_err(|error| error.to_string())?;
    wtx.set_durability(redb::Durability::Immediate)
        .map_err(|error| error.to_string())?;
    let record = read_record_in_wtx(&wtx, &batch.batch_id)?
        .ok_or_else(|| format!("mutation saga '{}' was not prepared", batch.batch_id))?;
    verify_replay_identity(batch, &record.batch)?;
    if record.status == MutationBatchStatus::Committed {
        // Idempotent cleanup for an older/interrupted terminalization that may
        // have left ciphertext behind.  Current commits delete it atomically below.
        {
            let mut private = wtx
                .open_table(PRIVATE_PAYLOADS)
                .map_err(|error| error.to_string())?;
            private
                .remove(batch.batch_id.as_str())
                .map_err(|error| error.to_string())?;
        }
        wtx.commit().map_err(|error| error.to_string())?;
        return Ok((record, true));
    }
    if record.status != MutationBatchStatus::Prepared {
        return Err(format!(
            "mutation saga '{}' is not resumable",
            batch.batch_id
        ));
    }
    let source_version = {
        let versions = wtx
            .open_table(VERSIONS)
            .map_err(|error| error.to_string())?;
        let version = match versions
            .get((batch.tenant.as_str(), batch.graph.as_str()))
            .map_err(|error| error.to_string())?
        {
            Some(value) => value.value(),
            None => INITIAL_DOMAIN_VERSION,
        };
        version
    };
    let committed = finish(
        &wtx,
        batch,
        Some(result_msgpack),
        committed_at_ms,
        source_version,
    )?;
    // Terminalizing the parent and erasing its encrypted staging body are one
    // atomic transition.  A crash observes either Prepared+payload or
    // Committed+result, never a Prepared parent whose only recovery source vanished.
    {
        let mut private = wtx
            .open_table(PRIVATE_PAYLOADS)
            .map_err(|error| error.to_string())?;
        private
            .remove(batch.batch_id.as_str())
            .map_err(|error| error.to_string())?;
    }
    commit(wtx, batch)?;
    Ok((committed, false))
}

fn read_private_payload_in_wtx(
    wtx: &WriteTransaction,
    batch_id: &str,
) -> Result<Option<Vec<u8>>, String> {
    let table = wtx
        .open_table(PRIVATE_PAYLOADS)
        .map_err(|error| error.to_string())?;
    let payload = table
        .get(batch_id)
        .map_err(|error| error.to_string())?
        .map(|value| value.value().to_vec());
    Ok(payload)
}

fn read_record_in_wtx(
    wtx: &WriteTransaction,
    batch_id: &str,
) -> Result<Option<MutationBatchRecord>, String> {
    let table = wtx.open_table(BATCHES).map_err(|error| error.to_string())?;
    let record = table
        .get(batch_id)
        .map_err(|error| error.to_string())?
        .map(|value| decode_batch_record(value.value()))
        .transpose()?;
    Ok(record)
}

fn verify_replay_identity(proposed: &MutationBatch, stored: &MutationBatch) -> Result<(), String> {
    let proposed_ops = rmp_serde::to_vec_named(&proposed.operations).map_err(|e| e.to_string())?;
    let stored_ops = rmp_serde::to_vec_named(&stored.operations).map_err(|e| e.to_string())?;
    if proposed.schema_version != stored.schema_version
        || proposed.batch_id != stored.batch_id
        || proposed.tenant != stored.tenant
        || proposed.graph != stored.graph
        || proposed.idempotency_key != stored.idempotency_key
        || proposed.context.principal != stored.context.principal
        || proposed.context.purpose != stored.context.purpose
        || proposed.context.policy_fingerprint != stored.context.policy_fingerprint
        || proposed.placement_epoch != stored.placement_epoch
        || proposed.fencing_token != stored.fencing_token
        || proposed.authoritative_state != stored.authoritative_state
        || proposed_ops != stored_ops
        || proposed.outbox != stored.outbox
    {
        return Err(format!(
            "IDEMPOTENCY_CONFLICT: key '{}' was already used by a different mutation",
            proposed.idempotency_key
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use eg_types::mutation_batch::{MutationDomain, MutationRequestContext, MutationSurface};
    use eg_types::protocol::Method;
    use eg_types::{MutationOperation, MUTATION_BATCH_VERSION};

    fn saga_batch() -> MutationBatch {
        MutationBatch {
            schema_version: MUTATION_BATCH_VERSION,
            batch_id: "digest-only-parent".to_string(),
            context: MutationRequestContext {
                request_id: 1,
                principal: format!("principal:sha256:{}", "a".repeat(64)),
                purpose: None,
                policy_fingerprint: None,
                trace_id: None,
            },
            tenant: "native".to_string(),
            graph: "cluster-admin".to_string(),
            placement_epoch: 0,
            idempotency_key: "digest-only-parent".to_string(),
            expected_graph_version: Some(0),
            fencing_token: None,
            authoritative_state: None,
            operations: vec![MutationOperation {
                ordinal: 0,
                surface: MutationSurface::Transaction,
                domain: MutationDomain::ControlPlane,
                method: Method::ApplyMutation {
                    event_type: "transaction_recovery_plan".to_string(),
                    query: format!("sha256:{}", "a".repeat(64)),
                },
            }],
            outbox: Vec::new(),
            created_at_ms: 1,
        }
    }

    #[test]
    fn private_payload_survives_prepare_resume_and_is_erased_with_terminal_result() {
        let dir = tempfile::tempdir().unwrap();
        let db = Database::create(dir.path().join("coordinator.redb")).unwrap();
        initialize(&db).unwrap();
        let batch = saga_batch();
        let original = b"authenticated-ciphertext-a";
        assert!(matches!(
            prepare_saga_with_private_payload(&db, &batch, 1, Some(original)).unwrap(),
            SagaBegin::Execute
        ));
        assert_eq!(
            read_private_payload(&db, &batch.batch_id).unwrap(),
            Some(original.to_vec())
        );

        // A retry carries a fresh AEAD nonce/ciphertext.  The originally committed
        // bytes remain authoritative and are not overwritten on Resume.
        assert!(matches!(
            prepare_saga_with_private_payload(&db, &batch, 2, Some(b"authenticated-ciphertext-b"))
                .unwrap(),
            SagaBegin::Resume(_)
        ));
        assert_eq!(
            read_private_payload(&db, &batch.batch_id).unwrap(),
            Some(original.to_vec())
        );

        commit_saga(&db, &batch, vec![0x01], 3).unwrap();
        assert_eq!(read_private_payload(&db, &batch.batch_id).unwrap(), None);
        assert_eq!(
            read_record(&db, &batch.batch_id).unwrap().unwrap().status,
            MutationBatchStatus::Committed
        );
    }

    #[test]
    fn native_outbox_uses_explicit_non_graph_version_semantics() {
        let dir = tempfile::tempdir().unwrap();
        let db = Database::create(dir.path().join("native.redb")).unwrap();
        initialize(&db).unwrap();
        let mut batch = saga_batch();
        batch.outbox.push(eg_types::MutationOutboxIntent {
            topic: "native.committed".to_string(),
            key: batch.batch_id.clone(),
            payload: Vec::new(),
            headers: std::collections::BTreeMap::new(),
        });
        prepare_saga(&db, &batch, 1).unwrap();
        commit_saga(&db, &batch, vec![0x01], 2).unwrap();

        let outbox = read_outbox(&db, &batch.batch_id).unwrap();
        assert_eq!(outbox.len(), 1);
        assert_eq!(outbox[0].schema_version, MUTATION_BATCH_VERSION);
        assert_eq!(outbox[0].version_scope, MutationVersionScope::NonGraph);
        assert_eq!(outbox[0].source_graph_version, NON_GRAPH_SOURCE_VERSION);
        outbox[0].validate().unwrap();
    }

    #[test]
    fn missing_native_version_is_zero_not_caller_seeded() {
        let dir = tempfile::tempdir().unwrap();
        let db = Database::create(dir.path().join("native.redb")).unwrap();
        initialize(&db).unwrap();
        let mut batch = saga_batch();
        batch.expected_graph_version = Some(7);
        let wtx = db.begin_write().unwrap();
        let error = begin(&wtx, &batch).unwrap_err();
        assert!(error.contains("authoritative version is 0"));
    }
}
