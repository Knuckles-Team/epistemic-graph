//! Durable cross-store commit-intent log for a mixed graph+user-table SQL
//! transaction (CONCEPT:EG-TXN.mixed-commit-intent, NE-004).
//!
//! One SQL `BEGIN … COMMIT` block that stages BOTH graph-node ops AND
//! user-table ops commits through two INDEPENDENT redb authorities,
//! sequentially: `commit_cross_modal_txn` (graph/vector/OWL) first, then
//! `TableStore::commit_txn_batch` (user tables) second — see
//! `WireSession::run_commit`'s doc in `wire/mod.rs`. Each side is
//! individually atomic (one redb `WriteTransaction`); the PAIR is not,
//! unless the recipe recorded here survives a crash between them.
//!
//! ## Protocol
//! 1. Before EITHER commit is attempted, the whole replay recipe — the
//!    graph-side FORWARD `Method`s, their pre-image COMPENSATING `Method`s
//!    (computed by the caller from the CURRENT durable node state, before
//!    the graph write lands), and the table-side statement replay log — is
//!    written to ONE owner-scoped file and fsynced ([`write_intent`]).
//! 2. The graph commit runs, keyed by the intent's own `operation_id` so a
//!    retry (recovery OR a live retry) is idempotent — the SAME coordinator
//!    key `commit_cross_modal_txn` already dedupes commits on.
//! 3. The table commit runs, keyed by the SAME `operation_id` — the SAME
//!    idempotency-key derivation `commit_table_txn` already dedupes on.
//! 4. On full success the intent file is deleted ([`delete_intent`]) —
//!    nothing left to recover.
//! 5. On a CLEAN (non-crash) table-commit rejection, the live caller
//!    compensates synchronously: replays `compensating_methods` under
//!    [`CommitIntent::compensation_operation_id`] (a id DETERMINISTIC in the
//!    original `operation_id`, so a crash mid-compensation is itself
//!    replay-safe), then deletes the intent.
//! 6. On a CRASH between step 2 and step 4, the intent file survives on
//!    disk. The next time THIS OWNER's connection touches its store, a lazy
//!    sweep (`WireSession::recover_owner_intents`) finds it via
//!    [`list_intents`] and reruns steps 2-5 — self-healing, no torn state.
//!
//! ## Privacy
//! Like `crate::server::sql_tables`, this never writes tenant, principal, or
//! filesystem detail into a filename or an error message: the owner
//! directory and the intent filename are one-way SHA-256 digests, and the
//! payload itself carries no tenant/principal string at all — recovery is
//! ONLY ever driven by a live, already-authenticated session for the
//! matching owner, so there is nothing identity-bearing to persist.
//!
//! `eg_query::TxnOp` is not `Serialize` (durable persistence has never lived
//! in that crate), so the table-side replay recipe is recorded as the
//! ORIGINAL literal input (SQL text, or a decoded `COPY` batch) rather than
//! the parsed op — replayed through the ordinary buffering dispatch on
//! recovery, which re-derives an equivalent `TxnOp` the exact same way the
//! original statement did.

use std::path::{Path, PathBuf};

use sha2::{Digest, Sha256};

use crate::protocol::Method;
use crate::server::access::CarrierAuthority;

const TXN_INTENT_DIR: &str = "txn-intent";
const INTENT_SCHEMA_VERSION: u16 = 1;
const MAX_INTENT_BYTES: usize = 16 * 1024 * 1024;
const MAX_INTENT_ITEMS: usize = 1_000_000;

/// One buffered table-store statement replay step (CONCEPT:EG-TXN.mixed-commit-intent), recorded as the wire
/// session buffers a table DML/DDL statement inside an open mixed
/// transaction. Replayed through the ordinary buffering dispatch on
/// recovery to rebuild an equivalent `TableTxn`.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) enum ReplayStep {
    /// A complete literal SQL statement that, when re-classified and
    /// re-dispatched with `in_txn = true`, buffers the SAME table op(s) it
    /// did originally.
    Sql(String),
    /// A decoded `COPY … FROM STDIN` batch (buffered directly as
    /// `TxnOp::Insert`, never as SQL text, so it is recorded structurally).
    CopyRows {
        table: String,
        columns: Vec<String>,
        rows: Vec<Vec<serde_json::Value>>,
    },
}

/// The durable recovery recipe for one mixed graph+table transaction.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub(crate) struct CommitIntent {
    schema_version: u16,
    pub(crate) graph: String,
    /// The hyphenated UUID text of the shared operation id (stored as a
    /// string, not `uuid::Uuid`, because this workspace's `uuid` dependency
    /// does not enable the `serde` feature).
    operation_id: String,
    pub(crate) forward_methods: Vec<Method>,
    pub(crate) compensating_methods: Vec<Method>,
    pub(crate) table_steps: Vec<ReplayStep>,
    #[allow(dead_code)] // recovery diagnostics / future TTL sweep, not read yet
    pub(crate) created_at_ms: u64,
}

impl CommitIntent {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        graph: String,
        operation_id: uuid::Uuid,
        forward_methods: Vec<Method>,
        compensating_methods: Vec<Method>,
        table_steps: Vec<ReplayStep>,
        created_at_ms: u64,
    ) -> Self {
        Self {
            schema_version: INTENT_SCHEMA_VERSION,
            graph,
            operation_id: operation_id.simple().to_string(),
            forward_methods,
            compensating_methods,
            table_steps,
            created_at_ms,
        }
    }

    pub(crate) fn operation_id(&self) -> uuid::Uuid {
        // Constructed only from `Self::new`/`decode`, both of which always
        // write a valid simple-form UUID string; a corrupt value cannot
        // reach here (`list_intents` already discards undecodable records).
        uuid::Uuid::parse_str(&self.operation_id).unwrap_or_else(|_| uuid::Uuid::nil())
    }

    /// A DETERMINISTIC child id for the compensating write, derived from
    /// this intent's own `operation_id` — so a retried/crashed compensation
    /// attempt keys onto the SAME idempotent coordinator id every time,
    /// distinct from the forward write's id (so the two never collide as
    /// the same MutationBatch identity).
    pub(crate) fn compensation_operation_id(&self) -> uuid::Uuid {
        let mut hasher = Sha256::new();
        hasher.update(b"epistemic-graph/txn-intent-compensation\0");
        hasher.update(self.operation_id.as_bytes());
        let digest = hasher.finalize();
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(&digest[..16]);
        uuid::Uuid::from_bytes(bytes)
    }
}

fn owner_dir(authority: &CarrierAuthority, persist_dir: &Path) -> PathBuf {
    let mut digest = Sha256::new();
    digest.update(b"epistemic-graph/txn-intent-owner\0");
    digest.update(authority.tenant_scope().as_bytes());
    digest.update([0]);
    digest.update(authority.agent_id().as_bytes());
    persist_dir
        .join(TXN_INTENT_DIR)
        .join(hex::encode(digest.finalize()))
}

fn intent_path(dir: &Path, operation_id: uuid::Uuid) -> PathBuf {
    // The filename is a SHA-256 digest of the (already opaque, random)
    // operation id, keeping every filename in this durable log the SAME
    // shape as `sql_tables.rs`'s owner files — a bare digest, never a raw
    // identifier.
    let mut digest = Sha256::new();
    digest.update(b"epistemic-graph/txn-intent-file\0");
    digest.update(operation_id.as_bytes());
    dir.join(format!("{}.intent", hex::encode(digest.finalize())))
}

/// Durably write (and fsync) `intent` to its owner-scoped file, creating the
/// owner directory if needed. Called BEFORE either half of a mixed commit is
/// attempted, so a crash at any point after this call returns is
/// self-healing (see the module doc).
pub(crate) fn write_intent(
    authority: &CarrierAuthority,
    persist_dir: &Path,
    intent: &CommitIntent,
) -> Result<(), String> {
    let dir = owner_dir(authority, persist_dir);
    std::fs::create_dir_all(&dir)
        .map_err(|_| "commit-intent directory is unavailable".to_string())?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700))
            .map_err(|_| "commit-intent directory permissions could not be applied".to_string())?;
    }
    let bytes = rmp_serde::to_vec_named(intent)
        .map_err(|_| "commit-intent record encode failed".to_string())?;
    eg_types::msgpack::validate_single_value(
        &bytes,
        eg_types::msgpack::MsgpackLimits::new(MAX_INTENT_BYTES, MAX_INTENT_ITEMS, 64),
    )
    .map_err(|_| "commit-intent record exceeds limits".to_string())?;
    let path = intent_path(&dir, intent.operation_id());
    let tmp_path = path.with_extension("intent.tmp");
    {
        use std::io::Write;
        let mut file = std::fs::File::create(&tmp_path)
            .map_err(|_| "commit-intent file could not be created".to_string())?;
        file.write_all(&bytes)
            .map_err(|_| "commit-intent file write failed".to_string())?;
        file.sync_all()
            .map_err(|_| "commit-intent file could not be durably synced".to_string())?;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&tmp_path, std::fs::Permissions::from_mode(0o600))
            .map_err(|_| "commit-intent file permissions could not be applied".to_string())?;
    }
    std::fs::rename(&tmp_path, &path)
        .map_err(|_| "commit-intent file could not be installed".to_string())?;
    // Best-effort durability for the rename's directory entry.
    #[cfg(unix)]
    {
        if let Ok(dir_handle) = std::fs::File::open(&dir) {
            let _ = dir_handle.sync_all();
        }
    }
    Ok(())
}

/// Delete an intent file once fully resolved (committed OR compensated).
/// A missing file is not an error — deletion is idempotent, matching every
/// other step in this recipe.
pub(crate) fn delete_intent(
    authority: &CarrierAuthority,
    persist_dir: &Path,
    operation_id: uuid::Uuid,
) -> Result<(), String> {
    let dir = owner_dir(authority, persist_dir);
    let path = intent_path(&dir, operation_id);
    match std::fs::remove_file(&path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(_) => Err("commit-intent file could not be removed".to_string()),
    }
}

/// List every leftover intent under `authority`'s owner directory — a crash
/// left these behind mid-2PC; a lazy recovery sweep resolves each. An
/// unreadable/corrupt/oversized entry is skipped (never panics a recovery
/// sweep); it stays on disk for the next sweep to retry.
pub(crate) fn list_intents(authority: &CarrierAuthority, persist_dir: &Path) -> Vec<CommitIntent> {
    let dir = owner_dir(authority, persist_dir);
    let Ok(entries) = std::fs::read_dir(&dir) else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("intent") {
            continue;
        }
        let Ok(bytes) = std::fs::read(&path) else {
            continue;
        };
        let Ok(intent) = eg_types::msgpack::decode_bounded::<CommitIntent>(
            &bytes,
            eg_types::msgpack::MsgpackLimits::new(MAX_INTENT_BYTES, MAX_INTENT_ITEMS, 64),
        ) else {
            continue;
        };
        if intent.schema_version == INTENT_SCHEMA_VERSION {
            out.push(intent);
        }
    }
    out
}
