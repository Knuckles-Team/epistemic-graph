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
//! 5. On a CLEAN (non-crash) first-attempt table-commit rejection, the live
//!    caller compensates synchronously: replays `compensating_methods` under
//!    [`CommitIntent::compensation_operation_id`] (a id DETERMINISTIC in the
//!    original `operation_id`, so a crash mid-compensation is itself
//!    replay-safe), then deletes the intent. A durable same-key table replay
//!    conflict returns without compensation because its graph phase may be the
//!    already-successful original commit; that conflicting intent is retired.
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

    /// Compare the durable replay recipe while ignoring creation time metadata.
    /// A retry may be reconstructed milliseconds later, but it must carry the
    /// same graph/table payload under the same operation id before it can reuse
    /// an intent that survived a crash.
    fn same_replay_recipe(&self, other: &Self) -> Result<bool, String> {
        let left = rmp_serde::to_vec_named(&(
            &self.schema_version,
            &self.operation_id,
            &self.graph,
            &self.forward_methods,
            &self.compensating_methods,
            &self.table_steps,
        ))
        .map_err(|_| "commit-intent replay recipe encode failed".to_string())?;
        let right = rmp_serde::to_vec_named(&(
            &other.schema_version,
            &other.operation_id,
            &other.graph,
            &other.forward_methods,
            &other.compensating_methods,
            &other.table_steps,
        ))
        .map_err(|_| "commit-intent replay recipe encode failed".to_string())?;
        Ok(left == right)
    }

    /// Stable payload-free descriptor for the exact table-side replay recipe.
    ///
    /// The SQL MutationBatch stores only the digest of this descriptor, never
    /// the statements themselves. Recovery can therefore prove that an
    /// already-committed batch belongs to this exact owner-scoped intent before
    /// repairing CREATE ownership, rather than trusting a generic idempotency
    /// conflict or the mere existence of a same-named table.
    pub(crate) fn table_operation_descriptor(&self) -> Result<String, String> {
        let encoded = rmp_serde::to_vec_named(&self.table_steps)
            .map_err(|_| "commit-intent table replay recipe encode failed".to_string())?;
        Ok(format!(
            "transaction:sha256:{}",
            hex::encode(Sha256::digest(encoded))
        ))
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
    // The filename is a SHA-256 digest of the opaque operation id, keeping
    // every filename in this durable log the SAME shape as `sql_tables.rs`'s
    // owner files — a bare digest, never a raw identifier.
    let mut digest = Sha256::new();
    digest.update(b"epistemic-graph/txn-intent-file\0");
    digest.update(operation_id.as_bytes());
    dir.join(format!("{}.intent", hex::encode(digest.finalize())))
}

fn sync_owner_dir(dir: &Path) -> Result<(), String> {
    let dir_handle = std::fs::File::open(dir)
        .map_err(|_| "commit-intent directory could not be opened for sync".to_string())?;
    dir_handle
        .sync_all()
        .map_err(|_| "commit-intent directory could not be durably synced".to_string())
}

#[cfg(test)]
type PreInstallHook = std::sync::Arc<dyn Fn(&Path, &Path) + Send + Sync>;

#[cfg(test)]
static PRE_INSTALL_HOOK: std::sync::OnceLock<std::sync::Mutex<Option<PreInstallHook>>> =
    std::sync::OnceLock::new();

#[cfg(test)]
fn invoke_pre_install_hook(tmp_path: &Path, path: &Path) {
    let hook = PRE_INSTALL_HOOK
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .expect("pre-install hook lock")
        .clone();
    if let Some(hook) = hook {
        hook(tmp_path, path);
    }
}

#[cfg(test)]
fn set_pre_install_hook(hook: Option<PreInstallHook>) {
    *PRE_INSTALL_HOOK
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .expect("pre-install hook lock") = hook;
}

/// Durably publish (and fsync) `intent` at its owner-scoped file, creating the
/// owner directory if needed. The record is first written and fsynced to a
/// unique same-directory temporary file, then installed with a no-replace hard
/// link. The canonical path is therefore either absent or complete; it is
/// never exposed while its bytes are being written. A concurrent or
/// crash-retry write cannot replace an existing recipe: an exact replay is
/// accepted as a no-op, while a changed recipe returns a conflict. Called
/// BEFORE either half of a mixed commit is attempted, so a crash at any point
/// after this call returns is self-healing (see the module doc).
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
    let tmp_path = dir.join(format!(
        ".{}.{}.tmp",
        path.file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("commit-intent"),
        uuid::Uuid::new_v4().simple()
    ));
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(&tmp_path)
        .map_err(|_| "commit-intent temporary file could not be created".to_string())?;
    use std::io::Write;
    file.write_all(&bytes).map_err(|_| {
        let _ = std::fs::remove_file(&tmp_path);
        "commit-intent temporary file write failed".to_string()
    })?;
    file.sync_all().map_err(|_| {
        let _ = std::fs::remove_file(&tmp_path);
        "commit-intent temporary file could not be durably synced".to_string()
    })?;
    drop(file);

    #[cfg(test)]
    invoke_pre_install_hook(&tmp_path, &path);

    // `hard_link` fails with AlreadyExists instead of replacing the canonical
    // entry, giving us the no-replace publication primitive available in the
    // standard library. The source and destination share this owner directory,
    // so publication is atomic and cannot expose a partially written record.
    match std::fs::hard_link(&tmp_path, &path) {
        Ok(()) => {
            let _ = std::fs::remove_file(&tmp_path);
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            let _ = std::fs::remove_file(&tmp_path);
            let existing_bytes = std::fs::read(&path)
                .map_err(|_| "existing commit-intent could not be read".to_string())?;
            let existing = eg_types::msgpack::decode_bounded::<CommitIntent>(
                &existing_bytes,
                eg_types::msgpack::MsgpackLimits::new(MAX_INTENT_BYTES, MAX_INTENT_ITEMS, 64),
            )
            .map_err(|_| "existing commit-intent is corrupt or oversized".to_string())?;
            if existing.operation_id() != intent.operation_id() {
                return Err("commit-intent path has a different operation id".to_string());
            }
            if existing.same_replay_recipe(intent)? {
                sync_owner_dir(&dir)?;
                return Ok(());
            }
            return Err(
                "IDEMPOTENCY_CONFLICT: commit-intent recipe differs from the durable retry"
                    .to_string(),
            );
        }
        Err(_) => {
            let _ = std::fs::remove_file(&tmp_path);
            return Err("commit-intent file could not be published".to_string());
        }
    }

    sync_owner_dir(&dir)?;
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

#[cfg(test)]
mod tests {
    use super::*;

    fn authority() -> CarrierAuthority {
        CarrierAuthority::from_verified(
            &crate::server::authority_context::VerifiedRequestContext::verified_for_test_in_tenant(
                "intent-test-agent",
                "intent-test-tenant",
            ),
        )
        .expect("build test authority")
    }

    struct PreInstallHookReset;

    impl Drop for PreInstallHookReset {
        fn drop(&mut self) {
            set_pre_install_hook(None);
        }
    }

    fn original_intent(operation_id: uuid::Uuid, created_at_ms: u64) -> CommitIntent {
        CommitIntent::new(
            "intent-test-graph".to_string(),
            operation_id,
            vec![Method::AddNode {
                node_id: "intent-test-node".to_string(),
                properties_msgpack: vec![1, 2, 3],
            }],
            vec![Method::RemoveNode {
                node_id: "intent-test-node".to_string(),
            }],
            vec![ReplayStep::Sql(
                "CREATE TABLE intent_test (id INT)".to_string(),
            )],
            created_at_ms,
        )
    }

    #[test]
    fn pre_install_pause_and_crash_retry_publish_complete_recipes() {
        let authority = authority();
        let persist_dir = tempfile::tempdir().expect("create intent test directory");
        let _hook_reset = PreInstallHookReset;
        let operation_id = uuid::Uuid::from_u128(0x1234);
        let original = original_intent(operation_id, 1);
        let dir = owner_dir(&authority, persist_dir.path());
        let path = intent_path(&dir, operation_id);

        // Pause the real write_intent path after its temporary file is fsynced
        // and before hard-link publication. The test thread is the concurrent
        // observer: it can inspect the complete temp record while the
        // canonical path remains absent, then release the writer.
        let (staged_tx, staged_rx) = std::sync::mpsc::sync_channel(0);
        let (resume_tx, resume_rx) = std::sync::mpsc::sync_channel(0);
        let resume_rx = std::sync::Arc::new(std::sync::Mutex::new(resume_rx));
        let pause_path = path.clone();
        let pause_hook: PreInstallHook = std::sync::Arc::new(move |tmp_path, path| {
            if path != pause_path {
                return;
            }
            staged_tx
                .send((tmp_path.to_path_buf(), path.to_path_buf()))
                .expect("report paused publication");
            resume_rx
                .lock()
                .expect("pause lock")
                .recv()
                .expect("resume publication");
        });
        set_pre_install_hook(Some(pause_hook));
        let writer_authority = authority.clone();
        let writer_dir = persist_dir.path().to_path_buf();
        let writer_intent = original.clone();
        let writer = std::thread::spawn(move || {
            write_intent(&writer_authority, &writer_dir, &writer_intent)
        });
        let (staged_path, observed_path) = staged_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("observe real pre-install pause");
        assert_eq!(observed_path, path);
        assert!(staged_path.exists(), "fsynced temp must remain observable");
        assert!(
            !path.exists(),
            "canonical intent must be absent before hard-link publication"
        );
        let staged_bytes = std::fs::read(&staged_path).expect("read fsynced temp");
        let staged = eg_types::msgpack::decode_bounded::<CommitIntent>(
            &staged_bytes,
            eg_types::msgpack::MsgpackLimits::new(MAX_INTENT_BYTES, MAX_INTENT_ITEMS, 64),
        )
        .expect("fsynced temp must contain a complete recipe");
        assert!(staged
            .same_replay_recipe(&original)
            .expect("compare staged recipe"));
        resume_tx.send(()).expect("resume real publication");
        writer
            .join()
            .expect("join paused writer")
            .expect("publish complete intent");
        set_pre_install_hook(None);

        let published_bytes = std::fs::read(&path).expect("read published intent");
        let published = eg_types::msgpack::decode_bounded::<CommitIntent>(
            &published_bytes,
            eg_types::msgpack::MsgpackLimits::new(MAX_INTENT_BYTES, MAX_INTENT_ITEMS, 64),
        )
        .expect("published intent must be complete");
        assert!(published
            .same_replay_recipe(&original)
            .expect("compare published recipe"));

        // A same-key retry may reconstruct fresh timestamps, but changing the
        // graph/table recipe must fail without replacing the durable binding.
        let mut altered = original_intent(operation_id, 2);
        altered.table_steps = vec![ReplayStep::Sql(
            "CREATE TABLE altered_intent_test (id INT)".to_string(),
        )];
        let error = write_intent(&authority, persist_dir.path(), &altered)
            .expect_err("changed same-key recipe must conflict");
        assert!(error.contains("IDEMPOTENCY_CONFLICT"), "{error}");

        let intents = list_intents(&authority, persist_dir.path());
        assert_eq!(intents.len(), 1, "the original intent must remain singular");
        assert_eq!(intents[0].created_at_ms, 1);

        // Simulate a process crash in the real path after temp fsync and
        // before hard-link. The temp file survives, but a retry can publish a
        // fresh complete record because no partial canonical file was exposed.
        let interrupted_id = uuid::Uuid::from_u128(0x5678);
        let interrupted = original_intent(interrupted_id, 4);
        let interrupted_path = intent_path(&dir, interrupted_id);
        let (crashed_tx, crashed_rx) = std::sync::mpsc::sync_channel(0);
        let crash_path = interrupted_path.clone();
        let crash_hook: PreInstallHook = std::sync::Arc::new(move |tmp_path, path| {
            if path != crash_path {
                return;
            }
            crashed_tx
                .send((tmp_path.to_path_buf(), path.to_path_buf()))
                .expect("report crashed publication");
            panic!("simulated crash before hard-link publication");
        });
        set_pre_install_hook(Some(crash_hook));
        let crashed_authority = authority.clone();
        let crashed_dir = persist_dir.path().to_path_buf();
        let crashed_intent = interrupted.clone();
        let crashed_writer = std::thread::spawn(move || {
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                write_intent(&crashed_authority, &crashed_dir, &crashed_intent)
            }))
        });
        let (crashed_temp, crashed_path) = crashed_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .expect("observe crashed pre-install path");
        assert_eq!(crashed_path, interrupted_path);
        assert!(
            !crashed_path.exists(),
            "crash must precede canonical publication"
        );
        assert!(
            crashed_temp.exists(),
            "crashed temp must remain for diagnosis"
        );
        let crashed_result = crashed_writer.join().expect("join crashed writer");
        assert!(
            crashed_result.is_err(),
            "test hook must interrupt publication"
        );
        set_pre_install_hook(None);

        write_intent(&authority, persist_dir.path(), &interrupted)
            .expect("retry after real pre-install crash");
        let retried_bytes = std::fs::read(&crashed_path).expect("read retried intent");
        let retried = eg_types::msgpack::decode_bounded::<CommitIntent>(
            &retried_bytes,
            eg_types::msgpack::MsgpackLimits::new(MAX_INTENT_BYTES, MAX_INTENT_ITEMS, 64),
        )
        .expect("retried intent must be complete");
        assert!(retried
            .same_replay_recipe(&interrupted)
            .expect("compare retried recipe"));
        let _ = std::fs::remove_file(&crashed_temp);
    }
}
