use super::commands::SHARD_WRITER_JOIN_TIMEOUT;
use super::*;
use std::sync::mpsc::sync_channel;
use std::sync::{Arc, Weak};
use std::thread::JoinHandle;

use crate::server::persistence::writer_reply::await_writer_reply;

// ── Sharded K-way durable writer (CONCEPT:EG-KG.backend.sharded-k-way-durable) ────────────────────────────
//
// redb is single-writer-PER-FILE: one authoritative shard + one writer thread
// serializes EVERY tenant's durable commits onto ONE core — a 64-core box writes
// on 1 core. EG-026 shards by graph into K independent redb files, each with its
// OWN writer thread / channel / `Pending` (incl. the EG-024 micro-linger + the
// EG-025 audit tail cache), so K cores commit in parallel. A graph ALWAYS routes
// to the same shard (`shard_index(graph_fname) % K`), so its data + audit chain +
// group-commit stay co-located and single-writer-correct PER SHARD — every
// durability invariant (commit-before-ack, group-commit, backpressure-not-drop)
// holds unchanged inside each shard.
//
// K = clamp(effective-cgroup-cpu/2, 1, 8), overridable via `EPISTEMIC_GRAPH_REDB_SHARDS`. Every K uses
// the same canonical `graph-<n>.redb` naming contract, including `graph-0.redb` for
// K=1. K is FIXED per persist-dir once created: `reconcile_shard_layout` validates
// and honors the current on-disk layout (changing K needs an offline migration).

/// Stable FNV-1a routing of a graph's sanitized fname to a shard index (CONCEPT:EG-KG.backend.sharded-k-way-durable).
/// Deterministic across processes/restarts (NOT `DefaultHasher` randomness) — a graph
/// MUST resolve to the same shard every boot or its durable rows become unreachable.
pub(crate) fn shard_index(graph_fname: &str, k: usize) -> usize {
    if k <= 1 {
        return 0;
    }
    let mut hash: u64 = 0xcbf29ce484222325;
    for b in graph_fname.as_bytes() {
        hash ^= *b as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    (hash % k as u64) as usize
}

/// Run one blocking closure PER shard CONCURRENTLY on the blocking pool and collect
/// their results in shard order (CONCEPT:AU-KG.backend.roadmap-f-parallel-cross, roadmap F — parallel cross-shard read
/// fan-out). EVERY task is spawned BEFORE any is awaited, which is the property that
/// makes a K-shard fan-out overlap instead of serialize (a spawn-then-await-each loop is
/// serial). The closures run off each shard's `begin_read()` MVCC snapshot (CONCEPT:EG-KG.storage.snapshot-read-off-writer),
/// so the fan-out never routes through a writer thread. The first error short-circuits.
pub(super) async fn join_blocking_in_order<T, F>(tasks: Vec<F>) -> Result<Vec<T>, String>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, String> + Send + 'static,
{
    let handles: Vec<_> = tasks.into_iter().map(tokio::task::spawn_blocking).collect();
    let mut out = Vec::with_capacity(handles.len());
    for h in handles {
        out.push(
            h.await
                .map_err(|e| format!("shard read join error: {e}"))??,
        );
    }
    Ok(out)
}

/// Resolve the shard count K (CONCEPT:EG-KG.backend.sharded-k-way-durable).
///   * Under the `raft` feature AND a configured Raft node (ADR-2 / W1.2,
///     `reports/wave1/ADR-scale-trio.md` §ADR-2): K == N raft groups — raft group `g`
///     owns redb shard `g`, so HA no longer forces K=1. The count follows the group
///     count (`EPISTEMIC_GRAPH_RAFT_GROUPS`, default cores-derived up to `MAX_SHARD_COUNT`);
///     `EPISTEMIC_GRAPH_REDB_SHARDS` does NOT apply under raft (K is pinned to N so the
///     group↔shard alignment is exact). An existing K=1 store on disk stays K=1 until the
///     offline `migrate-shards` tool rewrites its layout (the detected layout wins at open).
///   * `EPISTEMIC_GRAPH_REDB_SHARDS` overrides the non-raft count (clamped 1..=64).
///   * In `cfg(test)` default to 1 so the existing single-writer durability/audit/
///     group-commit tests run the byte-for-byte K=1 path unless they opt in via the env.
///   * Otherwise K = clamp(effective-cgroup-cpu/2, 1, 8) — mirrors the shared
///     `crate::autosize::detect_capacity()` seam.
pub(super) fn resolve_shard_count() -> usize {
    // Raft active ⇒ K == N groups (ADR-2 / W1.2), NOT the forced K=1 of the M2 spike.
    #[cfg(feature = "raft")]
    if std::env::var("EPISTEMIC_GRAPH_RAFT_NODE_ID").is_ok() {
        if std::env::var("EPISTEMIC_GRAPH_REDB_SHARDS").is_ok() {
            tracing::warn!(
                "EPISTEMIC_GRAPH_REDB_SHARDS is ignored under an active Raft node; the durable \
                 shard count follows EPISTEMIC_GRAPH_RAFT_GROUPS (K == N groups, group g owns \
                 shard g)"
            );
        }
        let groups = crate::raft::config::raft_group_count();
        tracing::info!(
            shards = groups,
            "raft active: opening K == N durable shards (ADR-2 W1.2 — raft group g owns redb \
             shard g; N parallel durable writers per node)"
        );
        return groups as usize;
    }
    if let Ok(v) = std::env::var("EPISTEMIC_GRAPH_REDB_SHARDS") {
        if let Ok(n) = v.trim().parse::<usize>() {
            return n.clamp(1, crate::redb_layout::MAX_SHARD_COUNT);
        }
    }
    #[cfg(test)]
    {
        1
    }
    #[cfg(not(test))]
    {
        let cpus = crate::autosize::detect_capacity().reserved_cpus();
        (cpus / 2).clamp(1, 8)
    }
}

/// Resolve the per-shard early-flush op threshold (CONCEPT:AU-KG.backend.b-auto-sizeb — auto-size the
/// previously HARDCODED `4096`). The writer flushes a `Pending` batch early once it
/// holds this many ops, bounding writer memory before the bounded channel saturates.
///   * `EPISTEMIC_GRAPH_REDB_FLUSH_THRESHOLD` may lower the automatic bound,
///     but is capped by it after the hard 64..=1_048_576 validation window.
///   * Else ~half the authoritative writer queue depth (`capacity`, itself
///     hardware-auto-sized via `Capacity::writer_queue()`), clamped 256..=16384.
pub(super) fn resolve_flush_threshold(capacity: usize) -> usize {
    let automatic = (capacity / 2).clamp(256, 16_384);
    if let Ok(v) = std::env::var("EPISTEMIC_GRAPH_REDB_FLUSH_THRESHOLD") {
        if let Ok(n) = v.trim().parse::<usize>() {
            if n > 0 {
                return n.clamp(64, 1_048_576).min(automatic);
            }
        }
    }
    automatic
}

/// The WRITER of one durable shard (CONCEPT:EG-KG.backend.sharded-k-way-durable): the off-reactor
/// group-commit thread that owns one shard file, its bounded channel, its `Pending`
/// (incl. the EG-024 linger + EG-025 audit tail cache) and its commit counters.
/// Single-writer-per-FILE, so K writers commit in parallel on K cores.
///
/// It is NOT the store: the store is [`crate::redb_store::shard::Shard`], the
/// kernel-owned `OwnerLayout::GraphShard` file this writer commits into and every
/// off-writer snapshot read is issued from. This type is the thread and the queue
/// in front of it, which is what its name says.
pub(super) struct ShardWriter {
    pub(super) db_path: String,
    /// `Weak` handle to THIS shard's kernel-owned store (CONCEPT:EG-KG.storage.snapshot-read-off-writer —
    /// snapshot reads off the writer). redb 4.1 is MVCC: a kernel-issued `ScopedRead`
    /// runs CONCURRENTLY with the single writer, so the point-read / read-through
    /// path `upgrade()`s this `Weak` and serves an evicted node DIRECTLY off
    /// `shard.read(&shard.graph(g)?)` without touching the writer's channel and
    /// without forcing a group-commit. `Weak`, not a strong clone, is deliberate:
    /// the writer thread owns the SOLE STRONG `Arc`, so the exclusive per-process
    /// file lock releases EXACTLY when that thread exits on `shutdown` — a reopen of
    /// the persist dir then succeeds, and a read after shutdown fails fast
    /// (upgrade ⇒ `None`) instead of pinning the lock. The `Shard` (not a raw
    /// database) is what is shared because it carries the bound-handle cache, so a
    /// warm graph never re-binds.
    pub(super) shard: Weak<Shard>,
    pub(super) tx: SyncSender<Cmd>,
    /// Group-commit batch-size / linger counters (CONCEPT:EG-KG.backend.adaptive-linger-coalesce), per shard.
    pub(super) stats: Arc<RedbCommitStats>,
    /// Value-blob cipher for snapshot reads off the writer (CONCEPT:EG-KG.storage.snapshot-read-off-writer). The same
    /// cipher the writer thread owns; resolved ONCE at open. `None` ⇒ encryption off ⇒
    /// the read path is byte-for-byte the plaintext path.
    #[cfg(feature = "security")]
    pub(super) cipher: Option<crate::crypto::ValueCipher>,
    /// Transaction-recovery-plan cipher (D-ORC-50, CONCEPT:EG-KG.txn.multi-op-occ-acid) —
    /// DELIBERATELY SEPARATE from `cipher` above. Resolved from
    /// `EPISTEMIC_GRAPH_TXN_RECOVERY_KEY` (falling back to the shared data key when that
    /// alone is set — see `crypto::resolve_txn_recovery_key`), so an operator can
    /// unblock multi-op OCC `Commit` durability WITHOUT enabling at-rest encryption of
    /// ordinary node/edge/property values. Turning this on never changes `cipher`, so it
    /// never changes the durable value format existing rows were written in — no
    /// read-path migration is implied. `None` ⇒ multi-op transaction commits that stage
    /// more than one op (e.g. a compare-and-set touching a node property + an ANN
    /// vector) fail durability with a "configure a key" error, same as before this seam.
    #[cfg(feature = "security")]
    pub(super) txn_recovery_cipher: Option<crate::crypto::ValueCipher>,
    pub(super) handle: parking_lot::Mutex<Option<JoinHandle<()>>>,
}

/// One shard file opened and DECIDED, with nothing written yet.
///
/// The second half of the two-phase open (see [`CanaryPlan`]): the persist dir's
/// refusal is collective, so every shard is planned before any shard is changed.
pub(super) struct PreparedShard {
    db_path: String,
    shard: Arc<Shard>,
    #[cfg(feature = "security")]
    cipher: Option<crate::crypto::ValueCipher>,
    #[cfg(feature = "security")]
    canary: Option<CanaryPlan>,
    #[cfg(feature = "security")]
    txn_recovery_cipher: Option<crate::crypto::ValueCipher>,
}

impl ShardWriter {
    /// Open (or create) `db_path` as a kernel-owned store and DECIDE its
    /// encryption posture, writing nothing.
    ///
    /// Every fallible, refusing step lives here and every durable one lives in
    /// [`Self::spawn`], so a persist dir whose open is refused is byte-identical
    /// to what it was before the call — see [`CanaryPlan`] for the bricking this
    /// separation prevents.
    pub(super) fn prepare(db_path: String) -> Result<PreparedShard, String> {
        // ONE shared `Shard` per file (CONCEPT:EG-KG.storage.snapshot-read-off-writer): the writer thread and
        // the snapshot-read path both hold a clone of this `Arc`, and redb's exclusive
        // per-file lock is why reads share it rather than re-opening.
        //
        // No schema bootstrap runs here any more. `StorageKernel::create_owner`
        // materializes the WHOLE declared `OwnerLayout::GraphShard` census — the 41
        // scope-prefixed tables, the 12 file-wide ones (`raft_meta` and
        // `encryption_canary` among them) and the ledger — and re-validates it on
        // every open, so the hand-written `initialize_canonical_tables` bootstrap
        // that used to run here is deleted: beside `create_owner` it would be a
        // second physical authority over the same file.
        let shard = Arc::new(Shard::open(std::path::Path::new(&db_path))?);
        // Encryption-at-rest (CONCEPT:EG-KG.sharding.row-level-security): resolve the value-blob cipher ONCE at
        // open from EPISTEMIC_GRAPH_ENCRYPTION_KEY (the KMS seam). `None` ⇒ encryption
        // OFF ⇒ the durable format + write/read paths are byte-for-byte unchanged.
        #[cfg(feature = "security")]
        let cipher = crate::crypto::ValueCipher::from_env_checked()?;
        // GOC-16: encryption-at-rest readiness posture (EPISTEMIC_GRAPH_ENCRYPTION_
        // REQUIRED). Previously the `None` branch here logged NOTHING at all — a
        // production deployment could run fully unencrypted with zero signal to the
        // operator. `Warn` (the shipped default) fixes the silence without changing
        // startup behavior; `On` fails closed BEFORE the writer thread spawns or any
        // listener binds, by returning the same `Result<Self, String>` every other
        // fallible step in this function already uses (`RedbBackend::open`
        // propagates it to `main.rs`'s existing `eprintln!` + `exit(1)` refusal).
        #[cfg(feature = "security")]
        let mut canary_plan: Option<CanaryPlan> = None;
        #[cfg(feature = "security")]
        match &cipher {
            Some(c) => {
                // GOC-16 / BUG-248: fail closed BEFORE the writer thread spawns or any
                // listener binds if the configured key does not match the key that
                // sealed this store's existing data — see `plan_encryption_canary`'s
                // doc for exactly what this does and does not cover.
                //
                // DECIDE only. The plan is applied in `spawn`, after every shard in
                // this persist dir has agreed — see `CanaryPlan`'s doc for why a
                // per-shard write here bricked a multi-shard dir on a refusal.
                canary_plan = Some(plan_encryption_canary(&shard, c)?);
            }
            None => {
                // BUG-PE-055: the no-key path never looked at the canary, so a store
                // whose values are SEALED opened cleanly with no key and then failed
                // one read at a time ("encrypted durable value requires configured key
                // material"). This is the symmetric partner of the wrong-key refusal
                // above, and it holds regardless of the required-mode posture — a
                // missing key for an encrypted store is not a posture choice.
                if shard_carries_encryption_metadata(&shard)? {
                    return Err(format!(
                        "refusing to open the durable graph store: this store carries \
                         encryption-at-rest metadata (its value blobs are sealed) but {} \
                         is not set, so every read of an existing value would fail. \
                         Configure the key reference and material that encrypted this \
                         store.",
                        crate::crypto::ENCRYPTION_KEY_ENV,
                    ));
                }
                match crate::crypto::encryption_required_mode() {
                    crate::crypto::EncryptionRequiredMode::Off => {}
                    crate::crypto::EncryptionRequiredMode::Warn => {
                        tracing::warn!(
                            "redb encryption-at-rest is OFF ({} is not set) — value blobs are \
                         stored in PLAINTEXT. Set {}=on to refuse to start instead of \
                         warning.",
                            crate::crypto::ENCRYPTION_KEY_ENV,
                            crate::crypto::ENCRYPTION_REQUIRED_ENV,
                        );
                    }
                    crate::crypto::EncryptionRequiredMode::On => {
                        return Err(format!(
                            "refusing to open the durable graph store: at-rest encryption is \
                         REQUIRED ({}=on) but {} is not set",
                            crate::crypto::ENCRYPTION_REQUIRED_ENV,
                            crate::crypto::ENCRYPTION_KEY_ENV,
                        ));
                    }
                }
            }
        }
        // Transaction-recovery-plan cipher (D-ORC-50) — resolved SEPARATELY from the
        // data-at-rest cipher above so enabling multi-op OCC transaction durability never
        // implies (and never requires) enabling at-rest encryption of existing plaintext
        // values. See `crypto::TXN_RECOVERY_KEY_ENV` for the full rationale.
        #[cfg(feature = "security")]
        let txn_recovery_cipher = crate::crypto::ValueCipher::from_env_for_txn_recovery();
        #[cfg(feature = "security")]
        if txn_recovery_cipher.is_some() && cipher.is_none() {
            tracing::info!(
                "redb transaction-recovery-plan sealing ENABLED via a dedicated key \
                 (EPISTEMIC_GRAPH_TXN_RECOVERY_KEY) — data-at-rest encryption remains OFF"
            );
        }
        Ok(PreparedShard {
            db_path,
            shard,
            #[cfg(feature = "security")]
            cipher,
            #[cfg(feature = "security")]
            canary: canary_plan,
            #[cfg(feature = "security")]
            txn_recovery_cipher,
        })
    }

    /// Commit this shard's decided encryption plan and spawn its group-commit
    /// writer thread. Called only after EVERY shard in the persist dir prepared
    /// successfully, so this is the first point at which anything is written.
    pub(super) fn spawn(
        prepared: PreparedShard,
        thread_name: String,
        capacity: usize,
        flush_threshold: usize,
        group_commit: RedbGroupCommitConfig,
    ) -> Result<Self, String> {
        let PreparedShard {
            db_path,
            shard,
            #[cfg(feature = "security")]
            cipher,
            #[cfg(feature = "security")]
            canary,
            #[cfg(feature = "security")]
            txn_recovery_cipher,
        } = prepared;
        // BUG-PE-055: log AFTER the decision, and say which of the three things
        // actually happened. The old unconditional "ENABLED" line was emitted
        // before the check ran, so it appeared even on a store that was about to
        // be refused — and, worse, it read identically whether the key was the
        // store's existing key or a brand-new one being imposed on it.
        #[cfg(feature = "security")]
        if let Some(plan) = canary {
            match apply_encryption_canary(&shard, plan)? {
                CanaryOutcome::Verified => tracing::info!(
                    "redb encryption-at-rest ENABLED (value blobs sealed with \
                     ChaCha20-Poly1305); the configured key matches this store's \
                     existing key binding"
                ),
                CanaryOutcome::UpgradedLegacy => tracing::info!(
                    "redb encryption-at-rest ENABLED (value blobs sealed with \
                     ChaCha20-Poly1305); this store's pre-key-lifecycle canary was \
                     verified and upgraded to a bound key reference"
                ),
                CanaryOutcome::Established => tracing::warn!(
                    "redb encryption-at-rest ENABLED (value blobs sealed with \
                     ChaCha20-Poly1305) and a NEW key binding was established: this \
                     store had no encryption canary and no durable rows, so {} is \
                     being used for the FIRST time here. If you expected this store \
                     to already hold data, it is not the store you meant — check the \
                     persist dir before writing to it.",
                    crate::crypto::ENCRYPTION_KEY_ENV,
                ),
            }
        }
        let (tx, rx) = sync_channel::<Cmd>(capacity.max(1));
        // Adaptive group-commit micro-linger config + observability (CONCEPT:EG-KG.backend.adaptive-linger-coalesce).
        // Resolved once by the backend open path (Configuration discipline); the
        // writer thread owns the supplied config and a clone of the stats Arc so
        // callers can read batch-size/throughput live.
        let stats = Arc::new(RedbCommitStats::default());
        let stats_writer = stats.clone();
        // Keep a clone of the cipher for the snapshot-read path (CONCEPT:EG-KG.storage.snapshot-read-off-writer); the
        // writer thread takes ownership of the original below.
        #[cfg(feature = "security")]
        let cipher_for_reads = cipher.clone();
        // A `Weak` for the off-writer snapshot-read path (CONCEPT:EG-KG.storage.snapshot-read-off-writer). The writer
        // thread below takes the SOLE STRONG `Arc`, so the redb file lock releases
        // exactly when that thread exits on shutdown — matching the pre-EG-027 lifetime
        // (a reopen after shutdown succeeds; a read after shutdown upgrades to `None`).
        let shard_weak = Arc::downgrade(&shard);
        let handle = std::thread::Builder::new()
            .name(thread_name)
            .spawn(move || {
                run(
                    rx,
                    shard,
                    group_commit,
                    stats_writer,
                    flush_threshold,
                    #[cfg(feature = "security")]
                    cipher,
                )
            })
            .map_err(|e| e.to_string())?;
        Ok(Self {
            db_path,
            shard: shard_weak,
            tx,
            stats,
            #[cfg(feature = "security")]
            cipher: cipher_for_reads,
            #[cfg(feature = "security")]
            txn_recovery_cipher,
            handle: parking_lot::Mutex::new(Some(handle)),
        })
    }

    /// Stop this shard's writer thread (flush + join). Idempotent.
    pub(super) fn shutdown(&self) {
        let handle = self.handle.lock().take();
        if let Some(handle) = handle {
            let (reply, rx) = std::sync::mpsc::sync_channel(1);
            if self.tx.send(Cmd::Shutdown { reply }).is_ok() {
                let _ = await_writer_reply(&rx, "shutdown");
            }
            // The ack above means the writer's loop has returned, so this join
            // is prompt in the healthy case. It is still bounded: a writer that
            // acked and then wedged in its own teardown would otherwise hang
            // shutdown forever.
            if let Err(error) = crate::bounded_join::join_within(
                handle,
                "the redb shard writer thread",
                SHARD_WRITER_JOIN_TIMEOUT,
            ) {
                eprintln!("redb shard writer did not shut down cleanly: {error}");
            }
        }
    }
}

impl Drop for ShardWriter {
    fn drop(&mut self) {
        // A backend may be released through its last `Arc` without an explicit
        // shutdown (test states and startup-error paths do this). Join the writer
        // here so its sole strong `Shard` handle and redb file lock are released
        // before the next in-process open, rather than detaching a resource-owning
        // thread until the scheduler happens to observe channel disconnect.
        self.shutdown();
    }
}
