use super::writer_commands::writer_command_arms;
use super::*;
use redb::ReadableTable;
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use tokio::sync::oneshot;

use crate::protocol::Method;
use crate::redb_store::shard::{Shard, ShardWrite};
use crate::redb_store::{
    clear_xshard_decision, clear_xshard_prepare, commit_change_envelope, commit_change_envelopes,
    commit_crossmodal, commit_mutation_batch, commit_mutation_batch_crossmodal,
    commit_mutation_batch_state, commit_ops, get_xshard_decision, get_xshard_decision_retain,
    get_xshard_prepare, purge_graph_rows, put_xshard_decision, put_xshard_prepare,
    put_xshard_recoverable_pending, read_graph_dump, scan_xshard_decisions, scan_xshard_prepares,
    write_graph_meta, RAFT_LOG,
};

// ── off-reactor group-commit writer thread ───────────────────────────────

/// How long the writer waits for work before flushing whatever it holds.
///
/// A commit-before-ack write never waits for this (a pending barrier commits the
/// instant the channel drains), so it bounds only how long a NON-acknowledged
/// internal batch sits unflushed. This is the fixed group-commit boundary; it
/// never changes the Immediate durability level.
const GROUP_COMMIT_TICK: Duration = Duration::from_millis(100);

pub(super) fn run(
    rx: Receiver<Cmd>,
    // Shared kernel-owned `Shard` (CONCEPT:EG-KG.storage.snapshot-read-off-writer): the writer OWNS one clone of
    // the Arc (kept alive for the thread's whole life); the `ShardWriter` holds a
    // `Weak` for off-writer snapshot reads. Rebound to `&Shard` immediately so every
    // commit below reads as the same single-writer loop it has always been.
    shard: Arc<Shard>,
    group_commit: RedbGroupCommitConfig,
    stats: Arc<RedbCommitStats>,
    // Auto-sized early-flush op threshold (CONCEPT:AU-KG.backend.b-auto-sizeb), per shard.
    flush_threshold: usize,
    #[cfg(feature = "security")] cipher: Option<crate::crypto::ValueCipher>,
) {
    // Borrow the shared handle for the rest of the loop; the owned Arc above stays
    // alive until `run` returns, so this reference is valid for the whole thread.
    let shard: &Shard = &shard;
    // Build the durable-crypto handle ONCE (borrows the owned cipher for the thread's
    // lifetime). No-op handle when encryption is off / not compiled.
    #[cfg(feature = "security")]
    let crypto = crate::redb_store::DurableCrypto::new(cipher.as_ref());
    #[cfg(not(feature = "security"))]
    let crypto = crate::redb_store::DurableCrypto::none();
    // Pending mutations folded into the NEXT group commit, each with its optional
    // commit-before-ack completion sender (CONCEPT:EG-KG.backend.authoritative-dispatch). After a commit, EVERY
    // sender in the batch is fired with the batch's result — one fsync, N notified.
    let mut pending: Pending = Pending::default();
    loop {
        match rx.recv_timeout(GROUP_COMMIT_TICK) {
            Ok(cmd) => {
                if process_command_batch(
                    cmd,
                    &rx,
                    shard,
                    &mut pending,
                    &group_commit,
                    flush_threshold,
                    crypto,
                    &stats,
                ) {
                    break;
                }
            }
            Err(RecvTimeoutError::Timeout) => {
                commit_pending(shard, &mut pending, crypto, &stats, false);
            }
            Err(RecvTimeoutError::Disconnected) => {
                commit_pending(shard, &mut pending, crypto, &stats, false);
                break;
            }
        }
    }
}

fn process_command_batch(
    cmd: Cmd,
    rx: &Receiver<Cmd>,
    shard: &Shard,
    pending: &mut Pending,
    group_commit: &RedbGroupCommitConfig,
    flush_threshold: usize,
    crypto: crate::redb_store::DurableCrypto<'_>,
    stats: &RedbCommitStats,
) -> bool {
    if handle_cmd(cmd, shard, pending, flush_threshold, crypto, stats) {
        commit_pending(shard, pending, crypto, stats, false);
        return true;
    }
    if drain_burst(rx, shard, pending, flush_threshold, crypto, stats) {
        commit_pending(shard, pending, crypto, stats, false);
        return true;
    }
    if pending.has_barrier() {
        match maybe_linger(
            rx,
            shard,
            pending,
            group_commit,
            flush_threshold,
            crypto,
            stats,
        ) {
            LingerOutcome::Stop => return true,
            LingerOutcome::Commit(lingered) => {
                commit_pending(shard, pending, crypto, stats, lingered);
            }
        }
    }
    false
}

fn commit_pending(
    shard: &Shard,
    pending: &mut Pending,
    crypto: crate::redb_store::DurableCrypto<'_>,
    stats: &RedbCommitStats,
    lingered: bool,
) {
    if !pending.is_empty() {
        stats.record(pending.ops.len(), lingered);
    }
    commit_and_notify(shard, pending, crypto);
}

fn drain_burst(
    rx: &Receiver<Cmd>,
    shard: &Shard,
    pending: &mut Pending,
    flush_threshold: usize,
    crypto: crate::redb_store::DurableCrypto<'_>,
    stats: &RedbCommitStats,
) -> bool {
    while let Ok(cmd) = rx.try_recv() {
        if handle_cmd(cmd, shard, pending, flush_threshold, crypto, stats) {
            return true;
        }
    }
    false
}

enum LingerOutcome {
    Commit(bool),
    Stop,
}

fn maybe_linger(
    rx: &Receiver<Cmd>,
    shard: &Shard,
    pending: &mut Pending,
    group_commit: &RedbGroupCommitConfig,
    flush_threshold: usize,
    crypto: crate::redb_store::DurableCrypto<'_>,
    stats: &RedbCommitStats,
) -> LingerOutcome {
    if !can_linger(group_commit, pending) {
        return LingerOutcome::Commit(false);
    }
    stats.linger_waiting.store(true, Ordering::Release);
    #[cfg(test)]
    if let Some(control) = group_commit.test_control.as_ref() {
        control.wait_until_released();
    }
    let result = rx.recv_timeout(group_commit.linger);
    stats.linger_waiting.store(false, Ordering::Release);
    match result {
        Ok(cmd) => finish_linger_command(cmd, rx, shard, pending, flush_threshold, crypto, stats),
        Err(RecvTimeoutError::Timeout) => LingerOutcome::Commit(true),
        Err(RecvTimeoutError::Disconnected) => {
            commit_pending(shard, pending, crypto, stats, true);
            LingerOutcome::Stop
        }
    }
}

fn can_linger(group_commit: &RedbGroupCommitConfig, pending: &Pending) -> bool {
    group_commit.linger > Duration::ZERO
        && pending.raft_log_ops.is_empty()
        && !pending.ops.is_empty()
        && pending.ops.len() < group_commit.shallow_threshold
}

fn finish_linger_command(
    cmd: Cmd,
    rx: &Receiver<Cmd>,
    shard: &Shard,
    pending: &mut Pending,
    flush_threshold: usize,
    crypto: crate::redb_store::DurableCrypto<'_>,
    stats: &RedbCommitStats,
) -> LingerOutcome {
    if handle_cmd(cmd, shard, pending, flush_threshold, crypto, stats)
        || drain_burst(rx, shard, pending, flush_threshold, crypto, stats)
    {
        commit_pending(shard, pending, crypto, stats, true);
        return LingerOutcome::Stop;
    }
    LingerOutcome::Commit(true)
}

/// Buffered mutations awaiting the next group commit, plus the commit-before-ack
/// completion senders that must be fired once the batch is durable.
#[derive(Default)]
struct Pending {
    ops: Vec<(String, Method)>,
    /// Raft log appends `(group_id, index, blob)` folded into the SAME group-commit
    /// transaction as `ops` (CONCEPT:EG-KG.storage.one-fsync-covers-raft) — one fsync covers both the log entry
    /// and the M2 graph mutation.
    raft_log_ops: Vec<(u64, u64, Vec<u8>)>,
    /// One per awaited (commit-before-ack) op in this batch.
    waiters: Vec<oneshot::Sender<Result<(), String>>>,
    /// O(1) per-graph audit-chain tail cache (CONCEPT:EG-KG.storage.embedded-store). Lives on `Pending`
    /// because `Pending` is owned by the writer thread's `run` loop for the thread's
    /// LIFETIME (not reset between batches) — so the cached `(seq, hash)` tail stays hot
    /// across group commits, and the per-op range-scan that was burning the now
    /// CPU-bound writer (post-EG-024) is gone. The writer is the sole AUDIT mutator, so
    /// the in-memory tail is authoritative; it is seeded once per graph (incl. after a
    /// restart) from a single scan inside `append_audit_entry`.
    #[cfg(feature = "security")]
    audit_tail: crate::redb_store::AuditTailCache,
    /// Per-graph provenance-anchor tail cache (CONCEPT:EG-KG.sharding.row-level-security), the
    /// `ProvenanceAnchorCommit` sibling of `audit_tail` above — lives on `Pending`
    /// for the same reason: the writer thread's LIFETIME, seeded once per graph
    /// from a single scan (`provenance_anchor_commit`), then kept hot in RAM.
    #[cfg(feature = "security")]
    provenance_anchor_cache: crate::redb_store::ProvenanceAnchorCache,
}

impl Pending {
    fn has_barrier(&self) -> bool {
        !self.waiters.is_empty()
    }
    fn is_empty(&self) -> bool {
        self.ops.is_empty() && self.raft_log_ops.is_empty() && self.waiters.is_empty()
    }
}

/// Returns true if the writer should stop. `Mutation` is buffered into `pending`
/// (committed at the next group boundary); `Checkpoint` is applied + committed
/// immediately (it carries its own reply).
fn handle_cmd(
    cmd: Cmd,
    shard: &Shard,
    pending: &mut Pending,
    // Auto-sized early-flush op threshold (CONCEPT:AU-KG.backend.b-auto-sizeb).
    flush_threshold: usize,
    crypto: crate::redb_store::DurableCrypto<'_>,
    // Group-commit observability (CONCEPT:EG-KG.backend.adaptive-linger-coalesce). Every flush this
    // function performs — the early-flush bound below AND every "flush pending
    // mutations first" side effect of a read/purge/register/etc. command — is a REAL
    // group commit and must be accounted identically to the run-loop's own
    // `commit_now`, or `commit_stats()` silently undercounts (down to 0) any batch
    // that happens to settle on one of these paths instead, even though the ops
    // durably landed and every commit-before-ack waiter still fires correctly. See
    // the local `flush` helper immediately below and GOC-70 rule 1 (account for the
    // op on whichever path it took — don't assert a specific path was taken).
    stats: &RedbCommitStats,
) -> bool {
    // Every `commit_and_notify` call in this function MUST go through this helper so
    // the batch is recorded before it is committed+acked, exactly like `run`'s
    // `commit_now` — same ordering guarantee (record happens-before the oneshot
    // notify on this single writer thread), just reused across every early-flush
    // call site instead of only the run-loop's own drain/linger/tick path.
    let flush = |pending: &mut Pending| {
        if !pending.is_empty() {
            stats.record(pending.ops.len(), false);
        }
        commit_and_notify(shard, pending, crypto);
    };
    writer_command_arms!(
        cmd,
        pending = pending,
        flush_threshold = flush_threshold,
        flush = flush,
        shard = shard,
        crypto = crypto,
    )
}

/// Commit all buffered mutations as ONE admitted scope group — the control member
/// plus one member per touched graph — then fire EVERY commit-before-ack waiter for
/// the ops in this batch with the batch's result (CONCEPT:EG-KG.backend.authoritative-dispatch).
/// Coalescing is preserved: N awaiting writers ride one commit / one fsync and are
/// all notified after it lands, and the Raft entries in the same `Pending` ride that
/// same fsync on the control member. A waiter is only signalled `Ok` once its op is
/// provably on disk.
///
/// There is no durability argument: every commit is `eg_storage`'s constant
/// `WRITE_DURABILITY` (`Durability::Immediate`) — see `run`'s `commit_now`.
///
/// A burst wider than one group does NOT fail and is not split by luck:
/// `commit_ops` sorts the drain by graph into a `BTreeMap`, splits that key list
/// with `redb_store::shard::chunk_graphs` (`MAX_SHARD_GROUP_GRAPHS` = 1023, the
/// kernel's member budget less the always-present control member) and commits
/// each chunk as one group / one fsync, deterministically and in order, so two
/// replicas chunk the same burst the same way. Handing it the WHOLE drain in ONE
/// call is what lets it see the full burst — this function must not pre-split.
fn commit_and_notify(
    shard: &Shard,
    pending: &mut Pending,
    crypto: crate::redb_store::DurableCrypto<'_>,
) {
    if pending.is_empty() {
        return;
    }
    let res = commit_ops(
        shard,
        &mut pending.ops,
        &mut pending.raft_log_ops,
        &shard_write_attempt_id("shard_drain"),
        crate::server::txn::now_ms(),
        crypto,
        // O(1) audit-chain tail cache (CONCEPT:EG-KG.storage.embedded-store), persistent across batches.
        #[cfg(feature = "security")]
        &mut pending.audit_tail,
    );
    let waiters = std::mem::take(&mut pending.waiters);
    let signal = res.map(|_| ());
    for w in waiters {
        let _ = w.send(signal.clone());
    }
}

// commit_ops / write_graph_meta / read_one_node now live in `crate::redb_store`
// (imported above) — shared verbatim with the embedded path, ONE durable format.

// ── One-shot writer-thread commits ───────────────────────────────────────
//
// The writer's own bookkeeping — a Raft truncation, a meta pointer, a minted
// capability, a planted test row — carries no caller operation identity, so under
// RF-RULING-005 it is the ledgered MAINTENANCE class: durable and version-bumping
// like any other mutation, but outside operation-replay conflict semantics. Both
// helpers ABORT the group on failure rather than dropping it: a dropped
// unfinished admission poisons the shared transaction; an aborted one decides
// nothing.

/// Run `rows` in ONE control-only maintenance group: the 12 file-wide tables.
pub(super) fn in_control_write<T>(
    shard: &Shard,
    label: &str,
    rows: impl FnOnce(&ShardWrite<'_>) -> Result<T, String>,
) -> Result<T, String> {
    in_write(shard, &[], label, rows)
}

/// Run `rows` in ONE maintenance group carrying `graph` plus the control member.
fn in_graph_write<T>(
    shard: &Shard,
    graph: &str,
    label: &str,
    rows: impl FnOnce(&ShardWrite<'_>) -> Result<T, String>,
) -> Result<T, String> {
    let members = shard.graph_members(&[graph])?;
    in_write(shard, &members, label, rows)
}

type ShardMember = (
    String,
    Arc<eg_storage::OwnedStoreHandle<eg_storage::GraphShardOwner>>,
);

fn in_write<T>(
    shard: &Shard,
    members: &[ShardMember],
    label: &str,
    rows: impl FnOnce(&ShardWrite<'_>) -> Result<T, String>,
) -> Result<T, String> {
    let op_id = shard_write_attempt_id(label);
    let (group, batches) = shard.admit_maintenance(members, &op_id)?;
    let applied = (|| {
        let write = ShardWrite::open(shard, &group, members, &batches)?;
        let value = rows(&write)?;
        write.finish()?;
        Ok(value)
    })();
    match applied {
        Ok(value) => {
            shard.commit_drain(group, &batches, crate::server::txn::now_ms())?;
            Ok(value)
        }
        Err(error) => {
            shard.mutations().abort_group(group)?;
            Err(error)
        }
    }
}

// ── Raft log/meta helpers (CONCEPT:EG-KG.storage.one-fsync-covers-raft) — run on the writer thread ───────
//
// `raft_log` and `raft_meta` are FILE-WIDE rows: their keys lead with the Raft
// group id, not with a graph name, so they belong to the shard file's own control
// scope and are reached through `write.control()` / `read.open_owner_table`.

/// Read a `[lo, hi]` inclusive log range for one group, in index order.
fn read_raft_log_range(
    shard: &Shard,
    gid: u64,
    lo: u64,
    hi: u64,
    crypto: crate::redb_store::DurableCrypto<'_>,
) -> Result<Vec<Vec<u8>>, String> {
    const MAX_RAFT_LOG_READ_ENTRIES: usize = 100_000;
    const MAX_RAFT_LOG_READ_BYTES: usize = 1024 * 1024 * 1024;
    let rtx = shard.control_read()?;
    let t = rtx.open_owner_table(RAFT_LOG)?;
    let mut out = Vec::new();
    let mut total_bytes = 0usize;
    for kv in t.range((gid, lo)..=(gid, hi)).map_err(|e| e.to_string())? {
        if out.len() >= MAX_RAFT_LOG_READ_ENTRIES {
            return Err("raft log read exceeds resource limits".to_string());
        }
        let (_, v) = kv.map_err(|e| e.to_string())?;
        let value = crypto.unseal(v.value())?;
        total_bytes = total_bytes
            .checked_add(value.len())
            .filter(|total| *total <= MAX_RAFT_LOG_READ_BYTES)
            .ok_or_else(|| "raft log read exceeds resource limits".to_string())?;
        out.push(value);
    }
    Ok(out)
}

/// Delete entries with index >= `from` for one group (conflict truncation).
fn delete_raft_log_from(shard: &Shard, gid: u64, from: u64) -> Result<(), String> {
    in_control_write(shard, "raft_log_delete_from", |write| {
        let mut t = write.control().open_table(RAFT_LOG)?;
        let mut keys = Vec::new();
        for kv in t
            .range((gid, from)..=(gid, u64::MAX))
            .map_err(|e| e.to_string())?
        {
            if let Ok((key, _)) = kv {
                keys.push(key.value().1);
            }
        }
        for idx in keys {
            t.remove((gid, idx)).map_err(|e| e.to_string())?;
        }
        Ok(())
    })
}

/// Delete entries with index <= `upto` for one group (purge/compaction).
fn purge_raft_log_upto(shard: &Shard, gid: u64, upto: u64) -> Result<(), String> {
    in_control_write(shard, "raft_log_purge_upto", |write| {
        let mut t = write.control().open_table(RAFT_LOG)?;
        let mut keys = Vec::new();
        for kv in t.range((gid, 0)..=(gid, upto)).map_err(|e| e.to_string())? {
            if let Ok((key, _)) = kv {
                keys.push(key.value().1);
            }
        }
        for idx in keys {
            t.remove((gid, idx)).map_err(|e| e.to_string())?;
        }
        Ok(())
    })
}

/// (first, last) present log index for one group.
fn raft_log_bounds(shard: &Shard, gid: u64) -> LogBoundsResult {
    let rtx = shard.control_read()?;
    let t = rtx.open_owner_table(RAFT_LOG)?;
    let mut range = t
        .range((gid, 0)..=(gid, u64::MAX))
        .map_err(|e| e.to_string())?;
    let first = range
        .next()
        .and_then(|kv| kv.ok().map(|(k, _)| k.value().1));
    // Re-scan for the last (the iterator was advanced by `next`).
    let last = t
        .range((gid, 0)..=(gid, u64::MAX))
        .map_err(|e| e.to_string())?
        .next_back()
        .and_then(|kv| kv.ok().map(|(k, _)| k.value().1));
    Ok((first, last))
}

/// Durably write one Raft metadata key for a group.
fn put_raft_meta(shard: &Shard, gid: u64, key: &str, val: &[u8]) -> Result<(), String> {
    in_control_write(shard, "raft_meta_put", |write| {
        let mut t = write.control().open_table(RAFT_META)?;
        t.insert((gid, key), val).map_err(|e| e.to_string())?;
        Ok(())
    })
}

/// Read one Raft metadata key for a group.
fn get_raft_meta(shard: &Shard, gid: u64, key: &str) -> Result<Option<Vec<u8>>, String> {
    let rtx = shard.control_read()?;
    let t = rtx.open_owner_table(RAFT_META)?;
    Ok(t.get((gid, key))
        .map_err(|e| e.to_string())?
        .map(|v| v.value().to_vec()))
}
