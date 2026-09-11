//! The shard file's own control rows: the cross-shard 2PC records and the three
//! materialized-view key spaces.
//!
//! Every table here is FILE-WIDE (`TableScope::StorePrivate`) -- keyed by
//! transaction id or view name, never by a graph -- so every access rides the
//! control member of an admitted group and opens a raw `redb` table rather than
//! a scoped one. The control scope is the only physical mutation owner for
//! these rows.

use super::shard::{Shard, ShardWrite};
use super::*;

/// The maintenance claim key for ONE ATTEMPT at a control-row write.
///
/// The kernel's maintenance claim is FIRST-WINS: the second batch presenting a
/// claim key resolves as a replay, and a replayed member writes nothing and
/// refuses owner rows. Deriving the key from the operation's SUBJECT
/// (`matview/put/{name}`, `xshard/decision-clear/{txn_id}`) therefore made every
/// control table writable exactly ONCE per subject for the life of the store --
/// a materialized view could never be refreshed after its first
/// materialization, and cross-shard decision cleanup could never run twice for
/// one transaction, which is exactly what crash recovery does when it
/// re-reconciles an in-doubt transaction. Both failed closed with "owner write
/// requires an admitted mutation batch", from a call site that looks like an
/// ordinary upsert.
///
/// These writes carry no replay requirement: their durability is the caller's
/// own 2PC record or refresh protocol, not a first-wins claim, and re-running
/// one must REDO it rather than resolve it. So the claim key is per attempt --
/// the same rule, for the same reason, that `shard::drain_batch` states for the
/// drain id. Treating the replay as a silent success instead would have been
/// worse than the error: it would durably drop every matview refresh after the
/// first. Regression: `shard_control_tests::
/// control_rows_are_repeatable_writes_not_first_wins_claims`.
fn control_write_attempt_id(op_id: &str) -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    static NONCE: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    let nonce = *NONCE.get_or_init(|| {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|since| since.as_nanos() as u64)
            .unwrap_or(0)
    });
    let seq = SEQ.fetch_add(1, Ordering::Relaxed);
    format!("{op_id}/{}-{nonce}:{seq}", std::process::id())
}

/// Run one control-row mutation as a ledgered maintenance member.
///
/// The control scope is always member zero of an admitted group, even when the
/// operation has no graph member. This keeps cross-shard recovery metadata and
/// view state under the same one-storage/one-mutation-owner rule as graph rows.
macro_rules! control_write {
    ($shard:expr, $op_id:expr, |$owner:ident| $body:block) => {{
        let attempt_id = control_write_attempt_id($op_id);
        let (group, batches) = $shard.admit_maintenance(&[], &attempt_id)?;
        let write = ShardWrite::open($shard, &group, &[], &batches)?;
        let result: Result<_, String> =
            (|$owner: &AdmittedOwnerWrite<'_, GraphShardOwner>| $body)(write.control());
        let finished = write.finish();
        match (result, finished) {
            (Ok(value), Ok(())) => $shard.commit_drain(group, &batches, 0).map(|()| value),
            (Err(error), _) | (Ok(_), Err(error)) => {
                $shard.mutations().abort_group(group)?;
                Err(error)
            }
        }
    }};
}

/// Durably persist one participant group's prepared slice (its own transaction).
pub(crate) fn put_xshard_prepare(
    shard: &Shard,
    txn_id: &str,
    gid: u64,
    slice: &[u8],
    crypto: DurableCrypto<'_>,
) -> Result<(), String> {
    let op_id = format!("xshard/prepare/{txn_id}/{gid}");
    control_write!(shard, &op_id, |control| {
        // Production MutationBatch parents require the environment data key
        // before entering this path, so their prepare bodies are ciphertext.
        // Retain the generic no-cipher behavior for low-level in-process Raft
        // harnesses.
        let sealed = crypto.seal(slice);
        let mut table = control
            .open_table::<(&str, u64), &[u8]>(XSHARD_PREPARE)
            .map_err(|e| e.to_string())?;
        table
            .insert((txn_id, gid), sealed.as_ref())
            .map_err(|e| e.to_string())?;
        Ok(())
    })
}

/// Read one participant's prepared slice by its exact composite key.
pub(crate) fn get_xshard_prepare(
    shard: &Shard,
    txn_id: &str,
    gid: u64,
    crypto: DurableCrypto<'_>,
) -> Result<Option<Vec<u8>>, String> {
    let read = shard.control_read()?;
    let table = read
        .open_owner_table(XSHARD_PREPARE)
        .map_err(|e| e.to_string())?;
    let sealed = table
        .get((txn_id, gid))
        .map_err(|e| e.to_string())?
        .map(|value| value.value().to_vec());
    sealed.map(|value| crypto.unseal(&value)).transpose()
}

/// Durably write the coordinator's decision row (the atomic commit point).
pub(crate) fn put_xshard_decision(
    shard: &Shard,
    txn_id: &str,
    commit: bool,
    retain_for_parent: bool,
) -> Result<(), String> {
    let op_id = format!("xshard/decision/{txn_id}");
    control_write!(shard, &op_id, |control| {
        let mut table = control
            .open_table::<&str, u8>(XSHARD_DECISION)
            .map_err(|e| e.to_string())?;
        let encoded = match (commit, retain_for_parent) {
            (false, false) => 0u8,
            (true, false) => 1u8,
            (false, true) => 2u8,
            (true, true) => 3u8,
        };
        table.insert(txn_id, encoded).map_err(|e| e.to_string())?;
        Ok(())
    })
}

/// Mark a parent-recoverable 2PC attempt as started but not yet decided. Value
/// 4 is deliberately not COMMIT/ABORT; recovery resolves it by presumed abort
/// while retaining that outcome until the MutationBatch parent is terminal.
pub(crate) fn put_xshard_recoverable_pending(shard: &Shard, txn_id: &str) -> Result<(), String> {
    let op_id = format!("xshard/pending/{txn_id}");
    control_write!(shard, &op_id, |control| {
        let mut table = control
            .open_table::<&str, u8>(XSHARD_DECISION)
            .map_err(|e| e.to_string())?;
        table.insert(txn_id, 4u8).map_err(|e| e.to_string())?;
        Ok(())
    })
}

/// Clear one participant's prepare record after resolution.
pub(crate) fn clear_xshard_prepare(shard: &Shard, txn_id: &str, gid: u64) -> Result<(), String> {
    let op_id = format!("xshard/prepare-clear/{txn_id}/{gid}");
    control_write!(shard, &op_id, |control| {
        let mut table = control
            .open_table::<(&str, u64), &[u8]>(XSHARD_PREPARE)
            .map_err(|e| e.to_string())?;
        table.remove((txn_id, gid)).map_err(|e| e.to_string())?;
        Ok(())
    })
}

/// Clear a resolved txn's decision record.
pub(crate) fn clear_xshard_decision(shard: &Shard, txn_id: &str) -> Result<(), String> {
    let op_id = format!("xshard/decision-clear/{txn_id}");
    control_write!(shard, &op_id, |control| {
        let mut table = control
            .open_table::<&str, u8>(XSHARD_DECISION)
            .map_err(|e| e.to_string())?;
        table.remove(txn_id).map_err(|e| e.to_string())?;
        Ok(())
    })
}

/// Scan every in-doubt prepare record `(txn_id, group_id, slice)` for recovery.
pub(crate) fn scan_xshard_prepares(shard: &Shard, crypto: DurableCrypto<'_>) -> XshardPrepareScan {
    let read = shard.control_read()?;
    let table = read
        .open_owner_table(XSHARD_PREPARE)
        .map_err(|e| e.to_string())?;
    let mut out = Vec::new();
    for row in table.iter().map_err(|e| e.to_string())? {
        let (key, value) = row.map_err(|e| e.to_string())?;
        let (txn_id, gid) = key.value();
        out.push((txn_id.to_string(), gid, crypto.unseal(value.value())?));
    }
    Ok(out)
}

/// Read a txn's durable decision (Some(true)=commit, Some(false)=abort, None=undecided).
pub(crate) fn get_xshard_decision(shard: &Shard, txn_id: &str) -> Result<Option<bool>, String> {
    let read = shard.control_read()?;
    let table = read
        .open_owner_table(XSHARD_DECISION)
        .map_err(|e| e.to_string())?;
    let encoded = table
        .get(txn_id)
        .map_err(|e| e.to_string())?
        .map(|value| value.value());
    match encoded {
        None | Some(4) => Ok(None),
        Some(0 | 2) => Ok(Some(false)),
        Some(1 | 3) => Ok(Some(true)),
        Some(_) => Err("corrupt cross-shard decision value".to_string()),
    }
}

/// Whether the decision/pending marker must survive participant recovery until a
/// separate MutationBatch parent receipt is durable.
pub(crate) fn get_xshard_decision_retain(shard: &Shard, txn_id: &str) -> Result<bool, String> {
    let read = shard.control_read()?;
    let table = read
        .open_owner_table(XSHARD_DECISION)
        .map_err(|e| e.to_string())?;
    let retain = table
        .get(txn_id)
        .map_err(|e| e.to_string())?
        .map(|value| matches!(value.value(), 2..=4))
        .unwrap_or(false);
    Ok(retain)
}

/// Scan digest-only decision keys for parent-aware startup GC. No prepared slice
/// or source payload is returned.
pub(crate) fn scan_xshard_decisions(shard: &Shard) -> XshardDecisionScan {
    let read = shard.control_read()?;
    let table = read
        .open_owner_table(XSHARD_DECISION)
        .map_err(|e| e.to_string())?;
    let mut rows = Vec::new();
    for row in table.iter().map_err(|e| e.to_string())? {
        let (key, value) = row.map_err(|e| e.to_string())?;
        let encoded = value.value();
        let outcome = match encoded {
            0 | 2 => Some(false),
            1 | 3 => Some(true),
            4 => None,
            _ => return Err("corrupt cross-shard decision value".to_string()),
        };
        rows.push((key.value().to_string(), outcome, matches!(encoded, 2..=4)));
    }
    Ok(rows)
}

/// Durably upsert a named materialized view's serialized blob.
#[cfg(feature = "compute-dist")]
pub(crate) fn put_matview(shard: &Shard, name: &str, blob: &[u8]) -> Result<(), String> {
    let op_id = format!("matview/put/{name}");
    control_write!(shard, &op_id, |control| {
        control
            .open_table::<&str, &[u8]>(MATVIEWS)
            .map_err(|e| e.to_string())?
            .insert(name, blob)
            .map_err(|e| e.to_string())?;
        Ok(())
    })
}

/// Scan every persisted materialized view `(name, blob)` for reload on boot.
#[cfg(feature = "compute-dist")]
pub(crate) fn scan_matviews(shard: &Shard) -> MatViewScanResult {
    let read = shard.control_read()?;
    let table = match read.open_owner_table(MATVIEWS) {
        Ok(table) => table,
        Err(_) => return Ok(Vec::new()),
    };
    let mut out = Vec::new();
    for row in table.iter().map_err(|e| e.to_string())? {
        let (key, value) = row.map_err(|e| e.to_string())?;
        out.push((key.value().to_string(), value.value().to_vec()));
    }
    Ok(out)
}

/// Durably upsert a PLAN-BACKED matview's serialized definition.
#[cfg(feature = "matview")]
pub(crate) fn put_plan_matview(shard: &Shard, name: &str, blob: &[u8]) -> Result<(), String> {
    let op_id = format!("plan-matview/put/{name}");
    control_write!(shard, &op_id, |control| {
        control
            .open_table::<&str, &[u8]>(PLAN_MATVIEWS)
            .map_err(|e| e.to_string())?
            .insert(name, blob)
            .map_err(|e| e.to_string())?;
        Ok(())
    })
}

/// Durably delete a plan-backed matview definition. A missing row is a clean no-op.
#[cfg(feature = "matview")]
pub(crate) fn delete_plan_matview(shard: &Shard, name: &str) -> Result<(), String> {
    let op_id = format!("plan-matview/delete/{name}");
    control_write!(shard, &op_id, |control| {
        control
            .open_table::<&str, &[u8]>(PLAN_MATVIEWS)
            .map_err(|e| e.to_string())?
            .remove(name)
            .map_err(|e| e.to_string())?;
        Ok(())
    })
}

/// Scan every persisted plan-backed matview `(name, definition-blob)` for reload on boot.
#[cfg(feature = "matview")]
pub(crate) fn scan_plan_matviews(shard: &Shard) -> Result<Vec<(String, Vec<u8>)>, String> {
    let read = shard.control_read()?;
    let table = match read.open_owner_table(PLAN_MATVIEWS) {
        Ok(table) => table,
        Err(_) => return Ok(Vec::new()),
    };
    let mut out = Vec::new();
    for row in table.iter().map_err(|e| e.to_string())? {
        let (key, value) = row.map_err(|e| e.to_string())?;
        out.push((key.value().to_string(), value.value().to_vec()));
    }
    Ok(out)
}

/// Durably upsert an incremental matview's operator-state snapshot.
#[cfg(feature = "matview")]
pub(crate) fn put_matview_operator_state(
    shard: &Shard,
    name: &str,
    blob: &[u8],
) -> Result<(), String> {
    let op_id = format!("matview-operator-state/put/{name}");
    control_write!(shard, &op_id, |control| {
        control
            .open_table::<&str, &[u8]>(MATVIEW_OPERATOR_STATE)
            .map_err(|e| e.to_string())?
            .insert(name, blob)
            .map_err(|e| e.to_string())?;
        Ok(())
    })
}

/// Durably delete an incremental matview's operator-state snapshot (missing = no-op).
#[cfg(feature = "matview")]
pub(crate) fn delete_matview_operator_state(shard: &Shard, name: &str) -> Result<(), String> {
    let op_id = format!("matview-operator-state/delete/{name}");
    control_write!(shard, &op_id, |control| {
        control
            .open_table::<&str, &[u8]>(MATVIEW_OPERATOR_STATE)
            .map_err(|e| e.to_string())?
            .remove(name)
            .map_err(|e| e.to_string())?;
        Ok(())
    })
}

/// Scan every persisted incremental-matview operator-state snapshot `(name, blob)`.
#[cfg(feature = "matview")]
pub(crate) fn scan_matview_operator_state(shard: &Shard) -> Result<Vec<(String, Vec<u8>)>, String> {
    let read = shard.control_read()?;
    let table = match read.open_owner_table(MATVIEW_OPERATOR_STATE) {
        Ok(table) => table,
        Err(_) => return Ok(Vec::new()),
    };
    let mut out = Vec::new();
    for row in table.iter().map_err(|e| e.to_string())? {
        let (key, value) = row.map_err(|e| e.to_string())?;
        out.push((key.value().to_string(), value.value().to_vec()));
    }
    Ok(out)
}
