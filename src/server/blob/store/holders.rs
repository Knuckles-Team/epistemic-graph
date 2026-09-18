//! Holder-scoped blob references.
//!
//! A reference is a row `(digest, holder)` in `cas_holders`, and the per-digest
//! `cas_refcount` is exactly the sum of those rows' counts, maintained in the
//! same transaction by the functions below and nowhere else. Because a holder
//! names who holds the reference, taking it twice is one row and releasing it
//! twice removes one row: a retried or replayed reference can neither leak nor
//! drop a count. A digest with no holder rows reads zero and becomes eligible
//! for the next sweep once its grace has passed.
//!
//! Two holder kinds exist:
//! * **named holders** (`owner:<scope>`, `pack:<tenant>:...`) have set
//!   semantics: count is always 1;
//! * the one **counted engine holder** (`engine:counted`) backs the in-process
//!   `incref`/`decref` of engine-owned artifacts, whose callers keep their own
//!   balanced accounting inside one process; its row carries a multiplicity.

use super::manifest::{
    decode_blob_value, encode_blob_value, validate_digest, validate_owner_scope,
    MAX_BLOB_GC_TRACKED_DIGESTS,
};
use super::{CAS_BLOBS, CAS_HOLDERS};
use eg_storage::{BlobOwner, BlobSharedWrite};
use eg_transaction::AdmittedOwnerWrite;
use redb::ReadableTable;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

const MAX_HOLDER_ID_BYTES: usize = 512;
const COUNTED_ENGINE_HOLDER: &str = "engine:counted";

fn validate_holder_text(raw: &str, what: &str) -> Result<(), String> {
    if raw.is_empty()
        || raw.len() > MAX_HOLDER_ID_BYTES
        || !raw.bytes().all(|byte| byte.is_ascii_graphic())
    {
        return Err(format!("{what} is invalid or exceeds resource limits"));
    }
    Ok(())
}

/// Who holds a reference: `<namespace>:<name>`, printable ASCII, bounded.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct HolderId(String);

impl HolderId {
    pub fn new(raw: &str) -> Result<Self, String> {
        validate_holder_text(raw, "blob holder id")?;
        match raw.split_once(':') {
            Some((namespace, name)) if !namespace.is_empty() && !name.is_empty() => {
                Ok(Self(raw.to_string()))
            }
            _ => Err("blob holder id must be <namespace>:<name>".to_string()),
        }
    }

    /// The holder a wire caller's own reference is recorded under.
    pub fn for_owner(owner_scope: &str) -> Result<Self, String> {
        validate_owner_scope(owner_scope)?;
        Self::new(&format!("owner:{owner_scope}"))
    }

    /// The multiplicity-carrying holder of in-process engine references.
    pub fn counted_engine() -> Self {
        Self(COUNTED_ENGINE_HOLDER.to_string())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn is_counted(&self) -> bool {
        self.0 == COUNTED_ENGINE_HOLDER
    }
}

/// A holder-id prefix owned by one authority (e.g. `pack:<tenant>`), matched on
/// a whole `:`-separated segment so `pack:t1` never covers `pack:t10:x`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HolderNamespace(String);

impl HolderNamespace {
    pub fn new(raw: &str) -> Result<Self, String> {
        validate_holder_text(raw, "blob holder namespace")?;
        if raw.ends_with(':') {
            return Err("blob holder namespace must not end with ':'".to_string());
        }
        Ok(Self(raw.to_string()))
    }

    pub fn contains(&self, holder: &str) -> bool {
        holder
            .strip_prefix(self.0.as_str())
            .is_some_and(|rest| rest.len() > 1 && rest.starts_with(':'))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum HolderAction {
    Acquire { owner_scope: String },
    Release,
}

/// Take or give back one holder's reference to one digest.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HolderChange {
    digest: String,
    holder: HolderId,
    action: HolderAction,
}

impl HolderChange {
    pub fn acquire(digest: &str, holder: HolderId, owner_scope: &str) -> Result<Self, String> {
        validate_digest(digest)?;
        validate_owner_scope(owner_scope)?;
        Ok(Self {
            digest: digest.to_string(),
            holder,
            action: HolderAction::Acquire {
                owner_scope: owner_scope.to_string(),
            },
        })
    }

    pub fn release(digest: &str, holder: HolderId) -> Result<Self, String> {
        validate_digest(digest)?;
        Ok(Self {
            digest: digest.to_string(),
            holder,
            action: HolderAction::Release,
        })
    }

    /// A wire caller's reference to its own blob, held under its owner scope.
    pub fn owner_acquire(digest: &str, owner_scope: &str) -> Result<Self, String> {
        Self::acquire(digest, HolderId::for_owner(owner_scope)?, owner_scope)
    }

    /// Release of the reference [`Self::owner_acquire`] took.
    pub fn owner_release(digest: &str, owner_scope: &str) -> Result<Self, String> {
        Self::release(digest, HolderId::for_owner(owner_scope)?)
    }
}

/// The digest's total reference count after a change, and whether the change
/// moved it (`false` for a repeated acquire or a release of an absent holder).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct HolderOutcome {
    pub holders: u64,
    pub changed: bool,
}

/// Release every holder in `namespace` that is not in `live`: the orphans a
/// crashed or abandoned owner of that namespace left behind.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HolderReconcile {
    namespace: HolderNamespace,
    live: BTreeSet<String>,
}

impl HolderReconcile {
    pub fn new(namespace: HolderNamespace, live: Vec<HolderId>) -> Result<Self, String> {
        if live.len() > MAX_BLOB_GC_TRACKED_DIGESTS {
            return Err("blob holder reconcile exceeds resource limits".to_string());
        }
        if let Some(stray) = live
            .iter()
            .find(|holder| !namespace.contains(holder.as_str()))
        {
            return Err(format!(
                "live blob holder {} is outside the reconciled namespace",
                stray.as_str()
            ));
        }
        Ok(Self {
            namespace,
            live: live.into_iter().map(|holder| holder.0).collect(),
        })
    }
}

/// What a reconcile released.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReconcileStats {
    pub holders_released: u64,
    pub references_released: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct HolderRow {
    owner_scope: String,
    acquired_at_ms: u64,
    count: u64,
}

fn read_holder(
    wtx: &AdmittedOwnerWrite<'_, BlobOwner>,
    digest: &str,
    holder: &str,
) -> Result<Option<HolderRow>, String> {
    let holders = wtx.open_table(CAS_HOLDERS)?;
    let row = holders
        .get((digest, holder))
        .map_err(|error| error.to_string())?;
    // Bind before returning: the guard borrows `holders` (E0597 otherwise).
    let decoded = row
        .map(|value| decode_blob_value(value.value()))
        .transpose();
    decoded
}

fn write_holder(
    wtx: &AdmittedOwnerWrite<'_, BlobOwner>,
    digest: &str,
    holder: &str,
    row: Option<&HolderRow>,
) -> Result<(), String> {
    let mut holders = wtx.open_table(CAS_HOLDERS)?;
    match row {
        Some(row) => {
            let bytes = encode_blob_value(row, "blob holder")?;
            holders
                .insert((digest, holder), bytes.as_slice())
                .map_err(|error| error.to_string())?;
        }
        None => {
            holders
                .remove((digest, holder))
                .map_err(|error| error.to_string())?;
        }
    }
    Ok(())
}

/// A reference may only be taken on something stored: a manifest or a chunk.
fn require_object(
    wtx: &AdmittedOwnerWrite<'_, BlobOwner>,
    shared: &BlobSharedWrite<'_>,
    digest: &str,
) -> Result<(), String> {
    let manifest = wtx
        .open_table(CAS_BLOBS)?
        .get(digest)
        .map_err(|error| error.to_string())?
        .is_some();
    if manifest || shared.chunk_present(digest)? {
        Ok(())
    } else {
        Err("unknown blob digest".to_string())
    }
}

fn signed(count: u64) -> Result<i64, String> {
    i64::try_from(count).map_err(|_| "blob reference count exceeds resource limits".to_string())
}

/// Apply one named-holder change inside an admitted blob write.
pub(super) fn apply_holder_change(
    wtx: &AdmittedOwnerWrite<'_, BlobOwner>,
    shared: &BlobSharedWrite<'_>,
    change: &HolderChange,
    at_ms: u64,
) -> Result<HolderOutcome, String> {
    if change.holder.is_counted() {
        return Err("the counted engine holder is adjusted, not acquired".to_string());
    }
    let digest = change.digest.as_str();
    let holder = change.holder.as_str();
    let existing = read_holder(wtx, digest, holder)?;
    let changed = match (&change.action, existing) {
        (HolderAction::Acquire { .. }, Some(_)) | (HolderAction::Release, None) => false,
        (HolderAction::Acquire { owner_scope }, None) => {
            require_object(wtx, shared, digest)?;
            let row = HolderRow {
                owner_scope: owner_scope.clone(),
                acquired_at_ms: at_ms,
                count: 1,
            };
            write_holder(wtx, digest, holder, Some(&row))?;
            shared.adjust_refcount(digest, 1)?;
            true
        }
        (HolderAction::Release, Some(row)) => {
            write_holder(wtx, digest, holder, None)?;
            shared.adjust_refcount(digest, -signed(row.count)?)?;
            true
        }
    };
    Ok(HolderOutcome {
        holders: shared.refcount(digest)?,
        changed,
    })
}

/// Move the counted engine holder's row multiplicity by `delta`, failing closed
/// on underflow of that holder's own count (a release accounted twice). The
/// caller moves `cas_refcount` by the same `delta` in the same write.
pub(super) fn adjust_counted_holder_row(
    wtx: &AdmittedOwnerWrite<'_, BlobOwner>,
    shared: &BlobSharedWrite<'_>,
    digest: &str,
    delta: i64,
    at_ms: u64,
) -> Result<(), String> {
    validate_digest(digest)?;
    let holder = COUNTED_ENGINE_HOLDER;
    let existing = read_holder(wtx, digest, holder)?;
    let current = existing.as_ref().map_or(0, |row| row.count);
    let updated = current
        .checked_add_signed(delta)
        .ok_or_else(|| "blob holder reference count underflow".to_string())?;
    if delta > 0 && existing.is_none() {
        require_object(wtx, shared, digest)?;
    }
    let row = (updated > 0).then(|| HolderRow {
        owner_scope: super::ENGINE_BLOB_OWNER_SCOPE.to_string(),
        acquired_at_ms: existing.map_or(at_ms, |row| row.acquired_at_ms),
        count: updated,
    });
    write_holder(wtx, digest, holder, row.as_ref())
}

/// Release every holder of the request's namespace that is not live.
pub(super) fn reconcile_holders(
    wtx: &AdmittedOwnerWrite<'_, BlobOwner>,
    shared: &BlobSharedWrite<'_>,
    request: &HolderReconcile,
) -> Result<ReconcileStats, String> {
    let orphans = orphan_holders(wtx, request)?;
    let mut stats = ReconcileStats::default();
    for (digest, holder, count) in orphans {
        write_holder(wtx, &digest, &holder, None)?;
        shared.adjust_refcount(&digest, -signed(count)?)?;
        stats.holders_released += 1;
        stats.references_released = stats.references_released.saturating_add(count);
    }
    Ok(stats)
}

fn orphan_holders(
    wtx: &AdmittedOwnerWrite<'_, BlobOwner>,
    request: &HolderReconcile,
) -> Result<Vec<(String, String, u64)>, String> {
    let holders = wtx.open_table(CAS_HOLDERS)?;
    let mut orphans = Vec::new();
    for entry in holders.iter().map_err(|error| error.to_string())? {
        let (key, value) = entry.map_err(|error| error.to_string())?;
        let (digest, holder) = key.value();
        if !request.namespace.contains(holder) || request.live.contains(holder) {
            continue;
        }
        if orphans.len() >= MAX_BLOB_GC_TRACKED_DIGESTS {
            return Err("blob holder reconcile exceeds resource limits".to_string());
        }
        let row: HolderRow = decode_blob_value(value.value())?;
        orphans.push((digest.to_string(), holder.to_string(), row.count));
    }
    Ok(orphans)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn holder_ids_are_namespaced_bounded_printable_ascii() {
        assert!(HolderId::new("owner:carrier-owner:ab12").is_ok());
        for bad in ["", "owner", ":name", "owner:", "own er:x", "ownér:x"] {
            assert!(HolderId::new(bad).is_err(), "{bad:?}");
        }
        assert!(HolderId::new(&format!("a:{}", "x".repeat(MAX_HOLDER_ID_BYTES))).is_err());
        assert_eq!(
            HolderId::for_owner("carrier-owner:ff").unwrap().as_str(),
            "owner:carrier-owner:ff"
        );
        assert!(HolderId::for_owner("").is_err());
        assert!(HolderId::counted_engine().is_counted());
    }

    #[test]
    fn a_namespace_covers_whole_segments_only() {
        let namespace = HolderNamespace::new("pack:t1").unwrap();
        assert!(namespace.contains("pack:t1:component:3"));
        assert!(!namespace.contains("pack:t10:component:3"));
        assert!(!namespace.contains("pack:t1:"));
        assert!(!namespace.contains("pack:t1"));
        assert!(HolderNamespace::new("pack:").is_err());
    }

    #[test]
    fn a_reconcile_refuses_live_holders_outside_its_namespace() {
        let namespace = HolderNamespace::new("pack:t1").unwrap();
        let outside = HolderId::new("pack:t2:x").unwrap();
        assert!(HolderReconcile::new(namespace.clone(), vec![outside]).is_err());
        let inside = HolderId::new("pack:t1:x").unwrap();
        assert!(HolderReconcile::new(namespace, vec![inside]).is_ok());
    }

    #[test]
    fn changes_validate_their_digest_and_owner() {
        let digest = "a".repeat(64);
        assert!(HolderChange::owner_acquire(&digest, "carrier-owner:1").is_ok());
        assert!(HolderChange::owner_acquire("nope", "carrier-owner:1").is_err());
        assert!(HolderChange::owner_acquire(&digest, "").is_err());
        assert!(HolderChange::owner_release(&digest, "carrier-owner:1").is_ok());
    }
}
