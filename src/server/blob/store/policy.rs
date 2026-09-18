//! What a blob garbage-collection pass may reclaim: the retention policy, the
//! owner scope, and the counts it reports.

use serde::{Deserialize, Serialize};

const DAY_MS: u64 = 24 * 60 * 60 * 1000;
/// Default grace after a manifest's latest commit before an unheld blob may be
/// reclaimed: long enough for any client to take its first holder.
pub const DEFAULT_GC_GRACE_MS: u64 = DAY_MS;
/// Default idle time after which an uncommitted upload is abandoned.
pub const DEFAULT_UPLOAD_TTL_MS: u64 = DAY_MS;
/// Upper bound on either duration, so a typo cannot disable reclamation forever.
const MAX_RETENTION_MS: u64 = 366 * DAY_MS;

/// Grace and upload-expiry durations for one garbage-collection pass.
///
/// Both are compared against the pass's own commit timestamp and the
/// timestamps recorded by earlier admitted writes, never a clock read inside
/// the store, so a replayed pass decides exactly what the original decided.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BlobRetentionPolicy {
    gc_grace_ms: u64,
    upload_ttl_ms: u64,
}

impl BlobRetentionPolicy {
    /// `gc_grace_ms` may be zero (reclaim unheld blobs at once); an upload TTL
    /// of zero would expire uploads still being written and is refused.
    pub fn new(gc_grace_ms: u64, upload_ttl_ms: u64) -> Result<Self, String> {
        if gc_grace_ms > MAX_RETENTION_MS || upload_ttl_ms == 0 || upload_ttl_ms > MAX_RETENTION_MS
        {
            return Err("blob retention policy is out of range".to_string());
        }
        Ok(Self {
            gc_grace_ms,
            upload_ttl_ms,
        })
    }

    pub fn gc_grace_ms(&self) -> u64 {
        self.gc_grace_ms
    }

    pub fn upload_ttl_ms(&self) -> u64 {
        self.upload_ttl_ms
    }

    /// Whether a manifest last committed at `committed_at_ms` is past its grace.
    pub(super) fn grace_elapsed(&self, committed_at_ms: u64, now_ms: u64) -> bool {
        committed_at_ms.saturating_add(self.gc_grace_ms) <= now_ms
    }

    /// Whether an upload last written at `last_active_ms` is abandoned.
    pub(super) fn upload_abandoned(&self, last_active_ms: u64, now_ms: u64) -> bool {
        last_active_ms.saturating_add(self.upload_ttl_ms) <= now_ms
    }
}

impl Default for BlobRetentionPolicy {
    fn default() -> Self {
        Self {
            gc_grace_ms: DEFAULT_GC_GRACE_MS,
            upload_ttl_ms: DEFAULT_UPLOAD_TTL_MS,
        }
    }
}

/// Whose blobs and uploads a pass may reclaim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GcOwnerScope {
    /// Every owner, plus chunks held directly rather than through a manifest.
    AllOwners,
    /// Only manifests and uploads of this exact owner scope.
    Owner(String),
}

impl GcOwnerScope {
    pub fn owner(owner_scope: &str) -> Result<Self, String> {
        super::manifest::validate_owner_scope(owner_scope)?;
        Ok(Self::Owner(owner_scope.to_string()))
    }

    pub(super) fn covers(&self, owner_scope: &str) -> bool {
        match self {
            Self::AllOwners => true,
            Self::Owner(owner) => owner == owner_scope,
        }
    }

    /// Directly held chunks have no owner, so only an all-owner pass reclaims them.
    pub(super) fn covers_direct_chunks(&self) -> bool {
        match self {
            Self::AllOwners => true,
            Self::Owner(_) => false,
        }
    }
}

/// One garbage-collection pass: its owner scope and retention policy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SweepRequest {
    owner: GcOwnerScope,
    policy: BlobRetentionPolicy,
}

impl SweepRequest {
    pub fn new(owner: GcOwnerScope, policy: BlobRetentionPolicy) -> Self {
        Self { owner, policy }
    }

    pub fn owner(&self) -> &GcOwnerScope {
        &self.owner
    }

    pub fn policy(&self) -> BlobRetentionPolicy {
        self.policy
    }
}

impl Default for SweepRequest {
    fn default() -> Self {
        Self::new(GcOwnerScope::AllOwners, BlobRetentionPolicy::default())
    }
}

/// What a sweep reclaimed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SweepStats {
    pub blobs_reclaimed: u64,
    pub chunks_reclaimed: u64,
    pub uploads_expired: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn policy_bounds_are_enforced() {
        assert!(BlobRetentionPolicy::new(0, 1).is_ok());
        assert!(BlobRetentionPolicy::new(MAX_RETENTION_MS, MAX_RETENTION_MS).is_ok());
        assert!(BlobRetentionPolicy::new(MAX_RETENTION_MS + 1, 1).is_err());
        assert!(BlobRetentionPolicy::new(0, 0).is_err());
        assert!(BlobRetentionPolicy::new(0, MAX_RETENTION_MS + 1).is_err());
    }

    #[test]
    fn grace_and_expiry_are_inclusive_at_the_boundary_and_saturate() {
        let policy = BlobRetentionPolicy::new(10, 5).unwrap();
        assert!(!policy.grace_elapsed(100, 109));
        assert!(policy.grace_elapsed(100, 110));
        assert!(!policy.upload_abandoned(100, 104));
        assert!(policy.upload_abandoned(100, 105));
        assert!(!policy.grace_elapsed(u64::MAX, u64::MAX - 1));
    }

    #[test]
    fn owner_scope_filters_manifests_and_direct_chunks() {
        let all = GcOwnerScope::AllOwners;
        let one = GcOwnerScope::owner("carrier-owner:a").unwrap();
        assert!(all.covers("carrier-owner:b") && all.covers_direct_chunks());
        assert!(one.covers("carrier-owner:a"));
        assert!(!one.covers("carrier-owner:b") && !one.covers_direct_chunks());
        assert!(GcOwnerScope::owner("").is_err());
    }
}
