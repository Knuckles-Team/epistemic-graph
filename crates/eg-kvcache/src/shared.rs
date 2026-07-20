//! The shared multi-instance KV-cache backend (CONCEPT:EG-KG.enrichment.content-address-separation).
//!
//! Where [`crate::TieredCache`] (EG-185) makes ONE instance survive OOM by tiering, the
//! shared backend lets MANY instances (parallel vLLM / LMCache workers) **share KV
//! blocks by content**. The seam is [`SharedKvBackend`]: an external connector computes
//! the token-block hash (e.g. over the prompt-prefix token ids), then `put_block` /
//! `get_block` / `contains` against the backend. Because the address IS the content
//! hash ([`crate::content_hash`]), two workers that produced the SAME KV page store it
//! **once** — the classic prefix-cache / LMCache dedup win — and a cold worker can
//! `get_block` a page a warm worker already computed.
//!
//! [`SharedKvIndex`] is the in-process implementation: a content-addressed,
//! **ref-counted** block table. Ref-counting is what makes it safe to share — a block
//! is only freed when the LAST holder releases it ([`SharedKvIndex::release`]).
//!
//! ## A networked backend (follow-up)
//!
//! A remote/distributed backend implements the SAME trait over the wire:
//!
//! * `get_block(hash)` → an RPC (or a read against a shared object store / Redis / a
//!   networked KV) keyed by the hash; the hash is globally stable (FNV-1a-128), so any
//!   node derives the same address.
//! * `put_block(hash, block)` → an idempotent upload; the server dedups by hash and
//!   bumps a distributed ref-count. `contains` is a cheap existence probe used to skip
//!   an upload the cluster already has.
//!
//! A worker would typically wrap a remote backend BEHIND a local [`SharedKvIndex`] (or a
//! [`crate::TieredCache`]) as an L1, falling through to the network on a local miss. The
//! networked impl, the vLLM/LMCache connector, and the server endpoint wiring are all
//! explicit follow-ups (see the crate report).

use std::collections::HashMap;
use std::sync::Arc;

use crate::hash::content_hash;
use crate::value::Block;
use crate::version::DataVersion;

/// The backend seam an external vLLM/LMCache connector calls to share KV blocks by
/// content hash (CONCEPT:EG-KG.enrichment.content-address-separation), with data-version invalidation layered on top
/// (CONCEPT:EG-KG.storage.content-addressed-put).
///
/// Implementors may be in-process ([`SharedKvIndex`]) or remote (an RPC / object-store
/// client — a follow-up). The `hash` is a content address (see [`crate::content_hash`]),
/// so identical blocks DEDUP regardless of which instance produced them. Each entry ALSO
/// carries the [`DataVersion`] it was derived at; once the store's current version moves
/// past it, the entry is a MISS (stale LLM/agent context is never served).
pub trait SharedKvBackend {
    /// Fetch the block stored under `hash`, if present AND still fresh for the store's
    /// current [`DataVersion`] (CONCEPT:EG-KG.storage.content-addressed-put). A stale entry (derived at an older
    /// version) reads as absent.
    fn get_block(&self, hash: &str) -> Option<Block>;

    /// Store `block` under `hash`, stamped with the [`DataVersion`] it was `derived_at`,
    /// incrementing its ref-count. Idempotent on content: a second `put_block` of the
    /// SAME hash does NOT re-store the bytes, it just bumps the ref-count (the dedup
    /// guarantee) and REFRESHES the entry's version to `derived_at` (a re-publish at a
    /// newer version un-stales it). Pass [`DataVersion::Agnostic`] for a pure,
    /// content-addressed KV page that must never be version-invalidated. Returns `true`
    /// iff this call created a NEW entry (the block was not already present).
    fn put_block(&mut self, hash: &str, block: Block, derived_at: DataVersion) -> bool;

    /// Whether a fresh (non-stale) block exists under `hash` — a cheap probe to skip a
    /// redundant upload. A stale entry reads as absent (CONCEPT:EG-KG.storage.content-addressed-put).
    fn contains(&self, hash: &str) -> bool;

    /// Advance the store's current data version (CONCEPT:EG-KG.storage.content-addressed-put) — the hook a graph
    /// write drives (a committed write bumps `GraphCore::version()`, KG-2.180, then calls
    /// this). EAGERLY retires every version-tagged entry that is now stale (freeing its
    /// bytes + ref-counts), mirroring `ResultCache`'s atomic version-bump retire
    /// (KG-2.233). [`DataVersion::Agnostic`] entries (pure KV pages) are untouched.
    fn set_data_version(&mut self, version: DataVersion);
}

/// Handle to a live zero-copy snapshot over a set of shared pages
/// (CONCEPT:EG-KG.memory.zero-copy-snapshot-fork). Opaque + `Copy`; minted by
/// [`SharedKvIndex::snapshot`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SnapshotId(pub u64);

/// Handle to a live branch forked off a [`SnapshotId`] (CONCEPT:EG-KG.memory.zero-copy-snapshot-fork).
/// A branch reads through the shared snapshot pages and writes copy-on-write into its own
/// overlay. Minted by [`SharedKvIndex::fork`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct BranchId(pub u64);

/// A pinned, content-addressed view over a set of shared pages (CONCEPT:EG-KG.memory.zero-copy-snapshot-fork).
///
/// `pages` holds an `Arc` clone of each captured page — a pointer + ref-count bump, NOT a
/// byte copy — so the page has ONE physical allocation shared by the snapshot, the
/// originating [`SharedEntry`], and every branch that reads it. `pinned` records the
/// block hashes whose [`SharedEntry`] ref-count we bumped so the entry stays addressable
/// for the snapshot's lifetime; [`SharedKvIndex::release_snapshot`] undoes those pins.
struct Snapshot {
    pages: HashMap<String, Arc<Vec<u8>>>,
    pinned: Vec<String>,
    branch_count: usize,
}

/// A branch over a [`Snapshot`] (CONCEPT:EG-KG.memory.zero-copy-snapshot-fork): unwritten keys resolve to the
/// shared snapshot page (zero-copy); a [`SharedKvIndex::branch_put`] writes a fresh page
/// into `overlay`, isolated from siblings and the shared snapshot (copy-on-write).
struct Branch {
    snapshot: SnapshotId,
    overlay: HashMap<String, Arc<Vec<u8>>>,
}

/// Occupancy statistics for the snapshot-fork layer (CONCEPT:EG-KG.memory.zero-copy-snapshot-fork) — the
/// numbers a test reads to PROVE zero-copy fan-out: `resident_fork_bytes` counts each
/// distinct physical allocation ONCE (deduped by `Arc` identity), so it stays flat at the
/// shared-page total regardless of how many branches fan out over it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ForkStats {
    /// Live snapshots.
    pub snapshots: usize,
    /// Live branches (the fan-out count).
    pub branches: usize,
    /// Distinct shared pages held across all snapshots.
    pub shared_pages: usize,
    /// Physical bytes of the distinct shared pages (ONE copy each).
    pub shared_bytes: usize,
    /// Distinct branch-local copy-on-write overlay pages across all branches.
    pub overlay_pages: usize,
    /// Physical bytes of the distinct overlay pages.
    pub overlay_bytes: usize,
    /// Total distinct physical bytes resident in the fork layer (shared + overlay, each
    /// allocation counted once). The zero-copy proof: this is `≈ shared_bytes` for a
    /// read-only fan-out of ANY branch count, NOT `branches × shared_bytes`.
    pub resident_fork_bytes: usize,
}

/// One content-addressed entry: the shared bytes, how many holders reference it, and the
/// [`DataVersion`] it was derived at (CONCEPT:EG-KG.storage.content-addressed-put).
struct SharedEntry {
    /// `Arc` so `get_block` hands out the bytes without a deep copy per reader.
    data: Arc<Vec<u8>>,
    refcount: usize,
    /// The data version this entry was derived at; [`DataVersion::Agnostic`] for a pure
    /// content-addressed page that is never version-invalidated.
    version: DataVersion,
}

/// Occupancy / dedup statistics for a [`SharedKvIndex`] (CONCEPT:EG-KG.enrichment.content-address-separation).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SharedStats {
    /// Distinct blocks stored (post-dedup).
    pub unique_blocks: usize,
    /// Sum of ref-counts across all blocks (logical holders).
    pub total_refs: usize,
    /// Bytes actually resident (distinct blocks only).
    pub resident_bytes: usize,
    /// Logical bytes requested (`Σ refcount × block_len`) — resident + dedup savings.
    pub logical_bytes: usize,
    /// Lifetime `put_block` calls that hit an EXISTING block (dedup hits).
    pub dedup_hits: u64,
    /// Lifetime `get_block` hits.
    pub get_hits: u64,
    /// Lifetime `get_block` misses.
    pub get_misses: u64,
    /// Lifetime entries RETIRED as stale by a [`SharedKvBackend::set_data_version`] bump
    /// (CONCEPT:EG-KG.storage.content-addressed-put) — the invalidation proof counter a test reads.
    pub stale_retired: u64,
}

impl SharedStats {
    /// Bytes SAVED by dedup (`logical_bytes - resident_bytes`).
    pub fn dedup_savings(&self) -> usize {
        self.logical_bytes.saturating_sub(self.resident_bytes)
    }
}

/// An in-process, content-addressed, ref-counted shared KV block store (CONCEPT:EG-KG.enrichment.content-address-separation).
///
/// The canonical [`SharedKvBackend`] implementation: identical blocks are stored ONCE
/// and ref-counted so they are freed only when the last holder [`release`](Self::release)s
/// them.
#[derive(Default)]
pub struct SharedKvIndex {
    blocks: HashMap<String, SharedEntry>,
    /// The store's current data version (CONCEPT:EG-KG.storage.content-addressed-put). Defaults to
    /// [`DataVersion::Agnostic`] — version-tracking is INACTIVE until a
    /// [`SharedKvIndex::set_data_version`] call, so a store used purely for
    /// content-addressed dedup (EG-186) behaves exactly as before (zero-cost / opt-in).
    current_version: DataVersion,
    dedup_hits: u64,
    get_hits: u64,
    get_misses: u64,
    stale_retired: u64,
    /// Live zero-copy snapshots (CONCEPT:EG-KG.memory.zero-copy-snapshot-fork) — each pins a set of
    /// shared pages by `Arc` so N branches fan out over ONE physical copy.
    snapshots: HashMap<SnapshotId, Snapshot>,
    /// Live branches: an `Arc`-shared snapshot + a copy-on-write branch-local overlay.
    branches: HashMap<BranchId, Branch>,
    next_snapshot: u64,
    next_branch: u64,
}

impl SharedKvIndex {
    /// A fresh, empty shared index.
    pub fn new() -> Self {
        SharedKvIndex::default()
    }

    /// Content-address `block` (compute its hash), store it (dedup + ref-count) as a
    /// version-[`Agnostic`](DataVersion::Agnostic) pure KV page, and return the hash — the
    /// convenience path when the caller has raw bytes rather than a precomputed
    /// token-hash. A content-addressed page IS its content, so it is never
    /// version-invalidated; use [`SharedKvBackend::put_block`] with a real
    /// [`DataVersion::At`] for DERIVED context that must go stale on a graph write.
    pub fn put_by_content(&mut self, block: Block) -> String {
        let hash = content_hash(&block);
        self.put_block(&hash, block, DataVersion::Agnostic);
        hash
    }

    /// The store's current data version (CONCEPT:EG-KG.storage.content-addressed-put).
    pub fn current_version(&self) -> DataVersion {
        self.current_version
    }

    /// Decrement the ref-count for `hash`; the block's bytes are freed when it reaches
    /// zero. Returns the REMAINING ref-count (`0` once dropped, `None` if unknown).
    pub fn release(&mut self, hash: &str) -> Option<usize> {
        if let Some(e) = self.blocks.get_mut(hash) {
            e.refcount -= 1;
            if e.refcount == 0 {
                self.blocks.remove(hash);
                Some(0)
            } else {
                Some(e.refcount)
            }
        } else {
            None
        }
    }

    /// The current ref-count for `hash` (`0` if absent).
    pub fn refcount(&self, hash: &str) -> usize {
        self.blocks.get(hash).map(|e| e.refcount).unwrap_or(0)
    }

    /// Occupancy + dedup statistics.
    pub fn stats(&self) -> SharedStats {
        let mut resident_bytes = 0usize;
        let mut logical_bytes = 0usize;
        let mut total_refs = 0usize;
        for e in self.blocks.values() {
            resident_bytes += e.data.len();
            logical_bytes += e.data.len() * e.refcount;
            total_refs += e.refcount;
        }
        SharedStats {
            unique_blocks: self.blocks.len(),
            total_refs,
            resident_bytes,
            logical_bytes,
            dedup_hits: self.dedup_hits,
            get_hits: self.get_hits,
            get_misses: self.get_misses,
            stale_retired: self.stale_retired,
        }
    }

    // ── Zero-copy snapshot-fork (CONCEPT:EG-KG.memory.zero-copy-snapshot-fork) ──────────────────────────
    //
    // The "LMCacheMPConnector snapshot → branch" primitive: pin the current shared pages
    // for a set of keys into a SNAPSHOT, then FORK N branches that all read those SAME
    // physical pages (one copy, shared by `Arc`), copy-on-write only when a branch writes.
    // This turns warm-fork fan-out from O(N) per-branch page COPIES into O(1) pointer
    // bumps — the zero-copy rung the AU `crossmodal_fork` `max_concurrency>1` path targets.

    /// Pin the current pages for `keys` into a zero-copy [`SnapshotId`]
    /// (CONCEPT:EG-KG.memory.zero-copy-snapshot-fork).
    ///
    /// For each present, non-stale key this captures an `Arc` clone of its page — a
    /// pointer + ref-count bump, NOT a byte copy — so the snapshot shares the SAME physical
    /// allocation as the live [`SharedEntry`]. It also bumps that entry's ref-count (a
    /// pin), so a concurrent [`release`](Self::release) cannot drop the page out from under
    /// the snapshot; [`release_snapshot`](Self::release_snapshot) undoes the pins. Stale or
    /// absent keys are skipped (they simply won't resolve for the snapshot's branches).
    pub fn snapshot<S: AsRef<str>>(&mut self, keys: &[S]) -> SnapshotId {
        let mut pages = HashMap::new();
        let mut pinned = Vec::new();
        let current = self.current_version;
        for k in keys {
            let k = k.as_ref();
            if let Some(e) = self.blocks.get_mut(k) {
                if e.version.is_fresh(current) {
                    // `Arc::clone` = share the ONE allocation (zero-copy); pin the entry.
                    pages.insert(k.to_string(), Arc::clone(&e.data));
                    e.refcount += 1;
                    pinned.push(k.to_string());
                }
            }
        }
        let id = SnapshotId(self.next_snapshot);
        self.next_snapshot += 1;
        self.snapshots.insert(
            id,
            Snapshot {
                pages,
                pinned,
                branch_count: 0,
            },
        );
        id
    }

    /// Fork a branch off `snapshot` (CONCEPT:EG-KG.memory.zero-copy-snapshot-fork) — an **O(1)** op that copies
    /// NO pages: the new branch shares every snapshot page by `Arc` and starts with an
    /// empty copy-on-write overlay. Returns `None` if the snapshot is unknown. This is the
    /// fan-out primitive: forking 8 or 128 branches costs 8 or 128 pointer bumps, and the
    /// resident page bytes stay flat (see [`fork_stats`](Self::fork_stats)).
    pub fn fork(&mut self, snapshot: SnapshotId) -> Option<BranchId> {
        let snap = self.snapshots.get_mut(&snapshot)?;
        snap.branch_count += 1;
        let id = BranchId(self.next_branch);
        self.next_branch += 1;
        self.branches.insert(
            id,
            Branch {
                snapshot,
                overlay: HashMap::new(),
            },
        );
        Some(id)
    }

    /// Zero-copy read for `branch` (CONCEPT:EG-KG.memory.zero-copy-snapshot-fork): the branch-local
    /// copy-on-write overlay first, else the shared snapshot page. Returns an `Arc` clone
    /// (a pointer bump, NOT a byte copy — so N branches reading the same key hand out N
    /// handles to ONE allocation). `None` if the branch is unknown or the key resolves in
    /// neither overlay nor snapshot.
    pub fn branch_get(&self, branch: BranchId, key: &str) -> Option<Arc<Vec<u8>>> {
        let b = self.branches.get(&branch)?;
        if let Some(v) = b.overlay.get(key) {
            return Some(Arc::clone(v));
        }
        let snap = self.snapshots.get(&b.snapshot)?;
        snap.pages.get(key).map(Arc::clone)
    }

    /// Copy-on-write write into `branch` (CONCEPT:EG-KG.memory.zero-copy-snapshot-fork): stores `val` as a
    /// fresh page in the branch-local overlay, leaving the shared snapshot page — and
    /// therefore every sibling branch — untouched (structural isolation). Returns `true`
    /// iff the branch exists and the write was applied (`false` for an unknown branch).
    pub fn branch_put(&mut self, branch: BranchId, key: &str, val: Block) -> bool {
        match self.branches.get_mut(&branch) {
            Some(b) => {
                b.overlay.insert(key.to_string(), Arc::new(val));
                true
            }
            None => false,
        }
    }

    /// Drop a branch (CONCEPT:EG-KG.memory.zero-copy-snapshot-fork), freeing its copy-on-write overlay and
    /// decrementing its snapshot's branch count. Returns `false` if the branch is unknown.
    pub fn drop_branch(&mut self, branch: BranchId) -> bool {
        if let Some(b) = self.branches.remove(&branch) {
            if let Some(snap) = self.snapshots.get_mut(&b.snapshot) {
                snap.branch_count = snap.branch_count.saturating_sub(1);
            }
            true
        } else {
            false
        }
    }

    /// Release a snapshot (CONCEPT:EG-KG.memory.zero-copy-snapshot-fork): drops its shared-page `Arc`s and
    /// UNPINS each captured block (mirrors [`release`](Self::release), freeing an entry
    /// when the snapshot held its last ref). Any pages still referenced by a live branch
    /// or another holder survive (that is the whole point of ref-counting). Returns
    /// `false` if the snapshot is unknown.
    pub fn release_snapshot(&mut self, snapshot: SnapshotId) -> bool {
        if let Some(snap) = self.snapshots.remove(&snapshot) {
            for h in &snap.pinned {
                if let Some(e) = self.blocks.get_mut(h) {
                    e.refcount -= 1;
                    if e.refcount == 0 {
                        self.blocks.remove(h);
                    }
                }
            }
            true
        } else {
            false
        }
    }

    /// Number of pages captured by `snapshot` (`None` if unknown).
    pub fn snapshot_page_count(&self, snapshot: SnapshotId) -> Option<usize> {
        self.snapshots.get(&snapshot).map(|s| s.pages.len())
    }

    /// Number of live branches forked off `snapshot` (`None` if unknown).
    pub fn snapshot_branch_count(&self, snapshot: SnapshotId) -> Option<usize> {
        self.snapshots.get(&snapshot).map(|s| s.branch_count)
    }

    /// Occupancy of the snapshot-fork layer (CONCEPT:EG-KG.memory.zero-copy-snapshot-fork) — the zero-copy
    /// proof. `resident_fork_bytes` counts each distinct physical allocation ONCE
    /// (deduped by `Arc` pointer identity), so a read-only fan-out of N branches over M
    /// shared pages reports `≈ shared_bytes` (M pages) — NOT `N × shared_bytes` — and a
    /// copy-on-write write adds exactly ONE overlay page, never N.
    pub fn fork_stats(&self) -> ForkStats {
        // Dedup allocations by `Arc` pointer identity: two `Arc`s to the same page share a
        // pointer, so each distinct physical allocation is counted exactly once.
        let mut shared: HashMap<*const Vec<u8>, usize> = HashMap::new();
        for snap in self.snapshots.values() {
            for p in snap.pages.values() {
                shared.insert(Arc::as_ptr(p), p.len());
            }
        }
        let mut overlay: HashMap<*const Vec<u8>, usize> = HashMap::new();
        for b in self.branches.values() {
            for p in b.overlay.values() {
                overlay.insert(Arc::as_ptr(p), p.len());
            }
        }
        let shared_bytes: usize = shared.values().sum();
        let overlay_bytes: usize = overlay.values().sum();
        // Resident = union of both (an overlay page is a distinct allocation, but guard
        // against any pointer appearing in both by taking the union).
        let mut resident: HashMap<*const Vec<u8>, usize> = shared.clone();
        resident.extend(overlay.iter());
        let resident_fork_bytes: usize = resident.values().sum();
        ForkStats {
            snapshots: self.snapshots.len(),
            branches: self.branches.len(),
            shared_pages: shared.len(),
            shared_bytes,
            overlay_pages: overlay.len(),
            overlay_bytes,
            resident_fork_bytes,
        }
    }
}

impl SharedKvBackend for SharedKvIndex {
    fn get_block(&self, hash: &str) -> Option<Block> {
        // NOTE: `&self` — stats are updated by callers that need them; a `&mut` variant
        // could count here. We keep the trait read-only-friendly for remote impls.
        // Freshness gate (CONCEPT:EG-KG.storage.content-addressed-put): a stale entry (derived at an older version)
        // reads as absent so no stale LLM/agent context is ever served. `set_data_version`
        // eagerly retires such entries; this lazy gate is the belt-and-suspenders backstop
        // (e.g. an entry re-put at a version already behind current).
        self.blocks.get(hash).and_then(|e| {
            if e.version.is_fresh(self.current_version) {
                Some((*e.data).clone())
            } else {
                None
            }
        })
    }

    fn put_block(&mut self, hash: &str, block: Block, derived_at: DataVersion) -> bool {
        if let Some(e) = self.blocks.get_mut(hash) {
            // Dedup: identical content already present — bump the ref-count and REFRESH
            // the version to `derived_at` (a re-publish at a newer version un-stales it).
            e.refcount += 1;
            e.version = derived_at;
            self.dedup_hits += 1;
            false
        } else {
            self.blocks.insert(
                hash.to_string(),
                SharedEntry {
                    data: Arc::new(block),
                    refcount: 1,
                    version: derived_at,
                },
            );
            true
        }
    }

    fn contains(&self, hash: &str) -> bool {
        self.blocks
            .get(hash)
            .is_some_and(|e| e.version.is_fresh(self.current_version))
    }

    fn set_data_version(&mut self, version: DataVersion) {
        self.current_version = version;
        // Eagerly retire every now-stale version-tagged entry (frees bytes + ref-counts),
        // mirroring ResultCache's atomic version-bump retire (KG-2.233). Agnostic pure-KV
        // pages are kept — their content address fully determines correctness.
        let before = self.blocks.len();
        self.blocks.retain(|_, e| e.version.is_fresh(version));
        self.stale_retired += (before - self.blocks.len()) as u64;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// CONCEPT:EG-KG.enrichment.content-address-separation — two instances putting the SAME block content dedup to one
    /// resident copy while the ref-count tracks both holders.
    #[test]
    fn eg186_shared_backend_dedups_identical_blocks() {
        let mut idx = SharedKvIndex::new();
        let block: Block = vec![9u8; 512];
        let h1 = idx.put_by_content(block.clone()); // "instance A"
        let h2 = idx.put_by_content(block.clone()); // "instance B", same content
        assert_eq!(h1, h2, "same content ⇒ same address");
        let s = idx.stats();
        assert_eq!(s.unique_blocks, 1, "stored ONCE despite two puts");
        assert_eq!(s.resident_bytes, 512, "only one physical copy resident");
        assert_eq!(idx.refcount(&h1), 2, "both holders ref-counted");
        assert_eq!(s.dedup_hits, 1);
        assert_eq!(
            s.dedup_savings(),
            512,
            "one full block's worth of bytes saved"
        );
    }

    /// CONCEPT:EG-KG.enrichment.content-address-separation — ref-counting frees a block only when the LAST holder releases.
    #[test]
    fn eg186_shared_backend_refcounts_and_frees_on_last_release() {
        let mut idx = SharedKvIndex::new();
        let h = idx.put_by_content(vec![1u8; 64]);
        idx.put_block(&h, vec![1u8; 64], DataVersion::Agnostic); // second holder
        assert_eq!(idx.refcount(&h), 2);
        assert_eq!(idx.release(&h), Some(1), "one holder left");
        assert!(idx.contains(&h), "still resident while referenced");
        assert_eq!(idx.release(&h), Some(0), "last release frees it");
        assert!(!idx.contains(&h), "block gone after final release");
        assert_eq!(
            idx.release(&h),
            None,
            "releasing an unknown block reports None"
        );
    }

    /// CONCEPT:EG-KG.enrichment.content-address-separation — get_block returns stored bytes and misses on an unknown hash.
    #[test]
    fn eg186_shared_backend_get_and_contains() {
        let mut idx = SharedKvIndex::new();
        let block: Block = vec![3, 1, 4, 1, 5, 9, 2, 6];
        let h = idx.put_by_content(block.clone());
        assert!(idx.contains(&h));
        assert_eq!(idx.get_block(&h), Some(block));
        assert_eq!(idx.get_block("deadbeef"), None, "unknown hash misses");
        assert!(!idx.contains("deadbeef"));
    }

    /// CONCEPT:EG-KG.enrichment.content-address-separation — put_block reports whether it created a NEW entry (upload-skip
    /// signal for a networked backend).
    #[test]
    fn eg186_put_block_reports_new_vs_deduped() {
        let mut idx = SharedKvIndex::new();
        assert!(
            idx.put_block("h", vec![0u8; 8], DataVersion::Agnostic),
            "first put creates the entry"
        );
        assert!(
            !idx.put_block("h", vec![0u8; 8], DataVersion::Agnostic),
            "second put is a dedup, not new"
        );
    }

    /// CONCEPT:EG-KG.enrichment.content-address-separation — different content lands under different addresses (no false dedup).
    #[test]
    fn eg186_distinct_content_distinct_address() {
        let mut idx = SharedKvIndex::new();
        let a = idx.put_by_content(vec![1u8; 32]);
        let b = idx.put_by_content(vec![2u8; 32]);
        assert_ne!(a, b);
        assert_eq!(idx.stats().unique_blocks, 2);
    }

    /// CONCEPT:EG-KG.storage.content-addressed-put — a versioned (derived-context) entry is served while the data
    /// version is unchanged, then MISSES after a `set_data_version` bump (a graph write),
    /// and a re-publish at the new version serves fresh content. The core invalidation
    /// contract for the shared backend.
    #[test]
    fn eg359_version_bump_invalidates_derived_context_entry() {
        let mut idx = SharedKvIndex::new();
        idx.set_data_version(DataVersion::At(0));
        // Derived context for "agent-ctx" assembled from graph data at version 0.
        assert!(idx.put_block("agent-ctx", b"ctx-at-v0".to_vec(), DataVersion::At(0)));
        assert_eq!(
            idx.get_block("agent-ctx").as_deref(),
            Some(&b"ctx-at-v0"[..])
        );
        assert!(idx.contains("agent-ctx"));

        // A graph write bumps the data version ⇒ the v0 context is now STALE ⇒ MISS,
        // and the stale entry is retired (bytes freed).
        idx.set_data_version(DataVersion::At(1));
        assert_eq!(idx.get_block("agent-ctx"), None, "stale context is a miss");
        assert!(!idx.contains("agent-ctx"));
        assert_eq!(idx.stats().unique_blocks, 0, "stale entry retired");
        assert_eq!(idx.stats().stale_retired, 1);

        // Re-publish the freshly-derived context at v1 ⇒ served again.
        assert!(idx.put_block("agent-ctx", b"ctx-at-v1".to_vec(), DataVersion::At(1)));
        assert_eq!(
            idx.get_block("agent-ctx").as_deref(),
            Some(&b"ctx-at-v1"[..])
        );
    }

    /// CONCEPT:EG-KG.storage.content-addressed-put — a pure content-addressed (Agnostic) KV page is NEVER invalidated
    /// by a version bump, so the EG-186 dedup win is preserved across graph writes.
    #[test]
    fn eg359_agnostic_pages_survive_version_bumps() {
        let mut idx = SharedKvIndex::new();
        let h = idx.put_by_content(vec![7u8; 128]); // Agnostic pure KV page
        idx.set_data_version(DataVersion::At(0));
        idx.set_data_version(DataVersion::At(42));
        assert!(idx.contains(&h), "pure KV page immune to version bumps");
        assert_eq!(idx.get_block(&h), Some(vec![7u8; 128]));
        assert_eq!(idx.stats().stale_retired, 0, "no pure page retired");
    }

    /// CONCEPT:EG-KG.storage.content-addressed-put — a re-publish of an existing versioned entry at a NEWER version
    /// refreshes its stamp so it is no longer stale (dedup path un-stales on republish).
    #[test]
    fn eg359_reput_refreshes_version_stamp() {
        let mut idx = SharedKvIndex::new();
        idx.set_data_version(DataVersion::At(5));
        idx.put_block("k", b"v".to_vec(), DataVersion::At(3)); // arrives already behind
        assert_eq!(idx.get_block("k"), None, "stale-on-arrival ⇒ miss");
        // Re-publish at the current version ⇒ fresh.
        idx.put_block("k", b"v".to_vec(), DataVersion::At(5));
        assert_eq!(idx.get_block("k").as_deref(), Some(&b"v"[..]));
    }

    // ── snapshot-fork (CONCEPT:EG-KG.memory.zero-copy-snapshot-fork) ─────────────────────────────

    /// CONCEPT:EG-KG.memory.zero-copy-snapshot-fork — a fork branch reads the SAME physical allocation as
    /// the snapshot page (proved by `Arc` pointer identity), NOT a copy.
    #[test]
    fn zerocopy_fork_shares_one_physical_allocation() {
        let mut idx = SharedKvIndex::new();
        let h = idx.put_by_content(vec![7u8; 1024]);
        // The originating entry's page pointer.
        let entry_ptr = Arc::as_ptr(&idx.blocks[&h].data);

        let snap = idx.snapshot(std::slice::from_ref(&h));
        let b1 = idx.fork(snap).unwrap();
        let b2 = idx.fork(snap).unwrap();

        // Both branches hand out an Arc to the very same allocation as the entry.
        let p1 = Arc::as_ptr(&idx.branch_get(b1, &h).unwrap());
        let p2 = Arc::as_ptr(&idx.branch_get(b2, &h).unwrap());
        assert_eq!(
            p1, entry_ptr,
            "branch reads the shared physical page (zero-copy)"
        );
        assert_eq!(p2, entry_ptr, "second branch shares the SAME allocation");
    }

    /// CONCEPT:EG-KG.memory.zero-copy-snapshot-fork — resident fork bytes stay flat at M pages as N branches
    /// fan out (NOT N×M), and a copy-on-write write adds exactly ONE overlay page.
    #[test]
    fn zerocopy_fork_resident_bytes_flat_vs_branch_count() {
        const M: usize = 3;
        const PAGE: usize = 4096;
        let mut idx = SharedKvIndex::new();
        let keys: Vec<String> = (0..M)
            .map(|i| {
                let mut p = vec![0u8; PAGE];
                p[0] = i as u8;
                idx.put_by_content(p)
            })
            .collect();

        let snap = idx.snapshot(&keys);
        let branches: Vec<BranchId> = (0..64).map(|_| idx.fork(snap).unwrap()).collect();

        let fs = idx.fork_stats();
        assert_eq!(fs.branches, 64);
        assert_eq!(fs.shared_pages, M);
        assert_eq!(
            fs.resident_fork_bytes,
            M * PAGE,
            "64 branches, still only M physical pages resident"
        );

        // CoW write on one branch ⇒ +1 overlay page, siblings unchanged.
        idx.branch_put(branches[0], &keys[0], vec![0xFF; PAGE]);
        assert_eq!(idx.branch_get(branches[0], &keys[0]).unwrap()[0], 0xFF);
        assert_eq!(
            idx.branch_get(branches[1], &keys[0]).unwrap()[0],
            0u8,
            "sibling still sees the shared snapshot page (isolation)"
        );
        let fs2 = idx.fork_stats();
        assert_eq!(fs2.overlay_pages, 1, "exactly one CoW page, not 64");
        assert_eq!(fs2.resident_fork_bytes, (M + 1) * PAGE);
    }

    /// CONCEPT:EG-KG.memory.zero-copy-snapshot-fork — release_snapshot undoes the pins and frees pages no
    /// longer referenced, while an explicitly-released originating ref keeps the page alive
    /// only as long as the snapshot pin holds.
    #[test]
    fn snapshot_pin_and_release_refcounts() {
        let mut idx = SharedKvIndex::new();
        let h = idx.put_by_content(vec![1u8; 32]); // refcount 1
        let snap = idx.snapshot(std::slice::from_ref(&h)); // pin ⇒ refcount 2
        assert_eq!(idx.refcount(&h), 2, "snapshot pins the entry");
        // Original holder releases ⇒ refcount 1, still resident thanks to the pin.
        assert_eq!(idx.release(&h), Some(1));
        assert!(idx.contains(&h), "snapshot pin keeps it addressable");
        // Release the snapshot ⇒ last ref gone ⇒ entry freed.
        assert!(idx.release_snapshot(snap));
        assert!(!idx.contains(&h), "entry freed after snapshot release");
    }

    /// CONCEPT:EG-KG.memory.zero-copy-snapshot-fork — unknown-handle paths are graceful (no panic).
    #[test]
    fn fork_unknown_handles_are_graceful() {
        let mut idx = SharedKvIndex::new();
        assert_eq!(idx.fork(SnapshotId(999)), None);
        assert_eq!(idx.branch_get(BranchId(999), "k"), None);
        assert!(!idx.branch_put(BranchId(999), "k", vec![0]));
        assert!(!idx.drop_branch(BranchId(999)));
        assert!(!idx.release_snapshot(SnapshotId(999)));
    }
}
