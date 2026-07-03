//! The shared multi-instance KV-cache backend (CONCEPT:EG-186).
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
/// content hash (CONCEPT:EG-186), with data-version invalidation layered on top
/// (CONCEPT:EG-364).
///
/// Implementors may be in-process ([`SharedKvIndex`]) or remote (an RPC / object-store
/// client — a follow-up). The `hash` is a content address (see [`crate::content_hash`]),
/// so identical blocks DEDUP regardless of which instance produced them. Each entry ALSO
/// carries the [`DataVersion`] it was derived at; once the store's current version moves
/// past it, the entry is a MISS (stale LLM/agent context is never served).
pub trait SharedKvBackend {
    /// Fetch the block stored under `hash`, if present AND still fresh for the store's
    /// current [`DataVersion`] (CONCEPT:EG-364). A stale entry (derived at an older
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
    /// redundant upload. A stale entry reads as absent (CONCEPT:EG-364).
    fn contains(&self, hash: &str) -> bool;

    /// Advance the store's current data version (CONCEPT:EG-364) — the hook a graph
    /// write drives (a committed write bumps `GraphCore::version()`, KG-2.180, then calls
    /// this). EAGERLY retires every version-tagged entry that is now stale (freeing its
    /// bytes + ref-counts), mirroring `ResultCache`'s atomic version-bump retire
    /// (KG-2.233). [`DataVersion::Agnostic`] entries (pure KV pages) are untouched.
    fn set_data_version(&mut self, version: DataVersion);
}

/// One content-addressed entry: the shared bytes, how many holders reference it, and the
/// [`DataVersion`] it was derived at (CONCEPT:EG-364).
struct SharedEntry {
    /// `Arc` so `get_block` hands out the bytes without a deep copy per reader.
    data: Arc<Vec<u8>>,
    refcount: usize,
    /// The data version this entry was derived at; [`DataVersion::Agnostic`] for a pure
    /// content-addressed page that is never version-invalidated.
    version: DataVersion,
}

/// Occupancy / dedup statistics for a [`SharedKvIndex`] (CONCEPT:EG-186).
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
    /// (CONCEPT:EG-364) — the invalidation proof counter a test reads.
    pub stale_retired: u64,
}

impl SharedStats {
    /// Bytes SAVED by dedup (`logical_bytes - resident_bytes`).
    pub fn dedup_savings(&self) -> usize {
        self.logical_bytes.saturating_sub(self.resident_bytes)
    }
}

/// An in-process, content-addressed, ref-counted shared KV block store (CONCEPT:EG-186).
///
/// The canonical [`SharedKvBackend`] implementation: identical blocks are stored ONCE
/// and ref-counted so they are freed only when the last holder [`release`](Self::release)s
/// them.
#[derive(Default)]
pub struct SharedKvIndex {
    blocks: HashMap<String, SharedEntry>,
    /// The store's current data version (CONCEPT:EG-364). Defaults to
    /// [`DataVersion::Agnostic`] — version-tracking is INACTIVE until a
    /// [`SharedKvIndex::set_data_version`] call, so a store used purely for
    /// content-addressed dedup (EG-186) behaves exactly as before (zero-cost / opt-in).
    current_version: DataVersion,
    dedup_hits: u64,
    get_hits: u64,
    get_misses: u64,
    stale_retired: u64,
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

    /// The store's current data version (CONCEPT:EG-364).
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
}

impl SharedKvBackend for SharedKvIndex {
    fn get_block(&self, hash: &str) -> Option<Block> {
        // NOTE: `&self` — stats are updated by callers that need them; a `&mut` variant
        // could count here. We keep the trait read-only-friendly for remote impls.
        // Freshness gate (CONCEPT:EG-364): a stale entry (derived at an older version)
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

    /// CONCEPT:EG-186 — two instances putting the SAME block content dedup to one
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

    /// CONCEPT:EG-186 — ref-counting frees a block only when the LAST holder releases.
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

    /// CONCEPT:EG-186 — get_block returns stored bytes and misses on an unknown hash.
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

    /// CONCEPT:EG-186 — put_block reports whether it created a NEW entry (upload-skip
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

    /// CONCEPT:EG-186 — different content lands under different addresses (no false dedup).
    #[test]
    fn eg186_distinct_content_distinct_address() {
        let mut idx = SharedKvIndex::new();
        let a = idx.put_by_content(vec![1u8; 32]);
        let b = idx.put_by_content(vec![2u8; 32]);
        assert_ne!(a, b);
        assert_eq!(idx.stats().unique_blocks, 2);
    }

    /// CONCEPT:EG-364 — a versioned (derived-context) entry is served while the data
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

    /// CONCEPT:EG-364 — a pure content-addressed (Agnostic) KV page is NEVER invalidated
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

    /// CONCEPT:EG-364 — a re-publish of an existing versioned entry at a NEWER version
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
}
