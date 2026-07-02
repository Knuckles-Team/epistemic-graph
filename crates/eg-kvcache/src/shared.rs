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

/// The backend seam an external vLLM/LMCache connector calls to share KV blocks by
/// content hash (CONCEPT:EG-186).
///
/// Implementors may be in-process ([`SharedKvIndex`]) or remote (an RPC / object-store
/// client — a follow-up). The `hash` is a content address (see [`crate::content_hash`]),
/// so identical blocks DEDUP regardless of which instance produced them.
pub trait SharedKvBackend {
    /// Fetch the block stored under `hash`, if present.
    fn get_block(&self, hash: &str) -> Option<Block>;

    /// Store `block` under `hash`, incrementing its ref-count. Idempotent on content: a
    /// second `put_block` of the SAME hash does NOT re-store the bytes, it just bumps
    /// the ref-count (the dedup guarantee). Returns `true` iff this call created a NEW
    /// entry (the block was not already present).
    fn put_block(&mut self, hash: &str, block: Block) -> bool;

    /// Whether a block exists under `hash` (a cheap probe to skip a redundant upload).
    fn contains(&self, hash: &str) -> bool;
}

/// One content-addressed entry: the shared bytes + how many holders reference it.
struct SharedEntry {
    /// `Arc` so `get_block` hands out the bytes without a deep copy per reader.
    data: Arc<Vec<u8>>,
    refcount: usize,
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
    dedup_hits: u64,
    get_hits: u64,
    get_misses: u64,
}

impl SharedKvIndex {
    /// A fresh, empty shared index.
    pub fn new() -> Self {
        SharedKvIndex::default()
    }

    /// Content-address `block` (compute its hash), store it (dedup + ref-count), and
    /// return the hash — the convenience path when the caller has raw bytes rather than
    /// a precomputed token-hash. Equivalent to `put_block(content_hash(&block), block)`.
    pub fn put_by_content(&mut self, block: Block) -> String {
        let hash = content_hash(&block);
        self.put_block(&hash, block);
        hash
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
        }
    }
}

impl SharedKvBackend for SharedKvIndex {
    fn get_block(&self, hash: &str) -> Option<Block> {
        // NOTE: `&self` — stats are updated by callers that need them; a `&mut` variant
        // could count here. We keep the trait read-only-friendly for remote impls.
        self.blocks.get(hash).map(|e| (*e.data).clone())
    }

    fn put_block(&mut self, hash: &str, block: Block) -> bool {
        if let Some(e) = self.blocks.get_mut(hash) {
            // Dedup: identical content already present — just bump the ref-count.
            e.refcount += 1;
            self.dedup_hits += 1;
            false
        } else {
            self.blocks.insert(
                hash.to_string(),
                SharedEntry {
                    data: Arc::new(block),
                    refcount: 1,
                },
            );
            true
        }
    }

    fn contains(&self, hash: &str) -> bool {
        self.blocks.contains_key(hash)
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
        idx.put_block(&h, vec![1u8; 64]); // second holder
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
            idx.put_block("h", vec![0u8; 8]),
            "first put creates the entry"
        );
        assert!(
            !idx.put_block("h", vec![0u8; 8]),
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
}
