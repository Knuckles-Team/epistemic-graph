//! # eg-kvcache — tiered + shared KV-cache store (CONCEPT:EG-KG.memory.byte-bounded-tiers / CONCEPT:EG-KG.enrichment.content-address-separation)
//!
//! A pure-Rust, Pi-lean leaf crate (a sibling of `eg-ann` / `eg-tensor`) that gives the
//! engine the substrate behind the vLLM / LMCache **shared-KV / OOM-offload** ask:
//!
//! * **[`TieredCache`] (CONCEPT:EG-KG.memory.byte-bounded-tiers)** — a three-tier (HOT raw-RAM / WARM
//!   compressed-RAM / COLD durable-blob) byte-budgeted, importance-scored cache for
//!   KV-cache blocks. Under memory pressure blocks are *demoted* down the tiers
//!   (their bytes are MOVED, not dropped) instead of the process OOM-ing; on access
//!   they are *promoted* back to HOT. [`TieredCache::pin`] keeps a hot working set
//!   (e.g. a shared prompt prefix) resident. The COLD tier is pluggable
//!   ([`cold::ColdStore`]): an in-RAM store by default, or a durable redb store behind
//!   the `durable` feature so offloaded bytes survive an OOM / restart.
//!
//! * **[`SharedKvBackend`] + [`SharedKvIndex`] (CONCEPT:EG-KG.enrichment.content-address-separation)** — a content-addressed,
//!   ref-counted backend so parallel instances SHARE KV blocks by token-hash: an
//!   identical page produced by two workers is stored once (dedup) and a cold worker can
//!   fetch a page a warm worker already computed. This is the seam an external
//!   vLLM/LMCache connector calls.
//!
//! * **Zero-copy snapshot-fork (CONCEPT:EG-KG.memory.zero-copy-snapshot-fork)** — the
//!   "LMCacheMPConnector snapshot → branch" primitive on [`SharedKvIndex`]:
//!   [`snapshot`](SharedKvIndex::snapshot) pins a set of shared pages, then
//!   [`fork`](SharedKvIndex::fork) fans out N branches that all read those SAME physical
//!   pages by `Arc` (ONE copy, regardless of branch count), copy-on-write only when a
//!   branch [`branch_put`](SharedKvIndex::branch_put)s. This makes warm-fork fan-out O(1)
//!   in copies — the zero-copy rung the agent-utilities `crossmodal_fork` `max_concurrency>1`
//!   path targets, replacing per-branch page COPIES with pointer bumps.
//!
//! ## Dependency posture (the Pi contract)
//!
//! The default build links NOTHING — the content hash (FNV-1a-128), the WARM compressor
//! (RLE + raw fallback) and the tiered LRU are all hand-written. Two optional features
//! pull a dependency: the durable COLD tier (`durable`) pulls `redb` (reusing the
//! engine's KG-2.195 durability tier), and a REAL WARM codec (CONCEPT:EG-KG.storage.rle-codec-default,
//! `compression` → `zstd`; optional `lz4` → `lz4_flex`) is selectable via
//! [`TieredCache::with_warm_codec`]. Both are OFF by default, so a `pi`/default build
//! carries no redb and no zstd — the base build stays lean.
//!
//! ## Not yet wired (explicit follow-ups)
//!
//! A NETWORKED [`SharedKvBackend`] (RPC / object-store), the concrete vLLM/LMCache
//! connector, and the graph-os server endpoint that exposes these types are deliberately
//! left as follow-ups — this crate is the pure engine-side substrate only.

pub mod cold;
pub mod compress;
pub mod hash;
pub mod shared;
pub mod tiered;
pub mod value;
pub mod version;

// ModalityContract retrofit (CONCEPT:E4): `impl ModalityContract for StoredBlock`,
// behind the crate's own opt-in `contract` feature (default OFF). See `src/contract.rs`.
#[cfg(feature = "contract")]
mod contract;

pub use compress::{Codec, StoredBlock};
pub use hash::content_hash;
pub use shared::{BranchId, ForkStats, SharedKvBackend, SharedKvIndex, SharedStats, SnapshotId};
pub use tiered::{CacheStats, Tier, TieredCache};
pub use value::{Block, CacheValue};
pub use version::DataVersion;

#[cfg(feature = "durable")]
pub use cold::{ColdKey, RedbColdStore};
pub use cold::{ColdStore, MemoryColdStore};

#[cfg(test)]
mod integration_tests {
    use super::*;

    /// CONCEPT:EG-KG.memory.byte-bounded-tiers + EG-186 — the end-to-end story: a shared backend dedups KV pages
    /// across "instances" while a local tiered cache offloads its working set under
    /// pressure, and every page remains recoverable from one side or the other.
    #[test]
    fn eg185_eg186_shared_dedup_plus_local_tiering_endtoend() {
        // Shared backend: two instances contribute the same prefix page.
        let mut shared = SharedKvIndex::new();
        let prefix: Block = vec![42u8; 256];
        let h_a = shared.put_by_content(prefix.clone());
        let h_b = shared.put_by_content(prefix.clone());
        assert_eq!(h_a, h_b);
        assert_eq!(
            shared.stats().unique_blocks,
            1,
            "EG-186: shared prefix stored once"
        );

        // A worker's local tiered cache: pin the shared prefix, then flood it.
        let mut local: TieredCache<String, Block> = TieredCache::new(300, 300, 1_000_000);
        local.put(h_a.clone(), shared.get_block(&h_a).unwrap());
        local.pin(&h_a);
        for i in 0..64u32 {
            local.put(format!("blk-{i}"), vec![i as u8; 100]);
        }
        // EG-185: pinned shared prefix never evicted; other blocks offloaded not lost.
        assert_eq!(local.tier_of(&h_a), Some(Tier::Hot));
        assert_eq!(local.get(&h_a), Some(prefix));
        assert_eq!(local.stats().drops, 0, "generous COLD ⇒ nothing dropped");
    }
}
