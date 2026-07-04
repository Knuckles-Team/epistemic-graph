//! The byte-block value model for the tiered cache (CONCEPT:EG-KG.memory.byte-bounded-tiers).
//!
//! A [`TieredCache`](crate::TieredCache) value is a **byte block** (a KV-cache page: a
//! contiguous run of attention key/value bytes for a token span). To move a value
//! DOWN a tier the cache must render it to bytes (to compress it for WARM, or to write
//! it to the durable COLD tier), and to promote it back UP it must rebuild the value
//! from those bytes. [`CacheValue`] is that seam.
//!
//! It is implemented for the canonical byte-block type [`Block`] (`Vec<u8>`), so the
//! common case — `TieredCache<K, Block>` — needs no work from the caller; richer value
//! types can implement it to participate in tiering.

use std::borrow::Cow;

/// The canonical KV-cache byte block: a page of contiguous key/value bytes.
pub type Block = Vec<u8>;

/// A cache value that can be rendered to / rebuilt from bytes so the tiered cache can
/// compress it (WARM) and offload it (COLD), per CONCEPT:EG-KG.memory.byte-bounded-tiers.
pub trait CacheValue: Clone {
    /// The value as a byte block. `Cow` so the common `Vec<u8>` case is zero-copy.
    fn as_bytes(&self) -> Cow<'_, [u8]>;

    /// Rebuild the value from a byte block (the inverse of [`as_bytes`](Self::as_bytes)),
    /// used when a block is promoted back into the HOT tier from WARM/COLD.
    fn from_bytes(bytes: &[u8]) -> Self;

    /// The value's size in bytes — the unit ALL capacity accounting is done in
    /// (CONCEPT:EG-KG.memory.byte-bounded-tiers: tiers are bounded by bytes, not entry count).
    fn byte_len(&self) -> usize {
        self.as_bytes().len()
    }
}

impl CacheValue for Vec<u8> {
    fn as_bytes(&self) -> Cow<'_, [u8]> {
        Cow::Borrowed(self)
    }
    fn from_bytes(bytes: &[u8]) -> Self {
        bytes.to_vec()
    }
    fn byte_len(&self) -> usize {
        self.len()
    }
}

impl CacheValue for String {
    fn as_bytes(&self) -> Cow<'_, [u8]> {
        Cow::Borrowed(str::as_bytes(self))
    }
    fn from_bytes(bytes: &[u8]) -> Self {
        String::from_utf8_lossy(bytes).into_owned()
    }
}
