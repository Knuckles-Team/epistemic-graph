// CONCEPT:EG-KG.storage.bloom-negative-lookup-guard — a lock-free Bloom filter that
// guards the durable read-through NEGATIVE-lookup path (`read_through::ReadThrough`,
// consulted by `GraphCore::read_through_get` on a RAM miss). A blocking redb
// point-read is only worth paying when the key might actually be durable; for a
// genuine absence (a mistyped node id, a dangling edge target, a speculative UQL/
// Cypher probe) the filter answers `false` and the caller skips the I/O entirely.
//
// Sized off the graph's known/expected node cardinality (the registry catalog
// count when known, else a modest default) — see `GraphCore::node_bloom` and its
// call sites in `graph.rs`/`registry.rs`. `insert` is called on every `AddNode`
// (and whenever a full/complete node set is (re)materialized); `might_contain`
// never produces a false NEGATIVE for an inserted key, so gating on it can only
// ever skip I/O for a key that was never inserted — a false POSITIVE just falls
// through to the existing redb point-read unchanged. Correctness is therefore
// unaffected; only negative-path latency improves.
//
// Pure-Rust, no new dependency: hashing is plain `std::hash::Hasher` (SipHash via
// `DefaultHasher`), double-hashed (Kirsch–Mitzenmacher) to derive `num_hashes`
// probe positions from two 64-bit hashes instead of running `num_hashes`
// independent hash functions.

use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};

const WORD_BITS: u64 = 64;

/// A concurrent, insert-only Bloom filter over node ids. `insert`/`might_contain`
/// both take `&self` (bit words are `AtomicU64`), so many `GraphTxn`s can insert
/// concurrently under a shared read guard on the filter's container — see
/// `GraphCore::node_bloom` (an `RwLock<NodeBloomFilter>`; the lock only guards
/// wholesale *replacement* of the filter on a full re-materialization, never a
/// single insert/query).
#[derive(Debug)]
pub struct NodeBloomFilter {
    bits: Vec<AtomicU64>,
    num_bits: u64,
    num_hashes: u32,
}

impl NodeBloomFilter {
    /// Size a filter for `expected_items` keys at roughly `false_positive_rate`
    /// (e.g. `0.01` for ~1%). Degenerates gracefully for `expected_items == 0`
    /// (a minimal filter that simply reports "maybe" more often, never
    /// incorrectly reports "no").
    pub fn new(expected_items: usize, false_positive_rate: f64) -> Self {
        let n = expected_items.max(1) as f64;
        let p = false_positive_rate.clamp(1e-6, 0.5);
        let ln2 = std::f64::consts::LN_2;
        let m_bits = (-(n * p.ln()) / (ln2 * ln2)).ceil().max(64.0);
        let k = ((m_bits / n) * ln2).round().clamp(1.0, 16.0) as u32;
        let num_words = ((m_bits as u64) + WORD_BITS - 1) / WORD_BITS;
        let mut bits = Vec::with_capacity(num_words as usize);
        bits.resize_with(num_words as usize, || AtomicU64::new(0));
        NodeBloomFilter {
            num_bits: num_words * WORD_BITS,
            bits,
            num_hashes: k,
        }
    }

    /// Two independent-enough 64-bit hashes of `key`, combined via double hashing
    /// (`h_i = h1 + i*h2`) below to cheaply derive `num_hashes` probe positions.
    fn hashes(key: &str) -> (u64, u64) {
        let mut h1 = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut h1);
        let a = h1.finish();

        let mut h2 = std::collections::hash_map::DefaultHasher::new();
        0xEBC0DEu64.hash(&mut h2);
        a.hash(&mut h2);
        key.hash(&mut h2);
        let b = h2.finish() | 1; // odd stride so it can't degenerate to 0

        (a, b)
    }

    fn positions(&self, key: &str) -> impl Iterator<Item = u64> + '_ {
        let (a, b) = Self::hashes(key);
        let num_bits = self.num_bits;
        (0..self.num_hashes).map(move |i| a.wrapping_add((i as u64).wrapping_mul(b)) % num_bits)
    }

    /// Record `key` as present. Never removed (classic Bloom filter semantics) —
    /// a subsequent `remove_node` leaves a stale bit, which only ever costs an
    /// extra fall-through redb read for that id, never a wrong answer.
    pub fn insert(&self, key: &str) {
        for pos in self.positions(key) {
            let word = (pos / WORD_BITS) as usize;
            let bit = 1u64 << (pos % WORD_BITS);
            self.bits[word].fetch_or(bit, Ordering::Relaxed);
        }
    }

    /// `false` ⇒ `key` was DEFINITELY never inserted (safe to skip the durable
    /// read). `true` ⇒ `key` might be present (fall through to the real read).
    pub fn might_contain(&self, key: &str) -> bool {
        self.positions(key).all(|pos| {
            let word = (pos / WORD_BITS) as usize;
            let bit = 1u64 << (pos % WORD_BITS);
            self.bits[word].load(Ordering::Relaxed) & bit != 0
        })
    }
}

impl Default for NodeBloomFilter {
    /// A modest default capacity for a freshly created graph with no known
    /// cardinality yet. Call sites that DO know an expected count (a
    /// re-materialization from the registry catalog) build via [`Self::new`]
    /// instead.
    fn default() -> Self {
        Self::new(65_536, 0.01)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inserted_keys_always_might_contain() {
        let filter = NodeBloomFilter::new(1_000, 0.01);
        let keys: Vec<String> = (0..1_000).map(|i| format!("node:{i}")).collect();
        for k in &keys {
            filter.insert(k);
        }
        for k in &keys {
            assert!(
                filter.might_contain(k),
                "no false negatives allowed for an inserted key: {k}"
            );
        }
    }

    #[test]
    fn absent_keys_are_usually_rejected() {
        let filter = NodeBloomFilter::new(1_000, 0.01);
        for i in 0..1_000 {
            filter.insert(&format!("node:{i}"));
        }
        let mut false_positives = 0u32;
        let probes = 5_000;
        for i in 1_000..(1_000 + probes) {
            if filter.might_contain(&format!("node:{i}")) {
                false_positives += 1;
            }
        }
        // Sized for ~1% FP; allow generous slack so the test isn't hash-lucky-flaky.
        assert!(
            false_positives < probes / 10,
            "false-positive rate too high: {false_positives}/{probes}"
        );
    }

    #[test]
    fn empty_filter_rejects_everything() {
        let filter = NodeBloomFilter::new(100, 0.01);
        assert!(!filter.might_contain("anything"));
    }
}
