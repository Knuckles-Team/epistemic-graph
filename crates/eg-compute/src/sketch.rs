// CONCEPT:EG-KG.compute.approximate-sketches (W4.5/N5) — pure-Rust, dependency-free
// probabilistic sketches for approximate query processing over unbounded-cardinality data:
//
//   * [`HyperLogLog`] (HLL) — approximate DISTINCT-value cardinality in O(2^precision) memory
//     regardless of how many items are inserted (Flajolet et al. 2007).
//   * [`CountMinSketch`] (CMS) — approximate per-item FREQUENCY in O(width*depth) memory,
//     never underestimating the true count (Cormode & Muthukrishnan 2005).
//   * [`MinHashSketch`] — approximate JACCARD SIMILARITY between two sets from a fixed-size
//     signature, without materializing either set (Broder 1997).
//
// Always-on (no heavy deps — matches `graph_algos`'s "Pure-Rust... Always-on" posture): every
// sketch hashes with the SAME two-salted-`DefaultHasher`-pass Kirsch-Mitzenmacher double-hashing
// idiom `eg_plan::cost::BloomFilter` already established as this codebase's dep-free sketch
// pattern (`std` only — no external hash/rand crate enters the tree).
//
// Consumed from TWO places, which is WHY this lives here rather than in either consumer
// directly: `eg-compute` is the lowest common ancestor of both in the workspace DAG
// (`eg-types → eg-core → eg-compute → eg-query → eg-plan`), so both can depend on it without a
// cycle (eg-query cannot depend on eg-plan, and vice versa is already the live direction).
//
//   * `eg-plan`'s `PlanStats`/`DistinctStats` — an [`HyperLogLog`] per resident column feeds the
//     planner an approximate distinct-value count, sharpening an EQUALITY predicate's
//     selectivity to `1/distinct_count` (see `eg-plan/src/cost.rs`'s `DistinctStats`) — the same
//     kind of real-distribution improvement `ColumnStats` already gave range predicates.
//   * `eg-query`'s SQL surface — `APPROX_DISTINCT`/`APPROX_FREQUENCY`/`MINHASH_SIGNATURE`
//     DataFusion aggregate UDFs wrap these structures directly (see
//     `eg-query/src/sql/sketch_udfs.rs`).

use std::hash::{Hash, Hasher};

/// Two independent 64-bit hashes of `item`, salted so they differ (Kirsch-Mitzenmacher double
/// hashing — mirrors `eg_plan::cost::BloomFilter::hashes`, the established dep-free precedent
/// every sketch below follows instead of pulling an external hash crate).
fn double_hash<T: Hash + ?Sized>(item: &T) -> (u64, u64) {
    let mut h1 = std::collections::hash_map::DefaultHasher::new();
    item.hash(&mut h1);
    let a = h1.finish();
    let mut h2 = std::collections::hash_map::DefaultHasher::new();
    item.hash(&mut h2);
    0x9E37_79B9_7F4A_7C15u64.hash(&mut h2); // salt so h2 != h1
    let b = h2.finish();
    (a, b)
}

/// The `i`-th of `k` derived hashes from a `(h1, h2)` pair: `g_i(x) = h1 + i*h2` — the standard
/// double-hashing simulation of `k` independent hash functions from just two (the same
/// derivation `BloomFilter::bit_index` uses for its `k` probe rounds).
fn nth_hash(h1: u64, h2: u64, i: u64) -> u64 {
    h1.wrapping_add(i.wrapping_mul(h2))
}

// ═══════════════════════════════ HyperLogLog ═══════════════════════════════

/// Default register-index precision. `m = 2^HLL_DEFAULT_PRECISION` registers; standard error ≈
/// `1.04/sqrt(m)`. `14` ⇒ `m = 16384` registers (16 KiB), standard error ≈ 0.81%.
pub const HLL_DEFAULT_PRECISION: u8 = 14;

/// A HyperLogLog cardinality-estimation sketch (Flajolet, Fusy, Gandouet & Meunier 2007, with
/// the small-range linear-counting correction the original paper specifies). Estimates the
/// number of DISTINCT items inserted using a FIXED `2^precision` bytes of memory regardless of
/// how many items — or duplicates — are inserted: the entire point of the structure is that an
/// `APPROX_DISTINCT` aggregate stays O(1)-memory where an exact `COUNT(DISTINCT x)` would need
/// an O(N) hash set.
///
/// Uses a 64-bit hash (not the original paper's 32-bit one), so — matching modern
/// implementations (Redis `PFCOUNT`, Google's HyperLogLog++) — no large-range bias correction is
/// needed: 64 bits of hash space make the collision-driven bias the original paper corrects for
/// at cardinalities near 2^32 unreachable at any realistic scale.
#[derive(Clone, Debug)]
pub struct HyperLogLog {
    precision: u8,
    registers: Vec<u8>,
}

impl HyperLogLog {
    /// A sketch with `2^precision` registers. `precision` is clamped to `4..=18` (16
    /// registers .. 262144 registers): below 4 the bias-correction constants below are
    /// undefined; above 18 spends more memory for accuracy no realistic query-planning or
    /// aggregate use needs (already sub-0.3% standard error at 18).
    pub fn new(precision: u8) -> Self {
        let p = precision.clamp(4, 18);
        Self {
            precision: p,
            registers: vec![0u8; 1usize << p],
        }
    }

    /// Insert one item (anything `Hash`).
    pub fn insert<T: Hash + ?Sized>(&mut self, item: &T) {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        item.hash(&mut h);
        self.insert_hash(h.finish());
    }

    /// Insert a PRE-COMPUTED 64-bit hash directly — lets a caller that already hashed the item
    /// for another purpose (or is streaming hashes from elsewhere) skip re-hashing.
    pub fn insert_hash(&mut self, hash: u64) {
        let p = self.precision as u32;
        let idx = (hash >> (64 - p)) as usize;
        // The remaining (64-p) low bits: register value = 1 + (position of the leftmost 1-bit
        // within JUST those bits) — the classic HLL "run length" observable. Masking first
        // guarantees the top `p` bits of `remaining` are zero, so `leading_zeros() - p + 1`
        // reads off exactly that position (including the all-zero case, which correctly yields
        // the maximal rank `(64-p)+1` with no separate branch: `0u64.leading_zeros() == 64`).
        let remaining = hash & ((1u64 << (64 - p)) - 1);
        let rank = (remaining.leading_zeros() - p + 1) as u8;
        if rank > self.registers[idx] {
            self.registers[idx] = rank;
        }
    }

    /// Number of registers (`2^precision`) — the sketch's fixed memory footprint in bytes.
    pub fn num_registers(&self) -> usize {
        self.registers.len()
    }

    /// The configured precision (introspection / serialization).
    pub fn precision(&self) -> u8 {
        self.precision
    }

    /// The raw register bytes (introspection / serialization — e.g. a DataFusion `Accumulator`
    /// STATE column: `precision() + registers()` round-trips through [`Self::from_raw_registers`]).
    pub fn registers(&self) -> &[u8] {
        &self.registers
    }

    /// Reconstruct a sketch directly from a previously-observed `(precision, registers)` pair —
    /// the inverse of [`Self::precision`]/[`Self::registers`]. `registers.len()` must equal
    /// `2^precision`; a mismatched length is corrected by truncating/zero-padding rather than
    /// panicking (a defensive fallback for a corrupt/foreign STATE payload — the sketch stays
    /// USABLE, just re-cold on the padded registers, rather than crashing the query).
    pub fn from_raw_registers(precision: u8, mut registers: Vec<u8>) -> Self {
        let p = precision.clamp(4, 18);
        registers.resize(1usize << p, 0);
        Self {
            precision: p,
            registers,
        }
    }

    /// Merge another sketch of the SAME precision into this one (register-wise max), producing
    /// the sketch of the UNION of both inserted sets — the operation that makes HLL usable as a
    /// DataFusion `Accumulator`'s multi-partition merge state. A precision mismatch is a no-op
    /// (a caller bug the type system doesn't prevent); compare `num_registers()` first to detect
    /// one.
    pub fn merge(&mut self, other: &HyperLogLog) {
        if self.precision != other.precision {
            return;
        }
        for (a, b) in self.registers.iter_mut().zip(&other.registers) {
            if *b > *a {
                *a = *b;
            }
        }
    }

    /// The bias-corrected cardinality estimate: Flajolet's raw harmonic-mean estimator, with the
    /// small-range linear-counting correction substituted whenever the raw estimate falls in the
    /// range where it is known to be biased (`<= 2.5m` with at least one still-empty register).
    pub fn estimate(&self) -> f64 {
        let m = self.registers.len() as f64;
        let sum_inv: f64 = self.registers.iter().map(|&r| 2f64.powi(-(r as i32))).sum();
        let raw = alpha(self.registers.len()) * m * m / sum_inv;

        let zeros = self.registers.iter().filter(|&&r| r == 0).count();
        if raw <= 2.5 * m && zeros > 0 {
            // Linear counting: E = m * ln(m/V), V = count of still-empty registers.
            m * (m / zeros as f64).ln()
        } else {
            raw
        }
    }
}

impl Default for HyperLogLog {
    /// A sketch at [`HLL_DEFAULT_PRECISION`] — the standard/recommended sizing or an ad hoc use
    /// that doesn't need a specific memory/accuracy trade-off.
    fn default() -> Self {
        Self::new(HLL_DEFAULT_PRECISION)
    }
}

/// The bias-correction constant `alpha_m` (Flajolet et al. 2007, §4). Depends only on `m`
/// (register count); the three small-`m` special cases match the original paper exactly, and
/// `m >= 128` uses its asymptotic closed form (every precision this module actually constructs
/// — `4..=18` ⇒ `m in 16..=262144` — is covered).
fn alpha(m: usize) -> f64 {
    match m {
        16 => 0.673,
        32 => 0.697,
        64 => 0.709,
        _ => 0.7213 / (1.0 + 1.079 / m as f64),
    }
}

// ═══════════════════════════════ Count-Min Sketch ═══════════════════════════════

/// A Count-Min Sketch (Cormode & Muthukrishnan 2005): approximate per-item FREQUENCY in
/// `O(width*depth)` counters. `estimate(item)` NEVER underestimates the true count (hash
/// collisions can only inflate a shared counter, never deflate it) — the sketch's core
/// guarantee, which is what makes "min across `depth` independent rows" a sound estimator: any
/// row NOT collided by another heavy item gives the exact count, and the minimum picks that row.
#[derive(Clone, Debug)]
pub struct CountMinSketch {
    width: usize,
    depth: usize,
    counters: Vec<Vec<u32>>,
}

impl CountMinSketch {
    /// An explicit `width x depth` counter matrix.
    pub fn new(width: usize, depth: usize) -> Self {
        let width = width.max(1);
        let depth = depth.max(1);
        Self {
            width,
            depth,
            counters: vec![vec![0u32; width]; depth],
        }
    }

    /// Size for a target `(epsilon, delta)` guarantee: with probability `>= 1-delta`, no
    /// estimate overshoots the true count by more than `epsilon * N` (`N` = total items
    /// inserted, counting repeats). Standard CMS sizing: `width = ceil(e/epsilon)`, `depth =
    /// ceil(ln(1/delta))` (`e` = Euler's number, from the sketch's Markov-inequality bound —
    /// mirrors `BloomFilter::new`'s analogous `(expected_items, fp_rate)` convenience
    /// constructor for the same "give the accuracy target, not raw dimensions" ergonomics).
    pub fn with_error_rate(epsilon: f64, delta: f64) -> Self {
        let epsilon = epsilon.clamp(1e-6, 1.0);
        let delta = delta.clamp(1e-9, 0.5);
        let width = (std::f64::consts::E / epsilon).ceil().max(1.0) as usize;
        let depth = (1.0 / delta).ln().ceil().max(1.0) as usize;
        Self::new(width, depth)
    }

    /// Record one occurrence of `item`.
    pub fn insert<T: Hash + ?Sized>(&mut self, item: &T) {
        self.insert_n(item, 1);
    }

    /// Record `count` occurrences of `item` in one call (a pre-aggregated batch update).
    pub fn insert_n<T: Hash + ?Sized>(&mut self, item: &T, count: u32) {
        let (h1, h2) = double_hash(item);
        for d in 0..self.depth {
            let idx = (nth_hash(h1, h2, d as u64) as usize) % self.width;
            self.counters[d][idx] = self.counters[d][idx].saturating_add(count);
        }
    }

    /// Estimated occurrence count of `item` — the minimum counter across all `depth` rows, so
    /// `estimate(item) >= true_count(item)` ALWAYS (never an underestimate).
    pub fn estimate<T: Hash + ?Sized>(&self, item: &T) -> u32 {
        let (h1, h2) = double_hash(item);
        (0..self.depth)
            .map(|d| {
                let idx = (nth_hash(h1, h2, d as u64) as usize) % self.width;
                self.counters[d][idx]
            })
            .min()
            .unwrap_or(0)
    }

    /// Merge another sketch of the SAME dimensions into this one (element-wise counter sum) —
    /// the union of both sketches' observed streams. A dimension mismatch is a no-op (a caller
    /// bug the type system doesn't prevent).
    pub fn merge(&mut self, other: &CountMinSketch) {
        if self.width != other.width || self.depth != other.depth {
            return;
        }
        for (row_a, row_b) in self.counters.iter_mut().zip(&other.counters) {
            for (a, b) in row_a.iter_mut().zip(row_b) {
                *a = a.saturating_add(*b);
            }
        }
    }

    /// `(width, depth)` — the sketch's fixed dimensions (introspection / tests).
    pub fn dims(&self) -> (usize, usize) {
        (self.width, self.depth)
    }

    /// The raw `depth x width` counter matrix (introspection / serialization — e.g. a
    /// DataFusion `Accumulator` STATE column, round-tripped through [`Self::from_raw`]).
    pub fn counters(&self) -> &[Vec<u32>] {
        &self.counters
    }

    /// Reconstruct a sketch directly from a previously-observed `(width, depth, counters)`
    /// triple — the inverse of [`Self::dims`]/[`Self::counters`]. A `counters` shape that
    /// doesn't match `depth x width` is corrected (rows/columns truncated or zero-padded) rather
    /// than panicking, mirroring [`HyperLogLog::from_raw_registers`]'s defensive posture for a
    /// corrupt/foreign STATE payload.
    pub fn from_raw(width: usize, depth: usize, mut counters: Vec<Vec<u32>>) -> Self {
        let width = width.max(1);
        let depth = depth.max(1);
        counters.resize(depth, vec![0u32; width]);
        for row in &mut counters {
            row.resize(width, 0);
        }
        Self {
            width,
            depth,
            counters,
        }
    }
}

// ═══════════════════════════════ MinHash ═══════════════════════════════

/// A MinHash sketch (Broder 1997): a fixed-size SIGNATURE from which the JACCARD similarity
/// `|A∩B|/|A∪B|` of two sets can be estimated without ever materializing either set — insert
/// every element of a set into a sketch, then compare two sketches' signatures.
#[derive(Clone, Debug)]
pub struct MinHashSketch {
    mins: Vec<u64>,
}

impl MinHashSketch {
    /// A sketch with `num_hashes` independent hash "permutations" (signature length). More
    /// hashes ⇒ lower similarity-estimate variance (`≈ sqrt(J(1-J)/num_hashes)`) at the cost of
    /// a larger signature.
    pub fn new(num_hashes: usize) -> Self {
        let k = num_hashes.max(1);
        Self {
            mins: vec![u64::MAX; k],
        }
    }

    /// Insert one element of the set this sketch summarizes.
    pub fn insert<T: Hash + ?Sized>(&mut self, item: &T) {
        let (h1, h2) = double_hash(item);
        for (i, slot) in self.mins.iter_mut().enumerate() {
            let h = nth_hash(h1, h2, i as u64);
            if h < *slot {
                *slot = h;
            }
        }
    }

    /// The signature (the `num_hashes` running minima) — the compact representation compared
    /// for similarity, and what a `MINHASH_SIGNATURE` aggregate returns.
    pub fn signature(&self) -> &[u64] {
        &self.mins
    }

    /// Build a sketch directly from an already-computed signature (e.g. one read back from a
    /// `MINHASH_SIGNATURE` aggregate result) — the inverse of [`Self::signature`], for comparing
    /// two previously-computed signatures without re-inserting either set's elements.
    pub fn from_signature(signature: Vec<u64>) -> Self {
        Self { mins: signature }
    }

    /// Estimated Jaccard similarity between the two sets these sketches summarize: the fraction
    /// of signature positions that agree (the MinHash estimator — Broder 1997; `E[agree] =
    /// J(A,B)` because a random permutation's minimum lands on a shared element with probability
    /// exactly `|A∩B|/|A∪B|`). `0.0` when the signatures have different lengths (a caller bug
    /// the type system doesn't prevent) or both are empty.
    pub fn jaccard(&self, other: &MinHashSketch) -> f64 {
        if self.mins.len() != other.mins.len() || self.mins.is_empty() {
            return 0.0;
        }
        let agree = self
            .mins
            .iter()
            .zip(&other.mins)
            .filter(|(a, b)| a == b)
            .count();
        agree as f64 / self.mins.len() as f64
    }

    /// Merge another sketch of the SAME signature length into this one (position-wise min),
    /// producing the sketch of the UNION of both summarized sets. A length mismatch is a no-op.
    pub fn merge(&mut self, other: &MinHashSketch) {
        if self.mins.len() != other.mins.len() {
            return;
        }
        for (a, b) in self.mins.iter_mut().zip(&other.mins) {
            if *b < *a {
                *a = *b;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── HyperLogLog ──────────────────────────────────────────────────────────────

    #[test]
    fn hll_empty_estimates_zero() {
        let h = HyperLogLog::new(HLL_DEFAULT_PRECISION);
        // An all-empty register set: raw estimate is huge (sum_inv = m), zeros = m, so linear
        // counting kicks in: m * ln(m/m) = m * ln(1) = 0.
        assert_eq!(h.estimate(), 0.0);
    }

    #[test]
    fn hll_small_known_cardinality_within_bound() {
        let mut h = HyperLogLog::new(HLL_DEFAULT_PRECISION);
        let n = 1_000usize;
        for i in 0..n {
            h.insert(&i);
        }
        let est = h.estimate();
        // Small-N is dominated by linear-counting variance, not the asymptotic 1.04/sqrt(m)
        // bound — a generous 15% relative-error allowance keeps this non-flaky while still
        // catching a badly broken estimator (e.g. off by 2x, or stuck at 0).
        let rel_err = (est - n as f64).abs() / n as f64;
        assert!(
            rel_err < 0.15,
            "n={n} est={est} rel_err={rel_err} (expected < 0.15)"
        );
    }

    #[test]
    fn hll_duplicates_do_not_inflate_estimate() {
        let mut h = HyperLogLog::new(HLL_DEFAULT_PRECISION);
        for _ in 0..1000 {
            h.insert(&"the-same-item");
        }
        let est = h.estimate();
        assert!(
            est < 3.0,
            "1000 inserts of ONE distinct item must estimate near 1, got {est}"
        );
    }

    #[test]
    fn hll_merge_is_union_cardinality() {
        let mut a = HyperLogLog::new(HLL_DEFAULT_PRECISION);
        let mut b = HyperLogLog::new(HLL_DEFAULT_PRECISION);
        for i in 0..5_000 {
            a.insert(&i);
        }
        for i in 4_000..9_000 {
            // overlaps a on [4000,5000)
            b.insert(&i);
        }
        a.merge(&b);
        let est = a.estimate();
        // True union cardinality = |[0,9000)| = 9000.
        let rel_err = (est - 9_000.0).abs() / 9_000.0;
        assert!(rel_err < 0.1, "merged est={est} rel_err={rel_err}");
    }

    #[test]
    fn hll_mismatched_precision_merge_is_noop() {
        let mut a = HyperLogLog::new(10);
        let b = HyperLogLog::new(12);
        let before = a.estimate();
        a.merge(&b);
        assert_eq!(a.estimate(), before, "precision mismatch must not merge");
    }

    #[test]
    fn hll_raw_registers_roundtrip() {
        let mut a = HyperLogLog::new(12);
        for i in 0..2_000 {
            a.insert(&i);
        }
        let (p, regs) = (a.precision(), a.registers().to_vec());
        let b = HyperLogLog::from_raw_registers(p, regs);
        assert_eq!(a.estimate(), b.estimate());
        assert_eq!(a.num_registers(), b.num_registers());
    }

    /// CONCEPT:EG-KG.query.approx-distinct-cardinality (W4.5/N5) — the ACCEPTANCE-CRITICAL
    /// check: a KNOWN-cardinality fixture of 10,000,000 DISTINCT values, asserting the estimate
    /// is within the sketch's OWN theoretical standard-error bound (not an arbitrary constant),
    /// with a modest safety multiplier against statistical noise so the test is not flaky.
    #[test]
    fn hll_10m_known_cardinality_within_error_bound() {
        const N: u64 = 10_000_000;
        let mut h = HyperLogLog::new(HLL_DEFAULT_PRECISION);
        for i in 0..N {
            h.insert_hash(splitmix64(i));
        }
        let est = h.estimate();

        let m = h.num_registers() as f64;
        let std_error = 1.04 / m.sqrt(); // HLL's own published relative standard error.
                                         // 6 standard errors is an extremely low false-fail probability (~2e-9 under normality)
                                         // while still being a REAL, theory-grounded bound — not a vacuous one.
        let allowed_rel_err = std_error * 6.0;
        let rel_err = (est - N as f64).abs() / N as f64;
        assert!(
            rel_err <= allowed_rel_err,
            "N={N} est={est} rel_err={rel_err:.5} allowed={allowed_rel_err:.5} (std_error={std_error:.5}, precision={})",
            HLL_DEFAULT_PRECISION
        );
    }

    /// A fast, well-distributed 64-bit mixer (Sebastiano Vigna's SplitMix64) used ONLY to spread
    /// the 10M test's sequential `0..N` indices into hash-like values before feeding
    /// `insert_hash` directly (skipping the `Hash`-trait + `DefaultHasher` round-trip per
    /// element keeps the 10M-row test fast — a few seconds instead of tens — while remaining a
    /// legitimate stand-in: `HyperLogLog::insert<T>` itself just does that same
    /// hash-then-`insert_hash` sequence, unit-tested separately above).
    fn splitmix64(mut x: u64) -> u64 {
        x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = x;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    // ── CountMinSketch ───────────────────────────────────────────────────────────

    #[test]
    fn cms_exact_when_no_collision_pressure() {
        let mut c = CountMinSketch::new(2048, 4);
        for _ in 0..37 {
            c.insert(&"a");
        }
        for _ in 0..5 {
            c.insert(&"b");
        }
        assert_eq!(c.estimate(&"a"), 37);
        assert_eq!(c.estimate(&"b"), 5);
        assert_eq!(c.estimate(&"never-inserted"), 0);
    }

    #[test]
    fn cms_never_underestimates() {
        // Deliberately tiny width to force heavy hash collisions, then verify the CORE
        // guarantee still holds: estimate is NEVER less than the true count, for every item.
        let mut c = CountMinSketch::new(4, 3);
        let mut truth: std::collections::HashMap<u32, u32> = std::collections::HashMap::new();
        for item in 0u32..200 {
            let reps = (item % 7) + 1;
            for _ in 0..reps {
                c.insert(&item);
            }
            *truth.entry(item).or_insert(0) += reps;
        }
        for (item, &true_count) in &truth {
            let est = c.estimate(item);
            assert!(
                est >= true_count,
                "item={item} true={true_count} est={est} — CMS must never underestimate"
            );
        }
    }

    #[test]
    fn cms_bounded_overestimate_at_target_error_rate() {
        // A heavy hitter's estimate must stay within epsilon*N of its true count, at the sized
        // (epsilon, delta) guarantee — the sketch's headline accuracy contract.
        let epsilon = 0.01;
        let mut c = CountMinSketch::with_error_rate(epsilon, 0.01);
        let total: u32 = 50_000;
        for i in 0..total {
            c.insert(&(i % 500)); // 500 distinct items, uniformly repeated.
        }
        let est = c.estimate(&0u32);
        let true_count = total / 500;
        let allowed = true_count as f64 + epsilon * total as f64;
        assert!(
            (est as f64) <= allowed,
            "est={est} true={true_count} allowed<= {allowed} (epsilon={epsilon}, N={total})"
        );
        assert!(est >= true_count, "must never underestimate");
    }

    #[test]
    fn cms_merge_sums_counts() {
        let mut a = CountMinSketch::new(1024, 4);
        let mut b = CountMinSketch::new(1024, 4);
        for _ in 0..10 {
            a.insert(&"x");
        }
        for _ in 0..7 {
            b.insert(&"x");
        }
        a.merge(&b);
        assert_eq!(a.estimate(&"x"), 17);
    }

    #[test]
    fn cms_dimension_mismatch_merge_is_noop() {
        let mut a = CountMinSketch::new(64, 3);
        let b = CountMinSketch::new(128, 3);
        a.insert(&"x");
        let before = a.estimate(&"x");
        a.merge(&b);
        assert_eq!(a.estimate(&"x"), before);
    }

    #[test]
    fn cms_raw_counters_roundtrip() {
        let mut a = CountMinSketch::new(256, 4);
        for _ in 0..12 {
            a.insert(&"y");
        }
        let (w, d) = a.dims();
        let counters = a.counters().to_vec();
        let b = CountMinSketch::from_raw(w, d, counters);
        assert_eq!(a.estimate(&"y"), b.estimate(&"y"));
        assert_eq!(a.dims(), b.dims());
    }

    // ── MinHash ──────────────────────────────────────────────────────────────────

    #[test]
    fn minhash_identical_sets_similarity_one() {
        let mut a = MinHashSketch::new(128);
        let mut b = MinHashSketch::new(128);
        for i in 0..500 {
            a.insert(&i);
            b.insert(&i);
        }
        assert_eq!(a.jaccard(&b), 1.0);
    }

    #[test]
    fn minhash_disjoint_sets_similarity_near_zero() {
        let mut a = MinHashSketch::new(256);
        let mut b = MinHashSketch::new(256);
        for i in 0..500 {
            a.insert(&i);
        }
        for i in 500..1000 {
            b.insert(&i);
        }
        let j = a.jaccard(&b);
        assert!(j < 0.1, "disjoint sets should estimate near-zero, got {j}");
    }

    #[test]
    fn minhash_known_overlap_within_tolerance() {
        // A = [0,100), B = [50,150): intersection = [50,100) = 50, union = [0,150) = 150.
        // True Jaccard = 50/150 = 0.3333...
        let k = 512;
        let mut a = MinHashSketch::new(k);
        let mut b = MinHashSketch::new(k);
        for i in 0..100 {
            a.insert(&i);
        }
        for i in 50..150 {
            b.insert(&i);
        }
        let est = a.jaccard(&b);
        let truth = 50.0 / 150.0;
        // Standard MinHash estimator variance ~ sqrt(J(1-J)/k) ≈ sqrt(0.333*0.667/512) ≈ 0.021;
        // a 4-sigma-ish 0.09 absolute tolerance keeps this non-flaky.
        assert!(
            (est - truth).abs() < 0.09,
            "est={est} truth={truth} (k={k})"
        );
    }

    #[test]
    fn minhash_signature_roundtrip() {
        let mut a = MinHashSketch::new(64);
        for i in 0..30 {
            a.insert(&i);
        }
        let sig = a.signature().to_vec();
        let b = MinHashSketch::from_signature(sig.clone());
        assert_eq!(
            a.jaccard(&b),
            1.0,
            "a sketch built FROM its own signature must be identical"
        );
        assert_eq!(b.signature(), sig.as_slice());
    }

    #[test]
    fn minhash_mismatched_length_jaccard_is_zero() {
        let a = MinHashSketch::new(32);
        let b = MinHashSketch::new(64);
        assert_eq!(a.jaccard(&b), 0.0);
    }

    #[test]
    fn minhash_merge_is_union() {
        let mut a = MinHashSketch::new(64);
        let mut b = MinHashSketch::new(64);
        let mut union = MinHashSketch::new(64);
        for i in 0..50 {
            a.insert(&i);
            union.insert(&i);
        }
        for i in 40..90 {
            b.insert(&i);
            union.insert(&i);
        }
        a.merge(&b);
        assert_eq!(a.signature(), union.signature());
    }
}
