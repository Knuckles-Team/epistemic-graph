//! Content-defined chunking (CONCEPT:EG-071).
//!
//! A hand-written **Gear/FastCDC** rolling-hash chunker that replaces the fixed
//! 2 MiB stride splitter. Boundaries are chosen by the BYTES, not by absolute
//! position, so inserting/removing data near the start of a blob re-synchronises
//! the chunk stream within ~one chunk and every chunk past the edit keeps its
//! original digest — the property that makes the sha256 CAS dedup + refcount GC
//! (see [`super::store`]) actually deduplicate edited copies. Fixed striding
//! shifted every downstream byte and defeated dedup; CDC does not.
//!
//! ## The algorithm (FastCDC, normalized chunking)
//!
//! A Gear rolling hash folds each byte into a 64-bit fingerprint:
//! `hash = (hash << 1) + GEAR[byte]`. The left shift gives the hash an intrinsic
//! **bounded window**: a byte's contribution is shifted entirely out of the 64-bit
//! word after 64 bytes, so the cut decision at any position depends only on the
//! preceding ≤64 bytes — this is the source of shift-resistance. A cut is taken at
//! the first position where `(hash & mask) == 0`.
//!
//! *Normalized chunking* uses TWO masks around the target average to tighten the
//! size distribution: in `[min, avg)` a stricter mask (`mask_s`, more bits → harder
//! to satisfy) suppresses sub-average cuts, and in `[avg, max)` a looser mask
//! (`mask_l`, fewer bits → easier to satisfy) forces a cut before the chunk grows
//! too large. Hard bounds `min`/`max` cap the tails. The fingerprint is reset to 0
//! at the start of each chunk (per-chunk, exactly as FastCDC does); re-sync after an
//! edit happens at the chunk level once a boundary again lands on original content.
//!
//! No external dependency: the gear table is generated at compile time.

/// 256-entry Gear table — one pseudo-random 64-bit value per byte value. Generated
/// deterministically at compile time with splitmix64 (CONCEPT:EG-071): a fixed
/// table keeps chunk boundaries — and therefore blob/chunk digests — stable across
/// builds and processes, which the content-addressed dedup relies on.
const GEAR: [u64; 256] = build_gear_table();

/// Build the Gear table with a splitmix64 sequence from a fixed seed. `const fn` so
/// the table is baked into the binary (no runtime init, no `lazy_static`/dep).
const fn build_gear_table() -> [u64; 256] {
    let mut table = [0u64; 256];
    let mut state: u64 = 0x9E37_79B9_7F4A_7C15; // golden-ratio seed
    let mut i = 0;
    while i < 256 {
        // splitmix64 step.
        state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^= z >> 31;
        table[i] = z;
        i += 1;
    }
    table
}

/// Default target average chunk size: 2 MiB — the centre of the 1–4 MB band the
/// streaming protocol targets (matches the pre-EG-071 fixed size). Min/max derive
/// from it in [`Chunker::from_avg`].
pub const DEFAULT_AVG_SIZE: usize = 2 * 1024 * 1024;

/// Normalization level: `mask_s` gets `avg_bits + NORMALIZATION` bits, `mask_l` gets
/// `avg_bits - NORMALIZATION` bits. 2 is the value the FastCDC paper recommends.
const NORMALIZATION: u32 = 2;

/// A content-defined chunker (CONCEPT:EG-071). Cheap to construct and `Copy`; holds
/// only the size bounds and the two precomputed normalized masks.
#[derive(Debug, Clone, Copy)]
pub struct Chunker {
    min: usize,
    avg: usize,
    max: usize,
    mask_s: u64,
    mask_l: u64,
}

impl Default for Chunker {
    fn default() -> Self {
        Self::from_avg(DEFAULT_AVG_SIZE)
    }
}

impl Chunker {
    /// A chunker whose hard bounds bracket `avg`: `min = avg/4`, `max = avg*4`
    /// (clamped to a sane floor). The common entry point — callers pass only the
    /// target average chunk size.
    pub fn from_avg(avg: usize) -> Self {
        let avg = avg.max(64);
        let min = (avg / 4).max(64);
        let max = (avg.saturating_mul(4)).max(avg + min);
        Self::new(min, avg, max)
    }

    /// A chunker with explicit `min`/`avg`/`max` bounds (bytes). Bounds are repaired
    /// to satisfy `min <= avg <= max` so [`next_cut`](Self::next_cut) can never
    /// violate them.
    pub fn new(min: usize, avg: usize, max: usize) -> Self {
        let min = min.max(1);
        let avg = avg.max(min);
        let max = max.max(avg);
        // Mask width follows the target average: avg ≈ 2^bits, so a `bits`-bit mask
        // gives a ~1/2^bits cut probability per byte ⇒ mean chunk ≈ avg.
        let bits = log2_floor(avg);
        let mask_s = low_mask(bits + NORMALIZATION);
        let mask_l = low_mask(bits.saturating_sub(NORMALIZATION));
        Self {
            min,
            avg,
            max,
            mask_s,
            mask_l,
        }
    }

    /// Minimum chunk size (bytes).
    pub fn min_size(&self) -> usize {
        self.min
    }

    /// Maximum chunk size (bytes) — the largest prefix [`next_cut`](Self::next_cut)
    /// can ever return, hence the upper bound on resident bytes a streaming splitter
    /// must buffer to find a boundary.
    pub fn max_size(&self) -> usize {
        self.max
    }

    /// Length of the next content-defined chunk at the front of `data` (FastCDC,
    /// normalized). Always in `[min, max]` except when `data` is shorter than `min`
    /// (then the whole remainder is one final chunk). The fingerprint resets per
    /// call, so this is a pure function of the leading bytes of `data`.
    pub fn next_cut(&self, data: &[u8]) -> usize {
        let len = data.len();
        if len <= self.min {
            return len;
        }
        // Never scan past `max`; never cut before `min`.
        let n = len.min(self.max);
        let center = self.avg.min(n);
        let mut hash: u64 = 0;
        let mut i = self.min;
        // Region 1 [min, center): stricter mask suppresses small chunks.
        while i < center {
            hash = (hash << 1).wrapping_add(GEAR[data[i] as usize]);
            if (hash & self.mask_s) == 0 {
                return i + 1;
            }
            i += 1;
        }
        // Region 2 [center, n): looser mask forces a boundary before `max`.
        while i < n {
            hash = (hash << 1).wrapping_add(GEAR[data[i] as usize]);
            if (hash & self.mask_l) == 0 {
                return i + 1;
            }
            i += 1;
        }
        // No content boundary inside the window → hard cut at `min(max, len)`.
        n
    }
}

/// Floor of log2(`v`) (0 for `v == 0`). Used to size the masks from the average.
fn log2_floor(v: usize) -> u32 {
    (usize::BITS - 1).saturating_sub(v.max(1).leading_zeros())
}

/// A contiguous `nbits`-bit low mask (`2^nbits - 1`), clamped to a usable range.
/// `(hash & mask) == 0` then fires with probability ~`2^-nbits` per byte, so a
/// `bits`-bit mask yields a mean chunk near `2^bits` bytes.
fn low_mask(nbits: u32) -> u64 {
    let nbits = nbits.clamp(1, 48);
    (1u64 << nbits) - 1
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Deterministic pseudo-random bytes (xorshift) — stand-in for real media so the
    /// chunker actually finds content boundaries.
    fn pseudo_random(n: usize, seed: u64) -> Vec<u8> {
        let mut out = Vec::with_capacity(n);
        let mut x = seed | 1;
        for _ in 0..n {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            out.push((x & 0xFF) as u8);
        }
        out
    }

    /// Split `data` into the full sequence of content-defined chunk lengths.
    fn chunk_lengths(chunker: &Chunker, data: &[u8]) -> Vec<usize> {
        let mut lens = Vec::new();
        let mut off = 0;
        while off < data.len() {
            let cut = chunker.next_cut(&data[off..]);
            assert!(cut > 0, "next_cut must make progress");
            lens.push(cut);
            off += cut;
        }
        lens
    }

    /// The gear table must be non-degenerate (distinct, deterministic).
    #[test]
    fn gear_table_is_deterministic_and_distinct() {
        let a = build_gear_table();
        let b = build_gear_table();
        assert_eq!(a, b, "table must be reproducible");
        let distinct: std::collections::HashSet<u64> = a.iter().copied().collect();
        assert_eq!(distinct.len(), 256, "all 256 gear entries distinct");
    }

    /// (c) Every interior chunk respects the min/max bounds (CONCEPT:EG-071). Only
    /// the final chunk may fall below `min` (the remainder).
    #[test]
    fn chunk_sizes_respect_min_and_max_bounds() {
        let chunker = Chunker::from_avg(4096);
        let data = pseudo_random(2_000_000, 0xABCD);
        let lens = chunk_lengths(&chunker, &data);
        assert!(lens.len() > 50, "expected many variable chunks, got {}", lens.len());
        let total: usize = lens.iter().sum();
        assert_eq!(total, data.len(), "lengths must cover the whole blob exactly");
        for (i, &l) in lens.iter().enumerate() {
            assert!(
                l <= chunker.max_size(),
                "chunk {i} len {l} exceeds max {}",
                chunker.max_size()
            );
            let is_last = i == lens.len() - 1;
            if !is_last {
                assert!(
                    l >= chunker.min_size(),
                    "interior chunk {i} len {l} below min {}",
                    chunker.min_size()
                );
            }
        }
        // The mean should land near the target average (loose band — CDC is random).
        let mean = total / lens.len();
        assert!(
            mean >= chunker.min_size() && mean <= chunker.max_size(),
            "mean chunk {mean} should sit within [min,max]"
        );
    }

    /// (b) An early insertion shifts only the first chunk(s); the rolling hash
    /// re-synchronises and the vast majority of downstream boundaries — hence chunk
    /// contents — are identical (CONCEPT:EG-071, shift-resistance). A fixed-stride
    /// splitter would share almost nothing here.
    #[test]
    fn early_insertion_preserves_most_boundaries() {
        let chunker = Chunker::from_avg(4096);
        let base = pseudo_random(2_000_000, 0x1234);

        // Insert 7 bytes near the very start.
        let mut edited = Vec::with_capacity(base.len() + 7);
        edited.extend_from_slice(&base[..100]);
        edited.extend_from_slice(b"INSERT!");
        edited.extend_from_slice(&base[100..]);

        // Reduce each blob to the multiset of its chunk byte-ranges' digests.
        let digests = |data: &[u8]| -> std::collections::HashSet<u64> {
            let mut set = std::collections::HashSet::new();
            let mut off = 0;
            while off < data.len() {
                let cut = chunker.next_cut(&data[off..]);
                // A cheap order-independent fingerprint of the chunk bytes.
                let mut h: u64 = 1469598103934665603;
                for &byte in &data[off..off + cut] {
                    h = (h ^ byte as u64).wrapping_mul(1099511628211);
                }
                set.insert(h);
                off += cut;
            }
            set
        };

        let a = digests(&base);
        let b = digests(&edited);
        let shared = a.intersection(&b).count();
        let frac = shared as f64 / a.len().min(b.len()) as f64;
        assert!(
            frac > 0.8,
            "early insertion should preserve most chunks (shared {shared}/{} = {frac:.2})",
            a.len().min(b.len())
        );
    }

    /// (a) at the chunker level: chunking then concatenating the chunks reproduces
    /// the input byte-for-byte (the CAS round-trip is asserted end-to-end in
    /// `stream.rs`).
    #[test]
    fn chunks_concatenate_to_original() {
        let chunker = Chunker::from_avg(8192);
        let data = pseudo_random(1_000_003, 0x55AA); // non-multiple length
        let mut rebuilt = Vec::with_capacity(data.len());
        let mut off = 0;
        while off < data.len() {
            let cut = chunker.next_cut(&data[off..]);
            rebuilt.extend_from_slice(&data[off..off + cut]);
            off += cut;
        }
        assert_eq!(rebuilt, data);
    }

    /// Boundaries are a pure function of the bytes — same input, same cuts every run.
    #[test]
    fn boundaries_are_deterministic() {
        let chunker = Chunker::from_avg(4096);
        let data = pseudo_random(500_000, 0x9);
        assert_eq!(chunk_lengths(&chunker, &data), chunk_lengths(&chunker, &data));
    }
}
