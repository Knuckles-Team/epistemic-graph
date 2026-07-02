//! The WARM-tier compressor for the tiered cache (CONCEPT:EG-185).
//!
//! The WARM tier keeps blocks in RAM but **compressed**, to fit more KV pages in the
//! same byte budget before anything has to spill to the (slow) COLD tier.
//!
//! **Choice of compressor.** To keep this crate dependency-free / Pi-lean (no `zstd` /
//! `lz4` C or heavy-Rust dep), the built-in compressor is a hand-written **byte-run
//! RLE** ([`rle_encode`]) with a **raw fallback**: [`compress`] only keeps the RLE
//! output when it is actually smaller than the input, otherwise it stores the block RAW
//! and marks it uncompressed. So correctness holds for ANY input (random / already
//! compressed data is stored verbatim, never expanded), and KV pages — which carry long
//! zero / low-entropy runs after quantization — get a real win. A production deployment
//! swaps in `zstd`/`lz4` behind a cargo feature that implements this same
//! encode/decode seam; the tier machinery is agnostic to which codec fills it.

/// A block resident in the WARM (or COLD) tier: possibly-compressed bytes plus the
/// metadata needed to reconstruct the original block (CONCEPT:EG-185).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredBlock {
    /// The stored bytes: RLE-compressed when `compressed`, else the raw block.
    pub data: Vec<u8>,
    /// Whether `data` is RLE-compressed (vs stored raw because RLE did not help).
    pub compressed: bool,
    /// The ORIGINAL (uncompressed) block length in bytes.
    pub orig_len: usize,
}

impl StoredBlock {
    /// Compress `bytes` into a stored block (RLE with raw fallback).
    pub fn encode(bytes: &[u8]) -> Self {
        let rle = rle_encode(bytes);
        if rle.len() < bytes.len() {
            StoredBlock {
                data: rle,
                compressed: true,
                orig_len: bytes.len(),
            }
        } else {
            StoredBlock {
                data: bytes.to_vec(),
                compressed: false,
                orig_len: bytes.len(),
            }
        }
    }

    /// Reconstruct the original block bytes.
    pub fn decode(&self) -> Vec<u8> {
        if self.compressed {
            rle_decode(&self.data, self.orig_len)
        } else {
            self.data.clone()
        }
    }

    /// The number of bytes this block occupies in its tier (the compressed size).
    pub fn stored_len(&self) -> usize {
        self.data.len()
    }
}

/// Byte-run RLE: emits `[run:u8][byte]` pairs, splitting runs longer than 255.
fn rle_encode(input: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < input.len() {
        let b = input[i];
        let mut run = 1usize;
        while i + run < input.len() && input[i + run] == b && run < 255 {
            run += 1;
        }
        out.push(run as u8);
        out.push(b);
        i += run;
    }
    out
}

/// Inverse of [`rle_encode`]; `orig_len` pre-sizes the output.
fn rle_decode(data: &[u8], orig_len: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(orig_len);
    let mut i = 0;
    while i + 1 < data.len() {
        let run = data[i];
        let b = data[i + 1];
        for _ in 0..run {
            out.push(b);
        }
        i += 2;
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// CONCEPT:EG-185 — a compressible (run-heavy) block round-trips AND shrinks in WARM.
    #[test]
    fn eg185_warm_compress_roundtrip_shrinks_run_heavy_block() {
        let mut block = vec![0u8; 4096];
        block[100..110].copy_from_slice(&[7u8; 10]);
        let sb = StoredBlock::encode(&block);
        assert!(sb.compressed, "run-heavy KV page should compress");
        assert!(
            sb.stored_len() < block.len(),
            "WARM must be smaller than the raw block"
        );
        assert_eq!(
            sb.decode(),
            block,
            "decode must reconstruct the exact block"
        );
    }

    /// CONCEPT:EG-185 — incompressible data is stored RAW (never expanded) and still
    /// round-trips (the raw-fallback correctness guarantee).
    #[test]
    fn eg185_warm_incompressible_falls_back_to_raw() {
        let block: Vec<u8> = (0..=255u8).cycle().take(1000).collect();
        let sb = StoredBlock::encode(&block);
        assert!(!sb.compressed, "no run structure ⇒ raw fallback");
        assert_eq!(
            sb.stored_len(),
            block.len(),
            "raw fallback never expands the block"
        );
        assert_eq!(sb.decode(), block);
    }

    /// CONCEPT:EG-185 — the empty block is handled.
    #[test]
    fn eg185_warm_empty_block_roundtrips() {
        let sb = StoredBlock::encode(&[]);
        assert_eq!(sb.decode(), Vec::<u8>::new());
    }
}
