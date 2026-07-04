//! The WARM-tier compressor for the tiered cache (CONCEPT:EG-KG.memory.byte-bounded-tiers, CONCEPT:EG-KG.storage.rle-codec-default).
//!
//! The WARM tier keeps blocks in RAM but **compressed**, to fit more KV pages in the
//! same byte budget before anything has to spill to the (slow) COLD tier.
//!
//! **Choice of compressor (CONCEPT:EG-KG.storage.rle-codec-default).** The WARM tier is codec-pluggable via
//! [`Codec`]:
//!
//! * [`Codec::Rle`] — a hand-written byte-run RLE ([`rle_encode`]). This is the
//!   **dependency-free default**, so the base build stays Pi-lean (no `zstd` / `lz4`
//!   C or heavy-Rust dep). KV pages carry long zero / low-entropy runs after
//!   quantization, so RLE gets a real win with zero dependencies.
//! * [`Codec::Zstd`] — a **real** general-purpose codec (`zstd`), behind the optional
//!   `compression` cargo feature (CONCEPT:EG-KG.storage.rle-codec-default). Far better ratios on realistic KV
//!   pages than RLE, at the cost of one dependency; selectable per-cache via
//!   [`crate::TieredCache::with_warm_codec`].
//! * [`Codec::Lz4`] — an OPTIONAL faster/lighter codec (`lz4_flex`, pure-Rust), behind
//!   the optional `lz4` cargo feature.
//!
//! **Raw fallback is universal.** Whatever codec is configured, [`StoredBlock::encode_with`]
//! only keeps the compressed output when it is actually smaller than the input; otherwise
//! it stores the block RAW ([`Codec::Raw`]) and marks it uncompressed. So correctness
//! holds for ANY input — random / already-compressed data is stored verbatim, **never
//! expanded** — regardless of which codec is chosen. The tier machinery is agnostic to
//! which codec fills it; a demote encodes with the configured codec and a promote decodes
//! by the tag recorded on the block.

/// The compression codec a [`StoredBlock`] was produced with (CONCEPT:EG-KG.storage.rle-codec-default).
///
/// This is BOTH the per-block tag (so [`StoredBlock::decode`] knows how to reconstruct)
/// AND the configuration knob a [`crate::TieredCache`] is built with
/// ([`crate::TieredCache::with_warm_codec`]). Configuring [`Codec::Raw`] disables warm
/// compression entirely; the real codecs ([`Codec::Zstd`] / [`Codec::Lz4`]) only exist
/// when their cargo feature is enabled, so a lean build cannot even name them.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum Codec {
    /// Stored verbatim — the raw fallback (compression disabled, or it did not help).
    Raw,
    /// Hand-rolled byte-run RLE — the dependency-free default (CONCEPT:EG-KG.memory.byte-bounded-tiers).
    #[default]
    Rle,
    /// `zstd`, a real general-purpose codec (CONCEPT:EG-KG.storage.rle-codec-default, feature `compression`).
    #[cfg(feature = "compression")]
    Zstd,
    /// `lz4_flex`, an optional faster/lighter pure-Rust codec (CONCEPT:EG-KG.storage.rle-codec-default, feature `lz4`).
    #[cfg(feature = "lz4")]
    Lz4,
}

/// A block resident in the WARM (or COLD) tier: possibly-compressed bytes plus the
/// metadata needed to reconstruct the original block (CONCEPT:EG-KG.memory.byte-bounded-tiers, CONCEPT:EG-KG.storage.rle-codec-default).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredBlock {
    /// The stored bytes: encoded with `codec` (or the raw block when `codec == Raw`).
    pub data: Vec<u8>,
    /// Which codec `data` was produced with — the tag [`StoredBlock::decode`] dispatches
    /// on (CONCEPT:EG-KG.storage.rle-codec-default). [`Codec::Raw`] means `data` is the verbatim block.
    pub codec: Codec,
    /// The ORIGINAL (uncompressed) block length in bytes.
    pub orig_len: usize,
}

impl StoredBlock {
    /// Compress `bytes` into a stored block with the dependency-free default codec
    /// (RLE, with raw fallback). Equivalent to `encode_with(Codec::Rle, bytes)`.
    pub fn encode(bytes: &[u8]) -> Self {
        Self::encode_with(Codec::Rle, bytes)
    }

    /// Compress `bytes` into a stored block with `codec`, falling back to RAW whenever
    /// the codec does not actually shrink the block (CONCEPT:EG-KG.storage.rle-codec-default). The raw fallback
    /// guarantees the stored form NEVER expands incompressible input, for any codec.
    pub fn encode_with(codec: Codec, bytes: &[u8]) -> Self {
        let encoded: Option<Vec<u8>> = match codec {
            Codec::Raw => None,
            Codec::Rle => Some(rle_encode(bytes)),
            #[cfg(feature = "compression")]
            Codec::Zstd => Some(zstd_encode(bytes)),
            #[cfg(feature = "lz4")]
            Codec::Lz4 => Some(lz4_encode(bytes)),
        };
        match encoded {
            // Keep the compressed form only if it is a real win (strictly smaller).
            Some(data) if data.len() < bytes.len() => StoredBlock {
                data,
                codec,
                orig_len: bytes.len(),
            },
            // RAW fallback: disabled codec, or compression did not help.
            _ => StoredBlock {
                data: bytes.to_vec(),
                codec: Codec::Raw,
                orig_len: bytes.len(),
            },
        }
    }

    /// Reconstruct the original block bytes by dispatching on the block's [`Codec`] tag.
    pub fn decode(&self) -> Vec<u8> {
        match self.codec {
            Codec::Raw => self.data.clone(),
            Codec::Rle => rle_decode(&self.data, self.orig_len),
            #[cfg(feature = "compression")]
            Codec::Zstd => zstd_decode(&self.data, self.orig_len),
            #[cfg(feature = "lz4")]
            Codec::Lz4 => lz4_decode(&self.data, self.orig_len),
        }
    }

    /// The number of bytes this block occupies in its tier (the compressed size).
    pub fn stored_len(&self) -> usize {
        self.data.len()
    }

    /// Whether `data` is compressed (vs stored raw because compression did not help or
    /// was disabled).
    pub fn compressed(&self) -> bool {
        self.codec != Codec::Raw
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

/// `zstd` encode at the library default level (CONCEPT:EG-KG.storage.rle-codec-default, feature `compression`).
///
/// Infallible for an in-memory source buffer (the only failure modes are I/O on the
/// reader/writer, which cannot happen for `&[u8]` / `Vec<u8>`), so we surface a panic
/// rather than thread a `Result` through the codec seam.
#[cfg(feature = "compression")]
fn zstd_encode(input: &[u8]) -> Vec<u8> {
    // Level 0 selects zstd's built-in default (~3): a strong ratio/speed balance.
    zstd::stream::encode_all(input, 0).expect("zstd encode of an in-memory buffer cannot fail")
}

/// `zstd` decode of bytes this crate itself produced (CONCEPT:EG-KG.storage.rle-codec-default).
#[cfg(feature = "compression")]
fn zstd_decode(data: &[u8], orig_len: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(orig_len);
    zstd::stream::copy_decode(data, &mut out)
        .expect("zstd decode of eg-kvcache-produced bytes cannot fail");
    out
}

/// `lz4_flex` encode with a length prefix (CONCEPT:EG-KG.storage.rle-codec-default, feature `lz4`).
#[cfg(feature = "lz4")]
fn lz4_encode(input: &[u8]) -> Vec<u8> {
    lz4_flex::compress_prepend_size(input)
}

/// `lz4_flex` decode of bytes this crate itself produced (CONCEPT:EG-KG.storage.rle-codec-default).
#[cfg(feature = "lz4")]
fn lz4_decode(data: &[u8], _orig_len: usize) -> Vec<u8> {
    lz4_flex::decompress_size_prepended(data)
        .expect("lz4 decode of eg-kvcache-produced bytes cannot fail")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A deterministic high-entropy (incompressible) buffer — an xorshift stream. Used to
    /// prove the raw fallback for EVERY codec: no codec can shrink this, so all must store
    /// it verbatim without expanding it.
    fn incompressible(len: usize) -> Vec<u8> {
        let mut s: u64 = 0x9E37_79B9_7F4A_7C15;
        (0..len)
            .map(|_| {
                s ^= s << 13;
                s ^= s >> 7;
                s ^= s << 17;
                (s & 0xff) as u8
            })
            .collect()
    }

    /// A compressible KV-page-like buffer: a long low-entropy run with a small marker,
    /// which every real codec shrinks well.
    fn compressible(len: usize) -> Vec<u8> {
        let mut block = vec![0u8; len];
        for (i, b) in block.iter_mut().enumerate().take(len / 8) {
            *b = (i % 4) as u8;
        }
        block
    }

    /// CONCEPT:EG-KG.memory.byte-bounded-tiers — a compressible (run-heavy) block round-trips AND shrinks in WARM.
    #[test]
    fn eg185_warm_compress_roundtrip_shrinks_run_heavy_block() {
        let mut block = vec![0u8; 4096];
        block[100..110].copy_from_slice(&[7u8; 10]);
        let sb = StoredBlock::encode(&block);
        assert!(sb.compressed(), "run-heavy KV page should compress");
        assert_eq!(sb.codec, Codec::Rle, "default codec is RLE");
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

    /// CONCEPT:EG-KG.memory.byte-bounded-tiers — incompressible data is stored RAW (never expanded) and still
    /// round-trips (the raw-fallback correctness guarantee).
    #[test]
    fn eg185_warm_incompressible_falls_back_to_raw() {
        let block: Vec<u8> = (0..=255u8).cycle().take(1000).collect();
        let sb = StoredBlock::encode(&block);
        assert!(!sb.compressed(), "no run structure ⇒ raw fallback");
        assert_eq!(sb.codec, Codec::Raw);
        assert_eq!(
            sb.stored_len(),
            block.len(),
            "raw fallback never expands the block"
        );
        assert_eq!(sb.decode(), block);
    }

    /// CONCEPT:EG-KG.memory.byte-bounded-tiers — the empty block is handled.
    #[test]
    fn eg185_warm_empty_block_roundtrips() {
        let sb = StoredBlock::encode(&[]);
        assert_eq!(sb.decode(), Vec::<u8>::new());
    }

    /// CONCEPT:EG-KG.storage.rle-codec-default — the default codec is still RLE (the dependency-free base build is
    /// unchanged); `encode` and `encode_with(Codec::Rle, ..)` agree.
    #[test]
    fn eg315_default_codec_is_rle_and_unchanged() {
        let block = compressible(4096);
        let a = StoredBlock::encode(&block);
        let b = StoredBlock::encode_with(Codec::Rle, &block);
        assert_eq!(a, b, "encode() is exactly encode_with(Rle, ..)");
        assert_eq!(a.codec, Codec::Rle);
        assert_eq!(a.decode(), block);
    }

    /// CONCEPT:EG-KG.storage.rle-codec-default — configuring `Codec::Raw` disables compression: the block is stored
    /// verbatim and still round-trips.
    #[test]
    fn eg315_raw_codec_stores_verbatim() {
        let block = compressible(4096);
        let sb = StoredBlock::encode_with(Codec::Raw, &block);
        assert_eq!(sb.codec, Codec::Raw);
        assert_eq!(sb.stored_len(), block.len(), "raw stores verbatim");
        assert_eq!(sb.decode(), block);
    }

    /// CONCEPT:EG-KG.storage.rle-codec-default — the real `zstd` codec round-trips a WARM block and achieves a
    /// ratio < 1 (strictly smaller) on compressible KV-page-like data.
    #[cfg(feature = "compression")]
    #[test]
    fn eg315_warm_zstd_roundtrip_ratio_below_one() {
        let block = compressible(8192);
        let sb = StoredBlock::encode_with(Codec::Zstd, &block);
        assert_eq!(
            sb.codec,
            Codec::Zstd,
            "compressible data keeps the zstd form"
        );
        assert!(
            sb.stored_len() < block.len(),
            "zstd WARM ratio must be < 1 on compressible data (got {} >= {})",
            sb.stored_len(),
            block.len()
        );
        assert_eq!(sb.decode(), block, "zstd must reconstruct the exact block");
    }

    /// CONCEPT:EG-KG.storage.rle-codec-default — `zstd` on incompressible (high-entropy) data NEVER expands it:
    /// the universal raw fallback kicks in.
    #[cfg(feature = "compression")]
    #[test]
    fn eg315_warm_zstd_incompressible_falls_back_to_raw() {
        let block = incompressible(4096);
        let sb = StoredBlock::encode_with(Codec::Zstd, &block);
        assert_eq!(sb.codec, Codec::Raw, "incompressible ⇒ raw fallback");
        assert_eq!(
            sb.stored_len(),
            block.len(),
            "raw fallback never expands incompressible data"
        );
        assert_eq!(sb.decode(), block);
    }

    /// CONCEPT:EG-KG.storage.rle-codec-default — the optional `lz4` codec round-trips and shrinks compressible data.
    #[cfg(feature = "lz4")]
    #[test]
    fn eg315_warm_lz4_roundtrip_ratio_below_one() {
        let block = compressible(8192);
        let sb = StoredBlock::encode_with(Codec::Lz4, &block);
        assert_eq!(sb.codec, Codec::Lz4);
        assert!(
            sb.stored_len() < block.len(),
            "lz4 WARM ratio must be < 1 on compressible data"
        );
        assert_eq!(sb.decode(), block);
    }

    /// CONCEPT:EG-KG.storage.rle-codec-default — `lz4` on incompressible data falls back to raw (never expands).
    #[cfg(feature = "lz4")]
    #[test]
    fn eg315_warm_lz4_incompressible_falls_back_to_raw() {
        let block = incompressible(4096);
        let sb = StoredBlock::encode_with(Codec::Lz4, &block);
        assert_eq!(sb.codec, Codec::Raw);
        assert_eq!(sb.stored_len(), block.len());
        assert_eq!(sb.decode(), block);
    }
}
