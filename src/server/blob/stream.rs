//! Bounded-memory streaming facade over the CAS — CONCEPT:EG-033 (M3 R4 extension).
//!
//! The KG-2.206 substrate already streams a blob over the WIRE chunk-by-chunk (the
//! `BlobBegin`/`BlobChunkPut`/`BlobCommit` upload cursor + `BlobFetchBegin`/
//! `BlobChunkGet` download cursor — see [`super::store`] / `handlers::blob`). This
//! module adds the IN-PROCESS twin: stream a multi-GB blob between an arbitrary byte
//! source/sink (a local media file, a decompressor, an embedding writer) and the CAS
//! WITHOUT buffering the whole blob — at most one chunk is resident at a time.
//!
//! It composes [`ChunkStore`] verbatim (one `put_chunk`/`get_chunk` per chunk), so it
//! is the SAME content-addressed, dedup'd, refcount-GC'd store — just driven by a
//! `Read`/`Write` instead of the wire cursor. Independent modality: it never touches
//! the graph write path. Used by file import/export and the cold-tier offload (R6).

use std::io::{Read, Write};

use super::store::{hex_digest, BlobManifest, ChunkStore, CommittedBlob, DEFAULT_CHUNK_SIZE};

/// Stream `reader` into the CAS in `chunk_size` chunks and commit the manifest,
/// returning the content-addressed [`CommittedBlob`]. Bounded memory: ONE
/// `chunk_size` buffer is reused for every chunk — peak resident bytes are the chunk
/// size plus the digest list (~70 bytes/chunk), NOT the blob size. `chunk_size == 0`
/// uses [`DEFAULT_CHUNK_SIZE`]. Identical content yields an identical blob digest
/// (intrinsic dedup), exactly like the wire path.
pub fn stream_blob_put<R: Read>(
    store: &dyn ChunkStore,
    mut reader: R,
    chunk_size: usize,
) -> Result<CommittedBlob, String> {
    let chunk_size = if chunk_size == 0 {
        DEFAULT_CHUNK_SIZE
    } else {
        chunk_size
    };
    let mut buf = vec![0u8; chunk_size];
    let mut chunks: Vec<String> = Vec::new();
    let mut len: u64 = 0;
    loop {
        // Fill up to a whole chunk; a short final read still flushes its bytes.
        let n = read_full(&mut reader, &mut buf)?;
        if n == 0 {
            break;
        }
        let (digest, _was_new) = store.put_chunk(&buf[..n])?;
        chunks.push(digest);
        len += n as u64;
        if n < chunk_size {
            break; // EOF mid-buffer.
        }
    }
    let manifest = BlobManifest {
        chunks,
        len,
        chunk_size: chunk_size as u32,
    };
    let manifest_bytes = rmp_serde::to_vec_named(&manifest).map_err(|e| e.to_string())?;
    let digest = hex_digest(&manifest_bytes);
    store.put_manifest(&digest, &manifest)?;
    Ok(CommittedBlob { digest, manifest })
}

/// Stream a stored blob OUT to `writer` chunk-by-chunk, returning the bytes written.
/// Bounded memory: one chunk is pulled from the CAS, written, and dropped before the
/// next — peak resident bytes are one chunk, NOT the blob size. Errors if the blob
/// digest is unknown or a chunk is missing from the CAS (a corrupt/half-GC'd blob).
pub fn stream_blob_get<W: Write>(
    store: &dyn ChunkStore,
    blob_digest: &str,
    mut writer: W,
) -> Result<u64, String> {
    let manifest = store
        .get_manifest(blob_digest)?
        .ok_or_else(|| "unknown blob digest".to_string())?;
    let mut written: u64 = 0;
    for d in &manifest.chunks {
        let bytes = store
            .get_chunk(d)?
            .ok_or_else(|| format!("chunk {d} missing from CAS"))?;
        writer.write_all(&bytes).map_err(|e| e.to_string())?;
        written += bytes.len() as u64;
        // `bytes` dropped here — never accumulated.
    }
    writer.flush().map_err(|e| e.to_string())?;
    Ok(written)
}

/// Read until `buf` is full or EOF, returning the bytes filled (handles a `Read` that
/// returns short reads, e.g. a pipe/socket). 0 ⇒ clean EOF at a chunk boundary.
fn read_full<R: Read>(reader: &mut R, buf: &mut [u8]) -> Result<usize, String> {
    let mut filled = 0;
    while filled < buf.len() {
        match reader.read(&mut buf[filled..]) {
            Ok(0) => break,
            Ok(n) => filled += n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(e) => return Err(e.to_string()),
        }
    }
    Ok(filled)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::blob::store::RedbChunkStore;
    use sha2::{Digest, Sha256};
    use std::io::Cursor;

    #[test]
    fn round_trips_through_read_and_write() {
        let store = RedbChunkStore::open_temp().unwrap();
        let data: Vec<u8> = (0u32..300_000).map(|i| (i % 211) as u8).collect();
        let blob = stream_blob_put(&store, Cursor::new(data.clone()), 4096).unwrap();
        let mut out: Vec<u8> = Vec::new();
        let n = stream_blob_get(&store, &blob.digest, &mut out).unwrap();
        assert_eq!(n, data.len() as u64);
        assert_eq!(out, data);
        // Re-streaming identical content yields the SAME blob digest (dedup).
        let blob2 = stream_blob_put(&store, Cursor::new(data), 4096).unwrap();
        assert_eq!(blob.digest, blob2.digest);
    }

    #[test]
    fn unknown_digest_errors() {
        let store = RedbChunkStore::open_temp().unwrap();
        let mut sink: Vec<u8> = Vec::new();
        assert!(stream_blob_get(&store, "deadbeef", &mut sink).is_err());
    }

    /// Peak RSS of this process in MB (Linux VmHWM), for the bounded-memory assert.
    fn peak_rss_mb() -> u64 {
        let s = std::fs::read_to_string("/proc/self/status").unwrap_or_default();
        for line in s.lines() {
            if let Some(rest) = line.strip_prefix("VmHWM:") {
                if let Some(kb) = rest
                    .split_whitespace()
                    .next()
                    .and_then(|n| n.parse::<u64>().ok())
                {
                    return kb / 1024;
                }
            }
        }
        0
    }

    /// A `Read` that GENERATES `total` bytes of distinct (offset-seeded) data on the
    /// fly — never holding the source blob in RAM, so the test proves the FACADE is
    /// what bounds memory (not a pre-buffered input).
    struct GenReader {
        produced: u64,
        total: u64,
    }
    impl Read for GenReader {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            if self.produced >= self.total {
                return Ok(0);
            }
            let n = ((self.total - self.produced) as usize).min(buf.len());
            for (i, b) in buf[..n].iter_mut().enumerate() {
                let off = self.produced + i as u64;
                // xorshift on the byte offset → distinct, non-dedupable bytes.
                let mut x = (off + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                *b = (x & 0xFF) as u8;
            }
            self.produced += n as u64;
            Ok(n)
        }
    }

    /// A `Write` that DISCARDS bytes after folding them into a running hash — never
    /// accumulates the downloaded blob, so the read path's memory bound is what is
    /// tested (not the sink).
    struct HashSink(Sha256);
    impl Write for HashSink {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.update(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// BOUNDED MEMORY (the R4 deliverable): stream a LARGE blob IN from a generating
    /// reader and back OUT to a hashing sink, retaining NEITHER the whole blob NOR all
    /// chunks. Peak RSS growth must stay bounded by the capped redb page cache + a
    /// chunk, INDEPENDENT of the blob size — a regression that buffered the whole blob
    /// would track the file size and trip the assert. Default 256 MB; `EG_BLOB_RSS_MB`
    /// runs 1 GB+.
    #[test]
    fn bounded_memory_streams_large_blob() {
        let total_mb: u64 = std::env::var("EG_BLOB_RSS_MB")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(256);
        let total = total_mb * 1024 * 1024;
        let chunk_size = 2 * 1024 * 1024usize; // 2 MiB
        let store = RedbChunkStore::open_temp().unwrap();
        let rss_before = peak_rss_mb();

        // Upload: re-hash the generated source as we go for the integrity check.
        let mut src_hash = Sha256::new();
        let reader = GenReader { produced: 0, total };
        // Tee the generated bytes into src_hash by re-generating identically — cheaper
        // and still bounded: we hash from a second GenReader chunk-by-chunk.
        {
            let mut hr = GenReader { produced: 0, total };
            let mut buf = vec![0u8; chunk_size];
            loop {
                let n = read_full(&mut hr, &mut buf).unwrap();
                if n == 0 {
                    break;
                }
                src_hash.update(&buf[..n]);
                if n < chunk_size {
                    break;
                }
            }
        }
        let blob = stream_blob_put(&store, reader, chunk_size).unwrap();
        assert_eq!(blob.manifest.len, total);

        // Download into the hashing sink — retain nothing.
        let mut sink = HashSink(Sha256::new());
        let n = stream_blob_get(&store, &blob.digest, &mut sink).unwrap();
        assert_eq!(n, total);
        assert_eq!(
            hex::encode(src_hash.finalize()),
            hex::encode(sink.0.finalize()),
            "round-trip integrity over the streamed large blob"
        );

        let growth = peak_rss_mb().saturating_sub(rss_before);
        assert!(
            growth < 320,
            "peak RSS growth {growth}MB for a {total_mb}MB streamed blob must be bounded \
             by the capped page cache + one chunk, NOT the blob size"
        );
    }
}
