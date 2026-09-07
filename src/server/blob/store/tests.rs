//! Unit tests for the blob CAS store's owner-write paths.

use super::*;

fn coordinator_batch(
    id: &str,
    request_id: u64,
    expected: u64,
    event_type: &str,
) -> MutationBatch {
    crate::server::mutation_batch::compile_opaque_method(
        crate::server::mutation_batch::CompileBatch {
            batch_id: id,
            request_id,
            principal: Some("system"),
            tenant: "tenant-opaque",
            graph: "scope-opaque",
            placement_epoch: 0,
            idempotency_key: id,
            expected_graph_version: Some(expected),
            fencing_token: None,
            created_at_ms: request_id,
            default_surface: crate::mutation_batch::MutationSurface::Other,
            authoritative_state: None,
        },
        &crate::protocol::Method::ApplyMutation {
            event_type: event_type.to_string(),
            query: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                .to_string(),
        },
        crate::mutation_batch::MutationSurface::Other,
        crate::mutation_batch::DurabilityDomain::BlobStore,
        "blob_coordinator_test",
    )
    .unwrap()
}

#[test]
fn direct_ref_acquire_compensation_and_gc_are_restart_replay_safe() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_string_lossy().to_string();
    let body = b"opaque-result";
    let digest = hex_digest(body);
    {
        let store = RedbChunkStore::open(&path).unwrap();
        let acquire = coordinator_batch("acquire", 1, 0, "blob_direct_ref_acquire_v1");
        assert_eq!(
            store.put_chunk_ref_batch(body, &acquire, 1).unwrap(),
            (digest.clone(), true, 1)
        );
        assert_eq!(
            store.put_chunk_ref_batch(body, &acquire, 1).unwrap(),
            (digest.clone(), true, 1),
            "acknowledgement-lost acquire must replay without another ref"
        );
        let release = coordinator_batch("release", 2, 1, "blob_direct_ref_release_v1");
        assert_eq!(store.adjust_ref_batch(&digest, -1, &release, 2).unwrap(), 0);
        assert_eq!(
            store.adjust_ref_batch(&digest, -1, &release, 2).unwrap(),
            0,
            "acknowledgement-lost compensation must not underflow"
        );
    }
    let store = RedbChunkStore::open(&path).unwrap();
    assert_eq!(store.refcount(&digest).unwrap(), 0);
    let swept = store.sweep().unwrap();
    assert_eq!(swept.chunks_reclaimed, 1);
    assert!(store.get_chunk(&digest).unwrap().is_none());
}

#[test]
fn stored_manifest_decoder_rejects_allocation_bombs_and_inconsistent_metadata() {
    let allocation_bomb = [0xdd, 0xff, 0xff, 0xff, 0xff];
    assert!(decode_manifest(&allocation_bomb).is_err());

    let inconsistent = BlobManifest {
        schema_version: BLOB_MANIFEST_VERSION,
        owner_scope: ENGINE_BLOB_OWNER_SCOPE.to_string(),
        chunks: vec!["0".repeat(64)],
        chunk_lens: vec![1],
        len: 2,
        chunk_size: 1,
    };
    let encoded = rmp_serde::to_vec_named(&inconsistent).unwrap();
    assert!(decode_manifest(&encoded).is_err());
}

#[test]
fn manifest_key_must_match_content_digest() {
    let store = RedbChunkStore::open_temp().unwrap();
    let manifest = BlobManifest {
        schema_version: BLOB_MANIFEST_VERSION,
        owner_scope: ENGINE_BLOB_OWNER_SCOPE.to_string(),
        chunks: Vec::new(),
        chunk_lens: Vec::new(),
        len: 0,
        chunk_size: 0,
    };
    assert!(store.put_manifest(&"0".repeat(64), &manifest).is_err());
}

fn chunked(store: &dyn ChunkStore, data: &[u8], chunk_size: usize) -> CommittedBlob {
    // Stream the data through the store one chunk at a time (bounded memory),
    // exactly as the protocol cursor does.
    let mut chunks = Vec::new();
    let mut chunk_lens = Vec::new();
    for part in data.chunks(chunk_size) {
        let (digest, _was_new) = store.put_chunk(part).unwrap();
        chunks.push(digest);
        chunk_lens.push(part.len() as u32);
    }
    let manifest = BlobManifest {
        schema_version: BLOB_MANIFEST_VERSION,
        owner_scope: ENGINE_BLOB_OWNER_SCOPE.to_string(),
        chunks,
        chunk_lens,
        len: data.len() as u64,
        chunk_size: chunk_size as u32,
    };
    let bytes = rmp_serde::to_vec_named(&manifest).unwrap();
    let digest = hex_digest(&bytes);
    store.put_manifest(&digest, &manifest).unwrap();
    CommittedBlob { digest, manifest }
}

fn reassemble(store: &dyn ChunkStore, manifest: &BlobManifest) -> Vec<u8> {
    let mut out = Vec::new();
    for d in &manifest.chunks {
        out.extend(store.get_chunk(d).unwrap().unwrap());
    }
    out
}

#[test]
fn roundtrip_integrity_and_content_address_is_stable() {
    let store = RedbChunkStore::open_temp().unwrap();
    let data: Vec<u8> = (0u32..500_000).map(|i| (i % 251) as u8).collect();
    let blob = chunked(&store, &data, 4096);
    assert_eq!(reassemble(&store, &blob.manifest), data);
    // Re-committing identical content yields the SAME blob digest.
    let blob2 = chunked(&store, &data, 4096);
    assert_eq!(blob.digest, blob2.digest);
}

#[test]
fn identical_content_dedups_chunks() {
    let store = RedbChunkStore::open_temp().unwrap();
    let data: Vec<u8> = (0u32..400_000).map(|i| (i % 97) as u8).collect();
    let b1 = chunked(&store, &data, 8192);
    let after_first = store.chunk_count().unwrap();
    let b2 = chunked(&store, &data, 8192);
    let after_second = store.chunk_count().unwrap();
    // The second upload of identical content stored ZERO new chunks.
    assert_eq!(b1.digest, b2.digest);
    assert_eq!(after_first, after_second);
    assert!(after_first > 0);
}

#[test]
fn gc_reclaims_orphan_keeps_referenced_shared() {
    let store = RedbChunkStore::open_temp().unwrap();
    // Two distinct blobs that SHARE a chunk (the first 8KB), plus a chunk
    // unique to each.
    let shared: Vec<u8> = vec![0xAB; 8192];
    let only_a: Vec<u8> = vec![0x11; 8192];
    let only_b: Vec<u8> = vec![0x22; 8192];

    let mut data_a = shared.clone();
    data_a.extend(&only_a);
    let mut data_b = shared.clone();
    data_b.extend(&only_b);

    let a = chunked(&store, &data_a, 8192);
    let b = chunked(&store, &data_b, 8192);
    // 3 distinct chunks: shared, only_a, only_b.
    assert_eq!(store.chunk_count().unwrap(), 3);

    // Reference both blobs (each from one :Media node).
    store.incref(&a.digest).unwrap();
    store.incref(&b.digest).unwrap();
    assert_eq!(store.refcount(&a.digest).unwrap(), 1);

    // Remove the reference to A only.
    assert_eq!(store.decref(&a.digest).unwrap(), 0);
    let stats = store.sweep().unwrap();
    // A's manifest is reclaimed; the shared chunk is KEPT (B still lists it),
    // so only A's unique chunk is reclaimed.
    assert_eq!(stats.blobs_reclaimed, 1);
    assert_eq!(stats.chunks_reclaimed, 1);
    assert!(store.get_manifest(&a.digest).unwrap().is_none());
    assert!(store.get_manifest(&b.digest).unwrap().is_some());
    // B is still fully reassemblable (shared chunk survived).
    assert_eq!(reassemble(&store, &b.manifest), data_b);
    assert_eq!(store.chunk_count().unwrap(), 2); // shared + only_b

    // Now drop B too → everything is reclaimed.
    assert_eq!(store.decref(&b.digest).unwrap(), 0);
    let stats = store.sweep().unwrap();
    assert_eq!(stats.blobs_reclaimed, 1);
    assert_eq!(stats.chunks_reclaimed, 2); // shared + only_b now orphan
    assert_eq!(store.chunk_count().unwrap(), 0);
    assert_eq!(store.blob_count().unwrap(), 0);
}

#[test]
fn shared_blob_survives_until_last_reference_drops() {
    let store = RedbChunkStore::open_temp().unwrap();
    // Two DISTINCT chunks (so chunk_reclaimed is unambiguous, not deduped to 1).
    let mut data: Vec<u8> = vec![0x5A; 8192];
    data.extend(std::iter::repeat_n(0x6B, 8192));
    let blob = chunked(&store, &data, 8192);
    // Two :Media nodes reference the SAME blob (dedup at the blob level).
    store.incref(&blob.digest).unwrap();
    assert_eq!(store.incref(&blob.digest).unwrap(), 2);

    // First reference removed → still referenced → sweep keeps it.
    assert_eq!(store.decref(&blob.digest).unwrap(), 1);
    let stats = store.sweep().unwrap();
    assert_eq!(stats.blobs_reclaimed, 0);
    assert!(store.get_manifest(&blob.digest).unwrap().is_some());

    // Last reference removed → sweep reclaims it.
    assert_eq!(store.decref(&blob.digest).unwrap(), 0);
    let stats = store.sweep().unwrap();
    assert_eq!(stats.blobs_reclaimed, 1);
    assert_eq!(stats.chunks_reclaimed, 2);
}

/// Peak RSS of this process, MB (Linux VmHWM). Used to assert bounded memory.
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

/// Fill a chunk with offset-seeded pseudo-random bytes (xorshift) so chunks are
/// DISTINCT (worst case for dedup — proves real storage) without holding the file.
fn fill_chunk(buf: &mut [u8], idx: u64) {
    let mut x = (idx + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    for b in buf.iter_mut() {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        *b = (x & 0xFF) as u8;
    }
}

/// BOUNDED MEMORY (the flagged correctness risk). Streams a LARGE blob through the
/// group-commit CAS one chunk at a time — generated, stored, dropped — then streams
/// it back re-hashing chunk-by-chunk, retaining NEITHER the whole blob NOR all
/// chunks. Peak RSS must stay near the group window (≈64 MiB at 32× 2 MiB),
/// INDEPENDENT of the blob size: a regression that stopped bounding the window
/// would make RSS track the file size and trip the assert. Default 256 MB (4× the
/// window); `EG_BLOB_RSS_MB` runs 1GB+.
// Measures WHOLE-PROCESS RSS, so it is only meaningful run in isolation: under the
// parallel test harness a sibling 256MB-blob test inflates the shared RSS and trips
// the bound (~347MB observed vs the 320 cap). Ignored by default; the CI
// "memory-regression" step runs it serially (`--ignored --test-threads=1`).
#[test]
#[ignore = "process-global RSS; run isolated via `--ignored --test-threads=1`"]
fn bounded_memory_large_blob_group_commit() {
    let total_mb: u64 = std::env::var("EG_BLOB_RSS_MB")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(256);
    let chunk_size = 2 * 1024 * 1024usize; // 2 MiB
    let n_chunks = (total_mb * 1024 * 1024).div_ceil(chunk_size as u64);
    let store = RedbChunkStore::open_temp().unwrap();

    let rss_before = peak_rss_mb();

    // Upload streaming — one chunk resident at a time.
    let mut digests = Vec::with_capacity(n_chunks as usize);
    let mut src_hash = Sha256::new();
    for i in 0..n_chunks {
        let mut buf = vec![0u8; chunk_size];
        fill_chunk(&mut buf, i);
        src_hash.update(&buf);
        let (digest, _was_new) = store.put_chunk(&buf).unwrap();
        digests.push(digest);
        // buf dropped here — never accumulated.
    }
    let chunk_lens = vec![chunk_size as u32; n_chunks as usize];
    let manifest = BlobManifest {
        schema_version: BLOB_MANIFEST_VERSION,
        owner_scope: ENGINE_BLOB_OWNER_SCOPE.to_string(),
        chunks: digests,
        chunk_lens,
        len: n_chunks * chunk_size as u64,
        chunk_size: chunk_size as u32,
    };
    let mbytes = rmp_serde::to_vec_named(&manifest).unwrap();
    let blob_digest = hex_digest(&mbytes);
    store.put_manifest(&blob_digest, &manifest).unwrap();

    // Download streaming — re-hash chunk-by-chunk, retain nothing.
    let got = store.get_manifest(&blob_digest).unwrap().unwrap();
    let mut dl_hash = Sha256::new();
    for d in &got.chunks {
        let bytes = store.get_chunk(d).unwrap().unwrap();
        dl_hash.update(&bytes);
    }
    assert_eq!(
        hex::encode(src_hash.finalize()),
        hex::encode(dl_hash.finalize()),
        "round-trip integrity over the streamed large blob"
    );

    // Bounded memory: peak RSS GROWTH over the run must be a small multiple of the
    // group window (32×2MiB = 64MiB) plus the redb page cache and transient
    // buffers — NOT the blob size. A regression that stopped bounding the staged
    // group would track the file size (1GB blob → ~1GB RSS) and blow past this.
    let peak = peak_rss_mb();
    let growth = peak.saturating_sub(rss_before);
    assert!(
        growth < 320,
        "peak RSS growth {growth}MB for a {total_mb}MB blob must be bounded by the \
         group window plus the page cache, not the file size"
    );
}

#[test]
fn sweep_reclaims_unreferenced_uploaded_blob() {
    // A blob that was uploaded+committed but never referenced by a node (an
    // abandoned upload) is reclaimed on sweep — no refcount entry == dead.
    let store = RedbChunkStore::open_temp().unwrap();
    let data: Vec<u8> = vec![0x7F; 4096];
    let blob = chunked(&store, &data, 4096);
    assert_eq!(store.blob_count().unwrap(), 1);
    let stats = store.sweep().unwrap();
    assert_eq!(stats.blobs_reclaimed, 1);
    assert_eq!(stats.chunks_reclaimed, 1);
    assert!(store.get_manifest(&blob.digest).unwrap().is_none());
}

/// REVIEW-root-c2-6bee2377 P0-1, the refcount half.
///
/// Concurrent `incref`s observed the same scope version and, under the retired
/// `{event}:v{version}` batch id, built BYTE-IDENTICAL maintenance batches;
/// `commit::begin` checks idempotency before the version expectation, so every
/// loser replayed the winner's record and its increment was LOST. The refcount
/// must equal the number of increments that returned `Ok`.
#[test]
fn concurrent_increfs_never_lose_a_reference() {
    let store = Arc::new(RedbChunkStore::open_temp().unwrap());
    let blob = chunked(store.as_ref(), b"refcounted payload", 8);
    let increfs = 8;
    std::thread::scope(|scope| {
        for _ in 0..increfs {
            let store = Arc::clone(&store);
            let digest = blob.digest.clone();
            scope.spawn(move || {
                store.incref(&digest).expect("a concurrent incref must not fail");
            });
        }
    });
    assert_eq!(
        store.refcount(&blob.digest).unwrap(),
        increfs as u64,
        "a concurrent incref was swallowed"
    );
}

/// The consequence a lost increment has: `sweep_rows` deletes every chunk whose
/// blob is at refcount 0, so a swallowed `incref` is a premature delete of a
/// LIVE chunk. A blob that N threads referenced and N-1 dereferenced is still
/// live, and a sweep must reclaim none of it — its bytes must still read back.
#[test]
fn a_sweep_racing_refcounts_never_deletes_a_live_chunk() {
    let store = Arc::new(RedbChunkStore::open_temp().unwrap());
    let payload = b"live chunk under concurrent refcounting".to_vec();
    let blob = chunked(store.as_ref(), &payload, 8);
    let holders = 8;
    std::thread::scope(|scope| {
        for index in 0..holders {
            let store = Arc::clone(&store);
            let digest = blob.digest.clone();
            scope.spawn(move || {
                store.incref(&digest).expect("incref");
                // Every holder but one hands its reference straight back.
                if index != 0 {
                    store.decref(&digest).expect("decref");
                }
            });
        }
    });
    assert_eq!(store.refcount(&blob.digest).unwrap(), 1);
    let stats = store.sweep().unwrap();
    assert_eq!(
        (stats.blobs_reclaimed, stats.chunks_reclaimed),
        (0, 0),
        "a live blob must not be swept"
    );
    assert_eq!(reassemble(store.as_ref(), &blob.manifest), payload);
}
