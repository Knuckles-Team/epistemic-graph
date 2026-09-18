//! Unit tests for the blob CAS store's owner-write paths.

use super::manifest::decode_manifest;
use super::test_support::{chunked, fresh, reassemble, Operation};
use super::*;
use std::sync::Arc;

/// A pass far past every grace period and upload TTL: what the defaults would
/// reclaim once enough time has gone by.
fn sweep_after_grace(store: &dyn ChunkStore, id: &str) -> SweepStats {
    store
        .sweep_batch(&SweepRequest::default(), &fresh(store, id), u64::MAX)
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
        let acquire = Operation::new(&store, "acquire");
        assert_eq!(
            store
                .put_chunk_ref_batch(body, &acquire.batch(), 1)
                .unwrap(),
            (digest.clone(), true, 1)
        );
        let consumed = store
            .put_chunk_ref_batch(body, &acquire.batch(), 1)
            .expect_err("the committed acquire nonce must be consumed");
        assert!(consumed.contains("REPLAY_NONCE_CONSUMED"), "{consumed}");
        assert_eq!(store.refcount(&digest).unwrap(), 1);
        assert_eq!(
            store
                .put_chunk_ref_batch(body, &acquire.attempt(1), 2)
                .unwrap(),
            (digest.clone(), true, 1),
            "a fresh-nonce acknowledgement-lost acquire must replay without another ref"
        );

        let release = Operation::new(&store, "release");
        assert_eq!(
            store
                .adjust_ref_batch(&digest, -1, &release.batch(), 3)
                .unwrap(),
            0
        );
        let consumed = store
            .adjust_ref_batch(&digest, -1, &release.batch(), 3)
            .expect_err("the committed release nonce must be consumed");
        assert!(consumed.contains("REPLAY_NONCE_CONSUMED"), "{consumed}");
        assert_eq!(store.refcount(&digest).unwrap(), 0);
        assert_eq!(
            store
                .adjust_ref_batch(&digest, -1, &release.attempt(1), 4)
                .unwrap(),
            0,
            "a fresh-nonce acknowledgement-lost compensation must replay without underflow"
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
    let stats = sweep_after_grace(&store, "sweep-a");
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
    let stats = sweep_after_grace(&store, "sweep-b");
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
    let stats = sweep_after_grace(&store, "sweep-held");
    assert_eq!(stats.blobs_reclaimed, 0);
    assert!(store.get_manifest(&blob.digest).unwrap().is_some());

    // Last reference removed → sweep reclaims it.
    assert_eq!(store.decref(&blob.digest).unwrap(), 0);
    let stats = sweep_after_grace(&store, "sweep-released");
    assert_eq!(stats.blobs_reclaimed, 1);
    assert_eq!(stats.chunks_reclaimed, 2);
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

    let rss = super::peak_rss::PeakRssWindow::open();

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
    let growth = rss.growth_mb();
    assert!(
        growth < 320,
        "peak RSS growth {growth}MB for a {total_mb}MB blob must be bounded by the \
         group window plus the page cache, not the file size"
    );
}

#[test]
fn an_unreferenced_committed_blob_survives_until_its_grace_has_passed() {
    // A blob that was committed but not yet referenced is inside the window
    // between `BlobCommit` and its first `BlobRef`: a sweep now must keep it, and
    // only a sweep past the grace period reclaims it.
    let store = RedbChunkStore::open_temp().unwrap();
    let data: Vec<u8> = vec![0x7F; 4096];
    let blob = chunked(&store, &data, 4096);
    assert_eq!(store.sweep().unwrap(), SweepStats::default());
    assert!(store.get_manifest(&blob.digest).unwrap().is_some());
    let stats = sweep_after_grace(&store, "sweep-after-grace");
    assert_eq!((stats.blobs_reclaimed, stats.chunks_reclaimed), (1, 1));
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
                store
                    .incref(&digest)
                    .expect("a concurrent incref must not fail");
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
    let stats = sweep_after_grace(store.as_ref(), "sweep-live");
    assert_eq!(
        (stats.blobs_reclaimed, stats.chunks_reclaimed),
        (0, 0),
        "a live blob must not be swept"
    );
    assert_eq!(reassemble(store.as_ref(), &blob.manifest), payload);
}

/// A production `RedbChunkStore` reference mutation fails closed on underflow:
/// the refusal aborts the surrounding maintenance transaction, leaving the
/// reference count and the authoritative scope version unchanged.
#[test]
fn reference_underflow_aborts_the_production_store_transaction() {
    let store = RedbChunkStore::open_temp().unwrap();
    let blob = chunked(&store, b"underflow payload", 8);
    let version_before =
        eg_transaction::version(&store.kernel.read_scope(&store.bootstrap).unwrap()).unwrap();

    let error = store
        .decref(&blob.digest)
        .expect_err("an absent engine reference must not saturate to zero");
    assert!(
        error.contains("blob holder reference count underflow"),
        "{error}"
    );
    assert_eq!(store.refcount(&blob.digest).unwrap(), 0);
    assert_eq!(
        eg_transaction::version(&store.kernel.read_scope(&store.bootstrap).unwrap()).unwrap(),
        version_before,
        "a refused reference write must not commit its maintenance batch"
    );
    assert_eq!(store.incref(&blob.digest).unwrap(), 1);
}

#[test]
fn a_reference_to_nothing_stored_is_refused() {
    let store = RedbChunkStore::open_temp().unwrap();
    let error = store.incref(&"ab".repeat(32)).unwrap_err();
    assert!(error.contains("unknown blob digest"), "{error}");
    assert_eq!(store.refcount(&"ab".repeat(32)).unwrap(), 0);
}

// ── X2–X6 planted regressions ───────────────────────────────────────────────
//
// Each test below is named for the PACK-IMPORT-DESIGN-DRAFT.md §13 finding it
// proves fixed. Run against base `5655fa767` (the old store, no grace period,
// no holder rows, no cursor high-water, and a live-chunk set built only from
// surviving manifests) every one of them either fails its assertion or fails
// to compile against the removed API — see `REVIEW-HOTSPOTS.md`.

// X2: `an_unreferenced_committed_blob_survives_until_its_grace_has_passed`
// above is the planted test for this finding (a sweep right after commit, with
// no grace elapsed, must reclaim nothing).

/// X3: a chunk shared between a manifest that becomes dead and a SEPARATE direct
/// reference (`put_chunk_ref_batch`, the obs/lake fast path) must survive a
/// sweep that reclaims the dead manifest. At base, `orphan_manifest_chunks`
/// collects every chunk a dead manifest names that no *surviving manifest*
/// lists, with no check of the chunk's own refcount — so a chunk also held
/// directly is deleted out from under its holder.
#[test]
fn a_chunk_shared_with_a_direct_reference_survives_the_manifest_that_named_it() {
    let store = RedbChunkStore::open_temp().unwrap();
    let shared_bytes = vec![0x42u8; 4096];

    // The direct-artifact fast path: refcount 1, keyed by the chunk's own digest.
    let acquire = Operation::new(&store, "direct-acquire");
    let (chunk_digest, _new, refcount) = store
        .put_chunk_ref_batch(&shared_bytes, &acquire.batch(), 1)
        .unwrap();
    assert_eq!(refcount, 1);

    // A manifest that ALSO names that exact chunk digest (identical bytes),
    // referenced once, then immediately released — eligible for the sweep.
    let manifest = BlobManifest {
        schema_version: BLOB_MANIFEST_VERSION,
        owner_scope: ENGINE_BLOB_OWNER_SCOPE.to_string(),
        chunks: vec![chunk_digest.clone()],
        chunk_lens: vec![shared_bytes.len() as u32],
        len: shared_bytes.len() as u64,
        chunk_size: shared_bytes.len() as u32,
    };
    let manifest_digest = hex_digest(&rmp_serde::to_vec_named(&manifest).unwrap());
    store.put_manifest(&manifest_digest, &manifest).unwrap();
    store.incref(&manifest_digest).unwrap();
    assert_eq!(store.decref(&manifest_digest).unwrap(), 0);

    // The manifest is dead (no holder, grace elapsed); the chunk is still held
    // directly and must survive.
    let stats = sweep_after_grace(&store, "sweep-shared-with-direct-ref");
    assert_eq!(stats.blobs_reclaimed, 1, "the dead manifest is reclaimed");
    assert_eq!(
        stats.chunks_reclaimed, 0,
        "the chunk still has a live direct reference"
    );
    assert!(store.get_manifest(&manifest_digest).unwrap().is_none());
    assert_eq!(
        store.get_chunk(&chunk_digest).unwrap().unwrap(),
        shared_bytes
    );
    assert_eq!(store.refcount(&chunk_digest).unwrap(), 1);
}

/// X4: an upload that begins and receives chunks but is never committed is
/// reclaimed once idle past the upload TTL — the upload row AND its chunks
/// that nothing else reaches — and left alone while still within the TTL. At
/// base, `sweep_rows` never reads `cas_uploads` at all.
#[test]
fn an_abandoned_upload_is_reclaimed_past_its_ttl_and_kept_before_it() {
    let store = RedbChunkStore::open_temp().unwrap();
    let cursor = test_support::begin_with_parts(
        &store,
        1,
        "carrier-owner:abandoned",
        &[b"only-in-upload".as_slice()],
        1_000,
    );
    assert_eq!(store.chunk_count().unwrap(), 1);

    // Still within the TTL: a sweep must not touch it.
    let request = SweepRequest::new(GcOwnerScope::AllOwners, BlobRetentionPolicy::default());
    let kept = store
        .sweep_batch(&request, &fresh(&store, "sweep-within-ttl"), 1_000 + 1)
        .unwrap();
    assert_eq!(kept, SweepStats::default());
    assert!(store.load_upload(cursor).unwrap().is_some());

    // Past the TTL: the upload row and its orphaned chunk are reclaimed.
    let past_ttl = 1_000 + BlobRetentionPolicy::default().upload_ttl_ms();
    let swept = store
        .sweep_batch(&request, &fresh(&store, "sweep-past-ttl"), past_ttl)
        .unwrap();
    assert_eq!(swept.uploads_expired, 1);
    assert_eq!(swept.chunks_reclaimed, 1);
    assert!(store.load_upload(cursor).unwrap().is_none());
    assert_eq!(store.chunk_count().unwrap(), 0);
}

/// X4 (owner-scoped): an abandoned upload of a DIFFERENT owner than the sweep
/// covers is left alone even past the TTL.
#[test]
fn an_abandoned_upload_outside_the_swept_owner_is_kept() {
    let store = RedbChunkStore::open_temp().unwrap();
    let cursor = test_support::begin_with_parts(
        &store,
        1,
        "carrier-owner:kept",
        &[b"untouched".as_slice()],
        1_000,
    );
    let past_ttl = 1_000 + BlobRetentionPolicy::default().upload_ttl_ms();
    let request = SweepRequest::new(
        GcOwnerScope::owner("carrier-owner:someone-else").unwrap(),
        BlobRetentionPolicy::default(),
    );
    let swept = store
        .sweep_batch(&request, &fresh(&store, "sweep-other-owner"), past_ttl)
        .unwrap();
    assert_eq!(swept, SweepStats::default());
    assert!(store.load_upload(cursor).unwrap().is_some());
}

/// X5: a cursor id names exactly one upload for the life of the store. Base's
/// `begin_upload_batch` silently ADOPTS a row that already exists under the
/// requested id (a no-op returning `Ok`), which is exactly the collision the
/// finding describes: a second actor's `begin_upload_batch` under a reused id
/// (what an in-memory allocator resets to after a restart) appends to the
/// first actor's still-open upload instead of failing.
#[test]
fn a_cursor_id_names_exactly_one_upload_for_the_life_of_the_store() {
    let store = RedbChunkStore::open_temp().unwrap();
    store
        .begin_upload_batch(
            5,
            8,
            "carrier-owner:first",
            &fresh(&store, "begin-first"),
            1,
        )
        .unwrap();
    let collision = store.begin_upload_batch(
        5,
        8,
        "carrier-owner:second-actor-after-restart",
        &fresh(&store, "begin-second"),
        2,
    );
    let error = collision.expect_err("a reused cursor id must be refused, not adopted");
    assert!(error.contains("already in use"), "{error}");

    // The first actor's upload is exactly as it was: untouched by the collision.
    let manifest = store.load_upload(5).unwrap().unwrap();
    assert_eq!(manifest.owner_scope, "carrier-owner:first");
}

/// X5 (restart, store level): the durable high-water mark moves in the SAME
/// transaction as `begin_upload_batch` and survives a close/reopen, so a fresh
/// allocator seeded from it (see `mod.rs`'s
/// `a_fresh_allocator_over_a_restarted_store_never_proposes_a_used_id`) never
/// proposes an id a prior process ever began.
#[test]
fn the_upload_cursor_high_water_mark_survives_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_string_lossy().to_string();
    {
        let store = RedbChunkStore::open(&path).unwrap();
        store
            .begin_upload_batch(3, 8, "carrier-owner:a", &fresh(&store, "begin-3"), 1)
            .unwrap();
        store
            .begin_upload_batch(9, 8, "carrier-owner:a", &fresh(&store, "begin-9"), 2)
            .unwrap();
        assert_eq!(store.upload_cursor_high_water().unwrap(), 9);
    }
    let reopened = RedbChunkStore::open(&path).unwrap();
    assert_eq!(reopened.upload_cursor_high_water().unwrap(), 9);
    // A cursor below the mark is still refused even though its own row
    // survived the restart too (id 3 is an open upload, not a free id).
    let reused = reopened.begin_upload_batch(
        3,
        8,
        "carrier-owner:b",
        &fresh(&reopened, "begin-3-again"),
        3,
    );
    assert!(reused.is_err());
}

/// X6: a named holder's acquire is idempotent across DISTINCT operations (not
/// only nonce replays of the same one) — the owner scope itself is the dedup
/// key, so two genuinely separate `BlobRef` calls from the same owner take one
/// reference, and two separate `BlobUnref` calls after only one acquire never
/// underflow.
#[test]
fn a_named_holder_acquire_and_release_are_idempotent_across_distinct_operations() {
    let store = RedbChunkStore::open_temp().unwrap();
    let blob = chunked(&store, b"holder-scoped payload", 8);
    let holder = HolderId::for_owner("carrier-owner:reader").unwrap();

    let first = store
        .holder_batch(
            &HolderChange::acquire(&blob.digest, holder.clone(), "carrier-owner:reader").unwrap(),
            &fresh(&store, "acquire-1"),
            1,
        )
        .unwrap();
    assert_eq!(
        first,
        HolderOutcome {
            holders: 1,
            changed: true
        }
    );

    // A SECOND, textually distinct operation (different batch id/nonce) taking
    // the SAME named reference must be a no-op, not a second count.
    let second = store
        .holder_batch(
            &HolderChange::acquire(&blob.digest, holder.clone(), "carrier-owner:reader").unwrap(),
            &fresh(&store, "acquire-2"),
            2,
        )
        .unwrap();
    assert_eq!(
        second,
        HolderOutcome {
            holders: 1,
            changed: false
        }
    );
    assert_eq!(store.refcount(&blob.digest).unwrap(), 1);

    let released = store
        .holder_batch(
            &HolderChange::release(&blob.digest, holder.clone()).unwrap(),
            &fresh(&store, "release-1"),
            3,
        )
        .unwrap();
    assert_eq!(
        released,
        HolderOutcome {
            holders: 0,
            changed: true
        }
    );

    // A second, distinct release of an already-released holder never underflows.
    let released_again = store
        .holder_batch(
            &HolderChange::release(&blob.digest, holder).unwrap(),
            &fresh(&store, "release-2"),
            4,
        )
        .unwrap();
    assert_eq!(
        released_again,
        HolderOutcome {
            holders: 0,
            changed: false
        }
    );
    assert_eq!(store.refcount(&blob.digest).unwrap(), 0);
}

/// X6: `BlobRef`/`BlobUnref` as the handler actually wires them —
/// `HolderChange::owner_acquire`/`owner_release` — round-trip the same
/// idempotency at the owner-scope convenience constructor.
#[test]
fn owner_acquire_and_release_are_the_named_holder_path() {
    let store = RedbChunkStore::open_temp().unwrap();
    let blob = chunked(&store, b"owner-scoped payload", 8);
    let acquire = HolderChange::owner_acquire(&blob.digest, "carrier-owner:writer").unwrap();
    store
        .holder_batch(&acquire, &fresh(&store, "owner-acquire"), 1)
        .unwrap();
    store
        .holder_batch(&acquire, &fresh(&store, "owner-acquire-again"), 2)
        .unwrap();
    assert_eq!(store.refcount(&blob.digest).unwrap(), 1);

    let release = HolderChange::owner_release(&blob.digest, "carrier-owner:writer").unwrap();
    store
        .holder_batch(&release, &fresh(&store, "owner-release"), 3)
        .unwrap();
    assert_eq!(store.refcount(&blob.digest).unwrap(), 0);
    let stats = sweep_after_grace(&store, "sweep-after-owner-release");
    assert_eq!(stats.blobs_reclaimed, 1);
}

// ── Crash-point and restart tests ───────────────────────────────────────────
//
// Every blob write passes the four `MutationCommitPhase` boundaries in
// `RedbChunkStore::complete_write`. Arming one with `killpoint::arm` injects a
// failure exactly there; a kill BEFORE the redb commit must leave the store
// byte-for-byte as it was (the whole write is one redb transaction, aborted on
// drop). A retry with the IDENTICAL nonce is refused
// (`REPLAY_NONCE_CONSUMED`, proven by `direct_ref_acquire_compensation_and_gc_are_restart_replay_safe`)
// whether or not anything committed — a nonce names one attempt, not one
// logical operation — so every retry below uses a FRESH nonce of the SAME
// idempotency key, exactly as a real caller's retry would.
// A kill BEFORE commit must let that fresh-nonce retry apply as a first
// attempt (nothing committed). A kill AFTER the commit
// (`AfterCommitBeforeAck`) simulates the ack never reaching the caller: the
// write IS durable, so the fresh-nonce retry must REPLAY the committed result
// rather than re-apply it, and the data survives a full close/reopen of the
// store.

fn assert_holder_write_is_all_or_nothing_at(phase: crate::mutation_batch::MutationCommitPhase) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_string_lossy().to_string();
    let digest;
    {
        let store = RedbChunkStore::open(&path).unwrap();
        let blob = chunked(&store, b"crash-point payload", 8);
        digest = blob.digest.clone();
        let version_before =
            eg_transaction::version(&store.kernel.read_scope(&store.bootstrap).unwrap()).unwrap();

        let acquire = HolderChange::owner_acquire(&digest, "carrier-owner:crash").unwrap();
        let operation = Operation::new(&store, "crash-acquire");
        super::killpoint::arm(Some(phase));
        let killed = store.holder_batch(&acquire, &operation.attempt(0), 1);
        assert!(
            killed.is_err(),
            "the injected failure at {phase:?} must surface"
        );

        if phase == crate::mutation_batch::MutationCommitPhase::AfterCommitBeforeAck {
            // The write landed; only the ack was lost. A fresh-nonce retry of
            // the SAME idempotency key must replay the committed outcome, not
            // double-acquire.
            let replayed = store
                .holder_batch(&acquire, &operation.attempt(1), 1)
                .unwrap();
            assert_eq!(
                replayed,
                HolderOutcome {
                    holders: 1,
                    changed: true
                }
            );
            assert_eq!(store.refcount(&digest).unwrap(), 1);
        } else {
            // Nothing committed: the scope version is untouched, and a
            // fresh-nonce retry of the same idempotency key applies as a first
            // attempt.
            assert_eq!(
                eg_transaction::version(&store.kernel.read_scope(&store.bootstrap).unwrap())
                    .unwrap(),
                version_before,
                "a killed write before commit must not advance the scope version"
            );
            let retried = store
                .holder_batch(&acquire, &operation.attempt(1), 1)
                .unwrap();
            assert_eq!(
                retried,
                HolderOutcome {
                    holders: 1,
                    changed: true
                }
            );
            assert_eq!(store.refcount(&digest).unwrap(), 1);
        }
    }
    // Restart: the surviving state (held once, never twice) is exactly what a
    // fresh open reads back.
    let reopened = RedbChunkStore::open(&path).unwrap();
    assert_eq!(reopened.refcount(&digest).unwrap(), 1);
}

#[test]
fn a_kill_before_any_row_write_leaves_the_store_untouched_and_replays_clean() {
    assert_holder_write_is_all_or_nothing_at(
        crate::mutation_batch::MutationCommitPhase::BeforeRows,
    );
}

#[test]
fn a_kill_after_rows_before_metadata_rolls_back_the_whole_transaction() {
    assert_holder_write_is_all_or_nothing_at(
        crate::mutation_batch::MutationCommitPhase::AfterRowsBeforeMetadata,
    );
}

#[test]
fn a_kill_before_commit_rolls_back_the_whole_transaction() {
    assert_holder_write_is_all_or_nothing_at(
        crate::mutation_batch::MutationCommitPhase::BeforeCommit,
    );
}

#[test]
fn a_kill_after_commit_before_ack_leaves_a_durable_write_that_replays_once() {
    assert_holder_write_is_all_or_nothing_at(
        crate::mutation_batch::MutationCommitPhase::AfterCommitBeforeAck,
    );
}
