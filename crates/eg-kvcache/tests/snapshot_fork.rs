//! Zero-copy snapshot-fork seam tests (CONCEPT:EG-KG.memory.zero-copy-snapshot-fork).
//!
//! The deliverable: an agent warm-fork fan-out must share ONE physical copy of the
//! candidate-set KV pages across N branches (zero-copy), with copy-on-write isolation
//! when a branch writes. These end-to-end tests drive the public crate API exactly as the
//! agent-utilities `crossmodal_fork` `max_concurrency>1` path (and the EG-187 HTTP surface)
//! would:
//!
//! * snapshot over M pages → fork N∈{8,32,128} branches → every branch reads byte-identical
//!   pages while resident/physical bytes stay flat at M pages (NOT N×M — the contrast to the
//!   forkserver per-branch COPY rung);
//! * a branch's copy-on-write write is isolated — siblings never observe it, and it adds
//!   exactly ONE overlay page, not N.

use eg_kvcache::{BranchId, ReleaseOutcome, SharedKvIndex};

/// A distinct M-page "candidate set" of `page`-sized KV pages, put into the shared index.
/// Returns the page keys (content-hash addresses).
fn seed_pages(idx: &mut SharedKvIndex, m: usize, page: usize) -> Vec<String> {
    (0..m)
        .map(|i| {
            let mut bytes = vec![0u8; page];
            // Stamp a few identifying bytes so a stale / cross-page read is detectable.
            bytes[0] = i as u8;
            bytes[page - 1] = (i as u8).wrapping_add(1);
            idx.put_by_content(bytes)
        })
        .collect()
}

/// CONCEPT:EG-KG.memory.zero-copy-snapshot-fork — the headline property: fork N branches over a snapshot
/// of M pages and resident physical bytes stay ≈ M pages regardless of N (zero-copy fan-out),
/// every branch reads identical bytes, and a CoW write is isolated to the writer.
#[test]
fn snapshot_fork_is_zero_copy_across_branch_counts() {
    const M: usize = 4;
    const PAGE: usize = 64 * 1024; // 64 KiB pages — realistic KV-page scale

    for &n in &[8usize, 32, 128] {
        let mut idx = SharedKvIndex::new();
        let keys = seed_pages(&mut idx, M, PAGE);
        // Sanity: the shared index holds exactly M physical pages up front.
        assert_eq!(idx.stats().resident_bytes, M * PAGE);

        // Snapshot the candidate set, then fan out N branches (each fork is O(1), no copy).
        let snap = idx.snapshot(&keys);
        let branches: Vec<BranchId> = (0..n)
            .map(|_| idx.fork(snap).expect("live snapshot"))
            .collect();
        assert_eq!(idx.snapshot_branch_count(snap), Some(n));

        // Every branch reads byte-identical pages for every key.
        for &b in &branches {
            for (i, k) in keys.iter().enumerate() {
                let got = idx.branch_get(b, k).expect("shared page resolves");
                assert_eq!(got.len(), PAGE);
                assert_eq!(got[0], i as u8, "page {i} head byte");
                assert_eq!(
                    got[PAGE - 1],
                    (i as u8).wrapping_add(1),
                    "page {i} tail byte"
                );
            }
        }

        // THE ZERO-COPY PROOF: resident physical fork bytes == M pages, independent of N.
        let fs = idx.fork_stats();
        assert_eq!(fs.branches, n);
        assert_eq!(fs.shared_pages, M, "M distinct shared pages for N={n}");
        assert_eq!(
            fs.resident_fork_bytes,
            M * PAGE,
            "resident stays flat at M pages for N={n}"
        );
        // ...and it strictly beats the naive per-branch-copy baseline (what the forkserver
        // rung pays: one full candidate-set copy PER branch).
        let naive_ncopy = n * M * PAGE;
        assert!(
            fs.resident_fork_bytes < naive_ncopy,
            "zero-copy ({} B) must beat naive N-copy ({} B) for N={n}",
            fs.resident_fork_bytes,
            naive_ncopy
        );

        // COPY-ON-WRITE ISOLATION: one branch overwrites page 0; siblings still see the
        // shared snapshot page, and only ONE extra physical page is now resident.
        let writer = branches[0];
        idx.branch_put(writer, &keys[0], vec![0xAB; PAGE]);
        assert_eq!(
            idx.branch_get(writer, &keys[0]).unwrap()[0],
            0xAB,
            "writer sees its CoW value"
        );
        for &b in &branches[1..] {
            assert_eq!(
                idx.branch_get(b, &keys[0]).unwrap()[0],
                0u8,
                "sibling unaffected by the writer's CoW (isolation)"
            );
        }
        let fs2 = idx.fork_stats();
        assert_eq!(
            fs2.overlay_pages, 1,
            "CoW added exactly one page, not N={n}"
        );
        assert_eq!(
            fs2.resident_fork_bytes,
            (M + 1) * PAGE,
            "resident grew by ONE page for the CoW write, not N"
        );
    }
}

/// CONCEPT:EG-KG.memory.zero-copy-snapshot-fork — branch lifecycle: dropping branches decrements the
/// snapshot's branch count and frees their overlays; releasing the snapshot unpins the
/// captured pages.
#[test]
fn snapshot_fork_lifecycle_frees_cleanly() {
    let mut idx = SharedKvIndex::new();
    let keys = seed_pages(&mut idx, 2, 1024);
    let snap = idx.snapshot(&keys);
    let b = idx.fork(snap).unwrap();
    idx.branch_put(b, &keys[0], vec![9u8; 1024]);
    assert_eq!(idx.fork_stats().overlay_pages, 1);

    assert_eq!(idx.drop_branch(b), ReleaseOutcome::Released);
    assert_eq!(idx.snapshot_branch_count(snap), Some(0));
    assert_eq!(
        idx.fork_stats().overlay_pages,
        0,
        "overlay freed with the branch"
    );

    assert_eq!(idx.release_snapshot(snap), ReleaseOutcome::Released);
    assert_eq!(idx.fork_stats().shared_pages, 0, "snapshot pages released");
    assert_eq!(idx.snapshot_page_count(snap), None, "snapshot gone");
}
