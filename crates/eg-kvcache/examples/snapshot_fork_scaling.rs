//! Timing + resident-bytes harness for the zero-copy snapshot-fork primitive
//! (CONCEPT:EG-KG.memory.zero-copy-snapshot-fork).
//!
//! Shows that forking N∈{8,32,128} branches over a snapshot of M KV pages is O(1) in
//! copies — resident physical bytes stay FLAT at the shared-page total as N grows, versus
//! the naive per-branch-copy baseline (`N × M × page`) that the local forkserver rung pays
//! (each branch receives its own serialized copy of the candidate set, 18–43 ms/branch of
//! COPY in the phase-2 benchmark).
//!
//! Run:  cargo run -p eg-kvcache --example snapshot_fork_scaling --release

use std::time::Instant;

use eg_kvcache::SharedKvIndex;

fn main() {
    const M: usize = 8; // pages in the candidate set
    const PAGE: usize = 64 * 1024; // 64 KiB per KV page
    let snapshot_bytes = M * PAGE;

    println!(
        "snapshot-fork scaling — M={M} pages × {PAGE} B = {} KiB candidate set\n",
        snapshot_bytes / 1024
    );
    println!(
        "{:>3}  {:>14}  {:>16}  {:>16}  {:>10}",
        "N", "fork_time", "resident_zerocopy", "naive_N_copy", "savings"
    );
    println!("{}", "-".repeat(70));

    for &n in &[8usize, 32, 128] {
        let mut idx = SharedKvIndex::new();
        // Seed the M-page candidate set once.
        let keys: Vec<String> = (0..M)
            .map(|i| {
                let mut p = vec![0u8; PAGE];
                p[0] = i as u8;
                idx.put_by_content(p)
            })
            .collect();

        let snap = idx.snapshot(&keys);

        // Time the fan-out: N O(1) forks, no page copies.
        let t0 = Instant::now();
        let branches: Vec<_> = (0..n).map(|_| idx.fork(snap).unwrap()).collect();
        let fork_time = t0.elapsed();

        // Touch every page from every branch to prove reads resolve (still zero-copy).
        let mut checksum = 0u64;
        for &b in &branches {
            for k in &keys {
                checksum += idx.branch_get(b, k).unwrap()[0] as u64;
            }
        }
        std::hint::black_box(checksum);

        let fs = idx.fork_stats();
        let naive = n * snapshot_bytes;
        let savings = 100.0 * (1.0 - fs.resident_fork_bytes as f64 / naive as f64);

        println!(
            "{:>3}  {:>12.1?}  {:>13} KiB  {:>13} KiB  {:>8.1}%",
            n,
            fork_time,
            fs.resident_fork_bytes / 1024,
            naive / 1024,
            savings
        );

        assert_eq!(
            fs.resident_fork_bytes, snapshot_bytes,
            "resident MUST stay flat at M pages (zero-copy) for N={n}"
        );
    }

    println!(
        "\nresident_zerocopy is FLAT (= M pages) across N — the fork adds pointer bumps, not \
         page copies.\nnaive_N_copy is the forkserver per-branch-copy rung this replaces."
    );
}
