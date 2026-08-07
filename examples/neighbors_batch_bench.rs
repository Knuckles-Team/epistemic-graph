//! Scratch benchmark for D-DPF-1: N `get_neighbors()` calls (lock acquired once
//! PER call) vs one `get_neighbors_batch()` call (lock acquired ONCE for the
//! whole batch). Not a permanent fixture — measures the in-process mechanism
//! only (no socket/RPC round-trip, which is where production cost actually
//! lives; see the deferred-item writeup for the live-pod numbers this closes).
//!
//! Run with: cargo run --release --example neighbors_batch_bench

use epistemic_graph::graph::GraphCore;

fn props() -> Vec<u8> {
    rmp_serde::to_vec_named(&serde_json::json!({})).unwrap()
}

fn main() {
    let core = GraphCore::new();
    let n = 2000usize;
    for i in 0..n {
        core.add_node(format!("n{i}"), props());
    }
    for i in 0..n {
        core.add_edge(format!("n{i}"), format!("n{}", (i + 1) % n), props())
            .unwrap();
    }

    let ids: Vec<String> = (0..n).map(|i| format!("n{i}")).collect();

    // Warm up.
    for id in &ids {
        let _ = core.get_neighbors(id);
    }
    let _ = core.get_neighbors_batch(&ids);

    let iterations = 20usize;

    let t0 = std::time::Instant::now();
    for _ in 0..iterations {
        for id in &ids {
            let _ = core.get_neighbors(id).unwrap();
        }
    }
    let per_node_elapsed = t0.elapsed();

    let t1 = std::time::Instant::now();
    for _ in 0..iterations {
        let _ = core.get_neighbors_batch(&ids);
    }
    let batched_elapsed = t1.elapsed();

    println!("nodes = {n}, iterations = {iterations}");
    println!(
        "per-node get_neighbors() x {n}: {:?} total, {:?} avg/iteration ({} lock acquisitions/iteration)",
        per_node_elapsed,
        per_node_elapsed / iterations as u32,
        n,
    );
    println!(
        "get_neighbors_batch({n} ids): {:?} total, {:?} avg/iteration (1 lock acquisition/iteration)",
        batched_elapsed,
        batched_elapsed / iterations as u32,
    );
    let speedup = per_node_elapsed.as_secs_f64() / batched_elapsed.as_secs_f64();
    println!("in-process speedup (lock-acquisition overhead only): {speedup:.2}x");
}
