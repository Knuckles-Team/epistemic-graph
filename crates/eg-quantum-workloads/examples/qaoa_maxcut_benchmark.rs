//! Q11 charter: "benchmarked on REALISTIC graph sizes, not toy circuits." Runs the
//! full UQL-pull -> QAOA -> commit pipeline at a ladder of candidate-subgraph sizes
//! up to `eg-quantum-sim::StateVectorSimulator`'s own `max_qubits_statevector` cap
//! (24), and prints REAL measured `wall_time_ms`/`peak_memory_bytes` straight off
//! each run's `QuantumResult` (never synthesized) plus the classical search cost
//! (`search_evaluations`) alongside it — QAOA's classical optimization loop is
//! usually the dominant cost at these sizes, not the one final quantum submission,
//! and this benchmark reports both honestly rather than only the flattering number.
//!
//! Run: `cargo run -p eg-quantum-workloads --release --example qaoa_maxcut_benchmark`

use std::time::Instant;

use eg_quantum_workloads::demo::build_concept_graph;
use eg_quantum_workloads::{pull_candidate_subgraph, run_qaoa_maxcut_local_sim, QaoaConfig};

fn main() {
    println!(
        "{:>8} {:>6} {:>8} {:>12} {:>14} {:>16} {:>10} {:>10} {:>10}",
        "n_qubits",
        "edges",
        "shots",
        "search_evals",
        "search_ms",
        "quantum_ms",
        "peak_MB",
        "cut_val",
        "approx_ratio"
    );

    // Node counts spanning the NISQ-sized charter minimum (8) up through the
    // StateVectorSimulator's actual max_qubits_statevector=24 ceiling -- "realistic"
    // here means "as large as this milestone's local-sim backend actually supports",
    // per Q11's own charter framing (a future Q3/Q4 backend raises this ceiling; this
    // benchmark is written to re-run unchanged against it).
    let sizes = [8usize, 12, 16, 20, 22];
    let universe_n = 60; // more nodes than any candidate pull, like a real KG.
    let edge_prob = 0.12;

    let core = build_concept_graph(universe_n, edge_prob, 7);

    for &n in &sizes {
        let uql = format!("MATCH (:Concept) |> LIMIT {n}");
        let subgraph = match pull_candidate_subgraph(&core, &uql, 24) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("n={n}: subgraph pull failed: {e}");
                continue;
            }
        };

        // Keep the grid coarse at larger n -- the search cost is O(grid_resolution^2
        // * 2^n * |E|) per layer, and this benchmark's job is to report that cost
        // honestly, not to hide it behind an unrealistically fine grid.
        let grid_resolution = if n <= 12 { 14 } else { 8 };
        let config = QaoaConfig {
            p: 1,
            grid_resolution,
            shots: 512,
            seed: 2026,
        };

        let search_start = Instant::now();
        let run = match run_qaoa_maxcut_local_sim(&subgraph, &config) {
            Ok(r) => r,
            Err(e) => {
                eprintln!("n={n}: QAOA run failed: {e}");
                continue;
            }
        };
        let wall_ms = search_start.elapsed().as_millis();
        // wall_ms includes BOTH the classical search AND the one quantum submission;
        // the quantum submission's own wall time is separately, exactly reported by
        // the backend itself on the QuantumResult (never estimated).
        let quantum_ms = run.quantum_result.wall_time_ms;
        let search_ms = wall_ms.saturating_sub(quantum_ms as u128);
        let peak_mb = run.quantum_result.peak_memory_bytes as f64 / (1024.0 * 1024.0);

        println!(
            "{:>8} {:>6} {:>8} {:>12} {:>14} {:>16} {:>10.3} {:>10.3} {:>10}",
            n,
            subgraph.edges.len(),
            config.shots,
            run.search_evaluations,
            search_ms,
            quantum_ms,
            peak_mb,
            run.best_cut_value,
            run.approx_ratio
                .map(|r| format!("{r:.3}"))
                .unwrap_or_else(|| "n/a".to_string()),
        );
    }
}
