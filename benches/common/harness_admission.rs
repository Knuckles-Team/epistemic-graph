//! The timed admission step and budget report shared by the harness
//! vocabulary benches (MEDULLA-ORCH-VOCAB-R801.6, MEDULLA-ORCH-VOCAB-R802.6).

use std::time::Instant;

use medulla::graph::GraphCore;
use medulla::protocol::Method;
use medulla::server::harness_admission::direct_add_node;

/// Admit write `index` through the direct AddNode path at `tx_now`, project
/// the admitted record into `core`, and return the elapsed nanoseconds.
pub fn admit_and_project(core: &GraphCore, index: usize, method: &Method, tx_now: u64) -> u128 {
    let started = Instant::now();
    let admitted = direct_add_node(method, core, tx_now)
        .unwrap_or_else(|error| panic!("write {index} refused: {error}"));
    let Method::AddNode {
        node_id: stored_id,
        properties_msgpack,
    } = &admitted
    else {
        panic!("admission must keep the AddNode method");
    };
    core.add_node(stored_id.clone(), properties_msgpack.clone());
    started.elapsed().as_nanos()
}

/// Print the p50, p99 and max latency, and exit nonzero when p99 misses the
/// budget.
pub fn report(label: &str, latencies_ns: &[u128], budget_p99_ns: u128) {
    let mut sorted = latencies_ns.to_vec();
    sorted.sort_unstable();
    let p50 = sorted[sorted.len() / 2];
    let p99 = sorted[(sorted.len() as f64 * 0.99) as usize];
    let max = *sorted.last().unwrap();
    println!("{label}:");
    println!("  p50 = {p50} ns");
    println!("  p99 = {p99} ns");
    println!("  max = {max} ns");
    if p99 < budget_p99_ns {
        println!("  PASS (budget: p99 under {budget_p99_ns} ns)");
    } else {
        println!("  FAIL (budget: p99 under {budget_p99_ns} ns)");
        std::process::exit(1);
    }
}
