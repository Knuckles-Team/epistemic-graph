//! Building a [`QuantumProgram`] from an induced subgraph over a candidate set
//! (lane Q5, addendum §1.1: "materializes the induced subgraph").
//!
//! The one workload this crate ships is deliberately small and honestly quantum: a
//! **GHZ-correlation circuit** over one qubit per candidate. Every candidate that is
//! part of the same connected component of the induced subgraph is entangled into a
//! single GHZ state (`H` on one qubit per component, then a `CX` chain along a
//! spanning tree of that component's edges) so that, upon measurement, EVERY qubit in
//! a component agrees on the same random bit across a shot — a correlation that is
//! genuinely quantum (no local hidden-variable/classical-coin-flip scheme reproduces
//! perfectly-correlated-yet-individually-50/50 outcomes without shared entanglement)
//! and analytically checkable: `P(all agree) = 1`, `P(qubit_i = 1) = 0.5`.
//!
//! This is the smallest circuit that: (a) is built FROM a real candidate/edge set
//! rather than hard-coded, (b) is Clifford-only, so planner rule R1 deterministically
//! routes it to the stabilizer backend (never statevector) — an end-to-end,
//! observable proof of R1, not just a unit test against a stub descriptor, and (c)
//! produces a per-candidate score ([`crate::job::consistency_scores`]) that is
//! meaningfully different from "every node gets the same constant" while staying
//! analytically verifiable. Domain workloads beyond this (QAOA Max-Cut, quantum-walk
//! ranking) are Q7/Q11 scope, not this lane's.

use eg_quantum_core::ir::{
    ClassicalBitRef, ClassicalRegister, ControlQubit, ControlState, GateInstruction, GateKind,
    Instruction, ProgramMetadata, QuantumProgram, IR_VERSION,
};

/// Name of the single classical register every program this module builds declares,
/// one bit per qubit, MSB-first (qubit 0 -> leftmost character of the outcome key) —
/// see `eg_quantum_sim::ClassicalMemory::bitstring`.
pub const OUTCOME_REGISTER: &str = "c";

/// Reduce an arbitrary edge list over `n` qubits to a spanning FOREST (one spanning
/// tree per connected component) via a simple union-find, dropping any edge that
/// would close a cycle. A cycle edge would apply a redundant `CX` that partially
/// disentangles the pair it touches (does not corrupt correctness of a *reduced*
/// input, but silently changes the physics from what the caller's edge list implies),
/// so this crate always calls this before building a circuit rather than trusting the
/// caller's edges to already be acyclic.
pub fn spanning_forest(n: u32, edges: &[(u32, u32)]) -> Vec<(u32, u32)> {
    let mut parent: Vec<u32> = (0..n).collect();
    fn find(parent: &mut [u32], x: u32) -> u32 {
        if parent[x as usize] != x {
            parent[x as usize] = find(parent, parent[x as usize]);
        }
        parent[x as usize]
    }
    let mut kept = Vec::new();
    for &(a, b) in edges {
        if a >= n || b >= n || a == b {
            continue; // out-of-range / self-loop edges are never entangling
        }
        let (ra, rb) = (find(&mut parent, a), find(&mut parent, b));
        if ra != rb {
            parent[ra as usize] = rb;
            kept.push((a, b));
        }
    }
    kept
}

/// Every connected component's chosen "root" qubit (the one that gets the initial
/// `H`) — the lowest-index qubit in each component of the spanning forest, PLUS every
/// qubit that has no edges at all (a singleton component, entangled with nothing).
/// Deterministic (sorted) so the same `(n, edges)` always yields the same program.
fn component_roots(n: u32, forest: &[(u32, u32)]) -> Vec<u32> {
    let mut parent: Vec<u32> = (0..n).collect();
    fn find(parent: &mut [u32], x: u32) -> u32 {
        if parent[x as usize] != x {
            parent[x as usize] = find(parent, parent[x as usize]);
        }
        parent[x as usize]
    }
    for &(a, b) in forest {
        let (ra, rb) = (find(&mut parent, a), find(&mut parent, b));
        if ra != rb {
            parent[ra as usize] = rb;
        }
    }
    let mut roots: std::collections::BTreeSet<u32> = std::collections::BTreeSet::new();
    for q in 0..n {
        roots.insert(find(&mut parent, q));
    }
    roots.into_iter().collect()
}

/// Every qubit's connected-component members (per `forest`), grouped — a singleton
/// qubit is its own one-element component. Used by [`crate::job::consistency_scores`]
/// to score each candidate against ITS OWN component's majority outcome rather than a
/// single global majority across unrelated components. Deterministic ordering:
/// components sorted by their lowest member, members sorted ascending within a
/// component.
pub fn components(n: u32, forest: &[(u32, u32)]) -> Vec<Vec<u32>> {
    let mut parent: Vec<u32> = (0..n).collect();
    fn find(parent: &mut [u32], x: u32) -> u32 {
        if parent[x as usize] != x {
            parent[x as usize] = find(parent, parent[x as usize]);
        }
        parent[x as usize]
    }
    for &(a, b) in forest {
        let (ra, rb) = (find(&mut parent, a), find(&mut parent, b));
        if ra != rb {
            parent[ra as usize] = rb;
        }
    }
    let mut grouped: std::collections::BTreeMap<u32, Vec<u32>> = std::collections::BTreeMap::new();
    for q in 0..n {
        grouped.entry(find(&mut parent, q)).or_default().push(q);
    }
    grouped.into_values().collect()
}

/// Build the GHZ-correlation [`QuantumProgram`] over `n_qubits` candidates, entangling
/// every pair the (already-reduced) `spanning_forest` connects. `n_qubits` must be
/// `<= 24` (the `sv-cpu` backend's own safety ceiling — the stabilizer backend this
/// circuit actually routes to has no such limit, but `estimate()`'s `preferred`
/// ordering is computed independent of which backend ends up registered, so callers
/// should not build unbounded programs on the strength of "it'll pick stabilizer
/// anyway").
pub fn induced_subgraph_ghz_program(n_qubits: u32, edges: &[(u32, u32)]) -> QuantumProgram {
    let forest = spanning_forest(n_qubits, edges);
    let mut instructions = Vec::new();
    for root in component_roots(n_qubits, &forest) {
        instructions.push(Instruction::Gate(GateInstruction {
            gate: GateKind::H,
            qubits: vec![root],
            controls: vec![],
            params: vec![],
        }));
    }
    // A breadth-first walk of the forest from each root so every CX's control qubit
    // has already been touched by the H (or a prior CX) before it fires -- otherwise
    // the entangling chain would not actually connect back to a superposed qubit.
    let mut adjacency: std::collections::BTreeMap<u32, Vec<u32>> =
        std::collections::BTreeMap::new();
    for &(a, b) in &forest {
        adjacency.entry(a).or_default().push(b);
        adjacency.entry(b).or_default().push(a);
    }
    let mut visited = vec![false; n_qubits as usize];
    for root in component_roots(n_qubits, &forest) {
        if visited[root as usize] {
            continue;
        }
        let mut queue = std::collections::VecDeque::new();
        queue.push_back(root);
        visited[root as usize] = true;
        while let Some(q) = queue.pop_front() {
            if let Some(neighbors) = adjacency.get(&q) {
                for &nbr in neighbors {
                    if !visited[nbr as usize] {
                        visited[nbr as usize] = true;
                        instructions.push(Instruction::Gate(GateInstruction {
                            gate: GateKind::X,
                            qubits: vec![nbr],
                            controls: vec![ControlQubit {
                                qubit: q,
                                state: ControlState::One,
                            }],
                            params: vec![],
                        }));
                        queue.push_back(nbr);
                    }
                }
            }
        }
    }
    for q in 0..n_qubits {
        instructions.push(Instruction::Measure {
            qubit: q,
            classical_bit: ClassicalBitRef {
                register: OUTCOME_REGISTER.to_string(),
                index: q,
            },
        });
    }
    QuantumProgram {
        ir_version: IR_VERSION,
        n_qubits,
        classical_registers: vec![ClassicalRegister {
            name: OUTCOME_REGISTER.to_string(),
            n_bits: n_qubits,
        }],
        parameters: vec![],
        instructions,
        metadata: ProgramMetadata {
            name: Some("eg-quantum-jobs.induced_subgraph_ghz".to_string()),
            source: Some("eg-quantum-jobs".to_string()),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spanning_forest_drops_cycle_edges() {
        // Triangle 0-1-2: exactly one edge must be dropped to stay acyclic.
        let forest = spanning_forest(3, &[(0, 1), (1, 2), (2, 0)]);
        assert_eq!(forest.len(), 2, "a 3-cycle reduces to a 2-edge tree");
    }

    #[test]
    fn spanning_forest_ignores_out_of_range_and_self_loops() {
        let forest = spanning_forest(3, &[(0, 5), (1, 1), (0, 1)]);
        assert_eq!(forest, vec![(0, 1)]);
    }

    #[test]
    fn program_is_clifford_and_valid() {
        let program = induced_subgraph_ghz_program(4, &[(0, 1), (1, 2), (2, 3)]);
        program.validate().expect("well-formed IR");
        assert!(program.is_clifford(), "H + CX + Measure is Clifford-only");
        assert_eq!(program.n_qubits, 4);
    }

    #[test]
    fn one_h_per_component_and_one_cx_per_forest_edge() {
        // Two components: {0,1,2} (chain) and {3} (singleton).
        let program = induced_subgraph_ghz_program(4, &[(0, 1), (1, 2)]);
        let h_count = program
            .instructions
            .iter()
            .filter(|i| matches!(i, Instruction::Gate(g) if g.gate == GateKind::H))
            .count();
        let cx_count = program
            .instructions
            .iter()
            .filter(|i| matches!(i, Instruction::Gate(g) if g.gate == GateKind::X && !g.controls.is_empty()))
            .count();
        assert_eq!(
            h_count, 2,
            "one H per connected component (incl. singleton)"
        );
        assert_eq!(cx_count, 2, "one CX per spanning-forest edge");
    }

    #[test]
    fn components_groups_by_forest_connectivity() {
        let forest = spanning_forest(5, &[(0, 1), (1, 2), (3, 4)]);
        let mut comps = components(5, &forest);
        for c in &mut comps {
            c.sort_unstable();
        }
        comps.sort();
        assert_eq!(comps, vec![vec![0, 1, 2], vec![3, 4]]);
    }

    #[test]
    fn empty_edges_is_all_singletons() {
        let program = induced_subgraph_ghz_program(3, &[]);
        let h_count = program
            .instructions
            .iter()
            .filter(|i| matches!(i, Instruction::Gate(g) if g.gate == GateKind::H))
            .count();
        assert_eq!(h_count, 3, "every candidate is its own component");
    }
}
