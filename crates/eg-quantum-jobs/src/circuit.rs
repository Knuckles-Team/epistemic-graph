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
use std::collections::{BTreeMap, BTreeSet, VecDeque};

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
    let mut kept = Vec::new();
    for &(a, b) in edges {
        if a >= n || b >= n || a == b {
            continue; // out-of-range / self-loop edges are never entangling
        }
        if union_sets(&mut parent, a, b) {
            kept.push((a, b));
        }
    }
    kept
}

/// Union-find `find` with path compression over a qubit `parent` array.
fn find_set(parent: &mut [u32], x: u32) -> u32 {
    if parent[x as usize] != x {
        parent[x as usize] = find_set(parent, parent[x as usize]);
    }
    parent[x as usize]
}

/// Merge `a`'s set into `b`'s; returns whether they were previously disjoint.
fn union_sets(parent: &mut [u32], a: u32, b: u32) -> bool {
    let (ra, rb) = (find_set(parent, a), find_set(parent, b));
    if ra != rb {
        parent[ra as usize] = rb;
    }
    ra != rb
}

/// The union-find `parent` array after merging every `forest` edge over `n` qubits.
fn forest_parents(n: u32, forest: &[(u32, u32)]) -> Vec<u32> {
    let mut parent: Vec<u32> = (0..n).collect();
    for &(a, b) in forest {
        union_sets(&mut parent, a, b);
    }
    parent
}

/// Every connected component's chosen "root" qubit (the one that gets the initial
/// `H`) — the lowest-index qubit in each component of the spanning forest, PLUS every
/// qubit that has no edges at all (a singleton component, entangled with nothing).
/// Deterministic (sorted) so the same `(n, edges)` always yields the same program.
fn component_roots(n: u32, forest: &[(u32, u32)]) -> Vec<u32> {
    let mut parent = forest_parents(n, forest);
    let roots: BTreeSet<u32> = (0..n).map(|q| find_set(&mut parent, q)).collect();
    roots.into_iter().collect()
}

/// Every qubit's connected-component members (per `forest`), grouped — a singleton
/// qubit is its own one-element component. Used by [`crate::job::consistency_scores`]
/// to score each candidate against ITS OWN component's majority outcome rather than a
/// single global majority across unrelated components. Deterministic ordering:
/// components sorted by their lowest member, members sorted ascending within a
/// component.
pub fn components(n: u32, forest: &[(u32, u32)]) -> Vec<Vec<u32>> {
    let mut parent = forest_parents(n, forest);
    let mut grouped: BTreeMap<u32, Vec<u32>> = BTreeMap::new();
    for q in 0..n {
        grouped.entry(find_set(&mut parent, q)).or_default().push(q);
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
    let roots = component_roots(n_qubits, &forest);
    let mut instructions: Vec<Instruction> = roots.iter().map(|&root| hadamard(root)).collect();
    // A breadth-first walk of the forest from each root so every CX's control qubit
    // has already been touched by the H (or a prior CX) before it fires -- otherwise
    // the entangling chain would not actually connect back to a superposed qubit.
    let adjacency = forest_adjacency(&forest);
    let mut visited = vec![false; n_qubits as usize];
    for &root in &roots {
        if !visited[root as usize] {
            push_breadth_first_cx_chain(root, &adjacency, &mut visited, &mut instructions);
        }
    }
    instructions.extend((0..n_qubits).map(measure_into_outcome_register));
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

fn hadamard(qubit: u32) -> Instruction {
    Instruction::Gate(GateInstruction {
        gate: GateKind::H,
        qubits: vec![qubit],
        controls: vec![],
        params: vec![],
    })
}

fn measure_into_outcome_register(qubit: u32) -> Instruction {
    Instruction::Measure {
        qubit,
        classical_bit: ClassicalBitRef {
            register: OUTCOME_REGISTER.to_string(),
            index: qubit,
        },
    }
}

/// Undirected adjacency lists of `forest`, neighbors in edge order.
fn forest_adjacency(forest: &[(u32, u32)]) -> BTreeMap<u32, Vec<u32>> {
    let mut adjacency: BTreeMap<u32, Vec<u32>> = BTreeMap::new();
    for &(a, b) in forest {
        adjacency.entry(a).or_default().push(b);
        adjacency.entry(b).or_default().push(a);
    }
    adjacency
}

/// Visit `root`'s component breadth-first, emitting one `CX(parent, child)` per newly
/// reached qubit.
fn push_breadth_first_cx_chain(
    root: u32,
    adjacency: &BTreeMap<u32, Vec<u32>>,
    visited: &mut [bool],
    instructions: &mut Vec<Instruction>,
) {
    let mut queue = VecDeque::new();
    queue.push_back(root);
    visited[root as usize] = true;
    while let Some(q) = queue.pop_front() {
        let neighbors = adjacency.get(&q).map(Vec::as_slice).unwrap_or_default();
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

    /// A compact `(gate, targets, controls)` view of every gate instruction.
    fn gate_shapes(program: &QuantumProgram) -> Vec<(GateKind, Vec<u32>, Vec<u32>)> {
        program
            .instructions
            .iter()
            .filter_map(|i| match i {
                Instruction::Gate(g) => Some((
                    g.gate.clone(),
                    g.qubits.clone(),
                    g.controls.iter().map(|c| c.qubit).collect(),
                )),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn ghz_program_instruction_sequence_is_pinned() {
        // Forest keeps (0,1), (1,2), (3,4) and drops the cycle edge (2,0). Component
        // roots are the union-find representatives 2, 4 and the singleton 5.
        let program = induced_subgraph_ghz_program(6, &[(0, 1), (1, 2), (3, 4), (2, 0)]);
        assert_eq!(
            gate_shapes(&program),
            vec![
                (GateKind::H, vec![2], vec![]),
                (GateKind::H, vec![4], vec![]),
                (GateKind::H, vec![5], vec![]),
                (GateKind::X, vec![1], vec![2]),
                (GateKind::X, vec![0], vec![1]),
                (GateKind::X, vec![3], vec![4]),
            ]
        );
        assert!(program.instructions.iter().all(|i| match i {
            Instruction::Gate(g) =>
                g.params.is_empty() && g.controls.iter().all(|c| c.state == ControlState::One),
            _ => true,
        }));
        let measured: Vec<(u32, String, u32)> = program
            .instructions
            .iter()
            .filter_map(|i| match i {
                Instruction::Measure {
                    qubit,
                    classical_bit,
                } => Some((*qubit, classical_bit.register.clone(), classical_bit.index)),
                _ => None,
            })
            .collect();
        let expected: Vec<(u32, String, u32)> = (0..6)
            .map(|q| (q, OUTCOME_REGISTER.to_string(), q))
            .collect();
        assert_eq!(measured, expected);
        assert_eq!(program.instructions.len(), 12);
        assert_eq!(
            program.classical_registers,
            vec![ClassicalRegister {
                name: OUTCOME_REGISTER.to_string(),
                n_bits: 6,
            }]
        );
        assert_eq!(
            program.metadata.name.as_deref(),
            Some("eg-quantum-jobs.induced_subgraph_ghz")
        );
        assert_eq!(program.metadata.source.as_deref(), Some("eg-quantum-jobs"));
        assert_eq!(program.ir_version, IR_VERSION);
        assert!(program.parameters.is_empty());
    }
}
