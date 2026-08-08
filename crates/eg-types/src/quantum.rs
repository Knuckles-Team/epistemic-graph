//! `QuantumOp` — the wire op `Method::Quantum` carries (Q8,
//! `plans/au-eg-program/program/quantum-native.md` +
//! `quantum-external-providers.md` §1.5), gated `quantum`. Mirrors
//! `jobs::JobOp`/`statechart::StatechartOp`'s "one Method variant, one internal op
//! enum" shape so the agent-facing quantum surface costs the wire protocol exactly
//! ONE new `Method` variant.
//!
//! This is the ONE thing that makes `eg-quantum-core`/`eg-quantum-gates`/
//! `eg-quantum-sim` (Rust-internal crates behind the default-off `quantum`/
//! `quantum-sim` features until this module exists) reachable from graph-os /
//! agent-utilities at all — see the program doc's 2026-08-07 status block: "no
//! job-plane, no wire protocol Method, and no KG concept mapping. That is the real
//! distance to 'usable through graph-os.'"
//!
//! Pure serde — no dependency on `eg-quantum-core` (which sits ABOVE this crate in
//! the DAG, and is not even a fellow leaf: `eg-types` is the bottom of the whole
//! engine DAG). `Expectation::program` therefore rides an opaque `serde_json::Value`
//! rather than `eg_quantum_core::ir::QuantumProgram` directly — the SAME shape that
//! type serializes to/from (see that crate's `ir.rs` doc: "a hand-built circuit from
//! an agent-utilities caller in Q8" is the exact producer this field exists for),
//! so no information is lost, only the compile-time dependency. The facade
//! (`src/server/handlers/quantum.rs`) is the one place that decodes it into the real
//! IR type and runs it against a registered `QuantumBackend`; this module only
//! shapes what crosses the wire.
//!
//! ## The exactness rule this wire shape is built to preserve
//!
//! Every op returns the FULL Q0 result/audit metadata (backend id, formalism, seed,
//! shots, circuit hash, `exact`, noise model id, fidelity hint, wall time, peak
//! memory — `eg_quantum_core::result::QuantumResult`'s own fields, Q9
//! observability) alongside an explicit `proposal: true` that the caller CANNOT
//! read past: `Rank`/`OptimizeQaoa` produce a ranking/partition DECISION, which is
//! never a fact regardless of whether the underlying amplitudes were computed
//! exactly, so the facade sets `proposal: true` unconditionally for those two ops.
//! `Expectation`'s derived scalar is a genuine candidate for `exact: true` (an
//! expectation value IS the kind of quantity Q0's `HardConstraint` gate exists for)
//! but this crate never constructs that judgment itself — see the facade for how
//! `QuantumResult::is_exact()`/`into_proposal()` are actually applied. The wire
//! shape's job is only to never let the metadata be dropped or the two concepts
//! (backend-exactness vs. this-op's-proposal-status) collapse into one boolean.
//!
//! ## R5 escape hatch (Q9 audit requirement)
//!
//! Every op accepts an optional `backend_id`: the planner's R0-R5 override
//! (`eg_quantum_core::planner::PlannerOptions::backend_id_override`), always
//! honoured when the named backend is registered. The facade audits every
//! non-`None` `backend_id` unconditionally (not only when it changes the outcome) —
//! see that module's doc.

use serde::{Deserialize, Serialize};

/// One candidate item for [`QuantumOp::Rank`], with the classical weight/score
/// signal the ranking circuit is built from.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuantumRankCandidate {
    pub id: String,
    pub weight: f64,
}

/// One weighted edge of the Max-Cut instance [`QuantumOp::OptimizeQaoa`] builds its
/// cost Hamiltonian from. `source`/`target` reference `nodes` by value, not index —
/// stable across a caller re-ordering `nodes`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuantumQaoaEdge {
    pub source: String,
    pub target: String,
    #[serde(default = "default_qaoa_edge_weight")]
    pub weight: f64,
}

fn default_qaoa_edge_weight() -> f64 {
    1.0
}

/// QAOA ansatz depth. `1` is the minimum useful depth (one cost + one mixer layer);
/// this is deliberately small because [`QuantumOp::OptimizeQaoa`] runs ONE fixed
/// parameter evaluation (no classical variational outer loop — see the facade's
/// module doc for why that is explicitly out of Q8's scope).
fn default_qaoa_p_layers() -> u32 {
    1
}

/// The agent-facing quantum control-plane operation (Q8). Nested under
/// `Method::Quantum { op }` — mirrors `acl::RbacAdminOp`/`jobs::JobOp`'s "one Method
/// variant, many operations" shape.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub enum QuantumOp {
    /// Rank candidate items by a caller-supplied weight signal through a
    /// deterministic amplitude-encoding + interference circuit. The agent NEVER
    /// sees qubits: it supplies `(id, weight)` pairs and a shot budget and gets
    /// back an ordering plus the full Q0 metadata. ALWAYS a proposal — see the
    /// module doc's exactness section.
    Rank {
        /// One qubit per candidate (bounded server-side — see the facade's
        /// `MAX_RANK_CANDIDATES`; an oversized request is rejected outright, never
        /// silently truncated).
        candidates: Vec<QuantumRankCandidate>,
        #[serde(default)]
        shots: Option<u64>,
        #[serde(default)]
        seed: Option<u64>,
        /// R5 escape hatch. Always honoured (if the named backend is registered),
        /// always audited (Q9). `None` lets the planner choose (R0-R4) — the
        /// default agent experience, and the only path that needs no hardware
        /// quota check.
        #[serde(default)]
        backend_id: Option<String>,
    },
    /// Run ONE fixed-parameter QAOA layer set over a caller-supplied Max-Cut
    /// instance (candidate node ids + weighted edges) and return a sampled cut
    /// assignment. NOT a variational optimizer loop — no classical outer loop, one
    /// evaluation at a canonical fixed angle schedule. ALWAYS a proposal.
    OptimizeQaoa {
        nodes: Vec<String>,
        edges: Vec<QuantumQaoaEdge>,
        #[serde(default = "default_qaoa_p_layers")]
        p_layers: u32,
        #[serde(default)]
        shots: Option<u64>,
        #[serde(default)]
        seed: Option<u64>,
        #[serde(default)]
        backend_id: Option<String>,
    },
    /// Compute a sampled Pauli-Z-string expectation value over a caller-supplied
    /// `QuantumProgram` (native IR, JSON-encoded exactly like
    /// `eg_quantum_core::ir::QuantumProgram`'s own serde round-trip —
    /// `eg-quantum-core`'s `qasm` module or the IR's own JSON shape both produce
    /// something this field accepts directly). `observable_qubits` names the
    /// Z-observable's support; every qubit in it MUST already be measured into a
    /// classical bit by `program` (the facade validates this and rejects
    /// otherwise — it never silently appends measurements to a caller's circuit).
    /// Restricted to Pauli-Z strings in Q8 v0 — X/Y support (via basis-rotation
    /// gates prepended before measurement) is a straightforward follow-up, not
    /// attempted here to keep the exactness/observability wiring the focus of this
    /// lane.
    Expectation {
        program: serde_json::Value,
        observable_qubits: Vec<u32>,
        #[serde(default)]
        shots: Option<u64>,
        #[serde(default)]
        seed: Option<u64>,
        #[serde(default)]
        backend_id: Option<String>,
    },
}
