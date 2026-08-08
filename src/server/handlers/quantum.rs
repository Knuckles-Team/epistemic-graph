//! The agent-facing quantum control-plane surface (Q8, CONCEPT:EG-KG.compute.quantum-agent-api,
//! feature `quantum`): `Method::Quantum { op }` — `quantum_rank`/`optimize_with_qaoa`/
//! `quantum_expectation` over a registered `eg_quantum_core::backend::QuantumBackend`
//! (today: `eg-quantum-sim`'s `sv-cpu`/`stabilizer`, chosen by the SAME R0-R5 planner
//! `eg-quantum-core` ships — this handler builds circuits and calls the planner, it
//! does not reimplement backend selection).
//!
//! This module is the thing that makes `plans/au-eg-program/program/quantum-native.md`'s
//! 2026-08-07 status block stop being true: "no job-plane, no wire protocol Method, and
//! no KG concept mapping" — the wire protocol Method half of that gap closes here.
//!
//! NOT graph-scoped: a quantum run reads no persisted graph state and writes nothing
//! durable in the engine (every result returns to the caller as a
//! [`eg_quantum_core::result::Proposal`], never committed here — see
//! `eg-capabilities`' policy doc for `Method::Quantum` for the full reasoning), so this
//! module self-routes `dispatch.rs`'s top-level match, ahead of the per-graph
//! `dispatch_graph_op` chain — exactly like `handlers::jobs`/`handlers::statechart`. It
//! therefore takes no `state`/`GraphCore` at all.
//!
//! ## Agents never see qubits unless they opt in
//!
//! [`QuantumOp::Rank`]/[`QuantumOp::OptimizeQaoa`] are the default, high-level agent
//! experience: a caller supplies `(id, weight)` pairs or a Max-Cut instance in plain
//! classical terms, and this module builds the circuit. [`QuantumOp::Expectation`] is
//! the explicit opt-in: the caller authors and submits a real `QuantumProgram` (the
//! same IR `eg-quantum-core`'s `qasm` module round-trips). No op requires a caller to
//! reason about circuits unless they choose `Expectation`.
//!
//! ## The exactness rule, preserved end to end
//!
//! [`QuantumResult::is_exact`] is surfaced verbatim as `"exact"` in every response,
//! but every op ALSO always sets `"proposal": true` — for `Rank`/`OptimizeQaoa`
//! because a ranking/partition DECISION is never a fact regardless of whether the
//! underlying amplitudes were computed exactly, and for `Expectation` because THIS
//! v0's `expectation_value` is always a sample-mean estimate over measured shot
//! counts (never a closed-form amplitude computation) — exactly the "sampled/noisy"
//! case Q0's exactness gate exists to catch, independent of the raw backend run's
//! own `is_exact()`. See `quantum.rs`'s crate-level doc in `eg-types` for the full
//! rationale, including the future (unimplemented) analytic-expectation path that
//! WOULD be a genuine `exact: true` candidate. Nothing in this module ever calls
//! `QuantumResult::into_hard_constraint` — that call belongs solely to Q7's future
//! epistemic commit path, and only ever succeeds on an `exact: true` result.
//!
//! ## R5 escape hatch, audited
//!
//! Every op accepts an optional `backend_id`. It is threaded straight through to
//! `eg_quantum_core::planner::PlannerOptions::backend_id_override` — ALWAYS honoured
//! when the named backend is registered (today: `sv-cpu`/`stabilizer` only; naming
//! anything else fails cleanly with `PlannerError::UnknownOverrideBackend`, since no
//! hardware/cloud provider is registered in this build — that is Q10's job). The full
//! `PlannerDecision.audit` trail (every rule considered, including what R0-R4 would
//! have picked and any override conflict) is ALWAYS returned in the response's
//! `"planner"` object — `eg-quantum-core`'s own planner doc calls this exact field
//! "what Q9 observability persists" — so the agent-utilities caller can persist it
//! into the SAME `:ToolCall`/`RunTrace` provenance model as everything else (a
//! `:QuantumJob` node, per that repo's `observability/trace_ontology.py`), never a new
//! provenance system. See `eg-capabilities`' `Method::Quantum` policy doc for why this
//! lives in response-payload provenance rather than the engine's own graph
//! tamper-evident audit chain.
//!
//! ## Quotas / cost budgets
//!
//! This engine handler enforces NO quota itself — the default backend (local
//! simulator) is free and effectively unlimited, and no hardware/cloud backend is
//! registered in this build at all (Q10). The control-plane budget GATE (the IBM Open
//! Plan's ~10-minutes-per-28-days shared resource this program's addendum calls out)
//! lives on the agent-utilities side, fail-closed, BEFORE a request with a non-local
//! `backend_id` override is ever sent over the wire — see that repo's
//! `agent_utilities/knowledge_graph/quantum/budget.py`. This handler's only quota-
//! adjacent behavior is the qubit-count ceilings below, which bound wall-time/memory
//! for the local simulator, not cost.

use std::collections::{BTreeMap, HashMap};

use eg_quantum_core::backend::{BackendDescriptor, BackendId, QuantumBackend, RunOptions};
use eg_quantum_core::estimate::{estimate, EstimateOptions};
use eg_quantum_core::ir::{
    ClassicalBitRef, ClassicalRegister, ControlQubit, ControlState, GateInstruction, GateKind,
    Instruction, ParamValue, ProgramMetadata, QuantumProgram, IR_VERSION,
};
use eg_quantum_core::planner::{select_backend, PlannerDecision, PlannerOptions, PlannerRule};
use eg_quantum_core::result::{Outcome, QuantumResult};
use eg_quantum_sim::stabilizer::StabilizerSimulator;
use eg_quantum_sim::statevector::StateVectorSimulator;
use eg_types::quantum::{QuantumOp, QuantumQaoaEdge, QuantumRankCandidate};

use crate::protocol::{Response, ResultPayload};

/// One qubit per candidate/node for `Rank`/`OptimizeQaoa` (the "agent never sees
/// qubits" default path). 16 keeps a dense statevector run (`2^16` = 65536
/// amplitudes) comfortably sub-second on any host this engine runs on, well inside
/// `StateVectorSimulator`'s own 24-qubit ceiling — a caller that genuinely needs
/// more submits a hand-built `Expectation` program instead (the explicit opt-in
/// path), where THIS ceiling does not apply (the backend's own resource limit does).
const MAX_HIGH_LEVEL_QUBITS: usize = 16;
/// Default shot count when a caller omits `shots` — enough for a stable empirical
/// distribution on a `<=16`-qubit circuit without an unbounded default.
const DEFAULT_SHOTS: u64 = 256;
/// A conservative statevector memory ceiling for `estimate()`'s `forbidden` bucket —
/// matches `StateVectorSimulator::new()`'s own 24-qubit (`256MiB`) safety comment.
const DEFAULT_MEMORY_BOUND_BYTES: u64 = 256 * 1024 * 1024;

/// Handle `Method::Quantum { op }` (Q8). Self-contained: builds its own backend
/// registry per call (both backends are cheap, stateless-besides-an-in-memory-job-
/// store), so the dispatch shell can call this directly with no per-graph routing
/// and no `state`.
pub(crate) async fn handle(req_id: u64, op: QuantumOp) -> Response {
    let outcome = match op {
        QuantumOp::Rank {
            candidates,
            shots,
            seed,
            backend_id,
        } => handle_rank(candidates, shots, seed, backend_id),
        QuantumOp::OptimizeQaoa {
            nodes,
            edges,
            p_layers,
            shots,
            seed,
            backend_id,
        } => handle_qaoa(nodes, edges, p_layers, shots, seed, backend_id),
        QuantumOp::Expectation {
            program,
            observable_qubits,
            shots,
            seed,
            backend_id,
        } => handle_expectation(program, observable_qubits, shots, seed, backend_id),
    };
    match outcome {
        Ok(payload) => Response::ok(req_id, ResultPayload::Json(payload)),
        Err(message) => Response::err(req_id, message),
    }
}

// ── shared backend registry + run plumbing ──────────────────────────────────

fn descriptor_of(backend: &dyn QuantumBackend) -> BackendDescriptor {
    BackendDescriptor {
        id: backend.backend_id(),
        family: backend.family(),
        capabilities: backend.capabilities(),
    }
}

/// Run `program` through the SAME planner (`eg_quantum_core::planner::select_backend`)
/// every other quantum caller in this workspace goes through — this function never
/// makes its own routing decision, only executes the planner's.
fn run_program(
    program: &QuantumProgram,
    shots: Option<u64>,
    seed: Option<u64>,
    backend_id_override: Option<String>,
) -> Result<(QuantumResult, PlannerDecision), String> {
    program
        .validate()
        .map_err(|e| format!("invalid circuit: {e}"))?;

    let sv = StateVectorSimulator::new();
    let stab = StabilizerSimulator::new();
    let available: Vec<BackendDescriptor> = vec![descriptor_of(&sv), descriptor_of(&stab)];

    let est = estimate(
        program,
        &EstimateOptions {
            shots,
            memory_bound_bytes: Some(DEFAULT_MEMORY_BOUND_BYTES),
            ..Default::default()
        },
    );
    let override_id = backend_id_override.as_deref().map(BackendId::from);
    let planner_opts = PlannerOptions {
        want_hardware: false,
        backend_id_override: override_id,
    };
    let decision = select_backend(&est, &available, &planner_opts).map_err(|e| e.to_string())?;

    let run_opts = RunOptions {
        shots: Some(shots.unwrap_or(DEFAULT_SHOTS)),
        seed,
        noise_model_id: None,
        parameter_bindings: BTreeMap::new(),
        timeout_ms: None,
    };
    let result = if decision.chosen == sv.backend_id() {
        sv.run(program, &run_opts)
    } else if decision.chosen == stab.backend_id() {
        stab.run(program, &run_opts)
    } else {
        return Err(format!(
            "planner selected backend '{}' but it is not registered in this build",
            decision.chosen
        ));
    }
    .map_err(|e| e.to_string())?;

    Ok((result, decision))
}

fn rule_name(rule: PlannerRule) -> &'static str {
    match rule {
        PlannerRule::R0HardConstraint => "r0_hard_constraint",
        PlannerRule::R1CliffordStabilizer => "r1_clifford_stabilizer",
        PlannerRule::R2Noise => "r2_noise",
        PlannerRule::R3Structure => "r3_structure",
        PlannerRule::R4Placement => "r4_placement",
        PlannerRule::R5Override => "r5_override",
    }
}

fn planner_json(
    decision: &PlannerDecision,
    override_requested: &Option<String>,
) -> serde_json::Value {
    serde_json::json!({
        "chosen_backend": decision.chosen.0,
        "chosen_family": serde_json::to_value(decision.family).unwrap_or(serde_json::Value::Null),
        "rule": rule_name(decision.rule),
        "audit_trail": decision
            .audit
            .iter()
            .map(|e| serde_json::json!({"rule": rule_name(e.rule), "note": e.note}))
            .collect::<Vec<_>>(),
        "backend_override_requested": override_requested,
    })
}

/// The full Q0 result metadata, unconditionally present in every response — this is
/// the Q9 observability contract. `proposal` is passed in by the caller: `Rank`/
/// `OptimizeQaoa` always pass `true`; `Expectation` passes through the backend's own
/// `is_exact()` unchanged (an expectation value is the kind of quantity Q0's
/// `HardConstraint` gate exists for, unlike a ranking/partition decision).
fn result_json(result: &QuantumResult, proposal: bool) -> serde_json::Value {
    serde_json::json!({
        "backend_id": result.backend_id.0,
        "formalism": serde_json::to_value(result.formalism).unwrap_or(serde_json::Value::Null),
        "seed": result.seed,
        "shots": result.shots,
        "circuit_hash": result.circuit_hash.to_hex(),
        "exact": result.is_exact(),
        "proposal": proposal,
        "noise_model_id": result.noise_model_id,
        "fidelity_hint": result.fidelity_hint,
        "wall_time_ms": result.wall_time_ms,
        "peak_memory_bytes": result.peak_memory_bytes,
    })
}

fn counts_of(outcome: &Outcome) -> Result<&BTreeMap<String, u64>, String> {
    match outcome {
        Outcome::Counts(counts) => Ok(counts),
        Outcome::ExpectationValue { .. } => Err(
            "backend returned an expectation-value outcome where shot counts were expected \
                 (this build's registered backends never produce that shape directly — this is a \
                 defensive check, not an expected path)"
                .to_string(),
        ),
    }
}

fn gate(kind: GateKind, qubits: &[u32]) -> Instruction {
    Instruction::Gate(GateInstruction {
        gate: kind,
        qubits: qubits.to_vec(),
        controls: vec![],
        params: vec![],
    })
}

fn rotation(kind: GateKind, qubits: &[u32], angle: f64) -> Instruction {
    Instruction::Gate(GateInstruction {
        gate: kind,
        qubits: qubits.to_vec(),
        controls: vec![],
        params: vec![ParamValue::Literal(angle)],
    })
}

fn cz(control: u32, target: u32) -> Instruction {
    Instruction::Gate(GateInstruction {
        gate: GateKind::Z,
        qubits: vec![target],
        controls: vec![ControlQubit {
            qubit: control,
            state: ControlState::One,
        }],
        params: vec![],
    })
}

fn measure_all(n_qubits: u32, register: &str) -> Vec<Instruction> {
    (0..n_qubits)
        .map(|i| Instruction::Measure {
            qubit: i,
            classical_bit: ClassicalBitRef {
                register: register.to_string(),
                index: i,
            },
        })
        .collect()
}

// ── Rank ─────────────────────────────────────────────────────────────────

/// One qubit per candidate, an amplitude encoding proportional to its normalized
/// weight (`Ry`), one ring of controlled-`Z` entangling gates for interference, and a
/// final `H` layer that moves the entangling layer's relative phase into measurable
/// Z-basis probability (a bare `Ry` + `CZ` + measure circuit would leave Z-basis
/// measurement probabilities UNCHANGED by the CZ layer, since CZ is diagonal in the
/// computational basis — the closing `H` is what makes the interference actually
/// visible in the sampled distribution this ranking is built from).
fn build_rank_program(candidates: &[QuantumRankCandidate]) -> Result<QuantumProgram, String> {
    if candidates.is_empty() {
        return Err("quantum_rank requires at least one candidate".to_string());
    }
    if candidates.len() > MAX_HIGH_LEVEL_QUBITS {
        return Err(format!(
            "quantum_rank supports at most {MAX_HIGH_LEVEL_QUBITS} candidates per call \
             (one qubit each); pre-filter candidates client-side and call again"
        ));
    }
    let n_qubits = candidates.len() as u32;
    let max_w = candidates
        .iter()
        .map(|c| c.weight)
        .fold(f64::NEG_INFINITY, f64::max);
    let min_w = candidates
        .iter()
        .map(|c| c.weight)
        .fold(f64::INFINITY, f64::min);
    let range = max_w - min_w;

    let mut instructions = Vec::new();
    for (i, c) in candidates.iter().enumerate() {
        let normalized = if range > 1e-12 {
            (c.weight - min_w) / range
        } else {
            0.5
        };
        instructions.push(rotation(
            GateKind::Ry,
            &[i as u32],
            normalized * std::f64::consts::PI,
        ));
    }
    if n_qubits >= 2 {
        for i in 0..n_qubits {
            let j = (i + 1) % n_qubits;
            instructions.push(cz(i, j));
        }
    }
    for i in 0..n_qubits {
        instructions.push(gate(GateKind::H, &[i]));
    }
    instructions.extend(measure_all(n_qubits, "c"));

    Ok(QuantumProgram {
        ir_version: IR_VERSION,
        n_qubits,
        classical_registers: vec![ClassicalRegister {
            name: "c".to_string(),
            n_bits: n_qubits,
        }],
        parameters: vec![],
        instructions,
        metadata: ProgramMetadata {
            name: Some("q8-quantum-rank".to_string()),
            source: Some("agent-utilities-q8".to_string()),
        },
    })
}

fn handle_rank(
    candidates: Vec<QuantumRankCandidate>,
    shots: Option<u64>,
    seed: Option<u64>,
    backend_id: Option<String>,
) -> Result<serde_json::Value, String> {
    let program = build_rank_program(&candidates)?;
    let (result, decision) = run_program(&program, shots, seed, backend_id.clone())?;
    let counts = counts_of(&result.outcome)?;
    let total: u64 = counts.values().sum();

    // Marginal P(qubit_i == 1) across every sampled shot -- the ranking score.
    let mut scored: Vec<(usize, f64)> = (0..candidates.len())
        .map(|i| {
            let ones: u64 = counts
                .iter()
                .filter(|(key, _)| key.as_bytes().get(i) == Some(&b'1'))
                .map(|(_, count)| *count)
                .sum();
            let p = if total > 0 {
                ones as f64 / total as f64
            } else {
                0.0
            };
            (i, p)
        })
        .collect();
    scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));

    let ranked: Vec<serde_json::Value> = scored
        .iter()
        .enumerate()
        .map(|(rank, (i, probability))| {
            serde_json::json!({
                "id": candidates[*i].id,
                "weight": candidates[*i].weight,
                "probability": probability,
                "rank": rank,
            })
        })
        .collect();

    let mut response = result_json(&result, true);
    response["operation"] = serde_json::Value::String("rank".to_string());
    response["planner"] = planner_json(&decision, &backend_id);
    response["ranked_candidates"] = serde_json::Value::Array(ranked);
    Ok(response)
}

// ── OptimizeQaoa ─────────────────────────────────────────────────────────

/// One fixed-parameter QAOA layer set (H-init, then `p_layers` × (Rzz cost + Rx
/// mixer) at a canonical FIXED angle schedule — NOT a variational optimizer loop; a
/// classical outer loop tuning `(gamma, beta)` is explicitly out of Q8's scope, see
/// the module doc) over a caller-supplied weighted Max-Cut instance.
const QAOA_GAMMA: f64 = std::f64::consts::FRAC_PI_4;
const QAOA_BETA: f64 = std::f64::consts::FRAC_PI_8;

fn build_qaoa_program(
    nodes: &[String],
    edges: &[QuantumQaoaEdge],
    p_layers: u32,
) -> Result<(QuantumProgram, Vec<(u32, u32, f64)>), String> {
    if nodes.is_empty() {
        return Err("optimize_with_qaoa requires at least one node".to_string());
    }
    if nodes.len() > MAX_HIGH_LEVEL_QUBITS {
        return Err(format!(
            "optimize_with_qaoa supports at most {MAX_HIGH_LEVEL_QUBITS} nodes per call \
             (one qubit each); pull a smaller candidate subgraph and call again"
        ));
    }
    if p_layers == 0 {
        return Err("p_layers must be at least 1".to_string());
    }
    let index_of: HashMap<&str, u32> = nodes
        .iter()
        .enumerate()
        .map(|(i, n)| (n.as_str(), i as u32))
        .collect();
    let mut resolved_edges = Vec::with_capacity(edges.len());
    for e in edges {
        let u = *index_of
            .get(e.source.as_str())
            .ok_or_else(|| format!("edge source '{}' is not in nodes", e.source))?;
        let v = *index_of
            .get(e.target.as_str())
            .ok_or_else(|| format!("edge target '{}' is not in nodes", e.target))?;
        if u == v {
            return Err(format!(
                "self-loop edge on node '{}' is not a valid Max-Cut edge",
                e.source
            ));
        }
        resolved_edges.push((u, v, e.weight));
    }

    let n_qubits = nodes.len() as u32;
    let mut instructions = Vec::new();
    for i in 0..n_qubits {
        instructions.push(gate(GateKind::H, &[i]));
    }
    for _layer in 0..p_layers {
        for (u, v, w) in &resolved_edges {
            instructions.push(rotation(GateKind::Rzz, &[*u, *v], 2.0 * QAOA_GAMMA * w));
        }
        for i in 0..n_qubits {
            instructions.push(rotation(GateKind::Rx, &[i], 2.0 * QAOA_BETA));
        }
    }
    instructions.extend(measure_all(n_qubits, "c"));

    let program = QuantumProgram {
        ir_version: IR_VERSION,
        n_qubits,
        classical_registers: vec![ClassicalRegister {
            name: "c".to_string(),
            n_bits: n_qubits,
        }],
        parameters: vec![],
        instructions,
        metadata: ProgramMetadata {
            name: Some("q8-optimize-with-qaoa".to_string()),
            source: Some("agent-utilities-q8".to_string()),
        },
    };
    Ok((program, resolved_edges))
}

fn handle_qaoa(
    nodes: Vec<String>,
    edges: Vec<QuantumQaoaEdge>,
    p_layers: u32,
    shots: Option<u64>,
    seed: Option<u64>,
    backend_id: Option<String>,
) -> Result<serde_json::Value, String> {
    let (program, resolved_edges) = build_qaoa_program(&nodes, &edges, p_layers)?;
    let (result, decision) = run_program(&program, shots, seed, backend_id.clone())?;
    let counts = counts_of(&result.outcome)?;

    let mut best_key: Option<&str> = None;
    let mut best_cut = f64::NEG_INFINITY;
    for key in counts.keys() {
        let bits: Vec<bool> = key.bytes().map(|b| b == b'1').collect();
        let cut: f64 = resolved_edges
            .iter()
            .filter(|(u, v, _)| bits.get(*u as usize) != bits.get(*v as usize))
            .map(|(_, _, w)| w)
            .sum();
        if cut > best_cut {
            best_cut = cut;
            best_key = Some(key.as_str());
        }
    }
    let best_key = best_key.ok_or("backend returned no measurement outcomes")?;
    let partition: serde_json::Map<String, serde_json::Value> = nodes
        .iter()
        .enumerate()
        .map(|(i, node_id)| {
            let bit = best_key.as_bytes().get(i).copied() == Some(b'1');
            (node_id.clone(), serde_json::Value::from(i32::from(bit)))
        })
        .collect();

    let mut response = result_json(&result, true);
    response["operation"] = serde_json::Value::String("optimize_qaoa".to_string());
    response["planner"] = planner_json(&decision, &backend_id);
    response["partition"] = serde_json::Value::Object(partition);
    response["cut_value"] = serde_json::json!(best_cut);
    response["p_layers"] = serde_json::json!(p_layers);
    response["variational_optimizer"] = serde_json::Value::String("fixed_params_v0".to_string());
    Ok(response)
}

// ── Expectation ──────────────────────────────────────────────────────────

/// Bit position of `(register, index)` within `ClassicalMemory::bitstring`'s
/// concatenated encoding (`eg-quantum-sim`'s own doc: registers in declaration
/// order, MSB-first within each register) — every declared register contributes
/// its `n_bits` to the running offset regardless of whether this program's
/// instructions ever measure into it.
fn bit_offsets(registers: &[ClassicalRegister]) -> HashMap<&str, usize> {
    let mut offsets = HashMap::new();
    let mut running = 0usize;
    for reg in registers {
        offsets.insert(reg.name.as_str(), running);
        running += reg.n_bits as usize;
    }
    offsets
}

fn resolve_observable_positions(
    program: &QuantumProgram,
    observable_qubits: &[u32],
) -> Result<Vec<usize>, String> {
    if observable_qubits.is_empty() {
        return Err("quantum_expectation requires at least one observable qubit".to_string());
    }
    let offsets = bit_offsets(&program.classical_registers);
    let mut qubit_to_bit: HashMap<u32, (&str, u32)> = HashMap::new();
    for instr in &program.instructions {
        if let Instruction::Measure {
            qubit,
            classical_bit,
        } = instr
        {
            qubit_to_bit.insert(
                *qubit,
                (classical_bit.register.as_str(), classical_bit.index),
            );
        }
    }
    observable_qubits
        .iter()
        .map(|q| {
            let (register, index) = qubit_to_bit.get(q).ok_or_else(|| {
                format!(
                    "observable qubit {q} is not measured into a classical bit by the \
                     submitted program -- quantum_expectation never auto-appends a \
                     measurement to a caller's circuit"
                )
            })?;
            let base = offsets.get(register).ok_or_else(|| {
                format!(
                    "classical register '{register}' is referenced by a Measure but not declared"
                )
            })?;
            Ok(base + *index as usize)
        })
        .collect()
}

/// Sample-mean estimate of `<psi| Z_{q0} Z_{q1} ... |psi>` from measured shot counts:
/// each shot contributes `+1`/`-1` per the parity of the observable qubits' measured
/// bits, averaged over shots. Standard error uses the fact that every per-shot
/// contribution is `+-1`-valued, so `Var[X] = 1 - E[X]^2` exactly (no separate
/// second-moment accumulation needed). ALWAYS treated as a sampled (non-exact)
/// estimate at this layer -- see `handle_expectation`.
fn expectation_from_counts(counts: &BTreeMap<String, u64>, positions: &[usize]) -> (f64, f64) {
    let total: u64 = counts.values().sum();
    if total == 0 {
        return (0.0, 0.0);
    }
    let mut sum = 0.0f64;
    for (key, count) in counts {
        let bytes = key.as_bytes();
        let mut parity = 1.0f64;
        for &p in positions {
            if bytes.get(p).copied() == Some(b'1') {
                parity *= -1.0;
            }
        }
        sum += parity * (*count as f64);
    }
    let n = total as f64;
    let mean = sum / n;
    let variance = (1.0 - mean * mean).max(0.0);
    let stderr = if n > 1.0 { (variance / n).sqrt() } else { 0.0 };
    (mean, stderr)
}

fn handle_expectation(
    program_json: serde_json::Value,
    observable_qubits: Vec<u32>,
    shots: Option<u64>,
    seed: Option<u64>,
    backend_id: Option<String>,
) -> Result<serde_json::Value, String> {
    let program: QuantumProgram = serde_json::from_value(program_json)
        .map_err(|e| format!("program does not decode as a QuantumProgram: {e}"))?;
    let positions = resolve_observable_positions(&program, &observable_qubits)?;
    let (result, decision) = run_program(&program, shots, seed, backend_id.clone())?;
    let counts = counts_of(&result.outcome)?;
    let (value, stderr) = expectation_from_counts(counts, &positions);

    // `expectation_value` is ALWAYS a sample-mean estimate over measured shot counts
    // in this implementation (never a closed-form amplitude computation), so it is
    // ALWAYS `proposal: true` here too -- an expectation value estimated this way is
    // exactly the "sampled/noisy" case Q0's `into_hard_constraint()` gate is built to
    // reject, independent of whatever `result.is_exact()` says about the raw backend
    // run underneath it (see the module doc's exactness section). A genuinely exact,
    // closed-form expectation path (no sampling, `stderr` structurally absent rather
    // than `0.0`) is a real future upgrade this response shape has room for -- it is
    // not implemented by this lane.
    let mut response = result_json(&result, true);
    response["operation"] = serde_json::Value::String("expectation".to_string());
    response["planner"] = planner_json(&decision, &backend_id);
    response["observable_qubits"] = serde_json::json!(observable_qubits);
    response["expectation_value"] = serde_json::json!(value);
    response["stderr"] = serde_json::json!(stderr);
    Ok(response)
}
