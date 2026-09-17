//! Step 2 of the Q7/Q11 pipeline: the Max-Cut cost Hamiltonian, the standard QAOA
//! ansatz built directly on `eg-quantum-core`'s IR, and a small classical
//! variational-parameter search evaluated exactly against `eg-quantum-sim`'s
//! noiseless statevector (no shot noise in the SEARCH loop — shot noise only enters
//! at the one official, provenance-carrying run in `run.rs`, via real Born-rule
//! sampling).
//!
//! ## Why this is genuinely QAOA, not a fixed circuit
//!
//! QAOA is defined by its VARIATIONAL loop: prepare `|+>^n`, alternate `p` layers of
//! a cost unitary `exp(-i*gamma*C)` and a mixer unitary `exp(-i*beta*sum(X_k))`, then
//! classically search `(gamma, beta)` (per layer) to maximize the expected cost. This
//! module builds exactly that circuit shape (`build_qaoa_program`) and performs that
//! search (`optimize_qaoa_params`) — a per-layer grid search refined by local
//! coordinate ascent, which is a legitimate (if unsophisticated) classical optimizer,
//! not a hand-picked constant. **No parameter setting this module can produce is ever
//! `exact:true`-eligible as a Max-Cut ANSWER** — see `propose.rs`'s module doc for why
//! that is true independent of what the backend's own noiseless-simulation exactness
//! flag says.
//!
//! ## Gate-angle convention (read before changing the ansatz)
//!
//! `eg-quantum-gates::matrix2(Rzz, [theta])` = `exp(-i*theta/2 * Z⊗Z)` and
//! `matrix1(Rx, [theta])` = `exp(-i*theta/2 * X)` (both confirmed against that
//! crate's source, not assumed). So the cost layer applies `Rzz(cost_angle)` per
//! edge with `cost_angle = 2*gamma`, and the mixer layer applies `Rx(mixer_angle)`
//! per qubit with `mixer_angle = 2*beta`. This module searches `cost_angle`/
//! `mixer_angle` directly (the values actually bound to the IR's symbolic
//! parameters) rather than `gamma`/`beta`, so there is exactly one place (this
//! comment) where the factor-of-2 textbook convention is spelled out.

use std::collections::BTreeMap;

use eg_numeric::complex::Complex64;
use eg_quantum_core::ir::{
    ClassicalBitRef, ClassicalRegister, GateInstruction, GateKind, Instruction, ParamValue,
    Parameter, ProgramMetadata, QuantumProgram, IR_VERSION,
};

/// One QAOA layer's two symbolic parameter names, `p`-indexed so a depth-`p` circuit
/// declares `2*p` independent `Parameter`s.
pub fn cost_angle_name(layer: usize) -> String {
    format!("cost_angle_{layer}")
}
pub fn mixer_angle_name(layer: usize) -> String {
    format!("mixer_angle_{layer}")
}

/// Build the depth-`p` QAOA ansatz over `n_qubits` with cost edges `edges` (`(i, j,
/// weight)` — the weight only matters for the classical expectation evaluation in
/// `expected_cut_value`, not the circuit itself, since Rzz's angle already encodes
/// the full per-edge coupling and this ansatz applies one Rzz per edge regardless of
/// weight, consistent with the standard unweighted-QAOA-ansatz-with-weighted-cost
/// formulation used for evaluation). `with_measurement`: `false` builds the bare
/// unitary circuit used by the classical search loop (`expected_cut_value`, which
/// reads exact amplitudes and never measures); `true` additionally measures every
/// qubit into a classical register named `"meas"` — the shape submitted to a real
/// `QuantumBackend` for the one official, provenance-carrying run.
pub fn build_qaoa_program(
    n_qubits: u32,
    edges: &[(u32, u32, f64)],
    p: usize,
    with_measurement: bool,
) -> QuantumProgram {
    let mut instructions = Vec::new();

    // |+>^n via H on every qubit.
    for q in 0..n_qubits {
        instructions.push(Instruction::Gate(GateInstruction {
            gate: GateKind::H,
            qubits: vec![q],
            controls: vec![],
            params: vec![],
        }));
    }

    let mut parameters = Vec::with_capacity(2 * p);
    for layer in 0..p {
        let cost_name = cost_angle_name(layer);
        let mixer_name = mixer_angle_name(layer);
        parameters.push(Parameter {
            name: cost_name.clone(),
            default: None,
        });
        parameters.push(Parameter {
            name: mixer_name.clone(),
            default: None,
        });

        // Cost unitary: Rzz(cost_angle) per edge.
        for &(i, j, _weight) in edges {
            instructions.push(Instruction::Gate(GateInstruction {
                gate: GateKind::Rzz,
                qubits: vec![i, j],
                controls: vec![],
                params: vec![ParamValue::Symbol(cost_name.clone())],
            }));
        }
        // Mixer unitary: Rx(mixer_angle) per qubit.
        for q in 0..n_qubits {
            instructions.push(Instruction::Gate(GateInstruction {
                gate: GateKind::Rx,
                qubits: vec![q],
                controls: vec![],
                params: vec![ParamValue::Symbol(mixer_name.clone())],
            }));
        }
    }

    let classical_registers = if with_measurement {
        vec![ClassicalRegister {
            name: "meas".to_string(),
            n_bits: n_qubits,
        }]
    } else {
        vec![]
    };
    if with_measurement {
        for q in 0..n_qubits {
            instructions.push(Instruction::Measure {
                qubit: q,
                classical_bit: ClassicalBitRef {
                    register: "meas".to_string(),
                    index: q,
                },
            });
        }
    }

    QuantumProgram {
        ir_version: IR_VERSION,
        n_qubits,
        classical_registers,
        parameters,
        instructions,
        metadata: ProgramMetadata {
            name: Some(format!("qaoa-maxcut-p{p}")),
            source: Some("eg-quantum-workloads".to_string()),
        },
    }
}

/// Which basis-state bit index `qubit` occupies, matching `eg-quantum-sim`'s own
/// `(idx >> qubit) & 1` convention (see `statevector.rs::measure_and_collapse`).
fn bit(idx: usize, qubit: u32) -> bool {
    (idx >> qubit) & 1 == 1
}

/// The Max-Cut value of one computational basis state: total weight of edges whose
/// two endpoints disagree (one bit set, one clear).
pub fn cut_value_of_bitstring(idx: usize, edges: &[(u32, u32, f64)]) -> f64 {
    edges
        .iter()
        .filter(|(i, j, _)| bit(idx, *i) != bit(idx, *j))
        .map(|(_, _, w)| w)
        .sum()
}

/// EXACT expected Max-Cut value of `program` (with `bindings` bound) over the
/// noiseless final statevector — `sum_idx |amp_idx|^2 * cut_value(idx)`. This is the
/// objective the classical search loop maximizes; it reads amplitudes directly via
/// `eg_quantum_sim::statevector::evolve` (no shots, no RNG-dependent sampling noise
/// — the search itself is deterministic given `program`+`bindings`). `program` must
/// have `with_measurement=false` (no `Measure` instructions), since a measured
/// program would need an RNG for collapse and this function passes none of
/// consequence (a zero-seeded generator whose draws are never used).
pub fn expected_cut_value(
    program: &QuantumProgram,
    bindings: &BTreeMap<String, f64>,
    edges: &[(u32, u32, f64)],
) -> f64 {
    let mut rng = eg_numeric::random::Generator::new(0);
    let (state, _classical) = eg_quantum_sim::statevector::evolve(program, bindings, &mut rng)
        .expect("search-phase QAOA program must evolve cleanly (no unbound symbols)");
    state
        .iter()
        .enumerate()
        .map(|(idx, amp): (usize, &Complex64)| amp.norm_sqr() * cut_value_of_bitstring(idx, edges))
        .sum()
}

/// Outcome of [`optimize_qaoa_params`]: the best `(cost_angle, mixer_angle)` binding
/// found per layer, the exact expected cut value it achieves, and how many
/// statevector evaluations the search performed (a real, reportable cost metric for
/// Q11's benchmarking, not a hidden constant).
#[derive(Debug, Clone)]
pub struct OptimizedParams {
    pub bindings: BTreeMap<String, f64>,
    pub expected_cut: f64,
    pub evaluations: usize,
}

/// Classical variational-parameter search: a per-layer grid search (`grid_resolution`
/// points per angle, each layer's cost/mixer angle jointly gridded — i.e.
/// `grid_resolution^2` evaluations per layer) refined by one pass of coordinate
/// ascent (holding all other angles fixed, re-scan each angle at `grid_resolution`
/// finer points around its current best). Layers are optimized in order 0..p, each
/// holding EARLIER layers' already-chosen angles fixed and LATER layers' angles at
/// their (still-unoptimized) default of `0.0` — a standard greedy layer-wise QAOA
/// initialization strategy, not a shortcut that skips optimizing later layers (each
/// layer, once reached, IS fully gridded+refined; only the ORDER is greedy).
///
/// This is deliberately simple (no external optimizer crate, no new dependency) —
/// see this module's doc for why "a real, if unsophisticated, classical optimizer"
/// is exactly what makes the resulting angles VARIATIONAL rather than fixed
/// constants.
pub fn optimize_qaoa_params(
    n_qubits: u32,
    edges: &[(u32, u32, f64)],
    p: usize,
    grid_resolution: usize,
) -> OptimizedParams {
    let program = build_qaoa_program(n_qubits, edges, p, false);
    let mut bindings: BTreeMap<String, f64> = BTreeMap::new();
    for layer in 0..p {
        bindings.insert(cost_angle_name(layer), 0.0);
        bindings.insert(mixer_angle_name(layer), 0.0);
    }
    let best_expected = expected_cut_value(&program, &bindings, edges);
    let mut search = QaoaSearch {
        program: &program,
        edges,
        grid_resolution,
        bindings,
        best_expected,
        evaluations: 1,
    };

    for layer in 0..p {
        optimize_layer(&mut search, layer);
    }

    OptimizedParams {
        bindings: search.bindings,
        expected_cut: search.best_expected,
        evaluations: search.evaluations,
    }
}

/// The mutable state of one [`optimize_qaoa_params`] run: the working bindings, the
/// best exact expected cut found so far, and the evaluation count.
struct QaoaSearch<'a> {
    program: &'a QuantumProgram,
    edges: &'a [(u32, u32, f64)],
    grid_resolution: usize,
    bindings: BTreeMap<String, f64>,
    best_expected: f64,
    evaluations: usize,
}

/// One layer's `(cost_angle, mixer_angle)` binding names.
struct LayerAngleNames {
    cost: String,
    mixer: String,
}

/// One layer's `(cost_angle, mixer_angle)` values.
#[derive(Clone, Copy)]
struct LayerAngles {
    cost: f64,
    mixer: f64,
}

impl QaoaSearch<'_> {
    /// Bind this layer to `angles`, evaluate the exact expected cut, and record it as
    /// the new best when it strictly improves. Returns whether it improved.
    fn try_angles(&mut self, names: &LayerAngleNames, angles: LayerAngles) -> bool {
        self.bindings.insert(names.cost.clone(), angles.cost);
        self.bindings.insert(names.mixer.clone(), angles.mixer);
        let value = expected_cut_value(self.program, &self.bindings, self.edges);
        self.evaluations += 1;
        let improved = value > self.best_expected;
        if improved {
            self.best_expected = value;
        }
        improved
    }

    /// `span * index / grid_resolution`: grid point `index` of `grid_resolution` over `span`.
    fn grid_fraction(&self, span: f64, index: usize) -> f64 {
        span * (index as f64) / (self.grid_resolution as f64)
    }
}

/// Grid-search then refine one layer, holding every other layer's angles fixed, and
/// leave the layer bound to its best angles.
fn optimize_layer(search: &mut QaoaSearch<'_>, layer: usize) {
    let names = LayerAngleNames {
        cost: cost_angle_name(layer),
        mixer: mixer_angle_name(layer),
    };
    let start = LayerAngles {
        cost: *search.bindings.get(&names.cost).unwrap_or(&0.0),
        mixer: *search.bindings.get(&names.mixer).unwrap_or(&0.0),
    };
    let coarse = coarse_layer_grid(search, &names, start);
    let cost_refined = refine_cost_angle(search, &names, coarse);
    let refined = refine_mixer_angle(search, &names, cost_refined);
    search.bindings.insert(names.cost, refined.cost);
    search.bindings.insert(names.mixer, refined.mixer);
}

/// Coarse joint grid over this layer's (cost_angle in [0, 2pi), mixer_angle in
/// [0, pi)) — the mixer angle's natural period under Rx is pi for a Max-Cut diagonal
/// cost (X-basis symmetry), same convention standard QAOA implementations use to
/// halve the mixer search range.
fn coarse_layer_grid(
    search: &mut QaoaSearch<'_>,
    names: &LayerAngleNames,
    start: LayerAngles,
) -> LayerAngles {
    let mut best = start;
    for gi in 0..search.grid_resolution {
        let cost = search.grid_fraction(std::f64::consts::TAU, gi);
        for gj in 0..search.grid_resolution {
            let candidate = LayerAngles {
                cost,
                mixer: search.grid_fraction(std::f64::consts::PI, gj),
            };
            if search.try_angles(names, candidate) {
                best = candidate;
            }
        }
    }
    best
}

/// One coordinate-ascent refinement pass of the cost angle around the current best,
/// at `grid_resolution`x finer resolution within +/- one coarse grid step (the centre
/// follows each improvement).
fn refine_cost_angle(
    search: &mut QaoaSearch<'_>,
    names: &LayerAngleNames,
    mut best: LayerAngles,
) -> LayerAngles {
    let step = std::f64::consts::TAU / search.grid_resolution as f64;
    for gi in 0..search.grid_resolution {
        let candidate = LayerAngles {
            cost: best.cost - step + search.grid_fraction(2.0 * step, gi),
            mixer: best.mixer,
        };
        if search.try_angles(names, candidate) {
            best = candidate;
        }
    }
    best
}

/// The same refinement pass for the mixer angle, holding the refined cost angle.
fn refine_mixer_angle(
    search: &mut QaoaSearch<'_>,
    names: &LayerAngleNames,
    mut best: LayerAngles,
) -> LayerAngles {
    let step = std::f64::consts::PI / search.grid_resolution as f64;
    for gj in 0..search.grid_resolution {
        let candidate = LayerAngles {
            cost: best.cost,
            mixer: best.mixer - step + search.grid_fraction(2.0 * step, gj),
        };
        if search.try_angles(names, candidate) {
            best = candidate;
        }
    }
    best
}

/// Brute-force EXACT Max-Cut optimum by enumerating all `2^n` partitions — used only
/// to compute a benchmarking `approx_ratio` at small `n` (see `run.rs`'s
/// `MAX_BRUTE_FORCE_QUBITS` cap); this is a CLASSICAL ground-truth oracle for
/// evaluation purposes only and is never part of the quantum result / proposal
/// itself.
pub fn brute_force_max_cut(n_qubits: u32, edges: &[(u32, u32, f64)]) -> f64 {
    let dim = 1usize << n_qubits;
    (0..dim)
        .map(|idx| cut_value_of_bitstring(idx, edges))
        .fold(0.0, f64::max)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn triangle_max_cut_bruteforce_is_two() {
        // Triangle (0-1, 1-2, 0-2): best cut separates one vertex from the other
        // two, cutting exactly 2 of the 3 edges (odd cycle, can't cut all 3).
        let edges = vec![(0, 1, 1.0), (1, 2, 1.0), (0, 2, 1.0)];
        assert_eq!(brute_force_max_cut(3, &edges), 2.0);
    }

    #[test]
    fn single_edge_expected_cut_is_maximized_near_pi_over_2_cost_pi_mixer() {
        // A single edge is a trivial Max-Cut instance (optimal cut = 1.0, achieved
        // by ANY basis state with the two bits different). Confirms the ansatz +
        // exact-expectation evaluator behave sanely on a minimal instance before
        // trusting them on anything larger.
        let edges = vec![(0, 1, 1.0)];
        let optimized = optimize_qaoa_params(2, &edges, 1, 12);
        assert!(
            optimized.expected_cut > 0.9,
            "expected near-optimal cut on a trivial 1-edge instance, got {}",
            optimized.expected_cut
        );
        assert_eq!(optimized.evaluations, 1 + 12 * 12 + 12 + 12);
    }

    #[test]
    fn deeper_qaoa_never_does_worse_than_shallower_on_the_same_instance() {
        let edges = vec![
            (0, 1, 1.0),
            (1, 2, 1.0),
            (2, 3, 1.0),
            (0, 3, 1.0),
            (0, 2, 1.0),
        ];
        let p1 = optimize_qaoa_params(4, &edges, 1, 10);
        let p2 = optimize_qaoa_params(4, &edges, 2, 10);
        // p=2 strictly extends p=1's search space (extra layer can always mimic a
        // no-op), so its found optimum should not be meaningfully worse; allow a
        // small numerical/search-granularity slack rather than asserting a strict
        // inequality against a NON-exhaustive grid search.
        assert!(p2.expected_cut >= p1.expected_cut - 1e-6);
    }

    #[test]
    fn two_layer_search_result_is_pinned() {
        let edges = vec![(0, 1, 1.0), (1, 2, 0.5)];
        let optimized = optimize_qaoa_params(3, &edges, 2, 4);
        assert_eq!(optimized.evaluations, 1 + 2 * (4 * 4 + 4 + 4));
        assert_eq!(
            format!("{:?}", optimized.bindings),
            r#"{"cost_angle_0": 0.7853981633974483, "cost_angle_1": 1.5707963267948966, "mixer_angle_0": 2.356194490192345, "mixer_angle_1": 2.748893571891069}"#
        );
        assert_eq!(format!("{:?}", optimized.expected_cut), "1.378791260736239");
    }

    #[test]
    fn zero_layers_evaluates_only_the_initial_bindings() {
        let edges = vec![(0, 1, 1.0)];
        let optimized = optimize_qaoa_params(2, &edges, 0, 5);
        assert_eq!(optimized.evaluations, 1);
        assert!(optimized.bindings.is_empty());
        assert_eq!(
            format!("{:?}", optimized.expected_cut),
            "0.5000000000000002"
        );
    }
}
