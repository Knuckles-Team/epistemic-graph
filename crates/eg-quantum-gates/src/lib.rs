// Portions of this file (the dense unitary matrix definitions below) are ported,
// with adaptation, from QuantRS2 (`open-source-libraries/quantrs`,
// `core/src/gate/functions.rs`), Copyright 2026 COOLJAPAN OU (Team KitaSan),
// licensed under the Apache License, Version 2.0. See `NOTICE` and `LICENSE` in
// this crate's root for the full attribution and license text, and
// `crates/eg-quantum-core/src/lib.rs` for the numeric-stack decision (D-QN-3) this
// crate executes: matrices are expressed over `eg_numeric::complex::Complex64`, not
// QuantRS2's `scirs2_core::Complex64` -- no SciRS2 dependency exists in this crate.

//! `eg-quantum-gates` -- dense unitary matrices for `eg_quantum_core::ir::GateKind`
//! (register D-QN-1, lane Q1 vendoring).
//!
//! This crate is deliberately narrow: given a [`GateKind`] and its resolved
//! (`f64`) parameters, return the gate's dense matrix as a fixed-size array of
//! [`Complex64`]. It knows nothing about qubit indices, controls, or circuits --
//! `eg-quantum-sim` (Q1) is the crate that walks a [`QuantumProgram`] and decides
//! WHERE to apply a matrix (which qubit(s), which control mask); this crate only
//! answers WHAT the matrix is.
//!
//! # What was vendored, what was not (the "strip aggressively" charter directive)
//!
//! QuantRS2's `core/src/gate/functions.rs` defines each gate as a struct
//! implementing a `GateOp` trait (`name()`/`qubits()`/`matrix()`/`as_any()`/
//! `clone_gate()`), carrying its own `QubitId` fields and heavy `Box<dyn GateOp>`
//! machinery. None of that scaffolding is vendored here: `eg-quantum-core::ir`
//! already has a closed `GateKind` enum and `u32` qubit indices that play the exact
//! same role, so re-vendoring QuantRS2's OOP gate-object system would duplicate a
//! contract this workspace already owns (and would be exactly the kind of
//! Python-facing/research-surface bloat register D-QN-1 charges Q1 to strip). What
//! IS vendored is the one thing worth owning: the actual complex matrix entries for
//! each gate -- the mathematical content, ported into free functions keyed by
//! [`GateKind`] instead of trait-object structs.
//!
//! Toffoli/Fredkin (3-qubit gates) are intentionally NOT ported: `eg-quantum-core`'s
//! `GateKind` vocabulary has no 3-qubit base gate (a Toffoli is expressed as
//! `GateKind::X` plus two `ControlQubit`s in the IR, which the *simulator*
//! decomposes via control-masking over this crate's single-qubit `X` matrix --
//! QuantRS2 itself does not offer a direct Toffoli matrix either, for the same
//! reason: see its `matrix()` implementations returning `UnsupportedOperation`).

use eg_numeric::complex::Complex64;
use eg_quantum_core::ir::GateKind;

/// Errors constructing a gate matrix. `GateKind` (from `eg-quantum-core`) derives
/// `PartialEq` but not `Eq` (it carries a `Custom(String)` variant used generically),
/// so this type mirrors that: `PartialEq` only, no `Eq`.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum GateError {
    /// The gate is not in this crate's matrix vocabulary at all (e.g. a
    /// `GateKind::Custom` escape-hatch gate, or a 3+-qubit base gate -- see the
    /// module doc for why those never get a direct matrix here).
    #[error("gate '{0:?}' has no dense matrix representation in eg-quantum-gates")]
    NoMatrix(GateKind),
    /// Wrong number of resolved parameters for a parametrized gate (every
    /// parametrized gate in this vocabulary takes exactly one angle).
    #[error("gate '{gate:?}' expects {expected} parameter(s), got {got}")]
    WrongArity {
        gate: GateKind,
        expected: usize,
        got: usize,
    },
    /// This gate belongs to the OTHER arity function (e.g. calling `matrix2` with a
    /// single-qubit gate, or `matrix1` with a two-qubit gate).
    #[error("gate '{0:?}' is not a {1}-qubit gate")]
    WrongArityShape(GateKind, &'static str),
}

fn c(re: f64, im: f64) -> Complex64 {
    Complex64::new(re, im)
}

fn one_param(gate: &GateKind, params: &[f64]) -> Result<f64, GateError> {
    if params.len() != 1 {
        return Err(GateError::WrongArity {
            gate: gate.clone(),
            expected: 1,
            got: params.len(),
        });
    }
    Ok(params[0])
}

/// The dense 2x2 matrix for a single-qubit base gate (no controls -- controls are a
/// simulator-level masking concern, see `eg-quantum-sim`). Row/column order is the
/// standard `{|0>, |1>}` computational basis.
pub fn matrix1(gate: &GateKind, params: &[f64]) -> Result<[[Complex64; 2]; 2], GateError> {
    match gate {
        GateKind::Id => Ok([[c(1.0, 0.0), c(0.0, 0.0)], [c(0.0, 0.0), c(1.0, 0.0)]]),
        GateKind::X => Ok([[c(0.0, 0.0), c(1.0, 0.0)], [c(1.0, 0.0), c(0.0, 0.0)]]),
        GateKind::Y => Ok([[c(0.0, 0.0), c(0.0, -1.0)], [c(0.0, 1.0), c(0.0, 0.0)]]),
        GateKind::Z => Ok([[c(1.0, 0.0), c(0.0, 0.0)], [c(0.0, 0.0), c(-1.0, 0.0)]]),
        GateKind::H => {
            let s = std::f64::consts::FRAC_1_SQRT_2;
            Ok([[c(s, 0.0), c(s, 0.0)], [c(s, 0.0), c(-s, 0.0)]])
        }
        GateKind::S => Ok([[c(1.0, 0.0), c(0.0, 0.0)], [c(0.0, 0.0), c(0.0, 1.0)]]),
        GateKind::Sdg => Ok([[c(1.0, 0.0), c(0.0, 0.0)], [c(0.0, 0.0), c(0.0, -1.0)]]),
        GateKind::T => {
            let phase = c(
                (std::f64::consts::PI / 4.0).cos(),
                (std::f64::consts::PI / 4.0).sin(),
            );
            Ok([[c(1.0, 0.0), c(0.0, 0.0)], [c(0.0, 0.0), phase]])
        }
        GateKind::Tdg => {
            let phase = c(
                (std::f64::consts::PI / 4.0).cos(),
                -(std::f64::consts::PI / 4.0).sin(),
            );
            Ok([[c(1.0, 0.0), c(0.0, 0.0)], [c(0.0, 0.0), phase]])
        }
        GateKind::Rx => {
            let theta = one_param(gate, params)?;
            let (cos, sin) = ((theta / 2.0).cos(), (theta / 2.0).sin());
            Ok([[c(cos, 0.0), c(0.0, -sin)], [c(0.0, -sin), c(cos, 0.0)]])
        }
        GateKind::Ry => {
            let theta = one_param(gate, params)?;
            let (cos, sin) = ((theta / 2.0).cos(), (theta / 2.0).sin());
            Ok([[c(cos, 0.0), c(-sin, 0.0)], [c(sin, 0.0), c(cos, 0.0)]])
        }
        GateKind::Rz => {
            let theta = one_param(gate, params)?;
            let phase_neg = c(0.0, -theta / 2.0).exp();
            let phase_pos = c(0.0, theta / 2.0).exp();
            Ok([[phase_neg, c(0.0, 0.0)], [c(0.0, 0.0), phase_pos]])
        }
        GateKind::Phase => {
            let lambda = one_param(gate, params)?;
            let e = c(0.0, lambda).exp();
            Ok([[c(1.0, 0.0), c(0.0, 0.0)], [c(0.0, 0.0), e]])
        }
        GateKind::Swap | GateKind::Rzz | GateKind::Rxx | GateKind::Ryy => {
            Err(GateError::WrongArityShape(gate.clone(), "1"))
        }
        GateKind::Custom(_) => Err(GateError::NoMatrix(gate.clone())),
    }
}

/// The dense 4x4 matrix for a two-qubit, control-free base gate. Row/column order
/// is the standard `{|00>, |01>, |10>, |11>}` computational basis over the gate's
/// two OWN qubits (not control qubits -- this crate's vocabulary has no two-qubit
/// gate that is itself a control/target pair; `eg-quantum-core`'s `GateKind::X`/`Y`/
/// `Z` plus a `ControlQubit` cover CNOT/CY/CZ, applied via `matrix1` + simulator
/// control-masking, not via this function). Every gate this vocabulary offers here
/// (`Swap`/`Rxx`/`Ryy`/`Rzz`) is symmetric under exchanging its two qubits, so which
/// physical qubit a caller treats as "first" vs "second" does not affect the result.
pub fn matrix2(gate: &GateKind, params: &[f64]) -> Result<[[Complex64; 4]; 4], GateError> {
    let z = c(0.0, 0.0);
    match gate {
        GateKind::Swap => Ok([
            [c(1.0, 0.0), z, z, z],
            [z, z, c(1.0, 0.0), z],
            [z, c(1.0, 0.0), z, z],
            [z, z, z, c(1.0, 0.0)],
        ]),
        GateKind::Rxx => {
            let theta = one_param(gate, params)?;
            let (cos, sin) = ((theta / 2.0).cos(), (theta / 2.0).sin());
            let mi_sin = c(0.0, -sin);
            Ok([
                [c(cos, 0.0), z, z, mi_sin],
                [z, c(cos, 0.0), mi_sin, z],
                [z, mi_sin, c(cos, 0.0), z],
                [mi_sin, z, z, c(cos, 0.0)],
            ])
        }
        GateKind::Ryy => {
            let theta = one_param(gate, params)?;
            let (cos, sin) = ((theta / 2.0).cos(), (theta / 2.0).sin());
            let i_sin = c(0.0, sin);
            let mi_sin = c(0.0, -sin);
            Ok([
                [c(cos, 0.0), z, z, i_sin],
                [z, c(cos, 0.0), mi_sin, z],
                [z, mi_sin, c(cos, 0.0), z],
                [i_sin, z, z, c(cos, 0.0)],
            ])
        }
        GateKind::Rzz => {
            let theta = one_param(gate, params)?;
            let phase_neg = c(0.0, -theta / 2.0).exp();
            let phase_pos = c(0.0, theta / 2.0).exp();
            Ok([
                [phase_neg, z, z, z],
                [z, phase_pos, z, z],
                [z, z, phase_pos, z],
                [z, z, z, phase_neg],
            ])
        }
        GateKind::Id
        | GateKind::X
        | GateKind::Y
        | GateKind::Z
        | GateKind::H
        | GateKind::S
        | GateKind::Sdg
        | GateKind::T
        | GateKind::Tdg
        | GateKind::Rx
        | GateKind::Ry
        | GateKind::Rz
        | GateKind::Phase => Err(GateError::WrongArityShape(gate.clone(), "2")),
        GateKind::Custom(_) => Err(GateError::NoMatrix(gate.clone())),
    }
}

/// Whether `gate` has a `matrix1` representation (no controls, no params check).
pub fn is_single_qubit_gate(gate: &GateKind) -> bool {
    !matches!(
        gate,
        GateKind::Swap | GateKind::Rzz | GateKind::Rxx | GateKind::Ryy | GateKind::Custom(_)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_unitary2(m: [[Complex64; 2]; 2]) {
        // U U^dagger == I, within floating tolerance.
        for i in 0..2 {
            for j in 0..2 {
                let mut acc = c(0.0, 0.0);
                for (mik, mjk) in m[i].iter().zip(m[j].iter()) {
                    acc += *mik * mjk.conj();
                }
                let expect = if i == j { 1.0 } else { 0.0 };
                assert!(
                    (acc.re - expect).abs() < 1e-12 && acc.im.abs() < 1e-12,
                    "not unitary at ({i},{j}): {acc:?}"
                );
            }
        }
    }

    fn assert_unitary4(m: [[Complex64; 4]; 4]) {
        for i in 0..4 {
            for j in 0..4 {
                let mut acc = c(0.0, 0.0);
                for (mik, mjk) in m[i].iter().zip(m[j].iter()) {
                    acc += *mik * mjk.conj();
                }
                let expect = if i == j { 1.0 } else { 0.0 };
                assert!(
                    (acc.re - expect).abs() < 1e-9 && acc.im.abs() < 1e-9,
                    "not unitary at ({i},{j}): {acc:?}"
                );
            }
        }
    }

    #[test]
    fn every_clifford_generator_matrix_is_unitary() {
        for g in [
            GateKind::Id,
            GateKind::X,
            GateKind::Y,
            GateKind::Z,
            GateKind::H,
            GateKind::S,
            GateKind::Sdg,
        ] {
            assert_unitary2(matrix1(&g, &[]).unwrap());
        }
    }

    #[test]
    fn t_and_tdg_are_adjoint() {
        let t = matrix1(&GateKind::T, &[]).unwrap();
        let tdg = matrix1(&GateKind::Tdg, &[]).unwrap();
        // T[1][1] * Tdg[1][1] should be 1 (phases cancel).
        let prod = t[1][1] * tdg[1][1];
        assert!((prod.re - 1.0).abs() < 1e-12 && prod.im.abs() < 1e-12);
        assert_unitary2(t);
        assert_unitary2(tdg);
    }

    #[test]
    fn rotation_gates_are_unitary_for_arbitrary_angle() {
        let theta = 0.6180339887; // arbitrary, not a special Clifford angle
        for g in [GateKind::Rx, GateKind::Ry, GateKind::Rz, GateKind::Phase] {
            assert_unitary2(matrix1(&g, &[theta]).unwrap());
        }
    }

    #[test]
    fn wrong_arity_is_an_error_not_a_panic() {
        assert_eq!(
            matrix1(&GateKind::Rx, &[]),
            Err(GateError::WrongArity {
                gate: GateKind::Rx,
                expected: 1,
                got: 0
            })
        );
        assert_eq!(
            matrix1(&GateKind::Rx, &[1.0, 2.0]),
            Err(GateError::WrongArity {
                gate: GateKind::Rx,
                expected: 1,
                got: 2
            })
        );
    }

    #[test]
    fn two_qubit_gates_reject_matrix1_and_vice_versa() {
        assert!(matches!(
            matrix1(&GateKind::Swap, &[]),
            Err(GateError::WrongArityShape(GateKind::Swap, "1"))
        ));
        assert!(matches!(
            matrix2(&GateKind::H, &[]),
            Err(GateError::WrongArityShape(GateKind::H, "2"))
        ));
    }

    #[test]
    fn custom_gate_has_no_matrix() {
        let g = GateKind::Custom("mystery".to_string());
        assert_eq!(matrix1(&g, &[]), Err(GateError::NoMatrix(g.clone())));
        assert_eq!(matrix2(&g, &[]), Err(GateError::NoMatrix(g)));
    }

    #[test]
    fn every_two_qubit_gate_matrix_is_unitary() {
        assert_unitary4(matrix2(&GateKind::Swap, &[]).unwrap());
        let theta = 1.2345;
        for g in [GateKind::Rxx, GateKind::Ryy, GateKind::Rzz] {
            assert_unitary4(matrix2(&g, &[theta]).unwrap());
        }
    }

    #[test]
    fn hadamard_matches_known_bell_prep_amplitude() {
        let h = matrix1(&GateKind::H, &[]).unwrap();
        let s = std::f64::consts::FRAC_1_SQRT_2;
        assert!((h[0][0].re - s).abs() < 1e-12);
        assert!((h[0][1].re - s).abs() < 1e-12);
        assert!((h[1][0].re - s).abs() < 1e-12);
        assert!((h[1][1].re - (-s)).abs() < 1e-12);
    }
}
