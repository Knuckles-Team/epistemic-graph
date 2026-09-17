//! # eg-numeric — the epistemic-graph numeric kernel (CONCEPT:AU-KG.compute.numeric-kernel)
//!
//! A slim, **BLAS/LAPACK-free** numeric kernel: `ndarray` (arrays / reductions /
//! element-wise) + `nalgebra` (pure-Rust LAPACK-class linalg) — the "one kernel, two
//! surfaces" foundation of the Analytics Program.
//!
//! * **Surface A (`python` feature):** a pyo3 extension module `epistemic_graph.numeric`
//!   with bounded built-in Python sequence conversion and Python detachment for kernels,
//!   consumed by `agent_utilities.numeric.xp`. It has no Python numeric runtime dependency.
//! * **Surface B (rlib, always):** the same pure kernel the engine links for
//!   in-database analytics (DataFusion UDFs, graph/vector/timeseries ops) — no FFI,
//!   compute-near-data. `python` is OFF by default so the engine links **no pyo3**
//!   (the Plan-01 no-pyo3-in-engine contract holds).
//!
//! The pure kernel (`reductions`, `elementwise`, `linalg`, `random`) is
//! parity-tested against the isolated developer reference implementation in
//! `agent_utilities` and in-crate tests.
//!
//! The statistics modules (`detkernel`, `calibration`, `risk`, `conformal`,
//! `ope`) are bit-reproducible across release targets: every transcendental goes
//! through the pinned software `libm` (the crate `clippy.toml` bans the std
//! float transcendentals crate-wide), reductions are serial and ordered, and
//! digests are taken over quantised integers.

pub mod cluster;
pub mod elementwise;
pub mod error;
pub mod linalg;
pub mod random;
pub mod reductions;
// Deterministic statistics for replayable decisions (Decide layer, §4.4 and §6
// of the design): pinned soft-float transcendentals and quantised digests
// (`detkernel`), calibration, risk control, conformal prediction and off-policy
// evaluation. Always built: they add only the pinned pure-Rust `libm`.
pub mod calibration;
pub mod conformal;
pub mod detkernel;
pub mod ope;
pub mod risk;
// ModalityContract retrofit (CONCEPT:E4): `impl ModalityContract for
// cluster::KMeansResult` + the `modality_conformance_tests!` battery. Behind the
// crate's own opt-in `contract` feature (default OFF). See `src/contract.rs`.
#[cfg(feature = "contract")]
mod contract;
// scipy.stats-parity ops (CONCEPT:EG-KG.compute.numeric-stats/EG-358). Gated behind `analytics` (pulled
// by `python`) so a `pi`/`default` engine build linking the rlib pulls no statrs.
#[cfg(feature = "analytics")]
pub mod stats;
// Complex64 surface (D-QN-3 handoff, executed in Q1 lane w3-quantum-q1): a thin
// `Complex64` re-export + generic dense-block index application that
// `eg-quantum-sim`'s statevector backend consumes. Behind the crate's own opt-in
// `complex` feature (default OFF) — see `src/complex.rs` for the numeric-stack
// decision this executes.
#[cfg(feature = "complex")]
pub mod complex;

pub use error::{NumericError, Result};

// ---------------------------------------------------------------------------
// Surface A — pyo3 Python extension module `epistemic_graph.numeric`.
// Gated behind the `python` feature so the engine-linked rlib pulls no pyo3.
// The operation names remain stable, but the boundary contract is deliberately
// built-in-only: scalar results are Python scalars, array results are nested
// Python lists, and NumPy-only constructors/dtype passthroughs are not exported.
// The richer compatibility facade is owned by the agent-utilities numeric layer.
// ---------------------------------------------------------------------------
#[cfg(feature = "python")]
// pyo3 bindings carry unavoidable boilerplate lints: PyErr .into() round-trips
// (useless_conversion), complex return types (type_complexity), and PyO3
// macro-generated cfgs are scoped to this optional extension module.
#[allow(clippy::useless_conversion, clippy::type_complexity, unexpected_cfgs)]
mod py;

#[cfg(test)]
mod tests {
    use crate::{linalg, reductions};
    use ndarray::{array, Array1};

    #[test]
    fn reductions_basic() {
        let a: Array1<f64> = array![1.0, 2.0, 3.0, 4.0];
        assert_eq!(reductions::sum(a.view()), 10.0);
        assert_eq!(reductions::mean(a.view()), 2.5);
        assert!((reductions::std(a.view(), 0) - 1.118_033_988_749_895).abs() < 1e-12);
        assert_eq!(reductions::argmax(a.view()).unwrap(), 3);
    }

    #[test]
    fn solve_matches_hand() {
        // [[3,2],[1,2]] x = [7,5] -> x = [1, 2]
        let a = array![[3.0, 2.0], [1.0, 2.0]];
        let b = array![7.0, 5.0];
        let x = linalg::solve(a.view(), b.view()).unwrap();
        assert!((x[0] - 1.0).abs() < 1e-10);
        assert!((x[1] - 2.0).abs() < 1e-10);
    }

    #[test]
    fn batch_l2_normalize_units() {
        // [3,4] → [0.6,0.8]; a zero vector is returned unchanged (safe divide).
        let out = linalg::batch_l2_normalize(&[vec![3.0, 4.0], vec![0.0, 0.0]]);
        assert!((out[0][0] - 0.6).abs() < 1e-12);
        assert!((out[0][1] - 0.8).abs() < 1e-12);
        assert_eq!(out[1], vec![0.0, 0.0]);
        // A unit vector's L2 norm is 1.
        let n = linalg::norm(ndarray::ArrayView1::from(&out[0]));
        assert!((n - 1.0).abs() < 1e-12);
    }

    #[test]
    fn singular_solve_errors() {
        let a = array![[1.0, 2.0], [2.0, 4.0]];
        let b = array![1.0, 2.0];
        assert!(linalg::solve(a.view(), b.view()).is_err());
    }
}
