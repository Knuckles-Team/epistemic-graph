//! Q6 — the numeric bridge: expectations, probability distributions and bitstrings
//! from a [`QuantumResult`] into `eg-numeric` arrays, plus a batch API for running
//! many small circuits (PROGRAM.md lane Q6: "bridge expectations, probability
//! distributions and bitstrings into eg-numeric; provide a batch API for many small
//! circuits").
//!
//! Every function here is a PURE, allocation-only transform — no I/O, no job-plane
//! coupling — so [`crate::job`] can call it while staging a result, and a future Q7/Q8
//! caller can call it directly on any `QuantumResult` it already holds (e.g. from a
//! synchronous `backend.run()`), independent of whether the result ever touched
//! `eg-jobs` at all.

use eg_numeric::error::NumericError;
use eg_quantum_core::backend::{BackendError, QuantumBackend, RunOptions};
use eg_quantum_core::ir::QuantumProgram;
use eg_quantum_core::result::{Outcome, QuantumResult};
use ndarray::Array1;

/// A [`QuantumResult`]'s `Outcome::Counts` decomposed into `eg-numeric`-shaped
/// arrays: the distinct bitstrings observed (lexicographically sorted — a stable,
/// deterministic order independent of `BTreeMap` iteration incidentally already being
/// sorted, so this stays correct even if `Outcome::Counts`' inner type ever changes)
/// and their normalized probabilities in the SAME order, `probabilities[i]`
/// corresponding to `bitstrings[i]`.
#[derive(Debug, Clone, PartialEq)]
pub struct BitstringDistribution {
    pub bitstrings: Vec<String>,
    pub probabilities: Array1<f64>,
    pub total_shots: u64,
}

/// Per-qubit marginal `P(qubit_i = 1)` estimated from `Outcome::Counts`, as one
/// `eg-numeric` array indexed `0..n_qubits` — the shape a caller wants to feed
/// straight into `eg-numeric`'s reduction/stats surface (e.g. comparing observed
/// marginals against a classical prior array element-wise).
pub fn marginal_probabilities(outcome: &Outcome, n_qubits: u32) -> Result<Array1<f64>, NumericError> {
    match outcome {
        Outcome::Counts(counts) => {
            let total: u64 = counts.values().sum();
            if total == 0 {
                return Err(NumericError::Shape(
                    "marginal_probabilities: Outcome::Counts has zero total shots".to_string(),
                ));
            }
            let mut ones = vec![0u64; n_qubits as usize];
            for (bitstring, count) in counts {
                let bits: Vec<char> = bitstring.chars().collect();
                if bits.len() != n_qubits as usize {
                    return Err(NumericError::Shape(format!(
                        "marginal_probabilities: outcome bitstring '{bitstring}' has {} bits, expected {n_qubits}",
                        bits.len()
                    )));
                }
                for (qubit, bit) in bits.iter().enumerate() {
                    if *bit == '1' {
                        ones[qubit] += count;
                    }
                }
            }
            Ok(Array1::from_vec(
                ones.into_iter()
                    .map(|c| c as f64 / total as f64)
                    .collect(),
            ))
        }
        Outcome::ExpectationValue { .. } => Err(NumericError::Shape(
            "marginal_probabilities requires Outcome::Counts, not an ExpectationValue".to_string(),
        )),
    }
}

/// The full observed bitstring distribution, normalized. Ordered by bitstring
/// (lexicographic), not by count, so two runs that observed the SAME support (even
/// with different shot totals) produce arrays a caller can compare index-for-index.
pub fn bitstring_distribution(outcome: &Outcome) -> Result<BitstringDistribution, NumericError> {
    match outcome {
        Outcome::Counts(counts) => {
            let total: u64 = counts.values().sum();
            if total == 0 {
                return Err(NumericError::Shape(
                    "bitstring_distribution: Outcome::Counts has zero total shots".to_string(),
                ));
            }
            let bitstrings: Vec<String> = counts.keys().cloned().collect(); // BTreeMap: already sorted
            let probabilities = Array1::from_vec(
                counts
                    .values()
                    .map(|&c| c as f64 / total as f64)
                    .collect(),
            );
            Ok(BitstringDistribution {
                bitstrings,
                probabilities,
                total_shots: total,
            })
        }
        Outcome::ExpectationValue { .. } => Err(NumericError::Shape(
            "bitstring_distribution requires Outcome::Counts, not an ExpectationValue".to_string(),
        )),
    }
}

/// Batch a set of `(value, stderr)` [`Outcome::ExpectationValue`] results into one
/// `eg-numeric` array of values plus a parallel array of standard errors (`NAN` where
/// a given result carried no `stderr`, e.g. an exact result — `NAN` rather than `0.0`
/// so a caller cannot mistake "no error reported" for "zero error", and `eg-numeric`'s
/// own reductions already treat `NAN` as the documented not-a-number sentinel).
pub fn expectation_array(results: &[QuantumResult]) -> Result<(Array1<f64>, Array1<f64>), NumericError> {
    let mut values = Vec::with_capacity(results.len());
    let mut stderrs = Vec::with_capacity(results.len());
    for result in results {
        match &result.outcome {
            Outcome::ExpectationValue { value, stderr } => {
                values.push(*value);
                stderrs.push(stderr.unwrap_or(f64::NAN));
            }
            Outcome::Counts(_) => {
                return Err(NumericError::Shape(
                    "expectation_array requires every result to carry Outcome::ExpectationValue"
                        .to_string(),
                ))
            }
        }
    }
    Ok((Array1::from_vec(values), Array1::from_vec(stderrs)))
}

/// Run a BATCH of many small circuits against one backend (PROGRAM.md Q6: "provide a
/// batch API for many small circuits") — the shape a variational sweep (many
/// parameter bindings of ONE compiled `QuantumProgram`) or a per-candidate-set QAOA
/// instance actually needs, instead of a caller hand-rolling the loop. Deliberately
/// SERIAL (no new thread-pool/rayon dependency — see PROGRAM.md's "prefer zero new
/// crates.io dependencies"): every `eg-quantum-sim` backend is already CPU-bound
/// in-process work with no I/O to overlap, so a caller that wants concurrency runs
/// several `run_batch` calls on their own executor/thread pool rather than this crate
/// importing one. Preserves input order; a per-circuit failure is `Err` at that index
/// rather than aborting the whole batch, so a caller can still use the results that
/// succeeded.
pub fn run_batch(
    backend: &dyn QuantumBackend,
    programs: &[(QuantumProgram, RunOptions)],
) -> Vec<Result<QuantumResult, BackendError>> {
    programs
        .iter()
        .map(|(program, opts)| backend.run(program, opts))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn counts(pairs: &[(&str, u64)]) -> Outcome {
        Outcome::Counts(pairs.iter().map(|(k, v)| (k.to_string(), *v)).collect())
    }

    #[test]
    fn marginal_probabilities_ghz_all_agree() {
        // A perfect GHZ over 3 qubits: only "000" and "111" observed, ~50/50.
        let outcome = counts(&[("000", 512), ("111", 512)]);
        let marginals = marginal_probabilities(&outcome, 3).unwrap();
        assert_eq!(marginals.len(), 3);
        for &p in marginals.iter() {
            assert!((p - 0.5).abs() < 1e-9, "GHZ marginal must be exactly 0.5, got {p}");
        }
    }

    #[test]
    fn marginal_probabilities_rejects_wrong_width() {
        let outcome = counts(&[("00", 10)]);
        let err = marginal_probabilities(&outcome, 3).unwrap_err();
        assert!(matches!(err, NumericError::Shape(_)));
    }

    #[test]
    fn bitstring_distribution_normalizes_and_sorts() {
        let outcome = counts(&[("11", 25), ("00", 75)]);
        let dist = bitstring_distribution(&outcome).unwrap();
        assert_eq!(dist.bitstrings, vec!["00", "11"]);
        assert!((dist.probabilities[0] - 0.75).abs() < 1e-12);
        assert!((dist.probabilities[1] - 0.25).abs() < 1e-12);
        assert_eq!(dist.total_shots, 100);
    }

    #[test]
    fn expectation_array_batches_values_and_marks_missing_stderr() {
        use eg_quantum_core::backend::BackendId;
        use eg_quantum_core::hash::circuit_hash;
        use eg_quantum_core::ir::{ProgramMetadata, QuantumProgram, IR_VERSION};
        use eg_quantum_core::result::Formalism;

        let program = QuantumProgram {
            ir_version: IR_VERSION,
            n_qubits: 1,
            classical_registers: vec![],
            parameters: vec![],
            instructions: vec![],
            metadata: ProgramMetadata::default(),
        };
        let hash = circuit_hash(&program).unwrap();
        let exact = QuantumResult::new_exact(
            BackendId::from("stub"),
            Formalism::Statevector,
            None,
            None,
            hash,
            0,
            0,
            Outcome::ExpectationValue {
                value: 0.5,
                stderr: None,
            },
        );
        let sampled = QuantumResult::new_inexact(
            BackendId::from("stub"),
            Formalism::Trajectory,
            Some(7),
            Some(100),
            hash,
            None,
            None,
            0,
            0,
            Outcome::ExpectationValue {
                value: -0.2,
                stderr: Some(0.05),
            },
        );
        let (values, stderrs) = expectation_array(&[exact, sampled]).unwrap();
        assert_eq!(values.to_vec(), vec![0.5, -0.2]);
        assert!(stderrs[0].is_nan());
        assert!((stderrs[1] - 0.05).abs() < 1e-12);
    }
}
