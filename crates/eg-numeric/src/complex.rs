//! Complex64 surface — the numeric-stack decision handoff from `eg-quantum-core`
//! (D-QN-3, lane w0-eg-quantum-contracts) executed here in Q1.
//!
//! **Decision (recorded in `eg-quantum-core::lib` and `eg-quantum-sim::lib`,
//! executed here): the quantum numeric path bridges to `eg-numeric`. It does not
//! carry a second numeric stack.** `nalgebra` (a mandatory dependency of this crate
//! already) resolves `num-complex 0.4.6` transitively — confirmed in the workspace
//! `Cargo.lock` before this feature was added. Promoting it to a direct dependency
//! behind this crate's own `complex` feature (default OFF, not folded into
//! `default`/`pi`/`node`/`full`) therefore adds **zero new crates** to the resolved
//! dependency graph; it only exposes a type this crate's own transitive graph
//! already builds.
//!
//! This module is deliberately domain-neutral — it knows about complex dense-vector
//! index arithmetic, not about qubits, gates, or circuits. `eg-quantum-sim` (Q1)
//! supplies the quantum vocabulary on top of `apply_block2`/`apply_block4`; keeping
//! that boundary here is what stops `eg-numeric` from picking up a quantum-shaped
//! API surface it would otherwise have no reason to carry.

pub use num_complex::Complex64;

/// Apply a dense 2x2 unitary (or any 2x2 matrix) to every pair of vector entries
/// that differ only in bit `bit` of their index, optionally gated by a mask/expected
/// predicate over the OTHER bits (a controlled-gate application).
///
/// `state.len()` must be a power of two and `bit < state.len().trailing_zeros()`.
/// `gate_fn(index) -> bool` decides, given the LOW index of a pair (the one with bit
/// `bit` == 0), whether this pair is touched at all — callers that need unconditional
/// application pass `|_| true`; callers implementing a controlled gate pass a
/// predicate that checks the relevant control bits of `index`.
///
/// This is the one primitive a 1-qubit statevector gate application needs; kept
/// generic (no "qubit"/"gate" vocabulary) so it is equally usable for any dense
/// tensor-index pairwise transform, not just quantum gates.
pub fn apply_block2(
    state: &mut [Complex64],
    bit: u32,
    m: [[Complex64; 2]; 2],
    gate_fn: impl Fn(usize) -> bool,
) {
    let stride = 1usize << bit;
    let n = state.len();
    let mut i0 = 0usize;
    while i0 < n {
        for offset in 0..stride {
            let lo = i0 + offset;
            let hi = lo + stride;
            if gate_fn(lo) {
                let a = state[lo];
                let b = state[hi];
                state[lo] = m[0][0] * a + m[0][1] * b;
                state[hi] = m[1][0] * a + m[1][1] * b;
            }
        }
        i0 += stride * 2;
    }
}

/// Apply a dense 4x4 unitary to every quadruple of vector entries that differ only
/// in bits `bit_a` and `bit_b` (`bit_a != bit_b`) of their index, gated the same way
/// as [`apply_block2`]. The quadruple is ordered `(bit_b, bit_a)` as the two most
/// significant bits of the local 2-bit index, i.e. row/column `0b(bit_b)(bit_a)` —
/// callers building `m` from a qubit-pair gate must order rows/columns to match.
pub fn apply_block4(
    state: &mut [Complex64],
    bit_a: u32,
    bit_b: u32,
    m: [[Complex64; 4]; 4],
    gate_fn: impl Fn(usize) -> bool,
) {
    assert!(bit_a != bit_b, "apply_block4 requires two distinct bits");
    let (lo_bit, hi_bit) = if bit_a < bit_b {
        (bit_a, bit_b)
    } else {
        (bit_b, bit_a)
    };
    let n = state.len();
    let mask_lo = 1usize << lo_bit;
    let mask_hi = 1usize << hi_bit;
    for base in 0..n {
        // Only process each quadruple once: base must have both bits == 0.
        if base & mask_lo != 0 || base & mask_hi != 0 {
            continue;
        }
        if !gate_fn(base) {
            continue;
        }
        let idx =
            |a_bit: usize, b_bit: usize| -> usize { base | (a_bit << bit_a) | (b_bit << bit_b) };
        let i00 = base;
        let i01 = idx(1, 0); // bit_a = 1, bit_b = 0
        let i10 = idx(0, 1); // bit_a = 0, bit_b = 1
        let i11 = idx(1, 1);
        // Local basis order (bit_b, bit_a): 00, 01, 10, 11 where the row bit
        // pattern is (bit_b_val, bit_a_val) read MSB-first.
        let v = [state[i00], state[i01], state[i10], state[i11]];
        let mut out = [Complex64::new(0.0, 0.0); 4];
        for (r, row) in m.iter().enumerate() {
            let mut acc = Complex64::new(0.0, 0.0);
            for (c, mc) in row.iter().enumerate() {
                acc += *mc * v[c];
            }
            out[r] = acc;
        }
        state[i00] = out[0];
        state[i01] = out[1];
        state[i10] = out[2];
        state[i11] = out[3];
    }
}

/// `<a|b>` — the standard Hermitian inner product.
pub fn inner_product(a: &[Complex64], b: &[Complex64]) -> Complex64 {
    assert_eq!(a.len(), b.len());
    a.iter()
        .zip(b.iter())
        .fold(Complex64::new(0.0, 0.0), |acc, (x, y)| acc + x.conj() * y)
}

/// L2 norm of a complex vector.
pub fn norm(a: &[Complex64]) -> f64 {
    inner_product(a, a).re.sqrt()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn c(re: f64, im: f64) -> Complex64 {
        Complex64::new(re, im)
    }

    #[test]
    fn apply_block2_hadamard_on_2_qubit_zero_state() {
        // |00> -> H on qubit 0 -> (|00> + |01>)/sqrt(2)
        let mut state = vec![c(1.0, 0.0), c(0.0, 0.0), c(0.0, 0.0), c(0.0, 0.0)];
        let s = 1.0 / std::f64::consts::SQRT_2;
        let h = [[c(s, 0.0), c(s, 0.0)], [c(s, 0.0), c(-s, 0.0)]];
        apply_block2(&mut state, 0, h, |_| true);
        assert!((state[0].re - s).abs() < 1e-12);
        assert!((state[1].re - s).abs() < 1e-12);
        assert!(state[2].norm() < 1e-12);
        assert!(state[3].norm() < 1e-12);
    }

    #[test]
    fn apply_block2_respects_control_predicate() {
        // CNOT(control=1, target=0) on |10> (index 0b10 = 2) must flip bit 0 -> |11>.
        // On |00> (index 0) it must do nothing (control bit 1 is 0).
        let mut state = vec![c(1.0, 0.0), c(0.0, 0.0), c(0.0, 0.0), c(0.0, 0.0)];
        let x = [[c(0.0, 0.0), c(1.0, 0.0)], [c(1.0, 0.0), c(0.0, 0.0)]];
        // control qubit is bit 1; only touch pairs where bit 1 == 1.
        apply_block2(&mut state, 0, x, |idx| (idx >> 1) & 1 == 1);
        assert_eq!(
            state,
            vec![c(1.0, 0.0), c(0.0, 0.0), c(0.0, 0.0), c(0.0, 0.0)]
        );

        let mut state2 = vec![c(0.0, 0.0), c(0.0, 0.0), c(1.0, 0.0), c(0.0, 0.0)];
        apply_block2(&mut state2, 0, x, |idx| (idx >> 1) & 1 == 1);
        assert_eq!(
            state2,
            vec![c(0.0, 0.0), c(0.0, 0.0), c(0.0, 0.0), c(1.0, 0.0)]
        );
    }

    #[test]
    fn apply_block4_cnot_matrix_matches_bitwise_cnot() {
        // 4x4 CNOT (control = bit_b, target = bit_a) applied to |10> must give |11>.
        let cnot: [[Complex64; 4]; 4] = [
            [c(1.0, 0.0), c(0.0, 0.0), c(0.0, 0.0), c(0.0, 0.0)],
            [c(0.0, 0.0), c(1.0, 0.0), c(0.0, 0.0), c(0.0, 0.0)],
            [c(0.0, 0.0), c(0.0, 0.0), c(0.0, 0.0), c(1.0, 0.0)],
            [c(0.0, 0.0), c(0.0, 0.0), c(1.0, 0.0), c(0.0, 0.0)],
        ];
        let mut state = vec![c(0.0, 0.0); 4];
        state[0b10] = c(1.0, 0.0); // bit_a=0 (=target), bit_b=1 (=control)
        apply_block4(&mut state, 0, 1, cnot, |_| true);
        let mut expect = vec![c(0.0, 0.0); 4];
        expect[0b11] = c(1.0, 0.0);
        assert_eq!(state, expect);
    }

    #[test]
    fn inner_product_and_norm_of_bell_pair_amplitude() {
        let s = 1.0 / std::f64::consts::SQRT_2;
        let bell = vec![c(s, 0.0), c(0.0, 0.0), c(0.0, 0.0), c(s, 0.0)];
        assert!((norm(&bell) - 1.0).abs() < 1e-12);
        assert!((inner_product(&bell, &bell).re - 1.0).abs() < 1e-12);
    }
}
