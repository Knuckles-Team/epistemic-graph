//! Behaviour of the deterministic kernels against closed forms.

use crate::common::{assert_close, level};
use eg_numeric::detkernel::kernels::{entropy, log_sigmoid, log_softmax, log_sum_exp, logit, sigmoid, softmax, softplus};
use eg_numeric::detkernel::math;
use eg_numeric::detkernel::optimise::{minimise_convex_bounded, minimise_convex_unbounded};
use eg_numeric::detkernel::quantise::{dequantise, quantise, QuantScale, QuantisedVector};
use eg_numeric::detkernel::rational::sums_to_one;
use eg_numeric::detkernel::reduce::{argmax_first, compensated_sum, order_ascending, order_descending, serial_sum};
use eg_numeric::detkernel::{Level, Propensity, StatError};
use eg_numeric::NumericError;

#[test]
fn log_sum_exp_matches_closed_forms_and_does_not_overflow() {
    assert_close(log_sum_exp(&[0.0, 0.0]).unwrap(), math::ln(2.0), 1e-15, "lse [0,0]");
    assert_close(log_sum_exp(&[1000.0, 1000.0]).unwrap(), 1000.0 + math::ln(2.0), 1e-12, "lse large");
    assert_close(log_sum_exp(&[-5.0]).unwrap(), -5.0, 0.0, "lse single");
    assert_eq!(log_sum_exp(&[]), Err(StatError::Empty { what: "log_sum_exp input" }));
    assert!(matches!(log_sum_exp(&[1.0, f64::NAN]), Err(StatError::NonFinite { index: 1, .. })));
}

#[test]
fn softmax_is_normalised_shift_invariant_and_exact_on_three_values() {
    let p = softmax(&[1.0, 2.0, 3.0]).unwrap();
    let denominator = math::exp(1.0) + math::exp(2.0) + math::exp(3.0);
    for (k, value) in p.iter().enumerate() {
        assert_close(*value, math::exp(k as f64 + 1.0) / denominator, 1e-15, "softmax entry");
    }
    assert_close(serial_sum(&p), 1.0, 1e-15, "softmax sum");
    let shifted = softmax(&[501.0, 502.0, 503.0]).unwrap();
    for (a, b) in p.iter().zip(&shifted) {
        assert_close(*a, *b, 1e-14, "shift invariance");
    }
    let log_p = log_softmax(&[1.0, 2.0, 3.0]).unwrap();
    assert_close(log_p[2], math::ln(p[2]), 1e-14, "log_softmax");
}

#[test]
fn entropy_sigmoid_softplus_and_logit_closed_forms() {
    assert_close(entropy(&[0.25; 4]).unwrap(), math::ln(4.0), 1e-15, "uniform entropy");
    assert_eq!(entropy(&[1.0, 0.0]).unwrap(), 0.0);
    assert!(entropy(&[0.5, 0.6]).is_err(), "off-simplex input is refused");
    assert_eq!(sigmoid(0.0), 0.5);
    assert!(sigmoid(-800.0) >= 0.0 && sigmoid(800.0) == 1.0);
    assert_close(sigmoid(1.7) + sigmoid(-1.7), 1.0, 1e-15, "sigmoid symmetry");
    assert_close(softplus(0.0), math::ln(2.0), 1e-15, "softplus(0)");
    assert_close(log_sigmoid(-1000.0), -1000.0, 1e-12, "log_sigmoid tail");
    assert_close(logit(sigmoid(2.5)).unwrap(), 2.5, 1e-12, "logit inverts sigmoid");
    assert!(logit(0.0).is_err() && logit(1.0).is_err());
}

#[test]
fn quantisation_rounds_half_even_and_encodes_canonically() {
    let unit = 1.0 / 4_294_967_296.0;
    assert_eq!(quantise(0.5 * unit, QuantScale::Q32).unwrap(), 0);
    assert_eq!(quantise(1.5 * unit, QuantScale::Q32).unwrap(), 2);
    assert_eq!(quantise(-2.5 * unit, QuantScale::Q32).unwrap(), -2);
    assert_eq!(quantise(0.25, QuantScale::Pico).unwrap(), 250_000_000_000);
    assert_eq!(dequantise(250_000_000_000, QuantScale::Pico), 0.25);
    assert!(matches!(quantise(f64::NAN, QuantScale::Pico), Err(StatError::NonFinite { .. })));
    assert_eq!(quantise(1e10, QuantScale::Pico), Err(StatError::QuantiseOverflow { index: 0 }));
    let vector = QuantisedVector::from_f64s(&[0.5, -1.0], QuantScale::Q32).unwrap();
    let mut expected = vec![2u8];
    expected.extend_from_slice(&2u64.to_be_bytes());
    expected.extend_from_slice(&2_147_483_648i64.to_be_bytes());
    expected.extend_from_slice(&(-4_294_967_296i64).to_be_bytes());
    assert_eq!(vector.canonical_bytes(), expected);
    assert_eq!(
        QuantisedVector::from_f64s(&[0.0, f64::INFINITY], QuantScale::Pico),
        Err(StatError::NonFinite { what: "quantise input", index: 1 })
    );
}

#[test]
fn rationals_reduce_and_compute_exact_ranks() {
    assert_eq!(level(2, 20), level(1, 10));
    assert_eq!(level(1, 10).rational().denominator(), 10);
    assert!(Level::new(0, 5).is_err() && Level::new(5, 5).is_err() && Level::new(1, 0).is_err());
    assert_eq!(level(1, 10).conformal_rank(9), 9);
    assert_eq!(level(1, 10).conformal_rank(8), 9);
    assert_eq!(level(1, 20).conformal_rank(99), 95);
    assert_eq!(level(1, 10).conformal_minimum_n(), 9);
    assert_eq!(level(1, 3).conformal_minimum_n(), 2);
    let third = Propensity::new(1, 3).unwrap();
    let parts = [Propensity::new(1, 2).unwrap(), third, Propensity::new(1, 6).unwrap()];
    assert!(sums_to_one(&parts).unwrap());
    assert!(!sums_to_one(&[Propensity::new(1, 2).unwrap(), third]).unwrap());
    assert_eq!(third.importance_weight(0.5), Some(1.5));
    assert_eq!(Propensity::new(0, 7).unwrap().importance_weight(0.5), None);
}

#[test]
fn convex_minimiser_finds_interior_and_boundary_optima() {
    let unbounded = minimise_convex_unbounded(|x| 2.0 * (x - 3.0), 0.0, 1.0, 200).unwrap();
    assert_close(unbounded, 3.0, 1e-12, "unbounded optimum");
    let left = minimise_convex_unbounded(|x| 2.0 * (x + 40.0), 0.0, 1.0, 200).unwrap();
    assert_close(left, -40.0, 1e-12, "descent to the left");
    let clipped = minimise_convex_bounded(|x| 2.0 * (x - 3.0), -1.0, 1.0, 200).unwrap();
    assert_eq!(clipped, 1.0);
    let interior = minimise_convex_bounded(|x| x - 0.25, 0.0, 1.0, 200).unwrap();
    assert_close(interior, 0.25, 1e-15, "bounded interior");
    assert!(minimise_convex_bounded(|_| f64::NAN, 0.0, 1.0, 10).is_err());
    assert!(minimise_convex_unbounded(|_| -1.0, 0.0, 1.0, 10).is_err());
}

#[test]
fn orderings_break_ties_by_index_and_sums_are_ordered() {
    let values = [0.5, 0.1, 0.5, 0.9];
    assert_eq!(order_ascending(&values), vec![1, 0, 2, 3]);
    assert_eq!(order_descending(&values), vec![3, 0, 2, 1]);
    assert_eq!(argmax_first(&[0.2, 0.7, 0.7], "x").unwrap(), 1);
    assert_eq!(serial_sum(&[1e16, 1.0, -1e16]), 0.0);
    assert_eq!(compensated_sum(&[1e16, 1.0, -1e16]), 1.0);
}

#[test]
fn errors_display_and_convert_to_the_numeric_error() {
    let error = StatError::UnsupportedAction { record: 3, action: 1 };
    assert_eq!(
        error.to_string(),
        "record 3: action 1 has target mass but logging propensity 0"
    );
    assert_eq!(
        NumericError::from(error),
        NumericError::Bounds("record 3: action 1 has target mass but logging propensity 0".into())
    );
}
