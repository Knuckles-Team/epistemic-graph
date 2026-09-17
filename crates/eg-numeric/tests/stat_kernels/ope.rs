//! Off-policy evaluation: hand-computed exact estimator values on a fixed
//! logged data set (so a planted arithmetic bug in any estimator is caught
//! exactly), support-refusal behaviour, and a seeded-draw property check that
//! IPS and doubly robust recover a planted policy value.

use crate::common::{assert_close, categorical, rng};
use eg_numeric::detkernel::{Propensity, StatError};
use eg_numeric::ope::{
    clipped_ips, doubly_robust, effective_sample_size, ips, require_support, snips, support_report,
    switch, EssGate, LoggedDecision,
};

/// Fixed logging policy `mu = [1/2, 1/4, 1/4]`, target `pi = [1/4, 1/4, 1/2]`
/// and a deterministic (noiseless) reward `r(a) = [1, 2, 3][a]`, replayed over
/// nine executed actions `[0, 0, 1, 1, 1, 2, 2, 2, 2]` — chosen so every
/// estimator's value is exact rational arithmetic, computed by hand in the
/// doc comments below and cross-checked against the code's output.
fn fixed_log() -> Vec<LoggedDecision> {
    let mu = [
        Propensity::new(2, 4).unwrap(),
        Propensity::new(1, 4).unwrap(),
        Propensity::new(1, 4).unwrap(),
    ];
    let pi = [0.25, 0.25, 0.5];
    let reward = [1.0, 2.0, 3.0];
    let actions = [0usize, 0, 1, 1, 1, 2, 2, 2, 2];
    actions
        .iter()
        .map(|&a| {
            LoggedDecision::new(a, reward[a], mu.to_vec(), pi.to_vec())
                .unwrap()
                .with_reward_model(vec![1.0, 2.0, 3.0])
                .unwrap()
        })
        .collect()
}

#[test]
fn ips_matches_the_hand_computed_importance_weighted_mean() {
    // weights = pi/mu = [0.5, 1.0, 2.0]; contributions per record:
    // 2 x (0.5 * 1) + 3 x (1.0 * 2) + 4 x (2.0 * 3) = 1 + 6 + 24 = 31, / 9.
    let result = ips(&fixed_log()).unwrap();
    assert_close(result.value, 31.0 / 9.0, 1e-9, "ips value");
    assert_eq!(result.n, 9);
    assert_close(result.max_weight, 2.0, 1e-12, "ips max weight");
    // sum w = 12, sum w^2 = 19.5 => ESS = 144 / 19.5.
    assert_close(result.effective_sample_size, 144.0 / 19.5, 1e-9, "ips ESS");
    assert!(result.std_error.is_finite() && result.std_error > 0.0);
}

#[test]
fn clipped_ips_caps_the_large_weight_only() {
    // cap = 1.5 leaves weights 0.5 and 1.0 alone and caps 2.0: contributions
    // become 2 x 0.5 + 3 x 2.0 + 4 x (1.5 * 3) = 1 + 6 + 18 = 25, / 9.
    let result = clipped_ips(&fixed_log(), 1.5).unwrap();
    assert_close(result.value, 25.0 / 9.0, 1e-9, "clipped ips value");
    assert_close(result.max_weight, 1.5, 1e-12, "clipped ips max weight");
    // sum w = 10, sum w^2 = 12.5 => ESS = 8 exactly.
    assert_close(result.effective_sample_size, 8.0, 1e-9, "clipped ips ESS");
    // a cap at or above the largest weight reproduces plain IPS exactly.
    let uncapped = clipped_ips(&fixed_log(), 100.0).unwrap();
    assert_close(
        uncapped.value,
        ips(&fixed_log()).unwrap().value,
        1e-12,
        "large cap matches IPS",
    );
    assert!(
        clipped_ips(&fixed_log(), 0.0).is_err() && clipped_ips(&fixed_log(), f64::NAN).is_err()
    );
}

#[test]
fn snips_self_normalises_by_the_weight_sum() {
    // sum(w r) = 31, sum(w) = 2x0.5 + 3x1.0 + 4x2.0 = 1 + 3 + 8 = 12.
    let result = snips(&fixed_log()).unwrap();
    assert_close(result.value, 31.0 / 12.0, 1e-9, "snips value");
}

#[test]
fn snips_refuses_a_log_whose_weights_sum_to_zero() {
    let mu = vec![
        Propensity::new(1, 2).unwrap(),
        Propensity::new(1, 2).unwrap(),
    ];
    // target puts zero mass on the executed action, so its importance weight
    // is exactly 0 (not unsupported: target[0] == 0 short-circuits).
    let zero_weight = LoggedDecision::new(0, 5.0, mu, vec![0.0, 1.0]).unwrap();
    let records = vec![zero_weight.clone(), zero_weight];
    assert!(snips(&records).is_err());
}

#[test]
fn doubly_robust_is_exact_when_the_reward_model_matches_the_reward() {
    // The reward model equals the reward for every action with zero noise, so
    // the correction term w (r - q(a)) is exactly 0 on every record and the
    // estimate reduces to sum_a pi(a) q(a) = 0.25*1 + 0.25*2 + 0.5*3 = 2.25,
    // independent of which actions were executed.
    let result = doubly_robust(&fixed_log()).unwrap();
    assert_close(result.value, 2.25, 1e-12, "doubly robust value");
}

#[test]
fn doubly_robust_requires_a_reward_model_on_every_record() {
    let mu = vec![
        Propensity::new(1, 2).unwrap(),
        Propensity::new(1, 2).unwrap(),
    ];
    let bare = LoggedDecision::new(0, 1.0, mu, vec![0.5, 0.5]).unwrap();
    assert!(doubly_robust(&[bare.clone()]).is_err());
    assert!(switch(&[bare], 1.0).is_err());
}

#[test]
fn switch_uses_the_reward_model_above_the_weight_threshold() {
    // weight_of = [0.5, 1.0, 2.0]; only action 2 exceeds tau = 1.5, so the
    // model term is pi(2) * q(2) = 0.5 * 3 = 1.5 on every record, plus the
    // executed weight's own contribution when that weight is <= tau:
    //   a0: 1.5 + 0.5*1 = 2.0 (x2); a1: 1.5 + 1.0*2 = 3.5 (x3);
    //   a2: 1.5 + 0 = 1.5 (x4, executed weight 2.0 > tau is dropped).
    let result = switch(&fixed_log(), 1.5).unwrap();
    let expected = (2.0 * 2.0 + 3.0 * 3.5 + 4.0 * 1.5) / 9.0;
    assert_close(result.value, expected, 1e-9, "switch value");
    assert_close(
        result.max_weight,
        1.0,
        1e-12,
        "switch only ever uses weight <= tau",
    );
    assert!(switch(&fixed_log(), -1.0).is_err());
    // tau above every weight reduces to plain IPS (nothing is switched).
    let never_switches = switch(&fixed_log(), 100.0).unwrap();
    assert_close(
        never_switches.value,
        ips(&fixed_log()).unwrap().value,
        1e-9,
        "high tau matches IPS",
    );
}

#[test]
fn effective_sample_size_matches_its_closed_form() {
    assert_close(
        effective_sample_size(&[2.0; 5]),
        5.0,
        1e-12,
        "equal weights: ESS = n",
    );
    assert_close(
        effective_sample_size(&[100.0, 0.0, 0.0]),
        1.0,
        1e-12,
        "one dominant weight: ESS = 1",
    );
    assert_eq!(effective_sample_size(&[0.0, 0.0]), 0.0);
    assert_eq!(effective_sample_size(&[]), 0.0);
}

#[test]
fn ess_gate_refuses_below_its_minimum() {
    let result = ips(&fixed_log()).unwrap();
    assert!(EssGate::new(5.0).unwrap().check(&result).is_ok());
    let refusal = EssGate::new(8.0).unwrap().check(&result);
    assert!(matches!(
        refusal,
        Err(StatError::InsufficientSamples {
            required: 8,
            actual: 7,
            ..
        })
    ));
    assert!(EssGate::new(0.0).is_err() && EssGate::new(f64::NAN).is_err());
}

#[test]
fn logged_decision_validates_propensities_and_target() {
    let mu = vec![
        Propensity::new(1, 2).unwrap(),
        Propensity::new(1, 4).unwrap(),
    ];
    // propensities sum to 3/4, not 1.
    assert!(LoggedDecision::new(0, 1.0, mu, vec![0.5, 0.5]).is_err());
    let zero_at_executed = vec![
        Propensity::new(0, 1).unwrap(),
        Propensity::new(1, 1).unwrap(),
    ];
    assert!(matches!(
        LoggedDecision::new(0, 1.0, zero_at_executed, vec![0.0, 1.0]),
        Err(StatError::ZeroLoggingPropensity { action: 0 })
    ));
    let ok_mu = vec![
        Propensity::new(1, 2).unwrap(),
        Propensity::new(1, 2).unwrap(),
    ];
    // target does not sum to 1.
    assert!(LoggedDecision::new(0, 1.0, ok_mu.clone(), vec![0.5, 0.6]).is_err());
    assert!(LoggedDecision::new(5, 1.0, ok_mu.clone(), vec![0.5, 0.5]).is_err());
    assert!(LoggedDecision::new(0, f64::NAN, ok_mu.clone(), vec![0.5, 0.5]).is_err());
    let decision = LoggedDecision::new(0, 1.0, ok_mu, vec![0.5, 0.5]).unwrap();
    assert!(decision.with_reward_model(vec![1.0]).is_err());
}

#[test]
fn support_checks_find_the_unsupported_action_and_its_mass() {
    let unsupported_mu = vec![
        Propensity::new(1, 1).unwrap(),
        Propensity::new(0, 1).unwrap(),
    ];
    // all target mass is on action 1, which the logging policy never takes.
    let unsupported = LoggedDecision::new(0, 1.0, unsupported_mu, vec![0.0, 1.0]).unwrap();
    assert_eq!(unsupported.unsupported_action(), Some(1));
    assert_close(
        unsupported.unsupported_mass(),
        1.0,
        1e-12,
        "unsupported mass",
    );

    let supported_mu = vec![
        Propensity::new(1, 2).unwrap(),
        Propensity::new(1, 2).unwrap(),
    ];
    let supported = LoggedDecision::new(0, 2.0, supported_mu, vec![0.5, 0.5]).unwrap();
    assert_eq!(supported.unsupported_action(), None);
    assert_close(
        supported.unsupported_mass(),
        0.0,
        1e-12,
        "fully supported record",
    );

    let records = vec![unsupported, supported.clone()];
    let report = support_report(&records).unwrap();
    assert_eq!((report.records, report.unsupported_records), (2, 1));
    assert_close(
        report.mean_unsupported_mass,
        0.5,
        1e-12,
        "mean unsupported mass",
    );

    assert!(matches!(
        require_support(&records),
        Err(StatError::UnsupportedAction {
            record: 0,
            action: 1
        })
    ));
    assert!(require_support(&[supported.clone()]).is_ok());
    assert!(ips(&records).is_err());
    assert!(require_support(&[]).is_err());

    // a single-record estimate reports an infinite standard error.
    let single = ips(&[supported]).unwrap();
    assert_eq!(single.n, 1);
    assert_close(single.value, 2.0, 1e-12, "single-record ips value");
    assert_eq!(single.std_error, f64::INFINITY);
}

/// Draw `n` actions from `mu`, build a fully logged decision for each with a
/// noiseless reward `base[a]` and the fixed target `pi`, so
/// `E[reward] = sum_a pi(a) base(a)` exactly.
fn planted_records(
    seed: u64,
    n: usize,
    mu: &[f64],
    pi: &[f64],
    base: &[f64],
) -> Vec<LoggedDecision> {
    let denominators = [10u64; 4];
    let logging: Vec<Propensity> = mu
        .iter()
        .zip(denominators)
        .map(|(&p, den)| Propensity::new((p * den as f64).round() as u64, den).unwrap())
        .collect();
    let mut generator = rng(seed);
    (0..n)
        .map(|_| {
            let action = categorical(&mut generator, mu);
            LoggedDecision::new(action, base[action], logging.clone(), pi.to_vec()).unwrap()
        })
        .collect()
}

#[test]
fn ips_and_doubly_robust_recover_a_planted_policy_value_over_seeded_draws() {
    let mu = [0.4, 0.3, 0.2, 0.1];
    let pi = [0.1, 0.2, 0.3, 0.4];
    let base = [1.0, 2.0, 3.0, 4.0];
    let true_value: f64 = pi.iter().zip(base).map(|(&p, b)| p * b).sum();
    assert_close(true_value, 3.0, 1e-9, "planted true value");

    let records = planted_records(42, 5000, &mu, &pi, &base);
    let ips_result = ips(&records).unwrap();
    let slack = 8.0 * ips_result.std_error;
    assert!(
        (ips_result.value - true_value).abs() <= slack,
        "ips {} vs true {true_value} +/- {slack}",
        ips_result.value
    );

    // A biased (imperfect) reward model still leaves doubly robust unbiased.
    let biased_model = vec![1.5, 2.2, 2.8, 3.5];
    let with_model: Vec<LoggedDecision> = records
        .iter()
        .cloned()
        .map(|r| r.with_reward_model(biased_model.clone()).unwrap())
        .collect();
    let dr_result = doubly_robust(&with_model).unwrap();
    let dr_slack = 8.0 * dr_result.std_error;
    assert!(
        (dr_result.value - true_value).abs() <= dr_slack,
        "doubly robust {} vs true {true_value} +/- {dr_slack}",
        dr_result.value
    );
}
