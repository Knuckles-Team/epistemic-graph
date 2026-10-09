//! EG-DECISION-ENGINE-R056: the exact 0-1 solver and its certificate
//! verification carry the same deterministic guarantees on a
//! statistical-feature-set-shaped model (many scored candidate variables,
//! several covering/budget constraints) as on the core assembly solver's own
//! candidate/capability/model shape (`determinism.rs`, `tamper_proof.rs`,
//! `exhaustive.rs`). Neither `solve` nor `verify` is special-cased to the
//! assembly shape; this is the same `random_model` generator `exhaustive.rs`
//! already cross-checks against the oracle, sized the way a statistical
//! decision's candidate set is: many 0-1 candidates, few covering rows.

use eg_compute::solve::solve;

use super::support::{random_model, solve_and_verify, wide_config};

fn statistical_feature_set_model() -> eg_compute::solve::model::Model {
    super::support::model(&random_model(90_210, 18, 8))
}

#[test]
fn certificate_verification_holds_on_a_statistical_feature_set_shaped_model() {
    let instance = statistical_feature_set_model();
    // `solve_and_verify` panics if the verifier rejects the solver's own
    // output, so a passing test IS the certificate-verification proof.
    let _ = solve_and_verify(&instance, &wide_config());
}

#[test]
fn branching_is_deterministic_on_a_statistical_feature_set_shaped_model() {
    let instance = statistical_feature_set_model();
    let first = solve(&instance, &wide_config());
    for _ in 0..3 {
        let again = solve(&instance, &wide_config());
        assert_eq!(again, first, "repeated solves must branch identically");
        assert_eq!(again.digest(), first.digest());
    }
}
