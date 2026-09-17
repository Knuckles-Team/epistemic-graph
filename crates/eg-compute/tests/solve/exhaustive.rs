//! Cross-check the solver against exhaustive enumeration of every selection.

use eg_compute::solve::model::ModelSpec;
use eg_compute::solve::{SolveStatus, Verdict};

use super::support::{
    exhaustive_optimum, level_key, model, random_model, solve_and_verify, wide_config,
};

/// Solve with room to finish, verify, and compare with the oracle.
fn assert_matches_oracle(spec: &ModelSpec) {
    let expected = exhaustive_optimum(spec);
    let (certificate, verdict) = solve_and_verify(&model(spec), &wide_config());
    let Some(best) = expected else {
        assert!(
            matches!(certificate.status, SolveStatus::Infeasible { .. }),
            "{spec:?}: {certificate:?}"
        );
        assert!(matches!(verdict, Verdict::ProvenInfeasible { .. }));
        return;
    };
    let incumbent = certificate
        .incumbent
        .as_ref()
        .unwrap_or_else(|| panic!("{spec:?}: {certificate:?}"));
    assert_eq!(level_key(spec, &incumbent.selected), best, "{spec:?}");
    let reported: Vec<(u32, i128)> = incumbent
        .objective
        .levels
        .iter()
        .map(|level| (level.unknown_selected, level.known.get()))
        .collect();
    assert_eq!(reported, best, "reported level values");
    assert_eq!(certificate.status, SolveStatus::Optimal, "{spec:?}");
    assert!(
        matches!(verdict, Verdict::ProvenOptimal { objective } if objective == incumbent.objective.scalar)
    );
}

#[test]
fn optimum_matches_exhaustive_search_on_random_small_models() {
    for seed in 0..400u64 {
        let variables = 2 + (seed % 11) as usize;
        assert_matches_oracle(&random_model(seed, variables, 7));
    }
}

#[test]
fn optimum_matches_exhaustive_search_at_twenty_variables() {
    for seed in 9_000..9_003u64 {
        assert_matches_oracle(&random_model(seed, 20, 12));
    }
}

#[test]
fn random_models_include_both_feasible_and_infeasible_cases() {
    let outcomes: Vec<bool> = (0..400u64)
        .map(|seed| exhaustive_optimum(&random_model(seed, 2 + (seed % 11) as usize, 7)).is_some())
        .collect();
    assert!(outcomes.iter().filter(|&&feasible| feasible).count() >= 40);
    assert!(outcomes.iter().filter(|&&feasible| !feasible).count() >= 40);
}
