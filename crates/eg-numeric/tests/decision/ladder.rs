//! The act/abstain ladder, keyed exploration and audit sampling (EH-020,
//! EH-026), including the propensity and exploration negative fixtures.

use eg_numeric::decision::exploration::{permit, plan, ExplorationPermit};
use eg_numeric::decision::head_eval::Evaluated;
use eg_numeric::decision::ladder::{decide, LadderInputs};
use eg_types::decision::statistical::keyed::{decision_seed, exploration_key, seed_commitment};
use eg_types::decision::statistical::{
    QuestionKind, QuestionSafety, StatisticalOutcome, StatisticalQuestion,
};
use eg_types::decision::{ColdStart, ExplorationBudget};

use super::common::{bounded, option_ids, policy, rational, statistical};

fn question(safety: QuestionSafety) -> StatisticalQuestion {
    StatisticalQuestion {
        question_id: "route".to_string(),
        kind: QuestionKind::Route,
        safety,
    }
}

fn explore() -> ColdStart {
    ColdStart::Explore {
        budget: ExplorationBudget {
            fraction: rational(1, 4),
            spend_at_risk_micros: 1_000,
            questions: bounded(vec!["route".to_string()]),
        },
    }
}

fn seed(decision: &str) -> [u8; 32] {
    decision_seed(&exploration_key(b"server-secret"), "sha256:state", decision)
}

fn reading() -> Evaluated {
    Evaluated {
        standardised: vec![vec![0.0]; 3],
        logits: vec![0.1, 2.0, 0.3],
        probabilities: None,
    }
}

#[test]
fn exploration_on_a_security_question_is_refused_not_skipped() {
    let refusal =
        permit(&policy(explore()), &question(QuestionSafety::Security)).expect_err("forbidden");
    assert_eq!(refusal.code, "EXPLORATION_FORBIDDEN");
    for safety in [
        QuestionSafety::Policy,
        QuestionSafety::WriteBack,
        QuestionSafety::Irreversible,
    ] {
        assert!(permit(&policy(explore()), &question(safety)).is_err());
    }
    assert_eq!(
        permit(
            &policy(ColdStart::DeterministicOnly),
            &question(QuestionSafety::Security)
        )
        .expect("off"),
        ExplorationPermit::Off
    );
}

#[test]
fn the_recorded_propensity_is_the_executed_policys_exact_probability() {
    // f = 1/4 over 3 options: greedy (1 - f) + f/3 = 5/6, any other f/3 = 1/12.
    let mut saw_explored = false;
    let mut saw_greedy = false;
    for n in 0..64 {
        let drawn = plan(&seed(&format!("d{n}")), rational(1, 4), 3, Some(1))
            .expect("plans")
            .expect("acts");
        let (num, den) = (drawn.propensity.numerator(), drawn.propensity.denominator());
        if drawn.chosen == 1 {
            assert_eq!((num, den), (5, 6));
        } else {
            assert_eq!((num, den), (1, 12));
        }
        saw_explored |= drawn.explored;
        saw_greedy |= !drawn.explored;
    }
    assert!(
        saw_explored && saw_greedy,
        "both branches occur over 64 keyed draws"
    );
}

#[test]
fn the_seed_is_unpredictable_without_the_server_key() {
    let a = decision_seed(&exploration_key(b"key-a"), "sha256:state", "d1");
    let b = decision_seed(&exploration_key(b"key-b"), "sha256:state", "d1");
    assert_ne!(seed_commitment(&a), seed_commitment(&b));
}

#[test]
fn deterministic_only_abstains_and_advisory_is_labelled_uncalibrated() {
    let ids = option_ids();
    let stat = statistical();
    let evaluated = reading();
    let strict = policy(ColdStart::DeterministicOnly);
    let s = seed("d");
    let inputs = LadderInputs {
        candidate_ids: &ids,
        head: None,
        reading: Some(&evaluated),
        policy: &strict,
        statistical: &stat,
        permit: ExplorationPermit::Off,
        seed: &s,
    };
    assert!(matches!(
        decide(&inputs).expect("decides").outcome,
        StatisticalOutcome::Abstained { .. }
    ));
    let advisory = policy(ColdStart::AdvisoryUncalibrated);
    let result = decide(&LadderInputs {
        policy: &advisory,
        ..inputs
    })
    .expect("decides");
    let StatisticalOutcome::Advisory { calibrated, scores } = result.outcome else {
        panic!("advisory expected")
    };
    assert!(!calibrated);
    assert!(
        scores.iter().all(|s| s.probability.is_none()),
        "no probability without calibration"
    );
}

#[test]
fn an_explored_choice_carries_no_risk_claim() {
    let ids = option_ids();
    let stat = statistical();
    let evaluated = reading();
    let explorer = policy(explore());
    let fraction = rational(1, 1);
    for n in 0..8 {
        let s = seed(&format!("x{n}"));
        let inputs = LadderInputs {
            candidate_ids: &ids,
            head: None,
            reading: Some(&evaluated),
            policy: &explorer,
            statistical: &stat,
            permit: ExplorationPermit::Budget(fraction),
            seed: &s,
        };
        let result = decide(&inputs).expect("decides");
        assert!(matches!(
            result.outcome,
            StatisticalOutcome::Explored { .. }
        ));
        assert!(result.calibration.is_none() && result.audit.is_none());
    }
}
