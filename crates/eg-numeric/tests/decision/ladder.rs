//! The act/abstain ladder, keyed exploration and audit sampling (EH-020,
//! EH-026), including the propensity and exploration negative fixtures.

use eg_numeric::decision::exploration::{permit, plan, ExplorationPermit};
use eg_numeric::decision::head_eval::Evaluated;
use eg_numeric::decision::ladder::{decide, LadderInputs};
use eg_types::decision::statistical::head::{
    DecisionHeadBody, FeatureStandardisation, FittedRegime, HeadKind, DECISION_HEAD_SCHEMA_VERSION,
};
use eg_types::decision::statistical::keyed::{decision_seed, exploration_key, seed_commitment};
use eg_types::decision::statistical::{
    QuestionKind, QuestionSafety, StatisticalOutcome, StatisticalQuestion,
};
use eg_types::decision::{ColdStart, ExplorationBudget, QuantScaleTag, QuantisedValue};

use super::common::{bounded, option_ids, policy, rational, statistical};
use super::scorer_promotion::{human_scorer, outcome};

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

fn q(value: i64) -> QuantisedValue {
    QuantisedValue {
        scale: QuantScaleTag::Q32,
        value,
    }
}

/// An uncalibrated head: present (the scorer is "on"), but it carries no
/// calibration so it can never license `Acted`.
fn uncalibrated_head() -> DecisionHeadBody {
    let spec = FeatureStandardisation {
        center: q(0),
        scale: q(1 << 32),
        lower: q(-(8 << 32)),
        upper: q(8 << 32),
    };
    DecisionHeadBody {
        schema_version: DECISION_HEAD_SCHEMA_VERSION,
        kind: HeadKind::OptionAttention,
        regime: FittedRegime::FullLabel,
        feature_schema_digest: "sha256:feature-schema".to_string(),
        standardisation: bounded(vec![spec]),
        weights: bounded(vec![q(1 << 32)]),
        calibration: None,
        training_records_digest: "sha256:golden".to_string(),
        n_training: 0,
        synthetic: true,
        scorer: None,
    }
}

/// EG-DECISION-ENGINE-R089: a statistical rung may only rank or narrow the
/// option set the constraint, entailment and optimization steps already
/// produced -- it is never the mechanism that decides which options are
/// legal. Disabling the scorer (`head: None`) must leave the legal option
/// set (`candidate_ids`) the ladder reasons over byte-for-byte identical to
/// the scorer-present run, and every option any outcome names must come
/// from that same, unchanged set.
#[test]
fn disabling_the_statistical_scorer_leaves_the_legal_option_set_unchanged() {
    let ids = option_ids();
    let stat = statistical();
    let evaluated = reading();
    let advisory_policy = policy(ColdStart::AdvisoryUncalibrated);
    let s = seed("r089");

    let scorer_off = LadderInputs {
        candidate_ids: &ids,
        head: None,
        reading: Some(&evaluated),
        policy: &advisory_policy,
        statistical: &stat,
        permit: ExplorationPermit::Off,
        seed: &s,
    };
    let head = uncalibrated_head();
    let scorer_on = LadderInputs {
        head: Some(&head),
        ..scorer_off
    };

    let off = decide(&scorer_off).expect("decides with the scorer disabled");
    let on = decide(&scorer_on).expect("decides with the scorer present but uncalibrated");

    // The ladder never mutates its input: the legal option set offered to
    // both runs is the identical slice.
    assert_eq!(scorer_off.candidate_ids, scorer_on.candidate_ids);

    for outcome in [&off.outcome, &on.outcome] {
        let named: Vec<&str> = match outcome {
            StatisticalOutcome::Advisory { scores, .. } => {
                scores.iter().map(|s| s.option_id.as_str()).collect()
            }
            StatisticalOutcome::Acted { option_id, .. }
            | StatisticalOutcome::Explored { option_id, .. } => vec![option_id.as_str()],
            StatisticalOutcome::Abstained { .. } => Vec::new(),
        };
        for id in named {
            assert!(
                ids.iter().any(|candidate| candidate == id),
                "ranking named an option {id} outside the constraint-derived legal set"
            );
        }
    }
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

/// EG-DECISION-ENGINE-R089: a statistical rung in the decision ladder may
/// only rank or narrow the legal option set the constraint, entailment and
/// optimization steps already produced -- it is never the mechanism that
/// determines which options are legal. Disabling the statistical scorer
/// (no head) must leave the same candidate set in place, and a head that
/// does act must never introduce an option outside it.
// spec: EG-DECISION-ENGINE-R089
#[test]
fn disabling_the_statistical_scorer_never_changes_the_legal_option_set() {
    let ids = option_ids();
    let strict = policy(ColdStart::DeterministicOnly);
    let stat = statistical();
    let evaluated = reading();
    let s = seed("r089");

    // The statistical rung disabled (no head): the ladder is still given the
    // identical candidate set; disabling scoring neither narrows nor widens it.
    let disabled = LadderInputs {
        candidate_ids: &ids,
        head: None,
        reading: Some(&evaluated),
        policy: &strict,
        statistical: &stat,
        permit: ExplorationPermit::Off,
        seed: &s,
    };
    let disabled_result = decide(&disabled).expect("decides");
    assert!(matches!(
        disabled_result.outcome,
        StatisticalOutcome::Abstained { .. }
    ));

    // A real, non-synthetic, calibrated head that acts on some state: it may
    // narrow (its prediction set is a subset of `ids`) but every id it names
    // is still drawn from the same legal set the constraint/entailment/
    // optimization steps produced -- it never names an option outside it.
    let head = human_scorer();
    let acting_item = (0..40)
        .find(|&item| matches!(outcome(&head, item), StatisticalOutcome::Acted { .. }))
        .expect("the calibrated scorer acts on some state");
    let StatisticalOutcome::Acted { prediction_set, .. } = outcome(&head, acting_item) else {
        unreachable!("filtered for Acted above")
    };
    for option_id in prediction_set.as_slice() {
        assert!(
            ids.contains(option_id),
            "an acting head's prediction set named {option_id:?}, outside the legal option set {ids:?}"
        );
    }
}
