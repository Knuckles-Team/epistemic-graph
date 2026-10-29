use super::*;

fn prediction() -> PredictionDeclaration {
    PredictionDeclaration {
        round: 3,
        fixes: (0..10).map(|i| format!("t{i}")).collect(),
        regressions: vec![],
    }
}

/// `up` tasks flip fail to pass as declared, `down` flip pass to fail.
fn outcomes(up: usize, down: usize) -> Vec<RoundTaskOutcome> {
    (0..10)
        .map(|i| {
            let task_id = format!("t{i}");
            if i < up {
                RoundTaskOutcome {
                    task_id,
                    passed_before: false,
                    passed_after: true,
                }
            } else if i < up + down {
                RoundTaskOutcome {
                    task_id,
                    passed_before: true,
                    passed_after: false,
                }
            } else {
                RoundTaskOutcome {
                    task_id,
                    passed_before: false,
                    passed_after: false,
                }
            }
        })
        .collect()
}

// spec: MEDULLA-DECISION-ENGINE-R136.1
#[test]
fn round_types_round_trip() {
    let defaults = EvolutionDefaults::default();
    assert_eq!(defaults.rounds, 1);
    assert_eq!(
        (
            defaults.flip_fraction.numerator(),
            defaults.flip_fraction.denominator()
        ),
        (1, 2)
    );
    let allocation = StageAllocation {
        stage: "propose".into(),
        model_ref: "model:large@3".into(),
        impact: 12,
    };
    let budget = RunBudget {
        max_cost: 5_000,
        max_stronger_stages: 1,
    };
    let value = serde_json::to_value((&prediction(), &defaults, &allocation, &budget)).unwrap();
    assert_eq!(
        serde_json::from_value::<(
            PredictionDeclaration,
            EvolutionDefaults,
            StageAllocation,
            RunBudget
        )>(value)
        .unwrap(),
        (prediction(), defaults, allocation, budget)
    );
    assert_eq!(
        serde_json::to_value(RoundVerdict::Unverified).unwrap(),
        "unverified"
    );
    assert!(serde_json::from_value::<RoundVerdict>(serde_json::json!("refuted")).is_err());
    use crate::decision::statistical::QuestionKind;
    assert_eq!(
        serde_json::to_value([QuestionKind::RoundVerdict, QuestionKind::StageAllocation]).unwrap(),
        serde_json::json!(["round_verdict", "stage_allocation"])
    );
}

// spec: MEDULLA-DECISION-ENGINE-R136.2
#[test]
fn verdict_follows_flip_fraction() {
    let defaults = EvolutionDefaults::default();
    let verdict = |up, down| decide_round_verdict(&prediction(), &outcomes(up, down), &defaults);
    assert_eq!(verdict(6, 4), RoundVerdict::Verified);
    assert_eq!(verdict(4, 6), RoundVerdict::Falsified);
    assert_eq!(verdict(5, 5), RoundVerdict::Unverified);
    assert_eq!(verdict(2, 2), RoundVerdict::Unverified);
    // A declared regression that does regress counts as flipping as declared.
    let regressions = PredictionDeclaration {
        round: 3,
        fixes: vec![],
        regressions: vec!["t0".into()],
    };
    assert_eq!(
        decide_round_verdict(&regressions, &outcomes(0, 1), &defaults),
        RoundVerdict::Verified
    );
    let empty = PredictionDeclaration {
        round: 3,
        fixes: vec![],
        regressions: vec![],
    };
    assert_eq!(
        decide_round_verdict(&empty, &outcomes(6, 0), &defaults),
        RoundVerdict::Unverified
    );
}
