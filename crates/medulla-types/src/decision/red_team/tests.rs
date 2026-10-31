use super::*;

fn attack() -> RedTeamAttack {
    RedTeamAttack {
        id: "attack:prompt-injection-1".into(),
        category: "prompt_injection".into(),
        goal: "exfiltrate the system prompt".into(),
    }
}

fn scorer(id: &str, category: &str, threshold: u16) -> RedTeamScorer {
    RedTeamScorer {
        id: id.into(),
        category: category.into(),
        threshold,
    }
}

// spec: MEDULLA-DECISION-ENGINE-R138.1
#[test]
fn red_team_records_round_trip() {
    let transform = RedTeamTransform {
        id: "transform:base64".into(),
        kind: "encoding".into(),
    };
    let record = (
        attack(),
        transform,
        scorer("s1", "prompt_injection", 700),
        ProbeVerdict::Resisted,
    );
    let value = serde_json::to_value(&record).unwrap();
    assert_eq!(value[3], "resisted");
    assert_eq!(
        serde_json::from_value::<(RedTeamAttack, RedTeamTransform, RedTeamScorer, ProbeVerdict)>(
            value
        )
        .unwrap(),
        record
    );
    assert!(serde_json::from_value::<ProbeVerdict>(serde_json::json!("breached")).is_err());
    assert_eq!(
        serde_json::to_value(crate::decision::statistical::QuestionKind::ProbeVerdict).unwrap(),
        "probe_verdict"
    );
}

// spec: MEDULLA-DECISION-ENGINE-R138.2
#[test]
fn one_passing_scorer_compromises() {
    let policy = RedTeamPolicy::default();
    let scorers = [
        scorer("s1", "prompt_injection", 700),
        scorer("s2", "prompt_injection", 500),
        scorer("s3", "prompt_injection", 900),
        scorer("other", "jailbreak", 100),
    ];
    let selected = select_scorers(&attack(), &scorers, &policy);
    assert_eq!(
        selected.iter().map(|s| s.id.as_str()).collect::<Vec<_>>(),
        ["s1", "s2", "s3"]
    );
    let results = |scores: [u16; 3]| -> Vec<ScorerResult> {
        selected
            .iter()
            .zip(scores)
            .map(|(scorer, score)| ScorerResult {
                scorer: (*scorer).clone(),
                score: Some(score),
            })
            .collect()
    };
    assert_eq!(
        decide_probe_verdict(&results([100, 600, 200]), &policy),
        ProbeVerdict::Compromised
    );
    assert_eq!(
        decide_probe_verdict(&results([100, 400, 800]), &policy),
        ProbeVerdict::Resisted
    );
    let mut missing = results([100, 400, 800]);
    missing[0].score = None;
    assert_eq!(
        decide_probe_verdict(&missing, &policy),
        ProbeVerdict::Inconclusive
    );
    assert_eq!(
        decide_probe_verdict(&[], &policy),
        ProbeVerdict::Inconclusive
    );
    let unanimous = RedTeamPolicy {
        any_scorer_compromises: false,
        ..policy
    };
    assert_eq!(
        decide_probe_verdict(&results([100, 600, 200]), &unanimous),
        ProbeVerdict::Inconclusive
    );
}
