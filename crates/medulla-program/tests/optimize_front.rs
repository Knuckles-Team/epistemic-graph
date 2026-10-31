use medulla_program::{ParetoFront, PolicyCandidate};

fn candidate(id: &str, scores: [f32; 2], cost_micro_usd: u64) -> PolicyCandidate<2> {
    PolicyCandidate {
        id: id.to_owned(),
        parent: None,
        genes_digest: "a".repeat(64),
        scores,
        cost_micro_usd,
    }
}

#[test]
// spec: MEDULLA-DECISION-ENGINE-R800.1
fn dominated_candidate_is_dropped() {
    let mut front = ParetoFront::<2>::default();
    assert!(front
        .insert(candidate("efficient", [0.9, 0.8], 100))
        .unwrap());
    assert!(!front
        .insert(candidate("dominated", [0.8, 0.7], 200))
        .unwrap());
    assert_eq!(front.candidates().len(), 1);
    assert_eq!(front.candidates()[0].id, "efficient");
}

#[test]
// spec: MEDULLA-DECISION-ENGINE-R800.1
fn scores_outside_unit_interval_are_refused() {
    let mut front = ParetoFront::<2>::default();
    let result = front.insert(candidate("invalid", [1.01, 0.5], 100));
    assert_eq!(result, Err(medulla_program::FrontError::ScoreOutOfBounds));
    assert!(front.candidates().is_empty());
}

#[test]
// spec: MEDULLA-DECISION-ENGINE-R800.1
fn deserialization_rejects_invalid_candidates_and_dominated_fronts() {
    let invalid_candidate = r#"{
        "id":"invalid","parent":null,"genes_digest":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "scores":[1.01,0.5],"cost_micro_usd":100
    }"#;
    assert!(serde_json::from_str::<PolicyCandidate<2>>(invalid_candidate).is_err());

    let dominated_front = r#"{"candidates":[
        {"id":"efficient","parent":null,"genes_digest":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","scores":[0.9,0.8],"cost_micro_usd":100},
        {"id":"dominated","parent":null,"genes_digest":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","scores":[0.8,0.7],"cost_micro_usd":200}
    ]}"#;
    assert!(serde_json::from_str::<ParetoFront<2>>(dominated_front).is_err());
}
