use eg_program::{ParetoFront, PolicyCandidate};

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
// spec: EG-DECISION-ENGINE-R800.1
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
// spec: EG-DECISION-ENGINE-R800.1
fn scores_outside_unit_interval_are_refused() {
    let mut front = ParetoFront::<2>::default();
    let result = front.insert(candidate("invalid", [1.01, 0.5], 100));
    assert_eq!(result, Err(eg_program::FrontError::ScoreOutOfBounds));
    assert!(front.candidates().is_empty());
}
