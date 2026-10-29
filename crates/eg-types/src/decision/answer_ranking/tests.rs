use super::*;

fn pair(first: &str, second: &str, preference: PairwisePreference) -> PairwiseComparison {
    PairwiseComparison {
        first: first.into(),
        second: second.into(),
        preference,
    }
}

/// Four candidates: `c` wins every comparison, `a` and `d` tie on score and
/// order by the lowest id, and `b` loses to everyone except a tie with `d`.
fn request() -> AnswerRankingRequest {
    use PairwisePreference::{First, Second, Tie};
    AnswerRankingRequest {
        candidate_ids: BoundedVec::new(vec!["d".into(), "c".into(), "b".into(), "a".into()])
            .unwrap(),
        comparisons: BoundedVec::new(vec![
            pair("a", "b", First),
            pair("a", "c", Second),
            pair("a", "d", Tie),
            pair("c", "b", First),
            pair("b", "d", Tie),
            pair("d", "c", Second),
        ])
        .unwrap(),
        verifier: None,
        retain_top_k: 2,
        max_revisions: 2,
    }
}

// spec: MEDULLA-DECISION-ENGINE-R134.104
#[test]
fn ranking_record_round_trips_and_requires_a_tie_rule() {
    let ranking = rank_answer_candidates(&request()).unwrap();
    let value = serde_json::to_value(&ranking).unwrap();
    assert_eq!(value["tie_rule"], "lowest_candidate_id");
    assert_eq!(value["verifier_present"], false);
    assert_eq!(
        serde_json::from_value::<AnswerCandidateRankingV1>(value.clone()).unwrap(),
        ranking
    );
    let mut missing = value.clone();
    missing.as_object_mut().unwrap().remove("tie_rule");
    assert!(serde_json::from_value::<AnswerCandidateRankingV1>(missing).is_err());
    let mut unknown = value;
    unknown["verifier"] = serde_json::json!("judge");
    assert!(serde_json::from_value::<AnswerCandidateRankingV1>(unknown).is_err());
}

// spec: MEDULLA-DECISION-ENGINE-R134.105
#[test]
fn four_candidates_yield_order_retention_and_revisions() {
    let ranking = rank_answer_candidates(&request()).unwrap();
    assert_eq!(
        ranking
            .global_order
            .iter()
            .map(|r| (r.candidate_id.as_str(), r.score))
            .collect::<Vec<_>>(),
        [("c", 6), ("a", 3), ("d", 2), ("b", 1)]
    );
    assert_eq!(ranking.retained_ids.as_slice(), ["c", "a"]);
    let revisions: Vec<(&str, &str, Vec<&str>)> = ranking
        .revision_actions
        .iter()
        .map(|r| {
            (
                r.candidate_id.as_str(),
                r.revise_toward.as_str(),
                r.lost_to.iter().map(String::as_str).collect(),
            )
        })
        .collect();
    assert_eq!(
        revisions,
        [("d", "c", vec!["c"]), ("b", "c", vec!["a", "c"])]
    );
    // Equal scores order by the lowest candidate id, whatever the input order.
    let mut tied = request();
    tied.comparisons = BoundedVec::new(vec![
        pair("a", "b", PairwisePreference::Tie),
        pair("a", "c", PairwisePreference::Tie),
        pair("a", "d", PairwisePreference::Tie),
        pair("b", "c", PairwisePreference::Tie),
        pair("b", "d", PairwisePreference::Tie),
        pair("c", "d", PairwisePreference::Tie),
    ])
    .unwrap();
    let tied = rank_answer_candidates(&tied).unwrap();
    assert_eq!(tied.retained_ids.as_slice(), ["a", "b"]);
    let mut reordered = request();
    reordered.comparisons =
        BoundedVec::new(request().comparisons.iter().cloned().rev().collect()).unwrap();
    assert_eq!(
        rank_answer_candidates(&reordered).unwrap().ranking_digest,
        rank_answer_candidates(&request()).unwrap().ranking_digest
    );
}

// spec: MEDULLA-DECISION-ENGINE-R134.107
#[test]
fn refusals_carry_typed_reasons() {
    let refuse = |edit: &dyn Fn(&mut AnswerRankingRequest)| {
        let mut input = request();
        edit(&mut input);
        rank_answer_candidates(&input).unwrap_err()
    };
    assert_eq!(
        refuse(&|r| r.verifier = Some("judge-model".into())),
        AnswerRankingRefusal::VerifierNamed {
            verifier: "judge-model".into()
        }
    );
    assert_eq!(
        refuse(&|r| {
            r.candidate_ids = BoundedVec::new(vec!["a".into()]).unwrap();
            r.comparisons = BoundedVec::default();
            r.retain_top_k = 1;
        }),
        AnswerRankingRefusal::TooFewCandidates { count: 1 }
    );
    assert_eq!(
        refuse(&|r| r.max_revisions = 1),
        AnswerRankingRefusal::RevisionBoundExceeded {
            needed: 2,
            bound: 1
        }
    );
    assert_eq!(
        refuse(&|r| r.retain_top_k = 0),
        AnswerRankingRefusal::InvalidRetain { retain_top_k: 0 }
    );
    assert_eq!(
        refuse(&|r| {
            let mut comparisons = r.comparisons.as_slice().to_vec();
            comparisons.pop();
            r.comparisons = BoundedVec::new(comparisons).unwrap();
        }),
        AnswerRankingRefusal::IncompleteComparisons { missing: 1 }
    );
    assert_eq!(
        refuse(&|r| {
            let mut comparisons = r.comparisons.as_slice().to_vec();
            comparisons[0] = pair("b", "c", PairwisePreference::Tie);
            r.comparisons = BoundedVec::new(comparisons).unwrap();
        }),
        AnswerRankingRefusal::DuplicateComparison {
            first: "c".into(),
            second: "b".into()
        }
    );
    assert_eq!(
        refuse(&|r| {
            let mut comparisons = r.comparisons.as_slice().to_vec();
            comparisons[0] = pair("a", "z", PairwisePreference::Tie);
            r.comparisons = BoundedVec::new(comparisons).unwrap();
        }),
        AnswerRankingRefusal::UnknownCandidate {
            candidate_id: "z".into()
        }
    );
    let reason = serde_json::to_value(refuse(&|r| r.max_revisions = 0)).unwrap();
    assert_eq!(reason["reason"], "revision_bound_exceeded");
}

// spec: MEDULLA-DECISION-ENGINE-R134.108
#[test]
fn premises_record_comparisons_order_retained_set_and_revisions() {
    let ranking = rank_answer_candidates(&request()).unwrap();
    let premises = answer_ranking_premises(&ranking);
    let claims: Vec<&PremiseRef> = premises
        .iter()
        .filter(|p| p.class == PremiseClass::Claim)
        .collect();
    assert_eq!(claims.len(), 6, "one premise per pairwise comparison");
    assert!(claims
        .iter()
        .any(|p| p.subject == "d|c" && p.fact == "answer_pairwise:preference=second"));
    let order: Vec<&PremiseRef> = premises
        .iter()
        .filter(|p| p.fact.starts_with("answer_global_order:"))
        .collect();
    assert_eq!(order.len(), 1, "one premise for the global order");
    assert!(order[0].fact.starts_with(
        "answer_global_order:order=c,a,d,b;tie_rule=lowest_candidate_id;retained=c,a;"
    ));
    assert!(order[0].fact.ends_with(&ranking.ranking_digest));
    let revisions: Vec<(&str, &str)> = premises
        .iter()
        .filter(|p| p.fact.starts_with("answer_revision:"))
        .map(|p| (p.subject.as_str(), p.fact.as_str()))
        .collect();
    assert_eq!(
        revisions,
        [
            ("d", "answer_revision:revise_toward=c;lost_to=c"),
            ("b", "answer_revision:revise_toward=c;lost_to=a,c"),
        ]
    );
}
