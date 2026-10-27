use super::*;

#[tokio::test]
async fn typed_quantum_rank_handler_returns_a_quantum_result() {
    let response = handle(
        41,
        QuantumOp::Rank {
            candidates: vec![
                QuantumRankCandidate {
                    id: "alpha".to_owned(),
                    weight: 0.25,
                },
                QuantumRankCandidate {
                    id: "beta".to_owned(),
                    weight: 0.75,
                },
            ],
            shots: Some(128),
            seed: Some(7),
            backend_id: None,
        },
    )
    .await;

    assert_eq!(response.id, 41);
    assert_eq!(response.error, None);
    let Some(ResultPayload::Json(value)) = response.result else {
        panic!("the Medulla quantum handler must return its typed result");
    };
    let result: QuantumRankResult = serde_json::from_value(value).unwrap();
    assert_eq!(result.operation, QuantumOperationKind::Rank);
    assert!(result.run.proposal);
    assert_eq!(result.ranked_candidates.len(), 2);
    assert!(result
        .ranked_candidates
        .iter()
        .any(|candidate| candidate.id == "alpha"));
    assert!(result
        .ranked_candidates
        .iter()
        .any(|candidate| candidate.id == "beta"));
}
