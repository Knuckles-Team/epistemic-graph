use super::*;

// spec: MEDULLA-DECISION-ENGINE-R175.1
#[test]
fn request_round_trips() {
    let request = RecommendRequestV1 {
        catalog_ref: "catalog:books@7".into(),
        query_embedding: vec![0.5, -0.25, 0.125],
        history_item_ids: vec!["item:1".into(), "item:2".into()],
        pause_steps: 8,
        top_k: 100,
        as_of: 1_700_000_000_000,
    };
    let value = serde_json::to_value(&request).unwrap();
    assert_eq!(
        serde_json::from_value::<RecommendRequestV1>(value.clone()).unwrap(),
        request
    );
    let mut pause = value.clone();
    pause["pause_steps"] = serde_json::json!(9);
    assert!(serde_json::from_value::<RecommendRequestV1>(pause).is_err());
    for bad in [0, 101] {
        let mut k = value.clone();
        k["top_k"] = serde_json::json!(bad);
        assert!(serde_json::from_value::<RecommendRequestV1>(k).is_err());
    }
    let answer = RecommendAnswerV1 {
        catalog_ref: request.catalog_ref.clone(),
        as_of: request.as_of,
        items: vec![RecommendedItem {
            item_id: "item:9".into(),
            semantic_id: SemanticId {
                time_bucket: 12,
                codes: [1, 2, 3, 255],
            },
            score_q32: 1 << 31,
        }],
    };
    let value = serde_json::to_value(&answer).unwrap();
    assert_eq!(
        value["items"][0]["semantic_id"]["codes"],
        serde_json::json!([1, 2, 3, 255])
    );
    assert_eq!(
        serde_json::from_value::<RecommendAnswerV1>(value).unwrap(),
        answer
    );
    assert_eq!(
        serde_json::to_value(crate::decision::statistical::QuestionKind::ItemRecommendation)
            .unwrap(),
        "item_recommendation"
    );
}
