use eg_types::sensitive_detect::{Detection, SensitiveDetectionMethod, SensitiveDetector};

#[test]
fn records_round_trip() {
    let detector = SensitiveDetector {
        id: "detector:card-number".into(),
        category: "payment_card".into(),
        method: SensitiveDetectionMethod::Checksum,
        threshold: 0.75,
    };
    let detection = Detection {
        asset: "column:payments.card_number".into(),
        detector: detector.id.clone(),
        sample_hits: 3,
        ratio: 0.3,
    };

    for encoded in [
        serde_json::to_string(&detector).unwrap(),
        serde_json::to_string(&detection).unwrap(),
    ] {
        assert!(serde_json::from_str::<serde_json::Value>(&encoded).is_ok());
    }
    assert_eq!(
        serde_json::from_str::<SensitiveDetector>(&serde_json::to_string(&detector).unwrap())
            .unwrap(),
        detector
    );
    assert_eq!(
        serde_json::from_str::<Detection>(&serde_json::to_string(&detection).unwrap()).unwrap(),
        detection
    );

    let unknown_method = serde_json::to_string(&detector)
        .unwrap()
        .replace("checksum", "heuristic");
    assert!(serde_json::from_str::<SensitiveDetector>(&unknown_method).is_err());

    let unknown_field = serde_json::to_string(&detection)
        .unwrap()
        .replace('}', ",\"matched_values\":[\"must not be stored\"]}");
    assert!(serde_json::from_str::<Detection>(&unknown_field).is_err());
}
