//! Sensitive-data detector records (EG-TYPED-PACKS-R803.1).

use eg_types::sensitive_detect::{
    Detection, DetectorMethod, SensitiveDetector, DETECTOR_THRESHOLD_INVALID, MAX_SAMPLE_VALUES,
};

// spec: EG-TYPED-PACKS-R803.1
#[test]
fn records_round_trip() {
    for method in [
        DetectorMethod::Pattern,
        DetectorMethod::Checksum,
        DetectorMethod::Dictionary,
        DetectorMethod::Model,
    ] {
        let detector = SensitiveDetector {
            id: "detector:card".into(),
            category: "payment_card".into(),
            method,
            threshold: 0.8,
        };
        detector.validate().unwrap();
        let json: SensitiveDetector =
            serde_json::from_value(serde_json::to_value(&detector).unwrap()).unwrap();
        assert_eq!(json, detector);
        let packed: SensitiveDetector =
            rmp_serde::from_slice(&rmp_serde::to_vec_named(&detector).unwrap()).unwrap();
        assert_eq!(packed, detector);
    }
    let detection = Detection {
        asset: "column:orders.card".into(),
        detector: "detector:card".into(),
        sample_hits: 9_100,
        ratio: 0.91,
    };
    detection.validate().unwrap();
    let wire = serde_json::to_value(&detection).unwrap();
    assert_eq!(
        wire.as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect::<std::collections::BTreeSet<_>>(),
        ["asset", "detector", "ratio", "sample_hits"].into(),
        "a detection carries counts, never matched values"
    );
    assert_eq!(
        serde_json::from_value::<Detection>(wire).unwrap(),
        detection
    );
}

// spec: EG-TYPED-PACKS-R803.1
#[test]
fn invalid_records_are_refused() {
    assert!(serde_json::from_value::<SensitiveDetector>(serde_json::json!({
        "id": "d", "category": "c", "method": "regex", "threshold": 0.5
    }))
    .is_err());
    for threshold in [-0.1, 1.1, f64::NAN, f64::INFINITY] {
        let detector = SensitiveDetector {
            id: "detector:card".into(),
            category: "payment_card".into(),
            method: DetectorMethod::Pattern,
            threshold,
        };
        let error = detector.validate().unwrap_err();
        assert_eq!(error.code, DETECTOR_THRESHOLD_INVALID);
        assert_eq!(error.field, "threshold");
    }
    let overfull = Detection {
        asset: "column:orders.card".into(),
        detector: "detector:card".into(),
        sample_hits: MAX_SAMPLE_VALUES + 1,
        ratio: 1.0,
    };
    assert_eq!(overfull.validate().unwrap_err().field, "sample_hits");
    let bad_ratio = Detection {
        sample_hits: 1,
        ratio: 1.5,
        ..overfull
    };
    assert_eq!(bad_ratio.validate().unwrap_err().field, "ratio");
}
