use eg_types::profile::{
    ColumnProfile, HistogramBucket, InvalidColumnProfile, ProfileMethod, TopValue,
};

fn profile() -> ColumnProfile {
    ColumnProfile {
        dataset: "pg:app.public.orders".to_string(),
        column: "status".to_string(),
        rows: 1_000,
        nulls: 10,
        distinct_estimate: 4,
        min: Some("cancelled".to_string()),
        max: Some("shipped".to_string()),
        top_k: vec![TopValue {
            value: "shipped".to_string(),
            count: 700,
        }],
        histogram: vec![HistogramBucket {
            lower: "a".to_string(),
            upper: "z".to_string(),
            count: 990,
        }],
        profiled_at: 1_761_000_000_000,
        method: ProfileMethod::Sample { size: 1_000 },
    }
}

// spec: EG-UNIFIED-DATA-PLANE-R761.1
#[test]
fn profile_round_trips() {
    let original = profile();
    assert_eq!(original.validate(), Ok(()));
    let json = serde_json::to_string(&original).unwrap();
    let back: ColumnProfile = serde_json::from_str(&json).unwrap();
    assert_eq!(back, original);
    assert!((back.null_ratio() - 0.01).abs() < 1e-12);

    let mut bad = profile();
    bad.nulls = 2_000;
    assert_eq!(bad.validate(), Err(InvalidColumnProfile::NullsExceedRows));
    let mut bad = profile();
    bad.distinct_estimate = 995;
    assert_eq!(
        bad.validate(),
        Err(InvalidColumnProfile::DistinctExceedsNonNull)
    );
}
