use medulla_types::quality::{QualityDimension, QualityRule, QualityScore, QualitySeverity};

fn rule() -> QualityRule {
    QualityRule {
        id: "orders.email.completeness".to_string(),
        target: "pg:app.public.orders.email".to_string(),
        dimension: QualityDimension::Completeness,
        expression: "count(email) / count(*)".to_string(),
        threshold: 0.98,
        severity: QualitySeverity::Critical,
    }
}

// spec: MEDULLA-UNIFIED-DATA-PLANE-R762.1
#[test]
fn rule_round_trips() {
    let original = rule();
    assert_eq!(original.validate(), Ok(()));
    let json = serde_json::to_string(&original).unwrap();
    assert_eq!(
        serde_json::from_str::<QualityRule>(&json).unwrap(),
        original
    );

    let score = original.score(0.97, 1_761_000_000_000);
    assert!(!score.passed);
    let json = serde_json::to_string(&score).unwrap();
    assert_eq!(serde_json::from_str::<QualityScore>(&json).unwrap(), score);

    let unknown = serde_json::to_string(&rule())
        .unwrap()
        .replace("completeness", "beauty");
    assert!(serde_json::from_str::<QualityRule>(&unknown).is_err());

    let mut bad = rule();
    bad.threshold = 1.5;
    assert_eq!(
        bad.validate().unwrap_err().code(),
        "QUALITY_THRESHOLD_INVALID"
    );
    let mut bad = rule();
    bad.expression = " ".to_string();
    assert_eq!(bad.validate().unwrap_err().code(), "QUALITY_RULE_INVALID");
}

// spec: MEDULLA-UNIFIED-DATA-PLANE-R762.7
#[test]
fn profile_yields_proposed_rules() {
    use medulla_types::profile::{ColumnProfile, ProfileMethod};
    use medulla_types::quality::{propose_rules, RuleState};
    let profile = ColumnProfile {
        dataset: "pg:app.public.customers".to_string(),
        column: "zip".to_string(),
        rows: 10_000,
        nulls: 0,
        distinct_estimate: 10_000,
        min: Some("01001".to_string()),
        max: Some("99950".to_string()),
        top_k: vec![],
        histogram: vec![],
        profiled_at: 0,
        method: ProfileMethod::Pushdown,
    };
    let mut proposals = propose_rules(&profile);
    let dimensions: Vec<QualityDimension> = proposals.iter().map(|p| p.rule.dimension).collect();
    assert_eq!(
        dimensions,
        [
            QualityDimension::Completeness,
            QualityDimension::Uniqueness,
            QualityDimension::Validity
        ]
    );
    for proposal in &proposals {
        assert_eq!(proposal.state, RuleState::Proposed);
        assert!(proposal.active().is_none(), "a proposal never evaluates");
        assert!(proposal.rule.validate().is_ok());
    }
    assert!(!proposals[0].approve(" "));
    assert!(proposals[0].active().is_none());
    assert!(proposals[0].approve("steward"));
    assert!(proposals[0].active().is_some());

    let sparse = ColumnProfile {
        nulls: 4_000,
        distinct_estimate: 12,
        min: Some("a".to_string()),
        ..profile
    };
    assert!(propose_rules(&sparse).is_empty());
}
