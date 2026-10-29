use super::*;

fn rules() -> Vec<LimitRule> {
    vec![
        LimitRule {
            id: "max-position".into(),
            metric: LimitMetric::PositionSize,
            bound: 10_000,
            scope: "account:a".into(),
        },
        LimitRule {
            id: "max-drawdown".into(),
            metric: LimitMetric::Drawdown,
            bound: 500,
            scope: "account:a".into(),
        },
        LimitRule {
            id: "max-sector".into(),
            metric: LimitMetric::SectorConcentration,
            bound: 2_500,
            scope: "sector:tech".into(),
        },
    ]
}

fn observe(metric: LimitMetric, scope: &str, value: i64) -> LimitObservation {
    LimitObservation {
        metric,
        scope: scope.into(),
        value,
    }
}

// spec: MEDULLA-DECISION-ENGINE-R131.1
#[test]
fn limit_types_round_trip() {
    let rule = rules().remove(2);
    let value = serde_json::to_value(&rule).unwrap();
    assert_eq!(value["metric"], "sector_concentration");
    assert_eq!(
        serde_json::from_value::<LimitRule>(value.clone()).unwrap(),
        rule
    );
    let mut unknown = value;
    unknown["metric"] = serde_json::json!("leverage");
    assert!(serde_json::from_value::<LimitRule>(unknown).is_err());
    let breach = LimitBreach {
        rule_id: "max-position".into(),
        observed: 12_000,
        bound: 10_000,
        signal_iri: "urn:signal:order-1".into(),
    };
    let value = serde_json::to_value(&breach).unwrap();
    assert_eq!(
        serde_json::from_value::<LimitBreach>(value).unwrap(),
        breach
    );
}

// spec: MEDULLA-DECISION-ENGINE-R131.2
#[test]
fn check_returns_all_breaches() {
    let action = GuardedAction {
        signal_iri: "urn:signal:order-1".into(),
        observations: vec![
            observe(LimitMetric::SectorConcentration, "sector:tech", 3_000),
            observe(LimitMetric::PositionSize, "account:a", 12_000),
            observe(LimitMetric::Drawdown, "account:a", 100),
            observe(LimitMetric::PositionSize, "account:b", 99_000),
        ],
        requested_bounds: vec![RequestedBound {
            rule_id: "max-drawdown".into(),
            bound: 5_000,
        }],
    };
    let breaches = check_limits(&action, &rules());
    assert_eq!(
        breaches
            .iter()
            .map(|b| (b.rule_id.as_str(), b.observed, b.bound))
            .collect::<Vec<_>>(),
        [
            ("max-position", 12_000, 10_000),
            ("max-sector", 3_000, 2_500)
        ],
        "two breaches in rule order; the loosening drawdown bound is ignored"
    );
    assert!(breaches
        .iter()
        .all(|b| b.signal_iri == "urn:signal:order-1"));
    let tightened = GuardedAction {
        requested_bounds: vec![RequestedBound {
            rule_id: "max-drawdown".into(),
            bound: 50,
        }],
        ..action
    };
    let breaches = check_limits(&tightened, &rules());
    assert_eq!(breaches[1].rule_id, "max-drawdown");
    assert_eq!((breaches[1].observed, breaches[1].bound), (100, 50));
    let quiet = GuardedAction {
        signal_iri: "urn:signal:order-2".into(),
        observations: vec![observe(LimitMetric::PositionSize, "account:a", 10_000)],
        requested_bounds: vec![],
    };
    assert!(check_limits(&quiet, &rules()).is_empty());
}
