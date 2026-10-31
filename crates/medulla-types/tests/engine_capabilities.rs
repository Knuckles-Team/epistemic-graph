use medulla_types::engine_capabilities::{
    ConstraintKind, EngineCapabilities, IndexKind, WireFeature, ENGINE_PARITY_CONSUMERS,
};

// spec: MEDULLA-ENGINE-PARITY-R020.1
#[test]
fn report_round_trips() {
    let report = EngineCapabilities {
        server_version: "16.0 (medulla 0.27.0)".to_string(),
        wire_features: vec![WireFeature::Tls, WireFeature::Startup, WireFeature::Tls],
        index_kinds: vec![IndexKind::Partial, IndexKind::Secondary],
        constraint_kinds: vec![ConstraintKind::Shacl, ConstraintKind::Unique],
        compat_matrix_digest: Some("sha256:00".to_string()),
        replication: false,
        pitr: true,
    }
    .canonical();
    assert_eq!(
        report.wire_features,
        vec![WireFeature::Startup, WireFeature::Tls]
    );
    let json = serde_json::to_string(&report).unwrap();
    assert_eq!(
        serde_json::from_str::<EngineCapabilities>(&json).unwrap(),
        report
    );
    assert!(
        serde_json::from_str::<EngineCapabilities>(&json.replace("\"tls\"", "\"gssapi\"")).is_err()
    );
    assert_eq!(
        report.wire_parameters()[0],
        ("server_version", report.server_version.clone())
    );

    let plan = include_str!("../../../specs/engine-parity/plan.md");
    for (rollups, consumer) in ENGINE_PARITY_CONSUMERS {
        assert!(!consumer.is_empty(), "{rollups} names no consumer");
        let first = rollups.split('-').next().unwrap();
        assert!(
            plan.lines()
                .any(|line| line.starts_with(&format!("| {first}"))),
            "plan.md Consumers has no row for {rollups}"
        );
    }
}
