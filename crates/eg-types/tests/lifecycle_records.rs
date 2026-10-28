//! Lifecycle record models (EG-SDLC-R002.1, R002.2, R003.1, R005.1, R006.1).

use eg_types::lifecycle::{
    record_deployment, release_range, Defect, DefectIntake, DefectStatus, Deployment,
    DeploymentInput, DeploymentOutcome, LifecycleKind, LifecycleTraceRequest, Release, Severity,
    SpecRowState, SpecState, SpecStatusAnswer, Symptom,
};

fn release(version: &str, digests: &[&str]) -> Release {
    Release {
        repo: "medulla".into(),
        version: version.into(),
        git_ref: format!("refs/tags/v{version}"),
        changeset_head: "c3".into(),
        artifact_digests: digests.iter().map(|d| d.to_string()).collect(),
        published_at: 100,
    }
}

fn input(digest: &str, outcome: DeploymentOutcome) -> DeploymentInput {
    DeploymentInput {
        environment: "prod".into(),
        workload: "engine".into(),
        image_digest: digest.into(),
        started_at: 1,
        finished_at: 2,
        outcome,
    }
}

// spec: EG-SDLC-R002.1
#[test]
fn records_round_trip() {
    let rel = release("1.0.0", &["sha256:aa"]);
    let json = serde_json::to_string(&rel).unwrap();
    assert_eq!(serde_json::from_str::<Release>(&json).unwrap(), rel);

    let dep = record_deployment(&[rel], input("sha256:aa", DeploymentOutcome::RolledBack));
    let json = serde_json::to_string(&dep).unwrap();
    assert!(json.contains("\"rolled_back\""));
    assert_eq!(serde_json::from_str::<Deployment>(&json).unwrap(), dep);

    let bad = json.replace("rolled_back", "exploded");
    assert!(serde_json::from_str::<Deployment>(&bad).is_err());
}

// spec: EG-SDLC-R002.2
#[test]
fn unknown_digest_stays_unattributed() {
    let releases = [
        release("1.0.0", &["sha256:aa"]),
        release("1.1.0", &["sha256:bb"]),
    ];
    let known = record_deployment(&releases, input("sha256:bb", DeploymentOutcome::Succeeded));
    assert_eq!(known.release_ref.as_deref(), Some("medulla@1.1.0"));
    assert_eq!(known.outcome, DeploymentOutcome::Succeeded);

    let unknown = record_deployment(&releases, input("sha256:zz", DeploymentOutcome::Succeeded));
    assert_eq!(unknown.release_ref, None);
    assert_eq!(unknown.outcome, DeploymentOutcome::Unattributed);

    let chain: Vec<String> = ["c1", "c2", "c3", "c4", "c5"].map(String::from).to_vec();
    assert_eq!(release_range(&chain, None, "c2").unwrap(), ["c1", "c2"]);
    assert_eq!(
        release_range(&chain, Some("c2"), "c5").unwrap(),
        ["c3", "c4", "c5"]
    );
    assert!(release_range(&chain, Some("c5"), "c5").unwrap().is_empty());
    assert_eq!(
        release_range(&chain, None, "nope").unwrap_err().code(),
        "RELEASE_HEAD_UNKNOWN"
    );
}

// spec: EG-SDLC-R003.1
#[test]
fn defect_round_trips() {
    let defect = Defect {
        id: "def-1".into(),
        repo: "medulla".into(),
        title: "gate failed".into(),
        intake: DefectIntake::GateFailure,
        signature_digest: "ab".repeat(32),
        severity: Severity::High,
        status: DefectStatus::New,
        symptoms: vec![Symptom {
            intake: DefectIntake::GateFailure,
            source_record_id: "gv-1".into(),
            observed_at: 7,
            text: "boom".into(),
        }],
        created_at: 5,
    };
    let json = serde_json::to_string(&defect).unwrap();
    assert!(json.contains("\"gate_failure\"") && json.contains("\"new\""));
    assert_eq!(serde_json::from_str::<Defect>(&json).unwrap(), defect);
    assert!(serde_json::from_str::<Defect>(&json.replace("\"new\"", "\"limbo\"")).is_err());
    assert!(serde_json::from_str::<Defect>(&json.replace("gate_failure", "telepathy")).is_err());
}

// spec: EG-SDLC-R005.1
#[test]
fn status_answer_round_trips() {
    let answer = SpecStatusAnswer {
        repo: "medulla".into(),
        rev: "c9".into(),
        rows: vec![SpecRowState {
            id: "EG-X-R001".into(),
            state: SpecState::Verified,
            landed_by: vec!["c1".into()],
            verified_by: vec!["t1".into()],
            withdrawn: false,
        }],
    };
    let json = serde_json::to_string(&answer).unwrap();
    assert!(json.contains("\"VERIFIED\""));
    assert_eq!(
        serde_json::from_str::<SpecStatusAnswer>(&json).unwrap(),
        answer
    );
    assert!(serde_json::from_str::<SpecStatusAnswer>(&json.replace("VERIFIED", "DONE")).is_err());
    assert!(SpecState::Specified < SpecState::Landed && SpecState::Landed < SpecState::Verified);
}

// spec: EG-SDLC-R006.1
#[test]
fn trace_request_bounds() {
    assert!(LifecycleTraceRequest::new("r".into(), 1, vec![]).is_ok());
    assert!(LifecycleTraceRequest::new("r".into(), 8, vec![LifecycleKind::Defect]).is_ok());
    for depth in [0, 9] {
        assert_eq!(
            LifecycleTraceRequest::new("r".into(), depth, vec![])
                .unwrap_err()
                .code(),
            "LIFECYCLE_TRACE_DEPTH"
        );
    }
    assert!(serde_json::from_str::<LifecycleTraceRequest>(r#"{"anchor":"r","depth":9}"#).is_err());
    let ok: LifecycleTraceRequest =
        serde_json::from_str(r#"{"anchor":"r","depth":3,"kinds":["test_case"]}"#).unwrap();
    assert_eq!(ok.kinds, [LifecycleKind::TestCase]);
    assert_eq!(
        LifecycleKind::parse("widget").unwrap_err().code(),
        "LIFECYCLE_KIND_UNKNOWN"
    );
}
