//! Round trips for the source permission, reconciliation and container records.

use std::collections::BTreeSet;

use eg_types::source_governance::{
    AccessSnapshot, ReconcilePlan, SourceContainer, SourceEnumeration, SourceGovernanceError,
    SourceGroup,
};
use serde_json::json;

fn set(items: &[&str]) -> BTreeSet<String> {
    items.iter().map(|item| item.to_string()).collect()
}

// spec: MEDULLA-INGEST-WORKER-R902.1
#[test]
fn access_snapshot_round_trips() {
    let snapshot = AccessSnapshot {
        source: "confluence".into(),
        record_ref: "page-42".into(),
        access_public: false,
        read_principals: set(&["user:ada"]),
        read_groups: set(&["group:eng", "group:legal"]),
        synced_at: 1_700_000_000_000,
    };
    assert_eq!(snapshot.validate(), Ok(()));
    let encoded = serde_json::to_value(&snapshot).unwrap();
    assert_eq!(
        serde_json::from_value::<AccessSnapshot>(encoded).unwrap(),
        snapshot
    );

    let group = SourceGroup {
        source: "confluence".into(),
        group_id: "group:eng".into(),
        members: set(&["user:ada", "user:grace"]),
        synced_at: 1_700_000_000_000,
    };
    assert_eq!(group.validate(), Ok(()));
    let encoded = serde_json::to_value(&group).unwrap();
    assert_eq!(
        serde_json::from_value::<SourceGroup>(encoded).unwrap(),
        group
    );

    let blank = AccessSnapshot {
        record_ref: " ".into(),
        ..snapshot
    };
    assert_eq!(
        blank.validate(),
        Err(SourceGovernanceError::BlankIdentifier {
            field: "record_ref"
        })
    );
    let unknown_field = json!({
        "source": "s", "group_id": "g", "members": [], "synced_at": 1, "owner": "x"
    });
    assert!(serde_json::from_value::<SourceGroup>(unknown_field).is_err());
}

// spec: MEDULLA-INGEST-WORKER-R903.1
#[test]
fn reconcile_plan_round_trips() {
    let enumeration = SourceEnumeration {
        source: "drive".into(),
        run_id: "run-7".into(),
        ids_digest: "sha256:abc".into(),
        complete: true,
    };
    assert_eq!(enumeration.validate(), Ok(()));
    let encoded = serde_json::to_value(&enumeration).unwrap();
    assert_eq!(
        serde_json::from_value::<SourceEnumeration>(encoded).unwrap(),
        enumeration
    );

    let plan = ReconcilePlan {
        source: "drive".into(),
        missing: set(&["file-1", "file-9"]),
        grace_until: 1_700_086_400_000,
    };
    assert_eq!(plan.validate(), Ok(()));
    let encoded = serde_json::to_value(&plan).unwrap();
    assert_eq!(
        serde_json::from_value::<ReconcilePlan>(encoded).unwrap(),
        plan
    );

    let blank_run = SourceEnumeration {
        run_id: String::new(),
        ..enumeration
    };
    assert_eq!(
        blank_run.validate(),
        Err(SourceGovernanceError::BlankIdentifier { field: "run_id" })
    );
}

// spec: MEDULLA-INGEST-WORKER-R904.1
#[test]
fn container_round_trips() {
    let root = SourceContainer {
        source: "slack".into(),
        container_id: "workspace".into(),
        parent_id: None,
        title: "Workspace".into(),
    };
    let child = SourceContainer {
        source: "slack".into(),
        container_id: "channel-ops".into(),
        parent_id: Some("workspace".into()),
        title: "#ops".into(),
    };
    for container in [&root, &child] {
        assert_eq!(container.validate(), Ok(()));
        let encoded = serde_json::to_value(container).unwrap();
        assert_eq!(
            &serde_json::from_value::<SourceContainer>(encoded).unwrap(),
            container
        );
    }
    let looped = SourceContainer {
        parent_id: Some("channel-ops".into()),
        ..child
    };
    assert_eq!(
        looped.validate(),
        Err(SourceGovernanceError::SelfParent("channel-ops".into()))
    );
    let ontology = include_str!("../../eg-core/ontology/sdd-v1.ttl");
    for term in [":SourceContainer", ":containedIn", ":inContainer"] {
        assert!(ontology.contains(term), "sdd-v1.ttl declares {term}");
    }
}
