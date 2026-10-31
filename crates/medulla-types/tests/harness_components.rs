//! Versioned harness components as graph facts (MEDULLA-ORCH-VOCAB-R801): the
//! serde shape of one version, the supersession behavior, the typed
//! refusals and the version-history evidence.

use medulla_types::orchestration::{
    current_harness_component, register_version, version_history, HarnessComponentKind,
    HarnessComponentRefusal, HarnessComponentVersion,
};

/// One valid version of the given component, with the content reference and
/// its digest kept in agreement and an evidence run on file.
fn version(
    component_id: &str,
    kind: HarnessComponentKind,
    content_ref: &str,
    valid_from: u64,
) -> HarnessComponentVersion {
    HarnessComponentVersion {
        component_id: component_id.to_string(),
        kind,
        content_ref: content_ref.to_string(),
        content_digest: HarnessComponentVersion::content_digest(content_ref),
        valid_from,
        valid_to: None,
        confidence: 0.8,
        evidence_run_id: format!("run-{component_id}-{valid_from}"),
        supersedes_id: None,
        applied_to_id: Some("agent:demo".to_string()),
    }
}

// spec: MEDULLA-ORCH-VOCAB-R801.1
#[test]
fn component_version_round_trips() {
    let version = version(
        "comp:prompt",
        HarnessComponentKind::SystemPrompt,
        "blob:abc",
        10,
    );
    let json = serde_json::to_string(&version).unwrap();
    let back: HarnessComponentVersion = serde_json::from_str(&json).unwrap();
    assert_eq!(back, version);

    // Every field survives the trip by name.
    let value: serde_json::Value = serde_json::from_str(&json).unwrap();
    for field in [
        "component_id",
        "kind",
        "content_ref",
        "content_digest",
        "valid_from",
        "confidence",
        "evidence_run_id",
        "applied_to_id",
    ] {
        assert!(value.get(field).is_some(), "field {field} missing");
    }
    assert_eq!(value["kind"], "system_prompt");

    // The kind is the closed set: a name outside it does not decode.
    let unknown_kind = json.replace("\"system_prompt\"", "\"widget\"");
    assert!(serde_json::from_str::<HarnessComponentVersion>(&unknown_kind).is_err());

    // The shape is strict: an unknown field is refused, not ignored.
    let unknown_field = json.replace('}', ",\"unknown_field\":1}");
    assert!(serde_json::from_str::<HarnessComponentVersion>(&unknown_field).is_err());
}

// spec: MEDULLA-ORCH-VOCAB-R801.2
#[test]
fn newer_version_supersedes_and_old_stays_readable() {
    let mut versions = Vec::new();
    let mut supersessions = Vec::new();

    let v1 = version(
        "comp:prompt",
        HarnessComponentKind::SystemPrompt,
        "blob:v1",
        10,
    );
    register_version(&mut versions, &mut supersessions, v1.clone(), 10).unwrap();
    let v2 = version(
        "comp:prompt",
        HarnessComponentKind::SystemPrompt,
        "blob:v2",
        20,
    );
    register_version(&mut versions, &mut supersessions, v2.clone(), 20).unwrap();

    // The new version carries the edge to the current one.
    assert_eq!(versions[1].supersedes_id, Some(v1.node_id()));
    assert_eq!(supersessions.len(), 1);
    assert_eq!(supersessions[0].new_version, v2.node_id());
    assert_eq!(supersessions[0].superseded_version, v1.node_id());
    assert_eq!(supersessions[0].valid_at, 20);

    // The older version's window closes, but the version itself stays
    // readable in the store.
    assert_eq!(versions[0].valid_to, Some(20));
    assert!(versions
        .iter()
        .any(|v| v.node_id() == v1.node_id() && v.evidence_run_id == v1.evidence_run_id));

    // The current-version read walks the validity windows: newest at each
    // time, nothing before the component existed.
    assert!(current_harness_component("comp:prompt", &versions, 5).is_none());
    assert_eq!(
        current_harness_component("comp:prompt", &versions, 10),
        Some(&versions[0])
    );
    assert_eq!(
        current_harness_component("comp:prompt", &versions, 15),
        Some(&versions[0])
    );
    assert_eq!(
        current_harness_component("comp:prompt", &versions, 20),
        Some(&versions[1])
    );
    assert_eq!(
        current_harness_component("comp:prompt", &versions, 99),
        Some(&versions[1])
    );

    // Replaying the same write (same content and metadata) is an idempotent
    // no-op: no second version, no second edge, no closed window.
    let v2_again = version(
        "comp:prompt",
        HarnessComponentKind::SystemPrompt,
        "blob:v2",
        20,
    );
    register_version(&mut versions, &mut supersessions, v2_again, 20).unwrap();
    assert_eq!(versions.len(), 2);
    assert_eq!(supersessions.len(), 1);
    assert!(versions
        .iter()
        .all(|v| v.node_id() != v2.node_id() || v.valid_to.is_none()));
}

// spec: MEDULLA-ORCH-VOCAB-R801.4
#[test]
fn evidence_kind_confidence_and_immutability_refusals() {
    let mut versions = Vec::new();
    let mut supersessions = Vec::new();

    // A version without an evidence run refuses.
    let mut no_evidence = version(
        "comp:prompt",
        HarnessComponentKind::SystemPrompt,
        "blob:v1",
        10,
    );
    no_evidence.evidence_run_id = String::new();
    assert_eq!(
        register_version(&mut versions, &mut supersessions, no_evidence, 10),
        Err(HarnessComponentRefusal::VersionWithoutEvidence)
    );
    assert!(versions.is_empty());

    // An unknown kind refuses by name (the closed set is the model).
    let unknown_kind = HarnessComponentRefusal::KindUnknown;
    assert_eq!(HarnessComponentKind::from_wire("widget"), Err(unknown_kind));

    // A confidence outside 0 to 1 refuses.
    let mut low_confidence = version(
        "comp:prompt",
        HarnessComponentKind::SystemPrompt,
        "blob:v1",
        10,
    );
    low_confidence.confidence = 1.01;
    assert_eq!(
        register_version(&mut versions, &mut supersessions, low_confidence, 10),
        Err(HarnessComponentRefusal::ConfidenceInvalid)
    );
    let mut high_confidence = version(
        "comp:prompt",
        HarnessComponentKind::SystemPrompt,
        "blob:v1",
        10,
    );
    high_confidence.confidence = -0.1;
    assert_eq!(
        register_version(&mut versions, &mut supersessions, high_confidence, 10),
        Err(HarnessComponentRefusal::ConfidenceInvalid)
    );

    // A published version never mutates: same identity, other metadata
    // refuses; the byte-identical write is a no-op.
    let base = version(
        "comp:prompt",
        HarnessComponentKind::SystemPrompt,
        "blob:v1",
        10,
    );
    register_version(&mut versions, &mut supersessions, base.clone(), 10).unwrap();
    let mut mutated = base.clone();
    mutated.confidence = 0.9;
    assert_eq!(
        register_version(&mut versions, &mut supersessions, mutated, 11),
        Err(HarnessComponentRefusal::VersionImmutable)
    );
    register_version(&mut versions, &mut supersessions, base.clone(), 12).unwrap();
    assert_eq!(versions.len(), 1);
    assert_eq!(supersessions.len(), 0);
}

// spec: MEDULLA-ORCH-VOCAB-R801.5
#[test]
fn version_history_cites_evidence_and_digests() {
    let mut versions = Vec::new();
    let mut supersessions = Vec::new();

    let v1 = version("comp:memory", HarnessComponentKind::Memory, "blob:m1", 10);
    let mut v2 = version("comp:memory", HarnessComponentKind::Memory, "blob:m2", 20);
    v2.confidence = 0.95;
    let mut v3 = version("comp:memory", HarnessComponentKind::Memory, "blob:m3", 30);
    v3.confidence = 0.99;
    register_version(&mut versions, &mut supersessions, v1.clone(), 10).unwrap();
    register_version(&mut versions, &mut supersessions, v2.clone(), 20).unwrap();
    register_version(&mut versions, &mut supersessions, v3.clone(), 30).unwrap();

    // The full chain, newest first, one entry per version.
    let history = version_history("comp:memory", &versions, &supersessions);
    assert_eq!(history.len(), 3);
    assert_eq!(history[0].version.node_id(), v3.node_id());
    assert_eq!(history[1].version.node_id(), v2.node_id());
    assert_eq!(history[2].version.node_id(), v1.node_id());

    // Every node carries its evidence run id, its confidence and its valid
    // interval, and the digest recomputes from the content reference. The
    // stored records carry the engine-filled supersedes pointers and the
    // engine-closed validity windows.
    let mut e1 = v1.clone();
    let mut e2 = v2.clone();
    let mut e3 = v3.clone();
    e1.valid_to = Some(20);
    e2.valid_to = Some(30);
    e2.supersedes_id = Some(e1.node_id());
    e3.supersedes_id = Some(e2.node_id());
    for (entry, expected) in history.iter().zip([e3, e2, e1]) {
        assert_eq!(entry.version.evidence_run_id, expected.evidence_run_id);
        assert_eq!(entry.version.confidence, expected.confidence);
        assert_eq!(entry.version.valid_from, expected.valid_from);
        assert_eq!(entry.version.valid_to, expected.valid_to);
        assert_eq!(entry.content_digest_recomputed, expected.content_digest);
        assert_eq!(entry.supersedes_id, expected.supersedes_id);
    }

    // The chain walks the supersedes pointers, not the insert order.
    assert_eq!(history[0].supersedes_id, Some(v2.node_id()));
    assert_eq!(history[1].supersedes_id, Some(v1.node_id()));
    assert_eq!(history[2].supersedes_id, None);

    // A component with no versions has no history.
    assert!(version_history("comp:empty", &versions, &supersessions).is_empty());
}
