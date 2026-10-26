//! Model media declarations cross the real signed pack-import boundary as facts.

use eg_types::agent_component::{AgentComponentFacts, ModelInputCapabilities, ModelInputMediaType};
use eg_types::connector_pack::{
    PackAnnotations, PackEntryKind, PackImportResult, PackViolationCode,
};

use super::{
    bind, build_pack, head_of, import_uploaded, imported, ok, Content, Served, ADMIN, TENANT,
};

fn capabilities(bytes: u64) -> ModelInputCapabilities {
    ModelInputCapabilities {
        media_types: vec![ModelInputMediaType::Text, ModelInputMediaType::Png],
        max_aggregate_bytes: bytes,
        max_parts: 2,
    }
}

fn model(connector: &str, vision: bool, bytes: u64) -> Content {
    Content {
        kind: PackEntryKind::ModelProfile,
        uri: format!("model-profile://{connector}/model"),
        name: "model".into(),
        media_type: "application/json",
        body: br#"{"name":"model","description":"Declared media limits."}"#.to_vec(),
        input_schema: None,
        annotations: PackAnnotations {
            model: Some(
                serde_json::from_value(serde_json::json!({
                    "provider": "fixture", "model_identity": "fixture/model",
                    "context_window_tokens": 8192, "max_output_tokens": 1024,
                    "supports_tools": false, "supports_structured_output": false,
                    "supports_vision": vision, "input_capabilities": capabilities(bytes)
                }))
                .unwrap(),
            ),
            ..Default::default()
        },
        references: Vec::new(),
    }
}

// spec: EG-DECISION-ENGINE-R050.1
#[tokio::test]
async fn declared_model_media_facts_round_trip_through_pack_import() {
    let connector = "model-media-roundtrip";
    let served = Served::new();
    bind(&served, connector, ADMIN).await;
    let first = imported(
        &served,
        &build_pack(connector, &[model(connector, true, 4096)]),
        None,
    )
    .await;
    let store = served.state.write().await.ensure_agent_library().unwrap();
    let id = format!("mcp:{connector}/model_profile/model");
    let original = store.current_component(TENANT, &id).unwrap().unwrap();
    original.validate().unwrap();
    assert!(matches!(
        &original.facts,
        AgentComponentFacts::ModelProfile { input_capabilities: Some(facts), .. }
            if *facts == capabilities(4096)
    ));
    assert!(store
        .current_component("other-tenant", &id)
        .unwrap()
        .is_none());

    // No body or model identity changes: the declaration alone revises the pin.
    let revised = imported(
        &served,
        &build_pack(connector, &[model(connector, true, 4097)]),
        Some(head_of(&first)),
    )
    .await;
    assert_ne!(revised.pack_digest, first.pack_digest);
    assert_eq!(revised.counts.revised, 1);
    assert_eq!(revised.counts.unchanged, 1, "server stays unchanged");
    let current = store.current_component(TENANT, &id).unwrap().unwrap();
    current.validate().unwrap();
    assert_eq!(current.component_id, original.component_id);
    assert_eq!(current.entry_revision, original.entry_revision + 1);
    assert_ne!(current.definition_digest, original.definition_digest);
    assert!(matches!(
        &current.facts,
        AgentComponentFacts::ModelProfile { input_capabilities: Some(facts), .. }
            if *facts == capabilities(4097)
    ));
    assert_eq!(
        store.component_revisions(TENANT, &id).unwrap(),
        vec![original, current],
        "the old digest-bound declaration remains readable"
    );
}

// spec: EG-DECISION-ENGINE-R050.1
#[tokio::test]
async fn nonvision_image_pack_refuses_without_owner_or_body_mutation() {
    for has_prior in [false, true] {
        let connector = "model-media-refusal";
        let served = Served::new();
        bind(&served, connector, ADMIN).await;
        let expected_head = if has_prior {
            Some(head_of(
                &imported(
                    &served,
                    &build_pack(connector, &[model(connector, true, 4096)]),
                    None,
                )
                .await,
            ))
        } else {
            None
        };
        let (mut index, archive) = build_pack(connector, &[model(connector, false, 4096)]);
        // Upload is a separate authorized operation, not an import side effect.
        index.archive.blob_digest = served.upload(&archive).await;
        let store = served.state.write().await.ensure_agent_library().unwrap();
        let before = store.connector_pack_refusal_snapshot(TENANT);
        let blob = served.state.read().await.blob.clone().unwrap();
        let counts = (
            blob.store.blob_count().unwrap(),
            blob.store.chunk_count().unwrap(),
        );
        let result: PackImportResult = ok(
            "non-vision media import",
            import_uploaded(&served, index, expected_head).await,
        );
        let PackImportResult::Rejected { violations, .. } = result else {
            panic!("non-vision image declaration must refuse: {result:?}");
        };
        assert!(violations.iter().any(|violation| {
            violation.code == PackViolationCode::InvalidComponent
                && violation
                    .detail
                    .contains("image input requires declared vision support")
        }));
        assert_eq!(store.connector_pack_refusal_snapshot(TENANT), before);
        assert_eq!(
            (
                blob.store.blob_count().unwrap(),
                blob.store.chunk_count().unwrap()
            ),
            counts,
            "refused declarations create no engine bodies"
        );
    }
}
