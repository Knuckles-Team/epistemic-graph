//! Bounded R073 storage acceptance through signed, served pack imports.

use std::collections::BTreeSet;

use eg_types::agent_component::AgentComponentOutboxEvent;
use eg_types::connector_pack::{PackImportResult, CONNECTOR_PACK_IMPORT_TOPIC};
use eg_types::contract::ResourceId;
use eg_types::mutation_batch::MutationOutboxRecord;

use super::{
    bind, build_pack, head_of, import, import_uploaded, imported, ok, refused, tool, Served, ADMIN,
    TENANT,
};

mod receipt;

const CONNECTOR: &str = "storage-acceptance";

async fn snapshot(served: &Served) -> serde_json::Value {
    served
        .state
        .write()
        .await
        .ensure_agent_library()
        .unwrap()
        .connector_pack_refusal_snapshot(TENANT)
}

// spec: EG-DECISION-ENGINE-R073.1
#[tokio::test]
async fn multi_component_import_emits_one_durable_event_per_component() {
    let served = Served::new();
    bind(&served, CONNECTOR, ADMIN).await;
    let pack = build_pack(
        CONNECTOR,
        &[tool(CONNECTOR, "a", "A."), tool(CONNECTOR, "b", "B.")],
    );
    let receipt = imported(&served, &pack, None).await;
    let before = snapshot(&served).await;
    let rows: Vec<MutationOutboxRecord> = serde_json::from_value(before["outbox"].clone()).unwrap();
    let rows: Vec<_> = rows
        .iter()
        .filter(|row| row.batch_id == receipt.batch_id)
        .collect();
    assert_eq!(rows.len(), 4, "server, two tools, and the pack import");
    let mut component_ids = BTreeSet::new();
    for (ordinal, row) in rows[..3].iter().enumerate() {
        assert_eq!(row.ordinal, ordinal as u32);
        assert_eq!(row.intent.topic, "eg.agent-component.revision.v1");
        let event: AgentComponentOutboxEvent =
            eg_storage::decode_ledger_record(&row.intent.payload).unwrap();
        event.validate().unwrap();
        assert_eq!(event.component.tenant_id, TENANT);
        assert_eq!(event.component.entry_revision, 1);
        assert!(component_ids.insert(event.component.component_id.clone()));
        assert_eq!(
            row.intent.key,
            format!("{TENANT}:{}:1", event.component.component_id)
        );
    }
    assert!(component_ids.contains(&format!("mcp:{CONNECTOR}/tool/a")));
    assert!(component_ids.contains(&format!("mcp:{CONNECTOR}/tool/b")));
    assert_eq!(rows[3].ordinal, 3);
    assert_eq!(rows[3].intent.topic, CONNECTOR_PACK_IMPORT_TOPIC);
    assert_eq!(rows[3].intent.headers["record_id"], receipt.record_id);
    let repeated: PackImportResult = ok(
        "unchanged import",
        import(&served, &pack, Some(head_of(&receipt))).await,
    );
    assert!(matches!(repeated, PackImportResult::Unchanged { .. }));
    assert_eq!(snapshot(&served).await, before);
}

// spec: EG-DECISION-ENGINE-R073.2
#[tokio::test]
async fn competing_imports_have_one_head_winner_without_loser_effects() {
    let served = Served::new();
    bind(&served, CONNECTOR, ADMIN).await;
    let initial = imported(
        &served,
        &build_pack(CONNECTOR, &[tool(CONNECTOR, "a", "Initial.")]),
        None,
    )
    .await;
    let left = build_pack(CONNECTOR, &[tool(CONNECTOR, "a", "Left.")]);
    let right = build_pack(CONNECTOR, &[tool(CONNECTOR, "a", "Right.")]);
    // Prepare both inputs before racing only the pack head compare-and-set.
    let mut left_index = left.0.clone();
    left_index.archive.blob_digest = served.upload(&left.1).await;
    let mut right_index = right.0.clone();
    right_index.archive.blob_digest = served.upload(&right.1).await;
    let (left_result, right_result) = tokio::join!(
        import_uploaded(&served, left_index, Some(head_of(&initial))),
        import_uploaded(&served, right_index, Some(head_of(&initial)))
    );
    assert_ne!(left_result.error.is_some(), right_result.error.is_some());
    let (winner, loser, loser_pack) = if left_result.error.is_none() {
        (left_result, right_result, &right)
    } else {
        (right_result, left_result, &left)
    };
    assert!(refused("competing head", loser).contains("PACK_HEAD_CONFLICT"));
    let winner: PackImportResult = ok("winning import", winner);
    let PackImportResult::Imported { receipt } = winner else {
        panic!("one changed pack must commit");
    };
    assert_eq!(receipt.binding_revision, initial.binding_revision + 1);
    let store = served.state.write().await.ensure_agent_library().unwrap();
    let status = store
        .connector_pack_status(TENANT, &ResourceId::new(CONNECTOR).unwrap())
        .unwrap();
    assert_eq!(status.head.unwrap().pack_digest, receipt.pack_digest);
    let before = snapshot(&served).await;
    assert_eq!(
        before["CONNECTOR_PACK_IMPORTS"].as_array().unwrap().len(),
        2
    );
    assert!(refused(
        "stale-head retry",
        import(&served, loser_pack, Some(head_of(&initial))).await
    )
    .contains("PACK_HEAD_CONFLICT"));
    assert_eq!(snapshot(&served).await, before);
}
