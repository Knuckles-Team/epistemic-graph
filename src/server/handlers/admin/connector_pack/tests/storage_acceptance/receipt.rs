//! R073.4 reads receipts produced by the signed served import, not synthetic receipts.

use eg_types::agent_library::AgentLibraryMutationContext;
use eg_types::connector_pack::{
    PackImportRecord, PackImportResult, CONNECTOR_PACK_IMPORT_TOPIC,
    CONNECTOR_PACK_RESULT_SCHEMA_ID,
};
use eg_types::contract::{Digest256, Nonce};
use eg_types::mutation::{MutationReceipt, MutationResult};
use eg_types::mutation_batch::{MutationBatchRecord, MutationBatchStatus, MutationOutboxIntent};

use crate::server::persistence::agent_library::AgentLibraryStore;
use crate::server::persistence::connector_pack::admitted::{PackWriteEffects, PackWriteIdentity};

use super::{
    bind, build_pack, head_of, imported, snapshot, tool, Served, ADMIN, CONNECTOR, TENANT,
};

fn evidence(store: &AgentLibraryStore, batch_id: &str) -> (MutationBatchRecord, MutationReceipt) {
    let read = store.connector_pack_test_read(TENANT).unwrap();
    let record = eg_transaction::read_ledger(&read, batch_id)
        .unwrap()
        .unwrap();
    assert_eq!(record.status, MutationBatchStatus::Committed);
    let envelope = record.batch.envelope.operation().unwrap();
    let key = envelope.authority.idempotency_key.as_ref().unwrap();
    let replay = eg_transaction::read_replay_operation(&read, key.as_str())
        .unwrap()
        .unwrap();
    assert_eq!(replay.batch_id, batch_id);
    assert_eq!(replay.identity, record.identity);
    let eg_storage::RecordedOperation::Receipt(receipt) = replay.recorded else {
        panic!("import must commit a typed receipt");
    };
    receipt.validate().unwrap();
    assert_eq!(receipt.disposition.as_str(), "committed");
    assert_eq!(
        receipt.operation_replay_digest,
        replay.operation_replay_digest
    );
    assert_eq!(receipt.envelope_digest, envelope.authority.context_digest);
    let result: MutationResult =
        eg_storage::decode_ledger_record(record.result_msgpack.as_ref().unwrap()).unwrap();
    assert_eq!(receipt.result, result);
    let rows = eg_transaction::read_outbox(&read, batch_id).unwrap();
    assert_eq!(rows.len(), record.batch.outbox.len());
    for (ordinal, (row, intent)) in rows.iter().zip(&record.batch.outbox).enumerate() {
        assert_eq!(row.ordinal, ordinal as u32);
        assert_eq!(&row.intent, intent);
        assert_eq!(row.identity, record.identity);
        assert_eq!(row.committed_version, record.committed_version);
    }
    (record, *receipt)
}

// Independent wire-contract oracle: never calls owner_batch_receipt or its digest helper.
fn effects(intents: &[MutationOutboxIntent]) -> Digest256 {
    let mut digest = Digest256::framed(b"eg/agent-library-effects/v1", &[]).unwrap();
    for (ordinal, intent) in intents.iter().enumerate() {
        let mut headers = Digest256::framed(b"eg/agent-library-effect-headers/v1", &[]).unwrap();
        for (key, value) in &intent.headers {
            headers = Digest256::framed(
                b"eg/agent-library-effect-header/v1",
                &[headers.as_bytes(), key.as_bytes(), value.as_bytes()],
            )
            .unwrap();
        }
        let item = Digest256::framed(
            b"eg/agent-library-effect/v1",
            &[
                intent.topic.as_bytes(),
                intent.key.as_bytes(),
                &intent.payload,
                headers.as_bytes(),
            ],
        )
        .unwrap();
        digest = Digest256::framed(
            b"eg/agent-library-effect-item/v1",
            &[
                digest.as_bytes(),
                &(ordinal as u64).to_be_bytes(),
                item.as_bytes(),
            ],
        )
        .unwrap();
    }
    digest
}

fn assert_all_fields_bound(intents: &[MutationOutboxIntent], expected: Digest256) {
    assert_eq!(
        effects(intents),
        expected,
        "durable receipt binds the complete batch"
    );
    for ordinal in 0..intents.len() {
        let check = |changed: Vec<MutationOutboxIntent>, field: &str| {
            assert_ne!(effects(&changed), expected, "intent {ordinal}: {field}");
        };
        let mut changed = intents.to_vec();
        changed.remove(ordinal);
        check(changed, "omission");
        if ordinal + 1 < intents.len() {
            let mut changed = intents.to_vec();
            changed.swap(ordinal, ordinal + 1);
            check(changed, "order");
        }
        for field in ["topic", "key", "payload", "headers"] {
            let mut changed = intents.to_vec();
            let intent = &mut changed[ordinal];
            match field {
                "topic" => intent.topic.push('!'),
                "key" => intent.key.push('!'),
                "payload" => intent.payload[0] ^= 1,
                "headers" => {
                    intent.headers.insert("extra".into(), "header".into());
                }
                _ => unreachable!(),
            }
            check(changed, field);
        }
        assert!(!intents[ordinal].headers.is_empty());
        for header in intents[ordinal].headers.keys() {
            let mut changed = intents.to_vec();
            changed[ordinal].headers.get_mut(header).unwrap().push('!');
            check(changed, "header value");
            let mut changed = intents.to_vec();
            let value = changed[ordinal].headers.remove(header).unwrap();
            check(changed.clone(), "header omission");
            changed[ordinal].headers.insert(format!("{header}!"), value);
            check(changed, "header name");
        }
    }
}

// spec: EG-DECISION-ENGINE-R073.4
#[tokio::test]
async fn committed_multi_intent_receipt_binds_all_effects_and_survives_replay() {
    let served = Served::new();
    bind(&served, CONNECTOR, ADMIN).await;
    // A prior head also lets the handler exercise its lost-response shortcut.
    let initial = imported(
        &served,
        &build_pack(CONNECTOR, &[tool(CONNECTOR, "a", "Initial.")]),
        None,
    )
    .await;
    let mut server = super::super::server(CONNECTOR);
    server.body =
        format!(r#"{{"contract_version":"1","instructions":"Changed.","name":"{CONNECTOR}"}}"#)
            .into_bytes();
    let pack = super::super::build_pack_with_server(
        CONNECTOR,
        &server,
        &[
            tool(CONNECTOR, "a", "Changed."),
            tool(CONNECTOR, "b", "New."),
        ],
    );
    let imported = imported(&served, &pack, Some(head_of(&initial))).await;
    let store = served.state.write().await.ensure_agent_library().unwrap();
    let (record, receipt) = evidence(&store, &imported.batch_id);
    let intents = &record.batch.outbox;
    assert_eq!(intents.len(), 4, "server, two tools, and pack");
    let mut components = std::collections::BTreeSet::new();
    for intent in &intents[..3] {
        assert_eq!(intent.topic, "eg.agent-component.revision.v1");
        let event: eg_types::agent_component::AgentComponentOutboxEvent =
            eg_storage::decode_ledger_record(&intent.payload).unwrap();
        event.validate().unwrap();
        assert!(components.insert(event.component.component_id));
    }
    assert!(components.contains(&format!("mcp:{CONNECTOR}/tool/a")));
    assert!(components.contains(&format!("mcp:{CONNECTOR}/tool/b")));
    assert_eq!(intents[3].topic, CONNECTOR_PACK_IMPORT_TOPIC);
    let pack_record: PackImportRecord =
        eg_storage::decode_ledger_record(&intents[3].payload).unwrap();
    assert!(components.contains(&pack_record.server.component.component_id));
    assert_eq!(pack_record.record_id, imported.record_id);
    assert_eq!(pack_record.pack_digest, imported.pack_digest);
    assert_all_fields_bound(intents, receipt.effect_digest.unwrap());
    let MutationResult::DomainResult {
        schema_id, payload, ..
    } = &receipt.result
    else {
        panic!("typed receipt must carry the import result");
    };
    assert_eq!(schema_id.as_str(), CONNECTOR_PACK_RESULT_SCHEMA_ID);
    let original: PackImportResult = eg_storage::decode_ledger_record(payload.as_slice()).unwrap();
    assert_eq!(
        original,
        PackImportResult::Imported {
            receipt: Box::new(imported.clone())
        }
    );

    // Rebuild attempt context from durable provenance; keep operation identity, change nonce.
    let envelope = record.batch.envelope.operation().unwrap();
    let authority = &envelope.authority;
    let event: eg_types::agent_component::AgentComponentOutboxEvent =
        eg_storage::decode_ledger_record(&intents[0].payload).unwrap();
    let context = AgentLibraryMutationContext {
        request_id: 9001,
        principal: store.owner_principal().into(),
        caller_principal: authority.actor.as_str().into(),
        attempt_nonce: Nonce::minted(),
        tenant_id: TENANT.into(),
        actor_scope: event.action_actor_scope,
        purpose_id: "connector-pack:import".into(),
        policy_revision: authority.policy_revision.as_str().into(),
        policy_digest: format!("sha256:{}", authority.policy_digest.to_hex()),
        policy_decision_id: authority.policy_decision_id.as_str().into(),
        idempotency_key: authority.idempotency_key.as_ref().unwrap().as_str().into(),
        expected_revision: None,
        trace_id: None,
        created_at_ms: record.committed_at_ms + 1,
    };
    assert_ne!(context.attempt_nonce, authority.nonce);
    let before = snapshot(&served).await;
    let pack_digest = pack_record.pack_digest.to_hex();
    let replay = || {
        store.commit_pack_write::<PackImportResult>(
            &context,
            PackWriteIdentity {
                purpose: "connector-pack:import",
                kind: "connector-pack-import",
                slug: "connector-pack",
                subject: CONNECTOR,
                revision: initial.binding_revision,
                discriminator: Some(&pack_digest),
                result_schema: CONNECTOR_PACK_RESULT_SCHEMA_ID,
                noun: "connector pack",
            },
            PackWriteEffects {
                operations: record.batch.operations.clone(),
                outbox: intents.clone(),
            },
            |_, _| panic!("replay must not apply owner rows"),
        )
    };
    assert_eq!(replay().unwrap(), original);
    let (replayed_batch, replayed_receipt) = evidence(&store, &imported.batch_id);
    assert_eq!(replayed_receipt, receipt);
    assert_eq!(
        serde_json::to_value(&replayed_batch).unwrap(),
        serde_json::to_value(&record).unwrap()
    );
    assert!(replay().unwrap_err().contains("REPLAY_NONCE_CONSUMED"));
    assert_eq!(snapshot(&served).await, before);

    // This served replay is a catalog shortcut, distinct from typed admission above.
    let repeated = super::imported(&served, &pack, Some(head_of(&initial))).await;
    assert_eq!(repeated, imported);
    assert_eq!(evidence(&store, &imported.batch_id).1, receipt);
    assert_eq!(snapshot(&served).await, before);
}
