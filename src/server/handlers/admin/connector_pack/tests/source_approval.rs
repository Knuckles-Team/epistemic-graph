//! R722.2 served source approval and current-contract reset proof.

use std::collections::BTreeMap;
use std::sync::Arc;

use eg_types::connector_pack::{PackAnnotations, PackEntryKind, PackSourceApproval};
use eg_types::contract::Digest256;
use eg_types::foreign_read::{
    ForeignReadAbortRequest, ForeignReadAbortResult, ForeignReadChunkRequest,
    ForeignReadChunkResult, ForeignReadCompleteRequest, ForeignReadCompleteResult,
    ForeignReadLimits, ForeignReadOpenRequest, ForeignReadOpenResult, ForeignReadRow,
    FOREIGN_READ_SCHEMA_VERSION,
};

use super::{
    approve_source, bind, build_pack, context, head_of, imported, ok, refused, tool, Content,
    Served, ADMIN, TENANT,
};

fn ontology(connector: &str, body: String) -> Content {
    Content {
        kind: PackEntryKind::Ontology,
        uri: format!("ontology://{connector}/core.ttl"),
        name: "core.ttl".to_string(),
        media_type: "text/turtle",
        body: body.into_bytes(),
        input_schema: None,
        annotations: PackAnnotations::default(),
        references: Vec::new(),
    }
}

const CONTRACT: &str = r#"@prefix ac: <https://knuckles-team.github.io/agent-connector-sdk/access#> .
<urn:supplier-contract> a ac:AccessContract ; ac:classIri <urn:Supplier> ;
 ac:sourceId "suppliers" ; ac:accessKind "mcp_tool" ; ac:operation "list_suppliers" ;
 ac:entity "Supplier" ; ac:keyField "id" ."#;

#[tokio::test]
async fn approve_source_requires_current_head_and_contract_digest() {
    let connector = "r722-approval";
    let served = Served::new();
    bind(&served, connector, ADMIN).await;
    let initial = build_pack(
        connector,
        &[
            tool(connector, "a", "A."),
            ontology(connector, CONTRACT.to_string()),
        ],
    );
    let first = imported(&served, &initial, None).await;
    let connector_id = eg_types::contract::ResourceId::new(connector).unwrap();
    let store = served.state.write().await.ensure_agent_library().unwrap();
    let source = store
        .connector_pack_members(TENANT, &connector_id)
        .unwrap()
        .into_iter()
        .find(|member| !member.source_registrations.is_empty())
        .expect("the imported ontology has one source registration");
    let registration = &source.source_registrations[0];
    let contract_iri = registration.contract_iri.clone();
    let resource_uri = registration.resource_uri.clone();
    let contract_digest = source.body_sha256;
    drop(store);

    let first_head = head_of(&first);
    let wrong_key = "connector-pack:wrong-tenant:approve-source";
    let mut wrong_context = context(wrong_key);
    wrong_context.tenant_id = "tenant-other".to_string();
    let wrong_tenant = refused(
        "wrong-tenant source approval",
        served
            .call_as(
                ADMIN,
                &["*"],
                wrong_key,
                crate::protocol::Method::ConnectorPack {
                    op: Box::new(eg_types::connector_pack::ConnectorPackOp::ApproveSource {
                        request: eg_types::connector_pack::ConnectorPackSourceApprovalRequest {
                            context: wrong_context,
                            connector: connector_id.clone(),
                            contract_iri: contract_iri.clone(),
                            resource_uri: resource_uri.clone(),
                            expected_head: first_head.clone(),
                            contract_digest,
                        },
                    }),
                },
            )
            .await,
    );
    assert!(wrong_tenant.contains("ACCESS_DENIED"), "{wrong_tenant}");

    let approved: eg_types::connector_pack::ConnectorPackSourceApprovalResult = ok(
        "approve source",
        approve_source(
            &served,
            connector,
            &contract_iri,
            &resource_uri,
            first_head.clone(),
            contract_digest,
        )
        .await,
    );
    assert_eq!(approved.registration.approval, PackSourceApproval::Approved);
    assert_eq!(
        approved.schema_version,
        eg_types::connector_pack::CONNECTOR_PACK_SCHEMA_VERSION
    );
    assert_eq!(approved.tenant_id, TENANT);
    assert_eq!(approved.connector, connector_id);
    assert_eq!(approved.head, first_head);
    assert!(!approved.batch_id.is_empty());
    assert!(approved.committed_version > 0);
    let store = served.state.write().await.ensure_agent_library().unwrap();
    assert!(store
        .verify_connector_pack_source_approval(&approved)
        .is_ok());
    let mut changed_receipt = approved.clone();
    changed_receipt.contract_digest = Digest256::from_bytes([0x5a; 32]);
    let refusal = store
        .verify_connector_pack_source_approval(&changed_receipt)
        .expect_err("a receipt with a changed body digest must not bind");
    assert!(refusal.contains("SOURCE_CONTRACT_MISMATCH"), "{refusal}");
    drop(store);
    let replay: eg_types::connector_pack::ConnectorPackSourceApprovalResult = ok(
        "replay approved source",
        approve_source(
            &served,
            connector,
            &contract_iri,
            &resource_uri,
            first_head.clone(),
            contract_digest,
        )
        .await,
    );
    assert_eq!(replay, approved);

    // An unrelated member/head change keeps the same source body and contract
    // identity; the approved source must remain approved.
    let unrelated = build_pack(
        connector,
        &[
            tool(connector, "a", "A."),
            tool(connector, "b", "B."),
            ontology(connector, CONTRACT.to_string()),
        ],
    );
    let second = imported(&served, &unrelated, Some(first_head.clone())).await;
    assert_ne!(second.pack_digest, first.pack_digest);
    let store = served.state.write().await.ensure_agent_library().unwrap();
    let current = store
        .connector_pack_members(TENANT, &connector_id)
        .unwrap()
        .into_iter()
        .find(|member| !member.source_registrations.is_empty())
        .expect("unchanged source member remains current");
    assert_eq!(
        current.source_registrations[0].approval,
        PackSourceApproval::Approved
    );
    drop(store);

    // A changed source body gets a new current head and is held again.
    let changed_body = CONTRACT.replace("list_suppliers", "list_customers");
    let changed = build_pack(
        connector,
        &[
            tool(connector, "a", "A."),
            ontology(connector, changed_body),
        ],
    );
    let third = imported(&served, &changed, Some(head_of(&second))).await;
    let store = served.state.write().await.ensure_agent_library().unwrap();
    let changed_member = store
        .connector_pack_members(TENANT, &connector_id)
        .unwrap()
        .into_iter()
        .find(|member| !member.source_registrations.is_empty())
        .expect("changed source member remains current");
    assert_eq!(
        changed_member.source_registrations[0].approval,
        PackSourceApproval::Held
    );
    drop(store);

    let absent = refused(
        "absent source registration",
        approve_source(
            &served,
            connector,
            &contract_iri,
            "ontology://r722-approval/missing.ttl",
            head_of(&third),
            changed_member.body_sha256,
        )
        .await,
    );
    assert!(absent.contains("SOURCE_REGISTRATION_NOT_FOUND"), "{absent}");

    let stale = refused(
        "stale source approval",
        approve_source(
            &served,
            connector,
            &contract_iri,
            &resource_uri,
            head_of(&second),
            contract_digest,
        )
        .await,
    );
    assert!(stale.contains("PACK_HEAD_CONFLICT"), "{stale}");
    let mismatch = refused(
        "changed source digest",
        approve_source(
            &served,
            connector,
            &contract_iri,
            &resource_uri,
            head_of(&third),
            contract_digest,
        )
        .await,
    );
    assert!(mismatch.contains("SOURCE_CONTRACT_MISMATCH"), "{mismatch}");
}

/// The four host-driven lifecycle methods must be reachable through the same
/// authenticated dispatch boundary as pack admission. This proves the
/// registered, approved-source route; FleetSourceExecutor production fetching
/// remains a separate served acceptance leaf.
#[tokio::test]
async fn foreign_read_four_phase_uses_authenticated_dispatch() {
    let connector = "r722-four-phase";
    let served = Served::new();
    let _env = served.with_graph_persistence().await;
    bind(&served, connector, ADMIN).await;
    let initial = build_pack(
        connector,
        &[
            tool(connector, "a", "A."),
            ontology(connector, CONTRACT.to_string()),
        ],
    );
    let first = imported(&served, &initial, None).await;
    let connector_id = eg_types::contract::ResourceId::new(connector).unwrap();
    let store = served.state.write().await.ensure_agent_library().unwrap();
    let member = store
        .connector_pack_members(TENANT, &connector_id)
        .unwrap()
        .into_iter()
        .find(|member| !member.source_registrations.is_empty())
        .expect("source registration");
    let registration = member.source_registrations[0].clone();
    let contract_digest = member.body_sha256;
    drop(store);

    let approved: eg_types::connector_pack::ConnectorPackSourceApprovalResult = ok(
        "approve source for four-phase route",
        approve_source(
            &served,
            connector,
            &registration.contract_iri,
            &registration.resource_uri,
            head_of(&first),
            contract_digest,
        )
        .await,
    );

    let source_name = registration.source_id.clone();
    let registered: String = ok(
        "register foreign source",
        served
            .call(
                "r722-four-phase:register",
                crate::protocol::Method::RegisterForeignSource {
                    name: source_name.clone(),
                    source: eg_types::wire::ForeignSourceSpec::Mcp {
                        server: connector.to_string(),
                        tool: "list_suppliers".to_string(),
                        capabilities: Default::default(),
                    },
                },
            )
            .await,
    );
    assert_eq!(registered, source_name);

    let limits = ForeignReadLimits {
        max_rows: 10,
        max_bytes: 1024,
        chunk_rows: 2,
        chunk_bytes: 512,
        deadline_ms: 10_000,
    };
    let plan_digest = Digest256::sha256(b"r722-four-phase-plan");
    let query_digest = Digest256::sha256(b"r722-four-phase-query");
    let opened: ForeignReadOpenResult = ok(
        "authenticated foreign read open",
        served
            .call(
                "r722-four-phase:open",
                crate::protocol::Method::ForeignReadOpen {
                    request: ForeignReadOpenRequest {
                        schema_version: FOREIGN_READ_SCHEMA_VERSION,
                        source_name: source_name.clone(),
                        approval: approved.clone(),
                        plan_digest,
                        query_digest,
                        limits,
                    },
                },
            )
            .await,
    );
    assert_eq!(opened.execution.query_digest, query_digest);
    assert_eq!(opened.next_sequence, 0);
    assert_eq!(opened.source_identity.source_ref, registration.resource_uri);

    let chunked: ForeignReadChunkResult = ok(
        "authenticated foreign read chunk",
        served
            .call(
                "r722-four-phase:chunk",
                crate::protocol::Method::ForeignReadChunk {
                    request: ForeignReadChunkRequest {
                        execution: opened.execution,
                        sequence: 0,
                        source_identity: opened.source_identity.clone(),
                        rows: vec![ForeignReadRow {
                            id: "supplier-1".to_string(),
                            score: Some(0.5),
                            columns: BTreeMap::from([(
                                "name".to_string(),
                                serde_json::Value::String("Acme".to_string()),
                            )]),
                            provenance: vec!["r722-four-phase:supplier-1".to_string()],
                        }],
                    },
                },
            )
            .await,
    );
    assert_eq!(chunked.accepted_rows, 1);
    assert_eq!(chunked.next_sequence, 1);

    let completed: ForeignReadCompleteResult = ok(
        "authenticated foreign read complete",
        served
            .call(
                "r722-four-phase:complete",
                crate::protocol::Method::ForeignReadComplete {
                    request: ForeignReadCompleteRequest {
                        execution: opened.execution,
                        sequence: 1,
                    },
                },
            )
            .await,
    );
    assert_eq!(completed.total_rows, 1);

    let abort_query_digest = Digest256::sha256(b"r722-four-phase-abort-query");
    let abort_opened: ForeignReadOpenResult = ok(
        "authenticated foreign read abort open",
        served
            .call(
                "r722-four-phase:abort-open",
                crate::protocol::Method::ForeignReadOpen {
                    request: ForeignReadOpenRequest {
                        schema_version: FOREIGN_READ_SCHEMA_VERSION,
                        source_name,
                        approval: approved,
                        plan_digest,
                        query_digest: abort_query_digest,
                        limits,
                    },
                },
            )
            .await,
    );
    let aborted: ForeignReadAbortResult = ok(
        "authenticated foreign read abort",
        served
            .call(
                "r722-four-phase:abort",
                crate::protocol::Method::ForeignReadAbort {
                    request: ForeignReadAbortRequest {
                        execution: abort_opened.execution,
                        sequence: 0,
                        reason: "test cleanup".to_string(),
                    },
                },
            )
            .await,
    );
    assert_eq!(aborted.sequence, 0);
    assert_eq!(aborted.reason, "test cleanup");
}

struct BoundRows;

impl eg_plan::federation::ForeignSource for BoundRows {
    fn fetch(&self) -> Result<eg_plan::RowSet, String> {
        Ok(eg_plan::RowSet::from_ids(["bound-row".to_string()]))
    }
}

#[tokio::test]
async fn bind_for_requires_owner_catalog_and_rechecks_approval() {
    let connector = "r722-bind";
    let served = Served::new();
    bind(&served, connector, ADMIN).await;
    let initial = build_pack(
        connector,
        &[
            tool(connector, "a", "A."),
            ontology(connector, CONTRACT.to_string()),
        ],
    );
    let first = imported(&served, &initial, None).await;
    let connector_id = eg_types::contract::ResourceId::new(connector).unwrap();
    let store = served.state.write().await.ensure_agent_library().unwrap();
    let member = store
        .connector_pack_members(TENANT, &connector_id)
        .unwrap()
        .into_iter()
        .find(|member| !member.source_registrations.is_empty())
        .expect("source registration");
    let registration = member.source_registrations[0].clone();
    let digest = member.body_sha256;
    drop(store);
    let approved: eg_types::connector_pack::ConnectorPackSourceApprovalResult = ok(
        "approve source for binding",
        approve_source(
            &served,
            connector,
            &registration.contract_iri,
            &registration.resource_uri,
            head_of(&first),
            digest,
        )
        .await,
    );

    let caller = crate::server::access::CarrierAuthority::from_verified(
        &crate::server::auth::VerifiedRequestContext::verified_for_test(ADMIN),
    )
    .unwrap();
    let spec = eg_types::wire::ForeignSourceSpec::Mcp {
        server: connector.to_string(),
        tool: "list_suppliers".to_string(),
        capabilities: Default::default(),
    };
    let catalog = crate::server::foreign_catalog::ForeignSourceCatalog::default();
    catalog
        .register(&caller, registration.source_id.clone(), spec.clone())
        .unwrap();
    catalog
        .attach_pack_binding(
            &caller,
            &registration.source_id,
            &spec,
            crate::server::foreign_catalog::ForeignSourcePackBinding {
                head: approved.head.clone(),
                registration: approved.registration.clone(),
                contract_digest: approved.contract_digest,
            },
        )
        .expect("owner catalog binds the durable source tuple");
    let descriptor = crate::server::foreign_catalog::ForeignSourceBindingDescriptor {
        name: registration.source_id.clone(),
        spec,
        registration: approved.registration.clone(),
        contract_digest: approved.contract_digest,
    };
    let store = served.state.write().await.ensure_agent_library().unwrap();
    let scoped = catalog
        .bind_for(
            &caller,
            store.as_ref(),
            &crate::isolation::IsolationLayer::new(),
            &approved,
            &descriptor,
            Arc::new(BoundRows),
        )
        .expect("approved source binds only from its owner's catalog");
    assert_eq!(
        scoped.registry().resolve(&descriptor.name).unwrap().ids(),
        vec!["bound-row"]
    );
    drop(store);

    for (field, value) in [
        ("class", "urn:OtherClass"),
        ("entity", "OtherEntity"),
        ("key", "other_id"),
    ] {
        let mut mismatched_registration = approved.clone();
        match field {
            "class" => mismatched_registration.registration.class_iri = value.to_string(),
            "entity" => mismatched_registration.registration.entity = value.to_string(),
            "key" => mismatched_registration.registration.key_field = value.to_string(),
            _ => unreachable!(),
        }
        let mismatch_refusal = match catalog.bind_registered_for(
            &caller,
            served
                .state
                .write()
                .await
                .ensure_agent_library()
                .unwrap()
                .as_ref(),
            &crate::isolation::IsolationLayer::new(),
            &mismatched_registration,
            &registration.source_id,
            Arc::new(BoundRows),
        ) {
            Ok(_) => panic!("a receipt for catalog B ({field}) must not bind catalog A"),
            Err(error) => error,
        };
        assert!(
            mismatch_refusal.contains("SOURCE_BINDING_MISMATCH"),
            "{field}: {mismatch_refusal}"
        );
    }

    let mut held_receipt = approved.clone();
    held_receipt.registration.approval = PackSourceApproval::Held;
    let mut held_descriptor = descriptor.clone();
    held_descriptor.registration.approval = PackSourceApproval::Held;
    let held_refusal = match catalog.bind_for(
        &caller,
        served
            .state
            .write()
            .await
            .ensure_agent_library()
            .unwrap()
            .as_ref(),
        &crate::isolation::IsolationLayer::new(),
        &held_receipt,
        &held_descriptor,
        Arc::new(BoundRows),
    ) {
        Ok(_) => panic!("a held registration must not bind"),
        Err(error) => error,
    };
    assert!(
        held_refusal.contains("SOURCE_APPROVAL_NOT_ADMITTED"),
        "{held_refusal}"
    );

    let mut changed_body = approved.clone();
    changed_body.contract_digest = Digest256::from_bytes([0x33; 32]);
    let mut changed_descriptor = descriptor.clone();
    changed_descriptor.contract_digest = changed_body.contract_digest;
    let body_refusal = match catalog.bind_for(
        &caller,
        served
            .state
            .write()
            .await
            .ensure_agent_library()
            .unwrap()
            .as_ref(),
        &crate::isolation::IsolationLayer::new(),
        &changed_body,
        &changed_descriptor,
        Arc::new(BoundRows),
    ) {
        Ok(_) => panic!("a receipt naming a changed body must not bind"),
        Err(error) => error,
    };
    assert!(
        body_refusal.contains("SOURCE_CONTRACT_MISMATCH"),
        "{body_refusal}"
    );

    let mut changed_spec = descriptor.clone();
    changed_spec.spec = eg_types::wire::ForeignSourceSpec::Mcp {
        server: connector.to_string(),
        tool: "different_operation".to_string(),
        capabilities: Default::default(),
    };
    let spec_refusal = match catalog.bind_for(
        &caller,
        served
            .state
            .write()
            .await
            .ensure_agent_library()
            .unwrap()
            .as_ref(),
        &crate::isolation::IsolationLayer::new(),
        &approved,
        &changed_spec,
        Arc::new(BoundRows),
    ) {
        Ok(_) => panic!("a descriptor different from the owner catalog must not bind"),
        Err(error) => error,
    };
    assert!(
        spec_refusal.contains("SOURCE_BINDING_MISMATCH"),
        "{spec_refusal}"
    );

    let other = crate::server::access::CarrierAuthority::from_verified(
        &crate::server::auth::VerifiedRequestContext::verified_for_test("other-agent"),
    )
    .unwrap();
    let refused = match catalog.bind_for(
        &other,
        served
            .state
            .write()
            .await
            .ensure_agent_library()
            .unwrap()
            .as_ref(),
        &crate::isolation::IsolationLayer::new(),
        &approved,
        &descriptor,
        Arc::new(BoundRows),
    ) {
        Ok(_) => panic!("same-tenant non-owner must not use the owner's catalog entry"),
        Err(error) => error,
    };
    assert!(
        refused.contains("SOURCE_BINDING_NOT_REGISTERED"),
        "{refused}"
    );

    let wrong_tenant = crate::server::access::CarrierAuthority::from_verified(
        &crate::server::auth::VerifiedRequestContext::verified_for_test_in_tenant(
            ADMIN,
            "tenant-other",
        ),
    )
    .unwrap();
    let tenant_refusal = match catalog.bind_for(
        &wrong_tenant,
        served
            .state
            .write()
            .await
            .ensure_agent_library()
            .unwrap()
            .as_ref(),
        &crate::isolation::IsolationLayer::new(),
        &approved,
        &descriptor,
        Arc::new(BoundRows),
    ) {
        Ok(_) => panic!("a source approval from another tenant must not bind"),
        Err(error) => error,
    };
    assert!(tenant_refusal.contains("ACCESS_DENIED"), "{tenant_refusal}");

    // A new published head with the same source body/tuple is a distinct
    // catalog binding. Approve that current head so the durable verifier
    // passes; the unchanged in-memory H1 catalog must still refuse H2 before
    // installing the driver.
    let second = build_pack(
        connector,
        &[
            tool(connector, "a", "A."),
            tool(connector, "b", "B."),
            ontology(connector, CONTRACT.to_string()),
        ],
    );
    let second_head = imported(&served, &second, Some(head_of(&first))).await;
    let store = served.state.write().await.ensure_agent_library().unwrap();
    let second_member = store
        .connector_pack_members(TENANT, &connector_id)
        .unwrap()
        .into_iter()
        .find(|member| !member.source_registrations.is_empty())
        .expect("same source registration remains on H2");
    let second_digest = second_member.body_sha256;
    drop(store);
    let approved_h2: eg_types::connector_pack::ConnectorPackSourceApprovalResult = ok(
        "approve same source on H2",
        approve_source(
            &served,
            connector,
            &registration.contract_iri,
            &registration.resource_uri,
            head_of(&second_head),
            second_digest,
        )
        .await,
    );
    assert_eq!(approved_h2.contract_digest, approved.contract_digest);
    let h2_refusal = match catalog.bind_registered_for(
        &caller,
        served
            .state
            .write()
            .await
            .ensure_agent_library()
            .unwrap()
            .as_ref(),
        &crate::isolation::IsolationLayer::new(),
        &approved_h2,
        &registration.source_id,
        Arc::new(BoundRows),
    ) {
        Ok(_) => panic!("H1 catalog must not bind an approved H2 receipt"),
        Err(error) => error,
    };
    assert!(
        h2_refusal.contains("SOURCE_BINDING_MISMATCH"),
        "{h2_refusal}"
    );
}
