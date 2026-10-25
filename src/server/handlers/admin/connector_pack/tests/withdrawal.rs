//! Signed current-discovery and historical-content acceptance.

use super::{
    bind, build_pack, context, head_of, import_uploaded_with, imported, ok, persistence_id, tool,
    Served, ADMIN, TENANT,
};
use crate::protocol::Method;
use eg_types::agent_component::{
    AgentComponentContentRequest, AgentComponentContentResult, AgentComponentKind,
    AgentComponentOp, AgentComponentPublishRequest, AgentComponentSearchPage,
    AgentComponentSearchRequest, ComponentDependency,
};
use eg_types::agent_library::AgentLibraryLifecycle;
use eg_types::connector_pack::{PackImportResult, PackViolationCode};

async fn search(served: &Served) -> Vec<String> {
    let page: AgentComponentSearchPage = ok(
        "Search",
        served
            .call(
                "withdrawal-search",
                Method::AgentComponent {
                    op: AgentComponentOp::Search {
                        request: AgentComponentSearchRequest {
                            tenant_id: TENANT.to_string(),
                            task: None,
                            capabilities: Vec::new(),
                            kinds: vec![AgentComponentKind::Tool],
                            read_only: false,
                            limit: None,
                            cursor: None,
                        },
                    },
                },
            )
            .await,
    );
    assert!(page.next_cursor.is_none());
    page.entries
        .into_iter()
        .map(|entry| entry.component_id)
        .collect()
}

// spec: EG-DECISION-ENGINE-R073.5
#[tokio::test]
async fn withdrawn_tool_leaves_search_but_keeps_historical_content() {
    let served = Served::new();
    let connector = "withdrawal-acceptance";
    bind(&served, connector, ADMIN).await;
    let kept = tool(connector, "kept", "Published positive control.");
    let withdrawn = tool(connector, "withdrawn", "Retained historical body.");
    let first = imported(
        &served,
        &build_pack(connector, &[kept.clone(), withdrawn.clone()]),
        None,
    )
    .await;
    let kept_id = format!("mcp:{connector}/tool/kept");
    let withdrawn_id = format!("mcp:{connector}/tool/withdrawn");
    assert_eq!(
        search(&served).await,
        vec![kept_id.clone(), withdrawn_id.clone()]
    );
    let pinned = served
        .state
        .write()
        .await
        .ensure_agent_library()
        .unwrap()
        .current_component(TENANT, &withdrawn_id)
        .unwrap()
        .unwrap();
    let second = imported(
        &served,
        &build_pack(connector, &[kept]),
        Some(head_of(&first)),
    )
    .await;
    assert_eq!(second.counts.withdrawn, 1);
    assert_eq!(search(&served).await, vec![kept_id]);
    let store = served.state.write().await.ensure_agent_library().unwrap();
    assert_eq!(
        store
            .current_component(TENANT, &withdrawn_id)
            .unwrap()
            .unwrap()
            .lifecycle,
        AgentLibraryLifecycle::Withdrawn
    );
    let content: AgentComponentContentResult = ok(
        "Content",
        served
            .call(
                "withdrawal-history",
                Method::AgentComponent {
                    op: AgentComponentOp::Content {
                        request: AgentComponentContentRequest {
                            tenant_id: TENANT.to_string(),
                            component_id: withdrawn_id.clone(),
                            entry_revision: Some(pinned.entry_revision),
                        },
                    },
                },
            )
            .await,
    );
    assert_eq!(content.body, withdrawn.body);

    let mut draft = crate::server::persistence::agent_component::test_component_draft(
        TENANT,
        "skill:new-withdrawn-pin",
    );
    draft.kind = AgentComponentKind::Skill;
    draft.requires = vec![ComponentDependency {
        component_id: withdrawn_id,
        kind: AgentComponentKind::Tool,
        definition_digest: pinned.definition_digest,
    }];
    let mut publish_context = context("withdrawal-new-pin");
    publish_context.principal = persistence_id(ADMIN);
    publish_context.caller_principal = persistence_id(ADMIN);
    publish_context.expected_revision = Some(0);
    let op = AgentComponentOp::Publish {
        request: Box::new(AgentComponentPublishRequest {
            context: publish_context,
            component: draft,
            evaluation_receipt_digest: None,
        }),
    };
    op.validate()
        .expect("the new-pin request must reach admission");
    let response = served
        .call("withdrawal-new-pin", Method::AgentComponent { op })
        .await;
    assert_eq!(
        response.error.as_deref(),
        Some("COMPONENT_WITHDRAWN"),
        "{response:?}"
    );
    assert!(
        response
            .error_detail
            .as_deref()
            .is_some_and(|detail| detail.contains("withdrawn from its connector pack")),
        "{response:?}"
    );
    assert!(response.result.is_none());
    assert!(store
        .current_component(TENANT, "skill:new-withdrawn-pin")
        .unwrap()
        .is_none());
}

/// Upload `pack` and import it over `head` with the given approval.
async fn import_approved(
    served: &Served,
    pack: &(eg_types::connector_pack::ConnectorPackIndex, Vec<u8>),
    head: eg_types::connector_pack::PackHeadRef,
    allow_mass_withdrawal: bool,
) -> PackImportResult {
    let (mut index, archive) = pack.clone();
    index.archive.blob_digest = served.upload(&archive).await;
    ok(
        "Import",
        import_uploaded_with(served, index, Some(head), allow_mass_withdrawal).await,
    )
}

/// A connector whose MCP server name changed re-keys every tool URI while the
/// component ids (connector + kind + name) stay the same. The import withdraws
/// every old URI, so it needs the mass-withdrawal approval, and with it the
/// import lands: each component keeps one revision under its new URI.
// spec: EG-TYPED-PACKS-R039
#[tokio::test]
async fn an_approved_mass_withdrawal_that_moves_every_uri_imports() {
    let served = Served::new();
    let connector = "moved-uris";
    bind(&served, connector, ADMIN).await;
    let alpha = tool(connector, "alpha", "Alpha tool.");
    let beta = tool(connector, "beta", "Beta tool.");
    let first = imported(
        &served,
        &build_pack(connector, &[alpha.clone(), beta.clone()]),
        None,
    )
    .await;
    let moved = |mut content: super::Content| {
        content.uri = format!("tool://renamed-server/{}", content.name);
        content
    };
    let tools = [moved(alpha), moved(beta)];
    let renamed = build_pack(connector, &tools);

    // Without approval the move is refused as a mass withdrawal.
    match import_approved(&served, &renamed, head_of(&first), false).await {
        PackImportResult::Rejected { violations, .. } => assert!(violations
            .iter()
            .any(|v| v.code == PackViolationCode::PackMassWithdrawal)),
        other => panic!("expected PACK_MASS_WITHDRAWAL, got {other:?}"),
    }

    // With the approval it lands. The tool bodies are unchanged, the failing
    // production case; a revised server entry gives the request its own pack
    // digest and so its own deterministic operation key.
    let mut server = super::server(connector);
    server.body = format!(
        r#"{{"contract_version":"1","instructions":"Renamed server.","name":"{connector}"}}"#
    )
    .into_bytes();
    let approved = super::build_pack_with_server(connector, &server, &tools);
    let receipt = match import_approved(&served, &approved, head_of(&first), true).await {
        PackImportResult::Imported { receipt } => receipt,
        other => panic!("approved import did not land: {other:?}"),
    };
    assert_eq!(receipt.counts.withdrawn, 2);
    assert_eq!(
        search(&served).await,
        vec![
            format!("mcp:{connector}/tool/alpha"),
            format!("mcp:{connector}/tool/beta")
        ]
    );
    let store = served.state.write().await.ensure_agent_library().unwrap();
    let members = store
        .connector_pack_members(
            TENANT,
            &eg_types::contract::ResourceId::new(connector).unwrap(),
        )
        .unwrap();
    for name in ["alpha", "beta"] {
        let old = members
            .iter()
            .find(|m| m.uri == format!("tool://{connector}/{name}"))
            .unwrap();
        let new = members
            .iter()
            .find(|m| m.uri == format!("tool://renamed-server/{name}"))
            .unwrap();
        assert_eq!(old.lifecycle, AgentLibraryLifecycle::Withdrawn);
        assert_eq!(new.lifecycle, AgentLibraryLifecycle::Published);
        assert_eq!(old.component_id, new.component_id);
        assert!(new.entry_revision > old.entry_revision);
    }
}
