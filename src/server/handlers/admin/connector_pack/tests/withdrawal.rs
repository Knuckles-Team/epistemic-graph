//! Signed current-discovery and historical-content acceptance.

use super::{
    bind, build_pack, context, head_of, imported, ok, refused, tool, Served, ADMIN, TENANT,
};
use crate::protocol::Method;
use eg_types::agent_component::{
    AgentComponentContentRequest, AgentComponentContentResult, AgentComponentKind,
    AgentComponentOp, AgentComponentPublishRequest, AgentComponentSearchPage,
    AgentComponentSearchRequest, ComponentDependency,
};
use eg_types::agent_library::AgentLibraryLifecycle;

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
    publish_context.expected_revision = Some(0);
    let error = refused(
        "Publish",
        served
            .call(
                "withdrawal-new-pin",
                Method::AgentComponent {
                    op: AgentComponentOp::Publish {
                        request: Box::new(AgentComponentPublishRequest {
                            context: publish_context,
                            component: draft,
                            evaluation_receipt_digest: None,
                        }),
                    },
                },
            )
            .await,
    );
    assert!(error.starts_with("COMPONENT_WITHDRAWN:"), "{error}");
    assert!(store
        .current_component(TENANT, "skill:new-withdrawn-pin")
        .unwrap()
        .is_none());
}
