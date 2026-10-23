//! Served metadata-catalog reads over an imported pack
//! (EG-TYPED-PACKS-R106.3, R108.16; EG-DECISION-ENGINE-R814.3).
//!
//! The entry point is the served `ConnectorPack.Import`; the reads are the
//! served `AgentComponent` catalog operations over the same component heads.

use eg_types::agent_component::catalog::{CatalogAssetKind, LineageHop, PromptCandidateRole};
use eg_types::agent_component::{
    AgentComponentOp, CatalogLineage, CatalogLineageRequest, CatalogSearchPage,
    CatalogSearchRequest, PromptCandidatePage, PromptCandidatesRequest,
};
use eg_types::connector_pack::{PackAnnotations, PackEntryKind};
use eg_types::contract::BoundedVec;

use super::{bind, build_pack, imported, ok, skill, tool, Content, Served, ADMIN, TENANT};
use crate::protocol::Method;

const CONNECTOR: &str = "catalog-acceptance";

fn prompt(name: &str, description: &str) -> Content {
    Content {
        kind: PackEntryKind::Prompt,
        uri: format!("prompt://{CONNECTOR}/{name}"),
        name: name.to_string(),
        media_type: "application/json",
        body: format!(r#"{{"description":"{description}","name":"{name}"}}"#).into_bytes(),
        input_schema: None,
        annotations: PackAnnotations::default(),
        references: Vec::new(),
    }
}

async fn component(served: &Served, key: &str, op: AgentComponentOp) -> crate::protocol::Response {
    served.call(key, Method::AgentComponent { op }).await
}

// spec: EG-TYPED-PACKS-R106.3.1
// spec: EG-DECISION-ENGINE-R814.3.1
#[tokio::test]
async fn test_import_reaches_the_one_projection_and_catalog_search() {
    let served = Served::new();
    bind(&served, CONNECTOR, ADMIN).await;
    let mut incidents = tool(CONNECTOR, "list_incidents", "List open incidents.");
    incidents.annotations.provides =
        BoundedVec::new(vec!["eg:capability/retrieval".to_string()]).unwrap();
    let change = tool(CONNECTOR, "create_change", "Create a change request.");
    let triage = skill(CONNECTOR, "triage", &[&incidents]);
    let guide = prompt("summarize-incident", "Summarize one incident.");
    imported(
        &served,
        &build_pack(CONNECTOR, &[incidents, change, triage, guide]),
        None,
    )
    .await;

    let page: CatalogSearchPage = ok(
        "CatalogSearch",
        component(
            &served,
            "catalog-search",
            AgentComponentOp::CatalogSearch {
                request: CatalogSearchRequest {
                    tenant_id: TENANT.to_string(),
                    query: "incident".to_string(),
                    kinds: Vec::new(),
                    connectors: Vec::new(),
                    limit: Some(10),
                },
            },
        )
        .await,
    );
    let first = &page
        .hits
        .iter()
        .find(|hit| hit.asset.name == "list_incidents")
        .expect("the term finds the tool")
        .asset;
    assert_eq!(first.kind, CatalogAssetKind::Tool);
    assert_eq!(first.connector, CONNECTOR);
    assert!(!first.owner.principal.is_empty(), "the importer owns it");
    assert!(first
        .glossary_terms
        .iter()
        .any(|term| term.term_id == "eg:capability/retrieval"));
    assert!(first
        .lineage
        .iter()
        .any(|edge| edge.hop == LineageHop::Serves));
    assert!(page
        .hits
        .iter()
        .any(|hit| hit.asset.kind == CatalogAssetKind::Prompt));
    assert!(page
        .hits
        .iter()
        .all(|hit| hit.asset.name != "create_change"));

    let lineage: CatalogLineage = ok(
        "CatalogLineage",
        component(
            &served,
            "catalog-lineage",
            AgentComponentOp::CatalogLineage {
                request: CatalogLineageRequest {
                    tenant_id: TENANT.to_string(),
                    asset_id: first.asset_id.clone(),
                },
            },
        )
        .await,
    );
    assert!(lineage.found);
    assert!(lineage
        .upstream
        .iter()
        .any(|edge| edge.hop == LineageHop::Uses && edge.from.ends_with("/skill/triage")));

    let candidates: PromptCandidatePage = ok(
        "PromptCandidates",
        component(
            &served,
            "prompt-candidates",
            AgentComponentOp::PromptCandidates {
                request: PromptCandidatesRequest {
                    tenant_id: TENANT.to_string(),
                    connectors: vec![CONNECTOR.to_string()],
                    capability: None,
                    system_only: false,
                    limit: None,
                },
            },
        )
        .await,
    );
    assert_eq!(candidates.total, 1);
    assert_eq!(candidates.candidates[0].name, "summarize-incident");
    assert_eq!(
        candidates.candidates[0].role,
        PromptCandidateRole::ToolGuidance
    );
}
