use std::collections::BTreeMap;

use super::*;
use crate::agent_component::{AgentComponentFacts, ComponentDependency};

const CONNECTOR: &str = "servicenow-mcp";
const SERVER: &str = "mcp:servicenow-mcp/server/servicenow-mcp";

fn entry(id: &str, kind: AgentComponentKind, name: &str, summary: &str) -> AgentComponentEntry {
    AgentComponentEntry {
        schema_version: 4,
        component_id: id.to_string(),
        kind,
        version: "1.2.0".to_string(),
        content_digest: format!("sha256:{}", "a".repeat(64)),
        content_ref: None,
        facts: AgentComponentFacts::Opaque,
        provenance: ComponentProvenance::McpServer {
            server: ComponentDependency {
                component_id: SERVER.to_string(),
                kind: AgentComponentKind::McpServer,
                definition_digest: format!("sha256:{}", "b".repeat(64)),
            },
            upstream_name: name.to_string(),
        },
        summary: summary.to_string(),
        classification: Vec::new(),
        requires: Vec::new(),
        declared_capabilities: Vec::new(),
        required_capabilities: Vec::new(),
        declared_required_capabilities: Vec::new(),
        attributes: BTreeMap::from([
            (PACK_CONNECTOR_ATTRIBUTE.to_string(), CONNECTOR.to_string()),
            (PACK_ENTRY_DIGEST_ATTRIBUTE.to_string(), "c".repeat(64)),
        ]),
        tenant_id: "tenant-a".to_string(),
        actor_scope: "principal-importer".to_string(),
        purpose_id: "connector-pack:import".to_string(),
        policy_digest: "d".repeat(64),
        source_revision: "connector-pack".to_string(),
        source_revision_digest: format!("sha256:{}", "e".repeat(64)),
        entry_revision: 3,
        lifecycle: AgentLibraryLifecycle::Published,
        definition_digest: format!("sha256:{}", "f".repeat(64)),
        created_at_ms: 1,
        updated_at_ms: 2,
    }
}

fn uses(target: &AgentComponentEntry) -> ComponentDependency {
    ComponentDependency {
        component_id: target.component_id.clone(),
        kind: target.kind,
        definition_digest: target.definition_digest.clone(),
    }
}

/// A servicenow-like pack: a server, two tools, a skill using one tool, a
/// resource the tool reads, a system prompt and a template prompt.
fn heads() -> Vec<AgentComponentEntry> {
    let server = entry(
        SERVER,
        AgentComponentKind::McpServer,
        CONNECTOR,
        "ServiceNow.",
    );
    let resource = entry(
        "mcp:servicenow-mcp/resource/incident-table",
        AgentComponentKind::McpResource,
        "incident_table",
        "The incident table schema.",
    );
    let mut list = entry(
        "mcp:servicenow-mcp/tool/list_incidents",
        AgentComponentKind::Tool,
        "list_incidents",
        "List open incidents by priority.",
    );
    list.classification = vec!["eg:capability/retrieval".to_string()];
    list.declared_capabilities = vec!["https://example.org/itsm#IncidentManagement".to_string()];
    list.requires = vec![uses(&resource)];
    let change = entry(
        "mcp:servicenow-mcp/tool/create_change",
        AgentComponentKind::Tool,
        "create_change",
        "Create a change request.",
    );
    let mut skill = entry(
        "mcp:servicenow-mcp/skill/triage",
        AgentComponentKind::Skill,
        "triage",
        "Triage an incident queue.",
    );
    skill.requires = vec![uses(&list)];
    let mut system = entry(
        "mcp:servicenow-mcp/prompt/itsm-agent",
        AgentComponentKind::McpPrompt,
        "itsm-agent",
        "You are an ITSM agent.",
    );
    system
        .attributes
        .insert(PROMPT_ROLE_ATTRIBUTE.to_string(), "system".to_string());
    system.classification = vec!["eg:capability/retrieval".to_string()];
    let template = entry(
        "mcp:servicenow-mcp/prompt/summarize-incident",
        AgentComponentKind::McpPrompt,
        "summarize-incident",
        "Summarize one incident.",
    );
    let mut withdrawn = entry(
        "mcp:servicenow-mcp/tool/old_incident_tool",
        AgentComponentKind::Tool,
        "old_incident_tool",
        "An incident tool that was withdrawn.",
    );
    withdrawn.lifecycle = AgentLibraryLifecycle::Withdrawn;
    vec![
        server, resource, list, change, skill, system, template, withdrawn,
    ]
}

fn search(query: &str) -> CatalogSearchRequest {
    CatalogSearchRequest {
        tenant_id: "tenant-a".to_string(),
        query: query.to_string(),
        kinds: Vec::new(),
        connectors: Vec::new(),
        limit: None,
    }
}

// spec: EG-TYPED-PACKS-R106.2.1
#[test]
fn every_asset_has_owner_term_lineage_and_class_link() {
    let page = catalog_search(&search("incident"), &heads());
    let list = page
        .hits
        .iter()
        .find(|hit| hit.asset.name == "list_incidents")
        .expect("the incident tool is an asset");
    let asset = &list.asset;
    assert_eq!(asset.kind, CatalogAssetKind::Tool);
    assert_eq!(asset.connector, CONNECTOR);
    assert_eq!(asset.owner.principal, "principal-importer");
    assert_eq!(asset.owner.source, CatalogOwnerSource::Importer);
    assert!(!asset.owner.owner_declared);
    assert!(asset.flags.contains(&"owner_missing".to_string()));
    let native = asset
        .glossary_terms
        .iter()
        .find(|term| term.term_id == "eg:capability/retrieval")
        .expect("the native capability is a glossary term");
    assert_eq!(native.label, "retrieval");
    assert_eq!(native.broader, vec!["eg:capability".to_string()]);
    assert!(asset
        .glossary_terms
        .iter()
        .any(|term| term.source == GlossarySource::DeclaredCapability
            && term.label == "IncidentManagement"));
    assert!(asset
        .concepts
        .contains(&"https://example.org/itsm#IncidentManagement".to_string()));
    let hops: Vec<(&str, &str, LineageHop)> = asset
        .lineage
        .iter()
        .map(|edge| (edge.from.as_str(), edge.to.as_str(), edge.hop))
        .collect();
    assert!(hops.contains(&(
        "connector:servicenow-mcp",
        SERVER,
        LineageHop::PublishesServer
    )));
    assert!(hops.contains(&(SERVER, asset.asset_id.as_str(), LineageHop::Serves)));
    assert!(hops.contains(&(
        asset.asset_id.as_str(),
        "mcp:servicenow-mcp/resource/incident-table",
        LineageHop::Uses
    )));
    assert!(hops.contains(&(
        "mcp:servicenow-mcp/skill/triage",
        asset.asset_id.as_str(),
        LineageHop::Uses
    )));
    assert_eq!(asset.pack_version.contract_version, "1.2.0");
    assert_eq!(asset.pack_version.entry_revision, 3);
}

// spec: EG-TYPED-PACKS-R108.16.1
#[test]
fn one_search_ranks_assets_and_terms_and_skips_withdrawn_and_servers() {
    let page = catalog_search(&search("incident"), &heads());
    let names: Vec<&str> = page
        .hits
        .iter()
        .map(|hit| hit.asset.name.as_str())
        .collect();
    assert!(
        !names.contains(&"old_incident_tool"),
        "withdrawn is not listed"
    );
    assert!(
        !names.contains(&"servicenow-mcp"),
        "a server is not an asset"
    );
    assert!(
        !names.contains(&"create_change"),
        "a non-match is not a hit"
    );
    for pair in page.hits.windows(2) {
        assert!(pair[0].score >= pair[1].score, "ranked by score");
    }
    for hit in &page.hits {
        assert_eq!(hit.score, hit.rank.total(), "score is its components");
    }
    assert_eq!(page.total_matched as usize, page.hits.len());
    assert_eq!(page.scanned, 6, "server and withdrawn rows are not scanned");

    let by_term = catalog_search(&search("retrieval"), &heads());
    let hit = by_term
        .hits
        .iter()
        .find(|hit| hit.asset.name == "list_incidents")
        .expect("a glossary term finds the asset");
    assert!(hit.rank.glossary > 0);
    assert_eq!(
        hit.matched_terms,
        vec!["eg:capability/retrieval".to_string()]
    );

    let mut tools_only = search("incident");
    tools_only.kinds = vec![CatalogAssetKind::Tool];
    tools_only.limit = Some(1);
    let page = catalog_search(&tools_only, &heads());
    assert_eq!(page.hits.len(), 1);
    assert_eq!(page.hits[0].asset.kind, CatalogAssetKind::Tool);

    let mut elsewhere = search("incident");
    elsewhere.connectors = vec!["github-mcp".to_string()];
    assert!(catalog_search(&elsewhere, &heads()).hits.is_empty());
}

// spec: EG-TYPED-PACKS-R108.16.1
#[test]
fn search_requests_are_bounded() {
    assert!(search("incident").validate().is_ok());
    assert!(search("  ").validate().is_err(), "a query needs a word");
    assert!(search(&"x".repeat(MAX_CATALOG_QUERY_BYTES + 1))
        .validate()
        .is_err());
    let mut zero = search("incident");
    zero.limit = Some(0);
    assert!(zero.validate().is_err());
}

// spec: EG-TYPED-PACKS-R106.2.1
#[test]
fn lineage_walks_connector_server_operation_and_resource() {
    let lineage = catalog_lineage(
        &CatalogLineageRequest {
            tenant_id: "tenant-a".to_string(),
            asset_id: "mcp:servicenow-mcp/resource/incident-table".to_string(),
        },
        &heads(),
    );
    assert!(lineage.found);
    assert!(lineage
        .upstream
        .iter()
        .any(|edge| edge.from == "mcp:servicenow-mcp/tool/list_incidents"
            && edge.hop == LineageHop::Uses));
    assert!(lineage
        .upstream
        .iter()
        .any(|edge| edge.hop == LineageHop::PublishesServer));
    assert!(lineage.downstream.is_empty());
    let missing = catalog_lineage(
        &CatalogLineageRequest {
            tenant_id: "tenant-a".to_string(),
            asset_id: "mcp:servicenow-mcp/tool/nope".to_string(),
        },
        &heads(),
    );
    assert!(!missing.found && missing.upstream.is_empty());
}

// spec: EG-DECISION-ENGINE-R814.1.1
#[test]
fn candidate_round_trips_and_kind_follows_role() {
    let request = PromptCandidatesRequest {
        tenant_id: "tenant-a".to_string(),
        connectors: Vec::new(),
        capability: None,
        system_only: false,
        limit: None,
    };
    request.validate().unwrap();
    let page = prompt_candidates(&request, &heads());
    assert_eq!(page.total, 2);
    let system = &page.candidates[0];
    assert_eq!(system.name, "itsm-agent");
    assert_eq!(system.role, PromptCandidateRole::System);
    assert_eq!(system.connector, CONNECTOR);
    assert_eq!(system.serves, vec!["eg:capability/retrieval".to_string()]);
    assert_eq!(page.candidates[1].role, PromptCandidateRole::ToolGuidance);
    let encoded = serde_json::to_value(&page).unwrap();
    assert_eq!(encoded["candidates"][0]["role"], "system");
    let decoded: PromptCandidatePage = serde_json::from_value(encoded).unwrap();
    assert_eq!(decoded, page);

    let only_system = PromptCandidatesRequest {
        system_only: true,
        ..request.clone()
    };
    assert_eq!(prompt_candidates(&only_system, &heads()).total, 1);
    let covering = PromptCandidatesRequest {
        capability: Some("eg:capability/retrieval".to_string()),
        ..request
    };
    let covered = prompt_candidates(&covering, &heads());
    assert_eq!(covered.total, 1, "only the prompt covering the capability");
}
