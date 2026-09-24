use super::super::records::RecordMeta;
use super::*;
use eg_types::fleet_catalog::DiscoveryScope;

const DIGEST: &str = "sha256:0000000000000000000000000000000000000000000000000000000000000000";

/// Built from JSON so a fact the Decide lanes add with a default does not
/// break this fixture.
fn entry(component_id: &str, kind: &str, facts: serde_json::Value) -> AgentComponentEntry {
    serde_json::from_value(serde_json::json!({
        "schema_version": 3,
        "component_id": component_id,
        "kind": kind,
        "version": "1",
        "content_digest": DIGEST,
        "facts": facts,
        "provenance": {"origin": "native"},
        "summary": format!("summary of {component_id}"),
        "attributes": {
            "mcp.uri": "skill://triage/SKILL.md",
            "mcp.media_type": "text/markdown",
            "skill.type": "workflow",
            "sdk.tool_mode": "condensed",
        },
        "tenant_id": "tenant-a",
        "actor_scope": "principal:sha256:importer",
        "purpose_id": "purpose",
        "policy_digest": DIGEST,
        "source_revision": "connector-pack",
        "source_revision_digest": DIGEST,
        "entry_revision": 2,
        "lifecycle": "published",
        "definition_digest": DIGEST,
        "created_at_ms": 1,
        "updated_at_ms": 2,
    }))
    .expect("fixture is a valid component entry")
}

fn tool(component_id: &str) -> AgentComponentEntry {
    entry(
        component_id,
        "tool",
        serde_json::json!({
            "facts": "tool",
            "effect": "read",
            "required_scopes": [],
            "input_schema_digest": DIGEST,
        }),
    )
}

fn opaque(component_id: &str, kind: &str) -> AgentComponentEntry {
    entry(component_id, kind, serde_json::json!({"facts": "opaque"}))
}

fn discovery(server: &str, connector: &str, visibility: FleetVisibility) -> VisibleDiscovery {
    VisibleDiscovery {
        record: StoredRecord {
            meta: RecordMeta {
                tenant_id: "tenant-a".to_string(),
                revision: 1,
                content_digest: "c".to_string(),
                written_at_ms: 5,
            },
            body: DiscoveryBody::reachable_for_tests(
                server,
                connector,
                DiscoveryScope::TenantLocal,
                "principal:sha256:observer",
            ),
        },
        visibility,
    }
}

fn binding(server: &str, connector: &str) -> ConnectorBinding {
    ConnectorBinding {
        server_name: server.to_string(),
        connector: ResourceId::new(connector).unwrap(),
        visibility: FleetVisibility::Tenant,
    }
}

fn row(kind: FleetCatalogKind, entry: &AgentComponentEntry) -> Option<FleetCatalogRow> {
    let desired = BTreeMap::new();
    let overrides = BTreeMap::new();
    component_row(
        kind,
        entry,
        &binding("github", "github"),
        &ProjectionInputs {
            desired: &desired,
            skill_overrides: &overrides,
        },
    )
}

#[test]
fn a_connector_is_bound_through_its_widest_observation() {
    let principal = FleetVisibility::Principal {
        principal: "alice".to_string(),
    };
    let bindings = connector_bindings(&[
        discovery("a-server", "shared", principal.clone()),
        discovery("z-server", "shared", FleetVisibility::Tenant),
        discovery("private", "private", principal.clone()),
    ]);
    assert_eq!(bindings.len(), 2);
    let shared = bindings
        .iter()
        .find(|b| b.connector.as_str() == "shared")
        .unwrap();
    assert_eq!(shared.visibility, FleetVisibility::Tenant);
    assert_eq!(shared.server_name, "z-server");
    let private = bindings
        .iter()
        .find(|b| b.connector.as_str() == "private")
        .unwrap();
    assert_eq!(private.visibility, principal);
}

#[test]
fn member_ids_round_trip_through_the_exact_prefix() {
    let connector = ResourceId::new("github").unwrap();
    let prefix = member_prefix(&connector, FleetCatalogKind::Tools).unwrap();
    assert_eq!(prefix, "mcp:github/tool/");
    assert_eq!(
        member_prefix(&connector, FleetCatalogKind::Discoveries),
        None
    );
    assert_eq!(
        parse_member_id("mcp:github/skill/triage%2Fnight"),
        Some((connector.clone(), FleetCatalogKind::Skills))
    );
    for invalid in [
        "mcp:github/ontology/x",
        "mcp:github/tool/",
        "mcp:github/tool/a/b",
        "skill:github/tool/x",
        "srvobs:github:tenant_local",
    ] {
        assert_eq!(parse_member_id(invalid), None, "{invalid}");
    }
}

#[test]
fn names_come_from_provenance_or_the_unescaped_id() {
    assert_eq!(
        unescape_pack_name("triage%2Fnight%20shift").as_deref(),
        Some("triage/night shift")
    );
    assert_eq!(unescape_pack_name("bad%2"), None);
    assert_eq!(unescape_pack_name("bad%zz"), None);
    let mut named = tool("mcp:github/tool/search%20issues");
    assert_eq!(component_name(&named), "search issues");
    named.provenance = serde_json::from_value(serde_json::json!({
        "origin": "mcp_server",
        "server": {
            "component_id": "mcp:github/mcp_server/github",
            "kind": "mcp_server",
            "definition_digest": DIGEST,
        },
        "upstream_name": "search_issues",
    }))
    .unwrap();
    assert_eq!(component_name(&named), "search_issues");
}

#[test]
fn every_content_kind_projects_its_typed_facts() {
    let Some(FleetCatalogRow::Tool { row: tool_row }) =
        row(FleetCatalogKind::Tools, &tool("mcp:github/tool/search"))
    else {
        panic!("a published tool projects as a tool row");
    };
    assert_eq!(tool_row.tool_mode, ToolMode::Condensed);
    assert_eq!(tool_row.input_schema_digest.as_deref(), Some(DIGEST));
    assert!(tool_row.component.enabled);
    assert_eq!(
        tool_row.component.acl.publisher,
        "principal:sha256:importer"
    );

    let Some(FleetCatalogRow::Resource { row: resource }) = row(
        FleetCatalogKind::Resources,
        &opaque("mcp:github/resource/triage", "mcp_resource"),
    ) else {
        panic!("a published resource projects as a resource row");
    };
    assert_eq!(resource.resource_kind, ResourceKind::Skill);
    assert_eq!(resource.media_type.as_deref(), Some("text/markdown"));

    assert!(matches!(
        row(
            FleetCatalogKind::Prompts,
            &opaque("mcp:github/prompt/p", "mcp_prompt")
        ),
        Some(FleetCatalogRow::Prompt { .. })
    ));
}

#[test]
fn only_served_members_of_the_listed_kind_project() {
    let mut withdrawn = tool("mcp:github/tool/gone");
    withdrawn.lifecycle = AgentLibraryLifecycle::Withdrawn;
    assert_eq!(row(FleetCatalogKind::Tools, &withdrawn), None);
    assert_eq!(
        row(
            FleetCatalogKind::Tools,
            &opaque("mcp:github/tool/no-facts", "tool")
        ),
        None
    );
    assert_eq!(
        row(FleetCatalogKind::Prompts, &tool("mcp:github/tool/t")),
        None
    );
    assert_eq!(
        row(FleetCatalogKind::Discoveries, &tool("mcp:github/tool/t")),
        None
    );
}

#[test]
fn an_override_beats_the_declaration_which_beats_the_default() {
    let skill = opaque("mcp:skills/skill/triage", "skill");
    let desired = BTreeMap::from([("github".to_string(), ServerDesiredState::Disabled)]);
    let overrides = BTreeMap::from([(skill.component_id.clone(), (SkillType::Graph, 7))]);
    let inputs = ProjectionInputs {
        desired: &desired,
        skill_overrides: &overrides,
    };
    let Some(FleetCatalogRow::Skill { row: overridden }) = component_row(
        FleetCatalogKind::Skills,
        &skill,
        &binding("github", "skills"),
        &inputs,
    ) else {
        panic!("a published skill projects as a skill row");
    };
    assert_eq!(overridden.skill_type, SkillType::Graph);
    assert_eq!(overridden.classification, "Skill Graph");
    assert_eq!(overridden.skill_type_source, SkillTypeSource::Override);
    assert_eq!(overridden.override_revision, Some(7));
    assert!(
        !overridden.component.enabled,
        "a disabled server disables its members"
    );

    let Some(FleetCatalogRow::Skill { row: declared }) = row(FleetCatalogKind::Skills, &skill)
    else {
        panic!("a published skill projects as a skill row");
    };
    assert_eq!(declared.skill_type, SkillType::Workflow);
    assert_eq!(declared.skill_type_source, SkillTypeSource::Declared);

    let mut undeclared = skill.clone();
    undeclared.attributes.remove("skill.type");
    let Some(FleetCatalogRow::Skill { row: defaulted }) =
        row(FleetCatalogKind::Skills, &undeclared)
    else {
        panic!("a published skill projects as a skill row");
    };
    assert_eq!(defaulted.skill_type, SkillType::Skill);
    assert_eq!(defaulted.skill_type_source, SkillTypeSource::Default);
}

fn tools(names: &[&str]) -> Vec<FleetCatalogRow> {
    names
        .iter()
        .filter_map(|name| {
            row(
                FleetCatalogKind::Tools,
                &tool(&format!("mcp:github/tool/{name}")),
            )
        })
        .collect()
}

fn list(limit: u16, cursor: Option<FleetCatalogCursor>) -> FleetCatalogListRequest {
    FleetCatalogListRequest {
        kind: FleetCatalogKind::Tools,
        query: None,
        grant_digests: BoundedVec::default(),
        limit: Some(limit),
        cursor,
    }
}

#[test]
fn snapshot_filters_case_insensitively_and_orders_by_name_then_id() {
    let rows = filtered_snapshot(tools(&["Zeta", "alpha", "Beta"]), Some("A")).unwrap();
    let names: Vec<&str> = rows.iter().map(|row| row.key().0).collect();
    assert_eq!(names, ["alpha", "Beta", "Zeta"]);
    let rows = filtered_snapshot(tools(&["Zeta", "alpha", "Beta"]), Some("ze")).unwrap();
    assert_eq!(rows.len(), 1);
    let described = filtered_snapshot(tools(&["alpha"]), Some("SUMMARY OF")).unwrap();
    assert_eq!(described.len(), 1, "the description is searched too");
}

#[test]
fn pages_resume_exclusively_and_go_stale_when_the_snapshot_changes() {
    let snapshot = filtered_snapshot(tools(&["charlie", "alpha", "bravo"]), None).unwrap();
    let first = page(snapshot.clone(), &list(2, None), 9, 10).unwrap();
    assert_eq!(first.total, 3);
    assert_eq!(first.rows.len(), 2);
    let cursor = first.next_cursor.clone().expect("a third row remains");
    let second = page(snapshot.clone(), &list(2, Some(cursor.clone())), 9, 11).unwrap();
    assert_eq!(second.rows.as_slice()[0].key().0, "charlie");
    assert!(second.next_cursor.is_none());

    let grown = filtered_snapshot(tools(&["charlie", "alpha", "bravo", "delta"]), None).unwrap();
    let stale = page(grown, &list(2, Some(cursor)), 9, 12).unwrap_err();
    assert!(stale.starts_with("FLEET_CURSOR_STALE"));

    let heartbeat = page(snapshot, &list(2, first.next_cursor.clone()), 99, 13).unwrap();
    assert_eq!(
        heartbeat.rows.len(),
        1,
        "a __commons__ revision change alone does not stale the cursor"
    );
}
