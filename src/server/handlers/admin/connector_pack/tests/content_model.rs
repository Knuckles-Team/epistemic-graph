//! The content model (pack design §3.5, review B3): identity follows content.

use eg_types::agent_component::AgentComponentEntry;
use eg_types::agent_library::AgentLibraryLifecycle;
use eg_types::connector_pack::{PackDispositionCounts, PackImportResult, PackViolationCode};

use super::{
    bind, build_pack, build_pack_with_server, head_of, import, imported, ok, server, skill, tool,
    Content, Served, ADMIN, TENANT,
};

const CONNECTOR: &str = "content-model";

fn component(connector: &str, kind: &str, name: &str) -> String {
    format!("mcp:{connector}/{kind}/{name}")
}

async fn current_revision(served: &Served, id: &str) -> (u64, AgentLibraryLifecycle) {
    let entry = current_component(served, id).await;
    (entry.entry_revision, entry.lifecycle)
}

async fn current_component(served: &Served, id: &str) -> AgentComponentEntry {
    let store = served.state.write().await.ensure_agent_library().unwrap();
    store.current_component(TENANT, id).unwrap().unwrap()
}

async fn current_server(served: &Served) -> AgentComponentEntry {
    let id = eg_types::connector_pack::pack_component_id(
        CONNECTOR,
        eg_types::connector_pack::PackEntryKind::McpServer,
        CONNECTOR,
    );
    current_component(served, &id).await
}

#[tokio::test]
async fn one_changed_tool_revises_only_that_tool() {
    let served = Served::new();
    bind(&served, CONNECTOR, ADMIN).await;
    let a = tool(CONNECTOR, "a", "Tool a.");
    let b = tool(CONNECTOR, "b", "Tool b.");
    let s = skill(CONNECTOR, "skill-a", &[&a]);
    let first = imported(
        &served,
        &build_pack(CONNECTOR, &[a.clone(), b, s.clone()]),
        None,
    )
    .await;
    assert_eq!(first.counts.published, 4, "server, two tools and a skill");
    let changed = tool(CONNECTOR, "b", "Tool b, revised.");
    let second = imported(
        &served,
        &build_pack(CONNECTOR, &[a, changed, s]),
        Some(head_of(&first)),
    )
    .await;
    assert_eq!(second.counts.revised, 1, "exactly the changed tool");
    assert_eq!(second.counts.unchanged, 3, "server, tool a and the skill");
    assert_eq!(
        current_revision(&served, &component(CONNECTOR, "tool", "a"))
            .await
            .0,
        1,
        "an unchanged entry is carried forward, never re-revisioned"
    );
    assert_eq!(
        current_revision(&served, &component(CONNECTOR, "tool", "b"))
            .await
            .0,
        2
    );
}

#[tokio::test]
async fn a_package_release_with_identical_content_is_unchanged() {
    let served = Served::new();
    bind(&served, CONNECTOR, ADMIN).await;
    let pack = build_pack(CONNECTOR, &[tool(CONNECTOR, "a", "Tool a.")]);
    let first = imported(&served, &pack, None).await;
    let original_server = current_server(&served).await;
    let tool_id = component(CONNECTOR, "tool", "a");
    let original_tool = current_component(&served, &tool_id).await;
    let mut release = pack.clone();
    release.0.server_package_version = "9.9.9".to_string();
    match ok(
        "Import",
        import(&served, &release, Some(head_of(&first))).await,
    ) {
        PackImportResult::Unchanged {
            pack_digest,
            binding_revision,
        } => {
            assert_eq!(pack_digest, first.pack_digest);
            assert_eq!(binding_revision, first.binding_revision);
        }
        other => panic!("a package release must be Unchanged: {other:?}"),
    }
    let released_server = current_server(&served).await;
    assert_eq!(
        released_server.definition_digest,
        original_server.definition_digest
    );
    assert_eq!(
        released_server.entry_revision,
        original_server.entry_revision
    );
    let released_tool = current_component(&served, &tool_id).await;
    assert_eq!(released_tool.entry_revision, original_tool.entry_revision);
    assert_eq!(
        released_tool.definition_digest,
        original_tool.definition_digest
    );
    assert_eq!(
        released_tool.provenance.pinned_component().unwrap(),
        original_tool.provenance.pinned_component().unwrap()
    );
}

/// R016/R050: fresh imports cannot hide package-dependent pins behind the
/// same-store `Unchanged` fast path. Both stores independently publish content.
#[tokio::test]
async fn package_versions_produce_equal_pins_in_independent_stores() {
    let original_store = Served::new();
    let release_store = Served::new();
    bind(&original_store, CONNECTOR, ADMIN).await;
    bind(&release_store, CONNECTOR, ADMIN).await;
    let pack = build_pack(CONNECTOR, &[tool(CONNECTOR, "a", "Tool a.")]);
    let mut release = pack.clone();
    release.0.server_package_version = "9.9.9".to_string();
    let original_receipt = imported(&original_store, &pack, None).await;
    let release_receipt = imported(&release_store, &release, None).await;
    assert_eq!(original_receipt.pack_digest, release_receipt.pack_digest);
    for receipt in [&original_receipt, &release_receipt] {
        assert_eq!(receipt.previous_pack_digest, None);
        assert_eq!(
            receipt.counts,
            PackDispositionCounts {
                published: 2,
                ..Default::default()
            }
        );
    }

    let original_server = current_server(&original_store).await;
    let release_server = current_server(&release_store).await;
    let tool_id = component(CONNECTOR, "tool", "a");
    let original_tool = current_component(&original_store, &tool_id).await;
    let release_tool = current_component(&release_store, &tool_id).await;
    for entry in [
        &original_server,
        &release_server,
        &original_tool,
        &release_tool,
    ] {
        assert_eq!(entry.entry_revision, 1);
        assert_eq!(entry.lifecycle, AgentLibraryLifecycle::Published);
    }
    let original_pin = original_tool.provenance.pinned_component().unwrap();
    let release_pin = release_tool.provenance.pinned_component().unwrap();
    assert_eq!(original_pin.component_id, original_server.component_id);
    assert_eq!(
        original_pin.definition_digest,
        original_server.definition_digest
    );
    assert_eq!(release_pin.component_id, release_server.component_id);
    assert_eq!(
        release_pin.definition_digest,
        release_server.definition_digest
    );
    assert_eq!(
        (
            &original_server.component_id,
            &original_server.definition_digest,
            original_pin
        ),
        (
            &release_server.component_id,
            &release_server.definition_digest,
            release_pin
        ),
        "package provenance must not change independently constructed server or dependent pins"
    );
}

/// EG-TYPED-PACKS-R016: vary exactly one declared server field while keeping
/// its component identity and package version fixed. Rebuild honest sections
/// and archive digests, then exercise the served import and persisted pin.
async fn assert_server_contract_change_revises_pin(field: &str, value: &str) {
    let served = Served::new();
    bind(&served, CONNECTOR, ADMIN).await;
    let entries = [tool(CONNECTOR, "a", "Tool a.")];
    let pack = build_pack(CONNECTOR, &entries);
    let first = imported(&served, &pack, None).await;
    let original = current_server(&served).await;
    assert_eq!(original.entry_revision, 1);

    let mut changed_server = server(CONNECTOR);
    let mut body: serde_json::Value = serde_json::from_slice(&changed_server.body).unwrap();
    body[field] = serde_json::json!(value);
    changed_server.body = serde_json::to_vec(&body).unwrap();
    let changed = build_pack_with_server(CONNECTOR, &changed_server, &entries);
    assert_eq!(
        changed.0.server_package_version,
        pack.0.server_package_version
    );
    assert_ne!(changed.0.pack_digest, pack.0.pack_digest);
    let second = imported(&served, &changed, Some(head_of(&first))).await;
    assert_ne!(second.pack_digest, first.pack_digest);
    assert_eq!(second.pack_digest, changed.0.pack_digest);
    assert_eq!(second.previous_pack_digest, Some(first.pack_digest));
    assert!(second.binding_revision > first.binding_revision);

    let revised = current_server(&served).await;
    assert_eq!(revised.component_id, original.component_id);
    assert_eq!(revised.entry_revision, 2);
    assert_ne!(revised.definition_digest, original.definition_digest);
    let dependent = current_component(&served, &component(CONNECTOR, "tool", "a")).await;
    let pin = dependent.provenance.pinned_component().unwrap();
    assert_eq!(pin.component_id, revised.component_id);
    assert_eq!(pin.definition_digest, revised.definition_digest);
    assert_eq!(
        dependent.entry_revision, 2,
        "the changed server pin revises its tool"
    );
    assert_eq!(
        second.counts,
        PackDispositionCounts {
            revised: 2,
            ..Default::default()
        },
        "server content and the dependent server pin both changed"
    );
}

#[tokio::test]
async fn a_server_contract_version_change_revises_its_pin() {
    assert_server_contract_change_revises_pin("contract_version", "2").await;
}

#[tokio::test]
async fn a_server_name_change_revises_its_pin() {
    assert_server_contract_change_revises_pin("name", "Renamed server").await;
}

#[tokio::test]
async fn a_server_instructions_change_revises_its_pin() {
    assert_server_contract_change_revises_pin("instructions", "Revised instructions.").await;
}

/// A served engine with `CONNECTOR` bound and four tools `a`..`d` imported.
async fn four_tools_imported() -> (
    Served,
    Vec<Content>,
    eg_types::connector_pack::PackImportReceipt,
) {
    let served = Served::new();
    bind(&served, CONNECTOR, ADMIN).await;
    let tools: Vec<_> = ["a", "b", "c", "d"]
        .into_iter()
        .map(|name| tool(CONNECTOR, name, "A tool."))
        .collect();
    let first = imported(&served, &build_pack(CONNECTOR, &tools), None).await;
    (served, tools, first)
}

#[tokio::test]
async fn an_absent_entry_is_withdrawn_and_returns_under_the_same_id() {
    let (served, tools, first) = four_tools_imported().await;
    let second = imported(
        &served,
        &build_pack(CONNECTOR, &tools[..3]),
        Some(head_of(&first)),
    )
    .await;
    assert_eq!(second.counts.withdrawn, 1);
    let d = component(CONNECTOR, "tool", "d");
    assert_eq!(
        current_revision(&served, &d).await.1,
        AgentLibraryLifecycle::Withdrawn
    );
    let third = imported(
        &served,
        &build_pack(CONNECTOR, &tools),
        Some(head_of(&second)),
    )
    .await;
    assert_eq!(third.counts.republished, 1);
    assert_eq!(
        current_revision(&served, &d).await,
        (3, AgentLibraryLifecycle::Published),
        "the returning entry keeps its component id"
    );
}

#[tokio::test]
async fn withdrawing_most_of_a_connector_needs_the_admin_override() {
    let (served, tools, first) = four_tools_imported().await;
    match ok(
        "Import",
        import(
            &served,
            &build_pack(CONNECTOR, &tools[..1]),
            Some(head_of(&first)),
        )
        .await,
    ) {
        PackImportResult::Rejected { violations, .. } => assert_eq!(
            violations.iter().map(|v| v.code).collect::<Vec<_>>(),
            [PackViolationCode::PackMassWithdrawal]
        ),
        other => panic!("a mass withdrawal must be rejected: {other:?}"),
    }
}
