//! The content model (pack design §3.5, review B3): identity follows content.

use eg_types::agent_library::AgentLibraryLifecycle;
use eg_types::connector_pack::{PackImportResult, PackViolationCode};

use super::{bind, build_pack, head_of, import, imported, ok, skill, tool, Served, ADMIN, TENANT};

const CONNECTOR: &str = "content-model";

fn component(connector: &str, kind: &str, name: &str) -> String {
    format!("mcp:{connector}/{kind}/{name}")
}

async fn current_revision(served: &Served, id: &str) -> (u64, AgentLibraryLifecycle) {
    let store = served.state.write().await.ensure_agent_library().unwrap();
    let entry = store.current_component(TENANT, id).unwrap().unwrap();
    (entry.entry_revision, entry.lifecycle)
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
}

#[tokio::test]
async fn an_absent_entry_is_withdrawn_and_returns_under_the_same_id() {
    let served = Served::new();
    bind(&served, CONNECTOR, ADMIN).await;
    let tools: Vec<_> = ["a", "b", "c", "d"]
        .into_iter()
        .map(|name| tool(CONNECTOR, name, "A tool."))
        .collect();
    let first = imported(&served, &build_pack(CONNECTOR, &tools), None).await;
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
    let served = Served::new();
    bind(&served, CONNECTOR, ADMIN).await;
    let tools: Vec<_> = ["a", "b", "c", "d"]
        .into_iter()
        .map(|name| tool(CONNECTOR, name, "A tool."))
        .collect();
    let first = imported(&served, &build_pack(CONNECTOR, &tools), None).await;
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
