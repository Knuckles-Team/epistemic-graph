//! PB3 (ruling D7): the admin binding decides who may import, retirement is
//! permanent, and the reconciler releases only what nothing holds.

use eg_types::connector_pack::{
    ConnectorPackOp, ConnectorPackReconcileRequest, ConnectorPackRetireRequest,
    ConnectorPackUnbindRequest, PackBodyReconcileReport, PackImportResult, PackRetireResult,
    PackViolationCode,
};
use eg_types::contract::{BoundedVec, Digest256, ResourceId};
use sha2::{Digest, Sha256};

use super::{
    bind, build_pack, context, head_of, import, imported, ok, persistence_id, refused, tool, Served,
    ADMIN, TENANT,
};

const CONNECTOR: &str = "pack-admin";

fn rejected_codes(result: PackImportResult) -> Vec<PackViolationCode> {
    match result {
        PackImportResult::Rejected { violations, .. } => {
            violations.iter().map(|violation| violation.code).collect()
        }
        other => panic!("expected a rejection, got {other:?}"),
    }
}

/// R015: an environment-configured importer is honored only as the initial
/// bootstrap default, before any administrator binds one; once an admin
/// `Bind` names a different importer, the binding governs even though the
/// environment value is still configured and still names the principal that
/// imported successfully a moment ago.
// spec: EG-TYPED-PACKS-R015
#[tokio::test]
async fn an_environment_importer_is_a_bootstrap_default_superseded_by_an_admin_bind() {
    const ENV_VAR: &str = "EPISTEMIC_GRAPH_CONNECTOR_PACK_IMPORTERS";
    const ENV_CONNECTOR: &str = "pack-admin-env-bootstrap";

    struct RestoreEnv(Option<std::ffi::OsString>);
    impl Drop for RestoreEnv {
        fn drop(&mut self) {
            match self.0.take() {
                Some(previous) => std::env::set_var(ENV_VAR, previous),
                None => std::env::remove_var(ENV_VAR),
            }
        }
    }

    let _env_lock = crate::crypto::acquire_test_env_lock().await;
    let _restore = RestoreEnv(std::env::var_os(ENV_VAR));
    std::env::set_var(
        ENV_VAR,
        format!("{ENV_CONNECTOR}={}", super::persistence_id(ADMIN)),
    );

    let served = Served::new();
    let pack = build_pack(ENV_CONNECTOR, &[tool(ENV_CONNECTOR, "a", "Tool a.")]);

    // No admin Bind yet for this connector: the environment-configured
    // importer is honored as the bootstrap default, so the admin principal
    // it names may import.
    imported(&served, &pack, None).await;

    // An administrator now binds a DIFFERENT importer. From here the admin
    // binding governs pack admission; the environment value stays
    // configured (still naming the admin principal) but no longer applies.
    bind(&served, ENV_CONNECTOR, "someone-else").await;
    assert_eq!(
        rejected_codes(ok("Import", import(&served, &pack, None).await)),
        [PackViolationCode::ImporterMismatch],
        "an admin Bind supersedes the environment bootstrap default"
    );
}

#[tokio::test]
async fn only_the_bound_importer_may_import() {
    let served = Served::new();
    let pack = build_pack(CONNECTOR, &[tool(CONNECTOR, "a", "Tool a.")]);
    // No binding and no bootstrap importer: nobody may import.
    assert_eq!(
        rejected_codes(ok("Import", import(&served, &pack, None).await)),
        [PackViolationCode::ImporterMismatch]
    );
    bind(&served, CONNECTOR, "someone-else").await;
    assert_eq!(
        rejected_codes(ok("Import", import(&served, &pack, None).await)),
        [PackViolationCode::ImporterMismatch]
    );
    bind(&served, CONNECTOR, ADMIN).await;
    imported(&served, &pack, None).await;
    let key = "unbind:pack-admin";
    let _: serde_json::Value = ok(
        "Unbind",
        served
            .pack(
                key,
                ConnectorPackOp::Unbind {
                    request: ConnectorPackUnbindRequest {
                        context: context(key),
                        connector: ResourceId::new(CONNECTOR).unwrap(),
                    },
                },
            )
            .await,
    );
    let error = refused(
        "second Unbind",
        served
            .pack(
                "unbind:pack-admin:again",
                ConnectorPackOp::Unbind {
                    request: ConnectorPackUnbindRequest {
                        context: context("unbind:pack-admin:again"),
                        connector: ResourceId::new(CONNECTOR).unwrap(),
                    },
                },
            )
            .await,
    );
    assert_eq!(error, "CONNECTOR_PACK_UNBOUND");
}

#[tokio::test]
async fn a_retired_entry_can_never_return() {
    let served = Served::new();
    bind(&served, CONNECTOR, ADMIN).await;
    let tools: Vec<_> = ["a", "b", "c"]
        .into_iter()
        .map(|name| tool(CONNECTOR, name, "A tool."))
        .collect();
    let first = imported(&served, &build_pack(CONNECTOR, &tools), None).await;
    let key = "retire:pack-admin:c";
    let retired: PackRetireResult = ok(
        "Retire",
        served
            .pack(
                key,
                ConnectorPackOp::Retire {
                    request: ConnectorPackRetireRequest {
                        context: context(key),
                        connector: ResourceId::new(CONNECTOR).unwrap(),
                        uris: BoundedVec::new(vec![tools[2].uri.clone()]).unwrap(),
                    },
                },
            )
            .await,
    );
    assert_eq!(retired.retired.len(), 1);
    // A new release that serves the retired entry again (plus a new tool, so
    // it is not simply the unchanged head).
    let mut returning = tools.clone();
    returning.push(tool(CONNECTOR, "d", "A new tool."));
    assert_eq!(
        rejected_codes(ok(
            "Import",
            import(
                &served,
                &build_pack(CONNECTOR, &returning),
                Some(head_of(&first))
            )
            .await
        )),
        [PackViolationCode::RetiredEntryReturned]
    );
}

#[tokio::test]
async fn the_reconciler_releases_an_orphan_body_and_keeps_every_held_one() {
    let served = Served::new();
    bind(&served, CONNECTOR, ADMIN).await;
    let pack = build_pack(CONNECTOR, &[tool(CONNECTOR, "a", "Tool a.")]);
    imported(&served, &pack, None).await;
    // A body copied by an import whose catalog commit then failed: stored
    // engine-owned, held by nothing the catalog knows about.
    let orphan = b"an orphaned connector body".to_vec();
    let blob = served.state.read().await.blob.clone().unwrap();
    blob.store
        .put_engine_bodies(
            TENANT,
            &[crate::server::blob::engine_bodies::EngineBody {
                sha256: Digest256::from_bytes(Sha256::digest(&orphan).into()),
                body: orphan,
            }],
            1,
        )
        .unwrap();
    let key = "reconcile:pack-admin";
    let report: PackBodyReconcileReport = ok(
        "ReconcileBodies",
        served
            .pack(
                key,
                ConnectorPackOp::ReconcileBodies {
                    request: ConnectorPackReconcileRequest {
                        context: context(key),
                    },
                },
            )
            .await,
    );
    assert_eq!(report.orphaned, 1, "exactly the orphan is released");
    assert_eq!(report.scanned, 2, "the server and the tool body stay held");
    let content: eg_types::agent_component::AgentComponentContentResult = ok(
        "Content",
        served
            .call(
                "content:pack-admin:a",
                crate::protocol::Method::AgentComponent {
                    op: eg_types::agent_component::AgentComponentOp::Content {
                        request: eg_types::agent_component::AgentComponentContentRequest {
                            tenant_id: TENANT.to_string(),
                            component_id: "mcp:pack-admin/tool/a".to_string(),
                            entry_revision: None,
                        },
                    },
                },
            )
            .await,
    );
    assert_eq!(
        content.body,
        tool(CONNECTOR, "a", "Tool a.").body,
        "a held body still reads back after the sweep"
    );
}

// spec: EG-TYPED-PACKS-R002
#[tokio::test]
async fn holder_refcounts_transition_across_unchanged_revised_and_withdrawn_entries() {
    let served = Served::new();
    bind(&served, CONNECTOR, ADMIN).await;
    let a = tool(CONNECTOR, "a", "Tool a.");
    let b = tool(CONNECTOR, "b", "Tool b.");
    let c = tool(CONNECTOR, "c", "Tool c.");
    let first = imported(&served, &build_pack(CONNECTOR, &[a.clone(), b.clone(), c.clone()]), None).await;
    // Next release: `a` unchanged, `b` revised (new body, new holder), `c`
    // withdrawn (dropped from the pack, its holder becomes an orphan).
    let b_revised = tool(CONNECTOR, "b", "Tool b, revised.");
    let second = imported(
        &served,
        &build_pack(CONNECTOR, &[a.clone(), b_revised.clone()]),
        Some(head_of(&first)),
    )
    .await;
    assert!(
        second.counts.unchanged >= 1 && second.counts.revised >= 1 && second.counts.withdrawn >= 1,
        "unchanged/revised/withdrawn dispositions all land in one commit: {:?}",
        second.counts
    );
    // `b`'s revised body and `c`'s withdrawn body are now orphans; `a` and the
    // revised `b` stay held.
    let key = "reconcile:pack-admin-transitions";
    let report: PackBodyReconcileReport = ok(
        "ReconcileBodies",
        served
            .pack(
                key,
                ConnectorPackOp::ReconcileBodies {
                    request: ConnectorPackReconcileRequest {
                        context: context(key),
                    },
                },
            )
            .await,
    );
    assert_eq!(
        report.orphaned, 2,
        "the old `b` body and the withdrawn `c` body are reclaimed: {report:?}"
    );
    let content: eg_types::agent_component::AgentComponentContentResult = ok(
        "Content",
        served
            .call(
                "content:pack-admin-transitions:b",
                crate::protocol::Method::AgentComponent {
                    op: eg_types::agent_component::AgentComponentOp::Content {
                        request: eg_types::agent_component::AgentComponentContentRequest {
                            tenant_id: TENANT.to_string(),
                            component_id: "mcp:pack-admin/tool/b".to_string(),
                            entry_revision: None,
                        },
                    },
                },
            )
            .await,
    );
    assert_eq!(
        content.body, b_revised.body,
        "the revised body, not the orphaned one, still reads back held"
    );
}

// spec: EG-TYPED-PACKS-R007
#[tokio::test]
async fn bind_unbind_and_retire_require_admin_connector_pack_and_the_head_survives_a_retired_reimport(
) {
    let served = Served::new();
    let connector = "pack-admin-scoped";
    let key = format!("bind:{connector}:scoped");
    let unscoped = served
        .call_as(
            "no-admin-scope-caller",
            &["connector:catalog-attest"],
            &key,
            crate::protocol::Method::ConnectorPack {
                op: Box::new(ConnectorPackOp::Bind {
                    request: eg_types::connector_pack::ConnectorPackBindRequest {
                        context: context(&key),
                        connector: ResourceId::new(connector).unwrap(),
                        importer: persistence_id(ADMIN),
                    },
                }),
            },
        )
        .await;
    assert!(
        unscoped.error.is_some(),
        "Bind must be refused without admin:connector-pack"
    );
    bind(&served, connector, ADMIN).await;
    let tools: Vec<_> = ["a", "b"]
        .into_iter()
        .map(|name| tool(connector, name, "A tool."))
        .collect();
    let first = imported(&served, &build_pack(connector, &tools), None).await;
    let before = served
        .state
        .write()
        .await
        .ensure_agent_library()
        .unwrap()
        .connector_pack_status(TENANT, &ResourceId::new(connector).unwrap())
        .unwrap();
    let retire_key = format!("retire:{connector}:b");
    let retired: PackRetireResult = ok(
        "Retire",
        served
            .pack(
                &retire_key,
                ConnectorPackOp::Retire {
                    request: ConnectorPackRetireRequest {
                        context: context(&retire_key),
                        connector: ResourceId::new(connector).unwrap(),
                        uris: BoundedVec::new(vec![tools[1].uri.clone()]).unwrap(),
                    },
                },
            )
            .await,
    );
    assert_eq!(retired.retired.len(), 1);
    let mut returning = tools.clone();
    returning.push(tool(connector, "d", "A new tool."));
    let error = refused(
        "reimporting a retired entry",
        import(
            &served,
            &build_pack(connector, &returning),
            Some(head_of(&first)),
        )
        .await,
    );
    assert_eq!(error, "RETIRED_ENTRY_RETURNED");
    let after = served
        .state
        .write()
        .await
        .ensure_agent_library()
        .unwrap()
        .connector_pack_status(TENANT, &ResourceId::new(connector).unwrap())
        .unwrap();
    assert_eq!(
        before, after,
        "the rejected reimport must not mutate the head"
    );
}

// spec: EG-TYPED-PACKS-R003
#[tokio::test]
async fn a_rejected_import_mutates_nothing_in_the_catalog() {
    let served = Served::new();
    let connector = "pack-admin-zero-mutation";
    bind(&served, connector, ADMIN).await;
    let first = imported(
        &served,
        &build_pack(connector, &[tool(connector, "a", "Tool a.")]),
        None,
    )
    .await;
    let before = served
        .state
        .write()
        .await
        .ensure_agent_library()
        .unwrap()
        .connector_pack_status(TENANT, &ResourceId::new(connector).unwrap())
        .unwrap();
    // A second release whose entry list is out of its required sort order
    // (validate_header requires ascending, unique URIs) -- malformed, so a
    // rejection that must leave the catalog untouched.
    let (mut index, archive) = build_pack(
        connector,
        &[
            tool(connector, "b", "Tool b."),
            tool(connector, "z", "Tool z."),
        ],
    );
    let mut entries: Vec<_> = index.entries.iter().cloned().collect();
    entries.reverse();
    index.entries = BoundedVec::new(entries).unwrap();
    index.archive.blob_digest = served.upload(&archive).await;
    let key = "import:pack-admin-zero-mutation:unsorted";
    let rejected = served
        .pack(
            key,
            ConnectorPackOp::Import {
                request: Box::new(eg_types::connector_pack::ConnectorPackImportRequest {
                    context: context(key),
                    index,
                    expected_head: Some(head_of(&first)),
                    allow_mass_withdrawal: false,
                }),
            },
        )
        .await;
    assert_eq!(
        rejected_codes(ok("Import", rejected)),
        [PackViolationCode::MalformedIndex]
    );
    let after = served
        .state
        .write()
        .await
        .ensure_agent_library()
        .unwrap()
        .connector_pack_status(TENANT, &ResourceId::new(connector).unwrap())
        .unwrap();
    assert_eq!(
        before, after,
        "a rejected import must commit zero catalog mutation"
    );
}

// spec: EG-TYPED-PACKS-R005
#[tokio::test]
async fn an_end_to_end_connector_fixture_imports_through_to_a_served_read() {
    let served = Served::new();
    let connector = "pack-admin-e2e";
    bind(&served, connector, ADMIN).await;
    let fixture = tool(connector, "fixture", "An end-to-end fixture tool.");
    imported(&served, &build_pack(connector, &[fixture.clone()]), None).await;
    let content: eg_types::agent_component::AgentComponentContentResult = ok(
        "Content",
        served
            .call(
                "content:pack-admin-e2e:fixture",
                crate::protocol::Method::AgentComponent {
                    op: eg_types::agent_component::AgentComponentOp::Content {
                        request: eg_types::agent_component::AgentComponentContentRequest {
                            tenant_id: TENANT.to_string(),
                            component_id: "mcp:pack-admin-e2e/tool/fixture".to_string(),
                            entry_revision: None,
                        },
                    },
                },
            )
            .await,
    );
    assert_eq!(
        content.body, fixture.body,
        "the fixture's served read matches exactly what Import landed"
    );
}
