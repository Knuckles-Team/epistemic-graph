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
    bind, build_pack, context, head_of, import, imported, ok, refused, tool, Served, ADMIN, TENANT,
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
