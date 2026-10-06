//! `ConnectorPack.attest_self_served_catalog` through the real signed dispatch:
//! a producer serving its own catalog obtains EG's binding on a fresh store,
//! imports under it, and is refused on every path the mounted attester is.

use eg_types::connector_pack::{
    ConnectorPackIndex, ConnectorPackOp, McpCatalogAuthorityStatusRequest,
    McpCatalogSnapshotBinding, McpSelfServedCatalogAttestRequest, PackImportResult,
    PackViolationCode,
};
use eg_types::contract::{Digest256, ResourceId};
use eg_types::result_contract::cluster::{
    RegisteredServerListPage, RegisteredServerListRequest, ServerDesiredState,
};

use crate::protocol::{Method, Response};

use super::{
    bind, build_pack, context, head_of, import, imported, ok, refused, tool, Served, ADMIN, TENANT,
};

const CONNECTOR: &str = "self-served";
/// Exactly the two grants the self-served attestation needs.
const ATTESTER: &[&str] = &["connector:catalog-attest", "admin:connector-pack"];

/// What the producer observed and asks EG to bind.
#[derive(Clone)]
struct Observation {
    registry_revision: u64,
    registry_digest: Digest256,
    registration_config_digest: Digest256,
    server_entry_digest: Digest256,
    four_family_digest: Digest256,
    expected_catalog_generation: Option<u64>,
    tenant_id: String,
}

async fn register(served: &Served, key: &str, desired: ServerDesiredState) {
    let _: serde_json::Value = ok(
        "RegisterServer",
        served
            .call(
                key,
                Method::RegisterServer {
                    name: CONNECTOR.to_string(),
                    url: "mcp-ref://self-served".to_string(),
                    resources_json: String::new(),
                    ttl_secs: 3_600,
                    transport: Default::default(),
                    desired,
                },
            )
            .await,
    );
}

/// Read the live registration, as the producer does before each attestation.
async fn observe(served: &Served, key: &str, pack: &(ConnectorPackIndex, Vec<u8>)) -> Observation {
    let page: RegisteredServerListPage = ok(
        "ListRegisteredServers",
        served
            .call(
                key,
                Method::ListRegisteredServers {
                    request: RegisteredServerListRequest {
                        limit: None,
                        cursor: None,
                    },
                },
            )
            .await,
    );
    let view = page
        .entries
        .iter()
        .find(|entry| entry.name == CONNECTOR)
        .expect("the producer's registration is live");
    Observation {
        registry_revision: page.registry_revision,
        registry_digest: page.registry_digest,
        registration_config_digest: super::super::catalog_authority::registration_config_digest(
            view,
        )
        .unwrap(),
        server_entry_digest: pack.0.server.body.sha256,
        four_family_digest: Digest256::from_bytes([3; 32]),
        expected_catalog_generation: None,
        tenant_id: TENANT.to_string(),
    }
}

async fn attest_as(served: &Served, scopes: &[&str], key: &str, seen: &Observation) -> Response {
    let mut request_context = context(key);
    request_context.tenant_id = seen.tenant_id.clone();
    let op = ConnectorPackOp::AttestSelfServedCatalog {
        request: Box::new(McpSelfServedCatalogAttestRequest {
            context: request_context,
            connector: ResourceId::new(CONNECTOR).unwrap(),
            server_name: CONNECTOR.to_string(),
            server_entry_digest: seen.server_entry_digest,
            registry_revision: seen.registry_revision,
            registry_digest: seen.registry_digest,
            registration_config_digest: seen.registration_config_digest,
            four_family_digest: seen.four_family_digest,
            expected_catalog_generation: seen.expected_catalog_generation,
        }),
    };
    served
        .call_as(
            ADMIN,
            scopes,
            key,
            Method::ConnectorPack { op: Box::new(op) },
        )
        .await
}

async fn attest(served: &Served, key: &str, seen: &Observation) -> McpCatalogSnapshotBinding {
    ok(
        "AttestSelfServedCatalog",
        attest_as(served, ATTESTER, key, seen).await,
    )
}

/// The same pack, re-digested under `binding`.
fn under(
    pack: &(ConnectorPackIndex, Vec<u8>),
    binding: &McpCatalogSnapshotBinding,
) -> (ConnectorPackIndex, Vec<u8>) {
    let (mut index, archive) = pack.clone();
    index.catalog = binding.clone();
    index.pack_digest = eg_types::connector_pack::digest::pack_digest(&index).unwrap();
    (index, archive)
}

fn rejected_as_malformed(response: Response) {
    match ok("Import", response) {
        PackImportResult::Rejected { violations, .. } => assert_eq!(
            violations
                .iter()
                .map(|violation| violation.code)
                .collect::<Vec<_>>(),
            [PackViolationCode::MalformedIndex]
        ),
        other => panic!("expected a rejection, got {other:?}"),
    }
}

async fn binding_status(served: &Served, key: &str) -> Option<McpCatalogSnapshotBinding> {
    ok(
        "CatalogBindingStatus",
        served
            .pack(
                key,
                ConnectorPackOp::CatalogBindingStatus {
                    request: McpCatalogAuthorityStatusRequest {
                        tenant_id: TENANT.to_string(),
                        server_name: CONNECTOR.to_string(),
                    },
                },
            )
            .await,
    )
}

/// A producer whose catalog is ready but whose server component exists only in
/// the pack it is about to import: one registration, one importer binding.
async fn ready(served: &Served) -> (ConnectorPackIndex, Vec<u8>) {
    register(served, "register:self-served", ServerDesiredState::Enabled).await;
    bind(served, CONNECTOR, ADMIN).await;
    build_pack(CONNECTOR, &[tool(CONNECTOR, "a", "Tool a.")])
}

#[tokio::test]
async fn a_fresh_store_issues_the_first_binding_and_reruns_are_unchanged() {
    let served = Served::new();
    let _env = served.with_graph_persistence().await;
    let pack = ready(&served).await;
    let seen = observe(&served, "observe:1", &pack).await;
    let binding = attest(&served, "attest:1", &seen).await;
    assert_eq!(
        (
            binding.configuration_revision,
            binding.catalog_generation,
            binding.child_connection_generation
        ),
        (1, 1, 1)
    );
    let pack = under(&pack, &binding);
    let receipt = imported(&served, &pack, None).await;
    assert_eq!(receipt.catalog, binding);

    // The import published the pinned server entry; re-attesting the same
    // catalog keeps the binding, so the same pack is Unchanged.
    let mut again = observe(&served, "observe:2", &pack).await;
    again.expected_catalog_generation = Some(1);
    assert_eq!(attest(&served, "attest:2", &again).await, binding);
    match ok(
        "re-import",
        import(&served, &pack, Some(head_of(&receipt))).await,
    ) {
        PackImportResult::Unchanged { .. } => {}
        other => panic!("an unchanged pack must answer Unchanged, got {other:?}"),
    }
    assert_eq!(
        binding_status(&served, "binding-status").await,
        Some(binding.clone())
    );

    // A stale expectation is refused (wire errors are sanitized, so the
    // proof is that the binding did not move); a changed catalog advances.
    refused(
        "stale",
        attest_as(&served, ATTESTER, "attest:3", &seen).await,
    );
    assert_eq!(
        binding_status(&served, "binding-status:stale").await,
        Some(binding.clone())
    );
    again.four_family_digest = Digest256::from_bytes([5; 32]);
    let advanced = attest(&served, "attest:4", &again).await;
    assert_eq!(advanced.configuration_revision, 1);
    assert_eq!(advanced.catalog_generation, 2);
}

#[tokio::test]
async fn an_import_must_carry_the_current_binding_and_the_pinned_server_entry() {
    let served = Served::new();
    let _env = served.with_graph_persistence().await;
    let pack = ready(&served).await;
    let mut seen = observe(&served, "observe:pin", &pack).await;
    seen.server_entry_digest = Digest256::from_bytes([7; 32]);
    let binding = attest(&served, "attest:pin", &seen).await;
    // EG's binding, but not the server entry it pinned.
    rejected_as_malformed(import(&served, &under(&pack, &binding), None).await);
    // The pinned entry's pack under a binding EG did not issue.
    let mut forged = binding;
    forged.snapshot_digest = Digest256::from_bytes([9; 32]);
    rejected_as_malformed(import(&served, &under(&pack, &forged), None).await);
}

#[tokio::test]
async fn attestation_is_refused_outside_its_tenant_grants_and_registration() {
    let served = Served::new();
    let _env = served.with_graph_persistence().await;
    register(&served, "register:refusals", ServerDesiredState::Enabled).await;
    let pack = build_pack(CONNECTOR, &[tool(CONNECTOR, "a", "Tool a.")]);
    let seen = observe(&served, "observe:refusals", &pack).await;
    // Not the bound importer of the connector.
    refused(
        "unbound",
        attest_as(&served, ATTESTER, "unbound", &seen).await,
    );
    bind(&served, CONNECTOR, ADMIN).await;
    let seen = observe(&served, "observe:bound", &pack).await;
    let mut other_tenant = seen.clone();
    other_tenant.tenant_id = "tenant-other".to_string();
    refused(
        "tenant",
        attest_as(&served, ATTESTER, "tenant", &other_tenant).await,
    );
    for (key, scopes) in [
        ("no-attest-scope", &["admin:connector-pack"][..]),
        ("no-admin-scope", &["connector:catalog-attest"][..]),
    ] {
        refused(key, attest_as(&served, scopes, key, &seen).await);
    }
    let mut stale_registry = seen.clone();
    stale_registry.registry_revision += 1;
    refused(
        "registry",
        attest_as(&served, ATTESTER, "registry", &stale_registry).await,
    );
    let mut moved_config = seen.clone();
    moved_config.registration_config_digest = Digest256::from_bytes([6; 32]);
    refused(
        "config",
        attest_as(&served, ATTESTER, "config", &moved_config).await,
    );
    register(&served, "register:disabled", ServerDesiredState::Disabled).await;
    let disabled = observe(&served, "observe:disabled", &pack).await;
    refused(
        "disabled",
        attest_as(&served, ATTESTER, "disabled", &disabled).await,
    );
    // Nothing was bound by any refusal.
    assert_eq!(
        binding_status(&served, "binding-status:refusals").await,
        None
    );
}
