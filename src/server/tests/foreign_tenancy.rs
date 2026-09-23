//! EH-373 foreign-source tenancy proofs through the FULL served dispatch chain
//! (CONCEPT:EG-KG.query.query-federation, CONCEPT:EG-KG.query.closure-backed-source).
//!
//! `RegisterForeignSource` used to write ONE process-global `name → spec` map that every
//! tenant's plan resolved against, so a tenant could make the server spend another
//! tenant's registered credential. These tests register under one verified tenant and
//! query from another, over every served name-resolving surface: a `Named`
//! `Op::ForeignScan` (`UnifiedQuery`), the UQL `FOREIGN "<name>"` marker
//! (`UnifiedQueryText`), and an NL-planned query (`NlQuery`).

use super::*;

const TENANT_A: &str = "tenant-a";
const TENANT_B: &str = "tenant-b";

/// Echoes the NL text back as the UQL to run, so an `NlQuery` exercises the NL handler's
/// own foreign binding with a deterministic plan.
struct EchoPlanner;

impl eg_plan::NlPlanner for EchoPlanner {
    fn plan(&self, nl: &str, _schema_hint: &str) -> Result<String, String> {
        Ok(nl.to_string())
    }
}

/// The UQL marker query every surface below runs: the remote's CITES-traversal set
/// `{d2, d3, d4}` replaces the local seed.
const FOREIGN_UQL: &str = "MATCH (:Doc) |> FOREIGN \"remote_docs\" |> LIMIT 10";

async fn register_in(
    state: &Arc<RwLock<ServerState>>,
    id: u64,
    tenant: &str,
    source: eg_types::wire::ForeignSourceSpec,
) {
    let method = Method::RegisterForeignSource {
        name: "remote_docs".into(),
        source,
    };
    assert_ok(&dispatch_in_tenant(state, id, tenant, method).await);
}

fn named_scan_plan() -> Method {
    Method::UnifiedQuery {
        plan: eg_plan::Plan::new(vec![
            eg_plan::Op::Scan {
                label: "Doc".into(),
            },
            eg_plan::Op::ForeignScan {
                source: Box::new(eg_types::wire::ForeignSourceSpec::Named {
                    name: "remote_docs".into(),
                }),
                join: true,
            },
            eg_plan::Op::Limit { k: 10 },
        ]),
    }
}

fn sorted_ids(resp: &crate::protocol::Response) -> Vec<String> {
    let mut ids = unified_ids(resp);
    ids.sort();
    ids
}

/// Tenant B naming tenant A's source is refused exactly like an unregistered name: the
/// error lists only B's own (empty) registrations, never `ACCESS_DENIED`, so it does not
/// reveal that another tenant uses the name.
fn assert_not_registered_for_caller(resp: &crate::protocol::Response, surface: &str) {
    let err = resp
        .error
        .as_deref()
        .unwrap_or_else(|| panic!("{surface}: tenant B must not resolve tenant A's source"));
    assert!(
        err.contains("no foreign source registered under name 'remote_docs'")
            && err.contains("registered: []")
            && !err.contains("ACCESS_DENIED"),
        "{surface}: cross-tenant name must look unregistered, got: {err}"
    );
}

/// A registers `remote_docs`; A's queries succeed on every surface and B's are refused.
/// A queries FIRST with the identical plan, so B's refusal also proves the result cache
/// is keyed by tenant (B is never served A's cached foreign rows).
#[tokio::test]
async fn foreign_source_resolves_only_for_the_registering_tenant() {
    let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
    let (_remote, remote_addr) = spawn_federation_remote().await;
    let local = test_state();
    build_unified_fixture(&local).await;
    register_in(&local, 900, TENANT_A, federation_remote_spec(remote_addr)).await;

    let a = dispatch_in_tenant(&local, 901, TENANT_A, named_scan_plan()).await;
    assert_ok(&a);
    assert_eq!(
        sorted_ids(&a),
        vec!["d2", "d3", "d4"],
        "local Docs joined with the remote set"
    );
    let b = dispatch_in_tenant(&local, 902, TENANT_B, named_scan_plan()).await;
    assert_not_registered_for_caller(&b, "Named ForeignScan");

    let text = || Method::UnifiedQueryText {
        text: FOREIGN_UQL.into(),
    };
    let a = dispatch_in_tenant(&local, 903, TENANT_A, text()).await;
    assert_ok(&a);
    assert_eq!(sorted_ids(&a), vec!["d2", "d3", "d4"]);
    let b = dispatch_in_tenant(&local, 904, TENANT_B, text()).await;
    assert_not_registered_for_caller(&b, "UQL FOREIGN marker");
}

/// Both tenants register the SAME name. Each resolves its OWN spec: A still reaches the
/// remote engine (B's registration did not overwrite A's), and B's query runs B's own
/// unreachable HTTP spec — it fails on the fetch, not on name resolution, and never
/// returns A's rows.
#[tokio::test]
async fn same_name_in_two_tenants_resolves_each_tenants_own_spec() {
    let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
    let (_remote, remote_addr) = spawn_federation_remote().await;
    let local = test_state();
    build_unified_fixture(&local).await;
    register_in(&local, 910, TENANT_A, federation_remote_spec(remote_addr)).await;
    let b_spec = eg_types::wire::ForeignSourceSpec::HttpJson {
        url: "http://127.0.0.1:9/eh373-tenant-b".into(),
        json_path: "data".into(),
        field_map: eg_types::wire::HttpFieldMap {
            id: "id".into(),
            score: None,
        },
    };
    register_in(&local, 911, TENANT_B, b_spec).await;

    let text = || Method::UnifiedQueryText {
        text: FOREIGN_UQL.into(),
    };
    let a = dispatch_in_tenant(&local, 912, TENANT_A, text()).await;
    assert_ok(&a);
    assert_eq!(
        sorted_ids(&a),
        vec!["d2", "d3", "d4"],
        "tenant B registering the same name must not overwrite tenant A's source"
    );
    let b = dispatch_in_tenant(&local, 913, TENANT_B, text()).await;
    let err = b
        .error
        .expect("tenant B's own spec points at an unreachable endpoint");
    assert!(
        !err.contains("no foreign source registered"),
        "tenant B must resolve its OWN registration, got: {err}"
    );
}

/// The NL handler binds the caller's tenant registry too: the same NL-planned
/// `FOREIGN` query succeeds for the registering tenant and is refused for another.
#[tokio::test]
async fn nl_query_foreign_leg_is_tenant_scoped() {
    let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
    crate::server::set_nl_planner(Arc::new(EchoPlanner));
    let (_remote, remote_addr) = spawn_federation_remote().await;
    let local = test_state();
    build_unified_fixture(&local).await;
    register_in(&local, 920, TENANT_A, federation_remote_spec(remote_addr)).await;

    let nl = || Method::NlQuery {
        text: FOREIGN_UQL.into(),
        graph: "__commons__".into(),
    };
    let a = dispatch_in_tenant(&local, 921, TENANT_A, nl()).await;
    assert_ok(&a);
    assert_eq!(sorted_ids(&a), vec!["d2", "d3", "d4"]);
    let b = dispatch_in_tenant(&local, 922, TENANT_B, nl()).await;
    assert_not_registered_for_caller(&b, "NlQuery");
}
