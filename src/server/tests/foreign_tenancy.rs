//! EH-373 foreign-source ownership proofs through the FULL served dispatch chain
//! (CONCEPT:EG-KG.query.query-federation, CONCEPT:EG-KG.query.closure-backed-source).
//!
//! `RegisterForeignSource` used to write ONE process-global `name → spec` map that every
//! caller's plan resolved against, so a principal could make the server spend another
//! principal's registered credential. One engine is bound to ONE tenant (every verified
//! carrier carries `EPISTEMIC_GRAPH_TENANT`), so the boundary is the principal: these
//! tests register as one principal (`worker1`) and query as another (`worker2`) in the
//! same tenant, both with R/W on `__commons__`, over every served name-resolving
//! surface: a `Named` `Op::ForeignScan` (`UnifiedQuery`), the UQL `FOREIGN "<name>"`
//! marker (`Uql`), and an NL-planned query (`NlQuery`).

use super::*;

const OWNER_A: &str = "worker1";
const OTHER_B: &str = "worker2";

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
    agent: &str,
    source: eg_types::wire::ForeignSourceSpec,
) {
    let method = Method::RegisterForeignSource {
        name: "remote_docs".into(),
        source,
    };
    assert_ok(&dispatch_as(state, id, agent, method).await);
}

/// A live remote engine (keep the returned handle alive) and a local fixture graph on
/// which `OWNER_A` registered `remote_docs` against that remote, as request `id`.
async fn registered_by_owner_a(id: u64) -> (Arc<RwLock<ServerState>>, Arc<RwLock<ServerState>>) {
    let (remote, remote_addr) = spawn_federation_remote().await;
    let local = multi_tenant_state().await;
    build_unified_fixture(&local).await;
    register_in(&local, id, OWNER_A, federation_remote_spec(remote_addr)).await;
    (remote, local)
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

/// [`sorted_ids`] of a `Method::Uql` response.
fn sorted_uql_ids(resp: &crate::protocol::Response) -> Vec<String> {
    let mut ids = crate::server::decode_uql_ids(resp);
    ids.sort();
    ids
}

/// A parameterless UQL statement.
fn uql(text: impl Into<String>) -> Method {
    Method::Uql {
        text: text.into(),
        params: Default::default(),
    }
}

/// Principal B naming principal A's source is refused exactly like an unregistered
/// name: the error lists only B's own (empty) registrations. It must be the foreign
/// not-found — never `ACCESS_DENIED` (an existence oracle) and never the envelope's
/// tenant-binding refusal (which would mean the request never reached the plan).
fn assert_not_registered_for_caller(resp: &crate::protocol::Response, surface: &str) {
    let err = resp
        .error_detail
        .as_deref()
        .unwrap_or_else(|| panic!("{surface}: principal B must not resolve principal A's source"));
    assert!(
        err.contains("no foreign source registered under name 'remote_docs' (registered: [])"),
        "{surface}: cross-principal name must look unregistered, got: {err}"
    );
    assert!(
        !err.contains("ACCESS_DENIED"),
        "{surface}: not an existence oracle: {err}"
    );
    assert!(
        !err.contains("does not match graph tenant"),
        "{surface}: refused at the envelope, not by the foreign catalog: {err}"
    );
}

/// A registers `remote_docs`; A's queries succeed on every surface and B's are refused.
/// A queries FIRST with the identical plan, so B's refusal also proves the result cache
/// is keyed by the foreign owner (B is never served A's cached foreign rows).
#[tokio::test]
async fn foreign_source_resolves_only_for_the_registering_principal() {
    let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
    let (_remote, local) = registered_by_owner_a(900).await;

    let a = dispatch_as(&local, 901, OWNER_A, named_scan_plan()).await;
    assert_ok(&a);
    assert_eq!(
        sorted_ids(&a),
        vec!["d2", "d3", "d4"],
        "local Docs joined with the remote set"
    );
    let b = dispatch_as(&local, 902, OTHER_B, named_scan_plan()).await;
    assert_not_registered_for_caller(&b, "Named ForeignScan");

    let text = || uql(FOREIGN_UQL);
    let a = dispatch_as(&local, 903, OWNER_A, text()).await;
    assert_ok(&a);
    assert_eq!(sorted_uql_ids(&a), vec!["d2", "d3", "d4"]);
    let b = dispatch_as(&local, 904, OTHER_B, text()).await;
    assert_not_registered_for_caller(&b, "UQL FOREIGN marker");
}

/// The column read uses the same owner boundary as UQL and rejects an unmapped
/// column before attempting the registered SQL connection.
#[tokio::test]
async fn foreign_column_read_is_owner_scoped_and_mapping_gated() {
    let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
    let state = multi_tenant_state().await;
    register_in(
        &state,
        940,
        OWNER_A,
        eg_types::wire::ForeignSourceSpec::Sql {
            dsn: "postgres://db.invalid/records".into(),
            query: "SELECT id, name FROM records".into(),
            id_field: "id".into(),
            score_field: None,
            columns: vec!["name".into()],
        },
    )
    .await;
    let query = || Method::QueryForeignColumns {
        name: "remote_docs".into(),
        columns: vec!["hidden".into()],
        predicates: vec![],
    };
    let owner = dispatch_as(&state, 941, OWNER_A, query()).await;
    assert!(
        owner
            .error
            .as_deref()
            .unwrap_or("")
            .starts_with("INVALID_ARGUMENT"),
        "mapped-column refusal must precede connection: {:?}",
        owner.error
    );
    let other = dispatch_as(&state, 942, OTHER_B, query()).await;
    assert!(other
        .error
        .as_deref()
        .unwrap_or("")
        .starts_with("INVALID_ARGUMENT"));
    let too_many = dispatch_as(
        &state,
        943,
        OWNER_A,
        Method::QueryForeignColumns {
            name: "remote_docs".into(),
            columns: vec!["name".into(); 129],
            predicates: vec![],
        },
    )
    .await;
    assert!(too_many
        .error
        .as_deref()
        .unwrap_or("")
        .starts_with("INVALID_ARGUMENT"));
    let numeric = dispatch_as(
        &state,
        944,
        OWNER_A,
        Method::QueryForeignColumns {
            name: "remote_docs".into(),
            columns: vec!["name".into()],
            predicates: vec![eg_types::wire::ForeignColumnPredicate {
                column: "name".into(),
                comparison: eg_types::wire::ForeignColumnComparison::Eq,
                value: serde_json::json!(42),
            }],
        },
    )
    .await;
    assert!(numeric
        .error
        .as_deref()
        .unwrap_or("")
        .starts_with("INVALID_ARGUMENT"));
}

/// A verified owner receives only projected columns after the local residual;
/// another owner cannot spend this HTTP source or read its mapped values.
#[tokio::test]
async fn foreign_column_http_read_returns_projected_filtered_rows() {
    use std::collections::BTreeMap;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    struct RestoreAllow(Option<std::ffi::OsString>);
    impl Drop for RestoreAllow {
        fn drop(&mut self) {
            let key = eg_plan::federation::HTTP_JSON_FEDERATION_ALLOW_ENV;
            if let Some(previous) = self.0.take() {
                std::env::set_var(key, previous);
            } else {
                std::env::remove_var(key);
            }
        }
    }

    let _env_lock = crate::crypto::acquire_test_env_lock().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let body = r#"{"data":[{"ref":"a","name":"Ada","age":19},{"ref":"b","name":"Bea","age":42},{"ref":"c","name":"Cam","age":55}]}"#;
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = [0; 1024];
        let _ = socket.read(&mut request).await.unwrap();
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        socket.write_all(response.as_bytes()).await.unwrap();
    });
    let key = eg_plan::federation::HTTP_JSON_FEDERATION_ALLOW_ENV;
    let _restore = RestoreAllow(std::env::var_os(key));
    std::env::set_var(key, &origin);

    let state = multi_tenant_state().await;
    register_in(
        &state,
        945,
        OWNER_A,
        eg_types::wire::ForeignSourceSpec::HttpJson {
            url: format!("{origin}/"),
            json_path: "data".into(),
            field_map: eg_types::wire::HttpFieldMap {
                id: "ref".into(),
                score: None,
                columns: BTreeMap::from([
                    ("name".into(), "name".into()),
                    ("age".into(), "age".into()),
                ]),
            },
        },
    )
    .await;
    let query = || Method::QueryForeignColumns {
        name: "remote_docs".into(),
        columns: vec!["name".into()],
        predicates: vec![eg_types::wire::ForeignColumnPredicate {
            column: "age".into(),
            comparison: eg_types::wire::ForeignColumnComparison::Ge,
            value: serde_json::json!(40),
        }],
    };
    let owner = dispatch_as(&state, 946, OWNER_A, query()).await;
    assert_ok(&owner);
    let Some(ResultPayload::Raw(raw)) = owner.result else {
        panic!("foreign column response must be typed MessagePack rows");
    };
    let rows: Vec<eg_types::wire::ForeignColumnRow> = rmp_serde::from_slice(&raw).unwrap();
    assert_eq!(
        rows.iter().map(|row| row.id.as_str()).collect::<Vec<_>>(),
        vec!["b", "c"]
    );
    assert_eq!(
        rows[0].columns,
        BTreeMap::from([("name".into(), serde_json::json!("Bea"))])
    );
    assert_eq!(
        rows[1].columns,
        BTreeMap::from([("name".into(), serde_json::json!("Cam"))])
    );
    let other = dispatch_as(&state, 947, OTHER_B, query()).await;
    assert!(other
        .error
        .as_deref()
        .unwrap_or("")
        .starts_with("INVALID_ARGUMENT"));
    tokio::time::timeout(std::time::Duration::from_secs(5), server)
        .await
        .expect("mock HTTP source was not called")
        .unwrap();
}

/// Both principals register the SAME name. Each resolves its OWN spec: A still reaches
/// the remote engine (B's registration did not overwrite A's), and B's query runs B's
/// own unreachable HTTP spec — it fails on the fetch, not on name resolution, and never
/// returns A's rows.
#[tokio::test]
async fn same_name_for_two_principals_resolves_each_owners_own_spec() {
    let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
    let (_remote, local) = registered_by_owner_a(910).await;
    let b_spec = eg_types::wire::ForeignSourceSpec::HttpJson {
        url: "http://127.0.0.1:9/eh373-principal-b".into(),
        json_path: "data".into(),
        field_map: eg_types::wire::HttpFieldMap {
            id: "id".into(),
            score: None,
            columns: Default::default(),
        },
    };
    register_in(&local, 911, OTHER_B, b_spec).await;

    let text = || uql(FOREIGN_UQL);
    let a = dispatch_as(&local, 912, OWNER_A, text()).await;
    assert_ok(&a);
    assert_eq!(
        sorted_uql_ids(&a),
        vec!["d2", "d3", "d4"],
        "principal B registering the same name must not overwrite principal A's source"
    );
    let b = dispatch_as(&local, 913, OTHER_B, text()).await;
    let err = b
        .error
        .expect("principal B's own spec points at an unreachable endpoint");
    assert!(
        !err.contains("no foreign source registered") && !err.contains("does not match"),
        "principal B must resolve its OWN registration, got: {err}"
    );
}

/// The NL handler binds the caller's owner-scoped registry too: the same NL-planned
/// `FOREIGN` query succeeds for the registering principal and is refused for another.
#[tokio::test]
async fn nl_query_foreign_leg_is_owner_scoped() {
    let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
    crate::server::set_nl_planner(Arc::new(EchoPlanner));
    let (_remote, local) = registered_by_owner_a(920).await;

    let nl = || Method::NlQuery {
        text: FOREIGN_UQL.into(),
        graph: "__commons__".into(),
    };
    let a = dispatch_as(&local, 921, OWNER_A, nl()).await;
    assert_ok(&a);
    assert_eq!(sorted_ids(&a), vec!["d2", "d3", "d4"]);
    let b = dispatch_as(&local, 922, OTHER_B, nl()).await;
    assert_not_registered_for_caller(&b, "NlQuery");
}

/// EH-378: principal B uses principal A's source only through the explicit,
/// engine-provisioned share role, by its qualified name; revoking the role stops use.
/// The granted success is result-cached, so the post-revocation refusal also proves the
/// cache key follows the grant.
#[tokio::test]
async fn shared_source_needs_an_explicit_grant() {
    let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
    let (_remote, local) = registered_by_owner_a(940).await;
    let qualified = crate::server::foreign_share::shared_name(OWNER_A, "remote_docs");
    let text = || {
        uql(format!(
            "MATCH (:Doc) |> FOREIGN \"{qualified}\" |> LIMIT 10"
        ))
    };
    let not_found =
        format!("no foreign source registered under name '{qualified}' (registered: [])");
    let assert_refused = |resp: &crate::protocol::Response, stage: &str| {
        let err = resp
            .error_detail
            .as_deref()
            .unwrap_or_else(|| panic!("{stage}: must be refused"));
        assert!(
            err.contains(&not_found),
            "{stage}: exact not-found, got: {err}"
        );
        assert!(!err.contains("ACCESS_DENIED") && !err.contains("does not match graph tenant"));
    };
    let set_other_roles = |roles: Vec<String>| {
        let local = local.clone();
        async move {
            let mut s = local.write().await;
            let mut identity = s.isolation.get_identity(OTHER_B).expect("worker2 exists");
            identity.roles = roles;
            s.isolation
                .try_register_agent(identity)
                .expect("update worker2 roles");
        }
    };
    let original = local
        .read()
        .await
        .isolation
        .get_identity(OTHER_B)
        .expect("worker2 exists")
        .roles;

    assert_refused(&dispatch_as(&local, 941, OTHER_B, text()).await, "no grant");

    let mut granted = original.clone();
    granted.push(crate::server::foreign_share::share_role(
        OWNER_A,
        "remote_docs",
    ));
    set_other_roles(granted).await;
    let used = dispatch_as(&local, 942, OTHER_B, text()).await;
    assert_ok(&used);
    assert_eq!(
        sorted_uql_ids(&used),
        vec!["d2", "d3", "d4"],
        "grantee uses the source"
    );

    set_other_roles(original).await;
    assert_refused(
        &dispatch_as(&local, 943, OTHER_B, text()).await,
        "after revoke",
    );
}

/// EH-378 namespace isolation: the RBAC model has no typed resource kinds, so the share
/// resource `foreign-source:<agent>/<name>` would collide with a graph of that exact
/// name (a grant on that graph would convey use of the source). `CreateGraph` must
/// refuse the reserved prefix with a typed error, before any durable commit, even for a
/// System caller.
#[tokio::test]
async fn a_graph_cannot_take_a_reserved_foreign_source_resource_name() {
    let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
    let local = multi_tenant_state().await;
    let reserved = crate::server::foreign_share::share_resource(OWNER_A, "remote_docs");
    let create = Method::CreateGraph {
        graph_name: reserved.clone(),
        graph_type: GraphType::Global,
    };
    let refused = dispatch_on_heap(&local, request(950, "__commons__", None, create)).await;
    let err = refused
        .error
        .expect("a reserved RBAC-resource name must not become a graph");
    assert!(err == "RESERVED_GRAPH_NAME", "typed refusal, got: {err}");
    assert!(
        !local.read().await.registry.exists(&reserved),
        "nothing was registered"
    );
}
