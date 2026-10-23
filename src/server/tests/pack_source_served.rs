//! EG-FEDERATED-QUERY-R722 through the served dispatch chain: pack sources register
//! into the one foreign catalog, the source executor is bound on the server, and a
//! served `Uql` and `QueryForeignColumns` over the class `Issue` return rows from
//! two connectors with per-row provenance and per-source outcomes. Nothing is
//! written to the store.

use std::collections::BTreeMap;
use std::sync::Mutex;

use eg_plan::pack_source::SourceExecutor;
use eg_types::connector_pack::{
    PackFieldBinding, PackPagination, PackSourceApproval, PackSourceKind, PackSourceRegistration,
    SourceCaller, SourceReadError, SourceReadErrorCode, SourceReadPage, SourceReadRequest,
};
use serde_json::json;

use super::*;

const CALLER: &str = "worker1";

/// Stands in for arbora's governed fleet call. `github-mcp` answers; `plane-mcp`
/// is down.
#[derive(Default)]
struct FleetExecutor {
    seen: Mutex<Vec<SourceCaller>>,
}

impl SourceExecutor for FleetExecutor {
    fn read(&self, request: &SourceReadRequest) -> Result<SourceReadPage, SourceReadError> {
        self.seen.lock().unwrap().push(request.caller.clone());
        let row = |id: &str, title: &str| -> BTreeMap<String, serde_json::Value> {
            BTreeMap::from([
                ("id".to_string(), json!(id)),
                ("title".to_string(), json!(title)),
            ])
        };
        match request.connector.as_str() {
            "github-mcp" => Ok(SourceReadPage {
                records: vec![row("1", "crash on start"), row("2", "typo")],
                next_page_token: None,
            }),
            "gitlab-api" => Ok(SourceReadPage {
                records: vec![row("17", "pipeline flake")],
                next_page_token: None,
            }),
            _ => Err(SourceReadError {
                code: SourceReadErrorCode::Unavailable,
                reason: "connector is not running".into(),
            }),
        }
    }
}

fn registration(connector: &str) -> PackSourceRegistration {
    PackSourceRegistration {
        connector: connector.into(),
        contract_iri: format!("contract://{connector}#issues"),
        mapping_iri: format!("vg://{connector}/Issue"),
        resource_uri: format!("data://{connector}/issues"),
        source_kind: PackSourceKind::McpTool,
        operation: format!("{connector}_issues"),
        class_iri: format!("https://knuckles-team.github.io/ontology/{connector}#Issue"),
        key_field: "id".into(),
        fields: vec![PackFieldBinding {
            field: "title".into(),
            property_iri: "https://schema.org/name".into(),
        }],
        pagination: PackPagination::Page,
        key_filter_pushdown: false,
        read_only: true,
        credential_ref: None,
        contract_digest: "sha256:v1".into(),
        approval: PackSourceApproval::Held,
    }
}

async fn wired_state(executor: Arc<FleetExecutor>) -> Arc<RwLock<ServerState>> {
    let state = multi_tenant_state().await;
    {
        let s = state.read().await;
        for connector in ["github-mcp", "gitlab-api", "plane-mcp"] {
            s.foreign_sources
                .register_pack_source(registration(connector))
                .expect("a read-only contract registers");
        }
        for connector in ["github-mcp", "gitlab-api"] {
            s.foreign_sources
                .approve_pack_source(&format!("vg://{connector}/Issue"), "sha256:v1")
                .expect("admission approves the mapping");
        }
        // plane-mcp is approved too, but its connector is down.
        s.foreign_sources
            .approve_pack_source("vg://plane-mcp/Issue", "sha256:v1")
            .unwrap();
        s.foreign_sources.install_source_executor(executor);
    }
    state
}

fn columns(names: &[&str]) -> Vec<String> {
    names.iter().map(|n| n.to_string()).collect()
}

// spec: EG-FEDERATED-QUERY-R722.7
#[tokio::test]
async fn served_uql_over_a_class_reads_both_sources_through_the_bound_executor() {
    let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
    let executor = Arc::new(FleetExecutor::default());
    let state = wired_state(executor.clone()).await;

    let resp = dispatch_as(
        &state,
        990,
        CALLER,
        Method::Uql {
            text: "FOREIGN SCAN 'vg:Issue' |> LIMIT 50".into(),
            params: Default::default(),
        },
    )
    .await;
    assert_ok(&resp);
    let mut ids = crate::server::decode_uql_ids(&resp);
    ids.sort();
    assert_eq!(ids, vec!["github-mcp/1", "github-mcp/2", "gitlab-api/17"]);

    // Every read ran for the verified caller, never an engine identity.
    let seen = executor.seen.lock().unwrap();
    assert!(!seen.is_empty());
    assert!(seen
        .iter()
        .all(|c| c.principal == CALLER && !c.tenant.is_empty()));
}

// spec: EG-FEDERATED-QUERY-R722.7
#[tokio::test]
async fn served_column_read_carries_provenance_and_names_the_failed_source() {
    let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
    let state = wired_state(Arc::new(FleetExecutor::default())).await;

    let resp = dispatch_as(
        &state,
        991,
        CALLER,
        Method::QueryForeignColumns {
            name: "vg:Issue".into(),
            columns: columns(&[
                "name",
                "_connector",
                "_contract_iri",
                "_mapping_iri",
                "_fetched_at_ms",
                "_outcome",
                "_outcome_reason",
            ]),
            predicates: vec![],
        },
    )
    .await;
    assert_ok(&resp);
    let Some(ResultPayload::Raw(raw)) = resp.result else {
        panic!("foreign column response must be typed MessagePack rows");
    };
    let rows: Vec<eg_types::wire::ForeignColumnRow> = rmp_serde::from_slice(&raw).unwrap();
    let instances: Vec<_> = rows
        .iter()
        .filter(|r| !r.id.starts_with("outcome:"))
        .collect();
    assert_eq!(instances.len(), 3);
    for row in &instances {
        let connector = row.id.split('/').next().unwrap();
        assert_eq!(row.columns["_connector"], json!(connector));
        assert_eq!(
            row.columns["_contract_iri"],
            json!(format!("contract://{connector}#issues"))
        );
        assert_eq!(
            row.columns["_mapping_iri"],
            json!(format!("vg://{connector}/Issue"))
        );
        assert!(row.columns["_fetched_at_ms"].as_u64().unwrap() > 0);
    }
    let outcome = |mapping: &str| {
        rows.iter()
            .find(|r| r.id == format!("outcome:{mapping}"))
            .unwrap_or_else(|| panic!("no outcome row for {mapping}"))
            .columns
            .clone()
    };
    assert_eq!(outcome("vg://github-mcp/Issue")["_outcome"], json!("OK"));
    let plane = outcome("vg://plane-mcp/Issue");
    assert_eq!(plane["_outcome"], json!("SOURCE_UNAVAILABLE"));
    assert_eq!(
        plane["_outcome_reason"],
        json!("SOURCE_UNAVAILABLE: connector is not running")
    );
}

// spec: EG-FEDERATED-QUERY-R722.7
#[tokio::test]
async fn served_read_without_a_bound_executor_or_with_a_held_mapping_names_the_gap() {
    let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
    let state = multi_tenant_state().await;
    {
        let s = state.read().await;
        s.foreign_sources
            .register_pack_source(registration("github-mcp"))
            .unwrap();
        let mut secret = registration("gitlab-api");
        secret.credential_ref = Some("glpat-literal".into());
        let refused = s.foreign_sources.register_pack_source(secret).unwrap_err();
        assert!(refused.starts_with("CONTRACT_SECRET:"), "{refused}");
    }
    let resp = dispatch_as(
        &state,
        992,
        CALLER,
        Method::QueryForeignColumns {
            name: "vg:Issue".into(),
            columns: columns(&["_mapping_iri", "_outcome", "_outcome_reason"]),
            predicates: vec![],
        },
    )
    .await;
    assert_ok(&resp);
    let Some(ResultPayload::Raw(raw)) = resp.result else {
        panic!("typed rows");
    };
    let rows: Vec<eg_types::wire::ForeignColumnRow> = rmp_serde::from_slice(&raw).unwrap();
    assert_eq!(rows.len(), 1, "a held mapping serves no instance rows");
    assert_eq!(rows[0].columns["_outcome"], json!("SOURCE_HELD"));
    assert!(rows[0].columns["_outcome_reason"]
        .as_str()
        .unwrap()
        .contains("vg://github-mcp/Issue"));

    // Approved but no executor bound: unavailable, never a direct connector call.
    {
        let s = state.read().await;
        s.foreign_sources
            .approve_pack_source("vg://github-mcp/Issue", "sha256:v1")
            .unwrap();
    }
    let resp = dispatch_as(
        &state,
        993,
        CALLER,
        Method::QueryForeignColumns {
            name: "vg:Issue".into(),
            columns: columns(&["_outcome"]),
            predicates: vec![],
        },
    )
    .await;
    let Some(ResultPayload::Raw(raw)) = resp.result else {
        panic!("typed rows");
    };
    let rows: Vec<eg_types::wire::ForeignColumnRow> = rmp_serde::from_slice(&raw).unwrap();
    assert_eq!(rows[0].columns["_outcome"], json!("SOURCE_UNAVAILABLE"));
}
