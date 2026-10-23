//! EG-FEDERATED-QUERY-R722.2: pack sources register held, serve only once approved,
//! and a class query federates every approved source through the source executor.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use eg_plan::federation::ForeignSource;
use eg_plan::pack_source::{
    federate_class, ClassFederationSource, PackSourceSet, RegisterOutcome, SourceExecutor,
};
use eg_types::connector_pack::{
    PackFieldBinding, PackPagination, PackSourceApproval, PackSourceKind, PackSourceRegistration,
    SourceCaller, SourceOutcomeStatus, SourceReadError, SourceReadErrorCode, SourceReadPage,
    SourceReadRequest, VirtualClassQuery,
};
use serde_json::{json, Value};

const SCHEMA_NAME: &str = "https://schema.org/name";
const STATE: &str = "https://knuckles-team.github.io/ontology#state";

fn registration(connector: &str, class_ns: &str, title_field: &str) -> PackSourceRegistration {
    PackSourceRegistration {
        connector: connector.into(),
        contract_iri: format!("contract://{connector}#issues"),
        mapping_iri: format!("vg://{connector}/Issue"),
        resource_uri: format!("data://{connector}/issues"),
        source_kind: PackSourceKind::McpTool,
        operation: format!("{connector}_issues"),
        class_iri: format!("{class_ns}#Issue"),
        key_field: "id".into(),
        fields: vec![
            PackFieldBinding {
                field: title_field.into(),
                property_iri: SCHEMA_NAME.into(),
            },
            PackFieldBinding {
                field: "state".into(),
                property_iri: STATE.into(),
            },
        ],
        pagination: PackPagination::Page,
        key_filter_pushdown: false,
        read_only: true,
        credential_ref: Some(format!("openbao://kv/{connector}")),
        contract_digest: "sha256:v1".into(),
        approval: PackSourceApproval::Held,
    }
}

fn caller() -> SourceCaller {
    SourceCaller {
        tenant: "homelab".into(),
        principal: "agent-a".into(),
    }
}

fn record(pairs: &[(&str, Value)]) -> BTreeMap<String, Value> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.clone()))
        .collect()
}

/// A fake executor standing in for arbora's governed fleet call. It serves
/// `github-mcp` in two pages and `gitlab-api` in one, and records every request.
#[derive(Default)]
struct FakeFleet {
    requests: Mutex<Vec<SourceReadRequest>>,
    fail: Mutex<BTreeMap<String, SourceReadErrorCode>>,
    slow: Mutex<Option<String>>,
    calls: AtomicUsize,
}

impl SourceExecutor for FakeFleet {
    fn read(&self, request: &SourceReadRequest) -> Result<SourceReadPage, SourceReadError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.requests.lock().unwrap().push(request.clone());
        if self.slow.lock().unwrap().as_deref() == Some(request.connector.as_str()) {
            std::thread::sleep(Duration::from_millis(500));
        }
        if let Some(code) = self.fail.lock().unwrap().get(&request.connector) {
            return Err(SourceReadError {
                code: *code,
                reason: format!("{} said no", request.connector),
            });
        }
        Ok(
            match (request.connector.as_str(), request.page_token.as_deref()) {
                ("github-mcp", None) => SourceReadPage {
                    records: vec![
                        record(&[
                            ("id", json!(1)),
                            ("title", json!("crash on start")),
                            ("state", json!("open")),
                        ]),
                        record(&[
                            ("id", json!(2)),
                            ("title", json!("typo")),
                            ("state", json!("closed")),
                        ]),
                    ],
                    next_page_token: Some("p2".into()),
                },
                ("github-mcp", Some("p2")) => SourceReadPage {
                    records: vec![
                        record(&[
                            ("id", json!(3)),
                            ("title", json!("slow query")),
                            ("state", json!("open")),
                        ]),
                        record(&[("title", json!("no key"))]),
                    ],
                    next_page_token: None,
                },
                ("gitlab-api", None) => SourceReadPage {
                    records: vec![record(&[
                        ("id", json!("17")),
                        ("name", json!("pipeline flake")),
                        ("state", json!("opened")),
                    ])],
                    next_page_token: None,
                },
                _ => SourceReadPage::default(),
            },
        )
    }
}

fn approved_set() -> PackSourceSet {
    let mut set = PackSourceSet::new();
    for reg in [
        registration(
            "github-mcp",
            "https://knuckles-team.github.io/ontology/github",
            "title",
        ),
        registration(
            "gitlab-api",
            "https://knuckles-team.github.io/ontology/gitlab",
            "name",
        ),
    ] {
        let mapping = reg.mapping_iri.clone();
        assert_eq!(set.register(reg).unwrap(), RegisterOutcome::Added);
        set.approve(&mapping, "sha256:v1").unwrap();
    }
    set
}

fn allow_all() -> impl Fn(&PackSourceRegistration) -> Result<(), String> + Send + Sync {
    |_| Ok(())
}

// spec: EG-FEDERATED-QUERY-R722.2
#[test]
fn held_serves_none_approved_serves_rows_and_changed_contract_returns_to_held() {
    let fleet = Arc::new(FakeFleet::default());
    let mut set = PackSourceSet::new();
    let github = registration(
        "github-mcp",
        "https://knuckles-team.github.io/ontology/github",
        "title",
    );
    assert_eq!(
        set.register(github.clone()).unwrap(),
        RegisterOutcome::Added
    );

    // Held: named in the answer, never read.
    let answer = federate_class(
        &set.for_class("Issue"),
        &VirtualClassQuery::new("Issue"),
        &caller(),
        Some(fleet.clone()),
        &allow_all(),
    );
    assert!(answer.rows.is_empty());
    assert_eq!(answer.sources[0].status, SourceOutcomeStatus::Held);
    assert!(answer.sources[0]
        .reason
        .as_deref()
        .unwrap()
        .contains("SOURCE_HELD"));
    assert!(answer.sources[0]
        .reason
        .as_deref()
        .unwrap()
        .contains("vg://github-mcp/Issue"));
    assert_eq!(
        fleet.calls.load(Ordering::SeqCst),
        0,
        "a held mapping is never read"
    );

    // An approval against another contract digest is refused.
    assert!(set.approve("vg://github-mcp/Issue", "sha256:old").is_err());
    set.approve("vg://github-mcp/Issue", "sha256:v1").unwrap();
    let answer = federate_class(
        &set.for_class("Issue"),
        &VirtualClassQuery::new("Issue"),
        &caller(),
        Some(fleet.clone()),
        &allow_all(),
    );
    assert_eq!(answer.rows.len(), 3);
    assert!(!answer.partial);

    // Re-import, same digest: nothing changes, the approval stands.
    assert_eq!(
        set.register(github.clone()).unwrap(),
        RegisterOutcome::Unchanged
    );
    assert_eq!(
        set.get("vg://github-mcp/Issue").unwrap().approval,
        PackSourceApproval::Approved
    );

    // Changed contract: replaced and back to held.
    let mut changed = github;
    changed.contract_digest = "sha256:v2".into();
    changed.approval = PackSourceApproval::Approved; // a caller cannot self-approve
    assert_eq!(set.register(changed).unwrap(), RegisterOutcome::Replaced);
    assert_eq!(
        set.get("vg://github-mcp/Issue").unwrap().approval,
        PackSourceApproval::Held
    );
}

// spec: EG-FEDERATED-QUERY-R722.7
#[test]
fn a_class_query_returns_rows_from_both_sources_with_provenance() {
    let fleet = Arc::new(FakeFleet::default());
    let set = approved_set();
    let answer = federate_class(
        &set.for_class("Issue"),
        &VirtualClassQuery::new("Issue"),
        &caller(),
        Some(fleet.clone()),
        &allow_all(),
    );
    assert!(!answer.partial, "{:?}", answer.sources);
    let ids: Vec<&str> = answer.rows.iter().map(|r| r.id.as_str()).collect();
    assert_eq!(
        ids,
        vec![
            "github-mcp/1",
            "github-mcp/2",
            "github-mcp/3",
            "gitlab-api/17"
        ]
    );
    // Two connectors' different source fields merge onto one class property.
    let names: Vec<&Value> = answer
        .rows
        .iter()
        .map(|r| &r.properties[SCHEMA_NAME])
        .collect();
    assert_eq!(names[0], &json!("crash on start"));
    assert_eq!(names[3], &json!("pipeline flake"));
    for row in &answer.rows {
        let connector = row.id.split('/').next().unwrap();
        assert_eq!(row.provenance.connector, connector);
        assert_eq!(
            row.provenance.contract_iri,
            format!("contract://{connector}#issues")
        );
        assert_eq!(
            row.provenance.mapping_iri,
            format!("vg://{connector}/Issue")
        );
        assert_eq!(row.provenance.contract_digest, "sha256:v1");
        assert!(row.provenance.fetched_at_ms > 0);
    }
    let github = answer
        .sources
        .iter()
        .find(|o| o.connector == "github-mcp")
        .unwrap();
    assert_eq!((github.pages, github.rows), (2, 3));
    assert!(github
        .reason
        .as_deref()
        .unwrap()
        .contains("1 records had no key"));

    // Every read carried the verified caller and the declared contract terms.
    let requests = fleet.requests.lock().unwrap();
    assert_eq!(requests.len(), 3);
    for request in requests.iter() {
        assert_eq!(request.caller, caller());
        assert_eq!(request.key_filter, None, "no key filter pushdown declared");
        assert_eq!(request.pagination, PackPagination::Page);
        assert!(request.deadline_ms > 0 && request.deadline_ms <= 10_000);
    }
}

// spec: EG-FEDERATED-QUERY-R722.7
#[test]
fn a_failed_source_is_a_named_partial_result_and_the_other_still_answers() {
    let fleet = Arc::new(FakeFleet::default());
    fleet
        .fail
        .lock()
        .unwrap()
        .insert("gitlab-api".into(), SourceReadErrorCode::Unavailable);
    let set = approved_set();
    let answer = federate_class(
        &set.for_class("Issue"),
        &VirtualClassQuery::new("Issue"),
        &caller(),
        Some(fleet.clone()),
        &allow_all(),
    );
    assert!(answer.partial);
    assert_eq!(answer.rows.len(), 3, "github rows still answer");
    let gitlab = answer
        .sources
        .iter()
        .find(|o| o.connector == "gitlab-api")
        .unwrap();
    assert_eq!(gitlab.status, SourceOutcomeStatus::Unavailable);
    assert_eq!(
        gitlab.reason.as_deref(),
        Some("SOURCE_UNAVAILABLE: gitlab-api said no")
    );

    // The executor refusing the caller's grant is a per-source NOT_GRANTED.
    fleet
        .fail
        .lock()
        .unwrap()
        .insert("gitlab-api".into(), SourceReadErrorCode::NotGranted);
    let answer = federate_class(
        &set.for_class("Issue"),
        &VirtualClassQuery::new("Issue"),
        &caller(),
        Some(fleet.clone()),
        &allow_all(),
    );
    let gitlab = answer
        .sources
        .iter()
        .find(|o| o.connector == "gitlab-api")
        .unwrap();
    assert_eq!(gitlab.status, SourceOutcomeStatus::NotGranted);
}

// spec: EG-FEDERATED-QUERY-R722.7
#[test]
fn engine_authority_timeouts_and_bounds_are_enforced_per_source() {
    let set = approved_set();

    // The engine-side authorizer leaves a source out before any call.
    let fleet = Arc::new(FakeFleet::default());
    let only_github = |r: &PackSourceRegistration| {
        if r.connector == "github-mcp" {
            Ok(())
        } else {
            Err(format!("caller holds no grant for {}", r.mapping_iri))
        }
    };
    let answer = federate_class(
        &set.for_class("Issue"),
        &VirtualClassQuery::new("Issue"),
        &caller(),
        Some(fleet.clone()),
        &only_github,
    );
    assert!(answer
        .rows
        .iter()
        .all(|r| r.provenance.connector == "github-mcp"));
    assert!(fleet
        .requests
        .lock()
        .unwrap()
        .iter()
        .all(|r| r.connector == "github-mcp"));
    let gitlab = answer
        .sources
        .iter()
        .find(|o| o.connector == "gitlab-api")
        .unwrap();
    assert_eq!(gitlab.status, SourceOutcomeStatus::NotGranted);

    // A slow source times out; the fast one still answers within the deadline.
    let fleet = Arc::new(FakeFleet::default());
    *fleet.slow.lock().unwrap() = Some("gitlab-api".into());
    let mut query = VirtualClassQuery::new("Issue");
    query.source_timeout_ms = 150;
    let started = std::time::Instant::now();
    let answer = federate_class(
        &set.for_class("Issue"),
        &query,
        &caller(),
        Some(fleet),
        &allow_all(),
    );
    assert!(
        started.elapsed() < Duration::from_millis(450),
        "the query waits for the deadline only"
    );
    let gitlab = answer
        .sources
        .iter()
        .find(|o| o.connector == "gitlab-api")
        .unwrap();
    assert_eq!(gitlab.status, SourceOutcomeStatus::Timeout);
    assert_eq!(answer.rows.len(), 3);

    // The page bound truncates and keeps what it read.
    let fleet = Arc::new(FakeFleet::default());
    let mut query = VirtualClassQuery::new("Issue");
    query.max_pages_per_source = 1;
    let answer = federate_class(
        &set.for_class("Issue"),
        &query,
        &caller(),
        Some(fleet),
        &allow_all(),
    );
    let github = answer
        .sources
        .iter()
        .find(|o| o.connector == "github-mcp")
        .unwrap();
    assert_eq!(github.status, SourceOutcomeStatus::Truncated);
    assert_eq!((github.pages, github.rows), (1, 2));

    // No executor bound: every approved source is unavailable, nothing panics.
    let answer = federate_class(
        &set.for_class("Issue"),
        &VirtualClassQuery::new("Issue"),
        &caller(),
        None,
        &allow_all(),
    );
    assert!(answer.rows.is_empty());
    assert!(answer
        .sources
        .iter()
        .all(|o| o.status == SourceOutcomeStatus::Unavailable));
}

// spec: EG-FEDERATED-QUERY-R722.2
#[test]
fn the_key_filter_is_pushed_only_when_declared_and_always_applied_locally() {
    let fleet = Arc::new(FakeFleet::default());
    let mut set = PackSourceSet::new();
    let mut github = registration(
        "github-mcp",
        "https://knuckles-team.github.io/ontology/github",
        "title",
    );
    github.key_filter_pushdown = true;
    set.register(github).unwrap();
    set.approve("vg://github-mcp/Issue", "sha256:v1").unwrap();
    let mut query = VirtualClassQuery::new("Issue");
    query.key = Some("2".into());
    let answer = federate_class(
        &set.for_class("Issue"),
        &query,
        &caller(),
        Some(fleet.clone()),
        &allow_all(),
    );
    // The fake ignores the pushed filter (a widened answer); the residual restores it.
    assert_eq!(answer.rows.len(), 1);
    assert_eq!(answer.rows[0].key, "2");
    assert!(fleet
        .requests
        .lock()
        .unwrap()
        .iter()
        .all(|r| r.key_filter.as_deref() == Some("2")));
}

// spec: EG-FEDERATED-QUERY-R722.7
#[test]
fn a_class_source_reads_through_the_foreign_registry_with_provenance_columns() {
    let fleet: Arc<dyn SourceExecutor> = Arc::new(FakeFleet::default());
    let set = approved_set();
    let source = ClassFederationSource {
        class: "Issue".into(),
        sources: set.for_class("Issue"),
        caller: caller(),
        executor: Some(fleet),
        authorize: Arc::new(|_| Ok(())),
    };
    let mut registry = eg_plan::federation::ForeignSourceRegistry::new();
    registry.register("vg:Issue", Arc::new(source));

    let ids: Vec<String> = registry
        .resolve("vg:Issue")
        .unwrap()
        .rows()
        .iter()
        .map(|r| r.id.clone())
        .collect();
    assert_eq!(
        ids,
        vec![
            "github-mcp/1",
            "github-mcp/2",
            "github-mcp/3",
            "gitlab-api/17"
        ]
    );

    let session = eg_plan::federation_opt::FederationSession::from_env();
    let columns: Vec<String> = [
        "name",
        "state",
        "_connector",
        "_mapping_iri",
        "_fetched_at_ms",
    ]
    .iter()
    .map(|c| c.to_string())
    .collect();
    let predicate = eg_plan::federation_opt::ColumnPredicate {
        column: "state".into(),
        comparison: eg_plan::federation_opt::Comparison::Ne,
        value: json!("closed"),
    };
    let rows = registry
        .query_columns("vg:Issue", &columns, &[predicate], &session)
        .unwrap();
    let got: Vec<(&str, &Value)> = rows
        .rows()
        .iter()
        .map(|r| (r.id.as_str(), &r.columns["_connector"]))
        .collect();
    assert_eq!(
        got,
        vec![
            ("github-mcp/1", &json!("github-mcp")),
            ("github-mcp/3", &json!("github-mcp")),
            ("gitlab-api/17", &json!("gitlab-api")),
        ]
    );
    assert_eq!(rows.rows()[2].columns["name"], json!("pipeline flake"));

    // Outcome rows name each source's status.
    let outcome_cols = vec!["_mapping_iri".to_string(), "_outcome".to_string()];
    let predicate = eg_plan::federation_opt::ColumnPredicate {
        column: "_outcome".into(),
        comparison: eg_plan::federation_opt::Comparison::Eq,
        value: json!("OK"),
    };
    let outcomes = registry
        .query_columns("vg:Issue", &outcome_cols, &[predicate], &session)
        .unwrap();
    assert_eq!(outcomes.rows().len(), 2);
    assert!(registry
        .query_columns("vg:Issue", &["password".to_string()], &[], &session)
        .is_err());
}
