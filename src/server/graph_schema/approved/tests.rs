//! EH-403 served acceptance: `GraphSchema.AttachApproved` through the real
//! signed dispatch, over a real redb authority holding the approval lease.

use std::sync::atomic::{AtomicU64, Ordering};

use serde::de::DeserializeOwned;
use serde_json::{json, Value};
use tokio::sync::RwLock;

use eg_types::control_lease::{
    ControlLeaseTarget, IssueControlLeaseRequest, TransitionControlLeaseRequest,
};
use eg_types::graph_schema::approval::{
    approved_candidate_digest, SCHEMA_APPROVAL_ACTION, SCHEMA_APPROVAL_LEASE_KIND,
};
use eg_types::graph_schema::{GraphSchemaCommitted, GraphSchemaSourcesView, SchemaSourceOriginView};

use super::*;
use crate::protocol::ResultPayload;
use crate::server::auth::dispatch_test_on_heap;
use crate::server::state::ServerState;

const SECRET: &str = "schema-approval-served-secret"; // sanitizer:ignore
const TENANT: &str = "tenant-shared";
const ADMIN: &str = "schema-approval-admin";
const GRAPH: &str = "drift-live";
const SOURCE: &str = "approved:container-manager-mcp";
const SHAPES: &str = "@prefix sh: <http://www.w3.org/ns/shacl#> .\n\
                      @prefix ex: <http://example/drift#> .\n\
                      ex:RecordShape a sh:NodeShape ; sh:targetClass ex:Record ; \
                      sh:property [ sh:path ex:name ; sh:datatype \
                      <http://www.w3.org/2001/XMLSchema#string> ] .\n";

struct Served {
    _dir: tempfile::TempDir,
    _env: crate::crypto::TestEnvReadGuard,
    state: Arc<RwLock<ServerState>>,
}

fn next_id() -> u64 {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

impl Served {
    async fn new() -> Self {
        let env = crate::crypto::acquire_test_env_read_lock().await;
        let dir = tempfile::tempdir().unwrap();
        let mut state = ServerState::new_for_test(SECRET, ServerState::test_isolation(ADMIN));
        state.persist_dir = Some(dir.path().to_string_lossy().into_owned());
        let backend = crate::server::persistence::redb_backend::RedbBackend::open(
            dir.path().join("graphs").to_string_lossy().into_owned(),
            64,
        )
        .expect("open the served graph backend");
        state.persistence = Some(Arc::new(backend));
        let served = Self {
            _dir: dir,
            _env: env,
            state: Arc::new(RwLock::new(state)),
        };
        let created = served
            .call(
                "__commons__",
                "create",
                Method::CreateGraph {
                    graph_name: GRAPH.to_string(),
                    graph_type: crate::protocol::GraphType::Global,
                },
            )
            .await;
        assert!(created.error.is_none(), "{:?}", created.error);
        served
    }

    async fn call(&self, graph: &str, key: &str, method: Method) -> Response {
        let id = next_id();
        let request = crate::server::auth::scoped_test_request_on(
            SECRET,
            id,
            graph,
            method,
            crate::server::auth::ScopedTestCaller {
                principal: ADMIN,
                tenant: TENANT,
                scopes: &["*"],
                nonce: &format!("schema-approval-nonce-{id}"),
                idempotency_key: key,
            },
        );
        dispatch_test_on_heap(&self.state, request).await
    }

    /// Issue an approval lease whose grant names `candidate`, and approve it
    /// (`active -> consumed`) unless `approve` is false.
    async fn approval(&self, lease_id: &str, candidate: &str, approve: bool) {
        let now = crate::server::txn::now_ms();
        let Value::Object(grant) = json!({
            "kind": SCHEMA_APPROVAL_ACTION,
            "target": SOURCE,
            "candidate_digest": candidate,
        }) else {
            unreachable!("the grant literal is an object")
        };
        let issue = IssueControlLeaseRequest {
            tenant: TENANT.to_string(),
            lease_id: lease_id.to_string(),
            kind: SCHEMA_APPROVAL_LEASE_KIND.to_string(),
            grant,
            issued_at_ms: now,
            expires_at_ms: now + 600_000,
            hard_expires_at_ms: now + 600_000,
            idempotency_key: format!("issue:{lease_id}"),
        };
        let method = Method::IssueControlLease { request: issue };
        let issued = self.call(GRAPH, &format!("issue:{lease_id}"), method).await;
        assert!(issued.error.is_none(), "{:?}", issued.error);
        if !approve {
            return;
        }
        let consume = TransitionControlLeaseRequest {
            tenant: TENANT.to_string(),
            lease_id: lease_id.to_string(),
            expected_revision: 1,
            to: ControlLeaseTarget::Consumed,
            idempotency_key: format!("approve:{lease_id}"),
        };
        let method = Method::TransitionControlLease { request: consume };
        let approved = self.call(GRAPH, &format!("approve:{lease_id}"), method).await;
        assert!(approved.error.is_none(), "{:?}", approved.error);
    }

    async fn attach_approved(&self, lease_id: &str) -> Response {
        let op = GraphSchemaOp::AttachApproved {
            source_id: SOURCE.to_string(),
            shapes_ttl: Some(SHAPES.to_string()),
            ontology_ttl: None,
            approval_lease_id: lease_id.to_string(),
            if_composed_digest: None,
        };
        let method = Method::GraphSchema { op: Box::new(op) };
        self.call(GRAPH, &format!("attach:{lease_id}"), method).await
    }
}

fn decode<T: DeserializeOwned>(response: Response) -> T {
    assert!(response.error.is_none(), "{:?}", response.error);
    match response.result.expect("a committed schema op answers") {
        ResultPayload::Raw(bytes) => rmp_serde::from_slice(&bytes).unwrap(),
        ResultPayload::Json(value) => serde_json::from_value(value).unwrap(),
        other => panic!("unexpected GraphSchema payload {other:?}"),
    }
}

fn refusal(response: &Response) -> &str {
    assert!(response.result.is_none(), "{:?}", response.result);
    response.error.as_deref().expect("the attach must be refused")
}

fn candidate() -> String {
    approved_candidate_digest(SOURCE, Some(SHAPES), None)
}

#[tokio::test]
async fn an_approved_candidate_attaches_under_its_approval_and_records_it() {
    let served = Served::new().await;
    served.approval("action_approval:ok", &candidate(), true).await;
    let committed: GraphSchemaCommitted =
        decode(served.attach_approved("action_approval:ok").await);
    assert!(committed.changed);

    let listed = served.call(GRAPH, "list", Method::GraphSchemaList).await;
    let listed: GraphSchemaSourcesView = decode(listed);
    let source = listed
        .dynamic_sources
        .iter()
        .find(|source| source.source_id == SOURCE)
        .expect("the approved source is attached");
    assert_eq!(
        source.origin,
        SchemaSourceOriginView::Approved {
            name: "container-manager-mcp".to_string(),
            approval_lease_id: "action_approval:ok".to_string(),
        }
    );
}

#[tokio::test]
async fn no_approval_a_pending_approval_or_another_candidates_approval_is_refused() {
    let served = Served::new().await;
    let missing = served.attach_approved("action_approval:absent").await;
    assert!(refusal(&missing).starts_with("SCHEMA_APPROVAL_REQUIRED"));

    served.approval("action_approval:pending", &candidate(), false).await;
    let pending = served.attach_approved("action_approval:pending").await;
    assert!(refusal(&pending).contains("still pending"));

    let other = approved_candidate_digest(SOURCE, Some("@prefix ex: <http://example/> ."), None);
    served.approval("action_approval:other", &other, true).await;
    let mismatched = served.attach_approved("action_approval:other").await;
    assert!(refusal(&mismatched).starts_with("SCHEMA_APPROVAL_MISMATCH"));

    let listed: GraphSchemaSourcesView =
        decode(served.call(GRAPH, "list-after", Method::GraphSchemaList).await);
    assert!(listed
        .dynamic_sources
        .iter()
        .all(|source| source.source_id != SOURCE));
}

#[tokio::test]
async fn a_generic_attach_cannot_write_the_approved_namespace() {
    let served = Served::new().await;
    let op = GraphSchemaOp::Attach {
        source_id: SOURCE.to_string(),
        shapes_ttl: Some(SHAPES.to_string()),
        ontology_ttl: None,
        if_composed_digest: None,
    };
    let response = served
        .call(GRAPH, "generic", Method::GraphSchema { op: Box::new(op) })
        .await;
    assert!(refusal(&response).contains("SCHEMA_SOURCE_RESERVED"));
}
