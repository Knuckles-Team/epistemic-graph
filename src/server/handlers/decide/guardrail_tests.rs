//! EG-DECISION-ENGINE-R127.2.2: real dispatcher, persisted rules and typed results.

use std::sync::Arc;
use tokio::sync::RwLock;

use crate::protocol::{Method, Request, Response, ResultPayload};
use crate::server::auth::VerifiedRequestContext;
use crate::server::persistence::agent_component::test_component_draft;
use crate::server::persistence::agent_fixtures::mutation_context;
use crate::server::persistence::agent_library::AgentLibraryStore;
use crate::server::state::ServerState;
use eg_types::agent_component::{
    AgentComponentKind, AgentComponentPublishRequest, AgentComponentRetireRequest,
};
use eg_types::decision::guardrail::{
    EntailmentStep, GuardrailEntailmentRequest, GuardrailEntailmentResult, GuardrailRule,
};

const TENANT: &str = "tenant-guardrail";

fn state(path: &std::path::Path) -> Arc<RwLock<ServerState>> {
    let mut server =
        ServerState::new_for_test("guardrail-secret", ServerState::test_isolation("asker"));
    server.persist_dir = Some(path.to_string_lossy().into_owned());
    Arc::new(RwLock::new(server))
}

fn publish(store: &AgentLibraryStore, tenant: &str, id: &str, class: &str, nonce: u8) {
    let mut component = test_component_draft(tenant, id);
    component.kind = AgentComponentKind::GuardrailRule;
    component.attributes = GuardrailRule {
        rule_id: id.into(),
        applies_to_class: class.into(),
        source_generation: "generation-7".into(),
    }
    .to_attributes();
    store
        .publish_component(AgentComponentPublishRequest {
            context: mutation_context(store, tenant, id, nonce, 0, "agent-component:publish"),
            component,
            evaluation_receipt_digest: None,
        })
        .unwrap();
}

async fn ask(state: &super::SharedState, tenant: &str, task: &str) -> Response {
    let method = Method::GuardrailEntailment {
        request: Box::new(GuardrailEntailmentRequest {
            tenant_id: tenant.into(),
            task_iri: task.into(),
        }),
    };
    // Round-trip the wire method before entering authenticated dispatch.
    let method = rmp_serde::from_slice(&rmp_serde::to_vec_named(&method).unwrap()).unwrap();
    let request = Request {
        id: 17,
        graph: "__commons__".into(),
        auth_token: String::new(),
        agent_id: Some("asker".into()),
        method,
    };
    let verified = VerifiedRequestContext::verified_for_test_in_tenant("asker", TENANT);
    Box::pin(crate::server::dispatch::dispatch_verified_request(
        state, request, verified,
    ))
    .await
}

fn decode(response: Response) -> (Vec<u8>, GuardrailEntailmentResult) {
    assert!(response.error.is_none(), "{response:?}");
    let Some(ResultPayload::Raw(bytes)) = response.result else {
        panic!("expected typed raw result");
    };
    let result = rmp_serde::from_slice(&bytes).unwrap();
    (bytes, result)
}

// spec: EG-DECISION-ENGINE-R127.2.2
#[tokio::test]
async fn guardrail_dispatch_proves_subclass_visibility_and_restart_stability() {
    let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
    let dir = tempfile::tempdir().unwrap();
    let server = state(dir.path());
    let store = server.write().await.ensure_agent_library().unwrap();
    publish(&store, TENANT, "rule:parent", "eg:task", 1);
    publish(&store, TENANT, "rule:exact", "eg:task/research", 2);
    publish(&store, "foreign-tenant", "rule:foreign", "eg:task", 3);
    publish(&store, TENANT, "rule:retired", "eg:task", 4);
    store
        .retire_component(AgentComponentRetireRequest {
            context: mutation_context(&store, TENANT, "retire", 5, 1, "agent-component:retire"),
            component_id: "rule:retired".into(),
        })
        .unwrap();
    let (before, result) = decode(ask(&server, TENANT, "eg:task/research").await);
    assert_eq!(result.schema_version, 1);
    assert_eq!(
        result.ontology_digest,
        eg_types::agent_ontology::ontology_digest()
    );
    assert_eq!(result.task_iri, "eg:task/research");
    assert_eq!(
        result
            .rules
            .iter()
            .map(|r| r.rule_id.as_str())
            .collect::<Vec<_>>(),
        vec!["rule:exact", "rule:parent"]
    );
    assert!(result.rules[0].entailment_path.is_empty());
    assert_eq!(
        result.rules[1].entailment_path,
        vec![EntailmentStep {
            narrower: "eg:task/research".into(),
            broader: "eg:task".into()
        }]
    );
    assert!(result
        .rules
        .iter()
        .all(|r| r.source_generation == "generation-7"));
    assert!(decode(ask(&server, TENANT, "eg:capability").await)
        .1
        .rules
        .is_empty());
    drop(store);
    drop(server);
    let reopened = state(dir.path());
    assert_eq!(
        before,
        decode(ask(&reopened, TENANT, "eg:task/research").await).0
    );
    assert!(decode(ask(&reopened, TENANT, "eg:capability").await)
        .1
        .rules
        .is_empty());
}

// spec: EG-DECISION-ENGINE-R127.2.2
#[tokio::test]
async fn guardrail_dispatch_refuses_foreign_tenants_and_malformed_tasks() {
    let _env_read_lock = crate::crypto::acquire_test_env_read_lock().await;
    let dir = tempfile::tempdir().unwrap();
    let server = state(dir.path());
    let foreign = ask(&server, "foreign-tenant", "eg:task/research").await;
    assert_eq!(foreign.error.as_deref(), Some("ACCESS_DENIED"));
    for task in ["", " ", "research", "eg:task research", ":task", "eg:"] {
        let response = ask(&server, TENANT, task).await;
        assert_eq!(
            response.error.as_deref(),
            Some("INVALID_ARGUMENT"),
            "{task}: {response:?}"
        );
    }
    assert!(
        !dir.path().join("agent_library.redb").exists(),
        "refusals must precede opening the owner store"
    );
}

// spec: EG-DECISION-ENGINE-R127.2.2
#[test]
fn guardrail_policy_is_read_only_and_declares_store_refusals() {
    let method = Method::GuardrailEntailment {
        request: Box::new(GuardrailEntailmentRequest {
            tenant_id: TENANT.into(),
            task_iri: "eg:task".into(),
        }),
    };
    let policy = eg_capabilities::policy(&method);
    assert!(!policy.mutates);
    assert_eq!(policy.authz_action, "agent:assemble-read");
    assert_eq!(
        policy.txn_participation,
        eg_capabilities::TxnParticipation::Snapshot
    );
    for code in ["MALFORMED_GUARDRAIL_RULE", "GUARDRAIL_SCOPE_TOO_LARGE"] {
        assert!(eg_capabilities::error_routing::method_allows_error(
            "GuardrailEntailment",
            code
        ));
    }
}
