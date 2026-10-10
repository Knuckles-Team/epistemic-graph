//! Served acceptance of `Method::CapabilityCoverage`
//! (EG-DECISION-ENGINE-R126.2.1): closure, per-capability covering
//! components, premise classes, visibility filtering and the absence of
//! invisible components, driven through the dispatcher's handler.

use std::sync::Arc;

use tokio::sync::RwLock;

use crate::protocol::ResultPayload;
use crate::server::auth::VerifiedRequestContext;
use crate::server::persistence::agent_component::test_component_draft;
use crate::server::persistence::agent_fixtures::mutation_context;
use crate::server::persistence::agent_library::AgentLibraryStore;
use crate::server::state::ServerState;
use eg_types::agent_component::{AgentComponentKind, AgentComponentPublishRequest};
use eg_types::contract::BoundedVec;
use eg_types::decision::coverage::{
    CapabilityCoverageOutcome, CapabilityCoverageRequest, CapabilityCoverageResult,
};
use eg_types::decision::record::PremiseClass;
use eg_types::decision::{AssemblyRequirements, EvidenceClass, LibraryCandidateScope};

const TENANT: &str = "tenant-coverage";
const FOREIGN: &str = "tenant-foreign";

struct Harness {
    _dir: tempfile::TempDir,
    state: Arc<RwLock<ServerState>>,
    store: Arc<AgentLibraryStore>,
}

async fn harness() -> Harness {
    let dir = tempfile::tempdir().unwrap();
    let mut server = ServerState::new_for_test(
        "coverage-test-secret",
        crate::isolation::IsolationLayer::new(),
    );
    server.persist_dir = Some(dir.path().to_string_lossy().into_owned());
    let state = Arc::new(RwLock::new(server));
    let store = state.write().await.ensure_agent_library().unwrap();
    Harness {
        _dir: dir,
        state,
        store,
    }
}

fn publish(
    store: &AgentLibraryStore,
    tenant: &str,
    nonce: u8,
    (id, kind): (&str, AgentComponentKind),
    classification: &[&str],
    declared: &[&str],
) {
    let mut draft = test_component_draft(tenant, id);
    draft.kind = kind;
    draft.classification = classification.iter().map(|c| c.to_string()).collect();
    draft.declared_capabilities = declared.iter().map(|c| c.to_string()).collect();
    store
        .publish_component(AgentComponentPublishRequest {
            context: mutation_context(
                store,
                tenant,
                &format!("seed:{id}"),
                nonce,
                0,
                "agent-component:publish",
            ),
            evaluation_receipt_digest: None,
            component: draft,
        })
        .expect("publishes");
}

fn seed(store: &AgentLibraryStore) {
    let web = "eg:capability/retrieval/web-search";
    let plan = "eg:capability/reasoning/plan";
    publish(
        store,
        TENANT,
        1,
        ("skill-plan", AgentComponentKind::Skill),
        &[plan],
        &[],
    );
    publish(
        store,
        TENANT,
        2,
        ("skill-web", AgentComponentKind::Skill),
        &[web],
        &[],
    );
    publish(
        store,
        TENANT,
        3,
        ("card-retrieval", AgentComponentKind::A2aAgentCard),
        &[],
        &["eg:capability/retrieval"],
    );
    // Invisible to TENANT: another tenant's covering skill and card.
    publish(
        store,
        FOREIGN,
        4,
        ("foreign-web", AgentComponentKind::Skill),
        &[web],
        &[],
    );
    publish(
        store,
        FOREIGN,
        5,
        ("foreign-card", AgentComponentKind::A2aAgentCard),
        &[],
        &["eg:capability/retrieval"],
    );
}

fn request(tenant: &str, tasks: &[&str]) -> CapabilityCoverageRequest {
    CapabilityCoverageRequest {
        tenant_id: tenant.to_string(),
        requirements: AssemblyRequirements {
            tasks: BoundedVec::new(tasks.iter().map(|t| t.to_string()).collect()).unwrap(),
            ..AssemblyRequirements::default()
        },
        candidates: LibraryCandidateScope {
            kinds: BoundedVec::new(vec![AgentComponentKind::Skill]).unwrap(),
            classification_under: None,
        },
    }
}

async fn ask(
    h: &Harness,
    request: CapabilityCoverageRequest,
) -> Result<CapabilityCoverageResult, String> {
    let verified = VerifiedRequestContext::verified_for_test_in_tenant("asker", TENANT);
    let response = super::handle_capability_coverage(&h.state, 1, &verified, request).await;
    if let Some(error) = response.error {
        return Err(error);
    }
    match response.result {
        Some(ResultPayload::Raw(bytes)) => Ok(rmp_serde::from_slice(&bytes).unwrap()),
        other => panic!("unexpected payload {other:?}"),
    }
}

// spec: EG-DECISION-ENGINE-R126.2.1
#[tokio::test]
async fn capability_coverage_is_served_with_closure_premises_and_visibility() {
    let h = harness().await;
    seed(&h.store);
    let result = ask(&h, request(TENANT, &["eg:task/research"]))
        .await
        .unwrap();
    let CapabilityCoverageOutcome::Covered { capabilities } = result.coverage else {
        panic!("research closes over native capabilities");
    };
    // Closure: research needs retrieval, summarize and plan.
    let iris: Vec<&str> = capabilities
        .iter()
        .map(|c| c.capability_iri.as_str())
        .collect();
    for needed in [
        "eg:capability/retrieval",
        "eg:capability/analysis/summarize",
        "eg:capability/reasoning/plan",
    ] {
        assert!(
            iris.contains(&needed),
            "{needed} missing from closure {iris:?}"
        );
    }
    let by_iri = |iri: &str| {
        capabilities
            .iter()
            .find(|c| c.capability_iri == iri)
            .unwrap()
    };

    // Retrieval: the visible web-search skill by derivation, the visible card as a claim.
    let retrieval = by_iri("eg:capability/retrieval");
    let covering: Vec<_> = retrieval
        .library
        .iter()
        .map(|l| l.derivation.covered_by.clone().unwrap())
        .collect();
    assert_eq!(covering, vec!["skill-web".to_string()]);
    let entry = &retrieval.library[0];
    assert_eq!(
        entry.derivation.chain.as_slice()[0].class,
        PremiseClass::Claim
    );
    assert!(entry
        .derivation
        .chain
        .iter()
        .skip(1)
        .all(|e| e.class == PremiseClass::Definition));
    assert_eq!(entry.evidence_class, EvidenceClass::Claim);
    assert_eq!(retrieval.a2a_card_ids, vec!["card-retrieval".to_string()]);
    // The closure premise is the native task definition.
    assert_eq!(retrieval.because.class, PremiseClass::Definition);

    // Plan: only the plan skill; summarize: nothing covers it.
    let plan = by_iri("eg:capability/reasoning/plan");
    assert_eq!(plan.library.len(), 1);
    assert_eq!(
        plan.library[0].derivation.covered_by.as_deref(),
        Some("skill-plan")
    );
    assert!(plan.a2a_card_ids.is_empty());
    let summarize = by_iri("eg:capability/analysis/summarize");
    assert!(summarize.library.is_empty() && summarize.a2a_card_ids.is_empty());

    // Invisible components never appear anywhere.
    for coverage in &capabilities {
        for library in &coverage.library {
            assert_ne!(
                library.derivation.covered_by.as_deref(),
                Some("foreign-web")
            );
        }
        assert!(!coverage.a2a_card_ids.iter().any(|id| id == "foreign-card"));
    }
}

// spec: EG-DECISION-ENGINE-R126.2.1
#[tokio::test]
async fn capability_coverage_refuses_a_foreign_tenant_and_abstains_on_unknown_tasks() {
    let h = harness().await;
    seed(&h.store);
    let refused = ask(&h, request(FOREIGN, &["eg:task/research"]))
        .await
        .unwrap_err();
    assert!(refused.starts_with("ACCESS_DENIED"), "{refused}");
    let result = ask(&h, request(TENANT, &["eg:task/not-a-task"]))
        .await
        .unwrap();
    assert!(matches!(
        result.coverage,
        CapabilityCoverageOutcome::Abstained { ref reasons } if !reasons.is_empty()
    ));
}
