//! Served acceptance of the statistical release (§11.2), end to end through
//! the handlers: publish a feature schema, decide without a head (abstain),
//! fit a head, refuse its publish without a passed receipt, evaluate it,
//! publish it with the receipt, and decide again (act). Plus the negative
//! fixtures that need the served path: exploration on a security question,
//! a foreign tenant, an idempotency conflict, graph candidates.

use std::sync::Arc;

use tokio::sync::RwLock;

use crate::protocol::ResultPayload;
use eg_types::agent_component::{
    AgentComponentKind, AgentComponentPublishRequest, ComponentDependency,
};
use eg_types::contract::BoundedVec;
use eg_types::decision::jobs::{
    DecisionJobOutput, DecisionJobState, EvalCandidate, LabelRegime, OpeEstimatorKind,
    OptimiserSpec, RecordWindow,
};
use eg_types::decision::statistical::body::encode_body;
use eg_types::decision::statistical::dataset::{
    ItemLabel, LabelSource, LabelledDataset, LabelledItem, LABELLED_DATASET_SCHEMA_VERSION,
};
use eg_types::decision::statistical::features::{
    FeatureKind, FeatureSchemaBody, FeatureSpec, MissingValue, FEATURE_SCHEMA_VERSION,
};
use eg_types::decision::statistical::{
    CandidateSource, DecideRequest, DecisionBatch, QuestionKind, QuestionSafety,
    StatisticalOutcome, StatisticalQuestion, TypedParam, TypedValue,
};
use eg_types::decision::{
    ColdStart, DecisionEvalOp, DecisionEvalRequest, DecisionFitOp, DecisionFitRequest,
    DecisionJobRecord, DecisionPolicyRef, ExplorationBudget, HeadKind, LibraryCandidateScope,
    QuantScaleTag, QuantisedValue, UnitRationalWire,
};

use crate::server::auth::VerifiedRequestContext;
use crate::server::persistence::agent_component::test_component_draft;
use crate::server::persistence::agent_fixtures::mutation_context;
use crate::server::persistence::agent_library::AgentLibraryStore;
use crate::server::state::ServerState;

const TENANT: &str = "tenant-shared";

struct Harness {
    _dir: tempfile::TempDir,
    state: Arc<RwLock<ServerState>>,
    store: Arc<AgentLibraryStore>,
    nonce: std::cell::Cell<u8>,
}

impl Harness {
    async fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let mut server = ServerState::new_for_test(
            "decide-stat-test-secret",
            crate::isolation::IsolationLayer::new(),
        );
        server.persist_dir = Some(dir.path().to_string_lossy().into_owned());
        let state = Arc::new(RwLock::new(server));
        let store = state.write().await.ensure_agent_library().unwrap();
        Self {
            _dir: dir,
            state,
            store,
            nonce: std::cell::Cell::new(1),
        }
    }

    fn commit(
        &self,
        draft: eg_types::agent_component::AgentComponentDraft,
        receipt: Option<String>,
    ) -> Result<ComponentDependency, String> {
        let nonce = self.nonce.get();
        self.nonce.set(nonce + 1);
        let (id, kind) = (draft.component_id.clone(), draft.kind);
        let request = AgentComponentPublishRequest {
            context: mutation_context(
                &self.store,
                TENANT,
                &format!("publish-{id}-{nonce}"),
                nonce,
                0,
                "agent-component:publish",
            ),
            component: draft,
            evaluation_receipt_digest: receipt,
        };
        let committed = self.store.publish_component(request)?;
        Ok(ComponentDependency {
            component_id: id,
            kind,
            definition_digest: committed.result.component.definition_digest,
        })
    }

    fn publish(
        &self,
        id: &str,
        kind: AgentComponentKind,
        summary: &str,
        body: Option<&impl serde::Serialize>,
        receipt: Option<String>,
    ) -> Result<ComponentDependency, String> {
        let mut draft = test_component_draft(TENANT, id);
        draft.kind = kind;
        draft.summary = summary.to_string();
        draft.classification = vec!["eg:capability/retrieval/web-search".to_string()];
        if let Some(body) = body {
            let encoded = encode_body(body).unwrap();
            draft.content_digest = encoded.content_digest;
            draft.attributes = encoded.attributes;
        }
        self.commit(draft, receipt)
    }

    /// Publish a policy the way the assembly lane serves it: canonical JSON in
    /// one attribute, `content_digest` = the policy digest.
    fn publish_policy(
        &self,
        id: &str,
        policy: &eg_types::decision::DecisionPolicy,
    ) -> ComponentDependency {
        let mut draft = test_component_draft(TENANT, id);
        draft.kind = AgentComponentKind::DecisionPolicy;
        draft.content_digest = eg_types::decision::digest::policy_digest(policy);
        draft.attributes.insert(
            eg_types::decision::policy::DECISION_POLICY_ATTRIBUTE.to_string(),
            serde_json::to_string(policy).unwrap(),
        );
        self.commit(draft, None).unwrap()
    }
}

fn verified() -> VerifiedRequestContext {
    VerifiedRequestContext::verified_for_test_in_tenant("decider", TENANT)
}

fn decode<T: serde::de::DeserializeOwned>(
    response: crate::protocol::Response,
) -> Result<T, String> {
    if let Some(error) = response.error {
        return Err(error);
    }
    match response.result {
        Some(ResultPayload::Raw(bytes)) => Ok(rmp_serde::from_slice(&bytes).unwrap()),
        other => panic!("unexpected payload {other:?}"),
    }
}

fn rational(n: u64, d: u64) -> UnitRationalWire {
    UnitRationalWire::new(n, d).unwrap()
}

fn schema() -> FeatureSchemaBody {
    FeatureSchemaBody {
        schema_version: FEATURE_SCHEMA_VERSION,
        features: BoundedVec::new(vec![
            FeatureSpec {
                name: "text".to_string(),
                kind: FeatureKind::TextBm25 {
                    key: "summary".to_string(),
                    param: "query".to_string(),
                },
                missing: MissingValue::Abstain,
            },
            FeatureSpec {
                name: "coverage".to_string(),
                kind: FeatureKind::CoverageFraction {
                    param: "needs".to_string(),
                },
                missing: MissingValue::Abstain,
            },
        ])
        .unwrap(),
    }
}

fn request(
    schema: &ComponentDependency,
    head: Option<ComponentDependency>,
    policy: DecisionPolicyRef,
    safety: QuestionSafety,
) -> DecideRequest {
    DecideRequest {
        tenant_id: TENANT.to_string(),
        question: StatisticalQuestion {
            question_id: "route.tools".to_string(),
            kind: QuestionKind::Route,
            safety,
        },
        candidates: CandidateSource::AgentLibrary {
            scope: LibraryCandidateScope {
                kinds: BoundedVec::new(vec![AgentComponentKind::Tool]).unwrap(),
                classification_under: None,
            },
        },
        feature_schema: schema.clone(),
        head,
        policy,
        params: BoundedVec::new(vec![
            TypedParam {
                name: "needs".to_string(),
                value: TypedValue::IriList(
                    BoundedVec::new(vec!["eg:capability/retrieval".to_string()]).unwrap(),
                ),
            },
            TypedParam {
                name: "query".to_string(),
                value: TypedValue::Text("web search engine".to_string()),
            },
        ])
        .unwrap(),
        max_records: None,
    }
}

async fn decide(h: &Harness, request: DecideRequest) -> Result<DecisionBatch, String> {
    decode(super::statistical::handle_decide(&h.state, 1, &verified(), request).await)
}

/// Gold items that are noisy copies of the live matrix, gold = the option the
/// query matches best (row 0 after sorting by id).
fn dataset(schema_digest: &str, ids: &[String], values: &[i64], n: usize) -> LabelledDataset {
    let items = (0..n)
        .map(|i| {
            let jitter = ((i % 7) as i64 - 3) << 24;
            LabelledItem {
                item_id: format!("gold-{i:04}"),
                recorded_at_ms: 1_000,
                class_key: "route".to_string(),
                candidate_ids: BoundedVec::new(ids.to_vec()).unwrap(),
                features: BoundedVec::new(values.iter().map(|v| v + jitter).collect()).unwrap(),
                label: ItemLabel::Gold {
                    acceptable: BoundedVec::new(vec![ids[0].clone()]).unwrap(),
                    source: LabelSource::SyntheticConstruction,
                },
                audit_inclusion: None,
            }
        })
        .collect();
    LabelledDataset {
        schema_version: LABELLED_DATASET_SCHEMA_VERSION,
        feature_schema_digest: schema_digest.to_string(),
        feature_names: BoundedVec::new(vec!["text".to_string(), "coverage".to_string()]).unwrap(),
        scale: QuantScaleTag::Q32,
        items: BoundedVec::new(items).unwrap(),
        synthetic: true,
    }
}

fn window() -> RecordWindow {
    RecordWindow {
        from_ms: 0,
        to_ms: u64::MAX,
    }
}

fn succeeded(job: &DecisionJobRecord) -> &DecisionJobOutput {
    match &job.state {
        DecisionJobState::Succeeded { output } => output.as_ref(),
        other => panic!("job did not succeed: {other:?}"),
    }
}

#[tokio::test]
async fn fit_evaluate_publish_and_decide_end_to_end() {
    let h = Harness::new().await;
    h.publish(
        "tool-a-search",
        AgentComponentKind::Tool,
        "web search engine for pages",
        None::<&()>,
        None,
    )
    .unwrap();
    h.publish(
        "tool-b-files",
        AgentComponentKind::Tool,
        "write files to disk",
        None::<&()>,
        None,
    )
    .unwrap();
    h.publish(
        "tool-c-mail",
        AgentComponentKind::Tool,
        "send an email message",
        None::<&()>,
        None,
    )
    .unwrap();
    let body = schema();
    let schema_pin = h
        .publish(
            "schema-route",
            AgentComponentKind::FeatureSchema,
            "route features",
            Some(&body),
            None,
        )
        .unwrap();
    let schema_digest = encode_body(&body).unwrap().content_digest;

    // No head under the default (deterministic-only) policy: abstain, with the
    // matrix recorded exactly and the record digest reproducible.
    let batch = decide(
        &h,
        request(
            &schema_pin,
            None,
            DecisionPolicyRef::Default,
            QuestionSafety::Ordinary,
        ),
    )
    .await
    .unwrap();
    let record = &batch.records.as_slice()[0];
    assert!(matches!(
        record.outcome,
        StatisticalOutcome::Abstained { .. }
    ));
    assert_eq!(
        eg_types::decision::digest::statistical_record_digest(record),
        record.record_digest
    );
    let eg_types::decision::statistical::FeatureMatrixRef::Inline {
        candidate_ids,
        values,
        ..
    } = &record.inputs.feature_matrix
    else {
        panic!("inline matrix")
    };
    let ids: Vec<String> = candidate_ids.iter().cloned().collect();
    assert_eq!(ids[0], "tool-a-search");

    // Fit on 400 gold items shaped like the live matrix.
    let data = dataset(&schema_digest, &ids, values.as_slice(), 400);
    let gold = super::stat_jobs::dataset_digest(&data).unwrap();
    let fit_request = DecisionFitRequest {
        tenant_id: TENANT.to_string(),
        idempotency_key: "fit-1".to_string(),
        head_kind: HeadKind::ListwiseLogistic,
        feature_schema: schema_pin.clone(),
        policy: DecisionPolicyRef::Default,
        label_regime: LabelRegime::FullLabel {
            gold_set_digest: gold.clone(),
        },
        window: window(),
        optimiser: OptimiserSpec {
            max_iterations: 60,
            tolerance: QuantisedValue {
                scale: QuantScaleTag::Q32,
                value: 1 << 12,
            },
            seed: 0,
        },
        dataset: data.clone(),
        approved_commit_principals: BoundedVec::default(),
    };
    let fit_op = || DecisionFitOp::Submit {
        request: Box::new(fit_request.clone()),
    };
    let job: DecisionJobRecord =
        decode(super::jobs::handle_decision_fit(&h.state, 2, &verified(), fit_op()).await).unwrap();
    let DecisionJobOutput::Fit {
        draft,
        draft_sha256,
        draft_length,
        ..
    } = succeeded(&job).clone()
    else {
        panic!("fit output")
    };
    let replay: DecisionJobRecord =
        decode(super::jobs::handle_decision_fit(&h.state, 3, &verified(), fit_op()).await).unwrap();
    assert_eq!(replay, job, "a retried submit is a replay");
    let mut conflicting = fit_request.clone();
    conflicting.optimiser.max_iterations = 10;
    let conflict = decode::<DecisionJobRecord>(
        super::jobs::handle_decision_fit(
            &h.state,
            4,
            &verified(),
            DecisionFitOp::Submit {
                request: Box::new(conflicting),
            },
        )
        .await,
    );
    assert!(conflict.unwrap_err().starts_with("IDEMPOTENCY_CONFLICT"));

    // Promotion needs a passed receipt naming exactly these bytes (EH-040).
    let refused = h.publish(
        "head-route",
        AgentComponentKind::DecisionHead,
        "route head",
        Some(draft.as_ref()),
        Some(format!("sha256:{}", "0".repeat(64))),
    );
    assert!(refused
        .unwrap_err()
        .starts_with("EVALUATION_RECEIPT_MISMATCH"));

    let eval = DecisionEvalRequest {
        tenant_id: TENANT.to_string(),
        idempotency_key: "eval-1".to_string(),
        candidate: EvalCandidate::DraftArtifact {
            sha256: draft_sha256,
            length: draft_length,
        },
        policy: DecisionPolicyRef::Default,
        estimators: BoundedVec::new(vec![OpeEstimatorKind::Ips]).unwrap(),
        gold_set_digest: Some(gold),
        window: window(),
        dataset: data,
        approved_commit_principals: BoundedVec::default(),
    };
    let job: DecisionJobRecord = decode(
        super::jobs::handle_decision_eval(
            &h.state,
            5,
            &verified(),
            DecisionEvalOp::Submit {
                request: Box::new(eval),
            },
        )
        .await,
    )
    .unwrap();
    let DecisionJobOutput::Eval { receipt } = succeeded(&job).clone() else {
        panic!("eval output")
    };
    let receipt = *receipt;
    assert!(
        receipt.passed,
        "gates: {:?}",
        receipt.failed_gates.as_slice()
    );
    let head_pin = h
        .publish(
            "head-route",
            AgentComponentKind::DecisionHead,
            "route head",
            Some(draft.as_ref()),
            Some(receipt.receipt_digest.clone()),
        )
        .unwrap();

    // With the calibrated head the engine acts, with the executed policy's
    // propensity (1: deterministic) and a risk statement.
    let batch = decide(
        &h,
        request(
            &schema_pin,
            Some(head_pin),
            DecisionPolicyRef::Default,
            QuestionSafety::Ordinary,
        ),
    )
    .await
    .unwrap();
    let record = &batch.records.as_slice()[0];
    let StatisticalOutcome::Acted {
        option_id,
        propensity,
        ..
    } = &record.outcome
    else {
        panic!("expected to act: {:?}", record.outcome)
    };
    assert_eq!(option_id, "tool-a-search");
    assert_eq!((propensity.numerator(), propensity.denominator()), (1, 1));
    assert!(record.calibration.is_some() && record.explanation.is_some() && record.audit.is_some());
    assert!(
        record.synthetic_evidence,
        "a head fitted on synthetic data says so"
    );
}

#[tokio::test]
async fn exploration_on_a_security_question_is_refused_by_the_served_path() {
    let h = Harness::new().await;
    h.publish(
        "tool-a",
        AgentComponentKind::Tool,
        "web search",
        None::<&()>,
        None,
    )
    .unwrap();
    let schema_pin = h
        .publish(
            "schema",
            AgentComponentKind::FeatureSchema,
            "features",
            Some(&schema()),
            None,
        )
        .unwrap();
    let mut policy = eg_types::decision::DecisionPolicy::engine_default();
    policy.cold_start = ColdStart::Explore {
        budget: ExplorationBudget {
            fraction: rational(1, 2),
            spend_at_risk_micros: 1,
            questions: BoundedVec::new(vec!["route.tools".to_string()]).unwrap(),
        },
    };
    let policy_pin = h.publish_policy("policy-explore", &policy);
    let pinned = DecisionPolicyRef::Pinned {
        component: policy_pin,
    };
    let error = decide(
        &h,
        request(&schema_pin, None, pinned.clone(), QuestionSafety::Security),
    )
    .await
    .unwrap_err();
    assert!(error.starts_with("EXPLORATION_FORBIDDEN"), "{error}");
    let batch = decide(
        &h,
        request(&schema_pin, None, pinned, QuestionSafety::Ordinary),
    )
    .await
    .unwrap();
    let exploration = batch.records.as_slice()[0]
        .inputs
        .exploration
        .as_ref()
        .expect("the draw is committed");
    assert!(
        exploration.seed_commitment.starts_with("sha256:") && exploration.revealed_seed.is_none()
    );
}

#[tokio::test]
async fn foreign_tenants_and_graph_candidates_are_refused() {
    let h = Harness::new().await;
    let schema_pin = h
        .publish(
            "schema",
            AgentComponentKind::FeatureSchema,
            "features",
            Some(&schema()),
            None,
        )
        .unwrap();
    let mut foreign = request(
        &schema_pin,
        None,
        DecisionPolicyRef::Default,
        QuestionSafety::Ordinary,
    );
    foreign.tenant_id = "tenant-other".to_string();
    assert!(decide(&h, foreign)
        .await
        .unwrap_err()
        .starts_with("ACCESS_DENIED"));
    let mut graph = request(
        &schema_pin,
        None,
        DecisionPolicyRef::Default,
        QuestionSafety::Ordinary,
    );
    graph.candidates = CandidateSource::Graph {
        graph: "kg".to_string(),
        plan: Box::new(eg_types::wire::Plan::new(Vec::new())),
    };
    assert!(decide(&h, graph)
        .await
        .unwrap_err()
        .starts_with("CANDIDATE_PLAN_REFUSED"));
    let mut wrong_pin = request(
        &schema_pin,
        None,
        DecisionPolicyRef::Default,
        QuestionSafety::Ordinary,
    );
    wrong_pin.feature_schema.definition_digest = format!("sha256:{}", "f".repeat(64));
    assert!(decide(&h, wrong_pin)
        .await
        .unwrap_err()
        .starts_with("COMPONENT_PIN_MISMATCH"));
}
