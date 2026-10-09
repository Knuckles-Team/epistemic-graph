//! Served acceptance of the statistical release (§11.2), end to end through
//! the handlers: publish a feature schema, decide without a head (abstain),
//! fit a head, refuse its publish without a passed receipt, evaluate it,
//! publish it with the receipt, and decide again (abstain on synthetic
//! calibration). Plus the negative
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
    DatasetSource, DecisionJobOutput, DecisionJobState, EvalCandidate, LabelRegime,
    OpeEstimatorKind, OptimiserSpec, RecordWindow,
};
use eg_types::decision::statistical::body::encode_body;
use eg_types::decision::statistical::dataset::OutcomeFidelity;
use eg_types::decision::statistical::dataset::{
    ItemLabel, LabelSource, LabelledDataset, LabelledItem, LABELLED_DATASET_SCHEMA_VERSION,
};
use eg_types::decision::statistical::features::{
    FeatureKind, FeatureSchemaBody, FeatureSpec, MissingValue, FEATURE_SCHEMA_VERSION,
};
use eg_types::decision::statistical::log::{
    DecisionLogCommitted, DecisionLogEntry, DecisionLogOp, DecisionOutcomeEvaluation, EntryInputs,
    OutcomeAggregate, OutcomeAggregateRequest, StoredEvaluation,
};
use eg_types::decision::statistical::FeatureMatrixRef;
use eg_types::decision::statistical::StatisticalDecisionRecord;
use eg_types::decision::statistical::{
    CandidateSource, DecideRequest, DecisionBatch, QuestionKind, QuestionSafety,
    StatisticalOutcome, StatisticalQuestion, TypedParam, TypedValue,
};
use eg_types::decision::EvidenceClass;
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
        Self::with_isolation(crate::isolation::IsolationLayer::new()).await
    }

    async fn with_isolation(isolation: crate::isolation::IsolationLayer) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let mut server = ServerState::new_for_test("decide-stat-test-secret", isolation);
        server.persist_dir = Some(dir.path().to_string_lossy().into_owned());
        #[cfg(feature = "blob")]
        {
            let cas =
                crate::server::blob::store::RedbChunkStore::open(dir.path().to_str().unwrap())
                    .unwrap();
            server.blob = Some(Arc::new(crate::server::blob::BlobCursors::new(Arc::new(
                cas,
            ))));
        }
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

fn ordinary_exploration_policy() -> eg_types::decision::DecisionPolicy {
    let mut policy = eg_types::decision::DecisionPolicy::engine_default();
    policy.cold_start = ColdStart::Explore {
        budget: ExplorationBudget {
            fraction: rational(1, 1),
            spend_at_risk_micros: 1,
            questions: BoundedVec::new(vec!["route.tools".to_string()]).unwrap(),
        },
    };
    policy
}

fn fit_optimiser() -> OptimiserSpec {
    OptimiserSpec {
        max_iterations: 60,
        tolerance: QuantisedValue {
            scale: QuantScaleTag::Q32,
            value: 1 << 12,
        },
        seed: 0,
    }
}

fn schema() -> FeatureSchemaBody {
    FeatureSchemaBody {
        schema_version: FEATURE_SCHEMA_VERSION,
        features: BoundedVec::new(vec![
            eg_types::test_support::decision::summary_text_feature(),
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

fn number_feature(key: &str) -> FeatureSpec {
    FeatureSpec {
        name: key.to_string(),
        kind: FeatureKind::Number {
            key: key.to_string(),
        },
        missing: MissingValue::Abstain,
    }
}

fn publish_route_schema(h: &Harness) -> (ComponentDependency, String) {
    let body = schema();
    let pin = h
        .publish(
            "schema-route",
            AgentComponentKind::FeatureSchema,
            "route features",
            Some(&body),
            None,
        )
        .unwrap();
    let digest = encode_body(&body).unwrap().content_digest;
    (pin, digest)
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
        belief_as_of: BoundedVec::default(),
    }
}

async fn decide(h: &Harness, request: DecideRequest) -> Result<DecisionBatch, String> {
    decode(super::statistical::handle_decide(&h.state, 1, &verified(), request).await)
}

async fn decide_and_assert_abstains(
    h: &Harness,
    schema_pin: &ComponentDependency,
    head_pin: Option<ComponentDependency>,
) -> DecisionBatch {
    let batch = decide(
        h,
        request(
            schema_pin,
            head_pin,
            DecisionPolicyRef::Default,
            QuestionSafety::Ordinary,
        ),
    )
    .await
    .unwrap();
    assert!(matches!(
        batch.records.as_slice()[0].outcome,
        StatisticalOutcome::Abstained { .. }
    ));
    batch
}

async fn receipt_timeline_first_page(
    h: &Harness,
    req_id: u64,
) -> eg_types::decision::DecisionReceiptTimelinePage {
    decode(
        super::jobs::handle_decision_eval(
            &h.state,
            req_id,
            &verified(),
            DecisionEvalOp::Timeline {
                request: eg_types::decision::DecisionReceiptTimelineRequest {
                    tenant_id: TENANT.to_string(),
                    after: None,
                    limit: 1,
                },
            },
        )
        .await,
    )
    .unwrap()
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

/// A caller can mark its inline labels Human and non-synthetic, but the
/// server has no independent gold-set authority for those bytes. Even with
/// disjoint fit/eval items, this path cannot publish a threshold claim.
async fn inline_human_labels_cannot_publish_threshold(
    h: &Harness,
    schema_pin: &ComponentDependency,
    heldout: LabelledDataset,
) {
    let mut training = heldout.clone();
    training.items = BoundedVec::new(
        training
            .items
            .as_slice()
            .iter()
            .cloned()
            .map(|mut item| {
                item.item_id = format!("train-{}", item.item_id);
                item
            })
            .collect(),
    )
    .unwrap();
    assert!(training
        .items
        .iter()
        .zip(heldout.items.iter())
        .all(|(train, eval)| train.item_id != eval.item_id));
    let fit_gold = super::stat_jobs::dataset_digest(&training).unwrap();
    let fit: DecisionJobRecord = decode(
        super::jobs::handle_decision_fit(
            &h.state,
            57,
            &verified(),
            DecisionFitOp::Submit {
                request: Box::new(DecisionFitRequest {
                    tenant_id: TENANT.to_string(),
                    idempotency_key: "fit-real-calibration".to_string(),
                    head_kind: HeadKind::ListwiseLogistic,
                    feature_schema: schema_pin.clone(),
                    policy: DecisionPolicyRef::Default,
                    label_regime: LabelRegime::FullLabel {
                        gold_set_digest: fit_gold,
                    },
                    window: window(),
                    optimiser: fit_optimiser(),
                    source: DatasetSource::Inline {
                        dataset: Box::new(training),
                    },
                }),
            },
        )
        .await,
    )
    .unwrap();
    let DecisionJobOutput::Fit {
        draft_sha256,
        draft_length,
        draft,
        ..
    } = succeeded(&fit)
    else {
        panic!("inline fit output")
    };
    assert!(draft.synthetic);
    assert!(draft.calibration.as_ref().is_some_and(|cal| cal.synthetic));
    let heldout_gold = super::stat_jobs::dataset_digest(&heldout).unwrap();
    let evaluated: DecisionJobRecord = decode(
        super::jobs::handle_decision_eval(
            &h.state,
            58,
            &verified(),
            DecisionEvalOp::Submit {
                request: Box::new(DecisionEvalRequest {
                    tenant_id: TENANT.to_string(),
                    idempotency_key: "eval-real-heldout".to_string(),
                    candidate: EvalCandidate::DraftArtifact {
                        sha256: draft_sha256.clone(),
                        length: *draft_length,
                    },
                    policy: DecisionPolicyRef::Default,
                    estimators: BoundedVec::new(vec![OpeEstimatorKind::Ips]).unwrap(),
                    gold_set_digest: Some(heldout_gold),
                    window: window(),
                    source: DatasetSource::Inline {
                        dataset: Box::new(heldout),
                    },
                    mode: eg_types::decision::EvalMode::OffPolicy,
                }),
            },
        )
        .await,
    )
    .unwrap();
    let DecisionJobOutput::Eval { receipt } = succeeded(&evaluated) else {
        panic!("inline evaluation output")
    };
    assert!(receipt.synthetic);
    assert!(receipt.calibration.as_ref().is_some_and(|cal| {
        cal.synthetic && cal.method == eg_types::decision::CalibrationMethod::Conformal
    }));
    let timeline: eg_types::decision::DecisionReceiptTimelinePage = decode(
        super::jobs::handle_decision_eval(
            &h.state,
            59,
            &verified(),
            DecisionEvalOp::Timeline {
                request: eg_types::decision::DecisionReceiptTimelineRequest {
                    tenant_id: TENANT.to_string(),
                    after: None,
                    limit: 10,
                },
            },
        )
        .await,
    )
    .unwrap();
    assert!(timeline.entries.is_empty());
    assert_eq!(timeline.next_after, None);
    assert!(h
        .store
        .decision_artifact(
            TENANT,
            &crate::server::persistence::decision_jobs::receipt_threshold_key(
                &receipt.receipt_digest,
            ),
        )
        .unwrap()
        .is_none());
}

/// What the route fixture publishes and reads back.
struct RouteFixture {
    schema_pin: ComponentDependency,
    schema_digest: String,
    ids: Vec<String>,
    values: Vec<i64>,
}

/// Publish three tools and the route feature schema, then decide without a
/// head under the default (deterministic-only) policy: that abstains, records
/// the matrix exactly, and returns it for fixtures shaped like it.
async fn route_fixture(h: &Harness) -> RouteFixture {
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
    let (schema_pin, schema_digest) = publish_route_schema(h);

    // No head under the default (deterministic-only) policy: abstain, with the
    // matrix recorded exactly and the record digest reproducible.
    let batch = decide_and_assert_abstains(h, &schema_pin, None).await;
    let record = &batch.records.as_slice()[0];
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

    RouteFixture {
        schema_pin,
        schema_digest,
        ids,
        values: values.as_slice().to_vec(),
    }
}

#[tokio::test]
async fn fit_evaluate_publish_and_decide_end_to_end() {
    let h = Harness::new().await;
    let RouteFixture {
        schema_pin,
        schema_digest,
        ids,
        values,
    } = route_fixture(&h).await;
    // Fit on 400 gold items shaped like the live matrix.
    let data = dataset(&schema_digest, &ids, &values, 400);
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
        optimiser: fit_optimiser(),
        source: DatasetSource::Inline {
            dataset: Box::new(data.clone()),
        },
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
        source: DatasetSource::Inline {
            dataset: Box::new(data),
        },
        mode: eg_types::decision::EvalMode::OffPolicy,
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
    let read: Option<eg_types::decision::DecisionEvalReceipt> = decode(
        super::jobs::handle_decision_eval(
            &h.state,
            51,
            &verified(),
            DecisionEvalOp::Receipt {
                request: eg_types::decision::DecisionReceiptGetRequest {
                    tenant_id: TENANT.to_string(),
                    receipt_digest: receipt.receipt_digest.clone(),
                },
            },
        )
        .await,
    )
    .unwrap();
    assert_eq!(read.as_ref(), Some(&receipt));
    let page: eg_types::decision::DecisionReceiptPage = decode(
        super::jobs::handle_decision_eval(
            &h.state,
            52,
            &verified(),
            DecisionEvalOp::Receipts {
                request: eg_types::decision::DecisionReceiptListRequest {
                    tenant_id: TENANT.to_string(),
                    after: None,
                    limit: 1,
                },
            },
        )
        .await,
    )
    .unwrap();
    assert_eq!(page.receipts.as_slice(), std::slice::from_ref(&receipt));
    assert_eq!(page.next_after, None);
    let timeline = receipt_timeline_first_page(&h, 54).await;
    // This fixture labels by synthetic construction, so it must not enter
    // the real-world calibrated timeline even though it has metrics.
    assert!(receipt.synthetic);
    assert!(timeline.entries.is_empty());
    assert_eq!(timeline.next_after, None);
    // A caller-authored Human label and synthetic=false flag do not prove
    // independent gold-set provenance or unlock the real-world timeline.
    let mut human_data = dataset(&schema_digest, &ids, &values, 400);
    human_data.synthetic = false;
    human_data.items = BoundedVec::new(
        human_data
            .items
            .as_slice()
            .iter()
            .cloned()
            .map(|mut item| {
                if let ItemLabel::Gold { source, .. } = &mut item.label {
                    *source = LabelSource::Human;
                }
                item
            })
            .collect(),
    )
    .unwrap();
    let human_gold = super::stat_jobs::dataset_digest(&human_data).unwrap();
    let heldout_data = human_data.clone();
    let human_eval = DecisionEvalRequest {
        tenant_id: TENANT.to_string(),
        idempotency_key: "eval-human-1".to_string(),
        candidate: EvalCandidate::DraftArtifact {
            sha256: receipt.head_digest.clone(),
            length: draft_length,
        },
        policy: DecisionPolicyRef::Default,
        estimators: BoundedVec::new(vec![OpeEstimatorKind::Ips]).unwrap(),
        gold_set_digest: Some(human_gold),
        window: window(),
        source: DatasetSource::Inline {
            dataset: Box::new(human_data),
        },
        mode: eg_types::decision::EvalMode::OffPolicy,
    };
    let human_job: DecisionJobRecord = decode(
        super::jobs::handle_decision_eval(
            &h.state,
            55,
            &verified(),
            DecisionEvalOp::Submit {
                request: Box::new(human_eval),
            },
        )
        .await,
    )
    .unwrap();
    let DecisionJobOutput::Eval {
        receipt: human_receipt,
    } = succeeded(&human_job).clone()
    else {
        panic!("human eval output")
    };
    assert!(human_receipt.synthetic);
    assert!(human_receipt.metrics.is_some());
    let timeline = receipt_timeline_first_page(&h, 56).await;
    assert!(timeline.entries.is_empty());
    assert_eq!(timeline.next_after, None);
    // The fitted calibration and caller-authored evaluation labels both lack
    // independently verified provenance.
    assert!(human_receipt
        .calibration
        .as_ref()
        .is_some_and(|c| c.synthetic));
    assert!(super::stat_jobs::threshold_assessment(
        &human_receipt,
        &super::stat_support::default_statistical_policy(),
    )
    .is_none());
    assert!(h
        .store
        .decision_artifact(
            TENANT,
            &crate::server::persistence::decision_jobs::receipt_threshold_key(
                &human_receipt.receipt_digest,
            ),
        )
        .unwrap()
        .is_none());
    inline_human_labels_cannot_publish_threshold(&h, &schema_pin, heldout_data).await;
    let foreign = decode::<Option<eg_types::decision::DecisionEvalReceipt>>(
        super::jobs::handle_decision_eval(
            &h.state,
            53,
            &verified(),
            DecisionEvalOp::Receipt {
                request: eg_types::decision::DecisionReceiptGetRequest {
                    tenant_id: "foreign".into(),
                    receipt_digest: receipt.receipt_digest.clone(),
                },
            },
        )
        .await,
    );
    assert!(foreign.unwrap_err().starts_with("ACCESS_DENIED"));
    let head_pin = h
        .publish(
            "head-route",
            AgentComponentKind::DecisionHead,
            "route head",
            Some(draft.as_ref()),
            Some(receipt.receipt_digest.clone()),
        )
        .unwrap();

    // An inline-fitted head is synthetic even when its caller claims Human
    // labels. Its apparent calibration cannot authorize an ordinary Act.
    let batch = decide_and_assert_abstains(&h, &schema_pin, Some(head_pin.clone())).await;
    let record = &batch.records.as_slice()[0];
    assert!(record.calibration.is_none());
    assert!(record.audit.is_none());
    assert!(record.logging_propensities.is_empty());
    assert!(
        record.synthetic_evidence,
        "a head fitted on synthetic data says so"
    );

    // The log and retention tests still need an executed decision. Explicit
    // ordinary-question exploration supplies one without claiming risk-bound
    // Act from the synthetic calibration.
    let policy = ordinary_exploration_policy();
    let policy_pin = h.publish_policy("policy-inline-explore", &policy);
    let explored = decide(
        &h,
        request(
            &schema_pin,
            Some(head_pin),
            DecisionPolicyRef::Pinned {
                component: policy_pin,
            },
            QuestionSafety::Ordinary,
        ),
    )
    .await
    .unwrap();
    let record = &explored.records.as_slice()[0];
    assert!(matches!(
        record.outcome,
        StatisticalOutcome::Explored { .. }
    ));
    assert_eq!(record.logging_propensities.len(), 3);

    decision_log_round_trip(&h, record.clone()).await;
    #[cfg(feature = "blob")]
    retention_compacts_and_verifies(&h, record).await;
    // The compacted record still trains: its inputs are restored from CAS.
    fit_from_the_engine_log(&h, &schema_pin, record).await;
    #[cfg(feature = "blob")]
    retention_retires(&h, record).await;
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

/// EH-059: graph candidates are the rows a SELECT-only plan returns over the
/// caller's RLS-filtered snapshot; a ranking stage is refused and an
/// unregistered caller is refused by the graph ACL before any row is read.
#[cfg(feature = "query")]
#[tokio::test]
async fn graph_candidates_are_read_through_acl_rls_and_a_select_only_plan() {
    let h = Harness::with_isolation(ServerState::test_isolation("decider")).await;
    {
        let mut guard = h.state.write().await;
        guard
            .registry
            .create_graph("kg-decide", crate::protocol::GraphType::Team, None)
            .unwrap();
        let core = guard.registry.get("kg-decide").unwrap().core.clone();
        for (id, score, summary) in [
            ("n-a", 0.9, "web search engine"),
            ("n-b", 0.1, "file writer"),
        ] {
            let props = serde_json::json!({"type": "Tool", "score": score, "summary": summary});
            core.add_node(id.to_string(), rmp_serde::to_vec_named(&props).unwrap());
        }
    }
    let body = FeatureSchemaBody {
        schema_version: FEATURE_SCHEMA_VERSION,
        features: BoundedVec::new(vec![
            number_feature("score"),
            eg_types::test_support::decision::summary_text_feature(),
        ])
        .unwrap(),
    };
    let schema_pin = h
        .publish(
            "schema-graph",
            AgentComponentKind::FeatureSchema,
            "graph features",
            Some(&body),
            None,
        )
        .unwrap();
    let graph_request = |plan: eg_types::wire::Plan| {
        let mut request = request(
            &schema_pin,
            None,
            DecisionPolicyRef::Default,
            QuestionSafety::Ordinary,
        );
        request.candidates = CandidateSource::Graph {
            graph: "kg-decide".to_string(),
            plan: Box::new(plan),
        };
        request
    };
    let select = eg_types::wire::Plan::new(vec![eg_types::wire::Op::Scan {
        label: "Tool".to_string(),
    }]);
    let batch = decide(&h, graph_request(select.clone())).await.unwrap();
    let record = &batch.records.as_slice()[0];
    let FeatureMatrixRef::Inline {
        candidate_ids,
        values,
        ..
    } = &record.inputs.feature_matrix
    else {
        panic!("inline matrix")
    };
    assert_eq!(
        candidate_ids.as_slice(),
        ["n-a".to_string(), "n-b".to_string()]
    );
    assert_eq!(values.len(), 4, "two visible rows, two features");
    assert!(matches!(
        record.candidate_source,
        eg_types::decision::CandidateSourceRecord::Graph { .. }
    ));

    let ranked = eg_types::wire::Plan::new(vec![
        eg_types::wire::Op::Scan {
            label: "Tool".to_string(),
        },
        eg_types::wire::Op::RankText {
            query: "web".to_string(),
        },
    ]);
    let refused = decide(&h, graph_request(ranked)).await.unwrap_err();
    assert!(refused.starts_with("CANDIDATE_PLAN_REFUSED"), "{refused}");

    let stranger = VerifiedRequestContext::verified_for_test_in_tenant("stranger", TENANT);
    let denied = decode::<DecisionBatch>(
        super::statistical::handle_decide(&h.state, 3, &stranger, graph_request(select)).await,
    );
    assert!(denied.unwrap_err().starts_with("ACCESS_DENIED"));
}

mod log_tests;
use log_tests::{decision_log_round_trip, fit_from_the_engine_log, log_op};
#[cfg(feature = "blob")]
use log_tests::{retention_compacts_and_verifies, retention_retires};

mod classes_tests;

mod a2a_tests;

mod consumer_tests;

#[cfg(feature = "query")]
mod retrieval_tests;

#[cfg(feature = "query")]
mod learning_tests;

#[cfg(feature = "query")]
mod evaluator_tests;
// EH-523: the served per-component slate split.
mod attribution_tests;
// EH-525: the learned reputation relation and SOURCE RELIABILITY.
#[cfg(feature = "query")]
mod reputation_tests;

mod nl_tests;

mod scorer_tests;
// EH-528: replay evaluation (its validation kernels are finance's).
#[cfg(feature = "finance")]
mod replay_tests;
