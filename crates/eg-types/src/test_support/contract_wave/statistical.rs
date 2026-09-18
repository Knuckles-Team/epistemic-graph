//! Valid sample values for the statistical decision surface.

use crate::agent_component::AgentComponentKind;
use crate::decision::{
    CalibrationMethod, CalibrationStatement, CandidateSource, DecideRequest, DecisionEvalOp,
    DecisionEvalRequest, DecisionFitOp, DecisionFitRequest, DecisionJobStatusRequest,
    DecisionPolicyRef, EvalCandidate, ExplorationRecord, FeatureMatrixRef, HeadKind, LabelRegime,
    LibraryCandidateScope, OpeEstimatorKind, OptimiserSpec, QuantScaleTag, QuantisedValue,
    QuestionKind, QuestionSafety, RecordWindow, RiskMethod, RiskStatement, ScoredOption,
    ShortlistProvenance, StatisticalOutcome, StatisticalQuestion, TypedParam, TypedValue,
};

use super::decision::{dependency, every_abstain_reason, unit_rational};
use super::{bounded, digest_text};

/// The question every statistical sample answers.
pub fn question() -> StatisticalQuestion {
    StatisticalQuestion {
        question_id: "route.ingestion".to_string(),
        kind: QuestionKind::Route,
        safety: QuestionSafety::Ordinary,
    }
}

/// Every typed parameter shape.
pub fn every_typed_value() -> Vec<TypedValue> {
    vec![
        TypedValue::Bool(true),
        TypedValue::Int(-7),
        TypedValue::Text("free text".to_string()),
        TypedValue::Iri("eg:capability/retrieval".to_string()),
        TypedValue::IriList(bounded(vec!["eg:modality/text".to_string()])),
        TypedValue::Rational(unit_rational(1, 4)),
    ]
}

/// One evaluate-only statistical question over library candidates.
pub fn decide_request() -> DecideRequest {
    DecideRequest {
        tenant_id: "tenant-a".to_string(),
        question: question(),
        candidates: CandidateSource::AgentLibrary {
            scope: LibraryCandidateScope {
                kinds: bounded(vec![AgentComponentKind::Tool]),
                classification_under: None,
            },
        },
        feature_schema: dependency("feature-schema-a", AgentComponentKind::FeatureSchema),
        head: Some(dependency("head-a", AgentComponentKind::DecisionHead)),
        policy: DecisionPolicyRef::Default,
        params: bounded(
            every_typed_value()
                .into_iter()
                .enumerate()
                .map(|(index, value)| TypedParam {
                    name: format!("param-{index}"),
                    value,
                })
                .collect(),
        ),
        max_records: Some(16),
    }
}

/// Both ways a feature matrix is pinned.
pub fn every_feature_matrix() -> Vec<FeatureMatrixRef> {
    vec![
        FeatureMatrixRef::Inline {
            candidate_ids: bounded(vec!["option-a".to_string(), "option-b".to_string()]),
            feature_names: bounded(vec!["recency".to_string()]),
            scale: QuantScaleTag::Pico,
            values: bounded(vec![1_000_000_000_000, -500_000_000_000]),
        },
        FeatureMatrixRef::Blob {
            sha256: digest_text(0xa1),
            length: 2_048,
            rows: 2,
            columns: 1,
        },
    ]
}

/// The shortlist provenance every statistical record carries.
pub fn shortlist() -> ShortlistProvenance {
    ShortlistProvenance {
        ann_recall_mode: Some("exact".to_string()),
        now_ms: 1_700_000_000_000,
        embedder_model_digest: Some(digest_text(0xa2)),
    }
}

/// A committed-then-revealed exploration draw.
pub fn exploration() -> ExplorationRecord {
    ExplorationRecord {
        seed_commitment: digest_text(0xa3),
        revealed_seed: Some(digest_text(0xa4)),
        budget_digest: digest_text(0xa5),
    }
}

/// Every conclusion a statistical decision can reach.
pub fn every_statistical_outcome() -> Vec<StatisticalOutcome> {
    vec![
        StatisticalOutcome::Acted {
            option_id: "option-a".to_string(),
            propensity: unit_rational(3, 4),
            prediction_set: bounded(vec!["option-a".to_string(), "option-b".to_string()]),
            risk: RiskStatement {
                method: RiskMethod::LearnThenTest,
                epsilon: unit_rational(1, 20),
                delta: unit_rational(1, 100),
                n_calibration: 4_096,
            },
        },
        StatisticalOutcome::Advisory {
            scores: bounded(vec![ScoredOption {
                option_id: "option-b".to_string(),
                score: QuantisedValue {
                    scale: QuantScaleTag::Q32,
                    value: 1_073_741_824,
                },
                probability: Some(unit_rational(1, 2)),
            }]),
            calibrated: false,
        },
        StatisticalOutcome::Abstained {
            reasons: bounded(every_abstain_reason()),
        },
    ]
}

/// Every calibration statement shape.
pub fn every_calibration() -> Vec<CalibrationStatement> {
    [
        CalibrationMethod::Temperature,
        CalibrationMethod::Vector,
        CalibrationMethod::Dirichlet,
        CalibrationMethod::Isotonic,
        CalibrationMethod::None,
    ]
    .into_iter()
    .map(|method| CalibrationStatement {
        method,
        alpha: Some(unit_rational(1, 10)),
        coverage_lower: Some(unit_rational(8, 10)),
        coverage_upper: Some(unit_rational(9, 10)),
        n_calibration: 1_024,
        synthetic: true,
    })
    .collect()
}

fn window() -> RecordWindow {
    RecordWindow {
        from_ms: 1_600_000_000_000,
        to_ms: 1_700_000_000_000,
    }
}

/// Both fit operations.
pub fn fit_ops() -> Vec<(&'static str, DecisionFitOp)> {
    vec![
        (
            "DecisionFit.submit",
            DecisionFitOp::Submit {
                request: DecisionFitRequest {
                    tenant_id: "tenant-a".to_string(),
                    idempotency_key: "fit-1".to_string(),
                    head_kind: HeadKind::ListwiseLogistic,
                    feature_schema: dependency(
                        "feature-schema-a",
                        AgentComponentKind::FeatureSchema,
                    ),
                    policy: DecisionPolicyRef::Default,
                    label_regime: LabelRegime::FullLabel {
                        gold_set_digest: digest_text(0xb1),
                    },
                    window: window(),
                    optimiser: OptimiserSpec {
                        max_iterations: 500,
                        tolerance: QuantisedValue {
                            scale: QuantScaleTag::Pico,
                            value: 1_000_000,
                        },
                        seed: 42,
                    },
                },
            },
        ),
        (
            "DecisionFit.status",
            DecisionFitOp::Status {
                request: DecisionJobStatusRequest {
                    tenant_id: "tenant-a".to_string(),
                    job_id: "fit-job-1".to_string(),
                },
            },
        ),
    ]
}

/// Both evaluation operations.
pub fn eval_ops() -> Vec<(&'static str, DecisionEvalOp)> {
    vec![
        (
            "DecisionEval.submit",
            DecisionEvalOp::Submit {
                request: DecisionEvalRequest {
                    tenant_id: "tenant-a".to_string(),
                    idempotency_key: "eval-1".to_string(),
                    candidate: EvalCandidate::DraftArtifact {
                        sha256: digest_text(0xb2),
                        length: 4_096,
                    },
                    policy: DecisionPolicyRef::Default,
                    estimators: bounded(vec![
                        OpeEstimatorKind::Ips,
                        OpeEstimatorKind::ClippedIps,
                        OpeEstimatorKind::Snips,
                        OpeEstimatorKind::Switch,
                        OpeEstimatorKind::DoublyRobust,
                    ]),
                    gold_set_digest: Some(digest_text(0xb3)),
                    window: window(),
                },
            },
        ),
        (
            "DecisionEval.status",
            DecisionEvalOp::Status {
                request: DecisionJobStatusRequest {
                    tenant_id: "tenant-a".to_string(),
                    job_id: "eval-job-1".to_string(),
                },
            },
        ),
    ]
}

/// The other evaluation candidate shape, so both arms are exercised.
pub fn published_head_candidate() -> EvalCandidate {
    EvalCandidate::PublishedHead {
        head: dependency("head-a", AgentComponentKind::DecisionHead),
    }
}
