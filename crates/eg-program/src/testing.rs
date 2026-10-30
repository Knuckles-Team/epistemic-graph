//! Request fixtures shared by the crate tests and the served-submit tests
//! (feature `test-support`).

use crate::*;
use eg_modality::{
    ArtifactId, Classification, DerivationId, EvidenceAddress, EvidenceLocus, EvidenceLocusId,
    OpaqueRef, PolicyEnvelope, PrivacyAttestation, ResourceId,
};
use sha2::{Digest, Sha256};

pub fn token(label: &str) -> String {
    hex::encode(Sha256::digest(label.as_bytes()))
}

pub fn opaque(namespace: &str, label: &str) -> OpaqueRef {
    OpaqueRef::scoped(namespace, &token(label)).expect("test opaque reference")
}

pub fn policy(label: &str) -> PolicyEnvelope {
    PolicyEnvelope {
        tenant_ref: opaque("tenant", &format!("{label}:tenant")),
        access_policy_ref: opaque("policy", &format!("{label}:access")),
        classification: Classification::Internal,
        retention_policy_ref: opaque("retention", &format!("{label}:retention")),
        deletion_policy_ref: opaque("deletion", &format!("{label}:deletion")),
        legal_hold_ref: None,
        purpose_refs: vec![opaque("purpose", &format!("{label}:purpose"))],
    }
}

fn evidence_address(modality: ProgramModality, index: usize) -> EvidenceAddress {
    match modality {
        ProgramModality::Text | ProgramModality::Document => {
            EvidenceAddress::CharacterRange { start: 0, end: 8 }
        }
        ProgramModality::Image => EvidenceAddress::ImageRegion {
            x: 0.0,
            y: 0.0,
            width: 4.0,
            height: 4.0,
        },
        ProgramModality::Audio => EvidenceAddress::AudioRange {
            start_ms: 0,
            end_ms: 10,
        },
        ProgramModality::TimeSeries => EvidenceAddress::MetricWindow {
            start_ms: 0,
            end_ms: 10,
        },
        ProgramModality::Video => EvidenceAddress::FrameRange {
            start_frame: 0,
            end_frame: 2,
        },
        ProgramModality::Graph | ProgramModality::Vector | ProgramModality::Binary => {
            EvidenceAddress::RowVersion {
                row_ref: opaque("row", &format!("row:{index}")),
                version: 1,
            }
        }
        ProgramModality::Table | ProgramModality::Tensor => EvidenceAddress::TableCellRange {
            row_start: 0,
            row_end: 0,
            col_start: 0,
            col_end: 1,
        },
        ProgramModality::Spatial => EvidenceAddress::Point { x: 1.0, y: 2.0 },
        ProgramModality::Code => EvidenceAddress::CodeSymbol {
            revision_ref: opaque("revision", &format!("revision:{index}")),
            symbol_ref: opaque("symbol", &format!("symbol:{index}")),
            start_line: 1,
            end_line: 2,
        },
        ProgramModality::Trace => EvidenceAddress::TraceSpan {
            trace_ref: opaque("trace", &format!("trace:{index}")),
            span_ref: opaque("span", &format!("span:{index}")),
        },
    }
}

pub fn request(optimizer: OptimizerKind) -> OptimizationRequest {
    let policy = policy("original");
    let program_ref = opaque("program", "program");
    let program = ProgramRevision {
        schema_version: PROGRAM_SCHEMA_VERSION,
        program_ref: program_ref.clone(),
        revision: 1,
        parent_ref: None,
        signature: SignatureSpec {
            signature_ref: opaque("signature", "signature"),
            instruction_ref: opaque("instruction", "instruction"),
            fields: vec![
                FieldSpec {
                    name: "input".to_string(),
                    role: FieldRole::Input,
                    schema_ref: opaque("schema", "input-schema"),
                    description_ref: None,
                    required: true,
                },
                FieldSpec {
                    name: "output".to_string(),
                    role: FieldRole::Output,
                    schema_ref: opaque("schema", "output-schema"),
                    description_ref: None,
                    required: true,
                },
            ],
        },
        module: ModuleKind::Predict,
        adapter: AdapterKind::Chat,
        tool_refs: (optimizer == OptimizerKind::Avatar)
            .then(|| opaque("tool", "governed-tool"))
            .into_iter()
            .collect(),
        policy: policy.clone(),
    };
    let held_out_index = ProgramModality::ALL.len();
    let examples = ProgramModality::ALL
        .into_iter()
        .chain([ProgramModality::Text])
        .enumerate()
        .map(|(index, modality)| {
            let artifact =
                ArtifactId::from_token(&token(&format!("artifact:{index}"))).expect("artifact id");
            TrainingExample {
                example_ref: opaque("example", &format!("example:{index}")),
                input_refs: vec![opaque("content", &format!("input:{index}"))],
                expected_output_ref: Some(opaque("content", &format!("expected:{index}"))),
                observed_output_ref: Some(opaque("content", &format!("observed:{index}"))),
                feedback_ref: Some(opaque("feedback", &format!("feedback:{index}"))),
                trace_ref: Some(opaque("trace", &format!("example-trace:{index}"))),
                split: if index == held_out_index {
                    ExampleSplit::Validation
                } else {
                    ExampleSplit::Train
                },
                outcome: if optimizer == OptimizerKind::Avatar && index % 2 == 1 {
                    ExampleOutcome::Failure
                } else {
                    ExampleOutcome::Success
                },
                score: 0.7 + (index as f64 / 100.0),
                weight: 1.0,
                evidence: vec![EvidenceBinding {
                    modality,
                    locus: EvidenceLocus {
                        id: EvidenceLocusId::from_token(&token(&format!("locus:{index}")))
                            .expect("locus id"),
                        subject: ResourceId::Artifact(artifact),
                        address: evidence_address(modality, index),
                        policy_ref: policy.access_policy_ref.clone(),
                        derivation_ref: DerivationId::from_token(&token(&format!(
                            "derivation:{index}"
                        )))
                        .expect("derivation id"),
                    },
                    policy: policy.clone(),
                }],
            }
        })
        .collect();
    OptimizationRequest {
        schema_version: PROGRAM_SCHEMA_VERSION,
        request_ref: opaque("optimization", "request"),
        program,
        corpus: TrainingCorpus {
            corpus_ref: opaque("corpus", "corpus"),
            snapshot_version: 7,
            privacy: PrivacyAttestation {
                scanner_ref: opaque("scanner", "scanner"),
                policy_version_ref: opaque("privacy_policy", "privacy-policy"),
                raw_pii_persisted: false,
                local_identifiers_persisted: false,
            },
            examples,
        },
        optimizer,
        budget: OptimizationBudget {
            max_candidates: 8,
            max_demonstrations: ProgramModality::ALL.len(),
            max_model_calls: match optimizer.execution() {
                OptimizerExecution::ModelTransportPlan | OptimizerExecution::CompositePlan => 8,
                _ => 0,
            },
            max_evaluator_calls: 0,
            max_training_steps: match optimizer.execution() {
                OptimizerExecution::TrainerPlan | OptimizerExecution::CompositePlan => 32,
                _ => 0,
            },
            seed: 42,
        },
        promotion: PromotionPolicy::default(),
        baseline: EvaluationSummary {
            subject_ref: program_ref,
            aggregate_score: 0.5,
            case_count: 100,
            score_source: ScoreSource::Verifier,
            modality_scores: ProgramModality::ALL
                .into_iter()
                .map(|modality| (modality, 0.5))
                .collect(),
            evidence_refs: vec![opaque("evaluation", "baseline-evidence")],
        },
        optimizer_artifacts: Vec::new(),
        candidate_evaluations: Vec::new(),
    }
}

