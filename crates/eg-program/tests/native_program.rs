use eg_modality::{
    ArtifactId, Classification, DerivationId, EvidenceAddress, EvidenceLocus, EvidenceLocusId,
    OpaqueRef, PolicyEnvelope, PrivacyAttestation, ResourceId,
};
use eg_program::{
    AdapterKind, EvaluationSummary, EvidenceBinding, ExampleOutcome, ExampleSplit, FieldRole,
    FieldSpec, ModuleKind, NativeCompiler, OptimizationBudget, OptimizationRequest,
    OptimizerArtifact, OptimizerArtifactKind, OptimizerExecution, OptimizerKind, PlanExecutor,
    PlanStepKind, ProgramModality, ProgramRevision, PromotionPolicy, SignatureSpec, TrainingCorpus,
    TrainingExample, PROGRAM_SCHEMA_VERSION,
};
use sha2::{Digest, Sha256};

fn token(label: &str) -> String {
    hex::encode(Sha256::digest(label.as_bytes()))
}

fn opaque(namespace: &str, label: &str) -> OpaqueRef {
    OpaqueRef::scoped(namespace, &token(label)).expect("test opaque reference")
}

fn policy(label: &str) -> PolicyEnvelope {
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

fn request(optimizer: OptimizerKind) -> OptimizationRequest {
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
    let examples = ProgramModality::ALL
        .into_iter()
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
                split: ExampleSplit::Train,
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

#[test]
fn all_modalities_compile_and_require_evidence_before_promotion() {
    let mut request = request(OptimizerKind::LabeledFewShot);
    let proposal = NativeCompiler::compile(&request).expect("compile proposal");
    assert!(!proposal.promoted);
    assert_eq!(proposal.candidates.len(), 1);
    assert_eq!(
        proposal.candidates[0].modalities,
        ProgramModality::ALL
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>()
    );

    request.candidate_evaluations.push(EvaluationSummary {
        subject_ref: proposal.candidates[0].candidate_ref.clone(),
        aggregate_score: 0.8,
        modality_scores: ProgramModality::ALL
            .into_iter()
            .map(|modality| (modality, 0.8))
            .collect(),
        evidence_refs: vec![opaque("evaluation", "candidate-evidence")],
    });
    let promoted = NativeCompiler::compile(&request).expect("compile evaluated candidate");
    assert!(promoted.promoted);
    assert_eq!(
        promoted.selected_candidate_ref,
        Some(proposal.candidates[0].candidate_ref.clone())
    );

    let encoded = rmp_serde::to_vec_named(&promoted).expect("encode result");
    let decoded: eg_program::OptimizationResult =
        rmp_serde::from_slice(&encoded).expect("decode result");
    assert_eq!(decoded, promoted);
}

#[test]
fn random_search_is_deterministic() {
    let mut request = request(OptimizerKind::BootstrapFewShotWithRandomSearch);
    request.budget.max_demonstrations = 6;
    let first = NativeCompiler::compile(&request).expect("first compile");
    let second = NativeCompiler::compile(&request).expect("second compile");
    assert_eq!(first, second);
    assert!(first.candidates.len() <= request.budget.max_candidates);
}

#[test]
fn optimizer_names_are_thirteen_distinct_current_spellings() {
    let names = OptimizerKind::ALL
        .into_iter()
        .map(OptimizerKind::as_str)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(names.len(), 13);
    assert!(names.contains("avatar"));
}

#[test]
fn every_optimizer_has_a_native_candidate_or_governed_execution_plan() {
    for optimizer in OptimizerKind::ALL {
        let result = NativeCompiler::compile(&request(optimizer)).expect("optimizer surface");
        assert!(!result.candidates.is_empty() || !result.plans.is_empty());
        let all_modalities = ProgramModality::ALL
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>();
        assert!(
            result
                .candidates
                .iter()
                .any(|candidate| candidate.modalities == all_modalities)
                || result
                    .plans
                    .iter()
                    .all(|plan| plan.modalities == all_modalities)
        );
    }
}

#[test]
fn model_plan_materializes_without_a_second_provider_client() {
    let mut request = request(OptimizerKind::MiproV2);
    let planned = NativeCompiler::compile(&request).expect("MIPRO plan");
    assert!(planned.candidates.is_empty());
    assert!(planned.plans[0]
        .executors()
        .contains(&PlanExecutor::ModelTransport));

    request.optimizer_artifacts.push(OptimizerArtifact {
        artifact_ref: opaque("instruction", "mipro-proposal"),
        kind: OptimizerArtifactKind::InstructionProposal,
        source_ref: request.program.program_ref.clone(),
        modalities: ProgramModality::ALL.into_iter().collect(),
        score: 0.8,
        evidence_refs: vec![opaque("evidence", "mipro-proposal")],
        access_policy_ref: request.program.policy.access_policy_ref.clone(),
    });
    let materialized = NativeCompiler::compile(&request).expect("MIPRO materialization");
    assert!(!materialized.candidates.is_empty());
    assert!(materialized.plans.is_empty());
}

#[test]
fn avatar_compares_tool_use_and_materializes_a_governed_tool_policy() {
    let mut request = request(OptimizerKind::Avatar);
    let planned = NativeCompiler::compile(&request).expect("Avatar comparator plan");
    assert!(planned.candidates.is_empty());
    assert_eq!(planned.plans.len(), 1);
    assert_eq!(
        planned.plans[0].step_kinds(),
        [PlanStepKind::CompareToolUse].into_iter().collect()
    );
    assert_eq!(
        planned.plans[0].executors(),
        [PlanExecutor::ModelTransport].into_iter().collect()
    );
    assert!(request
        .program
        .tool_refs
        .iter()
        .all(|reference| planned.plans[0].steps[0].input_refs.contains(reference)));

    let tool_policy_ref = opaque("tool_policy", "avatar-comparator-policy");
    request.optimizer_artifacts.push(OptimizerArtifact {
        artifact_ref: tool_policy_ref.clone(),
        kind: OptimizerArtifactKind::ToolPolicy,
        source_ref: request.corpus.corpus_ref.clone(),
        modalities: ProgramModality::ALL.into_iter().collect(),
        score: 0.9,
        evidence_refs: vec![opaque("evidence", "avatar-comparator-policy")],
        access_policy_ref: request.program.policy.access_policy_ref.clone(),
    });
    let materialized = NativeCompiler::compile(&request).expect("Avatar materialization");
    assert!(materialized.plans.is_empty());
    assert_eq!(materialized.candidates.len(), 1);
    assert_eq!(
        materialized.candidates[0].artifact_refs,
        vec![tool_policy_ref.clone()]
    );
    assert_eq!(materialized.candidates[0].instruction_ref, None);
    assert_eq!(
        materialized.candidates[0].tool_policy_ref,
        Some(tool_policy_ref)
    );
    assert_eq!(
        materialized.candidates[0].modalities,
        ProgramModality::ALL.into_iter().collect()
    );
}

#[test]
fn avatar_requires_tools_and_contrastive_trace_evidence() {
    let mut no_tools = request(OptimizerKind::Avatar);
    no_tools.program.tool_refs.clear();
    assert_eq!(
        NativeCompiler::compile(&no_tools),
        Err(eg_program::ProgramError::NoEligibleExamples)
    );

    let mut no_negative = request(OptimizerKind::Avatar);
    for example in &mut no_negative.corpus.examples {
        example.outcome = ExampleOutcome::Success;
    }
    assert_eq!(
        NativeCompiler::compile(&no_negative),
        Err(eg_program::ProgramError::NoEligibleExamples)
    );
}

#[test]
fn authority_rebind_replaces_every_caller_policy_scope() {
    let mut request = request(OptimizerKind::MiproV2);
    request.optimizer_artifacts.push(OptimizerArtifact {
        artifact_ref: opaque("instruction", "authority-proposal"),
        kind: OptimizerArtifactKind::InstructionProposal,
        source_ref: request.program.program_ref.clone(),
        modalities: ProgramModality::ALL.into_iter().collect(),
        score: 0.9,
        evidence_refs: vec![opaque("evidence", "authority-proposal")],
        access_policy_ref: request.program.policy.access_policy_ref.clone(),
    });
    let verified = policy("verified");
    let rebound = request
        .rebind_program_policy(verified.clone())
        .expect("authority rebind");
    assert_eq!(rebound.program.policy, verified);
    assert!(rebound.corpus.examples.iter().all(|example| example
        .evidence
        .iter()
        .all(|binding| binding.policy == rebound.program.policy
            && binding.locus.policy_ref == rebound.program.policy.access_policy_ref)));
    assert!(rebound
        .optimizer_artifacts
        .iter()
        .all(|artifact| artifact.access_policy_ref == rebound.program.policy.access_policy_ref));
}
