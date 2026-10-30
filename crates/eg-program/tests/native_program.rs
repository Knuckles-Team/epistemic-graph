use eg_modality::OpaqueRef;
use eg_program::{EvaluationSummary, ScoreSource, ExampleOutcome, ExampleSplit, GovernedContentRef, ModelCallRequest, ModelCallResponse, ModelFinishReason, ModelTier, ModelUsage, NativeCompiler, OptimizerArtifact, OptimizerArtifactKind, OptimizerKind, PlanExecutor, PlanStepKind, ProgramModality, ProgramRevisionIdentity, PromotionPolicy};
use eg_program::testing::{opaque, policy, request, token};

fn msgpack_round_trip<T>(value: &T) -> T
where
    T: serde::Serialize + serde::de::DeserializeOwned + PartialEq + std::fmt::Debug,
{
    let bytes = rmp_serde::to_vec_named(value).expect("encode contract");
    rmp_serde::from_slice(&bytes).expect("decode contract")
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
        case_count: 100,
        score_source: ScoreSource::Verifier,
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
fn program_contracts_round_trip_and_optimizer_catalog_is_closed() {
    let request = request(OptimizerKind::LabeledFewShot);
    assert_eq!(
        msgpack_round_trip(&request.program.signature),
        request.program.signature
    );
    assert_eq!(msgpack_round_trip(&request.program), request.program);
    assert_eq!(msgpack_round_trip(&request.corpus), request.corpus);
    let example = request
        .corpus
        .examples
        .first()
        .expect("fixture has one example per modality");
    assert_eq!(msgpack_round_trip(example), *example);

    let names = OptimizerKind::ALL
        .into_iter()
        .map(OptimizerKind::as_str)
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(names.len(), 13);
    assert_eq!(
        names,
        [
            "avatar",
            "better_together",
            "bootstrap_few_shot",
            "bootstrap_few_shot_with_random_search",
            "bootstrap_finetune",
            "copro",
            "gepa",
            "infer_rules",
            "knn_few_shot",
            "labeled_few_shot",
            "mipro_v2",
            "simba",
            "ensemble",
        ]
        .into_iter()
        .collect()
    );
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

#[test]
fn promotion_identity_and_model_trace_bind_to_one_revision() {
    let mut request = request(OptimizerKind::LabeledFewShot);
    let proposal = NativeCompiler::compile(&request).expect("compile proposal");
    request.candidate_evaluations.push(EvaluationSummary {
        subject_ref: proposal.candidates[0].candidate_ref.clone(),
        aggregate_score: 0.8,
        case_count: 100,
        score_source: ScoreSource::Verifier,
        modality_scores: ProgramModality::ALL
            .into_iter()
            .map(|modality| (modality, 0.8))
            .collect(),
        evidence_refs: vec![opaque("evaluation", "promotion-evidence")],
    });
    let result = NativeCompiler::compile(&request).expect("compile promoted candidate");
    assert!(result.promoted);
    let candidate = result.selected_candidate().expect("selected candidate");
    let mut bound_candidate = candidate.clone();
    bound_candidate.content_digest = token("promotion-bound-candidate");
    bound_candidate.candidate_ref =
        OpaqueRef::scoped("program_candidate", &bound_candidate.content_digest)
            .expect("candidate ref");
    bound_candidate.tool_policy_ref = Some(opaque("tool_policy", "promotion-tool-policy"));
    bound_candidate.model_profile_ref = Some(opaque("model_profile", "promotion-model"));
    let identity =
        ProgramRevisionIdentity::from_candidate(&request.program, &bound_candidate, None)
            .expect("derive revision identity");
    assert_eq!(identity.base_revision, request.program.revision);
    assert_eq!(identity.revision, request.program.revision + 1);
    assert_eq!(identity.program_ref, request.program.program_ref);
    assert_eq!(identity.policy, request.program.policy);
    assert_eq!(identity.tool_policy_ref, bound_candidate.tool_policy_ref);
    assert_eq!(
        identity.model_profile_ref,
        bound_candidate.model_profile_ref
    );

    let mut next_program = request.program.clone();
    next_program.revision += 1;
    next_program.parent_ref = Some(identity.revision_ref.clone());
    let mut next_candidate = bound_candidate.clone();
    next_candidate.content_digest = token("promotion-bound-candidate-next");
    next_candidate.candidate_ref =
        OpaqueRef::scoped("program_candidate", &next_candidate.content_digest)
            .expect("next candidate ref");
    let next_identity = ProgramRevisionIdentity::from_candidate(
        &next_program,
        &next_candidate,
        Some(identity.revision_ref.clone()),
    )
    .expect("derive the second revision from the observed active pointer");
    assert_eq!(
        next_identity.parent_ref,
        Some(identity.revision_ref.clone())
    );
    assert_ne!(next_identity.revision_ref, identity.revision_ref);

    let model_request = ModelCallRequest {
        request_ref: opaque("model_request", "promotion-request"),
        program_ref: identity.program_ref.clone(),
        program_revision_ref: identity.revision_ref.clone(),
        model_profile_ref: identity
            .model_profile_ref
            .clone()
            .expect("bound model profile"),
        tool_policy_ref: identity.tool_policy_ref.clone(),
        tier: ModelTier::Balanced,
        inputs: vec![GovernedContentRef {
            content_ref: opaque("content", "promotion-input"),
            modality: ProgramModality::Text,
            access_policy_ref: request.program.policy.access_policy_ref.clone(),
            evidence_locus_refs: vec![opaque("locus", "promotion-input")],
        }],
        output_schema_refs: vec![opaque("schema", "promotion-output")],
        max_output_tokens: 128,
        trace_ref: opaque("trace", "promotion-trace"),
    };
    let response = ModelCallResponse {
        response_ref: opaque("model_response", "promotion-response"),
        output_refs: vec![opaque("content", "promotion-output")],
        trace_ref: model_request.trace_ref.clone(),
        program_revision_ref: identity.revision_ref.clone(),
        usage: ModelUsage {
            input_tokens: 4,
            output_tokens: 5,
            cached_input_tokens: 0,
        },
        finish_reason: ModelFinishReason::Completed,
    };
    model_request.validate().expect("valid model request");
    model_request
        .validate_for_revision(&identity)
        .expect("model request resolves the durable revision bindings");
    response
        .validate_for(&model_request)
        .expect("trace and revision binding");

    let mut wrong_revision = response.clone();
    wrong_revision.program_revision_ref = opaque("program_revision", "other-revision");
    assert!(wrong_revision.validate_for(&model_request).is_err());
    let mut wrong_trace = response;
    wrong_trace.trace_ref = opaque("trace", "other-trace");
    assert!(wrong_trace.validate_for(&model_request).is_err());

    let mut wrong_profile = model_request.clone();
    wrong_profile.model_profile_ref = opaque("model_profile", "other-model");
    assert!(wrong_profile.validate_for_revision(&identity).is_err());
}

#[test]
fn invalid_optimization_requests_refuse_with_their_stable_code() {
    let mut no_heldout = request(OptimizerKind::LabeledFewShot);
    no_heldout
        .corpus
        .examples
        .retain(|example| example.split == ExampleSplit::Train);
    let error = no_heldout.validate().expect_err("train-only corpus");
    assert_eq!(error.code(), "PROGRAM_HELDOUT_MISSING");

    let mut over_budget = request(OptimizerKind::LabeledFewShot);
    over_budget.budget.max_candidates = usize::MAX;
    let error = over_budget.validate().expect_err("budget over the ceiling");
    assert_eq!(error.code(), "PROGRAM_BUDGET_EXCEEDED");
    assert!(request(OptimizerKind::LabeledFewShot).validate().is_ok());
}

fn evaluation(score: f64, cases: u64, source: ScoreSource) -> EvaluationSummary {
    EvaluationSummary {
        subject_ref: opaque("candidate", "challenger"),
        aggregate_score: score,
        case_count: cases,
        score_source: source,
        modality_scores: Default::default(),
        evidence_refs: vec![opaque("evaluation", "challenger-evidence")],
    }
}

#[test]
fn a_challenger_scored_on_too_few_cases_is_refused_before_the_run() {
    let mut request = request(OptimizerKind::LabeledFewShot);
    let mut small = evaluation(0.9, 3, ScoreSource::Verifier);
    small.modality_scores = request.baseline.modality_scores.clone();
    request.candidate_evaluations.push(small);
    let error = request.validate().expect_err("too few cases");
    assert_eq!(error.code(), "PROGRAM_EVAL_TOO_SMALL");
}

#[test]
fn challenger_verdicts_follow_the_margin_and_the_score_source() {
    use eg_program::ChallengerVerdict::{Loss, ModelJudgeOnly, Tie, Win};
    let policy = PromotionPolicy::default();
    assert_eq!(policy.min_score_delta, 0.02);
    let verdict = |score, source| policy.judge(0.5, &evaluation(score, 100, source));
    assert_eq!(verdict(0.53, ScoreSource::Verifier), Ok(Win));
    assert_eq!(verdict(0.51, ScoreSource::Verifier), Ok(Tie));
    assert_eq!(verdict(0.5, ScoreSource::Verifier), Ok(Tie));
    assert_eq!(verdict(0.4, ScoreSource::Verifier), Ok(Loss));
    assert_eq!(verdict(0.9, ScoreSource::ModelJudge), Ok(ModelJudgeOnly));
    assert_eq!(
        policy.judge(0.5, &evaluation(0.9, 3, ScoreSource::Verifier)),
        Err(eg_program::ProgramError::EvalTooSmall)
    );
}
