//! Deterministic native LM-program compilation and governed execution planning.
//!
//! Selection, composition, identity, and promotion are pure Rust kernels. Work
//! that needs inference, similarity search, evaluation, or model-weight training
//! is emitted as a reference-only plan for the engine's existing runtimes. This
//! crate never creates a second provider client or training authority.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};

use eg_modality::{OpaqueRef, PolicyEnvelope};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::{
    ExampleOutcome, ExampleSplit, OptimizationPlan, OptimizerArtifact, OptimizerArtifactKind,
    PlanExecutor, PlanStep, PlanStepKind, ProgramError, ProgramModality, ProgramRevision,
    TrainingCorpus, TrainingExample, MAX_OPTIMIZER_ARTIFACTS, PROGRAM_SCHEMA_VERSION,
};

pub const MAX_CANDIDATES: usize = 128;
pub const MAX_DEMONSTRATIONS: usize = 256;
pub const MAX_EVALUATIONS: usize = MAX_CANDIDATES;
pub const MAX_MODEL_CALLS: u64 = 100_000;
pub const MAX_EVALUATOR_CALLS: u64 = 1_000_000;
pub const MAX_TRAINING_STEPS: u64 = 10_000_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OptimizerKind {
    LabeledFewShot,
    BootstrapFewShot,
    BootstrapFewShotWithRandomSearch,
    Avatar,
    Copro,
    MiproV2,
    Simba,
    Gepa,
    BetterTogether,
    BootstrapFinetune,
    KnnFewShot,
    Ensemble,
    InferRules,
}

impl OptimizerKind {
    pub const ALL: [Self; 13] = [
        Self::LabeledFewShot,
        Self::BootstrapFewShot,
        Self::BootstrapFewShotWithRandomSearch,
        Self::Avatar,
        Self::Copro,
        Self::MiproV2,
        Self::Simba,
        Self::Gepa,
        Self::BetterTogether,
        Self::BootstrapFinetune,
        Self::KnnFewShot,
        Self::Ensemble,
        Self::InferRules,
    ];

    pub const fn execution(self) -> OptimizerExecution {
        match self {
            Self::LabeledFewShot
            | Self::BootstrapFewShot
            | Self::BootstrapFewShotWithRandomSearch
            | Self::Ensemble => OptimizerExecution::NativeKernel,
            Self::KnnFewShot => OptimizerExecution::GraphKernelPlan,
            Self::Avatar
            | Self::Copro
            | Self::MiproV2
            | Self::Simba
            | Self::Gepa
            | Self::InferRules => OptimizerExecution::ModelTransportPlan,
            Self::BootstrapFinetune => OptimizerExecution::TrainerPlan,
            Self::BetterTogether => OptimizerExecution::CompositePlan,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::LabeledFewShot => "labeled_few_shot",
            Self::BootstrapFewShot => "bootstrap_few_shot",
            Self::BootstrapFewShotWithRandomSearch => "bootstrap_few_shot_with_random_search",
            Self::Avatar => "avatar",
            Self::Copro => "copro",
            Self::MiproV2 => "mipro_v2",
            Self::Simba => "simba",
            Self::Gepa => "gepa",
            Self::BetterTogether => "better_together",
            Self::BootstrapFinetune => "bootstrap_finetune",
            Self::KnnFewShot => "knn_few_shot",
            Self::Ensemble => "ensemble",
            Self::InferRules => "infer_rules",
        }
    }

    pub const fn accepts_artifact(self, kind: OptimizerArtifactKind) -> bool {
        matches!(
            (self, kind),
            (
                Self::Copro | Self::MiproV2,
                OptimizerArtifactKind::InstructionProposal
            ) | (Self::Avatar, OptimizerArtifactKind::ToolPolicy)
                | (Self::Simba | Self::Gepa, OptimizerArtifactKind::Reflection)
                | (Self::InferRules, OptimizerArtifactKind::RuleSet)
                | (
                    Self::BootstrapFinetune,
                    OptimizerArtifactKind::FinetunedModel
                )
                | (
                    Self::BetterTogether,
                    OptimizerArtifactKind::InstructionProposal
                        | OptimizerArtifactKind::FinetunedModel
                )
                | (Self::KnnFewShot, OptimizerArtifactKind::NeighborScore)
                | (Self::Ensemble, OptimizerArtifactKind::EnsembleMember)
        )
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OptimizerExecution {
    NativeKernel,
    GraphKernelPlan,
    ModelTransportPlan,
    TrainerPlan,
    CompositePlan,
}

impl OptimizerExecution {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NativeKernel => "native_kernel",
            Self::GraphKernelPlan => "graph_kernel_plan",
            Self::ModelTransportPlan => "model_transport_plan",
            Self::TrainerPlan => "trainer_plan",
            Self::CompositePlan => "composite_plan",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OptimizationBudget {
    pub max_candidates: usize,
    pub max_demonstrations: usize,
    pub max_model_calls: u64,
    pub max_evaluator_calls: u64,
    #[serde(default)]
    pub max_training_steps: u64,
    pub seed: u64,
}

impl Default for OptimizationBudget {
    fn default() -> Self {
        Self {
            max_candidates: 16,
            max_demonstrations: 8,
            max_model_calls: 0,
            max_evaluator_calls: 0,
            max_training_steps: 0,
            seed: 0,
        }
    }
}

impl OptimizationBudget {
    fn validate(&self, optimizer: OptimizerKind) -> Result<(), ProgramError> {
        if !self.bounds_ok() {
            return Err(ProgramError::ResourceLimit);
        }
        if !self.executor_budget_matches(optimizer)
            || (optimizer == OptimizerKind::Ensemble && self.max_candidates < 3)
        {
            return Err(ProgramError::ResourceLimit);
        }
        Ok(())
    }

    /// Every count/limit field is within its `MAX_*` ceiling (and the two
    /// "must be at least 1" fields are non-zero).
    fn bounds_ok(&self) -> bool {
        !(self.max_candidates == 0
            || self.max_candidates > MAX_CANDIDATES
            || self.max_demonstrations == 0
            || self.max_demonstrations > MAX_DEMONSTRATIONS
            || self.max_model_calls > MAX_MODEL_CALLS
            || self.max_evaluator_calls > MAX_EVALUATOR_CALLS
            || self.max_training_steps > MAX_TRAINING_STEPS)
    }

    /// The `max_model_calls`/`max_training_steps` shape required by the optimizer's
    /// execution kind (a native/graph-kernel optimizer spends neither; a
    /// model-transport plan spends only model calls; a trainer plan spends only
    /// training steps; a composite plan spends both).
    fn executor_budget_matches(&self, optimizer: OptimizerKind) -> bool {
        match optimizer.execution() {
            OptimizerExecution::NativeKernel | OptimizerExecution::GraphKernelPlan => {
                self.max_model_calls == 0 && self.max_training_steps == 0
            }
            OptimizerExecution::ModelTransportPlan => {
                self.max_model_calls > 0 && self.max_training_steps == 0
            }
            OptimizerExecution::TrainerPlan => {
                self.max_model_calls == 0 && self.max_training_steps > 0
            }
            OptimizerExecution::CompositePlan => {
                self.max_model_calls > 0 && self.max_training_steps > 0
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PromotionPolicy {
    pub min_score_delta: f64,
    pub max_modality_regression: f64,
    pub require_all_observed_modalities: bool,
    pub min_evidence_refs: usize,
}

impl Default for PromotionPolicy {
    fn default() -> Self {
        Self {
            min_score_delta: 0.0,
            max_modality_regression: 0.0,
            require_all_observed_modalities: true,
            min_evidence_refs: 1,
        }
    }
}

impl PromotionPolicy {
    fn validate(&self) -> Result<(), ProgramError> {
        if !self.min_score_delta.is_finite()
            || !self.max_modality_regression.is_finite()
            || self.min_score_delta < 0.0
            || self.max_modality_regression < 0.0
            || self.max_modality_regression > 1.0
            || self.min_evidence_refs == 0
            || self.min_evidence_refs > crate::MAX_EVIDENCE_PER_EXAMPLE
        {
            return Err(ProgramError::InvalidPromotion);
        }
        Ok(())
    }
}

/// Externally observed evaluation results. Scores are admitted only with opaque
/// evidence references; outputs and judge rationales remain in governed CAS.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvaluationSummary {
    pub subject_ref: OpaqueRef,
    pub aggregate_score: f64,
    #[serde(default)]
    pub modality_scores: BTreeMap<ProgramModality, f64>,
    pub evidence_refs: Vec<OpaqueRef>,
}

impl EvaluationSummary {
    fn validate(&self) -> Result<(), ProgramError> {
        if !unit_score(self.aggregate_score)
            || self.evidence_refs.is_empty()
            || self.evidence_refs.len() > crate::MAX_EVIDENCE_PER_EXAMPLE
            || self.evidence_refs.iter().collect::<BTreeSet<_>>().len() != self.evidence_refs.len()
            || self
                .modality_scores
                .values()
                .any(|score| !unit_score(*score))
        {
            return Err(ProgramError::InvalidPromotion);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OptimizationRequest {
    pub schema_version: u16,
    pub request_ref: OpaqueRef,
    pub program: ProgramRevision,
    pub corpus: TrainingCorpus,
    pub optimizer: OptimizerKind,
    #[serde(default)]
    pub budget: OptimizationBudget,
    #[serde(default)]
    pub promotion: PromotionPolicy,
    pub baseline: EvaluationSummary,
    #[serde(default)]
    pub optimizer_artifacts: Vec<OptimizerArtifact>,
    #[serde(default)]
    pub candidate_evaluations: Vec<EvaluationSummary>,
}

impl OptimizationRequest {
    pub fn validate(&self) -> Result<(), ProgramError> {
        if self.schema_version != PROGRAM_SCHEMA_VERSION {
            return Err(ProgramError::UnsupportedVersion);
        }
        self.program.validate()?;
        self.corpus.validate(&self.program)?;
        self.budget.validate(self.optimizer)?;
        self.promotion.validate()?;
        self.baseline.validate()?;
        let observed = self.corpus.modalities();
        if !self.baseline_consistency_ok(&observed) {
            return Err(ProgramError::InvalidPromotion);
        }
        self.validate_avatar_examples()?;
        self.validate_artifacts(&observed)?;
        self.validate_candidate_evaluations()?;
        Ok(())
    }

    /// The baseline/limits checks: the baseline scores THIS program, artifact/
    /// evaluation counts stay within their ceilings, and every observed modality has
    /// a baseline score.
    fn baseline_consistency_ok(&self, observed: &BTreeSet<ProgramModality>) -> bool {
        self.baseline.subject_ref == self.program.program_ref
            && self.optimizer_artifacts.len() <= MAX_OPTIMIZER_ARTIFACTS
            && self.candidate_evaluations.len() <= MAX_EVALUATIONS
            && observed.is_subset(
                &self
                    .baseline
                    .modality_scores
                    .keys()
                    .copied()
                    .collect::<BTreeSet<_>>(),
            )
    }

    /// AVATAR needs tool refs plus at least one successful and one failed-with-feedback
    /// training example to have anything to contrast.
    fn validate_avatar_examples(&self) -> Result<(), ProgramError> {
        if self.optimizer != OptimizerKind::Avatar {
            return Ok(());
        }
        let positive = self.corpus.examples.iter().any(|example| {
            example.split == ExampleSplit::Train
                && example.outcome == ExampleOutcome::Success
                && example.trace_ref.is_some()
        });
        let negative = self.corpus.examples.iter().any(|example| {
            example.split == ExampleSplit::Train
                && example.outcome != ExampleOutcome::Success
                && example.trace_ref.is_some()
                && example.feedback_ref.is_some()
        });
        if self.program.tool_refs.is_empty() || !positive || !negative {
            return Err(ProgramError::NoEligibleExamples);
        }
        Ok(())
    }

    /// Validate every optimizer artifact: unique ref, accepted by this optimizer kind,
    /// scoped to an observed modality, and (for the ref-carrying kinds) sourced from
    /// this request's corpus/examples.
    fn validate_artifacts(&self, observed: &BTreeSet<ProgramModality>) -> Result<(), ProgramError> {
        let example_refs = self
            .corpus
            .examples
            .iter()
            .map(|example| example.example_ref.as_str())
            .collect::<BTreeSet<_>>();
        let mut artifact_refs = BTreeSet::new();
        for artifact in &self.optimizer_artifacts {
            artifact.validate(&self.program.policy)?;
            if !artifact_refs.insert(&artifact.artifact_ref)
                || !self.optimizer.accepts_artifact(artifact.kind)
                || !artifact.modalities.is_subset(observed)
                || (artifact.kind == OptimizerArtifactKind::ToolPolicy
                    && artifact.source_ref != self.corpus.corpus_ref)
                || (artifact.kind == OptimizerArtifactKind::NeighborScore
                    && !example_refs.contains(artifact.source_ref.as_str()))
            {
                return Err(ProgramError::InvalidArtifact);
            }
        }
        Ok(())
    }

    /// Validate every candidate evaluation: individually well-formed, and at most one
    /// per subject.
    fn validate_candidate_evaluations(&self) -> Result<(), ProgramError> {
        let mut subjects = BTreeSet::new();
        for evaluation in &self.candidate_evaluations {
            evaluation.validate()?;
            if !subjects.insert(evaluation.subject_ref.as_str()) {
                return Err(ProgramError::InvalidPromotion);
            }
        }
        Ok(())
    }

    /// Replace descriptive policy scope with authority-derived scope at trusted
    /// ingress. Caller scope never survives; immutable content identity remains.
    pub fn rebind_program_policy(mut self, policy: PolicyEnvelope) -> Result<Self, ProgramError> {
        self.program.policy = policy.clone();
        for example in &mut self.corpus.examples {
            for binding in &mut example.evidence {
                binding.locus.policy_ref = policy.access_policy_ref.clone();
                binding.policy = policy.clone();
            }
        }
        for artifact in &mut self.optimizer_artifacts {
            artifact.access_policy_ref = policy.access_policy_ref.clone();
        }
        self.validate()?;
        Ok(self)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CandidateRole {
    Proposal,
    EnsembleMember,
    Ensemble,
}

impl CandidateRole {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Proposal => "proposal",
            Self::EnsembleMember => "ensemble_member",
            Self::Ensemble => "ensemble",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProgramCandidate {
    pub candidate_ref: OpaqueRef,
    pub program_ref: OpaqueRef,
    pub optimizer: OptimizerKind,
    pub role: CandidateRole,
    pub demonstration_refs: Vec<OpaqueRef>,
    #[serde(default)]
    pub artifact_refs: Vec<OpaqueRef>,
    #[serde(default)]
    pub composition_refs: Vec<OpaqueRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instruction_ref: Option<OpaqueRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_policy_ref: Option<OpaqueRef>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_profile_ref: Option<OpaqueRef>,
    pub modalities: BTreeSet<ProgramModality>,
    pub policy: PolicyEnvelope,
    pub content_digest: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evaluation: Option<EvaluationSummary>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OptimizationCheckpoint {
    pub request_ref: OpaqueRef,
    pub corpus_ref: OpaqueRef,
    pub snapshot_version: u64,
    pub optimizer: OptimizerKind,
    pub generated_candidates: usize,
    pub generated_plans: usize,
    pub planned_steps: usize,
    pub evaluated_candidates: usize,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OptimizationResult {
    pub schema_version: u16,
    pub request_ref: OpaqueRef,
    pub program_ref: OpaqueRef,
    pub optimizer: OptimizerKind,
    pub candidates: Vec<ProgramCandidate>,
    #[serde(default)]
    pub plans: Vec<OptimizationPlan>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selected_candidate_ref: Option<OpaqueRef>,
    pub promoted: bool,
    pub checkpoint: OptimizationCheckpoint,
}

impl OptimizationResult {
    pub fn selected_candidate(&self) -> Option<&ProgramCandidate> {
        let selected = self.selected_candidate_ref.as_ref()?;
        self.candidates
            .iter()
            .find(|candidate| &candidate.candidate_ref == selected)
    }
}

#[derive(Debug, Default)]
pub struct NativeCompiler;

impl NativeCompiler {
    pub fn compile(request: &OptimizationRequest) -> Result<OptimizationResult, ProgramError> {
        let cancellation = AtomicBool::new(false);
        Self::compile_cancellable(request, &cancellation)
    }

    pub fn compile_cancellable(
        request: &OptimizationRequest,
        cancellation: &AtomicBool,
    ) -> Result<OptimizationResult, ProgramError> {
        request.validate()?;
        check_cancelled(cancellation)?;

        let eligible = eligible_examples(request);
        if eligible.is_empty() {
            return Err(ProgramError::NoEligibleExamples);
        }
        let example_modalities = eligible
            .iter()
            .map(|example| (example.example_ref.clone(), example.modalities()))
            .collect::<BTreeMap<_, _>>();
        let artifact_modalities = request
            .optimizer_artifacts
            .iter()
            .map(|artifact| (artifact.artifact_ref.clone(), artifact.modalities.clone()))
            .collect::<BTreeMap<_, _>>();
        let evaluations = request
            .candidate_evaluations
            .iter()
            .map(|evaluation| (evaluation.subject_ref.as_str(), evaluation))
            .collect::<BTreeMap<_, _>>();

        let specs = candidate_specs(request, &eligible, cancellation)?;
        let mut candidates = specs
            .into_iter()
            .map(|spec| {
                materialize_candidate(
                    request,
                    spec,
                    &example_modalities,
                    &artifact_modalities,
                    &evaluations,
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        candidates.sort_by(|left, right| left.content_digest.cmp(&right.content_digest));
        candidates.dedup_by(|left, right| left.candidate_ref == right.candidate_ref);

        maybe_add_ensemble_candidate(
            request,
            &mut candidates,
            &example_modalities,
            &artifact_modalities,
            &evaluations,
        )?;
        if candidates.len() > request.budget.max_candidates {
            candidates.truncate(request.budget.max_candidates);
        }
        if !evaluations_reference_known_candidates(&candidates, &evaluations) {
            return Err(ProgramError::InvalidPromotion);
        }

        let plans = assemble_plans(request, &eligible, &candidates)?;

        let selected_candidate_ref = candidates
            .iter()
            .filter(|candidate| promotable(request, candidate))
            .max_by(|left, right| compare_candidates(left, right))
            .map(|candidate| candidate.candidate_ref.clone());
        let evaluated_candidates = candidates
            .iter()
            .filter(|candidate| candidate.evaluation.is_some())
            .count();
        let generated_candidates = candidates.len();
        let planned_steps = plans.iter().map(|plan| plan.steps.len()).sum();
        let generated_plans = plans.len();
        Ok(OptimizationResult {
            schema_version: PROGRAM_SCHEMA_VERSION,
            request_ref: request.request_ref.clone(),
            program_ref: request.program.program_ref.clone(),
            optimizer: request.optimizer,
            candidates,
            plans,
            promoted: selected_candidate_ref.is_some(),
            selected_candidate_ref,
            checkpoint: OptimizationCheckpoint {
                request_ref: request.request_ref.clone(),
                corpus_ref: request.corpus.corpus_ref.clone(),
                snapshot_version: request.corpus.snapshot_version,
                optimizer: request.optimizer,
                generated_candidates,
                generated_plans,
                planned_steps,
                evaluated_candidates,
            },
        })
    }
}

/// The [`compile_cancellable`] ensemble step: when running the `Ensemble` optimizer
/// with fewer than 2 pre-supplied ensemble artifacts and at least 2 member candidates,
/// synthesize and append the ensemble candidate (its demonstrations are the union of
/// its members', capped at the budget).
fn maybe_add_ensemble_candidate(
    request: &OptimizationRequest,
    candidates: &mut Vec<ProgramCandidate>,
    example_modalities: &BTreeMap<OpaqueRef, BTreeSet<ProgramModality>>,
    artifact_modalities: &BTreeMap<OpaqueRef, BTreeSet<ProgramModality>>,
    evaluations: &BTreeMap<&str, &EvaluationSummary>,
) -> Result<(), ProgramError> {
    if !(request.optimizer == OptimizerKind::Ensemble
        && ensemble_artifacts(request).len() < 2
        && candidates.len() >= 2)
    {
        return Ok(());
    }
    let member_refs = candidates
        .iter()
        .map(|candidate| candidate.candidate_ref.clone())
        .collect::<Vec<_>>();
    let demonstration_refs = candidates
        .iter()
        .flat_map(|candidate| candidate.demonstration_refs.iter().cloned())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .take(request.budget.max_demonstrations)
        .collect::<Vec<_>>();
    let ensemble = materialize_candidate(
        request,
        CandidateSpec {
            role: CandidateRole::Ensemble,
            demonstration_refs,
            artifact_refs: Vec::new(),
            composition_refs: member_refs,
            instruction_ref: None,
            tool_policy_ref: None,
            model_profile_ref: None,
        },
        example_modalities,
        artifact_modalities,
        evaluations,
    )?;
    candidates.push(ensemble);
    Ok(())
}

/// Every evaluation's subject refers to a candidate that was actually generated
/// (guards against a stale/foreign evaluation being smuggled into promotion).
fn evaluations_reference_known_candidates(
    candidates: &[ProgramCandidate],
    evaluations: &BTreeMap<&str, &EvaluationSummary>,
) -> bool {
    let candidate_ids = candidates
        .iter()
        .map(|candidate| candidate.candidate_ref.as_str())
        .collect::<BTreeSet<_>>();
    evaluations
        .keys()
        .all(|subject_ref| candidate_ids.contains(*subject_ref))
}

/// Build the required plans, append the evaluation plan when the budget allows more
/// evaluator calls and some candidate is still unevaluated, then validate every plan
/// (policy included) before returning.
fn assemble_plans(
    request: &OptimizationRequest,
    eligible: &[&TrainingExample],
    candidates: &[ProgramCandidate],
) -> Result<Vec<OptimizationPlan>, ProgramError> {
    let mut plans = required_plans(request, eligible)?;
    if request.budget.max_evaluator_calls > 0
        && candidates
            .iter()
            .any(|candidate| candidate.evaluation.is_none())
    {
        plans.push(evaluation_plan(request, candidates)?);
    }
    for plan in &plans {
        plan.validate()?;
        if plan.policy != request.program.policy {
            return Err(ProgramError::PolicyMismatch);
        }
    }
    Ok(plans)
}

#[derive(Clone)]
struct CandidateSpec {
    role: CandidateRole,
    demonstration_refs: Vec<OpaqueRef>,
    artifact_refs: Vec<OpaqueRef>,
    composition_refs: Vec<OpaqueRef>,
    instruction_ref: Option<OpaqueRef>,
    tool_policy_ref: Option<OpaqueRef>,
    model_profile_ref: Option<OpaqueRef>,
}

fn candidate_specs(
    request: &OptimizationRequest,
    eligible: &[&TrainingExample],
    cancellation: &AtomicBool,
) -> Result<Vec<CandidateSpec>, ProgramError> {
    let cover = select_covering_modalities(eligible, request.budget.max_demonstrations);
    // The three optimizers below need `eligible`/`cancellation` for per-candidate
    // random selection; every other kind needs only the modality-covering
    // demonstration set, so it delegates to `candidate_specs_from_cover` — split
    // purely to keep each dispatcher's arm count (hence complexity) low, not for any
    // behavioral reason.
    match request.optimizer {
        OptimizerKind::BootstrapFewShotWithRandomSearch => {
            specs_bootstrap_random_search(request, eligible, cancellation)
        }
        OptimizerKind::MiproV2 => specs_mipro_v2(request, eligible, cancellation),
        OptimizerKind::Ensemble => specs_ensemble(request, eligible, cover, cancellation),
        _ => Ok(candidate_specs_from_cover(request, cover)),
    }
}

/// The [`candidate_specs`] arms that need only the modality-covering demonstration set
/// `cover` (no per-candidate randomness, so no `eligible`/`cancellation` needed).
fn candidate_specs_from_cover(
    request: &OptimizationRequest,
    cover: Vec<OpaqueRef>,
) -> Vec<CandidateSpec> {
    match request.optimizer {
        OptimizerKind::LabeledFewShot | OptimizerKind::BootstrapFewShot => {
            vec![proposal_spec(cover)]
        }
        OptimizerKind::Avatar => specs_avatar(request, &cover),
        OptimizerKind::Copro => specs_copro(request, &cover),
        OptimizerKind::Simba | OptimizerKind::Gepa => specs_simba_or_gepa(request, &cover),
        OptimizerKind::InferRules => specs_infer_rules(request, &cover),
        OptimizerKind::BootstrapFinetune => specs_bootstrap_finetune(request, &cover),
        OptimizerKind::BetterTogether => specs_better_together(request, &cover),
        OptimizerKind::KnnFewShot => specs_knn_few_shot(request),
        OptimizerKind::BootstrapFewShotWithRandomSearch
        | OptimizerKind::MiproV2
        | OptimizerKind::Ensemble => {
            unreachable!("candidate_specs routes these optimizers before delegating here")
        }
    }
}

/// A plain `Proposal`-role spec carrying only demonstrations — LABELED_FEW_SHOT /
/// BOOTSTRAP_FEW_SHOT's candidate, and the building block for the random-search variant.
fn proposal_spec(demonstration_refs: Vec<OpaqueRef>) -> CandidateSpec {
    CandidateSpec {
        role: CandidateRole::Proposal,
        demonstration_refs,
        artifact_refs: Vec::new(),
        composition_refs: Vec::new(),
        instruction_ref: None,
        tool_policy_ref: None,
        model_profile_ref: None,
    }
}

/// BOOTSTRAP_FEW_SHOT_WITH_RANDOM_SEARCH: one proposal per random demonstration
/// selection.
fn specs_bootstrap_random_search(
    request: &OptimizationRequest,
    eligible: &[&TrainingExample],
    cancellation: &AtomicBool,
) -> Result<Vec<CandidateSpec>, ProgramError> {
    Ok(random_selections(
        request,
        eligible,
        request.budget.max_candidates,
        cancellation,
    )?
    .into_iter()
    .map(proposal_spec)
    .collect())
}

/// AVATAR: one proposal per ranked tool-policy artifact, over the shared cover.
fn specs_avatar(request: &OptimizationRequest, cover: &[OpaqueRef]) -> Vec<CandidateSpec> {
    ranked_artifacts(request, OptimizerArtifactKind::ToolPolicy)
        .into_iter()
        .take(request.budget.max_candidates)
        .map(|artifact| CandidateSpec {
            role: CandidateRole::Proposal,
            demonstration_refs: cover.to_vec(),
            artifact_refs: vec![artifact.artifact_ref.clone()],
            composition_refs: Vec::new(),
            instruction_ref: None,
            tool_policy_ref: Some(artifact.artifact_ref.clone()),
            model_profile_ref: None,
        })
        .collect()
}

/// COPRO: one proposal per ranked instruction-proposal artifact, over the shared cover.
fn specs_copro(request: &OptimizationRequest, cover: &[OpaqueRef]) -> Vec<CandidateSpec> {
    ranked_artifacts(request, OptimizerArtifactKind::InstructionProposal)
        .into_iter()
        .take(request.budget.max_candidates)
        .map(|artifact| CandidateSpec {
            role: CandidateRole::Proposal,
            demonstration_refs: cover.to_vec(),
            artifact_refs: vec![artifact.artifact_ref.clone()],
            composition_refs: Vec::new(),
            instruction_ref: Some(artifact.artifact_ref.clone()),
            tool_policy_ref: None,
            model_profile_ref: None,
        })
        .collect()
}

/// MIPROv2: pair each ranked instruction with an independent random demonstration
/// selection (cycling instructions if there are fewer than candidates).
fn specs_mipro_v2(
    request: &OptimizationRequest,
    eligible: &[&TrainingExample],
    cancellation: &AtomicBool,
) -> Result<Vec<CandidateSpec>, ProgramError> {
    let instructions = ranked_artifacts(request, OptimizerArtifactKind::InstructionProposal);
    let selections = random_selections(
        request,
        eligible,
        request.budget.max_candidates,
        cancellation,
    )?;
    Ok(instructions
        .into_iter()
        .cycle()
        .zip(selections)
        .take(request.budget.max_candidates)
        .map(|(artifact, demonstration_refs)| CandidateSpec {
            role: CandidateRole::Proposal,
            demonstration_refs,
            artifact_refs: vec![artifact.artifact_ref.clone()],
            composition_refs: Vec::new(),
            instruction_ref: Some(artifact.artifact_ref.clone()),
            tool_policy_ref: None,
            model_profile_ref: None,
        })
        .collect())
}

/// SIMBA / GEPA: one proposal per ranked reflection artifact, over the shared cover.
fn specs_simba_or_gepa(request: &OptimizationRequest, cover: &[OpaqueRef]) -> Vec<CandidateSpec> {
    ranked_artifacts(request, OptimizerArtifactKind::Reflection)
        .into_iter()
        .take(request.budget.max_candidates)
        .map(|artifact| CandidateSpec {
            role: CandidateRole::Proposal,
            demonstration_refs: cover.to_vec(),
            artifact_refs: vec![artifact.artifact_ref.clone()],
            composition_refs: Vec::new(),
            instruction_ref: None,
            tool_policy_ref: None,
            model_profile_ref: None,
        })
        .collect()
}

/// INFER_RULES: one proposal per ranked rule-set artifact, over the shared cover.
fn specs_infer_rules(request: &OptimizationRequest, cover: &[OpaqueRef]) -> Vec<CandidateSpec> {
    ranked_artifacts(request, OptimizerArtifactKind::RuleSet)
        .into_iter()
        .take(request.budget.max_candidates)
        .map(|artifact| CandidateSpec {
            role: CandidateRole::Proposal,
            demonstration_refs: cover.to_vec(),
            artifact_refs: vec![artifact.artifact_ref.clone()],
            composition_refs: Vec::new(),
            instruction_ref: Some(artifact.artifact_ref.clone()),
            tool_policy_ref: None,
            model_profile_ref: None,
        })
        .collect()
}

/// BOOTSTRAP_FINETUNE: one proposal per ranked finetuned-model artifact, over the
/// shared cover.
fn specs_bootstrap_finetune(
    request: &OptimizationRequest,
    cover: &[OpaqueRef],
) -> Vec<CandidateSpec> {
    ranked_artifacts(request, OptimizerArtifactKind::FinetunedModel)
        .into_iter()
        .take(request.budget.max_candidates)
        .map(|artifact| CandidateSpec {
            role: CandidateRole::Proposal,
            demonstration_refs: cover.to_vec(),
            artifact_refs: vec![artifact.artifact_ref.clone()],
            composition_refs: Vec::new(),
            instruction_ref: None,
            tool_policy_ref: None,
            model_profile_ref: Some(artifact.artifact_ref.clone()),
        })
        .collect()
}

/// BETTER_TOGETHER: the cross product of ranked instructions x ranked finetuned
/// models, over the shared cover.
fn specs_better_together(request: &OptimizationRequest, cover: &[OpaqueRef]) -> Vec<CandidateSpec> {
    let instructions = ranked_artifacts(request, OptimizerArtifactKind::InstructionProposal);
    let models = ranked_artifacts(request, OptimizerArtifactKind::FinetunedModel);
    instructions
        .into_iter()
        .flat_map(|instruction| models.iter().map(move |model| (instruction, *model)))
        .take(request.budget.max_candidates)
        .map(|(instruction, model)| CandidateSpec {
            role: CandidateRole::Proposal,
            demonstration_refs: cover.to_vec(),
            artifact_refs: vec![instruction.artifact_ref.clone(), model.artifact_ref.clone()],
            composition_refs: Vec::new(),
            instruction_ref: Some(instruction.artifact_ref.clone()),
            tool_policy_ref: None,
            model_profile_ref: Some(model.artifact_ref.clone()),
        })
        .collect()
}

/// KNN_FEW_SHOT: the ranked neighbor-score artifacts' distinct source examples, up to
/// the demonstration budget, as a single candidate (none if no neighbor scored).
fn specs_knn_few_shot(request: &OptimizationRequest) -> Vec<CandidateSpec> {
    let mut selected = Vec::new();
    let mut artifacts = Vec::new();
    let mut seen = BTreeSet::new();
    for artifact in ranked_artifacts(request, OptimizerArtifactKind::NeighborScore) {
        if selected.len() >= request.budget.max_demonstrations {
            break;
        }
        if seen.insert(artifact.source_ref.as_str()) {
            selected.push(artifact.source_ref.clone());
            artifacts.push(artifact.artifact_ref.clone());
        }
    }
    (!selected.is_empty())
        .then_some(CandidateSpec {
            role: CandidateRole::Proposal,
            demonstration_refs: selected,
            artifact_refs: artifacts,
            composition_refs: Vec::new(),
            instruction_ref: None,
            tool_policy_ref: None,
            model_profile_ref: None,
        })
        .into_iter()
        .collect()
}

/// ENSEMBLE: with >=2 pre-supplied ensemble-member artifacts, ONE ensemble candidate
/// composed from them; otherwise fall back to random-search member candidates (one
/// fewer than the budget, leaving room for the synthesized ensemble candidate added
/// later in `compile_cancellable`).
fn specs_ensemble(
    request: &OptimizationRequest,
    eligible: &[&TrainingExample],
    cover: Vec<OpaqueRef>,
    cancellation: &AtomicBool,
) -> Result<Vec<CandidateSpec>, ProgramError> {
    let members = ensemble_artifacts(request);
    if members.len() >= 2 {
        return Ok(vec![CandidateSpec {
            role: CandidateRole::Ensemble,
            demonstration_refs: cover,
            artifact_refs: members
                .iter()
                .map(|artifact| artifact.artifact_ref.clone())
                .collect(),
            composition_refs: members
                .iter()
                .map(|artifact| artifact.source_ref.clone())
                .collect(),
            instruction_ref: None,
            tool_policy_ref: None,
            model_profile_ref: None,
        }]);
    }
    Ok(random_selections(
        request,
        eligible,
        request.budget.max_candidates.saturating_sub(1),
        cancellation,
    )?
    .into_iter()
    .map(|demonstration_refs| CandidateSpec {
        role: CandidateRole::EnsembleMember,
        demonstration_refs,
        artifact_refs: Vec::new(),
        composition_refs: Vec::new(),
        instruction_ref: None,
        tool_policy_ref: None,
        model_profile_ref: None,
    })
    .collect())
}

fn materialize_candidate(
    request: &OptimizationRequest,
    mut spec: CandidateSpec,
    example_modalities: &BTreeMap<OpaqueRef, BTreeSet<ProgramModality>>,
    artifact_modalities: &BTreeMap<OpaqueRef, BTreeSet<ProgramModality>>,
    evaluations: &BTreeMap<&str, &EvaluationSummary>,
) -> Result<ProgramCandidate, ProgramError> {
    spec.demonstration_refs.sort();
    spec.demonstration_refs.dedup();
    spec.artifact_refs.sort();
    spec.artifact_refs.dedup();
    spec.composition_refs.sort();
    spec.composition_refs.dedup();
    let modalities = spec
        .demonstration_refs
        .iter()
        .filter_map(|reference| example_modalities.get(reference))
        .chain(
            spec.artifact_refs
                .iter()
                .filter_map(|reference| artifact_modalities.get(reference)),
        )
        .flatten()
        .copied()
        .collect::<BTreeSet<_>>();
    if modalities.is_empty() {
        return Err(ProgramError::ModalityMismatch);
    }
    let content_digest = candidate_digest(request, &spec);
    let candidate_ref = OpaqueRef::new(format!("eg:program_candidate:{content_digest}"))
        .map_err(|_| ProgramError::InvalidProgram)?;
    let evaluation = evaluations
        .get(candidate_ref.as_str())
        .map(|value| (*value).clone());
    Ok(ProgramCandidate {
        candidate_ref,
        program_ref: request.program.program_ref.clone(),
        optimizer: request.optimizer,
        role: spec.role,
        demonstration_refs: spec.demonstration_refs,
        artifact_refs: spec.artifact_refs,
        composition_refs: spec.composition_refs,
        instruction_ref: spec.instruction_ref,
        tool_policy_ref: spec.tool_policy_ref,
        model_profile_ref: spec.model_profile_ref,
        modalities,
        policy: request.program.policy.clone(),
        content_digest,
        evaluation,
    })
}

fn required_plans(
    request: &OptimizationRequest,
    eligible: &[&TrainingExample],
) -> Result<Vec<OptimizationPlan>, ProgramError> {
    let observed = request.corpus.modalities();
    let input_refs = vec![
        request.program.program_ref.clone(),
        request.corpus.corpus_ref.clone(),
    ];
    let steps = required_plan_steps(request, eligible, &observed, input_refs)?;
    if steps.is_empty() {
        return Ok(Vec::new());
    }
    Ok(vec![OptimizationPlan {
        plan_ref: derived_ref(request, "plan", 0)?,
        optimizer: request.optimizer,
        steps,
        modalities: observed,
        policy: request.program.policy.clone(),
    }])
}

/// The steps [`required_plans`] needs: BETTER_TOGETHER composes up to two model-side
/// steps (see [`required_plan_steps_better_together`]); every other optimizer needs at
/// most the single missing-artifact step [`required_single_step_shape`] identifies.
fn required_plan_steps(
    request: &OptimizationRequest,
    eligible: &[&TrainingExample],
    observed: &BTreeSet<ProgramModality>,
    input_refs: Vec<OpaqueRef>,
) -> Result<Vec<PlanStep>, ProgramError> {
    if request.optimizer == OptimizerKind::BetterTogether {
        return required_plan_steps_better_together(request, observed, input_refs);
    }
    let Some((kind, executor, extra_inputs, budget)) =
        required_single_step_shape(request, eligible)
    else {
        return Ok(Vec::new());
    };
    let mut inputs = input_refs;
    inputs.extend(extra_inputs);
    Ok(vec![plan_step(
        request,
        0,
        kind,
        executor,
        inputs,
        Vec::new(),
        observed.clone(),
        budget,
    )?])
}

/// Which single missing-artifact step (if any) `required_plans` needs for this
/// optimizer/eligible-example state: `(kind, executor, extra_inputs, budget)`. `None`
/// when the optimizer needs no such step — either it isn't one of the single-step
/// kinds (BETTER_TOGETHER is handled separately), or its precondition (the artifact is
/// already supplied, or `eligible` already has enough examples) is already satisfied.
fn required_single_step_shape(
    request: &OptimizationRequest,
    eligible: &[&TrainingExample],
) -> Option<(PlanStepKind, PlanExecutor, Vec<OpaqueRef>, u64)> {
    match request.optimizer {
        OptimizerKind::Avatar if !has_artifact(request, OptimizerArtifactKind::ToolPolicy) => {
            Some((
                PlanStepKind::CompareToolUse,
                PlanExecutor::ModelTransport,
                request.program.tool_refs.clone(),
                request.budget.max_model_calls,
            ))
        }
        OptimizerKind::Copro | OptimizerKind::MiproV2
            if !has_artifact(request, OptimizerArtifactKind::InstructionProposal) =>
        {
            Some((
                PlanStepKind::ProposeInstruction,
                PlanExecutor::ModelTransport,
                Vec::new(),
                request.budget.max_model_calls,
            ))
        }
        OptimizerKind::Simba if !has_artifact(request, OptimizerArtifactKind::Reflection) => {
            Some((
                PlanStepKind::ReflectOnTrace,
                PlanExecutor::ModelTransport,
                Vec::new(),
                request.budget.max_model_calls,
            ))
        }
        OptimizerKind::Gepa if !has_artifact(request, OptimizerArtifactKind::Reflection) => Some((
            PlanStepKind::ParetoReflect,
            PlanExecutor::ModelTransport,
            Vec::new(),
            request.budget.max_model_calls,
        )),
        OptimizerKind::InferRules if !has_artifact(request, OptimizerArtifactKind::RuleSet) => {
            Some((
                PlanStepKind::ProposeRules,
                PlanExecutor::ModelTransport,
                Vec::new(),
                request.budget.max_model_calls,
            ))
        }
        OptimizerKind::BootstrapFinetune
            if !has_artifact(request, OptimizerArtifactKind::FinetunedModel) =>
        {
            Some((
                PlanStepKind::TrainWeights,
                PlanExecutor::Trainer,
                Vec::new(),
                request.budget.max_training_steps,
            ))
        }
        OptimizerKind::KnnFewShot
            if !has_artifact(request, OptimizerArtifactKind::NeighborScore) =>
        {
            Some((
                PlanStepKind::QuerySimilarity,
                PlanExecutor::GraphSimilarity,
                Vec::new(),
                eligible.len().max(1) as u64,
            ))
        }
        OptimizerKind::Ensemble if ensemble_artifacts(request).len() < 2 && eligible.len() < 2 => {
            Some((
                PlanStepKind::ComposePrograms,
                PlanExecutor::NativeKernel,
                Vec::new(),
                request.budget.max_candidates as u64,
            ))
        }
        _ => None,
    }
}

/// BETTER_TOGETHER's plan: propose an instruction (unless one is already supplied),
/// train a finetuned model (unless one is already supplied), then — only if at least
/// one of those two ran — a composition step depending on whichever ran, with inputs
/// drawn from either the fresh step's outputs or the already-supplied ranked artifacts.
fn required_plan_steps_better_together(
    request: &OptimizationRequest,
    observed: &BTreeSet<ProgramModality>,
    input_refs: Vec<OpaqueRef>,
) -> Result<Vec<PlanStep>, ProgramError> {
    let mut steps = Vec::new();
    let mut dependencies = Vec::new();
    let mut composition_inputs = Vec::new();
    if !has_artifact(request, OptimizerArtifactKind::InstructionProposal) {
        let step = plan_step(
            request,
            steps.len(),
            PlanStepKind::ProposeInstruction,
            PlanExecutor::ModelTransport,
            input_refs.clone(),
            Vec::new(),
            observed.clone(),
            request.budget.max_model_calls,
        )?;
        dependencies.push(step.step_ref.clone());
        composition_inputs.extend(step.output_refs.iter().cloned());
        steps.push(step);
    } else {
        composition_inputs.extend(
            ranked_artifacts(request, OptimizerArtifactKind::InstructionProposal)
                .into_iter()
                .map(|artifact| artifact.artifact_ref.clone()),
        );
    }
    if !has_artifact(request, OptimizerArtifactKind::FinetunedModel) {
        let step = plan_step(
            request,
            steps.len(),
            PlanStepKind::TrainWeights,
            PlanExecutor::Trainer,
            input_refs,
            Vec::new(),
            observed.clone(),
            request.budget.max_training_steps,
        )?;
        dependencies.push(step.step_ref.clone());
        composition_inputs.extend(step.output_refs.iter().cloned());
        steps.push(step);
    } else {
        composition_inputs.extend(
            ranked_artifacts(request, OptimizerArtifactKind::FinetunedModel)
                .into_iter()
                .map(|artifact| artifact.artifact_ref.clone()),
        );
    }
    if !dependencies.is_empty() {
        steps.push(plan_step(
            request,
            steps.len(),
            PlanStepKind::ComposePrograms,
            PlanExecutor::NativeKernel,
            composition_inputs,
            dependencies,
            observed.clone(),
            1,
        )?);
    }
    Ok(steps)
}

fn evaluation_plan(
    request: &OptimizationRequest,
    candidates: &[ProgramCandidate],
) -> Result<OptimizationPlan, ProgramError> {
    let input_refs = candidates
        .iter()
        .filter(|candidate| candidate.evaluation.is_none())
        .map(|candidate| candidate.candidate_ref.clone())
        .collect::<Vec<_>>();
    let modalities = candidates
        .iter()
        .flat_map(|candidate| candidate.modalities.iter().copied())
        .collect::<BTreeSet<_>>();
    let step = plan_step(
        request,
        10_000,
        PlanStepKind::EvaluateCandidates,
        PlanExecutor::Evaluator,
        input_refs,
        Vec::new(),
        modalities.clone(),
        request.budget.max_evaluator_calls,
    )?;
    Ok(OptimizationPlan {
        plan_ref: derived_ref(request, "evaluation_plan", 0)?,
        optimizer: request.optimizer,
        steps: vec![step],
        modalities,
        policy: request.program.policy.clone(),
    })
}

#[allow(clippy::too_many_arguments)]
fn plan_step(
    request: &OptimizationRequest,
    index: usize,
    kind: PlanStepKind,
    executor: PlanExecutor,
    input_refs: Vec<OpaqueRef>,
    depends_on: Vec<OpaqueRef>,
    modalities: BTreeSet<ProgramModality>,
    max_operations: u64,
) -> Result<PlanStep, ProgramError> {
    Ok(PlanStep {
        step_ref: derived_ref(request, kind.as_str(), index)?,
        kind,
        executor,
        input_refs,
        output_refs: vec![derived_ref(request, "plan_output", index)?],
        depends_on,
        modalities,
        max_operations,
    })
}

fn derived_ref(
    request: &OptimizationRequest,
    namespace: &str,
    index: usize,
) -> Result<OpaqueRef, ProgramError> {
    let mut digest = Sha256::new();
    digest.update(b"eg-program.plan.v1\0");
    digest.update(request.request_ref.as_str().as_bytes());
    digest.update(request.program.program_ref.as_str().as_bytes());
    digest.update(request.program.revision.to_le_bytes());
    digest.update(request.program.signature.signature_ref.as_str().as_bytes());
    digest.update(
        request
            .program
            .signature
            .instruction_ref
            .as_str()
            .as_bytes(),
    );
    digest.update(request.program.policy.tenant_ref.as_str().as_bytes());
    digest.update(request.program.policy.access_policy_ref.as_str().as_bytes());
    digest.update(request.corpus.corpus_ref.as_str().as_bytes());
    digest.update(request.corpus.snapshot_version.to_le_bytes());
    digest.update(request.optimizer.as_str().as_bytes());
    digest.update((request.budget.max_candidates as u64).to_le_bytes());
    digest.update((request.budget.max_demonstrations as u64).to_le_bytes());
    digest.update(request.budget.max_model_calls.to_le_bytes());
    digest.update(request.budget.max_evaluator_calls.to_le_bytes());
    digest.update(request.budget.max_training_steps.to_le_bytes());
    digest.update(request.budget.seed.to_le_bytes());
    digest.update(namespace.as_bytes());
    digest.update((index as u64).to_le_bytes());
    OpaqueRef::new(format!("eg:{namespace}:{}", hex::encode(digest.finalize())))
        .map_err(|_| ProgramError::InvalidPlan)
}

fn eligible_examples(request: &OptimizationRequest) -> Vec<&TrainingExample> {
    let mut eligible = request
        .corpus
        .examples
        .iter()
        .filter(|example| example.split == ExampleSplit::Train)
        .filter(|example| match request.optimizer {
            OptimizerKind::LabeledFewShot => example.expected_output_ref.is_some(),
            OptimizerKind::Avatar => {
                example.trace_ref.is_some()
                    && (example.outcome == ExampleOutcome::Success
                        || example.feedback_ref.is_some())
            }
            OptimizerKind::Simba | OptimizerKind::Gepa => {
                example.observed_output_ref.is_some()
                    && (example.feedback_ref.is_some() || example.trace_ref.is_some())
            }
            _ => {
                example.outcome == ExampleOutcome::Success && example.observed_output_ref.is_some()
            }
        })
        .collect::<Vec<_>>();
    eligible.sort_by(|left, right| {
        right
            .score
            .partial_cmp(&left.score)
            .unwrap_or(Ordering::Equal)
            .then_with(|| left.example_ref.cmp(&right.example_ref))
    });
    eligible
}

fn random_selections(
    request: &OptimizationRequest,
    eligible: &[&TrainingExample],
    count: usize,
    cancellation: &AtomicBool,
) -> Result<Vec<Vec<OpaqueRef>>, ProgramError> {
    let mut selections = Vec::with_capacity(count);
    for candidate_index in 0..count {
        check_cancelled(cancellation)?;
        let max_take = request.budget.max_demonstrations.min(eligible.len());
        let take = if candidate_index == 0 || max_take == 1 {
            max_take
        } else {
            1 + ((request.budget.seed as usize).wrapping_add(candidate_index - 1) % (max_take - 1))
        };
        let mut ranked = BinaryHeap::with_capacity(take.saturating_add(1));
        for example in eligible {
            check_cancelled(cancellation)?;
            let mut digest = Sha256::new();
            digest.update(b"eg-program.random-search.v1\0");
            digest.update(request.budget.seed.to_le_bytes());
            digest.update((candidate_index as u64).to_le_bytes());
            digest.update(example.example_ref.as_str().as_bytes());
            let item = (digest.finalize().to_vec(), example.example_ref.clone());
            if ranked.len() < take {
                ranked.push(item);
            } else if ranked.peek().is_some_and(|largest| item < *largest) {
                ranked.pop();
                ranked.push(item);
            }
        }
        let mut refs = ranked
            .into_iter()
            .map(|(_, reference)| reference)
            .collect::<Vec<_>>();
        refs.sort();
        selections.push(refs);
    }
    selections.sort();
    selections.dedup();
    Ok(selections)
}

fn select_covering_modalities(eligible: &[&TrainingExample], limit: usize) -> Vec<OpaqueRef> {
    let mut selected = BTreeSet::new();
    let observed = eligible
        .iter()
        .flat_map(|example| example.modalities())
        .collect::<BTreeSet<_>>();
    for modality in observed {
        if selected.len() >= limit {
            break;
        }
        if let Some(example) = eligible.iter().find(|example| {
            !selected.contains(&example.example_ref) && example.modalities().contains(&modality)
        }) {
            selected.insert(example.example_ref.clone());
        }
    }
    for example in eligible {
        if selected.len() >= limit {
            break;
        }
        selected.insert(example.example_ref.clone());
    }
    selected.into_iter().collect()
}

fn ranked_artifacts(
    request: &OptimizationRequest,
    kind: OptimizerArtifactKind,
) -> Vec<&OptimizerArtifact> {
    let mut artifacts = request
        .optimizer_artifacts
        .iter()
        .filter(|artifact| artifact.kind == kind)
        .collect::<Vec<_>>();
    artifacts.sort_by(|left, right| {
        right
            .score
            .partial_cmp(&left.score)
            .unwrap_or(Ordering::Equal)
            .then_with(|| left.artifact_ref.cmp(&right.artifact_ref))
    });
    artifacts
}

fn ensemble_artifacts(request: &OptimizationRequest) -> Vec<&OptimizerArtifact> {
    ranked_artifacts(request, OptimizerArtifactKind::EnsembleMember)
}

fn has_artifact(request: &OptimizationRequest, kind: OptimizerArtifactKind) -> bool {
    request
        .optimizer_artifacts
        .iter()
        .any(|artifact| artifact.kind == kind)
}

fn candidate_digest(request: &OptimizationRequest, spec: &CandidateSpec) -> String {
    let mut digest = Sha256::new();
    digest.update(b"eg-program.candidate.v3\0");
    digest.update(request.program.program_ref.as_str().as_bytes());
    digest.update(request.program.revision.to_le_bytes());
    digest.update(request.program.signature.signature_ref.as_str().as_bytes());
    digest.update(
        request
            .program
            .signature
            .instruction_ref
            .as_str()
            .as_bytes(),
    );
    digest.update(request.program.module.as_str().as_bytes());
    digest.update(request.program.adapter.as_str().as_bytes());
    digest.update(request.program.policy.tenant_ref.as_str().as_bytes());
    digest.update(request.program.policy.access_policy_ref.as_str().as_bytes());
    digest.update(request.corpus.corpus_ref.as_str().as_bytes());
    digest.update(request.corpus.snapshot_version.to_le_bytes());
    digest.update(request.optimizer.as_str().as_bytes());
    digest.update(request.budget.seed.to_le_bytes());
    digest.update(spec.role.as_str().as_bytes());
    digest_ref_list(&mut digest, b"demonstrations", &spec.demonstration_refs);
    digest_ref_list(&mut digest, b"artifacts", &spec.artifact_refs);
    digest_ref_list(&mut digest, b"composition", &spec.composition_refs);
    digest_optional_ref(&mut digest, b"instruction", spec.instruction_ref.as_ref());
    digest_optional_ref(&mut digest, b"tool_policy", spec.tool_policy_ref.as_ref());
    digest_optional_ref(
        &mut digest,
        b"model_profile",
        spec.model_profile_ref.as_ref(),
    );
    hex::encode(digest.finalize())
}

fn digest_ref_list(digest: &mut Sha256, label: &[u8], references: &[OpaqueRef]) {
    digest.update((label.len() as u64).to_le_bytes());
    digest.update(label);
    digest.update((references.len() as u64).to_le_bytes());
    for reference in references {
        digest.update((reference.as_str().len() as u64).to_le_bytes());
        digest.update(reference.as_str().as_bytes());
    }
}

fn digest_optional_ref(digest: &mut Sha256, label: &[u8], reference: Option<&OpaqueRef>) {
    digest.update((label.len() as u64).to_le_bytes());
    digest.update(label);
    digest.update([u8::from(reference.is_some())]);
    if let Some(reference) = reference {
        digest.update((reference.as_str().len() as u64).to_le_bytes());
        digest.update(reference.as_str().as_bytes());
    }
}

fn promotable(request: &OptimizationRequest, candidate: &ProgramCandidate) -> bool {
    if candidate.role == CandidateRole::EnsembleMember {
        return false;
    }
    let Some(evaluation) = &candidate.evaluation else {
        return false;
    };
    if evaluation.evidence_refs.len() < request.promotion.min_evidence_refs
        || evaluation.aggregate_score + f64::EPSILON
            < request.baseline.aggregate_score + request.promotion.min_score_delta
    {
        return false;
    }
    let observed = request.corpus.modalities();
    if request.promotion.require_all_observed_modalities
        && (!observed.is_subset(&candidate.modalities)
            || !observed.is_subset(
                &evaluation
                    .modality_scores
                    .keys()
                    .copied()
                    .collect::<BTreeSet<_>>(),
            ))
    {
        return false;
    }
    request
        .baseline
        .modality_scores
        .iter()
        .all(|(modality, baseline)| {
            evaluation
                .modality_scores
                .get(modality)
                .is_some_and(|score| score + request.promotion.max_modality_regression >= *baseline)
        })
}

fn compare_candidates(left: &ProgramCandidate, right: &ProgramCandidate) -> Ordering {
    let left_score = left
        .evaluation
        .as_ref()
        .map(|evaluation| evaluation.aggregate_score)
        .unwrap_or(f64::NEG_INFINITY);
    let right_score = right
        .evaluation
        .as_ref()
        .map(|evaluation| evaluation.aggregate_score)
        .unwrap_or(f64::NEG_INFINITY);
    left_score
        .partial_cmp(&right_score)
        .unwrap_or(Ordering::Equal)
        .then_with(|| right.content_digest.cmp(&left.content_digest))
}

fn check_cancelled(cancellation: &AtomicBool) -> Result<(), ProgramError> {
    if cancellation.load(AtomicOrdering::Relaxed) {
        Err(ProgramError::Cancelled)
    } else {
        Ok(())
    }
}

fn unit_score(score: f64) -> bool {
    score.is_finite() && (0.0..=1.0).contains(&score)
}
