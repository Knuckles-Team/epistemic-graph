//! Governed execution plans for optimizer work that belongs to an existing
//! engine runtime rather than to the deterministic program compiler.
//!
//! Plans are reference-only.  They describe work for the graph similarity
//! kernel, the engine-owned model transport, or the engine-owned trainer; they
//! never carry provider configuration, prompts, responses, model weights, or
//! source locators.

use std::collections::BTreeSet;

use eg_modality::{OpaqueRef, PolicyEnvelope};
use serde::{Deserialize, Serialize};

use crate::{OptimizerKind, ProgramError, ProgramModality, MAX_EVIDENCE_PER_EXAMPLE};

pub const MAX_OPTIMIZER_ARTIFACTS: usize = 4_096;
pub const MAX_PLAN_STEPS: usize = 64;
pub const MAX_PLAN_REFS: usize = 4_096;

/// A materialized, governed artifact supplied by an engine runtime on a later
/// deterministic compilation pass.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OptimizerArtifactKind {
    InstructionProposal,
    ToolPolicy,
    RuleSet,
    Reflection,
    NeighborScore,
    EnsembleMember,
    FinetunedModel,
}

impl OptimizerArtifactKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InstructionProposal => "instruction_proposal",
            Self::ToolPolicy => "tool_policy",
            Self::RuleSet => "rule_set",
            Self::Reflection => "reflection",
            Self::NeighborScore => "neighbor_score",
            Self::EnsembleMember => "ensemble_member",
            Self::FinetunedModel => "finetuned_model",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OptimizerArtifact {
    pub artifact_ref: OpaqueRef,
    pub kind: OptimizerArtifactKind,
    /// The example, trace, candidate, program, or corpus that the artifact was
    /// derived from.  It remains opaque to this crate.
    pub source_ref: OpaqueRef,
    pub modalities: BTreeSet<ProgramModality>,
    /// Similarity, proposal quality, reflection utility, or trainer quality in
    /// `[0, 1]`; the semantic meaning is fixed by `kind`.
    pub score: f64,
    pub evidence_refs: Vec<OpaqueRef>,
    pub access_policy_ref: OpaqueRef,
}

impl OptimizerArtifact {
    pub fn validate(&self, policy: &PolicyEnvelope) -> Result<(), ProgramError> {
        if self.modalities.is_empty()
            || self.evidence_refs.is_empty()
            || self.evidence_refs.len() > MAX_EVIDENCE_PER_EXAMPLE
            || !self.score.is_finite()
            || !(0.0..=1.0).contains(&self.score)
            || self.access_policy_ref != policy.access_policy_ref
            || self.evidence_refs.iter().collect::<BTreeSet<_>>().len() != self.evidence_refs.len()
        {
            return Err(ProgramError::InvalidArtifact);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlanExecutor {
    NativeKernel,
    GraphSimilarity,
    ModelTransport,
    Evaluator,
    Trainer,
}

impl PlanExecutor {
    pub const ALL: [Self; 5] = [
        Self::NativeKernel,
        Self::GraphSimilarity,
        Self::ModelTransport,
        Self::Evaluator,
        Self::Trainer,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NativeKernel => "native_kernel",
            Self::GraphSimilarity => "graph_similarity",
            Self::ModelTransport => "model_transport",
            Self::Evaluator => "evaluator",
            Self::Trainer => "trainer",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlanStepKind {
    QuerySimilarity,
    ProposeInstruction,
    CompareToolUse,
    ProposeRules,
    ReflectOnTrace,
    ParetoReflect,
    ComposePrograms,
    TrainWeights,
    EvaluateCandidates,
}

impl PlanStepKind {
    pub const ALL: [Self; 9] = [
        Self::QuerySimilarity,
        Self::ProposeInstruction,
        Self::CompareToolUse,
        Self::ProposeRules,
        Self::ReflectOnTrace,
        Self::ParetoReflect,
        Self::ComposePrograms,
        Self::TrainWeights,
        Self::EvaluateCandidates,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::QuerySimilarity => "query_similarity",
            Self::ProposeInstruction => "propose_instruction",
            Self::CompareToolUse => "compare_tool_use",
            Self::ProposeRules => "propose_rules",
            Self::ReflectOnTrace => "reflect_on_trace",
            Self::ParetoReflect => "pareto_reflect",
            Self::ComposePrograms => "compose_programs",
            Self::TrainWeights => "train_weights",
            Self::EvaluateCandidates => "evaluate_candidates",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanStep {
    pub step_ref: OpaqueRef,
    pub kind: PlanStepKind,
    pub executor: PlanExecutor,
    pub input_refs: Vec<OpaqueRef>,
    pub output_refs: Vec<OpaqueRef>,
    #[serde(default)]
    pub depends_on: Vec<OpaqueRef>,
    pub modalities: BTreeSet<ProgramModality>,
    /// Hard operation budget interpreted by the selected existing runtime.
    pub max_operations: u64,
}

impl PlanStep {
    fn validate(&self) -> Result<(), ProgramError> {
        if self.input_refs.is_empty()
            || self.output_refs.is_empty()
            || self.input_refs.len() > MAX_PLAN_REFS
            || self.output_refs.len() > MAX_PLAN_REFS
            || self.depends_on.len() > MAX_PLAN_STEPS
            || self.modalities.is_empty()
            || self.max_operations == 0
            || self.max_operations > crate::MAX_TRAINING_STEPS
            || !unique(&self.input_refs)
            || !unique(&self.output_refs)
            || !unique(&self.depends_on)
            || !disjoint(&self.input_refs, &self.output_refs)
        {
            return Err(ProgramError::InvalidPlan);
        }
        let expected_executor = match self.kind {
            PlanStepKind::QuerySimilarity => PlanExecutor::GraphSimilarity,
            PlanStepKind::ProposeInstruction
            | PlanStepKind::CompareToolUse
            | PlanStepKind::ProposeRules
            | PlanStepKind::ReflectOnTrace
            | PlanStepKind::ParetoReflect => PlanExecutor::ModelTransport,
            PlanStepKind::ComposePrograms => PlanExecutor::NativeKernel,
            PlanStepKind::TrainWeights => PlanExecutor::Trainer,
            PlanStepKind::EvaluateCandidates => PlanExecutor::Evaluator,
        };
        if self.executor != expected_executor {
            return Err(ProgramError::InvalidPlan);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OptimizationPlan {
    pub plan_ref: OpaqueRef,
    pub optimizer: OptimizerKind,
    pub steps: Vec<PlanStep>,
    pub modalities: BTreeSet<ProgramModality>,
    pub policy: PolicyEnvelope,
}

impl OptimizationPlan {
    pub fn validate(&self) -> Result<(), ProgramError> {
        if self.steps.is_empty() || self.steps.len() > MAX_PLAN_STEPS || self.modalities.is_empty()
        {
            return Err(ProgramError::InvalidPlan);
        }
        let mut seen_steps = BTreeSet::new();
        let mut outputs = BTreeSet::new();
        let mut covered_modalities = BTreeSet::new();
        for step in &self.steps {
            step.validate()?;
            if !step
                .depends_on
                .iter()
                .all(|dependency| seen_steps.contains(dependency))
                || !seen_steps.insert(step.step_ref.clone())
                || step
                    .output_refs
                    .iter()
                    .any(|output| !outputs.insert(output.clone()))
            {
                return Err(ProgramError::InvalidPlan);
            }
            covered_modalities.extend(step.modalities.iter().copied());
        }
        if !self.modalities.is_subset(&covered_modalities) {
            return Err(ProgramError::InvalidPlan);
        }
        Ok(())
    }

    pub fn executors(&self) -> BTreeSet<PlanExecutor> {
        self.steps.iter().map(|step| step.executor).collect()
    }

    pub fn step_kinds(&self) -> BTreeSet<PlanStepKind> {
        self.steps.iter().map(|step| step.kind).collect()
    }

    pub fn output_refs(&self) -> Vec<OpaqueRef> {
        self.steps
            .iter()
            .flat_map(|step| step.output_refs.iter().cloned())
            .collect()
    }
}

fn unique(values: &[OpaqueRef]) -> bool {
    values.iter().collect::<BTreeSet<_>>().len() == values.len()
}

fn disjoint(left: &[OpaqueRef], right: &[OpaqueRef]) -> bool {
    let right = right.iter().collect::<BTreeSet<_>>();
    left.iter().all(|value| !right.contains(value))
}
