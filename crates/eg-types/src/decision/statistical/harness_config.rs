//! Producer types and pure harness rules for R128.2, R128.3 and R128.5–R128.7.
//! Profile resolution, question dispatch and persistence are separate leaves.

use serde::{Deserialize, Serialize};

use crate::agent_component::{MeasuredFact, ObservationRef, ShellProficiency, StoppingBehaviour};
use crate::capacity_lease::LeasePriority;
use crate::contract::BoundedVec;
use crate::decision::numeric::UnitRationalWire;

mod context_policy;
pub use context_policy::{decide_context_policy, ContextPolicyDecision, HarnessDefaults};

/// Typed inputs whose field names match `DecideRequest.params` entries.
/// References use the opaque strings carried by `TypedValue::Iri`; token counts
/// use the same nonnegative `u32` range as `ToolSubsetRequestBody`.
/// Priority reuses the canonical capacity priority vocabulary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct HarnessConfigRequestBody {
    pub model_profile_ref: String,
    pub task_class: String,
    pub usable_context_budget_tokens: u32,
    pub priority: LeasePriority,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_trajectory_tokens: Option<u32>,
}

/// Each part carries its own decision or conservative fallback.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
#[cfg_attr(feature = "contract-schema", schemars(rename = "HarnessPart_{T}"))]
pub enum HarnessPart<T> {
    Decided {
        value: T,
    },
    Abstained {
        #[serde(deserialize_with = "deserialize_missing_facts")]
        missing_fact: BoundedVec<String, 64>,
        conservative_default: T,
    },
}

impl<T> HarnessPart<T> {
    /// An abstention must name at least one missing fact.
    pub fn validate(&self) -> Result<(), String> {
        match self {
            Self::Decided { .. } => Ok(()),
            Self::Abstained { missing_fact, .. } => validate_missing_facts(missing_fact),
        }
    }

    fn selected(&self) -> &T {
        match self {
            Self::Decided { value } => value,
            Self::Abstained {
                conservative_default,
                ..
            } => conservative_default,
        }
    }
}

fn validate_missing_facts(facts: &BoundedVec<String, 64>) -> Result<(), String> {
    if facts.is_empty() || facts.iter().any(|fact| fact.trim().is_empty()) {
        return Err("harness abstention must name each missing fact".into());
    }
    Ok(())
}

fn deserialize_missing_facts<'de, D>(deserializer: D) -> Result<BoundedVec<String, 64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let facts = BoundedVec::deserialize(deserializer)?;
    validate_missing_facts(&facts).map_err(serde::de::Error::custom)?;
    Ok(facts)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ContextPolicy {
    None,
    Elide,
    ElideThenSummarize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum PlanScaffold {
    On,
    Off,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ActionSpace {
    ToolSet,
    ShellOnly,
}

/// Exact evaluation data retained inside the statistical input digest.
/// A stored answer is not approval or a verified replay result. The evaluator
/// must resolve the profile pin and bind request fields to the outer params.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct HarnessConfigEvaluationEvidence {
    pub model_profile: crate::agent_component::ComponentDependency,
    pub request: HarnessConfigRequestBody,
    pub answer: HarnessConfigAnswer,
}

/// Q32 fractions reuse the checked unit rational with denominator 2^32.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct HarnessConfigAnswer {
    pub context_policy: HarnessPart<ContextPolicy>,
    #[serde(deserialize_with = "deserialize_q32_part")]
    pub soft_threshold: HarnessPart<UnitRationalWire>,
    #[serde(deserialize_with = "deserialize_q32_part")]
    pub hard_threshold: HarnessPart<UnitRationalWire>,
    #[serde(deserialize_with = "deserialize_q32_part")]
    pub recent_window_fraction: HarnessPart<UnitRationalWire>,
    pub plan_scaffold: HarnessPart<PlanScaffold>,
    pub action_space: HarnessPart<ActionSpace>,
    pub tool_subset: HarnessPart<BoundedVec<String, 256>>,
}

impl HarnessConfigAnswer {
    pub fn validate(&self) -> Result<(), String> {
        self.context_policy.validate()?;
        self.plan_scaffold.validate()?;
        self.action_space.validate()?;
        self.tool_subset.validate()?;
        for part in [
            &self.soft_threshold,
            &self.hard_threshold,
            &self.recent_window_fraction,
        ] {
            validate_q32_part(part)?;
        }
        Ok(())
    }
}

fn validate_q32_part(part: &HarnessPart<UnitRationalWire>) -> Result<(), String> {
    part.validate()?;
    if part.selected().denominator() != 1 << 32 {
        return Err("harness fraction must use Q32 denominator 2^32".into());
    }
    Ok(())
}

fn deserialize_q32_part<'de, D>(deserializer: D) -> Result<HarnessPart<UnitRationalWire>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let part = HarnessPart::deserialize(deserializer)?;
    validate_q32_part(&part).map_err(serde::de::Error::custom)?;
    Ok(part)
}

/// Why the plan-scaffold rule selected its part; the served explanation can
/// retain this reason alongside the measured inputs without inventing evidence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlanScaffoldReason {
    Accuracy,
    Cost,
    NoGainAblation,
    RetainScaffold,
    MissingEvidence,
}

/// Pure rule result, retaining the exact evaluations used by the decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanScaffoldDecision {
    pub part: HarnessPart<PlanScaffold>,
    pub reason: PlanScaffoldReason,
    pub evaluation_refs: Vec<ObservationRef>,
}

/// Select a scaffold from measured stopping behaviour and, for balanced
/// behaviour, a measured no-gain ablation. `true` means the cited ablation
/// observed no gain from the scaffold. Unknown evidence is never `false`.
pub fn decide_plan_scaffold(
    stopping: Option<&MeasuredFact<StoppingBehaviour>>,
    no_gain_ablation: Option<&MeasuredFact<bool>>,
) -> Result<PlanScaffoldDecision, String> {
    if let Some(fact) = stopping {
        fact.validate()?;
    }
    if let Some(fact) = no_gain_ablation {
        fact.validate()?;
    }
    let Some(stopping) = stopping else {
        return Ok(scaffold_abstention("stoppingBehaviour", Vec::new()));
    };
    let mut evaluation_refs = vec![stopping.evaluation_run_ref.clone()];
    let (value, reason) = match stopping.value {
        StoppingBehaviour::StopsEarly => (PlanScaffold::On, PlanScaffoldReason::Accuracy),
        StoppingBehaviour::OverVerifies => (PlanScaffold::On, PlanScaffoldReason::Cost),
        StoppingBehaviour::Balanced => {
            let Some(ablation) = no_gain_ablation else {
                return Ok(scaffold_abstention(
                    "planScaffoldNoGainAblation",
                    evaluation_refs,
                ));
            };
            evaluation_refs.push(ablation.evaluation_run_ref.clone());
            if ablation.value {
                (PlanScaffold::Off, PlanScaffoldReason::NoGainAblation)
            } else {
                (PlanScaffold::On, PlanScaffoldReason::RetainScaffold)
            }
        }
    };
    Ok(PlanScaffoldDecision {
        part: HarnessPart::Decided { value },
        reason,
        evaluation_refs,
    })
}

fn scaffold_abstention(
    missing: &str,
    evaluation_refs: Vec<ObservationRef>,
) -> PlanScaffoldDecision {
    PlanScaffoldDecision {
        part: HarnessPart::Abstained {
            missing_fact: BoundedVec::new(vec![missing.to_string()]).expect("one missing fact"),
            conservative_default: PlanScaffold::On,
        },
        reason: PlanScaffoldReason::MissingEvidence,
        evaluation_refs,
    }
}

/// Names of the facts the action-space rule still lacks, in reporting order.
fn missing_action_space_facts(
    proficiency_missing: bool,
    shell_centric_missing: bool,
) -> Vec<String> {
    let mut missing = Vec::new();
    if proficiency_missing {
        missing.push("shellProficiency".to_string());
    }
    if shell_centric_missing {
        missing.push("shellCentric".to_string());
    }
    missing
}

/// Resolve only from the supplied measured proficiency and resolved task fact.
/// Invalid evaluation references are refused, using the measured-fact owner.
pub fn decide_action_space(
    proficiency: Option<&MeasuredFact<ShellProficiency>>,
    shell_centric: Option<bool>,
) -> Result<HarnessPart<ActionSpace>, String> {
    if let Some(fact) = proficiency {
        fact.validate()?;
    }
    let missing = missing_action_space_facts(proficiency.is_none(), shell_centric.is_none());
    if !missing.is_empty() {
        return Ok(HarnessPart::Abstained {
            missing_fact: BoundedVec::new(missing).expect("at most two missing facts"),
            conservative_default: ActionSpace::ToolSet,
        });
    }
    let shell_only = proficiency.map(|fact| fact.value) == Some(ShellProficiency::High)
        && shell_centric == Some(true);
    Ok(HarnessPart::Decided {
        value: if shell_only {
            ActionSpace::ShellOnly
        } else {
            ActionSpace::ToolSet
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::agent_component::ObservationRef;

    fn measured<T>(value: T, evaluation_id: &str) -> MeasuredFact<T> {
        MeasuredFact {
            value,
            evaluation_run_ref: ObservationRef {
                evaluation_id: evaluation_id.into(),
                digest: format!("digest:{evaluation_id}"),
            },
        }
    }

    // spec: EG-DECISION-ENGINE-R128.6
    #[test]
    fn plan_scaffold_rules() {
        let ablation = measured(true, "eval:ablation");
        for (behaviour, reason) in [
            (StoppingBehaviour::StopsEarly, PlanScaffoldReason::Accuracy),
            (StoppingBehaviour::OverVerifies, PlanScaffoldReason::Cost),
        ] {
            let stopping = measured(behaviour, "eval:stopping");
            for evidence in [None, Some(&ablation)] {
                let result = decide_plan_scaffold(Some(&stopping), evidence).unwrap();
                assert_eq!(
                    result.part,
                    HarnessPart::Decided {
                        value: PlanScaffold::On
                    }
                );
                assert_eq!(result.reason, reason);
                assert_eq!(
                    result.evaluation_refs,
                    vec![stopping.evaluation_run_ref.clone()]
                );
            }
        }
        let stopping = measured(StoppingBehaviour::Balanced, "eval:balanced");
        for (no_gain, value, reason) in [
            (true, PlanScaffold::Off, PlanScaffoldReason::NoGainAblation),
            (false, PlanScaffold::On, PlanScaffoldReason::RetainScaffold),
        ] {
            let ablation = measured(no_gain, "eval:ablation");
            let result = decide_plan_scaffold(Some(&stopping), Some(&ablation)).unwrap();
            assert_eq!(result.part, HarnessPart::Decided { value });
            assert_eq!(result.reason, reason);
            assert_eq!(
                result.evaluation_refs,
                vec![
                    stopping.evaluation_run_ref.clone(),
                    ablation.evaluation_run_ref
                ]
            );
        }
    }

    // spec: EG-DECISION-ENGINE-R128.6
    #[test]
    fn plan_scaffold_missing_evidence_abstains() {
        let stopping = measured(StoppingBehaviour::Balanced, "eval:balanced");
        let ablation = measured(true, "eval:ablation");
        for (behaviour, evidence, missing, references) in [
            (None, None, "stoppingBehaviour", vec![]),
            (None, Some(&ablation), "stoppingBehaviour", vec![]),
            (
                Some(&stopping),
                None,
                "planScaffoldNoGainAblation",
                vec![stopping.evaluation_run_ref.clone()],
            ),
        ] {
            let result = decide_plan_scaffold(behaviour, evidence).unwrap();
            assert_eq!(
                result.part,
                HarnessPart::Abstained {
                    missing_fact: BoundedVec::new(vec![missing.into()]).unwrap(),
                    conservative_default: PlanScaffold::On,
                }
            );
            assert_eq!(result.reason, PlanScaffoldReason::MissingEvidence);
            assert_eq!(result.evaluation_refs, references);
            result.part.validate().unwrap();
        }
    }

    // spec: EG-DECISION-ENGINE-R128.6
    #[test]
    fn plan_scaffold_refuses_invalid_evaluation_references() {
        let stopping = measured(StoppingBehaviour::Balanced, "eval:balanced");
        let ablation = measured(true, "eval:ablation");
        for missing_id in [true, false] {
            let mut invalid_stopping = stopping.clone();
            let mut invalid_ablation = ablation.clone();
            for reference in [
                &mut invalid_stopping.evaluation_run_ref,
                &mut invalid_ablation.evaluation_run_ref,
            ] {
                if missing_id {
                    reference.evaluation_id.clear();
                } else {
                    reference.digest.clear();
                }
            }
            assert!(decide_plan_scaffold(Some(&invalid_stopping), Some(&ablation)).is_err());
            assert!(decide_plan_scaffold(Some(&stopping), Some(&invalid_ablation)).is_err());
        }
    }

    fn request_wire() -> serde_json::Value {
        serde_json::json!({
            "model_profile_ref": "urn:model:profile",
            "task_class": "urn:task:coding",
            "usable_context_budget_tokens": 8192,
            "priority": "interactive",
            "expected_trajectory_tokens": 4096
        })
    }

    // spec: EG-DECISION-ENGINE-R128.2
    #[test]
    fn body_requires_every_input() {
        let wire = request_wire();
        let body: HarnessConfigRequestBody = serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(serde_json::to_value(body).unwrap(), wire);
        for field in [
            "model_profile_ref",
            "task_class",
            "usable_context_budget_tokens",
            "priority",
        ] {
            let mut missing = wire.clone();
            missing.as_object_mut().unwrap().remove(field);
            assert!(
                serde_json::from_value::<HarnessConfigRequestBody>(missing).is_err(),
                "{field}"
            );
            let mut null = wire.clone();
            null[field] = serde_json::Value::Null;
            assert!(
                serde_json::from_value::<HarnessConfigRequestBody>(null).is_err(),
                "{field}"
            );
        }
    }

    // spec: EG-DECISION-ENGINE-R128.2
    #[test]
    fn body_allows_optional_trajectory_omission() {
        let mut wire = request_wire();
        wire.as_object_mut()
            .unwrap()
            .remove("expected_trajectory_tokens");
        let body: HarnessConfigRequestBody = serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(body.expected_trajectory_tokens, None);
        assert_eq!(serde_json::to_value(body).unwrap(), wire);
        wire["expected_trajectory_tokens"] = serde_json::Value::Null;
        assert_eq!(
            serde_json::from_value::<HarnessConfigRequestBody>(wire)
                .unwrap()
                .expected_trajectory_tokens,
            None
        );
    }

    // spec: EG-DECISION-ENGINE-R128.2
    #[test]
    fn body_checks_token_boundaries_and_priority_vocabulary() {
        for field in ["usable_context_budget_tokens", "expected_trajectory_tokens"] {
            for valid in [0, u32::MAX] {
                let mut wire = request_wire();
                wire[field] = valid.into();
                let body: HarnessConfigRequestBody = serde_json::from_value(wire.clone()).unwrap();
                assert_eq!(serde_json::to_value(body).unwrap(), wire);
            }
            for invalid in [
                serde_json::json!(-1),
                serde_json::json!(4294967296_u64),
                serde_json::json!(1.5),
                serde_json::json!("1"),
                serde_json::json!(true),
            ] {
                let mut wire = request_wire();
                wire[field] = invalid;
                assert!(
                    serde_json::from_value::<HarnessConfigRequestBody>(wire).is_err(),
                    "{field}"
                );
            }
        }
        for priority in [
            "interactive",
            "orchestration",
            "hydration",
            "background_ingestion",
        ] {
            let mut wire = request_wire();
            wire["priority"] = priority.into();
            let body: HarnessConfigRequestBody = serde_json::from_value(wire.clone()).unwrap();
            assert_eq!(serde_json::to_value(body).unwrap(), wire);
        }
        for field in ["model_profile_ref", "task_class", "priority"] {
            let mut wire = request_wire();
            wire[field] = serde_json::json!(1);
            assert!(
                serde_json::from_value::<HarnessConfigRequestBody>(wire).is_err(),
                "{field}"
            );
        }
        for invalid in ["", "urgent", "Interactive"] {
            let mut wire = request_wire();
            wire["priority"] = invalid.into();
            assert!(serde_json::from_value::<HarnessConfigRequestBody>(wire).is_err());
        }
        let mut wire = request_wire();
        wire["unexpected"] = true.into();
        assert!(serde_json::from_value::<HarnessConfigRequestBody>(wire).is_err());
    }

    fn decided<T>(value: T) -> HarnessPart<T> {
        HarnessPart::Decided { value }
    }
    fn abstained<T>(value: T) -> HarnessPart<T> {
        HarnessPart::Abstained {
            missing_fact: BoundedVec::new(vec!["requiredFact".into()]).unwrap(),
            conservative_default: value,
        }
    }
    fn answer() -> HarnessConfigAnswer {
        HarnessConfigAnswer {
            context_policy: decided(ContextPolicy::None),
            soft_threshold: decided(UnitRationalWire::new(0, 1 << 32).unwrap()),
            hard_threshold: decided(UnitRationalWire::new(1 << 32, 1 << 32).unwrap()),
            recent_window_fraction: decided(UnitRationalWire::new(1 << 31, 1 << 32).unwrap()),
            plan_scaffold: decided(PlanScaffold::Off),
            action_space: decided(ActionSpace::ToolSet),
            tool_subset: decided(BoundedVec::default()),
        }
    }
    fn round_trip(answer: HarnessConfigAnswer) {
        assert!(answer.validate().is_ok());
        assert_eq!(
            serde_json::from_value::<HarnessConfigAnswer>(serde_json::to_value(&answer).unwrap())
                .unwrap(),
            answer
        );
    }

    // spec: EG-DECISION-ENGINE-R128.3
    #[test]
    fn answer_round_trips_through_serde() {
        for context in [
            ContextPolicy::None,
            ContextPolicy::Elide,
            ContextPolicy::ElideThenSummarize,
        ] {
            for plan in [PlanScaffold::On, PlanScaffold::Off] {
                for action in [ActionSpace::ToolSet, ActionSpace::ShellOnly] {
                    let mut value = answer();
                    value.context_policy = decided(context);
                    value.plan_scaffold = decided(plan);
                    value.action_space = decided(action);
                    round_trip(value.clone());
                    value.context_policy = abstained(context);
                    value.plan_scaffold = abstained(plan);
                    value.action_space = abstained(action);
                    value.soft_threshold = abstained(*value.soft_threshold.selected());
                    value.hard_threshold = abstained(*value.hard_threshold.selected());
                    value.recent_window_fraction =
                        abstained(*value.recent_window_fraction.selected());
                    value.tool_subset =
                        abstained(BoundedVec::new(vec!["tool".into(); 256]).unwrap());
                    round_trip(value);
                }
            }
        }
    }

    // spec: EG-DECISION-ENGINE-R128.3
    #[test]
    fn answer_rejects_invalid_fractions_and_bounds() {
        for field in ["soft_threshold", "hard_threshold", "recent_window_fraction"] {
            for bad in [
                serde_json::json!({"numerator":-1,"denominator":4294967296u64}),
                serde_json::json!({"numerator":4294967297u64,"denominator":4294967296u64}),
                serde_json::json!({"numerator":0,"denominator":0}),
                serde_json::json!({"numerator":1,"denominator":2}),
            ] {
                for status in ["decided", "abstained"] {
                    let mut wire = serde_json::to_value(answer()).unwrap();
                    wire[field] = if status == "decided" {
                        serde_json::json!({"status":status,"value":bad})
                    } else {
                        serde_json::json!({"status":status,"missing_fact":["threshold"],"conservative_default":bad})
                    };
                    assert!(serde_json::from_value::<HarnessConfigAnswer>(wire).is_err());
                }
            }
        }
        let mut wire = serde_json::to_value(answer()).unwrap();
        wire["tool_subset"]["value"] = serde_json::json!(vec!["tool"; 257]);
        assert!(serde_json::from_value::<HarnessConfigAnswer>(wire).is_err());
        assert!(serde_json::from_value::<HarnessPart<ActionSpace>>(serde_json::json!({"status":"abstained","missing_fact":vec!["fact";65],"conservative_default":"tool_set"})).is_err());
    }

    // spec: EG-DECISION-ENGINE-R128.3
    #[test]
    fn abstention_requires_named_missing_facts() {
        for facts in [vec![], vec![""], vec![" \t"], vec!["shellCentric", ""]] {
            let part = HarnessPart::Abstained {
                missing_fact: BoundedVec::new(facts.into_iter().map(str::to_string).collect())
                    .unwrap(),
                conservative_default: ActionSpace::ToolSet,
            };
            assert!(part.validate().is_err());
            let wire = serde_json::to_value(&part).unwrap();
            assert!(serde_json::from_value::<HarnessPart<ActionSpace>>(wire.clone()).is_err());
            let mut value = answer();
            value.action_space = part;
            assert!(value.validate().is_err());
            let mut wire_answer = serde_json::to_value(answer()).unwrap();
            wire_answer["action_space"] = wire;
            assert!(serde_json::from_value::<HarnessConfigAnswer>(wire_answer).is_err());
        }
    }

    fn proficiency_fact(
        proficiency: Option<ShellProficiency>,
    ) -> Option<MeasuredFact<ShellProficiency>> {
        proficiency.map(|value| measured(value, "eval:42"))
    }

    fn expected_action_space(
        proficiency: Option<ShellProficiency>,
        shell_centric: Option<bool>,
    ) -> HarnessPart<ActionSpace> {
        let missing = missing_action_space_facts(proficiency.is_none(), shell_centric.is_none());
        if !missing.is_empty() {
            return HarnessPart::Abstained {
                missing_fact: BoundedVec::new(missing).unwrap(),
                conservative_default: ActionSpace::ToolSet,
            };
        }
        let shell_only = proficiency == Some(ShellProficiency::High) && shell_centric == Some(true);
        decided(if shell_only {
            ActionSpace::ShellOnly
        } else {
            ActionSpace::ToolSet
        })
    }

    // spec: EG-DECISION-ENGINE-R128.7
    #[test]
    fn shell_only_needs_both_facts() {
        for proficiency in [
            None,
            Some(ShellProficiency::Low),
            Some(ShellProficiency::Medium),
            Some(ShellProficiency::High),
        ] {
            for shell_centric in [None, Some(false), Some(true)] {
                let fact = proficiency_fact(proficiency);
                let actual = decide_action_space(fact.as_ref(), shell_centric).unwrap();
                assert_eq!(actual, expected_action_space(proficiency, shell_centric));
            }
        }
        let malformed = measured(ShellProficiency::High, "");
        assert!(decide_action_space(Some(&malformed), Some(true)).is_err());
    }

    fn harness_input_digest(inputs: &super::super::StatisticalInputs) -> String {
        use super::super::{QuestionKind, QuestionSafety, StatisticalQuestion};
        let question = StatisticalQuestion {
            question_id: "legacy-question".into(),
            kind: QuestionKind::Route,
            safety: QuestionSafety::Ordinary,
        };
        crate::decision::digest::digest_text(
            "eg/decision-statistical-inputs/v1",
            &(&question, inputs),
        )
    }

    fn legacy_inputs() -> (&'static str, super::super::StatisticalInputs) {
        let wire = concat!(
            "{\"feature_schema\":{\"component_id\":\"schema\",\"kind\":\"feature_schema\",\"definition_digest\":\"sh",
            "a256:old\"},\"head\":null,\"policy\":{\"policy\":\"default\"},\"policy_digest\":\"sha256:policy\",\"para",
            "ms\":[],\"classification_rules\":null,\"feature_matrix\":{\"matrix\":\"inline\",\"candidate_ids\":[],",
            "\"feature_names\":[],\"scale\":\"q32\",\"values\":[]},\"shortlist\":{\"ann_recall_mode\":null,\"now_ms\"",
            ":1,\"embedder_model_digest\":null},\"exploration\":null}"
        );
        (wire, serde_json::from_str(wire).unwrap())
    }

    // spec: EG-DECISION-ENGINE-R128.13.1
    // spec: EG-DECISION-ENGINE-R128.13.1.1
    #[test]
    fn absent_harness_evidence_preserves_legacy_bytes_and_digest() {
        let (wire, inputs) = legacy_inputs();
        assert!(inputs.harness_evaluation.is_none());
        assert_eq!(serde_json::to_string(&inputs).unwrap(), wire);
        assert_eq!(
            harness_input_digest(&inputs),
            "sha256:54f4cc951ca6be7ff7fbfc38b21c859705ef95bc79c38b9197be493377cc1bfe"
        );
    }

    // spec: EG-DECISION-ENGINE-R128.13.1
    // spec: EG-DECISION-ENGINE-R128.13.1.1
    #[test]
    fn harness_evidence_roundtrips_and_changes_input_digest() {
        use crate::agent_component::{AgentComponentKind, ComponentDependency};
        let (_, mut inputs) = legacy_inputs();
        let before = harness_input_digest(&inputs);
        let request: HarnessConfigRequestBody = serde_json::from_value(request_wire()).unwrap();
        let mut full_answer = answer();
        full_answer.tool_subset = HarnessPart::Abstained {
            missing_fact: BoundedVec::new(vec!["required_capabilities".into()]).unwrap(),
            conservative_default: BoundedVec::default(),
        };
        inputs.harness_evaluation = Some(HarnessConfigEvaluationEvidence {
            model_profile: ComponentDependency {
                component_id: request.model_profile_ref.clone(),
                kind: AgentComponentKind::ModelProfile,
                definition_digest: "sha256:pinned-profile".into(),
            },
            request,
            answer: full_answer,
        });
        let wire = serde_json::to_vec(&inputs).unwrap();
        let restored: super::super::StatisticalInputs = serde_json::from_slice(&wire).unwrap();
        assert_eq!(inputs, restored);
        let sealed = harness_input_digest(&inputs);
        assert_ne!(before, sealed);
        inputs
            .harness_evaluation
            .as_mut()
            .unwrap()
            .answer
            .plan_scaffold = decided(PlanScaffold::On);
        assert_ne!(sealed, harness_input_digest(&inputs));
    }
}
