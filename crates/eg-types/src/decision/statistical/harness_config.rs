//! Producer types and pure action-space selection for R128.3 and R128.7.
//! Profile resolution, question dispatch and persistence are separate leaves.

use serde::{Deserialize, Serialize};

use crate::agent_component::{MeasuredFact, ShellProficiency};
use crate::contract::BoundedVec;
use crate::decision::numeric::UnitRationalWire;

/// Each part carries its own decision or conservative fallback.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum HarnessPart<T> {
    Decided {
        value: T,
    },
    Abstained {
        missing_fact: BoundedVec<String, 64>,
        conservative_default: T,
    },
}

impl<T> HarnessPart<T> {
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

/// Resolve only from the supplied measured proficiency and resolved task fact.
/// Invalid evaluation references are refused, using the measured-fact owner.
pub fn decide_action_space(
    proficiency: Option<&MeasuredFact<ShellProficiency>>,
    shell_centric: Option<bool>,
) -> Result<HarnessPart<ActionSpace>, String> {
    if let Some(fact) = proficiency {
        fact.validate()?;
    }
    let mut missing = Vec::new();
    if proficiency.is_none() {
        missing.push("shellProficiency".to_string());
    }
    if shell_centric.is_none() {
        missing.push("shellCentric".to_string());
    }
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
                let fact = proficiency.map(|value| MeasuredFact {
                    value,
                    evaluation_run_ref: ObservationRef {
                        evaluation_id: "eval:42".into(),
                        digest: "opaque".into(),
                    },
                });
                let actual = decide_action_space(fact.as_ref(), shell_centric).unwrap();
                let mut missing = Vec::new();
                if proficiency.is_none() {
                    missing.push("shellProficiency".to_string());
                }
                if shell_centric.is_none() {
                    missing.push("shellCentric".to_string());
                }
                let expected = if !missing.is_empty() {
                    HarnessPart::Abstained {
                        missing_fact: BoundedVec::new(missing).unwrap(),
                        conservative_default: ActionSpace::ToolSet,
                    }
                } else {
                    decided(
                        if proficiency == Some(ShellProficiency::High)
                            && shell_centric == Some(true)
                        {
                            ActionSpace::ShellOnly
                        } else {
                            ActionSpace::ToolSet
                        },
                    )
                };
                assert_eq!(actual, expected);
            }
        }
        let malformed = MeasuredFact {
            value: ShellProficiency::High,
            evaluation_run_ref: ObservationRef {
                evaluation_id: String::new(),
                digest: "opaque".into(),
            },
        };
        assert!(decide_action_space(Some(&malformed), Some(true)).is_err());
    }
}
