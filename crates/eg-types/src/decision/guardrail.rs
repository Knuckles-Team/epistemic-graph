//! Guardrail entailment: which governance rules, standards and SHACL shapes
//! apply to a task class by ontology entailment (EG-DECISION-ENGINE-R127).
//!
//! A [`GuardrailRule`] is data, not a native ontology term: it names the task
//! class it governs (`applies_to_class`) by IRI. [`entailed_guardrails`]
//! reuses the same subclass-entailment primitive
//! [`crate::agent_ontology::broader_chain`] that
//! [`super::derivation::coverage_chain`] already proves for capability
//! coverage, applied here to task-class subsumption instead: a rule applies
//! to a task whenever the task IS the rule's class, or is subsumed by it, and
//! each answer carries the exact chain of `narrower -> broader` steps that
//! proves it, so a reader can re-check the entailment rather than trust a
//! boolean. The answer is advisory only; enforcement stays with the owning
//! policy gate (EG-DECISION-ENGINE-R127).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::agent_ontology::broader_chain;

/// One governance rule, standard or SHACL shape pack, and the task class it
/// governs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct GuardrailRule {
    pub rule_id: String,
    /// The task class (an IRI) this rule governs; entailed onto every
    /// narrower task class as well.
    pub applies_to_class: String,
    /// The schema package generation that introduced this rule.
    pub source_generation: String,
}

/// Attribute a `GuardrailRule` component carries its governed task class in.
pub const GUARDRAIL_APPLIES_TO_CLASS_ATTRIBUTE: &str = "applies_to_class";
/// Attribute a `GuardrailRule` component carries its source generation in.
pub const GUARDRAIL_SOURCE_GENERATION_ATTRIBUTE: &str = "source_generation";

impl GuardrailRule {
    /// The attributes a `GuardrailRule` component is published with
    /// (EG-DECISION-ENGINE-R127.2.1); the component id is `rule_id`.
    pub fn to_attributes(&self) -> BTreeMap<String, String> {
        BTreeMap::from([
            (
                GUARDRAIL_APPLIES_TO_CLASS_ATTRIBUTE.to_string(),
                self.applies_to_class.clone(),
            ),
            (
                GUARDRAIL_SOURCE_GENERATION_ATTRIBUTE.to_string(),
                self.source_generation.clone(),
            ),
        ])
    }

    /// Decode a stored `GuardrailRule` component back into the rule it
    /// carries, refusing a malformed one by name: a missing or blank
    /// attribute, or an `applies_to_class` that is not an IRI.
    pub fn from_attributes(
        rule_id: &str,
        attributes: &BTreeMap<String, String>,
    ) -> Result<Self, String> {
        let field = |name: &str| -> Result<String, String> {
            match attributes.get(name).map(|value| value.trim()) {
                Some(value) if !value.is_empty() => Ok(value.to_string()),
                _ => Err(format!(
                    "MALFORMED_GUARDRAIL_RULE: guardrail rule '{rule_id}' needs a non-empty \
                     '{name}' attribute"
                )),
            }
        };
        let applies_to_class = field(GUARDRAIL_APPLIES_TO_CLASS_ATTRIBUTE)?;
        if !is_iri(&applies_to_class) {
            return Err(format!(
                "MALFORMED_GUARDRAIL_RULE: guardrail rule '{rule_id}' applies_to_class \
                 '{applies_to_class}' is not an IRI"
            ));
        }
        Ok(Self {
            rule_id: rule_id.to_string(),
            applies_to_class,
            source_generation: field(GUARDRAIL_SOURCE_GENERATION_ATTRIBUTE)?,
        })
    }
}

/// An IRI here is a scheme- or prefix-qualified name with no whitespace
/// (`https://...`, `urn:...`, `eg:TaskClass`).
fn is_iri(value: &str) -> bool {
    value
        .split_once(':')
        .is_some_and(|(scheme, rest)| !scheme.is_empty() && !rest.is_empty())
        && !value.chars().any(char::is_whitespace)
}

/// One step of an entailment path: `narrower` is a native subclass of
/// `broader`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct EntailmentStep {
    pub narrower: String,
    pub broader: String,
}

/// One rule entailed onto a task, with the path that proves it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct EntailedGuardrail {
    pub rule_id: String,
    pub source_generation: String,
    /// Empty when `task_iri` IS the rule's own class; otherwise the
    /// `narrower -> broader` steps from `task_iri` up to it.
    pub entailment_path: Vec<EntailmentStep>,
}

/// Which of `rules` apply to `task_iri`, by ontology entailment
/// (EG-DECISION-ENGINE-R127): a rule whose `applies_to_class` is `task_iri`
/// itself, or an ancestor of it, is entailed, carrying the path that proves
/// it. A rule for an unrelated class (neither `task_iri` nor an ancestor) is
/// never entailed. Pure and stable: the same `task_iri` and `rules` always
/// produce the same answer, so premises stay stable across a restart.
pub fn entailed_guardrails(task_iri: &str, rules: &[GuardrailRule]) -> Vec<EntailedGuardrail> {
    rules
        .iter()
        .filter_map(|rule| {
            let path = broader_chain(task_iri, &rule.applies_to_class)?;
            Some(EntailedGuardrail {
                rule_id: rule.rule_id.clone(),
                source_generation: rule.source_generation.clone(),
                entailment_path: path
                    .into_iter()
                    .map(|(narrower, broader)| EntailmentStep {
                        narrower: narrower.to_string(),
                        broader: broader.to_string(),
                    })
                    .collect(),
            })
        })
        .collect()
}

/// EG-DECISION-ENGINE-R127.2: the guardrail-entailment query's single entry
/// point -- the one call a served method, backed by a stored
/// [`GuardrailRule`] set, and its generated client invoke. Depends on
/// EG-DECISION-ENGINE-R127.1's [`entailed_guardrails`]; validates the task
/// IRI itself (something a pure function need not do) before delegating,
/// refusing an empty one rather than silently answering with an empty rule
/// set.
pub fn guardrail_entailment_query(
    task_iri: &str,
    rules: &[GuardrailRule],
) -> Result<Vec<EntailedGuardrail>, String> {
    if task_iri.is_empty() {
        return Err("guardrail entailment requires a non-empty task IRI".to_string());
    }
    Ok(entailed_guardrails(task_iri, rules))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_empty_task_iri_is_refused_rather_than_silently_answering_empty() {
        let rules = vec![rule("governance:audit-trail", "eg:task")];
        let outcome = guardrail_entailment_query("", &rules);
        assert!(
            outcome.is_err(),
            "an empty task IRI must refuse, not silently answer with no rules"
        );
    }

    fn rule(id: &str, applies_to: &str) -> GuardrailRule {
        GuardrailRule {
            rule_id: id.to_string(),
            applies_to_class: applies_to.to_string(),
            source_generation: "gen-1".to_string(),
        }
    }

    // spec: EG-DECISION-ENGINE-R127.1
    #[test]
    fn a_rule_on_the_task_root_is_entailed_onto_a_subclassed_task() {
        let rules = vec![rule("governance:audit-trail", "eg:task")];
        let entailed = entailed_guardrails("eg:task/research", &rules);
        assert_eq!(entailed.len(), 1);
        assert_eq!(entailed[0].rule_id, "governance:audit-trail");
        assert_eq!(entailed[0].source_generation, "gen-1");
        assert_eq!(
            entailed[0].entailment_path,
            vec![EntailmentStep {
                narrower: "eg:task/research".to_string(),
                broader: "eg:task".to_string(),
            }]
        );
    }

    #[test]
    fn a_rule_on_the_tasks_own_class_entails_with_an_empty_path() {
        let rules = vec![rule("governance:research-standard", "eg:task/research")];
        let entailed = entailed_guardrails("eg:task/research", &rules);
        assert_eq!(entailed.len(), 1);
        assert!(entailed[0].entailment_path.is_empty());
    }

    // spec: EG-DECISION-ENGINE-R127.1
    #[test]
    fn a_rule_on_an_unrelated_class_is_never_entailed() {
        let rules = vec![rule("governance:capability-only", "eg:capability")];
        assert!(entailed_guardrails("eg:task/research", &rules).is_empty());
    }

    // spec: EG-DECISION-ENGINE-R127.1
    #[test]
    fn the_same_inputs_always_produce_the_same_premises() {
        let rules = vec![rule("governance:audit-trail", "eg:task")];
        let first = entailed_guardrails("eg:task/research", &rules);
        let second = entailed_guardrails("eg:task/research", &rules);
        assert_eq!(first, second, "premises stay stable across a restart");
    }
}
