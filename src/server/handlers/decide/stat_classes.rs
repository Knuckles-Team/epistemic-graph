//! Rule-derived capability classes for library candidates (EH-200).
//!
//! eg-rdf's `classify_components` derives `eg:profile/*` traits and native
//! `eg:capability/*` terms from each component's DECLARED typed facts, each
//! with the rule that fired. A derived class is only as strong as the facts it
//! reads, so it enters a decision as a CLAIM premise naming the rule, never as
//! a declared classification. The rule-set identity -- the rule names in
//! order, the `ClassificationPolicy` thresholds and the native ontology digest
//! -- is recorded with the decision, and a record replays only under the same
//! identity.

use std::collections::BTreeMap;

use eg_types::agent_component::AgentComponentEntry;

/// The classes each candidate was derived to belong to, as `(class, rule)`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(super) struct DerivedClasses {
    pub(super) by_component: BTreeMap<String, Vec<(String, String)>>,
    /// The rule-set identity; `None` in a build without the rule engine.
    pub(super) rules: Option<String>,
}

#[cfg(feature = "owl")]
mod imp {
    use super::*;
    use eg_rdf::rules::capability::{
        classify_components, ClassificationPolicy, ProfiledComponent, DERIVATION_RULES,
    };

    /// Domain of the classification rule-set identity.
    const RULES_DOMAIN: &str = "eg/decide-classification-rules/v1";

    fn policy() -> ClassificationPolicy {
        ClassificationPolicy::default()
    }

    /// The identity of the rules and thresholds classes are derived under.
    pub(in super::super) fn current_rules() -> Option<String> {
        let policy = policy();
        let names: Vec<&str> = DERIVATION_RULES.iter().map(|rule| rule.rule).collect();
        Some(eg_types::decision::digest::digest_text(
            RULES_DOMAIN,
            &(
                names,
                policy.long_context_tokens,
                policy.low_latency_p95_ms,
                eg_types::agent_ontology::ontology_digest(),
            ),
        ))
    }

    /// Derive the classes of every entry from its declared facts.
    pub(in super::super) fn derive(entries: &[AgentComponentEntry]) -> DerivedClasses {
        let components: Vec<ProfiledComponent<'_>> = entries
            .iter()
            .map(|entry| ProfiledComponent {
                component_id: &entry.component_id,
                facts: &entry.facts,
            })
            .collect();
        let derived = classify_components(
            &components,
            &policy(),
            &eg_rdf::owl::Ontology::default(),
            &eg_rdf::rules::RuleSet::default(),
        );
        let by_component = derived
            .components
            .into_iter()
            .map(|(id, classes)| {
                let mut pairs: Vec<(String, String)> = classes
                    .into_iter()
                    .map(|class| (class.class, class.proof.rule))
                    .collect();
                pairs.sort();
                pairs.dedup();
                (id, pairs)
            })
            .collect();
        DerivedClasses {
            by_component,
            rules: current_rules(),
        }
    }
}

#[cfg(not(feature = "owl"))]
mod imp {
    use super::*;

    pub(in super::super) fn current_rules() -> Option<String> {
        None
    }

    pub(in super::super) fn derive(_entries: &[AgentComponentEntry]) -> DerivedClasses {
        DerivedClasses::default()
    }
}

pub(super) use imp::{current_rules, derive};
