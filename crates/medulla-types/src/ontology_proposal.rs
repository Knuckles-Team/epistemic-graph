//! Ontology induction proposals and their admission scores
//! (MEDULLA-INGEST-WORKER-R002.1, R002.2).
//!
//! A proposal induced from unstructured text starts as a draft. It is scored
//! for property coverage and class redundancy; the ingest job body (R002.3)
//! admits it only when both checks pass.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

/// Admission state of an induced proposal.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ProposalState {
    #[default]
    Draft,
    Admitted,
}

/// One induced class and the property names it carries.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ProposedClass {
    pub name: String,
    #[serde(default)]
    pub properties: BTreeSet<String>,
}

/// One induced property and the class it belongs to.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ProposedProperty {
    pub name: String,
    pub domain: String,
}

/// Where a proposal came from.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ProposalProvenance {
    pub source_ref: String,
    pub induced_by: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct OntologyProposal {
    pub proposal_id: String,
    pub classes: Vec<ProposedClass>,
    pub properties: Vec<ProposedProperty>,
    #[serde(default)]
    pub state: ProposalState,
    pub provenance: ProposalProvenance,
}

impl OntologyProposal {
    /// A freshly induced proposal. It always starts as a draft.
    pub fn new(
        proposal_id: impl Into<String>,
        classes: Vec<ProposedClass>,
        properties: Vec<ProposedProperty>,
        provenance: ProposalProvenance,
    ) -> Self {
        Self {
            proposal_id: proposal_id.into(),
            classes,
            properties,
            state: ProposalState::Draft,
            provenance,
        }
    }
}

/// Admission thresholds. The defaults are starting values to be replaced by
/// measurement.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ProposalThresholds {
    pub min_coverage: f64,
    pub max_redundancy: f64,
}

impl Default for ProposalThresholds {
    fn default() -> Self {
        Self {
            min_coverage: 0.8,
            max_redundancy: 0.1,
        }
    }
}

/// The measured scores of one proposal.
#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ProposalScore {
    /// Share of the observed property names the proposal declares.
    pub coverage: f64,
    /// Share of classes whose property set duplicates an earlier class.
    pub redundancy: f64,
}

/// The first check a proposal failed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ProposalCheck {
    Coverage,
    Redundancy,
}

impl ProposalScore {
    /// `None` when both checks pass, otherwise the first failing check.
    pub fn failing_check(&self, thresholds: &ProposalThresholds) -> Option<ProposalCheck> {
        if self.coverage < thresholds.min_coverage {
            Some(ProposalCheck::Coverage)
        } else if self.redundancy > thresholds.max_redundancy {
            Some(ProposalCheck::Redundancy)
        } else {
            None
        }
    }
}

fn normalize(name: &str) -> String {
    name.trim().to_ascii_lowercase()
}

/// Score a proposal against the property names observed in the source text.
///
/// Coverage is the share of distinct observed names the proposal declares as
/// properties (1.0 when nothing was observed). Redundancy is the share of
/// classes that repeat an earlier class's name or non-empty property set.
pub fn score_proposal(proposal: &OntologyProposal, observed: &[String]) -> ProposalScore {
    let declared: BTreeSet<String> = proposal
        .properties
        .iter()
        .map(|p| normalize(&p.name))
        .chain(
            proposal
                .classes
                .iter()
                .flat_map(|c| c.properties.iter().map(|p| normalize(p))),
        )
        .collect();
    let observed: BTreeSet<String> = observed.iter().map(|o| normalize(o)).collect();
    let coverage = if observed.is_empty() {
        1.0
    } else {
        observed.intersection(&declared).count() as f64 / observed.len() as f64
    };

    let mut seen_names = BTreeSet::new();
    let mut seen_shapes: BTreeSet<BTreeSet<String>> = BTreeSet::new();
    let mut redundant = 0usize;
    for class in &proposal.classes {
        let shape: BTreeSet<String> = class.properties.iter().map(|p| normalize(p)).collect();
        let duplicate_name = !seen_names.insert(normalize(&class.name));
        let duplicate_shape = !shape.is_empty() && !seen_shapes.insert(shape);
        if duplicate_name || duplicate_shape {
            redundant += 1;
        }
    }
    let redundancy = if proposal.classes.is_empty() {
        0.0
    } else {
        redundant as f64 / proposal.classes.len() as f64
    };
    ProposalScore {
        coverage,
        redundancy,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn class(name: &str, props: &[&str]) -> ProposedClass {
        ProposedClass {
            name: name.into(),
            properties: props.iter().map(|p| p.to_string()).collect(),
        }
    }

    fn provenance() -> ProposalProvenance {
        ProposalProvenance {
            source_ref: "doc:1".into(),
            induced_by: "induction-v1".into(),
        }
    }

    fn observed(names: &[&str]) -> Vec<String> {
        names.iter().map(|n| n.to_string()).collect()
    }

    // spec: MEDULLA-INGEST-WORKER-R002.1
    #[test]
    fn proposal_round_trips_and_starts_as_draft() {
        let proposal = OntologyProposal::new(
            "p1",
            vec![class("Person", &["name", "born"])],
            vec![ProposedProperty {
                name: "name".into(),
                domain: "Person".into(),
            }],
            provenance(),
        );
        assert_eq!(proposal.state, ProposalState::Draft);
        let encoded = serde_json::to_value(&proposal).unwrap();
        assert_eq!(encoded["state"], "draft");
        let decoded: OntologyProposal = serde_json::from_value(encoded).unwrap();
        assert_eq!(decoded, proposal);
        let mut missing_state = serde_json::to_value(&proposal).unwrap();
        missing_state.as_object_mut().unwrap().remove("state");
        let decoded: OntologyProposal = serde_json::from_value(missing_state).unwrap();
        assert_eq!(decoded.state, ProposalState::Draft);
    }

    // spec: MEDULLA-INGEST-WORKER-R002.2
    #[test]
    fn score_proposal_checks_coverage_and_redundancy() {
        let thresholds = ProposalThresholds::default();
        let seen = observed(&["name", "born", "employer", "title", "city"]);

        let passing = OntologyProposal::new(
            "pass",
            vec![
                class("Person", &["name", "born", "city"]),
                class("Job", &["employer", "title"]),
            ],
            vec![],
            provenance(),
        );
        let score = score_proposal(&passing, &seen);
        assert_eq!(score.coverage, 1.0);
        assert_eq!(score.redundancy, 0.0);
        assert_eq!(score.failing_check(&thresholds), None);

        let thin = OntologyProposal::new(
            "thin",
            vec![class("Person", &["name", "born"])],
            vec![],
            provenance(),
        );
        let score = score_proposal(&thin, &seen);
        assert!((score.coverage - 0.4).abs() < 1e-9);
        assert_eq!(
            score.failing_check(&thresholds),
            Some(ProposalCheck::Coverage)
        );

        let redundant = OntologyProposal::new(
            "dup",
            vec![
                class("Person", &["name", "born", "city"]),
                class("Human", &["name", "born", "city"]),
                class("Job", &["employer", "title"]),
            ],
            vec![],
            provenance(),
        );
        let score = score_proposal(&redundant, &seen);
        assert_eq!(score.coverage, 1.0);
        assert!((score.redundancy - 1.0 / 3.0).abs() < 1e-9);
        assert_eq!(
            score.failing_check(&thresholds),
            Some(ProposalCheck::Redundancy)
        );

        // Boundary: exactly at both thresholds passes.
        let boundary = ProposalScore {
            coverage: 0.8,
            redundancy: 0.1,
        };
        assert_eq!(boundary.failing_check(&thresholds), None);
    }
}
