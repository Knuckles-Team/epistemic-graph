//! Assembly evidence depends on typed premises, never on suggestive labels.

use eg_types::decision::{DecisionOutcome, EvidenceClass, PremiseClass, PremiseProvenance};

use super::super::{assemble, replay_check};
use super::fixture::*;

// spec: EG-DECISION-ENGINE-R121.1
#[test]
fn assembly_evidence_uses_typed_premises_not_labels() {
    for (class, expected) in [
        (PremiseClass::Claim, EvidenceClass::Claim),
        (PremiseClass::Observation, EvidenceClass::Observation),
        (PremiseClass::Proof, EvidenceClass::Proof),
    ] {
        for label in ["claim", "observation", "proof"] {
            let mut library = research_library();
            for candidate in &mut library {
                // Keep provenance variants consistent with the typed premise;
                // the free-text identifiers are deliberately misleading.
                let provenance = match class {
                    PremiseClass::Observation => PremiseProvenance::Observation {
                        evaluation_id: format!("urn:{label}:evaluation"),
                    },
                    PremiseClass::Proof => PremiseProvenance::NativeOntology {
                        ontology_digest: digest_of(label),
                    },
                    _ => PremiseProvenance::Publisher {
                        component_id: format!("urn:{label}:publisher"),
                        definition_digest: digest_of(label),
                    },
                };
                let mut premises = candidate.fact_premises.as_slice().to_vec();
                for premise in &mut premises {
                    premise.class = class;
                    premise.fact = format!("{label}_evidence_class");
                    premise.subject = format!("urn:{label}:subject");
                    premise.provenance = provenance.clone();
                }
                candidate.fact_premises = bounded(premises);
            }
            // No capability requirement means no independent claim-class
            // coverage edge can mask a broken premise-class aggregation.
            let answer = assemble(inputs(request(&[], &[]), library), identity())
                .expect("the bounded assembly succeeds");
            assert!(matches!(
                answer.record.outcome,
                DecisionOutcome::Solved { .. }
            ));
            assert!(answer.record.derivations.is_empty());
            assert!(!answer.record.premises.is_empty());
            assert!(answer.record.premises.iter().all(|p| p.class == class));
            assert_eq!(answer.record.evidence_class, expected, "{class:?}/{label}");
            replay_check(&answer.record).expect("typed evidence survives record replay");
        }
    }
}
