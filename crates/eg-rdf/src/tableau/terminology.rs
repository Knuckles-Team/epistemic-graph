//! Terminology-scoped reasoning: the schema question, without the ABox.

use std::collections::BTreeSet;

use oxrdf::Triple;

use super::{
    needs_tableau, parse_dl_ontology, reason_el_rl_ontology, reason_hybrid_ontology, DlOntology,
    DlReasoningResult,
};

/// Reason over the TERMINOLOGY only: the same engines and the same EL/RL pass over the
/// whole triple set (a class-level classification: it does not decide individual
/// assertions), but the tableau sees the TBox and role box without the ABox.
///
/// This is the question a schema read or restore asks — is the terminology coherent,
/// which classes are unsatisfiable — without paying for the individuals the schema
/// ships on every call. Their consistency is decided where schema enters a graph, by
/// [`super::abox_consistency_within`] (EH-355, operator ruling (c)).
pub fn reason_dl_terminology(triples: &[Triple]) -> DlReasoningResult {
    if needs_tableau(triples) {
        reason_hybrid_ontology(triples, terminology_only(parse_dl_ontology(triples)))
    } else {
        reason_el_rl_ontology(triples)
    }
}

/// The parsed ontology without its ABox: TBox GCIs, role box and class signature only.
fn terminology_only(ont: DlOntology) -> DlOntology {
    DlOntology {
        gcis: ont.gcis,
        sub_roles: ont.sub_roles,
        transitive: ont.transitive,
        classes: ont.classes,
        abox_types: Vec::new(),
        abox_roles: Vec::new(),
        same_as: Vec::new(),
        different_from: Vec::new(),
        individuals: BTreeSet::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::super::reason_dl;
    use super::*;
    use crate::mapping::parse_turtle;

    const DOCUMENT: &str = r#"
@prefix ex:  <http://example.org/> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix rdfs:<http://www.w3.org/2000/01/rdf-schema#> .
ex:Bad owl:equivalentClass [ owl:intersectionOf ( ex:B [ owl:complementOf ex:B ] ) ] .
ex:a rdf:type ex:Bad .
"#;

    /// The terminology scope keeps every TBox consequence — `Bad` stays unsatisfiable —
    /// and drops the ABox, so the individual asserted into that unsatisfiable class is
    /// the graph's problem, not the schema's.
    #[test]
    fn terminology_scope_keeps_the_class_hierarchy_and_drops_the_abox() {
        let triples = parse_turtle(DOCUMENT).unwrap();
        let nothing = "<http://www.w3.org/2002/07/owl#Nothing>";
        let bad = "<http://example.org/Bad>";

        let terminology = reason_dl_terminology(&triples);
        assert!(terminology.instances.is_empty());
        assert!(terminology.consistent);
        assert!(terminology.subsumers[bad].contains(nothing));

        let full = reason_dl(&triples);
        assert_eq!(full.engine, terminology.engine);
        assert!(!full.consistent, "a : Bad is an ABox inconsistency");
        assert!(full.subsumers[bad].contains(nothing));
    }
}
