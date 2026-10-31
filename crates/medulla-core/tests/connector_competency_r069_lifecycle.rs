//! Registered-source acceptance for the sdlc_lifecycle-v1 term additions
//! (MEDULLA-CQ-CONN-R069.4 to R069.6, R069.10 to R069.14, R069.20 to R069.23).
//!
//! Each test loads the registered `core:sdlc_lifecycle@1` source through the
//! CoreSpec registry, asserts the added term with its stated domain and range,
//! and — for the two SHACL-constrained datatypes — proves a violating fixture
//! fails the companion shape (T-AUD-002: valid and invalid fixtures).
//! `:Branch`, `:Repository` and `:Commit` stay declared in the registered
//! software module; this module only references them.

use medulla_core::graph::{GraphSchemaSources, SchemaSourceOrigin};
use medulla_rdf::mapping::parse_turtle;
use medulla_rdf::oxrdf::{NamedNode, Triple};

const KG: &str = "http://knuckles.team/kg#";
const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const OWL: &str = "http://www.w3.org/2002/07/owl#";
const RDFS_DOMAIN: &str = "http://www.w3.org/2000/01/rdf-schema#domain";
const RDFS_RANGE: &str = "http://www.w3.org/2000/01/rdf-schema#range";
const XSD: &str = "http://www.w3.org/2001/XMLSchema#";

fn has_iri(triples: &[Triple], subject: &str, predicate: &str, object: &str) -> bool {
    triples.contains(&Triple::new(
        NamedNode::new(subject).unwrap(),
        NamedNode::new(predicate).unwrap(),
        NamedNode::new(object).unwrap(),
    ))
}

fn lifecycle_ontology() -> Vec<Triple> {
    let sources = GraphSchemaSources::default();
    let source = sources
        .core
        .get("core:sdlc_lifecycle@1")
        .unwrap_or_else(|| panic!("registered source core:sdlc_lifecycle@1 is missing"));
    match &source.origin {
        SchemaSourceOrigin::Core {
            module, version, ..
        } => {
            assert_eq!(module, "sdlc_lifecycle", "source module ownership changed");
            assert_eq!(*version, 1, "unexpected source version");
        }
        origin => panic!("core:sdlc_lifecycle@1 has non-core origin {origin:?}"),
    }
    parse_turtle(
        source
            .ontology_ttl
            .as_deref()
            .unwrap_or_else(|| panic!("registered sdlc_lifecycle source has no ontology")),
    )
    .expect("registered sdlc_lifecycle ontology parses")
}

/// The SHACL companion (MEDULLA-CQ-CONN-R069.11/R069.21 closed vocabularies).
fn lifecycle_shapes() -> &'static str {
    include_str!("../ontology/sdlc_lifecycle-v1.shapes.ttl")
}

#[allow(non_snake_case)]
mod ontology_audit_sdlc_lifecycle_v1 {
    use super::*;

    // spec: MEDULLA-CQ-CONN-R069.4
    #[test]
    fn branchOf_declared() {
        let triples = lifecycle_ontology();
        let term = format!("{KG}branchOf");
        assert!(
            has_iri(&triples, &term, RDF_TYPE, &format!("{OWL}ObjectProperty")),
            "branchOf is declared as an object property"
        );
        assert!(
            has_iri(&triples, &term, RDFS_DOMAIN, &format!("{KG}Branch")),
            "branchOf's domain is Branch"
        );
        assert!(
            has_iri(&triples, &term, RDFS_RANGE, &format!("{KG}Repository")),
            "branchOf's range is Repository"
        );
    }

    // spec: MEDULLA-CQ-CONN-R069.5
    #[test]
    fn headCommit_declared() {
        let triples = lifecycle_ontology();
        let term = format!("{KG}headCommit");
        assert!(
            has_iri(&triples, &term, RDF_TYPE, &format!("{OWL}ObjectProperty")),
            "headCommit is declared as an object property"
        );
        assert!(
            has_iri(&triples, &term, RDFS_DOMAIN, &format!("{KG}Branch")),
            "headCommit's domain is Branch"
        );
        assert!(
            has_iri(&triples, &term, RDFS_RANGE, &format!("{KG}Commit")),
            "headCommit's range is the canonical software Commit"
        );
    }

    // spec: MEDULLA-CQ-CONN-R069.6
    #[test]
    fn isDefaultBranch_declared() {
        let triples = lifecycle_ontology();
        let term = format!("{KG}isDefaultBranch");
        assert!(
            has_iri(&triples, &term, RDF_TYPE, &format!("{OWL}DatatypeProperty")),
            "isDefaultBranch is declared as a datatype property"
        );
        assert!(
            has_iri(&triples, &term, RDFS_DOMAIN, &format!("{KG}Branch")),
            "isDefaultBranch's domain is Branch"
        );
        assert!(
            has_iri(&triples, &term, RDFS_RANGE, &format!("{XSD}boolean")),
            "isDefaultBranch's range is xsd:boolean"
        );
    }

    // spec: MEDULLA-CQ-CONN-R069.10
    #[test]
    fn closesTicket_declared() {
        let triples = lifecycle_ontology();
        let term = format!("{KG}closesTicket");
        assert!(
            has_iri(&triples, &term, RDF_TYPE, &format!("{OWL}ObjectProperty")),
            "closesTicket is declared as an object property"
        );
        assert!(
            has_iri(
                &triples,
                &term,
                RDFS_DOMAIN,
                &format!("{KG}CodeChangeProposal")
            ),
            "closesTicket's domain is CodeChangeProposal"
        );
        assert!(
            has_iri(&triples, &term, RDFS_RANGE, &format!("{KG}Ticket")),
            "closesTicket's range is Ticket"
        );
    }

    // spec: MEDULLA-CQ-CONN-R069.11
    #[test]
    fn proposalState_declared() {
        let triples = lifecycle_ontology();
        let term = format!("{KG}proposalState");
        assert!(
            has_iri(&triples, &term, RDF_TYPE, &format!("{OWL}DatatypeProperty")),
            "proposalState is declared as a datatype property"
        );
        assert!(
            has_iri(
                &triples,
                &term,
                RDFS_DOMAIN,
                &format!("{KG}CodeChangeProposal")
            ),
            "proposalState's domain is CodeChangeProposal"
        );
        assert!(
            has_iri(&triples, &term, RDFS_RANGE, &format!("{XSD}string")),
            "proposalState's range is xsd:string"
        );

        // The closed vocabulary lives in the SHACL companion: a valid fixture
        // conforms, a violating fixture fails the shape.
        let valid = r#"
@prefix : <http://knuckles.team/kg#> .
@prefix ex: <http://example.org/cq-conn/> .
ex:pr-1 a :CodeChangeProposal ;
    :proposalState "merged" .
"#;
        let report = medulla_shacl::validate_turtle(lifecycle_shapes(), valid)
            .expect("valid proposalState fixture parses and validates");
        assert!(
            report.conforms,
            "a proposalState inside the closed vocabulary conforms: {:?}",
            report.results
        );

        let violating = valid.replace("merged", "pending");
        let report = medulla_shacl::validate_turtle(lifecycle_shapes(), &violating)
            .expect("violating proposalState fixture parses before validation");
        assert!(
            !report.conforms,
            "a proposalState outside open/merged/closed must fail the shape"
        );
        // Report terms carry their N-Triples lexical form: an IRI path is
        // `<iri>`, as the W3C closed-vocabulary precedent asserts.
        assert!(
            report
                .results
                .iter()
                .any(|result| { result.path.as_deref() == Some(&format!("<{KG}proposalState>")) }),
            "the violation is reported on the proposalState path: {:?}",
            report.results
        );
    }

    // spec: MEDULLA-CQ-CONN-R069.12
    #[test]
    fn requestedReviewer_declared() {
        let triples = lifecycle_ontology();
        let term = format!("{KG}requestedReviewer");
        assert!(
            has_iri(&triples, &term, RDF_TYPE, &format!("{OWL}ObjectProperty")),
            "requestedReviewer is declared as an object property"
        );
        assert!(
            has_iri(
                &triples,
                &term,
                RDFS_DOMAIN,
                &format!("{KG}CodeChangeProposal")
            ),
            "requestedReviewer's domain is CodeChangeProposal"
        );
        assert!(
            has_iri(&triples, &term, RDFS_RANGE, &format!("{KG}Person")),
            "requestedReviewer's range is Person"
        );
    }

    // spec: MEDULLA-CQ-CONN-R069.13
    #[test]
    fn PipelineJob_declared() {
        let triples = lifecycle_ontology();
        let term = format!("{KG}PipelineJob");
        assert!(
            has_iri(&triples, &term, RDF_TYPE, &format!("{OWL}Class")),
            "PipelineJob is declared as a class"
        );
    }

    // spec: MEDULLA-CQ-CONN-R069.14
    #[test]
    fn pipelineJobOf_declared() {
        let triples = lifecycle_ontology();
        let term = format!("{KG}pipelineJobOf");
        assert!(
            has_iri(&triples, &term, RDF_TYPE, &format!("{OWL}ObjectProperty")),
            "pipelineJobOf is declared as an object property"
        );
        assert!(
            has_iri(&triples, &term, RDFS_DOMAIN, &format!("{KG}PipelineJob")),
            "pipelineJobOf's domain is PipelineJob"
        );
        assert!(
            has_iri(&triples, &term, RDFS_RANGE, &format!("{KG}PipelineRun")),
            "pipelineJobOf's range is PipelineRun"
        );
    }

    // spec: MEDULLA-CQ-CONN-R069.20
    #[test]
    fn Worktree_declared() {
        let triples = lifecycle_ontology();
        let term = format!("{KG}Worktree");
        assert!(
            has_iri(&triples, &term, RDF_TYPE, &format!("{OWL}Class")),
            "Worktree is declared as a class"
        );
    }

    // spec: MEDULLA-CQ-CONN-R069.21
    #[test]
    fn worktreeState_declared() {
        let triples = lifecycle_ontology();
        let term = format!("{KG}worktreeState");
        assert!(
            has_iri(&triples, &term, RDF_TYPE, &format!("{OWL}DatatypeProperty")),
            "worktreeState is declared as a datatype property"
        );
        assert!(
            has_iri(&triples, &term, RDFS_DOMAIN, &format!("{KG}Worktree")),
            "worktreeState's domain is Worktree"
        );
        assert!(
            has_iri(&triples, &term, RDFS_RANGE, &format!("{XSD}string")),
            "worktreeState's range is xsd:string"
        );

        // The closed vocabulary lives in the SHACL companion: a valid fixture
        // conforms, a violating fixture fails the shape.
        let valid = r#"
@prefix : <http://knuckles.team/kg#> .
@prefix ex: <http://example.org/cq-conn/> .
ex:wt-1 a :Worktree ;
    :worktreeState "stale" .
"#;
        let report = medulla_shacl::validate_turtle(lifecycle_shapes(), valid)
            .expect("valid worktreeState fixture parses and validates");
        assert!(
            report.conforms,
            "a worktreeState inside the closed vocabulary conforms: {:?}",
            report.results
        );

        let violating = valid.replace("stale", "archived");
        let report = medulla_shacl::validate_turtle(lifecycle_shapes(), &violating)
            .expect("violating worktreeState fixture parses before validation");
        assert!(
            !report.conforms,
            "a worktreeState outside active/stale/dangling/merged must fail the shape"
        );
        // Report terms carry their N-Triples lexical form: an IRI path is
        // `<iri>`, as the W3C closed-vocabulary precedent asserts.
        assert!(
            report
                .results
                .iter()
                .any(|result| { result.path.as_deref() == Some(&format!("<{KG}worktreeState>")) }),
            "the violation is reported on the worktreeState path: {:?}",
            report.results
        );
    }

    // spec: MEDULLA-CQ-CONN-R069.22
    #[test]
    fn tracksBranch_declared() {
        let triples = lifecycle_ontology();
        let term = format!("{KG}tracksBranch");
        assert!(
            has_iri(&triples, &term, RDF_TYPE, &format!("{OWL}ObjectProperty")),
            "tracksBranch is declared as an object property"
        );
        assert!(
            has_iri(&triples, &term, RDFS_DOMAIN, &format!("{KG}Worktree")),
            "tracksBranch's domain is Worktree"
        );
        assert!(
            has_iri(&triples, &term, RDFS_RANGE, &format!("{KG}Branch")),
            "tracksBranch's range is Branch"
        );
    }

    // spec: MEDULLA-CQ-CONN-R069.23
    #[test]
    fn proposalBranch_declared() {
        let triples = lifecycle_ontology();
        let term = format!("{KG}proposalBranch");
        assert!(
            has_iri(&triples, &term, RDF_TYPE, &format!("{OWL}ObjectProperty")),
            "proposalBranch is declared as an object property"
        );
        assert!(
            has_iri(
                &triples,
                &term,
                RDFS_DOMAIN,
                &format!("{KG}CodeChangeProposal")
            ),
            "proposalBranch's domain is CodeChangeProposal"
        );
        assert!(
            has_iri(&triples, &term, RDFS_RANGE, &format!("{KG}Branch")),
            "proposalBranch's range is Branch"
        );
    }
}
