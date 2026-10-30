//! Registered-core ontology audits for the connector-competency term rows
//! (EG-CQ-CONN-R070/R073/R074/R076/R077/R078/R079/R082).
//!
//! Each module proves one ontology module: the term is declared with the
//! required owl type, domain and range in the registered core source, and —
//! where the row carries a SHACL requirement — the registered shapes
//! companion rejects a violating fixture.

use eg_core::graph::{GraphSchemaSources, SchemaSourceOrigin};
use eg_rdf::mapping::parse_turtle;
use eg_rdf::oxrdf::{NamedNode, NamedOrBlankNode, Term, Triple};

const KG: &str = "http://knuckles.team/kg#";
const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const RDF_FIRST: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#first";
const RDF_REST: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#rest";
const RDF_NIL: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#nil";
const RDFS_DOMAIN: &str = "http://www.w3.org/2000/01/rdf-schema#domain";
const RDFS_RANGE: &str = "http://www.w3.org/2000/01/rdf-schema#range";
const RDFS_SUBCLASSOF: &str = "http://www.w3.org/2000/01/rdf-schema#subClassOf";
const OWL_CLASS: &str = "http://www.w3.org/2002/07/owl#Class";
const OWL_OBJECT_PROPERTY: &str = "http://www.w3.org/2002/07/owl#ObjectProperty";
const OWL_DATATYPE_PROPERTY: &str = "http://www.w3.org/2002/07/owl#DatatypeProperty";
const OWL_UNION: &str = "http://www.w3.org/2002/07/owl#unionOf";
const XSD: &str = "http://www.w3.org/2001/XMLSchema#";
const SHACL_NODE_SHAPE: &str = "http://www.w3.org/ns/shacl#NodeShape";
const SHACL_TARGET_CLASS: &str = "http://www.w3.org/ns/shacl#targetClass";

fn has_iri(triples: &[Triple], subject: &str, predicate: &str, object: &str) -> bool {
    triples.contains(&Triple::new(
        NamedNode::new(subject).unwrap(),
        NamedNode::new(predicate).unwrap(),
        NamedNode::new(object).unwrap(),
    ))
}

fn node(iri: &str) -> NamedOrBlankNode {
    NamedNode::new(iri).unwrap().into()
}

/// Downcast a triple object to a graph node (IRI or blank node); `None` for
/// literals.
fn as_node(term: &Term) -> Option<NamedOrBlankNode> {
    match term {
        Term::NamedNode(named) => Some(NamedOrBlankNode::NamedNode(named.clone())),
        Term::BlankNode(blank) => Some(NamedOrBlankNode::BlankNode(blank.clone())),
        _ => None,
    }
}

/// True iff `prop`'s rdfs:domain is (or, for an anonymous `owl:unionOf`
/// class, covers) the named class `member`.
fn domain_covers(triples: &[Triple], prop: &str, member: &str) -> bool {
    let subject = node(prop);
    let predicate = NamedNode::new(RDFS_DOMAIN).unwrap();
    triples
        .iter()
        .filter(|t| t.subject == subject && t.predicate == predicate)
        .filter_map(|t| as_node(&t.object))
        .any(|domain| covers_domain(triples, &domain, member))
}

fn covers_domain(triples: &[Triple], domain: &NamedOrBlankNode, member: &str) -> bool {
    if domain == &node(member) {
        return true;
    }
    // An anonymous owl:unionOf class names its members through an rdf list.
    let union = NamedNode::new(OWL_UNION).unwrap();
    for list_head in triples
        .iter()
        .filter(|t| t.subject == *domain && t.predicate == union)
        .filter_map(|t| as_node(&t.object))
    {
        let mut current = list_head;
        for _ in 0..32 {
            let Some(item) = head_of(triples, &current) else {
                break;
            };
            if item == node(member) {
                return true;
            }
            let Some(next) = rest_of(triples, &current) else {
                break;
            };
            if next == node(RDF_NIL) {
                break;
            }
            current = next;
        }
    }
    false
}

fn head_of(triples: &[Triple], list: &NamedOrBlankNode) -> Option<NamedOrBlankNode> {
    let first = NamedNode::new(RDF_FIRST).unwrap();
    triples
        .iter()
        .find(|t| t.subject == *list && t.predicate == first)
        .and_then(|t| as_node(&t.object))
}

fn rest_of(triples: &[Triple], list: &NamedOrBlankNode) -> Option<NamedOrBlankNode> {
    let rest = NamedNode::new(RDF_REST).unwrap();
    triples
        .iter()
        .find(|t| t.subject == *list && t.predicate == rest)
        .and_then(|t| as_node(&t.object))
}

/// The registered ontology document for a core source, with module ownership
/// asserted (a source id must stay bound to its module).
fn registered_ontology(source_id: &str, expected_module: &str) -> std::sync::Arc<str> {
    let sources = GraphSchemaSources::default();
    sources
        .validate()
        .expect("the default core catalog validates");
    assert_core_source(&sources, source_id, expected_module);
    sources
        .core
        .get(source_id)
        .unwrap()
        .ontology_ttl
        .clone()
        .unwrap_or_else(|| panic!("registered source {source_id} has no ontology"))
}

/// The registered shapes companion for a core source, with module ownership
/// asserted.
fn registered_shapes(source_id: &str, expected_module: &str) -> std::sync::Arc<str> {
    let sources = GraphSchemaSources::default();
    sources
        .validate()
        .expect("the default core catalog validates");
    assert_core_source(&sources, source_id, expected_module);
    sources
        .core
        .get(source_id)
        .unwrap()
        .shapes_ttl
        .clone()
        .unwrap_or_else(|| panic!("registered source {source_id} has no shapes"))
}

fn assert_core_source(sources: &GraphSchemaSources, source_id: &str, expected_module: &str) {
    let source = sources
        .core
        .get(source_id)
        .unwrap_or_else(|| panic!("registered source {source_id} is missing"));
    match &source.origin {
        SchemaSourceOrigin::Core {
            module, version, ..
        } => {
            assert_eq!(module, expected_module, "source module ownership changed");
            assert_eq!(*version, 1, "unexpected source version");
        }
        origin => panic!("{source_id} has non-core origin {origin:?}"),
    }
}

#[allow(non_snake_case)]
mod ontology_audit_orchestration_v1 {
    use super::*;

    fn documents() -> (Vec<Triple>, Vec<Triple>) {
        let ontology = registered_ontology("core:orchestration@1", "orchestration");
        let shapes = registered_shapes("core:orchestration-shapes@1", "orchestration-shapes");
        (
            parse_turtle(&ontology).expect("the registered orchestration ontology parses"),
            parse_turtle(&shapes).expect("the registered orchestration shapes parse"),
        )
    }

    fn conforms(data: &str) -> bool {
        let shapes = registered_shapes("core:orchestration-shapes@1", "orchestration-shapes");
        eg_shacl::validate_turtle(&shapes, data)
            .expect("validation runs")
            .conforms
    }

    // spec: EG-CQ-CONN-R079.2
    #[test]
    fn promptVersion_declared() {
        let (triples, _shapes) = documents();
        let property = format!("{KG}promptVersion");
        assert!(has_iri(
            &triples,
            &property,
            RDF_TYPE,
            OWL_DATATYPE_PROPERTY
        ));
        assert!(has_iri(
            &triples,
            &property,
            RDFS_DOMAIN,
            &format!("{KG}Prompt")
        ));
        assert!(has_iri(
            &triples,
            &property,
            RDFS_RANGE,
            &format!("{XSD}integer")
        ));
    }

    // spec: EG-CQ-CONN-R079.3
    #[test]
    fn tokenCost_declared() {
        let (triples, _shapes) = documents();
        let property = format!("{KG}tokenCost");
        assert!(has_iri(
            &triples,
            &property,
            RDF_TYPE,
            OWL_DATATYPE_PROPERTY
        ));
        assert!(has_iri(
            &triples,
            &property,
            RDFS_DOMAIN,
            &format!("{KG}RunTrace")
        ));
        assert!(has_iri(
            &triples,
            &property,
            RDFS_RANGE,
            &format!("{XSD}decimal")
        ));
        // The registered shape allows a recorded zero and any positive cost,
        // and rejects a negative one.
        assert!(conforms(
            "@prefix : <http://knuckles.team/kg#> . :trace a :RunTrace ; :tokenCost 0.0 ."
        ));
        assert!(conforms(
            "@prefix : <http://knuckles.team/kg#> . :trace a :RunTrace ; :tokenCost 12.5 ."
        ));
        assert!(!conforms(
            "@prefix : <http://knuckles.team/kg#> . :trace a :RunTrace ; :tokenCost -1 ."
        ));
    }

    // spec: EG-CQ-CONN-R079.4
    #[test]
    fn tracePrompt_declared() {
        let (triples, _shapes) = documents();
        let property = format!("{KG}tracePrompt");
        assert!(has_iri(&triples, &property, RDF_TYPE, OWL_OBJECT_PROPERTY));
        assert!(has_iri(
            &triples,
            &property,
            RDFS_DOMAIN,
            &format!("{KG}RunTrace")
        ));
        assert!(has_iri(
            &triples,
            &property,
            RDFS_RANGE,
            &format!("{KG}Prompt")
        ));
    }

    // spec: EG-CQ-CONN-R079.5
    #[test]
    fn evaluationScore_declared() {
        let (triples, _shapes) = documents();
        let property = format!("{KG}evaluationScore");
        assert!(has_iri(
            &triples,
            &property,
            RDF_TYPE,
            OWL_DATATYPE_PROPERTY
        ));
        assert!(has_iri(
            &triples,
            &property,
            RDFS_RANGE,
            &format!("{XSD}decimal")
        ));
        assert!(domain_covers(&triples, &property, &format!("{KG}RunTrace")));
        assert!(domain_covers(&triples, &property, &format!("{KG}Prompt")));
    }
}

#[allow(non_snake_case)]
mod ontology_audit_personal_v1 {
    use super::*;

    fn triples() -> Vec<Triple> {
        let ontology = registered_ontology("core:personal@1", "personal");
        parse_turtle(&ontology).expect("the registered personal ontology parses")
    }

    // spec: EG-CQ-CONN-R076.1
    #[test]
    fn birthPlace_declared() {
        let triples = triples();
        let property = format!("{KG}birthPlace");
        assert!(has_iri(&triples, &property, RDF_TYPE, OWL_OBJECT_PROPERTY));
        assert!(has_iri(
            &triples,
            &property,
            RDFS_DOMAIN,
            &format!("{KG}Person")
        ));
        assert!(has_iri(
            &triples,
            &property,
            RDFS_RANGE,
            &format!("{KG}Place")
        ));
    }

    // spec: EG-CQ-CONN-R076.2
    #[test]
    fn birthYear_declared() {
        let triples = triples();
        let property = format!("{KG}birthYear");
        assert!(has_iri(
            &triples,
            &property,
            RDF_TYPE,
            OWL_DATATYPE_PROPERTY
        ));
        assert!(has_iri(
            &triples,
            &property,
            RDFS_DOMAIN,
            &format!("{KG}Person")
        ));
        assert!(has_iri(
            &triples,
            &property,
            RDFS_RANGE,
            &format!("{XSD}gYear")
        ));
    }

    // spec: EG-CQ-CONN-R076.3
    #[test]
    fn documentType_declared() {
        let triples = triples();
        let property = format!("{KG}documentType");
        assert!(has_iri(
            &triples,
            &property,
            RDF_TYPE,
            OWL_DATATYPE_PROPERTY
        ));
        assert!(has_iri(
            &triples,
            &property,
            RDFS_DOMAIN,
            &format!("{KG}Document")
        ));
        assert!(has_iri(
            &triples,
            &property,
            RDFS_RANGE,
            &format!("{XSD}string")
        ));
    }

    // spec: EG-CQ-CONN-R076.4
    #[test]
    fn dueDate_declared() {
        let triples = triples();
        let property = format!("{KG}dueDate");
        assert!(has_iri(
            &triples,
            &property,
            RDF_TYPE,
            OWL_DATATYPE_PROPERTY
        ));
        assert!(has_iri(
            &triples,
            &property,
            RDFS_DOMAIN,
            &format!("{KG}Document")
        ));
        assert!(has_iri(
            &triples,
            &property,
            RDFS_RANGE,
            &format!("{XSD}date")
        ));
    }

    // spec: EG-CQ-CONN-R076.5
    #[test]
    fn totalAmount_declared() {
        let triples = triples();
        let property = format!("{KG}totalAmount");
        assert!(has_iri(
            &triples,
            &property,
            RDF_TYPE,
            OWL_DATATYPE_PROPERTY
        ));
        assert!(has_iri(
            &triples,
            &property,
            RDFS_DOMAIN,
            &format!("{KG}Document")
        ));
        assert!(has_iri(
            &triples,
            &property,
            RDFS_RANGE,
            &format!("{XSD}decimal")
        ));
    }

    // spec: EG-CQ-CONN-R076.6
    #[test]
    fn FileShare_declared() {
        let triples = triples();
        assert!(has_iri(
            &triples,
            &format!("{KG}FileShare"),
            RDF_TYPE,
            OWL_CLASS
        ));
    }

    // spec: EG-CQ-CONN-R076.7
    #[test]
    fn sharesFile_declared() {
        let triples = triples();
        let property = format!("{KG}sharesFile");
        assert!(has_iri(&triples, &property, RDF_TYPE, OWL_OBJECT_PROPERTY));
        assert!(has_iri(
            &triples,
            &property,
            RDFS_DOMAIN,
            &format!("{KG}FileShare")
        ));
        assert!(has_iri(
            &triples,
            &property,
            RDFS_RANGE,
            &format!("{KG}Document")
        ));
    }

    // spec: EG-CQ-CONN-R076.8
    #[test]
    fn isPublicLink_declared() {
        let triples = triples();
        let property = format!("{KG}isPublicLink");
        assert!(has_iri(
            &triples,
            &property,
            RDF_TYPE,
            OWL_DATATYPE_PROPERTY
        ));
        assert!(has_iri(
            &triples,
            &property,
            RDFS_DOMAIN,
            &format!("{KG}FileShare")
        ));
        assert!(has_iri(
            &triples,
            &property,
            RDFS_RANGE,
            &format!("{XSD}boolean")
        ));
    }

    // spec: EG-CQ-CONN-R076.9
    #[test]
    fn shareExpiresAt_declared() {
        let triples = triples();
        let property = format!("{KG}shareExpiresAt");
        assert!(has_iri(
            &triples,
            &property,
            RDF_TYPE,
            OWL_DATATYPE_PROPERTY
        ));
        assert!(has_iri(
            &triples,
            &property,
            RDFS_DOMAIN,
            &format!("{KG}FileShare")
        ));
        assert!(has_iri(
            &triples,
            &property,
            RDFS_RANGE,
            &format!("{XSD}date")
        ));
    }
}

#[allow(non_snake_case)]
mod ontology_audit_action_v1 {
    use super::*;

    fn triples() -> Vec<Triple> {
        let ontology = registered_ontology("core:action@1", "action");
        parse_turtle(&ontology).expect("the registered action ontology parses")
    }

    // spec: EG-CQ-CONN-R082.1
    #[test]
    fn failureReason_declared() {
        let triples = triples();
        let property = format!("{KG}failureReason");
        assert!(has_iri(
            &triples,
            &property,
            RDF_TYPE,
            OWL_DATATYPE_PROPERTY
        ));
        assert!(has_iri(
            &triples,
            &property,
            RDFS_DOMAIN,
            &format!("{KG}ActionInvocation")
        ));
        assert!(has_iri(
            &triples,
            &property,
            RDFS_RANGE,
            &format!("{XSD}string")
        ));
    }
}

#[allow(non_snake_case)]
mod ontology_audit_archimate_v1 {
    use super::*;

    fn triples() -> Vec<Triple> {
        let ontology = registered_ontology("core:archimate@1", "archimate");
        parse_turtle(&ontology).expect("the registered archimate ontology parses")
    }

    // spec: EG-CQ-CONN-R070.5
    #[test]
    fn ConfigurationItem_declared() {
        let triples = triples();
        assert!(has_iri(
            &triples,
            &format!("{KG}ConfigurationItem"),
            RDF_TYPE,
            OWL_CLASS
        ));
        assert!(has_iri(
            &triples,
            &format!("{KG}ConfigurationItem"),
            RDFS_SUBCLASSOF,
            &format!("{KG}ApplicationLayer")
        ));
    }

    // spec: EG-CQ-CONN-R070.6
    #[test]
    fn affectsConfigurationItem_declared() {
        let triples = triples();
        let property = format!("{KG}affectsConfigurationItem");
        assert!(has_iri(&triples, &property, RDF_TYPE, OWL_OBJECT_PROPERTY));
        assert!(has_iri(
            &triples,
            &property,
            RDFS_RANGE,
            &format!("{KG}ConfigurationItem")
        ));
        // The domain is an anonymous owl:unionOf over Incident and Change.
        assert!(domain_covers(&triples, &property, &format!("{KG}Incident")));
        assert!(domain_covers(&triples, &property, &format!("{KG}Change")));
    }

    // spec: EG-CQ-CONN-R070.7
    #[test]
    fn incidentState_declared() {
        let triples = triples();
        let property = format!("{KG}incidentState");
        assert!(has_iri(
            &triples,
            &property,
            RDF_TYPE,
            OWL_DATATYPE_PROPERTY
        ));
        assert!(has_iri(
            &triples,
            &property,
            RDFS_DOMAIN,
            &format!("{KG}Incident")
        ));
        assert!(has_iri(
            &triples,
            &property,
            RDFS_RANGE,
            &format!("{XSD}string")
        ));
    }

    // spec: EG-CQ-CONN-R070.8
    #[test]
    fn incidentAssignee_declared() {
        let triples = triples();
        let property = format!("{KG}incidentAssignee");
        assert!(has_iri(&triples, &property, RDF_TYPE, OWL_OBJECT_PROPERTY));
        assert!(has_iri(
            &triples,
            &property,
            RDFS_DOMAIN,
            &format!("{KG}Incident")
        ));
        assert!(has_iri(
            &triples,
            &property,
            RDFS_RANGE,
            &format!("{KG}Person")
        ));
    }
}

#[allow(non_snake_case)]
mod ontology_audit_government_v1 {
    use super::*;

    fn triples() -> Vec<Triple> {
        let ontology = registered_ontology("core:government@1", "government");
        parse_turtle(&ontology).expect("the registered government ontology parses")
    }

    fn conforms(data: &str) -> bool {
        let shapes = registered_shapes("core:government-shapes@1", "government-shapes");
        eg_shacl::validate_turtle(&shapes, data)
            .expect("validation runs")
            .conforms
    }

    // spec: EG-CQ-CONN-R073.1
    #[test]
    fn shapes_module_loads() {
        let shapes = registered_shapes("core:government-shapes@1", "government-shapes");
        let triples = parse_turtle(&shapes).expect("the registered government shapes parse");
        let shape = format!("{KG}PrivacyAssessmentShape");
        assert!(has_iri(&triples, &shape, RDF_TYPE, SHACL_NODE_SHAPE));
        assert!(has_iri(
            &triples,
            &shape,
            SHACL_TARGET_CLASS,
            &format!("{KG}PrivacyAssessment")
        ));
    }

    // spec: EG-CQ-CONN-R073.2
    #[test]
    fn RiskScenario_declared() {
        let triples = triples();
        let class = format!("{KG}RiskScenario");
        assert!(has_iri(&triples, &class, RDF_TYPE, OWL_CLASS));
    }

    // spec: EG-CQ-CONN-R073.3
    #[test]
    fn mitigatesRisk_declared() {
        let triples = triples();
        let property = format!("{KG}mitigatesRisk");
        assert!(has_iri(&triples, &property, RDF_TYPE, OWL_OBJECT_PROPERTY));
        assert!(has_iri(
            &triples,
            &property,
            RDFS_DOMAIN,
            &format!("{KG}SecurityControl")
        ));
        assert!(has_iri(
            &triples,
            &property,
            RDFS_RANGE,
            &format!("{KG}RiskScenario")
        ));
    }

    // spec: EG-CQ-CONN-R073.4
    #[test]
    fn inFramework_declared() {
        let triples = triples();
        let property = format!("{KG}inFramework");
        assert!(has_iri(&triples, &property, RDF_TYPE, OWL_OBJECT_PROPERTY));
        assert!(has_iri(
            &triples,
            &property,
            RDFS_DOMAIN,
            &format!("{KG}SecurityControl")
        ));
        assert!(has_iri(
            &triples,
            &property,
            RDFS_RANGE,
            &format!("{KG}Framework")
        ));
    }

    // spec: EG-CQ-CONN-R073.5
    #[test]
    fn riskAffectsService_declared() {
        let triples = triples();
        let property = format!("{KG}riskAffectsService");
        assert!(has_iri(&triples, &property, RDF_TYPE, OWL_OBJECT_PROPERTY));
        assert!(has_iri(
            &triples,
            &property,
            RDFS_DOMAIN,
            &format!("{KG}RiskScenario")
        ));
        assert!(has_iri(
            &triples,
            &property,
            RDFS_RANGE,
            &format!("{KG}Service")
        ));
    }

    // spec: EG-CQ-CONN-R073.6
    #[test]
    fn PrivacyAssessment_declared() {
        let triples = triples();
        let class = format!("{KG}PrivacyAssessment");
        assert!(has_iri(&triples, &class, RDF_TYPE, OWL_CLASS));
    }

    // spec: EG-CQ-CONN-R073.7
    #[test]
    fn ProcessingActivity_declared() {
        let triples = triples();
        let class = format!("{KG}ProcessingActivity");
        assert!(has_iri(&triples, &class, RDF_TYPE, OWL_CLASS));
    }

    // spec: EG-CQ-CONN-R073.8
    #[test]
    fn assessmentCovers_declared() {
        let triples = triples();
        let property = format!("{KG}assessmentCovers");
        assert!(has_iri(&triples, &property, RDF_TYPE, OWL_OBJECT_PROPERTY));
        assert!(has_iri(
            &triples,
            &property,
            RDFS_DOMAIN,
            &format!("{KG}PrivacyAssessment")
        ));
        assert!(has_iri(
            &triples,
            &property,
            RDFS_RANGE,
            &format!("{KG}ProcessingActivity")
        ));
    }

    // spec: EG-CQ-CONN-R073.9
    #[test]
    fn assessmentDueDate_declared() {
        let triples = triples();
        let property = format!("{KG}assessmentDueDate");
        assert!(has_iri(
            &triples,
            &property,
            RDF_TYPE,
            OWL_DATATYPE_PROPERTY
        ));
        assert!(has_iri(
            &triples,
            &property,
            RDFS_DOMAIN,
            &format!("{KG}PrivacyAssessment")
        ));
        assert!(has_iri(
            &triples,
            &property,
            RDFS_RANGE,
            &format!("{XSD}date")
        ));
        // The registered shape requires the due date on every assessment.
        assert!(conforms(
            "@prefix : <http://knuckles.team/kg#> .\n\
             @prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n\
             :assessment a :PrivacyAssessment ; :assessmentDueDate \"2026-11-01\"^^xsd:date ."
        ));
        assert!(!conforms(
            "@prefix : <http://knuckles.team/kg#> . :assessment a :PrivacyAssessment ."
        ));
    }

    // spec: EG-CQ-CONN-R073.10
    #[test]
    fn assessedVendor_declared() {
        let triples = triples();
        let property = format!("{KG}assessedVendor");
        assert!(has_iri(&triples, &property, RDF_TYPE, OWL_OBJECT_PROPERTY));
        assert!(has_iri(
            &triples,
            &property,
            RDFS_DOMAIN,
            &format!("{KG}PrivacyAssessment")
        ));
        assert!(has_iri(
            &triples,
            &property,
            RDFS_RANGE,
            &format!("{KG}Vendor")
        ));
    }

    // spec: EG-CQ-CONN-R073.11
    #[test]
    fn assessmentStatus_declared() {
        let triples = triples();
        let property = format!("{KG}assessmentStatus");
        assert!(has_iri(
            &triples,
            &property,
            RDF_TYPE,
            OWL_DATATYPE_PROPERTY
        ));
        assert!(has_iri(
            &triples,
            &property,
            RDFS_DOMAIN,
            &format!("{KG}PrivacyAssessment")
        ));
        assert!(has_iri(
            &triples,
            &property,
            RDFS_RANGE,
            &format!("{XSD}string")
        ));
    }
}

#[allow(non_snake_case)]
mod ontology_audit_core_foundation_v1 {
    use super::*;

    fn triples() -> Vec<Triple> {
        let ontology = registered_ontology("core:foundation@1", "foundation");
        parse_turtle(&ontology).expect("the registered core-foundation ontology parses")
    }

    fn conforms(data: &str) -> bool {
        let shapes = registered_shapes("core:foundation-shapes@1", "foundation-shapes");
        eg_shacl::validate_turtle(&shapes, data)
            .expect("validation runs")
            .conforms
    }

    // spec: EG-CQ-CONN-R074.1
    #[test]
    fn shapes_module_loads() {
        let shapes = registered_shapes("core:foundation-shapes@1", "foundation-shapes");
        let triples = parse_turtle(&shapes).expect("the registered core-foundation shapes parse");
        let shape = format!("{KG}AgreementShape");
        assert!(has_iri(&triples, &shape, RDF_TYPE, SHACL_NODE_SHAPE));
        assert!(has_iri(
            &triples,
            &shape,
            SHACL_TARGET_CLASS,
            &format!("{KG}Agreement")
        ));
    }

    // spec: EG-CQ-CONN-R074.2
    #[test]
    fn GlossaryTerm_declared() {
        let triples = triples();
        let class = format!("{KG}GlossaryTerm");
        assert!(has_iri(&triples, &class, RDF_TYPE, OWL_CLASS));
        assert!(has_iri(
            &triples,
            &class,
            RDFS_SUBCLASSOF,
            &format!("{KG}Concept")
        ));
    }

    // spec: EG-CQ-CONN-R074.3
    #[test]
    fn termAssignedTo_declared() {
        let triples = triples();
        let property = format!("{KG}termAssignedTo");
        assert!(has_iri(&triples, &property, RDF_TYPE, OWL_OBJECT_PROPERTY));
        assert!(has_iri(
            &triples,
            &property,
            RDFS_DOMAIN,
            &format!("{KG}GlossaryTerm")
        ));
        assert!(has_iri(
            &triples,
            &property,
            RDFS_RANGE,
            &format!("{KG}Dataset")
        ));
    }

    // spec: EG-CQ-CONN-R074.4
    #[test]
    fn counterparty_declared() {
        let triples = triples();
        let property = format!("{KG}counterparty");
        assert!(has_iri(&triples, &property, RDF_TYPE, OWL_OBJECT_PROPERTY));
        assert!(has_iri(
            &triples,
            &property,
            RDFS_DOMAIN,
            &format!("{KG}Agreement")
        ));
        assert!(has_iri(
            &triples,
            &property,
            RDFS_RANGE,
            &format!("{KG}LegalEntity")
        ));
        // The registered shape requires a counterparty on every agreement.
        assert!(conforms(
            "@prefix : <http://knuckles.team/kg#> . :agreement a :Agreement ; :counterparty :party ."
        ));
        assert!(!conforms(
            "@prefix : <http://knuckles.team/kg#> . :agreement a :Agreement ."
        ));
    }

    // spec: EG-CQ-CONN-R074.5
    #[test]
    fn agreementExpiresAt_declared() {
        let triples = triples();
        let property = format!("{KG}agreementExpiresAt");
        assert!(has_iri(
            &triples,
            &property,
            RDF_TYPE,
            OWL_DATATYPE_PROPERTY
        ));
        assert!(has_iri(
            &triples,
            &property,
            RDFS_DOMAIN,
            &format!("{KG}Agreement")
        ));
        assert!(has_iri(
            &triples,
            &property,
            RDFS_RANGE,
            &format!("{XSD}date")
        ));
    }

    // spec: EG-CQ-CONN-R074.6
    #[test]
    fn hasRenewalAction_declared() {
        let triples = triples();
        let property = format!("{KG}hasRenewalAction");
        assert!(has_iri(&triples, &property, RDF_TYPE, OWL_OBJECT_PROPERTY));
        assert!(has_iri(
            &triples,
            &property,
            RDFS_DOMAIN,
            &format!("{KG}Agreement")
        ));
        assert!(has_iri(
            &triples,
            &property,
            RDFS_RANGE,
            &format!("{KG}Action")
        ));
    }
}

#[allow(non_snake_case)]
mod ontology_audit_virtual_graph_v1 {
    use super::*;

    const VG: &str = "http://knuckles.team/kg/virtual-graph#";

    fn triples() -> Vec<Triple> {
        let ontology = registered_ontology("core:virtual-graph@1", "virtual-graph");
        parse_turtle(&ontology).expect("the registered virtual-graph ontology parses")
    }

    // spec: EG-CQ-CONN-R077.1
    #[test]
    fn NamedGraph_declared() {
        let triples = triples();
        let class = format!("{VG}NamedGraph");
        assert!(has_iri(&triples, &class, RDF_TYPE, OWL_CLASS));
        assert!(has_iri(
            &triples,
            &class,
            RDFS_SUBCLASSOF,
            &format!("{KG}Dataset")
        ));
    }

    // spec: EG-CQ-CONN-R077.2
    #[test]
    fn tripleCount_declared() {
        let triples = triples();
        let property = format!("{VG}tripleCount");
        assert!(has_iri(&triples, &property, RDF_TYPE, OWL_DATATYPE_PROPERTY));
        assert!(has_iri(
            &triples,
            &property,
            RDFS_DOMAIN,
            &format!("{VG}NamedGraph")
        ));
        assert!(has_iri(
            &triples,
            &property,
            RDFS_RANGE,
            &format!("{XSD}integer")
        ));
    }

    // spec: EG-CQ-CONN-R077.3
    #[test]
    fn lastModifiedAt_declared() {
        let triples = triples();
        let property = format!("{VG}lastModifiedAt");
        assert!(has_iri(&triples, &property, RDF_TYPE, OWL_DATATYPE_PROPERTY));
        assert!(has_iri(
            &triples,
            &property,
            RDFS_DOMAIN,
            &format!("{VG}NamedGraph")
        ));
        assert!(has_iri(
            &triples,
            &property,
            RDFS_RANGE,
            &format!("{XSD}dateTime")
        ));
    }
}

#[allow(non_snake_case)]
mod ontology_audit_identity_v1 {
    use super::*;

    fn triples() -> Vec<Triple> {
        let ontology = registered_ontology("core:identity@1", "identity");
        parse_turtle(&ontology).expect("the registered identity ontology parses")
    }

    // spec: EG-CQ-CONN-R078.2
    #[test]
    fn Application_declared() {
        let triples = triples();
        assert!(has_iri(
            &triples,
            &format!("{KG}Application"),
            RDF_TYPE,
            OWL_CLASS
        ));
    }

    // spec: EG-CQ-CONN-R078.3
    #[test]
    fn OidcClient_declared() {
        let triples = triples();
        let class = format!("{KG}OidcClient");
        assert!(has_iri(&triples, &class, RDF_TYPE, OWL_CLASS));
        assert!(has_iri(
            &triples,
            &class,
            RDFS_SUBCLASSOF,
            &format!("{KG}Application")
        ));
    }

    // spec: EG-CQ-CONN-R078.4
    #[test]
    fn roleGrantsAccessTo_declared() {
        let triples = triples();
        let property = format!("{KG}roleGrantsAccessTo");
        assert!(has_iri(&triples, &property, RDF_TYPE, OWL_OBJECT_PROPERTY));
        assert!(has_iri(
            &triples,
            &property,
            RDFS_DOMAIN,
            &format!("{KG}IdentityRole")
        ));
        assert!(has_iri(
            &triples,
            &property,
            RDFS_RANGE,
            &format!("{KG}Application")
        ));
    }
}
