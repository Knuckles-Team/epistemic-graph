//! Deterministic composition and validation of keyed graph schema sources.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::OnceLock;

use eg_rdf::oxrdf::{BlankNode, Graph, NamedOrBlankNode, Term, Triple};

use crate::graph::{GraphSchemaSource, GraphSchemaSources, SchemaSourceOrigin};

const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const SH_NODE_SHAPE: &str = "http://www.w3.org/ns/shacl#NodeShape";
const SH_PROPERTY_SHAPE: &str = "http://www.w3.org/ns/shacl#PropertyShape";
const OWL_ONTOLOGY: &str = "http://www.w3.org/2002/07/owl#Ontology";
const OWL_IMPORTS: &str = "http://www.w3.org/2002/07/owl#imports";
const SH_SPARQL: &str = "http://www.w3.org/ns/shacl#sparql";
const SH_SELECT: &str = "http://www.w3.org/ns/shacl#select";
const SH_ASK: &str = "http://www.w3.org/ns/shacl#ask";
const SHAPE_OWNING_PREDICATES: &[&str] = &[
    "http://www.w3.org/ns/shacl#targetClass",
    "http://www.w3.org/ns/shacl#targetNode",
    "http://www.w3.org/ns/shacl#targetSubjectsOf",
    "http://www.w3.org/ns/shacl#targetObjectsOf",
    "http://www.w3.org/ns/shacl#property",
    "http://www.w3.org/ns/shacl#path",
];
const OWL_DECLARATION_OBJECTS: &[&str] = &[
    "http://www.w3.org/2002/07/owl#Class",
    "http://www.w3.org/2002/07/owl#ObjectProperty",
    "http://www.w3.org/2002/07/owl#DatatypeProperty",
    "http://www.w3.org/2002/07/owl#AnnotationProperty",
    "http://www.w3.org/2002/07/owl#Ontology",
];

#[derive(Clone, Debug)]
pub(crate) struct ComposedSchema {
    pub(crate) shapes: Graph,
    pub(crate) ontology: Vec<Triple>,
}

pub(crate) fn validate_and_compose(sources: &GraphSchemaSources) -> Result<ComposedSchema, String> {
    sources.validate()?;
    if sources.dynamic.is_empty() {
        static IMMUTABLE_CORE: OnceLock<Result<ComposedSchema, String>> = OnceLock::new();
        return IMMUTABLE_CORE
            .get_or_init(|| compose_validated_sources(sources))
            .clone();
    }
    compose_validated_sources(sources)
}

/// Search steps (deterministic rule rounds and explored branches, see
/// `eg_rdf::tableau::is_consistent_within`) the full-ABox tableau may spend where schema
/// ENTERS a graph. Measured 2026-09-22 on the build host: the shipped core corpus
/// (1 384 individuals, 3 521 role assertions) is decided within 10⁶ steps and not
/// within 10⁵; this allows ten times the corpus for the attached documents.
const SCHEMA_ENTRY_ABOX_STEPS: u64 = 10_000_000;

/// [`SCHEMA_ENTRY_ABOX_STEPS`] as a budget; beyond it the attach fails closed.
const SCHEMA_ENTRY_ABOX_BUDGET: eg_rdf::owl::DerivationBudget =
    eg_rdf::owl::DerivationBudget::new(SCHEMA_ENTRY_ABOX_STEPS);

/// [`validate_and_compose`] plus the full-ABox tableau consistency check (EH-355
/// operator ruling, option (c)): run where schema ENTERS a graph — `GraphSchema`
/// attach and ConnectorPack projection — but not on restore, which replays schema that
/// already passed here and checks the terminology only. Bounded by
/// [`SCHEMA_ENTRY_ABOX_BUDGET`]: an ABox the tableau cannot decide within it is refused
/// with `VALIDATION_BUDGET_EXCEEDED`, never admitted undecided.
pub(crate) fn validate_entering_schema(
    sources: &GraphSchemaSources,
) -> Result<ComposedSchema, String> {
    let composed = validate_and_compose(sources)?;
    match eg_rdf::tableau::abox_consistency_within(&composed.ontology, SCHEMA_ENTRY_ABOX_BUDGET) {
        Ok(true) => Ok(composed),
        Ok(false) => Err(
            "ONTOLOGY_INCONSISTENT: the composed schema's individual assertions have no model"
                .to_string(),
        ),
        Err(exhausted) => Err(exhausted.to_string()),
    }
}

fn compose_validated_sources(sources: &GraphSchemaSources) -> Result<ComposedSchema, String> {
    let shapes = compose_documents(sources.shapes(), true)?;
    let ontology = compose_documents(sources.ontologies(), false)?;
    validate_named_shape_ownership(&shapes)?;
    validate_core_declaration_ownership(sources)?;
    let ontology_triples: Vec<Triple> = ontology.iter().map(|(_, triple)| triple.clone()).collect();
    eg_rdf::owl::validate_supported_profile(&ontology_triples)?;

    let mut shape_graph = Graph::new();
    for (_, triple) in &shapes {
        shape_graph.insert(triple);
    }
    let ontology: Vec<Triple> = ontology.into_iter().map(|(_, triple)| triple).collect();
    // TERMINOLOGY scope (EH-355, operator ruling (c)): this composition runs on every
    // restore and read, and decides whether the composed SCHEMA is coherent — the
    // unsatisfiable classes it reports below. The EL/RL completion still sees every
    // triple, but it classifies classes, not individuals; the individual assertions are
    // decided by the full-ABox tableau where schema enters a graph
    // ([`validate_entering_schema`]), not again on each restore.
    let classification = eg_rdf::tableau::reason_dl_terminology(&ontology);
    if !classification.consistent {
        let nothing = "<http://www.w3.org/2002/07/owl#Nothing>";
        let unsatisfiable: Vec<&str> = classification
            .subsumers
            .iter()
            .filter(|(_, supers)| supers.contains(nothing))
            .map(|(class, _)| class.as_str())
            .collect();
        return Err(format!(
            "ONTOLOGY_INCONSISTENT: unsatisfiable classes: {:?}",
            unsatisfiable
        ));
    }
    Ok(ComposedSchema {
        shapes: shape_graph,
        ontology,
    })
}

fn compose_documents<'a>(
    documents: impl Iterator<Item = (&'a str, &'a str)>,
    shapes: bool,
) -> Result<Vec<(String, Triple)>, String> {
    let mut composed = Vec::new();
    let mut seen = BTreeSet::new();
    let mut seen_documents = BTreeSet::new();
    for (source_id, document) in documents {
        reject_base_directive(source_id, document, shapes)?;
        let document_digest = eg_types::contract::Digest256::sha256(document.as_bytes()).to_hex();
        if !seen_documents.insert(document_digest) {
            continue;
        }
        let parsed = eg_rdf::mapping::parse_turtle(document).map_err(|error| {
            format!(
                "{}: source '{source_id}': {error}",
                if shapes {
                    "SHAPES_INVALID"
                } else {
                    "ONTOLOGY_INVALID"
                }
            )
        })?;
        if parsed.len() > crate::graph::MAX_SCHEMA_DOCUMENT_TRIPLES {
            return Err(format!(
                "SCHEMA_SOURCES_TOO_LARGE: source '{source_id}' has {} triples; maximum is {}",
                parsed.len(),
                crate::graph::MAX_SCHEMA_DOCUMENT_TRIPLES
            ));
        }
        validate_imports(source_id, &parsed, shapes)?;
        if shapes {
            reject_remote_sparql_service(source_id, &parsed)?;
        }
        let scope =
            hex::encode(eg_types::contract::Digest256::sha256(source_id.as_bytes()).as_bytes());
        for triple in parsed {
            let triple = scope_blank_nodes(triple, &scope[..16])?;
            let key = triple.to_string();
            if seen.insert(key) {
                composed.push((source_id.to_string(), triple));
            }
        }
    }
    composed.sort_by_cached_key(|(_, triple)| triple.to_string());
    Ok(composed)
}

fn reject_base_directive(source_id: &str, document: &str, shapes: bool) -> Result<(), String> {
    let has_base = document.lines().any(|line| {
        let statement = line.split('#').next().unwrap_or_default().trim_start();
        let lower = statement.to_ascii_lowercase();
        lower.starts_with("@base")
            || lower
                .strip_prefix("base")
                .is_some_and(|suffix| suffix.chars().next().is_some_and(char::is_whitespace))
    });
    if has_base {
        return Err(format!(
            "{}: source '{source_id}' declares a base IRI",
            if shapes {
                "SHAPES_INVALID"
            } else {
                "ONTOLOGY_INVALID"
            }
        ));
    }
    Ok(())
}

fn validate_imports(source_id: &str, triples: &[Triple], shapes: bool) -> Result<(), String> {
    // Engine-authored modules are an atomic, digest-pinned catalog and may
    // import the engine foundation. Dynamic documents may only name their own
    // declared document IRI; imports are identity assertions and are never
    // dereferenced by composition.
    if source_id.starts_with(crate::graph::CORE_SOURCE_PREFIX) {
        return Ok(());
    }
    let document_iris: BTreeSet<&str> = triples
        .iter()
        .filter_map(|triple| {
            if triple.predicate.as_str() != RDF_TYPE
                || !matches!(&triple.object, Term::NamedNode(node) if node.as_str() == OWL_ONTOLOGY)
            {
                return None;
            }
            match &triple.subject {
                NamedOrBlankNode::NamedNode(node) => Some(node.as_str()),
                NamedOrBlankNode::BlankNode(_) => None,
            }
        })
        .collect();
    for triple in triples
        .iter()
        .filter(|triple| triple.predicate.as_str() == OWL_IMPORTS)
    {
        let allowed = matches!(&triple.object, Term::NamedNode(node) if document_iris.contains(node.as_str()));
        if !allowed {
            return Err(format!(
                "{}: source '{source_id}' imports a document outside its own authority",
                if shapes {
                    "SHAPES_INVALID"
                } else {
                    "ONTOLOGY_INVALID"
                }
            ));
        }
    }
    Ok(())
}

fn reject_remote_sparql_service(source_id: &str, triples: &[Triple]) -> Result<(), String> {
    let constraints: BTreeSet<&str> = triples
        .iter()
        .filter_map(|triple| {
            if triple.predicate.as_str() != SH_SPARQL {
                return None;
            }
            match &triple.object {
                Term::BlankNode(node) => Some(node.as_str()),
                _ => None,
            }
        })
        .collect();
    let contains_service = triples.iter().any(|triple| {
        let NamedOrBlankNode::BlankNode(subject) = &triple.subject else {
            return false;
        };
        if !constraints.contains(subject.as_str())
            || !matches!(triple.predicate.as_str(), SH_SELECT | SH_ASK)
        {
            return false;
        }
        matches!(&triple.object, Term::Literal(literal) if has_keyword(literal.value(), "service"))
    });
    if contains_service {
        return Err(format!(
            "SHAPES_INVALID: source '{source_id}' contains a remote SPARQL SERVICE clause"
        ));
    }
    Ok(())
}

fn has_keyword(text: &str, keyword: &str) -> bool {
    text.split(|character: char| !character.is_ascii_alphanumeric() && character != '_')
        .any(|token| token.eq_ignore_ascii_case(keyword))
}

pub(crate) fn scope_blank_nodes(triple: Triple, scope: &str) -> Result<Triple, String> {
    let subject = match triple.subject {
        NamedOrBlankNode::NamedNode(node) => NamedOrBlankNode::NamedNode(node),
        NamedOrBlankNode::BlankNode(node) => NamedOrBlankNode::BlankNode(scoped(node, scope)?),
    };
    let object = match triple.object {
        Term::NamedNode(node) => Term::NamedNode(node),
        Term::BlankNode(node) => Term::BlankNode(scoped(node, scope)?),
        Term::Literal(literal) => Term::Literal(literal),
        #[cfg(feature = "sparql-star")]
        Term::Triple(triple) => Term::Triple(Box::new(scope_blank_nodes(*triple, scope)?)),
    };
    Ok(Triple::new(subject, triple.predicate, object))
}

fn scoped(node: BlankNode, scope: &str) -> Result<BlankNode, String> {
    BlankNode::new(format!("s{scope}_{}", node.as_str()))
        .map_err(|error| format!("schema blank-node scoping failed: {error}"))
}

fn validate_named_shape_ownership(shapes: &[(String, Triple)]) -> Result<(), String> {
    let mut roots: BTreeMap<String, (String, BTreeSet<String>)> = BTreeMap::new();
    let shape_subjects: BTreeSet<String> = shapes
        .iter()
        .filter(|(_, triple)| is_shape_owning_triple(triple))
        .filter_map(|(_, triple)| match &triple.subject {
            NamedOrBlankNode::NamedNode(node) => Some(node.as_str().to_string()),
            NamedOrBlankNode::BlankNode(_) => None,
        })
        .collect();
    for (source, triple) in shapes {
        let NamedOrBlankNode::NamedNode(subject) = &triple.subject else {
            continue;
        };
        if !shape_subjects.contains(subject.as_str()) {
            continue;
        }
        let entry = roots
            .entry(subject.as_str().to_string())
            .or_insert_with(|| (source.clone(), BTreeSet::new()));
        if entry.0 != *source {
            return Err(format!(
                "SCHEMA_SOURCE_CONFLICT: named shape <{}> is owned by '{}' and '{}'",
                subject.as_str(),
                entry.0,
                source
            ));
        }
        entry.1.insert(triple.to_string());
    }
    Ok(())
}

fn is_shape_owning_triple(triple: &Triple) -> bool {
    (triple.predicate.as_str() == RDF_TYPE
        && matches!(&triple.object, Term::NamedNode(node) if matches!(node.as_str(), SH_NODE_SHAPE | SH_PROPERTY_SHAPE)))
        || SHAPE_OWNING_PREDICATES.contains(&triple.predicate.as_str())
}

fn validate_core_declaration_ownership(sources: &GraphSchemaSources) -> Result<(), String> {
    let core = core_declarations(&sources.core)?;
    for (source_id, source) in &sources.dynamic {
        validate_dynamic_source_ownership(source_id, source, &core)?;
    }
    Ok(())
}

fn core_declarations(
    sources: &BTreeMap<String, GraphSchemaSource>,
) -> Result<BTreeMap<String, BTreeSet<String>>, String> {
    let mut core = BTreeMap::<String, BTreeSet<String>>::new();
    for (source_id, source) in sources {
        let Some(document) = source.ontology_ttl.as_deref() else {
            continue;
        };
        for triple in eg_rdf::mapping::parse_turtle(document)
            .map_err(|error| format!("ONTOLOGY_INVALID: source '{source_id}': {error}"))?
        {
            if is_named_declaration(&triple) {
                core.entry(triple.subject.to_string())
                    .or_default()
                    .insert(triple.to_string());
            }
        }
    }
    Ok(core)
}

fn validate_dynamic_source_ownership(
    source_id: &str,
    source: &GraphSchemaSource,
    core: &BTreeMap<String, BTreeSet<String>>,
) -> Result<(), String> {
    if matches!(source.origin, SchemaSourceOrigin::Core { .. }) {
        return Err(format!(
            "SCHEMA_SOURCE_CONFLICT: '{source_id}' forges core origin"
        ));
    }
    let Some(document) = source.ontology_ttl.as_deref() else {
        return Ok(());
    };
    let triples = eg_rdf::mapping::parse_turtle(document)
        .map_err(|error| format!("ONTOLOGY_INVALID: source '{source_id}': {error}"))?;
    validate_dynamic_declarations(source_id, core, triples)
}

fn validate_dynamic_declarations(
    source_id: &str,
    core: &BTreeMap<String, BTreeSet<String>>,
    triples: Vec<Triple>,
) -> Result<(), String> {
    for triple in triples {
        if !is_named_declaration(&triple) {
            continue;
        }
        let Some(core_declarations) = core.get(&triple.subject.to_string()) else {
            continue;
        };
        if !core_declarations.contains(&triple.to_string()) {
            return Err(format!(
                "SCHEMA_SOURCE_CONFLICT: source '{source_id}' redefines core term {}",
                triple.subject
            ));
        }
    }
    Ok(())
}

fn is_named_declaration(triple: &Triple) -> bool {
    triple.predicate.as_str() == RDF_TYPE
        && matches!(triple.subject, NamedOrBlankNode::NamedNode(_))
        && matches!(&triple.object, Term::NamedNode(node) if OWL_DECLARATION_OBJECTS.contains(&node.as_str()))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::graph::{GraphSchemaSource, SchemaSourceOrigin};

    fn admin(name: &str, shapes: Option<&str>, ontology: Option<&str>) -> GraphSchemaSource {
        GraphSchemaSource::new(
            SchemaSourceOrigin::Admin {
                name: name.to_string(),
            },
            shapes.map(Arc::from),
            ontology.map(Arc::from),
            0,
        )
        .unwrap()
    }

    #[test]
    fn blank_nodes_are_scoped_per_distinct_document() {
        let mut sources = GraphSchemaSources::default();
        let left = r#"@prefix ex: <http://example/> . ex:A ex:p [ ex:v "left" ] ."#;
        let right = r#"@prefix ex: <http://example/> . ex:B ex:p [ ex:v "right" ] ."#;
        sources
            .attach_dynamic("admin:left".to_string(), admin("left", Some(left), None))
            .unwrap();
        sources
            .attach_dynamic("admin:right".to_string(), admin("right", Some(right), None))
            .unwrap();
        let composed = validate_and_compose(&sources).unwrap();
        let blanks: BTreeSet<String> = composed
            .shapes
            .iter()
            .filter_map(|triple| match &triple.object {
                eg_rdf::oxrdf::TermRef::BlankNode(node) => Some(node.as_str().to_string()),
                _ => None,
            })
            .collect();
        assert_eq!(blanks.len(), 2);
    }

    #[test]
    fn byte_identical_shape_documents_dedupe_without_conflict() {
        let document = r#"
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix ex: <http://example/> .
ex:Shape a sh:NodeShape ; sh:property [ sh:path ex:value ] .
"#;
        let mut sources = GraphSchemaSources::default();
        sources
            .attach_dynamic(
                "admin:first".to_string(),
                admin("first", Some(document), None),
            )
            .unwrap();
        sources
            .attach_dynamic(
                "admin:second".to_string(),
                admin("second", Some(document), None),
            )
            .unwrap();
        validate_and_compose(&sources).unwrap();
    }

    #[test]
    fn overlapping_named_shapes_are_rejected() {
        let mut sources = GraphSchemaSources::default();
        let prefix = "@prefix sh: <http://www.w3.org/ns/shacl#> . @prefix ex: <http://example/> . ";
        sources
            .attach_dynamic(
                "admin:first".to_string(),
                admin(
                    "first",
                    Some(&format!(
                        "{prefix} ex:Shape a sh:NodeShape ; sh:targetClass ex:A ."
                    )),
                    None,
                ),
            )
            .unwrap();
        sources
            .attach_dynamic(
                "admin:second".to_string(),
                admin(
                    "second",
                    Some(&format!(
                        "{prefix} ex:Shape a sh:NodeShape ; sh:targetClass ex:B ."
                    )),
                    None,
                ),
            )
            .unwrap();
        assert!(validate_and_compose(&sources)
            .unwrap_err()
            .contains("SCHEMA_SOURCE_CONFLICT"));
    }

    #[test]
    fn a_targeted_shape_iri_is_owned_even_without_an_explicit_shape_type() {
        let mut sources = GraphSchemaSources::default();
        let prefixes =
            "@prefix sh: <http://www.w3.org/ns/shacl#> . @prefix ex: <http://example/> . ";
        sources
            .attach_dynamic(
                "admin:first".to_string(),
                admin(
                    "first",
                    Some(&format!("{prefixes} ex:Shape sh:targetClass ex:A .")),
                    None,
                ),
            )
            .unwrap();
        sources
            .attach_dynamic(
                "admin:second".to_string(),
                admin(
                    "second",
                    Some(&format!("{prefixes} ex:Shape sh:deactivated true .")),
                    None,
                ),
            )
            .unwrap();
        assert!(validate_and_compose(&sources)
            .unwrap_err()
            .contains("SCHEMA_SOURCE_CONFLICT"));
    }

    #[test]
    fn dynamic_source_cannot_redeclare_a_core_term_differently() {
        let mut sources = GraphSchemaSources::default();
        let ontology = r#"
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix eg: <http://knuckles.team/kg#> .
eg:Tool a owl:ObjectProperty .
"#;
        sources
            .attach_dynamic(
                "admin:override".to_string(),
                admin("override", None, Some(ontology)),
            )
            .unwrap();
        assert!(validate_and_compose(&sources)
            .unwrap_err()
            .contains("redefines core term"));
    }

    #[test]
    fn dynamic_documents_cannot_choose_a_base_iri() {
        let mut sources = GraphSchemaSources::default();
        sources
            .attach_dynamic(
                "admin:base".to_string(),
                admin(
                    "base",
                    Some("@base <http://example/> . <Shape> <p> <o> ."),
                    None,
                ),
            )
            .unwrap();
        assert!(validate_and_compose(&sources)
            .unwrap_err()
            .contains("SHAPES_INVALID"));
    }

    #[test]
    fn remote_sparql_service_in_a_shape_fails_closed() {
        let mut sources = GraphSchemaSources::default();
        let shapes = r#"
@prefix sh: <http://www.w3.org/ns/shacl#> .
@prefix ex: <http://example/> .
ex:Shape a sh:NodeShape ; sh:targetClass ex:Thing ;
  sh:sparql [ sh:select "SELECT $this WHERE { SERVICE <https://remote.invalid/sparql> { $this ?p ?o } }" ] .
"#;
        sources
            .attach_dynamic(
                "admin:service".to_string(),
                admin("service", Some(shapes), None),
            )
            .unwrap();
        let error = validate_and_compose(&sources).unwrap_err();
        assert!(error.contains("SHAPES_INVALID"));
        assert!(error.contains("SERVICE"));
    }

    #[test]
    fn dynamic_import_may_only_name_its_own_document() {
        let mut sources = GraphSchemaSources::default();
        let external = r#"
@prefix owl: <http://www.w3.org/2002/07/owl#> .
<http://example/own> a owl:Ontology ; owl:imports <http://example/other> .
"#;
        sources
            .attach_dynamic(
                "admin:external".to_string(),
                admin("external", None, Some(external)),
            )
            .unwrap();
        assert!(validate_and_compose(&sources)
            .unwrap_err()
            .contains("outside its own authority"));

        let mut self_import = GraphSchemaSources::default();
        let own = r#"
@prefix owl: <http://www.w3.org/2002/07/owl#> .
<http://example/own> a owl:Ontology ; owl:imports <http://example/own> .
"#;
        self_import
            .attach_dynamic("admin:own".to_string(), admin("own", None, Some(own)))
            .unwrap();
        validate_and_compose(&self_import).unwrap();
    }

    #[test]
    fn unsupported_owl_axiom_fails_closed_with_typed_code() {
        let mut sources = GraphSchemaSources::default();
        let unsupported = r#"
@prefix ex: <http://example/> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
ex:parent a owl:AsymmetricProperty .
"#;
        sources
            .attach_dynamic(
                "admin:unsupported".to_string(),
                admin("unsupported", None, Some(unsupported)),
            )
            .unwrap();
        let error = validate_and_compose(&sources).unwrap_err();
        assert!(error.contains("OWL_UNSUPPORTED_CONSTRUCT"));
        assert!(error.contains("AsymmetricProperty"));
    }

    #[test]
    fn capability_fixture_retains_the_required_mixed_profile_axioms() {
        let capability = include_str!("../../../crates/eg-core/ontology/capability-v1.ttl");
        for required in [
            "owl:minCardinality",
            "owl:inverseOf",
            "owl:SymmetricProperty",
            "owl:TransitiveProperty",
            "owl:propertyChainAxiom",
        ] {
            assert!(capability.contains(required), "missing {required}");
        }
    }

    #[test]
    fn core_foundation_contains_the_complete_migrated_root_axiom_body() {
        let foundation = include_str!("../../../crates/eg-core/ontology/core-foundation-v1.ttl");
        let triples = eg_rdf::mapping::parse_turtle(foundation).unwrap();
        // 880 migrated triples less the two list-cell triples of `:Incident`, which
        // was removed from the Person/Organization/Server/Event disjointness because
        // it is a subclass of `:Event` (EH-356), plus the 8 triples declaring BFO
        // realizable entity and disposition, the category of `:Skill`.
        assert_eq!(triples.len(), 886);
        for required in [
            "http://knuckles.team/kg#Concept",
            "http://knuckles.team/kg#Evidence",
            "http://knuckles.team/kg#Tool",
            "http://knuckles.team/kg#derivedFrom",
            "http://knuckles.team/kg#validFrom",
        ] {
            assert!(triples.iter().any(|triple| {
                triple.subject.to_string().contains(required)
                    || triple.object.to_string().contains(required)
            }));
        }
        assert!(!foundation.contains("owl:imports"));
        assert!(foundation.contains("<http://knuckles.team/kg/core> a owl:Ontology"));
    }

    #[test]
    fn complete_core_corpus_pins_the_approved_authority_migration_delta() {
        let composed = validate_and_compose(&GraphSchemaSources::default()).unwrap();
        // 12,600 as migrated, less the 10 triples EH-356 removed to make the corpus
        // coherent: module-local domain/range on the shared kg:derivedFrom (sdd,
        // capability) and kg:dependsOn (software) — 6; module-local BFO recategorisation
        // of the core :Skill (a2a) and :LegalEntity (company) — 2; `:Incident` in the
        // AllDisjointClasses list of its own superclass `:Event` — 2. Then +99: the
        // module-local domain/range of 12 other shared properties (and the double domain
        // of infrastructure's :runsOn) moved onto 24 module-local sub-properties. Then
        // +8: BFO realizable entity and disposition, declared for `:Skill`.
        assert_eq!(composed.ontology.len(), 12_697);

        let ontology_subjects: BTreeSet<String> = composed
            .ontology
            .iter()
            .filter(|triple| {
                triple.predicate.as_str() == RDF_TYPE
                    && matches!(&triple.object, Term::NamedNode(node) if node.as_str() == OWL_ONTOLOGY)
            })
            .map(|triple| triple.subject.to_string())
            .collect();
        let imports = composed
            .ontology
            .iter()
            .filter(|triple| triple.predicate.as_str() == OWL_IMPORTS)
            .count();
        let semantic_axioms = composed
            .ontology
            .iter()
            .filter(|triple| {
                triple.predicate.as_str() != OWL_IMPORTS
                    && !ontology_subjects.contains(&triple.subject.to_string())
            })
            .count();

        // The former 29-file AU union has 12,671 raw triples. Its cyclic root
        // ontology/catalog metadata is intentionally replaced by the EG-owned
        // `/core` foundation and generated aggregate catalog. Every domain
        // axiom remains in the immutable catalog; only those 71 document-level
        // authority triples change.
        assert_eq!(ontology_subjects.len(), 30);
        assert_eq!(imports, 59);
        assert_eq!(semantic_axioms, 12_542);

        let count_type = |object: &str| {
            composed
                .ontology
                .iter()
                .filter(|triple| {
                    triple.predicate.as_str() == RDF_TYPE
                        && matches!(&triple.object, Term::NamedNode(node) if node.as_str() == object)
                })
                .count()
        };
        let count_predicate = |predicate: &str| {
            composed
                .ontology
                .iter()
                .filter(|triple| triple.predicate.as_str() == predicate)
                .count()
        };
        assert_eq!(
            count_type("http://www.w3.org/2002/07/owl#TransitiveProperty"),
            18
        );
        assert_eq!(
            count_type("http://www.w3.org/2002/07/owl#SymmetricProperty"),
            8
        );
        assert_eq!(
            count_predicate("http://www.w3.org/2002/07/owl#inverseOf"),
            30
        );
        assert_eq!(
            count_predicate("http://www.w3.org/2002/07/owl#propertyChainAxiom"),
            4
        );
        assert_eq!(
            count_predicate("http://www.w3.org/2002/07/owl#minCardinality"),
            8
        );
        assert_eq!(
            composed
                .ontology
                .iter()
                .filter(|triple| {
                    triple.predicate.as_str() == "http://www.w3.org/2000/01/rdf-schema#subClassOf"
                        && matches!(&triple.subject, NamedOrBlankNode::NamedNode(_))
                        && matches!(&triple.object, Term::NamedNode(_))
                })
                .count(),
            379
        );
    }

    /// EH-356: the shipped corpus must be coherent under the EL/RL reasoner itself —
    /// consistent, with no unsatisfiable named class — not only under whichever engine
    /// the compose gate routes to. The EL/RL completion once derived
    /// `BFO:Entity ⊑ kg:Code` from this corpus (an unsound range rule) and collapsed the
    /// root class, and the corpus itself carried five genuinely unsatisfiable classes.
    #[test]
    fn shipped_core_corpus_is_coherent_under_the_el_rl_reasoner() {
        let composed = validate_and_compose(&GraphSchemaSources::default()).unwrap();
        let classification = eg_rdf::owl::Reasoner::from_triples(&composed.ontology).classify();
        assert!(
            classification.unsatisfiable.is_empty(),
            "unsatisfiable named classes: {:?}",
            classification.unsatisfiable
        );
        assert!(classification.consistent);
        let entity = "<http://purl.obolibrary.org/obo/BFO_0000001>";
        assert_eq!(
            classification.subsumers[entity],
            BTreeSet::from([
                entity.to_string(),
                "<http://www.w3.org/2002/07/owl#Thing>".to_string()
            ]),
            "the BFO root must subsume nothing but itself and owl:Thing"
        );
        // Operator ruling 2026-09-22: a skill is a capacity its bearer can realize — a
        // BFO disposition (a specifically dependent continuant), not an independent
        // continuant and not an information artifact.
        let skill = &classification.subsumers["<http://knuckles.team/kg#Skill>"];
        let bfo = |id: &str| format!("<http://purl.obolibrary.org/obo/BFO_{id}>");
        assert!(
            skill.contains(&bfo("0000016")),
            "Skill ⊑ disposition: {skill:?}"
        );
        assert!(skill.contains(&bfo("0000020")));
        assert!(!skill.contains(&bfo("0000004")));
        assert!(!skill.contains(&bfo("0000031")));
    }

    /// A module must not constrain a shared property: OWL intersects every
    /// `rdfs:domain`/`rdfs:range` a property carries, so two modules' different
    /// meanings of one property type every edge as both (EH-356 found
    /// `derivedFrom`/`dependsOn`; twelve more were moved to module-local
    /// sub-properties). Each property's domain and range come from ONE core document.
    #[test]
    fn no_property_is_constrained_by_two_core_modules() {
        const DOMAIN: &str = "http://www.w3.org/2000/01/rdf-schema#domain";
        const RANGE: &str = "http://www.w3.org/2000/01/rdf-schema#range";
        let sources = GraphSchemaSources::default();
        let mut constrained_by: BTreeMap<String, BTreeSet<&str>> = BTreeMap::new();
        for (source_id, document) in sources.ontologies() {
            for triple in eg_rdf::mapping::parse_turtle(document).unwrap() {
                if matches!(triple.predicate.as_str(), DOMAIN | RANGE) {
                    constrained_by
                        .entry(triple.subject.to_string())
                        .or_default()
                        .insert(source_id);
                }
            }
        }
        let shared: Vec<_> = constrained_by
            .iter()
            .filter(|(_, modules)| modules.len() > 1)
            .collect();
        assert!(shared.is_empty(), "{shared:?}");
    }

    #[test]
    fn governance_core_slice_is_exactly_four_shapes_and_57_triples() {
        let document =
            include_str!("../../../crates/eg-core/ontology/governance-core-v1.shapes.ttl");
        let triples = eg_rdf::mapping::parse_turtle(document).unwrap();
        assert_eq!(triples.len(), 57);
        let roots: BTreeSet<String> = triples
            .iter()
            .filter(|triple| {
                triple.predicate.as_str() == RDF_TYPE
                    && matches!(&triple.object, Term::NamedNode(node) if node.as_str() == SH_NODE_SHAPE)
            })
            .filter_map(|triple| match &triple.subject {
                NamedOrBlankNode::NamedNode(node) => Some(node.as_str().to_string()),
                NamedOrBlankNode::BlankNode(_) => None,
            })
            .collect();
        assert_eq!(roots.len(), 4);
        assert!(roots.iter().all(|root| matches!(
            root.rsplit('#').next(),
            Some("ADRShape" | "CapabilityShape" | "PolicyShape" | "ToolShape")
        )));
        let blank_nodes: BTreeSet<String> = triples
            .iter()
            .flat_map(|triple| {
                let subject = match &triple.subject {
                    NamedOrBlankNode::BlankNode(node) => Some(node.as_str().to_string()),
                    NamedOrBlankNode::NamedNode(_) => None,
                };
                let object = match &triple.object {
                    Term::BlankNode(node) => Some(node.as_str().to_string()),
                    _ => None,
                };
                subject.into_iter().chain(object)
            })
            .collect();
        assert_eq!(blank_nodes.len(), 8);
    }
}
