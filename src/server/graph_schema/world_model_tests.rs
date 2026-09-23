//! World-model base ontology (operator ruling 2026-09-22, night): the module stays
//! coherent and bounded inside the full core corpus, and its wiring entails what the
//! connectors rely on — an organism in a habitat is located in the habitat's region,
//! a nutritional requirement is borne by an organism, a weather event occurs in a
//! weather system, and taxa are data (individuals), never classes.

use std::collections::BTreeSet;
use std::time::Instant;

use eg_rdf::oxrdf::{NamedNode, NamedOrBlankNode, Term, Triple};

use super::compose::{scope_blank_nodes, validate_and_compose};
use crate::graph::GraphSchemaSources;

const KG: &str = "http://knuckles.team/kg#";
const FOX: &str = "<http://example.org/world#fox>";
const WOODLAND: &str = "<http://example.org/world#woodland>";
const BAVARIA: &str = "<http://example.org/world#bavaria>";
const VULPES: &str = "<http://example.org/world#vulpes>";
const FOREST: &str = "<http://example.org/world#forest>";
const NEED_C: &str = "<http://example.org/world#needC>";
const VITAMIN_C: &str = "<http://example.org/world#vitaminC>";
const STORM: &str = "<http://example.org/world#storm>";
const BORNE_BY_ORGANISM: &str = "<http://example.org/world#BorneByOrganism>";
const IN_WEATHER_SYSTEM: &str = "<http://example.org/world#InWeatherSystem>";
const BFO_PROCESS: &str = "<http://purl.obolibrary.org/obo/BFO_0000015>";
const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const RDFS_SUBCLASS_OF: &str = "http://www.w3.org/2000/01/rdf-schema#subClassOf";
const OWL_CLASS: &str = "http://www.w3.org/2002/07/owl#Class";
const OWL_IMPORTS: &str = "http://www.w3.org/2002/07/owl#imports";
const SKOS_MAPPINGS: &[&str] = &[
    "http://www.w3.org/2004/02/skos/core#exactMatch",
    "http://www.w3.org/2004/02/skos/core#closeMatch",
    "http://www.w3.org/2004/02/skos/core#broadMatch",
    "http://www.w3.org/2004/02/skos/core#relatedMatch",
];
/// The modules the world-model wiring touches; entailment tests reason over these
/// plus a fixture, not the whole corpus, so each instance check stays small.
const WIRED_MODULES: &[&str] = &[
    "core:foundation@1",
    "core:energy_geopolitics@1",
    "core:medical@1",
    "core:world_model@1",
];
/// Search steps the shipped corpus's full-ABox tableau check needed before the world
/// model (< 10⁶, measured by eg-reasoning 2026-09-22); the world model must not push
/// the corpus past it.
const CORPUS_ABOX_STEPS: u64 = 1_000_000;

const FIXTURE: &str = r#"
@prefix : <http://knuckles.team/kg#> .
@prefix ex: <http://example.org/world#> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .

ex:BorneByOrganism owl:equivalentClass [ a owl:Restriction ;
    owl:onProperty :inheresIn ; owl:someValuesFrom :Organism ] .
ex:InWeatherSystem owl:equivalentClass [ a owl:Restriction ;
    owl:onProperty :occursIn ; owl:someValuesFrom :WeatherSystem ] .

ex:vulpes a :Taxon ; :scientificName "Vulpes vulpes" ; :termCurie "NCBITaxon:9627" ;
    :taxonRank :rank-species .
ex:forest a :EnvironmentTerm ; :termCurie "ENVO:01000174" .
ex:bavaria a :Region .
ex:fox :inTaxon ex:vulpes ; :hasHabitat ex:woodland .
ex:woodland :instanceOfTerm ex:forest ; :locatedIn ex:bavaria .

ex:vitaminC a :Nutrient ; :termCurie "CHEBI:29073" .
ex:needC a :NutritionalRequirement ; :nutritionalRequirementOf ex:fox ;
    :requirementForNutrient ex:vitaminC .
ex:bareNeed a :NutritionalRequirement .

ex:storm a :WeatherEvent ; :occurredAt ex:bavaria .
ex:flu a :ClinicalCondition .
"#;

fn kg(local: &str) -> String {
    format!("<{KG}{local}>")
}

fn parse_scoped(document: &str, scope: &str) -> Vec<Triple> {
    eg_rdf::mapping::parse_turtle(document)
        .unwrap()
        .into_iter()
        .map(|triple| scope_blank_nodes(triple, scope).unwrap())
        .collect()
}

/// The wired modules plus [`FIXTURE`], each document's blank nodes scoped apart.
fn wired_with_fixture() -> Vec<Triple> {
    let sources = GraphSchemaSources::default();
    let mut triples = Vec::new();
    for (index, (source_id, document)) in sources.ontologies().enumerate() {
        if WIRED_MODULES.contains(&source_id) {
            triples.extend(parse_scoped(document, &format!("m{index}")));
        }
    }
    triples.extend(parse_scoped(FIXTURE, "fixture"));
    triples
}

/// OWL 2 RL materialization (subclass, sub-property, domain/range, inverse, chains) of
/// the wired modules plus the fixture.
fn materialized() -> eg_rdf::rules::RuleReasonResult {
    let triples = wired_with_fixture();
    let ontology = eg_rdf::owl::parse_ontology(&triples);
    eg_rdf::rules::reason_triples(&triples, &ontology, &Default::default())
}

fn subject_iri(triple: &Triple) -> Option<&str> {
    match &triple.subject {
        NamedOrBlankNode::NamedNode(node) => Some(node.as_str()),
        NamedOrBlankNode::BlankNode(_) => None,
    }
}

fn object_iri(triple: &Triple) -> Option<&str> {
    match &triple.object {
        Term::NamedNode(node) => Some(node.as_str()),
        _ => None,
    }
}

#[test]
fn world_model_is_one_core_module_and_the_catalog_is_at_its_bound() {
    let sources = GraphSchemaSources::default();
    assert!(sources.core.contains_key("core:world_model@1"));
    assert_eq!(sources.core.len(), crate::graph::MAX_CORE_SCHEMA_SOURCES);
    let composed = validate_and_compose(&sources).unwrap();
    let classification = eg_rdf::owl::Reasoner::from_triples(&composed.ontology).classify();
    assert!(classification.consistent);
    assert!(
        classification.unsatisfiable.is_empty(),
        "{:?}",
        classification.unsatisfiable
    );
}

/// EH-355 budgets with the world model in the corpus: the restore-path terminology
/// check stays consistent and the full-ABox entry check is decided within the step
/// budget the pre-world-model corpus needed. Timings are printed for the WRAPUP
/// (eg-reasoning measured 2.6 s terminology, 2.7 s full ABox on R820).
#[test]
fn full_corpus_stays_within_the_recorded_reasoning_budgets() {
    let composed = validate_and_compose(&GraphSchemaSources::default()).unwrap();
    let started = Instant::now();
    let terminology = eg_rdf::tableau::reason_dl_terminology(&composed.ontology);
    let terminology_elapsed = started.elapsed();
    assert!(terminology.consistent);
    let started = Instant::now();
    let budget = eg_rdf::owl::DerivationBudget::new(CORPUS_ABOX_STEPS);
    let abox = eg_rdf::tableau::abox_consistency_within(&composed.ontology, budget);
    let abox_elapsed = started.elapsed();
    assert_eq!(abox, Ok(true));
    eprintln!(
        "world-model corpus: {} triples; terminology {terminology_elapsed:?}; \
         full ABox {abox_elapsed:?} within {CORPUS_ABOX_STEPS} steps",
        composed.ontology.len()
    );
}

#[test]
fn world_model_classes_sit_under_the_bfo_categories() {
    let composed = validate_and_compose(&GraphSchemaSources::default()).unwrap();
    let classification = eg_rdf::owl::Reasoner::from_triples(&composed.ontology).classify();
    let bfo = |id: &str| format!("<http://purl.obolibrary.org/obo/BFO_{id}>");
    let placed = [
        ("Organism", "0000030"),
        ("Patient", "0000030"),
        ("AnatomicalEntity", "0000040"),
        ("Habitat", "0000040"),
        ("Food", "0000040"),
        ("BiologicalProcess", "0000015"),
        ("WeatherObservation", "0000015"),
        ("NutritionalRequirement", "0000016"),
        ("Taxon", "0000031"),
        ("Nutrient", "0000031"),
        ("NutrientReferenceIntake", "0000031"),
    ];
    for (class, category) in placed {
        let subsumers = &classification.subsumers[&kg(class)];
        assert!(
            subsumers.contains(&bfo(category)),
            "{class} ⋢ BFO_{category}"
        );
    }
    let taxon = &classification.subsumers[&kg("Taxon")];
    assert!(
        !taxon.contains(&bfo("0000004")),
        "a taxon is not a material thing"
    );
    for place in ["Region", "Country", "Habitat", "WeatherSystem"] {
        assert!(classification.subsumers[&kg(place)].contains(&kg("Place")));
    }
}

#[test]
fn an_organism_with_a_habitat_is_located_in_the_habitats_region() {
    let result = materialized();
    assert!(result.holds(&kg("locatedIn"), &[FOX, WOODLAND]));
    assert!(result.holds(&kg("locatedIn"), &[FOX, BAVARIA]));
    assert!(result.holds(&kg("contains"), &[BAVARIA, FOX]));
    assert!(result.holds(&kg("habitatOf"), &[WOODLAND, FOX]));
    assert!(result.holds(&kg("Organism"), &[FOX]));
    assert!(result.holds(&kg("Habitat"), &[WOODLAND]));
    assert!(result.holds(&kg("Place"), &[BAVARIA]));
    assert!(result.holds(&kg("classifiedAs"), &[FOX, VULPES]));
    assert!(result.holds(&kg("classifiedAs"), &[WOODLAND, FOREST]));
}

#[test]
fn a_nutritional_requirement_is_borne_by_an_organism_that_requires_its_nutrient() {
    let result = materialized();
    assert!(result.holds(&kg("inheresIn"), &[NEED_C, FOX]));
    assert!(result.holds(&kg("bearerOf"), &[FOX, NEED_C]));
    assert!(result.holds(&kg("requiresNutrient"), &[FOX, VITAMIN_C]));
    assert!(result.holds(&kg("ChemicalEntityTerm"), &[VITAMIN_C]));

    // A requirement (or a clinical condition) nobody named a bearer for is still
    // borne by SOME organism.
    let dl = eg_rdf::tableau::parse_dl_ontology(&wired_with_fixture());
    for unbound in ["bareNeed", "flu"] {
        let individual = format!("<http://example.org/world#{unbound}>");
        assert!(eg_rdf::tableau::is_instance(
            &dl,
            &individual,
            BORNE_BY_ORGANISM
        ));
    }
}

#[test]
fn a_weather_event_occurs_in_a_weather_system_and_its_region() {
    let dl = eg_rdf::tableau::parse_dl_ontology(&wired_with_fixture());
    assert!(eg_rdf::tableau::is_instance(&dl, STORM, IN_WEATHER_SYSTEM));
    assert!(eg_rdf::tableau::is_instance(&dl, STORM, BFO_PROCESS));
    assert!(materialized().holds(&kg("occursIn"), &[STORM, BAVARIA]));
}

/// A taxon (a GDC) can never be an organism (an IC): the core
/// `AllDisjointClasses(IC, GDC, Process, TemporalRegion)` makes their intersection
/// unsatisfiable. Checked on the EL⁺/RL classification, which expands the n-ary
/// disjointness; the tableau does not read `owl:AllDisjointClasses` (reported to the
/// eg-rdf owner, see the lane WRAPUP), so an ABox-level check would pass vacuously.
#[test]
fn a_taxon_is_never_an_organism() {
    let mut triples = wired_with_fixture();
    triples.extend(parse_scoped(
        "@prefix : <http://knuckles.team/kg#> . \
         @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> . \
         <http://example.org/world#TaxonOrganism> rdfs:subClassOf :Taxon, :Organism .",
        "clash",
    ));
    let classification = eg_rdf::owl::Reasoner::from_triples(&triples).classify();
    assert!(classification
        .unsatisfiable
        .contains("<http://example.org/world#TaxonOrganism>"));
    assert!(!classification.unsatisfiable.contains(&kg("Taxon")));
    assert!(!classification.unsatisfiable.contains(&kg("Organism")));
}

/// Mappings, not imports: every native class the module declares carries at least
/// one skos mapping to an external ontology or Wikidata, and the only import is core.
#[test]
fn every_world_model_class_is_mapped_and_nothing_is_imported() {
    let document = include_str!("../../../crates/eg-core/ontology/world_model-v1.ttl");
    let triples = eg_rdf::mapping::parse_turtle(document).unwrap();
    let declared: BTreeSet<&str> = triples
        .iter()
        .filter(|triple| {
            triple.predicate.as_str() == RDF_TYPE && object_iri(triple) == Some(OWL_CLASS)
        })
        .filter_map(subject_iri)
        .collect();
    let mapped: BTreeSet<&str> = triples
        .iter()
        .filter(|triple| SKOS_MAPPINGS.contains(&triple.predicate.as_str()))
        .filter_map(subject_iri)
        .collect();
    // The one abstract umbrella: its subclasses carry the vocabulary-specific mappings.
    let exempt = format!("{KG}ReferenceTerm");
    let unmapped: Vec<&str> = declared
        .into_iter()
        .filter(|class| !mapped.contains(class) && *class != exempt)
        .collect();
    assert!(
        unmapped.is_empty(),
        "classes without an external mapping: {unmapped:?}"
    );
    let imports: Vec<&str> = triples
        .iter()
        .filter(|triple| triple.predicate.as_str() == OWL_IMPORTS)
        .filter_map(object_iri)
        .collect();
    assert_eq!(imports, ["http://knuckles.team/kg/core"]);
}

/// Large vocabularies are data (ruling rule 3): no core document declares a class
/// under a reference-term kind, so a species or a GO term can only enter as an
/// individual.
#[test]
fn no_core_class_specialises_a_reference_term_kind() {
    let reference_kinds = [
        "Taxon",
        "BiologicalProcessTerm",
        "AnatomyTerm",
        "EnvironmentTerm",
        "FoodTerm",
        "QualityTerm",
        "Nutrient",
    ]
    .map(|local| format!("{KG}{local}"));
    let sources = GraphSchemaSources::default();
    for (source_id, document) in sources.ontologies() {
        for triple in eg_rdf::mapping::parse_turtle(document).unwrap() {
            let specialises = triple.predicate.as_str() == RDFS_SUBCLASS_OF
                && object_iri(&triple)
                    .is_some_and(|object| reference_kinds.iter().any(|kind| kind == object));
            assert!(!specialises, "{source_id}: {triple}");
        }
    }
}

/// Continuant ⊥ Occurrent (operator ruling 2026-09-22; declared in core-foundation by
/// eg-reasoning). Restated here so these tests hold on either side of that landing;
/// the composed corpus dedupes the identical triple.
const PROCESS_COMPOSITION: &str = r#"
@prefix : <http://knuckles.team/kg#> .
@prefix ex: <http://example.org/world#> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
@prefix bfo: <http://purl.obolibrary.org/obo/BFO_> .

bfo:0000002 owl:disjointWith bfo:0000003 .
ex:harvest a :Event .
ex:threshing a bfo:0000015 ; :subProcessOf ex:harvest .
ex:Stage rdfs:subClassOf bfo:0000015,
    [ a owl:Restriction ; owl:onProperty :partOf ; owl:someValuesFrom bfo:0000015 ] .
ex:ParticipatingProcess rdfs:subClassOf bfo:0000015,
    [ a owl:Restriction ; owl:onProperty :participatedIn ; owl:someValuesFrom :Event ] .
"#;
const PROCESS_AS_PARTICIPANT: &str = "@prefix : <http://knuckles.team/kg#> . \
     <http://example.org/world#threshing> :participatedIn <http://example.org/world#harvest> .";

/// The foundation plus `documents`, each scoped apart.
fn foundation_with(documents: &[&str]) -> Vec<Triple> {
    let sources = GraphSchemaSources::default();
    let foundation = sources
        .ontologies()
        .find(|(source_id, _)| *source_id == "core:foundation@1")
        .map(|(_, document)| document)
        .unwrap();
    let mut triples = parse_scoped(foundation, "foundation");
    for (index, document) in documents.iter().enumerate() {
        triples.extend(parse_scoped(document, &format!("d{index}")));
    }
    triples
}

/// `triples` plus every class membership OWL 2 RL derives from them (domain, range,
/// subclass, sub-property), asserted as `rdf:type` so the tableau decides them.
fn with_derived_types(mut triples: Vec<Triple>) -> Vec<Triple> {
    let ontology = eg_rdf::owl::parse_ontology(&triples);
    let result = eg_rdf::rules::reason_triples(&triples, &ontology, &Default::default());
    let iri = |key: &str| {
        key.strip_prefix('<')
            .and_then(|key| key.strip_suffix('>'))
            .and_then(|key| NamedNode::new(key).ok())
    };
    for (class, args, _) in &result.derived {
        let [individual] = args.as_slice() else {
            continue;
        };
        if let (Some(subject), Some(class)) = (iri(individual), iri(class)) {
            let rdf_type = NamedNode::new(RDF_TYPE).unwrap();
            triples.push(Triple::new(subject, rdf_type, class));
        }
    }
    triples
}

/// Occurrent-to-occurrent composition is `subProcessOf`/`partOf`; only continuants
/// participate. A process that is part of an event is accepted; a process asserted to
/// have participated in an event is refused once Continuant ⊥ Occurrent holds.
#[test]
fn a_sub_process_is_accepted_and_a_participating_process_is_refused() {
    let composed = foundation_with(&[PROCESS_COMPOSITION]);
    let classification = eg_rdf::owl::Reasoner::from_triples(&composed).classify();
    let stage = "<http://example.org/world#Stage>";
    assert!(!classification.unsatisfiable.contains(stage));
    let participating = "<http://example.org/world#ParticipatingProcess>";
    assert!(classification.unsatisfiable.contains(participating));

    let accepted = with_derived_types(composed);
    let dl = eg_rdf::tableau::parse_dl_ontology(&accepted);
    assert!(eg_rdf::tableau::is_consistent(&dl));

    let refused = with_derived_types(foundation_with(&[
        PROCESS_COMPOSITION,
        PROCESS_AS_PARTICIPANT,
    ]));
    let dl = eg_rdf::tableau::parse_dl_ontology(&refused);
    assert!(!eg_rdf::tableau::is_consistent(&dl));
}
