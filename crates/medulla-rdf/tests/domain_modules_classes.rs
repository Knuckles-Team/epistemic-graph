//! Domain parent modules aligned to open standards: IT4IT delivery, data platform and
//! machine learning, media and content, search / retrieval / information sources
//! (MEDULLA-TYPED-PACKS-R105.6 to MEDULLA-TYPED-PACKS-R105.9).
#![cfg(feature = "owl")]

use std::collections::{BTreeMap, BTreeSet};

use medulla_rdf::mapping::parse_turtle;
use medulla_rdf::owl::Reasoner;
use medulla_rdf::oxrdf::{NamedNode, NamedOrBlankNode, Term, Triple};

const KG: &str = "http://knuckles.team/kg#";
const BFO: &str = "http://purl.obolibrary.org/obo/BFO_";
const RDF: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
const RDFS: &str = "http://www.w3.org/2000/01/rdf-schema#";
const OWL: &str = "http://www.w3.org/2002/07/owl#";
const SKOS: &str = "http://www.w3.org/2004/02/skos/core#";

const BASE: [&str; 10] = [
    include_str!("../../medulla-core/ontology/core-foundation-v1.ttl"),
    include_str!("../../medulla-core/ontology/company-v1.ttl"),
    include_str!("../../medulla-core/ontology/software-v1.ttl"),
    include_str!("../../medulla-core/ontology/sdlc_lifecycle-v1.ttl"),
    include_str!("../../medulla-core/ontology/infrastructure-v1.ttl"),
    include_str!("../../medulla-core/ontology/trm-v1.ttl"),
    include_str!("../../medulla-core/ontology/orchestration-v1.ttl"),
    include_str!("../../medulla-core/ontology/archimate-v1.ttl"),
    include_str!("../../medulla-core/ontology/sdd-v1.ttl"),
    include_str!("../../medulla-core/ontology/service_operations-v1.ttl"),
];
const MODULES: [(&str, &str); 4] = [
    (
        "it4it_delivery",
        include_str!("../../medulla-core/ontology/it4it_delivery-v1.ttl"),
    ),
    (
        "dataplatform",
        include_str!("../../medulla-core/ontology/dataplatform-v1.ttl"),
    ),
    ("media", include_str!("../../medulla-core/ontology/media-v1.ttl")),
    (
        "retrieval",
        include_str!("../../medulla-core/ontology/retrieval-v1.ttl"),
    ),
];

/// A class: local name and its direct parents (`kg:` names or `bfo:` ids).
type ClassRow = (&'static str, &'static [&'static str]);
/// An edge: name, domain classes, range classes, optional super-property.
type EdgeRow = (
    &'static str,
    &'static [&'static str],
    &'static [&'static str],
    Option<&'static str>,
);

const IT4IT_DELIVERY_CLASSES: &[ClassRow] = &[
    ("PortfolioBacklogItem", &["WorkItem"]),
    ("Backlog", &["Collection"]),
    ("Build", &["Job"]),
    ("BuildPackage", &["Artifact"]),
    ("DeploymentRun", &["RunEvent", "bfo:0000015"]),
    ("Environment", &["ResourceGroup"]),
    ("ServiceCatalogEntry", &["bfo:0000031"]),
    ("Subscription", &["bfo:0000031"]),
    ("ServiceRequest", &["Ticket"]),
    ("Alert", &["Event"]),
    ("KnownError", &["Problem"]),
    ("ServiceLevelTarget", &["bfo:0000031"]),
    ("Monitor", &["ConfigurationItem"]),
    ("Heartbeat", &["Observation"]),
    ("StatusPage", &["Document"]),
    ("Dashboard", &["Document"]),
    ("Panel", &["Document"]),
    ("ConfigurationItemCollection", &["ResourceGroup"]),
];

const IT4IT_DELIVERY_EDGES: &[EdgeRow] = &[
    ("deploysRelease", &["DeploymentRun"], &["Release"], None),
    (
        "deployedToEnvironment",
        &["DeploymentRun"],
        &["Environment"],
        None,
    ),
    ("producesPackage", &["Build"], &["BuildPackage"], None),
    ("includesPackage", &["Release"], &["BuildPackage"], None),
    (
        "deliversItem",
        &["Release"],
        &["PortfolioBacklogItem"],
        None,
    ),
    (
        "itemOfBacklog",
        &["PortfolioBacklogItem"],
        &["Backlog"],
        Some("partOf"),
    ),
    (
        "offersService",
        &["ServiceCatalogEntry"],
        &["BusinessService"],
        None,
    ),
    (
        "subscribesToEntry",
        &["Subscription"],
        &["ServiceCatalogEntry"],
        None,
    ),
    (
        "fulfilsEntry",
        &["ServiceRequest"],
        &["ServiceCatalogEntry"],
        None,
    ),
    ("alertsOn", &["Alert"], &["ConfigurationItem"], None),
    ("monitorsItem", &["Monitor"], &["ConfigurationItem"], None),
    ("checkOf", &["Heartbeat"], &["Monitor"], Some("partOf")),
    ("statusOf", &["StatusPage"], &["BusinessService"], None),
    ("panelOf", &["Panel"], &["Dashboard"], Some("partOf")),
    ("documentsProblem", &["KnownError"], &["Problem"], None),
    (
        "targetsService",
        &["ServiceLevelTarget"],
        &["BusinessService"],
        None,
    ),
];

const DATAPLATFORM_CLASSES: &[ClassRow] = &[
    ("DataCatalog", &["Collection"]),
    ("DatasetVersion", &["Dataset"]),
    ("DatasetSplit", &["Dataset"]),
    ("Distribution", &["bfo:0000031"]),
    ("DataAsset", &["Dataset"]),
    ("Schema", &["bfo:0000031"]),
    ("SchemaVersion", &["bfo:0000031"]),
    ("Table", &["Dataset"]),
    ("Column", &["bfo:0000031"]),
    ("Database", &["ConfigurationItem"]),
    ("Warehouse", &["ConfigurationItem"]),
    ("StorageContainer", &["ConfigurationItem"]),
    ("Snapshot", &["Artifact"]),
    ("Partition", &["Dataset"]),
    ("DataPipeline", &["bfo:0000015"]),
    ("Transformation", &["bfo:0000015"]),
    ("TransformationRun", &["RunEvent", "bfo:0000015"]),
    ("Notebook", &["Document"]),
    ("ModelVersion", &["Model", "bfo:0000031"]),
    ("ModelArtifact", &["Artifact"]),
    ("ModelEvaluation", &["Assessment", "bfo:0000031"]),
    ("Experiment", &["bfo:0000015"]),
    ("ExperimentRun", &["RunEvent", "bfo:0000015"]),
    ("MLFeature", &["bfo:0000031"]),
    ("HyperParameterSetting", &["bfo:0000031"]),
    ("Score", &["Metric"]),
    ("ScoreDefinition", &["bfo:0000031"]),
    ("Embedding", &["bfo:0000031"]),
    ("VectorIndex", &["ConfigurationItem"]),
    ("StreamSubscription", &["bfo:0000031"]),
    ("ChangeDataCapture", &["bfo:0000015"]),
    ("RdfGraph", &["Dataset"]),
    ("Corpus", &["Dataset"]),
];

const DATAPLATFORM_EDGES: &[EdgeRow] = &[
    ("inCatalog", &["Dataset"], &["DataCatalog"], Some("partOf")),
    ("hasDistribution", &["Dataset"], &["Distribution"], None),
    ("hasSchema", &["Table"], &["Schema"], None),
    ("hasColumn", &["Schema"], &["Column"], Some("hasPart")),
    (
        "dataplatformVersionOf",
        &["DatasetVersion", "SchemaVersion", "ModelVersion"],
        &["Dataset", "Schema", "Model"],
        Some("versionOf"),
    ),
    ("snapshotOf", &["Snapshot"], &["Table", "Dataset"], None),
    (
        "partitionOf",
        &["Partition"],
        &["Table", "Dataset"],
        Some("partOf"),
    ),
    ("storedIn", &["Dataset"], &["StorageContainer"], None),
    (
        "transformationOf",
        &["Transformation"],
        &["DataPipeline"],
        Some("partOf"),
    ),
    (
        "runOfTransformation",
        &["TransformationRun"],
        &["Transformation"],
        None,
    ),
    (
        "readsDataset",
        &["TransformationRun", "ExperimentRun"],
        &["Dataset"],
        None,
    ),
    ("writesDataset", &["TransformationRun"], &["Dataset"], None),
    (
        "runOfExperiment",
        &["ExperimentRun"],
        &["Experiment"],
        Some("partOf"),
    ),
    (
        "producesModelVersion",
        &["ExperimentRun"],
        &["ModelVersion"],
        None,
    ),
    ("trainedOn", &["ModelVersion"], &["Dataset"], None),
    ("usesFeature", &["ModelVersion"], &["MLFeature"], None),
    (
        "hasModelArtifact",
        &["ModelVersion"],
        &["ModelArtifact"],
        None,
    ),
    (
        "evaluatesModel",
        &["ModelEvaluation"],
        &["ModelVersion"],
        None,
    ),
    ("evaluatedOn", &["ModelEvaluation"], &["Dataset"], None),
    (
        "configuredWith",
        &["ExperimentRun"],
        &["HyperParameterSetting"],
        None,
    ),
    ("scoresRun", &["Score"], &["ExperimentRun"], None),
    ("definedBy", &["Score"], &["ScoreDefinition"], None),
    (
        "embeddingOf",
        &["Embedding"],
        &["Document", "Dataset"],
        None,
    ),
    (
        "embeddingIn",
        &["Embedding"],
        &["VectorIndex"],
        Some("partOf"),
    ),
];

const MEDIA_CLASSES: &[ClassRow] = &[
    ("MediaItem", &["CreativeWork"]),
    ("Video", &["MediaItem"]),
    ("AudioTrack", &["MediaItem"]),
    ("Photo", &["MediaItem"]),
    ("Book", &["CreativeWork"]),
    ("Audiobook", &["Book", "MediaItem"]),
    ("MediaCollection", &["Collection"]),
    ("Album", &["MediaCollection"]),
    ("Series", &["MediaCollection"]),
    ("Podcast", &["Series"]),
    ("Season", &["MediaCollection"]),
    ("MediaEpisode", &["MediaItem"]),
    ("Playlist", &["MediaCollection"]),
    ("MediaLibrary", &["MediaCollection"]),
    ("AssetStack", &["Collection"]),
    ("Genre", &["Category"]),
    ("Artist", &["Agent"]),
    ("BroadcastChannel", &["bfo:0000031"]),
    ("ChannelLineup", &["Collection"]),
    ("Recording", &["MediaItem"]),
    ("RecordingRule", &["Policy"]),
    ("LiveStream", &["MediaItem"]),
    ("Viewer", &["Role"]),
    ("MediaRequest", &["WorkItem"]),
    ("QualityProfile", &["Policy"]),
    ("Torrent", &["Artifact"]),
    ("Tracker", &["ConfigurationItem"]),
    ("GameTitle", &["CreativeWork"]),
    ("GameSystem", &["Concept"]),
    ("GameSave", &["Artifact"]),
    ("GameState", &["Artifact"]),
];

const MEDIA_EDGES: &[EdgeRow] = &[
    (
        "episodeOf",
        &["MediaEpisode"],
        &["Series", "Season"],
        Some("partOf"),
    ),
    ("seasonOf", &["Season"], &["Series"], Some("partOf")),
    ("trackOf", &["AudioTrack"], &["Album"], Some("partOf")),
    (
        "includesItem",
        &["MediaCollection"],
        &["MediaItem"],
        Some("hasPart"),
    ),
    (
        "inLibrary",
        &["MediaItem", "MediaCollection"],
        &["MediaLibrary"],
        Some("partOf"),
    ),
    (
        "performedBy",
        &["MediaItem", "MediaCollection"],
        &["Artist"],
        None,
    ),
    (
        "hasGenre",
        &["MediaItem", "MediaCollection"],
        &["Genre"],
        None,
    ),
    ("stackedIn", &["Photo"], &["AssetStack"], Some("partOf")),
    (
        "inLineup",
        &["BroadcastChannel"],
        &["ChannelLineup"],
        Some("partOf"),
    ),
    ("recordedFrom", &["Recording"], &["BroadcastChannel"], None),
    (
        "producesRecording",
        &["RecordingRule"],
        &["Recording"],
        None,
    ),
    ("watches", &["Viewer"], &["LiveStream"], None),
    (
        "requestsMedia",
        &["MediaRequest"],
        &["MediaItem", "MediaCollection"],
        None,
    ),
    (
        "usesQualityProfile",
        &["MediaRequest", "MediaLibrary"],
        &["QualityProfile"],
        None,
    ),
    (
        "transfersMedia",
        &["Torrent"],
        &["MediaItem", "MediaCollection"],
        None,
    ),
    ("announcedTo", &["Torrent"], &["Tracker"], None),
    ("saveOf", &["GameSave", "GameState"], &["GameTitle"], None),
    ("playsOn", &["GameTitle"], &["GameSystem"], None),
];

const RETRIEVAL_CLASSES: &[ClassRow] = &[
    ("InformationSource", &["bfo:0000031"]),
    ("FeedSource", &["InformationSource"]),
    ("FeedItem", &["Document"]),
    ("PublicationSource", &["InformationSource"]),
    ("SourceBackend", &["InformationSource"]),
    ("SearchEngine", &["ConfigurationItem"]),
    ("SearchQuery", &["bfo:0000031"]),
    ("SearchResult", &["Document"]),
    ("SearchHit", &["SearchResult"]),
    ("SearchIndex", &["Dataset"]),
    ("IndexAlias", &["bfo:0000031"]),
    ("IndexingRun", &["RunEvent", "bfo:0000015"]),
    ("Indexer", &["SearchEngine"]),
    ("WebResource", &["Document"]),
    ("WebCapture", &["Rendition"]),
    ("Citation", &["bfo:0000031"]),
];

const RETRIEVAL_EDGES: &[EdgeRow] = &[
    ("itemOfFeed", &["FeedItem"], &["FeedSource"], Some("partOf")),
    (
        "publishedBySource",
        &["Document", "FeedItem"],
        &["InformationSource"],
        None,
    ),
    (
        "readThrough",
        &["InformationSource"],
        &["SourceBackend"],
        None,
    ),
    ("answersQuery", &["SearchResult"], &["SearchQuery"], None),
    ("hitFor", &["SearchHit"], &["Document"], None),
    ("queriesIndex", &["SearchQuery"], &["SearchIndex"], None),
    ("runsOnEngine", &["SearchQuery"], &["SearchEngine"], None),
    ("servesIndex", &["SearchEngine"], &["SearchIndex"], None),
    ("aliasesIndex", &["IndexAlias"], &["SearchIndex"], None),
    ("builtByRun", &["SearchIndex"], &["IndexingRun"], None),
    ("captureOf", &["WebCapture"], &["WebResource"], None),
    ("hasCitation", &["Document"], &["Citation"], None),
    ("citesSource", &["Citation"], &["InformationSource"], None),
];

fn kg(name: &str) -> String {
    format!("{KG}{name}")
}

/// A parent written as a `kg:` local name or `bfo:<id>`.
fn parent_iri(parent: &str) -> String {
    match parent.strip_prefix("bfo:") {
        Some(id) => format!("{BFO}{id}"),
        None => kg(parent),
    }
}

fn base() -> Vec<Triple> {
    BASE.iter()
        .flat_map(|document| parse_turtle(document).unwrap())
        .collect()
}

fn module_triples(module: &str) -> Vec<Triple> {
    let document = MODULES.iter().find(|(name, _)| *name == module).unwrap().1;
    parse_turtle(document).unwrap()
}

fn all() -> Vec<Triple> {
    let mut triples = base();
    for (module, _) in MODULES {
        triples.extend(module_triples(module));
    }
    triples
}

fn has_iri(triples: &[Triple], subject: &str, predicate: &str, object: &str) -> bool {
    triples.contains(&Triple::new(
        NamedNode::new(subject).unwrap(),
        NamedNode::new(predicate).unwrap(),
        NamedNode::new(object).unwrap(),
    ))
}

fn literals(triples: &[Triple], subject: &str, predicate: &str) -> Vec<String> {
    triples
        .iter()
        .filter(|t| {
            t.subject == NamedNode::new(subject).unwrap().into()
                && t.predicate.as_str() == predicate
        })
        .filter_map(|t| match &t.object {
            Term::Literal(l) => Some(l.value().to_string()),
            _ => None,
        })
        .collect()
}

/// The named classes an `rdfs:domain` / `rdfs:range` value stands for.
fn members(triples: &[Triple], subject: &str, predicate: &str) -> Vec<String> {
    let mut found = Vec::new();
    for triple in triples.iter().filter(|t| {
        t.subject == NamedNode::new(subject).unwrap().into() && t.predicate.as_str() == predicate
    }) {
        match &triple.object {
            Term::NamedNode(node) => found.push(node.as_str().to_string()),
            Term::BlankNode(node) => {
                let mut list = triples
                    .iter()
                    .find(|t| {
                        t.subject == NamedOrBlankNode::BlankNode(node.clone())
                            && t.predicate.as_str() == format!("{OWL}unionOf")
                    })
                    .map(|t| t.object.clone());
                while let Some(Term::BlankNode(cell)) = list {
                    let cell = NamedOrBlankNode::BlankNode(cell);
                    for t in triples.iter().filter(|t| t.subject == cell) {
                        if t.predicate.as_str() == format!("{RDF}first") {
                            if let Term::NamedNode(n) = &t.object {
                                found.push(n.as_str().to_string());
                            }
                        }
                    }
                    list = triples
                        .iter()
                        .find(|t| t.subject == cell && t.predicate.as_str() == format!("{RDF}rest"))
                        .map(|t| t.object.clone());
                }
            }
            _ => {}
        }
    }
    found.sort();
    found
}

fn sorted(names: &[&str]) -> Vec<String> {
    let mut v: Vec<String> = names.iter().map(|n| kg(n)).collect();
    v.sort();
    v
}

fn check_classes(module: &str, rows: &[ClassRow]) {
    let triples = all();
    let own = module_triples(module);
    let owl_class = format!("{OWL}Class");
    let sub = format!("{RDFS}subClassOf");
    for (name, parents) in rows {
        let class = kg(name);
        assert!(
            has_iri(&own, &class, &format!("{RDF}type"), &owl_class),
            "{module}: {class} is declared in its module"
        );
        assert_eq!(
            literals(&own, &class, &format!("{RDFS}label")).len(),
            1,
            "{class} label"
        );
        assert_eq!(
            literals(&own, &class, &format!("{RDFS}comment")).len(),
            1,
            "{class} comment"
        );
        assert!(
            !literals(&own, &class, &format!("{SKOS}altLabel")).is_empty(),
            "{class} has an alias (codemod R3 matches on it)"
        );
        for parent in *parents {
            assert!(
                has_iri(&own, &class, &sub, &parent_iri(parent)),
                "{class} under {parent}"
            );
            if !parent.starts_with("bfo:") {
                let parent_node: NamedOrBlankNode =
                    NamedNode::new(parent_iri(parent)).unwrap().into();
                assert!(
                    triples.iter().any(|t| t.subject == parent_node
                        && (t.predicate.as_str() == sub
                            || (t.predicate.as_str() == format!("{RDF}type")
                                && t.object
                                    == Term::NamedNode(
                                        NamedNode::new(owl_class.clone()).unwrap()
                                    )))),
                    "{class}: parent {parent} exists"
                );
            }
        }
    }
    // Every class reaches a BFO class through its subclass chain.
    let mut parents_of: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for t in &triples {
        if t.predicate.as_str() == sub {
            if let (NamedOrBlankNode::NamedNode(s), Term::NamedNode(o)) = (&t.subject, &t.object) {
                parents_of
                    .entry(s.as_str().to_string())
                    .or_default()
                    .push(o.as_str().to_string());
            }
        }
    }
    for (name, _) in rows {
        let mut seen = BTreeSet::new();
        let mut todo = vec![kg(name)];
        let mut reached = false;
        while let Some(class) = todo.pop() {
            if class.starts_with(BFO) {
                reached = true;
                break;
            }
            if seen.insert(class.clone()) {
                todo.extend(parents_of.get(&class).cloned().unwrap_or_default());
            }
        }
        assert!(reached, "{name} sits under a BFO class");
    }
    let classification = Reasoner::from_triples(&triples).classify();
    assert!(classification.consistent);
    assert!(classification.unsatisfiable.is_empty());
}

fn check_edges(module: &str, rows: &[EdgeRow]) {
    let triples = all();
    let own = module_triples(module);
    for (name, domains, ranges, parent) in rows {
        let property = kg(name);
        assert!(
            has_iri(
                &own,
                &property,
                &format!("{RDF}type"),
                &format!("{OWL}ObjectProperty")
            ),
            "{name}"
        );
        assert_eq!(
            literals(&own, &property, &format!("{RDFS}label")).len(),
            1,
            "{name} label"
        );
        assert_eq!(
            literals(&own, &property, &format!("{RDFS}comment")).len(),
            1,
            "{name} comment"
        );
        assert_eq!(
            members(&triples, &property, &format!("{RDFS}domain")),
            sorted(domains),
            "{name} domain"
        );
        assert_eq!(
            members(&triples, &property, &format!("{RDFS}range")),
            sorted(ranges),
            "{name} range"
        );
        if let Some(parent) = parent {
            assert!(
                has_iri(
                    &own,
                    &property,
                    &format!("{RDFS}subPropertyOf"),
                    &kg(parent)
                ),
                "{name} under {parent}"
            );
        }
    }
}

/// No IRI that a domain module declares is declared by another module as well.
fn check_no_duplicate_declarations(module: &str, rows: &[ClassRow]) {
    let owl_class = format!("{OWL}Class");
    let own = module_triples(module);
    let core = base();
    let others: Vec<(&str, Vec<Triple>)> = MODULES
        .iter()
        .filter(|(m, _)| *m != module)
        .map(|(m, _)| (*m, module_triples(m)))
        .collect();
    for (name, _) in rows {
        let class = kg(name);
        assert!(has_iri(&own, &class, &format!("{RDF}type"), &owl_class));
        assert!(
            !has_iri(&core, &class, &format!("{RDF}type"), &owl_class),
            "{class} is not redeclared from the core modules"
        );
        for (other, triples) in &others {
            assert!(
                !has_iri(triples, &class, &format!("{RDF}type"), &owl_class),
                "{class} is not declared in {other}"
            );
        }
    }
}

// spec: MEDULLA-TYPED-PACKS-R105.6
#[test]
fn it4it_delivery_classes_exist_with_their_parents() {
    check_classes("it4it_delivery", IT4IT_DELIVERY_CLASSES);
    check_no_duplicate_declarations("it4it_delivery", IT4IT_DELIVERY_CLASSES);
}

// spec: MEDULLA-TYPED-PACKS-R105.6
#[test]
fn it4it_delivery_edges_declare_domain_and_range() {
    check_edges("it4it_delivery", IT4IT_DELIVERY_EDGES);
}

// spec: MEDULLA-TYPED-PACKS-R105.7
#[test]
fn dataplatform_classes_exist_with_their_parents() {
    check_classes("dataplatform", DATAPLATFORM_CLASSES);
    check_no_duplicate_declarations("dataplatform", DATAPLATFORM_CLASSES);
}

// spec: MEDULLA-TYPED-PACKS-R105.7
#[test]
fn dataplatform_edges_declare_domain_and_range() {
    check_edges("dataplatform", DATAPLATFORM_EDGES);
}

// spec: MEDULLA-TYPED-PACKS-R105.8
#[test]
fn media_classes_exist_with_their_parents() {
    check_classes("media", MEDIA_CLASSES);
    check_no_duplicate_declarations("media", MEDIA_CLASSES);
}

// spec: MEDULLA-TYPED-PACKS-R105.8
#[test]
fn media_edges_declare_domain_and_range() {
    check_edges("media", MEDIA_EDGES);
}

// spec: MEDULLA-TYPED-PACKS-R105.9
#[test]
fn retrieval_classes_exist_with_their_parents() {
    check_classes("retrieval", RETRIEVAL_CLASSES);
    check_no_duplicate_declarations("retrieval", RETRIEVAL_CLASSES);
}

// spec: MEDULLA-TYPED-PACKS-R105.9
#[test]
fn retrieval_edges_declare_domain_and_range() {
    check_edges("retrieval", RETRIEVAL_EDGES);
}

// spec: MEDULLA-TYPED-PACKS-R105.6
// spec: MEDULLA-TYPED-PACKS-R105.7
// spec: MEDULLA-TYPED-PACKS-R105.8
// spec: MEDULLA-TYPED-PACKS-R105.9
#[test]
fn reused_core_classes_are_not_redeclared_by_the_domain_modules() {
    // Release, Job, Runner, Metric, Dataset and Model keep their single home.
    for (module, _) in MODULES {
        let triples = module_triples(module);
        for name in [
            "Release", "Job", "Runner", "Metric", "Dataset", "Model", "Problem", "Incident",
        ] {
            assert!(
                !has_iri(
                    &triples,
                    &kg(name),
                    &format!("{RDF}type"),
                    &format!("{OWL}Class")
                ),
                "{module} must not redeclare {name}"
            );
        }
    }
}
