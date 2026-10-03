//! `OntologyInspect`: the engine reads ontology and shapes documents for
//! callers that own no RDF parser — the named classes and properties with their
//! labels, domains and ranges, the SHACL target classes, and the canonical digest a
//! connector manifest pins. Inline documents or the request graph's composed
//! GraphSchema sources; read-only either way.

use std::collections::{BTreeMap, BTreeSet};

use eg_rdf::oxrdf::{NamedOrBlankNode, Term, Triple};
use eg_types::contract::Digest256;
use eg_types::ontology_inspection::{
    OntologyClassView, OntologyInspection, OntologyPropertyView, MAX_INSPECT_DOCUMENTS,
};

use crate::graph::{GraphCore, GraphSchemaSources};
use crate::protocol::{Response, ResultPayload};

const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const RDFS_LABEL: &str = "http://www.w3.org/2000/01/rdf-schema#label";
const RDFS_COMMENT: &str = "http://www.w3.org/2000/01/rdf-schema#comment";
const RDFS_SUBCLASS_OF: &str = "http://www.w3.org/2000/01/rdf-schema#subClassOf";
const RDFS_DOMAIN: &str = "http://www.w3.org/2000/01/rdf-schema#domain";
const RDFS_RANGE: &str = "http://www.w3.org/2000/01/rdf-schema#range";
const OWL_CLASS: &str = "http://www.w3.org/2002/07/owl#Class";
const OWL_ONTOLOGY: &str = "http://www.w3.org/2002/07/owl#Ontology";
const OWL_OBJECT_PROPERTY: &str = "http://www.w3.org/2002/07/owl#ObjectProperty";
const OWL_DATATYPE_PROPERTY: &str = "http://www.w3.org/2002/07/owl#DatatypeProperty";
const OWL_SYMMETRIC_PROPERTY: &str = "http://www.w3.org/2002/07/owl#SymmetricProperty";
const SH_TARGET_CLASS: &str = "http://www.w3.org/ns/shacl#targetClass";
/// The per-subject predicates a vocabulary view is built from.
const TRACKED: &[&str] = &[
    RDF_TYPE,
    RDFS_LABEL,
    RDFS_COMMENT,
    RDFS_SUBCLASS_OF,
    RDFS_DOMAIN,
    RDFS_RANGE,
];

pub(super) fn handle_ontology_inspect(
    req_id: u64,
    core: &GraphCore,
    documents: &[String],
    source_ids: &[String],
) -> Response {
    match inspect(core, documents, source_ids) {
        Ok(view) => Response::ok(
            req_id,
            ResultPayload::of::<eg_types::result_contract::reasoning::OntologyInspect>(view),
        ),
        Err(error) => Response::err(req_id, format!("OntologyInspect: {error}")),
    }
}

/// The documents one inspection reads and the identity it reports for them.
struct Inspected {
    triples: Vec<Triple>,
    schema_digests: Vec<String>,
    composed_digest: Option<String>,
}

fn inspect(
    core: &GraphCore,
    documents: &[String],
    source_ids: &[String],
) -> Result<OntologyInspection, String> {
    let inspected = if documents.is_empty() {
        composed_documents(&core.schema_sources(), source_ids)?
    } else if source_ids.is_empty() {
        inline_documents(documents)?
    } else {
        return Err("give documents or source_ids, not both".to_string());
    };
    Ok(inspection_of(inspected))
}

fn inline_documents(documents: &[String]) -> Result<Inspected, String> {
    if documents.len() > MAX_INSPECT_DOCUMENTS {
        return Err(format!(
            "{} documents exceed the maximum of {MAX_INSPECT_DOCUMENTS}",
            documents.len()
        ));
    }
    let named: Vec<(String, &str)> = documents
        .iter()
        .enumerate()
        .map(|(index, document)| (format!("documents[{index}]"), document.as_str()))
        .collect();
    parsed(&named, None)
}

fn composed_documents(
    sources: &GraphSchemaSources,
    source_ids: &[String],
) -> Result<Inspected, String> {
    let wanted: BTreeSet<&str> = source_ids.iter().map(String::as_str).collect();
    let selected: Vec<(String, &str)> = sources
        .all()
        .filter(|(id, _)| wanted.is_empty() || wanted.contains(id))
        .flat_map(|(id, source)| {
            [source.shapes_ttl.as_deref(), source.ontology_ttl.as_deref()]
                .into_iter()
                .flatten()
                .map(move |document| (id.to_string(), document))
        })
        .collect();
    let found: BTreeSet<&str> = selected.iter().map(|(id, _)| id.as_str()).collect();
    if let Some(missing) = wanted.iter().find(|id| !found.contains(**id)) {
        return Err(format!("unknown GraphSchema source '{missing}'"));
    }
    parsed(&selected, Some(sources.composed_digest().to_hex()))
}

fn parsed(
    documents: &[(String, &str)],
    composed_digest: Option<String>,
) -> Result<Inspected, String> {
    let mut triples = Vec::new();
    let mut digests = BTreeSet::new();
    for (name, document) in documents {
        if document.len() > eg_types::graph_schema::MAX_SCHEMA_DOCUMENT_BYTES {
            return Err(format!(
                "{name} exceeds the maximum of {} bytes",
                eg_types::graph_schema::MAX_SCHEMA_DOCUMENT_BYTES
            ));
        }
        let document_triples = eg_rdf::mapping::parse_turtle(document)
            .map_err(|error| format!("{name} is not Turtle: {error}"))?;
        if document_triples.len() > crate::graph::MAX_SCHEMA_DOCUMENT_TRIPLES {
            return Err(format!("{name} has {} triples", document_triples.len()));
        }
        triples.extend(document_triples);
        digests.insert(Digest256::sha256(document.as_bytes()).to_hex());
    }
    Ok(Inspected {
        triples,
        schema_digests: digests.into_iter().collect(),
        composed_digest,
    })
}

fn inspection_of(inspected: Inspected) -> OntologyInspection {
    let facts = Facts::collect(&inspected.triples);
    let lines: BTreeSet<String> = inspected
        .triples
        .iter()
        .map(|triple| format!("{triple} ."))
        .collect();
    OntologyInspection {
        schema_digests: inspected.schema_digests,
        composed_digest: inspected.composed_digest,
        triple_count: lines.len() as u64,
        canonical_digest: canonical_digest(&inspected.triples, &lines),
        ontologies: facts
            .typed(OWL_ONTOLOGY)
            .map(|(iri, _)| iri.clone())
            .collect(),
        classes: facts.classes(),
        object_properties: facts.properties(OWL_OBJECT_PROPERTY),
        datatype_properties: facts.properties(OWL_DATATYPE_PROPERTY),
        shape_target_classes: facts.targets.into_iter().collect(),
    }
}

/// sha256 over the sorted distinct N-Triples lines joined by newlines — the same
/// bytes a blank-node-free graph's `urdna2015-sha256` identity hashes. Blank-node
/// labels are parser-assigned, so a document with any has no canonical digest here.
fn canonical_digest(triples: &[Triple], lines: &BTreeSet<String>) -> Option<String> {
    if triples.iter().any(has_blank_node) {
        return None;
    }
    let joined = lines
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>()
        .join("\n");
    Some(Digest256::sha256(joined.as_bytes()).to_hex())
}

fn has_blank_node(triple: &Triple) -> bool {
    matches!(triple.subject, NamedOrBlankNode::BlankNode(_))
        || matches!(triple.object, Term::BlankNode(_))
}

/// The tracked values of each named subject, and every named SHACL target.
#[derive(Default)]
struct Facts {
    subjects: BTreeMap<String, BTreeMap<&'static str, BTreeSet<String>>>,
    targets: BTreeSet<String>,
}

impl Facts {
    fn collect(triples: &[Triple]) -> Self {
        let mut facts = Self::default();
        for triple in triples {
            facts.record(triple);
        }
        facts
    }

    fn record(&mut self, triple: &Triple) {
        let predicate = triple.predicate.as_str();
        if predicate == SH_TARGET_CLASS {
            if let Term::NamedNode(target) = &triple.object {
                self.targets.insert(target.as_str().to_string());
            }
            return;
        }
        let Some(value) = term_value(predicate, &triple.object) else {
            return;
        };
        let (NamedOrBlankNode::NamedNode(subject), Some(key)) = (
            &triple.subject,
            TRACKED.iter().find(|tracked| **tracked == predicate),
        ) else {
            return;
        };
        self.subjects
            .entry(subject.as_str().to_string())
            .or_default()
            .entry(*key)
            .or_default()
            .insert(value);
    }

    fn typed(
        &self,
        class: &str,
    ) -> impl Iterator<Item = (&String, &BTreeMap<&'static str, BTreeSet<String>>)> {
        let class = class.to_string();
        self.subjects.iter().filter(move |(_, values)| {
            values
                .get(RDF_TYPE)
                .is_some_and(|types| types.contains(&class))
        })
    }

    fn classes(&self) -> Vec<OntologyClassView> {
        self.typed(OWL_CLASS)
            .map(|(iri, values)| OntologyClassView {
                iri: iri.clone(),
                label: first(values, RDFS_LABEL),
                comment: first(values, RDFS_COMMENT),
                parents: all(values, RDFS_SUBCLASS_OF),
            })
            .collect()
    }

    fn properties(&self, kind: &str) -> Vec<OntologyPropertyView> {
        self.typed(kind)
            .map(|(iri, values)| OntologyPropertyView {
                iri: iri.clone(),
                label: first(values, RDFS_LABEL),
                comment: first(values, RDFS_COMMENT),
                domains: all(values, RDFS_DOMAIN),
                ranges: all(values, RDFS_RANGE),
                symmetric: values
                    .get(RDF_TYPE)
                    .is_some_and(|types| types.contains(OWL_SYMMETRIC_PROPERTY)),
            })
            .collect()
    }
}

/// Vocabulary links must remain named terms; only annotations use lexical text.
fn term_value(predicate: &str, term: &Term) -> Option<String> {
    match term {
        Term::NamedNode(node) => Some(node.as_str().to_string()),
        Term::Literal(literal) if matches!(predicate, RDFS_LABEL | RDFS_COMMENT) => {
            Some(literal.value().to_string())
        }
        _ => None,
    }
}

fn first(values: &BTreeMap<&'static str, BTreeSet<String>>, key: &str) -> Option<String> {
    values.get(key).and_then(|set| set.iter().next().cloned())
}

fn all(values: &BTreeMap<&'static str, BTreeSet<String>>, key: &str) -> Vec<String> {
    values
        .get(key)
        .map(|set| set.iter().cloned().collect())
        .unwrap_or_default()
}

#[cfg(test)]
#[path = "inspect_tests.rs"]
mod tests;
