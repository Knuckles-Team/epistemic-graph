//! Shared RDF preprocessing for connector-pack admission.
//!
//! Extracted from the server without changing refusal codes, messages or limits.
//! These helpers are NOT an offline validation profile: callers still own byte
//! limits, identity/order checks, base policy, class/ABox reasoning and SHACL.
//! In particular, Turtle parsing allocates before applying the triple limit.

use eg_types::connector_pack::PackViolationCode;
use std::collections::BTreeSet;

mod union;
pub use union::{scoped_union, RdfUnion};

/// Check the existing pack per-document triple and term limits after parsing.
/// Callers choose the existing ontology/shapes syntax refusal code.
/// This is not a streaming allocation bound or a complete pack verdict.
pub fn validate_document(
    code: PackViolationCode,
    text: &str,
) -> Result<(), (PackViolationCode, &'static str)> {
    let triples =
        crate::mapping::parse_turtle(text).map_err(|_| (code, "RDF body is not valid Turtle"))?;
    if triples.len() > 100_000 {
        return Err((
            PackViolationCode::ValidationBudgetExceeded,
            "RDF graph exceeds the triple validation budget",
        ));
    }
    for triple in &triples {
        validate_rdf_triple(code, triple)?;
    }
    Ok(())
}

fn validate_rdf_triple(
    code: PackViolationCode,
    triple: &crate::oxrdf::Triple,
) -> Result<(), (PackViolationCode, &'static str)> {
    if triple.subject.to_string().len() > 4 * 1024 || triple.predicate.as_str().len() > 4 * 1024 {
        return Err((code, "RDF graph contains an IRI above the served bound"));
    }
    match &triple.object {
        crate::oxrdf::Term::Literal(literal) if literal.value().len() > 256 * 1024 => {
            Err((code, "RDF graph contains a literal above the served bound"))
        }
        crate::oxrdf::Term::NamedNode(node) if node.as_str().len() > 4 * 1024 => {
            Err((code, "RDF graph contains an IRI above the served bound"))
        }
        _ => Ok(()),
    }
}

/// Refuse imports outside the supplied pack ontology identities and the parent
/// catalog (EG-TYPED-PACKS-R105), including self-imports.
/// This policy performs no I/O and never resolves an import.
pub fn validate_ontology_imports(
    text: &str,
    current: &str,
    allowed: &BTreeSet<&str>,
) -> Result<(), &'static str> {
    use crate::oxrdf::Term;

    const OWL_IMPORTS: &str = "http://www.w3.org/2002/07/owl#imports";
    let triples = crate::mapping::parse_turtle(text)
        .map_err(|_| "ontology import policy could not parse its graph")?;
    for triple in triples {
        if triple.predicate.as_str() != OWL_IMPORTS {
            continue;
        }
        let Term::NamedNode(imported) = &triple.object else {
            return Err("owl:imports must name an ontology IRI from this pack");
        };
        if is_parent_ontology_iri(imported.as_str()) {
            continue;
        }
        if imported.as_str() == current || !allowed.contains(imported.as_str()) {
            return Err("owl:imports may name only an ontology entry in this pack");
        }
    }
    Ok(())
}

/// The parent catalog every connector vocabulary joins (EG-TYPED-PACKS-R105).
pub const PARENT_CATALOG_IRI: &str = "http://knuckles.team/kg";

const RDF_TYPE_IRI: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const OWL_CLASS_IRI: &str = "http://www.w3.org/2002/07/owl#Class";
const RDFS_CLASS_IRI: &str = "http://www.w3.org/2000/01/rdf-schema#Class";
const RDFS_SUBCLASS_OF_IRI: &str = "http://www.w3.org/2000/01/rdf-schema#subClassOf";
const OWL_ONTOLOGY_IRI: &str = "http://www.w3.org/2002/07/owl#Ontology";

/// Is `iri` the parent catalog or one of its ontology modules?
pub fn is_parent_ontology_iri(iri: &str) -> bool {
    iri == PARENT_CATALOG_IRI
        || iri
            .strip_prefix(PARENT_CATALOG_IRI)
            .and_then(|rest| rest.strip_prefix('/'))
            .is_some_and(|module| !module.is_empty() && !module.contains(['#', '/']))
}

/// Named classes (`owl:Class` / `rdfs:Class`) and named ontology IRIs one
/// document declares. Malformed Turtle declares nothing.
pub fn declared_classes_and_ontologies(text: &str) -> (BTreeSet<String>, BTreeSet<String>) {
    use crate::oxrdf::{NamedOrBlankNode, Term};

    let mut classes = BTreeSet::new();
    let mut ontologies = BTreeSet::new();
    for triple in crate::mapping::parse_turtle(text).unwrap_or_default() {
        let (NamedOrBlankNode::NamedNode(subject), Term::NamedNode(object)) =
            (&triple.subject, &triple.object)
        else {
            continue;
        };
        if triple.predicate.as_str() != RDF_TYPE_IRI {
            continue;
        }
        match object.as_str() {
            OWL_CLASS_IRI | RDFS_CLASS_IRI => {
                classes.insert(subject.as_str().to_string());
            }
            OWL_ONTOLOGY_IRI => {
                ontologies.insert(subject.as_str().to_string());
            }
            _ => {}
        }
    }
    (classes, ontologies)
}

/// The classes a set of pack ontologies declares that do not join the parent
/// graph: a declared class that is not itself a parent class and has no named
/// `rdfs:subClassOf` path, through any of these documents, to a parent class.
/// Sorted, so the warning a caller reads is stable.
pub fn ontology_orphan_classes(
    documents: &[&str],
    parent_classes: &BTreeSet<String>,
) -> Vec<String> {
    use crate::oxrdf::{NamedOrBlankNode, Term};
    use std::collections::BTreeMap;

    let mut declared = BTreeSet::new();
    let mut supers: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for text in documents {
        declared.extend(declared_classes_and_ontologies(text).0);
        for triple in crate::mapping::parse_turtle(text).unwrap_or_default() {
            if triple.predicate.as_str() != RDFS_SUBCLASS_OF_IRI {
                continue;
            }
            if let (NamedOrBlankNode::NamedNode(sub), Term::NamedNode(sup)) =
                (&triple.subject, &triple.object)
            {
                supers
                    .entry(sub.as_str().to_string())
                    .or_default()
                    .insert(sup.as_str().to_string());
            }
        }
    }
    let linked = |class: &String| {
        let mut seen = BTreeSet::new();
        let mut frontier = vec![class.clone()];
        while let Some(next) = frontier.pop() {
            if parent_classes.contains(&next) {
                return true;
            }
            if seen.insert(next.clone()) {
                frontier.extend(supers.get(&next).into_iter().flatten().cloned());
            }
        }
        false
    };
    declared
        .iter()
        .filter(|class| !linked(class))
        .cloned()
        .collect()
}

/// Named shapes used by the pack duplicate-shape warning; blank shapes are file-local.
/// Malformed Turtle yields no names, preserving the server helper behavior.
pub fn declared_shape_iris(text: &str) -> BTreeSet<String> {
    use crate::oxrdf::{NamedOrBlankNode, Term};

    const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
    const NODE_SHAPE: &str = "http://www.w3.org/ns/shacl#NodeShape";
    const PROPERTY_SHAPE: &str = "http://www.w3.org/ns/shacl#PropertyShape";
    crate::mapping::parse_turtle(text)
        .unwrap_or_default()
        .into_iter()
        .filter_map(|triple| {
            if triple.predicate.as_str() != RDF_TYPE
                || !matches!(&triple.object, Term::NamedNode(node) if matches!(node.as_str(), NODE_SHAPE | PROPERTY_SHAPE))
            {
                return None;
            }
            // Only a named shape can collide across files; a blank-node shape
            // is scoped to its own file by construction.
            if let NamedOrBlankNode::NamedNode(node) = triple.subject {
                Some(node.as_str().to_string())
            } else {
                None
            }
        })
        .collect()
}

/// One named `owl:ObjectProperty` with a named `rdfs:domain` and `rdfs:range`,
/// reduced to the IRI local names connectors use as resource and relation names.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct DeclaredObjectProperty {
    pub name: String,
    pub domain: String,
    pub range: String,
}

fn local_name(iri: &str) -> &str {
    iri.rsplit(['#', '/']).next().unwrap_or(iri)
}

/// The object properties the pack ontologies declare, with their domain and
/// range local names. A property lacking a named domain or range, or declaring
/// more than one of either, names no single relation and is left out. Malformed
/// Turtle declares nothing. Sorted, so the result is stable.
pub fn declared_object_properties(documents: &[&str]) -> Vec<DeclaredObjectProperty> {
    use crate::oxrdf::{NamedOrBlankNode, Term};
    use std::collections::BTreeMap;

    const OWL_OBJECT_PROPERTY: &str = "http://www.w3.org/2002/07/owl#ObjectProperty";
    const RDFS_DOMAIN: &str = "http://www.w3.org/2000/01/rdf-schema#domain";
    const RDFS_RANGE: &str = "http://www.w3.org/2000/01/rdf-schema#range";

    let mut properties = BTreeSet::new();
    let mut domains: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut ranges: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for text in documents {
        for triple in crate::mapping::parse_turtle(text).unwrap_or_default() {
            let (NamedOrBlankNode::NamedNode(subject), Term::NamedNode(object)) =
                (&triple.subject, &triple.object)
            else {
                continue;
            };
            let subject = subject.as_str().to_string();
            match triple.predicate.as_str() {
                RDF_TYPE_IRI if object.as_str() == OWL_OBJECT_PROPERTY => {
                    properties.insert(subject);
                }
                RDFS_DOMAIN => {
                    domains
                        .entry(subject)
                        .or_default()
                        .insert(object.as_str().to_string());
                }
                RDFS_RANGE => {
                    ranges
                        .entry(subject)
                        .or_default()
                        .insert(object.as_str().to_string());
                }
                _ => {}
            }
        }
    }
    let single = |map: &BTreeMap<String, BTreeSet<String>>, iri: &String| {
        map.get(iri)
            .filter(|set| set.len() == 1)
            .and_then(|set| set.iter().next())
            .map(|iri| local_name(iri).to_string())
    };
    properties
        .iter()
        .filter_map(|iri| {
            Some(DeclaredObjectProperty {
                name: local_name(iri).to_string(),
                domain: single(&domains, iri)?,
                range: single(&ranges, iri)?,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests;
