//! Shared RDF preprocessing for connector-pack admission.
//!
//! Extracted from the server without changing refusal codes, messages or limits.
//! These helpers are NOT an offline validation profile: callers still own byte
//! limits, identity/order checks, base policy, class/ABox reasoning and SHACL.
//! Document validation now stops collection at its byte/triple/term bounds;
//! the unbounded union helper remains for callers that own their own limits.

use eg_types::connector_pack::PackViolationCode;
use std::collections::BTreeSet;

mod union;
pub use union::{scoped_union, scoped_union_with_limits, RdfUnion, UnionLimitError};

/// Existing per-document and ontology-union admission triple limit.
pub const MAX_RDF_TRIPLES: usize = 100_000;
/// Admission cap for N-Triples expansion of a document or collected union.
pub const MAX_RDF_RENDERED_BYTES: usize = eg_types::connector_pack::MAX_PACK_ARCHIVE_BYTES as usize;

/// Enforce pack body bytes before parsing, then check streamed triples and terms.
/// Callers choose the existing ontology/shapes syntax refusal code.
/// This is not a complete pack verdict; limits and errors stop the stream early.
pub fn validate_document(
    code: PackViolationCode,
    text: &str,
) -> Result<(), (PackViolationCode, &'static str)> {
    if text.len() > eg_types::connector_pack::MAX_PACK_BODY_BYTES as usize {
        return Err((
            PackViolationCode::PackTooLarge,
            "section exceeds its served size bound",
        ));
    }
    let mut rendered_bytes = 0usize;
    for (count, triple) in crate::mapping::turtle_triples(text).enumerate() {
        let triple = triple.map_err(|_| (code, "RDF body is not valid Turtle"))?;
        validate_streamed_triple(code, &triple, count, &mut rendered_bytes)?;
    }
    Ok(())
}

fn validate_streamed_triple(
    code: PackViolationCode,
    triple: &crate::oxrdf::Triple,
    count: usize,
    rendered_bytes: &mut usize,
) -> Result<(), (PackViolationCode, &'static str)> {
    if count >= MAX_RDF_TRIPLES {
        return Err((
            PackViolationCode::ValidationBudgetExceeded,
            "RDF graph exceeds the triple validation budget",
        ));
    }
    validate_rdf_triple(code, triple)?;
    *rendered_bytes = rendered_bytes
        .saturating_add(triple.to_string().len())
        .saturating_add(3);
    if *rendered_bytes > MAX_RDF_RENDERED_BYTES {
        return Err((
            PackViolationCode::ValidationBudgetExceeded,
            "RDF graph rendering exceeds the byte validation budget",
        ));
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

/// Refuse imports outside the supplied pack ontology identities, including self-imports.
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
        if imported.as_str() == current || !allowed.contains(imported.as_str()) {
            return Err("owl:imports may name only an ontology entry in this pack");
        }
    }
    Ok(())
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

#[cfg(test)]
mod tests;
