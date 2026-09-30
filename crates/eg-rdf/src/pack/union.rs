//! The union of a pack's ontology (or shapes) files, with every file's blank
//! nodes kept to that file (rule G15).
//!
//! Concatenating Turtle text would merge `_:b0` of one file with `_:b0` of
//! another -- two unrelated anonymous shapes silently becoming one -- and let
//! one file's `@prefix` rebind another's. Each file is therefore parsed on its
//! own, its blank nodes are renamed into a file-local namespace, and the union
//! is the concatenation of those triples, also rendered as N-Triples for the
//! validators that take text.

use crate::oxrdf::{BlankNode, NamedOrBlankNode, Term, Triple};

/// A parsed union and its N-Triples rendering.
pub struct RdfUnion {
    pub triples: Vec<Triple>,
    pub ntriples: String,
}

/// Parse every document on its own and union them with file-scoped blank
/// nodes. `Err` carries the index of the first document that does not parse.
pub fn scoped_union(documents: &[String]) -> Result<RdfUnion, usize> {
    scoped_union_with_limits(documents, usize::MAX, usize::MAX).map_err(|error| error.file())
}

/// First document to fail syntax or allocation bounds while streaming a union.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnionLimitError {
    Parse { file: usize },
    Triples { file: usize },
    RenderedBytes { file: usize },
}

impl UnionLimitError {
    /// Index in the supplied document order, independent of source identities.
    pub fn file(self) -> usize {
        match self {
            Self::Parse { file } | Self::Triples { file } | Self::RenderedBytes { file } => file,
        }
    }
}

/// Stream independent documents into a bounded file-scoped union. Input byte
/// bounds must be checked before calling: the parser may allocate a single term.
/// Limits include the complete union, not each document separately.
pub fn scoped_union_with_limits(
    documents: &[String],
    max_triples: usize,
    max_rendered_bytes: usize,
) -> Result<RdfUnion, UnionLimitError> {
    let mut union = RdfUnion {
        triples: Vec::new(),
        ntriples: String::new(),
    };
    for (file, document) in documents.iter().enumerate() {
        for parsed in crate::mapping::turtle_triples(document) {
            let triple = parsed.map_err(|_| UnionLimitError::Parse { file })?;
            if union.triples.len() >= max_triples {
                return Err(UnionLimitError::Triples { file });
            }
            let triple = scope_triple(triple, file);
            let rendered = triple.to_string();
            if union
                .ntriples
                .len()
                .saturating_add(rendered.len())
                .saturating_add(3)
                > max_rendered_bytes
            {
                return Err(UnionLimitError::RenderedBytes { file });
            }
            union.ntriples.push_str(&rendered);
            union.ntriples.push_str(" .\n");
            union.triples.push(triple);
        }
    }
    Ok(union)
}

fn scope_triple(mut triple: Triple, file: usize) -> Triple {
    if let NamedOrBlankNode::BlankNode(node) = &triple.subject {
        triple.subject = scoped(node, file).into();
    }
    if let Term::BlankNode(node) = &triple.object {
        triple.object = scoped(node, file).into();
    }
    triple
}

fn scoped(node: &BlankNode, file: usize) -> BlankNode {
    BlankNode::new_unchecked(format!("f{file}x{}", node.as_str()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_same_blank_label_in_two_files_stays_two_nodes() {
        let doc = "_:b0 <urn:p> <urn:o> .".to_string();
        let union = scoped_union(&[doc.clone(), doc]).unwrap();
        assert_eq!(union.triples.len(), 2);
        assert_ne!(union.triples[0].subject, union.triples[1].subject);
        let reparsed = crate::mapping::parse_turtle(&union.ntriples).unwrap();
        assert_eq!(reparsed.len(), 2);
    }

    #[test]
    fn a_file_that_does_not_parse_is_named_by_index() {
        let good = "<urn:s> <urn:p> <urn:o> .".to_string();
        let bad = "not turtle".to_string();
        assert_eq!(scoped_union(&[good, bad]).err(), Some(1));
    }
}
