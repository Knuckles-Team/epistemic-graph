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
    let mut union = RdfUnion {
        triples: Vec::new(),
        ntriples: String::new(),
    };
    for (file, document) in documents.iter().enumerate() {
        let parsed = crate::mapping::parse_turtle(document).map_err(|_| file)?;
        for triple in parsed {
            let triple = scope_triple(triple, file);
            union.ntriples.push_str(&triple.to_string());
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
