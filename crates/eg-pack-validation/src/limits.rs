//! Allocation preflight before parsing, renaming or rendering any document.
use crate::Refusal;
use eg_types::connector_pack::{self, PackViolationCode};

pub const MAX_DOCUMENTS: usize = connector_pack::MAX_PACK_ENTRIES;
pub const MAX_DOCUMENT_BYTES: usize = connector_pack::MAX_PACK_BODY_BYTES as usize;
pub const MAX_INPUT_BYTES: usize = connector_pack::MAX_PACK_ARCHIVE_BYTES as usize;
/// Cap expansion of compact Turtle into N-Triples, independently for each union.
pub const MAX_RENDERED_BYTES: usize = eg_rdf::pack::MAX_RDF_RENDERED_BYTES;

pub(crate) fn preflight(ontologies: &[String], shapes: &[String]) -> Result<(), Refusal> {
    if ontologies.len().saturating_add(shapes.len()) > MAX_DOCUMENTS {
        return Err((
            PackViolationCode::PackTooLarge,
            "RDF documents exceed the pack entry bound",
        ));
    }
    let mut total = 0usize;
    for document in ontologies.iter().chain(shapes) {
        if document.len() > MAX_DOCUMENT_BYTES {
            return Err((
                PackViolationCode::PackTooLarge,
                "section exceeds its served size bound",
            ));
        }
        total = total.saturating_add(document.len());
        if total > MAX_INPUT_BYTES {
            return Err((
                PackViolationCode::PackTooLarge,
                "RDF documents exceed the pack archive byte bound",
            ));
        }
    }
    Ok(())
}
