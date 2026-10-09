//! The term vocabulary of a graph's composed schema (EH-389).
//!
//! `GraphSchemaList` answers WHICH documents are attached; this answers WHAT
//! they declare: every named class and property of the composed ontology, with
//! the source that declares it. A consumer that maps records onto canonical
//! classes (AU domain packs) checks its references against this list instead of
//! carrying its own copy of the ontology.
//!
//! Rows are ordered by `(iri, kind)` and paged by an opaque cursor naming the
//! last row returned. Every page carries the composed digest it was read at, so
//! a caller that walks several pages can prove it read one composition.

use serde::{Deserialize, Serialize};

use super::GraphSchemaErrorCode;
use crate::contract::BoundedVec;

/// Largest page one `GraphSchemaClasses` read returns.
pub const MAX_GRAPH_SCHEMA_TERMS_PAGE: usize = 1000;

/// What a declared term is, by the RDF type that declares it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum GraphSchemaTermKind {
    /// `owl:Class` or `rdfs:Class`.
    Class,
    /// `owl:ObjectProperty`.
    ObjectProperty,
    /// `owl:DatatypeProperty`.
    DatatypeProperty,
    /// `owl:AnnotationProperty`.
    AnnotationProperty,
    /// `rdf:Property` (an RDFS property with no OWL typing).
    Property,
}

/// Declaring type IRI -> term kind. `owl:Ontology` headers are not terms.
const DECLARATIONS: &[(&str, GraphSchemaTermKind)] = &[
    (
        "http://www.w3.org/2002/07/owl#Class",
        GraphSchemaTermKind::Class,
    ),
    (
        "http://www.w3.org/2000/01/rdf-schema#Class",
        GraphSchemaTermKind::Class,
    ),
    (
        "http://www.w3.org/2002/07/owl#ObjectProperty",
        GraphSchemaTermKind::ObjectProperty,
    ),
    (
        "http://www.w3.org/2002/07/owl#DatatypeProperty",
        GraphSchemaTermKind::DatatypeProperty,
    ),
    (
        "http://www.w3.org/2002/07/owl#AnnotationProperty",
        GraphSchemaTermKind::AnnotationProperty,
    ),
    (
        "http://www.w3.org/1999/02/22-rdf-syntax-ns#Property",
        GraphSchemaTermKind::Property,
    ),
];

impl GraphSchemaTermKind {
    /// The kind a `rdf:type` object IRI declares, if it declares a term.
    pub fn declared_by(type_iri: &str) -> Option<Self> {
        DECLARATIONS
            .iter()
            .find(|(declaring, _)| *declaring == type_iri)
            .map(|(_, kind)| *kind)
    }

    /// The wire token, also used inside the page cursor.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Class => "class",
            Self::ObjectProperty => "object_property",
            Self::DatatypeProperty => "datatype_property",
            Self::AnnotationProperty => "annotation_property",
            Self::Property => "property",
        }
    }

    fn parse(token: &str) -> Option<Self> {
        [
            Self::Class,
            Self::ObjectProperty,
            Self::DatatypeProperty,
            Self::AnnotationProperty,
            Self::Property,
        ]
        .into_iter()
        .find(|kind| kind.as_str() == token)
    }
}

/// One declared term of the composed schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct GraphSchemaTermView {
    /// Absolute IRI of the term.
    pub iri: String,
    /// The IRI's fragment, or its last path segment when it has none.
    pub local_name: String,
    pub kind: GraphSchemaTermKind,
    /// The first source (core before dynamic, then by key) declaring the term.
    pub source_id: String,
}

/// One page of the composed schema's declared terms.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct GraphSchemaClassesView {
    pub schema_version: u16,
    pub graph: String,
    /// The composition this page was read from; equal on every page of one walk.
    pub composed_digest: String,
    /// Terms matching the request's `kind` filter, across all pages.
    pub total_terms: u64,
    pub terms: BoundedVec<GraphSchemaTermView, MAX_GRAPH_SCHEMA_TERMS_PAGE>,
    /// Pass back as `cursor` for the next page; absent on the last page.
    #[serde(default)]
    pub next_cursor: Option<String>,
}

/// The `(iri, kind)` position a page cursor names.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct GraphSchemaTermPosition {
    pub iri: String,
    pub kind: GraphSchemaTermKind,
}

impl GraphSchemaTermPosition {
    /// The opaque cursor for this position. IRIs never contain a space, so the
    /// last space separates the IRI from the kind token.
    pub fn cursor(&self) -> String {
        format!("{} {}", self.iri, self.kind.as_str())
    }

    /// Parse a cursor minted by [`Self::cursor`].
    pub fn parse(cursor: &str) -> Result<Self, String> {
        let invalid = || {
            format!(
                "{}: cursor '{cursor}' was not minted by GraphSchemaClasses",
                GraphSchemaErrorCode::TermCursorInvalid.as_str()
            )
        };
        let (iri, token) = cursor.rsplit_once(' ').ok_or_else(invalid)?;
        let kind = GraphSchemaTermKind::parse(token).ok_or_else(invalid)?;
        if iri.is_empty() || iri.contains(' ') {
            return Err(invalid());
        }
        Ok(Self {
            iri: iri.to_string(),
            kind,
        })
    }
}

/// Validate a `GraphSchemaClasses` request's page size.
pub fn validate_term_page_limit(limit: u32) -> Result<usize, String> {
    let limit = usize::try_from(limit).unwrap_or(usize::MAX);
    if (1..=MAX_GRAPH_SCHEMA_TERMS_PAGE).contains(&limit) {
        return Ok(limit);
    }
    Err(format!(
        "{}: limit must be 1..={MAX_GRAPH_SCHEMA_TERMS_PAGE}",
        GraphSchemaErrorCode::TermPageInvalid.as_str()
    ))
}

/// The local name of an IRI: its fragment, else its last path segment.
pub fn term_local_name(iri: &str) -> &str {
    iri.rsplit_once('#')
        .or_else(|| iri.rsplit_once('/'))
        .map_or(iri, |(_, name)| name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_declaring_type_maps_to_one_kind_and_round_trips_its_token() {
        for (iri, kind) in DECLARATIONS {
            assert_eq!(GraphSchemaTermKind::declared_by(iri), Some(*kind));
            assert_eq!(GraphSchemaTermKind::parse(kind.as_str()), Some(*kind));
        }
        assert_eq!(
            GraphSchemaTermKind::declared_by("http://www.w3.org/2002/07/owl#Ontology"),
            None
        );
    }

    #[test]
    fn a_cursor_round_trips_and_a_foreign_one_is_refused() {
        let position = GraphSchemaTermPosition {
            iri: "https://example.org/core#Document".to_string(),
            kind: GraphSchemaTermKind::Class,
        };
        assert_eq!(
            GraphSchemaTermPosition::parse(&position.cursor()),
            Ok(position)
        );
        for foreign in ["", "no-kind", "https://x#A widget", " class"] {
            let error = GraphSchemaTermPosition::parse(foreign).unwrap_err();
            assert!(error.starts_with("SCHEMA_TERM_CURSOR_INVALID"), "{error}");
        }
    }

    #[test]
    fn the_page_limit_is_bounded_on_both_sides() {
        assert_eq!(validate_term_page_limit(1), Ok(1));
        assert_eq!(validate_term_page_limit(1000), Ok(1000));
        for limit in [0, 1001, u32::MAX] {
            let error = validate_term_page_limit(limit).unwrap_err();
            assert!(error.starts_with("SCHEMA_TERM_PAGE_INVALID"), "{error}");
        }
    }

    #[test]
    fn a_local_name_is_the_fragment_else_the_last_segment() {
        assert_eq!(term_local_name("https://x.org/core#Person"), "Person");
        assert_eq!(term_local_name("https://x.org/core/Person"), "Person");
        assert_eq!(term_local_name("urn:x"), "urn:x");
    }
}
