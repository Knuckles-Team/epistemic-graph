//! Typed RDF input and the ontology inspection view (EH-471/EH-472).
//!
//! Callers that orchestrate agents (agent-utilities, graph-os) own no RDF, OWL or
//! SHACL semantics (operator ruling 2026-09-24). They hand the engine plain typed
//! triples (`ShaclValidate.data_triples`) and read vocabulary back as typed views
//! (`OntologyInspect`), so neither side needs an RDF library outside EG.

use serde::{Deserialize, Serialize};

/// The most typed triples one request may carry (the same bound one schema
/// document may hold, `MAX_SCHEMA_DOCUMENT_TRIPLES`).
pub const MAX_TYPED_TRIPLES: usize = 100_000;

/// The most inline documents one `OntologyInspect` may carry.
pub const MAX_INSPECT_DOCUMENTS: usize = 64;

/// One RDF triple as data. `subject` and `predicate` are absolute IRIs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RdfTriple {
    pub subject: String,
    pub predicate: String,
    pub object: RdfObject,
}

/// A triple's object: an absolute IRI or a literal. A literal carries at most one
/// of `datatype` (an absolute IRI) and `language` (a BCP 47 tag); neither means a
/// plain string.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum RdfObject {
    Iri {
        iri: String,
    },
    Literal {
        lexical: String,
        #[serde(default)]
        datatype: Option<String>,
        #[serde(default)]
        language: Option<String>,
    },
}

/// One named class of an inspected vocabulary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct OntologyClassView {
    pub iri: String,
    /// The lexicographically first `rdfs:label`, when there is one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    /// The lexicographically first `rdfs:comment`, when there is one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    /// Named `rdfs:subClassOf` parents, sorted.
    pub parents: Vec<String>,
}

/// One named object or datatype property of an inspected vocabulary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct OntologyPropertyView {
    pub iri: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comment: Option<String>,
    /// Named `rdfs:domain` classes, sorted.
    pub domains: Vec<String>,
    /// Named `rdfs:range` classes or datatypes, sorted.
    pub ranges: Vec<String>,
    /// Declared `owl:SymmetricProperty`.
    pub symmetric: bool,
}

/// What `OntologyInspect` read: the vocabulary, SHACL targets and identity of a
/// set of ontology/shapes documents.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct OntologyInspection {
    /// Sorted sha256 of every document inspected.
    pub schema_digests: Vec<String>,
    /// The request graph's composed GraphSchema digest when committed sources
    /// were inspected; absent for inline documents.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub composed_digest: Option<String>,
    /// Distinct triples across the inspected documents.
    pub triple_count: u64,
    /// sha256 of the sorted, newline-joined N-Triples lines of the distinct
    /// triples (the `urdna2015-sha256` identity of a blank-node-free graph).
    /// Absent when a document has blank nodes, whose labels are not canonical.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub canonical_digest: Option<String>,
    /// Named `owl:Ontology` subjects, sorted.
    pub ontologies: Vec<String>,
    pub classes: Vec<OntologyClassView>,
    pub object_properties: Vec<OntologyPropertyView>,
    pub datatype_properties: Vec<OntologyPropertyView>,
    /// Named `sh:targetClass` objects, sorted.
    pub shape_target_classes: Vec<String>,
}
