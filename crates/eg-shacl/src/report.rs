//! The SHACL validation report (CONCEPT:EG-KG.ontology.concept-6) — a serde-serializable mirror of an
//! `sh:ValidationReport` (`conforms` + a list of `sh:ValidationResult`).
//!
//! Terms (focus node, path, value, shape) are carried as their canonical N-Triples
//! lexical form (`<iri>` / `_:b` / `"lit"^^<dt>`) so the report is a flat, transport-
//! friendly JSON value the engine handler returns verbatim — no oxrdf types leak across
//! the wire.

use serde::{Deserialize, Serialize};

/// Result severity — mirrors `sh:Violation` / `sh:Warning` / `sh:Info`. `Violation` is
/// the SHACL default; only a `Violation` makes a report non-conforming.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum Severity {
    Violation,
    Warning,
    Info,
}

impl Severity {
    /// The `sh:` IRI for this severity.
    pub fn iri(self) -> &'static str {
        match self {
            Severity::Violation => crate::vocab::VIOLATION,
            Severity::Warning => crate::vocab::WARNING,
            Severity::Info => crate::vocab::INFO,
        }
    }
}

/// One `sh:ValidationResult`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidationResult {
    /// `sh:focusNode` — the node the failing shape targeted (N-Triples term).
    pub focus_node: String,
    /// `sh:resultPath` — the property path of a property-shape violation, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    /// `sh:value` — the offending value node, if the component reports one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    /// `sh:sourceShape` — the shape that produced the result (N-Triples term).
    pub source_shape: String,
    /// `sh:sourceConstraintComponent` — the constraint-component IRI.
    pub constraint_component: String,
    /// `sh:resultMessage` — a human message (from `sh:message` or a generated default).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// `sh:resultSeverity`.
    pub severity: Severity,
}

/// An `sh:ValidationReport`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidationReport {
    /// `sh:conforms` — true iff there is no result of severity `Violation`.
    pub conforms: bool,
    /// The `sh:result` list.
    pub results: Vec<ValidationResult>,
}

impl ValidationReport {
    /// Build a report from its results, deriving `conforms`.
    ///
    /// `conforms` is `true` **iff there are no results of ANY severity** — the
    /// normative W3C definition (`sh:conforms` rdfs:comment: "True if the validation
    /// did not produce any validation results, and false otherwise"; confirmed by the
    /// SHACL test suite itself: `core/misc/severity-001`/`severity-002` and
    /// `sparql/node/sparql-003` all expect `sh:conforms "false"` for a report whose
    /// ONLY results are `sh:Warning`/`sh:Info`/a custom severity). This is a fix, not
    /// a new rule: severity does NOT change whether a shape's constraint failed —
    /// only whether that failure surfaces as `sh:Violation` vs `sh:Warning`/`sh:Info`
    /// in each [`ValidationResult`].
    pub fn from_results(results: Vec<ValidationResult>) -> Self {
        let conforms = results.is_empty();
        ValidationReport { conforms, results }
    }
}
