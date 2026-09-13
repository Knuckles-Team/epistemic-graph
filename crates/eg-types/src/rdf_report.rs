//! RDF load, update, rule-reasoning and shape-validation reports -- the result bodies of
//! `AddTriples`, `ApplyMutation`, `RunRules`, `ShaclValidate` and `ShexValidate`.
//!
//! The load, update and rule reports are the engine types themselves; eg-rdf re-exports
//! them. The SHACL and ShEx reports are wire projections of `eg_shacl::ValidationReport`
//! and `eg_shex::ShexReport`, whose result rows carry modality-contract impls that must
//! stay in their own crates.

use serde::{Deserialize, Serialize};

/// Summary of an RDF triple load (`eg_rdf::mapping::load_triples`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct LoadReport {
    /// Total triples consumed.
    pub triples: usize,
    /// Multi-valued literal extras encountered (second+ literal per `(s,p)`).
    pub multivalue: usize,
}

/// What a SPARQL Update run (`eg_rdf::update::execute`) changed.
#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct UpdateReport {
    /// Update operations applied (one per `;`-separated clause).
    pub operations: usize,
    /// Triples inserted.
    pub inserted: usize,
    /// Triples deleted.
    pub deleted: usize,
}

/// How a coordinated SPARQL-over-HTTP update ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum SparqlUpdateOutcome {
    /// Every planned graph committed its after-image.
    Committed,
    /// The forward commit did not take and every graph was restored.
    Compensated,
}

/// The durable saga result of a SPARQL-over-HTTP update that spans graphs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct CoordinatedSparqlUpdate {
    pub outcome: SparqlUpdateOutcome,
    pub updated_graphs: usize,
    pub created_graphs: usize,
}

/// The body of a `Method::ApplyMutation` result. A graph-scoped SPARQL Update answers
/// with its [`UpdateReport`]; one submitted through the SPARQL-over-HTTP event type is
/// coordinated across graphs and answers with the saga's [`CoordinatedSparqlUpdate`].
/// Untagged, so each variant is byte-identical on the wire to the struct it wraps.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ApplyMutationResult {
    Report(UpdateReport),
    Coordinated(CoordinatedSparqlUpdate),
}

/// One fact in a [`RuleReasonResponse`].
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RuleFact {
    pub predicate: String,
    pub args: Vec<String>,
    pub confidence: f64,
    pub derived: bool,
}

/// The serialisable rule-reasoning response.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RuleReasonResponse {
    pub facts: Vec<RuleFact>,
    pub same_as: Vec<(String, String)>,
    pub consistent: bool,
    pub conflicts: Vec<String>,
    pub registered_rules: Vec<String>,
}

/// SHACL result severity -- `sh:Violation` / `sh:Warning` / `sh:Info`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum ShaclSeverity {
    Violation,
    Warning,
    Info,
}

/// One `sh:ValidationResult`. Terms are N-Triples lexical forms.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ShaclValidationResult {
    /// `sh:focusNode`.
    pub focus_node: String,
    /// `sh:resultPath`, for a property-shape result.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    /// `sh:value`, when the constraint component reports one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    /// `sh:sourceShape`.
    pub source_shape: String,
    /// `sh:sourceConstraintComponent`.
    pub constraint_component: String,
    /// `sh:resultMessage`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// `sh:resultSeverity`.
    pub severity: ShaclSeverity,
}

/// An `sh:ValidationReport` (`Method::ShaclValidate`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ShaclValidationReport {
    /// `sh:conforms` -- true iff there are no results.
    pub conforms: bool,
    /// The `sh:result` list.
    pub results: Vec<ShaclValidationResult>,
}

/// One entry of a ShEx result shape map: one focus node tested against one shape label.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ShexNodeResult {
    /// The focus node (N-Triples term form).
    pub node: String,
    /// The shape label (an IRI, or `START`).
    pub shape: String,
    pub conforms: bool,
    /// Why the node does not conform, when it does not.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// A ShEx validation report over a whole shape map (`Method::ShexValidate`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct ShexValidationReport {
    /// True iff every tested (node, shape) pair conforms.
    pub conforms: bool,
    /// One result per shape-map entry, in input order.
    pub results: Vec<ShexNodeResult>,
}
