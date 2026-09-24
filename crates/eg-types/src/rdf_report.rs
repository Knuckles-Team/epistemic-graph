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

/// One fact in a [`RuleReasonResponse`].
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RuleFact {
    pub predicate: String,
    pub args: Vec<String>,
    pub confidence: f64,
    pub derived: bool,
    /// The fact's proof tree when the request asked to `explain`; `None` otherwise.
    pub proof: Option<RuleProofNode>,
}

/// One node of a rule-derivation proof tree (EH-197) — the rule-side twin of the OWL
/// reasoner's `ProofNodeWire`. `rule == "asserted"` marks a leaf: a fact whose
/// confidence is its own assertion. Any other `rule` names the rule that fired over
/// `premises` (each a full sub-proof, in rule-body order) to give `confidence`.
/// `truncated` marks a node whose premises were cut by the proof's depth or size
/// budget, or because the premise is already being proved higher up the same branch.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RuleProofNode {
    pub predicate: String,
    pub args: Vec<String>,
    pub rule: String,
    pub confidence: f64,
    pub premises: Vec<RuleProofNode>,
    pub truncated: bool,
}

/// Whether a SPARQL witness term is a resource or a literal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum SparqlObjectKind {
    Resource,
    Literal,
}

/// Whether a SPARQL row proof certifies the row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum SparqlProofCoverage {
    /// Every required triple pattern is instantiated by a listed triple of the graph,
    /// and the binding satisfies every required-scope FILTER.
    Complete,
    /// The query uses algebra a witness does not certify (property paths, UNION,
    /// MINUS, aggregates, GRAPH, SERVICE, its own FROM dataset, a non-SELECT form), or
    /// the witness search hit its step budget. The listed triples, if any, exist but
    /// do not by themselves prove the row.
    Partial,
}

/// One ground triple of the queried graph that a SPARQL row's witness uses (EH-197).
/// Subject and a resource object are in the evaluator's `<iri>` / `_:b` form; the
/// predicate is a bare IRI; a literal object is its lexical value.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SparqlWitnessTriple {
    pub subject: String,
    pub predicate: String,
    pub object: String,
    pub object_kind: SparqlObjectKind,
}

/// The proof of one SPARQL SELECT row (EH-197): the ground triples that instantiate
/// the query's patterns under the row's bindings.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct SparqlRowProof {
    /// Index of the row in the result's `rows`.
    pub row: u64,
    pub witnesses: Vec<SparqlWitnessTriple>,
    pub coverage: SparqlProofCoverage,
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
    /// Sorted unique document digests of every committed SHACL source used by
    /// this validation. Explicit ad-hoc shapes carry none, so governance
    /// consumers can fail closed rather than mistaking caller-supplied
    /// constraints for committed authority.
    pub schema_digests: Vec<String>,
    /// The request graph's composed digest when committed GraphSchema shapes
    /// were used; absent only for explicit ad-hoc shapes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub composed_digest: Option<String>,
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
