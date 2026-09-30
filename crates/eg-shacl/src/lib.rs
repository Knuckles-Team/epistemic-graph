//! eg-shacl — a pure-Rust **SHACL Core** validation engine (CONCEPT:EG-KG.ontology.concept-6).
//!
//! Validates an RDF **data graph** against an RDF **shapes graph** — both the
//! `eg_rdf::oxrdf::Graph` term/graph model this crate reuses (so it sits ABOVE eg-rdf in
//! the DAG; eg-rdf does not depend on eg-shacl → no cycle). The engine parses SHACL Core:
//!
//! * shape kinds — `sh:NodeShape` / `sh:PropertyShape` (implicit from `sh:path`);
//! * targets — `sh:targetClass`, `sh:targetNode`, `sh:targetSubjectsOf`,
//!   `sh:targetObjectsOf`;
//! * property paths — **predicate paths** (`sh:path <p>`); complex paths are recognised
//!   and refused;
//! * constraint components — cardinality (`sh:minCount`/`sh:maxCount`), `sh:datatype`,
//!   `sh:class`, `sh:nodeKind`, value range (`sh:minInclusive`/`sh:maxInclusive`/
//!   `sh:minExclusive`/`sh:maxExclusive`), string (`sh:minLength`/`sh:maxLength`/
//!   `sh:pattern`+`sh:flags`/`sh:languageIn`), `sh:in`, `sh:hasValue`, logical
//!   (`sh:and`/`sh:or`/`sh:not`/`sh:xone`), `sh:node`, `sh:property`, `sh:closed`
//!   (+ `sh:ignoredProperties`), and `sh:sparql` (a SPARQL-based constraint, evaluated
//!   by the pre-binding-aware engine in [`sparql`] — see [`shapes::SparqlConstraint`]).
//!
//! [`validate`] returns a `Result` of a serde-serializable [`ValidationReport`] =
//! `conforms` + a list of [`ValidationResult`] (`focus_node`, `path`, `value`,
//! `source_shape`, `constraint_component`, `message`, `severity`); the `Err` case is a
//! resource-budget refusal, unsupported or malformed shape syntax, or a
//! `sh:sparql` query that fails to parse or uses a construct this engine does not
//! evaluate (property paths, `MINUS`/`VALUES`/non-`SILENT` `SERVICE`/sub-`SELECT`/
//! aggregates/`EXISTS`/arithmetic, or rebinding `$this` — all constructs the W3C
//! SHACL-SPARQL test suite itself expects an implementation MAY decline).
//!
//! Validation shares a fixed ten-million-unit work allowance across shape parsing,
//! target/value selection, nested constraints, SPARQL evaluation, and ICV witnesses.
//! The existing shape-depth ceiling of 40 now returns an error when exhausted.
//! Work units conservatively charge graph traversals, candidates, and bindings;
//! this is a logical-work limit, not a wall-clock or process-memory guarantee.
//! Direct Turtle parsing still requires caller-supplied input allocation limits.
//!
//! Pi contract: pure Rust, no C/native dep — the `sh:sparql` engine parses with
//! `spargebra` (already pulled in transitively wherever `sparql` is on; pinned here as
//! its own direct dependency so `sh:sparql` works in a `shacl`-only build too).
//!
//! The [`icv`] module layers **Integrity Constraint Validation** (CONCEPT:EG-KG.ontology.wired-into-commit-write) on
//! top: the same shapes read as Stardog-style **closed-world** DB integrity constraints
//! ([`validate_icv`]), with a SPARQL **explain witness** per violation and a
//! [`check_write`] guard for constraint-enforced transactions.

mod budget;
mod parse_work;
mod supported;
pub use budget::{is_resource_refusal, DEPTH_EXCEEDED, MAX_VALIDATION_STEPS, WORK_EXCEEDED};

pub mod icv;
pub mod policy;
pub mod report;
pub mod shapes;
mod sparql;
pub mod validate;
pub mod vocab;

/// `ModalityContract` retrofit for [`report::ValidationResult`] (CONCEPT:E4). Behind
/// the crate's own opt-in `contract` feature (default OFF) — see `src/contract.rs`.
#[cfg(feature = "contract")]
mod contract;

pub use icv::{
    check_write, validate_icv, validate_icv_turtle, validate_icv_with_inferences, IcvReport,
    IcvViolation, WriteCheck,
};
pub use policy::{IcvPolicy, IcvPolicyRegistry};
pub use report::{Severity, ValidationReport, ValidationResult};
pub use validate::{graph_from_turtle, validate, validate_nodes, validate_turtle};

/// Re-export the RDF graph model so callers name `eg_shacl::Graph` without their own
/// oxrdf dependency (the shapes graph and data graph are both this type).
pub use eg_rdf::oxrdf::Graph;
