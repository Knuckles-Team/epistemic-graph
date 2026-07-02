//! eg-shacl — a pure-Rust **SHACL Core** validation engine (CONCEPT:EG-132).
//!
//! Validates an RDF **data graph** against an RDF **shapes graph** — both the
//! `eg_rdf::oxrdf::Graph` term/graph model this crate reuses (so it sits ABOVE eg-rdf in
//! the DAG; eg-rdf does not depend on eg-shacl → no cycle). The engine parses SHACL Core:
//!
//! * shape kinds — `sh:NodeShape` / `sh:PropertyShape` (implicit from `sh:path`);
//! * targets — `sh:targetClass`, `sh:targetNode`, `sh:targetSubjectsOf`,
//!   `sh:targetObjectsOf`;
//! * property paths — **predicate paths** (`sh:path <p>`); complex paths are recognised
//!   and skipped as an EG-132 follow-up;
//! * constraint components — cardinality (`sh:minCount`/`sh:maxCount`), `sh:datatype`,
//!   `sh:class`, `sh:nodeKind`, value range (`sh:minInclusive`/`sh:maxInclusive`/
//!   `sh:minExclusive`/`sh:maxExclusive`), string (`sh:minLength`/`sh:maxLength`/
//!   `sh:pattern`+`sh:flags`/`sh:languageIn`), `sh:in`, `sh:hasValue`, logical
//!   (`sh:and`/`sh:or`/`sh:not`/`sh:xone`), `sh:node`, and `sh:property`.
//!
//! [`validate`] returns a serde-serializable [`ValidationReport`] = `conforms` + a list of
//! [`ValidationResult`] (`focus_node`, `path`, `value`, `source_shape`,
//! `constraint_component`, `message`, `severity`).
//!
//! Pi contract: pure Rust, no C/native dep. `sh:sparql` constraints are parsed to a
//! deferred marker but NOT evaluated (a documented follow-up — see [`shapes::Constraint`]).
//!
//! The [`icv`] module layers **Integrity Constraint Validation** (CONCEPT:EG-146) on
//! top: the same shapes read as Stardog-style **closed-world** DB integrity constraints
//! ([`validate_icv`]), with a SPARQL **explain witness** per violation and a
//! [`check_write`] guard for constraint-enforced transactions.

pub mod icv;
pub mod report;
pub mod shapes;
pub mod validate;
pub mod vocab;

pub use icv::{
    check_write, validate_icv, validate_icv_turtle, validate_icv_with_inferences, IcvReport,
    IcvViolation, WriteCheck,
};
pub use report::{Severity, ValidationReport, ValidationResult};
pub use validate::{graph_from_turtle, validate, validate_turtle};

/// Re-export the RDF graph model so callers name `eg_shacl::Graph` without their own
/// oxrdf dependency (the shapes graph and data graph are both this type).
pub use eg_rdf::oxrdf::Graph;
