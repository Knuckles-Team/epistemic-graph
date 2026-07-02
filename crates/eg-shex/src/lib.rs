//! eg-shex — a pure-Rust **ShEx (Shape Expressions) Core** validation engine
//! (CONCEPT:EG-133).
//!
//! The complement to [`eg-shacl`](../eg_shacl) (CONCEPT:EG-132): where SHACL constrains an
//! RDF graph with an RDF shapes graph, ShEx validates that focus nodes **conform** to
//! *shape expressions*. This crate reuses the `eg_rdf::oxrdf::Graph` term/graph model for
//! the data graph (so it sits ABOVE eg-rdf in the DAG; eg-rdf does not depend on eg-shex →
//! no cycle) and parses a **ShExJ** (JSON abstract-syntax) schema:
//!
//! * shape expressions — `NodeConstraint`, `Shape`, and the `ShapeAnd`/`ShapeOr`/
//!   `ShapeNot` combinators, plus IRI-string shape references;
//! * node constraints — `nodeKind` (iri/bnode/literal/nonliteral), `datatype`, string
//!   facets (`length`/`minlength`/`maxlength`/`pattern`+`flags`), numeric facets
//!   (`mininclusive`/`maxinclusive`/`minexclusive`/`maxexclusive`), and value sets;
//! * triple constraints — `predicate` + `valueExpr` + cardinality (`min`/`max`, `-1` ⇒
//!   unbounded), composed by the `EachOf` (`;`) and `OneOf` (`|`) triple-expression
//!   combinators;
//! * `closed` shapes with `extra` predicates.
//!
//! [`validate`] takes a [`Schema`], a data [`Graph`] and a [`ShapeMap`] (focus node →
//! shape label) and returns a serde-serializable [`ShexReport`] (`conforms` + a per-node
//! result list). [`validate_shexj`] is the parse-and-validate convenience.
//!
//! ## Scope (ShExJ vs ShExC)
//!
//! ShEx has two syntaxes: **ShExC** (the compact, Turtle-like grammar) and **ShExJ** (the
//! JSON abstract syntax). This crate implements **ShExJ** — it is the canonical,
//! unambiguous form and is far more robust to parse than the compact grammar. A **ShExC**
//! front-end is a documented EG-133 follow-up.
//!
//! Deferred EG-133 follow-ups: ShExC parsing; inverse (`^`) triple constraints; full
//! backtracking partition for triple constraints that share a predicate; value-set stem /
//! language-stem ranges; `EXTERNAL` shapes; `totaldigits`/`fractiondigits`; semantic
//! actions.
//!
//! Pi contract: pure Rust, no C/native dep (`regex` for pattern facets rides eg-rdf's
//! sparql stack; `serde_json` for ShExJ is already resolved workspace-wide).

pub mod report;
pub mod schema;
pub mod validate;

pub use report::{NodeResult, ShexReport};
pub use schema::{
    NodeConstraint, NodeKind, Schema, Shape, ShapeExpr, ShapeLabel, TripleExpr, ValueSetValue,
    START,
};
pub use validate::{graph_from_turtle, validate, validate_shexj, ShapeMap, ShapeMapEntry};

/// Re-export the RDF graph model so callers name `eg_shex::Graph` without their own oxrdf
/// dependency (the data graph is this type).
pub use eg_rdf::oxrdf::Graph;
