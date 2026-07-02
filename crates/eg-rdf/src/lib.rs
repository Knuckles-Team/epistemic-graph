// oxrdf 0.3 renamed `Subject` -> `NamedOrBlankNode`; the legible `Subject` alias
// is still re-exported (deprecated). Keep using it for readability.
#![allow(deprecated)]

//! eg-rdf — native RDF/SPARQL surface over the epistemic-graph property-graph
//! (Lane W increment 1: W1 `CONCEPT:KG-2.217`, W2 `CONCEPT:KG-2.218`).
//!
//! This is the productionized form of the `spike/rdf-owl` feasibility spike. It
//! proves — and now ships — that RDF/SPARQL sit NATIVELY over the engine's
//! property-graph + redb substrate (NOT as a Python layer):
//!
//!   * `mapping` (W1) — the RDF dataset ⇄ property-graph mapping plus Turtle /
//!     N-Triples parse + serialize round-trip. A resource object becomes a typed
//!     edge `{type: predicate}`; a literal object becomes a typed JSON property
//!     cell preserving its xsd datatype + `@lang`; `rdf:type` folds into the engine
//!     `type` label; a named graph is a graph in the registry.
//!   * `quads` (W1, feature `rdf-redb`) — an OPT-IN lossless redb `quads` table for
//!     the ONE lossy edge of the property-graph mapping: a subject holding two
//!     different literals for the SAME predicate (the property blob is key-unique).
//!     Used only when a predicate is multi-valued; the property-graph stays the
//!     query-fast default.
//!   * `sparql` (W2) — `spargebra`'s SPARQL 1.1 algebra COMPILED to scans over the
//!     engine's `GraphView` (BGP + FILTER + OPTIONAL + UNION + basic property
//!     paths). No second copy of the graph, no embedded oxigraph store.
//!
//! Pi contract: every dep is feature-gated OFF by default, so a `default`/`pi`
//! build links none of oxrdf/oxttl/spargebra (all pure-Rust regardless).

/// Re-export the oxrdf term model so callers (the engine handler) can name `Triple`
/// etc. without taking their own oxrdf dependency.
#[cfg(feature = "rdf")]
pub use oxrdf;

#[cfg(feature = "rdf")]
pub mod mapping;

#[cfg(feature = "rdf-redb")]
pub mod quads;

/// Re-export the spargebra SPARQL algebra/term/update model so the engine (the
/// `/sparql` HTTP endpoint) can name `Update` / `GraphUpdateOperation` without taking
/// its own spargebra dependency.
#[cfg(feature = "sparql")]
pub use spargebra;

#[cfg(feature = "sparql")]
pub mod sparql;

/// SPARQL 1.1 UPDATE executed over the native property-graph write ops (CONCEPT:EG-017):
/// INSERT/DELETE DATA, DELETE/INSERT … WHERE, CLEAR/CREATE/DROP GRAPH, with named-graph
/// routing through the `GraphStore` trait. Behind `sparql` (needs spargebra's Update).
#[cfg(feature = "sparql")]
pub mod update;

/// W3 — the native OWL 2 (EL⁺ + RL) reasoner (CONCEPT:KG-2.219). Classification,
/// consistency checking, incremental materialization + justifications over the
/// oxttl-parsed ontology. Pure Rust; behind `owl` (implies `rdf`).
#[cfg(feature = "owl")]
pub mod owl;

/// EG-021 — user-defined custom rules + instance-level (ABox) OWL 2 RL / Datalog
/// reasoning with `owl:sameAs` equality, run in one confidence-propagating
/// forward-chaining fixpoint. The second Stardog-parity gap (custom rules) plus the
/// instance side of the broader OWL-2 axioms (`owl.rs` covers the TBox/concept side).
/// [`rules::run_rule_reasoning`] / [`rules::run_rule_reasoning_on_view`] are the
/// server-op-ready entry points (a `RunRules` op is a thin pass-through). Behind `owl`.
#[cfg(feature = "owl")]
pub mod rules;

/// W5 — the OWL-DL **tableau** reasoner (CONCEPT:EG-059). A pure-Rust description-logic
/// tableau (ALCQO + role hierarchy) that DECIDES the constructs the monotone EL⁺/RL
/// [`owl`] path cannot: full cardinality restrictions (`≥n`/`≤n`, qualified),
/// `complementOf`/negation (via negation-normal-form), `oneOf`/`hasValue` nominals, and
/// reasoning-by-cases over a `unionOf` SUPERCLASS. Entry points: [`tableau::is_consistent`],
/// [`tableau::is_subsumed`], [`tableau::is_instance`], [`tableau::classify_dl`], and the
/// [`tableau::reason_dl`] engine-picker (EL⁺/RL fast path when the ontology is inside the
/// tractable envelope, tableau only when a DL construct forces it). Pure Rust; behind
/// `owl-dl` (implies `owl`). Pi contract: OUT of `pi`, folded into node/full.
#[cfg(feature = "owl-dl")]
pub mod tableau;
