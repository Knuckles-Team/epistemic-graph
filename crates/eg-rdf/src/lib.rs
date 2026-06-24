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

#[cfg(feature = "sparql")]
pub mod sparql;
