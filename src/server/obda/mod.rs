//! CA-13 — OBDA wire method + Python client seam.
//!
//! ## W01 baseline correction (measured 2026-08-26)
//!
//! The lane brief's evidence ("MEASURED (protocol.rs, full-text grep): no `Method::Obda*`
//! variant exists... `src/server/mod.rs:5409-5417` references OBDA only inside a
//! test-fixture comment — no dispatch arm") was a **naming-pattern false negative**, not a
//! functionality gap: grepping for the literal string `Obda` inside `Method::` variant
//! names never matches `Method::SparqlVirtual`, which is the wire method that already
//! does exactly what this lane's mission needs. Re-measured here:
//!
//! * `crates/eg-types/src/protocol.rs:3507` — `Method::SparqlVirtual { query, mapping,
//!   tables, external_sources }`, feature-gated `obda`.
//! * `src/server/handlers/rdf.rs:238` — the dispatch arm (`handle_sparql_virtual` →
//!   `sparql_virtual`), which resolves each `tables` entry through
//!   `sql_catalog_acl::open_authorized_table` (RLS-scoped read, the SAME ACL path every
//!   other `Sql`/`Sparql` read uses — never a bypass), registers each as an
//!   `eg_rdf::obda::ObdaSource`, parses `mapping` (auto-detecting standard R2RML Turtle
//!   vs. the compact EG-101 textual form via `eg_rdf::obda::parse_r2rml_turtle` /
//!   `parse_mapping`), and runs `eg_rdf::obda::run_outcome_virtual`.
//! * `src/server/access.rs:2161` and `crates/eg-capabilities/src/lib.rs:2189,2848` —
//!   already classified (`mutates: false`, `authz_action: "sparql:read"`,
//!   `durability_domain: None`, `audited: false`, `idempotent: true`,
//!   `txn_participation: Snapshot`) and present in `eg-capabilities`' `ALL_METHODS`
//!   consistency test.
//! * `epistemic_graph/client.py` — `RdfClient.sparql_virtual` (pre-existing, ~line
//!   10578) already sends `Method::SparqlVirtual` and decodes the row-dict result.
//!
//! This history predates the CA program entirely (`git log`: `EG-101`, `EG-305`,
//! `W4.11` — the OBDA/R2RML/live-external-SQL work landed across several older waves,
//! long before CA-13 was written). CA-17's own W00 stub comment (this file, before this
//! commit) already named `Method::SparqlVirtual` correctly, so the false premise lived
//! only in the lane brief's evidence section, not in the codebase.
//!
//! ## W02 design decision
//!
//! Given the above, CA-13 does **not** add a new `Method::ObdaMapping{Load,Eval}`
//! variant, does **not** touch `crates/eg-types/src/protocol.rs`, and does **not**
//! introduce a server-side mapping registry on `ServerState`. `eg_rdf::obda` is itself
//! stateless — every `SparqlVirtual` call re-parses the mapping and re-scans only the
//! query-relevant columns/rows into a transient view, then discards it
//! (`crates/eg-rdf/src/obda.rs` module doc) — and `Method::SparqlVirtual` already
//! matches that shape exactly, satisfying this lane's own Authority invariant ("a
//! loaded mapping and its `ObdaSource` are never a second write path"). Introducing a
//! stateful `Load`/`Eval` split server-side would be a REGRESSION from the existing
//! design, not an improvement, and would collide with `crates/eg-types/src/protocol.rs`
//! (`file-ownership.yaml` FO-CA-029, ordered CA-13 (W2) -> CA-16 (W3)) for no benefit —
//! CA-16 has already landed its own variant there (`merge(ca-16)`, `main`), so avoiding
//! that file entirely also sidesteps a real rebase collision.
//!
//! The Load/Eval ergonomics the brief asks for (so au's CA-23 gets a `client.obda.load(
//! mapping_turtle, source_name)` / `client.obda.evaluate(query)` pair, matching its
//! current per-connector `r2rml_mappings` dict shape) are instead implemented ENTIRELY
//! client-side, in `epistemic_graph/client.py`'s `ObdaClient` (`FO-CA-027`, exclusive to
//! this lane): `load` remembers `mapping_turtle` under `source_name` in a plain dict;
//! `evaluate` sends the identical `SparqlVirtual` request `RdfClient.sparql_virtual`
//! sends, with `tables = [source_name]`. Re-`load`-ing a `source_name` is idempotent
//! (last-load-wins — a dict assignment; nothing server-side to double-register).
//! `source_name` doubles as the engine SQL table name the mapping's
//! `logical_source`/`rr:tableName` must reference (created via `QueryClient` DDL or
//! `import_sqlite_file`) — one mapping, one backing table, matching CA-23's
//! per-connector shape.
//!
//! ## The one real engine-side gap this lane found and fixed
//!
//! `eg_rdf::obda::run_outcome_virtual` computes which predicates a query statically
//! references (`wanted_predicates`) purely to PUSH DOWN column projection — a query
//! naming a predicate no loaded `TriplesMap` declares simply materialized zero triples
//! for it and returned zero solutions, silently indistinguishable from "this predicate
//! has data, just none matching the rest of the query" (a legitimate, correct SPARQL
//! answer). That is exactly the failure mode this lane's P6 negative case and
//! Acceptance gate 4 forbid ("querying an unregistered predicate returns a typed error,
//! demonstrated on purpose"). CA-13 added `VirtualGraph::declared_predicates()` and a
//! reject-with-typed-error check at the top of `run_outcome_virtual`
//! (`crates/eg-rdf/src/obda.rs`) — see that module's tests (`ca13_*`) for the positive/
//! negative/known-bad coverage, including a determinism check that a genuinely
//! unrestricted query (`?s ?p ?o`) is unaffected, since its predicate set cannot be
//! statically enumerated and the check is a no-op in that case by design.
//!
//! ## What remains genuinely out of this lane's scope
//!
//! Row-level RLS enforcement on OBDA-materialized rows is `sql_catalog_acl`'s job
//! (`src/server/sql_catalog_acl.rs`, owned elsewhere) — CA-13 proves (in
//! `crates/eg-rdf/src/obda.rs`'s `ca13_source_row_restriction_is_never_bypassed_by_obda`)
//! that the OBDA materialize/evaluate pipeline itself introduces NO bypass of whatever
//! row set its `ObdaSource` hands it, which is the boundary this lane owns; it does not
//! re-test `sql_catalog_acl`'s own ACL/RLS predicate machinery, which is exercised by
//! its own test suite. Deleting au's hardcoded `r2rml_mappings` dict
//! (`agent_utilities/knowledge_graph/core/owl_bridge.py:1455-1479`) is CA-23's job,
//! gated on this lane's `ObdaClient` landing (`file-ownership.yaml` FO-CA-006/008 note).
