# Shared SHACL refusal hardening

The shared `eg-shacl` evaluator is used by SHACL validation, explicit-focus
validation, ICV reports, write guards, and connector-pack admission. These fixes
belong there, not in an offline CLI.

## Reproduced before the change

At `9a01cccf41584ae1dfab473ee14919435f73377f`, four native fixtures executed:

- A one-edge `sh:node` chain ending in `sh:in ()` reports a violation, but a
  45-edge chain incorrectly reports conformance in both SHACL and ICV. The
  recursion cutoff returned success. This can bypass integrity constraints.
- `sh:equals ex:q` on a property shape with an `ex:p` value and no `ex:q` value
  incorrectly reports conformance in both APIs: the unsupported predicate was
  discarded. This is a second integrity-validation bypass.
- A 61-triple graph of 12 duplicated `sh:and` levels completes 8,191 shape
  evaluations. This demonstrates exponential amplification despite a small
  graph-size estimate. No runtime work counter existed. This small repro did
  **not** execute ten million operations or establish process exhaustion.
- SHACL presentation annotations, `rdfs:label`, and application annotations
  correctly leave conformance unchanged; that behavior is preserved.

The command was `cargo test --offline --locked -p eg-shacl --features
 eg-rdf/owl-dl --test hardening -- --nocapture` with one compiler job. Compilation
completed in 5m13s and all four before-state assertions passed in 0.06s.

## Shared behavior after the change

Every validation starts a fixed ten-million-unit allowance. Shape parsing is
charged conservatively by shapes-graph size; target/value candidates, constraint
checks, membership scans, SPARQL scans, binding joins, transformations, and ICV
witness processing debit the same allowance. Nested shape evaluation retains
its existing depth ceiling of 40 but now refuses at exhaustion. SPARQL evaluation
also guards recursive algebra/expression evaluation. ICV shape-registry and
prefix-import traversal are iterative; prefix imports only inspect supplied
in-graph triples and never fetch resources.

The existing `Result<_, String>` contract is preserved. `WORK_EXCEEDED` and
`DEPTH_EXCEEDED`, classified by `is_resource_refusal`, identify budget failures.
No partial report or conformance result is returned on exhaustion. The pack
adapter maps either resource error to the existing `ValidationBudgetExceeded`;
other evaluation errors remain `ShapesInvalid`.

An upfront shared audit refuses unknown SHACL predicates, custom constraint
components, unsupported paths, invalid values for supported parameters, ambiguous
singleton parameters, and malformed/cyclic constraint lists. Known presentation
and report metadata and external annotation predicates remain accepted. A boolean
`"1"^^xsd:boolean` activates `sh:closed`/`sh:deactivated` like `true`.

## Release boundary and limitations

The core commit changes only `eg-shacl` and this note. That crate is identical
between main `29301642` and the extraction stack, so the core fix can be
cherry-picked independently onto main. The following pack-adapter commit depends
on the shared orchestration extraction in PR26. Neither commit requires storage,
authentication, a running server, a dependency cycle, or a package release.

This is targeted engine hardening, **not a completed fail-closed offline CLI**.
Work units are logical traversal/candidate/binding costs, not a wall-clock or
process-memory guarantee. Direct Turtle parsing still needs caller-owned upfront
allocation limits. This change does not certify every SHACL/SPARQL semantic
feature: implicit class targets and the evaluator's documented SPARQL subset and
deviations still require explicit profile decisions and differential fixtures.
The versioned profile, exact UTF-8 document identities/digests, bounded output,
and CLI are not delivered here. No attachment or authorization is inferred from
these checks.

A follow-up experiment with 45 nested parenthesized SPARQL negations stalled in
`spargebra` parsing before evaluation. The native test was interrupted; it is not
counted as passing validation. AST evaluation depth is tested directly instead.
Charging query bytes before parsing does not bound parser runtime. A bounded
parser strategy (or an explicit supported-profile restriction agreed for the
shared engine) is required before advertising hostile-input-safe CLI validation.
