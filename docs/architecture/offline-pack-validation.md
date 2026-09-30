# Shared offline pack validation: staged boundary

Status: **shared preprocessing and admission orchestration; no standalone
validation profile or CLI is shipped by this change.** A successful preprocessing
call does not establish ontology consistency, shape conformance, pack admission, graph attachment or
authorization.

## First stage: shared engine code

`eg_rdf::pack` (feature `rdf`) now owns the former connector-pack server helpers:

| API | Shared behavior (including second-stage limits) |
| --- | --- |
| `validate_document` | Enforce the 2 MiB body bound before the engine Turtle parser, then stream at most 100,000 triples with existing 4 KiB term checks and 256 KiB literal values; return the existing `PackViolationCode` and static detail. |
| `validate_ontology_imports` | Imports must be named IRIs of other supplied ontology entries; refuse self-imports, external imports and non-IRI objects; never fetch. |
| `declared_shape_iris` | Discover named node/property shapes for existing duplicate-IRI warnings; malformed input gives no names and must separately fail document validation. |
| `scoped_union` | Parse files independently in caller order, scope subject/object blank nodes by file index, and render the union as N-Triples; report the first malformed file index. |

The server imports or delegates to these functions. The old private union module
is removed, so it cannot diverge from a future offline consumer. The first
extraction added no dependency, package or wire type; the second stage adds the
orchestration crate below, still using the same RDF parser and reasoners.
The `rdf`-disabled refusal path remains in the server.

The first stage retained the existing subject IRI bound counting its rendered
angle brackets. The second stage intentionally strengthens allocation refusals:
`validate_document` checks bytes before parsing and stops on the first streamed
syntax, triple or term refusal; a term failure can now precede malformed Turtle
later in the document. `scoped_union` retains its original unbounded contract;
new admission callers use `scoped_union_with_limits` instead. Neither helper is
a complete validator.

## Second stage: shared orchestration and allocation guards

`eg-pack-validation` sits above `eg-rdf` (with `owl-dl`), `eg-shacl` and
`eg-types`. It adds no external dependencies. The facade's `shacl` feature opts
into this crate; disabled-feature refusals stay in the server. The call requires
no storage, authentication or server startup, although `eg-rdf` still has its
existing compile-time dependency on `eg-core`.

The server now directly aliases `eg_pack_validation::validate_unions`. It is the
former `validate_rdf_unions`/`validate_shapes_union` orchestration with the same
class/ABox checker, SHACL evaluator, static refusal messages and 10,000,000
reasoning and SHACL graph-size-product limits. Entry identity/import/base/term
validation remains a prerequisite. This is not yet a stable Pipelines CLI.

Before allocating parsed unions, preflight checks both document lists together:
up to 1,024 documents, at most 2 MiB per document and 16 MiB total UTF-8 bytes,
reusing `eg-types` pack constants. Limits are inclusive. Then the existing Turtle
parser is streamed through `mapping::turtle_triples`; the ordinary
`parse_turtle` also uses that iterator. Each union stops before accumulating more
than 100,000 triples or 16 MiB of rendered N-Triples. Streamed entry validation also
checks that rendered-byte bound before import/shape-discovery passes. Prefix expansion therefore
cannot amplify a small text into an unbounded collected union. A single parsed
term is still bounded by its input document, not by a parser-level lexical cap.

New resource failures use `PackTooLarge` for input bytes/count and
`ValidationBudgetExceeded` for collected/rendered union bounds. Allocation
preflight intentionally wins over all parse/semantic errors; streamed syntax
errors win at their position, before testing the current triple's count. All
otherwise admissible inputs use the original ontology-then-shapes order.
The new shapes-union and rendered-byte limits are intentional stricter refusals,
not claims that the first extraction had those bounds.

### Why this stage does not ship a passing CLI verdict

Inspection found that SHACL `Validator::validate_focus` returns `Ok(())` past
`MAX_DEPTH`, and `node_conforms` returns `Ok(true)`. A recursion limit must instead
produce a typed refusal before a fail-closed profile is exposed. The existing
10-million graph-size product also does not meter recursive evaluation or
SPARQL joins, and shape parsing ignores unrecognized predicates. Moving these
functions does not fix those semantics. A bounded CLI requires an independently
reviewed shared SHACL hardening change, not a CLI-only check or a second RDF
interpretation.

## Remaining work before a CLI can be a CI gate

1. Establish the versioned profile in an EG-owned component above `eg-rdf` and
   `eg-shacl`. The orchestration has moved into `eg-pack-validation`; make the
   future CLI invoke it after its profile prerequisites. Reuse
   `eg_rdf::tableau::check_pack_ontology` for
   class and ABox checks and the existing SHACL/ICV evaluator; do not implement
   a second semantics stack. Runtime storage, auth and server startup are not
   prerequisites for this library call.
2. Make resource bounds enforceable before allocation/evaluation. Admission now
   streams bounded unions, while the SHACL guard still checks `shape_triples * max(ontology_triples, 1)` rather than
   metering all evaluator work. Centralize document/count/total-byte, nesting,
   triple, reasoning, evaluation and diagnostic limits in the profile. Preserve
   admission's existing 2 MiB body cap and refusal codes; make any stronger
   profile restrictions explicit and test the engine consumer too. Do not
   silently replace the existing 10,000,000 reasoning or SHACL budgets.
3. Audit the supported syntax boundary before advertising fail-closed coverage.
   The server already rejects non-predicate SHACL paths and SERVICE text;
   `ShapesGraph::parse_shape` gathers known constraints but does not validate
   every predicate as a supported constraint. Unsupported path evaluation itself
   yields no values (`validate.rs`). RDF-star feature unification and recursive
   blank-node terms also need an explicit policy before a stable profile. Add
   refusals in the shared EG implementation with engine-path regressions, not
   a CLI-only interpretation of OWL or SHACL.
4. Add a thin versioned JSON CLI once the profile is ready. It must not claim
   attachment or authorization, fetch imports, read an implicit live baseline,
   or accept user-overridden resource limits. Pipelines owns installation,
   exact revision/artifact pinning, chosen profile and invocation.

## Proposed versioned I/O contract (not implemented)

The request identifies a profile and an ordered list of uniquely identified
ontology/shape documents, each containing its exact UTF-8 text. Composition
requires explicit baseline source documents, individually identified in that
same list. A receipt, digest or remote IRI is not a substitute for source text.
Preserve order through parsing, first-refusal selection and input-digest output;
never concatenate Turtle before parsing. Reject duplicate identities and
unsupported profiles deterministically.

The report carries the EG implementation revision, profile version, ordered
per-document SHA-256 digests over the exact UTF-8 bytes, a verdict and bounded
structured diagnostics using existing EG refusal codes. Include role and
identity beside every digest. A top-level input digest, if needed, must use a
versioned canonical framing of identities, roles, order and byte digests.
Diagnostics must state truncation and cannot include unbounded source excerpts.
Any incomplete evaluation is a refusal, never a passing verdict. Process errors
must remain distinguishable from completed validation refusals.

## Verification boundary

The first-stage crate fixtures cover malformed ontology/shapes documents,
100,000/100,001 triples, literal byte limits, the existing IRI measurement,
self/external/non-IRI imports, named shape discovery, repeated blank labels,
within-file blank references, prefix isolation, document order and union
round-trip. Existing server import-policy tests consume the re-exported shared
functions. They are not a substitute for an engine build.

Before enabling the CLI gate, run identical fixtures through the CLI and engine
consumer: malformed Turtle, forbidden imports, blank-node isolation, unsupported
shapes, class contradiction, ABox contradiction, each budget exhausted, valid
multi-document packs and explicit-baseline composition. Compare profile,
ordered digests, verdict and diagnostic codes/order. Include unavailable-feature
and unsupported-profile refusals. No passing CLI evidence is claimed by this
stage.

Second-stage library fixtures add class/ABox contradictions, blank shape
isolation, non-predicate paths, SERVICE rejection, SHACL nonconformance,
aggregate preflight precedence and exact boundaries, multi-document triple
exhaustion, the existing SHACL product budget and actual exhaustion of the fixed
10-million reasoning budget. Parser fixtures exercise rendered expansion limits,
first failing document indices and the intentional early-refusal ordering.
Server feature builds and CLI/engine differential execution remain separate
required evidence; library fixtures do not stand in for them.
