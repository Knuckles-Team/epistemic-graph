# Shared offline pack validation: staged boundary

Status: **preprocessing extraction only; no standalone validation profile or CLI
is shipped by this change.** A successful preprocessing call does not establish
ontology consistency, shape conformance, pack admission, graph attachment or
authorization.

## First stage: shared engine code

`eg_rdf::pack` (feature `rdf`) now owns the former connector-pack server helpers:

| API | Preserved behavior |
| --- | --- |
| `validate_document` | Parse Turtle using the engine parser, then enforce 100,000 triples, existing 4 KiB term checks and 256 KiB literal values; return the existing `PackViolationCode` and static detail. |
| `validate_ontology_imports` | Imports must be named IRIs of other supplied ontology entries; refuse self-imports, external imports and non-IRI objects; never fetch. |
| `declared_shape_iris` | Discover named node/property shapes for existing duplicate-IRI warnings; malformed input gives no names and must separately fail document validation. |
| `scoped_union` | Parse files independently in caller order, scope subject/object blank nodes by file index, and render the union as N-Triples; report the first malformed file index. |

The server imports or delegates to these functions. The old private union module
is removed, so it cannot diverge from a future offline consumer. No RDF library,
parser, reasoner, dependency, feature default, wire type or package is added.
The `rdf`-disabled refusal path remains in the server.

This is intentionally a behavior-preserving extraction. It retains details such
as the subject IRI bound counting its rendered angle brackets, and only applying
the triple count after parsing. `scoped_union` is a preprocessing primitive and
does not apply byte/triple limits itself. Callers cannot treat any of these
helpers as a complete bounded validator.

## Remaining work before a CLI can be a CI gate

1. Establish the versioned profile in an EG-owned component above `eg-rdf` and
   `eg-shacl`. Move the existing `validate_rdf_unions` / `validate_shapes_union`
   orchestration out of the server and have server admission and the CLI invoke
   that same implementation. Reuse `eg_rdf::tableau::check_pack_ontology` for
   class and ABox checks and the existing SHACL/ICV evaluator; do not implement
   a second semantics stack. Runtime storage, auth and server startup are not
   prerequisites for this library call.
2. Make resource bounds enforceable before allocation/evaluation. Today
   `mapping::parse_turtle` collects all triples before checking 100,000, and the
   SHACL guard checks `shape_triples * max(ontology_triples, 1)` rather than
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
first stage.
