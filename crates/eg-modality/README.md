# eg-modality (CONCEPT:E4)

The `ModalityContract` trait + conformance harness — see `src/lib.rs` and
`src/contract.rs` module docs for the full design rationale (why 4 core + 4
default-empty methods, why `RowSetShape`/`StagedWrite` are DAG-safe dups rather than
re-exports of `eg_plan`'s real types, why `eg-modality` depends on `eg-types` only).

This file is the **retrofit plan**: the order the remaining 17 modality-shaped
crates should adopt `ModalityContract` in, and why that order.

## v1 status (this increment)

Implemented, each behind its own opt-in `contract` feature (default OFF):

- `eg-tensor::Tensor`
- `eg-geo::Geometry`

Everything else is **not yet retrofitted**. This crate only defines the seam;
adopting it in a given modality crate is that crate's own additive change (add the
optional `eg-modality` path-dep + a `contract` feature, `impl ModalityContract` +
`impl ConformanceTestable`, invoke `modality_conformance_tests!` once) — never a
breaking one, since the trait carries no required wiring into the crate's existing
public API.

## Retrofit order for the rest

1. **`eg-tsdb` / `eg-stream` next.** Both already have a staging-shaped concept
   that maps directly onto `txn_stage`/`StagedWrite`: `eg-tsdb::StagedSeries` (an
   in-txn overlay of uncommitted `(ts, field_values)` points, read-through for
   read-your-own-writes, then merged or dropped — CONCEPT:EG-KG.query.txn-tsdb-read-your)
   and `eg-stream`'s CEP window buffering. Lowest friction: the shape already
   exists, this only gives it a name other modalities share. `cdc_topic` is also the
   most natural non-`None` answer here (`eg-tsdb`/`eg-stream` are the modalities
   closest to the existing `CdcEvent`/streaming surface in `eg-types::wire`).

2. **`eg-rdf` — the reference non-trivial `provenance()`.** `owl::Justification
   { rule, axioms, premises }` plus `Classification::confidence` is the ONE modality
   crate today with real derivation history. Mapping it to `Provenance { source,
   detail, confidence }` losslessly is the proof that the default-empty
   `provenance()` hook is not just a stub — it is meant to be filled in exactly
   here. Do this AFTER tsdb/stream so the txn_stage/cdc_topic pattern is proven on
   two more crates first.

3. **`eg-epistemic` — the reference "does everything" implementation.** Once the
   contract has been exercised on 4 real, structurally different modalities
   (tensor, geo, tsdb/stream, rdf), `eg-epistemic` is where a from-scratch modality
   should implement `ModalityContract` from day one (not retrofit it) — proving the
   trait is usable forward, not just backward-compatible.

4. **The rest**, lowest-friction (pure-serde leaves, no heavy dep to gate around)
   first: `eg-ann` (a vector-index leaf, similar shape to `eg-tensor`), `eg-shacl` /
   `eg-shex` (validation surfaces sitting above `eg-rdf` — `provenance` likely
   delegates to the underlying RDF term's), `eg-text` (BM25/Tantivy — `to_rowset`'s
   score is the natural BM25 rank), `eg-lake` (Parquet/Delta — `txn_stage` maps onto
   its async materialization tier), `eg-kvcache` (a cache leaf — `cdc_topic` is
   almost certainly `None`, `analytics_ops` empty), then the remaining wire/query
   surface crates (`eg-query`, `eg-graphql`, `eg-wasm`) as/if a concrete modality
   value type emerges in each that a caller would want to stage/rowset-project
   directly (several of these are pure protocol/executor crates with no modality
   VALUE type of their own — `ModalityContract` may simply not apply to them, which
   is a legitimate outcome, not a gap).

## Non-goals of this increment

- No all-19 retrofit — see above.
- No wiring into `eg-plan`'s executor, the server dispatch, or the wire protocol.
  `ModalityContract` is a capability-discovery/testing seam a modality crate can
  adopt; nothing in the engine calls it yet.
- `EvidenceSpan` (X1) resolvers: **landed** for the modalities that have a real
  located representation — `eg-text::TextHit` (whole-document `DocumentSpan`;
  `TextHit` itself does not track offsets, see `eg-text/src/contract.rs`),
  `eg-compute::ast::symbol::Symbol` (`CodeSymbol`, exact file/line range),
  `eg-tsdb::traces::Span` (`TraceSpan`, exact trace/span id) — plus `eg-rdf::ProofNode`
  documenting WHY it stays `None` (a derived entailment has no artifact to locate INTO;
  see `eg-rdf/src/contract.rs`). `eg-plan::KnowledgeSet::from_rowset` (behind its own
  `epistemic` feature) now calls these to populate `KnowledgeRow::evidence_refs` for
  rows whose stored node shape decodes as one of these types. Image/Audio/Video
  (`EvidenceSpan::ImageRegion`/`AudioSegment`/`VideoShot`) have NO modality crate/value
  type in this workspace yet — left un-implemented rather than fabricated; add the
  resolver when a concrete image/audio/video modality value type lands.

## EG-P1-1: mandatory modality registry + first-class TCK

Codex P1 feedback: promote `ModalityContract` from an opt-in conformance macro to a
mandatory RUNTIME registry plus a genuine, PROVABLE first-class Test Compatibility Kit
(TCK) — "first-class" must mean something machine-checkable per modality, not "it
compiles against the trait".

### The registry

`register_modality` / `registered_modalities` (in `src/registry.rs`) are a process-wide
`OnceLock<Mutex<Vec<ModalityDescriptor>>>` inventory — the same zero-new-dependency
pattern already used elsewhere in this workspace (`eg-tensor::gpu`, `eg-core::index`/
`graph`, `eg-ann::distance`, `eg-plan::runtime`/`cost`, `eg-query::cypher::proc`,
`eg-compute::reasoning_closure`), rather than a new `linkme`/`inventory` proc-macro
dependency (neither is used anywhere in this workspace today, and `eg-modality` is
deliberately the thinnest possible seam every modality leaf depends on).

**Why this still counts as "mandatory", without `linkme`'s auto-discovery:**
`modality_conformance_tests!` — the macro every `ModalityContract` implementer already
invokes exactly once, unconditionally — now calls `register_modality` as part of its
OWN generated test battery. Every existing (~17) and future implementer is wired into
the registry with **zero edits** to any pilot's `contract.rs`, the moment its
`#[cfg(test)]` suite runs. See `src/registry.rs` module docs for the full tradeoff
writeup (and when to revisit `linkme` — a real always-on server process that must
enumerate every linked modality with no test/init call anywhere).

**Known limitation (documented, not hidden):** the registry is process-scoped. Each
`eg-*` crate's `cargo test` is its own OS process, so `registered_modalities()` inside
`eg-tensor`'s test binary only ever sees `"tensor"`, never `"geo"` too. A single
cross-crate inventory needs something ABOVE every modality crate (a future
`epistemic-graph`-level TCK binary that imports and exercises all of them) — this
increment makes registration itself mandatory-by-construction; a whole-fleet
aggregator is follow-up work, not blocked by anything here.

### The 12-point TCK

`TckPoint`/`TckStatus`/`TckPointResult`/`TckReport` + `tck_report::<T>()` (in
`src/tck.rs`) evaluate every `ConformanceTestable` modality against 12 points:

1. stable versioned schema/ids
2. ingest (+ streaming where applicable)
3. codec / unsupported-format behavior
4. storage + secondary-index + stats presence
5. typed query operators
6. txn participation OR declared saga/outbox contract
7. CDC / delete / tombstone / retention / GC
8. tenant / row / region policy
9. provenance + evidence-location + lineage
10. backup / restore / migrate / recover
11. single-node failure
12. interop / workload smoke

Each point resolves to `Pass` or `NotImplemented(reason)` — **never** silently
skipped or defaulted to green. Four points (2, 4, 10, 11) have **no corresponding
method on `ModalityContract` v1 at all**, so `tck_report` honestly reports
`NotImplemented("... no hook exists yet")` for every modality on those four — closing
that gap means extending the TRAIT itself (a follow-up workstream), not faking a
result here. The other eight are computed for real from what the trait already
exposes (see `src/tck.rs` module docs for exactly how each is decided).

### Capability parity (generated)

Produced by running `cargo test -p <crate> --features contract -- --nocapture` per
crate and reading each `tck_report_is_generated_and_registered` test's printed table
(`TckReport::render_table()`). Re-run the same command to regenerate/verify; nothing
below is hand-guessed. `eg-modality`'s own in-crate self-test dogfoods the harness on
`SmokeValue` (a fixture that overrides all 8 `ModalityContract` methods) without any
`--features` flag.

| modality | crate | schema/ids | ingest | codec | storage/index/stats | typed query | txn/saga | CDC/delete/GC | tenant/policy | provenance/evidence | backup/restore | single-node | smoke | **first-class** |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `smoke` | eg-modality (self-test) | PASS | NI | PASS | NI | PASS | PASS | PASS | PASS | PASS | NI | NI | PASS | 8/12 |
| `tensor` | eg-tensor | PASS | NI | PASS | NI | PASS | PASS | NI | NI | NI | NI | NI | PASS | 5/12 |
| `geo` | eg-geo | PASS | NI | PASS | NI | PASS | PASS | NI | NI | NI | NI | NI | PASS | 5/12 |
| `rdf` | eg-rdf | PASS | NI | PASS | NI | PASS | PASS | NI | NI | **PASS** | NI | NI | PASS | 6/12 |
| `epistemic` | eg-epistemic | PASS | NI | PASS | NI | PASS | PASS | NI | **PASS** | **PASS** | NI | NI | PASS | 7/12 |

(NI = `NotImplemented`.) The trend is the point: `smoke` (a fixture built to override
every hook) tops out at 8/12 because 4 points have no trait hook to satisfy AT ALL
today; among the real modalities, `rdf`/`epistemic` — the crates with genuine
derivation history / evidence-kind bookkeeping — correctly PASS `provenance/evidence`
and (`epistemic` only) `tenant/policy`, while `tensor`/`geo` correctly do NOT (they
have nothing to report there, by the trait's own default-empty design). No modality is
first-class (12/12) yet — closing `ingest`/`storage-stats`/`backup`/`single-node`
requires extending `ModalityContract` itself, tracked as this workstream's explicit
follow-up, not silently marked done.
