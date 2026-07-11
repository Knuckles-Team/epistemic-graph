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

**Cross-process caveat + the fleet aggregator.** The registry is process-scoped: each
`eg-*` crate's `cargo test` is its own OS process, so `registered_modalities()` inside
`eg-tensor`'s test binary only ever sees `"tensor"`. A single cross-crate inventory
therefore needs something ABOVE the modality crates to register them all in one
process. That is `crates/eg-modality/tests/fleet_tck.rs` — an integration test that
pulls the pilot crates in as **dev-dependencies** (with their `contract` feature),
registers each, and renders one combined parity table via `render_fleet_table`. It is
a test rather than a `src/bin/` binary because a binary links only `[dependencies]`
(where eg-modality cannot list the pilots without inverting the DAG), whereas a test
links `[dev-dependencies]`, and Cargo explicitly permits the resulting dev-dependency
cycle. `cargo build -p eg-modality` and the ~17 downstream crates are unaffected; only
`cargo test -p eg-modality` builds the pilots. Run it with:

```
cargo test -p eg-modality --test fleet_tck -- --nocapture
```

### The 12-point TCK — COMPLETE (every point has a real hook)

`TckPoint`/`TckStatus`/`TckPointResult`/`TckReport` + `tck_report::<T>()` /
`render_fleet_table` (in `src/tck.rs`) evaluate every `ConformanceTestable` modality
against 12 points, each backed by a REAL `ModalityContract` method:

| # | point | backing hook |
|---|---|---|
| 1 | stable versioned schema/ids | `storage_kind` + `to_rowset` |
| 2 | ingest (+ streaming where applicable) | `ingest_report` *(EG-P1-1)* |
| 3 | codec / unsupported-format behavior | `txn_stage` + `decode_staged` |
| 4 | storage + secondary-index + stats presence | `storage_stats` *(EG-P1-1)* |
| 5 | typed query operators | `analytics_ops` |
| 6 | txn participation OR declared saga/outbox | `txn_stage` / `rollback` |
| 7 | CDC / delete / tombstone / retention / GC | `cdc_topic` |
| 8 | tenant / row / region policy | `policy_labels` |
| 9 | provenance + evidence-location + lineage | `provenance` / `evidence` |
| 10 | backup / restore / migrate / recover | `backup_selfcheck` *(EG-P1-1)* |
| 11 | single-node failure / recovery | `recovery_selfcheck` *(EG-P1-1)* |
| 12 | interop / workload smoke | base round-trip |

There are **no structurally-unmeasurable points left** — the four that once had no
trait method (2, 4, 10, 11) now map to the four EG-P1-1 hooks (`ingest_report`,
`storage_stats`, `backup_selfcheck`, `recovery_selfcheck`), each with a default that
returns "unsupported" so the ~17 existing implementers keep compiling untouched.

Each point resolves to one of three HONEST statuses, never a silent green:

- **`Pass`** — a real check succeeded.
- **`N/A`** (`NotApplicable(reason)`) — the modality declared, via
  `tck_not_applicable`, that the point genuinely does not apply to its nature, WITH a
  concrete reason. Counts toward first-class (it is not a gap). Allowed **only** with a
  real reason — the reason string is the accountability record.
- **`NOT_IMPLEMENTED`** (`NotImplemented(reason)`) — a real hook exists but this
  modality has not wired it (the default), or a wired self-check FAILED. The only
  status that is an outstanding gap.

`is_first_class()` == no `NOT_IMPLEMENTED` remains (every point Pass or N/A).

### Capability parity (generated)

Regenerate the fleet table with `cargo test -p eg-modality --test fleet_tck --
--nocapture`; per-crate tables come from `cargo test -p <crate> --features contract --
--nocapture` (each pilot's `tck_report_is_generated_and_registered` test prints its
`TckReport::render_table()`). Nothing below is hand-guessed. `eg-modality`'s own
in-crate self-test proves 12/12 is reachable with NO dev-deps: `SmokeValue` overrides
every hook (incl. the four EG-P1-1 ones) and its
`smoke_value_is_fully_first_class_12_of_12` test asserts a fully-green report.

| modality | crate | schema | ingest | codec | storage | query | txn | cdc | policy | prov | backup | recover | smoke | **first-class** |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| `smoke` | eg-modality (self-test) | PASS | PASS | PASS | PASS | PASS | PASS | PASS | PASS | PASS | PASS | PASS | PASS | **12/12 ✓** |
| `tensor` | eg-tensor | PASS | PASS | PASS | PASS | PASS | PASS | N/A | N/A | N/A | PASS | PASS | PASS | **12/12 ✓** |
| `geo` | eg-geo | PASS | PASS | PASS | PASS | PASS | PASS | N/A | N/A | N/A | PASS | PASS | PASS | **12/12 ✓** |
| `rdf` | eg-rdf | PASS | NI | PASS | NI | PASS | PASS | NI | NI | **PASS** | NI | NI | PASS | 6/12 |
| `epistemic` | eg-epistemic | PASS | NI | PASS | NI | PASS | PASS | NI | **PASS** | **PASS** | NI | NI | PASS | 7/12 |

(NI = `NOT_IMPLEMENTED`.) The picture is now honest AND complete:

- **`tensor`/`geo` are first-class at 12/12** — 9 real `PASS` (including the four
  EG-P1-1 hooks, each a genuine round-trip through the modality's own durable codec:
  tensor's byte-blob CAS form, geo's lossless WKB) plus 3 `N/A` for the points a bare
  numeric array / spatial literal genuinely does not own (CDC/delete/GC — a store-layer
  concern for an immutable value; tenant/policy — enforced at `eg-core::isolation`;
  provenance/lineage — recorded by the producing plan operator, not the value). `geo`
  additionally reports `has_secondary_index: true` (its R-tree) where `tensor` honestly
  reports `false`.
- **`rdf`/`epistemic` are NOT yet first-class** — they have not (yet) overridden the
  four EG-P1-1 hooks, so those points honestly read `NOT_IMPLEMENTED`, NOT a fake pass.
  They correctly `PASS` `provenance` (both) and `policy` (`epistemic`), which
  tensor/geo declare N/A — proving the differentiation is real. Bringing them to 12/12
  is a matter of implementing the same four hooks over their own durable forms (a
  straightforward per-crate follow-up, no trait or TCK change required — the mechanism
  is now complete and proven on two pilots).
