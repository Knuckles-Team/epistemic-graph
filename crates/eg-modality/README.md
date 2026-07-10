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
- No resolver for `EvidenceSpan` (X1) — only the shape is defined (`src/evidence.rs`).
