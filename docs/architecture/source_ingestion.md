# Native source ingestion authority

`SourceIngest` is the RF-ADR-009 boundary for connector observations. The
request contains typed records and relationships, their provenance, a provider
checkpoint with its exact expected previous value, and exact Connector Manifest
references. It cannot contain a tenant, mapping body, graph mutation, prior
durable live-set diff, or caller-selected authority.

The server executes one governed flow:

1. Resolve every record's `manifest:<connector>#schema_mappings/<key>` and every
   relationship's
   `manifest:<connector>#resources/<resource>/relations/<relation>` from the
   verified tenant's current published Connector Manifest. Unknown, stale,
   withdrawn, headless, ambiguous, mixed-revision, or caller-supplied mapping
   content fails closed.
2. Capture each canonical raw record and relationship in Blob CAS and admit an
   idempotent owner-scoped holder. Content duplicates share CAS bytes without
   losing per-observation provenance.
3. Apply the selected schema and relation projections. With `strict_schema`,
   undeclared entity fields and undeclared relationship properties are rejected.
   EG derives each relationship ID from the verified tenant, connector, stream,
   exact relation reference, and source/target identities; callers never mint a
   competing hash. Re-observing identical relationship content is set-like;
   conflicting authority or raw content fails closed.
4. Derive the source partition's new live set and any tombstones from EG's
   durable marker. Reconcile snapshots also replace the authoritative
   relationship set; relationships absent from the new snapshot, or incident
   to a withdrawn entity, are removed with durable tombstone receipts. The
   caller never computes either diff.
5. Commit mapped nodes and edges, tombstones, raw references, lineage,
   governance, checkpoint compare-and-swap, the new live-set marker, and the
   durable receipt through one `ChangeEnvelope` transaction.

The three modes have distinct semantics:

- `full` is non-authoritative for deletion when it carries observations. An
  empty full result is accepted only with `empty_authoritative_approval` plus
  the verified `source:reconcile-empty` capability; that combination explicitly
  declares an authoritative empty snapshot and tombstones EG's prior live set.
- `delta` may carry provider-declared withdrawals. EG applies only those
  explicit tombstones and updates its durable live set. An empty delta is a
  valid no-change poll: it atomically advances the checkpoint and emits a
  zero-affected receipt.
- `reconcile` carries the complete `authoritative_live_ids`. EG compares that
  set with its prior durable set and derives tombstones atomically. An empty set
  additionally requires both a non-empty `empty_authoritative_approval`
  reference and the verified `source:reconcile-empty` capability.

The checkpoint comparison is part of that final graph transaction. A missing
`expected_previous_checkpoint` means “no checkpoint has ever committed”; it is
not a last-write-wins request. Concurrent, stale, or out-of-order submissions
conflict without advancing the watermark or changing the live-set marker. A
provider `content_hash` is optional. When supplied it is bound into checkpoint
CAS and receipts exactly as supplied; EG does not invent a second source-content
digest for providers that do not expose one.

Raw admission intentionally precedes mapping and final ingest. A crash after
admission leaves recoverable content-addressed material; it never advances the
source cursor or produces a success receipt. The terminal
`SourceIngestionReceipt` is returned only after the canonical commit. Its
`receipt_digest` and committed content are stable on idempotent replay; only the
response disposition changes from `committed` to `replayed`. The receipt's
stable `receipt_id` is the same identity returned as `last_receipt_id` by status.

`SourceIngestStatus { connector, stream }` is the read-only restart and failover
authority. It reads the latest committed marker directly from durable storage
and returns the full accepted checkpoint, checkpoint/content/live-set digests,
including the separate entity and relationship live-set digests, plus the last
batch digest, receipt identity, and graph version. Connectors may cache that
projection but must not replace it with a local durable checkpoint or silently
fall back when it is unavailable.

Generated Python consumers construct the transparent direct-field
`epistemic_graph.generated.source_ingestion.SourceIngestionRequest` and call
`epistemic_graph.generated.ingestion.send_source_ingest`. Restart recovery uses
`epistemic_graph.generated.ingestion.SourceIngestStatusRequest` with the typed
`send_source_ingest_status` sender; its result is
`epistemic_graph.generated.source_ingestion.SourceIngestStatus`. The request's
`canonical_digest()` returns EG's framed 64-hex batch digest. SDKs must use these
generated models and helpers rather than maintaining parallel DTOs, digest
algorithms, or checkpoint stores.
