# Native source ingestion authority

`SourceIngest` is the RF-ADR-009 boundary for connector records. The request
contains raw records, source provenance, a cursor with its expected previous
value, and an exact Connector Manifest schema-mapping reference. It cannot
contain a tenant, mapping body, graph mutation, or caller-selected authority.

The server executes one governed flow:

1. Resolve the mapping from the verified tenant's current published Connector
   Manifest member row. Unknown, stale, withdrawn, headless, or ambiguous
   references fail closed before raw admission. Mapping content is persisted by
   ConnectorPack import and is never supplied by this request.
2. Capture each canonical raw record in Blob CAS and admit an idempotent
   owner-scoped holder. Content duplicates share CAS bytes without losing their
   per-source provenance binding.
3. Validate every required mapping field and deterministically lower the batch
   to `AddNode` operations. Callers cannot supply pre-mapped properties.
4. Commit the mapped operations, raw artifact references, lineage, governance,
   content version, cursor compare-and-swap, audit/outbox rows and durable
   receipt through the existing `ChangeEnvelope` authority.

The cursor comparison is part of that final graph transaction. A missing
`expected_previous_cursor` means “no cursor has ever committed”; it is not a
last-write-wins request. Concurrent or out-of-order pages therefore conflict
rather than silently advancing the watermark.

Raw admission intentionally precedes mapping and final ingest. A crash after
admission leaves recoverable content-addressed material; it never advances the
source cursor or produces a success receipt. The terminal
`SourceIngestionReceipt` is returned only after the canonical commit. Its
`receipt_digest` and committed content are stable on idempotent replay; only the
response disposition changes from `committed` to `replayed`.
