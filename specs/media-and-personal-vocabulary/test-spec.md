# EG-MEDIA-VOCAB — Test specification

Governing spec: [spec.md](spec.md).

| Test ID | Requirement | Level | Setup and input | Expected observation | Evidence |
|---|---|---|---|---|---|
| T-001 | FR-001 | Unit | load media-v1.ttl, content-v1.ttl with shapes | classes resolve, `:Transcript` not redefined, SHACL pass/fail fixtures behave | PENDING |
| T-002 | FR-002 | Contract | per-connector mapping fixture | every row names an existing class/property and `vg:sourceKind` mcp | PENDING |
| T-003 | FR-003 | Integration | recorded arr, qbittorrent, jellyfin responses | films downloaded but not in library, provenance per leg | PENDING |
| T-004 | FR-004 | Negative | connector fixture lacking the operation | typed abstention naming the operation and owner | PENDING |

## Negative and boundary cases

Missing leg (jellyfin unconnected): typed abstention. Raw email in Subscriber: SHACL violation. Hash case mismatch: normalised to lower case. Distinct queries returning the same URL keep distinct SearchResult identities. Two posts linking the same article remain separate. Repeated same-source records remain idempotent. Same numeric provider IDs for Film and TelevisionSeries do not merge. Ontology collision fixtures keep Team unchanged, socialScheduledAt distinct from scheduledFor, and downloadRequestState distinct from TRM requestState. Invalid timestamp/state values fail the domain-specific SHACL shapes. Tenant denial follows the engine contract.

## Quality and release proof

`cargo test -p eg-core`, repo CCCC/jscpd/dupehound/KISS gates; record revision and output link per row.

<!-- spec-definition-patch:owner-batch-20261012-media-and-personal-vocabulary -->
## Reviewed residual acceptance targets — owner batch 2026-10-12

All targets below are planned acceptance, not executed tests or receipts. Use the real producer/consumer boundary named for each row and retain existing covered behavior.

- **`EG-MEDIA-VOCAB-R002.5` / EG:50:** Link the existing SDK paper retrieval/metadata producer to EG SourceIngest; define citation-edge construction and fact assertion with source provenance, plus one unidentified-paper refusal that preserves healthy paper ingestion. Do not duplicate provider retrieval or assert unreviewed paper-serving registration exists. Producer/consumer path: SDK paper provider search/download -> PaperRecord/DocumentReceipt -> generated SourceIngest -> EG ResearchPaper/citation facts. SDK plan explicitly says paper-serving registration is not established by its specification. Source review `b6d12a184f1c5812c8f4fd56a0a299e48381e85ad9b18af4eb0ed19faf3d7e0b`. Exercise the served EG producer and named consumer where that owner contract exists; assert its defined output/refusal. These are planned cases only, not test or receipt evidence.
<!-- /spec-definition-patch -->

<!-- spec-definition-patch:final-mappings-20261012 -->
## Final mapped planned acceptance — 2026-10-12

No cases listed here have run; they describe required future acceptance, not evidence.

- **`EG-MEDIA-VOCAB-R001.100` / EG:69; source `38f71f6a205cf8df27631a4aa86ec3312a91f7902dfd8bfe843db2a142d1d30f`:** Use an authorized tagged media item and independently read back tag provenance and media revision; query with allowed and denied principals and assert only authorized results are returned. Refuse a tag with missing producer/evidence provenance and stale media revision. Keep a separate pending test for the actual vision producer once its identity and contract are resolved. Producer/consumer: EG owns tag assertion storage, provenance and authorized search. Vision extraction producer identity/execution remains held for its owner; this child does not claim that producer is implemented or served. Parent `EG-MEDIA-VOCAB-R001` remains intact.
<!-- /spec-definition-patch -->


<!-- spec-definition-patch:owner-dispatch-20261012-tests -->
## Planned served acceptance — meeting and bookmarks

No test in this section has run; each owner receipt remains pending.

- `EG-MEDIA-VOCAB-R001.101`: serve transcript corrections/finalization, independent digest readback, bounded summary-context read, AU provider invocation and citation-bound summary commit. Verify stale summaries after correction, 128-span/16384-byte/8192-token policy bounds, and refusal for missing/invalid/exceeded limit policy, unfinalized/stale transcript, unauthorized provider, mismatched invocation receipt and invalid citation.
- `EG-MEDIA-VOCAB-R001.102`: ingest through SDK SourceIngest R005.7-.12 and EG ChangeEnvelope commit; independently read back the bookmark and receipt, semantic-search under a pinned index, and read relations under the same snapshot. Verify unknown metadata, paging, 100-hit/500-relationship/1048576-byte bounds, absent/invalid/exceeded cap refusal, stale index and no denied count/edge leakage. GraphOS/WebUI separately test mindmap and CSV/JSON export.
<!-- /spec-definition-patch -->
