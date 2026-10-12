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
## Planned R002.5 owner acceptance — 2026-10-12

All targets below are planned acceptance, not completed tests or receipts. Use each named producer/consumer boundary. Retain existing covered behavior.

- **`EG-MEDIA-VOCAB-R002.5` / EG:50:** Planned only. Ingest SDK `PaperRecord`/`DocumentReceipt` through EG `Method::SourceIngest`. Read back `cv:ResearchPaper`, available DOI/arXiv/publication/citation-count facts, receipt/document digest, and field provenance. Exact replay is idempotent. Refuse unidentified papers, receipt revision/digest mismatch, incomplete authorized artifacts, and provenance mismatch before mutation. Preserve a healthy sibling in a mixed result. A citation-count-only record emits no edge. SDK reference IDs, identity normalization, and paper-serving registration remain missing. Hold citation-edge acceptance until that SDK contract is reviewed. Preserve R002.1/.4, SDK R005.1/.2/.4, and EG-REPO-INGEST-R002. Source SHA-256 `b6d12a184f1c5812c8f4fd56a0a299e48381e85ad9b18af4eb0ed19faf3d7e0b`. No implementation/test/readback evidence is claimed.
<!-- /spec-definition-patch -->

<!-- spec-definition-patch:final-mappings-20261012 -->
## Final mapped planned acceptance — 2026-10-12

No cases listed here have run; they describe required future acceptance, not evidence.

- **`EG-MEDIA-VOCAB-R001.100` / EG:69; source `38f71f6a205cf8df27631a4aa86ec3312a91f7902dfd8bfe843db2a142d1d30f`:** Read back tag provenance and media revision from an authorized item. Query as allowed and denied principals. Assert only authorized results appear. Refuse missing producer/evidence provenance and stale revisions. Keep vision-producer testing pending until its identity and contract are resolved. EG owns tag storage, provenance, and authorized search. Vision extraction remains held. This child claims no served producer. Parent `EG-MEDIA-VOCAB-R001` remains intact.
<!-- /spec-definition-patch -->


<!-- spec-definition-patch:owner-dispatch-20261012-tests -->
## Planned served acceptance — meeting and bookmarks

No test in this section has run; each owner receipt remains pending.

- `EG-MEDIA-VOCAB-R001.101`: Serve transcript corrections and finalization. Independently read back the digest. Read bounded summary context and commit a citation-bound summary with AU invocation receipt. Corrections stale prior summaries. Test caps of 128 spans, 16384 bytes, and 8192 tokens. Refuse absent/invalid/exceeded caps, stale/unfinalized transcript, unauthorized provider, mismatched receipt, and invalid citations.
- `EG-MEDIA-VOCAB-R001.102`: Ingest through SDK SourceIngest R005.7-.12 and EG ChangeEnvelope commit. Read back bookmark and receipt. Search under a pinned index and read relations under that snapshot. Test unknown metadata and paging. Test caps of 100 hits, 500 relations, and 1048576 bytes. Refuse invalid caps, stale indexes, and denied count/edge leakage. GraphOS/WebUI separately test served mindmap and CSV/JSON export.
<!-- /spec-definition-patch -->
