# EG-MEDIA-VOCAB — Media and personal-content vocabulary

## Dependencies and unresolved owner contracts

- `SDK-SOURCE-INGEST-R005.7-.12` is the existing envelope prerequisite for `EG-MEDIA-VOCAB-R001.102`. It reuses the EG ingestion path. `WEBUI-APPS-R001.6` owns mindmap/export presentation. The GraphOS query/export adapter ID is unlinked; this remains a coordinator hold.
- `WEBUI-APPS-R001.5` consumes `EG-MEDIA-VOCAB-R001.101`. Reviewed evidence lacks AU admission and invocation-receipt producer IDs. `AU-SEMANTIC-R030` remains pending for admission, retry accounting, and disabled-provider refusal. Producer contracts and served receipts remain required. Wrapper retries do not count as one invocation.
- The vision producer for `EG-MEDIA-VOCAB-R001.100` remains held. R002.5 defines receipt-bound paper facts. Citation edges remain held until SDK serves cited-paper IDs, target identity normalization, and paper-serving registration.

Owner: epistemic-graph. Cross-repo IDs: EG-CQ-CONN (consumer, specs/connector-competency); connector owners arr-mcp, audiobookshelf-mcp, media-downloader, owncast-agent, rom-manager, audio-transcriber, scholarx, searxng-mcp (operations).
Public provenance: gap found by specs/connector-competency (vocabulary MISSING for 15 connectors).

## Purpose and user stories

An operator asks questions that span media and personal-content systems (arr, qbittorrent, jellyfin, audiobookshelf, rom-manager, owncast, media-downloader, audio-transcriber, freshrss, scholarx, searxng, pulselink, postiz, listmonk, mattermost). The engine needs one shared vocabulary and join keys so each connector's declared access contract is fetched at query time and the answers join, without copying the sources.

## Requirements and acceptance

- FR-001: media-v1.ttl and content-v1.ttl define the classes and properties in plan.md, reusing existing core terms (R001 to R004).
- FR-002: each of the 15 connectors has a virtual-graph mapping table (operation -> class/property -> contract kind) and fixtures (R005 to R019).
- FR-003: two cross-source examples run through the federated planner (R020).
- FR-004: where a connector cannot supply the data, the question is BLOCKED, answered by a typed abstention naming the missing operation, with a cross-repo row owned by that connector (X rows).
- SC-001: no duplicate class IRI across ontology files except documented connector aliases.

## Scope and interfaces

Vocabulary, shapes, mapping fixtures and tests in epistemic-graph. Connector operations belong to the connector packages. Mechanisms are the existing decision-engine, federated-query and virtual-graph specs.

## Traceability

| Requirement | Design section | Test ID | Evidence |
|---|---|---|---|
| FR-001 | plan.md Vocabulary | T-001 | PENDING |
| FR-002 | plan.md Connector mapping tables | T-002 | PENDING |
| FR-003 | plan.md Worked examples | T-003 | PENDING |
| FR-004 | plan.md Decisions | T-004 | PENDING |

## Open questions

None; every caveat is CONFIRMED, DECIDED or BLOCKED in plan.md Decisions.

<!-- spec-definition-patch:owner-batch-20261012-media-and-personal-vocabulary -->
## Owner definition extensions — 2026-10-12

### `EG-MEDIA-VOCAB-R002.5` — EG:50

**Defined EG consumer contract.** `SourceIngest` consumes the existing SDK `PaperRecord` and matching `DocumentReceipt`. It adds no retrieval provider or parallel path. The tenant-scoped `PaperRecord` retains provider and stable identity, revision, title, authors, abstract, publication date, DOI/arXiv identifiers, and field provenance. Tenant scope comes from its `PaperQuery` and matching receipt. The receipt binds tenant, paper identity/revision, source URL, media type, byte count, SHA-256, and authorized local artifact. EG asserts `cv:ResearchPaper` and available DOI, arXiv, publication-date, and citation-count facts. Each fact carries exact source-field provenance, receipt digest, and document digest. Identity and replay follow SDK `exact_identity`. Byte-identical identity/revision/document replay is idempotent. Missing stable identity, receipt mismatch, incomplete artifact, or provenance mismatch refuses that paper before graph mutation. Healthy siblings remain ingestible from partial results.

**Exact open dependency.** SDK records and receipts define no cited-paper identifiers. The SDK specification lacks paper-serving registration. `cv:citationCount` is scalar and cannot define `cv:cites` edges. Hold citation-edge construction and full graph acceptance until SDK serves reference IDs, target identity normalization, and registration. Do not infer provider identities or edges. Preserve `EG-MEDIA-VOCAB-R002.1`, `.4`, `SDK-SOURCE-INGEST-R005.1`, `.2`, `.4`, and `EG-REPO-INGEST-R002`.

Source record SHA-256: `b6d12a184f1c5812c8f4fd56a0a299e48381e85ad9b18af4eb0ed19faf3d7e0b`. Definition only; implementation and served readback are not claimed.

<!-- /spec-definition-patch -->

<!-- spec-definition-patch:final-mappings-20261012 -->
## Final mapped contract extensions — 2026-10-12

These definitions are planned contracts; they do not claim implementation or served evidence.

### `EG-MEDIA-VOCAB-R001.100` — EG:69

Bind each tag to media item/revision, taxonomy ID/version, object/context, and confidence. Also bind evidence reference, producer ID/revision, and provenance digest. Store tags as source-backed item assertions. Do not store unqualified labels. Search returns authorized tags only. Bind matched tag IDs, media revisions, and authorization revision. Missing provenance or stale media revision refuses the assertion. Unauthorized queries reveal no protected tags. Vision producer identity and served extraction contract remain unresolved. Do not invent them.

**Producer/consumer boundary:** EG owns tag storage, provenance, and authorized search. The vision producer remains held for its owner. This child claims no served producer.

**Source record SHA-256:** `38f71f6a205cf8df27631a4aa86ec3312a91f7902dfd8bfe843db2a142d1d30f`.

**Preserved parent:** `EG-MEDIA-VOCAB-R001` remains unchanged.
<!-- /spec-definition-patch -->


<!-- spec-definition-patch:owner-dispatch-20261012-spec -->
## Cross-owner meeting transcript and bookmark contracts

`MeetingTranscriptRevisionV1` appends, corrects, deletes, and finalizes ordered immutable revisions. Existing SourceIngest/ChangeEnvelope authority persists them. The final digest seals segment order and citations. Corrections create revisions and stale prior summaries. `MeetingSummaryContextV1` returns bounded spans from a current finalized transcript. It binds admitted provider, policy revision, and context digest. AU invokes the selected provider and supplies its receipt. `CommitMeetingSummaryV1` validates that receipt and each claim citation before persistence. EG does not use generative weights or mint provider receipts.

For bookmarks, `BookmarkSemanticSearchV1` and `BookmarkRelationsV1` reuse certified SDK SourceIngest envelopes. They use the current semantic index and UQL relation path. Both bind ingestion/source/index/snapshot/authorization revisions. Unknown metadata stays explicit. Filter denied edges/counts before aggregation. GraphOS owns the served adapter. WebUI R001.5 and R001.6 own meeting/bookmark presentation. These interfaces are definitions only. No source, provider, or consumer receipt is claimed.
<!-- /spec-definition-patch -->
