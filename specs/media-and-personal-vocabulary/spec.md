# EG-MEDIA-VOCAB — Media and personal-content vocabulary

## Dependencies and unresolved owner contracts

- `SDK-SOURCE-INGEST-R005.7-.12` is the existing source-envelope prerequisite for bookmark ingestion under `EG-MEDIA-VOCAB-R001.102`; it does not create a second EG ingestion path. `WEBUI-APPS-R001.6` owns mindmap/export presentation. GraphOS query/export adapter requirement ID remains unlinked and is a coordinator hold.
- `WEBUI-APPS-R001.5` is the meeting summary UI consumer for `EG-MEDIA-VOCAB-R001.101`. AU provider admission and invocation receipt producer IDs are absent from reviewed owner evidence; reserved `AU-SEMANTIC-R030` remains pending for admission, retry-attempt accounting and disabled-provider refusal. Exact producer contracts and served receipts remain required; wrapper-default retries are not treated as one invocation.
- The vision extraction producer for `EG-MEDIA-VOCAB-R001.100` remains held. `EG-MEDIA-VOCAB-R002.5` defines EG receipt-bound paper facts; citation edges remain held until SDK defines and serves cited-paper IDs, target identity normalization and paper-serving registration.

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

**Defined EG consumer contract.** `SourceIngest` consumes the existing SDK `PaperRecord` and matching `DocumentReceipt`; it does not add a retrieval provider or parallel ingest path. The accepted tenant-scoped `PaperRecord` retains its SDK provider/stable canonical identity, revision, title, authors, abstract, publication date, DOI/arXiv identifiers and per-field provenance; tenant scope comes from its `PaperQuery` and matching `DocumentReceipt`. The receipt binds tenant, paper identity and revision, source URL, media type, byte count, SHA-256 and authorized local artifact. EG asserts `cv:ResearchPaper` and the available `cv:doi`, `cv:arxivId`, `cv:publishedAt` and `cv:citationCount` facts with the exact source field provenance plus receipt/document digest. Identity and replay follow SDK `exact_identity`; a byte-identical identity/revision/document replay is idempotent. A missing stable identity, receipt/record mismatch, incomplete artifact or provenance mismatch refuses only that paper before its graph write; healthy records in a partial search result remain ingestible.

**Exact open dependency.** SDK `PaperRecord`/`DocumentReceipt` do not define a list of cited-paper identifiers, and paper-serving registration is explicitly absent from the SDK specification. The existing `cv:citationCount` is a scalar count and cannot define `cv:cites` edges. Therefore citation-edge construction and full citation-graph acceptance remain held until the SDK owner defines and serves (1) the cited-paper ID field, (2) its target identity and normalization rule, and (3) paper-serving registration. No provider identity or edge is inferred. Preserve `EG-MEDIA-VOCAB-R002.1`, `.4`, `SDK-SOURCE-INGEST-R005.1`, `.2`, `.4`, and `EG-REPO-INGEST-R002`.

Source record SHA-256: `b6d12a184f1c5812c8f4fd56a0a299e48381e85ad9b18af4eb0ed19faf3d7e0b`. Definition only; implementation and served readback are not claimed.

<!-- /spec-definition-patch -->

<!-- spec-definition-patch:final-mappings-20261012 -->
## Final mapped contract extensions — 2026-10-12

These definitions are planned contracts; they do not claim implementation or served evidence.

### `EG-MEDIA-VOCAB-R001.100` — EG:69

Define each tag assertion with media item/revision, tag taxonomy id/version, object-or-context value, confidence, evidence span/reference, opaque producer id/revision and provenance digest. Store tags as source-backed assertions attached to the media item, not as unqualified labels. Search returns only tags visible under the caller’s media/item authorization and binds the matched tag ids, media revisions and authorization revision. Missing provenance or stale media revision refuses the assertion; unauthorized query returns no protected tag data. The actual vision producer identity and its served extraction contract remain an explicit dependency and are not invented here.

**Producer/consumer boundary:** EG owns tag assertion storage, provenance and authorized search. Vision extraction producer identity/execution remains held for its owner; this child does not claim that producer is implemented or served.

**Source record SHA-256:** `38f71f6a205cf8df27631a4aa86ec3312a91f7902dfd8bfe843db2a142d1d30f`.

**Preserved parent:** `EG-MEDIA-VOCAB-R001` remains unchanged.
<!-- /spec-definition-patch -->


<!-- spec-definition-patch:owner-dispatch-20261012-spec -->
## Cross-owner meeting transcript and bookmark contracts

`MeetingTranscriptRevisionV1` appends, corrects, deletes and finalizes immutable ordered transcript revisions under the existing SourceIngest/ChangeEnvelope durability authority. A final transcript digest seals segment order and citations; corrections create a new revision and stale prior summaries. `MeetingSummaryContextV1` returns only bounded spans from a current finalized transcript and binds an explicitly admitted local/hosted provider, policy revision and context digest. AU executes the selected provider and supplies its invocation receipt; `CommitMeetingSummaryV1` validates that receipt and every claim citation before persistence. EG does not run generative weights or mint provider receipts.

For bookmarks, `BookmarkSemanticSearchV1` and `BookmarkRelationsV1` reuse certified SDK SourceIngest envelopes, the current semantic-search index and UQL graph relation path. Both bind ingestion/source/index/snapshot/authorization revisions; metadata remains explicitly unknown, and denied edges/counts are filtered before result aggregation. GraphOS owns the served adapter; WebUI R001.5 and R001.6 own meeting and bookmark presentation. These are definition-only interfaces; no source, provider or consumer receipt is claimed.
<!-- /spec-definition-patch -->
