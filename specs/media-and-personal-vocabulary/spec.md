# EG-MEDIA-VOCAB — Media and personal-content vocabulary

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
## Reviewed owner residuals — 2026-10-12

### `EG-MEDIA-VOCAB-R002.5` — EG:50

Original objective: Research ingestion extracts paper metadata, builds the citation graph and asserts the extracted facts into the knowledge store, refusing papers without identifiable metadata.

EG-owned residual: CitationCount is not citation-edge graph construction. Explicit extracted-fact assertion through EG and per-paper refusal when no identifiable metadata can be obtained remain unbound. Canonical-ID validation is partial support, but per-provider failures do not prove per-paper failure handling.

Producer/consumer boundary: SDK paper provider search/download -> PaperRecord/DocumentReceipt -> generated SourceIngest -> EG ResearchPaper/citation facts. SDK plan explicitly says paper-serving registration is not established by its specification.

Preserve existing coverage: `EG-MEDIA-VOCAB-R002.1`, `EG-MEDIA-VOCAB-R002.4`, `SDK-SOURCE-INGEST-R005.1`, `SDK-SOURCE-INGEST-R005.2`, `SDK-SOURCE-INGEST-R005.4`, `EG-REPO-INGEST-R002`.

Owner source hash: `b6d12a184f1c5812c8f4fd56a0a299e48381e85ad9b18af4eb0ed19faf3d7e0b`. This definition adds no AU/GraphOS/SDK implementation or acceptance claim.
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
