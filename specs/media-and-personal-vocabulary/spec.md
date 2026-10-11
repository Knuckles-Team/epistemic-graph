# EG-MEDIA-VOCAB — Media and personal-content vocabulary

Status: PROPOSED. Owner: epistemic-graph. Cross-repo IDs: EG-CQ-CONN (consumer, specs/connector-competency); connector owners arr-mcp, audiobookshelf-mcp, media-downloader, owncast-agent, rom-manager, audio-transcriber, scholarx, searxng-mcp (operations).
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
