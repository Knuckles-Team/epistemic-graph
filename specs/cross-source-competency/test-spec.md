# EG-CQ-XSRC — Test specification

Governing spec: [spec.md](spec.md).

| Test ID | Requirement | Level | Setup and input | Expected observation | Evidence |
|---|---|---|---|---|---|
| T-001 | FR-001 | Shape | Authority fixtures: valid, no source, no class, no rank, equal-rank tie, unapproved mapping | Valid accepted. Each fault refused. Unapproved mapping reports uncovered | PENDING |
| T-002 | FR-002 | Contract | Load `supply_chain-v1.ttl`, seed authority rows | Terms present; `select_sources` names the declared source per concept | PENDING |
| T-003 | FR-003 | Integration | Three vendor records and three service records across sources | One node after approval, separate before. Join empty before approval | PENDING |
| T-004 | FR-004 | Integration | Class-only SPARQL fixture | Planned sources equal authority-derived set; no connector name in query | PENDING |
| T-005 | FR-005 | Unit | Recorded rows, each abstention kind | Report shape, freshness and abstentions as specified | PENDING |
| T-006 | FR-006 | End to end | Recorded responses. Then live. Then procurement absent. Then authority swap | Non-empty provenance-carrying answer. Typed abstention. Sources differ with equal query text | PENDING |
| T-007 | FR-007 | End to end | Three generalization fixtures | Each non-empty with provenance, and abstention when its source is absent | PENDING |

## Negative and boundary cases

Equal-rank authority. Authority on an unapproved mapping. Unconnected rank-1 source with and without `vg:allowFallback`. Unapproved `owl:sameAs`. A report item with no source. A copy with no watermark. A connector name in the query text.

## Quality and release proof

Focused `cargo test` for `eg-rdf`, `eg-types` and `eg-core` ontology tests; `pipelines-hook spec-status` and `spec-decomposition`. Live acceptance runs through graph-os and records revision, connected sources and output. A source test alone is not served acceptance.
