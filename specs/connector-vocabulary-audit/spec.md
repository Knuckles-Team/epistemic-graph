# EG-CONN-AUDIT-001 — Connector vocabulary audit

**Owner:** epistemic-graph (EG) · **State:** proposed · Cross-repo IDs: EG-CQ-CONN-001 (corrected, not edited here)

## Outcome and boundary

`EG-CQ-CONN-001` assigned each of 66 connectors a vocabulary file from file names only. This spec opens the 19 distinct assigned files and audits the 43 connectors that received one: 4 FIT, 21 PARTIAL, 18 WRONG (the 23 MISSING connectors belong to other lanes). It records the per-connector verdict, the missing terms (126, across 16 files including one new `commerce-v1.ttl`) and the 18 reassignments as rows. It does not edit `specs/connector-competency`.

Explicit decided outcomes: home-assistant to `infrastructure-v1`, langfuse to `orchestration-v1`, ciso-assistant and onetrust to `government-v1`. See [design.md](design.md).

## Requirements and acceptance

Rows `EG-CONN-AUDIT-R001` to `R017` are rollups: one per vocabulary file that needs additions (each child adds one class or property, with a SHACL shape where a constraint is implied, plus setup rows for a shapes companion or the new file) and one `Reassignments` rollup with a child per WRONG connector. Children are 5-minute units naming the file they touch and the proving test.

## Virtual-first

Nothing here copies source data. The audit only fixes which ontology terms the competency questions bind to; answers are fetched through each connector's access contract at query time (`specs/federated-query-and-reasoning`, `specs/unified-data-plane`).

## Traceability

See [test-spec.md](test-spec.md).
