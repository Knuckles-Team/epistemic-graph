# Canonical structural properties

Property blobs are open MessagePack objects, but structural meaning is deliberately
narrow. Writers and readers use one key per structural concept; arbitrary application
properties are not silently promoted through aliases.

| Context | Current structural field | Meaning |
|---|---|---|
| Property-graph edge | `relationship` | The edge name used by Cypher, UQL, GraphQL, RDF projection, reasoning, mining, jobs, and TMS |
| Typed `EdgeData` | `relationship` | The serialized typed-edge relationship |
| Knowledge currency node | `node_type` | `KnowledgeRow.kind` and modality dispatch |
| RDF node typing | `type` | The RDF `rdf:type` value folded onto the node for RDF/property-graph querying; this is node data, never an edge relationship |
| Cypher primary / secondary labels | `node_type`, `labels` | `MATCH` labels, node-map projection, `CREATE`/`MERGE`, label removal, and `db.labels()` |

The removed edge keys `type`, `rel_type`, `relation`, and `relationship_type` are not
accepted as relationship aliases. A payload may still contain an ordinary property
named `type`; it does not make an edge typed. Untyped edges remain valid topology, but
typed traversals do not fabricate a relationship for them, and GraphQL schema
introspection rejects an edge that needs a relationship field but has none.

For Cypher, payload fields named `type` or `label` are also not node-label aliases.
A bare `RETURN n` materializes the node property map with an authoritative virtual
`id` (the graph key) and canonical `node_type`; callers do not need a client-side
node-id-to-map projection step.

External connectors normalize source-native schema at ingress and persist
`relationship`. They do not store source aliases alongside the canonical field. This
keeps every query surface and reasoning subsystem on the same physical representation.

`scripts/check_canonical_property_schema.py` enforces this contract in pre-commit and
Rust CI. The gate is intentionally scoped to structural readers and writers, so it does
not ban legitimate domain payloads named `type`.
