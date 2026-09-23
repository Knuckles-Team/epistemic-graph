# Ontology and SHACL authority

Epistemic Graph is the sole runtime authority for OWL/RDF ontology composition,
reasoning, Datalog materialization, and SHACL validation. Agent Utilities is a
control-plane consumer. It does not load a private root ontology, resolve
`owl:imports`, run a Python reasoner, or validate with a local SHACL engine.

## Composition

The immutable EG catalog contains the platform root and domain modules. Each
component may contribute versioned semantic resources through an
`agent-connector-sdk` `ConnectorContent` provider. GraphOS captures those
resources and attaches the resulting pack through `GraphSchema.AttachPack`.
`GraphSchemaList` returns immutable and dynamic source sets plus the catalog and
composed digests for the requested graph snapshot.

Agent Utilities contributes one pack resource:

- `agent_utilities/ontology/shapes/governance.shapes.ttl`

The public provider is `agent_utilities.content.agent_utilities_content()`. It
declares package content only; it does not interpret, attach, or write semantic
data.

## Runtime contracts

- `OwlReason` reads the composed GraphSchema and returns digest-bound class and
  property entailments.
- `OwlExplain` returns digest-bound class-subsumption proof trees.
- `RunDatalogReasoning` materializes committed-schema entailments through EG's
  durable write gateway.
- `ShaclValidate` with the `shapes` field omitted validates against the composed
  GraphSchema snapshot and returns the exact composed digest used.
- An explicit non-empty `shapes` document is reserved for bounded specialist
  checks. It is still interpreted by EG and deliberately has no committed-schema
  receipt.

Governance callers fail closed when a result lacks its required schema digest,
reports an inconsistent ontology, or uses the ad-hoc validation mode.

## Adding semantic content

1. Put the ontology or shape resource in its owning component package.
2. Register the exact resource with that component's `ConnectorContent` provider.
3. Add byte, digest, RDF-semantic, and package-data tests in the owning package.
4. Provision the provider through the SDK and attach its pack through GraphOS.
5. Verify `GraphSchemaList`, reasoning receipts, and validation receipts against
   the target graph.

Do not add ontology files under `agent_utilities/knowledge_graph`, introduce a
Python fallback reasoner, or pass governance shapes inline.
