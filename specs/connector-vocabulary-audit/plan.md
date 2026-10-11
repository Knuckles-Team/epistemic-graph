# EG-CONN-AUDIT-001 — Implementation plan

Owner: epistemic-graph. This change specifies work; it delivers no ontology term,
connector operation, mapping or answered competency question.

## Canonical ownership and overlap

- [Source authority](../source-authority/spec.md) owns source selection; this
  audit consumes that contract and adds no discovery or ranking mechanism.
- [Data platform](../data-platform-vocabulary/spec.md) owns store terms and
  secret-metadata rules. R010 only describes named-graph metadata; it does not
  duplicate discovery or store access.
- [Media and personal content](../media-and-personal-vocabulary/spec.md) owns
  media/content terms and mappings. R009 owns the existing personal module's
  missing fields. A Contact is declared once in commerce; connector joins reuse
  the approved opaque identity contract, never raw email matching.
- [Supply chain](../supply-chain-vocabulary/spec.md) owns Supplier, Customer and
  supply terms. R016 owns commerce terms, reusing the shared kg namespace and
  those classes. PurchaseOrder must retain connector-compatible kg IRIs;
  never narrow existing shared suppliedBy or ownedBy domains. Party may denote
  a person or organization; do not infer all contacts are organizations.
- [Orchestration vocabulary](../orchestration-vocabulary/spec.md) owns assembly
  terms and the orchestration shapes registration (EG-ORCH-VOCAB-R008.1).
  R012.1 verifies/reuses that registration when present; it never creates a
  second shapes loader or overwrites existing constraints. R012.2-.5 add only
  trace/prompt metadata, not a second runtime or projection writer.
- [Placement](../placement-and-worker-policy/spec.md) owns worker selection
  and policy. This audit introduces no scheduling or placement behavior.

## Producer-first implementation and acceptance

1. Resolve every abbreviated term to its full IRI using its owning file's
   prefix block. Most modules share kg#; infrastructure and virtual-graph use
   distinct namespaces. R001.19 references kg:Repository from R002.2, not a new
   infrastructure Repository. Core/archimate Incident is one shared IRI.
2. Reuse existing module/shape registrations and verify them with the named load
   test. New class/property rows depend on their module setup. Shape-bearing
   rows also depend on every property/class they constrain, even when numbered
   later. Numeric order alone is not a dependency order.
3. A class row proves its declaration, not undeclared illustrative fields.
   Before a property/shape or mapping consumes a missing field, add a bounded
   producer child here with exact IRI, domain, range and a bound test. In
   particular relSource/relTarget, partyRole, orderedFrom, invoiceDate,
   closeDate, emailAddress, worksAt and completedAt require declaration or
   explicit reuse evidence. Do not silently invent these fields in fixtures.
4. Each property test checks exact RDF type, domain and range, including
   imported classes. Alternative domains use an explicit union or a compatible
   unconstrained shared property, never multiple domains that imply intersection.
   Each shape test accepts a valid fixture and rejects the stated violation.
5. R017 updates both the exact Coverage row and the two Vocabulary references
   in connector-competency, only after its producer terms pass. Parse those
   cells and resolve required IRIs. A grep count or decomposition lint alone
   cannot prove a correct reassignment.
6. Run focused ontology/load/shape tests and regenerate catalog/digests and
   delivery records. Mark only complete, tested children delivered. Specification
   text and planned test names are not implementation or validation evidence.

## External dependencies and privacy

Connector packages own payload availability, operations and source mappings.
Their live field support is unverified here. A vocabulary row can complete
without it; a live competency remains blocked until the connector owner supplies
operation/schema and contract-test evidence. Missing support returns the existing
typed abstention with the missing operation, never a fabricated answer.

Fixtures use synthetic identifiers and records. Do not commit credentials,
secret values, real emails, medical records, financial transactions, share URLs
or raw connection errors. lastConnectionError/failureReason carry redacted,
allowlisted diagnostic codes. Metadata exposure remains subject to existing
scope, provenance and identity controls. No new source access is authorized here.
