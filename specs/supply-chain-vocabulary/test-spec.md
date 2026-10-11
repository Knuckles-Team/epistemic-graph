# EG-SUPPLY-VOCAB-001 — Test specification

Status: PROPOSED. Governing spec: [spec.md](spec.md).

| Test ID | Requirement | Level | Setup and input | Expected observation | Evidence |
|---|---|---|---|---|---|
| T-001 | FR-001 | Reasoner | Four-edge physical chain fixture | `suppliesService` inferred for a typed Service via serviceRunsOn; bare container/pod runsOn does not infer it or a Service type; nameless supplier refused | PENDING |
| T-001.1 | EG-SUPPLY-VOCAB-R001.1.1 | Native parser | Exact shared ontology file | Class and superclass triples, labels, suppliesItem domain/range, no suppliedBy redeclaration | `crates/eg-rdf/tests/supply_chain_declarations.rs` |
| T-001.2 | EG-SUPPLY-VOCAB-R001.1.2 | Catalog and reasoner | Default catalog plus supplier, item and vendor instances | Valid registered document; Vendor and domain/range entailment; no reverse Supplier inference | PENDING |
| T-002 | FR-002 | Reasoner | Service, workload, image, version fixture | `usesComponent` inferred; versionless component and bad severity refused | PENDING |
| T-003 | FR-003 | Shape | Contract fixtures: valid, no customer, no service; ontology scan | Faults refused; no `ChangeRequest` class; `affectsService` accepts `kg:Change` | PENDING |
| T-004 | FR-004 | Shape | `HostLimit` fixtures: valid, no limit, unknown kind | Faults refused; no stored `OverLimit` | PENDING |
| T-005 | FR-005 | Shape | Capability link from a service, from a non-service, via `kg:supports` | Only the service form accepted | PENDING |
| T-006 | FR-006 | Contract | D4 table and recorded responses | Every concept has an authority row or a typed abstention | PENDING |
| T-007 | FR-007 | Integration | Blocking-key fixture and a name-only candidate | Proposal inactive until approved; join empty before | PENDING |
| T-008 | SC-001 | Parse | All new `.ttl`, `.rq` | Parse clean; query uses classes only | PENDING |

## Negative and boundary cases

Unapproved mapping makes a class uncovered; unconnected procurement source gives an abstention; a name-only supplier match across erpnext and dockerhub is never joined before approval; a limit with a reading but no limit is refused.

## Quality and release proof

`cargo test -p eg-core` for the ontology and shape tests and `uvx pipelines-hook spec-status` and `spec-decomposition`; record revision and output at landing. A fixture test is not served acceptance; served acceptance is `EG-CQ-XSRC-R007`.
