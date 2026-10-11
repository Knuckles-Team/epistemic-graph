# EG-SUPPLY-VOCAB — Test specification

Status: PROPOSED. Governing spec: [spec.md](spec.md).

| Test ID | Requirement | Level | Setup and input | Expected observation | Evidence |
|---|---|---|---|---|---|
| T-001 | FR-001 | Reasoner | Four-edge physical chain fixture | `suppliesService` inferred for a typed Service via serviceRunsOn. Bare container/pod runsOn does not infer it or a Service type. Nameless supplier refused | PENDING |
| T-001.1 | EG-SUPPLY-VOCAB-R001.1.1 | Native parser | Exact shared ontology file | Class and superclass triples, labels, suppliesItem domain/range, no suppliedBy redeclaration | `crates/eg-rdf/tests/supply_chain_declarations.rs` |
| T-001.2 | EG-SUPPLY-VOCAB-R001.1.2 | Catalog and reasoner | Default catalog plus supplier, item and vendor instances | Valid registered document. Vendor and domain/range entailment. No reverse Supplier inference | PENDING |
| T-002 | FR-002 | Reasoner | Service, workload, image, version fixture | `usesComponent` inferred; versionless component and bad severity refused | PENDING |
| T-003 | FR-003 | Shape | Contract fixtures: valid, no customer, no service; ontology scan | Faults refused; no `ChangeRequest` class; `affectsService` accepts `kg:Change` | PENDING |
| T-003.1 | EG-SUPPLY-VOCAB-R003.7 | Native parser | `commercial-v1.ttl` and fixtures | Test asserts both `Customer` and `Supplier` entail `Organization` and no `:Party` class exists. | `crates/eg-rdf/tests/commercial_terms.rs::party_links_reuse_organization` |
| T-003.2 | EG-SUPPLY-VOCAB-R003.8 | Native parser | `commercial-v1.ttl` and fixtures | Test lists the class; the grep finds exactly one declaration. | `crates/eg-rdf/tests/commercial_terms.rs::purchase_order_declared_once` |
| T-003.3 | EG-SUPPLY-VOCAB-R003.9 | Native parser | `commercial-v1.ttl` and fixtures | Test lists the class and property with domain and range. | `crates/eg-rdf/tests/commercial_terms.rs::purchase_invoice_terms_listed` |
| T-003.4 | EG-SUPPLY-VOCAB-R003.10 | Native parser | `commercial-v1.ttl` and fixtures | Test lists the class and property with domain and range. | `crates/eg-rdf/tests/commercial_terms.rs::opportunity_terms_listed` |
| T-003.5 | EG-SUPPLY-VOCAB-R003.11 | Native parser | `commercial-v1.ttl` and fixtures | Test lists `contactOf` with domain and range; no `Contact` class exists. | `crates/eg-rdf/tests/commercial_terms.rs::contact_reuses_person` |
| T-003.6 | EG-SUPPLY-VOCAB-R003.12 | Native parser | `commercial-v1.ttl` and fixtures | Test lists the property; a fixture with a plain string is refused. | `crates/eg-rdf/tests/commercial_terms.rs::last_activity_at_datetime` |
| T-004 | FR-004 | Shape | `HostLimit` fixtures: valid, no limit, unknown kind | Faults refused; no stored `OverLimit` | PENDING |
| T-005 | FR-005 | Shape | Capability link from a service, from a non-service, via `kg:supports` | Only the service form accepted | PENDING |
| T-006 | FR-006 | Contract | D4 table and recorded responses | Every concept has an authority row or a typed abstention | PENDING |
| T-007 | FR-007 | Integration | Blocking-key fixture and a name-only candidate | Proposal inactive until approved; join empty before | PENDING |
| T-008 | SC-001 | Parse | All new `.ttl`, `.rq` | Parse clean; query uses classes only | PENDING |

## Commercial registry and shape acceptance

Named tests below are planned, not executed evidence.

| Test ID | Requirement | Level | Setup and input | Expected observation | Evidence |
|---|---|---|---|---|---|
| T-COM-013 | EG-SUPPLY-VOCAB-R003.13 | Contract | `commercial_vocabulary::commercial_module_loads` in `crates/eg-core/tests/commercial_vocabulary.rs` | Load through CoreSpec; assert module and catalog registration. Reject a missing or duplicate registration. | PENDING |
| T-COM-014 | EG-SUPPLY-VOCAB-R003.14 | Contract | `commercial_vocabulary::commercial_shapes_load` in `crates/eg-core/tests/commercial_vocabulary.rs` | Load the registered companion and existing contract/capability shapes. Reject missing registration or overwritten existing shapes. | PENDING |
| T-COM-015 | EG-SUPPLY-VOCAB-R003.15 | Contract | `commercial_vocabulary::commercial_shared_namespace` in `crates/eg-core/tests/commercial_vocabulary.rs` | Check exact IRIs; reject duplicate incompatible declarations and accidental domain narrowing. Existing supplier and customer fixtures remain valid. | PENDING |
| T-COM-016 | EG-SUPPLY-VOCAB-R003.16 | Contract | `commercial_vocabulary::commercial_close_date_declared` in `crates/eg-core/tests/commercial_vocabulary.rs` | Assert canonical property IRI, datatype-property type and dateTime range. Reject an incompatible existing declaration before adding a shape. | PENDING |
| T-COM-017 | EG-SUPPLY-VOCAB-R003.17 | Contract | `commercial_vocabulary::commercial_opportunity_date_shape` in `crates/eg-core/tests/commercial_vocabulary.rs` | Load the registered shape. Accept a valid dated opportunity; reject missing closeDate and an invalid date datatype. | PENDING |

## Negative and boundary cases

Unapproved mapping makes a class uncovered. Unconnected procurement source gives an abstention. A name-only supplier match across erpnext and dockerhub is never joined before approval. A limit with a reading but no limit is refused.

## Quality and release proof

`cargo test -p eg-core` for the ontology and shape tests and `uvx pipelines-hook spec-status` and `spec-decomposition`; record revision and output at landing. A fixture test is not served acceptance; served acceptance is `EG-CQ-XSRC-R007`.
