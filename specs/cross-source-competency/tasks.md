# MEDULLA-CQ-XSRC — Implementation tasks

Governing spec: [spec.md](spec.md). Design: [plan.md](plan.md). Producers first; each child is one 5-minute unit.

- [ ] Concept-to-source authority is declared in the ontology (rollup). **MEDULLA-CQ-XSRC-R001**
  - [ ] Add `vg:SourceAuthority`, `vg:authoritativeSource`, `vg:authorityFor` and `vg:authorityRank` to the virtual-graph vocabulary. **MEDULLA-CQ-XSRC-R001.1**
  - [ ] SHACL shape: a `vg:SourceAuthority` names exactly one connected source, one class and one rank. **MEDULLA-CQ-XSRC-R001.2**
  - [ ] Authority applies through subclass entailment. **MEDULLA-CQ-XSRC-R001.3**
  - [ ] Authority records the approval of its mapping. **MEDULLA-CQ-XSRC-R001.4**
- [ ] Supply-chain concepts and the sources that are authoritative for them (rollup). **MEDULLA-CQ-XSRC-R002**
  - [ ] Add `:Supplier` as a subclass of `:Vendor`, and `:SupplyChainItem` with `:suppliesItem`; preserve source-owned `:suppliedBy` declarations. **MEDULLA-CQ-XSRC-R002.1**
  - [ ] Add `:SoftwareComponent`, `:ComponentVersion` and `:publishedBy`. **MEDULLA-CQ-XSRC-R002.2**
  - [ ] Add `:Vulnerability` and `:affectsComponent`. **MEDULLA-CQ-XSRC-R002.3**
  - [ ] SHACL shape for supplier and component. **MEDULLA-CQ-XSRC-R002.4**
  - [ ] Source-authority mapping: suppliers and purchase items. **MEDULLA-CQ-XSRC-R002.5**
  - [ ] Source-authority mapping: software components and vulnerabilities. **MEDULLA-CQ-XSRC-R002.6**
  - [ ] Virtual mapping fixture: erpnext supplier table to `:Supplier`. **MEDULLA-CQ-XSRC-R002.7**
- [ ] Service concepts and the sources that are authoritative for them (rollup). **MEDULLA-CQ-XSRC-R003**
  - [ ] Source-authority mapping: `:Service` health. **MEDULLA-CQ-XSRC-R003.1**
  - [ ] Source-authority mapping: deployed workloads and images. **MEDULLA-CQ-XSRC-R003.2**
  - [ ] Source-authority mapping: business capabilities. **MEDULLA-CQ-XSRC-R003.3**
  - [ ] Virtual mapping fixture: uptime monitor to `:Service`. **MEDULLA-CQ-XSRC-R003.4**
  - [ ] Virtual mapping fixture: container-manager workload to `:Service` and `:ContainerImage`. **MEDULLA-CQ-XSRC-R003.5**
- [ ] Linking relation between a supply-chain entity and a service, with entity resolution across sources (rollup). **MEDULLA-CQ-XSRC-R004**
  - [ ] Add `:usesComponent` (service to component version) and `:suppliesService`. **MEDULLA-CQ-XSRC-R004.1**
  - [ ] SHACL shape: `:usesComponent` domain and range. **MEDULLA-CQ-XSRC-R004.2**
  - [ ] Resolve a vendor in erpnext, leanix and dockerhub to one `:Supplier`. **MEDULLA-CQ-XSRC-R004.3**
  - [ ] Resolve a service in uptime, container-manager and the CMDB to one `:Service`. **MEDULLA-CQ-XSRC-R004.4**
  - [ ] Query-time link: a join may only cross an approved `owl:sameAs`. **MEDULLA-CQ-XSRC-R004.5**
- [ ] The federated query is planned from the ontology's source declarations (rollup). **MEDULLA-CQ-XSRC-R005**
  - [ ] `select_sources` orders candidate mappings by `vg:authorityRank`. **MEDULLA-CQ-XSRC-R005.1**
  - [ ] Query fixture: the supply-chain impact query over classes only. **MEDULLA-CQ-XSRC-R005.2**
  - [ ] The plan is derived, not authored: planned sources equal the declared authorities. **MEDULLA-CQ-XSRC-R005.3**
  - [ ] An unconnected authoritative source yields a typed abstention, not a silent fallback. **MEDULLA-CQ-XSRC-R005.4**
  - [ ] `EXPLAIN` shows the authority premise for each planned source. **MEDULLA-CQ-XSRC-R005.5**
- [ ] The unified report shape (rollup). **MEDULLA-CQ-XSRC-R006**
  - [ ] Add the typed `SupplyChainImpactReport` model. **MEDULLA-CQ-XSRC-R006.1**
  - [ ] Report carries freshness per source. **MEDULLA-CQ-XSRC-R006.2**
  - [ ] Report abstention variants. **MEDULLA-CQ-XSRC-R006.3**
  - [ ] Assemble the report from the federated rows. **MEDULLA-CQ-XSRC-R006.4**
  - [ ] Serve the report through Arbora. **MEDULLA-CQ-XSRC-R006.5**
- [ ] End-to-end acceptance from real connected data (rollup). **MEDULLA-CQ-XSRC-R007**
  - [ ] Acceptance on recorded connector responses. **MEDULLA-CQ-XSRC-R007.1**
  - [ ] Acceptance against the live connected sources. **MEDULLA-CQ-XSRC-R007.2**
  - [ ] Acceptance with the procurement source not connected. **MEDULLA-CQ-XSRC-R007.3**
  - [ ] Decisive test: changing only the ontology's authority declaration changes the sources used. **MEDULLA-CQ-XSRC-R007.4**
- [ ] Generalization: three more cross-source questions of the same shape (rollup). **MEDULLA-CQ-XSRC-R008**
  - [ ] Which services are exposed by a vulnerability in an image we deploy? (rollup) **MEDULLA-CQ-XSRC-R008.1**
    - [ ] Query fixture and authority rows: vulnerable deployed image to exposed service. **MEDULLA-CQ-XSRC-R008.1.1**
    - [ ] Acceptance: exposed services listed with image, vulnerability and sources. **MEDULLA-CQ-XSRC-R008.1.2**
  - [ ] Which business capabilities depend on a host that is over its temperature or capacity limit? (rollup) **MEDULLA-CQ-XSRC-R008.2**
    - [ ] Add a host limit-breach term to the ontology. **MEDULLA-CQ-XSRC-R008.2.1**
    - [ ] SHACL shape: a limit-bearing host carries both reading and limit. **MEDULLA-CQ-XSRC-R008.2.2**
    - [ ] Authority rows and query fixture: capability to service to host over a limit. **MEDULLA-CQ-XSRC-R008.2.3**
    - [ ] Acceptance: capabilities at risk listed with the host, reading and limit. **MEDULLA-CQ-XSRC-R008.2.4**
  - [ ] Which open incidents or changes touch a service a customer contract depends on? (rollup) **MEDULLA-CQ-XSRC-R008.3**
    - [ ] Add `:CustomerContract` and `:coversService`; reuse canonical `kg:Change` (the `:BusinessProcess` change), with no commercial `:ChangeRequest`. **MEDULLA-CQ-XSRC-R008.3.1**
    - [ ] SHACL shape: a contract names a customer and at least one service. **MEDULLA-CQ-XSRC-R008.3.2**
    - [ ] Authority rows and query fixture for `:Incident` and canonical `:Change` to service to contract. **MEDULLA-CQ-XSRC-R008.3.3**
    - [ ] Acceptance: open incidents and changes listed with the service and contract. **MEDULLA-CQ-XSRC-R008.3.4**

## Agent-memory substrate claims (2026-10-22)

- [ ] **MEDULLA-CQ-XSRC-R009:** Acceptance: high-confidence agent claims against a new external report (rollup).
  - [ ] **MEDULLA-CQ-XSRC-R009.1:** Query fixture: claims that touch the report.
  - [ ] **MEDULLA-CQ-XSRC-R009.2:** Evidence clustering.
  - [ ] **MEDULLA-CQ-XSRC-R009.3:** Contradiction decay analysis.
  - [ ] **MEDULLA-CQ-XSRC-R009.4:** Served acceptance through arbora.
  - [ ] **MEDULLA-CQ-XSRC-R009.5:** Refusal.
