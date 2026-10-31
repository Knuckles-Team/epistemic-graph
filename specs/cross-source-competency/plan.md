# MEDULLA-CQ-XSRC — Design and implementation plan

Governing spec: [spec.md](spec.md).

## Existing system and reuse

Found in `crates/medulla-core/ontology/`: `vg:SourceConnection`, `vg:MetadataContract`, `vg:VirtualMapping` with `vg:mapsOntologyClass`, `vg:approved` (`virtual_graph-v1.ttl`, shapes in `virtual_graph-v1.shapes.ttl`); `:Service`, `:Host`, `:Node`, `:Rack`, `:CoolingUnit`, `:ContainerImage`, `:Incident`, `:dependsOn` (service to service), `:runsOn`, `:hostedBy`, `:buildsImageFor`, `:affectsEntity` (`infrastructure-v1.ttl`); `:InfrastructureHost`, `:hostsSoftware` (`company_infra-v1.ttl`); `:BusinessCapability`, `:ApplicationEvent`, `:ErpNextIssue` (`archimate-v1.ttl`); `:Vendor`, `:TechnologyProduct`, `:vulnerabilityCount` (`trm-v1.ttl`); `:Organization`, `:supports`, `:alignedWith` (`core-foundation-v1.ttl`); `:SupplyRoute` (`energy_geopolitics-v1.ttl`, unrelated sense).

MISSING: any authority term (source S is authoritative for concept C); `:Supplier`; `:SupplyChainItem`; `:SoftwareComponent`; `:ComponentVersion`; `:Vulnerability`; `:usesComponent`; `:suppliesService`; host temperature and capacity limit terms. The commercial vocabulary supplies `:Customer`, `:CustomerContract`, and `:coversService`; their cross-source authority and source mappings remain child work. The existing `:Change` is reused; no commercial `:ChangeRequest` is added.

Already built and cited, not rebuilt: ontology-driven source selection (`MEDULLA-FEDERATED-QUERY-R072`), operation-bound API/MCP sources (`R073`), SQL pushdown (`R046`, `R069`), R2RML virtual graphs (`MEDULLA-UNIFIED-DATA-PLANE-R005`, `R006`), the approval-gated virtual mapping vocabulary (`R037`), entity resolution proposals (`R030`), and freshness (`R038.1`).

## Architecture

The authority declaration is data in the graph. `select_sources` reads it, ranks mappings, and the federated planner executes only the chosen mappings. The report is assembled from the rows and the premises. Nothing in the query text names a connector.

Authority declaration: a `vg:SourceAuthority` links a `vg:SourceConnection` to an ontology class by `vg:authoritativeSource` and `vg:authorityFor`, with `vg:authorityRank` and an optional `vg:allowFallback`. Subclass entailment lets a superclass declaration serve its subclasses. It is effective only while its mapping is approved.

Concepts and sources (judgement where noted):

| Concept | Rank 1 | Rank 2 | Basis |
|---|---|---|---|
| Supplier, purchase item | erpnext-mcp | leanix-mcp | transactional procurement over catalogue (judgement) |
| Third-party risk | ciso-assistant-mcp | onetrust-mcp | judgement |
| Customer account, contract | salesforce-mcp | twenty-mcp | judgement |
| Container image | dockerhub-mcp | github-mcp | registry says what an image is (judgement) |
| Vulnerability | gitlab-mcp | github-mcp | judgement |
| Service health | uptime-mcp | lgtm-mcp | judgement |
| Service identity | servicenow-mcp (CMDB) | leanix-mcp | judgement |
| Deployed workload, placement | container-manager-mcp | portainer-mcp | judgement |
| Host hardware | systems-manager-mcp | caddy-mcp / technitium-dns-mcp (ingress and names only) | judgement |
| Business capability | leanix-mcp | archimate-mcp | judgement |
| Incident, change | servicenow-mcp | erpnext-mcp | judgement |

These are seed rows, not engine behavior; an operator changes them as ontology data.

## Interfaces and data model

New vocabulary in `virtual_graph-v1.ttl` (authority), new `supply_chain-v1.ttl` and shapes (supply chain, `R002` to `R004`), additions to `infrastructure-v1.ttl` (host limits), a commercial file for contracts, and `crates/medulla-types/src/supply_chain_report.rs`. Fresh-data decision: no migration; new terms only. Versioning follows the existing `-v1` ontology files.

Linking: `:suppliesItem` (supplier to procurement item), `:usesComponent` (service to component version), `:publishedBy`, and `:suppliesService` as the chain supplier to service. Entity resolution reuses `MEDULLA-UNIFIED-DATA-PLANE-R030`: proposals are inactive until approved, and a join never crosses an unapproved link.

Federated query sketch (classes only; MISSING terms marked):

```
SELECT ?service ?supplier ?version ?vuln WHERE {
  ?service a :Service ;
           :usesComponent ?version .          # MISSING :usesComponent
  ?version :publishedBy ?supplier .           # MISSING :publishedBy
  OPTIONAL { ?vuln :affectsComponent ?version }   # MISSING
}
```

Report: `SupplyChainImpactReport { service_impacts[{service, items[{supplier, component_version, vulnerability?, impact_kind, source}]}], sources_used[{source, class, authority_premise, freshness}], abstentions[Unconnected|Uncovered|Disconnected|UnapprovedMapping] }`.

## Live integration path

Question, then Arbora intent tool (Arbora repo), then Medulla `select_sources` reading authority rows, then the federated planner over approved virtual mappings and operation-bound connectors, then report assembly. The decision engine is not duplicated: capability coverage (`MEDULLA-DECISION-ENGINE-R126`) answers which components can serve a task; this spec answers which data source is authoritative for a concept.

## Quality and release gates

`pipelines-hook spec-status --write` and `spec-decomposition`; focused cargo tests per child; CCCC and dupehound on the new Rust file; no duplicate planner. The acceptance rows run through the served surface.

## Risks, alternatives, and decisions

- Judgement: the source rankings above are proposed, not discovered; the ontology makes them changeable without code.
- Risk: connector operation contracts for erpnext, uptime and container-manager may not exist yet (cross-repo, `R002.7`, `R003.4`, `R003.5`).
- Alternative rejected: naming connectors in the query. It fails the exit gate (`R007.4`).
- Entity resolution quality for vendor names is unproven; approval gating bounds the risk.

**D3. Canonical change vocabulary: DECIDED reuse `kg:Change`, no commercial `kg:ChangeRequest`.**
`kg:Change` in `archimate-v1.ttl` is the ServiceNow change request and is a subclass of `kg:BusinessProcess`. The `service_operations-v1.ttl` module retains its distinct `kg:ChangeRequest` subclass of `kg:Change` for IT4IT `:touches` semantics; this amendment does not remove or redefine it. `MEDULLA-SUPPLY-VOCAB-R003.6` records the amendment to `MEDULLA-CQ-XSRC-R008.3.1`. Source mappings and served cross-source acceptance remain child work.

**D5. Shared supplier/item relation: DECIDED `:suppliesItem`, no `:suppliedBy` redeclaration.**
`kg:suppliesItem` is the canonical EG relation. It links `kg:Supplier` to `kg:SupplyChainItem`. Existing `kg:suppliedBy` declarations remain source-owned, including the ERPNext purchase-order relation. This competency does not narrow or redeclare them. This amends `MEDULLA-CQ-XSRC-R002.1`; `MEDULLA-SUPPLY-VOCAB-R003.6` records the cross-spec amendment.

**D6. Service-to-capability relation: DECIDED `kg:supportsCapability`.**
Checked `kg:supports` (`core-foundation-v1.ttl`): it is the epistemic evidence-to-claim link (an Evidence node to the belief/claim it supports), not a service-to-capability relation. No archimate relation carries service to capability (`archimate-v1.ttl` declares `:BusinessCapability` with no service link). The commercial shapes already target `kg:supportsCapability` (Service → BusinessCapability), so the R008.2.3 fixture uses it and `commercial-v1.ttl` now declares it.

## Agent memory acceptance question (R009)

R009 is the acceptance question for the agent-memory substrate. It composes `MEDULLA-FEDERATED-QUERY-R720` stages with `MEDULLA-DECISION-ENGINE-R700.7` contradiction records. arbora serves the program through [`ARBORA-OPS-R018`](https://github.com/Knuckles-Team/arbora/blob/main/specs/hosted-api-operations/requirements.md). `MEDULLA-DURABLE-KERNEL-R042.2` measures its latency.
