# EG-CQ-XSRC-001 — Design and implementation plan

Status: PROPOSED. Governing spec: [spec.md](spec.md).

## Existing system and reuse

Found in `crates/eg-core/ontology/`: `vg:SourceConnection`, `vg:MetadataContract`, `vg:VirtualMapping` with `vg:mapsOntologyClass`, `vg:approved` (`virtual_graph-v1.ttl`, shapes in `virtual_graph-v1.shapes.ttl`); `:Service`, `:Host`, `:Node`, `:Rack`, `:CoolingUnit`, `:ContainerImage`, `:Incident`, `:dependsOn` (service to service), `:runsOn`, `:hostedBy`, `:buildsImageFor`, `:affectsEntity` (`infrastructure-v1.ttl`); `:InfrastructureHost`, `:hostsSoftware` (`company_infra-v1.ttl`); `:BusinessCapability`, `:ApplicationEvent`, `:ErpNextIssue` (`archimate-v1.ttl`); `:Vendor`, `:TechnologyProduct`, `:vulnerabilityCount` (`trm-v1.ttl`); `:Organization`, `:supports`, `:alignedWith` (`core-foundation-v1.ttl`); `:SupplyRoute` (`energy_geopolitics-v1.ttl`, unrelated sense).

MISSING: any authority term (source S is authoritative for concept C); `:Supplier`; `:SupplyChainItem`; `:SoftwareComponent`; `:ComponentVersion`; `:Vulnerability`; `:usesComponent`; `:suppliesService`; `:CustomerContract`; `:ChangeRequest`; `:coversService`; host temperature and capacity limit terms; a `:Customer` class.

Already built and cited, not rebuilt: ontology-driven source selection (`EG-FEDERATED-QUERY-R072`), operation-bound API/MCP sources (`R073`), SQL pushdown (`R046`, `R069`), R2RML virtual graphs (`EG-UNIFIED-DATA-PLANE-R005`, `R006`), the approval-gated virtual mapping vocabulary (`R037`), entity resolution proposals (`R030`), and freshness (`R038.1`).

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

New vocabulary in `virtual_graph-v1.ttl` (authority), new `supply_chain-v1.ttl` and shapes (supply chain, `R002` to `R004`), additions to `infrastructure-v1.ttl` (host limits), a commercial file for contracts, and `crates/eg-types/src/supply_chain_report.rs`. Fresh-data decision: no migration; new terms only. Versioning follows the existing `-v1` ontology files.

Linking: `:usesComponent` (service to component version), `:publishedBy`, and `:suppliesService` as the chain supplier to service. Entity resolution reuses `EG-UNIFIED-DATA-PLANE-R030`: proposals are inactive until approved, and a join never crosses an unapproved link.

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

Question, then graph-os intent tool (graph-os repo), then EG `select_sources` reading authority rows, then the federated planner over approved virtual mappings and operation-bound connectors, then report assembly. The decision engine is not duplicated: capability coverage (`EG-DECISION-ENGINE-R126`) answers which components can serve a task; this spec answers which data source is authoritative for a concept.

## Quality and release gates

`pipelines-hook spec-status --write` and `spec-decomposition`; focused cargo tests per child; CCCC and dupehound on the new Rust file; no duplicate planner. The acceptance rows run through the served surface.

## Risks, alternatives, and decisions

- Judgement: the source rankings above are proposed, not discovered; the ontology makes them changeable without code.
- Risk: connector operation contracts for erpnext, uptime and container-manager may not exist yet (cross-repo, `R002.7`, `R003.4`, `R003.5`).
- Alternative rejected: naming connectors in the query. It fails the exit gate (`R007.4`).
- Entity resolution quality for vendor names is unproven; approval gating bounds the risk.
