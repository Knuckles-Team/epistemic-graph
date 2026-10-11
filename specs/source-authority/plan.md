# EG-SRC-AUTH-001 plan (design)

## Problem and evidence

- `crates/eg-core/ontology/virtual_graph-v1.ttl` defines `vg:SourceConnection`, `vg:MetadataContract`, `vg:VirtualMapping`, `vg:PredicateFieldPair` and the properties `vg:mapsOntologyClass`, `vg:mapsEntity`, `vg:keyField`, `vg:approved`. A grep of the file and of `virtual_graph-v1.shapes.ttl` for `authority`, `rank`, `priority`, `fresh` finds nothing. There is no statement that a source is the authority for a concept.
- `vg:approved` is a plain `xsd:boolean` on a `vg:VirtualMapping`, `maxCount 1`, with no shape tying it to who set it. In Rust code outside tests nothing reads it (grep `vg:approved` over `crates`, `src`: one hit, the fixture `src/server/graph_schema/virtual_graph_tests.rs:35`). "Only an approved mapping serves reads" (`EG-UNIFIED-DATA-PLANE-R037`) is therefore a stated rule with no enforcing reader yet.
- Competency specs need it: `specs/orchestration-competency/requirements.md` rows `.3` of `R001` to `R007` say "declaring which connected source is the authority ... (`vg:mapsOntologyClass` plus the MISSING authority statement)"; `specs/cross-source-competency/plan.md` line 9 lists "any authority term" as MISSING.

## Architecture

```
question (classes only, e.g. :Incident, :Service)
   |  graph-os intent tool -> EG method (read-only)
   v
eg-rdf::source_selection::select_sources   (R072: hop search + subclass entailment)
   |  per class/facet ranks origin-approved vg:SourceAuthority declarations   <-- NEW
   |     candidate = authority row approved by origin (D3), matching class/facet/scope
   |  order: facet > class > superclass, scope, rank        (D4)
   |  then validate winner's mapping approval, caller connection, contract and freshness (D5)
   |     unavailable winner abstains; another source may answer only with explicit allowFallback
   v
SelectionVerdict per class/facet: Covered{winner, alternates} | Abstained{typed reason} | Uncovered | Disconnected
   |  each with Premise::Authority / Tiebreak / Rejected
   v
federated planner (R073 operation-bound ForeignSourceSpec, R046/R069 pushdown, residual filter kept)
   v  fetch at query time over MCP / A2A / API / GraphQL / virtual-graph
answer rows + provenance (source, record id, as-of) + "sources used and why" (from the premises)
```

| Component | Repository, module |
|---|---|
| Vocabulary and shapes | epistemic-graph, `crates/eg-core/ontology/virtual_graph-v1.ttl`, `virtual_graph-v1.shapes.ttl` (already loaded as core sources by `crates/eg-core/src/graph/schema_sources.rs` lines 809 to 817) |
| Authority-aware selection, abstention and audit types | epistemic-graph, `crates/eg-rdf/src/source_selection.rs` (extend; no new planner) |
| Approval of authority | epistemic-graph, governed-change ledger `crates/eg-types/src/governed_change.rs` plus `GraphSchema.AttachApproved` (`src/server/graph_schema/approved.rs`) |
| Registered, connected source lookup | epistemic-graph, owner-scoped foreign catalog (`EG-FEDERATED-QUERY-R013`), variants in `crates/eg-types/src/wire_query_modalities.rs` |
| Printing the audit block | graph-os (cross-repo row) |

### What R072/R073 already read (D2, CONFIRMED)

I opened `crates/eg-rdf/src/source_selection.rs` (384 lines) and the federated rows.

- `select_sources(fixture, roots, hop_budget)` returns one `CoverageResult` per class: `Covered { class, mapping_name, path, premises }`, `Uncovered`, `Disconnected`. The `Premise` enum has `Label`, `Relation`, `Subclass`, `Mapping { served_class, mapping_name }`, `SourceGeneration { source, generation }`. Nothing authority-like: no rank, scope, freshness, connection-state or approval-origin.
- The input is `CoverageFixture`, whose `mappings` field is `BTreeMap<String, (mapping name, source, generation)>` keyed by class and filled by `with_mapping` through `insert`. **Two sources mapping one class silently overwrite each other: the last insert wins.** The module doc says the live ontology and mapping catalog search is "a later child". So R072 today has no tie-breaking and no live read; the fixture is the whole implementation (`R072.1` only).
- `EG-FEDERATED-QUERY-R072` text: for each class "EG names the approved virtual mapping that serves it, or the class itself as uncovered" (singular mapping). `R073.1` added `ForeignSourceSpec::{Api, Mcp, A2a, GraphQl}` with `ForeignOperationCapabilities` (`wire_query_modalities.rs` lines 324 to 485); an unbound or ungranted spec refuses with a typed error. `R013`: foreign sources are keyed by owning tenant and actor, and resolving outside the registering principal is `ACCESS_DENIED`.
- Therefore: the foreign catalog row says **what a source can do and who may use it**; the mapping says **what it maps**; nothing says **which of several candidates should answer**. This spec adds only that.

## Vocabulary

All terms go in `crates/eg-core/ontology/virtual_graph-v1.ttl` (namespace `vg:`); shapes in `virtual_graph-v1.shapes.ttl`. Names for the first five match `EG-CQ-XSRC-R001.1`.

| Term | Kind, domain, range | Meaning |
|---|---|---|
| `vg:SourceAuthority` | class | One statement: this connected source answers this concept. |
| `vg:authoritativeSource` | object, `vg:SourceAuthority` to `vg:SourceConnection`, exactly 1 | The source. |
| `vg:authorityFor` | object, to `owl:Class`, exactly 1 | The concept (class). |
| `vg:authorityFacet` | object, to `rdf:Property`, 0..1 | When present, the row governs only this property of the class (D1). |
| `vg:authorityRank` | datatype, `xsd:positiveInteger`, exactly 1 | 1 is most authoritative. |
| `vg:authorityMapping` | object, to `vg:VirtualMapping`, exactly 1 | The approved mapping that serves the row. The row has no effect while the mapping is unapproved. |
| `vg:scopeTenant` | datatype `xsd:string`, 0..1 | Applies to this tenant only; absent means every tenant of the graph. |
| `vg:scopePartition` | datatype `xsd:string`, 0..1, form `<discoveredField>=<value>` | Applies to one partition of the source (for example `env=prod`). Pushed as a key filter only if the source declares the capability; the local residual filter always runs (R073). |
| `vg:freshnessMaxAgeSeconds` | datatype `xsd:nonNegativeInteger`, 0..1 | The answer must be no older than this. Absent means live read required. Checked against the source watermark of `EG-UNIFIED-DATA-PLANE-R038.1`. |
| `vg:authorityContract` | datatype `xsd:string`, exactly 1, in `"mcp" "a2a" "api" "graphql" "virtual-graph"` | The access contract the row's fetch uses. Must agree with the connection's `vg:sourceKind`: api to `api`, mcp to `mcp`, a2a to `a2a`, graphql to `graphql`, sql, iceberg, warehouse, sparql to `virtual-graph`. |
| `vg:materialisation` | datatype `xsd:string`, 0..1, in `"virtual" "accelerated"`, default virtual | Whether the engine may serve from a copy. |
| `vg:materialisationReason` | datatype `xsd:string`, required when `accelerated` | Why a copy is justified (for example "source rate limit"). |
| `vg:allowFallback` | datatype `xsd:boolean`, 0..1, default false | Whether the next rank may answer when this one cannot (D5). |
| `vg:foreignSourceName` | datatype `xsd:string`, `vg:SourceConnection`, 0..1 | The key of the source in the owner-scoped foreign catalog; the only link from the ontology to the connected-source registry. It carries no credential (the existing `vg:EndpointRefShape` is `sh:closed`; this property sits on the connection, not on the endpoint). |
| `vg:approved` | existing; domain widened to `vg:VirtualMapping` or `vg:SourceAuthority` | Same flag, same meaning, now also on an authority row. Its trust rule is D3. |

### SHACL (in `virtual_graph-v1.shapes.ttl`, Core plus aggregate-free `sh:sparql` with the `OPTIONAL ... FILTER(!BOUND)` form, as the file header requires)

`vg:SourceAuthorityShape`, `sh:targetClass vg:SourceAuthority`:
1. `authoritativeSource`, `authorityFor`, `authorityMapping`: `minCount 1`, `maxCount 1`, `sh:nodeKind sh:IRI`; `authoritativeSource` also `sh:class vg:SourceConnection`.
2. `authorityRank`: exactly 1, `xsd:positiveInteger`.
3. `authorityContract`: exactly 1, `sh:in` the five values.
4. `sh:sparql`: the mapping's contract belongs to the named source (`authorityMapping` then `mapsContract` then `contractSource` equals `authoritativeSource`).
5. `sh:sparql`: `vg:mapsOntologyClass` of the mapping equals `authorityFor` (an authority needs a mapping of that exact class; subclasses inherit by entailment at plan time, D4).
6. `sh:sparql`: if `authorityFacet` is present, some `PredicateFieldPair` of the mapping has `vg:predicate` equal to it. A facet row cannot govern a property the mapping does not project.
7. `sh:sparql`: `authorityContract` agrees with the connection's `sourceKind` (table above).
8. `sh:sparql`: `materialisation "accelerated"` requires `materialisationReason` and `freshnessMaxAgeSeconds` (a watermark bound; mirrors `AccelerationPolicy` refusing a missing watermark); and the connection must declare capability `copy`.
9. `sh:sparql`: no two rows share (`authorityFor`, `authorityFacet` or none, `scopeTenant` or none, `scopePartition` or none, `authorityRank`) with different sources: an equal-rank tie inside one graph is refused at attach. (Ties that only appear after composing several schema sources are caught at plan time, D4.)
10. `vg:approved`: `maxCount 1`, `xsd:boolean`.

`vg:SourceConnectionShape` gains `vg:foreignSourceName` (`maxCount 1`, string).

## Wiring

- **Read path:** `eg-rdf::source_selection` gains `AuthorityRow` (parsed from the composed graph schema sources: `GraphSchemaSources` in `crates/eg-core/src/graph/schema_sources.rs`) and a `ConnectedSources` view built from the owner-scoped foreign catalog of the calling tenant and actor. `select_sources` takes both. The fixture type stays for unit tests; the live read is the "later child" the module doc names, and it is where this spec plugs in.
- **Connected check:** a row is *connected* when its connection's `vg:foreignSourceName` resolves in the caller's catalog scope (R013) to a spec whose kind matches `vg:authorityContract` (R073 variants `Api`, `Mcp`, `A2a`, `GraphQl`; the existing SQL, Iceberg, Spark and SPARQL source kinds map to `virtual-graph`). A name that exists only under another principal counts as unconnected for this caller, with the R013 refusal kept for execution.
- **Scopes:** only existing scopes: `kg:read` style read scope of the select method; `governance:propose` for proposing. One new approver scope, `governance:approve-source-authority`, is a BLOCKED dependency below (owner: engine identity, `EG-IDENTITY-R004` generated scope registry; it cannot be hand-added).
- **Approval path:** `GOVERNED_KINDS` in `crates/eg-types/src/governed_change.rs` holds one kind. A second kind `governed.source-authority` (proposer `governance:propose`, approver `governance:approve-source-authority`, two-person rule already built in) is the approval record. `GraphSchema.AttachApproved` (`src/server/graph_schema/approved.rs`) currently renders repair shapes only (`render_repair_shapes`, error "needs a repair operation"); it gains a second candidate type "authority" whose candidate bytes are the authority Turtle and whose digest the approval lease binds (`verify_schema_approval` already binds kind, target and `candidate_digest`).
- **Pack entry:** a connector pack's ontology section is bounded Turtle (`EG-TYPED-PACKS-R033`), SHACL cross-validated (`R035`), and attaches as a `pack:<connector>` schema source (`SchemaSourceOrigin::Pack`, `schema_sources.rs` line 410). A new attach-time check refuses `vg:approved true` inside a `Pack` or `Ingestion` origin source (D3).

## Decisions

**D1. Per class or per facet: DECIDED, class-level with an optional facet override.**
Reasoning: the mapping (`vg:VirtualMapping`) is one class bound to one entity with many predicate-field pairs, and R072 selects per class. A purely class-level authority covers the common case ("ServiceNow for `:Incident`") and keeps the tie-break simple. But the competency specs need facets: `EG-CQ-XSRC-R003.1` assumes them and its `spec.md` leaves the choice open; host identity comes from a CMDB while host temperature comes from a hardware-management connector, and both describe `:Host`. A facet-only model would force one row per property for every class (the exit gate has dozens of properties). Decision: the default row has no `vg:authorityFacet` and governs the whole class; a row with `vg:authorityFacet` overrides it for that one property, and SHACL item 6 forces the facet to be projected by the mapping. Alternative rejected: facet-only (verbose, no gain); class-only (cannot express the host case, would force splitting `:Host` into artificial subclasses).

**D2. Does R072 read anything authority-like: CONFIRMED no.** Evidence in Architecture above: `crates/eg-rdf/src/source_selection.rs` `Premise` and `CoverageResult` carry no rank, scope, freshness, approval origin or connection state, and `CoverageFixture::with_mapping` overwrites on duplicate class. The foreign catalog (R013, R073) records capability and grant, which is necessary for "connected" but says nothing about preference. Everything in this spec is therefore additive to a fixture-only slice.

**D3. Who may assert authority: DECIDED, operator approval only; connectors propose.**
Rules:
1. A `vg:SourceAuthority` declaration is eligible for ranking only if its schema source origin is `Operator`, `Admin`, or `Approved` (the last needs `approval_lease_id`, `schema_sources.rs` line 415). Serving through the winning declaration additionally requires its `vg:authorityMapping` to be approved under the same origin rule. An unapproved mapping or unconnected source does not remove an approved authority declaration before ranking: D5 returns the typed refusal or permits only its explicitly declared fallback. The boolean `vg:approved` in the data is **not** trusted alone: `Pack` and `Ingestion` origin sources are refused if they contain `vg:approved true`, and the planner derives approval from the origin, not the flag. The flag stays as the human-readable marker and the shape-level mirror.
2. A connector pack (typed pack, `pack:<connector>` source) may ship classes, a `vg:SourceConnection`, a `vg:MetadataContract` and **proposed** mappings and authority rows. A proposal is inert: it appears in the "unapproved candidates" list of an answer but never selects.
3. Approval is the governed two-person change `governed.source-authority`: proposer needs `governance:propose`, approver `governance:approve-source-authority`, different identities, bound to the exact candidate digest; the result attaches through `AttachApproved`. This reuses the mechanism that already guards schema repair, not a new one.
Reasoning: authority decides which system's data the platform treats as truth and who is blamed for a wrong answer; a connector claiming authority over itself is a conflict of interest and a supply-chain risk (a malicious or buggy pack could displace a trusted source). Alternative: allow pack self-claim at the lowest rank only. Rejected: rank 1 is "lowest" lexically and the default for a concept with no other claim, so self-claim still wins by default.

**D4. Tie-breaking when two sources claim one concept: DECIDED, total order, no name-order fallback.**
For a question class `C` (and facet `p` when the question reads `p`), candidate rows are origin-approved authority declarations (D3) whose scope matches the caller and question, and whose `authorityFor` is `C` or an entailed superclass of `C` (subclass entailment of R072; the `EG-CQ-XSRC-R001.3` rule). Mapping approval, caller connection, contract compatibility and freshness are checked after ranking in D5; they must not pre-filter candidates and silently promote an alternate. Pack and ingestion proposals remain excluded from ranking. Order:
1. Facet row for `p` beats class row.
2. Row on `C` itself beats a row on the nearest superclass beats a farther superclass (the more specific declaration overrides).
3. A row with matching `scopeTenant` beats unscoped; a matching `scopePartition` beats unpartitioned.
4. Lower `authorityRank` wins.
5. Still equal and different sources: **no pick**. Verdict `Abstained::Ambiguous { rows }` listing both rows. A tie within one graph is already refused by SHACL item 9; this branch covers ties created by composing two schema sources.
Losers are kept as ordered `alternates`. Alternates never answer unless the winner's row sets `vg:allowFallback true` and the winner abstains (D5).
For comparison, R072 today picks the last inserted mapping; this order replaces that.

**D5. Unconnected authority: DECIDED, typed abstention by default, declared fallback only.**
The winner is checked in order: its mapping is approved (else `UnapprovedMapping`), connected for the caller (else `Unconnected`), contract kind matches the registered spec (else `ContractMismatch`), fresh enough (else `Stale`; a stale accelerated copy falls back to a live read if the contract allows, as `AccelerationPolicy::resolve_mode` already does, and abstains `Stale` otherwise). On any failure: if the failing row has `allowFallback true`, the next ordered alternate is tried and the premise records `FallbackFrom { row, reason }`; else the verdict is `Abstained { reason, source, concept }`. The engine never silently substitutes another source or another class (R072 already refuses the class substitution). Typed reasons: `Unconnected`, `UnapprovedMapping`, `Stale`, `ContractMismatch`, `Ambiguous`, plus the existing `Uncovered` and `Disconnected`. `EG-CQ-XSRC-R006.3` consumes these as its `abstentions` variants (it lists the first four of: Unconnected, Uncovered, Disconnected, UnapprovedMapping; this spec adds `Stale`, `ContractMismatch`, `Ambiguous`).
No authority row at all for a class: if exactly one approved mapping covers it, serve it and mark the premise `Implicit` (keeps R072 behaviour); if more than one approved mapping covers it, `Abstained::Ambiguous` (this removes the silent overwrite).

**D6. Materialisation: DECIDED, virtual default.** `vg:materialisation` absent or `virtual` means fetch at query time. `accelerated` is allowed only with a reason, a freshness bound and the source `copy` capability; execution of the copy is `EG-UNIFIED-DATA-PLANE-R038` and is not re-specified.

## Audit trail: "sources used and why"

New `Premise` variants in `source_selection.rs`: `Authority { row, source, class, facet, rank, scope, origin, generation }`, `Tiebreak { winner, loser, rule }` (rule is one of the D4 steps), `Rejected { row, reason }`, `FallbackFrom { row, reason }`, `Implicit { mapping }`. The answer's provenance block is a list built from the verdicts, one entry per class or facet:

```
sources_used: [{ concept, facet?, source, contract, rank, scope, freshness_seen, freshness_required,
                 mode: live|accelerated, why: [premises as text] }]
not_used:     [{ concept, source, reason: Unconnected|UnapprovedMapping|Stale|ContractMismatch|Ambiguous|Proposed }]
```

The type is `AuthorityAudit` in `crates/eg-types` (so it appears in the generated contract); graph-os renders it. It is computed from premises, never separately, so it cannot disagree with the selection.

## Worked example

Question: "Which open incidents touch services that a customer contract depends on?" Classes: `:Incident`, `:Service`, `:CustomerContract`.

Rows (approved): `A1`: ServiceNow, `:Incident`, rank 1, contract `api`, freshness 300 s. `A2`: PagerDuty, `:Incident`, rank 2, `allowFallback` on A1. `A3`: uptime source, `:Service`, rank 1, contract `mcp`. `A4`: ERP, `:CustomerContract`, rank 1, contract `mcp`. A pack-supplied Jira row for `:Incident`, rank 1, is **proposed** only.

Trace: R072 hop search links the three classes. `:Incident`: candidates A1 and A2 (Jira proposal excluded, listed as `Proposed`); step 4 picks A1; A2 is an alternate; A1 is connected (the catalog has `Api` spec `servicenow` for this tenant), fresh watermark 40 s old, so `Covered`. The planner emits a query that names classes only:

```sparql
SELECT ?incident ?service ?contract WHERE {
  ?incident a :Incident ; :affects ?service .
  ?contract a :CustomerContract ; :coversService ?service .
}
```

R073 fetches each class from its source at query time with the local residual filter. If the ServiceNow spec is not registered for this caller: A1 fails `Unconnected`, A1 has `allowFallback`, so A2 answers and the audit shows `FallbackFrom { A1, Unconnected }`; if A1 lacked `allowFallback` the answer carries `Abstained { Unconnected, servicenow, :Incident }` while `:Service` and `:CustomerContract` still report. Swapping A1 and A2 ranks changes `sources_used` with the query text unchanged (SC-001).

## Out of scope

Executing the fetch, pushdown and the copy pipeline (R073, R046, R069, R038); entity resolution across sources (`EG-UNIFIED-DATA-PLANE-R030`); the served intent tool and its rendering (graph-os); pack import validation rules; writing back to a source; automatic learning of authority from observed accuracy (a later proposal path through the same approval).

## Blocked dependencies

- BLOCKED: scope `governance:approve-source-authority` and kind `governed.source-authority`. Owner: epistemic-graph engine identity (`EG-IDENTITY-R004` generated scope registry, scopes are generated from capability declarations, not hand-added). Child `EG-SRC-AUTH-R003.1` declares it; `R003.3` onward wait on it.
- BLOCKED: the live ontology and mapping-catalog search of `EG-FEDERATED-QUERY-R072` (a later child, not in the repository). `R004.1` supplies the authority-aware slice against the fixture; the live read child `R004.5` is the join point and is owned by `federated-query-and-reasoning`.
