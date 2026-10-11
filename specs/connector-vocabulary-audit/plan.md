# EG-CONN-AUDIT — Implementation plan

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

## Design

### Problem and evidence

`specs/connector-competency` assigned each of 66 live connectors a vocabulary file under `crates/eg-core/ontology/` from a directory listing, without opening the files, and marked 23 as MISSING. This audit opens each of the 19 distinct assigned files once and judges the 43 assigned connectors against the two competency questions each (Questions table in `specs/connector-competency/design.md`). It does not edit that directory; it records corrections as rows here.

Verdict rule. **FIT**: the file already declares the classes and properties that carry both questions. **PARTIAL**: the file carries the central class of the question but lacks named terms. **WRONG**: the file carries neither question's central class; the table names the correct file (or MISSING).

Evidence that the file-name method misfired (all read from the files):

- `governance-core-v1.shapes.ttl` is four NodeShapes (ADR, ServiceCapability, Policy, Tool) and no class or property; the module is registered with `ontology: None` (`crates/eg-core/src/graph/schema_sources.rs`, `governance-shapes` entry).
- `catalog-v1.ttl` is a generated 40-line `owl:imports` aggregate with zero terms.
- `company-v1.ttl` is the agent-company model (`:Company`, `:WorkItem`, `:KeyPerformanceIndicator`, `:PayrollRecord`); no `Customer`, `Supplier`, `PurchaseOrder`, `Invoice`, `Opportunity` or `Contact` exists in any `.ttl`.
- `life-v1.ttl` is biology; `environment-v1.ttl` is ecology and weather; `harness-v1.ttl` models agent-harness scaffolding evolution.
- Terms are in a sibling file the name did not suggest: Home Assistant `:Device`/`:Entity` and the fan-manager `:HealthTrend` family are in `infrastructure-v1.ttl`; Paperless `:Tag`/`:Correspondent` and Gramps `:Family` are in `personal-v1.ttl`; `:PullRequest`, `:MergeRequest`, `:PipelineRun` are in `sdlc_lifecycle-v1.ttl`.
- Classes used but never declared: `:Repository` (referenced by `infrastructure-v1` `ContainerImage` and `sdlc_lifecycle-v1` `aliasOf`), `:Application` (range of `identity-v1` `:assignedApp`). No `Worktree` or `Branch` term exists.
- Two Incident IRIs exist: `core-foundation-v1` and `archimate-v1` both describe `http://knuckles.team/kg#Incident` (the latter adds the ApplicationEvent crosswalk); `infrastructure-v1` owns the distinct infrastructure namespace Incident. ServiceNow uses the shared kg IRI, never the infrastructure Incident.

### Totals

| Verdict | Connectors |
|---|---|
| FIT | 4 (container-manager, fan-manager, microsoft, uptime) |
| PARTIAL | 21 |
| WRONG | 18 |
| Total audited | 43 |

Terms to add: 125 distinct across 16 files (one new file, `commerce-v1.ttl`). Rows: 17 rollups (16 file rollups plus Reassignments) and 156 children.

### Architecture

This is a vocabulary correction. No new mechanism: answers still come from the engine's federated query over each connector's access contract (`specs/federated-query-and-reasoning`, `specs/unified-data-plane`), source authority from `native_source_connector-v1.ttl`, and virtual mappings from `virtual_graph-v1.ttl`. The audit only fixes which ontology terms the competency questions bind to.

```
competency question -> vocabulary file (this audit's table) -> classes/properties
   -> virtual mapping (vg:mapsOntologyClass) -> connector access contract (MCP/API/GraphQL)
   -> fetch at query time -> provenance-carrying answer or typed abstention
```

Owners: ontology files and `schema_sources.rs` registration are epistemic-graph (`crates/eg-core`); connector-side mappings stay with the connector package (cross-repo, not in this spec).

### Per-connector audit

| Connector | Assigned file | Verdict | Terms that carry the questions | Missing terms |
|---|---|---|---|---|
| ansible-tower | infrastructure-v1.ttl | **PARTIAL** | :Host; :Workload | `:AutomationJob`, `:AutomationJobTemplate`, `:jobStatus`, `:ranAgainstHost` |
| archimate | archimate-v1.ttl | **PARTIAL** | :BusinessProcess; :BusinessActor; :ApplicationEvent; enterprise:ArchiMateElement | `:ApplicationComponent`, `:ArchimateRelationship`, `:relationshipType` |
| archivebox | documentation-v1.ttl | **PARTIAL** | :archived; :snapshotDigest; :recordedAt; :sourceRef; core:Document | `:WebSnapshot`, `:ArchiveResult`, `:hasArchiveResult`, `:extractorName`, `:resultStatus` |
| aris | process_intelligence-v1.ttl | **PARTIAL** | :ProcessModelVersion; :ProcessEventType; :ProcessEvent; :directlyFollows | `:EpcFunction`, `:EpcRuleOperator`, `:flowsTo` |
| atlassian | sdlc_lifecycle-v1.ttl | **PARTIAL** | sdlc:Ticket; software:JiraIssue; :routedTo; :CodeChangeProposal | `:blockedBy`, `:assignedTo`, `:ticketStatus`, `:closesTicket`, `:proposalState` |
| caddy | infrastructure-v1.ttl | **PARTIAL** | :ProxyRoute; :routesTo; :ReverseProxy; :DnsRecord; :Service/:Endpoint | `:routeHostname`, `:upstreamAddress` |
| camunda | process_intelligence-v1.ttl | **PARTIAL** | :ProcessModelVersion; :ProcessEvent; :Deviation; archimate:BusinessTask; archimate:ApplicationEvent | `:ProcessInstance`, `:instanceOfProcessModel`, `:stoppedAtActivity`, `:raisedIncident` |
| ciso-assistant | governance-core-v1.shapes.ttl | **WRONG** -> **government-v1.ttl** | government-v1: :SecurityControl; :ControlFamily; :controlId; :implementsControl; core:Framework; enterprise:complianceStatus | `:RiskScenario`, `:mitigatesRisk`, `:inFramework`, `:riskAffectsService` |
| container-manager | infrastructure-v1.ttl | **FIT** | :Container; :Pod; :Deployment; :ContainerImage; :signal restart_count on :HealthTrend; sdlc:deploys | - |
| dockerhub | software-v1.ttl | **WRONG** -> **infrastructure-v1.ttl** | infrastructure-v1: :ContainerImage; :ContainerRegistry; sdlc:builtFrom | `:lastPushedAt`, `:pullCount`, `:Repository` |
| egeria | catalog-v1.ttl | **WRONG** -> **core-foundation-v1.ttl** | core-foundation-v1: :Dataset; :DataClassification; :classifiedAs; :Concept; :GovernedEntity | `:GlossaryTerm`, `:termAssignedTo` |
| emerald-exchange | finance-v1.ttl | **PARTIAL** | :VersionedOrder; :orderStatus; :filledQty; :ExchangeBackend; :executedOn; :Account; :Activity (transfer) | `:orderQty` |
| erpnext | company-v1.ttl | **WRONG** -> **commerce-v1.ttl** | none: company-v1 is the agent-company model (:Company, :WorkItem, :KeyPerformanceIndicator, :PayrollRecord); only :ErpNextIssue exists (archimate-v1) | `:Party`, `:PurchaseOrder`, `:PurchaseInvoice` |
| fan-manager | infrastructure-v1.ttl | **FIT** | :HealthTrend (:signal, :avg, :max); :HealthBaseline; :HealthAnomaly; :CoolingUnit; :Host | - |
| firefly-iii | finance-v1.ttl | **PARTIAL** | :Account; :accountKind; :baseCurrency; :Activity (position-bound) | `:Budget`, `:budgetLimit`, `:LedgerTransaction`, `:chargedToBudget`, `:transactionAmount` |
| github | software-v1.ttl | **WRONG** -> **sdlc_lifecycle-v1.ttl** | sdlc_lifecycle-v1: :PullRequest; :CodeChangeProposal; :CheckRun; :PipelineRun; :triggersPipeline; software:Commit | `:requestedReviewer`, `:closesTicket`, `:proposalState`, `:Repository` |
| gitlab | software-v1.ttl | **WRONG** -> **sdlc_lifecycle-v1.ttl** | sdlc_lifecycle-v1: :MergeRequest; :PipelineRun; :PipelineRunEvent; :aliasOf; software:Commit | `:PipelineJob`, `:pipelineJobOf`, `:Branch`, `:branchOf`, `:headCommit`, `:isDefaultBranch` |
| gramps | life-v1.ttl | **WRONG** -> **personal-v1.ttl** | personal-v1: :Family (documented as Gramps Web); core:Person; core:hasParent; :attachedFile | `:birthPlace`, `:birthYear` |
| home-assistant | environment-v1.ttl | **WRONG** -> **infrastructure-v1.ttl** | infrastructure-v1: :Device; :Entity (documented as Home Assistant Entity); :HealthBaseline; :HealthAnomaly; :Endpoint | `:entityState`, `:unavailableSince` |
| jena | virtual_graph-v1.ttl | **PARTIAL** | :SourceConnection; :MetadataContract; :discoveredEntity; :mapsOntologyClass | `:NamedGraph`, `:tripleCount`, `:lastModifiedAt` |
| keycloak | identity-v1.ttl | **PARTIAL** | :IdentityUser; :IdentityRole; :hasRole; :IdentityGroup; :memberOfGroup; :AuthenticationEvent | `:Application`, `:OidcClient`, `:roleGrantsAccessTo` |
| langfuse | harness-v1.ttl | **WRONG** -> **orchestration-v1.ttl** | orchestration-v1: :Prompt; :Model; :RunTrace; :RunEvent; :usesModel; :hasPrompt; core:OutcomeEvaluation | `:promptVersion`, `:tokenCost`, `:tracePrompt`, `:evaluationScore` |
| leanix | enterprise-v1.ttl | **PARTIAL** | :EAFactSheet; :externalToolId; :complianceStatus; :ArchiMateElement | `:factSheetType`, `:lifecyclePhase`, `:activeUserCount` |
| legal-peripherals | government-v1.ttl | **WRONG** -> **core-foundation-v1.ttl** | core-foundation-v1: :Agreement; :LegalEntity; :Regulation; enterprise:effectiveDate | `:counterparty`, `:agreementExpiresAt`, `:hasRenewalAction` |
| lgtm | infrastructure-v1.ttl | **PARTIAL** | :ObservabilityStack; :MetricsExporter; :HealthAnomaly; :Service | `:Alert`, `:alertSeverity`, `:alertState`, `:firingSince`, `:alertsOn` |
| mealie | nutrition-v1.ttl | **PARTIAL** | :Food; :FoodTerm; :NutrientAmount; :consumes | `:Recipe`, `:usesIngredient`, `:ShoppingListItem`, `:MealPlanEntry` |
| microsoft | calendar-v1.ttl | **FIT** | :CalendarEvent; :scheduledStart; :scheduledEnd; :attendedBy; identity:IdentityUser | - |
| nextcloud | documentation-v1.ttl | **WRONG** -> **personal-v1.ttl** | personal-v1: :attachedFile; core:Document; core:Person | `:FileShare`, `:sharesFile`, `:isPublicLink`, `:shareExpiresAt` |
| okta | identity-v1.ttl | **PARTIAL** | :IdentityUser; :IdentityGroup; :memberOfGroup; :hasRole; :assignedApp | `:Application`, `:accountStatus` |
| onetrust | governance-core-v1.shapes.ttl | **WRONG** -> **government-v1.ttl** | government-v1 (+ trm-v1): :SecurityControl; trm:Vendor; enterprise:complianceStatus | `:PrivacyAssessment`, `:ProcessingActivity`, `:assessmentCovers`, `:assessmentDueDate`, `:assessedVendor`, `:assessmentStatus` |
| paperless-ngx | documentation-v1.ttl | **WRONG** -> **personal-v1.ttl** | personal-v1: :Correspondent; :Tag (both documented as Paperless-ngx); core:Document | `:documentType`, `:dueDate`, `:totalAmount` |
| plane | sdlc_lifecycle-v1.ttl | **PARTIAL** | sdlc:Ticket; :routedTo; :triggers | `:IterationCycle`, `:inCycle`, `:WorkModule`, `:inModule`, `:ticketDueDate`, `:ticketStatus` |
| portainer | infrastructure-v1.ttl | **PARTIAL** | :ContainerStack; :Workload; :Service; sdlc:aliasOf; sdlc:builtFrom | `:desiredReplicas`, `:runningReplicas`, `:stackSourceRepository`, `:ManagementEnvironment` |
| repository-manager | software-v1.ttl | **WRONG** -> **sdlc_lifecycle-v1.ttl** | sdlc_lifecycle-v1 (partial): :CodeChangeProposal; :aliasOf; software:Commit. No Repository, Branch or Worktree class exists anywhere | `:Worktree`, `:worktreeState`, `:tracksBranch`, `:proposalBranch`, `:Repository`, `:Branch`, `:proposalState` |
| salesforce | company-v1.ttl | **WRONG** -> **commerce-v1.ttl** | none (see erpnext) | `:Opportunity`, `:Contact`, `:Party` |
| servicenow | enterprise-v1.ttl | **WRONG** -> **archimate-v1.ttl** | archimate-v1: :Incident (rdfs:subClassOf :ApplicationEvent); :Change (rdfs:subClassOf :BusinessProcess); :BusinessProcess | `:ConfigurationItem`, `:affectsConfigurationItem`, `:incidentState`, `:incidentAssignee` |
| stirlingpdf | documentation-v1.ttl | **WRONG** -> **action-v1.ttl** | action-v1: :ActionInvocation; :actsOnObject; :invokes; :OntologyAction (producesEffect external) | `:failureReason` |
| systems-manager | infrastructure-v1.ttl | **PARTIAL** | :Host; :OperatingSystem (documented to emit update signals) | `:pendingUpdateCount`, `:uptimeSeconds` |
| technitium-dns | infrastructure-v1.ttl | **PARTIAL** | :DnsZone; :DnsRecord; :DNSService; :NetworkInterface; :resolvesDNSFor | `:recordType`, `:recordValue` |
| tunnel-manager | infrastructure-v1.ttl | **PARTIAL** | :Host; :Tunnel; :connectsVia | `:lastConnectionStatus`, `:lastConnectionError` |
| twenty | company-v1.ttl | **WRONG** -> **commerce-v1.ttl** | none (see erpnext) | `:Contact`, `:Opportunity`, `:Party`, `:lastActivityAt` |
| uptime | infrastructure-v1.ttl | **FIT** | :Service; :Endpoint (up/down, latency, status code); :HealthTrend; :monitors | - |
| wger | nutrition-v1.ttl | **PARTIAL** | :NutritionalRequirement; :NutrientReferenceIntake; :Food | `:WorkoutRoutine`, `:Exercise`, `:includesExercise`, `:WorkoutSession` |

### Vocabulary

Terms to add, by file. Each is one child row (a 5-minute unit: one class or property, plus its SHACL shape where the constraint is implied). Prefixed names use the file's default prefix `:`; domains, ranges and shapes are in the child rows of `requirements.md`.

| File | Terms | Names |
|---|---|---|
| `infrastructure-v1.ttl` | 25 | `:AutomationJob`, `:AutomationJobTemplate`, `:jobStatus`, `:ranAgainstHost`, `:routeHostname`, `:upstreamAddress`, `:Alert`, `:alertSeverity`, `:alertState`, `:firingSince`, `:alertsOn`, `:lastPushedAt`, `:pullCount`, `:entityState`, `:unavailableSince`, `:desiredReplicas`, `:runningReplicas`, `:stackSourceRepository`, `:ManagementEnvironment`, `:pendingUpdateCount`, `:uptimeSeconds`, `:recordType`, `:recordValue`, `:lastConnectionStatus`, `:lastConnectionError` |
| `sdlc_lifecycle-v1.ttl` | 22 | `:Repository`, `:Branch`, `:branchOf`, `:headCommit`, `:isDefaultBranch`, `:blockedBy`, `:assignedTo`, `:ticketStatus`, `:closesTicket`, `:proposalState`, `:requestedReviewer`, `:PipelineJob`, `:pipelineJobOf`, `:IterationCycle`, `:inCycle`, `:WorkModule`, `:inModule`, `:ticketDueDate`, `:Worktree`, `:worktreeState`, `:tracksBranch`, `:proposalBranch` |
| `archimate-v1.ttl` | 7 | `:ApplicationComponent`, `:ArchimateRelationship`, `:relationshipType`, `:ConfigurationItem`, `:affectsConfigurationItem`, `:incidentState`, `:incidentAssignee` |
| `documentation-v1.ttl` | 5 | `:WebSnapshot`, `:ArchiveResult`, `:hasArchiveResult`, `:extractorName`, `:resultStatus` |
| `process_intelligence-v1.ttl` | 7 | `:EpcFunction`, `:EpcRuleOperator`, `:flowsTo`, `:ProcessInstance`, `:instanceOfProcessModel`, `:stoppedAtActivity`, `:raisedIncident` |
| `government-v1.ttl` | 10 | `:RiskScenario`, `:mitigatesRisk`, `:inFramework`, `:riskAffectsService`, `:PrivacyAssessment`, `:ProcessingActivity`, `:assessmentCovers`, `:assessmentDueDate`, `:assessedVendor`, `:assessmentStatus` |
| `core-foundation-v1.ttl` | 5 | `:GlossaryTerm`, `:termAssignedTo`, `:counterparty`, `:agreementExpiresAt`, `:hasRenewalAction` |
| `finance-v1.ttl` | 6 | `:orderQty`, `:Budget`, `:budgetLimit`, `:LedgerTransaction`, `:chargedToBudget`, `:transactionAmount` |
| `personal-v1.ttl` | 9 | `:birthPlace`, `:birthYear`, `:documentType`, `:dueDate`, `:totalAmount`, `:FileShare`, `:sharesFile`, `:isPublicLink`, `:shareExpiresAt` |
| `virtual_graph-v1.ttl` | 3 | `:NamedGraph`, `:tripleCount`, `:lastModifiedAt` |
| `identity-v1.ttl` | 4 | `:Application`, `:OidcClient`, `:roleGrantsAccessTo`, `:accountStatus` |
| `orchestration-v1.ttl` | 4 | `:promptVersion`, `:tokenCost`, `:tracePrompt`, `:evaluationScore` |
| `enterprise-v1.ttl` | 3 | `:factSheetType`, `:lifecyclePhase`, `:activeUserCount` |
| `nutrition-v1.ttl` | 8 | `:Recipe`, `:usesIngredient`, `:ShoppingListItem`, `:MealPlanEntry`, `:WorkoutRoutine`, `:Exercise`, `:includesExercise`, `:WorkoutSession` |
| `action-v1.ttl` | 1 | `:failureReason` |
| `commerce-v1.ttl` | 6 | `:Party`, `:PurchaseOrder`, `:PurchaseInvoice`, `:Opportunity`, `:Contact`, `:lastActivityAt` |

Shapes: only `finance-v1` has a `.shapes.ttl` companion today; the other files are registered with `shapes: None`. Each file rollup with a constrained term has a first child that creates the companion and registers it, so shape children have a home.

### Wiring

- Ontology files: `crates/eg-core/ontology/<file>.ttl`; registration: `CoreSpec` entries in `crates/eg-core/src/graph/schema_sources.rs` (precedent `governance-shapes`, `personal`, `government`).
- `catalog-v1.ttl` is generated; it is never hand-edited. The new `commerce` module appears in it by regeneration.
- Connector mappings to the new terms are `vg:VirtualMapping` rows (`virtual_graph-v1.ttl`: `mapsOntologyClass`, `mapsEntity`, `hasPredicateFieldPair`); they belong to the connector lanes, not here.
- Reassignment rows change the Coverage table and `Vocabulary:` text in `specs/connector-competency` after their producer terms land, with exact row-level assertions in both documents.
- Scopes: none added; reads use the scopes the engine contract already lists.

### Decisions

The four named cases, each DECIDED.

**home-assistant: DECIDED, reassign to `infrastructure-v1.ttl`.** CONFIRMED by reading the files: `infrastructure-v1` declares `:Device` ("A physical/logical Home-Assistant device") and `:Entity` ("A Home-Assistant entity ... part_of a :Device"), plus `:HealthBaseline`/`:HealthAnomaly` (normal range and out-of-range readings) and `:Endpoint` (up/down). `environment-v1` is ENVO/weather ontology (`:Habitat`, `:WeatherObservation` from stations and models) and has nothing for a home sensor. Missing and added: `:entityState`, `:unavailableSince` (the cross-source question needs "went unavailable at the same time as a monitor failure"). Alternative rejected: keep `environment-v1` and add sensor classes there; it would put device telemetry into a module whose terms are ENVO individuals and duplicate `infrastructure-v1`.

**langfuse: DECIDED, reassign to `orchestration-v1.ttl`.** `harness-v1` classes are `:Harness`, `:Hook`, `:Plugin`, `:HarnessVariant`, `:HarnessEdit`: scaffolding for evolving an agent. Langfuse holds LLM traces, token cost, prompt versions and eval scores. `orchestration-v1` already has `:Prompt`, `:Model`, `:RunTrace`, `:RunEvent`, `:usesModel`, `:hasPrompt`, and core has `:OutcomeEvaluation`. Added: `:promptVersion`, `:tokenCost`, `:tracePrompt`, `:evaluationScore`. Alternative rejected: harness-v1 (no run, cost or prompt-version term; adding them would blur harness configuration with run telemetry).

**ciso-assistant: DECIDED, reassign to `government-v1.ttl`.** The assigned file has no class or property (see Problem). `government-v1` has `:SecurityControl`, `:ControlFamily`, `:controlId`, `:implementsControl`, `:belongsToFamily`; core has `:Framework` and `:Regulation`; `enterprise-v1` has `:complianceStatus` (compliant, non-compliant, waived, pending-review) for the first question. Added: `:RiskScenario`, `:mitigatesRisk`, `:inFramework`, `:riskAffectsService` (links a risk to an infrastructure `:Service`, the second question). Caveat: the existing terms are NIST 800-53 flavoured; `:controlId` and `:SecurityControl` are used generically for ISO/CIS frameworks too, and `:inFramework` distinguishes them. Alternative rejected: a new `governance-core-v1.ttl` base next to the shapes file; it would duplicate `government-v1`/`trm-v1` and the shapes file's targets (ADR, Capability, Policy, Tool) are unrelated to risk and control.

**onetrust: DECIDED, reassign to `government-v1.ttl`.** Same reason for the shapes file. Privacy assessments and processing activities have no term anywhere (`trm-v1:Assessment` is a comparative technology scoring run and is the wrong semantics; `medical-v1:ConsentRecord` is clinical). Added in `government-v1`: `:PrivacyAssessment`, `:ProcessingActivity`, `:assessmentCovers`, `:assessmentDueDate`, `:assessmentStatus`, `:assessedVendor` (range `trm-v1:Vendor`, which already `rdfs:subClassOf :Organization` and aligns to LeanIX `Provider`). Overdue is derived (`assessmentDueDate` earlier than as-of and status not approved), not stored. Alternative rejected: reuse `trm-v1:Assessment`.

Other decisions:

- **erpnext, salesforce, twenty: DECIDED, MISSING, one new file `commerce-v1.ttl`.** No ERP/CRM vocabulary exists. One file for the three (party, purchase order, invoice, opportunity, contact) rather than three, because their cross-source questions join on the same party. `Contact` must be declared once; the listmonk/postiz/mattermost lanes (assigned MISSING in the parent spec) should reuse it. The first child of that rollup creates the file.
- **servicenow: DECIDED, `archimate-v1.ttl`.** `:Incident` and `:Change` are bound there as crosswalk anchors; `infrastructure-v1:Incident` is a different concept and must not be used for ServiceNow. Added: `:ConfigurationItem`, `:affectsConfigurationItem`, `:incidentState`, `:incidentAssignee`.
- **github, gitlab, repository-manager: DECIDED, `sdlc_lifecycle-v1.ttl`.** It already owns the pull request, merge request and pipeline classes; `:Repository`, `:Branch`, `:Worktree` are declared there once and shared. `software-v1` stays the code-graph file (`:Commit`, `:File`).
- **dockerhub: DECIDED, `infrastructure-v1.ttl`** (`:ContainerImage`, `:ContainerRegistry`).
- **egeria: DECIDED, `core-foundation-v1.ttl`** (`:Dataset`, `:DataClassification`, `:classifiedAs`); `catalog-v1.ttl` is generated and stays term-free.
- **gramps, paperless-ngx, nextcloud: DECIDED, `personal-v1.ttl`.** Its `:Family`, `:Correspondent`, `:Tag` and `:attachedFile` are documented as Gramps Web and Paperless-ngx terms.
- **legal-peripherals: DECIDED, `core-foundation-v1.ttl`** (`:Agreement`, `:LegalEntity`, `:Regulation`). Alternative: `company-v1.ttl:CompanyLicense` (`licenseExpiration`, `renewalRequired`); rejected because a contract is not a license.
- **stirlingpdf: DECIDED, `action-v1.ttl`** (`:ActionInvocation` with `:actsOnObject`, status and `producesEffect external`); added `:failureReason`.
- **BLOCKED for live competency acceptance (not for vocabulary declarations):** Not established by this audit: whether each connector's live payload carries the added fields (for example Tower `last_job_run`, Portainer replica counts); that is a connector-lane mapping check, owner: the connector package under `agent-packages/`.

### Worked example

Question (home-assistant, single-source): "Which sensors reported a value outside their normal range in the last 24 hours?"

```sparql
PREFIX : <http://knuckles.team/kg/infrastructure#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
SELECT ?entity ?signal ?observed ?p95 WHERE {
  VALUES ?cutoff { "2026-01-02T00:00:00Z"^^xsd:dateTime }
  ?entity a :Entity .
  ?a a :HealthAnomaly ; :affectsEntity ?entity ; :observed ?observed ; :observedAt ?t ; :signal ?signal .
  ?b a :HealthBaseline ; :affectsEntity ?entity ; :signal ?signal ; :p95 ?p95 .
  FILTER(?t >= ?cutoff)
}
```

The caller supplies the cutoff for a frozen as-of of 2026-01-03T00:00:00Z. Entity joins use the object-valued affectsEntity edge, not the string-valued entity property, and match the signal to avoid combining unrelated baselines.

Under the old assignment (`environment-v1`) no class in the query exists; under the corrected one every term is declared in `infrastructure-v1.ttl`. Answer shape: rows of entity, observed value, baseline p95, with provenance (source, record id, as-of) from the virtual mapping, or a typed abstention naming the unconnected Home Assistant source.

### Out of scope

The 23 MISSING connectors; connector-side mapping code; applying `specs/connector-competency` edits in this specification-only PR; copying source data; the cross-source federation mechanism (existing specs).
