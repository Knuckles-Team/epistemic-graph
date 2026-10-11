# EG-CQ-CONN — Design and implementation plan

Status: PROPOSED. Owner: epistemic-graph. The governing behavior and requirement
IDs are in [spec.md](spec.md) and [requirements.md](requirements.md). The complete
question and vocabulary inventory remains in [design.md](plan.md).

## Architecture and reuse

The engine answers connector competency questions through its existing
[federated query contract](../federated-query-and-reasoning/spec.md) and
[unified data plane](../unified-data-plane/spec.md). Reuse their ontology-driven
source selection, approved virtual mappings, query planning, and provenance
interfaces. Do not introduce a separate connector-specific query planner.

The ontology describes source access contracts. Records remain at their source
and are fetched at query time through MCP, A2A, API, GraphQL, or virtual-graph
contracts. Materialisation is an explicit optimisation with a stated reason,
never a prerequisite for the competency questions. The R067 children define
acceptance for each contract kind and declared materialisation.

## Ownership and interfaces

EG owns the ontology vocabulary, query behavior, provenance, typed abstentions,
and engine-side verification. Connector transports and mappings belong to the
[connector SDK and package owners](https://github.com/Knuckles-Team/agent-connector-sdk/tree/main/specs).
The [graph-os serving owner](https://github.com/Knuckles-Team/graph-os/tree/main/specs)
provides the integrated query surface. Cross-repository prerequisites remain
explicit dependencies, not engine implementations or evidence of delivery.

Each connector rollup has a single-source question, a cross-source question,
and an acceptance test. Fixtures preserve the connector's declared response
shape and stable record identities. Results carry source, record ID, and as-of
metadata; unavailable required sources produce typed abstentions naming the
source, rather than a successful empty answer. Cross-source joins must use the
existing approved mapping and identity-resolution contracts.

## Implementation sequence and proof

1. Inspect the proposed vocabulary assignment in [design.md](plan.md) against
   the actual ontology and connector contracts. Resolve every MISSING entry
   before implementing its question; filename-based assignments are proposals.
2. Add or extend vocabulary and source mappings using the existing legal owner.
   Preserve all 66 connector rows and their stable requirement IDs.
3. Implement recorded, disposable fixtures for each question without live
   credentials or a network dependency in the PR gate.
4. Execute both question paths and the negative unavailable-source path through
   the existing engine interfaces, following [test-spec.md](test-spec.md).
5. Verify R067 contract-kind coverage and the served integration surface.
   Update [tasks.md](tasks.md) and generated status only with exact-revision
   evidence; no proposal or cross-repository dependency counts as landed.

## Quality gates and risks

Run the public-spec check and generated spec-status check for documentation.
For implementation, run focused owner tests, language-native checks, and the
repository's applicable architecture, duplication, and release gates. Missing
connector operations, incomplete vocabulary, and unapproved cross-source
identity links remain explicit acceptance blockers. Do not replace those gaps
with copied source data or an independently implemented planner.

## Design

### Method

Each live connector gets one rollup with three children: a single-source competency question, a cross-source question that needs at least one other connector, and an acceptance test. Questions are specific to what the system holds. The vocabulary column was decided from the single listing of `crates/eg-core/ontology/` by file name only (files were not opened); MISSING means no file obviously matches, so a vocabulary file must be added before the question can be answered.

Mechanism is existing engine functionality, not new: cross-source answers use federated query and foreign sources (`specs/federated-query-and-reasoning`, `specs/unified-data-plane`); source authority and connector mapping use the native source connector vocabulary (`native_source_connector-v1.ttl`). Typed abstention names the unconnected source when a linked connector has no data. Rows here only add the question, the vocabulary assignment and the acceptance test.

### Questions

| Row | Connector | Single-source question | Cross-source question (other connectors; link) | Vocabulary |
|---|---|---|---|---|
| R001 | ansible-tower | Which job templates failed in their last run, and against which inventory hosts? | Which hosts that failed their last Tower run also have an open ServiceNow change or incident? (servicenow, systems-manager; managed host (configuration item)) | infrastructure-v1 |
| R002 | archimate | Which application components serve a given business process, and through which relationships? | Which ArchiMate application components have no matching LeanIX fact sheet or ServiceNow CI? (leanix, servicenow; application / configuration item) | archimate-v1 |
| R003 | archivebox | Which archived snapshots of a given domain failed one or more extractors, and when? | Which archived pages were first seen as FreshRSS articles but never captured by ArchiveBox? (freshrss, searxng; URL / web resource) | documentation-v1 |
| R004 | aris | Which EPC functions in a process model have no outgoing control-flow connection? | Which ARIS-modelled processes have a deployed Camunda definition, and which do not? (camunda, archimate; business process) | process_intelligence-v1 |
| R005 | arr | Which monitored movies and episodes are wanted but have no file, and which indexer last searched them? | Which wanted Radarr titles are currently downloading in qBittorrent and not yet visible in Jellyfin? (qbittorrent, jellyfin; media item (title)) | MISSING |
| R006 | atlassian | Which Jira issues in a project are blocked and unassigned, and by which linked issue? | Which open Jira issues reference a GitHub pull request that is already merged? (github, plane; work item / pull request) | sdlc_lifecycle-v1 |
| R007 | audiobookshelf | Which audiobook series have a book with listening progress stopped partway, and for which user? | Which audiobooks exist both in Audiobookshelf and as a downloaded file from media-downloader, and which are only one side? (jellyfin, media-downloader; media item (title)) | MISSING |
| R008 | audio-transcriber | Which recordings were transcribed with a low-confidence segment, and what text was affected? | Which transcribed recordings mention a topic that also appears in PulseLink social or video transcripts? (pulselink, paperless-ngx; document / transcript) | MISSING |
| R009 | caddy | Which routes proxy to a given upstream, and which hostnames are served by each site block? | Which Caddy hostnames have no DNS record in Technitium or no Uptime Kuma monitor? (technitium-dns, uptime; hostname) | infrastructure-v1 |
| R010 | camunda | Which process instances have an open incident, and at which activity did they stop? | Which running Camunda process definitions have an open ServiceNow incident raised against their service? (aris, servicenow; business process) | process_intelligence-v1 |
| R011 | ciso-assistant | Which controls in an applied framework are non-compliant and which risk scenarios do they mitigate? | Which CISO Assistant risks map to a ServiceNow CMDB service with an open incident? (onetrust, servicenow; control / risk) | government-v1 |
| R012 | clarity | Which pages have the highest rage-click or dead-click rate over the last week? | Which pages with a spike in rage clicks were changed by a pull request merged in the previous week? (plane, github; web page / release) | MISSING |
| R013 | container-manager | Which containers or pods are restarting repeatedly, and from which image? | Which running images have a newer tag on Docker Hub than the one deployed? (dockerhub, portainer; container image) | infrastructure-v1 |
| R014 | data-science | Which trained models have validation metrics below their baseline run, and on which dataset? | Which fine-tuned models have no evaluation dataset trace in Langfuse and no stored artifact in the object store? (langfuse, objectstore; model / dataset) | MISSING |
| R015 | dockerhub | Which repositories in an organization have not been pushed to in a year, and what are their pull counts? | Which Docker Hub repositories correspond to a GitHub repository with recent commits but no recent image push? (container-manager, github; container image / repository) | infrastructure-v1 |
| R016 | documentdb | Which collections have the most documents and which fields are missing in more than half of them? | Which documentdb collections duplicate a table in the SQL database, and which side is newer? (sql, objectstore; dataset / collection) | MISSING |
| R017 | egeria | Which glossary terms are linked to a data asset with a confidential classification? | Which Egeria-cataloged assets correspond to a live SQL table, and which are cataloged but missing? (sql, jena; data asset) | core-foundation-v1 |
| R018 | emerald-exchange | Which open orders on an exchange are partially filled and what is their remaining quantity? | Which exchange fills correspond to a deposit or withdrawal recorded in the Firefly III ledger? (firefly-iii, home-assistant; account / currency) | finance-v1 |
| R019 | erpnext | Which open purchase orders are late, and from which supplier? | Which ERPNext suppliers with late orders are also marked as active accounts in the Twenty CRM or paid in Firefly III? (twenty, firefly-iii; party (supplier / customer)) | commercial-v1 |
| R020 | fan-manager | Which fans ran above 80 percent duty while the host temperature stayed above its threshold? | Which hosts had fan duty at maximum during a window in which Grafana alerted on temperature? (systems-manager, lgtm; host) | infrastructure-v1 |
| R021 | firefly-iii | Which budgets were exceeded this month, and by which transactions? | Which Firefly III expenses match an ERPNext purchase invoice by amount and date? (erpnext, emerald-exchange; account / transaction) | finance-v1 |
| R022 | freshrss | Which unread articles from a given feed were published in the last 48 hours? | Which FreshRSS articles also appear in SearXNG results for the same query, and which are new? (archivebox, searxng; URL / article) | MISSING |
| R023 | github | Which open pull requests have failing checks and no reviewer assigned? | Which GitHub pull requests close a Jira issue that is still in an open state? (gitlab, atlassian; repository / change request) | sdlc_lifecycle-v1 |
| R024 | gitlab | Which pipelines on the default branch failed in the last day, and at which job? | Which repositories exist in GitLab and GitHub with diverging default-branch heads? (github, repository-manager; repository / pipeline) | sdlc_lifecycle-v1 |
| R025 | gramps | Which people in the tree were born in a given place before a given year, and who are their parents? | Which Gramps people have a photo or document reference that exists as a file in Nextcloud? (nextcloud; person / media) | personal-v1 |
| R026 | home-assistant | Which sensors reported a value outside their normal range in the last 24 hours? | Which Home Assistant devices went unavailable at the same time as an Uptime Kuma monitor failure? (fan-manager, uptime; device / host) | infrastructure-v1 |
| R027 | jellyfin | Which movies in a library have never been played and were added more than a year ago? | Which Jellyfin items have no matching Radarr or Sonarr entry, and so cannot be re-fetched? (arr, qbittorrent; media item (title)) | MISSING |
| R028 | jena | Which named graphs contain the most triples and which have no recent update? | Which Jena named graphs describe the same entities as an Egeria-cataloged asset? (egeria, sql; graph / dataset) | virtual_graph-v1 |
| R029 | kafka | Which consumer groups lag by more than a threshold on which topic partitions? | Which Kafka consumer groups with sustained lag belong to a service that Grafana is alerting on? (langfuse, lgtm; topic / service) | MISSING |
| R030 | keycloak | Which clients can a given user sign in to, and through which role? | Which people have access to a service that has an open security incident? (okta, servicenow; person / service) | identity-v1 |
| R031 | langfuse | Which traces had the highest token cost and which prompt version produced them? | Which prompt versions with falling evaluation scores were last changed by a merged GitHub pull request? (data-science, github; prompt / model) | orchestration-v1 |
| R032 | leanix | Which fact sheets of type application have lifecycle phase end-of-life but still have active users? | Which end-of-life LeanIX applications still have an active ServiceNow CI and open incidents? (archimate, servicenow; application) | enterprise-v1 |
| R033 | legal-peripherals | Which contracts expire within 90 days and have no renewal action recorded? | Which expiring contracts have a scanned copy in Paperless-ngx and a matching OneTrust vendor assessment? (paperless-ngx, onetrust; contract / document) | core-foundation-v1 |
| R034 | lgtm | Which alerts are firing right now, with what severity and for how long? | Which firing Grafana alerts have no open ServiceNow incident, and which incidents have no alert? (uptime, servicenow; service / alert) | infrastructure-v1 |
| R035 | listmonk | Which campaigns had an open rate below the list average, and to which lists were they sent? | Which subscribers on a Listmonk list have no matching Twenty contact, and which contacts are unsubscribed? (twenty, postiz; contact / audience) | MISSING |
| R036 | mattermost | Which channels have had no posts in 90 days but still have more than ten members? | Which Mattermost members have no active identity-provider account, and which people lack a Mattermost account? (keycloak, okta; person) | MISSING |
| R037 | mealie | Which recipes use an ingredient that is also on the current shopping list? | Which planned Mealie meals exceed the daily calorie target set in wger, and what did the groceries cost in Firefly III? (wger, firefly-iii; food / ingredient) | nutrition-v1 |
| R038 | media-downloader | Which download jobs failed in the last week, with what error, for which source URL? | Which completed downloads were never added to Jellyfin or Audiobookshelf? (audiobookshelf, jellyfin; media item / URL) | MISSING |
| R039 | microsoft | Which meetings tomorrow overlap, and which attendees are double-booked? | Which attendees of an upcoming meeting have no identity-provider account or Mattermost presence? (okta, mattermost; person) | calendar-v1 |
| R040 | nextcloud | Which shared folders are accessible by external links and when do those shares expire? | Which publicly shared Nextcloud files also exist as Paperless-ngx documents tagged confidential? (keycloak, paperless-ngx; file / person) | personal-v1 |
| R041 | objectstore | Which buckets hold the most data and which objects have not been read in a year? | Which object-store artifacts are not referenced by any model or dataset in the data-science connector? (data-science, documentdb; dataset / artifact) | MISSING |
| R042 | okta | Which users hold a given application assignment through which group? | Which Okta users were deactivated but still hold a Keycloak role or an open ServiceNow assignment? (keycloak, servicenow; person / role) | identity-v1 |
| R043 | onetrust | Which privacy assessments are overdue and which processing activities do they cover? | Which vendors with an overdue OneTrust assessment have open ERPNext purchase orders? (ciso-assistant, erpnext; processing activity / vendor) | government-v1 |
| R044 | openbao | Which secret paths have not been rotated in 180 days, and which policies grant read on them? | Which services running in container-manager read a secret path that has no recent rotation? (keycloak, container-manager; secret / service) | MISSING |
| R045 | owncast | Which streams had the highest peak viewer count and how long did they run? | Which Owncast broadcasts were announced through a Postiz post, and what was the audience versus the post reach? (postiz, mattermost; broadcast / audience) | MISSING |
| R046 | paperless-ngx | Which documents tagged as invoice have no correspondent or no due date? | Which Paperless-ngx invoices have no matching ERPNext purchase invoice or Firefly III expense? (erpnext, firefly-iii; document / invoice) | personal-v1 |
| R047 | plane | Which work items in the active cycle are overdue and which module do they belong to? | Which Plane work items link to a GitHub pull request that has merged but are not marked done? (github, atlassian; work item) | sdlc_lifecycle-v1 |
| R048 | portainer | Which stacks have services with fewer running replicas than desired, and on which endpoint? | Which Portainer stacks deploy from a Git repository whose latest GitLab pipeline failed? (container-manager, gitlab; stack / repository) | infrastructure-v1 |
| R049 | postiz | Which scheduled posts failed to publish and on which channel? | Which Postiz posts promote a campaign that Listmonk also sent, and which got the better engagement? (owncast, listmonk; campaign / channel) | MISSING |
| R050 | pulselink | Which videos on a topic had transcripts mentioning a given term this week? | Which topics appear in PulseLink video transcripts and SearXNG news but not in any archived page? (searxng, audio-transcriber; topic / transcript) | MISSING |
| R051 | qbittorrent | Which torrents are stalled, with what ratio and how long since last activity? | Which stalled torrents belong to a wanted Radarr or Sonarr title, and so block it from import? (arr, jellyfin; media item (torrent)) | MISSING |
| R052 | repository-manager | Which worktrees are stale or dangling relative to their base branch? | Which local worktrees track a branch whose pull request has already been merged on GitHub or GitLab? (github, gitlab; repository / branch) | sdlc_lifecycle-v1 |
| R053 | rom-manager | Which ROMs in a platform collection fail a checksum match against the reference set? | Which ROMs were obtained by a media-downloader job and which are still unconverted? (media-downloader, jellyfin; game / file) | MISSING |
| R054 | salesforce | Which open opportunities close this quarter, with what amount and owner? | Which Salesforce accounts have an open ServiceNow incident and an opportunity closing this quarter? (twenty, servicenow; account / customer) | commercial-v1 |
| R055 | scholarx | Which papers cite a given paper and were published in the last two years? | Which papers on a topic are in ScholarX but were never surfaced by a SearXNG query for that topic? (searxng, langfuse; paper / topic) | MISSING |
| R056 | searxng | Which engines returned no results for a given query, and which returned the most? | Which URLs from a SearXNG query are not yet archived in ArchiveBox? (freshrss, archivebox; URL / query) | MISSING |
| R057 | servicenow | Which incidents are open on a given configuration item and who is assigned? | Which open incidents on a CI also show a Grafana alert and a failing Uptime Kuma monitor? (okta, lgtm; configuration item / service) | archimate-v1 |
| R058 | sql | Which tables have no primary key, and which columns reference tables that do not exist? | Which SQL tables are not cataloged in Egeria, and which cataloged tables no longer exist? (egeria, documentdb; table / data asset) | MISSING |
| R059 | stirlingpdf | Which operations failed on which PDFs and with what error? | Which PDFs processed by Stirling-PDF were stored in Paperless-ngx or Nextcloud, and which were not? (paperless-ngx, nextcloud; document) | action-v1 |
| R060 | systems-manager | Which hosts have pending OS updates and what is their uptime? | Which hosts with pending updates have no Ansible Tower patch job scheduled and no ServiceNow change? (ansible-tower, servicenow; host (configuration item)) | infrastructure-v1 |
| R061 | technitium-dns | Which zones contain records pointing to an address that no host answers on? | Which DNS A records have no Caddy route and no Uptime Kuma monitor? (caddy, uptime; hostname) | infrastructure-v1 |
| R062 | tunnel-manager | Which SSH hosts in the inventory failed their last connection, and with what error? | Which hosts that tunnel-manager cannot reach are still listed as managed in Ansible Tower? (ansible-tower, systems-manager; host) | infrastructure-v1 |
| R063 | twenty | Which people at a company have no logged activity in the last 60 days while a deal is open? | Which Twenty contacts also exist as Salesforce contacts with different email addresses? (salesforce, listmonk; person / company) | commercial-v1 |
| R064 | uptime | Which monitored services were down in the last 24 hours and for how long? | Which services Uptime Kuma saw down had a Grafana alert or a Caddy route error at the same time? (lgtm, caddy; service / hostname) | infrastructure-v1 |
| R065 | vector | Which collections have the most vectors and which have an embedding dimension that mismatches the model? | Which Paperless-ngx documents have no embedding stored in the vector database? (data-science, paperless-ngx; embedding / document) | MISSING |
| R066 | wger | Which workout routines were not completed in the last two weeks and which exercises were skipped? | Which weeks of low workout completion coincide with a Mealie meal plan above the calorie target? (mealie, home-assistant; person / nutrition plan) | nutrition-v1 |

### Coverage

| Connector | Domain | Vocabulary |
|---|---|---|
| ansible-tower | IT operations | infrastructure-v1.ttl |
| archimate | engineering | archimate-v1.ttl |
| archivebox | knowledge | documentation-v1.ttl |
| aris | governance | process_intelligence-v1.ttl |
| arr | media | MISSING |
| atlassian | engineering | sdlc_lifecycle-v1.ttl |
| audiobookshelf | media | MISSING |
| audio-transcriber | media | MISSING |
| caddy | IT operations | infrastructure-v1.ttl |
| camunda | business | process_intelligence-v1.ttl |
| ciso-assistant | governance | government-v1.ttl |
| clarity | data | MISSING |
| container-manager | IT operations | infrastructure-v1.ttl |
| data-science | data | MISSING |
| dockerhub | engineering | infrastructure-v1.ttl |
| documentdb | data | MISSING |
| egeria | governance | core-foundation-v1.ttl |
| emerald-exchange | finance | finance-v1.ttl |
| erpnext | business | commercial-v1.ttl |
| fan-manager | home | infrastructure-v1.ttl |
| firefly-iii | finance | finance-v1.ttl |
| freshrss | knowledge | MISSING |
| github | engineering | sdlc_lifecycle-v1.ttl |
| gitlab | engineering | sdlc_lifecycle-v1.ttl |
| gramps | knowledge | personal-v1.ttl |
| home-assistant | home | infrastructure-v1.ttl |
| jellyfin | media | MISSING |
| jena | data | virtual_graph-v1.ttl |
| kafka | data | MISSING |
| keycloak | identity | identity-v1.ttl |
| langfuse | engineering | orchestration-v1.ttl |
| leanix | business | enterprise-v1.ttl |
| legal-peripherals | governance | core-foundation-v1.ttl |
| lgtm | IT operations | infrastructure-v1.ttl |
| listmonk | business | MISSING |
| mattermost | business | MISSING |
| mealie | home | nutrition-v1.ttl |
| media-downloader | media | MISSING |
| microsoft | business | calendar-v1.ttl |
| nextcloud | knowledge | personal-v1.ttl |
| objectstore | data | MISSING |
| okta | identity | identity-v1.ttl |
| onetrust | governance | government-v1.ttl |
| openbao | IT operations | MISSING |
| owncast | media | MISSING |
| paperless-ngx | knowledge | personal-v1.ttl |
| plane | engineering | sdlc_lifecycle-v1.ttl |
| portainer | IT operations | infrastructure-v1.ttl |
| postiz | business | MISSING |
| pulselink | knowledge | MISSING |
| qbittorrent | media | MISSING |
| repository-manager | engineering | sdlc_lifecycle-v1.ttl |
| rom-manager | media | MISSING |
| salesforce | business | commercial-v1.ttl |
| scholarx | knowledge | MISSING |
| searxng | knowledge | MISSING |
| servicenow | IT operations | archimate-v1.ttl |
| sql | data | MISSING |
| stirlingpdf | knowledge | action-v1.ttl |
| systems-manager | IT operations | infrastructure-v1.ttl |
| technitium-dns | IT operations | infrastructure-v1.ttl |
| tunnel-manager | IT operations | infrastructure-v1.ttl |
| twenty | business | commercial-v1.ttl |
| uptime | IT operations | infrastructure-v1.ttl |
| vector | data | MISSING |
| wger | home | nutrition-v1.ttl |

Connectors with no matching vocabulary: **23** of 66 (arr, audiobookshelf, audio-transcriber, clarity, data-science, documentdb, freshrss, jellyfin, kafka, listmonk, mattermost, media-downloader, objectstore, openbao, owncast, postiz, pulselink, qbittorrent, rom-manager, scholarx, searxng, sql, vector).

## Vocabulary audit design (folded from EG-CONN-AUDIT, 2026-10-11)

#### Architecture

This is a vocabulary correction. No new mechanism: answers still come from the engine's federated query over each connector's access contract (`specs/federated-query-and-reasoning`, `specs/unified-data-plane`), source authority from `native_source_connector-v1.ttl`, and virtual mappings from `virtual_graph-v1.ttl`. The audit only fixes which ontology terms the competency questions bind to.

```
competency question -> vocabulary file (this audit's table) -> classes/properties
   -> virtual mapping (vg:mapsOntologyClass) -> connector access contract (MCP/API/GraphQL)
   -> fetch at query time -> provenance-carrying answer or typed abstention
```

Owners: ontology files and `schema_sources.rs` registration are epistemic-graph (`crates/eg-core`); connector-side mappings stay with the connector package (cross-repo, not in this spec).

#### Vocabulary

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

#### Wiring

- Ontology files: `crates/eg-core/ontology/<file>.ttl`; registration: `CoreSpec` entries in `crates/eg-core/src/graph/schema_sources.rs` (precedent `governance-shapes`, `personal`, `government`).
- `catalog-v1.ttl` is generated; it is never hand-edited. The new `commerce` module appears in it by regeneration.
- Connector mappings to the new terms are `vg:VirtualMapping` rows (`virtual_graph-v1.ttl`: `mapsOntologyClass`, `mapsEntity`, `hasPredicateFieldPair`); they belong to the connector lanes, not here.
- Reassignment rows change the Coverage table and `Vocabulary:` text in `specs/connector-competency` after their producer terms land, with exact row-level assertions in both documents.
- Scopes: none added; reads use the scopes the engine contract already lists.

#### Worked example

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

#### Out of scope

The 23 MISSING connectors; connector-side mapping code; applying `specs/connector-competency` edits in this specification-only PR; copying source data; the cross-source federation mechanism (existing specs).

Note: where the folded text says `commerce-v1.ttl`, `EG-CQ-CONN-R083` supersedes it: erpnext, salesforce and twenty use `commercial-v1.ttl` from `EG-SUPPLY-VOCAB`. Child-row references of the form `EG-CONN-AUDIT-R0nn` are mapped in the ID map below.


## Audit record

#### Problem and evidence

`specs/connector-competency` assigned each of 66 live connectors a vocabulary file under `crates/eg-core/ontology/` from a directory listing, without opening the files, and marked 23 as MISSING. This audit opens each of the 19 distinct assigned files once and judges the 43 assigned connectors against the two competency questions each (Questions table in `specs/connector-competency/design.md`). It does not edit that directory; it records corrections as rows here.

Verdict rule. **FIT**: the file already declares the classes and properties that carry both questions. **PARTIAL**: the file carries the central class of the question but lacks named terms. **WRONG**: the file carries neither question's central class; the table names the correct file (or MISSING).

Evidence that the file-name method misfired (all read from the files):

- `governance-core-v1.shapes.ttl` is four NodeShapes (ADR, ServiceCapability, Policy, Tool) and no class or property; the module is registered with `ontology: None` (`crates/eg-core/src/graph/schema_sources.rs`, `governance-shapes` entry).
- `catalog-v1.ttl` is a generated 40-line `owl:imports` aggregate with zero terms.
- `company-v1.ttl` is the agent-company model (`:Company`, `:WorkItem`, `:KeyPerformanceIndicator`, `:PayrollRecord`); no `Customer`, `Supplier`, `PurchaseOrder`, `Invoice`, `Opportunity` or `Contact` exists in any `.ttl`.
- `life-v1.ttl` is biology; `environment-v1.ttl` is ecology and weather; `harness-v1.ttl` models agent-harness scaffolding evolution.
- Terms are in a sibling file the name did not suggest: Home Assistant `:Device`/`:Entity` and the fan-manager `:HealthTrend` family are in `infrastructure-v1.ttl`. Paperless `:Tag`/`:Correspondent` and Gramps `:Family` are in `personal-v1.ttl`. `:PullRequest`, `:MergeRequest`, `:PipelineRun` are in `sdlc_lifecycle-v1.ttl`.
- Classes used but never declared: `:Repository` (referenced by `infrastructure-v1` `ContainerImage` and `sdlc_lifecycle-v1` `aliasOf`), `:Application` (range of `identity-v1` `:assignedApp`). No `Worktree` or `Branch` term exists.
- Two Incident IRIs exist: `core-foundation-v1` and `archimate-v1` both describe `http://knuckles.team/kg#Incident` (the latter adds the ApplicationEvent crosswalk); `infrastructure-v1` owns the distinct infrastructure namespace Incident. ServiceNow uses the shared kg IRI, never the infrastructure Incident.

#### Totals

| Verdict | Connectors |
|---|---|
| FIT | 4 (container-manager, fan-manager, microsoft, uptime) |
| PARTIAL | 21 |
| WRONG | 18 |
| Total audited | 43 |

Terms to add: 125 distinct across 16 files (one new file, `commerce-v1.ttl`). Rows: 17 rollups (16 file rollups plus Reassignments) and 156 children.

#### Per-connector audit

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

#### Decisions

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


## Amendments

### 2026-10-11 Vocabulary reassignments and DECIDED outcomes (from EG-CONN-AUDIT)

Evidence: the audit opened each of the 19 assigned ontology files. The Audit record above holds the method and per-connector table. Rows `EG-CQ-CONN-R0nn.1` and `.2` for each connector below, the Coverage table and the Questions-table vocabulary column were corrected in this pass. The ontology terms themselves land through `EG-CQ-CONN-R068` to `R083`.

| Connector | Old statement (vocabulary) | New statement (vocabulary) |
|---|---|---|
| ciso-assistant | `governance-core-v1.shapes.ttl` | `government-v1.ttl` |
| dockerhub | `software-v1.ttl` | `infrastructure-v1.ttl` |
| egeria | `catalog-v1.ttl` | `core-foundation-v1.ttl` |
| erpnext | `company-v1.ttl` | `commercial-v1.ttl` |
| github | `software-v1.ttl` | `sdlc_lifecycle-v1.ttl` |
| gitlab | `software-v1.ttl` | `sdlc_lifecycle-v1.ttl` |
| gramps | `life-v1.ttl` | `personal-v1.ttl` |
| home-assistant | `environment-v1.ttl` | `infrastructure-v1.ttl` |
| langfuse | `harness-v1.ttl` | `orchestration-v1.ttl` |
| legal-peripherals | `government-v1.ttl` | `core-foundation-v1.ttl` |
| nextcloud | `documentation-v1.ttl` | `personal-v1.ttl` |
| onetrust | `governance-core-v1.shapes.ttl` | `government-v1.ttl` |
| paperless-ngx | `documentation-v1.ttl` | `personal-v1.ttl` |
| repository-manager | `software-v1.ttl` | `sdlc_lifecycle-v1.ttl` |
| salesforce | `company-v1.ttl` | `commercial-v1.ttl` |
| servicenow | `enterprise-v1.ttl` | `archimate-v1.ttl` |
| stirlingpdf | `documentation-v1.ttl` | `action-v1.ttl` |
| twenty | `company-v1.ttl` | `commercial-v1.ttl` |

DECIDED outcomes: home-assistant uses `infrastructure-v1.ttl` (`:Device`, `:Entity` are documented as Home Assistant terms). Langfuse uses `orchestration-v1.ttl` (`harness-v1` models harness scaffolding, not LLM traces). Ciso-assistant and onetrust use `government-v1.ttl` (`governance-core-v1.shapes.ttl` holds four NodeShapes and no class or property).

Commerce exception: the audit proposed a new `commerce-v1.ttl` for erpnext, salesforce and twenty. This duplicates `EG-SUPPLY-VOCAB` (`commercial-v1.ttl`), so those three connectors are reassigned to `commercial-v1.ttl` (row `EG-CQ-CONN-R083`). Terms the audit wanted that `EG-SUPPLY-VOCAB` does not specify (compared by name in its requirements.md): `:Party`, `:PurchaseOrder` (only named in `EG-SUPPLY-VOCAB-R005.3`, never declared), `:PurchaseInvoice`, `:Opportunity`, `:Contact`, `:lastActivityAt`. It does cover `:Customer`.

The 18 `EG-CONN-AUDIT-R017.n` reassignment rows are not carried as rows: the correction is made here directly and the producer terms are tracked by `R068` to `R083`.

ID map (old to new):

| Old ID | New ID |
|---|---|
| `EG-CONN-AUDIT-R001` (and `.n` children) | `EG-CQ-CONN-R068` (children keep `.n`, except R016 children and R017 children, which are not carried) |
| `EG-CONN-AUDIT-R002` (and `.n` children) | `EG-CQ-CONN-R069` (children keep `.n`, except R016 children and R017 children, which are not carried) |
| `EG-CONN-AUDIT-R003` (and `.n` children) | `EG-CQ-CONN-R070` (children keep `.n`, except R016 children and R017 children, which are not carried) |
| `EG-CONN-AUDIT-R004` (and `.n` children) | `EG-CQ-CONN-R071` (children keep `.n`, except R016 children and R017 children, which are not carried) |
| `EG-CONN-AUDIT-R005` (and `.n` children) | `EG-CQ-CONN-R072` (children keep `.n`, except R016 children and R017 children, which are not carried) |
| `EG-CONN-AUDIT-R006` (and `.n` children) | `EG-CQ-CONN-R073` (children keep `.n`, except R016 children and R017 children, which are not carried) |
| `EG-CONN-AUDIT-R007` (and `.n` children) | `EG-CQ-CONN-R074` (children keep `.n`, except R016 children and R017 children, which are not carried) |
| `EG-CONN-AUDIT-R008` (and `.n` children) | `EG-CQ-CONN-R075` (children keep `.n`, except R016 children and R017 children, which are not carried) |
| `EG-CONN-AUDIT-R009` (and `.n` children) | `EG-CQ-CONN-R076` (children keep `.n`, except R016 children and R017 children, which are not carried) |
| `EG-CONN-AUDIT-R010` (and `.n` children) | `EG-CQ-CONN-R077` (children keep `.n`, except R016 children and R017 children, which are not carried) |
| `EG-CONN-AUDIT-R011` (and `.n` children) | `EG-CQ-CONN-R078` (children keep `.n`, except R016 children and R017 children, which are not carried) |
| `EG-CONN-AUDIT-R012` (and `.n` children) | `EG-CQ-CONN-R079` (children keep `.n`, except R016 children and R017 children, which are not carried) |
| `EG-CONN-AUDIT-R013` (and `.n` children) | `EG-CQ-CONN-R080` (children keep `.n`, except R016 children and R017 children, which are not carried) |
| `EG-CONN-AUDIT-R014` (and `.n` children) | `EG-CQ-CONN-R081` (children keep `.n`, except R016 children and R017 children, which are not carried) |
| `EG-CONN-AUDIT-R015` (and `.n` children) | `EG-CQ-CONN-R082` (children keep `.n`, except R016 children and R017 children, which are not carried) |
| `EG-CONN-AUDIT-R016` (and `.n` children) | `EG-CQ-CONN-R083` (children keep `.n`, except R016 children and R017 children, which are not carried) |
| `EG-CONN-AUDIT-R017` and `.1` to `.18` | Amendment above |
| `EG-CONN-AUDIT` (spec ID) | `EG-CQ-CONN` |

### 2026-10-11 Restore commercial reassignment refusal tests

Old statement: R083 checked the commercial Coverage names without exact-cell and missing-producer refusal tests.
New statement: R083 includes those checks and EG-SUPPLY-VOCAB-R003.13 through R003.17 registry and shape prerequisites.
Evidence: former EG-CONN-AUDIT-R017.4, R017.15 and R017.18 required these checks for erpnext, salesforce and twenty.
This amendment does not deliver ontology terms or verify connector answers. Source payload support remains unverified.

### 2026-10-11 Restore noncommercial mapping acceptance

Old statement: the preceding amendment removed all 18 reassignment rows after correcting their documentation.
New statement: these 15 existing acceptance children retain the noncommercial mapping checks and producer dependencies.
Evidence: revision `b52f912f`, `EG-CONN-AUDIT-R017.1` through `EG-CONN-AUDIT-R017.18`, required these checks.
This amendment supersedes the claim that corrected cells alone discharge those acceptance conditions.
The current vocabulary decisions remain unchanged.
The commercial connectors retain separate ownership under `EG-CQ-CONN-R083`.
The legacy IDs below record lineage, not newly active rows or completed work.
Legacy retirement records remain pending shared-gate support; this amendment does not renumber legacy IDs.
All restored acceptance conditions remain SPECIFIED until implementation and named tests provide evidence.

| Historical requirement | Connector | Existing acceptance owner | Expected vocabulary |
|---|---|---|---|
| `EG-CONN-AUDIT-R017.1` | ciso-assistant | `EG-CQ-CONN-R011.3` | `government-v1.ttl` |
| `EG-CONN-AUDIT-R017.2` | dockerhub | `EG-CQ-CONN-R015.3` | `infrastructure-v1.ttl` |
| `EG-CONN-AUDIT-R017.3` | egeria | `EG-CQ-CONN-R017.3` | `core-foundation-v1.ttl` |
| `EG-CONN-AUDIT-R017.5` | github | `EG-CQ-CONN-R023.3` | `sdlc_lifecycle-v1.ttl` |
| `EG-CONN-AUDIT-R017.6` | gitlab | `EG-CQ-CONN-R024.3` | `sdlc_lifecycle-v1.ttl` |
| `EG-CONN-AUDIT-R017.7` | gramps | `EG-CQ-CONN-R025.3` | `personal-v1.ttl` |
| `EG-CONN-AUDIT-R017.8` | home-assistant | `EG-CQ-CONN-R026.3` | `infrastructure-v1.ttl` |
| `EG-CONN-AUDIT-R017.9` | langfuse | `EG-CQ-CONN-R031.3` | `orchestration-v1.ttl` |
| `EG-CONN-AUDIT-R017.10` | legal-peripherals | `EG-CQ-CONN-R033.3` | `core-foundation-v1.ttl` |
| `EG-CONN-AUDIT-R017.11` | nextcloud | `EG-CQ-CONN-R040.3` | `personal-v1.ttl` |
| `EG-CONN-AUDIT-R017.12` | onetrust | `EG-CQ-CONN-R043.3` | `government-v1.ttl` |
| `EG-CONN-AUDIT-R017.13` | paperless-ngx | `EG-CQ-CONN-R046.3` | `personal-v1.ttl` |
| `EG-CONN-AUDIT-R017.14` | repository-manager | `EG-CQ-CONN-R052.3` | `sdlc_lifecycle-v1.ttl` |
| `EG-CONN-AUDIT-R017.16` | servicenow | `EG-CQ-CONN-R057.3` | `archimate-v1.ttl` |
| `EG-CONN-AUDIT-R017.17` | stirlingpdf | `EG-CQ-CONN-R059.3` | `action-v1.ttl` |

Each acceptance owner lists its exact producer dependencies in `requirements.md`.
The test must resolve full IRIs from registered modules, including cross-module prerequisites.
DockerHub depends on `EG-CQ-CONN-R069.2` for SDLC `Repository`; its primary vocabulary remains infrastructure.
The implementation must retain each producer's current representation, domain, range, and module ownership.
The documentation correction does not implement these producer terms or prove runtime competency.

### 2026-10-11 Preserve historical requirement IDs

Old statement: migration removed historical requirement rows and deferred retirement records.
New statement: these permanent IDs remain explicit RETIRED tombstones with named replacements.
Evidence: PR #413 migration, PR #418 vocabulary decisions, PR #419 commercial acceptance, and PR #421 noncommercial acceptance.
The shared retirement parser now permits historical prefixes on retired rows.
The amendment supersedes earlier pending-retirement statements and broad R016-to-R083 mapping.
R016 producers belong to EG-SUPPLY-VOCAB-R003; R017 commercial acceptance belongs to EG-CQ-CONN-R083.
Current active requirements, status evidence, and vocabulary decisions remain unchanged.
These 173 historical IDs are not delivered requirements or runtime evidence.

#### Historical acceptance record

Source: `b52f912f:specs/connector-vocabulary-audit/requirements.md` and its generated `status.json`.
Every original row below was SPECIFIED, with empty landed_in and verified_by evidence.
The following original requirement and verification text is preserved verbatim for historical interpretation only.

| Historical ID | Original requirement | Original verification |
|---|---|---|
| `EG-CONN-AUDIT-R001` | **infrastructure-v1.ttl additions rollup.** 26 child rows, one per missing term (plus setup rows); becomes LANDED when every child is LANDED. Children: `EG-CONN-AUDIT-R001.1` to `EG-CONN-AUDIT-R001.26`. | Children landed. |
| `EG-CONN-AUDIT-R001.1` | **shapes companion for infrastructure-v1.** Create `crates/eg-core/ontology/infrastructure-v1.shapes.ttl` (precedent: `finance-v1.shapes.ttl`) and set `shapes: Some(include_str!(...))` on the `infrastructure` CoreSpec in `schema_sources.rs`. Empty shapes graph is valid; each term child below adds its own NodeShape. Reuse an existing companion without replacing its shapes; delivery still requires the named registry-load test. | `ontology_audit_infrastructure_v1::shapes_module_loads`. |
| `EG-CONN-AUDIT-R001.2` | **AutomationJob.** Add owl:Class `:AutomationJob` (subClassOf :Workload-adjacent bfo process; one Tower job run) to `crates/eg-core/ontology/infrastructure-v1.ttl`. SHACL: AutomationJob: jobStatus minCount 1, in `infrastructure-v1.shapes.ttl`. Needed by: ansible-tower. | `ontology_audit_infrastructure_v1::AutomationJob_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain) and that a violating fixture fails the shape. |
| `EG-CONN-AUDIT-R001.3` | **AutomationJobTemplate.** Add owl:Class `:AutomationJobTemplate` (template a job instantiates) to `crates/eg-core/ontology/infrastructure-v1.ttl`. Needed by: ansible-tower. | `ontology_audit_infrastructure_v1::AutomationJobTemplate_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R001.4` | **jobStatus.** Add owl:DatatypeProperty `:jobStatus` (AutomationJob -> xsd:string (successful|failed|running|canceled)) to `crates/eg-core/ontology/infrastructure-v1.ttl`. SHACL: sh:in on jobStatus, in `infrastructure-v1.shapes.ttl`. Needed by: ansible-tower. | `ontology_audit_infrastructure_v1::jobStatus_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain) and that a violating fixture fails the shape. |
| `EG-CONN-AUDIT-R001.5` | **ranAgainstHost.** Add owl:ObjectProperty `:ranAgainstHost` (AutomationJob -> :Host) to `crates/eg-core/ontology/infrastructure-v1.ttl`. Needed by: ansible-tower. | `ontology_audit_infrastructure_v1::ranAgainstHost_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R001.6` | **routeHostname.** Add owl:DatatypeProperty `:routeHostname` (ProxyRoute -> xsd:string) to `crates/eg-core/ontology/infrastructure-v1.ttl`. SHACL: routeHostname minCount 1 on ProxyRoute, in `infrastructure-v1.shapes.ttl`. Needed by: caddy. | `ontology_audit_infrastructure_v1::routeHostname_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain) and that a violating fixture fails the shape. |
| `EG-CONN-AUDIT-R001.7` | **upstreamAddress.** Add owl:DatatypeProperty `:upstreamAddress` (ProxyRoute -> xsd:string) to `crates/eg-core/ontology/infrastructure-v1.ttl`. Needed by: caddy. | `ontology_audit_infrastructure_v1::upstreamAddress_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R001.8` | **Alert.** Add owl:Class `:Alert` (a firing or resolved alert rule instance on a :Service) to `crates/eg-core/ontology/infrastructure-v1.ttl`. SHACL: Alert: alertSeverity, alertState minCount 1, in `infrastructure-v1.shapes.ttl`. Needed by: lgtm. | `ontology_audit_infrastructure_v1::Alert_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain) and that a violating fixture fails the shape. |
| `EG-CONN-AUDIT-R001.9` | **alertSeverity.** Add owl:DatatypeProperty `:alertSeverity` (Alert -> xsd:string) to `crates/eg-core/ontology/infrastructure-v1.ttl`. Needed by: lgtm. | `ontology_audit_infrastructure_v1::alertSeverity_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R001.10` | **alertState.** Add owl:DatatypeProperty `:alertState` (Alert -> xsd:string (firing|resolved|pending)) to `crates/eg-core/ontology/infrastructure-v1.ttl`. Needed by: lgtm. | `ontology_audit_infrastructure_v1::alertState_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R001.11` | **firingSince.** Add owl:DatatypeProperty `:firingSince` (Alert -> xsd:dateTime) to `crates/eg-core/ontology/infrastructure-v1.ttl`. Needed by: lgtm. | `ontology_audit_infrastructure_v1::firingSince_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R001.12` | **alertsOn.** Add owl:ObjectProperty `:alertsOn` (Alert -> :Service) to `crates/eg-core/ontology/infrastructure-v1.ttl`. Needed by: lgtm. | `ontology_audit_infrastructure_v1::alertsOn_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R001.13` | **lastPushedAt.** Add owl:DatatypeProperty `:lastPushedAt` (ContainerImage -> xsd:dateTime) to `crates/eg-core/ontology/infrastructure-v1.ttl`. Needed by: dockerhub. | `ontology_audit_infrastructure_v1::lastPushedAt_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R001.14` | **pullCount.** Add owl:DatatypeProperty `:pullCount` (ContainerImage -> xsd:integer) to `crates/eg-core/ontology/infrastructure-v1.ttl`. SHACL: pullCount minInclusive 0, in `infrastructure-v1.shapes.ttl`. Needed by: dockerhub. | `ontology_audit_infrastructure_v1::pullCount_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain) and that a violating fixture fails the shape. |
| `EG-CONN-AUDIT-R001.15` | **entityState.** Add owl:DatatypeProperty `:entityState` (ha :Entity -> xsd:string (value or unavailable)) to `crates/eg-core/ontology/infrastructure-v1.ttl`. Needed by: home-assistant. | `ontology_audit_infrastructure_v1::entityState_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R001.16` | **unavailableSince.** Add owl:DatatypeProperty `:unavailableSince` (ha :Entity -> xsd:dateTime) to `crates/eg-core/ontology/infrastructure-v1.ttl`. Needed by: home-assistant. | `ontology_audit_infrastructure_v1::unavailableSince_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R001.17` | **desiredReplicas.** Add owl:DatatypeProperty `:desiredReplicas` (ContainerStack -> xsd:integer) to `crates/eg-core/ontology/infrastructure-v1.ttl`. SHACL: minInclusive 0, in `infrastructure-v1.shapes.ttl`. Needed by: portainer. | `ontology_audit_infrastructure_v1::desiredReplicas_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain) and that a violating fixture fails the shape. |
| `EG-CONN-AUDIT-R001.18` | **runningReplicas.** Add owl:DatatypeProperty `:runningReplicas` (ContainerStack -> xsd:integer) to `crates/eg-core/ontology/infrastructure-v1.ttl`. SHACL: minInclusive 0, in `infrastructure-v1.shapes.ttl`. Needed by: portainer. | `ontology_audit_infrastructure_v1::runningReplicas_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain) and that a violating fixture fails the shape. |
| `EG-CONN-AUDIT-R001.19` | **stackSourceRepository.** Add owl:ObjectProperty `:stackSourceRepository` (ContainerStack -> :Repository) to `crates/eg-core/ontology/infrastructure-v1.ttl`. Needed by: portainer. | `ontology_audit_infrastructure_v1::stackSourceRepository_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R001.20` | **ManagementEnvironment.** Add owl:Class `:ManagementEnvironment` (a Portainer environment (named so it does not collide with the probed :Endpoint)) to `crates/eg-core/ontology/infrastructure-v1.ttl`. Needed by: portainer. | `ontology_audit_infrastructure_v1::ManagementEnvironment_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R001.21` | **pendingUpdateCount.** Add owl:DatatypeProperty `:pendingUpdateCount` (OperatingSystem -> xsd:integer) to `crates/eg-core/ontology/infrastructure-v1.ttl`. SHACL: minInclusive 0, in `infrastructure-v1.shapes.ttl`. Needed by: systems-manager. | `ontology_audit_infrastructure_v1::pendingUpdateCount_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain) and that a violating fixture fails the shape. |
| `EG-CONN-AUDIT-R001.22` | **uptimeSeconds.** Add owl:DatatypeProperty `:uptimeSeconds` (OperatingSystem -> xsd:integer) to `crates/eg-core/ontology/infrastructure-v1.ttl`. Needed by: systems-manager. | `ontology_audit_infrastructure_v1::uptimeSeconds_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R001.23` | **recordType.** Add owl:DatatypeProperty `:recordType` (DnsRecord -> xsd:string (the existing :dnsType is typed on :DNSService, not the record)) to `crates/eg-core/ontology/infrastructure-v1.ttl`. Needed by: technitium-dns. | `ontology_audit_infrastructure_v1::recordType_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R001.24` | **recordValue.** Add owl:DatatypeProperty `:recordValue` (DnsRecord -> xsd:string) to `crates/eg-core/ontology/infrastructure-v1.ttl`. Needed by: technitium-dns. | `ontology_audit_infrastructure_v1::recordValue_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R001.25` | **lastConnectionStatus.** Add owl:DatatypeProperty `:lastConnectionStatus` (Host -> xsd:string (ok|failed)) to `crates/eg-core/ontology/infrastructure-v1.ttl`. Needed by: tunnel-manager. | `ontology_audit_infrastructure_v1::lastConnectionStatus_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R001.26` | **lastConnectionError.** Add owl:DatatypeProperty `:lastConnectionError` (Host -> xsd:string) to `crates/eg-core/ontology/infrastructure-v1.ttl`. Needed by: tunnel-manager. | `ontology_audit_infrastructure_v1::lastConnectionError_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R002` | **sdlc_lifecycle-v1.ttl additions rollup.** 23 child rows, one per missing term (plus setup rows); becomes LANDED when every child is LANDED. Children: `EG-CONN-AUDIT-R002.1` to `EG-CONN-AUDIT-R002.23`. | Children landed. |
| `EG-CONN-AUDIT-R002.1` | **shapes companion for sdlc_lifecycle-v1.** Create `crates/eg-core/ontology/sdlc_lifecycle-v1.shapes.ttl` (precedent: `finance-v1.shapes.ttl`) and set `shapes: Some(include_str!(...))` on the `sdlc_lifecycle` CoreSpec in `schema_sources.rs`. Empty shapes graph is valid; each term child below adds its own NodeShape. Reuse an existing companion without replacing its shapes; delivery still requires the named registry-load test. | `ontology_audit_sdlc_lifecycle_v1::shapes_module_loads`. |
| `EG-CONN-AUDIT-R002.2` | **Repository.** Add owl:Class `:Repository` (source repository (class used by infra:ContainerImage/aliasOf but never declared)) to `crates/eg-core/ontology/sdlc_lifecycle-v1.ttl`. Needed by: dockerhub, github, repository-manager. | `ontology_audit_sdlc_lifecycle_v1::Repository_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R002.3` | **Branch.** Add owl:Class `:Branch` (a ref in a Repository) to `crates/eg-core/ontology/sdlc_lifecycle-v1.ttl`. Needed by: gitlab, repository-manager. | `ontology_audit_sdlc_lifecycle_v1::Branch_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R002.4` | **branchOf.** Add owl:ObjectProperty `:branchOf` (Branch -> Repository) to `crates/eg-core/ontology/sdlc_lifecycle-v1.ttl`. Needed by: gitlab. | `ontology_audit_sdlc_lifecycle_v1::branchOf_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R002.5` | **headCommit.** Add owl:ObjectProperty `:headCommit` (Branch -> software:Commit) to `crates/eg-core/ontology/sdlc_lifecycle-v1.ttl`. Needed by: gitlab. | `ontology_audit_sdlc_lifecycle_v1::headCommit_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R002.6` | **isDefaultBranch.** Add owl:DatatypeProperty `:isDefaultBranch` (Branch -> xsd:boolean) to `crates/eg-core/ontology/sdlc_lifecycle-v1.ttl`. Needed by: gitlab. | `ontology_audit_sdlc_lifecycle_v1::isDefaultBranch_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R002.7` | **blockedBy.** Add owl:ObjectProperty `:blockedBy` (Ticket -> Ticket) to `crates/eg-core/ontology/sdlc_lifecycle-v1.ttl`. Needed by: atlassian. | `ontology_audit_sdlc_lifecycle_v1::blockedBy_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R002.8` | **assignedTo.** Add owl:ObjectProperty `:assignedTo` (Ticket -> :Person) to `crates/eg-core/ontology/sdlc_lifecycle-v1.ttl`. Needed by: atlassian. | `ontology_audit_sdlc_lifecycle_v1::assignedTo_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R002.9` | **ticketStatus.** Add owl:DatatypeProperty `:ticketStatus` (Ticket -> xsd:string) to `crates/eg-core/ontology/sdlc_lifecycle-v1.ttl`. Needed by: atlassian, plane. | `ontology_audit_sdlc_lifecycle_v1::ticketStatus_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R002.10` | **closesTicket.** Add owl:ObjectProperty `:closesTicket` (CodeChangeProposal -> Ticket) to `crates/eg-core/ontology/sdlc_lifecycle-v1.ttl`. Needed by: atlassian, github. | `ontology_audit_sdlc_lifecycle_v1::closesTicket_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R002.11` | **proposalState.** Add owl:DatatypeProperty `:proposalState` (CodeChangeProposal -> xsd:string (open|merged|closed)) to `crates/eg-core/ontology/sdlc_lifecycle-v1.ttl`. SHACL: sh:in, in `sdlc_lifecycle-v1.shapes.ttl`. Needed by: atlassian, github, repository-manager. | `ontology_audit_sdlc_lifecycle_v1::proposalState_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain) and that a violating fixture fails the shape. |
| `EG-CONN-AUDIT-R002.12` | **requestedReviewer.** Add owl:ObjectProperty `:requestedReviewer` (CodeChangeProposal -> :Person) to `crates/eg-core/ontology/sdlc_lifecycle-v1.ttl`. Needed by: github. | `ontology_audit_sdlc_lifecycle_v1::requestedReviewer_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R002.13` | **PipelineJob.** Add owl:Class `:PipelineJob` (one job inside a PipelineRun) to `crates/eg-core/ontology/sdlc_lifecycle-v1.ttl`. Needed by: gitlab. | `ontology_audit_sdlc_lifecycle_v1::PipelineJob_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R002.14` | **pipelineJobOf.** Add owl:ObjectProperty `:pipelineJobOf` (PipelineJob -> PipelineRun) to `crates/eg-core/ontology/sdlc_lifecycle-v1.ttl`. Needed by: gitlab. | `ontology_audit_sdlc_lifecycle_v1::pipelineJobOf_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R002.15` | **IterationCycle.** Add owl:Class `:IterationCycle` (Plane cycle / sprint) to `crates/eg-core/ontology/sdlc_lifecycle-v1.ttl`. Needed by: plane. | `ontology_audit_sdlc_lifecycle_v1::IterationCycle_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R002.16` | **inCycle.** Add owl:ObjectProperty `:inCycle` (Ticket -> IterationCycle) to `crates/eg-core/ontology/sdlc_lifecycle-v1.ttl`. Needed by: plane. | `ontology_audit_sdlc_lifecycle_v1::inCycle_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R002.17` | **WorkModule.** Add owl:Class `:WorkModule` (Plane module grouping tickets) to `crates/eg-core/ontology/sdlc_lifecycle-v1.ttl`. Needed by: plane. | `ontology_audit_sdlc_lifecycle_v1::WorkModule_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R002.18` | **inModule.** Add owl:ObjectProperty `:inModule` (Ticket -> WorkModule) to `crates/eg-core/ontology/sdlc_lifecycle-v1.ttl`. Needed by: plane. | `ontology_audit_sdlc_lifecycle_v1::inModule_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R002.19` | **ticketDueDate.** Add owl:DatatypeProperty `:ticketDueDate` (Ticket -> xsd:date) to `crates/eg-core/ontology/sdlc_lifecycle-v1.ttl`. Needed by: plane. | `ontology_audit_sdlc_lifecycle_v1::ticketDueDate_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R002.20` | **Worktree.** Add owl:Class `:Worktree` (a linked git worktree of a Repository) to `crates/eg-core/ontology/sdlc_lifecycle-v1.ttl`. Needed by: repository-manager. | `ontology_audit_sdlc_lifecycle_v1::Worktree_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R002.21` | **worktreeState.** Add owl:DatatypeProperty `:worktreeState` (Worktree -> xsd:string (active|stale|dangling|merged)) to `crates/eg-core/ontology/sdlc_lifecycle-v1.ttl`. SHACL: sh:in, in `sdlc_lifecycle-v1.shapes.ttl`. Needed by: repository-manager. | `ontology_audit_sdlc_lifecycle_v1::worktreeState_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain) and that a violating fixture fails the shape. |
| `EG-CONN-AUDIT-R002.22` | **tracksBranch.** Add owl:ObjectProperty `:tracksBranch` (Worktree -> Branch) to `crates/eg-core/ontology/sdlc_lifecycle-v1.ttl`. Needed by: repository-manager. | `ontology_audit_sdlc_lifecycle_v1::tracksBranch_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R002.23` | **proposalBranch.** Add owl:ObjectProperty `:proposalBranch` (CodeChangeProposal -> Branch) to `crates/eg-core/ontology/sdlc_lifecycle-v1.ttl`. Needed by: repository-manager. | `ontology_audit_sdlc_lifecycle_v1::proposalBranch_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R003` | **archimate-v1.ttl additions rollup.** 8 child rows, one per missing term (plus setup rows); becomes LANDED when every child is LANDED. Children: `EG-CONN-AUDIT-R003.1` to `EG-CONN-AUDIT-R003.8`. | Children landed. |
| `EG-CONN-AUDIT-R003.1` | **shapes companion for archimate-v1.** Create `crates/eg-core/ontology/archimate-v1.shapes.ttl` (precedent: `finance-v1.shapes.ttl`) and set `shapes: Some(include_str!(...))` on the `archimate` CoreSpec in `schema_sources.rs`. Empty shapes graph is valid; each term child below adds its own NodeShape. Reuse an existing companion without replacing its shapes; delivery still requires the named registry-load test. | `ontology_audit_archimate_v1::shapes_module_loads`. |
| `EG-CONN-AUDIT-R003.2` | **ApplicationComponent.** Add owl:Class `:ApplicationComponent` (subClassOf :ApplicationLayer; anchor for LeanIX/Backstage components) to `crates/eg-core/ontology/archimate-v1.ttl`. Needed by: archimate. | `ontology_audit_archimate_v1::ApplicationComponent_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R003.3` | **ArchimateRelationship.** Add owl:Class `:ArchimateRelationship` (reified relationship) to `crates/eg-core/ontology/archimate-v1.ttl`. SHACL: ArchimateRelationship: relationshipType, relSource, relTarget minCount 1, in `archimate-v1.shapes.ttl`. Needed by: archimate. | `ontology_audit_archimate_v1::ArchimateRelationship_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain) and that a violating fixture fails the shape. |
| `EG-CONN-AUDIT-R003.4` | **relationshipType.** Add owl:DatatypeProperty `:relationshipType` (ArchimateRelationship -> xsd:string (Serving|Realization|Assignment|...)) to `crates/eg-core/ontology/archimate-v1.ttl`. Needed by: archimate. | `ontology_audit_archimate_v1::relationshipType_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R003.5` | **ConfigurationItem.** Add owl:Class `:ConfigurationItem` (subClassOf :ApplicationLayer; crosswalk anchor for a ServiceNow CI) to `crates/eg-core/ontology/archimate-v1.ttl`. Needed by: servicenow. | `ontology_audit_archimate_v1::ConfigurationItem_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R003.6` | **affectsConfigurationItem.** Add owl:ObjectProperty `:affectsConfigurationItem` (Incident|Change -> ConfigurationItem) to `crates/eg-core/ontology/archimate-v1.ttl`. Needed by: servicenow. | `ontology_audit_archimate_v1::affectsConfigurationItem_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R003.7` | **incidentState.** Add owl:DatatypeProperty `:incidentState` (Incident -> xsd:string (new|in_progress|resolved|closed)) to `crates/eg-core/ontology/archimate-v1.ttl`. Needed by: servicenow. | `ontology_audit_archimate_v1::incidentState_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R003.8` | **incidentAssignee.** Add owl:ObjectProperty `:incidentAssignee` (Incident -> :Person) to `crates/eg-core/ontology/archimate-v1.ttl`. Needed by: servicenow. | `ontology_audit_archimate_v1::incidentAssignee_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R004` | **documentation-v1.ttl additions rollup.** 6 child rows, one per missing term (plus setup rows); becomes LANDED when every child is LANDED. Children: `EG-CONN-AUDIT-R004.1`, `EG-CONN-AUDIT-R004.2`, `EG-CONN-AUDIT-R004.3`, `EG-CONN-AUDIT-R004.4`, `EG-CONN-AUDIT-R004.5`, `EG-CONN-AUDIT-R004.6`. | Children landed. |
| `EG-CONN-AUDIT-R004.1` | **shapes companion for documentation-v1.** Create `crates/eg-core/ontology/documentation-v1.shapes.ttl` (precedent: `finance-v1.shapes.ttl`) and set `shapes: Some(include_str!(...))` on the `documentation` CoreSpec in `schema_sources.rs`. Empty shapes graph is valid; each term child below adds its own NodeShape. Reuse an existing companion without replacing its shapes; delivery still requires the named registry-load test. | `ontology_audit_documentation_v1::shapes_module_loads`. |
| `EG-CONN-AUDIT-R004.2` | **WebSnapshot.** Add owl:Class `:WebSnapshot` (subClassOf :Document; a captured URL snapshot) to `crates/eg-core/ontology/documentation-v1.ttl`. SHACL: WebSnapshot: sourceRef minCount 1 (the URL), in `documentation-v1.shapes.ttl`. Needed by: archivebox. | `ontology_audit_documentation_v1::WebSnapshot_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain) and that a violating fixture fails the shape. |
| `EG-CONN-AUDIT-R004.3` | **ArchiveResult.** Add owl:Class `:ArchiveResult` (one extractor outcome for a snapshot) to `crates/eg-core/ontology/documentation-v1.ttl`. SHACL: ArchiveResult: extractorName, resultStatus minCount 1, in `documentation-v1.shapes.ttl`. Needed by: archivebox. | `ontology_audit_documentation_v1::ArchiveResult_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain) and that a violating fixture fails the shape. |
| `EG-CONN-AUDIT-R004.4` | **hasArchiveResult.** Add owl:ObjectProperty `:hasArchiveResult` (WebSnapshot -> ArchiveResult) to `crates/eg-core/ontology/documentation-v1.ttl`. Needed by: archivebox. | `ontology_audit_documentation_v1::hasArchiveResult_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R004.5` | **extractorName.** Add owl:DatatypeProperty `:extractorName` (ArchiveResult -> xsd:string) to `crates/eg-core/ontology/documentation-v1.ttl`. Needed by: archivebox. | `ontology_audit_documentation_v1::extractorName_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R004.6` | **resultStatus.** Add owl:DatatypeProperty `:resultStatus` (ArchiveResult -> xsd:string (succeeded|failed|skipped)) to `crates/eg-core/ontology/documentation-v1.ttl`. Needed by: archivebox. | `ontology_audit_documentation_v1::resultStatus_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R005` | **process_intelligence-v1.ttl additions rollup.** 7 child rows, one per missing term (plus setup rows); becomes LANDED when every child is LANDED. Children: `EG-CONN-AUDIT-R005.1`, `EG-CONN-AUDIT-R005.2`, `EG-CONN-AUDIT-R005.3`, `EG-CONN-AUDIT-R005.4`, `EG-CONN-AUDIT-R005.5`, `EG-CONN-AUDIT-R005.6`, `EG-CONN-AUDIT-R005.7`. | Children landed. |
| `EG-CONN-AUDIT-R005.1` | **EpcFunction.** Add owl:Class `:EpcFunction` (subClassOf :ProcessEventType; ARIS EPC function) to `crates/eg-core/ontology/process_intelligence-v1.ttl`. Needed by: aris. | `ontology_audit_process_intelligence_v1::EpcFunction_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R005.2` | **EpcRuleOperator.** Add owl:Class `:EpcRuleOperator` (ARIS split/join (AND|OR|XOR) between EPC nodes) to `crates/eg-core/ontology/process_intelligence-v1.ttl`. Needed by: aris. | `ontology_audit_process_intelligence_v1::EpcRuleOperator_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R005.3` | **flowsTo.** Add owl:ObjectProperty `:flowsTo` (model-level control-flow edge: EPC node -> EPC node (distinct from instance-level :directlyFollows)) to `crates/eg-core/ontology/process_intelligence-v1.ttl`. Needed by: aris. | `ontology_audit_process_intelligence_v1::flowsTo_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R005.4` | **ProcessInstance.** Add owl:Class `:ProcessInstance` (one running or finished execution of a ProcessModelVersion) to `crates/eg-core/ontology/process_intelligence-v1.ttl`. Needed by: camunda. | `ontology_audit_process_intelligence_v1::ProcessInstance_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R005.5` | **instanceOfProcessModel.** Add owl:ObjectProperty `:instanceOfProcessModel` (ProcessInstance -> :ProcessModelVersion) to `crates/eg-core/ontology/process_intelligence-v1.ttl`. Needed by: camunda. | `ontology_audit_process_intelligence_v1::instanceOfProcessModel_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R005.6` | **stoppedAtActivity.** Add owl:ObjectProperty `:stoppedAtActivity` (ProcessInstance -> :ProcessEventType) to `crates/eg-core/ontology/process_intelligence-v1.ttl`. Needed by: camunda. | `ontology_audit_process_intelligence_v1::stoppedAtActivity_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R005.7` | **raisedIncident.** Add owl:ObjectProperty `:raisedIncident` (ProcessInstance -> :ApplicationEvent (archimate crosswalk)) to `crates/eg-core/ontology/process_intelligence-v1.ttl`. Needed by: camunda. | `ontology_audit_process_intelligence_v1::raisedIncident_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R006` | **government-v1.ttl additions rollup.** 11 child rows, one per missing term (plus setup rows); becomes LANDED when every child is LANDED. Children: `EG-CONN-AUDIT-R006.1` to `EG-CONN-AUDIT-R006.11`. | Children landed. |
| `EG-CONN-AUDIT-R006.1` | **shapes companion for government-v1.** Create `crates/eg-core/ontology/government-v1.shapes.ttl` (precedent: `finance-v1.shapes.ttl`) and set `shapes: Some(include_str!(...))` on the `government` CoreSpec in `schema_sources.rs`. Empty shapes graph is valid; each term child below adds its own NodeShape. Reuse an existing companion without replacing its shapes; delivery still requires the named registry-load test. | `ontology_audit_government_v1::shapes_module_loads`. |
| `EG-CONN-AUDIT-R006.2` | **RiskScenario.** Add owl:Class `:RiskScenario` (a risk scenario in a risk assessment) to `crates/eg-core/ontology/government-v1.ttl`. Needed by: ciso-assistant. | `ontology_audit_government_v1::RiskScenario_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R006.3` | **mitigatesRisk.** Add owl:ObjectProperty `:mitigatesRisk` (SecurityControl -> RiskScenario) to `crates/eg-core/ontology/government-v1.ttl`. Needed by: ciso-assistant. | `ontology_audit_government_v1::mitigatesRisk_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R006.4` | **inFramework.** Add owl:ObjectProperty `:inFramework` (SecurityControl -> core:Framework) to `crates/eg-core/ontology/government-v1.ttl`. Needed by: ciso-assistant. | `ontology_audit_government_v1::inFramework_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R006.5` | **riskAffectsService.** Add owl:ObjectProperty `:riskAffectsService` (RiskScenario -> infra:Service) to `crates/eg-core/ontology/government-v1.ttl`. Needed by: ciso-assistant. | `ontology_audit_government_v1::riskAffectsService_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R006.6` | **PrivacyAssessment.** Add owl:Class `:PrivacyAssessment` (DPIA/PIA instance) to `crates/eg-core/ontology/government-v1.ttl`. Needed by: onetrust. | `ontology_audit_government_v1::PrivacyAssessment_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R006.7` | **ProcessingActivity.** Add owl:Class `:ProcessingActivity` (a records-of-processing entry) to `crates/eg-core/ontology/government-v1.ttl`. Needed by: onetrust. | `ontology_audit_government_v1::ProcessingActivity_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R006.8` | **assessmentCovers.** Add owl:ObjectProperty `:assessmentCovers` (PrivacyAssessment -> ProcessingActivity) to `crates/eg-core/ontology/government-v1.ttl`. Needed by: onetrust. | `ontology_audit_government_v1::assessmentCovers_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R006.9` | **assessmentDueDate.** Add owl:DatatypeProperty `:assessmentDueDate` (PrivacyAssessment -> xsd:date) to `crates/eg-core/ontology/government-v1.ttl`. SHACL: assessmentDueDate minCount 1, in `government-v1.shapes.ttl`. Needed by: onetrust. | `ontology_audit_government_v1::assessmentDueDate_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain) and that a violating fixture fails the shape. |
| `EG-CONN-AUDIT-R006.10` | **assessedVendor.** Add owl:ObjectProperty `:assessedVendor` (PrivacyAssessment -> trm:Vendor) to `crates/eg-core/ontology/government-v1.ttl`. Needed by: onetrust. | `ontology_audit_government_v1::assessedVendor_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R006.11` | **assessmentStatus.** Add owl:DatatypeProperty `:assessmentStatus` (PrivacyAssessment -> xsd:string (draft|in_review|approved); overdue is derived from as-of and assessmentDueDate) to `crates/eg-core/ontology/government-v1.ttl`. Needed by: onetrust. | `ontology_audit_government_v1::assessmentStatus_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R007` | **core-foundation-v1.ttl additions rollup.** 6 child rows, one per missing term (plus setup rows); becomes LANDED when every child is LANDED. Children: `EG-CONN-AUDIT-R007.1`, `EG-CONN-AUDIT-R007.2`, `EG-CONN-AUDIT-R007.3`, `EG-CONN-AUDIT-R007.4`, `EG-CONN-AUDIT-R007.5`, `EG-CONN-AUDIT-R007.6`. | Children landed. |
| `EG-CONN-AUDIT-R007.1` | **shapes companion for core-foundation-v1.** Create `crates/eg-core/ontology/core-foundation-v1.shapes.ttl` (precedent: `finance-v1.shapes.ttl`) and set `shapes: Some(include_str!(...))` on the `core-foundation` CoreSpec in `schema_sources.rs`. Empty shapes graph is valid; each term child below adds its own NodeShape. Reuse an existing companion without replacing its shapes; delivery still requires the named registry-load test. | `ontology_audit_core_foundation_v1::shapes_module_loads`. |
| `EG-CONN-AUDIT-R007.2` | **GlossaryTerm.** Add owl:Class `:GlossaryTerm` (subClassOf :Concept; a governed business term) to `crates/eg-core/ontology/core-foundation-v1.ttl`. Needed by: egeria. | `ontology_audit_core_foundation_v1::GlossaryTerm_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R007.3` | **termAssignedTo.** Add owl:ObjectProperty `:termAssignedTo` (GlossaryTerm -> :Dataset) to `crates/eg-core/ontology/core-foundation-v1.ttl`. Needed by: egeria. | `ontology_audit_core_foundation_v1::termAssignedTo_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R007.4` | **counterparty.** Add owl:ObjectProperty `:counterparty` (Agreement -> :LegalEntity) to `crates/eg-core/ontology/core-foundation-v1.ttl`. SHACL: counterparty minCount 1, in `core-foundation-v1.shapes.ttl`. Needed by: legal-peripherals. | `ontology_audit_core_foundation_v1::counterparty_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain) and that a violating fixture fails the shape. |
| `EG-CONN-AUDIT-R007.5` | **agreementExpiresAt.** Add owl:DatatypeProperty `:agreementExpiresAt` (Agreement -> xsd:date) to `crates/eg-core/ontology/core-foundation-v1.ttl`. Needed by: legal-peripherals. | `ontology_audit_core_foundation_v1::agreementExpiresAt_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R007.6` | **hasRenewalAction.** Add owl:ObjectProperty `:hasRenewalAction` (Agreement -> :Action) to `crates/eg-core/ontology/core-foundation-v1.ttl`. Needed by: legal-peripherals. | `ontology_audit_core_foundation_v1::hasRenewalAction_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R008` | **finance-v1.ttl additions rollup.** 7 child rows, one per missing term (plus setup rows); becomes LANDED when every child is LANDED. Children: `EG-CONN-AUDIT-R008.1`, `EG-CONN-AUDIT-R008.2`, `EG-CONN-AUDIT-R008.3`, `EG-CONN-AUDIT-R008.4`, `EG-CONN-AUDIT-R008.5`, `EG-CONN-AUDIT-R008.6`, `EG-CONN-AUDIT-R008.7`. | Children landed. |
| `EG-CONN-AUDIT-R008.1` | **shapes companion for finance-v1.** Create `crates/eg-core/ontology/finance-v1.shapes.ttl` (precedent: `finance-v1.shapes.ttl`) and set `shapes: Some(include_str!(...))` on the `finance` CoreSpec in `schema_sources.rs`. Empty shapes graph is valid; each term child below adds its own NodeShape. Reuse an existing companion without replacing its shapes; delivery still requires the named registry-load test. | `ontology_audit_finance_v1::shapes_module_loads`. |
| `EG-CONN-AUDIT-R008.2` | **orderQty.** Add owl:DatatypeProperty `:orderQty` (VersionedOrder -> xsd:float) to `crates/eg-core/ontology/finance-v1.ttl`. SHACL: filledQty sh:lessThanOrEquals orderQty, in `finance-v1.shapes.ttl`. Needed by: emerald-exchange. | `ontology_audit_finance_v1::orderQty_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain) and that a violating fixture fails the shape. |
| `EG-CONN-AUDIT-R008.3` | **Budget.** Add owl:Class `:Budget` (a spending budget) to `crates/eg-core/ontology/finance-v1.ttl`. Needed by: firefly-iii. | `ontology_audit_finance_v1::Budget_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R008.4` | **budgetLimit.** Add owl:DatatypeProperty `:budgetLimit` (Budget -> xsd:decimal) to `crates/eg-core/ontology/finance-v1.ttl`. SHACL: minCount 1, in `finance-v1.shapes.ttl`. Needed by: firefly-iii. | `ontology_audit_finance_v1::budgetLimit_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain) and that a violating fixture fails the shape. |
| `EG-CONN-AUDIT-R008.5` | **LedgerTransaction.** Add owl:Class `:LedgerTransaction` (a personal-ledger transaction (not position-bound, unlike :Activity)) to `crates/eg-core/ontology/finance-v1.ttl`. Needed by: firefly-iii. | `ontology_audit_finance_v1::LedgerTransaction_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R008.6` | **chargedToBudget.** Add owl:ObjectProperty `:chargedToBudget` (LedgerTransaction -> Budget) to `crates/eg-core/ontology/finance-v1.ttl`. Needed by: firefly-iii. | `ontology_audit_finance_v1::chargedToBudget_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R008.7` | **transactionAmount.** Add owl:DatatypeProperty `:transactionAmount` (LedgerTransaction -> xsd:decimal) to `crates/eg-core/ontology/finance-v1.ttl`. Needed by: firefly-iii. | `ontology_audit_finance_v1::transactionAmount_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R009` | **personal-v1.ttl additions rollup.** 9 child rows, one per missing term (plus setup rows); becomes LANDED when every child is LANDED. Children: `EG-CONN-AUDIT-R009.1` to `EG-CONN-AUDIT-R009.9`. | Children landed. |
| `EG-CONN-AUDIT-R009.1` | **birthPlace.** Add owl:ObjectProperty `:birthPlace` (core:Person -> core:Place) to `crates/eg-core/ontology/personal-v1.ttl`. Needed by: gramps. | `ontology_audit_personal_v1::birthPlace_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R009.2` | **birthYear.** Add owl:DatatypeProperty `:birthYear` (core:Person -> xsd:gYear) to `crates/eg-core/ontology/personal-v1.ttl`. Needed by: gramps. | `ontology_audit_personal_v1::birthYear_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R009.3` | **documentType.** Add owl:DatatypeProperty `:documentType` (core:Document -> xsd:string) to `crates/eg-core/ontology/personal-v1.ttl`. Needed by: paperless-ngx. | `ontology_audit_personal_v1::documentType_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R009.4` | **dueDate.** Add owl:DatatypeProperty `:dueDate` (core:Document -> xsd:date) to `crates/eg-core/ontology/personal-v1.ttl`. Needed by: paperless-ngx. | `ontology_audit_personal_v1::dueDate_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R009.5` | **totalAmount.** Add owl:DatatypeProperty `:totalAmount` (core:Document -> xsd:decimal) to `crates/eg-core/ontology/personal-v1.ttl`. Needed by: paperless-ngx. | `ontology_audit_personal_v1::totalAmount_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R009.6` | **FileShare.** Add owl:Class `:FileShare` (a Nextcloud share of a file or folder) to `crates/eg-core/ontology/personal-v1.ttl`. Needed by: nextcloud. | `ontology_audit_personal_v1::FileShare_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R009.7` | **sharesFile.** Add owl:ObjectProperty `:sharesFile` (FileShare -> core:Document) to `crates/eg-core/ontology/personal-v1.ttl`. Needed by: nextcloud. | `ontology_audit_personal_v1::sharesFile_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R009.8` | **isPublicLink.** Add owl:DatatypeProperty `:isPublicLink` (FileShare -> xsd:boolean) to `crates/eg-core/ontology/personal-v1.ttl`. Needed by: nextcloud. | `ontology_audit_personal_v1::isPublicLink_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R009.9` | **shareExpiresAt.** Add owl:DatatypeProperty `:shareExpiresAt` (FileShare -> xsd:date) to `crates/eg-core/ontology/personal-v1.ttl`. Needed by: nextcloud. | `ontology_audit_personal_v1::shareExpiresAt_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R010` | **virtual_graph-v1.ttl additions rollup.** 3 child rows, one per missing term (plus setup rows); becomes LANDED when every child is LANDED. Children: `EG-CONN-AUDIT-R010.1`, `EG-CONN-AUDIT-R010.2`, `EG-CONN-AUDIT-R010.3`. | Children landed. |
| `EG-CONN-AUDIT-R010.1` | **NamedGraph.** Add owl:Class `:NamedGraph` (subClassOf core:Dataset; a graph in a triple store) to `crates/eg-core/ontology/virtual_graph-v1.ttl`. Needed by: jena. | `ontology_audit_virtual_graph_v1::NamedGraph_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R010.2` | **tripleCount.** Add owl:DatatypeProperty `:tripleCount` (NamedGraph -> xsd:integer) to `crates/eg-core/ontology/virtual_graph-v1.ttl`. Needed by: jena. | `ontology_audit_virtual_graph_v1::tripleCount_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R010.3` | **lastModifiedAt.** Add owl:DatatypeProperty `:lastModifiedAt` (NamedGraph -> xsd:dateTime) to `crates/eg-core/ontology/virtual_graph-v1.ttl`. Needed by: jena. | `ontology_audit_virtual_graph_v1::lastModifiedAt_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R011` | **identity-v1.ttl additions rollup.** 5 child rows, one per missing term (plus setup rows); becomes LANDED when every child is LANDED. Children: `EG-CONN-AUDIT-R011.1`, `EG-CONN-AUDIT-R011.2`, `EG-CONN-AUDIT-R011.3`, `EG-CONN-AUDIT-R011.4`, `EG-CONN-AUDIT-R011.5`. | Children landed. |
| `EG-CONN-AUDIT-R011.1` | **shapes companion for identity-v1.** Create `crates/eg-core/ontology/identity-v1.shapes.ttl` (precedent: `finance-v1.shapes.ttl`) and set `shapes: Some(include_str!(...))` on the `identity` CoreSpec in `schema_sources.rs`. Empty shapes graph is valid; each term child below adds its own NodeShape. Reuse an existing companion without replacing its shapes; delivery still requires the named registry-load test. | `ontology_audit_identity_v1::shapes_module_loads`. |
| `EG-CONN-AUDIT-R011.2` | **Application.** Add owl:Class `:Application` (declare the class already used as range of :assignedApp) to `crates/eg-core/ontology/identity-v1.ttl`. Needed by: keycloak, okta. | `ontology_audit_identity_v1::Application_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R011.3` | **OidcClient.** Add owl:Class `:OidcClient` (subClassOf :Application) to `crates/eg-core/ontology/identity-v1.ttl`. Needed by: keycloak. | `ontology_audit_identity_v1::OidcClient_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R011.4` | **roleGrantsAccessTo.** Add owl:ObjectProperty `:roleGrantsAccessTo` (IdentityRole -> Application) to `crates/eg-core/ontology/identity-v1.ttl`. Needed by: keycloak. | `ontology_audit_identity_v1::roleGrantsAccessTo_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R011.5` | **accountStatus.** Add owl:DatatypeProperty `:accountStatus` (IdentityUser -> xsd:string) to `crates/eg-core/ontology/identity-v1.ttl`. SHACL: sh:in (ACTIVE|SUSPENDED|DEACTIVATED), in `identity-v1.shapes.ttl`. Needed by: okta. | `ontology_audit_identity_v1::accountStatus_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain) and that a violating fixture fails the shape. |
| `EG-CONN-AUDIT-R012` | **orchestration-v1.ttl additions rollup.** 5 child rows, one per missing term (plus setup rows); becomes LANDED when every child is LANDED. Children: `EG-CONN-AUDIT-R012.1`, `EG-CONN-AUDIT-R012.2`, `EG-CONN-AUDIT-R012.3`, `EG-CONN-AUDIT-R012.4`, `EG-CONN-AUDIT-R012.5`. | Children landed. |
| `EG-CONN-AUDIT-R012.1` | **shapes companion for orchestration-v1.** Create `crates/eg-core/ontology/orchestration-v1.shapes.ttl` (precedent: `finance-v1.shapes.ttl`) and set `shapes: Some(include_str!(...))` on the `orchestration` CoreSpec in `schema_sources.rs`. Empty shapes graph is valid; each term child below adds its own NodeShape. Reuse an existing companion without replacing its shapes; delivery still requires the named registry-load test. | `ontology_audit_orchestration_v1::shapes_module_loads`. |
| `EG-CONN-AUDIT-R012.2` | **promptVersion.** Add owl:DatatypeProperty `:promptVersion` (Prompt -> xsd:integer) to `crates/eg-core/ontology/orchestration-v1.ttl`. Needed by: langfuse. | `ontology_audit_orchestration_v1::promptVersion_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R012.3` | **tokenCost.** Add owl:DatatypeProperty `:tokenCost` (RunTrace -> xsd:decimal) to `crates/eg-core/ontology/orchestration-v1.ttl`. SHACL: minInclusive 0, in `orchestration-v1.shapes.ttl`. Needed by: langfuse. | `ontology_audit_orchestration_v1::tokenCost_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain) and that a violating fixture fails the shape. |
| `EG-CONN-AUDIT-R012.4` | **tracePrompt.** Add owl:ObjectProperty `:tracePrompt` (RunTrace -> Prompt) to `crates/eg-core/ontology/orchestration-v1.ttl`. Needed by: langfuse. | `ontology_audit_orchestration_v1::tracePrompt_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R012.5` | **evaluationScore.** Add owl:DatatypeProperty `:evaluationScore` (RunTrace|Prompt -> xsd:decimal) to `crates/eg-core/ontology/orchestration-v1.ttl`. Needed by: langfuse. | `ontology_audit_orchestration_v1::evaluationScore_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R013` | **enterprise-v1.ttl additions rollup.** 4 child rows, one per missing term (plus setup rows); becomes LANDED when every child is LANDED. Children: `EG-CONN-AUDIT-R013.1`, `EG-CONN-AUDIT-R013.2`, `EG-CONN-AUDIT-R013.3`, `EG-CONN-AUDIT-R013.4`. | Children landed. |
| `EG-CONN-AUDIT-R013.1` | **shapes companion for enterprise-v1.** Create `crates/eg-core/ontology/enterprise-v1.shapes.ttl` (precedent: `finance-v1.shapes.ttl`) and set `shapes: Some(include_str!(...))` on the `enterprise` CoreSpec in `schema_sources.rs`. Empty shapes graph is valid; each term child below adds its own NodeShape. Reuse an existing companion without replacing its shapes; delivery still requires the named registry-load test. | `ontology_audit_enterprise_v1::shapes_module_loads`. |
| `EG-CONN-AUDIT-R013.2` | **factSheetType.** Add owl:DatatypeProperty `:factSheetType` (EAFactSheet -> xsd:string) to `crates/eg-core/ontology/enterprise-v1.ttl`. Needed by: leanix. | `ontology_audit_enterprise_v1::factSheetType_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R013.3` | **lifecyclePhase.** Add owl:DatatypeProperty `:lifecyclePhase` (EAFactSheet -> xsd:string (plan|active|phaseOut|endOfLife)) to `crates/eg-core/ontology/enterprise-v1.ttl`. SHACL: sh:in, in `enterprise-v1.shapes.ttl`. Needed by: leanix. | `ontology_audit_enterprise_v1::lifecyclePhase_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain) and that a violating fixture fails the shape. |
| `EG-CONN-AUDIT-R013.4` | **activeUserCount.** Add owl:DatatypeProperty `:activeUserCount` (EAFactSheet -> xsd:integer) to `crates/eg-core/ontology/enterprise-v1.ttl`. Needed by: leanix. | `ontology_audit_enterprise_v1::activeUserCount_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R014` | **nutrition-v1.ttl additions rollup.** 8 child rows, one per missing term (plus setup rows); becomes LANDED when every child is LANDED. Children: `EG-CONN-AUDIT-R014.1` to `EG-CONN-AUDIT-R014.8`. | Children landed. |
| `EG-CONN-AUDIT-R014.1` | **Recipe.** Add owl:Class `:Recipe` (a recipe) to `crates/eg-core/ontology/nutrition-v1.ttl`. Needed by: mealie. | `ontology_audit_nutrition_v1::Recipe_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R014.2` | **usesIngredient.** Add owl:ObjectProperty `:usesIngredient` (Recipe -> :Food) to `crates/eg-core/ontology/nutrition-v1.ttl`. Needed by: mealie. | `ontology_audit_nutrition_v1::usesIngredient_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R014.3` | **ShoppingListItem.** Add owl:Class `:ShoppingListItem` (item on a shopping list, refers to a Food) to `crates/eg-core/ontology/nutrition-v1.ttl`. Needed by: mealie. | `ontology_audit_nutrition_v1::ShoppingListItem_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R014.4` | **MealPlanEntry.** Add owl:Class `:MealPlanEntry` (a planned meal on a date for a Recipe) to `crates/eg-core/ontology/nutrition-v1.ttl`. Needed by: mealie. | `ontology_audit_nutrition_v1::MealPlanEntry_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R014.5` | **WorkoutRoutine.** Add owl:Class `:WorkoutRoutine` (a planned set of exercises) to `crates/eg-core/ontology/nutrition-v1.ttl`. Needed by: wger. | `ontology_audit_nutrition_v1::WorkoutRoutine_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R014.6` | **Exercise.** Add owl:Class `:Exercise` (an exercise) to `crates/eg-core/ontology/nutrition-v1.ttl`. Needed by: wger. | `ontology_audit_nutrition_v1::Exercise_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R014.7` | **includesExercise.** Add owl:ObjectProperty `:includesExercise` (WorkoutRoutine -> Exercise) to `crates/eg-core/ontology/nutrition-v1.ttl`. Needed by: wger. | `ontology_audit_nutrition_v1::includesExercise_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R014.8` | **WorkoutSession.** Add owl:Class `:WorkoutSession` (a performed instance of a routine with completedAt) to `crates/eg-core/ontology/nutrition-v1.ttl`. Needed by: wger. | `ontology_audit_nutrition_v1::WorkoutSession_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R015` | **action-v1.ttl additions rollup.** 1 child rows, one per missing term (plus setup rows); becomes LANDED when every child is LANDED. Children: `EG-CONN-AUDIT-R015.1`. | Children landed. |
| `EG-CONN-AUDIT-R015.1` | **failureReason.** Add owl:DatatypeProperty `:failureReason` (ActionInvocation -> xsd:string) to `crates/eg-core/ontology/action-v1.ttl`. Needed by: stirlingpdf. | `ontology_audit_action_v1::failureReason_declared` loads the module and asserts the term (ASK over rdf:type / rdfs:domain). |
| `EG-CONN-AUDIT-R017` | **Reassignments rollup.** 18 child rows, one per WRONG connector; becomes LANDED when every child is LANDED. Children: `EG-CONN-AUDIT-R017.1` to `EG-CONN-AUDIT-R017.18`. | Children landed. |
| `EG-CONN-AUDIT-R017.1` | **reassign ciso-assistant.** Correct the vocabulary of `ciso-assistant` from `governance-core-v1.shapes.ttl` to `government-v1.ttl` in `specs/connector-competency` (Coverage table in design.md and the `Vocabulary:` text of its `.1`/`.2` rows). Owner: epistemic-graph. This specification-only PR does not apply the edit. The implementation updates both design.md and requirements.md after all named producer terms have landed. Depends on the term children of `government-v1.ttl` above (:RiskScenario, :mitigatesRisk, :inFramework, :riskAffectsService). Reason: shapes-only file: its four NodeShapes target ADR, ServiceCapability, Policy and Tool; the module is registered with `ontology: None` (schema_sources.rs:589-594). | `connector_audit_reassignment::ciso_assistant` parses the exact connector row and both Vocabulary cells, resolves every required IRI from the registered target modules, and rejects a missing producer term or an unrelated-row-only edit. Decomposition lint also passes. |
| `EG-CONN-AUDIT-R017.2` | **reassign dockerhub.** Correct the vocabulary of `dockerhub` from `software-v1.ttl` to `infrastructure-v1.ttl` in `specs/connector-competency` (Coverage table in design.md and the `Vocabulary:` text of its `.1`/`.2` rows). Owner: epistemic-graph. This specification-only PR does not apply the edit. The implementation updates both design.md and requirements.md after all named producer terms have landed. Depends on the term children of `infrastructure-v1.ttl` above (:lastPushedAt, :pullCount, :Repository). Reason: software-v1 is code symbols/commits/Jira; no image or registry class. | `connector_audit_reassignment::dockerhub` parses the exact connector row and both Vocabulary cells, resolves every required IRI from the registered target modules, and rejects a missing producer term or an unrelated-row-only edit. Decomposition lint also passes. |
| `EG-CONN-AUDIT-R017.3` | **reassign egeria.** Correct the vocabulary of `egeria` from `catalog-v1.ttl` to `core-foundation-v1.ttl` in `specs/connector-competency` (Coverage table in design.md and the `Vocabulary:` text of its `.1`/`.2` rows). Owner: epistemic-graph. This specification-only PR does not apply the edit. The implementation updates both design.md and requirements.md after all named producer terms have landed. Depends on the term children of `core-foundation-v1.ttl` above (:GlossaryTerm, :termAssignedTo). Reason: catalog-v1.ttl is a 40-line generated owl:imports aggregate with zero terms. | `connector_audit_reassignment::egeria` parses the exact connector row and both Vocabulary cells, resolves every required IRI from the registered target modules, and rejects a missing producer term or an unrelated-row-only edit. Decomposition lint also passes. |
| `EG-CONN-AUDIT-R017.4` | **reassign erpnext.** Correct the vocabulary of `erpnext` from `company-v1.ttl` to `commerce-v1.ttl` in `specs/connector-competency` (Coverage table in design.md and the `Vocabulary:` text of its `.1`/`.2` rows). Owner: epistemic-graph. This specification-only PR does not apply the edit. The implementation updates both design.md and requirements.md after all named producer terms have landed. Depends on the term children of `commerce-v1.ttl` above (:CommerceFile, :Party, :PurchaseOrder, :PurchaseInvoice). Reason: no supplier, purchase order or invoice class anywhere in ontology/. | `connector_audit_reassignment::erpnext` parses the exact connector row and both Vocabulary cells, resolves every required IRI from the registered target modules, and rejects a missing producer term or an unrelated-row-only edit. Decomposition lint also passes. |
| `EG-CONN-AUDIT-R017.5` | **reassign github.** Correct the vocabulary of `github` from `software-v1.ttl` to `sdlc_lifecycle-v1.ttl` in `specs/connector-competency` (Coverage table in design.md and the `Vocabulary:` text of its `.1`/`.2` rows). Owner: epistemic-graph. This specification-only PR does not apply the edit. The implementation updates both design.md and requirements.md after all named producer terms have landed. Depends on the term children of `sdlc_lifecycle-v1.ttl` above (:requestedReviewer, :closesTicket, :proposalState, :Repository). Reason: software-v1 has :Commit/:Author but no pull request, check or pipeline class. | `connector_audit_reassignment::github` parses the exact connector row and both Vocabulary cells, resolves every required IRI from the registered target modules, and rejects a missing producer term or an unrelated-row-only edit. Decomposition lint also passes. |
| `EG-CONN-AUDIT-R017.6` | **reassign gitlab.** Correct the vocabulary of `gitlab` from `software-v1.ttl` to `sdlc_lifecycle-v1.ttl` in `specs/connector-competency` (Coverage table in design.md and the `Vocabulary:` text of its `.1`/`.2` rows). Owner: epistemic-graph. This specification-only PR does not apply the edit. The implementation updates both design.md and requirements.md after all named producer terms have landed. Depends on the term children of `sdlc_lifecycle-v1.ttl` above (:PipelineJob, :pipelineJobOf, :Branch, :branchOf, :headCommit, :isDefaultBranch). Reason: pipelines and merge requests live in sdlc_lifecycle-v1. | `connector_audit_reassignment::gitlab` parses the exact connector row and both Vocabulary cells, resolves every required IRI from the registered target modules, and rejects a missing producer term or an unrelated-row-only edit. Decomposition lint also passes. |
| `EG-CONN-AUDIT-R017.7` | **reassign gramps.** Correct the vocabulary of `gramps` from `life-v1.ttl` to `personal-v1.ttl` in `specs/connector-competency` (Coverage table in design.md and the `Vocabulary:` text of its `.1`/`.2` rows). Owner: epistemic-graph. This specification-only PR does not apply the edit. The implementation updates both design.md and requirements.md after all named producer terms have landed. Depends on the term children of `personal-v1.ttl` above (:birthPlace, :birthYear). Reason: life-v1 is biology (:Organism, :Taxon); its only person term is `:Person rdfs:subClassOf :Organism`. | `connector_audit_reassignment::gramps` parses the exact connector row and both Vocabulary cells, resolves every required IRI from the registered target modules, and rejects a missing producer term or an unrelated-row-only edit. Decomposition lint also passes. |
| `EG-CONN-AUDIT-R017.8` | **reassign home-assistant.** Correct the vocabulary of `home-assistant` from `environment-v1.ttl` to `infrastructure-v1.ttl` in `specs/connector-competency` (Coverage table in design.md and the `Vocabulary:` text of its `.1`/`.2` rows). Owner: epistemic-graph. This specification-only PR does not apply the edit. The implementation updates both design.md and requirements.md after all named producer terms have landed. Depends on the term children of `infrastructure-v1.ttl` above (:entityState, :unavailableSince). Reason: environment-v1 is ecology/weather (:Habitat, :WeatherObservation from stations and models). | `connector_audit_reassignment::home_assistant` parses the exact connector row and both Vocabulary cells, resolves every required IRI from the registered target modules, and rejects a missing producer term or an unrelated-row-only edit. Decomposition lint also passes. |
| `EG-CONN-AUDIT-R017.9` | **reassign langfuse.** Correct the vocabulary of `langfuse` from `harness-v1.ttl` to `orchestration-v1.ttl` in `specs/connector-competency` (Coverage table in design.md and the `Vocabulary:` text of its `.1`/`.2` rows). Owner: epistemic-graph. This specification-only PR does not apply the edit. The implementation updates both design.md and requirements.md after all named producer terms have landed. Depends on the term children of `orchestration-v1.ttl` above (:promptVersion, :tokenCost, :tracePrompt, :evaluationScore). Reason: harness-v1 models harness scaffolding evolution (:Hook, :Plugin, :HarnessVariant, :HarnessEdit), not LLM calls, cost or prompts. | `connector_audit_reassignment::langfuse` parses the exact connector row and both Vocabulary cells, resolves every required IRI from the registered target modules, and rejects a missing producer term or an unrelated-row-only edit. Decomposition lint also passes. |
| `EG-CONN-AUDIT-R017.10` | **reassign legal-peripherals.** Correct the vocabulary of `legal-peripherals` from `government-v1.ttl` to `core-foundation-v1.ttl` in `specs/connector-competency` (Coverage table in design.md and the `Vocabulary:` text of its `.1`/`.2` rows). Owner: epistemic-graph. This specification-only PR does not apply the edit. The implementation updates both design.md and requirements.md after all named producer terms have landed. Depends on the term children of `core-foundation-v1.ttl` above (:counterparty, :agreementExpiresAt, :hasRenewalAction). Reason: government-v1 is NIST 800-53/FedRAMP/citizen services; no contract term. | `connector_audit_reassignment::legal_peripherals` parses the exact connector row and both Vocabulary cells, resolves every required IRI from the registered target modules, and rejects a missing producer term or an unrelated-row-only edit. Decomposition lint also passes. |
| `EG-CONN-AUDIT-R017.11` | **reassign nextcloud.** Correct the vocabulary of `nextcloud` from `documentation-v1.ttl` to `personal-v1.ttl` in `specs/connector-competency` (Coverage table in design.md and the `Vocabulary:` text of its `.1`/`.2` rows). Owner: epistemic-graph. This specification-only PR does not apply the edit. The implementation updates both design.md and requirements.md after all named producer terms have landed. Depends on the term children of `personal-v1.ttl` above (:FileShare, :sharesFile, :isPublicLink, :shareExpiresAt). Reason: documentation-v1 projects repository Markdown pages (:DocumentationPage, :DocumentationRevision), no file or share. | `connector_audit_reassignment::nextcloud` parses the exact connector row and both Vocabulary cells, resolves every required IRI from the registered target modules, and rejects a missing producer term or an unrelated-row-only edit. Decomposition lint also passes. |
| `EG-CONN-AUDIT-R017.12` | **reassign onetrust.** Correct the vocabulary of `onetrust` from `governance-core-v1.shapes.ttl` to `government-v1.ttl` in `specs/connector-competency` (Coverage table in design.md and the `Vocabulary:` text of its `.1`/`.2` rows). Owner: epistemic-graph. This specification-only PR does not apply the edit. The implementation updates both design.md and requirements.md after all named producer terms have landed. Depends on the term children of `government-v1.ttl` above (:PrivacyAssessment, :ProcessingActivity, :assessmentCovers, :assessmentDueDate, :assessedVendor, :assessmentStatus). Reason: shapes-only file, as for ciso-assistant; no assessment or processing-activity class anywhere. | `connector_audit_reassignment::onetrust` parses the exact connector row and both Vocabulary cells, resolves every required IRI from the registered target modules, and rejects a missing producer term or an unrelated-row-only edit. Decomposition lint also passes. |
| `EG-CONN-AUDIT-R017.13` | **reassign paperless-ngx.** Correct the vocabulary of `paperless-ngx` from `documentation-v1.ttl` to `personal-v1.ttl` in `specs/connector-competency` (Coverage table in design.md and the `Vocabulary:` text of its `.1`/`.2` rows). Owner: epistemic-graph. This specification-only PR does not apply the edit. The implementation updates both design.md and requirements.md after all named producer terms have landed. Depends on the term children of `personal-v1.ttl` above (:documentType, :dueDate, :totalAmount). Reason: documentation-v1 is Markdown page governance; Paperless tags and correspondents already live in personal-v1. | `connector_audit_reassignment::paperless_ngx` parses the exact connector row and both Vocabulary cells, resolves every required IRI from the registered target modules, and rejects a missing producer term or an unrelated-row-only edit. Decomposition lint also passes. |
| `EG-CONN-AUDIT-R017.14` | **reassign repository-manager.** Correct the vocabulary of `repository-manager` from `software-v1.ttl` to `sdlc_lifecycle-v1.ttl` in `specs/connector-competency` (Coverage table in design.md and the `Vocabulary:` text of its `.1`/`.2` rows). Owner: epistemic-graph. This specification-only PR does not apply the edit. The implementation updates both design.md and requirements.md after all named producer terms have landed. Depends on the term children of `sdlc_lifecycle-v1.ttl` above (:Worktree, :worktreeState, :tracksBranch, :proposalBranch, :Repository, :Branch, :proposalState). Reason: no worktree or branch term in any .ttl. | `connector_audit_reassignment::repository_manager` parses the exact connector row and both Vocabulary cells, resolves every required IRI from the registered target modules, and rejects a missing producer term or an unrelated-row-only edit. Decomposition lint also passes. |
| `EG-CONN-AUDIT-R017.15` | **reassign salesforce.** Correct the vocabulary of `salesforce` from `company-v1.ttl` to `commerce-v1.ttl` in `specs/connector-competency` (Coverage table in design.md and the `Vocabulary:` text of its `.1`/`.2` rows). Owner: epistemic-graph. This specification-only PR does not apply the edit. The implementation updates both design.md and requirements.md after all named producer terms have landed. Depends on the term children of `commerce-v1.ttl` above (:Opportunity, :Contact, :Party). Reason: no CRM class in ontology/. | `connector_audit_reassignment::salesforce` parses the exact connector row and both Vocabulary cells, resolves every required IRI from the registered target modules, and rejects a missing producer term or an unrelated-row-only edit. Decomposition lint also passes. |
| `EG-CONN-AUDIT-R017.16` | **reassign servicenow.** Correct the vocabulary of `servicenow` from `enterprise-v1.ttl` to `archimate-v1.ttl` in `specs/connector-competency` (Coverage table in design.md and the `Vocabulary:` text of its `.1`/`.2` rows). Owner: epistemic-graph. This specification-only PR does not apply the edit. The implementation updates both design.md and requirements.md after all named producer terms have landed. Depends on the term children of `archimate-v1.ttl` above (:ConfigurationItem, :affectsConfigurationItem, :incidentState, :incidentAssignee). Reason: enterprise-v1 has neither :Incident nor a configuration-item class. | `connector_audit_reassignment::servicenow` parses the exact connector row and both Vocabulary cells, resolves every required IRI from the registered target modules, and rejects a missing producer term or an unrelated-row-only edit. Decomposition lint also passes. |
| `EG-CONN-AUDIT-R017.17` | **reassign stirlingpdf.** Correct the vocabulary of `stirlingpdf` from `documentation-v1.ttl` to `action-v1.ttl` in `specs/connector-competency` (Coverage table in design.md and the `Vocabulary:` text of its `.1`/`.2` rows). Owner: epistemic-graph. This specification-only PR does not apply the edit. The implementation updates both design.md and requirements.md after all named producer terms have landed. Depends on the term children of `action-v1.ttl` above (:failureReason). Reason: documentation-v1 has no operation or failure term. | `connector_audit_reassignment::stirlingpdf` parses the exact connector row and both Vocabulary cells, resolves every required IRI from the registered target modules, and rejects a missing producer term or an unrelated-row-only edit. Decomposition lint also passes. |
| `EG-CONN-AUDIT-R017.18` | **reassign twenty.** Correct the vocabulary of `twenty` from `company-v1.ttl` to `commerce-v1.ttl` in `specs/connector-competency` (Coverage table in design.md and the `Vocabulary:` text of its `.1`/`.2` rows). Owner: epistemic-graph. This specification-only PR does not apply the edit. The implementation updates both design.md and requirements.md after all named producer terms have landed. Depends on the term children of `commerce-v1.ttl` above (:Contact, :Opportunity, :Party, :lastActivityAt). Reason: no CRM class in ontology/. | `connector_audit_reassignment::twenty` parses the exact connector row and both Vocabulary cells, resolves every required IRI from the registered target modules, and rejects a missing producer term or an unrelated-row-only edit. Decomposition lint also passes. |

#### Reproduce historical lineage validation

Run this standard-library check from the repository root with both historical commits available.
It checks exact mappings, ownership, unique IDs, original acceptance, generated retirement, and unchanged active records.
This check validates documentation lineage only.

```python
import json
import re
import subprocess
from pathlib import Path

BASE = 'a6f9ff712f71dd3799963e85dfc7a891dc9bef34'
SOURCE = 'b52f912f'
PREFIX = 'EG-CONN-AUDIT-R'
OWNERS = ('connector-competency', 'supply-chain-vocabulary')

def historical(ref, path):
    return subprocess.check_output(['git', 'show', f'{ref}:{path}'], text=True)

def rows(text):
    return {m[1]: line for line in text.splitlines()
            if (m := re.match(r'\| `([^`]+)` \|', line))}

old = rows(historical(SOURCE, 'specs/connector-vocabulary-audit/requirements.md'))
assert len(old) == 173
noncommercial = {1: 11, 2: 15, 3: 17, 5: 23, 6: 24, 7: 25, 8: 26,
                 9: 31, 10: 33, 11: 40, 12: 43, 13: 46, 14: 52, 16: 57, 17: 59}
commercial = {None: [''], 1: ['.13'], 2: ['.14'], 3: ['.15'], 4: ['.7'],
              5: ['.8'], 6: ['.9'], 7: ['.10', '.16', '.17'], 8: ['.11'], 9: ['.12']}
all_rows = {}
for path in Path('specs').glob('*/requirements.md'):
    for key, line in rows(path.read_text()).items():
        assert key not in all_rows, key
        all_rows[key] = (path.parent.name, line)
assert {key for key in all_rows if key.startswith(PREFIX)} == set(old)
for key, original in old.items():
    suffix = key.removeprefix(PREFIX).split('.')
    number = int(suffix[0])
    child = int(suffix[1]) if len(suffix) > 1 else None
    owner = OWNERS[1] if number == 16 else OWNERS[0]
    if number <= 15:
        targets = [f'EG-CQ-CONN-R{number + 67:03}' + (f'.{child}' if child else '')]
    elif number == 16:
        targets = ['EG-SUPPLY-VOCAB-R003' + item for item in commercial[child]]
    elif child is None:
        targets = [f'EG-CQ-CONN-R{v:03}.3' for v in noncommercial.values()] + ['EG-CQ-CONN-R083']
    else:
        targets = [f'EG-CQ-CONN-R{noncommercial[child]:03}.3'] if child in noncommercial else ['EG-CQ-CONN-R083']
    actual_owner, line = all_rows[key]
    assert actual_owner == owner and '**RETIRED: replaced by ' in line, key
    assert re.findall(r'`([^`]+)`', line)[1:] == targets, key
    assert all(target in all_rows for target in targets), key
    assert original in Path(f'specs/{owner}/plan.md').read_text(), key
for owner in OWNERS:
    path = f'specs/{owner}'
    before = rows(historical(BASE, path + '/requirements.md'))
    after = rows(Path(path + '/requirements.md').read_text())
    assert {k: v for k, v in after.items() if not k.startswith(PREFIX)} == before
    previous = json.loads(historical(BASE, path + '/status.json'))['requirements']
    current = json.loads(Path(path + '/status.json').read_text())['requirements']
    assert [r for r in current if not r['id'].startswith(PREFIX)] == previous
    retired = [r for r in current if r['id'].startswith(PREFIX)]
    assert all(r['delivery_state'] == 'RETIRED' for r in retired)
print('PASS: 173 exact, unique retirement mappings; original acceptance retained; active rows and evidence unchanged')
```

Validation on 2026-10-11: all assertions passed for 173 historical IDs.
The shared spec-standard, spec-decomposition, and spec-status checks also passed.
