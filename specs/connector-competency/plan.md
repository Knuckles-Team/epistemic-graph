# EG-CQ-CONN — Design and implementation plan

Status: PROPOSED. Owner: epistemic-graph. The governing behavior and requirement
IDs are in [spec.md](spec.md) and [requirements.md](requirements.md). The complete
question and vocabulary inventory remains in [design.md](plan.md).

## Architecture and reuse

The engine answers connector competency questions through its existing
[federated query contract](../federated-query-and-reasoning/spec.md) and
[unified data plane](../unified-data-plane/spec.md). Reuse their ontology-driven
source selection, approved virtual mappings, query planning, and provenance
interfaces; do not introduce a separate connector-specific query planner.

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
| R011 | ciso-assistant | Which controls in an applied framework are non-compliant and which risk scenarios do they mitigate? | Which CISO Assistant risks map to a ServiceNow CMDB service with an open incident? (onetrust, servicenow; control / risk) | governance-core-v1.shapes |
| R012 | clarity | Which pages have the highest rage-click or dead-click rate over the last week? | Which pages with a spike in rage clicks were changed by a pull request merged in the previous week? (plane, github; web page / release) | MISSING |
| R013 | container-manager | Which containers or pods are restarting repeatedly, and from which image? | Which running images have a newer tag on Docker Hub than the one deployed? (dockerhub, portainer; container image) | infrastructure-v1 |
| R014 | data-science | Which trained models have validation metrics below their baseline run, and on which dataset? | Which fine-tuned models have no evaluation dataset trace in Langfuse and no stored artifact in the object store? (langfuse, objectstore; model / dataset) | MISSING |
| R015 | dockerhub | Which repositories in an organization have not been pushed to in a year, and what are their pull counts? | Which Docker Hub repositories correspond to a GitHub repository with recent commits but no recent image push? (container-manager, github; container image / repository) | software-v1 |
| R016 | documentdb | Which collections have the most documents and which fields are missing in more than half of them? | Which documentdb collections duplicate a table in the SQL database, and which side is newer? (sql, objectstore; dataset / collection) | MISSING |
| R017 | egeria | Which glossary terms are linked to a data asset with a confidential classification? | Which Egeria-cataloged assets correspond to a live SQL table, and which are cataloged but missing? (sql, jena; data asset) | catalog-v1 |
| R018 | emerald-exchange | Which open orders on an exchange are partially filled and what is their remaining quantity? | Which exchange fills correspond to a deposit or withdrawal recorded in the Firefly III ledger? (firefly-iii, home-assistant; account / currency) | finance-v1 |
| R019 | erpnext | Which open purchase orders are late, and from which supplier? | Which ERPNext suppliers with late orders are also marked as active accounts in the Twenty CRM or paid in Firefly III? (twenty, firefly-iii; party (supplier / customer)) | company-v1 |
| R020 | fan-manager | Which fans ran above 80 percent duty while the host temperature stayed above its threshold? | Which hosts had fan duty at maximum during a window in which Grafana alerted on temperature? (systems-manager, lgtm; host) | infrastructure-v1 |
| R021 | firefly-iii | Which budgets were exceeded this month, and by which transactions? | Which Firefly III expenses match an ERPNext purchase invoice by amount and date? (erpnext, emerald-exchange; account / transaction) | finance-v1 |
| R022 | freshrss | Which unread articles from a given feed were published in the last 48 hours? | Which FreshRSS articles also appear in SearXNG results for the same query, and which are new? (archivebox, searxng; URL / article) | MISSING |
| R023 | github | Which open pull requests have failing checks and no reviewer assigned? | Which GitHub pull requests close a Jira issue that is still in an open state? (gitlab, atlassian; repository / change request) | software-v1 |
| R024 | gitlab | Which pipelines on the default branch failed in the last day, and at which job? | Which repositories exist in GitLab and GitHub with diverging default-branch heads? (github, repository-manager; repository / pipeline) | software-v1 |
| R025 | gramps | Which people in the tree were born in a given place before a given year, and who are their parents? | Which Gramps people have a photo or document reference that exists as a file in Nextcloud? (nextcloud; person / media) | life-v1 |
| R026 | home-assistant | Which sensors reported a value outside their normal range in the last 24 hours? | Which Home Assistant devices went unavailable at the same time as an Uptime Kuma monitor failure? (fan-manager, uptime; device / host) | environment-v1 |
| R027 | jellyfin | Which movies in a library have never been played and were added more than a year ago? | Which Jellyfin items have no matching Radarr or Sonarr entry, and so cannot be re-fetched? (arr, qbittorrent; media item (title)) | MISSING |
| R028 | jena | Which named graphs contain the most triples and which have no recent update? | Which Jena named graphs describe the same entities as an Egeria-cataloged asset? (egeria, sql; graph / dataset) | virtual_graph-v1 |
| R029 | kafka | Which consumer groups lag by more than a threshold on which topic partitions? | Which Kafka consumer groups with sustained lag belong to a service that Grafana is alerting on? (langfuse, lgtm; topic / service) | MISSING |
| R030 | keycloak | Which clients can a given user sign in to, and through which role? | Which people have access to a service that has an open security incident? (okta, servicenow; person / service) | identity-v1 |
| R031 | langfuse | Which traces had the highest token cost and which prompt version produced them? | Which prompt versions with falling evaluation scores were last changed by a merged GitHub pull request? (data-science, github; prompt / model) | harness-v1 |
| R032 | leanix | Which fact sheets of type application have lifecycle phase end-of-life but still have active users? | Which end-of-life LeanIX applications still have an active ServiceNow CI and open incidents? (archimate, servicenow; application) | enterprise-v1 |
| R033 | legal-peripherals | Which contracts expire within 90 days and have no renewal action recorded? | Which expiring contracts have a scanned copy in Paperless-ngx and a matching OneTrust vendor assessment? (paperless-ngx, onetrust; contract / document) | government-v1 |
| R034 | lgtm | Which alerts are firing right now, with what severity and for how long? | Which firing Grafana alerts have no open ServiceNow incident, and which incidents have no alert? (uptime, servicenow; service / alert) | infrastructure-v1 |
| R035 | listmonk | Which campaigns had an open rate below the list average, and to which lists were they sent? | Which subscribers on a Listmonk list have no matching Twenty contact, and which contacts are unsubscribed? (twenty, postiz; contact / audience) | MISSING |
| R036 | mattermost | Which channels have had no posts in 90 days but still have more than ten members? | Which Mattermost members have no active identity-provider account, and which people lack a Mattermost account? (keycloak, okta; person) | MISSING |
| R037 | mealie | Which recipes use an ingredient that is also on the current shopping list? | Which planned Mealie meals exceed the daily calorie target set in wger, and what did the groceries cost in Firefly III? (wger, firefly-iii; food / ingredient) | nutrition-v1 |
| R038 | media-downloader | Which download jobs failed in the last week, with what error, for which source URL? | Which completed downloads were never added to Jellyfin or Audiobookshelf? (audiobookshelf, jellyfin; media item / URL) | MISSING |
| R039 | microsoft | Which meetings tomorrow overlap, and which attendees are double-booked? | Which attendees of an upcoming meeting have no identity-provider account or Mattermost presence? (okta, mattermost; person) | calendar-v1 |
| R040 | nextcloud | Which shared folders are accessible by external links and when do those shares expire? | Which publicly shared Nextcloud files also exist as Paperless-ngx documents tagged confidential? (keycloak, paperless-ngx; file / person) | documentation-v1 |
| R041 | objectstore | Which buckets hold the most data and which objects have not been read in a year? | Which object-store artifacts are not referenced by any model or dataset in the data-science connector? (data-science, documentdb; dataset / artifact) | MISSING |
| R042 | okta | Which users hold a given application assignment through which group? | Which Okta users were deactivated but still hold a Keycloak role or an open ServiceNow assignment? (keycloak, servicenow; person / role) | identity-v1 |
| R043 | onetrust | Which privacy assessments are overdue and which processing activities do they cover? | Which vendors with an overdue OneTrust assessment have open ERPNext purchase orders? (ciso-assistant, erpnext; processing activity / vendor) | governance-core-v1.shapes |
| R044 | openbao | Which secret paths have not been rotated in 180 days, and which policies grant read on them? | Which services running in container-manager read a secret path that has no recent rotation? (keycloak, container-manager; secret / service) | MISSING |
| R045 | owncast | Which streams had the highest peak viewer count and how long did they run? | Which Owncast broadcasts were announced through a Postiz post, and what was the audience versus the post reach? (postiz, mattermost; broadcast / audience) | MISSING |
| R046 | paperless-ngx | Which documents tagged as invoice have no correspondent or no due date? | Which Paperless-ngx invoices have no matching ERPNext purchase invoice or Firefly III expense? (erpnext, firefly-iii; document / invoice) | documentation-v1 |
| R047 | plane | Which work items in the active cycle are overdue and which module do they belong to? | Which Plane work items link to a GitHub pull request that has merged but are not marked done? (github, atlassian; work item) | sdlc_lifecycle-v1 |
| R048 | portainer | Which stacks have services with fewer running replicas than desired, and on which endpoint? | Which Portainer stacks deploy from a Git repository whose latest GitLab pipeline failed? (container-manager, gitlab; stack / repository) | infrastructure-v1 |
| R049 | postiz | Which scheduled posts failed to publish and on which channel? | Which Postiz posts promote a campaign that Listmonk also sent, and which got the better engagement? (owncast, listmonk; campaign / channel) | MISSING |
| R050 | pulselink | Which videos on a topic had transcripts mentioning a given term this week? | Which topics appear in PulseLink video transcripts and SearXNG news but not in any archived page? (searxng, audio-transcriber; topic / transcript) | MISSING |
| R051 | qbittorrent | Which torrents are stalled, with what ratio and how long since last activity? | Which stalled torrents belong to a wanted Radarr or Sonarr title, and so block it from import? (arr, jellyfin; media item (torrent)) | MISSING |
| R052 | repository-manager | Which worktrees are stale or dangling relative to their base branch? | Which local worktrees track a branch whose pull request has already been merged on GitHub or GitLab? (github, gitlab; repository / branch) | software-v1 |
| R053 | rom-manager | Which ROMs in a platform collection fail a checksum match against the reference set? | Which ROMs were obtained by a media-downloader job and which are still unconverted? (media-downloader, jellyfin; game / file) | MISSING |
| R054 | salesforce | Which open opportunities close this quarter, with what amount and owner? | Which Salesforce accounts have an open ServiceNow incident and an opportunity closing this quarter? (twenty, servicenow; account / customer) | company-v1 |
| R055 | scholarx | Which papers cite a given paper and were published in the last two years? | Which papers on a topic are in ScholarX but were never surfaced by a SearXNG query for that topic? (searxng, langfuse; paper / topic) | MISSING |
| R056 | searxng | Which engines returned no results for a given query, and which returned the most? | Which URLs from a SearXNG query are not yet archived in ArchiveBox? (freshrss, archivebox; URL / query) | MISSING |
| R057 | servicenow | Which incidents are open on a given configuration item and who is assigned? | Which open incidents on a CI also show a Grafana alert and a failing Uptime Kuma monitor? (okta, lgtm; configuration item / service) | enterprise-v1 |
| R058 | sql | Which tables have no primary key, and which columns reference tables that do not exist? | Which SQL tables are not cataloged in Egeria, and which cataloged tables no longer exist? (egeria, documentdb; table / data asset) | MISSING |
| R059 | stirlingpdf | Which operations failed on which PDFs and with what error? | Which PDFs processed by Stirling-PDF were stored in Paperless-ngx or Nextcloud, and which were not? (paperless-ngx, nextcloud; document) | documentation-v1 |
| R060 | systems-manager | Which hosts have pending OS updates and what is their uptime? | Which hosts with pending updates have no Ansible Tower patch job scheduled and no ServiceNow change? (ansible-tower, servicenow; host (configuration item)) | infrastructure-v1 |
| R061 | technitium-dns | Which zones contain records pointing to an address that no host answers on? | Which DNS A records have no Caddy route and no Uptime Kuma monitor? (caddy, uptime; hostname) | infrastructure-v1 |
| R062 | tunnel-manager | Which SSH hosts in the inventory failed their last connection, and with what error? | Which hosts that tunnel-manager cannot reach are still listed as managed in Ansible Tower? (ansible-tower, systems-manager; host) | infrastructure-v1 |
| R063 | twenty | Which people at a company have no logged activity in the last 60 days while a deal is open? | Which Twenty contacts also exist as Salesforce contacts with different email addresses? (salesforce, listmonk; person / company) | company-v1 |
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
| ciso-assistant | governance | governance-core-v1.shapes.ttl |
| clarity | data | MISSING |
| container-manager | IT operations | infrastructure-v1.ttl |
| data-science | data | MISSING |
| dockerhub | engineering | software-v1.ttl |
| documentdb | data | MISSING |
| egeria | governance | catalog-v1.ttl |
| emerald-exchange | finance | finance-v1.ttl |
| erpnext | business | company-v1.ttl |
| fan-manager | home | infrastructure-v1.ttl |
| firefly-iii | finance | finance-v1.ttl |
| freshrss | knowledge | MISSING |
| github | engineering | software-v1.ttl |
| gitlab | engineering | software-v1.ttl |
| gramps | knowledge | life-v1.ttl |
| home-assistant | home | environment-v1.ttl |
| jellyfin | media | MISSING |
| jena | data | virtual_graph-v1.ttl |
| kafka | data | MISSING |
| keycloak | identity | identity-v1.ttl |
| langfuse | engineering | harness-v1.ttl |
| leanix | business | enterprise-v1.ttl |
| legal-peripherals | governance | government-v1.ttl |
| lgtm | IT operations | infrastructure-v1.ttl |
| listmonk | business | MISSING |
| mattermost | business | MISSING |
| mealie | home | nutrition-v1.ttl |
| media-downloader | media | MISSING |
| microsoft | business | calendar-v1.ttl |
| nextcloud | knowledge | documentation-v1.ttl |
| objectstore | data | MISSING |
| okta | identity | identity-v1.ttl |
| onetrust | governance | governance-core-v1.shapes.ttl |
| openbao | IT operations | MISSING |
| owncast | media | MISSING |
| paperless-ngx | knowledge | documentation-v1.ttl |
| plane | engineering | sdlc_lifecycle-v1.ttl |
| portainer | IT operations | infrastructure-v1.ttl |
| postiz | business | MISSING |
| pulselink | knowledge | MISSING |
| qbittorrent | media | MISSING |
| repository-manager | engineering | software-v1.ttl |
| rom-manager | media | MISSING |
| salesforce | business | company-v1.ttl |
| scholarx | knowledge | MISSING |
| searxng | knowledge | MISSING |
| servicenow | IT operations | enterprise-v1.ttl |
| sql | data | MISSING |
| stirlingpdf | knowledge | documentation-v1.ttl |
| systems-manager | IT operations | infrastructure-v1.ttl |
| technitium-dns | IT operations | infrastructure-v1.ttl |
| tunnel-manager | IT operations | infrastructure-v1.ttl |
| twenty | business | company-v1.ttl |
| uptime | IT operations | infrastructure-v1.ttl |
| vector | data | MISSING |
| wger | home | nutrition-v1.ttl |

Connectors with no matching vocabulary: **23** of 66 (arr, audiobookshelf, audio-transcriber, clarity, data-science, documentdb, freshrss, jellyfin, kafka, listmonk, mattermost, media-downloader, objectstore, openbao, owncast, postiz, pulselink, qbittorrent, rom-manager, scholarx, searxng, sql, vector).
