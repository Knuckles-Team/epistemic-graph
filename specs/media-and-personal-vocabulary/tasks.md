# EG-MEDIA-VOCAB — Implementation tasks

Governing spec: [spec.md](spec.md). Design: [plan.md](plan.md).

Tasks mirror requirements.md rows; check a task only when its evidence is linked.

- [ ] **EG-MEDIA-VOCAB-R001:** Media and personal-content vocabulary: media-v1.
- [ ] **EG-MEDIA-VOCAB-R002:** Content-channel vocabulary: content-v1.
- [ ] **EG-MEDIA-VOCAB-R003:** Cross-source identity keys.
- [ ] **EG-MEDIA-VOCAB-R004:** SHACL shapes.
- [ ] **EG-MEDIA-VOCAB-R005:** arr virtual-graph mapping.
- [ ] **EG-MEDIA-VOCAB-R006:** jellyfin virtual-graph mapping.
- [ ] **EG-MEDIA-VOCAB-R007:** audiobookshelf virtual-graph mapping.
- [ ] **EG-MEDIA-VOCAB-R008:** qbittorrent virtual-graph mapping.
- [ ] **EG-MEDIA-VOCAB-R009:** rom-manager virtual-graph mapping.
- [ ] **EG-MEDIA-VOCAB-R010:** owncast virtual-graph mapping.
- [ ] **EG-MEDIA-VOCAB-R011:** media-downloader virtual-graph mapping.
- [ ] **EG-MEDIA-VOCAB-R012:** audio-transcriber virtual-graph mapping.
- [ ] **EG-MEDIA-VOCAB-R013:** freshrss virtual-graph mapping.
- [ ] **EG-MEDIA-VOCAB-R014:** scholarx virtual-graph mapping.
- [ ] **EG-MEDIA-VOCAB-R015:** searxng virtual-graph mapping.
- [ ] **EG-MEDIA-VOCAB-R016:** pulselink virtual-graph mapping.
- [ ] **EG-MEDIA-VOCAB-R017:** postiz virtual-graph mapping.
- [ ] **EG-MEDIA-VOCAB-R018:** listmonk virtual-graph mapping.
- [ ] **EG-MEDIA-VOCAB-R019:** mattermost virtual-graph mapping.
- [ ] **EG-MEDIA-VOCAB-R020:** Cross-source worked examples.
- [ ] **EG-MEDIA-VOCAB-R021:** Cross-repo, owner arr-mcp: per-title 'last searched indexer' (Radarr/Sonarr keep no per-title search log; Prowlarr history is query-keyed, not title-keyed): add a prowlarr_action history-by-title projection.
- [ ] **EG-MEDIA-VOCAB-R022:** Cross-repo, owner audiobookshelf-mcp: user listening progress: add a library_operations or users action for media progress per user and item.
- [ ] **EG-MEDIA-VOCAB-R023:** Cross-repo, owner media-downloader: see media-downloader B1.
- [ ] **EG-MEDIA-VOCAB-R024:** Cross-repo, owner rom-manager: reference-set verification: add a romm_roms verify action returning per-ROM checksum verdict.
- [ ] **EG-MEDIA-VOCAB-R025:** Cross-repo, owner media-downloader: see media-downloader B1.
- [ ] **EG-MEDIA-VOCAB-R026:** Cross-repo, owner owncast-agent: past broadcast list with start/end and peak viewers: add an owncast_internal stream-history action or persist sessions.
- [ ] **EG-MEDIA-VOCAB-R027:** Cross-repo, owner media-downloader: download history: add a list_downloads operation returning job id, source URL, status, error, completedAt, output path.
- [ ] **EG-MEDIA-VOCAB-R028:** Cross-repo, owner audio-transcriber: transcript list/get operation by recording id and date.
- [ ] **EG-MEDIA-VOCAB-R029:** Cross-repo, owner audio-transcriber: expose avg_logprob (confidence) per segment.
- [ ] **EG-MEDIA-VOCAB-R030:** Cross-repo, owner scholarx: citing-papers operation: add sx_search action 'citations' returning papers that cite a given id.
- [ ] **EG-MEDIA-VOCAB-R031:** Cross-repo, owner searxng-mcp: pass through per-engine result counts and unresponsive_engines from the SearXNG response.
- [ ] **EG-MEDIA-VOCAB-R032:** Cross-repo, owner arr-mcp: rename connector :Episode to mv:TelevisionEpisode subclass (collision with core :Episode).
- [ ] **EG-MEDIA-VOCAB-R033:** Cross-repo, owner jellyfin-mcp and audiobookshelf-mcp: stop declaring :Author in the shared namespace; use mv:contributor to :Person.

<!-- spec-definition-patch:owner-batch-20261012-media-and-personal-vocabulary -->
## R002.5 owner implementation and held-dependency tasks — 2026-10-12

- [ ] **EG-MEDIA-VOCAB-R002.5 (EG:50):** Define receipt-bound `PaperRecord` consumption through existing `Method::SourceIngest`. Validate SDK exact identity, matching paper/revision, and complete authorized `DocumentReceipt`. Assert available metadata with source-field provenance. Make exact replay idempotent. Refuse identity/receipt/artifact/provenance mismatch per paper. Retain healthy siblings. Preserve R002.1/.4, SDK R005.1/.2/.4, and EG-REPO-INGEST-R002.
- [ ] **EG-MEDIA-VOCAB-R002.5 acceptance:** Read back facts and provenance. Prove idempotent replay. Refuse one unidentified row while committing its healthy sibling. Test receipt revision/digest/artifact/provenance mismatches before graph writes. Confirm count-only records create no citation edges. Citation edges remain held because SDK lacks reference IDs and paper-serving registration. SDK must define and serve reference IDs and target identity normalization before that case passes.
- [ ] **EG-MEDIA-VOCAB-R002.5 status:** SPECIFIED until implementation and exact served source/readback evidence are independently reviewed. Source hash `b6d12a184f1c5812c8f4fd56a0a299e48381e85ad9b18af4eb0ed19faf3d7e0b`.
<!-- /spec-definition-patch -->

<!-- spec-definition-patch:final-mappings-20261012 -->
## Final mapped definition tasks — 2026-10-12

- [ ] `EG-MEDIA-VOCAB-R001.100`: implement the typed fields, decision/refusal behavior and owner boundary stated in requirements; preserve `EG-MEDIA-VOCAB-R001`.
- [ ] `EG-MEDIA-VOCAB-R001.100` acceptance: Use an authorized tagged media item. Read back tag provenance and media revision. Query with allowed and denied principals. Assert only authorized results are returned. Refuse missing producer/evidence provenance and stale media revision. Keep vision-producer testing pending until its identity and contract are resolved.
<!-- /spec-definition-patch -->


<!-- spec-definition-patch:owner-dispatch-20261012-tasks -->
## Cross-owner transcript and bookmark tasks — 2026-10-12

- [ ] `EG-MEDIA-VOCAB-R001.101`: Define ordered immutable transcript revisions, corrections, and finalization. Bound finalized summary context with positive finite caps. Limits are 128 spans, 16384 bytes per segment, and 8192 tokens. Require provider admission. Validate summary receipts and citations through durable commit authority. Refuse absent or invalid caps.
- [ ] `EG-MEDIA-VOCAB-R001.102`: Define bookmark semantic-search and relation reads. Set positive finite caps of 100 hits, 500 relations, and 1048576 response bytes. Refuse absent, invalid, or exceeded caps. Reuse SDK SourceIngest R005.7-.12, existing SourceIngest/ChangeEnvelope commit, SemanticSearch, and UQL.
- [ ] Preserve EG's no-generative-weights boundary and AU receipt authority. Bind source/index/snapshot revisions. Preserve unknown metadata and authorization filtering. Independently read back stored results.
<!-- /spec-definition-patch -->
