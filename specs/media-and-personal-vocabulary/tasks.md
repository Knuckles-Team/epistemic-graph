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
