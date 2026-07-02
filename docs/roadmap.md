# Universal-DB parity roadmap

epistemic-graph converges many modalities under one durable engine and one planner. This page tracks the
**remaining gaps** to full drop-in parity, with an honest status. It is the companion to the
[capability matrix](capabilities.md) and the [subsystems reference](architecture/subsystems.md). Status:
**🔶 in-progress** · **🗺 designed, not started**.

> The **Universal-DB Program** (EG-045..297) that this page used to track is essentially complete: the
> whole SQL/SPARQL/OWL/Cypher/GraphQL parity backlog shipped, and this cycle (waves 18–22) added the
> multi-wire adapters, the message broker, the observability stack, GIS, tensors, streams, the LLM
> KV-cache, and the agent-memory primitives. Those are now in [**Shipped in 2.2.0**](#shipped-in-220)
> below. What remains is a short list of **genuinely-deferred** items.

## Forward roadmap — genuinely deferred

| Item | Status | Notes |
|------|:------:|-------|
| **ROS2 / DDS bridge** | 🗺 | A robotics transport bridge (DDS/RTPS) onto the broker + tensor/stream modalities; no wire adapter yet. |
| **Admin console UI** | 🗺 | A browser admin surface (tenants, shards, RBAC, backup/PITR). The engine exposes the APIs; the UI is unbuilt. |
| **Live dashboards UI** | 🗺 | A Grafana-style dashboard front-end over the PromQL/logs/traces query APIs (EG-172/162/163). The query side ships; the UI does not. |
| **Python LMCache connector** | 🗺 | A shipped `pip`-installable vLLM/LMCache remote-backend client for the EG-187 KV-cache endpoint (the server surface + contract exist; the packaged Python connector does not). |
| **Cross-region async replica** | 🗺 | Asynchronous (non-Raft) cross-region follower replication, beyond the synchronous multi-Raft groups + super-cluster federated *read* (EG-243). |
| **Exactly-once broker delivery** | 🗺 | Idempotent-producer / transactional exactly-once semantics on top of the at-least-once publisher-confirms + acks (EG-284). |
| **Raster tiles / Shapefile / KML** | 🗺 | GIS I/O beyond the shipped GeoJSON/WKB/GPX + MVT vector tiles (EG-264/265): raster tile pyramids, ESRI Shapefile, and KML readers/writers. |
| **PL/pgSQL procedural bodies** | 🗺 | Stored-procedure/function *procedural* execution (loops, variables, control flow) beyond the shipped SQL views + DDL. |
| **Memory → weights distillation** | 🗺 | Distilling consolidated agent-memory (EG-220/221) into model weights (a fine-tune/LoRA export), beyond the retrieval-time context assembly (EG-195). |

---

## Shipped in 2.2.0

Everything below was previously listed here as roadmap and is now ✅ — see the
[capability matrix](capabilities.md) for the per-operation evidence and [concepts](concepts.md) for the
authoritative `CONCEPT:EG-*` definitions.

### SQL — full Postgres + multi-wire parity
- Compound/complex DML WHERE, `INSERT … SELECT`, `UPDATE…FROM`/`DELETE…USING` (EG-045/046/047).
- `ON CONFLICT` upsert + `RETURNING` (EG-048); wire transactions `BEGIN`/`COMMIT` mixing graph + user tables
  with RYOW + `TransactionStatus` (EG-049); `CREATE VIEW` / `DROP VIEW` durable catalog (EG-072).
- SQL DDL + arbitrary user tables + `COPY`.

### SPARQL — full Stardog/GraphDB parity
- Content negotiation (XML/CSV/TSV/Turtle; EG-050), rich FILTER (regex/arithmetic/`IN`/builtins; EG-053),
  `FROM`/`FROM NAMED` (EG-054), sub-SELECT · SERVICE federation · MINUS · negated property sets
  (EG-051/052/055/056), SPO/POS triple index + selectivity join-ordering (EG-057).
- `ASK`/`CONSTRUCT`/`DESCRIBE` + `UPDATE` + the W3C `/sparql` endpoint; true named-graph quad dataset.

### OWL / RDF validation
- `rdfs:range` in EL completion (EG-058), OWL-DL tableau (EG-059), SWRL/RuleML rules (EG-060).
- **SHACL Core** validation (`eg-shacl`, EG-132), **ShEx** validation (`eg-shex`, EG-133), and
  **Integrity Constraint Validation** with guard mode (EG-146).

### Graph — full Neo4j parity + Bolt wire
- Cypher `REMOVE` (EG-061), `ORDER BY`/`SKIP`/`WITH`/`OPTIONAL MATCH`/`OR`/aggregation/`DISTINCT` (EG-062),
  var-length + fixed hops + path binding (EG-063); writes (`CREATE`/`MERGE`/`SET`/`DELETE`).
- **Neo4j Bolt v4.4** wire adapter so native drivers connect (EG-159).

### GraphQL
- Subscriptions over CDC (EG-064), fragments/variables/directives (EG-065), Relay pagination (EG-066).
- **Apollo Federation v2 subgraph** (EG-295) + **enterprise hardening** (APQ, depth/cost limits,
  introspection toggle; EG-296); GraphQL mutations.

### Time-series · Vector · Blob
- `Op::Window` execution (EG-067), per-point retention trim (EG-068), cross-shard kNN merge (EG-069),
  hybrid metadata pre-filtering (EG-070), content-defined chunking (EG-071).
- **Exact/flat vector index + recall@k harness** (EG-297).

### Multi-wire adapters (one `WireProtocol` trait)
- The **`WireProtocol`/`WireSession` keystone** with pgwire refactored behind it (EG-074), plus
  **SQLite** (EG-075), **MySQL/MariaDB** (EG-076), **MSSQL TDS** (EG-077), **Redis RESP** (EG-174), and the
  **S3 REST** surface (EG-176).

### Message broker (Phase Y)
- The native engine task queue `ClaimNext` (KG-2.303) grown into a RabbitMQ-class broker: exchanges/routing +
  **AMQP** wire (EG-275), **MQTT** (EG-281) and **STOMP** (EG-282) wires, DLQ (EG-276), TTL/expiry (EG-277),
  priority queues (EG-278), delayed/scheduled delivery (EG-279), consumer groups + prefetch/QoS (EG-280),
  publisher confirms + consumer acks (EG-284), and replayable append-log streams (EG-283).

### Observability (Phase T)
- Log ingestion (OTLP/`_bulk`/syslog; EG-160), Parquet-on-object-store segments (EG-161), log search +
  `_search` API (EG-162), distributed traces (EG-163), VRL ingest pipelines (EG-165), **PromQL** query API
  (EG-172), and **super-cluster federated search** (EG-243).

### GIS (eg-geo)
- Spatial modality (EG-083) built to real-GIS depth: full geometry model (EG-257), DE-9IM + RCC8/Egenhofer
  (EG-258/155), constructive algebra (EG-259), geodesic ops (EG-256), CRS registry + reprojection
  (EG-255/262), durable R-tree (EG-263), GeoJSON/WKB/GPX I/O (EG-264), map tiling + MVT (EG-265),
  routing/isochrones/TSP (EG-266), map-anchored tasks (EG-267), and GeoSPARQL (EG-261).

### New modality engines + subsystems
- **eg-tensor** N-D array store (EG-085), **eg-stream** windowed CEP (EG-088), **eg-kvcache** tiered +
  shared KV-cache with the vLLM/LMCache server endpoint (EG-185/186/187), and the **agent-memory**
  primitives — summary tier (EG-220), episodic→semantic consolidation (EG-221), decay/reinforcement
  (EG-222), trajectory memory (EG-099), LeanRAG retrieval (EG-195).

### Unified planner / UQL · Distribution
- `Op::Foreign` name → `ForeignSourceSpec` resolution (EG-073), NL → query dual-mode (EG-078/079/080).
- Multi-Raft groups + `GroupRouter` + online resharding, N-participant cross-shard 2PC with crash recovery,
  parallel-commit + read-only-participant fast path (EG-081), non-blocking commit (EG-082).

---

**Reading this as a contributor?** The forward-roadmap table above is the whole of what's left. Each item
lands with tests + a `docs/concepts.md` entry, and flips to ✅ in the [capability matrix](capabilities.md)
as it merges.
