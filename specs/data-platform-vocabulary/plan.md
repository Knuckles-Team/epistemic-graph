# EG-DATAPLAT-VOCAB — Data-platform vocabulary: design and implementation plan

Governing spec: [spec.md](spec.md).

## Problem and evidence

`specs/connector-competency` (EG-CQ-CONN-001, rows R012 clarity, R014 data-science, R016 documentdb, R029 kafka, R041 objectstore, R044 openbao, R058 sql, R065 vector) lists these eight connectors as "Vocabulary: MISSING".

- `ls crates/eg-core/ontology/` holds no data-store vocabulary. `virtual_graph-v1.ttl` (EG-UNIFIED-DATA-PLANE-R037) defines only the attachment layer: `vg:SourceConnection`, `vg:EndpointRef`, `vg:MetadataContract` (with untyped `vg:discoveredEntity` / `discoveredField` strings), `vg:VirtualMapping`, `vg:PredicateFieldPair`. `native_source_connector-v1.ttl` has one class (`SourceDocument`) with three properties. `catalog-v1.ttl` has two prefixes and no store classes.
- `vg:sourceKind` is a closed list (`api mcp a2a graphql sql iceberg warehouse sparql`, enforced in `virtual_graph-v1.shapes.ttl`). It has no `document`, so the MongoDB/DocumentDB adapter of EG-UNIFIED-DATA-PLANE-R019 cannot be recorded as a connection today.
- Each connector package already ships a package-local ontology in the shared `http://knuckles.team/kg#` namespace (`sql_mcp/ontology/database.ttl`, `documentdb.ttl`, `objectstore.ttl`, `vector.ttl`, `kafka.ttl`, `openbao.ttl`, `clarity.ttl`, `datascience.ttl`) and a `mappings/source.yaml`. Those describe what the package emits into the graph. They are not an engine core schema source, they use unprefixed local names, and they overlap each other (`:collectionName` in vector and documentdb, `:ownedBy`, `:metricName` in clarity and data-science).

The gap is therefore a single engine-side typed vocabulary, `core:data-platform@1`, that (a) names a store as a source in one shared set of terms and (b) says how a store's contents map to domain concepts through `vg:VirtualMapping`.

## Architecture

```
question -> ontology class (dp:Table, dp:ConsumerGroup, dp:SecretPath ...)
         -> vg:VirtualMapping (approved, dp:StoreMapping) -> vg:MetadataContract
         -> vg:SourceConnection (sourceKind sql | document | mcp)
         -> contract fetch at query time:
              sql / document : engine adapter + R2RML virtual graph (unified-data-plane R005, R013, R019)
              others         : MCP operation named in the mapping row (connector package)
         -> answer with provenance (source, record id, as-of) or typed abstention
```

| Piece | Owner | Module |
|---|---|---|
| `core:data-platform@1` classes and properties | epistemic-graph | `crates/eg-core/ontology/data_platform-v1.ttl` |
| SHACL shapes (closed vocabularies, secret-metadata refusal) | epistemic-graph | `crates/eg-core/ontology/data_platform-v1.shapes.ttl` |
| `dp:StoreMapping` rows for each connector operation | epistemic-graph | mapping fixtures under `crates/eg-core/ontology/fixtures/data_platform/` |
| Discovery, R2RML compile, pushdown, acceleration | epistemic-graph | existing unified-data-plane rows, cited below, not re-specified |
| Missing connector operations | each connector package | see Decisions (cross-repo rows) |

Dependency direction: `data_platform-v1` imports `virtual_graph-v1`; nothing in `virtual_graph-v1` imports it. Package-local ontologies stay as the connector's emitted shape; each term gets an `rdfs:subClassOf` / `owl:equivalentProperty` alignment row from the package term to the `dp:` term (task rows R008.x) so the two cannot drift silently.

## Vocabulary

Prefix `dp:` = `http://knuckles.team/kg/data-platform#`. All classes/properties live in `data_platform-v1.ttl` unless stated. "Domain/Range" given where not obvious.

**Shared (6):** `dp:DataStore` (class; one per attached store, `dp:viaConnection` -> `vg:SourceConnection`); `dp:storeFamily` (string; closed: `relational document object vector stream secret analytics ml`); `dp:inStore` (dp container -> `dp:DataStore`); `dp:asOf` (xsd:dateTime, observation time of a measured property); `dp:viaConnection`; `dp:StoreMapping` (subclass of `vg:VirtualMapping`).

**Relational (9):** `dp:Database`, `dp:Schema`, `dp:Table`, `dp:View`, `dp:Column`, `dp:ForeignKey`; `dp:inSchema`, `dp:hasColumn`, `dp:referencesTable` (ForeignKey -> Table), `dp:isPrimaryKey`, `dp:sqlDialect`. Mirrors the typed catalog of unified-data-plane R003.1 (`AttachedCatalogGraph`).

**Document (6):** `dp:Collection`, `dp:DocumentField` (a path in the inferred shape of R019.1), `dp:fieldPath`, `dp:fieldType` (closed list = `DocumentFieldType` incl. `Mixed`), `dp:presenceRatio` (0..1), `dp:documentCount`.

**Object (5):** `dp:Bucket`, `dp:StoredObject`, `dp:objectKey`, `dp:byteSize`, `dp:lastModified`. No content property exists on `StoredObject` by design.

**Vector (5):** `dp:VectorCollection`, `dp:EmbeddingSpace` (model + dimension + metric), `dp:embeddingModel`, `dp:dimensions`, `dp:distanceMetric`; link `dp:usesEmbeddingSpace`, `dp:vectorCount`. Vectors and chunk text are never properties.

**Stream (8):** `dp:Topic`, `dp:Partition`, `dp:ConsumerGroup`, `dp:PartitionLag` (one group x partition measurement), `dp:partitionOf`, `dp:subscribesTo`, `dp:lag` (xsd:long), `dp:groupState`.

**Secret metadata:** `dp:SecretEngine`, `dp:SecretPath`, `dp:AccessPolicy`, `dp:RotationState`, `dp:hasRotationState`, `dp:lastRotatedAt`, `dp:currentVersion`, `dp:pathName` (SecretPath -> xsd:string), `dp:PolicyPathGrant` (class), `dp:grantPolicy` (PolicyPathGrant -> AccessPolicy), `dp:grantPath` (PolicyPathGrant -> SecretPath), `dp:grantsCapability` (PolicyPathGrant -> xsd:string; one or more closed values `read list create update delete sudo deny`), `dp:leaseExpiresAt`. There is no value, ciphertext, token, HCL-rule-body or key property in the namespace.

**Analytics (5):** `dp:AnalyticsSite`, `dp:BehaviorMetric`, `dp:metricName`, `dp:metricValue`, `dp:windowDays`; dimension via `dp:dimensionName` / `dp:dimensionValue` (e.g. `URL`). `dp:AnalyticsSession` is declared but only populated if the connector exposes sessions (it does not today; see Decisions).

**ML (8):** `dp:Dataset` (subclass of existing `:Dataset` from core-foundation), `dp:Model` (a general trained-model class), `dp:TrainingRun`, `dp:EvaluationResult`, `dp:trainedOn`, `dp:producedModel`, `dp:evaluatedOn`, `dp:baselineRun`.

Core alignment: `dp:Dataset` reuses the existing analysis-dataset meaning through subclassing. The existing `:Model` describes LLM/embedding deployment models; do not equate the broader `dp:Model` to it. Only an explicitly qualified LLM/embedding subtype of `dp:Model` may also subclass `:Model`. Kafka `dp:Topic` is distinct from the existing AgentBus `:Topic`.

**Mapping (4, on `dp:StoreMapping`):** `dp:contractKind` (closed: `r2rml-sql`, `document-shape`, `mcp-operation`), `dp:viaOperation` (string `tool:action`, required when `contractKind = mcp-operation`), `dp:accelerationMode` (`virtual` | `materialised`), `dp:materialisationReason` (required when `materialised`).

`core:virtual-graph@1` gets one edit: `document` added to the `vg:sourceKind` list and to its `sh:in` (task R001.3).

### SHACL (`data_platform-v1.shapes.ttl`)

Same style as `virtual_graph-v1.shapes.ttl`: SHACL Core plus aggregate-free `sh:sparql`, absence via `OPTIONAL ... FILTER(!BOUND())`.

- `dp:StoreFamilyShape`: `dp:storeFamily` exactly one, `sh:in` the eight families.
- `dp:StoreMappingShape`: `dp:accelerationMode` exactly one `sh:in ("virtual" "materialised")`; `dp:contractKind` exactly one; `sh:sparql` rule: `materialised` without `dp:materialisationReason` is a violation; `contractKind = mcp-operation` without `dp:viaOperation` is a violation. Virtual is the default and needs no reason.
- `dp:SecretPathShape` / `dp:AccessPolicyShape` / `dp:RotationStateShape` / `dp:SecretEngineShape` / `dp:PolicyPathGrantShape`: `sh:closed true` with `sh:ignoredProperties (rdf:type rdfs:label)`, listing only the metadata properties above and the applicable shared `dp:inStore`, `dp:viaConnection` and `dp:asOf` metadata properties. Each `dp:PolicyPathGrant` has exactly one `dp:grantPolicy`, exactly one `dp:grantPath`, and at least one enumerated `dp:grantsCapability`; permissions are scoped to that policy/path pair, never flattened onto the policy. A fixture gives one policy read on path A and deny on path B and preserves that association. Any other property (a value, `data`, `token`, `password`, `rules`, `ciphertext`) is itself the violation, the same technique as `vg:EndpointRefShape`. A second rule (`sh:sparql`) rejects any `dp:SecretPath` whose `dp:pathName` literal contains a `=` or a base64-looking run longer than 64 characters (guard against a value pasted into a path).
- `dp:VectorCollectionShape`: `dp:dimensions` positive integer, one `dp:EmbeddingSpace`.
- `dp:PartitionLagShape`: `dp:lag` integer >= 0, `dp:asOf` required.

## Per-connector mapping tables

"Op" = the package tool and action read from the package source. Contract kind: **R2RML** (engine adapter + virtual graph), **DOC** (engine document-shape adapter), **MCP** (named operation, `dp:viaOperation`). Mode: **V** virtual-first; **M** has a declared reason to be materialised.

### sql (`sql-mcp`: `sql_schema`, `sql_query`, `sql_execute`, `sql_admin`)

| Op | Class / property | Contract | Mode |
|---|---|---|---|
| `sql_schema` `schemas` | `dp:Schema`, `dp:inSchema` | R2RML via engine catalog (R003); MCP fallback | V |
| `sql_schema` `tables`, `views`, `materialized_views` | `dp:Table`, `dp:View` | same | V |
| `sql_schema` `columns` | `dp:Column`, `dp:hasColumn`, `dp:isPrimaryKey` | same | V |
| `sql_schema` `foreign_keys`, `constraints` | `dp:ForeignKey`, `dp:referencesTable` | same | V |
| `sql_schema` `indexes`, `ddl`, `catalog` | `dp:Table` metadata only | same | V |
| `sql_query` `execute` / `explain` | rows of a mapped entity (`vg:VirtualMapping`) | R2RML pushdown (R006) | V |
| `sql_admin` `capabilities`, `dialects`, `version` | `dp:sqlDialect`, `vg:declaredCapability` | MCP | V |

Rows stay in the source; only catalog metadata is held (R038).

### documentdb (`documentdb-mcp`: `documentdb_system`, `_collections`, `_crud`, `_analysis`)

| Op | Class / property | Contract | Mode |
|---|---|---|---|
| `documentdb_system` `list_databases` | `dp:Database` | MCP | V |
| `documentdb_collections` `list_collections` | `dp:Collection`, `dp:inStore` | DOC (R019.1) / MCP | V |
| `documentdb_system` `run_command` (`collStats`) | `dp:documentCount` | MCP | V |
| `documentdb_analysis` `aggregate` (field-presence pipeline) | `dp:DocumentField`, `dp:presenceRatio` | DOC sampling (R004) | V |
| `documentdb_analysis` `distinct` | value enumeration for a mapped key | MCP | V |
| `documentdb_crud` `find` / `find_one` | documents of a mapped entity | DOC / MCP | V |

### objectstore (`objectstore-mcp`: `buckets`, `objects`)

| Op | Class / property | Contract | Mode |
|---|---|---|---|
| `buckets` `stores`, `list`, `exists`, `info` | `dp:DataStore`, `dp:Bucket`, `dp:inStore` | MCP | V |
| `objects` `list` | `dp:StoredObject`, `dp:objectKey`, `dp:byteSize`, `dp:lastModified` | MCP | V |
| `objects` `head`, `metadata_get` | same, plus user metadata keys as `vg:discoveredField` | MCP | V |
| `objects` `get` | object bytes | not mapped; never a graph property | n/a |

### vector (`vector-mcp`: `vector_collection_management`, `vector_search`)

| Op | Class / property | Contract | Mode |
|---|---|---|---|
| `vector_collection_management` `list_collections` | `dp:VectorCollection` | MCP | V |
| `vector_search` `semantic_search` / `lexical_search` / `search` | hit ids resolved to source documents, not vectors | MCP | V |
| (none) collection stats | `dp:vectorCount`, `dp:dimensions`, `dp:EmbeddingSpace` | missing | BLOCKED, see Decisions |

### kafka (`kafka-mcp`: `kafka_topics`, `kafka_partitions`, `kafka_groups`, `kafka_cluster`, `kafka_cdc_*`)

| Op | Class / property | Contract | Mode |
|---|---|---|---|
| `kafka_topics` `list`, `describe`, `configs` | `dp:Topic` | MCP | V |
| `kafka_partitions` `list`, `get` | `dp:Partition`, `dp:partitionOf` | MCP | V |
| `kafka_groups` `list`, `describe`, `consumers` | `dp:ConsumerGroup`, `dp:groupState`, `dp:subscribesTo` | MCP | V |
| `kafka_groups` `lags`, `lag_summary`; `kafka_cdc_lag` | `dp:PartitionLag`, `dp:lag`, `dp:asOf` | MCP | V for current lag; **M** for "sustained" lag |
| `kafka_cluster` `list_brokers`, `list_acls` | broker/ACL metadata (reuse package terms; out of the 8 families) | MCP | V |
| `kafka_records` `consume` | messages | not mapped; records are never copied | n/a |

### openbao (`openbao-mcp`: `openbao_mcp_sys`, `_kv`, `_logical`, `_auth`, `openbao_ingest_mounts`)

| Op | Class / property | Contract | Mode |
|---|---|---|---|
| `openbao_mcp_sys` `get_mounts`; `openbao_ingest_mounts` | `dp:SecretEngine` (mount path, engine type, version) | MCP | V |
| `openbao_mcp_logical` `list` (keys only) | `dp:SecretPath` (path name) | MCP, list only | V |
| `openbao_mcp_sys` `get_health`, `seal_status`, `leader` | server state on `dp:DataStore` | MCP | V |
| `kg_ingest.ingest_policies` (policy names) | `dp:AccessPolicy` (name only) | existing whitelist | V |
| `openbao_mcp_kv` `kv2_get`, `openbao_mcp_logical` `read`/`unwrap` | secret values | **REFUSED**: never mapped, see Decisions | n/a |
| (none) KV v2 metadata (`created_time`, versions) | `dp:RotationState` | missing | BLOCKED |
| (none) policy-to-path grants | `dp:grantsCapability` | missing | BLOCKED |

### clarity (`clarity-api`: `clarity_insights` `get_data_export`, `clarity_ingest_insights`)

| Op | Class / property | Contract | Mode |
|---|---|---|---|
| `get_data_export` (params `number_of_days` 1-3, `dimension_1..3`) | `dp:BehaviorMetric`, `dp:metricName`, `dp:metricValue`, `dp:windowDays`, `dp:dimensionName/Value` | MCP | V for <= 3 days; **M** for longer windows |
| `clarity_ingest_insights` | `dp:AnalyticsSite` plus insights | existing package mapping | V |

### data-science (`data-science-mcp`: `load_dataset`, `describe_dataset`, `dataset_lineage`, `fit_model`, `evaluate_model`, `rank_models`, `cross_validate`, `train_*`)

| Op | Class / property | Contract | Mode |
|---|---|---|---|
| `load_dataset`, `describe_dataset`, `split_dataset` | `dp:Dataset` | MCP | V |
| `dataset_lineage` | `dp:trainedOn`, `dp:Dataset` provenance | MCP | V |
| `fit_model`, `train_sft/dpo/grpo/ppo` | `dp:TrainingRun`, `dp:producedModel` | MCP (call-time only) | **M** (see Decisions) |
| `evaluate_model`, `cross_validate` | `dp:EvaluationResult`, `dp:evaluatedOn`, `dp:metricValue` | MCP | **M** |
| `rank_models` | `dp:Model` (ranked by test R2) | MCP | V |

## Wiring

- `crates/eg-core/ontology/` is the directory the core schema sources load from; add `data_platform-v1.ttl` and `.shapes.ttl` beside `virtual_graph-v1.*`, registered as `core:data-platform@1` exactly as `core:virtual-graph@1` is (EG-UNIFIED-DATA-PLANE-R037).
- Mappings are `vg:VirtualMapping` instances (subclass `dp:StoreMapping`); approval gating is `crates/eg-plan/src/virtual_graph_mapping.rs` (R005.1: only `Approved` mappings serve reads). No new approval path.
- Discovery output (`vg:MetadataContract`, R038.2) is the input the `dp:` classes type; no discovery code is added here.
- Scopes: only the engine contract's existing read scopes; no new scope.

## Decisions

1. **CONFIRMED sql.** `sql_schema` actions `schemas tables views materialized_views sequences columns indexes foreign_keys constraints ddl sample view_definition table_comment catalog` (`sql_mcp/mcp/mcp_sql.py` lines 252-265) cover "tables with no primary key" and "columns referencing missing tables". Cross question (R058.2) also needs egeria; vocabulary only.
2. **CONFIRMED documentdb.** `documentdb_analysis` `aggregate` and `documentdb_system` `run_command` (`documentdb_mcp/tools/analysis.py`, `system.py`) give document counts and per-field presence; `list_collections` is in `tools/collections.py`.
3. **DECIDED schema discovery is engine-side.** Relational and document discovery is `AttachedCatalogGraph` (unified-data-plane R003/R003.1), inferred document shape (R004, R019.1) and per-source-kind `MetadataContract` discovery (R038.2). `dp:` classes only type the entries those produce. The connectors' `sql_schema` / `list_collections` operations are the fallback discovery path when no engine adapter exists for the dialect (kind `mcp`), invoked through the same R038.2 entry; they are not a second implementation, and no task here writes discovery code. Alternative rejected: have the connector push its schema into the graph (that is the package `kg_ingest.py` path, already scoped to metadata and retained only for connectors without an engine adapter).
4. **DECIDED virtual-first for all eight, materialised only with a reason.** sql, documentdb, objectstore, vector, openbao: virtual, no reason to copy. kafka: "sustained lag" needs history, which a point-in-time `lags` call cannot give. Reason = lag time series via R038 `AccelerationPolicy` (metadata only, no records). clarity: export window is limited to 3 days (`mcp_insights.py`), so "last week" needs a daily snapshot of aggregate metrics (no session replays). data-science: the ML engine registry is in-process (`MLEngine`, `reset_ml_engine`), so run history must be recorded as `dp:TrainingRun` / `dp:EvaluationResult` metadata when a run happens. Reason recorded in the mapping.
5. **BLOCKED objectstore "not read in a year".** `objects` exposes `lastModified` only; there is no last-access operation (grep `last_accessed|atime` in `objectstore_mcp` is empty). Owner: objectstore-mcp (cross-repo row R009.1). Interim: the single-source question answers with `lastModified` and says so, or abstains typed.
6. **BLOCKED vector stats and document linkage.** `vector_collection_management` has no stats action: no vector count, dimension or embedding model per collection, and no get-by-id to map a chunk to its source document ("Paperless documents with no embedding"). Owner: vector-mcp (R009.2).
7. **BLOCKED openbao rotation age and policy grants.** `openbao_mcp_kv` has only `kv2_get kv2_put kv2_delete kv2_patch kv1_*`; there is no KV v2 metadata read (`created_time`, version list) and no policy read; `kg_ingest.ingest_policies` deliberately stores names only because rules reference paths. Owner: openbao-mcp (R009.3: add a metadata-only operation returning path, engine, current version, last-written time and a capability matrix per policy with no values and no HCL). Until then both openbao questions abstain typed.
8. **BLOCKED clarity "rage-click or dead-click rate".** The export takes dimensions Browser, Device, Country, OS, Source, Medium, Campaign, Channel, URL. The metric list returned is Clarity's, and the package never filters or names rage/dead-click metrics. CONFIRMED only that URL is a dimension. Owner: clarity-api (R009.4: surface the named metrics and confirm the export returns click-quality metrics by URL).
9. **BLOCKED data-science baseline comparison.** `rank_models` ranks by test R2 only. There is no operation returning a baseline run, a training run list or validation metric history. Owner: data-science-mcp (R009.5: a run-registry read operation).
10. **CONFIRMED kafka.** `kafka_groups` `lags` and `lag_summary` (`kafka_mcp/mcp/mcp_kafka.py` lines 176-178) answer "which groups lag per partition". The Grafana alerting join is cross-source (federated-query), not vocabulary.
11. **DECIDED secrets: metadata only.** `core:data-platform@1` has no property that can hold a secret value; the `sh:closed` shapes make any extra property a violation, the refusal test (R005.3) feeds a `SecretPath` with a `value` triple and expects rejection, and the mapping validator refuses a `dp:StoreMapping` whose `dp:viaOperation` is `openbao_mcp_kv:kv2_get`, `openbao_mcp_kv:kv1_get`, `openbao_mcp_logical:read` or `:unwrap` (denylist in the shape, R005.4). Alternative (allow values but redact at read) rejected: redaction is a runtime filter that can fail; absence from the vocabulary cannot.
12. **DECIDED `document` source kind.** Add `document` to `vg:sourceKind` rather than reuse `sql`; the other six non-SQL stores are reached as kind `mcp`.

## Worked example

Question (R058.1): "Which tables have no primary key?" over a PostgreSQL source.

1. `vg:SourceConnection` (kind `sql`, no credential in `vg:endpointRef`) exists; discovery (R038.2) wrote a `vg:MetadataContract` with entities/fields.
2. An approved `dp:StoreMapping` binds entity `tables` to `dp:Table` and field `is_primary_key` to `dp:isPrimaryKey` (contractKind `r2rml-sql`, mode `virtual`).
3. Query sketch: `SELECT ?t WHERE { ?t a dp:Table ; dp:inStore ?s . OPTIONAL { ?t dp:hasColumn ?c . ?c dp:isPrimaryKey true } FILTER(!BOUND(?c)) }`, compiled to the source's catalog SQL through R2RML pushdown (R006).
4. Answer shape: rows `{table, schema, source, record_id, as_of}`; if the connection is absent, a typed abstention naming `sql`.

## Out of scope

Discovery, R2RML compilation, pushdown, CDC and acceleration mechanics (unified-data-plane). Cross-source joins (federated-query-and-reasoning). The competency questions and coverage table (connector-competency). Connector code changes (cross-repo rows only). Any secret value in any form.
