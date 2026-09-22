# Reasoning API reference

> **GENERATED** by `scripts/gen_api_docs.py` from `contract/methods.json` and `contract/schemas/method.request.json` / `contract/schemas/result.reasoning.json` -- do not hand-edit. Regenerate with `python3 scripts/gen_api_docs.py --write`. 13 methods in this namespace. See also the machine-checked policy ledger at [`capabilities.generated.md`](../capabilities.generated.md) and the [OpenAPI document](../openapi.json) / [Swagger UI](../swagger-ui.md).

## `GetRdf`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `rdf:read` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `Snapshot` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

_No parameters._

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | string | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/GetRdf`, `contract/schemas/result.reasoning.json#/methods/GetRdf`.

## `GraphSchema`

X9. Gateway-routed exactly like IcvConfigure: every op attaches, replaces or detaches one keyed schema source through the graph commit kernel, so it is audited, emits CDC, and is recorded in the native Raft GraphState catalog

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `security:admin` |
| Mutates | `true` |
| Durability domain | `GraphRedb` |
| Idempotent | `true` |
| Audited | `true` |
| Emits CDC | `true` |
| Txn participation | `Atomic` |
| Replay class | `OperationIdentity` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED`, `CONFLICT`, `IDEMPOTENCY_CONFLICT`, `REDIRECTED`, `READ_ONLY` |
| Format identities | `STORAGE_KERNEL_SCHEMA_VERSION`, `GRAPH_SNAPSHOT_SCHEMA_VERSION`, `GRAPH_META_SCHEMA_VERSION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `op` | `GraphSchemaOp` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `GraphSchemaCommitted` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/GraphSchema`, `contract/schemas/result.reasoning.json#/methods/GraphSchema`.

## `GraphSchemaList`

X9. Reads the request graph's schema-source set and its composed digest; a separate method rather than an op because a read op inside a gateway-routed method would need a runtime-conditional gateway plan

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `security:admin` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `Snapshot` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

_No parameters._

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `GraphSchemaSourcesView` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/GraphSchemaList`, `contract/schemas/result.reasoning.json#/methods/GraphSchemaList`.

## `IcvConfigure`

state-backed MutationBatch

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `security:admin` |
| Mutates | `true` |
| Durability domain | `GraphRedb` |
| Idempotent | `true` |
| Audited | `true` |
| Emits CDC | `true` |
| Txn participation | `Atomic` |
| Replay class | `OperationIdentity` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED`, `CONFLICT`, `IDEMPOTENCY_CONFLICT`, `REDIRECTED`, `READ_ONLY` |
| Format identities | `STORAGE_KERNEL_SCHEMA_VERSION`, `GRAPH_SNAPSHOT_SCHEMA_VERSION`, `GRAPH_META_SCHEMA_VERSION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `graph` | string \| null | no |  |
| `mode` | string | yes |  |
| `shapes` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | boolean | Bool |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/IcvConfigure`, `contract/schemas/result.reasoning.json#/methods/IcvConfigure`.

## `OwlExplain`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `owl:read` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `Snapshot` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `ontology` | string | no | Extra OWL axioms as Turtle (empty ⇒ reason over the graph's own axioms). |
| `sub` | string | yes | The SUBCLASS side of the subsumption to explain (a class IRI, `<...>` or bare — canonicalized the same way `target_class` is elsewhere). |
| `sup` | string | yes | The SUPERCLASS side of the subsumption to explain. |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `OwlExplainResult` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/OwlExplain`, `contract/schemas/result.reasoning.json#/methods/OwlExplain`.

## `OwlReason`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `owl:read` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `Snapshot` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `class_base` | string | no | The absolute namespace a bare string node `type` (e.g. `"Agent"`) is bridged into before classification (`eg_rdf::owl::bridge_type_to_class`) — independent of `target_class`, which ONLY controls filtering (BUG-281: the two used to be conflated, so an empty `target_class` — its own documented "all classes" case — could never supply a namespace, and a caller wanting "reason over everything" always hit `OwlReason requires an absolute target class`). Empty ⇒ fall back to `target_class`'s own namespace when `target_class` is absolute (the pre-existing convenience for a caller that only ever set one field); a class bridge for a bare string `type` is only possible once SOME absolute namespace is available from either field. |
| `min_confidence` | number (double) | no | Confidence threshold τ in `[0,1]` (CONCEPT:EG-KG.ontology.concept-13). The result carries a per-entailment confidence (axioms/facts may be uncertain; the closure propagates it — `eg:confidence` annotations × the per-node confidence × Ebbinghaus decay). Only entailments with `confidence ≥ min_confidence` are returned. `0.0` keeps everything (and a HARD ontology yields all `1.0`). |
| `ontology` | string | no | Extra OWL axioms as Turtle (empty ⇒ reason over the graph's own axioms). |
| `target_class` | string | no | When set, restrict the returned instance memberships to this class (its inferred members) — the materialize-one-class shape. Empty ⇒ all classes. |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `OwlReasonResult` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/OwlReason`, `contract/schemas/result.reasoning.json#/methods/OwlReason`.

## `OwlReasonDistributed`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `owl:read` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `Snapshot` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `class_base` | string | no | See `OwlReason::class_base` (BUG-281) — independent of `target_class`. |
| `graphs` | array of string | yes | The graphs (shards) whose axioms + facts to union and reason over. |
| `min_confidence` | number (double) | no | Confidence threshold τ in `[0,1]` (see `OwlReason::min_confidence`). |
| `ontology` | string | no | Extra OWL axioms as Turtle (a shared TBox over the sharded ABox; empty ⇒ only the axioms already present across the graphs). |
| `target_class` | string | no | Restrict instance memberships to this class (empty ⇒ all classes). |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `OwlReasonResult` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/OwlReasonDistributed`, `contract/schemas/result.reasoning.json#/methods/OwlReasonDistributed`.

## `RunDatalogReasoning`

state-backed MutationBatch commits inferred facts; operation-identity replay prevents duplicate materialization/audit/CDC

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `reasoning:write` |
| Mutates | `true` |
| Durability domain | `GraphRedb` |
| Idempotent | `true` |
| Audited | `true` |
| Emits CDC | `true` |
| Txn participation | `Atomic` |
| Replay class | `OperationIdentity` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED`, `CONFLICT`, `IDEMPOTENCY_CONFLICT`, `REDIRECTED`, `READ_ONLY` |
| Format identities | `STORAGE_KERNEL_SCHEMA_VERSION`, `GRAPH_SNAPSHOT_SCHEMA_VERSION`, `GRAPH_META_SCHEMA_VERSION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `domain_rules` | array of array of any | no | (property, domain_type) — subjects of `property` are inferred to be `domain_type`. |
| `inverse_properties` | array of array of any | no |  |
| `property_chains` | array of array of any | no | (predicate_a, predicate_b, inferred_predicate) — chain composition. |
| `range_rules` | array of array of any | no | (property, range_type) — objects of `property` are inferred to be `range_type`. |
| `subclass_relations` | array of array of any | no |  |
| `subproperty_relations` | array of array of any | no |  |
| `symmetric_properties` | array of string | no |  |
| `transitive_properties` | array of string | no |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `DatalogReasoningResult` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/RunDatalogReasoning`, `contract/schemas/result.reasoning.json#/methods/RunDatalogReasoning`.

## `RunRules`

READ-ONLY (EG-P0-2/L11 handler audit): handle_run_rules reasons over an off-lock analysis_snapshot and returns inferred triples, no writeback -- unlike its sibling RunDatalogReasoning which materialises in-place. Corrected from a prior mutates=true semantic guess; now agrees with access.rs (never a write there)

| Property | Value |
|---|---|
| Stability | `internal` |
| Authz action | `reasoning:read` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `Snapshot` |
| Replay class | `NotReplayable` |
| Consumer profiles |  |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `derived_only` | boolean | no | When true, return only the DERIVED facts (omit the asserted base). |
| `min_confidence` | number (double) | no | Drop facts whose confidence is below this threshold. |
| `ontology_ttl` | string | no | Optional Turtle carrying extra TBox axioms AND/OR ABox facts (empty ⇒ reason over the graph's own folded axioms/facts only). |
| `query_predicate` | string \| null | no | When set, restrict the returned facts to this predicate (IRI or bare name). |
| `rules` | array of string | no | User rule strings (SWRL-ish / Datalog syntax). |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `RuleReasonResponse` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/RunRules`, `contract/schemas/result.reasoning.json#/methods/RunRules`.

## `ShaclValidate`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `validation:read` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `Snapshot` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `data_graph` | string | no | The data graph as a Turtle document; empty ⇒ use the request's live graph. |
| `shapes` | string \| null | no | An explicit shapes graph as Turtle. Omitted/empty uses the request graph's composed GraphSchema authority. |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `ShaclValidationReport` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/ShaclValidate`, `contract/schemas/result.reasoning.json#/methods/ShaclValidate`.

## `ShexValidate`

| Property | Value |
|---|---|
| Stability | `internal` |
| Authz action | `validation:read` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `Snapshot` |
| Replay class | `NotReplayable` |
| Consumer profiles |  |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `data_graph` | string | no | The data graph as a Turtle document; empty ⇒ use the request's live graph. |
| `schema` | string | yes | The ShEx schema as a ShExJ (JSON abstract-syntax) document. |
| `shape_map` | array of array of string | no | The shape map: `[node_iri, shape_label]` pairs. `shape_label` may be `"START"`. |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `ShexValidationReport` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/ShexValidate`, `contract/schemas/result.reasoning.json#/methods/ShexValidate`.

## `Sparql`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `sparql:read` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `Snapshot` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `base_iri` | string | no | Projection base namespace IRI. Empty ⇒ identity projection. |
| `query` | string | yes |  |
| `type_convention` | string | no | `rdf:type` object naming: `"camel"` ⇒ CamelCase the type local name under `base_iri`; empty / `"raw"` ⇒ verbatim. Only meaningful with `base_iri`. |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `SparqlResult` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/Sparql`, `contract/schemas/result.reasoning.json#/methods/Sparql`.

## `SparqlVirtual`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `sparql:read` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `Snapshot` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `external_sources` | array of `ObdaExternalSource` | no | LIVE external relational sources (Postgres/MySQL) registered as foreign OBDA sources IN ADDITION to `tables` (CONCEPT:EG-KG.query.obda-predicate-pushdown, W4.11). Each binds a `logical_source` name to an external DB table; the query's column projection AND its row-level `FILTER`s are pushed into a real `SELECT … WHERE …`. Needs a `federation-sql` server build for the live path. Empty ⇒ engine-own-tables-only (the prior behavior). |
| `mapping` | string | yes | An R2RML Turtle document OR the compact EG-101 textual mapping form. |
| `query` | string | yes | The SPARQL query to run against the virtual graph. |
| `tables` | array of string | yes | The user-table names the mapping's `TriplesMap`s reference as `logical_source`s — each is registered as a foreign source under its own name before the mapping is parsed and the query is run. |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `SparqlResult` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/SparqlVirtual`, `contract/schemas/result.reasoning.json#/methods/SparqlVirtual`.
