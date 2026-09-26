# Query API reference

> **GENERATED** by `scripts/gen_api_docs.py` from `contract/methods.json` and `contract/schemas/method.request.json` / `contract/schemas/result.query.json` -- do not hand-edit. Regenerate with `python3 scripts/gen_api_docs.py --write`. 31 methods in this namespace. See also the machine-checked policy ledger at [`capabilities.generated.md`](../capabilities.generated.md) and the [OpenAPI document](../openapi.json) / [Swagger UI](../swagger-ui.md).

## `CausalCounterfactual`

EPI-P3-6 Pearl point-counterfactual over a request-carried SCM + a fully-observed unit; handler additionally gated `epistemic-causal`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `explain:read` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `Snapshot` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `ACCESS_DENIED`, `AST_INPUT_INVALID`, `AST_INPUT_LIMIT`, `AUTHENTICATION_REQUIRED`, `AUTH_AUDIENCE_MISMATCH`, `AUTH_POLICY_VERSION_MISMATCH`, `AUTH_TENANT_MISMATCH`, `BUSY`, `CANCELLED`, `CAPACITY_DENIED`, `ENGINE_DEADLINE_EXCEEDED`, `ENGINE_RESOURCE_EXHAUSTED`, `ENGINE_UNAVAILABLE`, `INTERNAL`, `INVALID_ARGUMENT`, `NODE_MISMATCH`, `OPERATION_REDIRECTED`, `RESULT_TOO_LARGE`, `SQL_OWNER_REPAIR_PENDING`, `STALE_OUTBOX_LEASE`, `STALE_ROUTE`, `TIMEOUT`, `UQL_BUDGET_EXCEEDED`, `UQL_CREDENTIAL_BEARING_SPEC`, `UQL_UNSUPPORTED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `actual` | object | yes |  |
| `do_values` | object | yes |  |
| `variables` | array of `StructuralEquationWire` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `CausalCounterfactualResult` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/CausalCounterfactual`, `contract/schemas/result.query.json#/methods/CausalCounterfactual`.

## `CausalEstimate`

EPI-P3-3/P3-6 do-calculus intervention OR observational conditioning (selected by `mode`) over a request-carried SCM; handler additionally gated `epistemic-causal`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `explain:read` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `Snapshot` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `ACCESS_DENIED`, `AST_INPUT_INVALID`, `AST_INPUT_LIMIT`, `AUTHENTICATION_REQUIRED`, `AUTH_AUDIENCE_MISMATCH`, `AUTH_POLICY_VERSION_MISMATCH`, `AUTH_TENANT_MISMATCH`, `BUSY`, `CANCELLED`, `CAPACITY_DENIED`, `ENGINE_DEADLINE_EXCEEDED`, `ENGINE_RESOURCE_EXHAUSTED`, `ENGINE_UNAVAILABLE`, `INTERNAL`, `INVALID_ARGUMENT`, `NODE_MISMATCH`, `OPERATION_REDIRECTED`, `RESULT_TOO_LARGE`, `SQL_OWNER_REPAIR_PENDING`, `STALE_OUTBOX_LEASE`, `STALE_ROUTE`, `TIMEOUT`, `UQL_BUDGET_EXCEEDED`, `UQL_CREDENTIAL_BEARING_SPEC`, `UQL_UNSUPPORTED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `do_values` | object | yes |  |
| `mode` | `CausalQueryModeWire` (enum: `Intervene`, `Observe`) | yes |  |
| `variables` | array of `StructuralEquationWire` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `CausalEstimateResult` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/CausalEstimate`, `contract/schemas/result.query.json#/methods/CausalEstimate`.

## `CypherQuery`

runtime-conditional; writes execute against a staged graph and publish only after durable MutationBatch commit

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `query:cypher` |
| Mutates | `true` |
| Durability domain | `GraphRedb` |
| Idempotent | `false` |
| Audited | `true` |
| Emits CDC | `false` |
| Txn participation | `Atomic` |
| Replay class | `OperationIdentity` |
| Consumer profiles | `python` |
| Error set | `ACCESS_DENIED`, `AST_INPUT_INVALID`, `AST_INPUT_LIMIT`, `AUTHENTICATION_REQUIRED`, `AUTH_AUDIENCE_MISMATCH`, `AUTH_POLICY_VERSION_MISMATCH`, `AUTH_TENANT_MISMATCH`, `BUSY`, `CANCELLED`, `CAPACITY_DENIED`, `CONFLICT`, `CORRUPT_MUTATION_LEDGER`, `ENGINE_DEADLINE_EXCEEDED`, `ENGINE_RESOURCE_EXHAUSTED`, `ENGINE_UNAVAILABLE`, `IDEMPOTENCY_CONFLICT`, `INTERNAL`, `INVALID_ARGUMENT`, `NODE_MISMATCH`, `OPERATION_REDIRECTED`, `READ_ONLY`, `REDIRECTED`, `REPLAY_NONCE_CONSUMED`, `RESULT_TOO_LARGE`, `SQL_OWNER_REPAIR_PENDING`, `STALE_OUTBOX_LEASE`, `STALE_ROUTE`, `TIMEOUT`, `UQL_BUDGET_EXCEEDED`, `UQL_CREDENTIAL_BEARING_SPEC`, `UQL_UNSUPPORTED` |
| Format identities | `STORAGE_KERNEL_SCHEMA_VERSION`, `GRAPH_SNAPSHOT_SCHEMA_VERSION`, `GRAPH_META_SCHEMA_VERSION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `mode` | `CypherMode` (enum: `read`, `write`) | yes | Exact requested execution authority; no implicit or inferred mode. |
| `query` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | any | Raw | {'reason': 'query-rows', 'summary': "rows whose columns and value types are chosen by the caller's query text"} |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/CypherQuery`, `contract/schemas/result.query.json#/methods/CypherQuery`.

## `Decide`

RF-ADR-010 DL-4. Evaluate-only: scores the tenant-visible library candidates under a pinned feature schema, head and policy and answers a batch of sealed statistical records; it commits none of them. Graph-sourced candidates are refused until graph-sourced records land

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `query:decide` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `Snapshot` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `ACCESS_DENIED`, `ASSEMBLY_INPUTS_INVALID`, `ASSEMBLY_MODEL_INVALID`, `AUTHENTICATION_REQUIRED`, `AUTH_AUDIENCE_MISMATCH`, `AUTH_POLICY_VERSION_MISMATCH`, `AUTH_TENANT_MISMATCH`, `BUSY`, `CANCELLED`, `CANDIDATE_FACTS_CHANGED`, `CANDIDATE_PLAN_REFUSED`, `CANDIDATE_SCOPE_TOO_LARGE`, `CANDIDATE_SET_TOO_LARGE`, `CAPACITY_DENIED`, `CAPACITY_UNAVAILABLE`, `CERTIFICATE_REJECTED`, `COMPONENT_BODY_UNAVAILABLE`, `COMPONENT_PIN_MISMATCH`, `COMPONENT_WITHDRAWN`, `CORRUPT_DECISION_ARTIFACT`, `CORRUPT_DECISION_RECORD`, `DATASET_INVALID`, `DECISIONS`, `DECISION_LOG_TOO_LARGE`, `DECISION_RECORD_VERSION_UNSUPPORTED`, `DECISION_REPLAY_MISMATCH`, `DERIVATION_REJECTED`, `ENGINE_DEADLINE_EXCEEDED`, `ENGINE_RESOURCE_EXHAUSTED`, `ENGINE_UNAVAILABLE`, `EVALUATION_RECEIPT_MISMATCH`, `EVALUATION_RECEIPT_REQUIRED`, `EVAL_CANDIDATE_NOT_FOUND`, `EXPLORATION_FORBIDDEN`, `FEATURE_SCHEMA_INVALID`, `HEAD_INVALID`, `IDEMPOTENCY_CONFLICT`, `INTERNAL`, `INVALID_ARGUMENT`, `JOB_NOT_FOUND`, `LOOK_AHEAD`, `NODE_MISMATCH`, `NO_ADMISSIBLE_LABELS`, `NUMERIC_REFUSED`, `OPERATION_REDIRECTED`, `PARAMETER_INVALID`, `POLICY_BODY_UNAVAILABLE`, `POLICY_LOOSENING`, `RECORD_TOO_LARGE`, `REPLAY_POLICY_DEPENDENT`, `REPLAY_SPEC_INVALID`, `STALE_CATALOG`, `STALE_OUTBOX_LEASE`, `STALE_ROUTE`, `TIMEOUT`, `TRIALS_UNDERSTATED`, `UNSUPPORTED_COALITION`, `UQL_BUDGET_EXCEEDED`, `UQL_CREDENTIAL_BEARING_SPEC`, `UQL_UNSUPPORTED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `request` | `DecideRequest` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `DecisionBatch` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/Decide`, `contract/schemas/result.query.json#/methods/Decide`.

## `EdgeIndex`

EH-351/EH-352, runtime-conditional: status reads; create, refresh and drop write the verified tenant's SQL catalog (registration and generations) and the request graph's IndexManager. Drop is fenced: a build in flight never activates after it

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `semantic:binding-write` |
| Mutates | `true` |
| Durability domain | `GraphRedb` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `Atomic` |
| Replay class | `OperationIdentity` |
| Consumer profiles | `python` |
| Error set | `ACCESS_DENIED`, `AST_INPUT_INVALID`, `AST_INPUT_LIMIT`, `AUTHENTICATION_REQUIRED`, `AUTH_AUDIENCE_MISMATCH`, `AUTH_POLICY_VERSION_MISMATCH`, `AUTH_TENANT_MISMATCH`, `BUSY`, `CANCELLED`, `CAPACITY_DENIED`, `CONFLICT`, `CORRUPT_MUTATION_LEDGER`, `ENGINE_DEADLINE_EXCEEDED`, `ENGINE_RESOURCE_EXHAUSTED`, `ENGINE_UNAVAILABLE`, `IDEMPOTENCY_CONFLICT`, `INTERNAL`, `INVALID_ARGUMENT`, `NODE_MISMATCH`, `OPERATION_REDIRECTED`, `READ_ONLY`, `REDIRECTED`, `RESULT_TOO_LARGE`, `SQL_OWNER_REPAIR_PENDING`, `STALE_OUTBOX_LEASE`, `STALE_ROUTE`, `TIMEOUT`, `UQL_BUDGET_EXCEEDED`, `UQL_CREDENTIAL_BEARING_SPEC`, `UQL_UNSUPPORTED` |
| Format identities | `STORAGE_KERNEL_SCHEMA_VERSION`, `GRAPH_SNAPSHOT_SCHEMA_VERSION`, `GRAPH_META_SCHEMA_VERSION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `op` | `EdgeIndexOp` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `EdgeIndexStatusView` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/EdgeIndex`, `contract/schemas/result.query.json#/methods/EdgeIndex`.

## `EdgeSearch`

EH-351: edge-native vector or BM25 search of the request graph; the caller's row-level security, the edge type and the property filters are applied inside the index walk, and edges come back as edges (endpoints and parallel-edge ordinal)

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `semantic:binding-read` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `Snapshot` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `ACCESS_DENIED`, `AST_INPUT_INVALID`, `AST_INPUT_LIMIT`, `AUTHENTICATION_REQUIRED`, `AUTH_AUDIENCE_MISMATCH`, `AUTH_POLICY_VERSION_MISMATCH`, `AUTH_TENANT_MISMATCH`, `BUSY`, `CANCELLED`, `CAPACITY_DENIED`, `ENGINE_DEADLINE_EXCEEDED`, `ENGINE_RESOURCE_EXHAUSTED`, `ENGINE_UNAVAILABLE`, `INTERNAL`, `INVALID_ARGUMENT`, `NODE_MISMATCH`, `OPERATION_REDIRECTED`, `RESULT_TOO_LARGE`, `SQL_OWNER_REPAIR_PENDING`, `STALE_OUTBOX_LEASE`, `STALE_ROUTE`, `TIMEOUT`, `UQL_BUDGET_EXCEEDED`, `UQL_CREDENTIAL_BEARING_SPEC`, `UQL_UNSUPPORTED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `request` | `EdgeSearchRequest` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `EdgeSearchView` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/EdgeSearch`, `contract/schemas/result.query.json#/methods/EdgeSearch`.

## `EpistemicStatus`

L53 (EPI-P3-5) acceptance capstone; handler additionally gated `epistemic-tms`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `explain:read` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `Snapshot` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `ACCESS_DENIED`, `AST_INPUT_INVALID`, `AST_INPUT_LIMIT`, `AUTHENTICATION_REQUIRED`, `AUTH_AUDIENCE_MISMATCH`, `AUTH_POLICY_VERSION_MISMATCH`, `AUTH_TENANT_MISMATCH`, `BUSY`, `CANCELLED`, `CAPACITY_DENIED`, `ENGINE_DEADLINE_EXCEEDED`, `ENGINE_RESOURCE_EXHAUSTED`, `ENGINE_UNAVAILABLE`, `INTERNAL`, `INVALID_ARGUMENT`, `NODE_MISMATCH`, `OPERATION_REDIRECTED`, `RESULT_TOO_LARGE`, `SQL_OWNER_REPAIR_PENDING`, `STALE_OUTBOX_LEASE`, `STALE_ROUTE`, `TIMEOUT`, `UQL_BUDGET_EXCEEDED`, `UQL_CREDENTIAL_BEARING_SPEC`, `UQL_UNSUPPORTED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `node_id` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `EpistemicStatusResult` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/EpistemicStatus`, `contract/schemas/result.query.json#/methods/EpistemicStatus`.

## `ExplainBelief`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `explain:read` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `Snapshot` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `ACCESS_DENIED`, `AST_INPUT_INVALID`, `AST_INPUT_LIMIT`, `AUTHENTICATION_REQUIRED`, `AUTH_AUDIENCE_MISMATCH`, `AUTH_POLICY_VERSION_MISMATCH`, `AUTH_TENANT_MISMATCH`, `BUSY`, `CANCELLED`, `CAPACITY_DENIED`, `ENGINE_DEADLINE_EXCEEDED`, `ENGINE_RESOURCE_EXHAUSTED`, `ENGINE_UNAVAILABLE`, `INTERNAL`, `INVALID_ARGUMENT`, `NODE_MISMATCH`, `OPERATION_REDIRECTED`, `RESULT_TOO_LARGE`, `SQL_OWNER_REPAIR_PENDING`, `STALE_OUTBOX_LEASE`, `STALE_ROUTE`, `TIMEOUT`, `UQL_BUDGET_EXCEEDED`, `UQL_CREDENTIAL_BEARING_SPEC`, `UQL_UNSUPPORTED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `disclosure_level` | one of: `DisclosureLevelWire` (enum: `Full`, `Skeleton`, `ExistenceOnly`) \| null | no |  |
| `node_id` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `ExplainBeliefResponse` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/ExplainBelief`, `contract/schemas/result.query.json#/methods/ExplainBelief`.

## `ExplainEvidence`

CONCEPT:EG-X1 multimodal-citation resolver; handler additionally gated `evidence-graph`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `explain:read` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `Snapshot` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `ACCESS_DENIED`, `AST_INPUT_INVALID`, `AST_INPUT_LIMIT`, `AUTHENTICATION_REQUIRED`, `AUTH_AUDIENCE_MISMATCH`, `AUTH_POLICY_VERSION_MISMATCH`, `AUTH_TENANT_MISMATCH`, `BUSY`, `CANCELLED`, `CAPACITY_DENIED`, `ENGINE_DEADLINE_EXCEEDED`, `ENGINE_RESOURCE_EXHAUSTED`, `ENGINE_UNAVAILABLE`, `INTERNAL`, `INVALID_ARGUMENT`, `NODE_MISMATCH`, `OPERATION_REDIRECTED`, `RESULT_TOO_LARGE`, `SQL_OWNER_REPAIR_PENDING`, `STALE_OUTBOX_LEASE`, `STALE_ROUTE`, `TIMEOUT`, `UQL_BUDGET_EXCEEDED`, `UQL_CREDENTIAL_BEARING_SPEC`, `UQL_UNSUPPORTED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `node_id` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `ExplainEvidenceResult` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/ExplainEvidence`, `contract/schemas/result.query.json#/methods/ExplainEvidence`.

## `ExplainPlan`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `explain:read` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `Snapshot` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `ACCESS_DENIED`, `AST_INPUT_INVALID`, `AST_INPUT_LIMIT`, `AUTHENTICATION_REQUIRED`, `AUTH_AUDIENCE_MISMATCH`, `AUTH_POLICY_VERSION_MISMATCH`, `AUTH_TENANT_MISMATCH`, `BUSY`, `CANCELLED`, `CAPACITY_DENIED`, `ENGINE_DEADLINE_EXCEEDED`, `ENGINE_RESOURCE_EXHAUSTED`, `ENGINE_UNAVAILABLE`, `INTERNAL`, `INVALID_ARGUMENT`, `NODE_MISMATCH`, `OPERATION_REDIRECTED`, `RESULT_TOO_LARGE`, `SQL_OWNER_REPAIR_PENDING`, `STALE_OUTBOX_LEASE`, `STALE_ROUTE`, `TIMEOUT`, `UQL_BUDGET_EXCEEDED`, `UQL_CREDENTIAL_BEARING_SPEC`, `UQL_UNSUPPORTED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `plan` | `Plan` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `ExplainPlanResult` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/ExplainPlan`, `contract/schemas/result.query.json#/methods/ExplainPlan`.

## `ExplainPolicy`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `explain:read` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `Snapshot` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `ACCESS_DENIED`, `AST_INPUT_INVALID`, `AST_INPUT_LIMIT`, `AUTHENTICATION_REQUIRED`, `AUTH_AUDIENCE_MISMATCH`, `AUTH_POLICY_VERSION_MISMATCH`, `AUTH_TENANT_MISMATCH`, `BUSY`, `CANCELLED`, `CAPACITY_DENIED`, `ENGINE_DEADLINE_EXCEEDED`, `ENGINE_RESOURCE_EXHAUSTED`, `ENGINE_UNAVAILABLE`, `INTERNAL`, `INVALID_ARGUMENT`, `NODE_MISMATCH`, `OPERATION_REDIRECTED`, `RESULT_TOO_LARGE`, `SQL_OWNER_REPAIR_PENDING`, `STALE_OUTBOX_LEASE`, `STALE_ROUTE`, `TIMEOUT`, `UQL_BUDGET_EXCEEDED`, `UQL_CREDENTIAL_BEARING_SPEC`, `UQL_UNSUPPORTED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `plan` | `Plan` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `ExplainPolicyResult` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/ExplainPolicy`, `contract/schemas/result.query.json#/methods/ExplainPolicy`.

## `ExplainProvenance`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `explain:read` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `Snapshot` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `ACCESS_DENIED`, `AST_INPUT_INVALID`, `AST_INPUT_LIMIT`, `AUTHENTICATION_REQUIRED`, `AUTH_AUDIENCE_MISMATCH`, `AUTH_POLICY_VERSION_MISMATCH`, `AUTH_TENANT_MISMATCH`, `BUSY`, `CANCELLED`, `CAPACITY_DENIED`, `ENGINE_DEADLINE_EXCEEDED`, `ENGINE_RESOURCE_EXHAUSTED`, `ENGINE_UNAVAILABLE`, `INTERNAL`, `INVALID_ARGUMENT`, `NODE_MISMATCH`, `OPERATION_REDIRECTED`, `RESULT_TOO_LARGE`, `SQL_OWNER_REPAIR_PENDING`, `STALE_OUTBOX_LEASE`, `STALE_ROUTE`, `TIMEOUT`, `UQL_BUDGET_EXCEEDED`, `UQL_CREDENTIAL_BEARING_SPEC`, `UQL_UNSUPPORTED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `plan` | `Plan` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `EvidenceBundle` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/ExplainProvenance`, `contract/schemas/result.query.json#/methods/ExplainProvenance`.

## `ExplainProvenanceByIds`

CONCEPT:EG-KB-CURRENCY — ID-seeded sibling of ExplainProvenance, same policy profile

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `explain:read` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `Snapshot` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `ACCESS_DENIED`, `AST_INPUT_INVALID`, `AST_INPUT_LIMIT`, `AUTHENTICATION_REQUIRED`, `AUTH_AUDIENCE_MISMATCH`, `AUTH_POLICY_VERSION_MISMATCH`, `AUTH_TENANT_MISMATCH`, `BUSY`, `CANCELLED`, `CAPACITY_DENIED`, `ENGINE_DEADLINE_EXCEEDED`, `ENGINE_RESOURCE_EXHAUSTED`, `ENGINE_UNAVAILABLE`, `INTERNAL`, `INVALID_ARGUMENT`, `NODE_MISMATCH`, `OPERATION_REDIRECTED`, `RESULT_TOO_LARGE`, `SQL_OWNER_REPAIR_PENDING`, `STALE_OUTBOX_LEASE`, `STALE_ROUTE`, `TIMEOUT`, `UQL_BUDGET_EXCEEDED`, `UQL_CREDENTIAL_BEARING_SPEC`, `UQL_UNSUPPORTED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `ids` | array of string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `EvidenceBundle` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/ExplainProvenanceByIds`, `contract/schemas/result.query.json#/methods/ExplainProvenanceByIds`.

## `GetChangeCursor`

Typed source cursors are tenant/graph/partition scoped

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `ingest:read` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `Snapshot` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `ACCESS_DENIED`, `AST_INPUT_INVALID`, `AST_INPUT_LIMIT`, `AUTHENTICATION_REQUIRED`, `AUTH_AUDIENCE_MISMATCH`, `AUTH_POLICY_VERSION_MISMATCH`, `AUTH_TENANT_MISMATCH`, `BUSY`, `CANCELLED`, `CAPACITY_DENIED`, `ENGINE_DEADLINE_EXCEEDED`, `ENGINE_RESOURCE_EXHAUSTED`, `ENGINE_UNAVAILABLE`, `INTERNAL`, `INVALID_ARGUMENT`, `NODE_MISMATCH`, `OPERATION_REDIRECTED`, `RESULT_TOO_LARGE`, `SQL_OWNER_REPAIR_PENDING`, `STALE_OUTBOX_LEASE`, `STALE_ROUTE`, `TIMEOUT`, `UQL_BUDGET_EXCEEDED`, `UQL_CREDENTIAL_BEARING_SPEC`, `UQL_UNSUPPORTED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `partition` | string | no |  |
| `source` | string | yes |  |
| `tenant` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | one of: `ChangeCursor` \| null | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/GetChangeCursor`, `contract/schemas/result.query.json#/methods/GetChangeCursor`.

## `GetChangeEnvelope`

Verified tenant-scoped reconciliation read

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `ingest:read` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `Snapshot` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `ACCESS_DENIED`, `AST_INPUT_INVALID`, `AST_INPUT_LIMIT`, `AUTHENTICATION_REQUIRED`, `AUTH_AUDIENCE_MISMATCH`, `AUTH_POLICY_VERSION_MISMATCH`, `AUTH_TENANT_MISMATCH`, `BUSY`, `CANCELLED`, `CAPACITY_DENIED`, `ENGINE_DEADLINE_EXCEEDED`, `ENGINE_RESOURCE_EXHAUSTED`, `ENGINE_UNAVAILABLE`, `INTERNAL`, `INVALID_ARGUMENT`, `NODE_MISMATCH`, `OPERATION_REDIRECTED`, `RESULT_TOO_LARGE`, `SQL_OWNER_REPAIR_PENDING`, `STALE_OUTBOX_LEASE`, `STALE_ROUTE`, `TIMEOUT`, `UQL_BUDGET_EXCEEDED`, `UQL_CREDENTIAL_BEARING_SPEC`, `UQL_UNSUPPORTED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `envelope_id` | string | yes |  |
| `tenant` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | one of: `ChangeEnvelopeRecord` \| null | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/GetChangeEnvelope`, `contract/schemas/result.query.json#/methods/GetChangeEnvelope`.

## `GetContentVersion`

Typed content versions are never compared lexically

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `ingest:read` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `Snapshot` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `ACCESS_DENIED`, `AST_INPUT_INVALID`, `AST_INPUT_LIMIT`, `AUTHENTICATION_REQUIRED`, `AUTH_AUDIENCE_MISMATCH`, `AUTH_POLICY_VERSION_MISMATCH`, `AUTH_TENANT_MISMATCH`, `BUSY`, `CANCELLED`, `CAPACITY_DENIED`, `ENGINE_DEADLINE_EXCEEDED`, `ENGINE_RESOURCE_EXHAUSTED`, `ENGINE_UNAVAILABLE`, `INTERNAL`, `INVALID_ARGUMENT`, `NODE_MISMATCH`, `OPERATION_REDIRECTED`, `RESULT_TOO_LARGE`, `SQL_OWNER_REPAIR_PENDING`, `STALE_OUTBOX_LEASE`, `STALE_ROUTE`, `TIMEOUT`, `UQL_BUDGET_EXCEEDED`, `UQL_CREDENTIAL_BEARING_SPEC`, `UQL_UNSUPPORTED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `object_id` | string | yes |  |
| `tenant` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | one of: `ContentVersion` \| null | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/GetContentVersion`, `contract/schemas/result.query.json#/methods/GetContentVersion`.

## `GetContextView`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `node:read` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `Snapshot` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `ACCESS_DENIED`, `AST_INPUT_INVALID`, `AST_INPUT_LIMIT`, `AUTHENTICATION_REQUIRED`, `AUTH_AUDIENCE_MISMATCH`, `AUTH_POLICY_VERSION_MISMATCH`, `AUTH_TENANT_MISMATCH`, `BUSY`, `CANCELLED`, `CAPACITY_DENIED`, `ENGINE_DEADLINE_EXCEEDED`, `ENGINE_RESOURCE_EXHAUSTED`, `ENGINE_UNAVAILABLE`, `INTERNAL`, `INVALID_ARGUMENT`, `NODE_MISMATCH`, `OPERATION_REDIRECTED`, `RESULT_TOO_LARGE`, `SQL_OWNER_REPAIR_PENDING`, `STALE_OUTBOX_LEASE`, `STALE_ROUTE`, `TIMEOUT`, `UQL_BUDGET_EXCEEDED`, `UQL_CREDENTIAL_BEARING_SPEC`, `UQL_UNSUPPORTED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `agent_id` | string | yes |  |
| `max_tokens` | integer (uint32) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `ContextView` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/GetContextView`, `contract/schemas/result.query.json#/methods/GetContextView`.

## `GraphQl`

runtime-conditional; ordinary writes stage through MutationBatch and cross-modal commit atomically includes universal status/fence/idempotency/outbox

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `query:graphql` |
| Mutates | `true` |
| Durability domain | `GraphRedb` |
| Idempotent | `false` |
| Audited | `true` |
| Emits CDC | `false` |
| Txn participation | `Atomic` |
| Replay class | `OperationIdentity` |
| Consumer profiles | `python` |
| Error set | `ACCESS_DENIED`, `AST_INPUT_INVALID`, `AST_INPUT_LIMIT`, `AUTHENTICATION_REQUIRED`, `AUTH_AUDIENCE_MISMATCH`, `AUTH_POLICY_VERSION_MISMATCH`, `AUTH_TENANT_MISMATCH`, `BUSY`, `CANCELLED`, `CAPACITY_DENIED`, `CONFLICT`, `CORRUPT_MUTATION_LEDGER`, `ENGINE_DEADLINE_EXCEEDED`, `ENGINE_RESOURCE_EXHAUSTED`, `ENGINE_UNAVAILABLE`, `IDEMPOTENCY_CONFLICT`, `INTERNAL`, `INVALID_ARGUMENT`, `NODE_MISMATCH`, `OPERATION_REDIRECTED`, `READ_ONLY`, `REDIRECTED`, `REPLAY_NONCE_CONSUMED`, `RESULT_TOO_LARGE`, `SQL_OWNER_REPAIR_PENDING`, `STALE_OUTBOX_LEASE`, `STALE_ROUTE`, `TIMEOUT`, `UQL_BUDGET_EXCEEDED`, `UQL_CREDENTIAL_BEARING_SPEC`, `UQL_UNSUPPORTED` |
| Format identities | `STORAGE_KERNEL_SCHEMA_VERSION`, `GRAPH_SNAPSHOT_SCHEMA_VERSION`, `GRAPH_META_SCHEMA_VERSION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `query` | string | yes |  |
| `variables` | any | no | Optional GraphQL `$variables` — a JSON object bound at execution (CONCEPT:EG-KG.query.fragments-variables-directives variables, wired through the wire path as an EG-064 follow-up). The handler binds these via `execute_with_variables` (`@skip`/`@include` + `$var` args). `None` is encoded explicitly and means an empty binding. |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | any | Raw | {'reason': 'query-rows', 'summary': "rows whose columns and value types are chosen by the caller's query text"} |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/GraphQl`, `contract/schemas/result.query.json#/methods/GraphQl`.

## `KnowledgeStream`

one RequestContext/RLS/placement-bound stream with the sole native Arrow IPC projection for all seven query families

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `query:stream` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `Snapshot` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `ACCESS_DENIED`, `AST_INPUT_INVALID`, `AST_INPUT_LIMIT`, `AUTHENTICATION_REQUIRED`, `AUTH_AUDIENCE_MISMATCH`, `AUTH_POLICY_VERSION_MISMATCH`, `AUTH_TENANT_MISMATCH`, `BUSY`, `CANCELLED`, `CAPACITY_DENIED`, `DECISION_CLAUSE_IN_UQL`, `ENGINE_DEADLINE_EXCEEDED`, `ENGINE_RESOURCE_EXHAUSTED`, `ENGINE_UNAVAILABLE`, `INTERNAL`, `INVALID_ARGUMENT`, `NODE_MISMATCH`, `OPERATION_REDIRECTED`, `RESULT_TOO_LARGE`, `SQL_OWNER_REPAIR_PENDING`, `STALE_OUTBOX_LEASE`, `STALE_ROUTE`, `TIMEOUT`, `UQL_BUDGET_EXCEEDED`, `UQL_CREDENTIAL_BEARING_SPEC`, `UQL_DUPLICATE_BINDING`, `UQL_EMPTY_PARAMETER_NAME`, `UQL_EXPECTED_INTEGER`, `UQL_FEATURE_NOT_IN_BUILD`, `UQL_INVALID_NUMBER`, `UQL_INVALID_RANGE`, `UQL_NESTING_TOO_DEEP`, `UQL_NULL_LITERAL`, `UQL_PARAMETER_TYPE`, `UQL_STATEMENT_NOT_PIPELINE`, `UQL_TRAILING_TOKENS`, `UQL_UNBOUND_PARAMETER`, `UQL_UNEXPECTED_CHARACTER`, `UQL_UNEXPECTED_TOKEN`, `UQL_UNKNOWN_BINDING`, `UQL_UNKNOWN_CHANNEL`, `UQL_UNKNOWN_FUNCTION`, `UQL_UNKNOWN_STAGE`, `UQL_UNSUPPORTED`, `UQL_UNSUPPORTED_VERSION`, `UQL_UNTERMINATED_IDENTIFIER`, `UQL_UNTERMINATED_STRING`, `UQL_UNUSED_BINDING`, `UQL_UNUSED_PARAMETER` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `request` | `KnowledgeStreamRequest` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `KnowledgeStreamBatch` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/KnowledgeStream`, `contract/schemas/result.query.json#/methods/KnowledgeStream`.

## `MaterializationStatus`

read-only status from the durable per-graph incremental reasoning authority

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `explain:read` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `Snapshot` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `ACCESS_DENIED`, `AST_INPUT_INVALID`, `AST_INPUT_LIMIT`, `AUTHENTICATION_REQUIRED`, `AUTH_AUDIENCE_MISMATCH`, `AUTH_POLICY_VERSION_MISMATCH`, `AUTH_TENANT_MISMATCH`, `BUSY`, `CANCELLED`, `CAPACITY_DENIED`, `ENGINE_DEADLINE_EXCEEDED`, `ENGINE_RESOURCE_EXHAUSTED`, `ENGINE_UNAVAILABLE`, `INTERNAL`, `INVALID_ARGUMENT`, `NODE_MISMATCH`, `OPERATION_REDIRECTED`, `RESULT_TOO_LARGE`, `SQL_OWNER_REPAIR_PENDING`, `STALE_OUTBOX_LEASE`, `STALE_ROUTE`, `TIMEOUT`, `UQL_BUDGET_EXCEEDED`, `UQL_CREDENTIAL_BEARING_SPEC`, `UQL_UNSUPPORTED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `id` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `MaterializationStatusResult` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/MaterializationStatus`, `contract/schemas/result.query.json#/methods/MaterializationStatus`.

## `NlQuery`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `query:nl` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `false` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `Snapshot` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `ACCESS_DENIED`, `AST_INPUT_INVALID`, `AST_INPUT_LIMIT`, `AUTHENTICATION_REQUIRED`, `AUTH_AUDIENCE_MISMATCH`, `AUTH_POLICY_VERSION_MISMATCH`, `AUTH_TENANT_MISMATCH`, `BUSY`, `CANCELLED`, `CAPACITY_DENIED`, `DECISION_CLAUSE_IN_UQL`, `ENGINE_DEADLINE_EXCEEDED`, `ENGINE_RESOURCE_EXHAUSTED`, `ENGINE_UNAVAILABLE`, `INTERNAL`, `INVALID_ARGUMENT`, `NODE_MISMATCH`, `OPERATION_REDIRECTED`, `RESULT_TOO_LARGE`, `SQL_OWNER_REPAIR_PENDING`, `STALE_OUTBOX_LEASE`, `STALE_ROUTE`, `TIMEOUT`, `UQL_BUDGET_EXCEEDED`, `UQL_CREDENTIAL_BEARING_SPEC`, `UQL_DUPLICATE_BINDING`, `UQL_EMPTY_PARAMETER_NAME`, `UQL_EXPECTED_INTEGER`, `UQL_FEATURE_NOT_IN_BUILD`, `UQL_INVALID_NUMBER`, `UQL_INVALID_RANGE`, `UQL_NESTING_TOO_DEEP`, `UQL_NULL_LITERAL`, `UQL_PARAMETER_TYPE`, `UQL_STATEMENT_NOT_PIPELINE`, `UQL_TRAILING_TOKENS`, `UQL_UNBOUND_PARAMETER`, `UQL_UNEXPECTED_CHARACTER`, `UQL_UNEXPECTED_TOKEN`, `UQL_UNKNOWN_BINDING`, `UQL_UNKNOWN_CHANNEL`, `UQL_UNKNOWN_FUNCTION`, `UQL_UNKNOWN_STAGE`, `UQL_UNSUPPORTED`, `UQL_UNSUPPORTED_VERSION`, `UQL_UNTERMINATED_IDENTIFIER`, `UQL_UNTERMINATED_STRING`, `UQL_UNUSED_BINDING`, `UQL_UNUSED_PARAMETER` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `graph` | string | no |  |
| `text` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | array of array of any | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/NlQuery`, `contract/schemas/result.query.json#/methods/NlQuery`.

## `RankByProvenance`

EPI-P3-3 provenance-aware retrieval ranking; handler additionally gated `epistemic-causal`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `explain:read` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `Snapshot` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `ACCESS_DENIED`, `AST_INPUT_INVALID`, `AST_INPUT_LIMIT`, `AUTHENTICATION_REQUIRED`, `AUTH_AUDIENCE_MISMATCH`, `AUTH_POLICY_VERSION_MISMATCH`, `AUTH_TENANT_MISMATCH`, `BUSY`, `CANCELLED`, `CAPACITY_DENIED`, `ENGINE_DEADLINE_EXCEEDED`, `ENGINE_RESOURCE_EXHAUSTED`, `ENGINE_UNAVAILABLE`, `INTERNAL`, `INVALID_ARGUMENT`, `NODE_MISMATCH`, `OPERATION_REDIRECTED`, `RESULT_TOO_LARGE`, `SQL_OWNER_REPAIR_PENDING`, `STALE_OUTBOX_LEASE`, `STALE_ROUTE`, `TIMEOUT`, `UQL_BUDGET_EXCEEDED`, `UQL_CREDENTIAL_BEARING_SPEC`, `UQL_UNSUPPORTED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `candidates` | array of `RetrievalCandidateWire` | yes |  |
| `weights` | `RankWeightsWire` | no |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `RankByProvenanceResult` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/RankByProvenance`, `contract/schemas/result.query.json#/methods/RankByProvenance`.

## `RecomputeMaterialization`

fenced recompute/writeback resolves provenance from the authoritative graph and fsyncs the per-graph projection

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `reasoning:write` |
| Mutates | `true` |
| Durability domain | `ReasoningProjection` |
| Idempotent | `false` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `Atomic` |
| Replay class | `OperationIdentity` |
| Consumer profiles | `python` |
| Error set | `ACCESS_DENIED`, `AST_INPUT_INVALID`, `AST_INPUT_LIMIT`, `AUTHENTICATION_REQUIRED`, `AUTH_AUDIENCE_MISMATCH`, `AUTH_POLICY_VERSION_MISMATCH`, `AUTH_TENANT_MISMATCH`, `BUSY`, `CANCELLED`, `CAPACITY_DENIED`, `CONFLICT`, `CORRUPT_MUTATION_LEDGER`, `ENGINE_DEADLINE_EXCEEDED`, `ENGINE_RESOURCE_EXHAUSTED`, `ENGINE_UNAVAILABLE`, `IDEMPOTENCY_CONFLICT`, `INTERNAL`, `INVALID_ARGUMENT`, `NODE_MISMATCH`, `OPERATION_REDIRECTED`, `READ_ONLY`, `REDIRECTED`, `RESULT_TOO_LARGE`, `SQL_OWNER_REPAIR_PENDING`, `STALE_OUTBOX_LEASE`, `STALE_ROUTE`, `TIMEOUT`, `UQL_BUDGET_EXCEEDED`, `UQL_CREDENTIAL_BEARING_SPEC`, `UQL_UNSUPPORTED` |
| Format identities | `STORAGE_KERNEL_SCHEMA_VERSION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `derived_id` | string | yes |  |
| `expected_source_graph_version` | integer (uint64) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `RecomputeMaterializationResult` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/RecomputeMaterialization`, `contract/schemas/result.query.json#/methods/RecomputeMaterialization`.

## `ResolveConflict`

EPI-P3-7 (gap-fill) standalone Dung argumentation (grounded/preferred/stable) conflict resolution over a BeliefGraph snapshot; handler additionally gated `epistemic-tms`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `explain:read` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `Snapshot` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `ACCESS_DENIED`, `AST_INPUT_INVALID`, `AST_INPUT_LIMIT`, `AUTHENTICATION_REQUIRED`, `AUTH_AUDIENCE_MISMATCH`, `AUTH_POLICY_VERSION_MISMATCH`, `AUTH_TENANT_MISMATCH`, `BUSY`, `CANCELLED`, `CAPACITY_DENIED`, `ENGINE_DEADLINE_EXCEEDED`, `ENGINE_RESOURCE_EXHAUSTED`, `ENGINE_UNAVAILABLE`, `INTERNAL`, `INVALID_ARGUMENT`, `NODE_MISMATCH`, `OPERATION_REDIRECTED`, `RESULT_TOO_LARGE`, `SQL_OWNER_REPAIR_PENDING`, `STALE_OUTBOX_LEASE`, `STALE_ROUTE`, `TIMEOUT`, `UQL_BUDGET_EXCEEDED`, `UQL_CREDENTIAL_BEARING_SPEC`, `UQL_UNSUPPORTED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `node_ids` | array of string | yes |  |
| `semantics` | string | no |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `ResolveConflictResult` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/ResolveConflict`, `contract/schemas/result.query.json#/methods/ResolveConflict`.

## `Sql`

runtime-conditional; graph DML uses staged graph state while table/catalog writes atomically commit SQL rows plus MutationBatch status/fence/idempotency/outbox

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `query:sql` |
| Mutates | `true` |
| Durability domain | `GraphRedb` |
| Idempotent | `false` |
| Audited | `true` |
| Emits CDC | `false` |
| Txn participation | `Atomic` |
| Replay class | `OperationIdentity` |
| Consumer profiles | `python` |
| Error set | `ACCESS_DENIED`, `AST_INPUT_INVALID`, `AST_INPUT_LIMIT`, `AUTHENTICATION_REQUIRED`, `AUTH_AUDIENCE_MISMATCH`, `AUTH_POLICY_VERSION_MISMATCH`, `AUTH_TENANT_MISMATCH`, `BUSY`, `CANCELLED`, `CAPACITY_DENIED`, `CONFLICT`, `CORRUPT_MUTATION_LEDGER`, `ENGINE_DEADLINE_EXCEEDED`, `ENGINE_RESOURCE_EXHAUSTED`, `ENGINE_UNAVAILABLE`, `IDEMPOTENCY_CONFLICT`, `INTERNAL`, `INVALID_ARGUMENT`, `NODE_MISMATCH`, `OPERATION_REDIRECTED`, `READ_ONLY`, `REDIRECTED`, `RESULT_TOO_LARGE`, `SQL_OWNER_REPAIR_PENDING`, `STALE_OUTBOX_LEASE`, `STALE_ROUTE`, `TIMEOUT`, `UQL_BUDGET_EXCEEDED`, `UQL_CREDENTIAL_BEARING_SPEC`, `UQL_UNSUPPORTED` |
| Format identities | `STORAGE_KERNEL_SCHEMA_VERSION`, `GRAPH_SNAPSHOT_SCHEMA_VERSION`, `GRAPH_META_SCHEMA_VERSION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `params_msgpack` | array of integer (uint8) | no |  |
| `query` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | any | Raw | {'reason': 'query-rows', 'summary': "rows whose columns and value types are chosen by the caller's query text"} |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/Sql`, `contract/schemas/result.query.json#/methods/Sql`.

## `StaleMaterializations`

bulk opaque stale references from the durable per-graph incremental reasoning authority

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `explain:read` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `Snapshot` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `ACCESS_DENIED`, `AST_INPUT_INVALID`, `AST_INPUT_LIMIT`, `AUTHENTICATION_REQUIRED`, `AUTH_AUDIENCE_MISMATCH`, `AUTH_POLICY_VERSION_MISMATCH`, `AUTH_TENANT_MISMATCH`, `BUSY`, `CANCELLED`, `CAPACITY_DENIED`, `ENGINE_DEADLINE_EXCEEDED`, `ENGINE_RESOURCE_EXHAUSTED`, `ENGINE_UNAVAILABLE`, `INTERNAL`, `INVALID_ARGUMENT`, `NODE_MISMATCH`, `OPERATION_REDIRECTED`, `RESULT_TOO_LARGE`, `SQL_OWNER_REPAIR_PENDING`, `STALE_OUTBOX_LEASE`, `STALE_ROUTE`, `TIMEOUT`, `UQL_BUDGET_EXCEEDED`, `UQL_CREDENTIAL_BEARING_SPEC`, `UQL_UNSUPPORTED` |

**Request parameters**

_No parameters._

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `StaleMaterializationsResult` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/StaleMaterializations`, `contract/schemas/result.query.json#/methods/StaleMaterializations`.

## `TxnUnifiedQuery`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `txn:read` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `Saga` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `ACCESS_DENIED`, `AST_INPUT_INVALID`, `AST_INPUT_LIMIT`, `AUTHENTICATION_REQUIRED`, `AUTH_AUDIENCE_MISMATCH`, `AUTH_POLICY_VERSION_MISMATCH`, `AUTH_TENANT_MISMATCH`, `BUSY`, `CANCELLED`, `CAPACITY_DENIED`, `ENGINE_DEADLINE_EXCEEDED`, `ENGINE_RESOURCE_EXHAUSTED`, `ENGINE_UNAVAILABLE`, `INTERNAL`, `INVALID_ARGUMENT`, `NODE_MISMATCH`, `OPERATION_REDIRECTED`, `RESULT_TOO_LARGE`, `SQL_OWNER_REPAIR_PENDING`, `STALE_OUTBOX_LEASE`, `STALE_ROUTE`, `TIMEOUT`, `UQL_BUDGET_EXCEEDED`, `UQL_CREDENTIAL_BEARING_SPEC`, `UQL_UNSUPPORTED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `plan` | `Plan` | yes |  |
| `txn_id` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | array of array of any | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/TxnUnifiedQuery`, `contract/schemas/result.query.json#/methods/TxnUnifiedQuery`.

## `TxnUql`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `txn:read` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `Saga` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `ACCESS_DENIED`, `AST_INPUT_INVALID`, `AST_INPUT_LIMIT`, `AUTHENTICATION_REQUIRED`, `AUTH_AUDIENCE_MISMATCH`, `AUTH_POLICY_VERSION_MISMATCH`, `AUTH_TENANT_MISMATCH`, `BUSY`, `CANCELLED`, `CAPACITY_DENIED`, `ENGINE_DEADLINE_EXCEEDED`, `ENGINE_RESOURCE_EXHAUSTED`, `ENGINE_UNAVAILABLE`, `INTERNAL`, `INVALID_ARGUMENT`, `NODE_MISMATCH`, `OPERATION_REDIRECTED`, `RESULT_TOO_LARGE`, `SQL_OWNER_REPAIR_PENDING`, `STALE_OUTBOX_LEASE`, `STALE_ROUTE`, `TIMEOUT`, `UQL_BUDGET_EXCEEDED`, `UQL_CREDENTIAL_BEARING_SPEC`, `UQL_UNSUPPORTED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `params` | object | no |  |
| `text` | string | yes |  |
| `txn_id` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `UqlResult` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/TxnUql`, `contract/schemas/result.query.json#/methods/TxnUql`.

## `UnifiedQuery`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `query:unified` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `Snapshot` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `ACCESS_DENIED`, `AST_INPUT_INVALID`, `AST_INPUT_LIMIT`, `AUTHENTICATION_REQUIRED`, `AUTH_AUDIENCE_MISMATCH`, `AUTH_POLICY_VERSION_MISMATCH`, `AUTH_TENANT_MISMATCH`, `BUSY`, `CANCELLED`, `CAPACITY_DENIED`, `ENGINE_DEADLINE_EXCEEDED`, `ENGINE_RESOURCE_EXHAUSTED`, `ENGINE_UNAVAILABLE`, `INTERNAL`, `INVALID_ARGUMENT`, `NODE_MISMATCH`, `OPERATION_REDIRECTED`, `RESULT_TOO_LARGE`, `SQL_OWNER_REPAIR_PENDING`, `STALE_OUTBOX_LEASE`, `STALE_ROUTE`, `TIMEOUT`, `UQL_BUDGET_EXCEEDED`, `UQL_CREDENTIAL_BEARING_SPEC`, `UQL_UNSUPPORTED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `plan` | `Plan` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | array of array of any | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/UnifiedQuery`, `contract/schemas/result.query.json#/methods/UnifiedQuery`.

## `Uql`

UQL statement: typed params, EXPLAIN/PROFILE, LET programs, RETURN channels; read-only

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `query:unified` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `Snapshot` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `ACCESS_DENIED`, `AST_INPUT_INVALID`, `AST_INPUT_LIMIT`, `AUTHENTICATION_REQUIRED`, `AUTH_AUDIENCE_MISMATCH`, `AUTH_POLICY_VERSION_MISMATCH`, `AUTH_TENANT_MISMATCH`, `BUSY`, `CANCELLED`, `CAPACITY_DENIED`, `DECISION_CLAUSE_IN_UQL`, `ENGINE_DEADLINE_EXCEEDED`, `ENGINE_RESOURCE_EXHAUSTED`, `ENGINE_UNAVAILABLE`, `INTERNAL`, `INVALID_ARGUMENT`, `NODE_MISMATCH`, `OPERATION_REDIRECTED`, `RESULT_TOO_LARGE`, `SQL_OWNER_REPAIR_PENDING`, `STALE_OUTBOX_LEASE`, `STALE_ROUTE`, `TIMEOUT`, `UQL_BUDGET_EXCEEDED`, `UQL_CREDENTIAL_BEARING_SPEC`, `UQL_DUPLICATE_BINDING`, `UQL_EMPTY_PARAMETER_NAME`, `UQL_EXPECTED_INTEGER`, `UQL_FEATURE_NOT_IN_BUILD`, `UQL_INVALID_NUMBER`, `UQL_INVALID_RANGE`, `UQL_NESTING_TOO_DEEP`, `UQL_NULL_LITERAL`, `UQL_PARAMETER_TYPE`, `UQL_STATEMENT_NOT_PIPELINE`, `UQL_TRAILING_TOKENS`, `UQL_UNBOUND_PARAMETER`, `UQL_UNEXPECTED_CHARACTER`, `UQL_UNEXPECTED_TOKEN`, `UQL_UNKNOWN_BINDING`, `UQL_UNKNOWN_CHANNEL`, `UQL_UNKNOWN_FUNCTION`, `UQL_UNKNOWN_STAGE`, `UQL_UNSUPPORTED`, `UQL_UNSUPPORTED_VERSION`, `UQL_UNTERMINATED_IDENTIFIER`, `UQL_UNTERMINATED_STRING`, `UQL_UNUSED_BINDING`, `UQL_UNUSED_PARAMETER` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `params` | object | no |  |
| `text` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `UqlResult` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/Uql`, `contract/schemas/result.query.json#/methods/Uql`.

## `WhatChanged`

L53 (EPI-P3-5) bitemporal diff; handler additionally gated `epistemic-tms`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `explain:read` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `Snapshot` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `ACCESS_DENIED`, `AST_INPUT_INVALID`, `AST_INPUT_LIMIT`, `AUTHENTICATION_REQUIRED`, `AUTH_AUDIENCE_MISMATCH`, `AUTH_POLICY_VERSION_MISMATCH`, `AUTH_TENANT_MISMATCH`, `BUSY`, `CANCELLED`, `CAPACITY_DENIED`, `ENGINE_DEADLINE_EXCEEDED`, `ENGINE_RESOURCE_EXHAUSTED`, `ENGINE_UNAVAILABLE`, `INTERNAL`, `INVALID_ARGUMENT`, `NODE_MISMATCH`, `OPERATION_REDIRECTED`, `RESULT_TOO_LARGE`, `SQL_OWNER_REPAIR_PENDING`, `STALE_OUTBOX_LEASE`, `STALE_ROUTE`, `TIMEOUT`, `UQL_BUDGET_EXCEEDED`, `UQL_CREDENTIAL_BEARING_SPEC`, `UQL_UNSUPPORTED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `tx_from` | integer (uint64) | yes |  |
| `tx_to` | integer (uint64) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `WhatChangedResult` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/WhatChanged`, `contract/schemas/result.query.json#/methods/WhatChanged`.
