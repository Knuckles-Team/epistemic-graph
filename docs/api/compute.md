# Compute API reference

> **GENERATED** by `scripts/gen_api_docs.py` from `contract/methods.json` and `contract/schemas/method.request.json` / `contract/schemas/result.compute.json` -- do not hand-edit. Regenerate with `python3 scripts/gen_api_docs.py --write`. 133 methods in this namespace. See also the machine-checked policy ledger at [`capabilities.generated.md`](../capabilities.generated.md) and the [OpenAPI document](../openapi.json) / [Swagger UI](../swagger-ui.md).

## `BatchL2Normalize`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:semantic` |
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
| `vectors` | array of array of number (double) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | array of array of number (double) | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/BatchL2Normalize`, `contract/schemas/result.compute.json#/methods/BatchL2Normalize`.

## `BetweennessCentrality`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:graph-algo` |
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
| `result` | array of array of any | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/BetweennessCentrality`, `contract/schemas/result.compute.json#/methods/BetweennessCentrality`.

## `ClusterHierarchyClusters`

VIZ-1: GET clusters(graph, level, parent_cluster_id?) from the cached hierarchy

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:graph-algo` |
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
| `level` | integer (uint) | yes |  |
| `parent_cluster_id` | string \| null | no |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `ClusterLevelView` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/ClusterHierarchyClusters`, `contract/schemas/result.compute.json#/methods/ClusterHierarchyClusters`.

## `ClusterHierarchyExpand`

VIZ-1: GET expand(graph, cluster_id) -- a level-1 cluster's member nodes/edges read live off the graph

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:graph-algo` |
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
| `cluster_id` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `ClusterExpansion` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/ClusterHierarchyExpand`, `contract/schemas/result.compute.json#/methods/ClusterHierarchyExpand`.

## `ClusterHierarchyRefresh`

VIZ-1 hierarchical Leiden clustering for million-node graph visualization: (re)computes and durably caches the cluster hierarchy in its own non-authoritative store (server::persistence::cluster_hierarchy_store), never as graph nodes/edges -- see ClusterHierarchyClusters/ClusterHierarchyExpand

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:graph-algo` |
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
| `label` | string \| null | no | Optional: restrict the clustered projection to nodes of this one type — mirrors `MineCommunity::label`. |
| `resolution` | number (double) | no | Leiden resolution γ (higher ⇒ more, smaller clusters). |
| `seed` | integer (uint64) | no |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `ClusterHierarchySummary` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/ClusterHierarchyRefresh`, `contract/schemas/result.compute.json#/methods/ClusterHierarchyRefresh`.

## `CommunityDetectEphemeral`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:graph-algo` |
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
| `edges` | array of array of any | yes |  |
| `node_ids` | array of string | yes |  |
| `resolution` | number (double) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | array of array of string | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/CommunityDetectEphemeral`, `contract/schemas/result.compute.json#/methods/CommunityDetectEphemeral`.

## `CommunityDetection`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:graph-algo` |
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
| `resolution` | number (double) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | array of array of string | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/CommunityDetection`, `contract/schemas/result.compute.json#/methods/CommunityDetection`.

## `ComputeSimilarityEdges`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:graph-algo` |
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
| `threshold` | number (double) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | array of array of any | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/ComputeSimilarityEdges`, `contract/schemas/result.compute.json#/methods/ComputeSimilarityEdges`.

## `ConnectedComponents`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:graph-algo` |
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
| `result` | array of array of string | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/ConnectedComponents`, `contract/schemas/result.compute.json#/methods/ConnectedComponents`.

## `DegreeCentrality`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:graph-algo` |
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
| `node_id` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | number (double) | Float |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/DegreeCentrality`, `contract/schemas/result.compute.json#/methods/DegreeCentrality`.

## `DegreeCentralityAll`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:graph-algo` |
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
| `result` | array of array of any | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/DegreeCentralityAll`, `contract/schemas/result.compute.json#/methods/DegreeCentralityAll`.

## `DistributedCompute`

read-only Pregel/GAS computation; materialization uses the distinct Create*/Refresh* methods

| Property | Value |
|---|---|
| Stability | `internal` |
| Authz action | `distcompute:read` |
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
| `algo` | `DistAlgo` | yes |  |
| `graphs` | array of string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `DistResult` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/DistributedCompute`, `contract/schemas/result.compute.json#/methods/DistributedCompute`.

## `DsAdamStep`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:datascience` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `beta1` | number (double) | yes |  |
| `beta2` | number (double) | yes |  |
| `eps` | number (double) | yes |  |
| `grads` | array of number (double) | yes |  |
| `lr` | number (double) | yes |  |
| `m` | array of number (double) | yes |  |
| `params` | array of number (double) | yes |  |
| `t` | integer (uint64) | yes |  |
| `v` | array of number (double) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `AdamResult` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/DsAdamStep`, `contract/schemas/result.compute.json#/methods/DsAdamStep`.

## `DsComputeStats`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:datascience` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `data` | array of array of number (double) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `DatasetStats` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/DsComputeStats`, `contract/schemas/result.compute.json#/methods/DsComputeStats`.

## `DsCrossEntropy`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:datascience` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `labels` | array of integer (uint) | yes |  |
| `logits` | array of array of number (double) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `CrossEntropyResult` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/DsCrossEntropy`, `contract/schemas/result.compute.json#/methods/DsCrossEntropy`.

## `DsDpoLoss`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:datascience` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `beta` | number (double) | yes |  |
| `policy_chosen` | array of number (double) | yes |  |
| `policy_rejected` | array of number (double) | yes |  |
| `ref_chosen` | array of number (double) | yes |  |
| `ref_rejected` | array of number (double) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `DpoResult` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/DsDpoLoss`, `contract/schemas/result.compute.json#/methods/DsDpoLoss`.

## `DsFitEstimator`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:datascience` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `estimator` | string | yes |  |
| `params` | `EstimatorParams` | no |  |
| `x` | array of array of number (double) | yes |  |
| `y` | array of number (double) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `FittedModel` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/DsFitEstimator`, `contract/schemas/result.compute.json#/methods/DsFitEstimator`.

## `DsGrpoSurrogate`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:datascience` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `advantage` | array of number (double) | yes |  |
| `clip_eps` | number (double) | yes |  |
| `logprob` | array of number (double) | yes |  |
| `old_logprob` | array of number (double) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `GrpoResult` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/DsGrpoSurrogate`, `contract/schemas/result.compute.json#/methods/DsGrpoSurrogate`.

## `DsKMeans`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:datascience` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `data` | array of array of number (double) | yes |  |
| `k` | integer (uint) | yes |  |
| `max_iter` | integer (uint) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `KMeansResult` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/DsKMeans`, `contract/schemas/result.compute.json#/methods/DsKMeans`.

## `DsKlDivergence`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:datascience` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `logprob` | array of number (double) | yes |  |
| `ref_logprob` | array of number (double) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | number (double) | Float |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/DsKlDivergence`, `contract/schemas/result.compute.json#/methods/DsKlDivergence`.

## `DsLinearRegression`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:datascience` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `x` | array of array of number (double) | yes |  |
| `y` | array of number (double) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `RegressionResult` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/DsLinearRegression`, `contract/schemas/result.compute.json#/methods/DsLinearRegression`.

## `DsLogSoftmax`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:datascience` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `logits` | array of number (double) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | array of number (double) | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/DsLogSoftmax`, `contract/schemas/result.compute.json#/methods/DsLogSoftmax`.

## `DsPca`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:datascience` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `data` | array of array of number (double) | yes |  |
| `n_components` | integer (uint) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `PCAResult` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/DsPca`, `contract/schemas/result.compute.json#/methods/DsPca`.

## `DsPredictEstimator`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:datascience` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `model` | `FittedModel` | yes |  |
| `x` | array of array of number (double) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | array of number (double) | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/DsPredictEstimator`, `contract/schemas/result.compute.json#/methods/DsPredictEstimator`.

## `DsSgdStep`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:datascience` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `grads` | array of number (double) | yes |  |
| `lr` | number (double) | yes |  |
| `params` | array of number (double) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | array of number (double) | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/DsSgdStep`, `contract/schemas/result.compute.json#/methods/DsSgdStep`.

## `DsSoftmax`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:datascience` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `logits` | array of number (double) | yes |  |
| `temperature` | number (double) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | array of number (double) | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/DsSoftmax`, `contract/schemas/result.compute.json#/methods/DsSoftmax`.

## `DsTrainTestSplit`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:datascience` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `data` | array of array of number (double) | yes |  |
| `labels` | array of number (double) | yes |  |
| `seed` | integer (uint64) | yes |  |
| `shuffle` | boolean | yes |  |
| `test_ratio` | number (double) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `TrainTestSplitResult` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/DsTrainTestSplit`, `contract/schemas/result.compute.json#/methods/DsTrainTestSplit`.

## `FinanceAdfTest`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:finance` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `max_lag` | integer (uint) | yes |  |
| `series` | array of number (double) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `AdfResult` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/FinanceAdfTest`, `contract/schemas/result.compute.json#/methods/FinanceAdfTest`.

## `FinanceAlphaCombinationEngine`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:finance` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `lookback` | integer (uint) | yes |  |
| `returns_matrix` | array of array of number (double) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | array of number (double) | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/FinanceAlphaCombinationEngine`, `contract/schemas/result.compute.json#/methods/FinanceAlphaCombinationEngine`.

## `FinanceAvellanedaStoikov`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:finance` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `gamma` | number (double) | yes |  |
| `inventory` | number (double) | yes |  |
| `kappa` | number (double) | yes |  |
| `mid` | number (double) | yes |  |
| `sigma` | number (double) | yes |  |
| `tau` | number (double) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `Quote` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/FinanceAvellanedaStoikov`, `contract/schemas/result.compute.json#/methods/FinanceAvellanedaStoikov`.

## `FinanceBayesianKelly`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:finance` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `alpha` | number (double) | yes |  |
| `beta` | number (double) | yes |  |
| `c` | number (double) | yes |  |
| `n_quadrature` | integer (uint) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | number (double) | Float |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/FinanceBayesianKelly`, `contract/schemas/result.compute.json#/methods/FinanceBayesianKelly`.

## `FinanceBlackLitterman`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:finance` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `cov_matrix` | array of array of number (double) | yes |  |
| `market_weights` | array of number (double) | yes |  |
| `pick_matrix` | array of array of number (double) | yes |  |
| `risk_aversion` | number (double) | yes |  |
| `tau` | number (double) | yes |  |
| `views` | array of number (double) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `OptimizationResult` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/FinanceBlackLitterman`, `contract/schemas/result.compute.json#/methods/FinanceBlackLitterman`.

## `FinanceBreakevenAlpha`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:finance` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `delta` | number (double) | yes |  |
| `p` | number (double) | yes |  |
| `v_h` | number (double) | yes |  |
| `v_l` | number (double) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | number (double) | Float |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/FinanceBreakevenAlpha`, `contract/schemas/result.compute.json#/methods/FinanceBreakevenAlpha`.

## `FinanceBrierScore`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:finance` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `forecasts` | array of number (double) | yes |  |
| `outcomes` | array of number (double) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | number (double) | Float |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/FinanceBrierScore`, `contract/schemas/result.compute.json#/methods/FinanceBrierScore`.

## `FinanceCombineAlphas`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:finance` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `signals` | array of array of number (double) | yes |  |
| `weights` | array of number (double) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | array of number (double) | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/FinanceCombineAlphas`, `contract/schemas/result.compute.json#/methods/FinanceCombineAlphas`.

## `FinanceConvergenceGate`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:finance` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `min_agree` | integer (uint) | yes |  |
| `strengths` | array of number (double) | yes |  |
| `strong_threshold` | number (double) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `ConvergenceGate` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/FinanceConvergenceGate`, `contract/schemas/result.compute.json#/methods/FinanceConvergenceGate`.

## `FinanceCrossSectionalRank`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:finance` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `cross_section` | array of array of number (double) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | array of array of number (double) | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/FinanceCrossSectionalRank`, `contract/schemas/result.compute.json#/methods/FinanceCrossSectionalRank`.

## `FinanceCvar`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:finance` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `confidence` | number (double) | yes |  |
| `returns` | array of number (double) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | number (double) | Float |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/FinanceCvar`, `contract/schemas/result.compute.json#/methods/FinanceCvar`.

## `FinanceDeflatedSharpe`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:finance` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `n_trials` | integer (uint) | yes |  |
| `observed_sr` | number (double) | yes |  |
| `sr_returns` | array of number (double) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | number (double) | Float |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/FinanceDeflatedSharpe`, `contract/schemas/result.compute.json#/methods/FinanceDeflatedSharpe`.

## `FinanceDetectRegimes`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:finance` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `max_iter` | integer (uint) | yes |  |
| `n_states` | integer (uint) | yes |  |
| `observations` | array of number (double) | yes |  |
| `tol` | number (double) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `RegimeResult` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/FinanceDetectRegimes`, `contract/schemas/result.compute.json#/methods/FinanceDetectRegimes`.

## `FinanceDieboldMariano`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:finance` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `h` | integer (uint) | yes |  |
| `losses_a` | array of number (double) | yes |  |
| `losses_b` | array of number (double) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `DieboldMariano` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/FinanceDieboldMariano`, `contract/schemas/result.compute.json#/methods/FinanceDieboldMariano`.

## `FinanceDownsideDeviation`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:finance` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `returns` | array of number (double) | yes |  |
| `target` | number (double) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | number (double) | Float |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/FinanceDownsideDeviation`, `contract/schemas/result.compute.json#/methods/FinanceDownsideDeviation`.

## `FinanceDrawdownSeries`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:finance` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `returns` | array of number (double) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | array of number (double) | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/FinanceDrawdownSeries`, `contract/schemas/result.compute.json#/methods/FinanceDrawdownSeries`.

## `FinanceEffectiveIndependentN`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:finance` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `returns_matrix` | array of array of number (double) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | number (double) | Float |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/FinanceEffectiveIndependentN`, `contract/schemas/result.compute.json#/methods/FinanceEffectiveIndependentN`.

## `FinanceEfficientFrontier`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:finance` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `cov_matrix` | array of array of number (double) | yes |  |
| `expected_returns` | array of number (double) | yes |  |
| `target_return` | number (double) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `OptimizationResult` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/FinanceEfficientFrontier`, `contract/schemas/result.compute.json#/methods/FinanceEfficientFrontier`.

## `FinanceEmpiricalKelly`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:finance` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `b` | number (double) | yes |  |
| `historical_returns` | array of number (double) | yes |  |
| `n_simulations` | integer (uint) | yes |  |
| `p` | number (double) | yes |  |
| `seed` | integer (uint64) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | number (double) | Float |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/FinanceEmpiricalKelly`, `contract/schemas/result.compute.json#/methods/FinanceEmpiricalKelly`.

## `FinanceEwma`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:finance` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `span` | integer (uint) | yes |  |
| `values` | array of number (double) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | array of number (double) | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/FinanceEwma`, `contract/schemas/result.compute.json#/methods/FinanceEwma`.

## `FinanceExpectedPnlRate`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:finance` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `a` | number (double) | yes |  |
| `alpha` | number (double) | yes |  |
| `delta` | number (double) | yes |  |
| `kappa` | number (double) | yes |  |
| `p` | number (double) | yes |  |
| `v_h` | number (double) | yes |  |
| `v_l` | number (double) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | number (double) | Float |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/FinanceExpectedPnlRate`, `contract/schemas/result.compute.json#/methods/FinanceExpectedPnlRate`.

## `FinanceForensicReport`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:finance` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `prior_year` | `YearData` | yes |  |
| `this_year` | `YearData` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `ForensicReport` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/FinanceForensicReport`, `contract/schemas/result.compute.json#/methods/FinanceForensicReport`.

## `FinanceGlostenMilgromSpread`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:finance` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `alpha` | number (double) | yes |  |
| `p` | number (double) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | number (double) | Float |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/FinanceGlostenMilgromSpread`, `contract/schemas/result.compute.json#/methods/FinanceGlostenMilgromSpread`.

## `FinanceGltQuotes`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:finance` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `a` | number (double) | yes |  |
| `gamma` | number (double) | yes |  |
| `inventory` | number (double) | yes |  |
| `kappa` | number (double) | yes |  |
| `mid` | number (double) | yes |  |
| `sigma` | number (double) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `Quote` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/FinanceGltQuotes`, `contract/schemas/result.compute.json#/methods/FinanceGltQuotes`.

## `FinanceHardimanBouchaud`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:finance` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `n_windows` | integer (uint) | yes |  |
| `t_horizon` | number (double) | yes |  |
| `times` | array of number (double) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | number (double) | Float |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/FinanceHardimanBouchaud`, `contract/schemas/result.compute.json#/methods/FinanceHardimanBouchaud`.

## `FinanceHawkesMle`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:finance` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `max_iter` | integer (uint) | yes |  |
| `t_horizon` | number (double) | yes |  |
| `times` | array of number (double) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `HawkesFit` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/FinanceHawkesMle`, `contract/schemas/result.compute.json#/methods/FinanceHawkesMle`.

## `FinanceInformationCoefficient`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:finance` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `forward_returns` | array of number (double) | yes |  |
| `signal` | array of number (double) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | number (double) | Float |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/FinanceInformationCoefficient`, `contract/schemas/result.compute.json#/methods/FinanceInformationCoefficient`.

## `FinanceInformationRatio`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:finance` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `ic` | number (double) | yes |  |
| `n_independent` | number (double) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | number (double) | Float |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/FinanceInformationRatio`, `contract/schemas/result.compute.json#/methods/FinanceInformationRatio`.

## `FinanceKalmanBeta`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:finance` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `asset_returns` | array of number (double) | yes |  |
| `beta0` | number (double) | yes |  |
| `market_returns` | array of number (double) | yes |  |
| `p0` | number (double) | yes |  |
| `q` | number (double) | yes |  |
| `r` | number (double) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `KalmanState` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/FinanceKalmanBeta`, `contract/schemas/result.compute.json#/methods/FinanceKalmanBeta`.

## `FinanceKalmanFilter1d`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:finance` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `f` | number (double) | yes |  |
| `h` | number (double) | yes |  |
| `observations` | array of number (double) | yes |  |
| `p0` | number (double) | yes |  |
| `q` | number (double) | yes |  |
| `r` | number (double) | yes |  |
| `x0` | number (double) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `KalmanState` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/FinanceKalmanFilter1d`, `contract/schemas/result.compute.json#/methods/FinanceKalmanFilter1d`.

## `FinanceKalmanVolatility`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:finance` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `annualization` | number (double) | yes |  |
| `log_var0` | number \| null | no |  |
| `p0` | number (double) | yes |  |
| `q` | number (double) | yes |  |
| `r` | number (double) | yes |  |
| `returns` | array of number (double) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | array of number (double) | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/FinanceKalmanVolatility`, `contract/schemas/result.compute.json#/methods/FinanceKalmanVolatility`.

## `FinanceKellyFraction`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:finance` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `c` | number (double) | yes |  |
| `fraction` | number (double) | yes |  |
| `q` | number (double) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | number (double) | Float |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/FinanceKellyFraction`, `contract/schemas/result.compute.json#/methods/FinanceKellyFraction`.

## `FinanceKyleLambda`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:finance` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `price_changes` | array of number (double) | yes |  |
| `signed_order_flow` | array of number (double) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | number (double) | Float |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/FinanceKyleLambda`, `contract/schemas/result.compute.json#/methods/FinanceKyleLambda`.

## `FinanceLogitQuotes`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:finance` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `boundary_m` | number (double) | yes |  |
| `gamma` | number (double) | yes |  |
| `inventory` | number (double) | yes |  |
| `kappa` | number (double) | yes |  |
| `p_mid` | number (double) | yes |  |
| `sigma` | number (double) | yes |  |
| `tau` | number (double) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `Quote` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/FinanceLogitQuotes`, `contract/schemas/result.compute.json#/methods/FinanceLogitQuotes`.

## `FinanceMarketImpact`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:finance` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `average_daily_volume` | number (double) | yes |  |
| `daily_volatility` | number (double) | yes |  |
| `impact_coefficient` | number (double) | yes |  |
| `order_quantity` | number (double) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | number (double) | Float |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/FinanceMarketImpact`, `contract/schemas/result.compute.json#/methods/FinanceMarketImpact`.

## `FinanceMarkovTransitionMatrix`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:finance` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `n_states` | integer (uint) | yes |  |
| `states` | array of integer (uint) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | array of array of number (double) | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/FinanceMarkovTransitionMatrix`, `contract/schemas/result.compute.json#/methods/FinanceMarkovTransitionMatrix`.

## `FinanceMatchOrders`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:finance` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `orders` | array of `Order` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | array of `Fill` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/FinanceMatchOrders`, `contract/schemas/result.compute.json#/methods/FinanceMatchOrders`.

## `FinanceMaxDrawdown`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:finance` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `returns` | array of number (double) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | number (double) | Float |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/FinanceMaxDrawdown`, `contract/schemas/result.compute.json#/methods/FinanceMaxDrawdown`.

## `FinanceMeanReversion`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:finance` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `values` | array of number (double) | yes |  |
| `window` | integer (uint) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | array of number (double) | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/FinanceMeanReversion`, `contract/schemas/result.compute.json#/methods/FinanceMeanReversion`.

## `FinanceMicropriceSeries`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:finance` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `ask_px` | array of number (double) | yes |  |
| `ask_sz` | array of number (double) | yes |  |
| `bid_px` | array of number (double) | yes |  |
| `bid_sz` | array of number (double) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | array of number (double) | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/FinanceMicropriceSeries`, `contract/schemas/result.compute.json#/methods/FinanceMicropriceSeries`.

## `FinanceMomentum`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:finance` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `lookback` | integer (uint) | yes |  |
| `prices` | array of number (double) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | array of number (double) | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/FinanceMomentum`, `contract/schemas/result.compute.json#/methods/FinanceMomentum`.

## `FinanceMonteCarloVar`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:finance` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `confidence` | number (double) | yes |  |
| `mean` | number (double) | yes |  |
| `n_simulations` | integer (uint) | yes |  |
| `std_dev` | number (double) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | number (double) | Float |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/FinanceMonteCarloVar`, `contract/schemas/result.compute.json#/methods/FinanceMonteCarloVar`.

## `FinanceOfiSeries`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:finance` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `ask_px` | array of number (double) | yes |  |
| `ask_sz` | array of number (double) | yes |  |
| `bid_px` | array of number (double) | yes |  |
| `bid_sz` | array of number (double) | yes |  |
| `ts` | array of number (double) | yes |  |
| `window_secs` | number (double) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | array of number (double) | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/FinanceOfiSeries`, `contract/schemas/result.compute.json#/methods/FinanceOfiSeries`.

## `FinanceOptimizePortfolio`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:finance` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `cov_matrix` | array of array of number (double) | yes |  |
| `expected_returns` | array of number (double) | yes |  |
| `max_weight` | number \| null | no |  |
| `min_weight` | number \| null | no |  |
| `risk_free_rate` | number (double) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `OptimizationResult` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/FinanceOptimizePortfolio`, `contract/schemas/result.compute.json#/methods/FinanceOptimizePortfolio`.

## `FinanceOrderBookImbalance`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:finance` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `v_ask` | array of number (double) | yes |  |
| `v_bid` | array of number (double) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | array of number (double) | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/FinanceOrderBookImbalance`, `contract/schemas/result.compute.json#/methods/FinanceOrderBookImbalance`.

## `FinanceOuCalibrate`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:finance` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `dt` | number (double) | yes |  |
| `spread` | array of number (double) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `OuParams` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/FinanceOuCalibrate`, `contract/schemas/result.compute.json#/methods/FinanceOuCalibrate`.

## `FinanceOuOptimalThresholds`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:finance` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `cost` | number (double) | yes |  |
| `mu` | number (double) | yes |  |
| `sigma` | number (double) | yes |  |
| `sigma_eq` | number (double) | yes |  |
| `theta` | number (double) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `OuThresholds` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/FinanceOuOptimalThresholds`, `contract/schemas/result.compute.json#/methods/FinanceOuOptimalThresholds`.

## `FinancePairsTrading`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:finance` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `lookback` | integer (uint) | yes |  |
| `prices_a` | array of number (double) | yes |  |
| `prices_b` | array of number (double) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | array of number (double) | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/FinancePairsTrading`, `contract/schemas/result.compute.json#/methods/FinancePairsTrading`.

## `FinancePosteriorCredibleInterval`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:finance` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `alpha` | number (double) | yes |  |
| `beta` | number (double) | yes |  |
| `level` | number (double) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `PosteriorCredibleInterval` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/FinancePosteriorCredibleInterval`, `contract/schemas/result.compute.json#/methods/FinancePosteriorCredibleInterval`.

## `FinanceProbabilityBacktestOverfit`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:finance` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `insample` | array of array of number (double) | yes |  |
| `oos` | array of array of number (double) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | number (double) | Float |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/FinanceProbabilityBacktestOverfit`, `contract/schemas/result.compute.json#/methods/FinanceProbabilityBacktestOverfit`.

## `FinancePurgedCpcv`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:finance` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `embargo` | integer (uint) | yes |  |
| `n_groups` | integer (uint) | yes |  |
| `n_samples` | integer (uint) | yes |  |
| `n_test_groups` | integer (uint) | yes |  |
| `purge_window` | integer (uint) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | array of `CvSplit` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/FinancePurgedCpcv`, `contract/schemas/result.compute.json#/methods/FinancePurgedCpcv`.

## `FinanceQueueImbalance`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:finance` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `ask_q` | array of number (double) | yes |  |
| `ask_rate` | array of number (double) | yes |  |
| `bid_q` | array of number (double) | yes |  |
| `bid_rate` | array of number (double) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `QueueSignal` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/FinanceQueueImbalance`, `contract/schemas/result.compute.json#/methods/FinanceQueueImbalance`.

## `FinanceRealizedVolTick`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:finance` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `mid` | array of number (double) | yes |  |
| `window` | integer (uint) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | array of number (double) | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/FinanceRealizedVolTick`, `contract/schemas/result.compute.json#/methods/FinanceRealizedVolTick`.

## `FinanceRiskMetrics`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:finance` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `returns` | array of number (double) | yes |  |
| `risk_free_rate` | number (double) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `RiskMetrics` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/FinanceRiskMetrics`, `contract/schemas/result.compute.json#/methods/FinanceRiskMetrics`.

## `FinanceRiskParity`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:finance` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `cov_matrix` | array of array of number (double) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `OptimizationResult` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/FinanceRiskParity`, `contract/schemas/result.compute.json#/methods/FinanceRiskParity`.

## `FinanceRollingZscore`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:finance` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `values` | array of number (double) | yes |  |
| `window` | integer (uint) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | array of number (double) | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/FinanceRollingZscore`, `contract/schemas/result.compute.json#/methods/FinanceRollingZscore`.

## `FinanceSabrCalibrate`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:finance` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `beta` | number (double) | yes |  |
| `f` | number (double) | yes |  |
| `market_vols` | array of number (double) | yes |  |
| `strikes` | array of number (double) | yes |  |
| `t` | number (double) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `SabrFit` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/FinanceSabrCalibrate`, `contract/schemas/result.compute.json#/methods/FinanceSabrCalibrate`.

## `FinanceSabrImpliedVol`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:finance` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `alpha` | number (double) | yes |  |
| `beta` | number (double) | yes |  |
| `f` | number (double) | yes |  |
| `k` | number (double) | yes |  |
| `nu` | number (double) | yes |  |
| `rho` | number (double) | yes |  |
| `t` | number (double) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | number (double) | Float |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/FinanceSabrImpliedVol`, `contract/schemas/result.compute.json#/methods/FinanceSabrImpliedVol`.

## `FinanceSabrSmile`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:finance` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `alpha` | number (double) | yes |  |
| `beta` | number (double) | yes |  |
| `f` | number (double) | yes |  |
| `nu` | number (double) | yes |  |
| `rho` | number (double) | yes |  |
| `strikes` | array of number (double) | yes |  |
| `t` | number (double) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | array of number (double) | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/FinanceSabrSmile`, `contract/schemas/result.compute.json#/methods/FinanceSabrSmile`.

## `FinanceSignalDecay`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:finance` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `half_life` | number (double) | yes |  |
| `signal` | array of number (double) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | array of number (double) | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/FinanceSignalDecay`, `contract/schemas/result.compute.json#/methods/FinanceSignalDecay`.

## `FinanceSpreadReversion`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:finance` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `ask_px` | array of number (double) | yes |  |
| `bid_px` | array of number (double) | yes |  |
| `window` | integer (uint) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `SpreadReversion` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/FinanceSpreadReversion`, `contract/schemas/result.compute.json#/methods/FinanceSpreadReversion`.

## `FinanceStressTest`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:finance` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `cov_matrix` | array of array of number (double) | yes |  |
| `expected_returns` | array of number (double) | yes |  |
| `shock_factors` | array of number (double) | yes |  |
| `weights` | array of number (double) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | array of number (double) | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/FinanceStressTest`, `contract/schemas/result.compute.json#/methods/FinanceStressTest`.

## `FinanceSurveillanceRisk`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:finance` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `baseline_sigma` | number (double) | yes |  |
| `buy_vol` | array of number (double) | yes |  |
| `p_mean` | array of number (double) | yes |  |
| `price_changes` | array of number (double) | yes |  |
| `sell_vol` | array of number (double) | yes |  |
| `signed_flow` | array of number (double) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `SurveillanceRisk` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/FinanceSurveillanceRisk`, `contract/schemas/result.compute.json#/methods/FinanceSurveillanceRisk`.

## `FinanceTwap`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:finance` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `interval_secs` | integer (uint64) | yes |  |
| `n_slices` | integer (uint) | yes |  |
| `start_time` | integer (uint64) | yes |  |
| `total_quantity` | number (double) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | array of array of any | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/FinanceTwap`, `contract/schemas/result.compute.json#/methods/FinanceTwap`.

## `FinanceVar`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:finance` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `confidence` | number (double) | yes |  |
| `returns` | array of number (double) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | number (double) | Float |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/FinanceVar`, `contract/schemas/result.compute.json#/methods/FinanceVar`.

## `FinanceVpinPm`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:finance` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `buy_vol` | array of number (double) | yes |  |
| `p_mean` | array of number (double) | yes |  |
| `sell_vol` | array of number (double) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | number (double) | Float |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/FinanceVpinPm`, `contract/schemas/result.compute.json#/methods/FinanceVpinPm`.

## `FinanceVwap`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:finance` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `interval_secs` | integer (uint64) | yes |  |
| `start_time` | integer (uint64) | yes |  |
| `total_quantity` | number (double) | yes |  |
| `volume_profile` | array of number (double) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | array of array of any | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/FinanceVwap`, `contract/schemas/result.compute.json#/methods/FinanceVwap`.

## `FindCycle`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:graph-algo` |
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
| `result` | array \| null | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/FindCycle`, `contract/schemas/result.compute.json#/methods/FindCycle`.

## `GetBlastRadius`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:graph-algo` |
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
| `max_depth` | integer (uint) | yes |  |
| `node_id` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | array of string | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/GetBlastRadius`, `contract/schemas/result.compute.json#/methods/GetBlastRadius`.

## `GetShortestPath`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:graph-algo` |
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
| `source_id` | string | yes |  |
| `target_id` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | array \| null | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/GetShortestPath`, `contract/schemas/result.compute.json#/methods/GetShortestPath`.

## `GraphColoring`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:graph-algo` |
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
| `result` | array of array of any | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/GraphColoring`, `contract/schemas/result.compute.json#/methods/GraphColoring`.

## `GraphLearnFit`

mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `graphlearn:write` |
| Mutates | `true` |
| Durability domain | `GraphRedb` |
| Idempotent | `false` |
| Audited | `true` |
| Emits CDC | `false` |
| Txn participation | `Atomic` |
| Replay class | `OperationIdentity` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED`, `CONFLICT`, `IDEMPOTENCY_CONFLICT`, `REDIRECTED`, `READ_ONLY` |
| Format identities | `STORAGE_KERNEL_SCHEMA_VERSION`, `GRAPH_SNAPSHOT_SCHEMA_VERSION`, `GRAPH_META_SCHEMA_VERSION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `params` | `GraphLearnParams` | no | Training + architecture knobs (all defaulted). |
| `source` | `GraphSource` | yes | The graph-derived subgraph to learn over (node label + relation/direction). |
| `writeback` | boolean | no | Materialize the learned per-feature `:EdgeFunction` nodes (a graph write). |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `LinkPredictorFit` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/GraphLearnFit`, `contract/schemas/result.compute.json#/methods/GraphLearnFit`.

## `GraphLearnPredict`

mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `graphlearn:write` |
| Mutates | `true` |
| Durability domain | `GraphRedb` |
| Idempotent | `false` |
| Audited | `true` |
| Emits CDC | `false` |
| Txn participation | `Atomic` |
| Replay class | `OperationIdentity` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED`, `CONFLICT`, `IDEMPOTENCY_CONFLICT`, `REDIRECTED`, `READ_ONLY` |
| Format identities | `STORAGE_KERNEL_SCHEMA_VERSION`, `GRAPH_SNAPSHOT_SCHEMA_VERSION`, `GRAPH_META_SCHEMA_VERSION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `candidate_pairs` | array of array of any | no | Explicit candidate pairs `(src, dst)` to score. Empty ⇒ score the top-k highest-probability MISSING links across the subgraph. |
| `model` | any | yes | A fitted `KanLinkModel` blob (as returned by `GraphLearnFit`). |
| `source` | `GraphSource` | yes | The subgraph providing the structural features (usually the same source). |
| `top_k` | integer (uint) | no | Cap on returned predictions (0 ⇒ uncapped). |
| `writeback` | boolean | no | Materialize each scored pair as a typed `:PredictedEdge` node (a graph write). |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `LinkPrediction` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/GraphLearnPredict`, `contract/schemas/result.compute.json#/methods/GraphLearnPredict`.

## `MatchOntologyTerms`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:semantic` |
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
| `query` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | array of `OntologyMatch` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/MatchOntologyTerms`, `contract/schemas/result.compute.json#/methods/MatchOntologyTerms`.

## `MineAnomaly`

mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `mining:write` |
| Mutates | `true` |
| Durability domain | `GraphRedb` |
| Idempotent | `false` |
| Audited | `true` |
| Emits CDC | `false` |
| Txn participation | `Atomic` |
| Replay class | `OperationIdentity` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED`, `CONFLICT`, `IDEMPOTENCY_CONFLICT`, `REDIRECTED`, `READ_ONLY` |
| Format identities | `STORAGE_KERNEL_SCHEMA_VERSION`, `GRAPH_SNAPSHOT_SCHEMA_VERSION`, `GRAPH_META_SCHEMA_VERSION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `algorithm` | `AnomalyAlgorithm` (enum: `zscore`, `isoforest`, `lof`, `ocsvm`) | no | Which detector to run. |
| `as_claim` | boolean | no | ADDITIONALLY materialize a `:Claim` (+ `:Evidence`) per flagged anomaly (E6) — see [`Method::MineAssociate::as_claim`]. Confidence is seeded from the row's anomaly score. Requires `writeback`. |
| `features` | array of array of number (double) | no | Explicit feature matrix — each row a point. Empty ⇒ use `values`/`source`. |
| `gamma` | number (double) | no | One-Class SVM RBF gamma; `≤ 0` ⇒ the `1/n_features` default. |
| `k` | integer (uint) | no | LOF neighbor count. |
| `kernel` | `SvmKernel` (enum: `rbf`, `linear`) | no | One-Class SVM kernel: `rbf` (default) · `linear`. |
| `n_trees` | integer (uint) | no | Isolation Forest tree count. |
| `nu` | number (double) | no | One-Class SVM ν ∈ (0,1] (upper bound on the outlier fraction). |
| `plan` | one of: `Plan` \| null | no | Fused retrieve→mine plan (CONCEPT:EG-KG.mining.fused-plan-source) — see `MineCluster::plan`. Takes precedence over `source`; ignored when `features`/`values` is non-empty. |
| `sample_size` | integer (uint) | no | Isolation Forest subsample size. |
| `seed` | integer (uint64) | no | Seed for Isolation Forest (deterministic). |
| `source` | one of: `VectorSource` \| null | no | Graph-derived vector source (node embeddings). Used when `features` and `values` are both empty. |
| `threshold` | number \| null | no | Flag threshold (higher score = more anomalous). Unset ⇒ per-algorithm default. |
| `values` | array of number (double) | no | 1-D series convenience — each scalar becomes a one-element row (e.g. a tsdb window for root-cause analysis). Used when `features` is empty. |
| `writeback` | boolean | no | Materialize each flagged row as a typed `:Anomaly` node linked to its source. |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `AnomalyMiningResult` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/MineAnomaly`, `contract/schemas/result.compute.json#/methods/MineAnomaly`.

## `MineAssociate`

mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `mining:write` |
| Mutates | `true` |
| Durability domain | `GraphRedb` |
| Idempotent | `false` |
| Audited | `true` |
| Emits CDC | `false` |
| Txn participation | `Atomic` |
| Replay class | `OperationIdentity` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED`, `CONFLICT`, `IDEMPOTENCY_CONFLICT`, `REDIRECTED`, `READ_ONLY` |
| Format identities | `STORAGE_KERNEL_SCHEMA_VERSION`, `GRAPH_SNAPSHOT_SCHEMA_VERSION`, `GRAPH_META_SCHEMA_VERSION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `algorithm` | `MineAlgorithm` (enum: `fpgrowth`, `apriori`, `eclat`) | no | Which frequent-itemset engine to run (all agree; FP-Growth default). |
| `as_claim` | boolean | no | ADDITIONALLY materialize a first-class epistemic object per rule (E6, CONCEPT:EG-KG.epistemic.epistemic-substrate): a `:Claim` (confidence seeded from the rule's quality score, normalized to `[0,1]`) plus a provenance `:Evidence` node, both `SUPPORTS`-linked to the claim so the `eg_epistemic` belief layer can propagate confidence over the mined finding. Requires `writeback` (the `:AssociationRule` node is the claim's evidence anchor). Gated `all(mining, epistemic)`; unset ⇒ write-back is byte-identical. |
| `min_confidence` | number (double) | no | Minimum rule confidence (0.0–1.0) to emit. |
| `min_support` | number (double) | no | Minimum fractional support (0.0–1.0) an itemset must meet. |
| `source` | one of: `TransactionSource` \| null | no | Graph-derived transaction source (compute-near-data). Used when `transactions` is empty. |
| `transactions` | array of array of string | no | Explicit transactions — each a set of item labels. Empty ⇒ use `source`. |
| `writeback` | boolean | no | Materialize each rule as a typed `:AssociationRule` node linked to its item nodes (the discovery flywheel). Makes this a graph write. |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `AssociationMiningResult` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/MineAssociate`, `contract/schemas/result.compute.json#/methods/MineAssociate`.

## `MineCausalImpact`

mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `mining:write` |
| Mutates | `true` |
| Durability domain | `GraphRedb` |
| Idempotent | `false` |
| Audited | `true` |
| Emits CDC | `false` |
| Txn participation | `Atomic` |
| Replay class | `OperationIdentity` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED`, `CONFLICT`, `IDEMPOTENCY_CONFLICT`, `REDIRECTED`, `READ_ONLY` |
| Format identities | `STORAGE_KERNEL_SCHEMA_VERSION`, `GRAPH_SNAPSHOT_SCHEMA_VERSION`, `GRAPH_META_SCHEMA_VERSION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `as_claim` | boolean | no | ADDITIONALLY materialize a `:Claim` (+ `:Evidence`) for the estimate (E6) — see [`Method::MineAssociate::as_claim`]. Confidence is seeded from the estimate's own significance (`1 - two_sided_p`). Requires `writeback`. |
| `control` | array of number (double) | no | The control series for difference-in-differences. Empty ⇒ plain interrupted-time-series (no control). |
| `intervention_index` | integer (uint) | no | Index of the FIRST post-intervention observation (in BOTH series for DiD). |
| `series` | array of number (double) | no | The (treatment, for DiD) series to analyze — required. |
| `series_id` | string | no | Optional identity for the write-back `:CausalEffect` node. Empty ⇒ derived from the input series + algorithm. |
| `writeback` | boolean | no | Materialize the estimate as a typed `:CausalEffect` node. |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `CausalImpactMiningResult` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/MineCausalImpact`, `contract/schemas/result.compute.json#/methods/MineCausalImpact`.

## `MineClassifyFit`

the one Mine* family member that is unconditionally read-only (produces a model blob, never writes back)

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `mining:read` |
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
| `algorithm` | `ClassifyAlgorithm` | no | Which classifier to fit. |
| `alpha` | number (double) | no | Multinomial NB Laplace smoothing. |
| `c` | number (double) | no | Linear-SVC inverse-regularization C. |
| `epochs` | integer (uint) | no | Logistic / SVC gradient-descent epochs. |
| `k` | integer (uint) | no | k-NN neighbor count. |
| `l2` | number (double) | no | Logistic L2 regularization strength. |
| `lr` | number (double) | no | Logistic / SVC learning rate. |
| `plan` | one of: `Plan` \| null | no | Fused retrieve→mine plan (CONCEPT:EG-KG.mining.fused-plan-source) — see `MineCluster::plan`. Takes precedence over `source`; ignored when `x` is non-empty. NOTE: `y` labels must still align by position with the plan's resulting row order. |
| `source` | one of: `VectorSource` \| null | no | Graph-derived vector source (node embeddings). Used when `x` is empty. |
| `x` | array of array of number (double) | no | Explicit feature matrix — each row a sample. Empty ⇒ use `source`. |
| `y` | array of integer (int64) | no | Integer class labels, one per row (required). |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `ClassifierFitResult` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/MineClassifyFit`, `contract/schemas/result.compute.json#/methods/MineClassifyFit`.

## `MineClassifyPredict`

mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `mining:write` |
| Mutates | `true` |
| Durability domain | `GraphRedb` |
| Idempotent | `false` |
| Audited | `true` |
| Emits CDC | `false` |
| Txn participation | `Atomic` |
| Replay class | `OperationIdentity` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED`, `CONFLICT`, `IDEMPOTENCY_CONFLICT`, `REDIRECTED`, `READ_ONLY` |
| Format identities | `STORAGE_KERNEL_SCHEMA_VERSION`, `GRAPH_SNAPSHOT_SCHEMA_VERSION`, `GRAPH_META_SCHEMA_VERSION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `as_claim` | boolean | no | ADDITIONALLY materialize a `:Claim` (+ `:Evidence`) per prediction (D3, mirroring E6) — see [`Method::MineAssociate::as_claim`]. Confidence is seeded from the prediction's OWN max class probability (`out.proba[i]`'s argmax), already `[0,1]` by construction (a probability simplex row). Requires `writeback`. |
| `model` | `FittedClassifier` | yes | The fitted model blob from `MineClassifyFit`. |
| `plan` | one of: `Plan` \| null | no | Fused retrieve→mine plan (CONCEPT:EG-KG.mining.fused-plan-source) — see `MineCluster::plan`. Takes precedence over `source`; ignored when `x` is non-empty. |
| `source` | one of: `VectorSource` \| null | no | Graph-derived vector source (node embeddings). Used when `x` is empty. |
| `writeback` | boolean | no | Materialize each prediction as a typed `:Classification` node linked to its source node. |
| `x` | array of array of number (double) | no | Explicit feature matrix — each row a sample. Empty ⇒ use `source`. |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `ClassificationMiningResult` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/MineClassifyPredict`, `contract/schemas/result.compute.json#/methods/MineClassifyPredict`.

## `MineCluster`

mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `mining:write` |
| Mutates | `true` |
| Durability domain | `GraphRedb` |
| Idempotent | `false` |
| Audited | `true` |
| Emits CDC | `false` |
| Txn participation | `Atomic` |
| Replay class | `OperationIdentity` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED`, `CONFLICT`, `IDEMPOTENCY_CONFLICT`, `REDIRECTED`, `READ_ONLY` |
| Format identities | `STORAGE_KERNEL_SCHEMA_VERSION`, `GRAPH_SNAPSHOT_SCHEMA_VERSION`, `GRAPH_META_SCHEMA_VERSION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `algorithm` | `ClusterAlgorithm` (enum: `dbscan`, `hierarchical`, `gmm`, `kmedoids`) | no | Which clustering engine to run. |
| `as_claim` | boolean | no | ADDITIONALLY materialize a `:Claim` (+ `:Evidence`) per cluster (E6) — see [`Method::MineAssociate::as_claim`]. Confidence is seeded from the cluster's compactness score. Requires `writeback`. |
| `eps` | number (double) | no | DBSCAN neighborhood radius. |
| `features` | array of array of number (double) | no | Explicit feature matrix — each row a point. Empty ⇒ use `source`. |
| `k` | integer (uint) | no | Target cluster count for hierarchical / GMM / k-medoids. |
| `linkage` | `Linkage` (enum: `single`, `complete`, `average`) | no | Hierarchical linkage: `single` · `complete` · `average` (default). |
| `max_iter` | integer (uint) | no | EM / PAM iteration cap (GMM, k-medoids). |
| `min_pts` | integer (uint) | no | DBSCAN minimum points (incl. self) for a core point. |
| `plan` | one of: `Plan` \| null | no | Fused retrieve→mine plan (CONCEPT:EG-KG.mining.fused-plan-source): an upstream cross-modal RETRIEVAL plan (`Op::Scan|Filter|Traverse|Rank|…`), executed FIRST over the resident graph/vector/SQL/time modalities; the resulting RowSet ids are then resolved to their stored embeddings (the SAME lookup `VectorSource` uses) to build this op's feature rows — so `retrieve → cluster → writeback` is ONE plan, ONE round-trip (compute-near-data, no client marshalling between retrieve and mine). Takes precedence over `source` when present; ignored when `features` is non-empty. Gated additionally on `query` (the plan algebra lives behind that feature) — a `mining`-only build without `query` drops this field. |
| `seed` | integer (uint64) | no | Seed for GMM's k-means++ init (deterministic). |
| `source` | one of: `VectorSource` \| null | no | Graph-derived vector source (node embeddings). Used when `features` is empty. |
| `writeback` | boolean | no | Materialize each cluster as a typed `:Cluster` node linked to members. |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `ClusterMiningResult` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/MineCluster`, `contract/schemas/result.compute.json#/methods/MineCluster`.

## `MineCommunity`

mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `mining:write` |
| Mutates | `true` |
| Durability domain | `GraphRedb` |
| Idempotent | `false` |
| Audited | `true` |
| Emits CDC | `false` |
| Txn participation | `Atomic` |
| Replay class | `OperationIdentity` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED`, `CONFLICT`, `IDEMPOTENCY_CONFLICT`, `REDIRECTED`, `READ_ONLY` |
| Format identities | `STORAGE_KERNEL_SCHEMA_VERSION`, `GRAPH_SNAPSHOT_SCHEMA_VERSION`, `GRAPH_META_SCHEMA_VERSION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `algorithm` | `CommunityAlgorithm` (enum: `louvain`, `labelprop`) | no | Which existing GDS kernel to run. |
| `as_claim` | boolean | no | ADDITIONALLY materialize a `:Claim` (+ `:Evidence`) per community (E6) — see [`Method::MineAssociate::as_claim`]. Confidence is seeded from the community's own internal-edge density (already `[0,1]`). Requires `writeback`. |
| `label` | string \| null | no | Optional: restrict the projected graph to nodes of this one type. |
| `max_iterations` | integer (uint) | no | Iteration/sweep cap. |
| `resolution` | number (double) | no | Louvain modularity resolution (ignored by label-propagation). |
| `seed` | integer (uint64) | no | Seed for Louvain's deterministic shuffle (ignored by label-propagation). |
| `weighted` | boolean | no | Weight neighbor votes by edge weight (label-propagation only; ignored by Louvain). |
| `writeback` | boolean | no | Materialize each community as a typed `:Community` node linked to its members. |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `CommunityMiningResult` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/MineCommunity`, `contract/schemas/result.compute.json#/methods/MineCommunity`.

## `MineEntityResolve`

mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `mining:write` |
| Mutates | `true` |
| Durability domain | `GraphRedb` |
| Idempotent | `false` |
| Audited | `true` |
| Emits CDC | `false` |
| Txn participation | `Atomic` |
| Replay class | `OperationIdentity` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED`, `CONFLICT`, `IDEMPOTENCY_CONFLICT`, `REDIRECTED`, `READ_ONLY` |
| Format identities | `STORAGE_KERNEL_SCHEMA_VERSION`, `GRAPH_SNAPSHOT_SCHEMA_VERSION`, `GRAPH_META_SCHEMA_VERSION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `as_claim` | boolean | no | ADDITIONALLY materialize a `:Claim` (+ `:Evidence`) per match (E6) — see [`Method::MineAssociate::as_claim`]. Confidence is seeded from the match's OWN similarity. Requires `writeback`. |
| `block_keys` | array of string | no | Blocking key per record, same length as `records`. All-empty-string (or shorter than `records`) ⇒ one global block (no blocking). |
| `bucket_precision` | integer (int32) | no | Grid-bucket rounding precision for the `vectors`/`source` blocking path. |
| `ids` | array of string | no | Optional external ids parallel to `records`/`vectors` (the explicit paths only — `source` supplies its own resident node ids). Shorter than the input ⇒ missing entries fall back to their index. |
| `records` | array of array of string | no | Token-attribute records (Jaccard record linkage). Empty ⇒ use `vectors`/`source`. |
| `source` | one of: `VectorSource` \| null | no | Graph-derived vector source (node embeddings) — used when `records` and `vectors` are both empty. |
| `threshold` | number (double) | no | Minimum similarity (Jaccard or Cosine, `[0,1]`) to emit a match. |
| `vectors` | array of array of number (double) | no | Explicit embedding rows (cosine entity resolution). Used when `records` is empty; empty ⇒ use `source`. |
| `writeback` | boolean | no | Materialize each match as a typed `:EntityMatch` node linked to both members (when they are resident node ids). |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `EntityResolutionMiningResult` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/MineEntityResolve`, `contract/schemas/result.compute.json#/methods/MineEntityResolve`.

## `MineForecast`

mutates is a conservative upper bound; writeback=true enters the canonical durable mutation path

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `mining:write` |
| Mutates | `true` |
| Durability domain | `GraphRedb` |
| Idempotent | `false` |
| Audited | `true` |
| Emits CDC | `false` |
| Txn participation | `Atomic` |
| Replay class | `OperationIdentity` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED`, `CONFLICT`, `IDEMPOTENCY_CONFLICT`, `REDIRECTED`, `READ_ONLY` |
| Format identities | `STORAGE_KERNEL_SCHEMA_VERSION`, `GRAPH_SNAPSHOT_SCHEMA_VERSION`, `GRAPH_META_SCHEMA_VERSION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `algorithm` | `ForecastAlgorithm` (enum: `arima`, `holtwinters`, `stl`) | no | Which forecasting engine to run. |
| `alpha` | number (double) | no | Holt-Winters level smoothing. |
| `as_claim` | boolean | no | ADDITIONALLY materialize a `:Claim` (+ `:Evidence`) for the forecast (E6) — see [`Method::MineAssociate::as_claim`]. Confidence is seeded from the forecast's `confidence` band level. Requires `writeback`. |
| `beta` | number (double) | no | Holt-Winters trend smoothing. |
| `confidence` | number (double) | no | Two-sided confidence level for the forecast band (e.g. `0.95`). |
| `d` | integer (uint) | no | ARIMA differencing order. |
| `gamma` | number (double) | no | Holt-Winters seasonal smoothing. |
| `horizon` | integer (uint) | no | Steps to forecast beyond the series. |
| `p` | integer (uint) | no | ARIMA autoregressive order. |
| `period` | integer (uint) | no | Seasonal period for Holt-Winters / STL (`0` ⇒ non-seasonal Holt linear-trend fallback for Holt-Winters; trend-only for STL). |
| `q` | integer (uint) | no | ARIMA moving-average order. |
| `series_id` | string | no | Optional identity for the write-back `:Forecast` node; when it names a resident node, the forecast is linked `FORECAST_OF` → that node. Empty ⇒ the node id is derived from the input `values` + `algorithm`. |
| `values` | array of number (double) | no | The 1-D series to forecast (required — a tsdb window handed in by the caller). |
| `writeback` | boolean | no | Materialize the forecast as a typed `:Forecast` node. |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `ForecastMiningResult` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/MineForecast`, `contract/schemas/result.compute.json#/methods/MineForecast`.

## `MineOntologyGap`

mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `mining:write` |
| Mutates | `true` |
| Durability domain | `GraphRedb` |
| Idempotent | `false` |
| Audited | `true` |
| Emits CDC | `false` |
| Txn participation | `Atomic` |
| Replay class | `OperationIdentity` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED`, `CONFLICT`, `IDEMPOTENCY_CONFLICT`, `REDIRECTED`, `READ_ONLY` |
| Format identities | `STORAGE_KERNEL_SCHEMA_VERSION`, `GRAPH_SNAPSHOT_SCHEMA_VERSION`, `GRAPH_META_SCHEMA_VERSION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `as_claim` | boolean | no | ADDITIONALLY materialize a `:Claim` (+ `:Evidence`) per gap (E6) — see [`Method::MineAssociate::as_claim`]. Confidence is seeded from the gap kind's fixed documented severity (`eg_compute::mining::ontology_gap::GapKind::severity`). Requires `writeback`. |
| `label` | string \| null | no | Optional: restrict the scan to class nodes of this one type (`None` ⇒ every node whose `type`/`node_type` is `Class` or `OwlClass`). |
| `writeback` | boolean | no | Materialize each gap as a typed `:OntologyGap` node linked to its class. |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `OntologyGapMiningResult` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/MineOntologyGap`, `contract/schemas/result.compute.json#/methods/MineOntologyGap`.

## `MineProcess`

mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `mining:write` |
| Mutates | `true` |
| Durability domain | `GraphRedb` |
| Idempotent | `false` |
| Audited | `true` |
| Emits CDC | `false` |
| Txn participation | `Atomic` |
| Replay class | `OperationIdentity` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED`, `CONFLICT`, `IDEMPOTENCY_CONFLICT`, `REDIRECTED`, `READ_ONLY` |
| Format identities | `STORAGE_KERNEL_SCHEMA_VERSION`, `GRAPH_SNAPSHOT_SCHEMA_VERSION`, `GRAPH_META_SCHEMA_VERSION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `as_claim` | boolean | no | ADDITIONALLY materialize a `:Claim` (+ `:Evidence`) for the model (E6) — see [`Method::MineAssociate::as_claim`]. Confidence is seeded from the fraction of observed activity pairs classified `causal`/`parallel` (vs. `choice`) — a log-coverage proxy, already `[0,1]`. Requires `writeback`. |
| `process_id` | string | no | Optional identity for the write-back `:ProcessModel` node. Empty ⇒ derived from the mined footprint's own shape. |
| `traces` | array of array of string | no | Ordered activity-label traces — each a time-ordered event sequence (an activity may repeat within a trace). Required. |
| `writeback` | boolean | no | Materialize the footprint as a typed `:ProcessModel` node. |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `ProcessMiningResult` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/MineProcess`, `contract/schemas/result.compute.json#/methods/MineProcess`.

## `MineReduce`

mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `mining:write` |
| Mutates | `true` |
| Durability domain | `GraphRedb` |
| Idempotent | `false` |
| Audited | `true` |
| Emits CDC | `false` |
| Txn participation | `Atomic` |
| Replay class | `OperationIdentity` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED`, `CONFLICT`, `IDEMPOTENCY_CONFLICT`, `REDIRECTED`, `READ_ONLY` |
| Format identities | `STORAGE_KERNEL_SCHEMA_VERSION`, `GRAPH_SNAPSHOT_SCHEMA_VERSION`, `GRAPH_META_SCHEMA_VERSION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `algorithm` | `ReduceAlgorithm` | no | Which reduction engine to run. |
| `as_claim` | boolean | no | ADDITIONALLY materialize a `:Claim` (+ `:Evidence`) — D3, mirroring E6 — but ONLY for `svd` (`ReduceAlgorithm::Svd`), the one engine with a principled `[0,1]` quality score: the retained EXPLAINED-VARIANCE RATIO (`Σ retained singular_values² / Σ ALL row sum-of-squares`, i.e. how much of the rows' total variance the kept components capture). `lda`/`umap`/`tsne` have no such score (LDA's discriminant eigenvalues aren't returned; UMAP/t-SNE are approximate neighborhood LAYOUTS with no reconstruction-error analogue) — for those, `as_claim=true` is a documented no-op (no claim is written; see [`Method::MineAssociate::as_claim`] for the general shape). Requires `writeback`. |
| `epochs` | integer (uint) | no | UMAP / t-SNE optimization epochs. |
| `labels` | array of integer (int64) | no | Class labels, one per row — REQUIRED for LDA (ignored otherwise). |
| `lr` | number (double) | no | t-SNE learning rate. |
| `min_dist` | number (double) | no | UMAP minimum embedded distance. |
| `n_components` | integer (uint) | no | Target dimensionality of the embedding. |
| `n_neighbors` | integer (uint) | no | UMAP neighbor count. |
| `perplexity` | number (double) | no | t-SNE perplexity. |
| `plan` | one of: `Plan` \| null | no | Fused retrieve→mine plan (CONCEPT:EG-KG.mining.fused-plan-source) — see `MineCluster::plan`. Takes precedence over `source`; ignored when `x` is non-empty. |
| `seed` | integer (uint64) | no | Seed for UMAP / t-SNE (deterministic layout). |
| `source` | one of: `VectorSource` \| null | no | Graph-derived vector source (node embeddings). Used when `x` is empty. |
| `writeback` | boolean | no | Materialize each row's reduced vector as a typed `:Embedding2D` node. |
| `x` | array of array of number (double) | no | Explicit feature matrix — each row a point. Empty ⇒ use `source`. |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `ReductionMiningResult` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/MineReduce`, `contract/schemas/result.compute.json#/methods/MineReduce`.

## `MineRetrievalQuality`

mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `mining:write` |
| Mutates | `true` |
| Durability domain | `GraphRedb` |
| Idempotent | `false` |
| Audited | `true` |
| Emits CDC | `false` |
| Txn participation | `Atomic` |
| Replay class | `OperationIdentity` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED`, `CONFLICT`, `IDEMPOTENCY_CONFLICT`, `REDIRECTED`, `READ_ONLY` |
| Format identities | `STORAGE_KERNEL_SCHEMA_VERSION`, `GRAPH_SNAPSHOT_SCHEMA_VERSION`, `GRAPH_META_SCHEMA_VERSION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `as_claim` | boolean | no | ADDITIONALLY materialize a `:Claim` (+ `:Evidence`) for the report (E6) — see [`Method::MineAssociate::as_claim`]. Confidence is seeded from the report's own F1 (harmonic mean of precision@k/recall@k, already `[0,1]`). Requires `writeback`. |
| `k` | integer (uint) | no | Precision/recall/MRR cutoff. `0` ⇒ use each trace's full retrieved list. |
| `query_id` | string | no | Optional identity for the write-back `:RetrievalQuality` node. Empty ⇒ derived from the input traces. |
| `traces` | array of `RetrievalTraceSpec` | no | Retrieval traces to evaluate — required. |
| `writeback` | boolean | no | Materialize the aggregate report as a typed `:RetrievalQuality` node. |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `RetrievalQualityMiningResult` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/MineRetrievalQuality`, `contract/schemas/result.compute.json#/methods/MineRetrievalQuality`.

## `MineRiskPropagation`

mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `mining:write` |
| Mutates | `true` |
| Durability domain | `GraphRedb` |
| Idempotent | `false` |
| Audited | `true` |
| Emits CDC | `false` |
| Txn participation | `Atomic` |
| Replay class | `OperationIdentity` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED`, `CONFLICT`, `IDEMPOTENCY_CONFLICT`, `REDIRECTED`, `READ_ONLY` |
| Format identities | `STORAGE_KERNEL_SCHEMA_VERSION`, `GRAPH_SNAPSHOT_SCHEMA_VERSION`, `GRAPH_META_SCHEMA_VERSION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `as_claim` | boolean | no | ADDITIONALLY materialize a `:Claim` (+ `:Evidence`) per scored node (E6) — see [`Method::MineAssociate::as_claim`]. Confidence is seeded from the node's own propagated share (already `[0,1]`, mass-conserving). Requires `writeback`. |
| `damping` | number (double) | no | Damping factor (probability of following an edge vs. restarting to `seed`). |
| `edges` | array of array of any | no | Weighted directed edges `(from_id, to_id, weight)`; `weight` clamped `>= 0`. |
| `max_iterations` | integer (uint) | no | Hard iteration cap. |
| `nodes` | array of string | no | Node ids, index-aligned with `seed` and referenced by `edges`. |
| `seed` | array of number (double) | no | Seed risk per node, index-aligned with `nodes` (any non-negative scale — normalized internally; all-zero ⇒ all-zero result). |
| `tolerance` | number (double) | no | L1 convergence tolerance. |
| `writeback` | boolean | no | Materialize each node's propagated score as a typed `:RiskScore` node. |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `RiskPropagationMiningResult` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/MineRiskPropagation`, `contract/schemas/result.compute.json#/methods/MineRiskPropagation`.

## `MineRootCause`

mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `mining:write` |
| Mutates | `true` |
| Durability domain | `GraphRedb` |
| Idempotent | `false` |
| Audited | `true` |
| Emits CDC | `false` |
| Txn participation | `Atomic` |
| Replay class | `OperationIdentity` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED`, `CONFLICT`, `IDEMPOTENCY_CONFLICT`, `REDIRECTED`, `READ_ONLY` |
| Format identities | `STORAGE_KERNEL_SCHEMA_VERSION`, `GRAPH_SNAPSHOT_SCHEMA_VERSION`, `GRAPH_META_SCHEMA_VERSION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `as_claim` | boolean | no | ADDITIONALLY materialize a `:Claim` (+ `:Evidence`) for the top candidate (E6) — see [`Method::MineAssociate::as_claim`]. Confidence mirrors `anomaly`'s `score / (1 + score)` mapping over the candidate's OWN raw responsibility score (normalizing against the candidate list would be trivially `1.0` for the top candidate). Requires `writeback`. |
| `decay` | number (double) | no | Per-hop score decay `(0,1]` (mirrors PageRank's damping factor). |
| `edges` | array of array of any | no | Dependency edges `(cause_id, effect_id, weight)`; `weight` clamped to `[0,1]`. |
| `max_hops` | integer (uint) | no | Search depth cap. |
| `nodes` | array of string | no | Node ids, index-aligned with `scores` and referenced by `edges`. |
| `scores` | array of number (double) | no | Anomaly score per node, index-aligned with `nodes` (negative clamps to `0.0`). |
| `symptom` | string | no | The already-flagged anomalous node whose root cause to find (required). |
| `writeback` | boolean | no | Materialize the top candidate as a typed `:RootCause` node linked to the symptom. |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `RootCauseMiningResult` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/MineRootCause`, `contract/schemas/result.compute.json#/methods/MineRootCause`.

## `MineSequence`

mutates is a conservative upper bound; writeback=true enters the canonical durable mutation path

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `mining:write` |
| Mutates | `true` |
| Durability domain | `GraphRedb` |
| Idempotent | `false` |
| Audited | `true` |
| Emits CDC | `false` |
| Txn participation | `Atomic` |
| Replay class | `OperationIdentity` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED`, `CONFLICT`, `IDEMPOTENCY_CONFLICT`, `REDIRECTED`, `READ_ONLY` |
| Format identities | `STORAGE_KERNEL_SCHEMA_VERSION`, `GRAPH_SNAPSHOT_SCHEMA_VERSION`, `GRAPH_META_SCHEMA_VERSION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `algorithm` | `MineSeqAlgorithm` (enum: `prefixspan`, `gsp`) | no | Which sequential-pattern engine to run (both agree; PrefixSpan default). |
| `as_claim` | boolean | no | ADDITIONALLY materialize a `:Claim` (+ `:Evidence`) per pattern (E6) — see [`Method::MineAssociate::as_claim`]. Confidence is seeded from the pattern's support. Requires `writeback`. |
| `min_support` | number (double) | no | Minimum fractional support (0.0–1.0) a pattern must meet. |
| `sequences` | array of array of string | no | Explicit ordered sequences — each a time-ordered list of item labels. Empty ⇒ use `source`. |
| `source` | one of: `SequenceSource` \| null | no | Graph-derived sequence source (compute-near-data). Used when `sequences` is empty. |
| `writeback` | boolean | no | Materialize each pattern as a typed `:SequentialPattern` node linked to its resident item nodes. |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `SequenceMiningResult` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/MineSequence`, `contract/schemas/result.compute.json#/methods/MineSequence`.

## `MineSubgraph`

mutates is a conservative upper bound; writeback=true for gspan enters the canonical durable mutation path

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `mining:write` |
| Mutates | `true` |
| Durability domain | `GraphRedb` |
| Idempotent | `false` |
| Audited | `true` |
| Emits CDC | `false` |
| Txn participation | `Atomic` |
| Replay class | `OperationIdentity` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED`, `CONFLICT`, `IDEMPOTENCY_CONFLICT`, `REDIRECTED`, `READ_ONLY` |
| Format identities | `STORAGE_KERNEL_SCHEMA_VERSION`, `GRAPH_SNAPSHOT_SCHEMA_VERSION`, `GRAPH_META_SCHEMA_VERSION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `algorithm` | `SubgraphAlgorithm` (enum: `gspan`, `motif`) | no | Which algorithm to run. |
| `as_claim` | boolean | no | ADDITIONALLY materialize a `:Claim` (+ `:Evidence`) per frequent pattern (E6, `gspan` only) — see [`Method::MineAssociate::as_claim`]. Confidence is seeded from the pattern's support. Requires `writeback`. |
| `label` | string \| null | no | Optional: restrict the host graph to nodes of this one type. `None` ⇒ the whole resident graph (heterogeneous). |
| `max_edges` | integer (uint) | no | Pattern-size growth cap (tractability). Ignored by `motif`. |
| `min_support` | number (double) | no | Minimum fractional support (0.0–1.0, of the host's total edge count) a pattern's embedding count must meet. Ignored by `motif`. |
| `writeback` | boolean | no | Materialize each frequent pattern as a typed `:FrequentSubgraph` node (`gspan` only — a no-op for `motif`, which has no patterns). |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `SubgraphMiningResult` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/MineSubgraph`, `contract/schemas/result.compute.json#/methods/MineSubgraph`.

## `MineText`

mutates is a conservative upper bound; writeback=true for lda/nmf enters the canonical durable mutation path

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `mining:write` |
| Mutates | `true` |
| Durability domain | `GraphRedb` |
| Idempotent | `false` |
| Audited | `true` |
| Emits CDC | `false` |
| Txn participation | `Atomic` |
| Replay class | `OperationIdentity` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED`, `CONFLICT`, `IDEMPOTENCY_CONFLICT`, `REDIRECTED`, `READ_ONLY` |
| Format identities | `STORAGE_KERNEL_SCHEMA_VERSION`, `GRAPH_SNAPSHOT_SCHEMA_VERSION`, `GRAPH_META_SCHEMA_VERSION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `algorithm` | `TextAlgorithm` (enum: `tfidf`, `lda`, `nmf`) | no | Which text-mining engine to run. |
| `alpha` | number (double) | no | LDA symmetric doc-topic Dirichlet prior. |
| `as_claim` | boolean | no | ADDITIONALLY materialize a `:Claim` (+ `:Evidence`) per topic (D3, mirroring E6) — `lda`/`nmf` only, a no-op for `tfidf` (which has no topics, mirroring `writeback`). Quality = the topic's mean doc-membership strength among the documents DOMINANTLY assigned to it (`mean(doc_topics[d][t])` over docs `d` whose argmax topic is `t`) — a topic-coherence proxy: both LDA's Dirichlet posterior and NMF's row-normalized `W` are already `[0,1]` distributions that sum to 1 across topics (see `eg_compute::mining::text` module docs), so this is a principled, already-bounded score requiring no extra normalization. Requires `writeback`. |
| `beta` | number (double) | no | LDA symmetric topic-term Dirichlet prior. |
| `docs` | array of array of string | no | Explicit pre-tokenized documents. Empty ⇒ use `source`. |
| `iterations` | integer (uint) | no | Gibbs sweeps (`lda`) / multiplicative-update iterations (`nmf`). |
| `k` | integer (uint) | no | Topic count for `lda`/`nmf`. |
| `seed` | integer (uint64) | no | Seed for LDA's Gibbs sampler / NMF's initial factors (deterministic). |
| `source` | one of: `TextSource` \| null | no | Graph-derived text source (compute-near-data). Used when `docs` is empty. |
| `top_n` | integer (uint) | no | How many terms to keep per document/topic row. |
| `writeback` | boolean | no | Materialize each topic as a typed `:Topic` node (`lda`/`nmf` only — a no-op for `tfidf`, which has no topics to write back). |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `TextMiningResult` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/MineText`, `contract/schemas/result.compute.json#/methods/MineText`.

## `MinimumSpanningTree`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:graph-algo` |
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
| `result` | array of array of any | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/MinimumSpanningTree`, `contract/schemas/result.compute.json#/methods/MinimumSpanningTree`.

## `MiningPipelineCompare`

read-only: diffs two model versions' held-out metrics

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `mining:read` |
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
| `name` | string | yes | The pipeline whose two versions to compare. |
| `version_a` | integer (uint64) | yes | The two `:Model` versions to diff (held-out metrics). |
| `version_b` | integer (uint64) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `PipelineComparison` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/MiningPipelineCompare`, `contract/schemas/result.compute.json#/methods/MiningPipelineCompare`.

## `MiningPipelineEvaluate`

read-only: scores a stored versioned model against a labeled set

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `mining:read` |
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
| `name` | string | yes | The pipeline whose stored model to score. |
| `source` | one of: `GraphSource` \| null | no | Node source to build the evaluation features from (via the model's stored feature recipe). Empty ⇒ use explicit `x`. |
| `version` | integer (uint64) | no | Model version to evaluate; `0` ⇒ the currently-served version. |
| `x` | array of array of number (double) | no |  |
| `y` | array of integer (int64) | no | Ground-truth integer labels; empty ⇒ read the model's `label_property`. |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `PipelineEvaluation` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/MiningPipelineEvaluate`, `contract/schemas/result.compute.json#/methods/MiningPipelineEvaluate`.

## `MiningPipelinePredict`

mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field (materializes :Prediction nodes)

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `mining:write` |
| Mutates | `true` |
| Durability domain | `GraphRedb` |
| Idempotent | `false` |
| Audited | `true` |
| Emits CDC | `false` |
| Txn participation | `Atomic` |
| Replay class | `OperationIdentity` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED`, `CONFLICT`, `IDEMPOTENCY_CONFLICT`, `REDIRECTED`, `READ_ONLY` |
| Format identities | `STORAGE_KERNEL_SCHEMA_VERSION`, `GRAPH_SNAPSHOT_SCHEMA_VERSION`, `GRAPH_META_SCHEMA_VERSION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `name` | string | yes | The pipeline to predict with. |
| `source` | one of: `GraphSource` \| null | no | Node source to predict over (rebuilds features via the model's recipe). Empty ⇒ use explicit `x`. |
| `version` | integer (uint64) | no | Model version; `0` ⇒ the currently-served version. |
| `writeback` | boolean | no | Materialize each prediction as a typed `:Prediction` node linked to its source node (a graph write). |
| `x` | array of array of number (double) | no |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `PipelinePrediction` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/MiningPipelinePredict`, `contract/schemas/result.compute.json#/methods/MiningPipelinePredict`.

## `MiningPipelineServe`

always writes the :ServedModel pointer to deploy a version

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `mining:write` |
| Mutates | `true` |
| Durability domain | `GraphRedb` |
| Idempotent | `false` |
| Audited | `true` |
| Emits CDC | `false` |
| Txn participation | `Atomic` |
| Replay class | `OperationIdentity` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED`, `CONFLICT`, `IDEMPOTENCY_CONFLICT`, `REDIRECTED`, `READ_ONLY` |
| Format identities | `STORAGE_KERNEL_SCHEMA_VERSION`, `GRAPH_SNAPSHOT_SCHEMA_VERSION`, `GRAPH_META_SCHEMA_VERSION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `name` | string | yes | The pipeline whose version to deploy. |
| `version` | integer (uint64) | yes | The `:Model` version to mark served (predict-by-name then resolves it). |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `PipelineServeResult` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/MiningPipelineServe`, `contract/schemas/result.compute.json#/methods/MiningPipelineServe`.

## `MiningPipelineTrain`

mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field (persists a versioned :Model artifact)

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `mining:write` |
| Mutates | `true` |
| Durability domain | `GraphRedb` |
| Idempotent | `false` |
| Audited | `true` |
| Emits CDC | `false` |
| Txn participation | `Atomic` |
| Replay class | `OperationIdentity` |
| Consumer profiles | `python` |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED`, `CONFLICT`, `IDEMPOTENCY_CONFLICT`, `REDIRECTED`, `READ_ONLY` |
| Format identities | `STORAGE_KERNEL_SCHEMA_VERSION`, `GRAPH_SNAPSHOT_SCHEMA_VERSION`, `GRAPH_META_SCHEMA_VERSION` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `name` | string | yes | Pipeline name — versioned `:Model` artifacts are keyed by it (`v1`, `v2`…). |
| `source` | one of: `GraphSource` \| null | no | The graph-derived node source the feature steps read. Empty ⇒ `x` explicit. |
| `spec` | `PipelineSpec` | yes | The composable pipeline recipe (features → split → model). |
| `writeback` | boolean | no | Persist the fitted model as a versioned `:Model` node (a graph write). `false` ⇒ dry-run: fit + report metrics without materializing an artifact. |
| `x` | array of array of number (double) | no | Explicit feature matrix — each row a sample. Empty ⇒ built from `source` via the spec's feature steps. |
| `y` | array of integer (int64) | no | Explicit integer labels aligned to the rows / source node order. Empty ⇒ read from each node's `spec.label_property` (node classification). |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `PipelineTrainResult` | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/MiningPipelineTrain`, `contract/schemas/result.compute.json#/methods/MiningPipelineTrain`.

## `PageRank`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:graph-algo` |
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
| `damping` | number (double) | yes |  |
| `iterations` | integer (uint) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | array of array of any | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/PageRank`, `contract/schemas/result.compute.json#/methods/PageRank`.

## `PersonalizedPageRank`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:graph-algo` |
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
| `damping` | number (double) | yes |  |
| `iterations` | integer (uint) | yes |  |
| `seed_nodes` | array of array of any | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | array of array of any | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/PersonalizedPageRank`, `contract/schemas/result.compute.json#/methods/PersonalizedPageRank`.

## `ResolveCandidates`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:graph-algo` |
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
| `merge_threshold` | number (double) | yes |  |
| `node_type` | string \| null | no |  |
| `sim_threshold` | number (double) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | array of `MergeProposal` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/ResolveCandidates`, `contract/schemas/result.compute.json#/methods/ResolveCandidates`.

## `RunUdf`

executes a registered sandboxed function; treated as read/compute unless the UDF itself writes back (not modeled -- the wire protocol has no writeback flag here)

| Property | Value |
|---|---|
| Stability | `internal` |
| Authz action | `udf:exec` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `false` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `Snapshot` |
| Replay class | `NotReplayable` |
| Consumer profiles |  |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `id` | string | yes |  |
| `input` | array of integer (uint8) | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | any | Raw | {'reason': 'caller-bytes', 'summary': 'opaque bytes the caller wrote or a caller-supplied program produced'} |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/RunUdf`, `contract/schemas/result.compute.json#/methods/RunUdf`.

## `Solve`

pure compute: a bounded 0-1 integer programme with an independently verifiable certificate. Reads no store, no clock and no float, so it participates in no transaction

| Property | Value |
|---|---|
| Stability | `internal` |
| Authz action | `compute:solve` |
| Mutates | `false` |
| Durability domain | `None` |
| Idempotent | `true` |
| Audited | `false` |
| Emits CDC | `false` |
| Txn participation | `None` |
| Replay class | `NotReplayable` |
| Consumer profiles |  |
| Error set | `INVALID_ARGUMENT`, `ACCESS_DENIED` |

**Request parameters**

| Parameter | Type | Required | Description |
|---|---|:---:|---|
| `request` | `SolveRequest` | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `SolveResult` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/Solve`, `contract/schemas/result.compute.json#/methods/Solve`.

## `StronglyConnectedComponents`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:graph-algo` |
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
| `result` | array of array of string | Json |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/StronglyConnectedComponents`, `contract/schemas/result.compute.json#/methods/StronglyConnectedComponents`.

## `TopologicalSort`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:graph-algo` |
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
| `result` | array of string | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/TopologicalSort`, `contract/schemas/result.compute.json#/methods/TopologicalSort`.

## `Vf2SubgraphMatch`

| Property | Value |
|---|---|
| Stability | `stable` |
| Authz action | `compute:graph-algo` |
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
| `max_results` | integer (uint) | no |  |
| `max_steps` | integer (uint) | no |  |
| `pattern_graph_name` | string | yes |  |

**Result**

| Body | Type | Encoding | Dynamic |
|---|---|---|---|
| `result` | `Vf2MatchResult` | Raw |  |

Full machine-checked schema: `contract/schemas/method.request.json#/methods/Vf2SubgraphMatch`, `contract/schemas/result.compute.json#/methods/Vf2SubgraphMatch`.
