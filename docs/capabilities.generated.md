# Epistemic Graph -- Generated Capability Ledger

> **This file is GENERATED and is the AUTHORITATIVE machine-checked capability 
> truth (CONCEPT:EG-P0-1)** -- regenerate with `cargo run -p eg-capabilities --bin 
> gen_ledger`. It is derived from the exhaustive, no-wildcard `policy()` match in 
> `crates/eg-capabilities/src/lib.rs`, which the compiler forces to stay in sync with 
> every `Method` variant. The hand-maintained `docs/capabilities.md` predates this 
> table and is NOT authoritative; it has not yet been reconciled/retired (out of 
> scope for this workstream).

> `mutates` marked `~true` means the value is a conservative UPPER BOUND: the real 
> runtime answer is conditional (a `writeback` flag, or a parsed query) -- see the 
> `note` column and the consistency test's `RUNTIME_CONDITIONAL` table.

| Method | Mutates | Durability | Authz action | Idempotent | Audited | Emits CDC | Txn participation | Note |
|---|---|---|---|---|---|---|---|---|
| `AddNode` | true | GraphRedb | `node:write` | false | true | true | Atomic |  |
| `RemoveNode` | true | GraphRedb | `node:write` | true | true | true | Atomic |  |
| `HasNode` | false | None | `node:read` | true | false | false | Snapshot |  |
| `GetNodes` | false | None | `node:read` | true | false | false | Snapshot |  |
| `GetNodesByLabel` | false | None | `node:read` | true | false | false | Snapshot |  |
| `GetNodeProperties` | false | None | `node:read` | true | false | false | Snapshot |  |
| `CompareAndSetNodeFields` | true | GraphRedb | `node:write` | true | true | true | Atomic |  |
| `ClaimNext` | true | GraphRedb | `node:write` | false | true | false | Atomic |  |
| `DeclareExchange` | true | Outbox | `broker:admin` | true | true | false | Atomic |  |
| `DeleteExchange` | true | Outbox | `broker:admin` | true | true | false | Atomic |  |
| `BindQueue` | true | Outbox | `broker:admin` | true | true | false | Atomic |  |
| `UnbindQueue` | true | Outbox | `broker:admin` | true | true | false | Atomic |  |
| `Publish` | ~true | Outbox | `broker:publish` | false | true | false | Atomic | PublishIdempotent is the one exception (producer-id/seq dedup makes replays idempotent by construction) |
| `DeclareQueue` | true | Outbox | `broker:admin` | true | true | false | Atomic |  |
| `PublishEx` | ~true | Outbox | `broker:publish` | false | true | false | Atomic | PublishIdempotent is the one exception (producer-id/seq dedup makes replays idempotent by construction) |
| `BrokerConsume` | true | Outbox | `broker:consume` | false | true | false | Atomic |  |
| `BrokerAck` | true | Outbox | `broker:ack` | true | true | false | Atomic |  |
| `BrokerReject` | true | Outbox | `broker:ack` | true | true | false | Atomic |  |
| `SweepExpired` | true | Outbox | `broker:admin` | true | true | false | Atomic |  |
| `StreamDeclare` | true | Outbox | `stream:admin` | true | true | false | Atomic |  |
| `StreamPublish` | true | Outbox | `stream:write` | false | true | false | Atomic |  |
| `StreamRead` | false | None | `stream:read` | true | false | false | Snapshot |  |
| `StreamTrim` | true | Outbox | `stream:admin` | true | true | false | Atomic |  |
| `StreamCommitOffset` | true | Outbox | `stream:admin` | true | true | false | Atomic |  |
| `StreamCommittedOffset` | false | None | `stream:read` | true | false | false | Snapshot |  |
| `PublishConfirmed` | ~true | Outbox | `broker:publish` | false | true | false | Atomic | PublishIdempotent is the one exception (producer-id/seq dedup makes replays idempotent by construction) |
| `PublishIdempotent` | ~true | Outbox | `broker:publish` | true | true | false | Atomic | PublishIdempotent is the one exception (producer-id/seq dedup makes replays idempotent by construction) |
| `BrokerAckTag` | true | Outbox | `broker:ack` | true | true | false | Atomic |  |
| `BrokerNackTag` | true | Outbox | `broker:ack` | true | true | false | Atomic |  |
| `CreateSummaryNode` | true | GraphRedb | `memory:write` | false | true | false | Atomic |  |
| `Consolidate` | true | GraphRedb | `memory:write` | false | true | false | Atomic |  |
| `Reinforce` | true | GraphRedb | `memory:write` | false | true | false | Atomic |  |
| `DecayNode` | true | GraphRedb | `memory:write` | false | true | false | Atomic |  |
| `DecayMemories` | true | GraphRedb | `memory:write` | false | true | false | Atomic |  |
| `EvictBelow` | true | GraphRedb | `memory:write` | false | true | false | Atomic |  |
| `Maintain` | true | GraphRedb | `memory:write` | false | true | false | Atomic |  |
| `SummaryChildren` | false | None | `memory:read` | true | false | false | Snapshot |  |
| `SummariesAtLevel` | false | None | `memory:read` | true | false | false | Snapshot |  |
| `AddSceneObject` | true | GraphRedb | `scene:write` | false | true | false | Atomic |  |
| `SetPose` | true | GraphRedb | `scene:write` | true | true | false | Atomic |  |
| `Reparent` | true | GraphRedb | `scene:write` | true | true | false | Atomic |  |
| `WorldTransform` | false | None | `scene:read` | true | false | false | Snapshot |  |
| `SceneChildren` | false | None | `scene:read` | true | false | false | Snapshot |  |
| `StartTrajectory` | true | GraphRedb | `memory:write` | false | true | false | Atomic |  |
| `AppendStep` | true | GraphRedb | `memory:write` | false | true | false | Atomic |  |
| `DiscountedReturn` | false | None | `memory:read` | true | false | false | Snapshot |  |
| `BestTrajectory` | false | None | `memory:read` | true | false | false | Snapshot |  |
| `GetNodePropertiesBatch` | false | None | `node:read` | true | false | false | Snapshot |  |
| `HasNodesBatch` | false | None | `node:read` | true | false | false | Snapshot |  |
| `NodeCount` | false | None | `node:read` | true | false | false | Snapshot |  |
| `NodeIds` | false | None | `node:read` | true | false | false | Snapshot |  |
| `AddEdge` | true | GraphRedb | `edge:write` | false | true | true | Atomic |  |
| `RemoveEdge` | true | GraphRedb | `edge:write` | true | true | true | Atomic |  |
| `InvalidateEdge` | true | GraphRedb | `edge:write` | true | true | false | Atomic |  |
| `SupersedeEdge` | true | GraphRedb | `edge:write` | true | true | false | Atomic |  |
| `HasEdge` | false | None | `edge:read` | true | false | false | Snapshot |  |
| `GetEdges` | false | None | `edge:read` | true | false | false | Snapshot |  |
| `GetTriples` | false | None | `rdf:read` | true | false | false | Snapshot |  |
| `ClearGraph` | true | GraphRedb | `graph:admin` | true | true | true | Atomic |  |
| `GetEdgeProperties` | false | None | `edge:read` | true | false | false | Snapshot |  |
| `GetEdgePropertiesBatch` | false | None | `edge:read` | true | false | false | Snapshot |  |
| `EdgeCount` | false | None | `edge:read` | true | false | false | Snapshot |  |
| `InDegree` | false | None | `edge:read` | true | false | false | Snapshot |  |
| `OutDegree` | false | None | `edge:read` | true | false | false | Snapshot |  |
| `GetPredecessors` | false | None | `edge:read` | true | false | false | Snapshot |  |
| `GetSuccessors` | false | None | `edge:read` | true | false | false | Snapshot |  |
| `GetNeighbors` | false | None | `edge:read` | true | false | false | Snapshot |  |
| `UnionGetNodeProperties` | false | None | `node:read` | true | false | false | Snapshot |  |
| `UnionGetNodesByLabel` | false | None | `node:read` | true | false | false | Snapshot |  |
| `UnionGetNeighbors` | false | None | `node:read` | true | false | false | Snapshot |  |
| `TopologicalSort` | false | None | `compute:graph-algo` | true | false | false | Snapshot |  |
| `FindCycle` | false | None | `compute:graph-algo` | true | false | false | Snapshot |  |
| `GetShortestPath` | false | None | `compute:graph-algo` | true | false | false | Snapshot |  |
| `GetBlastRadius` | false | None | `compute:graph-algo` | true | false | false | Snapshot |  |
| `DegreeCentrality` | false | None | `compute:graph-algo` | true | false | false | Snapshot |  |
| `DegreeCentralityAll` | false | None | `compute:graph-algo` | true | false | false | Snapshot |  |
| `BetweennessCentrality` | false | None | `compute:graph-algo` | true | false | false | Snapshot |  |
| `PageRank` | false | None | `compute:graph-algo` | true | false | false | Snapshot |  |
| `PersonalizedPageRank` | false | None | `compute:graph-algo` | true | false | false | Snapshot |  |
| `ConnectedComponents` | false | None | `compute:graph-algo` | true | false | false | Snapshot |  |
| `StronglyConnectedComponents` | false | None | `compute:graph-algo` | true | false | false | Snapshot |  |
| `MinimumSpanningTree` | false | None | `compute:graph-algo` | true | false | false | Snapshot |  |
| `CommunityDetection` | false | None | `compute:graph-algo` | true | false | false | Snapshot |  |
| `CommunityDetectEphemeral` | false | None | `compute:graph-algo` | true | false | false | Snapshot |  |
| `GraphColoring` | false | None | `compute:graph-algo` | true | false | false | Snapshot |  |
| `ComputeSimilarityEdges` | false | None | `compute:graph-algo` | true | false | false | Snapshot |  |
| `ResolveCandidates` | false | None | `compute:graph-algo` | true | false | false | Snapshot |  |
| `PruneByLifecycle` | ~true | None | `node:admin` | false | false | false | Atomic | write per access.rs but intentionally excluded from wal.rs (recomputable maintenance op, per wal.rs doc comment) |
| `GetContextView` | false | None | `node:read` | true | false | false | Snapshot |  |
| `BatchUpdate` | true | GraphRedb | `node:write` | false | true | false | Atomic |  |
| `MultiGraphBatchUpdate` | ~true | None | `node:write` | false | false | false | Saga | spans multiple graphs in one call (not a single-graph Atomic unit); NOT present in access.rs's write classifier or wal.rs's durable set at all -- flagged as a possible coverage gap in both |
| `Metrics` | false | None | `service:control` | true | false | false | None |  |
| `EvictLRU` | ~true | None | `node:admin` | false | false | false | Atomic | write per access.rs but intentionally excluded from wal.rs (recomputable maintenance op, per wal.rs doc comment) |
| `DecaySweep` | ~true | None | `node:admin` | false | false | false | Atomic | write per access.rs but intentionally excluded from wal.rs (recomputable maintenance op, per wal.rs doc comment) |
| `TouchNodes` | ~true | None | `node:admin` | false | false | false | Atomic | write per access.rs but intentionally excluded from wal.rs (recomputable maintenance op, per wal.rs doc comment) |
| `ToMsgpack` | false | None | `graph:read` | true | false | false | Snapshot |  |
| `FromMsgpack` | ~true | None | `graph:admin` | false | false | false | Atomic | write per access.rs but absent from wal.rs durable set (EG-P0-3); typically used for bulk restore, not incremental WAL logging |
| `GetLedger` | false | None | `ledger:read` | true | false | false | Snapshot |  |
| `ClearLedger` | ~true | None | `ledger:admin` | true | false | false | Atomic | write per access.rs but absent from wal.rs durable set (EG-P0-3) |
| `ApplyLedger` | ~true | None | `ledger:write` | false | false | false | Atomic | write per access.rs but absent from wal.rs durable set (EG-P0-3) |
| `AuditVerify` | false | None | `security:audit` | true | false | false | Snapshot |  |
| `GetSubgraph` | false | None | `node:read` | true | false | false | Snapshot |  |
| `Fork` | false | None | `graph:read` | true | false | false | Snapshot | returns the forked snapshot to the caller; never registers/persists it server-side |
| `DiffAgainst` | false | None | `graph:read` | true | false | false | Snapshot |  |
| `CompactNodesByType` | ~true | None | `node:admin` | false | false | false | Atomic | write per access.rs but absent from wal.rs durable set (EG-P0-3); recomputable |
| `RunDatalogReasoning` | ~true | None | `reasoning:write` | false | false | false | Atomic | write per access.rs but absent from wal.rs durable set (EG-P0-3); inferred facts are recomputable |
| `CreateGraph` | ~true | None | `graph:admin` | true | false | false | Atomic | NOT present in access.rs's write classifier (it targets the registry, not an existing graph's content) -- the registry-level ACL for this is out of this workstream's scope |
| `DeleteGraph` | true | None | `graph:admin` | true | false | false | Atomic |  |
| `ListGraphs` | false | None | `graph:read` | true | false | false | Snapshot |  |
| `Reshard` | ~true | None | `admin:cluster` | false | false | false | Saga | NOT present in access.rs's write classifier at all (server-admin action, not a per-graph content write) -- governed by a separate admin gate, out of this workstream's scope |
| `CatalogAssign` | ~true | None | `admin:cluster` | true | false | false | Saga | NOT present in access.rs's write classifier at all -- governed elsewhere |
| `CatalogReassign` | ~true | None | `admin:cluster` | true | false | false | Saga | NOT present in access.rs's write classifier at all -- governed elsewhere |
| `CatalogRemove` | ~true | None | `admin:cluster` | true | false | false | Saga | NOT present in access.rs's write classifier at all -- governed elsewhere |
| `CatalogList` | false | None | `admin:cluster-read` | true | false | false | Snapshot |  |
| `RebalancePlan` | false | None | `admin:cluster-read` | true | false | false | Snapshot |  |
| `RebalanceExecute` | ~true | None | `admin:cluster` | false | false | false | Saga | NOT present in access.rs's write classifier at all -- governed elsewhere |
| `Backup` | false | None | `admin:backup` | true | false | false | Snapshot | reads a consistent snapshot out to a bundle; does not mutate the live graph |
| `Restore` | ~true | None | `admin:backup` | false | false | false | Saga | NOT present in access.rs's write classifier at all (server-admin action) -- governed elsewhere |
| `CreateChannel` | ~true | None | `channel:admin` | true | false | false | Atomic | NOT present in access.rs's write classifier at all -- flagged as a possible access.rs coverage gap |
| `JoinChannel` | ~true | None | `channel:admin` | true | false | false | Atomic | NOT present in access.rs's write classifier at all -- flagged as a possible access.rs coverage gap |
| `LeaveChannel` | ~true | None | `channel:admin` | true | false | false | Atomic | NOT present in access.rs's write classifier at all -- flagged as a possible access.rs coverage gap |
| `CloseChannel` | ~true | None | `channel:admin` | true | false | false | Atomic | NOT present in access.rs's write classifier at all -- flagged as a possible access.rs coverage gap |
| `SendMessage` | ~true | None | `channel:write` | false | false | false | Atomic | NOT present in access.rs's write classifier at all -- flagged as a possible access.rs coverage gap |
| `GetChannelMessages` | false | None | `channel:read` | true | false | false | Snapshot |  |
| `ListChannels` | false | None | `channel:read` | true | false | false | Snapshot |  |
| `GetChannelMembers` | false | None | `channel:read` | true | false | false | Snapshot |  |
| `Ping` | false | None | `service:control` | true | false | false | None |  |
| `Health` | false | None | `service:control` | true | false | false | None |  |
| `Shutdown` | ~true | None | `service:admin` | true | false | false | None | a server lifecycle action, not a graph-content mutation -- governed by a separate admin gate |
| `Checkpoint` | ~true | None | `service:admin` | true | false | false | None | flushes the ALREADY-durable WAL tail into a snapshot; does not itself add new data |
| `ResourceStats` | false | None | `service:control` | true | false | false | None |  |
| `Reconcile` | ~true | None | `graph:write` | false | false | false | Saga | CRDT merge of remote state; write per access.rs but absent from wal.rs durable set (EG-P0-3) |
| `ApplyMutation` | ~true | None | `graph:write` | false | false | false | Atomic | write per access.rs but absent from wal.rs durable set (EG-P0-3) |
| `ParseRepository` | ~true | None | `node:admin` | false | false | false | Atomic | write per access.rs but intentionally excluded from wal.rs (recomputable from source tree, per wal.rs doc comment) |
| `Vf2SubgraphMatch` | false | None | `compute:graph-algo` | true | false | false | Snapshot |  |
| `ParseFile` | false | None | `compute:parse` | true | false | false | None |  |
| `ParseFiles` | false | None | `compute:parse` | true | false | false | None |  |
| `IndexRepository` | false | None | `compute:parse` | true | false | false | None |  |
| `ObserveScreen` | false | None | `compute:vision` | false | false | false | None |  |
| `AddEmbedding` | true | GraphRedb | `node:write` | false | true | false | Atomic |  |
| `SemanticSearch` | false | None | `compute:semantic` | true | false | false | Snapshot |  |
| `Discover` | false | None | `compute:semantic` | true | false | false | Snapshot |  |
| `MatchOntologyTerms` | false | None | `compute:semantic` | true | false | false | Snapshot |  |
| `SpectralCluster` | false | None | `compute:semantic` | true | false | false | Snapshot |  |
| `HypergraphEncodeInteraction` | false | None | `compute:semantic` | true | false | false | Snapshot |  |
| `BatchCosineSimilarity` | false | None | `compute:semantic` | true | false | false | Snapshot |  |
| `BatchL2Normalize` | false | None | `compute:semantic` | true | false | false | Snapshot |  |
| `FindSimilarPairs` | false | None | `compute:semantic` | true | false | false | Snapshot |  |
| `FinanceOptimizePortfolio` | false | None | `compute:finance` | true | false | false | None |  |
| `FinanceRiskParity` | false | None | `compute:finance` | true | false | false | None |  |
| `FinanceBlackLitterman` | false | None | `compute:finance` | true | false | false | None |  |
| `FinanceEfficientFrontier` | false | None | `compute:finance` | true | false | false | None |  |
| `DsLinearRegression` | false | None | `compute:datascience` | true | false | false | None |  |
| `DsKMeans` | false | None | `compute:datascience` | true | false | false | None |  |
| `DsPca` | false | None | `compute:datascience` | true | false | false | None |  |
| `DsComputeStats` | false | None | `compute:datascience` | true | false | false | None |  |
| `DsTrainTestSplit` | false | None | `compute:datascience` | true | false | false | None |  |
| `DsFitEstimator` | false | None | `compute:datascience` | true | false | false | None |  |
| `DsPredictEstimator` | false | None | `compute:datascience` | true | false | false | None |  |
| `DsSoftmax` | false | None | `compute:datascience` | true | false | false | None |  |
| `DsLogSoftmax` | false | None | `compute:datascience` | true | false | false | None |  |
| `DsCrossEntropy` | false | None | `compute:datascience` | true | false | false | None |  |
| `DsDpoLoss` | false | None | `compute:datascience` | true | false | false | None |  |
| `DsGrpoSurrogate` | false | None | `compute:datascience` | true | false | false | None |  |
| `DsKlDivergence` | false | None | `compute:datascience` | true | false | false | None |  |
| `DsAdamStep` | false | None | `compute:datascience` | true | false | false | None |  |
| `DsSgdStep` | false | None | `compute:datascience` | true | false | false | None |  |
| `FinanceVar` | false | None | `compute:finance` | true | false | false | None |  |
| `FinanceCvar` | false | None | `compute:finance` | true | false | false | None |  |
| `FinanceMaxDrawdown` | false | None | `compute:finance` | true | false | false | None |  |
| `FinanceDrawdownSeries` | false | None | `compute:finance` | true | false | false | None |  |
| `FinanceDownsideDeviation` | false | None | `compute:finance` | true | false | false | None |  |
| `FinanceRiskMetrics` | false | None | `compute:finance` | true | false | false | None |  |
| `FinanceMonteCarloVar` | false | None | `compute:finance` | true | false | false | None |  |
| `FinanceStressTest` | false | None | `compute:finance` | true | false | false | None |  |
| `FinanceDetectRegimes` | false | None | `compute:finance` | true | false | false | None |  |
| `FinanceRollingZscore` | false | None | `compute:finance` | true | false | false | None |  |
| `FinanceEwma` | false | None | `compute:finance` | true | false | false | None |  |
| `FinanceSignalDecay` | false | None | `compute:finance` | true | false | false | None |  |
| `FinanceCombineAlphas` | false | None | `compute:finance` | true | false | false | None |  |
| `FinanceCrossSectionalRank` | false | None | `compute:finance` | true | false | false | None |  |
| `FinanceMomentum` | false | None | `compute:finance` | true | false | false | None |  |
| `FinanceMeanReversion` | false | None | `compute:finance` | true | false | false | None |  |
| `FinanceInformationCoefficient` | false | None | `compute:finance` | true | false | false | None |  |
| `FinanceTwap` | false | None | `compute:finance` | true | false | false | None |  |
| `FinanceVwap` | false | None | `compute:finance` | true | false | false | None |  |
| `FinanceMarketImpact` | false | None | `compute:finance` | true | false | false | None |  |
| `FinancePairsTrading` | false | None | `compute:finance` | true | false | false | None |  |
| `FinanceMatchOrders` | false | None | `compute:finance` | true | false | false | None |  |
| `FinanceAvellanedaStoikov` | false | None | `compute:finance` | true | false | false | None |  |
| `FinanceGltQuotes` | false | None | `compute:finance` | true | false | false | None |  |
| `FinanceLogitQuotes` | false | None | `compute:finance` | true | false | false | None |  |
| `FinanceGlostenMilgromSpread` | false | None | `compute:finance` | true | false | false | None |  |
| `FinanceExpectedPnlRate` | false | None | `compute:finance` | true | false | false | None |  |
| `FinanceBreakevenAlpha` | false | None | `compute:finance` | true | false | false | None |  |
| `FinanceOfiSeries` | false | None | `compute:finance` | true | false | false | None |  |
| `FinanceMicropriceSeries` | false | None | `compute:finance` | true | false | false | None |  |
| `FinanceVpinPm` | false | None | `compute:finance` | true | false | false | None |  |
| `FinanceHawkesMle` | false | None | `compute:finance` | true | false | false | None |  |
| `FinanceHardimanBouchaud` | false | None | `compute:finance` | true | false | false | None |  |
| `FinanceKyleLambda` | false | None | `compute:finance` | true | false | false | None |  |
| `FinanceSurveillanceRisk` | false | None | `compute:finance` | true | false | false | None |  |
| `FinanceKellyFraction` | false | None | `compute:finance` | true | false | false | None |  |
| `FinanceBayesianKelly` | false | None | `compute:finance` | true | false | false | None |  |
| `FinancePosteriorCredibleInterval` | false | None | `compute:finance` | true | false | false | None |  |
| `FinancePurgedCpcv` | false | None | `compute:finance` | true | false | false | None |  |
| `FinanceDeflatedSharpe` | false | None | `compute:finance` | true | false | false | None |  |
| `FinanceProbabilityBacktestOverfit` | false | None | `compute:finance` | true | false | false | None |  |
| `FinanceDieboldMariano` | false | None | `compute:finance` | true | false | false | None |  |
| `FinanceForensicReport` | false | None | `compute:finance` | true | false | false | None |  |
| `FinanceKalmanFilter1d` | false | None | `compute:finance` | true | false | false | None |  |
| `FinanceKalmanBeta` | false | None | `compute:finance` | true | false | false | None |  |
| `FinanceKalmanVolatility` | false | None | `compute:finance` | true | false | false | None |  |
| `FinanceAdfTest` | false | None | `compute:finance` | true | false | false | None |  |
| `FinanceOuCalibrate` | false | None | `compute:finance` | true | false | false | None |  |
| `FinanceOuOptimalThresholds` | false | None | `compute:finance` | true | false | false | None |  |
| `FinanceMarkovTransitionMatrix` | false | None | `compute:finance` | true | false | false | None |  |
| `FinanceOrderBookImbalance` | false | None | `compute:finance` | true | false | false | None |  |
| `FinanceQueueImbalance` | false | None | `compute:finance` | true | false | false | None |  |
| `FinanceRealizedVolTick` | false | None | `compute:finance` | true | false | false | None |  |
| `FinanceSpreadReversion` | false | None | `compute:finance` | true | false | false | None |  |
| `FinanceInformationRatio` | false | None | `compute:finance` | true | false | false | None |  |
| `FinanceEffectiveIndependentN` | false | None | `compute:finance` | true | false | false | None |  |
| `FinanceAlphaCombinationEngine` | false | None | `compute:finance` | true | false | false | None |  |
| `FinanceBrierScore` | false | None | `compute:finance` | true | false | false | None |  |
| `FinanceConvergenceGate` | false | None | `compute:finance` | true | false | false | None |  |
| `FinanceEmpiricalKelly` | false | None | `compute:finance` | true | false | false | None |  |
| `FinanceSabrImpliedVol` | false | None | `compute:finance` | true | false | false | None |  |
| `FinanceSabrSmile` | false | None | `compute:finance` | true | false | false | None |  |
| `FinanceSabrCalibrate` | false | None | `compute:finance` | true | false | false | None |  |
| `RegisterIdentity` | ~true | None | `security:admin` | true | false | false | Atomic | NOT present in access.rs's write classifier at all -- flagged as a possible access.rs coverage gap |
| `RbacAdmin` | ~true | None | `security:admin` | true | false | false | Atomic | NOT present in access.rs's write classifier at all -- flagged as a possible access.rs coverage gap |
| `ApplyMultisigMutation` | ~true | None | `security:admin` | false | false | false | Saga | write per access.rs but absent from wal.rs durable set (EG-P0-3); multisig threshold-gated, multi-party |
| `Sql` | ~true | None | `query:sql` | false | false | false | Atomic | mutates is a conservative upper bound: the REAL access::requires_write(m) depends on the parsed statement kind (eg_query::classify); write-but-absent-from-wal.rs's durable set too (EG-P0-3) -- SQL DML durability rides the user-table store's own persistence, not the graph WAL |
| `CypherQuery` | ~true | None | `query:cypher` | false | false | false | Atomic | mutates is a conservative upper bound: the REAL access::requires_write(m) depends on a keyword scan of the query text (access::cypher_is_write); also write-but-absent-from-wal.rs's durable set (EG-P0-3) |
| `GraphQl` | ~true | None | `query:graphql` | false | false | false | Atomic | mutates is a conservative upper bound: the REAL access::requires_write(m) depends on the parsed operation kind (query vs mutation); also write-but-absent-from-wal.rs's durable set (EG-P0-3) |
| `UnifiedQuery` | false | None | `query:unified` | true | false | false | Snapshot |  |
| `UnifiedQueryText` | false | None | `query:unified` | true | false | false | Snapshot |  |
| `ExplainPlan` | false | None | `explain:read` | true | false | false | Snapshot |  |
| `ExplainProvenance` | false | None | `explain:read` | true | false | false | Snapshot |  |
| `ExplainPolicy` | false | None | `explain:read` | true | false | false | Snapshot |  |
| `ExplainBelief` | false | None | `explain:read` | true | false | false | Snapshot |  |
| `NlQuery` | false | None | `query:nl` | false | false | false | Snapshot |  |
| `RegisterForeignSource` | ~true | None | `federation:admin` | true | false | false | Atomic | NOT present in access.rs's write classifier at all; policy marks it mutates=true on semantic grounds (registers a foreign-source config) -- flagged as a possible access.rs coverage gap |
| `RegisterUdf` | ~true | None | `udf:admin` | true | false | false | Atomic | NOT present in access.rs's write classifier at all; policy marks it mutates=true on semantic grounds -- flagged as a possible access.rs coverage gap |
| `RunUdf` | false | None | `udf:exec` | false | false | false | Snapshot | executes a registered sandboxed function; treated as read/compute unless the UDF itself writes back (not modeled -- the wire protocol has no writeback flag here) |
| `DistributedCompute` | ~true | None | `distcompute:write` | false | false | false | Saga | NOT present in access.rs's write classifier at all; policy marks it mutates=true on semantic grounds (cross-shard Pregel writeback) -- flagged as a possible access.rs coverage gap |
| `CreateMatView` | ~true | None | `matview:admin` | false | false | false | Saga | NOT present in access.rs's write classifier at all -- flagged as a possible access.rs coverage gap |
| `GetMatView` | false | None | `matview:read` | true | false | false | Snapshot |  |
| `RefreshMatView` | ~true | None | `matview:admin` | false | false | false | Saga | NOT present in access.rs's write classifier at all -- flagged as a possible access.rs coverage gap |
| `PlanMatViewDefine` | ~true | None | `matview:admin` | true | false | false | Saga | NOT present in access.rs's write classifier at all -- flagged as a possible access.rs coverage gap |
| `PlanMatViewGet` | false | None | `matview:read` | true | false | false | Snapshot |  |
| `PlanMatViewRefresh` | ~true | None | `matview:admin` | false | false | false | Saga | NOT present in access.rs's write classifier at all -- flagged as a possible access.rs coverage gap |
| `PlanMatViewDrop` | ~true | None | `matview:admin` | true | false | false | Saga | NOT present in access.rs's write classifier at all -- flagged as a possible access.rs coverage gap |
| `BeginTxn` | false | None | `txn:control` | false | false | false | Saga |  |
| `TxnAddNode` | ~true | None | `txn:write` | false | false | false | Saga | self-routes before dispatch_graph_op (see dispatch.rs); access::requires_write is never consulted for these -- write-access is enforced once at BeginTxn, not per-op here (out of this workstream's scope to verify) |
| `TxnRemoveNode` | ~true | None | `txn:write` | true | false | false | Saga | self-routes before dispatch_graph_op; see TxnAddNode note |
| `TxnAddEdge` | ~true | None | `txn:write` | false | false | false | Saga | self-routes before dispatch_graph_op (see dispatch.rs); access::requires_write is never consulted for these -- write-access is enforced once at BeginTxn, not per-op here (out of this workstream's scope to verify) |
| `TxnRemoveEdge` | ~true | None | `txn:write` | true | false | false | Saga | self-routes before dispatch_graph_op; see TxnAddNode note |
| `TxnCas` | ~true | None | `txn:write` | true | false | false | Saga | self-routes before dispatch_graph_op; see TxnAddNode note |
| `TxnAddEmbedding` | ~true | None | `txn:write` | false | false | false | Saga | self-routes before dispatch_graph_op (see dispatch.rs); access::requires_write is never consulted for these -- write-access is enforced once at BeginTxn, not per-op here (out of this workstream's scope to verify) |
| `TxnBlobRef` | ~true | None | `txn:write` | false | false | false | Saga | self-routes before dispatch_graph_op (see dispatch.rs); access::requires_write is never consulted for these -- write-access is enforced once at BeginTxn, not per-op here (out of this workstream's scope to verify) |
| `TxnAddMeasurement` | ~true | None | `txn:write` | false | false | false | Saga | self-routes before dispatch_graph_op (see dispatch.rs); access::requires_write is never consulted for these -- write-access is enforced once at BeginTxn, not per-op here (out of this workstream's scope to verify) |
| `TxnAxiom` | ~true | None | `txn:write` | false | false | false | Saga | self-routes before dispatch_graph_op (see dispatch.rs); access::requires_write is never consulted for these -- write-access is enforced once at BeginTxn, not per-op here (out of this workstream's scope to verify) |
| `TxnConstruct` | ~true | None | `txn:write` | false | false | false | Saga | self-routes before dispatch_graph_op (see dispatch.rs); access::requires_write is never consulted for these -- write-access is enforced once at BeginTxn, not per-op here (out of this workstream's scope to verify) |
| `TxnPlanWriteback` | ~true | None | `txn:write` | false | false | false | Saga | self-routes before dispatch_graph_op (see dispatch.rs); access::requires_write is never consulted for these -- write-access is enforced once at BeginTxn, not per-op here (out of this workstream's scope to verify) |
| `TxnMaterializeBelief` | ~true | None | `txn:write` | false | false | false | Saga | self-routes before dispatch_graph_op (see dispatch.rs); access::requires_write is never consulted for these -- write-access is enforced once at BeginTxn, not per-op here (out of this workstream's scope to verify) |
| `TxnUnifiedQuery` | false | None | `txn:read` | true | false | false | Saga |  |
| `TxnUnifiedQueryText` | false | None | `txn:read` | true | false | false | Saga |  |
| `Commit` | ~true | None | `txn:control` | false | false | false | Saga | the durable-apply moment of the multi-op OCC transaction; self-routes before dispatch_graph_op |
| `Rollback` | false | None | `txn:control` | false | false | false | Saga |  |
| `TsAppend` | ~true | SeriesRedb | `timeseries:write` | false | false | false | Atomic | self-routes before dispatch_graph_op, targeting its own series.redb (separate from graph.redb/blob.redb/kv.redb) |
| `TsRange` | false | None | `timeseries:read` | true | false | false | Snapshot |  |
| `TsAsofJoin` | false | None | `timeseries:read` | true | false | false | Snapshot |  |
| `TsWindow` | false | None | `timeseries:read` | true | false | false | Snapshot |  |
| `TsGapFill` | false | None | `timeseries:read` | true | false | false | Snapshot |  |
| `BlobBegin` | ~true | BlobRedb | `blob:write` | false | false | false | Saga | multi-call chunked-upload protocol (Begin ... ChunkPut* ... Commit); no single-call atomicity; durable via its own blob.redb (group-committed Immediate), self-routes before dispatch_graph_op |
| `BlobChunkPut` | ~true | BlobRedb | `blob:write` | false | false | false | Saga | durable via its own blob.redb (group-committed Immediate); self-routes before dispatch_graph_op |
| `BlobCommit` | ~true | BlobRedb | `blob:write` | false | false | false | Saga | multi-call chunked-upload protocol (Begin ... ChunkPut* ... Commit); no single-call atomicity; durable via its own blob.redb (group-committed Immediate), self-routes before dispatch_graph_op |
| `BlobFetchBegin` | false | None | `blob:read` | true | false | false | Snapshot |  |
| `BlobChunkGet` | false | None | `blob:read` | true | false | false | Snapshot |  |
| `BlobFetchEnd` | false | None | `blob:read` | true | false | false | Snapshot |  |
| `BlobRef` | ~true | BlobRedb | `blob:write` | false | false | false | Atomic | refcount increment; idempotent-ish but re-invocation adds another ref, so not idempotent; durable via blob.redb |
| `BlobUnref` | ~true | BlobRedb | `blob:write` | false | false | false | Atomic | durable via blob.redb |
| `BlobGc` | ~true | BlobRedb | `blob:admin` | true | false | false | Atomic | durable via blob.redb |
| `KvGet` | false | None | `kv:read` | true | false | false | Snapshot |  |
| `KvPut` | ~true | KvRedb | `kv:write` | false | false | false | Atomic | durable via its own kv.redb (redb::Durability::Immediate, commit-before-ack) -- self-routes before dispatch_graph_op and before wal.rs's per-graph WAL entirely; NOT a wal.rs gap, just a parallel durability domain |
| `KvDelete` | ~true | KvRedb | `kv:write` | true | false | false | Atomic | durable via its own kv.redb (redb::Durability::Immediate); self-routes before dispatch_graph_op |
| `KvScan` | false | None | `kv:read` | true | false | false | Snapshot |  |
| `KvCas` | ~true | KvRedb | `kv:write` | false | false | false | Atomic | durable via its own kv.redb (redb::Durability::Immediate, commit-before-ack) -- self-routes before dispatch_graph_op and before wal.rs's per-graph WAL entirely; NOT a wal.rs gap, just a parallel durability domain |
| `ImportSqliteFile` | ~true | None | `sqlite:import` | false | false | false | Atomic | NOT present in access.rs's write classifier at all -- flagged as a possible access.rs coverage gap |
| `ExportSqliteFile` | false | None | `sqlite:export` | true | false | false | Snapshot |  |
| `AddTriples` | true | GraphRedb | `rdf:write` | false | true | false | Atomic |  |
| `GetRdf` | false | None | `rdf:read` | true | false | false | Snapshot |  |
| `RemoveTriples` | true | GraphRedb | `rdf:write` | true | true | false | Atomic |  |
| `DropNamedGraph` | true | GraphRedb | `rdf:write` | true | true | false | Atomic |  |
| `Sparql` | false | None | `sparql:read` | true | false | false | Snapshot |  |
| `SparqlVirtual` | false | None | `sparql:read` | true | false | false | Snapshot |  |
| `OwlReason` | false | None | `owl:read` | true | false | false | Snapshot |  |
| `OwlReasonDistributed` | false | None | `owl:read` | true | false | false | Snapshot |  |
| `OwlExplain` | false | None | `owl:read` | true | false | false | Snapshot |  |
| `RunRules` | ~true | None | `reasoning:write` | false | false | false | Atomic | NOT present in access.rs's write classifier at all despite being the sibling of RunDatalogReasoning (which IS); policy marks it mutates=true on semantic grounds -- flagged as a possible access.rs coverage gap, not yet confirmed against the handler |
| `ShaclValidate` | false | None | `validation:read` | true | false | false | Snapshot |  |
| `IcvConfigure` | ~true | None | `security:admin` | true | false | false | Atomic | write per access.rs but absent from wal.rs durable set (EG-P0-3); a build without `security` drops audit entirely |
| `ShexValidate` | false | None | `validation:read` | true | false | false | Snapshot |  |
| `CdcRead` | false | None | `cdc:read` | true | false | false | Snapshot |  |
| `RegisterContinuousQuery` | ~true | None | `cdc:admin` | true | false | false | Atomic | NOT present in access.rs's write classifier at all -- flagged as a possible access.rs coverage gap |
| `ReadContinuousQuery` | false | None | `cdc:read` | true | false | false | Snapshot |  |
| `DropContinuousQuery` | ~true | None | `cdc:admin` | true | false | false | Atomic | NOT present in access.rs's write classifier at all -- flagged as a possible access.rs coverage gap |
| `Watch` | false | None | `cdc:read` | false | false | false | None | opens a push subscription; not a snapshot read nor a mutation |
| `RegisterTrigger` | ~true | None | `cdc:admin` | true | false | false | Atomic | NOT present in access.rs's write classifier at all -- flagged as a possible access.rs coverage gap |
| `DropTrigger` | ~true | None | `cdc:admin` | true | false | false | Atomic | NOT present in access.rs's write classifier at all -- flagged as a possible access.rs coverage gap |
| `ListTriggers` | false | None | `cdc:read` | true | false | false | Snapshot |  |
| `FiredTriggers` | false | None | `cdc:read` | true | false | false | Snapshot |  |
| `CepSubscribe` | ~true | None | `cep:admin` | true | false | false | Atomic | NOT present in access.rs's write classifier at all -- flagged as a possible access.rs coverage gap |
| `CepPoll` | false | None | `cep:read` | true | false | false | Snapshot |  |
| `CepUnsubscribe` | ~true | None | `cep:admin` | true | false | false | Atomic | NOT present in access.rs's write classifier at all -- flagged as a possible access.rs coverage gap |
| `MineAssociate` | ~true | GraphRedb | `mining:write` | false | true | false | Atomic | mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field |
| `MineCluster` | ~true | GraphRedb | `mining:write` | false | true | false | Atomic | mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field |
| `MineAnomaly` | ~true | GraphRedb | `mining:write` | false | true | false | Atomic | mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field |
| `MineClassifyFit` | false | None | `mining:read` | true | false | false | Snapshot | the one Mine* family member that is unconditionally read-only (produces a model blob, never writes back) |
| `MineClassifyPredict` | ~true | GraphRedb | `mining:write` | false | true | false | Atomic | mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field |
| `MineReduce` | ~true | GraphRedb | `mining:write` | false | true | false | Atomic | mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field |
| `GraphLearnFit` | ~true | GraphRedb | `graphlearn:write` | false | true | false | Atomic | mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field |
| `GraphLearnPredict` | ~true | GraphRedb | `graphlearn:write` | false | true | false | Atomic | mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field |
| `MineSequence` | ~true | GraphRedb | `mining:write` | false | true | false | Atomic | mutates is a conservative upper bound (writeback-conditional per access.rs); wal.rs::is_durable_mutation now covers the writeback=true case (EG-P0-3 fixed) |
| `MineForecast` | ~true | GraphRedb | `mining:write` | false | true | false | Atomic | mutates is a conservative upper bound (writeback-conditional per access.rs); wal.rs::is_durable_mutation now covers the writeback=true case (EG-P0-3 fixed) |
| `MineText` | ~true | GraphRedb | `mining:write` | false | true | false | Atomic | mutates is a conservative upper bound (writeback AND non-tfidf/non-motif algorithm-conditional per access.rs); wal.rs::is_durable_mutation now covers the lda/nmf writeback=true case (EG-P0-3 fixed) |
| `MineSubgraph` | ~true | GraphRedb | `mining:write` | false | true | false | Atomic | mutates is a conservative upper bound (writeback AND non-tfidf/non-motif algorithm-conditional per access.rs); wal.rs::is_durable_mutation now covers the gspan writeback=true case (EG-P0-3 fixed) |
| `MineEntityResolve` | ~true | GraphRedb | `mining:write` | false | true | false | Atomic | mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field |
| `MineCausalImpact` | ~true | GraphRedb | `mining:write` | false | true | false | Atomic | mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field |
| `MineProcess` | ~true | GraphRedb | `mining:write` | false | true | false | Atomic | mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field |
| `MineRootCause` | ~true | GraphRedb | `mining:write` | false | true | false | Atomic | mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field |
| `MineRiskPropagation` | ~true | GraphRedb | `mining:write` | false | true | false | Atomic | mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field |
| `MineOntologyGap` | ~true | GraphRedb | `mining:write` | false | true | false | Atomic | mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field |
| `MineRetrievalQuality` | ~true | GraphRedb | `mining:write` | false | true | false | Atomic | mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field |
| `MineCommunity` | ~true | GraphRedb | `mining:write` | false | true | false | Atomic | mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field |
