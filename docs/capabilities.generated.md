# Epistemic Graph -- Generated Capability Ledger

> **This file is GENERATED and is the AUTHORITATIVE machine-checked capability
> truth (CONCEPT:EG-P0-1)** -- regenerate with `cargo run -p eg-capabilities --bin
> gen_ledger`. It is derived from the exhaustive, no-wildcard `policy()` match in
> `crates/eg-capabilities/src/lib.rs`, which the compiler forces to stay in sync with
> every `Method` variant. `docs/capabilities.md` describes surface-level feature
> parity; this generated table is authoritative for per-method policy.

> `mutates` marked `~true` means the value is a conservative UPPER BOUND: the real
> runtime answer is conditional (an operation, a `writeback` flag, or a parsed
> query) -- see the `note` column. `VolatileControl` is explicit non-durable
> process/session state; `None` is reserved for methods with no state transition.

| Method | Mutates | Durability | Authz action | Idempotent | Audited | Emits CDC | Txn participation | Note |
|---|---|---|---|---|---|---|---|---|
| `AddNode` | true | GraphRedb | `node:write` | false | true | true | Atomic |  |
| `CreateNodeIfAbsent` | true | GraphRedb | `node:write` | false | true | true | Atomic | atomic create returns true only to the inserting writer, so its result is not cross-request cacheable |
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
| `Publish` | true | Outbox | `broker:publish` | false | true | false | Atomic | PublishIdempotent is the one exception (producer-id/seq dedup makes replays idempotent by construction) |
| `DeclareQueue` | true | Outbox | `broker:admin` | true | true | false | Atomic |  |
| `PublishEx` | true | Outbox | `broker:publish` | false | true | false | Atomic | PublishIdempotent is the one exception (producer-id/seq dedup makes replays idempotent by construction) |
| `BrokerConsume` | true | Outbox | `broker:consume` | false | true | false | Atomic |  |
| `BrokerAck` | true | Outbox | `broker:ack` | true | true | false | Atomic |  |
| `BrokerReject` | true | Outbox | `broker:ack` | true | true | false | Atomic |  |
| `ClaimWorkItem` | true | GraphRedb | `work:claim` | false | true | false | Atomic | engine-native tenant/fair WorkItem lease claim |
| `RenewWorkItemLease` | true | GraphRedb | `work:write` | true | true | false | Atomic | lease epoch and fencing token are validated atomically |
| `CommitWorkItemResult` | true | GraphRedb | `work:write` | true | true | false | Atomic | terminal result references and outbox commit atomically |
| `CancelWorkItem` | true | GraphRedb | `work:write` | true | true | false | Atomic | pending cancellation never steals an active lease |
| `DeferWorkItem` | true | GraphRedb | `work:write` | true | true | false | Atomic | fenced lease release schedules retry without consuming an attempt |
| `SweepExpired` | true | Outbox | `broker:admin` | true | true | false | Atomic |  |
| `StreamDeclare` | true | Outbox | `stream:admin` | true | true | false | Atomic |  |
| `StreamPublish` | true | Outbox | `stream:write` | false | true | false | Atomic |  |
| `StreamRead` | false | None | `stream:read` | true | false | false | Snapshot |  |
| `StreamTrim` | true | Outbox | `stream:admin` | true | true | false | Atomic |  |
| `StreamCommitOffset` | true | Outbox | `stream:admin` | true | true | false | Atomic |  |
| `StreamCommittedOffset` | false | None | `stream:read` | true | false | false | Snapshot |  |
| `PublishConfirmed` | true | Outbox | `broker:publish` | false | true | false | Atomic | PublishIdempotent is the one exception (producer-id/seq dedup makes replays idempotent by construction) |
| `PublishIdempotent` | true | Outbox | `broker:publish` | true | true | false | Atomic | PublishIdempotent is the one exception (producer-id/seq dedup makes replays idempotent by construction) |
| `BrokerAckTag` | true | Outbox | `broker:ack` | false | true | false | Atomic | current-generation result must not be replay-cached across requests |
| `BrokerNackTag` | true | Outbox | `broker:ack` | false | true | false | Atomic | current-generation result must not be replay-cached across requests |
| `BrokerRenewTag` | true | Outbox | `broker:ack` | false | true | false | Atomic | current-generation result must not be replay-cached across requests |
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
| `GetEdgesPage` | false | None | `edge:read` | true | false | false | Snapshot |  |
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
| `PruneByLifecycle` | true | GraphRedb | `node:admin` | false | false | false | Atomic | state-backed MutationBatch commits the resulting authoritative image |
| `GetContextView` | false | None | `node:read` | true | false | false | Snapshot |  |
| `BatchUpdate` | true | GraphRedb | `node:write` | false | true | false | Atomic |  |
| `MultiGraphBatchUpdate` | true | ControlRedb | `node:write` | true | false | false | Saga | durable parent coordinator with per-graph MutationBatch children |
| `Metrics` | false | None | `service:control` | true | false | false | None |  |
| `EvictLRU` | true | GraphRedb | `node:admin` | false | false | false | Atomic | state-backed MutationBatch commits the resulting authoritative image |
| `DecaySweep` | true | GraphRedb | `node:admin` | false | false | false | Atomic | state-backed MutationBatch commits the resulting authoritative image |
| `TouchNodes` | true | GraphRedb | `node:admin` | false | false | false | Atomic | state-backed MutationBatch commits the resulting authoritative image |
| `ToMsgpack` | false | None | `graph:read` | true | false | false | Snapshot |  |
| `FromMsgpack` | true | GraphRedb | `graph:admin` | false | true | true | Atomic | state-backed MutationBatch commits the imported authoritative image |
| `GetLedger` | false | None | `ledger:read` | true | false | false | Snapshot |  |
| `ClearLedger` | true | GraphRedb | `ledger:admin` | true | true | true | Atomic | state-backed MutationBatch |
| `ApplyLedger` | true | GraphRedb | `ledger:write` | false | true | true | Atomic | state-backed MutationBatch |
| `AuditVerify` | false | None | `security:audit` | true | false | false | Snapshot |  |
| `GetSubgraph` | false | None | `node:read` | true | false | false | Snapshot |  |
| `Fork` | false | None | `graph:read` | true | false | false | Snapshot | returns the forked snapshot to the caller; never registers/persists it server-side |
| `DiffAgainst` | false | None | `graph:read` | true | false | false | Snapshot |  |
| `CompactNodesByType` | true | GraphRedb | `node:admin` | false | true | true | Atomic | state-backed MutationBatch |
| `RunDatalogReasoning` | true | GraphRedb | `reasoning:write` | false | true | true | Atomic | state-backed MutationBatch commits inferred facts |
| `ApplyChangeEnvelope` | true | GraphRedb | `ingest:write` | true | true | true | Atomic | Engine-native object/material/governance/version/cursor/outbox commit; verified context is mandatory |
| `ApplyChangeEnvelopes` | true | GraphRedb | `ingest:write` | true | true | true | Atomic | Batch envelope coordinator: one coalesced graph transaction per shard-partition; same policy class as ApplyChangeEnvelope |
| `GetChangeEnvelope` | false | None | `ingest:read` | true | false | false | Snapshot | Verified tenant-scoped reconciliation read |
| `GetContentVersion` | false | None | `ingest:read` | true | false | false | Snapshot | Typed content versions are never compared lexically |
| `GetChangeCursor` | false | None | `ingest:read` | true | false | false | Snapshot | Typed source cursors are tenant/graph/partition scoped |
| `ServedModality` | ~true | GraphRedb | `modality:write` | false | true | true | Atomic | runtime-conditional: authority/query/events/capabilities are verified read snapshots; ingest/delete/cold/restore commit an encrypted state-backed MutationBatch |
| `CreateGraph` | true | GraphRedb | `graph:admin` | true | false | false | Atomic | native lifecycle MutationBatch before registry publication |
| `DeleteGraph` | true | GraphRedb | `graph:admin` | true | false | false | Atomic | native lifecycle MutationBatch before registry eviction |
| `ListGraphs` | false | None | `graph:read` | true | false | false | Snapshot |  |
| `Reshard` | true | ControlRedb | `admin:cluster` | true | false | false | Saga | prepared/committed admin MutationBatch saga |
| `CatalogAssign` | true | ControlRedb | `admin:cluster` | true | false | false | Saga | prepared/committed admin MutationBatch saga |
| `CatalogReassign` | true | ControlRedb | `admin:cluster` | true | false | false | Saga | prepared/committed admin MutationBatch saga |
| `CatalogRemove` | true | ControlRedb | `admin:cluster` | true | false | false | Saga | prepared/committed admin MutationBatch saga |
| `CatalogList` | false | None | `admin:cluster-read` | true | false | false | Snapshot |  |
| `RebalancePlan` | false | None | `admin:cluster-read` | true | false | false | Snapshot |  |
| `RebalanceExecute` | true | ControlRedb | `admin:cluster` | true | false | false | Saga | prepared/committed admin MutationBatch saga |
| `PlacementRoute` | false | None | `admin:cluster-read` | true | false | false | Snapshot | engine-authoritative complete route; single-node returns authoritative unplaced group 0/epoch 0, while clustered routing requires a live MultiRaft control leader |
| `RaftAddLearner` | true | ControlRedb | `admin:cluster` | true | false | false | Saga | leader-only openraft add_learner; attaches a non-voting replica without changing the voter set |
| `RaftChangeMembership` | true | ControlRedb | `admin:cluster` | true | false | false | Saga | leader-only openraft change_membership; sets the group's exact voter set (the usual way to promote a learner added via RaftAddLearner) |
| `PlacementAdmin` | true | ControlRedb | `admin:cluster` | false | false | false | Saga | raft-replicated placement-catalog admin op (Assign/Move/AbortMove, the placement DECISION + PLAN->EXECUTE->CATALOG-UPDATE legs): MultiRaft::placement_assign / TenantManager::move_partition / abort_move commit through the DEFAULT group's own client_write / commit_placement, not this gateway's per-graph MutationBatch |
| `Backup` | false | None | `admin:backup` | true | false | false | Snapshot | reads a consistent snapshot out to a bundle; does not mutate the live graph |
| `Restore` | true | ControlRedb | `admin:backup` | true | false | false | Saga | prepared/committed admin MutationBatch saga |
| `CreateChannel` | true | ControlRedb | `channel:admin` | true | false | false | Saga | opaque prepared/committed session-control MutationBatch; message/member payloads stay out of the ledger |
| `JoinChannel` | true | ControlRedb | `channel:admin` | true | false | false | Saga | opaque prepared/committed session-control MutationBatch |
| `LeaveChannel` | true | ControlRedb | `channel:admin` | true | false | false | Saga | opaque prepared/committed session-control MutationBatch |
| `CloseChannel` | true | ControlRedb | `channel:admin` | true | false | false | Saga | opaque prepared/committed session-control MutationBatch |
| `SendMessage` | true | ControlRedb | `channel:write` | true | false | false | Saga | request-scoped opaque session-control receipt prevents acknowledgement-lost duplicate sends |
| `GetChannelMessages` | false | None | `channel:read` | true | false | false | Snapshot |  |
| `ListChannels` | false | None | `channel:read` | true | false | false | Snapshot |  |
| `GetChannelMembers` | false | None | `channel:read` | true | false | false | Snapshot |  |
| `Ping` | false | None | `service:control` | true | false | false | None |  |
| `Health` | false | None | `service:control` | true | false | false | None |  |
| `Shutdown` | true | VolatileControl | `service:admin` | true | false | false | None | explicitly ephemeral process control; never acknowledges a user-data commit |
| `CancelRequest` | false | None | `service:control` | true | false | false | None |  |
| `ResourceStats` | false | None | `service:control` | true | false | false | None |  |
| `Reconcile` | true | GraphRedb | `graph:write` | false | true | true | Saga | state-backed MutationBatch commits the merged image |
| `ApplyMutation` | true | GraphRedb | `graph:write` | false | true | true | Atomic | state-backed MutationBatch |
| `Vf2SubgraphMatch` | false | None | `compute:graph-algo` | true | false | false | Snapshot |  |
| `ParseFile` | false | None | `compute:parse` | true | false | false | None |  |
| `ParseFiles` | false | None | `compute:parse` | true | false | false | None |  |
| `IndexRepository` | false | None | `compute:parse` | true | false | false | None |  |
| `ObserveScreen` | false | None | `compute:vision` | false | false | false | None |  |
| `AddEmbedding` | true | GraphRedb | `node:write` | false | true | false | Atomic |  |
| `SemanticSearch` | false | None | `compute:semantic` | true | false | false | Snapshot |  |
| `Discover` | false | None | `compute:semantic` | true | false | false | Snapshot |  |
| `MatchOntologyTerms` | false | None | `compute:semantic` | true | false | false | Snapshot |  |
| `BatchL2Normalize` | false | None | `compute:semantic` | true | false | false | Snapshot |  |
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
| `RegisterIdentity` | true | ControlRedb | `security:admin` | true | false | false | Atomic | RBAC/identity snapshot and MutationBatch metadata share one rbac.redb WTX |
| `RbacAdmin` | ~true | ControlRedb | `security:admin` | true | false | false | Atomic | runtime-conditional: List is a read; role and grant updates share one rbac.redb WTX with MutationBatch metadata |
| `ApplyMultisigMutation` | true | GraphRedb | `security:admin` | true | true | true | Saga | threshold validation translates into the graph MutationBatch gateway |
| `AnalyticsJob` | ~true | JobsRedb | `jobs:write` | false | false | false | Atomic | runtime-conditional: Status is a read; Submit/Cancel/Resume commit through the native jobs.redb MutationBatch gateway |
| `Statechart` | ~true | StatechartRedb | `statechart:write` | false | false | false | Atomic | runtime-conditional: GetState/List are reads; Define/Instantiate/SendEvent commit to the native statecharts.redb store (CONCEPT:INT-P2-2) |
| `Sql` | ~true | GraphRedb | `query:sql` | false | true | false | Atomic | runtime-conditional; graph DML uses staged graph state while table/catalog writes atomically commit SQL rows plus MutationBatch status/fence/idempotency/outbox |
| `CypherQuery` | ~true | GraphRedb | `query:cypher` | false | true | false | Atomic | runtime-conditional; writes execute against a staged graph and publish only after durable MutationBatch commit |
| `GraphQl` | ~true | GraphRedb | `query:graphql` | false | true | false | Atomic | runtime-conditional; ordinary writes stage through MutationBatch and cross-modal commit atomically includes universal status/fence/idempotency/outbox |
| `KnowledgeStream` | false | None | `query:stream` | true | false | false | Snapshot | one RequestContext/RLS/placement-bound stream with the sole native Arrow IPC projection for all seven query families |
| `UnifiedQuery` | false | None | `query:unified` | true | false | false | Snapshot |  |
| `UnifiedQueryText` | false | None | `query:unified` | true | false | false | Snapshot |  |
| `ExplainPlan` | false | None | `explain:read` | true | false | false | Snapshot |  |
| `ExplainProvenance` | false | None | `explain:read` | true | false | false | Snapshot |  |
| `ExplainProvenanceByIds` | false | None | `explain:read` | true | false | false | Snapshot | CONCEPT:EG-KB-CURRENCY — ID-seeded sibling of ExplainProvenance, same policy profile |
| `ExplainPolicy` | false | None | `explain:read` | true | false | false | Snapshot |  |
| `ExplainBelief` | false | None | `explain:read` | true | false | false | Snapshot |  |
| `EpistemicStatus` | false | None | `explain:read` | true | false | false | Snapshot | L53 (EPI-P3-5) acceptance capstone; handler additionally gated `epistemic-tms` |
| `WhatChanged` | false | None | `explain:read` | true | false | false | Snapshot | L53 (EPI-P3-5) bitemporal diff; handler additionally gated `epistemic-tms` |
| `RecomputeMaterialization` | true | ReasoningProjection | `reasoning:write` | false | false | false | Atomic | fenced recompute/writeback resolves provenance from the authoritative graph and fsyncs the per-graph projection |
| `MaterializationStatus` | false | None | `explain:read` | true | false | false | Snapshot | read-only status from the durable per-graph incremental reasoning authority |
| `StaleMaterializations` | false | None | `explain:read` | true | false | false | Snapshot | bulk opaque stale references from the durable per-graph incremental reasoning authority |
| `ResolveConflict` | false | None | `explain:read` | true | false | false | Snapshot | EPI-P3-7 (gap-fill) standalone Dung argumentation (grounded/preferred/stable) conflict resolution over a BeliefGraph snapshot; handler additionally gated `epistemic-tms` |
| `ExplainEvidence` | false | None | `explain:read` | true | false | false | Snapshot | CONCEPT:EG-X1 multimodal-citation resolver; handler additionally gated `evidence-graph` |
| `CausalEstimate` | false | None | `explain:read` | true | false | false | Snapshot | EPI-P3-3/P3-6 do-calculus intervention OR observational conditioning (selected by `mode`) over a request-carried SCM; handler additionally gated `epistemic-causal` |
| `CausalCounterfactual` | false | None | `explain:read` | true | false | false | Snapshot | EPI-P3-6 Pearl point-counterfactual over a request-carried SCM + a fully-observed unit; handler additionally gated `epistemic-causal` |
| `RankByProvenance` | false | None | `explain:read` | true | false | false | Snapshot | EPI-P3-3 provenance-aware retrieval ranking; handler additionally gated `epistemic-causal` |
| `NlQuery` | false | None | `query:nl` | false | false | false | Snapshot |  |
| `RegisterForeignSource` | true | ControlRedb | `federation:admin` | true | false | false | Saga | opaque prepared/committed control receipt; endpoint configuration is not duplicated in the ledger |
| `RegisterUdf` | true | ControlRedb | `udf:admin` | true | false | false | Saga | opaque prepared/committed control receipt; module bytes are not duplicated in the ledger |
| `RunUdf` | false | None | `udf:exec` | false | false | false | Snapshot | executes a registered sandboxed function; treated as read/compute unless the UDF itself writes back (not modeled -- the wire protocol has no writeback flag here) |
| `DistributedCompute` | false | None | `distcompute:read` | true | false | false | Snapshot | read-only Pregel/GAS computation; materialization uses the distinct Create*/Refresh* methods |
| `CreateMatView` | true | ControlRedb | `matview:admin` | true | false | false | Saga | prepared/committed control-plane MutationBatch saga |
| `GetMatView` | false | None | `matview:read` | true | false | false | Snapshot |  |
| `RefreshMatView` | true | ControlRedb | `matview:admin` | true | false | false | Saga | prepared/committed control-plane MutationBatch saga |
| `PlanMatViewDefine` | true | ControlRedb | `matview:admin` | true | false | false | Saga | prepared/committed control-plane MutationBatch saga |
| `PlanMatViewGet` | false | None | `matview:read` | true | false | false | Snapshot |  |
| `PlanMatViewRefresh` | true | ControlRedb | `matview:admin` | true | false | false | Saga | prepared/committed control-plane MutationBatch saga |
| `PlanMatViewDrop` | true | ControlRedb | `matview:admin` | true | false | false | Saga | prepared/committed control-plane MutationBatch saga |
| `BeginTxn` | true | ControlRedb | `txn:control` | false | false | false | Saga | encrypted Raft-native transaction staging authority |
| `TxnAddNode` | true | ControlRedb | `txn:write` | false | false | false | Saga | encrypted Raft-native staging; Commit owns graph publication |
| `TxnRemoveNode` | true | ControlRedb | `txn:write` | true | false | false | Saga | encrypted Raft-native staging; Commit owns graph publication |
| `TxnAddEdge` | true | ControlRedb | `txn:write` | false | false | false | Saga | encrypted Raft-native staging; Commit owns graph publication |
| `TxnRemoveEdge` | true | ControlRedb | `txn:write` | true | false | false | Saga | encrypted Raft-native staging; Commit owns graph publication |
| `TxnCas` | true | ControlRedb | `txn:write` | true | false | false | Saga | encrypted Raft-native staging; Commit owns graph publication |
| `TxnAddEmbedding` | true | ControlRedb | `txn:write` | false | false | false | Saga | encrypted Raft-native cross-modal staging |
| `TxnBlobRef` | true | ControlRedb | `txn:write` | false | false | false | Saga | encrypted Raft-native cross-modal staging |
| `TxnAddMeasurement` | true | ControlRedb | `txn:write` | false | false | false | Saga | encrypted Raft-native cross-modal staging |
| `TxnAxiom` | true | ControlRedb | `txn:write` | false | false | false | Saga | encrypted Raft-native cross-modal staging |
| `TxnConstruct` | true | ControlRedb | `txn:write` | false | false | false | Saga | encrypted Raft-native cross-modal staging |
| `TxnPlanWriteback` | true | ControlRedb | `txn:write` | false | false | false | Saga | encrypted Raft-native cross-modal staging |
| `TxnMaterializeBelief` | true | ControlRedb | `txn:write` | false | false | false | Saga | encrypted Raft-native cross-modal staging |
| `TxnUnifiedQuery` | false | None | `txn:read` | true | false | false | Saga |  |
| `TxnUnifiedQueryText` | false | None | `txn:read` | true | false | false | Saga |  |
| `Commit` | true | ControlRedb | `txn:control` | true | false | false | Saga | named parent receipt plus atomic graph/cross-modal child batches |
| `Rollback` | true | ControlRedb | `txn:control` | false | false | false | Saga | encrypted Raft-native transaction staging removal |
| `TsAppend` | true | SeriesRedb | `timeseries:write` | false | false | false | Atomic | graph ACL + placement policy precede the tenant/graph/series-scoped series.redb write |
| `TsRange` | false | None | `timeseries:read` | true | false | false | Snapshot |  |
| `TsAsofJoin` | false | None | `timeseries:read` | true | false | false | Snapshot |  |
| `TsWindow` | false | None | `timeseries:read` | true | false | false | Snapshot |  |
| `TsGapFill` | false | None | `timeseries:read` | true | false | false | Snapshot |  |
| `BlobBegin` | true | BlobRedb | `blob:write` | false | false | false | Saga | multi-call chunked-upload protocol (Begin ... ChunkPut* ... Commit); no single-call atomicity; durable via its own blob.redb (group-committed Immediate), self-routes before dispatch_graph_op |
| `BlobChunkPut` | true | BlobRedb | `blob:write` | false | false | false | Saga | durable via its own blob.redb (group-committed Immediate); self-routes before dispatch_graph_op |
| `BlobCommit` | true | BlobRedb | `blob:write` | false | false | false | Saga | multi-call chunked-upload protocol (Begin ... ChunkPut* ... Commit); no single-call atomicity; durable via its own blob.redb (group-committed Immediate), self-routes before dispatch_graph_op |
| `BlobFetchBegin` | false | None | `blob:read` | true | false | false | Snapshot |  |
| `BlobChunkGet` | false | None | `blob:read` | true | false | false | Snapshot |  |
| `BlobFetchEnd` | false | None | `blob:read` | true | false | false | Snapshot |  |
| `BlobRef` | true | BlobRedb | `blob:write` | false | false | false | Atomic | refcount increment; idempotent-ish but re-invocation adds another ref, so not idempotent; durable via blob.redb |
| `BlobUnref` | true | BlobRedb | `blob:write` | false | false | false | Atomic | durable via blob.redb |
| `BlobGc` | true | BlobRedb | `blob:admin` | true | false | false | Atomic | durable via blob.redb |
| `KvGet` | false | None | `kv:read` | true | false | false | Snapshot |  |
| `KvPut` | true | KvRedb | `kv:write` | false | false | false | Atomic | durable via its own kv.redb (redb::Durability::Immediate, commit-before-ack); self-routes before graph dispatch |
| `KvDelete` | true | KvRedb | `kv:write` | true | false | false | Atomic | durable via its own kv.redb (redb::Durability::Immediate); self-routes before dispatch_graph_op |
| `KvScan` | false | None | `kv:read` | true | false | false | Snapshot |  |
| `KvCas` | true | KvRedb | `kv:write` | false | false | false | Atomic | durable via its own kv.redb (redb::Durability::Immediate, commit-before-ack); self-routes before graph dispatch |
| `ImportSqliteFile` | true | ControlRedb | `admin:sqlite-file` | true | true | false | Atomic | native SQL-catalog MutationBatch; logical transfer name is excluded from the durable receipt |
| `ExportSqliteFile` | false | None | `admin:sqlite-file` | true | true | false | Snapshot | operator-provisioned transfer root; logical filenames only |
| `AddTriples` | true | GraphRedb | `rdf:write` | false | true | false | Atomic |  |
| `GetRdf` | false | None | `rdf:read` | true | false | false | Snapshot |  |
| `RemoveTriples` | true | GraphRedb | `rdf:write` | true | true | false | Atomic |  |
| `DropNamedGraph` | true | GraphRedb | `rdf:write` | true | true | false | Atomic |  |
| `Sparql` | false | None | `sparql:read` | true | false | false | Snapshot |  |
| `SparqlVirtual` | false | None | `sparql:read` | true | false | false | Snapshot |  |
| `OwlReason` | false | None | `owl:read` | true | false | false | Snapshot |  |
| `OwlReasonDistributed` | false | None | `owl:read` | true | false | false | Snapshot |  |
| `OwlExplain` | false | None | `owl:read` | true | false | false | Snapshot |  |
| `RunRules` | false | None | `reasoning:read` | true | false | false | Snapshot | READ-ONLY (EG-P0-2/L11 handler audit): handle_run_rules reasons over an off-lock analysis_snapshot and returns inferred triples, no writeback -- unlike its sibling RunDatalogReasoning which materialises in-place. Corrected from a prior mutates=true semantic guess; now agrees with access.rs (never a write there) |
| `ShaclValidate` | false | None | `validation:read` | true | false | false | Snapshot |  |
| `IcvConfigure` | true | GraphRedb | `security:admin` | true | true | true | Atomic | state-backed MutationBatch |
| `ShexValidate` | false | None | `validation:read` | true | false | false | Snapshot |  |
| `CdcRead` | false | None | `cdc:read` | true | false | false | Snapshot |  |
| `RegisterContinuousQuery` | true | ControlRedb | `cdc:admin` | true | false | false | Saga | opaque prepared/committed session-control MutationBatch |
| `ReadContinuousQuery` | false | None | `cdc:read` | true | false | false | Snapshot |  |
| `DropContinuousQuery` | true | ControlRedb | `cdc:admin` | true | false | false | Saga | opaque prepared/committed session-control MutationBatch |
| `Watch` | false | None | `cdc:read` | false | false | false | None | opens a push subscription; not a snapshot read nor a mutation |
| `RegisterTrigger` | true | ControlRedb | `cdc:admin` | true | false | false | Saga | opaque prepared/committed session-control MutationBatch |
| `DropTrigger` | true | ControlRedb | `cdc:admin` | true | false | false | Saga | opaque prepared/committed session-control MutationBatch |
| `ListTriggers` | false | None | `cdc:read` | true | false | false | Snapshot |  |
| `FiredTriggers` | false | None | `cdc:read` | true | false | false | Snapshot |  |
| `CepSubscribe` | true | ControlRedb | `cep:admin` | true | false | false | Saga | opaque prepared/committed session-control MutationBatch |
| `CepPoll` | false | None | `cep:read` | true | false | false | Snapshot |  |
| `CepUnsubscribe` | true | ControlRedb | `cep:admin` | true | false | false | Saga | opaque prepared/committed session-control MutationBatch |
| `MineAssociate` | ~true | GraphRedb | `mining:write` | false | true | false | Atomic | mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field |
| `MineCluster` | ~true | GraphRedb | `mining:write` | false | true | false | Atomic | mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field |
| `MineAnomaly` | ~true | GraphRedb | `mining:write` | false | true | false | Atomic | mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field |
| `MineClassifyFit` | false | None | `mining:read` | true | false | false | Snapshot | the one Mine* family member that is unconditionally read-only (produces a model blob, never writes back) |
| `MineClassifyPredict` | ~true | GraphRedb | `mining:write` | false | true | false | Atomic | mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field |
| `MineReduce` | ~true | GraphRedb | `mining:write` | false | true | false | Atomic | mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field |
| `GraphLearnFit` | ~true | GraphRedb | `graphlearn:write` | false | true | false | Atomic | mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field |
| `GraphLearnPredict` | ~true | GraphRedb | `graphlearn:write` | false | true | false | Atomic | mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field |
| `MineSequence` | ~true | GraphRedb | `mining:write` | false | true | false | Atomic | mutates is a conservative upper bound; writeback=true enters the canonical durable mutation path |
| `MineForecast` | ~true | GraphRedb | `mining:write` | false | true | false | Atomic | mutates is a conservative upper bound; writeback=true enters the canonical durable mutation path |
| `MineText` | ~true | GraphRedb | `mining:write` | false | true | false | Atomic | mutates is a conservative upper bound; writeback=true for lda/nmf enters the canonical durable mutation path |
| `MineSubgraph` | ~true | GraphRedb | `mining:write` | false | true | false | Atomic | mutates is a conservative upper bound; writeback=true for gspan enters the canonical durable mutation path |
| `MineEntityResolve` | ~true | GraphRedb | `mining:write` | false | true | false | Atomic | mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field |
| `MineCausalImpact` | ~true | GraphRedb | `mining:write` | false | true | false | Atomic | mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field |
| `MineProcess` | ~true | GraphRedb | `mining:write` | false | true | false | Atomic | mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field |
| `MineRootCause` | ~true | GraphRedb | `mining:write` | false | true | false | Atomic | mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field |
| `MineRiskPropagation` | ~true | GraphRedb | `mining:write` | false | true | false | Atomic | mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field |
| `MineOntologyGap` | ~true | GraphRedb | `mining:write` | false | true | false | Atomic | mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field |
| `MineRetrievalQuality` | ~true | GraphRedb | `mining:write` | false | true | false | Atomic | mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field |
| `MineCommunity` | ~true | GraphRedb | `mining:write` | false | true | false | Atomic | mutates is a conservative upper bound: the REAL access::requires_write(m) returns the runtime `writeback` field |
