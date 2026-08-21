# Architecture

This is the curated map of the `architecture/` tree (37 pages). It groups every page by what it's
actually about — the commit model, the analytics/reasoning plane, distribution and scaling,
performance, media/voice/visualization, the robotics/GPU tail, and security/access hardening — so
you can go straight to the layer you need instead of scanning a flat file list. Pages marked
**★** are the release-defining contract pages tracked directly in site navigation and by the
documentation-contract gate (`scripts/check_documentation_contract.py`); everything else here is
just as real, only demoted out of the sidebar.

For the engine's single guiding design principle, see
**[North Star: Seamless](../north_star.md)** — every cross-modal read/write path (a write that
touches OWL, vector, graph, and time-series together) is implemented at *every* wire surface
(RPC, every SQL wire, SPARQL, GraphQL, …), never merely flagged at the one it was first built for.

## Core commit & governance model

The transactional backbone: one durable commit point, one structural property schema, one served
modality identity, and the substrate they all sit on.

| Page | What it covers |
|---|---|
| [The master-of-all engine](engine.md) | The deep architectural reference: durability, cross-modal ACID, RDF/OWL mapping, the RLS request path, streaming/CDC, federation, multi-Raft + cross-shard 2PC, tenant lifecycle. |
| ★ [Verified Request Authority](request_authority.md) | The `eg2.` request envelope every call carries — request id, graph, method, digest, timestamp, nonce, idempotency key, ACL agent, tenant, audience, policy version, roles, scopes, delegation chain, trace context. |
| ★ [Authoritative MutationBatch](mutation_batch.md) | The engine's durable mutation currency — commit-before-ack staging, durable idempotency/status, ordered projection outbox, the native WorkItem state machine. |
| ★ [Governed ChangeEnvelope](change_envelope.md) | One native transaction for external graph/object material, policy, lineage, evidence, typed content versions/cursors, and the outbox. |
| ★ [Canonical structural properties](canonical-property-schema.md) | Why structural meaning uses one key per concept — property blobs are open MessagePack, but structural fields aren't silently promoted through aliases. |
| ★ [Governed modality serving](modality_serving.md) | Universal Artifact/Occurrence/Rendition/Segment/Feature/EvidenceLocus identities, the `ServedModality` ingest/query/lifecycle service, and the one cursor-driven `KnowledgeStream` protocol shared by every modality. |
| [Subsystems (C4 containers)](subsystems.md) | How the broker, observability stack, KV-cache, and every wire adapter compose on the one `GraphCore` + redb-authoritative store + unified `RowSet` planner. |
| [One build, opt-in layers](tiers.md) | The feature-composition map: the one main full-featured build plus the opt-in `cluster` (HA Raft) and `full-extras` (GPU/ROS2) layers. |
| [Epistemic OS Hardening — capability catalog](epistemic-os-hardening.md) | The code-verified, line-anchored catalog of everything the hardening program (Phase 0 → Phase 3 + "exceed" tracks) shipped. |

## Analytics, reasoning & optimization

The numeric/analytics kernel, the incremental-reasoning plane, and self-improving program
optimization.

| Page | What it covers |
|---|---|
| ★ [Analytics Program — one kernel, two surfaces](analytics_program.md) | The BLAS/LAPACK-free Rust numeric kernel (`eg-numeric`) that serves both Python-side array math and in-database analytics over engine-resident data. |
| ★ [Numeric kernel](numeric_kernel.md) | The kernel foundation of the Analytics Program — compiled kernel, Agent Utilities numeric surface, and native engine operators. |
| ★ [Distributed analytics & incremental reasoning](distributed-analytics-reasoning.md) | Verified coordinator RPCs for leased/fenced remote workers, durable typed results, and outbox/cursor-driven TMS, conflict, causal, and materialization maintenance. |
| ★ [Native program optimization](native-program-optimization.md) | The `submit_program_optimization` contract, 13 Rust-native optimizer families, evidence across all 14 modalities, governed runtime plan steps, evaluation-gated promotion. |
| [Lakehouse LTAP interop](lakehouse_ltap.md) | The LTAP (Lakehouse-Transactional-Analytical Processing) superset: external engines read the store as open Parquet + Delta/Iceberg with zero ETL. |
| [Data mining](../mining.md) | `graph_mine` / `/api/mining` — pattern and structure mining over engine-resident graphs. |
| [Graph learning](../graphlearn.md) | `graph_learn` / `/api/graphlearn` — learned representations over the same substrate. |
| [Analytics in UQL (EG-353)](../analytics_in_uql.md) | Analytics operators reachable directly from the [Unified Query Language](../uql.md). |

## Distribution & scaling

Raft clustering, resharding, admission control, and the correctness harness that gates every
distributed/durability claim.

| Page | What it covers |
|---|---|
| [Engine Scaling Program (M1/M2/M3)](scaling_program.md) | The claim-status vocabulary (`DESIGNED`/`IMPLEMENTED`/`UNIT-PROVEN`/`LAB-PROVEN`/`LIVE`) and the milestone map for the whole scaling program. |
| [Scale claim register](scale_claims.md) | The machine-readable evidence-state register backing the M1/M2/M3 docs. |
| [Multi-Raft cluster status (M2)](m2_raft_status.md) | Current-main status and handoff for M2 Raft hardening. |
| [Catalog-driven resharding (M3)](m3_resharding.md) | Current-main status handoff for catalog-driven resharding. |
| [M3 cross-node elasticity planner](m3-cross-node-elasticity.md) | The policy-only planning layer for cross-node graph/shard elasticity — proposals only; placement/reshard stays the sole mutator. |
| [Cluster deployment & migration](cluster_deployment.md) | Running the engine as an HA Raft cluster across runtime-selected nodes and converting the current authoritative node into the cluster seed without data loss. |
| [Bounded shard/Raft drain contract](shard_drain.md) | The side-effect-free state machine a membership owner uses before removing a node. |
| [Per-graph write coalescer](write_coalescer.md) | Turning N concurrent single-op writes to one graph into one topology-lock acquisition per batch. |
| [Reserved read-admission lane](reserved_read_lane.md) | The admission lane reserved for reads/queries so an interactive request is never shed behind an ingestion write firehose. |
| [Unified IndexManager](index_manager.md) | One registry/seam over the engine's secondary indexes, so the planner consults one place instead of each index individually. |
| [Correctness + load harness](correctness_harness.md) | The standing proof-engine gating every distributed/durability claim: multi-Raft, M2 redb durability, distributed transactions, PITR/replication. |
| [Cgroup-aware capacity resolution](cgroup_capacity.md) | The single resource-capacity seam deriving queue/concurrency/runtime/memory/ASR limits from host + cgroup observation. |

## Performance & complexity

| Page | What it covers |
|---|---|
| ★ [Hot-path complexity ledger](hot-path-complexity.md) | Algorithmic bounds (not benchmark claims) for the hottest request paths. |
| [D-OP-1: RLS projection cache](d-op-1-projection-cache.md) | Caching `project_core()`'s RLS projection per `(actor, graph version)`. |
| [Push-gate execution evidence](push_gate_evidence.md) | `ci_gate_replica.py` — the single producer for workflow-derived heavy checks in the pre-push gate. |

## Media, voice & visualization

| Page | What it covers |
|---|---|
| [Native ASR provider (GOC-33)](native_asr.md) | Native Rust speech-to-text, reachable over the engine's own wire protocol as `Method::Asr`. |
| [Native Piper-ONNX TTS (GOC-34)](native_tts_piper.md) | The native text-to-speech provider behind the `tts.*` wire contract. |
| [Native visualization (D-VZ-1)](native_visualization.md) | The LOD-native visualization stack (declarative chart IR, columnar store, decimation/density kernels) in place of a `matplotlib`-style library. |
| [Remote KV-cache HTTP backend](kvcache_remote_backend.md) | The `kvcache-server` feature exposing the shared, content-addressed KV-cache backend so parallel vLLM/LMCache instances share blocks by token-hash. |

## Robotics, GPU & the distribution tail

| Page | What it covers |
|---|---|
| [Distribution, Robotics & GPU tail (EG-3.x)](distribution_robotics_gpu.md) | The last features of Program B — all feature-gated into the opt-in `full-extras` layer; the default build links none of the heavy deps. |
| [The unified in-process engine (PyO3) — design](unified-inprocess-engine.md) | Embedding the Rust engine in-process via PyO3 for the single-binary deployment shape. |

## Security, access & hardening

| Page | What it covers |
|---|---|
| [RBAC action unification — Phase 1](rbac-action-unification.md) | Blast-radius map for unifying two RBAC action vocabularies. Phase 1 verdict: not yet safe to fully unify; no behavior changed by the document itself. |
| [Native capacity & WorkItem admission authority](native-control-authority.md) | The additive native protocol for the GOC-21 capacity lease and GOC-19 WorkItem submit surfaces; the engine's redb tables are authoritative, schedulers are projections. |

---

## Design principle: design for a network boundary

Every out-of-process invocation crosses a process boundary — serialize, socket round-trip,
deserialize. A call is **not** a cheap function call. **Batch, never per-element:** ship work into
a single round-trip over data already resident in the graph (one all-pairs op, not a Python loop),
and keep tight per-element math in-process. The [Rust Compute Guide](../rust_compute_guide.md)
explains how this shapes every caller.
