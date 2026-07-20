# Epistemic Graph

`epistemic-graph` is the unified, Rust-native **"master-of-all" data & compute engine** for the
agent-utilities ecosystem. It collapses a graph database, vector index, SQL warehouse, triple-store +
OWL reasoner, time-series DB, content-addressed blob store, and full-text index into **one durable
engine** with **one cross-modal `RowSet` planner**, exposed to Python out-of-process over
length-prefixed **MessagePack on UDS/TCP** (HMAC-authenticated, no PyO3) or embedded in-process on the
edge.

It is **durable by default** (redb-authoritative — an acked write survives `kill -9`) and scales by
configuration alone, from an embedded edge process to a multi-node Raft cluster with cross-shard
transactions.

> **[North Star: Seamless](north_star.md)** — every cross-modal seam (a write→read path that crosses
> OWL/vector/graph/timeseries/relational) is implemented **at every surface** — RPC, all SQL wires,
> SPARQL, GraphQL — never merely flagged. Each surface is a thin parser/router onto the same committed
> seam. The North Star page is the current seam-verification matrix.

```mermaid
flowchart LR
    EDGE["edge / local process<br/>EmbeddedEngine library handle"]
    NODE["single durable server<br/>main build"]
    CLUSTER["HA cluster<br/>Raft + pgwire + cross-shard 2PC"]
    EDGE --> NODE --> CLUSTER
```

It also fronts **many wire protocols** and **cross-cutting subsystems** on that one substrate — a
message broker, an observability stack, GIS, tensors, streams, and an LLM KV-cache — so the same durable
store answers a Postgres, Neo4j, Redis, S3, AMQP, or PromQL client:

```mermaid
flowchart TB
    subgraph Wires["Wire adapters (one WireProtocol exec path)"]
        W["native · pgwire · sqlite · mysql · mssql · bolt · redis · s3 · amqp · mqtt · stomp · obs"]
    end
    subgraph Subsys["Cross-cutting subsystems"]
        BR["broker"]
        OB["observability"]
        MM["agent-memory"]
        KV["KV-cache"]
    end
    subgraph Modalities["Modality engines"]
        MOD["graph · vector · SQL · RDF/OWL · SHACL/ShEx · TSDB · text · GIS · tensor · stream · BLOB"]
    end
    CORE["one durable substrate<br/>GraphCore + redb-authoritative + unified RowSet planner"]

    Wires --> CORE
    Subsys --> CORE
    CORE --> Modalities
```

See the [subsystems reference](architecture/subsystems.md) for how each composes on the substrate.

> **Honesty first.** Every capability is tracked operation-by-operation in the
> **[capabilities & parity matrix](capabilities.md)**; per-method authority, durability,
> audit, CDC, and transaction facts come from the
> **[generated capability ledger](capabilities.generated.md)**. External hardware and
> multi-host campaigns are release-certification evidence, not unimplemented source.

## Start here

- **[Capabilities & parity matrix](capabilities.md)** — the operation-by-operation truth table per
  interface. Read this to know exactly what works today.
- **[Technical Overview](overview.md)** — the crate DAG, the unified `RowSet` planner pipeline, and the
  always-on graph algorithms.
- **Per-interface guides** — [SQL](interfaces/sql.md) · [SPARQL](interfaces/sparql.md) ·
  [Cypher](interfaces/cypher.md) · [GraphQL](interfaces/graphql.md) · [Vector](interfaces/vector.md) ·
  [Time-series](interfaces/timeseries.md) · [KV & Blob](interfaces/kv_blob.md) ·
  [Ontology lifecycle](interfaces/ontology.md).
- **[Master-of-all engine](architecture/engine.md)** — the deep architecture: durability, cross-modal
  ACID, RDF/OWL mapping, the RLS request path, streaming/CDC, federation, multi-Raft + cross-shard 2PC,
  and the tenant lifecycle.
- **[Authoritative MutationBatch](architecture/mutation_batch.md)** — commit-before-ack staging,
  durable idempotency/status, ordered projection outbox, and the native WorkItem state machine.
- **[Governed ChangeEnvelope](architecture/change_envelope.md)** — one native transaction for
  external graph/object material, policy, lineage, evidence, typed content versions/cursors, and
  the outbox, with verified request binding and cluster-safe replay.
- **[Governed modality serving](architecture/modality_serving.md)** — universal opaque
  Artifact/Occurrence/Rendition/Segment/Feature/EvidenceLocus identities, 12/12 plus native-probe
  production media TCKs, a verified-context `ServedModality` ingest/typed-query/lifecycle service
  with concrete native decoding, AEAD-sealed state, and bounded resources, plus the one cursor-driven `KnowledgeStream`
  protocol shared by graph, SQL, RDF, vector,
  time series, jobs, and cross-modal queries. It returns bounded Arrow KnowledgeBatches after the
  normal RequestContext, RLS, and placement gates; Arrow is the sole result projection.
- **[Distributed analytics and incremental reasoning](architecture/distributed-analytics-reasoning.md)** —
  verified coordinator RPCs for leased/fenced remote workers, durable typed results with transactional claims, and
  outbox/cursor-driven TMS, conflict, causal and materialization maintenance.
- **[Native program optimization](architecture/native-program-optimization.md)** — the operational
  `submit_program_optimization` contract, 13 Rust-native optimizer families, evidence across all
  14 modalities, governed runtime plan steps, and evaluation-gated promotion.

## Operate & deploy

- **[One build, opt-in layers](architecture/tiers.md)** — the feature-composition map: the one main
  full-featured build plus the `cluster` (HA raft) and `full-extras` (GPU/ROS2) opt-in build layers.
- **[Engine modes](engine_modes.md)** — endpoint resolution, one shared local server, supervised
  autostart of the packaged main binary, and the embedded library path.
- **[Deployment (database)](deployment.md)** — Docker, prebuilt wheels, single-node, and HA-cluster
  recipes for every scale.
- **[Service Mode](service_mode.md)** — the wire protocol, authentication, multi-graph management,
  isolation policy, and Prometheus metrics.
- **[Cost model & capacity](cost_model.md)** — the per-tenant memory budget, autoscale signals, and
  embedded-to-cluster footprint planning.

## Reference

- **[Rust Compute Guide](rust_compute_guide.md)** — adding a capability across protocol/server/client.
- **[Transport Benchmarks](benchmarks.md)** — measured per-operation latency over MessagePack.
- **[Concept Registry](concepts.md)** — the stable `CONCEPT` identifiers that trace the engine's ideas.
- **[Binary promotion](deploy/binary_promotion.md)** — promoting a new engine binary through a deployment registry.
- Architecture deep-dives: [Write coalescer](architecture/write_coalescer.md) ·
  [Index manager](architecture/index_manager.md) · [Correctness harness](architecture/correctness_harness.md).

## Design principle: design for a network boundary

Every out-of-process invocation crosses a process boundary — serialize, socket round-trip, deserialize.
A call is **not** a cheap function call. **Batch, never per-element:** ship work into a single
round-trip over data already resident in the graph (one all-pairs op, not a Python loop), and keep
tight per-element math in-process. The [Rust Compute Guide](rust_compute_guide.md) explains how this
shapes every caller.
