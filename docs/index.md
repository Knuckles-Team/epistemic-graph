# Epistemic Graph

`epistemic-graph` is the Rust-native compute engine for the agent-utilities
ecosystem. It consolidates graph algorithms, quantitative finance, data science
primitives, AST analysis, and OWL/Datalog reasoning into a single
high-performance binary, exposed to Python out-of-process through a long-running
Tokio daemon that speaks length-prefixed **MessagePack over Unix Domain Sockets
(or TCP)** and authenticates with **HMAC-SHA256**.

This site collects the engineering references for operating and extending the
engine. Use the navigation to move between the technical overview, the service-mode
operations guide, the Rust compute reference, the measured transport benchmarks,
and the concept registry.

## Documentation map

- [Technical Overview](overview.md) — the compiled Rust data structures and the
  graph algorithms (topological sort, cycle detection, shortest path, blast radius,
  PageRank, community detection) that the engine exposes.
- [Service Mode](service_mode.md) — running the engine as a daemon: socket and TCP
  endpoints, authentication, sharding, and health management.
- [Rust Compute Guide](RUST_COMPUTE_GUIDE.md) — the architecture of the compute
  modules and the procedure for adding a new capability across the protocol, server,
  and client layers.
- [Transport Benchmarks](benchmarks.md) — measured per-operation latency over the
  MessagePack transport, with reproduction instructions.
- [Concept Registry](concepts.md) — the stable `CONCEPT` identifiers that trace the
  engine's core ideas across code and documentation.

## Design principle

Every invocation crosses a process boundary: serialize, socket round-trip,
deserialize. A call is not a cheap function call. Batch work into a single
round-trip over data already resident in the graph, and keep tight per-element math
in-process — the [Rust Compute Guide](RUST_COMPUTE_GUIDE.md) explains how this shapes
every caller.
