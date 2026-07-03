# ADR: Python ⇄ Rust boundary is a socket service, not FFI

**Status:** Accepted · **Concept:** KG-2.19 (Tokio Service Layer)

## Context

Reviewers frequently assume a Rust-backed Python library uses an **in-process
FFI / PyO3 extension**, and reason about its risks accordingly (ABI breakage,
GIL interplay, "contracts at the FFI boundary"). That assumption is wrong for
this engine, and the wrong mental model leads to the wrong hardening advice.

## Decision

The engine is exposed to Python **out-of-process** via a long-running Tokio
service speaking **length-prefixed MessagePack** (4-byte big-endian length +
`rmp_serde` body) over **Unix Domain Sockets** (default) or **TCP**,
authenticated with **HMAC-SHA256**. There is **no PyO3 / in-process extension**;
`Cargo.toml` declares `crate-type = ["rlib"]` and `scripts/check_no_pyo3.sh`
enforces the absence of PyO3 in source and built wheels.

- Server framing: `src/server.rs` (`handle_connection`).
- Client framing: `epistemic_graph/client.py` (`_send`).
- The "boundary contract" is therefore the **wire protocol** — the `Method`
  enum in `src/protocol.rs` (externally tagged: `{"method": ..., "params": ...}`)
  and `ResultPayload`. New fields use `#[serde(default)]` for backward
  compatibility (see `RunDatalogReasoning`).

## Consequences

- Hardening focuses on the **wire protocol and transport**: schema/version
  compatibility via serde defaults, HMAC auth, socket permissions, and
  backpressure — *not* ABI/FFI concerns.
- The engine can be restarted, replaced, or scaled independently of the Python
  process; many clients (MCP server, CLI, UIs, ingestion) share one engine,
  eliminating embedded-DB file-lock contention.
- Because there is no PyO3/codegen, the Python client hand-mirrors the `Method`
  enum by sending variant names as strings — a silent-drift risk. A CI gate
  (`tests/test_protocol_parity.py`, run in `rust-ci.yml`) closes it: it parses
  the `Method` enum and the client's `_send(...)` calls and asserts the two stay
  in lockstep (no client method without a Rust variant; unbound variants ratcheted
  against a committed baseline). This is the FFI-free equivalent of a generated
  binding's compile-time check.

## Durability reality (correcting a stale assumption)

> **Updated.** This section previously described the crate as "L1 only", a cache in
> front of a separate PostgreSQL/LadybugDB durable tier. That **L0/L1/L2/L3** tier
> vocabulary is gone — the engine is now a **durable source of truth in its own
> right** (CONCEPT:KG-2.195, "the flip").

Built with the `redb` feature (in the one main build and the `cluster` layer), the
persist dir is the **authoritative store**: an acked
write is fsynced to redb before the Response (commit-before-ack) and survives `kill -9`.
The optional Postgres/pg-age, neo4j, falkordb, or ladybug backends in `agent-utilities`
are now **mirrors** written-through for interop / BI / DR — not the system of record.
The opt-in `EPISTEMIC_GRAPH_PERSIST_BACKEND=snapshot` mode reverts to the older
rebuildable-cache behavior, where the local `.mp` snapshot exists only for fast warm
restart.

SPARQL is no longer rudimentary: the engine ships a native **SPARQL 1.1 SELECT** surface
(spargebra → GraphView scans, CONCEPT:KG-2.218) plus an **OWL 2 EL⁺/RL reasoner**
(CONCEPT:KG-2.219), both composable into the unified planner. Trust scoring and
human-in-the-loop gating remain **not** engine features — they live in the
agent-utilities orchestration layer (request/grant-approval, risk-veto, blast-radius).
See [the master-of-all engine](architecture/engine.md) for the durable + reasoning
architecture.
