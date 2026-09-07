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
  and `ResultPayload`. Contract changes are cut over atomically across every
  in-repository client; old DTO shapes and serde-default compatibility readers are
  deleted in the same change.

## Consequences

- Hardening focuses on the **wire protocol and transport**: the sole current schema,
  `eg2.` verification, socket permissions, and
  backpressure — *not* ABI/FFI concerns.
- The engine can be restarted, replaced, or scaled independently of the Python
  process; many clients (MCP server, CLI, UIs, ingestion) share one engine,
  eliminating embedded-DB file-lock contention.
- There is still no PyO3/FFI, but the Python client no longer hand-mirrors the
  `Method` enum: RF-RULING-003 made `crates/eg-capabilities` the single contract
  registry and `gen_contract` emits `epistemic_graph/generated/*.py` from it, so a
  method id is spelled in generated code only. `gen_contract --check` (run in
  `release.yml` and on pre-push) byte-diffs the regenerated artifacts against the
  tree, and the eg-capabilities bijection test proves every wire variant has exactly
  one descriptor. This is the FFI-free equivalent of a generated binding's
  compile-time check, with no ratchet file in the loop.

## Durability reality (correcting a stale assumption)

> **Updated.** This section previously described the crate as a cache in front of
> a separate PostgreSQL/LadybugDB durable store. That numeric graph-storage hierarchy
> vocabulary is gone — the engine is now a **durable source of truth in its own
> right** (CONCEPT:AU-KG.backend.backend-modes, "the flip").

Built with the `redb` feature (in the one main build and the `cluster` layer), the
persist dir is the **authoritative store**: an acked
write is fsynced to redb before the Response (commit-before-ack) and survives `kill -9`.
The optional Postgres/pg-age, neo4j, falkordb, or ladybug backends in `agent-utilities`
are now **mirrors** written-through for interop / BI / DR — not the system of record.
Served mode has one persistence contract: authoritative redb with
commit-before-ack and a mandatory durable directory.

SPARQL is no longer rudimentary: the engine ships a native **SPARQL 1.1 SELECT** surface
(spargebra → GraphView scans, CONCEPT:EG-KG.ontology.concept-11) plus an **OWL 2 EL⁺/RL reasoner**
(CONCEPT:EG-KG.ontology.incremental-materialization), both composable into the unified planner. Trust scoring and
human-in-the-loop gating remain **not** engine features — they live in the
agent-utilities orchestration layer (request/grant-approval, risk-veto, blast-radius).
See [the master-of-all engine](architecture/engine.md) for the durable + reasoning
architecture.

## Auth reality (current)

The served engine accepts only the `eg2.` verified request-context envelope. It
binds method/body, graph, tenant, audience, authenticated principal,
effective agent, roles, scopes, policy version, delegation, timestamp, nonce, and
idempotency key under HMAC-SHA256 and commits nonce acceptance to durable replay
state before dispatch. Startup requires the `security` feature, non-empty secret,
audience/tenant/policy values, durable state, and a trusted signer registry. The
request envelope does not replace transport confidentiality: routable native TCP
must use the configured TLS/mTLS boundary. Auxiliary listeners remain loopback-only.
See [Service mode](service_mode.md#authentication-protocol) for the full contract.
