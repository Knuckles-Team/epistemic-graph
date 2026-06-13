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

## Tiered-backend reality (correcting a related assumption)

This crate is **L1 only**: an in-memory `petgraph` core plus local MessagePack
snapshots (`src/persist.rs`) for fast warm restart. The snapshot is a *cache*,
not a system of record. The **durable** tier (PostgreSQL / pggraph) and any
LadybugDB L2 live in `agent-utilities`' backend layer, selected via
`create_backend()` — not in this repo. SPARQL is currently limited to
rudimentary `INSERT/DELETE DATA` parsing in `ApplyMutation`; full SPARQL is not
implemented. Trust scoring and human-in-the-loop gating are **not** engine
features — they live in the agent-utilities orchestration layer
(request/grant-approval, risk-veto, blast-radius).
