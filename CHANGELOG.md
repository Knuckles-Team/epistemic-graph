# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Added
- **Protocol drift gate (CONCEPT:KG-2.19)** — `tests/test_protocol_parity.py` asserts the
  hand-written Python client and the Rust `Method` enum (165 variants) stay in lockstep across the
  PyO3-free MessagePack boundary: no client `_send("X")` without a matching variant, and the set of
  variants with no client binding is ratcheted against `tests/protocol_unbound_baseline.txt`. Wired
  into `rust-ci.yml` as a fast `--noconftest` job (no wheel build). Documented in
  `docs/transport_boundary_adr.md`.

- **`ParseFiles` batch AST op (CONCEPT:KG-2.16)** — parse N files in ONE round-trip instead of N
  per-file `ParseFile` calls. `Method::ParseFiles { files_msgpack }` (a MessagePack
  `Vec<(path, source)>`) → `parser::tree_sitter::parse_files`, which fans the files across rayon
  (each parse is stateless), returns an **ordered** `Vec<ParseResult>` (1:1 with input), and maps a
  per-file parse failure to an empty result so the batch never aborts. `Health` now advertises
  `version` + `ops` (e.g. `["ParseFiles"]`) for client capability negotiation. Client:
  `GraphOperationsClient.parse_files()` + `EpistemicGraphClient.supports()`. Version → 0.27.0.
- **Training loss / optimizer kernels (CONCEPT:KG-2.22)** — `src/datascience/training.rs`: pure-Rust
  `softmax` / `log_softmax`, `cross_entropy` (+ analytic grad), `dpo_loss` (Bradley-Terry, + chosen/rejected
  grads), `grpo_surrogate` (PPO/GRPO clipped, + grad with zero-grad clip region), `kl_divergence` (Schulman k3),
  and `adam_step` / `sgd_step` optimizers. The Wave-C / C1 performance path for the in-house training substrate
  — mirrors the pure-Python reference (`agent-utilities graph/training_signals.py`) and the torch kernels
  (`data-science-mcp trainers/objectives.py`), letting a trainer batch a step over the wire in one round-trip.
  Exposed end-to-end: `Method::Ds*` variants (`src/protocol.rs`), dispatch arms (`src/server.rs`), and
  `client.datascience.{softmax,log_softmax,cross_entropy,dpo_loss,grpo_surrogate,kl_divergence,adam_step,sgd_step}`
  (`epistemic_graph/client.py`, auto-exposed on the sync client). No candle/GPU — matches the existing pure-Rust
  `datascience` style. Tests: 8 inline Rust unit tests + 8 Python round-trip tests (`tests/test_compute_primitives.py`).

## [0.1.0] — 2026-05-24

### Added
- Initial Rust `epistemic-graph` engine implementation using `petgraph` stable graph.
- PyO3-based Python native extension bindings.
- DFS-based cycle detection returning exact cycle paths.
- BFS-based shortest path search and blast radius calculator.
- Applied ecosystem package standards including pre-commit, bumpversion, gitattributes, codespell, and pytest suite.
- Multi-stage testing Dockerfile and compose layout.
