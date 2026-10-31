# Medulla constitution

This repository inherits the [organization constitution](https://github.com/Knuckles-Team/pipelines/blob/main/.specify/memory/constitution.md) version 1.0.0. Every article of that constitution applies here. This file adds only the rules and limits of this repository.

**Inherits**: organization constitution v1.0.0 | **Version**: 1.0.0 | **Ratified**: 2026-10-31 | **Last amended**: 2026-10-31

## Organization standards

- [Spec standard](https://github.com/Knuckles-Team/pipelines/blob/main/reference/spec-standard.md): one owning spec per capability, computed states, `Spec:` trailers.
- [Ecosystem boundaries](https://github.com/Knuckles-Team/pipelines/blob/main/reference/ecosystem-boundaries.md): which repository owns which requirement.
- [Wording standard](https://github.com/Knuckles-Team/pipelines/blob/main/reference/ste.md): sentences of 20 words or fewer, with `must`, `should`, or `may`.

## Repository articles

### R-I. The engine is native Rust and the only fact store

Facts, records, storage, query, retrieval, ingest, reasoning and decisions are native Rust code in this repository. No other repository stores or decides these things.

**Enforced by:** `cargo clippy -- -D warnings`, `.config/arch-lint.toml`, `.config/deny.toml` and the forbidden-import matrix of the [ecosystem boundaries](https://github.com/Knuckles-Team/pipelines/blob/main/reference/ecosystem-boundaries.md).

**Plan check:** Does the change put facts, storage and decisions in Rust code here, with no second store?

### R-II. Every wait has a deadline

Code never blocks without a deadline. It uses a bounded wait that fails naming the missed rendezvous. Lock poisoning is an explicit decision through the shared recovery helper.

**Enforced by:** `clippy.toml` `disallowed-methods`, which the clippy gate runs with warnings denied.

**Plan check:** Does every wait and lock in the change use a bounded or explicit-recovery form?

### R-III. Every row sets budgets and names a benchmark

A requirement row states a latency budget and a memory budget and names the benchmark that measures them.

**Enforced by:** [Ecosystem boundaries](https://github.com/Knuckles-Team/pipelines/blob/main/reference/ecosystem-boundaries.md), medulla row; `.config/nextest.toml` for the test profile.

**Plan check:** Does each new row state both budgets and name its benchmark?

## Repository limits

The Rust KISS limits below replace the Python limits of the organization table for Rust code. Complexity and clone limits follow the organization table.

| Limit | Value | Source |
|---|---|---|
| Indentation depth | 5 | `.config/kiss.toml` |
| Returns per function | 8 | `.config/kiss.toml` |
| Arguments per function | 8 | `.config/kiss.toml` |
| Statements per function | 35 | `.config/kiss.toml` |
| Local variables per function | 20 | `.config/kiss.toml` |
| Calls per function | 45 | `.config/kiss.toml` |
| Methods per class | 13 | `.config/kiss.toml` |
| Imported names per file | 50 | `.config/kiss.toml` |
| Statements per file | 250 | `.config/kiss.toml` |
| Lines per file | 900 | `.config/kiss.toml` |
| Functions per file | 40 | `.config/kiss.toml` |
| Import cycle size | 0 | `.config/kiss.toml` |
| Dependency depth | 4 | `.config/kiss.toml` |

Amend this file in a reviewed change. State the impact in the pull request. When the organization constitution changes its version, update the **Inherits** line in the same change.
