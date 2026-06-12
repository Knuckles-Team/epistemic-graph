// CONCEPT:KG-2.22 — Compute Modules
//
// Core compute primitives. `semantic` (HNSW-backed embedding search) is the only
// member and carries no heavy deps, so it lives in `eg-core` alongside the graph.

pub mod semantic;
