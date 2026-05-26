# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [0.1.0] — 2026-05-24

### Added
- Initial Rust `epistemic-graph` engine implementation using `petgraph` stable graph.
- PyO3-based Python native extension bindings.
- DFS-based cycle detection returning exact cycle paths.
- BFS-based shortest path search and blast radius calculator.
- Applied ecosystem package standards including pre-commit, bumpversion, gitattributes, codespell, and pytest suite.
- Multi-stage testing Dockerfile and compose layout.
