# epistemic-graph

[![GitHub Repo stars](https://img.shields.io/github/stars/Knuckles-Team/epistemic-graph)](https://github.com/Knuckles-Team/epistemic-graph/stargazers)
[![GitHub forks](https://img.shields.io/github/forks/Knuckles-Team/epistemic-graph)](https://github.com/Knuckles-Team/epistemic-graph/forks)
[![GitHub contributors](https://img.shields.io/github/contributors/Knuckles-Team/epistemic-graph)](https://github.com/Knuckles-Team/epistemic-graph/graphs/contributors)
[![GitHub license](https://img.shields.io/github/license/Knuckles-Team/epistemic-graph)](LICENSE)
[![GitHub last commit (by committer)](https://img.shields.io/github/last-commit/Knuckles-Team/epistemic-graph)](https://github.com/Knuckles-Team/epistemic-graph/commits/main)
[![GitHub pull requests](https://img.shields.io/github/issues-pr/Knuckles-Team/epistemic-graph)](https://github.com/Knuckles-Team/epistemic-graph/pulls)
[![GitHub closed pull requests](https://img.shields.io/github/issues-pr-closed/Knuckles-Team/epistemic-graph)](https://github.com/Knuckles-Team/epistemic-graph/pulls?q=is%3Apr+is%3Aclosed)
[![GitHub issues](https://img.shields.io/github/issues/Knuckles-Team/epistemic-graph)](https://github.com/Knuckles-Team/epistemic-graph/issues)
[![GitHub top language](https://img.shields.io/github/languages/top/Knuckles-Team/epistemic-graph)](https://github.com/Knuckles-Team/epistemic-graph)
[![GitHub language count](https://img.shields.io/github/languages/count/Knuckles-Team/epistemic-graph)](https://github.com/Knuckles-Team/epistemic-graph)
[![GitHub repo size](https://img.shields.io/github/repo-size/Knuckles-Team/epistemic-graph)](https://github.com/Knuckles-Team/epistemic-graph)
[![GitHub repo file count (file type)](https://img.shields.io/github/directory-file-count/Knuckles-Team/epistemic-graph)](https://github.com/Knuckles-Team/epistemic-graph)
[![PyPI - Version](https://img.shields.io/pypi/v/epistemic-graph)](https://pypi.org/project/epistemic-graph/)
[![PyPI - Downloads](https://img.shields.io/pypi/dd/epistemic-graph)](https://pypi.org/project/epistemic-graph/)
[![PyPI - License](https://img.shields.io/pypi/l/epistemic-graph)](https://pypi.org/project/epistemic-graph/)
[![PyPI - Wheel](https://img.shields.io/pypi/wheel/epistemic-graph)](https://pypi.org/project/epistemic-graph/)
[![PyPI - Implementation](https://img.shields.io/pypi/implementation/epistemic-graph)](https://pypi.org/project/epistemic-graph/)
<img src="https://img.shields.io/badge/version-2.27.0-blue" alt="Version">
[![Build](https://github.com/Knuckles-Team/epistemic-graph/actions/workflows/release.yml/badge.svg)](https://github.com/Knuckles-Team/epistemic-graph/actions/workflows/release.yml)
[![Documentation](https://github.com/Knuckles-Team/epistemic-graph/actions/workflows/pages.yml/badge.svg)](https://knuckles-team.github.io/epistemic-graph/)

## Overview

epistemic-graph is a Rust-native durable database and compute engine for connected knowledge and multimodal data. Use it on its own, or as the storage and reasoning authority behind agent-utilities and GraphOS.

## Key Capabilities

- Store graph, SQL, RDF/OWL, vector, time-series, text, and blob data under one durable authority.
- Query and compute across data families through a unified planner.
- Keep evidence, provenance, and temporal context with the records they describe.
- Apply authenticated access and tenant isolation, with optional cluster replication.

## Documentation

The [Epistemic Graph documentation](https://knuckles-team.github.io/epistemic-graph/) covers installation, service configuration, interfaces, architecture, and operations. The [capability matrix](docs/capabilities.md) lists supported operations.

## Architecture

Client libraries and database protocols use the same authenticated Rust runtime, planner, and transaction boundary. The engine owns durable data and reasoning; agent orchestration and connector execution remain in their respective projects.

## Quick Start

Install the Python package. Configure the required authentication and persistence settings from the [service guide](https://knuckles-team.github.io/epistemic-graph/service_mode/) before starting the server.

```bash
uvx --from epistemic-graph epistemic-graph-server
```

## Contributing

Issues and pull requests are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) for project guidelines.

## License

MIT — see [LICENSE](LICENSE).
