# Forward roadmap — genuinely deferred

> This is an **internal, contributor-facing** page, not a headline doc. The authoritative,
> operation-by-operation status lives in the **[capability matrix](capabilities.md)**; every shipped
> capability has a deep-dive under [Query Surfaces](interfaces/sql.md) or
> [Analytics & Distribution](architecture/analytics_program.md), and the authoritative `CONCEPT:EG-*`
> definitions are in [concepts](concepts.md). The historical "Universal-DB Program" backlog
> (EG-045..345 — SQL/SPARQL/OWL/Cypher/GraphQL parity, multi-wire adapters, broker, observability, GIS,
> tensors, streams, KV-cache, agent-memory, the LTAP lakehouse, real ANN pushdown, GDS, PL/pgSQL, SQLite
> `.db` I/O, raster pyramids, the numeric kernel + Surface-B analytics UDFs, Calvin, ROS2/DDS) is
> **shipped** — see the capability matrix and `CHANGELOG.md`.

What remains is a short list of genuinely-deferred items. Each is folded, as a note, into the deep-dive
that owns it.

| Item | Status | Owning deep-dive |
|------|:------:|------------------|
| **Admin console UI** — browser surface for tenants/shards/RBAC/backup-PITR (the engine exposes the APIs; the UI is unbuilt) | 🗺 | [Operations Runbook](operations/runbook.md) |
| **Live dashboards UI** — a Grafana-style front-end over the shipped PromQL/logs/traces query APIs (the query side ships; the UI does not) | 🗺 | [Observability](interfaces/observability.md) |
| **Memory → weights distillation** — distilling consolidated agent-memory into a fine-tune/LoRA export, beyond retrieval-time context assembly | 🗺 | [Agent Memory](interfaces/memory.md) |
| **GPU offload beyond distance/elementwise** — reasoning / ANN-build kernels on the GPU (the distance + elementwise CUDA kernels ship and auto-validate on any GPU host) | 🔶 | [Distribution / Robotics / GPU](architecture/distribution_robotics_gpu.md) |
| **Full CycloneDDS-C `rmw` leg** — zero-config live-`ros2` interop via `rmw` topic-name/type-hash mangling (the rosbridge-WS bridge + the pure-Rust `rustdds` RTPS leg ship; the C-toolchain `rmw` leg stays a documented, gated option) | 🗺 | [Distribution / Robotics / GPU](architecture/distribution_robotics_gpu.md) |
| **Calvin multi-node epoch routing** — routing a restarted OLLP txn into a specific epoch of the multi-node sequencer fan-in (single-sequencer OLLP + recon-restart ship) | 🔶 | [Distribution / Robotics / GPU](architecture/distribution_robotics_gpu.md) |
| **Numeric Surface-B — graph/timeseries unification** — bringing graph-algo + timeseries ops under the `eg-numeric` kernel via native `Method` surfaces (the vector/stat/linalg UDFs + cross-modal analytics ship) | 🗺 | [Analytics Program](architecture/analytics_program.md) · [Numeric Kernel](architecture/numeric_kernel.md) |
| **Numeric migration P2/P3/P5** — the *agent-utilities-side* swap of its 598 numpy sites to the `xp` shim and the eventual numpy/scipy drop (the kernel wheel now publishes to PyPI, so this is a downstream dependency swap) | 🗺 | [Numeric Kernel](architecture/numeric_kernel.md) |

Legend: **🔶 in-progress** · **🗺 designed, not started**.
