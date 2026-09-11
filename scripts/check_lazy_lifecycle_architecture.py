#!/usr/bin/env python3
"""Static gate for bounded, generation-safe lazy graph lifecycle invariants."""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(Path(__file__).resolve().parent))

from rust_module_tree import read_module_tree  # noqa: E402


def read(relative: str) -> str:
    """The compiler-declared production source of a Rust module, not one file.

    Every assertion below names a behaviour a *module* must carry. Reading only
    the facade file made each one silently stale the moment that module was
    decomposed into `<name>.rs` + `<name>/**` -- which is exactly what happened
    to `src/server/dispatch.rs`: `PARTIAL_MATERIALIZATION`, `graph_lifecycle`
    and `index_manifests` all still exist, they just moved into
    `dispatch/graph_pipeline.rs` and `dispatch/router/*.rs`. The module walker
    follows the `mod`/`include!` declarations the compiler follows, so a
    decomposition no longer reads as a missing invariant, and the negative
    assertions below now cover the whole module instead of its facade. The
    test-inclusive view is deliberate: it is the exact superset of the single
    file this gate used to read, so repointing adds the module's declared
    children without silently narrowing any existing assertion.
    """

    return read_module_tree(relative, root_dir=ROOT, include_tests=True)


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(f"lazy lifecycle architecture gate failed: {message}")


def require_registry_source_version_fence(registry: str) -> None:
    """Require the registry's source-version drift check."""

    if "snapshot_changed(" in registry:
        require(
            "snapshot_changed(manifest_ref, &page)" in registry,
            "lazy page application no longer calls the source-version fence",
        )
        require(
            "prior != page.source_snapshot_version" in registry,
            "snapshot_changed lost its source-version drift body",
        )
    else:
        require(
            "prior_snapshot != page.source_snapshot_version" in registry,
            "paged source-version drift is not fenced",
        )


def require_registry_contract(registry: str) -> None:
    """Require the generation and partial-materialization registry fences."""
    for token in (
        "incarnation_id",
        "LazyOpenTicket",
        "is_current_handle",
        "AtomicBool",
        "MaterializationManifest",
        "MaterializationPhase::Partial",
        "source_snapshot_version",
        "completeness_cursor",
        "apply_lazy_page_to_handle",
    ):
        require(token in registry, f"registry is missing {token}")
    require(
        "record.cancellation.store(true" in registry,
        "delete does not cancel in-flight incarnation work",
    )
    require_registry_source_version_fence(registry)


def main() -> None:
    registry = read("crates/eg-core/src/registry.rs")
    require_registry_contract(registry)

    lifecycle = read("src/server/persistence/cold_offload.rs")
    for token in (
        "DEFAULT_PRODUCTION_MAX_RESIDENT_GRAPHS",
        "DEFAULT_PRODUCTION_LAZY_OPEN_PAGE_SIZE",
        "mutation_batch::lock_graph",
        "spawn_blocking",
        "apply_lazy_page_to_handle",
    ):
        require(token in lifecycle, f"server lifecycle path is missing {token}")
    require(
        "KNOWN RESIDUAL RACE" not in lifecycle,
        "same-name stale-page race is still documented as unresolved",
    )

    index = read("crates/eg-core/src/index.rs")
    for field in (
        "source_snapshot_version",
        "build_version",
        "IndexCompletenessCursor",
        "IndexValidity",
        "rebuild_server_indexes",
        "server_manifests",
    ):
        require(field in index, f"maintained index manifest is missing {field}")

    served = read("src/server/secondary_indexes.rs")
    # Coverage enforcement moved from the version-only `covers(version)` to
    # `covers_source(version, nodes, edges)`, which ALSO fails closed on
    # node/edge cursor drift. This gate tracks that stronger invariant, and
    # additionally refuses a regression back to the deprecated weaker call --
    # a served read must never re-acquire the ability to look "covered" while
    # its source cursors have drifted.
    require(
        "covers_source(source_snapshot_version" in served,
        "text/spatial availability does not enforce snapshot coverage",
    )
    require(
        "manifest().covers(" not in served,
        "served availability uses the deprecated version-only covers(); it must "
        "use covers_source(), which also fails closed on node/edge cursor drift",
    )
    require("ix.clear()" in served, "text recovery does not remove stale documents")

    dispatch = read("src/server/dispatch.rs")
    require(
        '"PARTIAL_MATERIALIZATION"' in dispatch,
        "partial whole-graph reads are not explicit",
    )
    require(
        '"graph_lifecycle"' in dispatch and '"index_manifests"' in dispatch,
        "health/list responses omit lifecycle or index watermarks",
    )

    durable = read("src/redb_store.rs")
    require(
        "encode_meta_with_incarnation" in durable,
        "durable graph metadata omits immutable incarnation identity",
    )
    require(
        "source_snapshot_version" in durable,
        "durable page reads omit source snapshot version",
    )

    print("lazy lifecycle architecture gate passed")


if __name__ == "__main__":
    main()
