#!/usr/bin/env python3
"""Static gate for bounded, generation-safe lazy graph lifecycle invariants."""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def read(relative: str) -> str:
    return (ROOT / relative).read_text(encoding="utf-8")


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(f"lazy lifecycle architecture gate failed: {message}")


def main() -> None:
    registry = read("crates/eg-core/src/registry.rs")
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
    require(
        "prior_snapshot != page.source_snapshot_version" in registry,
        "paged source-version drift is not fenced",
    )

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
