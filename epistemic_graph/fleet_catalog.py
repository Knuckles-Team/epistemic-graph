"""Typed fleet catalog client (EH-345).

The Rust wire contract owns every shape here: discovery observations, operator
overrides and the typed rows the engine projects from the server registry and
connector-pack ``AgentComponent`` records.  This module composes the generated
DTOs and per-operation senders into one ergonomic seam; it holds no catalog of
its own and never falls back to a second store.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from ._transport import EngineTransport
from .generated.cluster import (
    send_fleet_catalog_clear_override,
    send_fleet_catalog_list,
    send_fleet_catalog_lookup,
    send_fleet_catalog_record_discovery,
    send_fleet_catalog_set_override,
)
from .generated.fleet_catalog import (
    FleetCatalogCursor,
    FleetCatalogKind,
    FleetCatalogListRequest,
    FleetCatalogLookup,
    FleetCatalogLookupRequest,
    FleetCatalogPage,
    FleetDiscoveryRecordRequest,
    FleetOverrideClearRequest,
    FleetOverrideSetRequest,
    FleetWriteReceipt,
)

__all__ = ["FleetCatalogClient", "FleetCatalogSnapshotError"]


class FleetCatalogSnapshotError(RuntimeError):
    """An exhaustive read saw the snapshot change or the engine repeat itself.

    The caller restarts the read; a partial or mixed snapshot is never returned.
    """


def fleet_row_id(row: Any) -> str:
    """The stable id of one projected row: a discovery id or a component id."""
    body = row.row
    component = getattr(body, "component", None)
    return str(component.id if component is not None else body.id)


class FleetCatalogClient:
    """Typed async facade over the generated ``FleetCatalog`` senders.

    Writes are compare-and-set: ``expected_revision=None`` writes against the
    revision the engine reads, ``0`` requires that no record exists, ``n``
    requires revision ``n``.  A byte-identical repeat answers ``replayed``.
    Tenant and principal are bound by the engine from the verified request
    context; no request carries either.
    """

    def __init__(self, client: EngineTransport) -> None:
        self._client = client

    async def record_discovery(
        self, request: FleetDiscoveryRecordRequest
    ) -> FleetWriteReceipt:
        """Record one server's latest observation under one discovery scope."""
        return await send_fleet_catalog_record_discovery(self._client, request)

    async def set_override(self, request: FleetOverrideSetRequest) -> FleetWriteReceipt:
        """Durably override one field of a published component (admin)."""
        return await send_fleet_catalog_set_override(self._client, request)

    async def clear_override(
        self, request: FleetOverrideClearRequest
    ) -> FleetWriteReceipt:
        """Clear one durable override (admin). A tombstone revision, not a delete."""
        return await send_fleet_catalog_clear_override(self._client, request)

    async def page(self, request: FleetCatalogListRequest) -> FleetCatalogPage:
        """Read one bounded page, fenced to the snapshot digest it was cut from."""
        return await send_fleet_catalog_list(self._client, request)

    async def lookup(
        self, ids: Iterable[str], *, grant_digests: Iterable[str] = ()
    ) -> FleetCatalogLookup:
        """The visible rows for ``ids``; unknown and invisible ids are absent."""
        request = FleetCatalogLookupRequest(
            ids=list(ids), grant_digests=list(grant_digests)
        )
        return await send_fleet_catalog_lookup(self._client, request)

    async def list_all(
        self,
        kind: FleetCatalogKind | str,
        *,
        query: str | None = None,
        grant_digests: Iterable[str] = (),
        page_size: int | None = None,
    ) -> tuple[Any, ...]:
        """Read one kind's visible snapshot to exhaustion.

        Every page must carry the same snapshot digest and total; a cursor may
        not repeat and a row may not appear twice.  Any of those means the
        snapshot moved, and the read raises rather than splice two catalogs.
        """
        digests = list(grant_digests)
        cursor: FleetCatalogCursor | None = None
        identity: tuple[str, int] | None = None
        seen_ids: set[str] = set()
        seen_cursors: set[tuple[str, str]] = set()
        rows: list[Any] = []
        while True:
            page = await self.page(
                FleetCatalogListRequest(
                    kind=FleetCatalogKind(kind),
                    query=query,
                    grant_digests=digests,
                    limit=page_size,
                    cursor=cursor,
                )
            )
            identity = _check_identity(identity, page)
            _append_unique(rows, seen_ids, page.rows)
            cursor = page.next_cursor
            if cursor is None:
                return _complete(rows, page.total)
            _check_cursor(seen_cursors, cursor)


def _check_identity(
    expected: tuple[str, int] | None, page: FleetCatalogPage
) -> tuple[str, int]:
    identity = (str(page.snapshot_digest), int(page.total))
    if expected is not None and identity != expected:
        raise FleetCatalogSnapshotError("fleet catalog snapshot changed during read")
    return identity


def _append_unique(rows: list[Any], seen_ids: set[str], page_rows: Any) -> None:
    for row in page_rows:
        row_id = fleet_row_id(row)
        if row_id in seen_ids:
            raise FleetCatalogSnapshotError(f"fleet catalog repeated row {row_id!r}")
        seen_ids.add(row_id)
        rows.append(row)


def _check_cursor(seen: set[tuple[str, str]], cursor: FleetCatalogCursor) -> None:
    key = (cursor.after_name, cursor.after_id)
    if key in seen:
        raise FleetCatalogSnapshotError("fleet catalog pagination repeated a cursor")
    seen.add(key)


def _complete(rows: list[Any], total: int) -> tuple[Any, ...]:
    if len(rows) != total:
        raise FleetCatalogSnapshotError(
            f"fleet catalog returned {len(rows)} rows for a total of {total}"
        )
    return tuple(rows)
