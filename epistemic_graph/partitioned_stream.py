"""Fixed-partition delivery log over epistemic-graph's durable native streams.

Applications keep their own message envelopes and inbox transactions. This port
owns only stable tenant partitioning and consumer cursors. A caller commits a
record *after* its application transaction succeeds; rereading an uncommitted
record is intentional and must be handled idempotently by that application.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class StreamRecord:
    """One retained native stream record and its durable offset."""

    stream: str
    partition: int
    offset: int
    payload: bytes


class _PartitionedStreamBase:
    """Shared durable layout and request validation for both EG client variants.

    ``partitions`` is part of the durable namespace layout. Keep it fixed across
    restarts; changing it requires an explicit drain/migration of old partitions.
    No retention policy is installed, since count or age trimming could discard
    messages that an application has not committed yet.
    """

    def __init__(
        self,
        broker: Any,
        *,
        namespace: str,
        partitions: int = 6,
        max_batch: int = 200,
    ) -> None:
        if not namespace or not all(
            char.isascii() and (char.isalnum() or char in "-_")
            for char in namespace
        ):
            raise ValueError(
                "namespace must contain only ASCII letters, digits, - or _"
            )
        if not 1 <= partitions <= 256:
            raise ValueError("partitions must be between 1 and 256")
        if not 1 <= max_batch <= 1000:
            raise ValueError("max_batch must be between 1 and 1000")
        self._broker = broker
        self.namespace = namespace
        self.partitions = partitions
        self.max_batch = max_batch

    @staticmethod
    def _digest(value: str) -> str:
        if not value:
            raise ValueError("tenant, route and group must be nonempty")
        return hashlib.sha256(value.encode("utf-8")).hexdigest()

    def partition_for(self, route: str) -> int:
        """Map a stable recipient or topic route to a fixed partition."""
        digest = bytes.fromhex(self._digest(route))
        return int.from_bytes(digest[:8], "big") % self.partitions

    def stream_for(self, tenant: str, partition: int) -> str:
        if not 0 <= partition < self.partitions:
            raise ValueError("partition is outside the configured range")
        # Hash the raw tenant so the durable node ID does not reveal its name.
        return f"{self.namespace}.{self._digest(tenant)}.p{partition}"

    def _append_details(
        self, tenant: str, route: str, payload: bytes, now_ms: int
    ) -> tuple[int, str]:
        if not isinstance(payload, bytes):
            raise TypeError("payload must be bytes")
        if now_ms < 0:
            raise ValueError("now_ms must be nonnegative")
        partition = self.partition_for(route)
        stream = self.stream_for(tenant, partition)
        return partition, stream

    def _read_details(
        self, tenant: str, partition: int, group: str, limit: int
    ) -> tuple[str, str]:
        if not 1 <= limit <= self.max_batch:
            raise ValueError("limit is outside the configured batch bound")
        stream = self.stream_for(tenant, partition)
        group_ref = self._digest(group)
        return stream, group_ref

    @staticmethod
    def _records(
        stream: str, partition: int, rows: list[tuple[int, bytes]]
    ) -> list[StreamRecord]:
        return [
            StreamRecord(stream, partition, int(offset), bytes(payload))
            for offset, payload in rows
        ]

    def _commit_details(
        self, tenant: str, group: str, record: StreamRecord
    ) -> str:
        if record.stream != self.stream_for(tenant, record.partition):
            raise ValueError("record does not belong to this tenant and partition")
        if record.offset < 0:
            raise ValueError("record offset must be nonnegative")
        return self._digest(group)

class PartitionedStreamLog(_PartitionedStreamBase):
    """EG-owned partitioned log over the async ``BrokerClient``."""

    async def append(
        self, tenant: str, route: str, payload: bytes, *, now_ms: int
    ) -> StreamRecord:
        """Durably append bytes and return the engine-assigned offset."""
        partition, stream = self._append_details(tenant, route, payload, now_ms)
        # Native publish creates the durable offset counter if absent. Declaring
        # a policy on every send would risk replacing an operator's retention.
        offset = await self._broker.stream_publish(stream, payload, now_ms)
        return StreamRecord(stream, partition, int(offset), payload)

    async def read(
        self, tenant: str, partition: int, group: str, *, limit: int = 100
    ) -> list[StreamRecord]:
        """Read from the first uncommitted offset, with a strict batch bound."""
        stream, group_ref = self._read_details(tenant, partition, group, limit)
        committed = await self._broker.stream_committed_offset(stream, group_ref)
        from_offset = 0 if committed is None else int(committed) + 1
        rows = await self._broker.stream_read(
            stream, from_offset=from_offset, max=limit
        )
        return self._records(stream, partition, rows)

    async def commit(self, tenant: str, group: str, record: StreamRecord) -> None:
        """Commit a processed record; the application owns its inbox transaction."""
        group_ref = self._commit_details(tenant, group, record)
        await self._broker.stream_commit_offset(record.stream, group_ref, record.offset)


class SyncPartitionedStreamLog(_PartitionedStreamBase):
    """The same EG stream contract over ``SyncEpistemicGraphClient.broker``."""

    def append(
        self, tenant: str, route: str, payload: bytes, *, now_ms: int
    ) -> StreamRecord:
        partition, stream = self._append_details(tenant, route, payload, now_ms)
        offset = self._broker.stream_publish(stream, payload, now_ms)
        return StreamRecord(stream, partition, int(offset), payload)

    def read(
        self, tenant: str, partition: int, group: str, *, limit: int = 100
    ) -> list[StreamRecord]:
        stream, group_ref = self._read_details(tenant, partition, group, limit)
        committed = self._broker.stream_committed_offset(stream, group_ref)
        from_offset = 0 if committed is None else int(committed) + 1
        rows = self._broker.stream_read(
            stream, from_offset=from_offset, max=limit
        )
        return self._records(stream, partition, rows)

    def commit(self, tenant: str, group: str, record: StreamRecord) -> None:
        group_ref = self._commit_details(tenant, group, record)
        self._broker.stream_commit_offset(record.stream, group_ref, record.offset)
