"""Fixed-partition delivery log over epistemic-graph's durable native streams.

Applications keep their own message envelopes and inbox transactions. This port
owns only stable tenant partitioning and consumer cursors. A caller commits a
record *after* its application transaction succeeds; rereading an uncommitted
record is intentional and must be handled idempotently by that application.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class StreamRecord:
    """One retained native stream record and its durable offset."""

    stream: str
    partition: int
    offset: int
    payload: bytes


def delivery_depth_from_stats(value: Any) -> int:
    """Normalize native stream queue metrics to a nonnegative backpressure depth."""

    def sum_numeric(item: Any) -> int:
        if isinstance(item, dict):
            return sum(sum_numeric(child) for child in item.values())
        if isinstance(item, list | tuple):
            return sum(sum_numeric(child) for child in item)
        try:
            return max(0, int(item))
        except (TypeError, ValueError):
            return 0

    if isinstance(value, dict):
        queues = value.get("queues")
        if isinstance(queues, dict):
            return sum(sum_numeric(item) for item in queues.values())
        depths = [
            delivery_depth_from_stats(item)
            for key, item in value.items()
            if key.lower() in {"depth", "queue_depth", "ready", "messages", "lag"}
            or isinstance(item, dict | list | tuple)
        ]
        return max(depths, default=0)
    if isinstance(value, list | tuple):
        return max((delivery_depth_from_stats(item) for item in value), default=0)
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return 0


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
            char.isascii() and (char.isalnum() or char in "-_") for char in namespace
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

    def _commit_details(self, tenant: str, group: str, record: StreamRecord) -> str:
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
        rows = self._broker.stream_read(stream, from_offset=from_offset, max=limit)
        return self._records(stream, partition, rows)

    def commit(self, tenant: str, group: str, record: StreamRecord) -> None:
        group_ref = self._commit_details(tenant, group, record)
        self._broker.stream_commit_offset(record.stream, group_ref, record.offset)


class SyncMessageDeliveryLog:
    """EG-owned bounded materializer, cursor and digest-only poison queue.

    The application supplies its envelope decoder and performs its own inbox
    transaction before calling :meth:`ack`. A failed transaction leaves the
    cursor unchanged so the same record is replayed.
    """

    def __init__(
        self,
        broker: Any,
        *,
        namespace: str,
        group: str,
        partitions: int = 6,
    ) -> None:
        self._log = SyncPartitionedStreamLog(
            broker, namespace=namespace, partitions=partitions
        )
        self._dlq = SyncPartitionedStreamLog(
            broker, namespace=f"{namespace}_dlq", partitions=partitions
        )
        self.group = group
        self.partitions = partitions

    def append(
        self, tenant: str, route: str, payload: bytes, *, now_ms: int
    ) -> StreamRecord:
        return self._log.append(tenant, route, payload, now_ms=now_ms)

    def receive(
        self,
        tenant: str,
        decoder: Callable[[bytes], dict[str, Any] | None],
        *,
        max_messages: int = 200,
    ) -> list[tuple[dict[str, Any], StreamRecord]]:
        remaining = max(
            0, min(int(max_messages), self.partitions * self._log.max_batch)
        )
        messages: list[tuple[dict[str, Any], StreamRecord]] = []
        for partition in range(self.partitions):
            if remaining == 0:
                break
            quota = min(
                self._log.max_batch,
                max(
                    1,
                    (remaining + self.partitions - partition - 1)
                    // (self.partitions - partition),
                ),
            )
            for record in self._log.read(tenant, partition, self.group, limit=quota):
                envelope = decoder(record.payload)
                if envelope is None:
                    self._dead_letter(tenant, record, "decode_error")
                    self._log.commit(tenant, self.group, record)
                    continue
                messages.append((envelope, record))
                remaining -= 1
        # Never reorder within a partition: a later committed offset must not
        # skip an earlier uncommitted record.
        return messages

    def _dead_letter(self, tenant: str, record: StreamRecord, reason: str) -> None:
        diagnostic = json.dumps(
            {
                "stream": record.stream,
                "offset": record.offset,
                "sha256": hashlib.sha256(record.payload).hexdigest(),
                "bytes": len(record.payload),
                "reason": reason,
            },
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        self._dlq.append(tenant, "poison", diagnostic, now_ms=0)

    def ack(self, tenant: str, record: StreamRecord) -> None:
        self._log.commit(tenant, self.group, record)

    def nack(self, tenant: str, record: StreamRecord, *, requeue: bool = True) -> None:
        if (
            record.stream != self._log.stream_for(tenant, record.partition)
            or record.offset < 0
        ):
            raise ValueError("record does not belong to this tenant and partition")
        if not requeue:
            self._dead_letter(tenant, record, "rejected_envelope")
            self._log.commit(tenant, self.group, record)

    def read_dlq(self, tenant: str, *, max_messages: int = 50) -> list[dict[str, Any]]:
        if max_messages <= 0:
            return []
        rows: list[dict[str, Any]] = []
        for partition in range(self.partitions):
            for record in self._dlq.read(
                tenant,
                partition,
                "delivery-dlq-inspection",
                limit=min(max_messages, self._dlq.max_batch),
            ):
                rows.append(json.loads(record.payload))
                if len(rows) >= max_messages:
                    return rows
        return rows
