"""Fixed-partition native stream contracts for application delivery logs."""

from __future__ import annotations

import asyncio
import inspect

import pytest

from epistemic_graph.client import BrokerClient
from epistemic_graph.partitioned_stream import (
    PartitionedStreamLog,
    SyncMessageDeliveryLog,
    SyncPartitionedStreamLog,
)

pytestmark = pytest.mark.no_engine


class FakeBroker:
    def __init__(self) -> None:
        self.rows: dict[str, list[tuple[int, bytes]]] = {}
        self.cursors: dict[tuple[str, str], int] = {}
        self.read_calls: list[tuple[str, int, int]] = []

    def stream_publish(self, stream: str, payload: bytes, now_ms: int) -> int:
        assert now_ms >= 0
        rows = self.rows.setdefault(stream, [])
        offset = len(rows)
        rows.append((offset, payload))
        return offset

    def stream_committed_offset(self, stream: str, group: str) -> int | None:
        return self.cursors.get((stream, group))

    def stream_read(
        self, stream: str, *, from_offset: int, max: int
    ) -> list[tuple[int, bytes]]:
        self.read_calls.append((stream, from_offset, max))
        return [row for row in self.rows.get(stream, []) if row[0] >= from_offset][:max]

    def stream_commit_offset(self, stream: str, group: str, offset: int) -> str:
        self.cursors[(stream, group)] = offset
        return "ok"


class FakeAsyncBroker:
    def __init__(self) -> None:
        self.sync = FakeBroker()

    async def stream_publish(self, stream: str, payload: bytes, now_ms: int) -> int:
        return self.sync.stream_publish(stream, payload, now_ms)

    async def stream_committed_offset(self, stream: str, group: str) -> int | None:
        return self.sync.stream_committed_offset(stream, group)

    async def stream_read(
        self, stream: str, *, from_offset: int, max: int
    ) -> list[tuple[int, bytes]]:
        return self.sync.stream_read(stream, from_offset=from_offset, max=max)

    async def stream_commit_offset(self, stream: str, group: str, offset: int) -> str:
        return self.sync.stream_commit_offset(stream, group, offset)


def test_async_port_matches_real_broker_client_contract() -> None:
    for name in (
        "stream_publish",
        "stream_read",
        "stream_committed_offset",
        "stream_commit_offset",
    ):
        assert inspect.iscoroutinefunction(getattr(BrokerClient, name))

    async def roundtrip() -> None:
        log = PartitionedStreamLog(FakeAsyncBroker(), namespace="agent_bus")
        sent = await log.append("tenant", "route", b"message", now_ms=1)
        assert await log.read("tenant", sent.partition, "group") == [sent]
        await log.commit("tenant", "group", sent)
        assert await log.read("tenant", sent.partition, "group") == []

    asyncio.run(roundtrip())


def test_uncommitted_messages_replay_and_committed_cursor_resumes() -> None:
    broker = FakeBroker()
    log = SyncPartitionedStreamLog(broker, namespace="agent_bus", partitions=6)
    sent = log.append("tenant-a", "recipient-a", b"message", now_ms=1)
    first = log.read("tenant-a", sent.partition, "materializer")
    assert first == [sent]
    assert log.read("tenant-a", sent.partition, "materializer") == [sent]
    log.commit("tenant-a", "materializer", first[0])
    assert log.read("tenant-a", sent.partition, "materializer") == []
    assert broker.read_calls[-1][1:] == (sent.offset + 1, 100)


def test_tenant_and_group_cursors_are_isolated() -> None:
    broker = FakeBroker()
    log = SyncPartitionedStreamLog(broker, namespace="agent_bus")
    a = log.append("tenant-a", "route", b"a", now_ms=1)
    b = log.append("tenant-b", "route", b"b", now_ms=1)
    assert a.stream != b.stream
    assert b"tenant-a" not in a.stream.encode()
    log.commit("tenant-a", "group-1", a)
    assert log.read("tenant-a", a.partition, "group-1") == []
    assert log.read("tenant-a", a.partition, "group-2") == [a]
    assert log.read("tenant-b", b.partition, "group-1") == [b]


def test_fixed_partition_and_read_bound() -> None:
    broker = FakeBroker()
    log = SyncPartitionedStreamLog(broker, namespace="agent_bus", max_batch=2)
    rows = [log.append("t", "same-route", bytes([n]), now_ms=n) for n in range(3)]
    assert {row.stream for row in rows} == {rows[0].stream}
    assert log.read("t", rows[0].partition, "g", limit=2) == rows[:2]
    assert broker.read_calls[-1][2] == 2


def test_reject_cross_tenant_cursor_commit() -> None:
    log = SyncPartitionedStreamLog(FakeBroker(), namespace="agent_bus")
    record = log.append("tenant-a", "route", b"x", now_ms=1)
    with pytest.raises(ValueError, match="does not belong"):
        log.commit("tenant-b", "group", record)


def test_delivery_materializer_commits_only_after_ack_and_poison_dlq() -> None:
    broker = FakeBroker()
    delivery = SyncMessageDeliveryLog(
        broker, namespace="agent_bus", group="inbox", partitions=4
    )
    delivery.append("tenant", "route", b"secret poison", now_ms=1)
    assert delivery.receive("tenant", lambda _raw: None) == []
    dlq = delivery.read_dlq("tenant")
    assert len(dlq) == 1 and dlq[0]["reason"] == "decode_error"
    assert "secret" not in str(dlq)

    delivery.append("tenant", "route", b"valid", now_ms=2)

    def decoder(raw: bytes) -> dict[str, str]:
        return {"payload": raw.decode()}

    first = delivery.receive("tenant", decoder)
    assert first == delivery.receive("tenant", decoder)
    assert first[0][0] == {"payload": "valid"}
    delivery.nack("tenant", first[0][1], requeue=True)
    assert first == delivery.receive("tenant", decoder)
    delivery.ack("tenant", first[0][1])
    assert delivery.receive("tenant", decoder) == []
    assert delivery.read_dlq("tenant", max_messages=0) == []


def test_delivery_nack_rejects_foreign_record_before_dlq_write() -> None:
    delivery = SyncMessageDeliveryLog(
        FakeBroker(), namespace="agent_bus", group="inbox"
    )
    record = delivery.append("tenant-a", "route", b"private", now_ms=1)
    with pytest.raises(ValueError, match="does not belong"):
        delivery.nack("tenant-b", record, requeue=False)
    assert delivery.read_dlq("tenant-b") == []
