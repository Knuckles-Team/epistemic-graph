"""Current-only wire contracts used by native A2A delegation."""

from __future__ import annotations

from typing import Any

import msgpack
import pytest

from epistemic_graph.client import BrokerClient, NodeClient

# Fake-client unit tests only -- never needs the shared native engine (see
# conftest.py's session-scoped `start_epistemic_graph_server` fixture,
# which this marker exempts this module from triggering).
pytestmark = pytest.mark.no_engine


class _FakeClient:
    def __init__(self) -> None:
        self.sent: list[tuple[str, dict[str, Any] | None]] = []

    async def _send(self, method: str, params: dict[str, Any] | None = None) -> Any:
        self.sent.append((method, params))
        return {
            "CreateNodeIfAbsent": True,
            "BrokerAckTag": False,
            "BrokerNackTag": "absent",
            "BrokerRenewTag": True,
        }[method]


@pytest.mark.asyncio
async def test_create_if_absent_uses_one_native_atomic_operation() -> None:
    fake = _FakeClient()
    nodes = NodeClient(fake)  # type: ignore[arg-type]

    assert await nodes.create_if_absent("work:1", {"status": "pending"}) is True
    method, params = fake.sent.pop()
    assert method == "CreateNodeIfAbsent"
    assert params is not None
    assert params["node_id"] == "work:1"
    assert msgpack.unpackb(bytes(params["properties_msgpack"]), raw=False) == {
        "status": "pending"
    }


@pytest.mark.asyncio
async def test_tag_operations_always_carry_owner_and_explicit_clock() -> None:
    fake = _FakeClient()
    broker = BrokerClient(fake)  # type: ignore[arg-type]

    assert await broker.ack_tag(7, consumer="worker-a") is False
    assert (
        await broker.nack_tag(8, consumer="worker-b", requeue=True, now_ms=1_000)
        == "absent"
    )
    assert (
        await broker.renew_tag(9, consumer="worker-c", now_ms=1_100, lease_ms=500)
        is True
    )
    assert fake.sent == [
        ("BrokerAckTag", {"delivery_tag": 7, "consumer": "worker-a"}),
        (
            "BrokerNackTag",
            {
                "delivery_tag": 8,
                "consumer": "worker-b",
                "requeue": True,
                "now_ms": 1_000,
            },
        ),
        (
            "BrokerRenewTag",
            {
                "delivery_tag": 9,
                "consumer": "worker-c",
                "now_ms": 1_100,
                "lease_ms": 500,
            },
        ),
    ]
