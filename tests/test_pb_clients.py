"""B1.7 multi-language client drivers — Python bindings (CONCEPT:EG-KG.ingest.broker-streams-namespaces).

Covers the thin Python surface added for the Program-B engine `Method`s that had no
client binding: the native broker + append-log streams (EG-275..284/314), RBAC admin
(EG-092), online backup/restore (EG-090), and NL->query (EG-080).

Two layers:

* **Wire-shape (fake client):** verify each binding sends the exact `Method` name +
  param shape the Rust dispatch expects — especially the externally-tagged
  `RbacAdminOp` / `ResourceSelector` enums, which are easy to get wrong. No engine.
* **Live E2E (`live_client`):** drive the broker + streams + RBAC end-to-end against
  the ephemeral `--features full` engine the suite already boots (conftest), proving
  the bytes round-trip and the handlers respond. Gated on an own readiness probe so a
  slow engine boot SKIPs (never ERRORs) these tests.
"""

from __future__ import annotations

import os
import tempfile
import time
from typing import Any

import pytest
from conftest import request_context

from epistemic_graph.client import (
    AdminClient,
    BrokerClient,
    QueryClient,
    RbacClient,
    SyncEpistemicGraphClient,
)

# ─────────────────────────── Wire-shape (fake client) ───────────────────────────


class _FakeClient:
    """Records every ``(method, params, graph)`` and returns a canned per-method
    payload mimicking the engine's ``ResultPayload`` (Count/Bool/String/Raw/Json)."""

    def __init__(self) -> None:
        self.sent: list[tuple[str, dict[str, Any] | None, str | None]] = []

    async def _send(
        self,
        method: str,
        params: dict[str, Any] | None = None,
        graph: str | None = None,
    ) -> Any:
        self.sent.append((method, params, graph))
        return _CANNED.get(method)


_CANNED: dict[str, Any] = {
    "DeclareExchange": "ok",
    "DeclareQueue": "ok",
    "BindQueue": "ok",
    "Publish": 1,
    "PublishConfirmed": {"delivery_tag": 7, "confirmed": True},
    "PublishIdempotent": {"confirmed": True, "duplicate": False, "delivered": 1},
    "BrokerConsume": ["msg:1", {"body": "hi"}],
    "BrokerAck": True,
    "BrokerReject": "requeued",
    "StreamPublish": 3,
    "StreamRead": [[0, b"a"], [1, b"b"]],
    "StreamCommittedOffset": 5,
    "RbacAdmin": "grant_added",
    "Backup": {"nodes": 10, "shards": 1},
    "Restore": {
        "stage_ref": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "restored_shards": 2,
    },
    "NlQuery": [{"id": "n1"}],
}


@pytest.mark.asyncio
async def test_broker_wire_shapes() -> None:
    fake = _FakeClient()
    b = BrokerClient(fake)  # type: ignore[arg-type]

    await b.declare_exchange("events", "topic")
    await b.declare_queue("q1", dl_exchange="dlx", max_delivery_count=3, max_priority=5)
    await b.bind_queue("events", "q1", "user.*")
    await b.publish("events", "user.signup", b"payload")
    tok = await b.publish_confirmed("events", "user.x", b"p", priority=2, now_ms=100)
    idem = await b.publish_idempotent(
        "events", "user.y", b"p", producer_id="prodA", seq=1, now_ms=100
    )
    msg = await b.consume("q1", group="g", consumer="c1", now_ms=100, prefetch=1)
    await b.ack("q1", "msg:1")
    await b.reject("q1", "msg:1", requeue=True, now_ms=100)

    methods = [m for m, _, _ in fake.sent]
    assert methods == [
        "DeclareExchange",
        "DeclareQueue",
        "BindQueue",
        "Publish",
        "PublishConfirmed",
        "PublishIdempotent",
        "BrokerConsume",
        "BrokerAck",
        "BrokerReject",
    ]
    # Exact param shapes the Rust dispatch destructures.
    by = {m: p for m, p, _ in fake.sent}
    assert by["DeclareExchange"] == {"exchange": "events", "kind": "topic"}
    assert by["DeclareQueue"] == {
        "queue": "q1",
        "dl_exchange": "dlx",
        "dl_routing_key": None,
        "max_delivery_count": 3,
        "message_ttl_ms": None,
        "queue_expiry_ms": None,
        "max_priority": 5,
    }
    assert by["Publish"] == {
        "exchange": "events",
        "routing_key": "user.signup",
        "payload": b"payload",
    }
    publish_idempotent = by["PublishIdempotent"]
    assert publish_idempotent is not None
    assert publish_idempotent["producer_id"] == "prodA"
    assert publish_idempotent["seq"] == 1
    assert by["BrokerConsume"] == {
        "queue": "q1",
        "group": "g",
        "consumer": "c1",
        "now_ms": 100,
        "lease_ms": 0,
        "prefetch": 1,
    }
    # Typed returns decoded from Raw payloads.
    assert tok == {"delivery_tag": 7, "confirmed": True}
    assert idem == {"confirmed": True, "duplicate": False, "delivered": 1}
    assert msg == ("msg:1", {"body": "hi"})


@pytest.mark.asyncio
async def test_stream_wire_shapes() -> None:
    fake = _FakeClient()
    b = BrokerClient(fake)  # type: ignore[arg-type]

    await b.stream_declare("s1", max_messages=1000)
    off = await b.stream_publish("s1", b"evt", now_ms=100)
    msgs = await b.stream_read("s1", from_offset=0, max=10)
    await b.stream_commit_offset("s1", "g", 4)
    committed = await b.stream_committed_offset("s1", "g")

    by = {m: p for m, p, _ in fake.sent}
    assert by["StreamDeclare"] == {
        "stream": "s1",
        "max_messages": 1000,
        "max_age_ms": None,
    }
    assert by["StreamPublish"] == {"stream": "s1", "payload": b"evt", "now_ms": 100}
    assert by["StreamRead"] == {"stream": "s1", "from_offset": 0, "max": 10}
    assert off == 3
    assert msgs == [(0, b"a"), (1, b"b")]
    assert committed == 5


@pytest.mark.asyncio
async def test_rbac_wire_shapes() -> None:
    """The externally-tagged ``RbacAdminOp`` / ``ResourceSelector`` shapes the Rust
    ``serde`` enums expect — the highest-risk part of the binding."""
    fake = _FakeClient()
    r = RbacClient(fake)  # type: ignore[arg-type]

    await r.add_role("reader", parents=["base"])
    await r.remove_role("reader")
    await r.add_grant("reader", {"Graph": "agent:planner"}, "Read", "Allow")
    await r.add_grant("admin", "All", "Admin", "Allow")
    await r.remove_grant("reader", {"Label": "Doc"}, "Write", "Deny")
    await r.list()

    ops = []
    for _, p, _ in fake.sent:
        assert p is not None
        ops.append(p["op"])
    assert ops[0] == {"AddRole": {"name": "reader", "parents": ["base"]}}
    assert ops[1] == {"RemoveRole": "reader"}
    assert ops[2] == {
        "AddGrant": {
            "role": "reader",
            "resource": {"Graph": "agent:planner"},
            "action": "Read",
            "effect": "Allow",
        }
    }
    # A bare "All" selector is a plain string (unit variant), not a dict.
    assert ops[3]["AddGrant"]["resource"] == "All"
    assert ops[4] == {
        "RemoveGrant": {
            "role": "reader",
            "resource": {"Label": "Doc"},
            "action": "Write",
            "effect": "Deny",
        }
    }
    assert ops[5] == "List"


@pytest.mark.asyncio
async def test_admin_and_nl_wire_shapes() -> None:
    fake = _FakeClient()
    admin = AdminClient(fake)  # type: ignore[arg-type]
    q = QueryClient(fake)  # type: ignore[arg-type]

    rep = await admin.backup("scheduled-001", label="nightly")
    res = await admin.restore("scheduled-001", target_shards=2)
    rows = await q.nl_query("all agents that cite paper X", graph="agent:planner")

    by = {m: (p, g) for m, p, g in fake.sent}
    assert by["Backup"][0] == {"destination": "scheduled-001", "label": "nightly"}
    assert by["Restore"][0] == {"source": "scheduled-001", "target_shards": 2}
    # NlQuery carries the text AND the target graph in params (`Method::NlQuery`'s
    # own `graph` field -- the `/nl` HTTP facade has no request envelope, so the
    # graph must ride the method itself) as well as in the envelope.
    assert by["NlQuery"][0] == {
        "text": "all agents that cite paper X",
        "graph": "agent:planner",
    }
    assert by["NlQuery"][1] == "agent:planner"
    assert rep == {"nodes": 10, "shards": 1}
    assert res == {
        "stage_ref": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "restored_shards": 2,
    }
    assert rows == [{"id": "n1"}]


# ──────────────────────────────── Live E2E ──────────────────────────────────────
#
# These drive the real ephemeral engine the session fixture (conftest) boots with
# ``--features full``. That fixture waits a FIXED window for the UDS to appear; a cold
# ``cargo run`` link step can exceed it, so we gate the live tests on our OWN readiness
# probe and SKIP (never ERROR) if the shared engine isn't reachable — keeping the suite
# green on a slow-boot box while still exercising the full path when the engine is up.

_LIVE_GRAPH = "pb_clients_live_test"


@pytest.fixture
def live_client():
    """A connected sync client on a dedicated graph, or a clean skip if the ephemeral
    engine never came up within the readiness window."""
    socket_path = os.environ["GRAPH_SERVICE_SOCKET"]
    secret = os.environ.get("GRAPH_SERVICE_AUTH_SECRET", "")
    deadline = time.time() + 45.0
    client = None
    while time.time() < deadline:
        if os.path.exists(socket_path):
            try:
                client = SyncEpistemicGraphClient.connect(
                    socket_path=socket_path,
                    auth_secret=secret,
                    graph_name=_LIVE_GRAPH,
                    verified_context=request_context(),
                )
                client.ping()
                break
            except Exception:
                client = None
        time.sleep(0.5)
    if client is None:
        pytest.skip("ephemeral engine not reachable (conftest startup window)")
    # mypy doesn't treat pytest.skip() as NoReturn, so narrow explicitly: the skip
    # above always exits before this point when client is None.
    assert client is not None
    # A dedicated graph so broker/RBAC state never collides with other tests.
    try:
        client.tenants.create(_LIVE_GRAPH, "Agent")
    except RuntimeError:
        pass  # already exists
    return client


def test_broker_streams_live(live_client) -> None:
    """Drive the broker + streams end-to-end against the ephemeral engine: declare,
    publish, consume, ack, and replay a stream by offset."""
    c = live_client
    now = 1_000_000

    assert c.broker.declare_exchange("events", "direct") == "ok"
    assert c.broker.declare_queue("orders") == "ok"
    assert c.broker.bind_queue("events", "orders", "new") == "ok"

    delivered = c.broker.publish("events", "new", b"order-1")
    assert delivered == 1

    msg = c.broker.consume("orders", group="g1", consumer="c1", now_ms=now, prefetch=0)
    assert msg is not None
    node_id, _props = msg
    assert c.broker.ack("orders", node_id) is True
    # Queue drained.
    assert c.broker.consume("orders", group="g1", consumer="c1", now_ms=now) is None

    # Publisher confirm allocates a monotonic delivery tag.
    tok = c.broker.publish_confirmed("events", "new", b"order-2", now_ms=now)
    assert tok["confirmed"] is True
    assert isinstance(tok["delivery_tag"], int)

    # Effectively-once: the same (producer, seq) is a dropped-but-confirmed duplicate.
    first = c.broker.publish_idempotent(
        "events", "new", b"o", producer_id="p", seq=1, now_ms=now
    )
    dup = c.broker.publish_idempotent(
        "events", "new", b"o", producer_id="p", seq=1, now_ms=now
    )
    assert first["duplicate"] is False
    assert dup["duplicate"] is True

    # Append-log stream: publish, replay by offset (non-destructive), commit offset.
    assert c.broker.stream_declare("audit", max_messages=100) == "ok"
    o0 = c.broker.stream_publish("audit", b"e0", now_ms=now)
    o1 = c.broker.stream_publish("audit", b"e1", now_ms=now)
    assert o1 == o0 + 1
    read = c.broker.stream_read("audit", from_offset=0, max=10)
    assert [payload for _off, payload in read] == [b"e0", b"e1"]
    assert c.broker.stream_commit_offset("audit", "g1", o1) == "ok"
    assert c.broker.stream_committed_offset("audit", "g1") == o1


def test_rbac_live(live_client) -> None:
    """RBAC admin round-trips against the security-enabled engine: add role + grant,
    list them back, then remove."""
    c = live_client

    assert c.rbac.add_role("reader") == "role_added"
    assert c.rbac.add_grant("reader", {"Graph": c._client._graph_name}, "Read") == (
        "grant_added"
    )
    policy = c.rbac.list()
    role_names = {r.get("name") for r in policy.get("roles", [])}
    assert "reader" in role_names
    assert any(g.get("role") == "reader" for g in policy.get("grants", []))

    removed = c.rbac.remove_grant("reader", {"Graph": c._client._graph_name}, "Read")
    assert removed.get("removed") is True
    assert c.rbac.remove_role("reader") == "role_removed"


def test_backup_reaches_handler(live_client) -> None:
    """The backup binding reaches the engine handler. The suite's engine runs WITHOUT a
    persist dir, so an on-disk backup isn't available — we assert the call round-trips to
    the handler (a report dict on a redb/persist build, or the documented clean error),
    never a client-side crash."""
    c = live_client
    with tempfile.TemporaryDirectory() as d:
        dest = os.path.join(d, "bundle")
        try:
            report = c.admin.backup(dest, label="test")
            assert isinstance(report, dict)
        except RuntimeError as e:
            # Non-persist / non-redb build: a clear engine-side error, not a panic.
            assert any(
                k in str(e).lower() for k in ("backup", "redb", "persist", "available")
            )


def test_nl_query_reaches_handler(live_client) -> None:
    """NlQuery binding reaches the handler. No NL planner is configured in the test
    engine, so the engine returns the clear 'no NL planner configured' error — proving
    the wire path works without a live LLM."""
    c = live_client
    with pytest.raises(RuntimeError) as ei:
        c.query.nl_query("count all nodes")
    assert "planner" in str(ei.value).lower() or "nl" in str(ei.value).lower()
