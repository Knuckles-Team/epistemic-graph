"""The Python signer's ``eg2.`` body equals the server's for every method.

The engine MACs ``Method::canonical_body_bytes()`` of the request it DECODED:
Rust declaration order, serde defaults materialized, maps sorted, f32 and byte
widths per field. ``contract/fixtures/method_body_vectors.json`` is rendered by
``gen_contract`` from that exact decoder and encoder, one client-shaped request
per catalog method (alphabetical keys, defaults omitted -- the pydantic sender
shape) plus every typed contract sample. Replaying each request through the
client's own signer must reproduce the server's bytes; a signer that restates
field order or defaults instead of delegating to eg-types fails here by name.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import msgpack
import pytest

from epistemic_graph.client import _canonical_method_body
from epistemic_graph.generated import METHOD_IDS
from epistemic_graph.generated.agent_component import (
    AgentComponentContentRequest,
    AgentComponentKind,
    AgentComponentOpCurrent,
    AgentComponentSearchRequest,
)
from epistemic_graph.generated.storage import (
    send_agent_component_content,
    send_agent_component_current,
    send_agent_component_search,
)

pytestmark = pytest.mark.no_engine

_FIXTURE = (
    Path(__file__).parents[1] / "contract" / "fixtures" / "method_body_vectors.json"
)
_VECTORS: list[dict[str, Any]] = json.loads(_FIXTURE.read_text(encoding="utf-8"))[
    "vectors"
]


def _request(vector: dict[str, Any]) -> dict[str, Any]:
    request = msgpack.unpackb(
        bytes.fromhex(vector["request_msgpack"]), raw=False, strict_map_key=False
    )
    assert isinstance(request, dict)
    return request


def test_vectors_cover_every_published_method() -> None:
    covered = {vector["method"] for vector in _VECTORS}
    assert METHOD_IDS - covered == set()


@pytest.mark.parametrize(
    "vector", _VECTORS, ids=[vector["label"] for vector in _VECTORS]
)
def test_signer_body_is_the_server_canonical_body(vector: dict[str, Any]) -> None:
    request = _request(vector)
    body = _canonical_method_body(request["method"], request.get("params"))
    assert len(body) == vector["canonical_len"]
    assert hashlib.sha256(body).hexdigest() == vector["canonical_sha256"]


class _Capture:
    """Records what a generated sender hands to ``_send``."""

    def __init__(self) -> None:
        self.params: dict[str, Any] | None = None

    async def _send(
        self,
        method: str,
        params: dict[str, Any] | None,
        graph: str | None,
        *,
        idempotency_key: str | None = None,
    ) -> Any:
        del method, graph, idempotency_key
        self.params = params
        raise _Sent


class _Sent(Exception):
    """Stops a sender after it produced its wire params."""


async def _sent_params(sender: Any, request: Any) -> dict[str, Any]:
    capture = _Capture()
    with pytest.raises(_Sent):
        await sender(capture, request)
    assert capture.params is not None
    return capture.params


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("sender", "request_model", "explicit"),
    [
        (
            send_agent_component_search,
            AgentComponentSearchRequest(
                tenant_id="tenant-a", kinds=[AgentComponentKind.SKILL], limit=10
            ),
            {
                "op": "search",
                "request": {
                    "tenant_id": "tenant-a",
                    "task": None,
                    "capabilities": [],
                    "kinds": ["skill"],
                    "read_only": False,
                    "limit": 10,
                    "cursor": None,
                },
            },
        ),
        (
            send_agent_component_current,
            AgentComponentOpCurrent(
                op="current", tenant_id="tenant-a", component_id="skill:a"
            ),
            {"op": "current", "tenant_id": "tenant-a", "component_id": "skill:a"},
        ),
        (
            send_agent_component_content,
            AgentComponentContentRequest(tenant_id="tenant-a", component_id="skill:a"),
            {
                "op": "content",
                "request": {
                    "tenant_id": "tenant-a",
                    "component_id": "skill:a",
                    "entry_revision": None,
                },
            },
        ),
    ],
)
async def test_typed_agent_component_senders_sign_the_explicit_request_body(
    sender: Any, request_model: Any, explicit: dict[str, Any]
) -> None:
    """au-core R5: these senders drop defaults and use pydantic key order; the
    request with every field explicit in declaration order is the one the
    engine accepted. Both must sign the same canonical body."""
    params = await _sent_params(sender, request_model)
    assert _canonical_method_body("AgentComponent", params) == (
        _canonical_method_body("AgentComponent", {"op": explicit})
    )
