"""The engine's method-body vectors (``contract/fixtures/method_body_vectors.json``),
shared by the vector replay tests and the strict-model tests."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import msgpack

FIXTURE = (
    Path(__file__).parents[1] / "contract" / "fixtures" / "method_body_vectors.json"
)
VECTORS: list[dict[str, Any]] = json.loads(FIXTURE.read_text(encoding="utf-8"))[
    "vectors"
]


def vector_request(vector: dict[str, Any]) -> dict[str, Any]:
    """The request document one vector's msgpack bytes decode to."""
    request = msgpack.unpackb(
        bytes.fromhex(vector["request_msgpack"]), raw=False, strict_map_key=False
    )
    assert isinstance(request, dict)
    return request
