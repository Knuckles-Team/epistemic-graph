"""RDF namespace wire-contract tests."""

from __future__ import annotations

from typing import Any

import pytest

from epistemic_graph.client import RdfClient


class _FakeClient:
    def __init__(self, result: Any) -> None:
        self.result = result
        self.sent: list[tuple[str, dict[str, Any] | None]] = []

    async def _send(self, method: str, params: dict[str, Any] | None = None) -> Any:
        self.sent.append((method, params))
        return self.result


@pytest.mark.asyncio
async def test_validate_shacl_sends_both_inline_graphs() -> None:
    report = {"conforms": True, "results": []}
    fake = _FakeClient(report)
    rdf = RdfClient(fake)  # type: ignore[arg-type]

    result = await rdf.validate_shacl("shapes", "data")

    assert result == report
    assert fake.sent == [
        ("ShaclValidate", {"shapes": "shapes", "data_graph": "data"})
    ]
