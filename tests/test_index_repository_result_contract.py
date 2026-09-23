"""Typed public-client contract for one bounded IndexRepository batch."""

from __future__ import annotations

from typing import Any

import pytest
from _client_fixtures import RecordingTransport, SentCall
from pydantic import ValidationError

from epistemic_graph.client import GraphOperationsClient
from epistemic_graph.generated.index_repository import IndexFileStatus, IndexResult

_ZERO_DIGEST = "sha256:" + ("0" * 64)
_ONE_DIGEST = "sha256:" + ("1" * 64)


def _result_payload() -> dict[str, Any]:
    return {
        "nodes": [],
        "edges": [],
        "symbols_extracted": 0,
        "files_parsed": 1,
        "file_outcomes": [
            {
                "file_path": "src/main.py",
                "status": "success",
                "content_digest": _ZERO_DIGEST,
                "parser_capability_digest": _ONE_DIGEST,
                "diagnostics": [],
            },
            {
                "file_path": "README.txt",
                "status": "unsupported",
                "content_digest": _ONE_DIGEST,
                "parser_capability_digest": _ZERO_DIGEST,
                "diagnostics": [
                    {
                        "code": "unsupported_extension",
                        "message": "Unsupported file extension",
                    }
                ],
            },
        ],
        "calls_resolved": 0,
        "calls_unresolved": 0,
        "calls_scope_resolved": 0,
        "calls_type_resolved": 0,
        "inherits_edges": 0,
        "realizes_edges": 0,
        "similar_edges": 0,
        "imports_resolved": 0,
        "imports_unresolved": 0,
    }


class _Client(RecordingTransport):
    def reply(self, call: SentCall) -> dict[str, Any]:
        assert call.graph is None
        assert call.idempotency_key is None
        return _result_payload()


@pytest.mark.asyncio
@pytest.mark.no_engine
async def test_public_client_returns_one_typed_outcome_per_file_in_order() -> None:
    client: Any = _Client()
    result = await GraphOperationsClient(client).index_repository(
        [("src/main.py", b""), ("README.txt", b"")]
    )

    assert isinstance(result, IndexResult)
    assert [item.file_path for item in result.file_outcomes] == [
        "src/main.py",
        "README.txt",
    ]
    assert [item.status for item in result.file_outcomes] == [
        IndexFileStatus.SUCCESS,
        IndexFileStatus.UNSUPPORTED,
    ]
    assert [call.method for call in client.sent] == ["IndexRepository"]


@pytest.mark.no_engine
def test_diagnostics_bound_is_enforced_by_generated_type() -> None:
    payload = _result_payload()
    payload["file_outcomes"][0]["diagnostics"] = [
        {"code": f"error_{index}", "message": "bounded"} for index in range(9)
    ]

    with pytest.raises(ValidationError):
        IndexResult.model_validate(payload)
