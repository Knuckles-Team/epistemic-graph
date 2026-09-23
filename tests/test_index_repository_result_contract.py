"""Typed public-client contract for one bounded IndexRepository batch."""

from __future__ import annotations

from typing import Any

import pytest
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


class _Client:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any] | None]] = []
        self.graphs: list[str | None] = []

    async def _send(
        self,
        method: str,
        params: dict[str, Any] | None,
        graph: str | None,
        *,
        idempotency_key: str | None,
    ) -> dict[str, Any]:
        self.calls.append((method, params))
        self.graphs.append(graph)
        assert idempotency_key is None
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
    assert [method for method, _ in client.calls] == ["IndexRepository"]
    assert client.graphs == [None]


@pytest.mark.no_engine
def test_diagnostics_bound_is_enforced_by_generated_type() -> None:
    payload = _result_payload()
    payload["file_outcomes"][0]["diagnostics"] = [
        {"code": f"error_{index}", "message": "bounded"} for index in range(9)
    ]

    with pytest.raises(ValidationError):
        IndexResult.model_validate(payload)


@pytest.mark.asyncio
@pytest.mark.no_engine
async def test_branch_aware_scope_travels_as_the_typed_wire_field() -> None:
    from epistemic_graph.generated.index_repository import IndexRepositoryScope

    scope = IndexRepositoryScope.model_validate(
        {
            "repository_id": "local-git:team/project",
            "refs": [
                {
                    "ref_name": "refs/heads/main",
                    "revision_id": "a" * 40,
                    "status": "live",
                }
            ],
            "file_versions": [
                {
                    "ref_name": "refs/heads/main",
                    "path": "src/main.py",
                    "blob_digest": _ZERO_DIGEST,
                }
            ],
        }
    )
    client: Any = _Client()
    await GraphOperationsClient(client).index_repository(
        [("src/main.py", b""), ("README.txt", b"")], scope=scope, graph="repositories"
    )

    params = client.calls[0][1]
    assert params["scope"] == {
        "repository_id": "local-git:team/project",
        "refs": [
            {"ref_name": "refs/heads/main", "revision_id": "a" * 40, "status": "live"}
        ],
        "tombstones": [],
        "file_versions": [
            {
                "ref_name": "refs/heads/main",
                "path": "src/main.py",
                "blob_digest": _ZERO_DIGEST,
            }
        ],
    }
    assert client.graphs == ["repositories"]
