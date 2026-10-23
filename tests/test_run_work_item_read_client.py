"""Owner-only immutable native run admission recovery client contract."""

from __future__ import annotations

import asyncio
import copy
from typing import Any, cast

import pytest
from _client_send_stub import CapturingEngine
from test_work_item_read_client import VIEW
from test_work_item_retry_identity import _submit_request

from epistemic_graph.client import EpistemicGraphClient, WorkItemClient

pytestmark = pytest.mark.no_engine


def admission() -> dict[str, Any]:
    request = _submit_request()
    request.update(
        run_id="run",
        work_item_id=None,
        metadata={"premises": [{"class": "observed", "ref": "fact:1"}]},
    )
    return {"work_item": copy.deepcopy(VIEW), "admission": request}


def read(engine: CapturingEngine) -> dict[str, Any] | None:
    client = WorkItemClient(cast(EpistemicGraphClient, engine))
    return asyncio.run(client.get_run(tenant="tenant-fixture", run_id="run"))


def test_owner_run_read_preserves_exact_admission() -> None:
    value = admission()
    engine = CapturingEngine(value)
    result = read(engine)
    assert result == value
    assert type(result["admission"]["context"]["authentication_method"]) is str
    assert result["admission"]["metadata"]["premises"][0]["class"] == "observed"
    assert engine.sent == [
        ("GetRunWorkItem", {"tenant": "tenant-fixture", "run_id": "run"})
    ]


def test_owner_run_read_returns_none_without_fallback() -> None:
    engine = CapturingEngine(None)
    assert read(engine) is None
    assert len(engine.sent) == 1


@pytest.mark.parametrize("target", ["work_item", "admission"])
def test_owner_run_read_refuses_lease_authority_fields(target: str) -> None:
    value = admission()
    value[target]["fencing_token"] = 42
    with pytest.raises(RuntimeError):
        read(CapturingEngine(value))


@pytest.mark.parametrize(
    "field,value", [("run_id", "foreign"), ("work_item_id", "chosen")]
)
def test_owner_run_read_refuses_wrong_binding(field: str, value: str) -> None:
    result = admission()
    result["admission"][field] = value
    with pytest.raises(RuntimeError):
        read(CapturingEngine(result))
