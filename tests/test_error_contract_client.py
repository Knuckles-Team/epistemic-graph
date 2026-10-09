"""Client display of the engine's code and separate human-readable detail."""

import pytest

from epistemic_graph import EngineResponseError
from epistemic_graph.client import (
    ResultTooLargeError,
    StaleRouteError,
    _raise_send_error,
)

pytestmark = pytest.mark.no_engine


@pytest.mark.spec("EG-TYPED-PACKS-R087")
def test_declared_error_detail_is_visible_without_replacing_code() -> None:
    with pytest.raises(EngineResponseError) as error:
        _raise_send_error(
            {
                "error": "AUTH_TENANT_MISMATCH",
                "error_detail": "request context tenant does not match graph tenant",
            }
        )
    assert str(error.value) == (
        "AUTH_TENANT_MISMATCH: request context tenant does not match graph tenant"
    )
    assert error.value.code == "AUTH_TENANT_MISMATCH"
    assert error.value.detail == "request context tenant does not match graph tenant"


def test_typed_result_too_large_error_preserves_detail() -> None:
    with pytest.raises(
        ResultTooLargeError, match="RESULT_TOO_LARGE: bounded result cap"
    ) as error:
        _raise_send_error(
            {"error": "RESULT_TOO_LARGE", "error_detail": "bounded result cap"}
        )
    assert error.value.code == "RESULT_TOO_LARGE"
    assert error.value.detail == "bounded result cap"


def test_error_code_is_taken_from_wire_without_message_parsing() -> None:
    with pytest.raises(EngineResponseError) as error:
        _raise_send_error(
            {"error": "ACCESS_DENIED", "error_detail": "secret: INTERNAL"}
        )
    assert error.value.code == "ACCESS_DENIED"
    assert error.value.detail == "secret: INTERNAL"

    with pytest.raises(EngineResponseError) as legacy:
        _raise_send_error({"error": "RESULT_TOO_LARGE: legacy detail"})
    assert type(legacy.value) is EngineResponseError
    assert legacy.value.code == "RESULT_TOO_LARGE: legacy detail"
    assert legacy.value.detail is None


def test_malformed_error_code_is_not_promoted_to_engine_refusal() -> None:
    with pytest.raises(
        RuntimeError, match="invalid engine response error code"
    ) as error:
        _raise_send_error({"error": {"code": "ACCESS_DENIED"}})
    assert not isinstance(error.value, EngineResponseError)


def test_structured_redirect_keeps_wire_code_and_route() -> None:
    operation = {
        "schema_version": "1",
        "operation_id": "request:7",
        "status": "redirected",
        "result_kind": None,
        "result_ref": None,
        "error": None,
        "redirect": {
            "kind": "placement",
            "target_ref": "tenant:graph",
            "group": 5,
            "epoch": 12,
            "fencing_token": 501012,
            "leader_ref": "node:2",
        },
    }
    with pytest.raises(StaleRouteError) as error:
        _raise_send_error({"error": "OPERATION_REDIRECTED", "result": operation})
    assert isinstance(error.value, EngineResponseError)
    assert error.value.code == "OPERATION_REDIRECTED"
    assert error.value.detail is None
    assert error.value.target_ref == "tenant:graph"
    assert error.value.group == 5
