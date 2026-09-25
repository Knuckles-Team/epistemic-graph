"""Client display of the engine's code and separate human-readable detail."""

import pytest

from epistemic_graph.client import ResultTooLargeError, _raise_send_error

pytestmark = pytest.mark.no_engine


def test_declared_error_detail_is_visible_without_replacing_code() -> None:
    with pytest.raises(RuntimeError) as error:
        _raise_send_error(
            {
                "error": "AUTH_TENANT_MISMATCH",
                "error_detail": "request context tenant does not match graph tenant",
            }
        )
    assert str(error.value) == (
        "AUTH_TENANT_MISMATCH: request context tenant does not match graph tenant"
    )


def test_typed_result_too_large_error_preserves_detail() -> None:
    with pytest.raises(
        ResultTooLargeError, match="RESULT_TOO_LARGE: bounded result cap"
    ):
        _raise_send_error(
            {"error": "RESULT_TOO_LARGE", "error_detail": "bounded result cap"}
        )
