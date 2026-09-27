"""Self-model pointer contract over a caller-bound graph read."""

import pytest

from epistemic_graph.self_model_read import (
    read_current_self_model,
    read_previous_self_model,
)


def _model(node_id: str = "sm:1") -> dict:
    return {"id": node_id, "node_type": "memory_retriever", "version": 1}


def test_current_uses_bound_parameter_and_returns_one_model() -> None:
    calls: list[tuple[str, dict]] = []

    def read(query: str, params: dict) -> list[dict]:
        calls.append((query, params))
        return [{"sm": _model()}]

    assert read_current_self_model(read) == _model()
    assert calls[0][1] == {"anchor_id": "self:agent-model"}
    assert "CURRENT_SELF_MODEL" in calls[0][0]
    assert "LIMIT 2" in calls[0][0]


def test_missing_pointer_is_none_and_ambiguous_pointer_fails() -> None:
    assert read_current_self_model(lambda _q, _p: []) is None
    with pytest.raises(ValueError, match="ambiguous"):
        read_current_self_model(
            lambda _q, _p: [{"sm": _model()}, {"sm": _model("sm:2")}]
        )


def test_rejects_malformed_or_wrong_type_target() -> None:
    with pytest.raises(ValueError, match="malformed"):
        read_current_self_model(lambda _q, _p: [{"sm": {"id": "sm:1"}}])
    with pytest.raises(ValueError, match="malformed"):
        read_current_self_model(
            lambda _q, _p: [{"sm": {**_model(), "node_type": "Secret"}}]
        )


def test_previous_rejects_self_cycle_and_propagates_read_denial() -> None:
    with pytest.raises(ValueError, match="cannot supersede itself"):
        read_previous_self_model(lambda _q, _p: [{"prev": _model()}], node_id="sm:1")

    def denied(_query: str, _params: dict) -> list[dict]:
        raise PermissionError("denied")

    with pytest.raises(PermissionError, match="denied"):
        read_previous_self_model(denied, node_id="sm:2")
