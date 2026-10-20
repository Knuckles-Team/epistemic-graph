"""Validate optional native usage readback without inferring missing counts."""

from typing import Any

_FIELDS = {"run_id", "context_id", "tenant", "principal", "counts"}
_COUNTS = {"requests", "tool_calls", "input_tokens", "output_tokens", "total_tokens"}


def validate_committed_run_usage(
    value: Any, *, tenant: str, run_id: str, context_id: Any
) -> None:
    if not isinstance(value, dict) or set(value) != _FIELDS:
        raise RuntimeError("native committed usage receipt is malformed")
    for field in _FIELDS - {"counts"}:
        identity = value[field]
        if not isinstance(identity, str) or not identity.strip() or len(identity) > 512:
            raise RuntimeError("native committed usage identity is malformed")
    if (
        value["tenant"] != tenant
        or value["run_id"] != run_id
        or value["context_id"] != context_id
    ):
        raise RuntimeError("native committed usage attribution mismatch")
    counts = value["counts"]
    if not isinstance(counts, dict) or not counts or not set(counts) <= _COUNTS:
        raise RuntimeError("native committed usage counts are malformed")
    if any(
        type(count) is not int or not 0 <= count <= 9_007_199_254_740_991
        for count in counts.values()
    ):
        raise RuntimeError("native committed usage count is invalid")
