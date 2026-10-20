"""R801.7 strict client receipt validation; native persistence tested separately."""

from copy import deepcopy

import pytest

from epistemic_graph._run_usage import validate_committed_run_usage

pytestmark = pytest.mark.no_engine
RECEIPT = {
    "tenant": "tenant",
    "run_id": "run",
    "context_id": "context",
    "principal": "owner",
    "counts": {"input_tokens": 0, "output_tokens": 9_007_199_254_740_991},
}


def check(value: object) -> None:
    validate_committed_run_usage(
        value, tenant="tenant", run_id="run", context_id="context"
    )


# spec: EG-DECISION-ENGINE-R801.7
def test_observed_zero_and_safe_bound_preserved_without_synthesizing_counts() -> None:
    value = deepcopy(RECEIPT)
    check(value)
    assert value == RECEIPT
    assert "total_tokens" not in value["counts"]


# spec: EG-DECISION-ENGINE-R801.7
@pytest.mark.parametrize("field", ["tenant", "run_id", "context_id"])
def test_foreign_attribution_rejected(field: str) -> None:
    value = deepcopy(RECEIPT)
    value[field] = "foreign"
    with pytest.raises(RuntimeError, match="attribution"):
        check(value)


# spec: EG-DECISION-ENGINE-R801.7
@pytest.mark.parametrize(
    "counts",
    [
        {},
        {"input_tokens": True},
        {"input_tokens": -1},
        {"input_tokens": 9_007_199_254_740_992},
        {"input_tokens": 1.5},
        {"cost": 10},
        None,
    ],
)
def test_invalid_or_inferred_counts_rejected(counts: object) -> None:
    value = {**RECEIPT, "counts": counts}
    with pytest.raises(RuntimeError):
        check(value)


# spec: EG-DECISION-ENGINE-R801.7
@pytest.mark.parametrize(
    "value", [None, {}, {**RECEIPT, "principal": ""}, {**RECEIPT, "extra": 1}]
)
def test_malformed_receipt_rejected(value: object) -> None:
    with pytest.raises(RuntimeError):
        check(value)
