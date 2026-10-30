"""The Python client reaches the native point-in-time liquidity operation."""

import pytest

from epistemic_graph.client import EngineResponseError

DAY = 86_400_000


def observation(series, day, value, source):
    return {
        "series": series,
        "observation_ms": day * DAY,
        "vintage_ms": (day + 1) * DAY,
        "observed_ms": (day + 1) * DAY + 1,
        "value_milliunits": value,
        "source_ref": source,
    }


def request():
    return {
        "version": 1,
        "as_of_ms": 12 * DAY,
        "max_age_ms": 10 * DAY,
        "observations": [
            observation("walcl", 9, 6_000_000_000, "fred:walcl"),
            observation("walcl", 2, 5_900_000_000, "fred:walcl-prior"),
            observation("wdtgal", 9, 1_000_000_000, "fred:tga"),
            observation("rrpontsyd", 10, 200_000, "fred:rrp"),
        ],
    }


def test_liquidity_panel_preserves_exact_units_and_sources(clean_graph):
    panel = clean_graph.finance.market("liquidity_panel", request=request())
    assert panel["version"] == 1
    assert panel["max_age_ms"] == 10 * DAY
    assert panel["net_liquidity_usd"] == 4_800_000_000_000
    assert panel["heat"][0]["direction"] == "increasing"
    assert panel["heat"][0]["previous_source_ref"] == "fred:walcl-prior"
    assert panel["heat"][2]["contribution_usd"] == -200_000_000_000


def test_liquidity_panel_refuses_a_future_capture(clean_graph):
    bad = request()
    bad["observations"][0]["observed_ms"] = bad["as_of_ms"] + 1
    with pytest.raises(EngineResponseError, match="future"):
        clean_graph.finance.market("liquidity_panel", request=bad)


@pytest.mark.no_engine
def test_generated_liquidity_request_contract():
    from epistemic_graph.generated.models import MethodFinanceMarketParams

    parsed = MethodFinanceMarketParams.model_validate(
        {"op": {"op": "liquidity_panel", "request": request()}}
    )
    assert parsed.model_dump(mode="json")["op"]["op"] == "liquidity_panel"
