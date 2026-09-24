"""EH-517: the Pine companion of the finance-market SuperTrend/ATR kernels.

The companion (``crates/eg-compute/tests/fixtures/finance_market/
supertrend_companion.pine``) replays the engine's integer recurrence on a
TradingView chart. TradingView is the only runtime for it, so these static
checks pin what can drift silently here: its defaults are the golden fixture's
``super_trend_10_3``, its fixed-point scale is the reference's, it states every
recurrence rule the reference implements, and it is an indicator that can never
place an order.
"""

from __future__ import annotations

import importlib.util
import json
import re
from pathlib import Path

import pytest

pytestmark = pytest.mark.no_engine

FIXTURES = (
    Path(__file__).resolve().parents[1]
    / "crates"
    / "eg-compute"
    / "tests"
    / "fixtures"
    / "finance_market"
)
PINE = (FIXTURES / "supertrend_companion.pine").read_text(encoding="utf-8")


def _reference():
    spec = importlib.util.spec_from_file_location(
        "finance_reference", FIXTURES / "reference.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _input_default(name: str) -> str:
    match = re.search(rf"^{name} = input\.\w+\(([^,]+),", PINE, re.MULTILINE)
    assert match, name
    return match.group(1).strip()


def test_defaults_are_the_golden_super_trend() -> None:
    golden = json.loads((FIXTURES / "golden.json").read_text(encoding="utf-8"))
    assert "super_trend_10_3" in golden["indicators"]
    assert int(_input_default("atrPeriod")) == 10
    assert float(_input_default("factor")) == 3.0
    assert _input_default("heikin") == "false"


def test_the_fixed_point_scale_is_the_reference_scale() -> None:
    match = re.search(r"^MILLI = (\d+)$", PINE, re.MULTILINE)
    assert match and int(match.group(1)) == _reference().MILLI


@pytest.mark.parametrize(
    "rule",
    [
        # half-away-from-zero division, the reference's div_round
        "2 * math.abs(numerator) + denominator",
        # Wilder seed then smoothing
        "divRound(seedTotal, atrPeriod)",
        "divRound(atr * (atrPeriod - 1) + trueRange, atrPeriod)",
        # band offset in thousandths of the multiplier
        "divRound(atr * mult, MILLI)",
        # bands only ratchet
        "upper < upperPrev or closingBefore > upperPrev ? upper : upperPrev",
        "lower > lowerPrev or closingBefore < lowerPrev ? lower : lowerPrev",
        # first computable bar is bearish; a flip needs a strict crossing
        "direction := -1",
        "direction == -1 and c > upper",
        "direction == 1 and c < lower",
        # Heikin-Ashi over the milli-tick candle
        "divRound(rawOpen + rawHigh + rawLow + rawClose, 4)",
    ],
)
def test_the_companion_states_each_reference_rule(rule: str) -> None:
    assert rule in PINE


def test_the_companion_is_an_indicator_that_never_trades() -> None:
    assert PINE.startswith("//@version=5\n")
    assert re.search(r"^indicator\(", PINE, re.MULTILINE)
    assert "strategy" not in re.sub(r"//.*", "", PINE)
