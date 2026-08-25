"""Plan 01 / Plan 02 regression tests: PyO3 excision, length-prefixed framing
binary-safety, and pure-Python quant correctness.

These run without a live server so they are deterministic in CI.
"""

from __future__ import annotations

import struct
import subprocess
from pathlib import Path

import msgpack
import pytest

# Pure/static test -- never needs the shared native engine (see
# conftest.py's session-scoped `start_epistemic_graph_server` fixture,
# which this marker exempts this module from triggering).
pytestmark = pytest.mark.no_engine

REPO = Path(__file__).resolve().parents[1]


def test_no_pyo3_gate_passes():
    """The binary-aware no-PyO3 gate must pass (source + .so + maturin)."""
    script = REPO / "scripts" / "check_no_pyo3.sh"
    result = subprocess.run(
        ["bash", str(script)], capture_output=True, text=True, cwd=REPO
    )
    assert result.returncode == 0, f"no-pyo3 gate failed:\n{result.stdout}\n{result.stderr}"


def test_no_compiled_extension_ships():
    """No stale `_epistemic_graph*.so` may sit in the package dir."""
    pkg = REPO / "epistemic_graph"
    leftovers = list(pkg.glob("_epistemic_graph*.so")) + list(
        pkg.glob("_epistemic_graph*.pyi")
    )
    assert not leftovers, f"PyO3 binary vestiges present: {leftovers}"


def test_quant_imports_without_pyo3():
    """quant.py must import with no compiled extension present."""
    from epistemic_graph import quant  # noqa: F401

    assert quant.moving_average([1, 2, 3, 4], 2) == [1.0, 1.5, 2.5, 3.5]
    ema = quant.exponential_moving_average([1.0, 2.0, 3.0], 0.5)
    assert ema == [1.0, 1.5, 2.25]


def test_quant_rolling_variance_and_zscore():
    from epistemic_graph import quant

    var = quant.rolling_variance([2.0, 4.0, 6.0], 3)
    assert var[0] == 0.0  # single-element window -> 0
    assert var[2] == pytest.approx(4.0)  # sample variance of [2,4,6]
    z = quant.rolling_zscore([1.0, 1.0, 1.0], 3)
    assert z == [0.0, 0.0, 0.0]  # zero variance -> 0, no div-by-zero


def test_quant_order_matching_crosses_book():
    from epistemic_graph import quant

    bids, asks, trades = quant.simulate_order_matching(
        bids=[(99.0, 5.0)],
        asks=[(100.0, 3.0), (101.0, 4.0)],
        price=100.5,
        volume=4.0,
        is_buy=True,
    )
    # Buys 3@100 (fully) then stops at 101 (above limit 100.5).
    assert trades == [(100.0, 3.0)]
    assert asks == [(101.0, 4.0)]
    assert bids == [(99.0, 5.0)]


def test_length_prefixed_framing_is_binary_safe():
    """The headline correctness fix: a MessagePack payload containing 0x0A
    (newline) bytes must round-trip intact under length-prefixed framing.

    Newline framing would have truncated this payload at the first 0x0A.
    """
    request = {
        "id": 7,
        "method": "AddNode",
        # Embed raw 0x0A bytes inside binary properties.
        "params": {"blob": b"\x0a\x0a\xde\xad\x0a\xbe\xef\x0a"},
    }
    body = msgpack.packb(request, use_bin_type=True)
    assert b"\x0a" in body, "payload must actually contain newline bytes"

    # Frame: 4-byte big-endian length prefix + body.
    framed = struct.pack(">I", len(body)) + body

    # Decode the way the server/client do.
    (n,) = struct.unpack(">I", framed[:4])
    decoded = msgpack.unpackb(framed[4 : 4 + n], raw=False)

    assert decoded["params"]["blob"] == b"\x0a\x0a\xde\xad\x0a\xbe\xef\x0a"
    assert decoded["id"] == 7


# ── group_relative_advantage (RL reward kernel, CONCEPT:EG-KG.domains.quant-finance) ───────────


def test_group_relative_advantage_grpo_and_dr_grpo():
    from epistemic_graph.quant import group_relative_advantage

    # GRPO: (r−μ)/σ — symmetric, mean maps to 0.
    adv = group_relative_advantage([1.0, 2.0, 3.0])
    assert adv[1] == 0.0
    assert adv[0] == pytest.approx(-adv[2])
    # Dr.GRPO length_unbiased: centered (r−μ), no /σ division.
    assert group_relative_advantage([1.0, 2.0, 3.0], length_unbiased=True) == [
        -1.0,
        0.0,
        1.0,
    ]
    # degenerate groups → zeros
    assert group_relative_advantage([]) == []
    assert group_relative_advantage([5.0]) == [0.0]
    assert group_relative_advantage([2.0, 2.0, 2.0]) == [0.0, 0.0, 0.0]


# (cross-package parity with agent-utilities' batch_normalized_advantage is asserted
# from the agent-utilities side — tests/test_training_signals.py — where both packages
# import cleanly; agent-utilities depends on epistemic-graph, not the reverse.)
