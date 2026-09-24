"""End-to-end round trips of ``FinanceMarket`` (EH-413..EH-418).

Full path: Python client -> UDS -> Rust dispatch -> result, over the
session-scoped server + ``clean_graph`` sync client from conftest.py. Bars are
stored in the engine's own time-series store in the bar-series layout, read
back, decoded and resolved as of a point in time; the same bars then drive the
indicators, the trend signal, the scanner, the calibrated confidence and a
sealed backtest-run record. Everything here is informational only.
"""

from __future__ import annotations

from typing import Any

DAY = 86_400_000_000_000
START = 19_000 * DAY
FIELDS = [
    "close_time_hi",
    "close_time_lo",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "status",
    "revision",
    "known_at_hi",
    "known_at_lo",
]
SPEC = {
    "version": 1,
    "kind": {
        "kind": "super_trend",
        "atr_period": 3,
        "multiplier_milli": 1_000,
        "basis": "raw",
    },
}
SERIES = {
    "listing_id": "test:BTC/USD:spot",
    "price_basis": "trade",
    "timeframe": {"unit": "day"},
    "calendar_id": "utc-24x7",
}


def _bar(index: int, close: int, revision: int = 0, delay: int = 0) -> dict[str, Any]:
    open_time = START + index * DAY
    return {
        "open_time": open_time,
        "close_time": open_time + DAY,
        "open": close,
        "high": close + 50,
        "low": close - 50,
        "close": close,
        "volume": 10,
        "status": "final",
        "revision": revision,
        "known_at": open_time + DAY + delay,
    }


def _bars() -> list[dict[str, Any]]:
    closes = [1_000, 1_010, 990, 1_000, 1_200, 1_400, 1_600, 1_300, 900, 700, 650]
    return [_bar(i, close) for i, close in enumerate(closes)]


def test_bars_round_trip_through_the_time_series_store(clean_graph):
    bars = _bars()
    correction = _bar(4, 1_150, revision=1, delay=5 * DAY)
    points = clean_graph.finance.market("encode_points", records=[*bars, correction])
    assert all(len(point["values"]) == len(FIELDS) for point in points)
    clean_graph.timeseries.append(
        "bars-test",
        [(point["ts"], point["values"]) for point in points],
        field_names=FIELDS,
        bucket_ns=30 * DAY,
    )
    stored = clean_graph.timeseries.range("bars-test", START, START + 20 * DAY)
    assert len(stored) == len(bars) + 1
    records = clean_graph.finance.market(
        "decode_points", points=[{"ts": ts, "values": values} for ts, values in stored]
    )
    then = clean_graph.finance.market(
        "resolve", records=records, as_of=START + 6 * DAY, finality="final_only"
    )
    now = clean_graph.finance.market(
        "resolve", records=records, as_of=None, finality="final_only"
    )
    assert then[4]["close"] == 1_200
    assert now[4]["close"] == 1_150 and now[4]["revision"] == 1


def test_signal_replay_scan_and_advance_agree(clean_graph):
    bars = _bars()
    replayed = clean_graph.finance.market(
        "signal_replay",
        request={"series": SERIES, "spec": SPEC, "records": bars},
    )
    state = replayed["state"]
    assert state["data_status"] == "valid"
    flips = replayed["current"]
    assert [flip["to"] for flip in flips] == ["bullish", "bearish"]
    assert all(record["status"] == "emitted" for record in replayed["records"])
    head = clean_graph.finance.market(
        "signal_replay",
        request={"series": SERIES, "spec": SPEC, "records": bars[:6]},
    )["state"]
    advanced = clean_graph.finance.market("signal_advance", state=head, bars=bars[6:])
    assert advanced["state"] == state
    page = clean_graph.finance.market(
        "signal_scan", request={"states": [head, state], "limit": 10}
    )
    assert page["counts"]["total"] == 1 and page["superseded"] == 1
    assert page["rows"][0]["direction"] == "bearish"


def test_indicators_rollup_confidence_and_backtest_record(clean_graph):
    bars = _bars()
    sma = clean_graph.finance.market(
        "indicators",
        bars=bars,
        spec={"version": 1, "kind": {"kind": "sma", "period": 2}},
    )
    assert sma[0]["value"] == {"kind": "warming"}
    assert sma[1]["value"] == {"kind": "line", "value": 1_005_000}
    weeks = clean_graph.finance.market(
        "rollup",
        bars=bars,
        calendar={"kind": "utc24x7"},
        timeframe={"unit": "week"},
        watermark=START + 30 * DAY,
    )
    assert sum(week["volume"] for week in weeks) == 10 * len(bars)
    abstained = clean_graph.finance.market(
        "flip_confidence",
        request={
            "indicator_version": "super_trend@1",
            "timeframe": {"unit": "day"},
            "asset_class": "crypto",
            "horizon_bars": 5,
            "direction": "bullish",
            "features": {
                "timeframe_agreement": True,
                "above_200w_sma": False,
                "regime": 0,
            },
            "data_status": "valid",
            "history": [],
            "alpha_permille": 100,
            "n_min": 10,
        },
    )
    assert abstained["outcome"] == "abstained"
    assert abstained["reason"]["reason"] == "insufficient_history"
    run = clean_graph.finance.market("backtest_run", draft=_draft())
    assert run["informational_only"] is True
    assert run["digest"].startswith("sha256:")
    assert run["validation"]["cpcv_splits"] == 15
    again = clean_graph.finance.market("backtest_run", draft=_draft())
    assert again == run


def _draft() -> dict[str, Any]:
    returns = [((i * 37) % 11 - 4) / 1000 for i in range(48)]
    return {
        "strategy": "super_trend@1 long-only",
        "signal_keys": ["sha256:" + "0" * 64],
        "data_revisions": [
            {
                "series_id": "bars-test",
                "source_revision": "sha256:" + "1" * 64,
                "known_as_of": START + 20 * DAY,
            }
        ],
        "universe": [{"listing_id": "test:BTC/USD:spot", "from": 0, "until": None}],
        "costs": {"fee_bps": 10, "slippage_bps": 5},
        "fill_rule": "next_bar_open",
        "fills": [
            {
                "listing_id": "test:BTC/USD:spot",
                "known_at": START + 5 * DAY,
                "fill_at": START + 5 * DAY,
                "fill_price": 1_400,
                "direction": "bullish",
            }
        ],
        "returns": returns,
        "validation": {
            "n_groups": 6,
            "n_test_groups": 2,
            "purge_window": 2,
            "embargo": 1,
            "n_trials": 5,
            "insample": [[0.4, 0.1, 0.3], [0.2, 0.5, 0.1], [0.3, 0.2, 0.6]],
            "oos": [[0.1, 0.3, 0.2], [0.4, 0.1, 0.2], [0.2, 0.4, 0.1]],
        },
        "supersedes": None,
    }
