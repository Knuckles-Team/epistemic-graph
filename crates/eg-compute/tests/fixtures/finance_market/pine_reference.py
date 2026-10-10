#!/usr/bin/env python3
"""Pine-semantics float reference: SMA, EMA, 200-week SMA, 20/21 band, Heikin-Ashi.

``reference.py`` re-implements the engine's *integer* recurrences bit for bit;
this script is the other half of EG-FINANCE-PRIMITIVES-R016: it evaluates the
same indicators with TradingView Pine v5 built-in semantics in IEEE floats, in
price units, with no engine rounding, so the Rust parity test proves the
integer kernels stay within a stated milli-tick tolerance of Pine:

* ``ta.sma(close, n)``: ``na`` for the first ``n - 1`` bars, then the window mean.
* ``ta.ema(close, n)``: ``alpha = 2 / (n + 1)``; ``na`` for the first ``n - 1``
  bars, seeded on bar ``n`` by ``ta.sma(close, n)`` (the same seeding Pine
  documents for ``ta.rma``), then ``alpha * close + (1 - alpha) * ema[1]``.
* Heikin-Ashi (``ticker.heikinashi``): ``haClose = (o + h + l + c) / 4``,
  ``haOpen = na(haOpen[1]) ? (o + c) / 2 : (haOpen[1] + haClose[1]) / 2``,
  ``haHigh = max(h, haOpen, haClose)``, ``haLow = min(l, haOpen, haClose)``.
* The 200-week SMA is ``ta.sma(close, 200)`` on the weekly series; the 20/21
  band is ``ta.sma(close, 20)`` with ``ta.ema(close, 21)`` on the weekly series.

The bar series are generated here (deterministic, no RNG) and written into the
fixture, so the Rust test reads bars and Pine values from one file. ATR and
SuperTrend Pine parity live in ``supertrend_companion.pine``.

Run: ``python3 pine_reference.py`` (rewrites ``pine_parity.json`` beside this file).
"""

from __future__ import annotations

import json
from pathlib import Path

DAILY_BARS = 60
WEEKLY_BARS = 230


def series(count: int, base: int, swing: int) -> list[list[int]]:
    """Integer-tick OHLC bars: deterministic zig-zag drift, gaps and a flat run."""
    out: list[list[int]] = []
    close = base
    for i in range(count):
        open_ = close + ((i * 37) % 11 - 5) * (swing // 20) if i % 9 == 8 else close
        move = ((i * 7919) % 23 - 11) * swing // 10 + (i % 5) * 3
        if 30 <= i < 34:
            close_ = high = low = open_
        else:
            close_ = max(10, open_ + move)
            high = max(open_, close_) + (i * 13) % (swing // 2 + 1)
            low = max(1, min(open_, close_) - (i * 17) % (swing // 2 + 1))
        out.append([open_, high, low, close_])
        close = close_
    return out


def sma(closes: list[float], n: int) -> list[float | None]:
    return [
        None if i + 1 < n else sum(closes[i + 1 - n : i + 1]) / n
        for i in range(len(closes))
    ]


def ema(closes: list[float], n: int) -> list[float | None]:
    alpha = 2.0 / (n + 1)
    out: list[float | None] = []
    prev: float | None = None
    for i, x in enumerate(closes):
        if i + 1 < n:
            out.append(None)
            continue
        prev = sum(closes[:n]) / n if prev is None else alpha * x + (1 - alpha) * prev
        out.append(prev)
    return out


def heikin_ashi(bars: list[list[int]]) -> list[list[float]]:
    out: list[list[float]] = []
    ha_open: float | None = None
    ha_close: float | None = None
    for o, h, lo, c in bars:
        close = (o + h + lo + c) / 4.0
        if ha_open is None or ha_close is None:
            open_ = (o + c) / 2.0
        else:
            open_ = (ha_open + ha_close) / 2.0
        out.append([open_, max(h, open_, close), min(lo, open_, close), close])
        ha_open, ha_close = open_, close
    return out


def main() -> None:
    daily = series(DAILY_BARS, 50_000, 800)
    weekly = series(WEEKLY_BARS, 20_000, 2_000)
    dc = [float(b[3]) for b in daily]
    wc = [float(b[3]) for b in weekly]
    band_sma, band_ema = sma(wc, 20), ema(wc, 21)
    fixture = {
        "generator": "pine_reference.py",
        "units": "price ticks (engine values are milli-ticks / 1000)",
        "daily": daily,
        "weekly": weekly,
        "expected": {
            "sma_20": sma(dc, 20),
            "ema_21": ema(dc, 21),
            "heikin_ashi": heikin_ashi(daily),
            "sma_200_weekly": sma(wc, 200),
            "band_20_21_weekly": [
                None if a is None or e is None else [a, e]
                for a, e in zip(band_sma, band_ema, strict=True)
            ],
        },
    }
    path = Path(__file__).with_name("pine_parity.json")
    path.write_text(json.dumps(fixture, indent=1) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
