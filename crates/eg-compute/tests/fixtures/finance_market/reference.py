#!/usr/bin/env python3
"""Independent integer reference for the finance-market kernel golden vectors.

This re-implements, in plain Python integers and separately from the Rust
kernels, the published recurrences of ``eg_compute::finance::market``: Wilder
ATR, the ATR trailing-trend line (SuperTrend, raw and Heikin-Ashi basis), SMA,
EMA, the 20/21 band, Heikin-Ashi candles and the 200-week SMA over an ISO-week
rollup. It generates the same deterministic bar series as the Rust golden test
(a 64-bit LCG) and writes ``golden.json``: the sha256 of every indicator's
canonical output text plus sampled values and the trend-flip bar closes. The
Rust test must reproduce every digest bit for bit on every build host, and a
Pine companion can replay the same series against the same fixture.

Run: ``python3 reference.py`` (rewrites ``golden.json`` beside this file).
"""

from __future__ import annotations

import datetime
import hashlib
import json
from collections import deque
from pathlib import Path

MILLI = 1_000
NS_PER_DAY = 86_400_000_000_000
MASK = (1 << 64) - 1
BARS = 1_498  # 214 whole ISO weeks
SEED = 0x5EED_F1A7_2026_0924


def days_from_civil(year: int, month: int, day: int) -> int:
    """Days since 1970-01-01 (proleptic Gregorian)."""
    return (datetime.date(year, month, day) - datetime.date(1970, 1, 1)).days


def div_round(numerator: int, denominator: int) -> int:
    """Round half away from zero."""
    magnitude = (abs(numerator) * 2 + denominator) // (denominator * 2)
    return -magnitude if numerator < 0 else magnitude


class Lcg:
    """The 64-bit PCG-multiplier LCG the Rust test also uses."""

    def __init__(self, seed: int) -> None:
        self.state = seed & MASK

    def next(self) -> int:
        self.state = (self.state * 6364136223846793005 + 1442695040888963407) & MASK
        return self.state >> 33


def bars() -> list[dict[str, int]]:
    """Daily bars from Monday 2019-01-07: a random walk with gaps and a flat run."""
    rng = Lcg(SEED)
    start = days_from_civil(2019, 1, 7) * NS_PER_DAY
    close = 100_000
    out = []
    for index in range(BARS):
        open_ = close
        if index % 50 == 49:
            open_ = max(1_000, close + rng.next() % 5_001 - 2_500)
        if 300 <= index < 320:
            high = low = close_ = open_
        else:
            close_ = max(1_000, open_ + rng.next() % 4_001 - 2_000)
            high = max(open_, close_) + rng.next() % 1_500
            low = max(1, min(open_, close_) - rng.next() % 1_500)
        volume = rng.next() % 1_000_000
        open_time = start + index * NS_PER_DAY
        out.append(
            {
                "open_time": open_time,
                "close_time": open_time + NS_PER_DAY,
                "open": open_,
                "high": high,
                "low": low,
                "close": close_,
                "volume": volume,
            }
        )
        close = close_
    return out


def weekly(daily: list[dict[str, int]]) -> list[dict[str, int]]:
    """ISO-week (Monday UTC) rollup of whole weeks."""
    out: list[dict[str, int]] = []
    for bar in daily:
        day = bar["open_time"] // NS_PER_DAY
        monday = day - (day + 3) % 7
        start = monday * NS_PER_DAY
        if out and out[-1]["open_time"] == start:
            acc = out[-1]
            acc["high"] = max(acc["high"], bar["high"])
            acc["low"] = min(acc["low"], bar["low"])
            acc["close"] = bar["close"]
            acc["volume"] += bar["volume"]
        else:
            out.append(dict(bar, open_time=start, close_time=start + 7 * NS_PER_DAY))
    return out


def true_range(high: int, low: int, prev_close: int | None) -> int:
    if prev_close is None:
        return high - low
    return max(high - low, abs(high - prev_close), abs(low - prev_close))


class Wilder:
    def __init__(self, period: int) -> None:
        self.period, self.count, self.total = period, 0, 0
        self.value: int | None = None

    def step(self, x: int) -> int | None:
        self.count += 1
        if self.value is not None:
            self.value = div_round(self.value * (self.period - 1) + x, self.period)
            return self.value
        self.total += x
        if self.count == self.period:
            self.value = div_round(self.total, self.period)
        return self.value


class Sma:
    def __init__(self, period: int) -> None:
        self.period, self.window = period, deque[int]()

    def step(self, x: int) -> int | None:
        self.window.append(x)
        if len(self.window) > self.period:
            self.window.popleft()
        if len(self.window) < self.period:
            return None
        return div_round(sum(self.window), self.period)


class Ema:
    def __init__(self, period: int) -> None:
        self.period, self.count, self.total = period, 0, 0
        self.value: int | None = None

    def step(self, x: int) -> int | None:
        self.count += 1
        if self.value is not None:
            self.value += div_round((x - self.value) * 2, self.period + 1)
            return self.value
        self.total += x
        if self.count == self.period:
            self.value = div_round(self.total, self.period)
        return self.value


class HeikinAshi:
    def __init__(self) -> None:
        self.open: int | None = None
        self.close: int | None = None

    def step(self, o: int, h: int, lo: int, c: int) -> tuple[int, int, int, int]:
        close = div_round(o + h + lo + c, 4)
        if self.open is None or self.close is None:
            open_ = div_round(o + c, 2)
        else:
            open_ = div_round(self.open + self.close, 2)
        self.open, self.close = open_, close
        return open_, max(h, open_, close), min(lo, open_, close), close


def milli(bar: dict[str, int]) -> tuple[int, int, int, int]:
    return (
        bar["open"] * MILLI,
        bar["high"] * MILLI,
        bar["low"] * MILLI,
        bar["close"] * MILLI,
    )


def supertrend(
    series: list[dict[str, int]], period: int, mult: int, heikin: bool
) -> list[str]:
    """TradingView ``ta.supertrend`` in integer milli-ticks."""
    atr, ha = Wilder(period), HeikinAshi()
    prev_close: int | None = None
    upper_prev: int | None = None
    lower_prev: int | None = None
    direction: str | None = None
    out = []
    for bar in series:
        o, h, lo, c = milli(bar)
        if heikin:
            o, h, lo, c = ha.step(o, h, lo, c)
        value = atr.step(true_range(h, lo, prev_close))
        closing_before: int | None = prev_close
        prev_close = c
        if value is None:
            out.append("w")
            continue
        mid = div_round(h + lo, 2)
        offset = div_round(value * mult, MILLI)
        upper, lower = mid + offset, mid - offset
        if (
            upper_prev is not None
            and lower_prev is not None
            and closing_before is not None
        ):
            upper = (
                upper
                if upper < upper_prev or closing_before > upper_prev
                else upper_prev
            )
            lower = (
                lower
                if lower > lower_prev or closing_before < lower_prev
                else lower_prev
            )
        if direction is None:
            direction = "bear"
        elif direction == "bear" and c > upper:
            direction = "bull"
        elif direction == "bull" and c < lower:
            direction = "bear"
        upper_prev, lower_prev = upper, lower
        line = lower if direction == "bull" else upper
        out.append(f"t,{line},{value},{direction}")
    return out


def lines(
    series: list[dict[str, int]], kernel: Sma | Ema | Wilder, atr: bool
) -> list[str]:
    out, prev_close = [], None
    for bar in series:
        _, h, lo, c = milli(bar)
        value = kernel.step(true_range(h, lo, prev_close) if atr else c)
        prev_close = c
        out.append("w" if value is None else f"l,{value}")
    return out


def band(series: list[dict[str, int]], sma_period: int, ema_period: int) -> list[str]:
    sma, ema, out = Sma(sma_period), Ema(ema_period), []
    for bar in series:
        c = bar["close"] * MILLI
        a, e = sma.step(c), ema.step(c)
        out.append("w" if a is None or e is None else f"b,{a},{e}")
    return out


def candles(series: list[dict[str, int]]) -> list[str]:
    ha, out = HeikinAshi(), []
    for bar in series:
        o, h, lo, c = ha.step(*milli(bar))
        out.append(f"c,{o},{h},{lo},{c}")
    return out


def entry(series: list[dict[str, int]], values: list[str]) -> dict[str, object]:
    text = "".join(
        f"{bar['open_time']}:{value}\n"
        for bar, value in zip(series, values, strict=True)
    )
    samples = {
        str(i): values[i]
        for i in (0, 13, 14, 199, 200, len(values) - 1)
        if i < len(values)
    }
    return {
        "sha256": hashlib.sha256(text.encode()).hexdigest(),
        "points": len(values),
        "samples": samples,
    }


def flips(series: list[dict[str, int]], values: list[str]) -> list[int]:
    out, previous = [], None
    for bar, value in zip(series, values, strict=True):
        if value == "w":
            continue
        direction = value.rsplit(",", 1)[1]
        if previous is not None and direction != previous:
            out.append(bar["close_time"])
        previous = direction
    return out


def main() -> None:
    daily = bars()
    weeks = weekly(daily)
    trail = supertrend(daily, 10, 3_000, heikin=False)
    golden = {
        "seed": SEED,
        "bars": BARS,
        "daily_sha256": hashlib.sha256(
            "".join(
                f"{b['open_time']},{b['open']},{b['high']},{b['low']},{b['close']},{b['volume']}\n"
                for b in daily
            ).encode()
        ).hexdigest(),
        "weeks": len(weeks),
        "indicators": {
            "atr_14": entry(daily, lines(daily, Wilder(14), atr=True)),
            "super_trend_10_3": entry(daily, trail),
            "super_trend_10_3_heikin_ashi": entry(
                daily, supertrend(daily, 10, 3_000, heikin=True)
            ),
            "sma_20": entry(daily, lines(daily, Sma(20), atr=False)),
            "ema_21": entry(daily, lines(daily, Ema(21), atr=False)),
            "band_20_21_weekly": entry(weeks, band(weeks, 20, 21)),
            "heikin_ashi": entry(daily, candles(daily)),
            "sma_200_weekly": entry(weeks, lines(weeks, Sma(200), atr=False)),
        },
        "super_trend_10_3_flips": flips(daily, trail),
    }
    path = Path(__file__).with_name("golden.json")
    path.write_text(
        json.dumps(golden, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


if __name__ == "__main__":
    main()
