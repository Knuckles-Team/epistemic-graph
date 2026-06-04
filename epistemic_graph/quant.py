"""Quant numerical helpers for the epistemic-graph client layer.

CONCEPT:KG-2.18

Pure-Python rolling-statistics and order-matching primitives used by the
ergonomic client API. These were previously routed through an in-process
compiled ``EpistemicGraph`` extension; that coupling has been removed in favour
of the out-of-process Tokio + MessagePack service (see ``client.py``). Hot-path
numeric work that genuinely needs the Rust engine is exposed through the
service protocol (``finance`` methods); the lightweight helpers below run
locally without any compiled extension so the module imports cleanly in any
environment.
"""

from __future__ import annotations

from math import sqrt


def moving_average(values: list[float], window: int) -> list[float]:
    """Simple moving average with a trailing window.

    Partial (shorter) windows are used for the first ``window - 1`` points so
    the output length matches the input length.
    """
    if window <= 0:
        raise ValueError("window must be positive")
    if not values:
        return []
    result: list[float] = []
    for i in range(len(values)):
        start = max(0, i - window + 1)
        slice_vals = values[start : i + 1]
        result.append(sum(slice_vals) / len(slice_vals))
    return result


def exponential_moving_average(values: list[float], alpha: float) -> list[float]:
    """Exponential moving average with smoothing factor ``alpha`` in (0, 1]."""
    if not 0.0 < alpha <= 1.0:
        raise ValueError("alpha must be in (0, 1]")
    if not values:
        return []
    result: list[float] = [values[0]]
    for v in values[1:]:
        result.append(alpha * v + (1.0 - alpha) * result[-1])
    return result


def rolling_variance(values: list[float], window: int) -> list[float]:
    """Rolling sample variance (ddof=1) over a trailing window.

    Windows shorter than two elements yield ``0.0`` (variance undefined for a
    single sample), matching the partial-window convention of
    :func:`moving_average`.
    """
    if window <= 0:
        raise ValueError("window must be positive")
    if not values:
        return []
    result: list[float] = []
    for i in range(len(values)):
        start = max(0, i - window + 1)
        slice_vals = values[start : i + 1]
        n = len(slice_vals)
        if n < 2:
            result.append(0.0)
            continue
        mean = sum(slice_vals) / n
        result.append(sum((x - mean) ** 2 for x in slice_vals) / (n - 1))
    return result


def rolling_zscore(values: list[float], window: int) -> list[float]:
    """Rolling z-score of the latest point relative to its trailing window.

    A zero-variance window yields ``0.0`` (no deviation to normalise).
    """
    if window <= 0:
        raise ValueError("window must be positive")
    if not values:
        return []
    variances = rolling_variance(values, window)
    means = moving_average(values, window)
    result: list[float] = []
    for i, v in enumerate(values):
        std = sqrt(variances[i])
        result.append((v - means[i]) / std if std > 0.0 else 0.0)
    return result


def simulate_order_matching(
    bids: list[tuple[float, float]],
    asks: list[tuple[float, float]],
    price: float,
    volume: float,
    is_buy: bool,
) -> tuple[
    list[tuple[float, float]], list[tuple[float, float]], list[tuple[float, float]]
]:
    """Match a marketable limit order against an L2 book.

    Returns ``(remaining_bids, remaining_asks, trades)`` where ``trades`` is a
    list of ``(price, volume)`` fills. A buy crosses against asks priced at or
    below ``price`` (best/lowest first); a sell crosses against bids priced at
    or above ``price`` (best/highest first). Untouched levels are preserved.
    """
    trades: list[tuple[float, float]] = []
    remaining = volume

    if is_buy:
        ask_book = sorted(asks, key=lambda x: x[0])  # ascending price
        new_asks: list[tuple[float, float]] = []
        for ask_price, ask_vol in ask_book:
            if remaining > 0 and ask_price <= price:
                take = min(remaining, ask_vol)
                remaining -= take
                trades.append((ask_price, take))
                leftover = ask_vol - take
                if leftover > 0:
                    new_asks.append((ask_price, leftover))
            else:
                new_asks.append((ask_price, ask_vol))
        bid_book = sorted(bids, key=lambda x: x[0], reverse=True)
        return bid_book, new_asks, trades

    bid_book = sorted(bids, key=lambda x: x[0], reverse=True)  # descending price
    new_bids: list[tuple[float, float]] = []
    for bid_price, bid_vol in bid_book:
        if remaining > 0 and bid_price >= price:
            take = min(remaining, bid_vol)
            remaining -= take
            trades.append((bid_price, take))
            leftover = bid_vol - take
            if leftover > 0:
                new_bids.append((bid_price, leftover))
        else:
            new_bids.append((bid_price, bid_vol))
    ask_book = sorted(asks, key=lambda x: x[0])
    return new_bids, ask_book, trades
