"""High-performance compiled Quant FFI Engine.

CONCEPT:KG-2.18
"""

import math

from ._epistemic_graph import EpistemicGraph

_engine = EpistemicGraph()


def moving_average(values: list[float], window: int) -> list[float]:
    """Calculate simple moving average with a sliding window."""
    if not values:
        return []
    result = []
    for i in range(len(values)):
        start = max(0, i - window + 1)
        slice_vals = values[start : i + 1]
        result.append(sum(slice_vals) / len(slice_vals))
    return result


def exponential_moving_average(values: list[float], alpha: float) -> list[float]:
    """Calculate exponential moving average (exponential decay)."""
    return _engine.compute_exponential_decay(values, alpha)


def rolling_variance(values: list[float], window: int) -> list[float]:
    """Calculate rolling sample variance (ddof=1 when N > 1)."""
    if not values:
        return []
    result = []
    for i in range(len(values)):
        start = max(0, i - window + 1)
        slice_vals = values[start : i + 1]
        n = len(slice_vals)
        if n <= 1:
            result.append(0.0)
            continue
        mean = sum(slice_vals) / n
        var = sum((x - mean) ** 2 for x in slice_vals) / (n - 1)
        result.append(var)
    return result


def rolling_zscore(values: list[float], window: int) -> list[float]:
    """Calculate rolling sample z-score."""
    if not values:
        return []
    result = []
    for i in range(len(values)):
        start = max(0, i - window + 1)
        slice_vals = values[start : i + 1]
        n = len(slice_vals)
        if n <= 1:
            result.append(0.0)
            continue
        mean = sum(slice_vals) / n
        var = sum((x - mean) ** 2 for x in slice_vals) / (n - 1)
        std = math.sqrt(var)
        if std > 0.0:
            result.append((values[i] - mean) / std)
        else:
            result.append(0.0)
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
    """Simulate order matching against L2 book limits."""
    # Ensure books are sorted correctly
    # Bids (buys): highest price first
    bid_book = sorted(bids, key=lambda x: x[0], reverse=True)
    # Asks (sells): lowest price first
    ask_book = sorted(asks, key=lambda x: x[0])

    trades = []
    remaining_vol = volume

    if is_buy:
        new_asks = []
        for ask_price, ask_vol in ask_book:
            if ask_price <= price and remaining_vol > 0:
                fill_vol = min(remaining_vol, ask_vol)
                remaining_vol -= fill_vol
                ask_vol -= fill_vol
                trades.append((ask_price, fill_vol))
            if ask_vol > 0:
                new_asks.append((ask_price, ask_vol))
        return bid_book, new_asks, trades
    else:
        new_bids = []
        for bid_price, bid_vol in bid_book:
            if bid_price >= price and remaining_vol > 0:
                fill_vol = min(remaining_vol, bid_vol)
                remaining_vol -= fill_vol
                bid_vol -= fill_vol
                trades.append((bid_price, fill_vol))
            if bid_vol > 0:
                new_bids.append((bid_price, bid_vol))
        return new_bids, ask_book, trades
