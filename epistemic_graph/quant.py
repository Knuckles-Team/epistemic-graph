"""High-performance compiled Quant epistemic-graph Engine.

CONCEPT:KG-2.18

All numerical computations delegate to the Rust-compiled EpistemicGraph
core via PyO3 epistemic-graph.  The Python layer provides ergonomic API wrappers
while the Rust core handles the hot-path arithmetic.
"""

from ._epistemic_graph import EpistemicGraph

_engine = EpistemicGraph()


def moving_average(values: list[float], window: int) -> list[float]:
    """Calculate simple moving average with a sliding window.

    Attempts Rust ``compute_rolling_mean`` epistemic-graph first; falls back to
    Python if the method is not yet compiled into the binary.
    """
    try:
        return _engine.compute_rolling_mean(values, window)
    except AttributeError:
        # compute_rolling_mean is defined in .pyi stubs but not yet
        # compiled into all builds — graceful Python fallback.
        if not values:
            return []
        result: list[float] = []
        for i in range(len(values)):
            start = max(0, i - window + 1)
            slice_vals = values[start : i + 1]
            result.append(sum(slice_vals) / len(slice_vals))
        return result


def exponential_moving_average(values: list[float], alpha: float) -> list[float]:
    """Calculate exponential moving average (exponential decay)."""
    return _engine.compute_exponential_decay(values, alpha)


def rolling_variance(values: list[float], window: int) -> list[float]:
    """Calculate rolling sample variance.

    Wraps the Rust ``compute_rolling_std`` and squares each element
    to convert standard deviation → variance, preserving the existing
    API contract (callers expect variance, Rust exposes std).
    """
    stds = _engine.compute_rolling_std(values, window)
    return [s * s for s in stds]


def rolling_zscore(values: list[float], window: int) -> list[float]:
    """Calculate rolling z-score.

    Delegates to Rust ``compute_rolling_zscore`` for performance.
    """
    return _engine.compute_rolling_zscore(values, window)


def simulate_order_matching(
    bids: list[tuple[float, float]],
    asks: list[tuple[float, float]],
    price: float,
    volume: float,
    is_buy: bool,
) -> tuple[
    list[tuple[float, float]], list[tuple[float, float]], list[tuple[float, float]]
]:
    """Simulate order matching against L2 book limits.

    Bridges the ergonomic Python API (explicit bids/asks/price/volume/is_buy)
    to the Rust core's order-tuple format, providing both a unified backend
    and a caller-friendly interface.

    The Rust API expects ``orders: list[tuple[side, type, price, volume]]``
    and returns dicts.  This wrapper converts both directions so callers
    keep using the familiar ``(bids, asks, trades)`` tuple return.
    """
    # Build the Rust-compatible order list from the ergonomic Python args
    side = "buy" if is_buy else "sell"
    orders: list[tuple[str, str, float, float]] = [("order_1", side, price, volume)]

    try:
        raw_results = _engine.simulate_order_matching(bids, asks, orders)
        # Parse Rust results back into the Python tuple format
        trades: list[tuple[float, float]] = []
        for r in raw_results:
            trade_price = float(r.get("match_price", 0))
            trade_vol = float(r.get("match_volume", 0))
            trades.append((trade_price, trade_vol))

        # Rebuild remaining books after fills
        filled_volume = sum(t[1] for t in trades)
        remaining_vol = max(0.0, volume - filled_volume)

        if is_buy:
            ask_book = sorted(asks, key=lambda x: x[0])
            new_asks: list[tuple[float, float]] = []
            consumed = volume - remaining_vol
            for ask_price, ask_vol in ask_book:
                if ask_price <= price and consumed > 0:
                    take = min(consumed, ask_vol)
                    consumed -= take
                    leftover = ask_vol - take
                    if leftover > 0:
                        new_asks.append((ask_price, leftover))
                else:
                    new_asks.append((ask_price, ask_vol))
            bid_book = sorted(bids, key=lambda x: x[0], reverse=True)
            return bid_book, new_asks, trades
        else:
            bid_book = sorted(bids, key=lambda x: x[0], reverse=True)
            new_bids: list[tuple[float, float]] = []
            consumed = volume - remaining_vol
            for bid_price, bid_vol in bid_book:
                if bid_price >= price and consumed > 0:
                    take = min(consumed, bid_vol)
                    consumed -= take
                    leftover = bid_vol - take
                    if leftover > 0:
                        new_bids.append((bid_price, leftover))
                else:
                    new_bids.append((bid_price, bid_vol))
            ask_book = sorted(asks, key=lambda x: x[0])
            return new_bids, ask_book, trades

    except Exception as e:
        print(f"Rust simulate_order_matching failed: {e}")
        raise
