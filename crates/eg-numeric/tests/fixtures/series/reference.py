#!/usr/bin/env python3
"""Independent pandas/numpy reference for the series-kernel goldens (EH-522).

Re-computes, with pandas rolling windows and numpy, every function of the UQL
``DERIVE`` table that ``eg_numeric::series`` implements, over two deterministic
series (a 64-bit LCG walk with a flat run, and a second walk). It writes
``golden.json`` beside this file: each function's parameters and its output, with
``null`` for warm-up / undefined values. The Rust test
(``crates/eg-numeric/tests/series_golden.rs``) must match every value within a
relative 1e-9. Test-only: EG never links pandas.

Conventions pinned here (they are the kernels' documented contract):
``rstd``/``zscore`` population (``ddof=0``), a z-score is 0 on a flat window,
``ewma`` is ``ewm(adjust=False)``, ``rrank`` is ``rolling(w).rank()`` (average
ties), ``ic`` is the Spearman correlation of the window's ranks.

Run: ``python3 reference.py`` (rewrites ``golden.json``).
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

MASK = (1 << 64) - 1
N = 300


def walk(seed: int, flat: range) -> list[float]:
    state, level, out = seed, 100.0, []
    for i in range(N):
        state = (state * 6364136223846793005 + 1442695040888963407) & MASK
        step = ((state >> 33) % 21) - 10
        if i not in flat:
            level += step / 4.0
        out.append(level)
    return out


def clean(series: pd.Series) -> list[float | None]:
    return [float(v) if np.isfinite(v) else None for v in series.to_numpy(dtype=float)]


def rolling_ic(x: pd.Series, y: pd.Series, w: int) -> pd.Series:
    out = [np.nan] * len(x)
    for t in range(w - 1, len(x)):
        rx = x.iloc[t + 1 - w : t + 1].rank().to_numpy()
        ry = y.iloc[t + 1 - w : t + 1].rank().to_numpy()
        dx, dy = rx - rx.mean(), ry - ry.mean()
        den = np.sqrt((dx * dx).sum() * (dy * dy).sum())
        out[t] = (dx * dy).sum() / den if den > 1e-12 else np.nan
    return pd.Series(out)


def zscore(x: pd.Series, w: int) -> pd.Series:
    mean = x.rolling(w).mean()
    std = x.rolling(w).std(ddof=0)
    z = (x - mean) / std
    return z.where(~(std <= 1e-12), 0.0).where(std.notna())


def kalman_level(z: pd.Series, q: float, r: float) -> pd.Series:
    out, x, p = [], None, r
    for obs in z:
        if x is None:
            x = obs
        else:
            p = p + q
            k = p / (p + r)
            x = x + k * (obs - x)
            p = p * (1.0 - k)
        out.append(x)
    return pd.Series(out)


def kalman_beta(z: pd.Series, h: pd.Series, q: float, r: float) -> pd.Series:
    out, beta, p = [], 0.0, 1.0
    for obs, hh in zip(z, h):
        p = p + q
        s = hh * p * hh + r
        k = p * hh / s if abs(s) > 1e-18 else 0.0
        beta = beta + k * (obs - hh * beta)
        p = p * (1.0 - k * hh)
        out.append(beta)
    return pd.Series(out)


def cases(x: pd.Series, y: pd.Series) -> list[dict]:
    w, k = 20, 3
    table = [
        ("lag", [k], x.shift(k)),
        ("diff", [k], x - x.shift(k)),
        ("ret", [k], x / x.shift(k) - 1.0),
        ("logret", [k], np.log(x / x.shift(k))),
        ("rmean", [w], x.rolling(w).mean()),
        ("rstd", [w], x.rolling(w).std(ddof=0)),
        ("rsum", [w], x.rolling(w).sum()),
        ("rmin", [w], x.rolling(w).min()),
        ("rmax", [w], x.rolling(w).max()),
        ("rrank", [w], x.rolling(w).rank()),
        ("zscore", [w], zscore(x, w)),
        ("ewma", [12.0], x.ewm(span=12.0, adjust=False).mean()),
        ("ewma_halflife", [3.5], x.ewm(halflife=3.5, adjust=False).mean()),
        ("rcorr", [w], x.rolling(w).corr(y)),
        ("ic", [w], rolling_ic(x, y, w)),
        ("wsum", [w], (x * y).rolling(w).sum()),
        ("kalman", [0.01, 0.5], kalman_level(x, 0.01, 0.5)),
        ("kbeta", [0.01, 0.5], kalman_beta(x, y / 100.0, 0.01, 0.5)),
    ]
    return [{"func": f, "params": p, "values": clean(v)} for f, p, v in table]


def main() -> None:
    x = pd.Series(walk(0x5EED_5E81_E5, range(140, 170)))
    y = pd.Series(walk(0xA11C_E0FF_EE, range(0)))
    golden = {"x": list(x), "y": list(y), "cases": cases(x, y)}
    path = Path(__file__).with_name("golden.json")
    path.write_text(json.dumps(golden, indent=1) + "\n")


if __name__ == "__main__":
    main()
