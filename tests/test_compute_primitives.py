"""Round-trip tests for the data-science and extended-finance compute methods
exposed over the Tokio/MessagePack protocol (CONCEPT:KG-2.20, KG-2.22).

These exercise the full path: Python client -> UDS -> Rust dispatch -> result.
Uses the session-scoped server + `clean_graph` sync client from conftest.py.
"""

import math


# ── Data science primitives ───────────────────────────────────────────────


def test_linear_regression_roundtrip(clean_graph):
    # y = 2*x0 + 1  -> intercept ~1, coef ~2
    x = [[0.0], [1.0], [2.0], [3.0], [4.0]]
    y = [1.0, 3.0, 5.0, 7.0, 9.0]
    res = clean_graph.datascience.linear_regression(x, y)
    assert math.isclose(res["intercept"], 1.0, abs_tol=1e-6)
    assert math.isclose(res["coefficients"][0], 2.0, abs_tol=1e-6)
    assert math.isclose(res["r_squared"], 1.0, abs_tol=1e-9)


def test_compute_stats_roundtrip(clean_graph):
    data = [[1.0, 10.0], [2.0, 20.0], [3.0, 30.0]]
    res = clean_graph.datascience.compute_stats(data)
    assert res["n_samples"] == 3
    assert res["n_features"] == 2
    assert math.isclose(res["means"][0], 2.0, abs_tol=1e-9)
    assert math.isclose(res["means"][1], 20.0, abs_tol=1e-9)
    # perfectly correlated columns
    assert math.isclose(res["correlation_matrix"][0][1], 1.0, abs_tol=1e-9)


def test_train_test_split_roundtrip(clean_graph):
    data = [[float(i)] for i in range(10)]
    labels = [float(i) for i in range(10)]
    res = clean_graph.datascience.train_test_split(data, labels, test_ratio=0.2)
    assert len(res["x_train"]) == 8
    assert len(res["x_test"]) == 2
    assert len(res["y_train"]) == 8
    assert len(res["y_test"]) == 2


def test_kmeans_roundtrip(clean_graph):
    data = [[0.0, 0.0], [0.1, 0.1], [9.0, 9.0], [9.1, 9.1]]
    res = clean_graph.datascience.kmeans(data, k=2, max_iter=50)
    assert len(res["labels"]) == 4
    # first two points cluster together, last two together
    assert res["labels"][0] == res["labels"][1]
    assert res["labels"][2] == res["labels"][3]
    assert res["labels"][0] != res["labels"][2]


def test_pca_roundtrip(clean_graph):
    data = [[1.0, 1.0], [2.0, 2.0], [3.0, 3.0], [4.0, 4.0]]
    res = clean_graph.datascience.pca(data, n_components=1)
    assert len(res["components"]) == 1
    assert len(res["transformed"]) == 4


# ── Estimators (fit/predict round-trip) ───────────────────────────────────


def _xy_linear(n=30):
    x = [[float(i), float(i % 4)] for i in range(n)]
    y = [2.0 * xi[0] + 3.0 * xi[1] + 1.0 for xi in x]
    return x, y


def test_ridge_fit_predict_roundtrip(clean_graph):
    x, y = _xy_linear()
    model = clean_graph.datascience.fit_estimator(
        "ridge", x, y, {"alpha": 1e-6}
    )
    assert model["kind"] == "Linear"
    preds = clean_graph.datascience.predict_estimator(model, x)
    err = max(abs(p - t) for p, t in zip(preds, y))
    assert err < 1e-2


def test_decisiontree_fit_predict_roundtrip(clean_graph):
    x = [[float(i) / 20.0] for i in range(20)]
    y = [0.0 if xi[0] < 0.5 else 10.0 for xi in x]
    model = clean_graph.datascience.fit_estimator("decisiontree", x, y)
    assert model["kind"] == "Tree"
    preds = clean_graph.datascience.predict_estimator(model, x)
    assert max(abs(p - t) for p, t in zip(preds, y)) < 1e-6


def test_randomforest_fit_predict_roundtrip(clean_graph):
    x = [[(i - 30) / 10.0] for i in range(60)]
    y = [xi[0] ** 2 for xi in x]
    model = clean_graph.datascience.fit_estimator(
        "randomforest", x, y, {"n_estimators": 30, "random_state": 1}
    )
    assert model["kind"] == "Forest"
    preds = clean_graph.datascience.predict_estimator(model, x)
    rmse = (sum((p - t) ** 2 for p, t in zip(preds, y)) / len(y)) ** 0.5
    assert rmse < 1.5


def test_svr_fit_predict_roundtrip(clean_graph):
    x = [[i * 0.15] for i in range(40)]
    y = [math.sin(xi[0]) for xi in x]
    model = clean_graph.datascience.fit_estimator(
        "svr", x, y,
        {"C": 10.0, "epsilon": 0.05, "gamma": 0.5, "kernel": "rbf", "max_iter": 3000},
    )
    assert model["kind"] == "Svr"
    preds = clean_graph.datascience.predict_estimator(model, x)
    rmse = (sum((p - t) ** 2 for p, t in zip(preds, y)) / len(y)) ** 0.5
    assert rmse < 0.4


# ── Extended finance: risk ────────────────────────────────────────────────


def test_var_and_cvar_roundtrip(clean_graph):
    returns = [-0.05, -0.02, 0.01, 0.03, -0.04, 0.02, -0.01, 0.04]
    var = clean_graph.finance.var(returns, confidence=0.95)
    cvar = clean_graph.finance.cvar(returns, confidence=0.95)
    assert isinstance(var, float)
    assert isinstance(cvar, float)
    # CVaR (expected shortfall) is at least as severe as VaR
    assert cvar >= var - 1e-9


def test_risk_metrics_roundtrip(clean_graph):
    returns = [0.01, -0.02, 0.015, -0.005, 0.02, -0.01]
    res = clean_graph.finance.risk_metrics(returns, risk_free_rate=0.0)
    for key in ("max_drawdown", "volatility", "sortino_ratio"):
        assert key in res


# ── Extended finance: regime / signals ────────────────────────────────────


def test_detect_regimes_roundtrip(clean_graph):
    # two clear regimes: low then high
    obs = [0.0, 0.1, -0.1, 0.0] * 5 + [5.0, 5.1, 4.9, 5.0] * 5
    res = clean_graph.finance.detect_regimes(obs, n_states=2, max_iter=50, tol=1e-4)
    assert len(res["states"]) == len(obs)
    assert len(res["means"]) == 2


def test_momentum_roundtrip(clean_graph):
    prices = [10.0, 11.0, 12.0, 13.0, 14.0]
    res = clean_graph.finance.momentum(prices, lookback=2)
    assert len(res) == len(prices)
    # last value: 14/12 - 1
    assert math.isclose(res[-1], 14.0 / 12.0 - 1.0, abs_tol=1e-9)


# ── Extended finance: execution ───────────────────────────────────────────


def test_twap_roundtrip(clean_graph):
    res = clean_graph.finance.twap(100.0, n_slices=4, start_time=0, interval_secs=60)
    assert len(res) == 4
    # each slice 25 units; serialized as [ts, qty]
    assert math.isclose(res[0][1], 25.0, abs_tol=1e-9)


def test_match_orders_roundtrip(clean_graph):
    orders = [
        {"id": "b1", "side": "buy", "price": 100.0, "quantity": 5.0, "timestamp": 1},
        {"id": "s1", "side": "sell", "price": 99.0, "quantity": 5.0, "timestamp": 2},
    ]
    fills = clean_graph.finance.match_orders(orders)
    assert isinstance(fills, list)
    assert len(fills) >= 1
