"""Round-trip tests for the state-space / stat-arb / signal-combination kernels
(CONCEPT:EG-KG.domains.state-space-statistical-arbitrage / KG-2.20i). Full path: client -> UDS -> Rust -> result.
Uses the session-scoped server + `clean_graph` sync client from conftest.py.
"""

import math

# ── State-space (Kalman, ADF, OU, Markov) ──────────────────────────────────


def test_kalman_beta_roundtrip(clean_graph):
    rm = [math.sin(i * 0.1) * 0.01 for i in range(300)]
    ra = [1.5 * m + 1e-5 * ((i % 3) - 1) for i, m in enumerate(rm)]
    out = clean_graph.finance.kalman_beta(rm, ra, q=1e-6, r=1e-4)
    assert len(out["states"]) == 300
    assert abs(out["states"][-1] - 1.5) < 0.2  # recovers the true beta


def test_kalman_volatility_roundtrip(clean_graph):
    rets = [0.01 * math.sin(i * 0.3) for i in range(250)]
    vol = clean_graph.finance.kalman_volatility(rets, q=0.1, r=1.0)
    assert len(vol) == 250
    assert all(v >= 0.0 for v in vol)


def test_adf_test_roundtrip(clean_graph):
    # AR(0.2) stationary series (deterministic LCG noise)
    seed = 12345
    xs = [0.0]
    for _ in range(400):
        seed = (seed * 6364136223846793005 + 1442695040888963407) % (2**64)
        e = ((seed >> 33) / (2**31)) - 1.0
        xs.append(0.2 * xs[-1] + e)
    res = clean_graph.finance.adf_test(xs, max_lag=1)
    assert res["stationary_5pct"] is True
    assert res["statistic"] < res["crit_5pct"]


def test_ou_calibrate_and_thresholds_roundtrip(clean_graph):
    s = [0.5]
    for i in range(500):
        s.append(s[-1] + 0.3 * (0.5 - s[-1]) + 0.01 * ((i % 11) - 5))
    p = clean_graph.finance.ou_calibrate(s, dt=1.0)
    assert p["theta"] > 0.0
    assert abs(p["mu"] - 0.5) < 0.2
    th = clean_graph.finance.ou_optimal_thresholds(
        p["theta"], p["mu"], p["sigma"], p["sigma_eq"], cost=0.001
    )
    assert th["entry_long"] < th["exit"] < th["entry_short"]


def test_markov_transition_roundtrip(clean_graph):
    m = clean_graph.finance.markov_transition_matrix([0, 1, 1, 2, 0, 1, 2, 2, 0], 3)
    assert len(m) == 3
    for row in m:
        assert abs(sum(row) - 1.0) < 1e-9


# ── Signal combination / sizing / calibration ──────────────────────────────


def test_information_ratio_and_effective_n_roundtrip(clean_graph):
    ir = clean_graph.finance.information_ratio(0.05, 50.0)
    assert abs(ir - 0.05 * math.sqrt(50)) < 1e-6
    same = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0]
    neff = clean_graph.finance.effective_independent_n([same, same])
    assert neff < 1.5  # two identical signals ≈ 1 independent


def test_alpha_combination_and_obi_roundtrip(clean_graph):
    m = [
        [0.01, 0.02, -0.01, 0.03, 0.0, 0.01, 0.02, -0.02],
        [-0.01, 0.0, 0.02, -0.01, 0.01, 0.0, -0.01, 0.02],
        [0.02, -0.01, 0.0, 0.01, -0.02, 0.01, 0.0, 0.01],
    ]
    w = clean_graph.finance.alpha_combination_engine(m, lookback=4)
    assert abs(sum(abs(x) for x in w) - 1.0) < 1e-9
    obi = clean_graph.finance.order_book_imbalance([100.0, 50.0], [0.0, 50.0])
    assert abs(obi[0] - 1.0) < 1e-9


def test_brier_and_convergence_gate_roundtrip(clean_graph):
    assert abs(clean_graph.finance.brier_score([0.5] * 4, [1.0, 0.0, 1.0, 0.0]) - 0.25) < 1e-9
    g = clean_graph.finance.convergence_gate([0.9, 0.8, 0.95, 0.85, 0.9], 0.6, 5)
    assert g["pass"] is True and g["direction"] == 1
    g2 = clean_graph.finance.convergence_gate([0.9, 0.8, 0.1, 0.0, 0.7], 0.6, 5)
    assert g2["pass"] is False


def test_empirical_kelly_roundtrip(clean_graph):
    stable = [0.02] * 200
    f = clean_graph.finance.empirical_kelly(0.6, 1.0, stable, n_simulations=500, seed=42)
    assert f > 0.0
    assert clean_graph.finance.empirical_kelly(0.4, 1.0, stable, 100, 1) == 0.0
