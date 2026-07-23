"""Round-trip tests for the market-microstructure, sizing, validation, and
forensic-accounting kernels (CONCEPT:EG-KG.domains.market-microstructure-sizing-backtest / KG-2.20g).

Full path: Python client -> UDS -> Rust dispatch -> result. Uses the
session-scoped server + `clean_graph` sync client from conftest.py.
"""

import math

# ── Market making / microstructure ────────────────────────────────────────


def test_avellaneda_stoikov_roundtrip(clean_graph):
    flat = clean_graph.finance.avellaneda_stoikov(
        mid=100.0, inventory=0.0, sigma=0.02, gamma=0.1, kappa=1.5, tau=1.0
    )
    assert math.isclose(flat["reservation"], 100.0, abs_tol=1e-9)
    assert flat["bid"] < flat["ask"]
    assert flat["withdraw"] is False
    # long inventory skews reservation below mid (wants to offload)
    long = clean_graph.finance.avellaneda_stoikov(
        mid=100.0, inventory=10.0, sigma=0.02, gamma=0.1, kappa=1.5, tau=1.0
    )
    assert long["reservation"] < 100.0


def test_logit_quotes_bounded_and_cap(clean_graph):
    q = clean_graph.finance.logit_quotes(
        p_mid=0.5, inventory=0.0, sigma=0.5, gamma=0.1, kappa=1.5, tau=1.0,
        boundary_m=100.0,
    )
    assert 0.0 < q["bid"] < q["ask"] < 1.0
    assert q["withdraw"] is False
    q2 = clean_graph.finance.logit_quotes(
        p_mid=0.02, inventory=500.0, sigma=0.5, gamma=0.1, kappa=1.5, tau=1.0,
        boundary_m=100.0,
    )
    assert q2["withdraw"] is True


def test_microprice_and_vpin_roundtrip(clean_graph):
    mp = clean_graph.finance.microprice_series([0.49], [100.0], [0.51], [100.0])
    assert math.isclose(mp[0], 0.5, abs_tol=1e-9)
    toxic = clean_graph.finance.vpin_pm([100.0], [0.0], [0.5])
    balanced = clean_graph.finance.vpin_pm([50.0], [50.0], [0.5])
    assert toxic > balanced


def test_kyle_lambda_and_surveillance_roundtrip(clean_graph):
    # price change = 0.5 * signed flow ⇒ Kyle λ ≈ 0.5
    flow = [float(i) for i in range(1, 21)]
    dp = [0.5 * q for q in flow]
    assert math.isclose(clean_graph.finance.kyle_lambda(dp, flow), 0.5, abs_tol=1e-6)
    # toxic one-sided flow scores higher legal risk than benign balanced flow
    toxic = clean_graph.finance.surveillance_risk(
        buy_vol=[500.0, 500.0, 500.0], sell_vol=[5.0, 5.0, 5.0], p_mean=[0.5, 0.5, 0.5],
        signed_flow=[8.0, 9.0, 10.0], price_changes=[0.4, 0.45, 0.5], baseline_sigma=1.0,
    )
    benign = clean_graph.finance.surveillance_risk(
        buy_vol=[110.0, 110.0, 110.0], sell_vol=[100.0, 100.0, 100.0], p_mean=[0.5, 0.5, 0.5],
        signed_flow=[1.0, -1.0, 1.0], price_changes=[0.01, -0.01, 0.01], baseline_sigma=1.0,
    )
    assert 0.0 <= benign["legal_risk_score"] <= 1.0
    assert toxic["legal_risk_score"] > benign["legal_risk_score"]
    assert toxic["detection_hazard"] > benign["detection_hazard"]


def test_ofi_and_breakeven_roundtrip(clean_graph):
    ofi = clean_graph.finance.ofi_series(
        ts=[0.0, 0.5, 1.0], bid_px=[0.49, 0.49, 0.50], bid_sz=[100.0, 120.0, 130.0],
        ask_px=[0.51, 0.51, 0.51], ask_sz=[100.0, 90.0, 90.0], window_secs=1.0,
    )
    assert len(ofi) == 3
    be = clean_graph.finance.breakeven_alpha(delta=0.01, p=0.5)
    assert 0.0 < be < 1.0


def test_hawkes_mle_roundtrip(clean_graph):
    times = [i * 0.3 + (i % 3) * 0.05 for i in range(50)]
    fit = clean_graph.finance.hawkes_mle(times, t_horizon=20.0, max_iter=200)
    assert fit["mu"] > 0.0
    assert 0.0 <= fit["branching_ratio"] < 1.0


# ── Position sizing ────────────────────────────────────────────────────────


def test_kelly_roundtrip(clean_graph):
    # q=0.6, c=0.5 -> f*=0.2; quarter-kelly -> 0.05
    f = clean_graph.finance.kelly_fraction(q=0.6, c=0.5, fraction=0.25)
    assert math.isclose(f, 0.05, abs_tol=1e-9)
    assert clean_graph.finance.kelly_fraction(q=0.4, c=0.5, fraction=0.25) == 0.0


def test_bayesian_kelly_shrinks_with_uncertainty(clean_graph):
    tight = clean_graph.finance.bayesian_kelly(60.0, 40.0, 0.5, 32)
    wide = clean_graph.finance.bayesian_kelly(6.0, 4.0, 0.5, 32)
    assert tight >= wide
    ci = clean_graph.finance.posterior_credible_interval(60.0, 40.0, 0.05)
    assert ci["lower"] < 0.6 < ci["upper"]


# ── Backtest validation ────────────────────────────────────────────────────


def test_purged_cpcv_roundtrip(clean_graph):
    splits = clean_graph.finance.purged_cpcv(120, n_groups=6, n_test_groups=2,
                                             purge_window=5, embargo=5)
    assert len(splits) == 15  # C(6,2)
    for s in splits:
        tset = set(s["test"])
        assert all(i not in tset for i in s["train"])


def test_deflated_sharpe_and_dm_roundtrip(clean_graph):
    rets = [0.01 + 0.001 * ((i % 7) - 3) for i in range(100)]
    dsr = clean_graph.finance.deflated_sharpe(1.5, 10, rets)
    assert 0.0 <= dsr <= 1.0
    a = [1.0] * 50
    b = [2.0] * 50
    dm = clean_graph.finance.diebold_mariano(a, b, 1)
    assert dm["a_better"] is True


# ── Forensic accounting ────────────────────────────────────────────────────


def _healthy_years():
    prior = {
        "sales": 1000.0, "cogs": 600.0, "sga": 150.0, "net_income": 180.0,
        "cfo": 200.0, "receivables": 100.0, "current_assets": 500.0,
        "current_liabilities": 200.0, "ppe_net": 400.0, "depreciation": 40.0,
        "total_assets": 1200.0, "total_liabilities": 400.0, "long_term_debt": 200.0,
        "retained_earnings": 600.0, "ebit": 250.0, "market_cap": 3000.0, "shares": 100.0,
    }
    this = dict(prior)
    this.update({
        "sales": 1100.0, "cogs": 650.0, "net_income": 210.0, "cfo": 240.0,
        "receivables": 105.0, "current_assets": 560.0, "total_assets": 1320.0,
        "retained_earnings": 750.0, "ebit": 290.0, "long_term_debt": 180.0,
    })
    return this, prior


def test_forensic_report_clean(clean_graph):
    this, prior = _healthy_years()
    r = clean_graph.finance.forensic_report(this, prior)
    assert r["verdict"] == "CLEAN"
    assert r["m_score"] < -1.78
    assert r["z_score"] > 2.99
    assert r["f_score"] >= 6


def test_forensic_report_accruals_blowout(clean_graph):
    this, prior = _healthy_years()
    this["net_income"] = 600.0
    this["cfo"] = 50.0
    r = clean_graph.finance.forensic_report(this, prior)
    assert r["accruals_ratio"] > 0.25
    assert r["verdict"] == "INVESTIGATE"
