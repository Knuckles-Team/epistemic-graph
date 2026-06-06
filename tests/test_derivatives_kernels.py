"""Round-trip tests for the SABR volatility-surface kernels (CONCEPT:KG-2.20j).
Full path: client -> UDS -> Rust -> result. Uses `clean_graph` from conftest.
"""


def test_sabr_implied_vol_and_smile(clean_graph):
    v = clean_graph.finance.sabr_implied_vol(100.0, 100.0, 1.0, 0.2, 0.5, -0.3, 0.4)
    assert v > 0.0
    smile = clean_graph.finance.sabr_smile(100.0, [80.0, 90.0, 100.0, 110.0, 120.0],
                                           1.0, 0.2, 0.5, -0.5, 0.5)
    assert len(smile) == 5 and all(s > 0 for s in smile)
    assert smile[0] > smile[4]  # negative-rho downside skew


def test_sabr_calibration_recovers(clean_graph):
    strikes = [80.0, 90.0, 100.0, 110.0, 120.0]
    market = clean_graph.finance.sabr_smile(100.0, strikes, 1.0, 0.25, 0.5, -0.3, 0.4)
    fit = clean_graph.finance.sabr_calibrate(100.0, 1.0, strikes, market, beta=0.5)
    assert fit["rmse"] < 1e-3
    assert abs(fit["alpha"] - 0.25) < 0.05
