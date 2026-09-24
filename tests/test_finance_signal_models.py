"""End-to-end round trips of ``FinanceSignalModels`` (EH-423 / AUD-30).

Full path: Python client -> UDS -> Rust dispatch -> result, over the
session-scoped server + ``clean_graph`` sync client from conftest.py. The two
models moved here from agent-utilities' Python; the reference values are the
Python module's own outputs. Informational only.
"""

from __future__ import annotations

import pytest

INSIDER = {
    "sigma_v": 0.3,
    "sigma_u": 1.0,
    "enforcement": 0.7,
    "surveillance_kappa": 1.0,
    "criminal_penalty": 0.05,
    "civil_penalty_rate": 1.0,
    "horizon": 1.0,
}


def test_bayes_fuse_seeds_measured_priors_and_drops_overfit_ones(clean_graph):
    fused = clean_graph.finance.signal_models(
        "bayes_fuse",
        request={
            "priors": [
                {
                    "name": "a",
                    "directional_accuracy": 0.7,
                    "standalone_sharpe": 0.8,
                    "pbo": 0.2,
                },
                {
                    "name": "b",
                    "directional_accuracy": 0.6,
                    "standalone_sharpe": 0.5,
                    "pbo": 0.9,
                },
            ],
            "directions": {"c": -1, "a": 1},
        },
    )
    assert fused["posterior_up"] == pytest.approx(0.5877103109656301, abs=1e-12)
    assert fused["seeded"] == 1
    assert [source["name"] for source in fused["sources"]] == ["a", "c"]


def test_insider_equilibrium_schedule_and_policy(clean_graph):
    analysis = clean_graph.finance.signal_models(
        "insider_equilibrium", request={"inputs": INSIDER, "steps": 4}
    )
    equilibrium = analysis["equilibrium"]
    assert equilibrium["intensity"] == pytest.approx(0.35947712418300654, abs=1e-12)
    assert equilibrium["binding_lever"] == "criminal"
    intensities = [sample["intensity"] for sample in analysis["schedule"]]
    assert intensities == sorted(intensities)
    assert analysis["policy"]["verdict"] == "criminal_is_the_lever"
    assert analysis["policy"]["criminal_intensity_floor"] == pytest.approx(
        0.1285714285714286, abs=1e-12
    )
