"""Round-trip tests for the ecosystem-utilization gap-fill program.

Closes three "wire-first" gaps the synergy-skills audit found: the engine had a
real capability implemented + server-tested, but the Python client had no way to
reach it.

1. ``ExplainBelief.disclosure_level`` (EPI-P3-4/L51, feature ``epistemic-redaction``)
   — the client's :meth:`QueryClient.explain_belief` now accepts a
   ``disclosure_level`` and gets back a redacted proof.
2. The durable analytics-job plane (CONCEPT:INT-P2-1, feature ``jobs``) — the new
   ``client.jobs`` sub-client (submit/status/cancel/resume) and the general
   ``client.cancel_request``.
3. Causal ``observe``/``counterfactual`` (EPI-P3-6, feature ``epistemic-causal``) —
   ``Method::CausalEstimate`` now carries a ``mode`` (``Intervene``/``Observe``) and
   a new ``Method::CausalCounterfactual`` wires Pearl's point-counterfactual recipe,
   both reachable via :meth:`QueryClient.causal_estimate`/:meth:`causal_counterfactual`.
4. Standalone paraconsistent conflict resolution (EPI-P3-7, feature ``epistemic-tms``)
   — a new ``Method::ResolveConflict`` reaches ``eg_epistemic::tms``'s Dung
   grounded/preferred/stable argumentation semantics directly (previously reachable
   only COMPOSED inside ``epistemic_status``), via :meth:`QueryClient.resolve_conflict`.

This is self-contained: it builds + manages its OWN server process, independent of
the session-wide ``full`` fixture in ``conftest.py``. ``epistemic-causal``/
``epistemic-redaction``/``jobs``/``epistemic-tms`` are all now folded into ``full``
(see the root ``Cargo.toml``'s ``full`` feature list) -- ``FEATURES`` below also adds
``viz-static-export`` even though this module never exercises it, purely so its own
``_build()`` targets the SAME feature string as ``test_viz_client.py``'s (that file's
own "self-building-fixture pattern" this docstring already referenced). Both modules
build to the SAME shared ``target-isolated`` output (`.cargo/config.toml`); if they
requested different feature strings, whichever ran second would pay a ~40-60s relink
of the other's build (see ``test_graceful_shutdown.py``'s "cold `cargo run`" comment)
— often enough to blow the 60s pytest-timeout. A strict superset is always a safe
substitute for a subset request, so aligning the two eliminates the thrash.
"""

from __future__ import annotations

import os
import socket as _socket
import subprocess
import time

import pytest
from conftest import (
    TEST_AGENT_ID,
    TEST_SIGNER_KEY,
    bootstrap_context,
    find_server_binary,
    request_context,
    strict_server_env,
)

from epistemic_graph.client import SyncEpistemicGraphClient

RUST_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
AUTH_SECRET = "test-epi-gapfill-roundtrip-secret"
FEATURES = "full viz-static-export"


def _build() -> str | None:
    """Build the gap-fill binary ONCE and return its path (or None on failure)."""
    r = subprocess.run(
        ["cargo", "build", "--features", FEATURES],
        cwd=RUST_DIR,
        capture_output=True,
        text=True,
    )
    if r.returncode != 0:
        return None
    # `find_server_binary()` honors `CARGO_TARGET_DIR`/the repo's own
    # `.cargo/config.toml` (`target-isolated`) -- the build above lands wherever
    # that resolves, which is NOT necessarily the hardcoded legacy `target/debug`.
    return find_server_binary()


def _launch(
    binary: str, socket_path: str, state_dir: str, persist_dir: str
) -> subprocess.Popen:
    # `persist_dir` MUST be passed through to `strict_server_env` explicitly.
    # This module launches its OWN dedicated server, independent of the shared
    # session engine in conftest.py -- but that session fixture's `os.environ.
    # update(server_env)` leaves `GRAPH_SERVICE_PERSIST_DIR` pointing at ITS OWN
    # persist dir in the ambient process environment for the rest of the pytest
    # run. Omitting `persist_dir` here means `env = {**os.environ, **strict_
    # server_env(...)}` silently inherits that ambient value instead of this
    # module's own, and the dedicated server then refuses to start ("persist dir
    # ... is already locked by another epistemic-graph engine") because the
    # still-running session engine already holds that directory's lock -- the
    # exact ambient-global-state class this repo's AGENTS.md (GOC-70) calls out.
    os.makedirs(persist_dir, exist_ok=True)
    env = {
        **os.environ,
        **strict_server_env(state_dir, auth_secret=AUTH_SECRET, persist_dir=persist_dir),
    }
    if os.path.exists(socket_path):
        os.remove(socket_path)
    proc = subprocess.Popen(
        [binary, "--socket-path", socket_path],
        cwd=RUST_DIR,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    for _ in range(120):
        if os.path.exists(socket_path):
            try:
                s = _socket.socket(_socket.AF_UNIX, _socket.SOCK_STREAM)
                s.connect(socket_path)
                s.close()
                return proc
            except OSError:
                pass
        if proc.poll() is not None:
            out, err = proc.communicate()
            raise RuntimeError(f"server exited early: {err.decode(errors='replace')}")
        time.sleep(0.5)
    raise RuntimeError("server did not come up in time")


@pytest.fixture(scope="module")
def gapfill_client(tmp_path_factory):
    binary = _build()
    if binary is None:
        pytest.skip(f'build with --features "{FEATURES}" failed in this environment')
        return  # pragma: no cover - pytest.skip is NoReturn
    runtime = tmp_path_factory.mktemp("epi-gapfill")
    socket_path = str(runtime / "engine.sock")
    proc = _launch(
        binary, socket_path, str(runtime / "security"), str(runtime / "persist")
    )
    bootstrap = SyncEpistemicGraphClient.connect(
        socket_path=socket_path,
        auth_secret=AUTH_SECRET,
        verified_context=bootstrap_context(),
    )
    try:
        bootstrap.consensus.bootstrap_system_identity(
            agent_id=TEST_AGENT_ID,
            signer_id=TEST_AGENT_ID,
            signer_key=TEST_SIGNER_KEY,
        )
    finally:
        bootstrap.close()
    client = SyncEpistemicGraphClient.connect(
        socket_path=socket_path,
        graph_name="gapfill",
        auth_secret=AUTH_SECRET,
        verified_context=request_context(),
    )
    try:
        client.tenants.create("gapfill")
        yield client
    finally:
        client.close()
        proc.terminate()
        proc.wait()
        if os.path.exists(socket_path):
            os.remove(socket_path)


# ── Gap 1: ExplainBelief.disclosure_level ────────────────────────────────────


def test_explain_belief_disclosure_level_round_trip(gapfill_client):
    client = gapfill_client
    client.nodes.add("gap1:root", {"type": "Claim", "confidence": 0.9})

    # disclosure_level=None (default) — byte-for-byte the classic un-redacted tree.
    classic = client.query.explain_belief("gap1:root")
    assert "root" in classic
    assert classic["root"]["claim"] == "gap1:root"

    # disclosure_level="Full" — same topology as the classic tree, wrapped in the
    # redacted-result envelope.
    full = client.query.explain_belief("gap1:root", disclosure_level="Full")
    assert full["level"] == "Full"
    assert full["existence"] in ("Supported", "Contradicted", "Uncertain")
    assert full["root"]["claim"] == "gap1:root"

    # disclosure_level="ExistenceOnly" — the most redacted view: no tree at all,
    # just the existence signal.
    existence_only = client.query.explain_belief(
        "gap1:root", disclosure_level="ExistenceOnly"
    )
    assert existence_only["level"] == "ExistenceOnly"
    assert existence_only["root"] is None


# ── Gap 2: the durable analytics-job plane (client.jobs / client.cancel_request) ──


def _job_state_name(state) -> str:
    """``JobState`` is externally-tagged: the bare string ``"Submitted"`` for that
    one unit variant, or ``{"VariantName": {...fields...}}`` for the rest — see
    :class:`~epistemic_graph.client.JobsClient`'s docstring."""
    if isinstance(state, str):
        return state
    assert isinstance(state, dict) and len(state) == 1, (
        f"unexpected state shape: {state!r}"
    )
    return next(iter(state))


def _mine_associate_kind() -> dict:
    return {
        "MineAssociate": {
            "transactions": [["a", "b"], ["a", "b", "c"], ["a"]],
            "min_support": 0.1,
            "min_confidence": 0.1,
            "algorithm": "fpgrowth",
        }
    }


def test_jobs_submit_status_round_trip_to_success(gapfill_client):
    client = gapfill_client

    job = client.jobs.submit("gapfill", _mine_associate_kind())
    job_id = job["job_id"]
    assert job_id
    assert _job_state_name(job["state"]) in ("Submitted", "Running", "Succeeded")

    # status: poll until the (tiny, in-process) job finishes or a bounded timeout.
    state_name = _job_state_name(job["state"])
    for _ in range(50):
        status = client.jobs.status(job_id)
        state_name = _job_state_name(status["state"])
        if state_name in ("Succeeded", "Failed", "Cancelled"):
            break
        time.sleep(0.1)
    assert state_name == "Succeeded", f"job did not succeed in time: {state_name}"

    # Both cancel and resume are explicit invalid-transition ERRORS on an
    # already-terminal job (an already-finished job's result is never
    # retroactively discarded/re-run) — proving the client surfaces the crate's
    # real guard, not silently swallowing it.
    with pytest.raises(RuntimeError, match="terminal state"):
        client.jobs.cancel(job_id)
    with pytest.raises(RuntimeError, match="resume requires"):
        client.jobs.resume(job_id)


def test_jobs_cancel_round_trip(gapfill_client):
    """A REAL successful cancel: submitted and cancelled with no delay, so the job
    is (almost always) still ``Submitted`` or freshly ``Running`` — proving the
    non-error cancel path the previous test's terminal-state case doesn't cover.
    `request_cancel` transitions a still-``Submitted`` job straight to
    ``Cancelled`` (nothing is running yet to cooperatively observe the flag); a
    job that already started stays ``Running`` with ``cancel_requested=True``
    until its executor observes the flag at its next checkpoint."""
    client = gapfill_client

    job = client.jobs.submit("gapfill", _mine_associate_kind())
    job_id = job["job_id"]
    try:
        cancelled = client.jobs.cancel(job_id)
    except RuntimeError as e:
        # Lost the race to the (tiny, fast) job's own completion — still a clean,
        # well-formed error naming the terminal-state guard, never a crash.
        assert "terminal state" in str(e)
        return
    assert cancelled["job_id"] == job_id
    state_name = _job_state_name(cancelled["state"])
    assert state_name == "Cancelled" or (
        state_name == "Running" and cancelled["cancel_requested"] is True
    ), f"expected Cancelled or Running+cancel_requested, got {cancelled!r}"


def test_cancel_request_round_trip(gapfill_client):
    client = gapfill_client
    # No request with this id is in flight — must return False, never raise
    # (cancelling a request that already finished/never existed is a no-op).
    assert client.cancel_request(999_999_999) is False


# ── Gap 3: causal observe / counterfactual (EPI-P3-6) ────────────────────────

# The SAME confounded-graph fixture eg_epistemic::causal's own unit tests use:
# Z (confounder) -> X, Z -> Y, X -> Y.
_VARIABLES = [
    {"id": "z", "parents": [], "bias": 0.0, "noise_var": 1.0},
    {"id": "x", "parents": [["z", 1.0]], "bias": 0.0, "noise_var": 0.25},
    {
        "id": "y",
        "parents": [["z", 1.0], ["x", 0.5]],
        "bias": 0.0,
        "noise_var": 0.25,
    },
]


def test_causal_estimate_observe_differs_from_intervene_under_confounding(
    gapfill_client,
):
    client = gapfill_client

    interventional = client.query.causal_estimate(
        _VARIABLES, {"x": 2.0}, mode="Intervene"
    )
    # The client default is still encoded explicitly in the current wire request.
    default_mode = client.query.causal_estimate(_VARIABLES, {"x": 2.0})
    observational = client.query.causal_estimate(_VARIABLES, {"x": 2.0}, mode="Observe")

    est_do = dict(interventional["estimates"])
    est_default = dict(default_mode["estimates"])
    est_obs = dict(observational["estimates"])

    assert est_do["x"]["mean"] == pytest.approx(2.0, abs=1e-6)
    assert est_do["x"]["variance"] == pytest.approx(0.0, abs=1e-9)
    assert est_default["x"]["mean"] == pytest.approx(est_do["x"]["mean"], abs=1e-9)
    assert est_default["y"]["mean"] == pytest.approx(est_do["y"]["mean"], abs=1e-9)

    # do(X=2): the confounder Z's edge into X is cut, so E[Y|do(X=2)] = 0.5*2 = 1.0
    # exactly (the TRUE structural effect) and Z's own belief stays at the prior 0.
    assert est_do["y"]["mean"] == pytest.approx(1.0, abs=1e-6)
    assert est_do["z"]["mean"] == pytest.approx(0.0, abs=1e-6)

    # observe(X=2): ordinary conditioning is biased by the Z->X->Y backdoor path —
    # E[Y|X=2] = (Cov(Y,X)/Var(X))*2 = (1.625/1.25)*2 = 2.6 — and conditioning on X
    # also moves belief about the (uncut) confounder Z itself.
    assert est_obs["y"]["mean"] == pytest.approx(2.6, abs=1e-6)
    assert abs(est_obs["z"]["mean"]) > 1e-6

    # The whole point of do-calculus: the two modes must disagree substantially on
    # the SAME input.
    assert abs(est_do["y"]["mean"] - est_obs["y"]["mean"]) > 1.0


def test_causal_counterfactual_matches_hand_derivation(gapfill_client):
    client = gapfill_client

    # A fully-observed unit consistent with the structural equations: z=1.0,
    # x = 1*z + noise_x (noise_x=0.5 => x=1.5), y = 1*z + 0.5*x + noise_y
    # (noise_y=0.2 => y=1.0+0.75+0.2=1.95) — the SAME fixture the crate's own
    # `counterfactual_changes_downstream_outcome_only` unit test hand-derives.
    actual = {"z": 1.0, "x": 1.5, "y": 1.95}
    result = client.query.causal_counterfactual(_VARIABLES, actual, {"x": 4.0})
    values = dict(result["values"])

    # Z is upstream of X, unaffected by the intervention: reproduces its actual
    # value exactly.
    assert values["z"] == pytest.approx(1.0, abs=1e-6)
    # X is pinned to the counterfactual value.
    assert values["x"] == pytest.approx(4.0, abs=1e-6)
    # Y is downstream: y_cf = 1*z_actual + 0.5*4.0 + noise_y(=0.2) = 3.2, using the
    # SAME abduced noise — not the actual 1.95.
    assert values["y"] == pytest.approx(3.2, abs=1e-6)
    assert abs(values["y"] - actual["y"]) > 1.0


def test_causal_counterfactual_requires_a_fully_observed_unit(gapfill_client):
    client = gapfill_client
    with pytest.raises(RuntimeError, match="fully-observed unit"):
        client.query.causal_counterfactual(_VARIABLES, {"z": 1.0}, {"x": 4.0})


# ── Gap 5: standalone Dung argumentation conflict resolution (EPI-P3-7) ─────


# The SAME textbook mutual-conflict AF eg_epistemic::tms's own unit tests use:
# "gap5:a" <-> "gap5:b" (symmetric ATTACKS, unresolved conflict), "gap5:c" unattacked.
def _seed_conflict_graph(client):
    client.nodes.add("gap5:a", {"type": "Claim", "confidence": 0.5})
    client.nodes.add("gap5:b", {"type": "Claim", "confidence": 0.5})
    client.nodes.add("gap5:c", {"type": "Claim", "confidence": 0.9})
    client.edges.add("gap5:a", "gap5:b", {"relationship": "ATTACKS"})
    client.edges.add("gap5:b", "gap5:a", {"relationship": "ATTACKS"})


def test_resolve_conflict_grounded_is_paraconsistent(gapfill_client):
    client = gapfill_client
    _seed_conflict_graph(client)

    result = client.query.resolve_conflict(
        ["gap5:a", "gap5:b", "gap5:c"], semantics="grounded"
    )
    assert result["semantics"] == "grounded"
    assert result["surviving"] == ["gap5:c"]
    assert sorted(result["undecided"]) == ["gap5:a", "gap5:b"]
    assert result["defeated"] == []
    assert len(result["extension_sets"]) == 1
    assert result["extension_sets"][0] == ["gap5:c"]

    # Default omitted (mode entirely unset) must be byte-for-byte "grounded".
    default_result = client.query.resolve_conflict(["gap5:a", "gap5:b", "gap5:c"])
    assert default_result == result


def test_resolve_conflict_preferred_resolves_credulously(gapfill_client):
    client = gapfill_client
    _seed_conflict_graph(client)

    result = client.query.resolve_conflict(
        ["gap5:a", "gap5:b", "gap5:c"], semantics="preferred"
    )
    assert result["surviving"] == ["gap5:c"]
    assert sorted(result["undecided"]) == ["gap5:a", "gap5:b"]
    assert result["defeated"] == []
    # The two maximal admissible sets {a,c} and {b,c}.
    assert len(result["extension_sets"]) == 2


def test_resolve_conflict_unknown_semantics_raises(gapfill_client):
    client = gapfill_client
    _seed_conflict_graph(client)
    with pytest.raises(RuntimeError, match="grounded|preferred|stable"):
        client.query.resolve_conflict(["gap5:a"], semantics="bogus")
