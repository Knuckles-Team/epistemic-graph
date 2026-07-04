"""numpy-parity CI gate for the compiled ``eg-numeric`` kernel (CONCEPT:EG-346).

Unlike the agent-utilities corpus (``tests/test_numeric_parity.py``, KG-2.312) which
exercises the ``xp`` *shim* (kernel OR numpy fallback), this test is **engine-side and
self-contained**: it imports the compiled Surface-A extension DIRECTLY and asserts every
kernel op equals its numpy reference (``np.allclose``), including the mandatory edge cases
(nan/inf, singular matrix, empty). It is the gate that FAILS CI if the Rust kernel ever
diverges from numpy — so the ``xp`` shim can be made kernel-LIVE (CONCEPT:KG-2.315) with a
standing correctness guarantee, not just the numpy fallback.

Run against the freshly-built wheel::

    PYO3_USE_ABI3_FORWARD_COMPATIBILITY=1 \
      maturin build --release -m crates/eg-numeric/Cargo.toml --features python
    pip install target/wheels/eg_numeric-*.whl
    pytest crates/eg-numeric/tests/test_kernel_parity.py --noconftest -q

The module is discovered as ``epistemic_graph.numeric`` (folded build) or ``numeric``
(standalone wheel) — the same two names the ``xp`` shim probes. If neither imports the
test is SKIPPED (so a no-wheel checkout is green), but the CI job builds+installs the wheel
first, so in CI it always runs the real kernel.
"""

from __future__ import annotations

import importlib

import numpy as np
import pytest

_k = None
for _name in ("epistemic_graph.numeric", "numeric"):
    try:
        _m = importlib.import_module(_name)
    except Exception:
        continue
    if getattr(_m, "__kernel__", None) == "eg-numeric":
        _k = _m
        break

pytestmark = pytest.mark.skipif(
    _k is None, reason="eg-numeric kernel wheel not installed (build with maturin --features python)"
)


def _close(a, b, atol=1e-6, rtol=1e-6):
    return np.allclose(
        np.asarray(a, float), np.asarray(b, float), atol=atol, rtol=rtol, equal_nan=True
    )


# --------------------------------------------------------------------------- #
# reductions / stats
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("seed", range(8))
def test_reductions(seed):
    rng = np.random.default_rng(seed)
    a = rng.normal(0, 5, int(rng.integers(2, 40))).astype(np.float64)
    assert _close(_k.sum(a), np.sum(a))
    assert _close(_k.prod(a), np.prod(a))
    assert _close(_k.mean(a), np.mean(a))
    assert _close(_k.std(a, ddof=0), np.std(a))
    assert _close(_k.std(a, ddof=1), np.std(a, ddof=1))
    assert _close(_k.var(a, ddof=0), np.var(a))
    assert _close(_k.var(a, ddof=1), np.var(a, ddof=1))
    assert _close(_k.amin(a), np.min(a))
    assert _close(_k.amax(a), np.max(a))
    assert _k.argmin(a) == int(np.argmin(a))
    assert _k.argmax(a) == int(np.argmax(a))
    assert _close(_k.argsort(a), np.argsort(a, kind="stable"))
    assert _close(_k.cumsum(a), np.cumsum(a))
    assert _close(_k.cumprod(a), np.cumprod(a))
    for q in (0.0, 25.0, 50.0, 90.0, 100.0):
        assert _close(_k.percentile(a, q), np.percentile(a, q))
    for q in (0.1, 0.5, 0.99):
        assert _close(_k.quantile(a, q), np.quantile(a, q))


# --------------------------------------------------------------------------- #
# element-wise
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("seed", range(6))
def test_elementwise(seed):
    rng = np.random.default_rng(100 + seed)
    a = rng.normal(0, 3, int(rng.integers(2, 30))).astype(np.float64)
    b = rng.normal(0, 3, a.size).astype(np.float64)
    pos = np.abs(a) + 0.01
    assert _close(_k.sqrt(pos), np.sqrt(pos))
    assert _close(_k.log(pos), np.log(pos))
    assert _close(_k.exp(a), np.exp(a))
    assert _close(_k.absolute(a), np.abs(a))
    assert _close(_k.tanh(a), np.tanh(a))
    assert _close(_k.clip(a, -1.0, 1.0), np.clip(a, -1.0, 1.0))
    assert _close(_k.maximum(a, b), np.maximum(a, b))
    assert _close(_k.minimum(a, b), np.minimum(a, b))
    assert _close(
        _k.where_((a > 0).tolist(), a, b), np.where(a > 0, a, b)
    )
    assert np.array_equal(np.asarray(_k.isnan(a)), np.isnan(a))


def test_elementwise_edge_nan_inf():
    edge = np.array([np.nan, np.inf, -np.inf, 0.0, -2.5, 7.0], dtype=np.float64)
    assert _close(
        _k.nan_to_num(edge, 0.0, 1e300, -1e300),
        np.nan_to_num(edge, nan=0.0, posinf=1e300, neginf=-1e300),
    )
    assert np.array_equal(np.asarray(_k.isnan(edge)), np.isnan(edge))
    assert _close(_k.absolute(edge), np.abs(edge))
    z = np.zeros_like(edge)
    assert _close(_k.maximum(edge, z), np.maximum(edge, z))
    assert _k.argmin(edge) == int(np.argmin(edge))
    assert _k.argmax(edge) == int(np.argmax(edge))


def test_empty_edges():
    # mean of empty is nan (numpy parity); min/max reject empty with LinAlgError.
    assert np.isnan(_k.mean(np.array([], dtype=np.float64)))
    with pytest.raises((ValueError, _k.LinAlgError)):
        _k.amin(np.array([], dtype=np.float64))
    with pytest.raises((ValueError, _k.LinAlgError)):
        _k.amax(np.array([], dtype=np.float64))


def test_single_element():
    assert _k.amin(np.array([3.0])) == 3.0
    assert _k.amax(np.array([3.0])) == 3.0


# --------------------------------------------------------------------------- #
# linalg — reconstruction for factorizations (factor signs/bases are
# implementation-defined), exact for solve/det/inv/norm/dot.
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("seed", range(8))
def test_linalg(seed):
    rng = np.random.default_rng(200 + seed)
    n = int(rng.integers(2, 7))
    m = n + int(rng.integers(0, 4))
    A = rng.normal(0, 2, (m, n)).astype(np.float64)
    Sq = (rng.normal(0, 2, (n, n)) + n * np.eye(n)).astype(np.float64)
    x = rng.normal(0, 2, n).astype(np.float64)
    v = rng.normal(0, 2, n).astype(np.float64)

    assert _close(_k.norm(v), np.linalg.norm(v))
    assert _close(_k.norm_ord(v, 1.0), np.linalg.norm(v, 1))
    assert _close(_k.norm_ord(v, np.inf), np.linalg.norm(v, np.inf))
    assert _close(_k.dot(v, x), np.dot(v, x))

    b = Sq @ x
    assert _close(_k.solve(Sq, b), np.linalg.solve(Sq, b))

    B = rng.normal(0, 2, (n, n)).astype(np.float64)
    assert _close(_k.matmul(Sq, B), Sq @ B)

    assert _close(_k.svdvals(A), np.linalg.svd(A, compute_uv=False))
    U, s, Vt = _k.svd(A)
    assert _close(U[:, : len(s)] @ np.diag(s) @ Vt[: len(s), :], A)

    S = (Sq + Sq.T) / 2
    w, V = _k.eigh(S)
    assert _close(w, np.linalg.eigvalsh(S))
    assert _close(V @ np.diag(w) @ V.T, S)

    assert _close(_k.pinv(A), np.linalg.pinv(A))

    bt = rng.normal(0, 2, m).astype(np.float64)
    assert _close(_k.lstsq(A, bt), np.linalg.lstsq(A, bt, rcond=None)[0])

    Q, R = _k.qr(A)
    assert _close(Q @ R, A)

    SPD = (S @ S.T + n * np.eye(n)).astype(np.float64)
    L = _k.cholesky(SPD)
    assert _close(L @ L.T, SPD)

    assert _close(_k.det(Sq), np.linalg.det(Sq))
    assert _close(_k.inv(Sq), np.linalg.inv(Sq))
    for p in (0, 1, 3, -2):
        assert _close(_k.matrix_power(Sq, p), np.linalg.matrix_power(Sq, p), atol=1e-5)


def test_linalg_singular_raises():
    sing = np.array([[1.0, 2.0], [2.0, 4.0]])
    with pytest.raises(_k.LinAlgError):
        _k.solve(sing, np.array([1.0, 2.0]))
    with pytest.raises(_k.LinAlgError):
        _k.inv(sing)
    with pytest.raises(_k.LinAlgError):
        _k.cholesky(np.array([[1.0, 2.0], [2.0, 1.0]]))  # not positive-definite


# --------------------------------------------------------------------------- #
# random — determinism (seed-reproducible) + distributional parity.
# --------------------------------------------------------------------------- #
def test_random_determinism_and_distribution():
    a1 = _k.normal(0.0, 1.0, 100000, 42)
    a2 = _k.normal(0.0, 1.0, 100000, 42)
    assert np.array_equal(a1, a2)  # same seed → identical stream
    assert abs(float(np.mean(a1))) < 0.02
    assert abs(float(np.std(a1)) - 1.0) < 0.02
    u = _k.uniform(-1.0, 1.0, 100000, 7)
    assert u.min() >= -1.0 and u.max() <= 1.0
    ints = _k.integers(0, 10, 10000, 5)
    assert ints.min() >= 0 and ints.max() < 10


def test_kernel_marker():
    assert _k.__kernel__ == "eg-numeric"
