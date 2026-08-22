"""Focused contract for the NumPy-free Surface-A Python boundary."""

from __future__ import annotations

import importlib
import os
import subprocess
import sys
from typing import Any

import pytest

pytestmark = pytest.mark.no_engine


def _kernel() -> Any:
    # ONLY the folded-in, natively injected extension — there is no separately
    # published/installed `eg-numeric` package (CONCEPT:EG-346). A bare top-level
    # `numeric` import only resolves when `eg-numeric` was installed standalone,
    # which is the exact architectural remnant this contract must never validate
    # against: that package's version is permanently `0.1.0`, which let a stale
    # six-week-old build masquerade as "already satisfied" and silently mask a
    # real regression (see scripts/ci_gate_replica.py's 2026-08-21 incident note).
    try:
        module = importlib.import_module("epistemic_graph.numeric")
    except ImportError:
        pytest.skip("epistemic_graph.numeric is not installed (folded wheel missing)")
    if getattr(module, "__kernel__", None) != "eg-numeric":
        pytest.skip("epistemic_graph.numeric is not installed (folded wheel missing)")
    return module


def test_import_and_native_calls_succeed_with_numpy_import_blocked() -> None:
    """A fresh interpreter proves module initialization never imports NumPy."""

    module = _kernel()
    module_dir = os.path.dirname(module.__file__)
    code = r"""
import builtins
import importlib

real_import = builtins.__import__
def deny_numpy(name, *args, **kwargs):
    if name == "numpy" or name.startswith("numpy."):
        raise ModuleNotFoundError("NumPy is forbidden at the native boundary")
    return real_import(name, *args, **kwargs)

builtins.__import__ = deny_numpy
kernel = importlib.import_module("numeric")
assert kernel.__kernel__ == "eg-numeric"
assert kernel.sum([1, 2, 3, 4]) == 10.0
assert kernel.mean([[1, 2], [3, 4]], axis=0) == [2.0, 3.0]
assert kernel.sqrt([1, 4, 9]) == [1.0, 2.0, 3.0]
assert kernel.solve([[3, 2], [1, 2]], [7, 5]) == [1.0, 2.0]
assert kernel.isnan([1.0, float("nan")]) == [False, True]
assert kernel.nan_to_num(
    [[1.0, float("nan")], [float("inf"), float("-inf")]],
    nan=0.0,
    posinf=9.0,
    neginf=-9.0,
) == [[1.0, 0.0], [9.0, -9.0]]
"""
    env = dict(os.environ)
    env["PYTHONPATH"] = module_dir
    subprocess.run([sys.executable, "-c", code], env=env, check=True)


def test_builtin_boundary_rejects_unsafe_shapes_and_types() -> None:
    kernel = _kernel()

    assert kernel.sum(3) == 3.0
    assert kernel.sum([]) == 0.0
    with pytest.raises(ValueError, match="rectangular"):
        kernel.sum([[1, 2], [3]])
    with pytest.raises(ValueError, match="text, bytes, or a mapping"):
        kernel.sum("123")
    with pytest.raises(ValueError, match="text, bytes, or a mapping"):
        kernel.sum(b"123")
    with pytest.raises(ValueError, match="text, bytes, or a mapping"):
        kernel.sum({"value": 1})

    over_rank: Any = 1
    for _ in range(9):
        over_rank = [over_rank]
    with pytest.raises(ValueError, match="rank-8"):
        kernel.sum(over_rank)

    with pytest.raises(ValueError, match="element limit"):
        kernel.sum(range(1_000_001))


def test_native_outputs_are_bounded_before_allocation() -> None:
    kernel = _kernel()

    for function, args in (
        (kernel.normal, (0.0, 1.0, 1_000_001, 1)),
        (kernel.uniform, (0.0, 1.0, 1_000_001, 1)),
        (kernel.integers, (0, 10, 1_000_001, 1)),
    ):
        with pytest.raises(ValueError, match="output size"):
            function(*args)

    with pytest.raises(ValueError, match="condition"):
        kernel.where_(range(1_000_001), [1.0], [2.0])
    with pytest.raises(ValueError, match="k exceeds"):
        kernel.kmeans([[1.0, 2.0]], 1_000_001)
    with pytest.raises(ValueError, match="max_iter exceeds"):
        kernel.kmeans([[1.0, 2.0]], 1, 10_001)


def test_native_random_rejects_invalid_parameters_without_panicking() -> None:
    kernel = _kernel()

    with pytest.raises(ValueError, match="normal"):
        kernel.normal(0.0, -1.0, 1, 1)
    with pytest.raises(ValueError, match="uniform"):
        kernel.uniform(1.0, 1.0, 1, 1)
    with pytest.raises(ValueError, match="integers"):
        kernel.integers(2, 2, 1, 1)


def test_native_choice_and_permutation_indices_are_bounded_batches() -> None:
    kernel = _kernel()

    assert kernel.choice_indices(0, 0, True, None, 7) == []
    sampled = kernel.choice_indices(32, 16, False, None, 7)
    assert len(sampled) == 16
    assert len(set(sampled)) == 16
    assert all(0 <= index < 32 for index in sampled)

    weighted = kernel.choice_indices(4, 128, True, [0.0, 1.0, 3.0, 0.0], 7)
    assert set(weighted) <= {1, 2}

    permutation = kernel.permutation_indices(32, 7)
    assert sorted(permutation) == list(range(32))

    with pytest.raises(ValueError, match="weights"):
        kernel.choice_indices(2, 1, True, [float("nan"), 1.0], 7)
    with pytest.raises(ValueError, match="weights"):
        kernel.choice_indices(2, 1, True, [0.0, 0.0], 7)
    with pytest.raises(ValueError, match="population"):
        kernel.choice_indices(2, 3, False, None, 7)
    with pytest.raises(ValueError, match="output size"):
        kernel.choice_indices(2, 1_000_001, True, None, 7)
    with pytest.raises(ValueError, match="population size"):
        kernel.choice_indices(1_000_001, 1, True, None, 7)


# ── NE-249: array construction, natively, with no NumPy ──────────────────────
#
# These names used to exist on this module only because it did
# `m.add(name, numpy.getattr(name))`. `b7d5825` removed that passthrough --
# correctly, the kernel must not import NumPy -- but did not reimplement the
# surface, so parity was lost as collateral rather than as a decision. The
# tests below pin the restored behaviour AND the property that made the
# passthrough wrong in the first place: every result is a plain builtin, never
# an array object.


def _is_builtin_tree(value: Any) -> bool:
    """True when `value` is scalars and lists all the way down."""

    if isinstance(value, bool) or isinstance(value, (int, float)):
        return True
    if isinstance(value, list):
        return all(_is_builtin_tree(item) for item in value)
    return False


def test_construction_returns_builtin_lists_never_an_array_object() -> None:
    kernel = _kernel()

    for produced in (
        kernel.zeros(3),
        kernel.ones((2, 2)),
        kernel.full((2,), 7.0),
        kernel.eye(3),
        kernel.arange(5),
        kernel.linspace(0.0, 1.0, 5),
        kernel.array([[1, 2], [3, 4]]),
    ):
        assert _is_builtin_tree(produced), produced
        assert type(produced).__module__ == "builtins"


def test_constructors_match_numpy_semantics() -> None:
    kernel = _kernel()

    assert kernel.zeros(3) == [0.0, 0.0, 0.0]
    assert kernel.zeros((2, 2)) == [[0.0, 0.0], [0.0, 0.0]]
    assert kernel.ones((2, 3)) == [[1.0] * 3] * 2
    assert kernel.full((3,), 2.5) == [2.5, 2.5, 2.5]
    # `empty` is deliberately zero-filled here, NOT uninitialized: these values
    # cross into Python as real list elements, so "whatever was in the
    # allocation" would publish arbitrary heap contents.
    assert kernel.empty((2,)) == [0.0, 0.0]
    assert kernel.eye(2) == [[1.0, 0.0], [0.0, 1.0]]
    assert kernel.eye(2, 3) == [[1.0, 0.0, 0.0], [0.0, 1.0, 0.0]]
    assert kernel.eye(3, k=1) == [
        [0.0, 1.0, 0.0],
        [0.0, 0.0, 1.0],
        [0.0, 0.0, 0.0],
    ]


def test_arange_follows_the_one_argument_is_stop_convention() -> None:
    kernel = _kernel()

    assert kernel.arange(4) == [0.0, 1.0, 2.0, 3.0]
    assert kernel.arange(1, 4) == [1.0, 2.0, 3.0]
    assert kernel.arange(0, 1, 0.25) == [0.0, 0.25, 0.5, 0.75]
    assert kernel.arange(4, 0) == []
    with pytest.raises(ValueError, match="non-zero"):
        kernel.arange(0, 5, 0)


def test_linspace_endpoint_behaviour() -> None:
    kernel = _kernel()

    assert kernel.linspace(0.0, 1.0, 5) == [0.0, 0.25, 0.5, 0.75, 1.0]
    assert kernel.linspace(0.0, 1.0, 4, endpoint=False) == [0.0, 0.25, 0.5, 0.75]
    assert kernel.linspace(2.0, 3.0, 1) == [2.0]
    assert kernel.linspace(0.0, 1.0, 0) == []


def test_array_validates_rather_than_passing_through() -> None:
    """`array` is not an identity function: it enforces the same
    rectangularity, rank and element ceilings every other op enforces, so a
    caller that round-trips through it holds something the whole surface
    accepts."""

    kernel = _kernel()

    assert kernel.array([1, 2, 3]) == [1.0, 2.0, 3.0]
    assert kernel.asarray([[1, 2], [3, 4]]) == [[1.0, 2.0], [3.0, 4.0]]
    with pytest.raises(ValueError, match="rectangular"):
        kernel.array([[1, 2], [3]])
    with pytest.raises(ValueError, match="text, bytes, or a mapping"):
        kernel.array("nope")


def test_isclose_matches_numpy_tolerance_and_nan_rules() -> None:
    kernel = _kernel()

    assert kernel.isclose([1.0, 2.0], [1.0, 2.0000000001]) == [True, True]
    assert kernel.isclose([1.0], [1.5]) == [False]
    assert kernel.isclose([float("nan")], [float("nan")]) == [False]
    assert kernel.isclose([float("nan")], [float("nan")], equal_nan=True) == [True]
    assert kernel.isclose([float("inf")], [float("inf")]) == [True]
    assert kernel.isclose([float("inf")], [float("-inf")]) == [False]
    with pytest.raises(ValueError, match="shape mismatch"):
        kernel.isclose([1.0, 2.0], [1.0])


def test_shape_manipulation_round_trips() -> None:
    kernel = _kernel()

    assert kernel.reshape([1, 2, 3, 4], (2, 2)) == [[1.0, 2.0], [3.0, 4.0]]
    assert kernel.concatenate([[1, 2], [3, 4]]) == [1.0, 2.0, 3.0, 4.0]
    assert kernel.concatenate([[[1, 2]], [[3, 4]]], axis=0) == [
        [1.0, 2.0],
        [3.0, 4.0],
    ]
    assert kernel.stack([[1, 2], [3, 4]]) == [[1.0, 2.0], [3.0, 4.0]]
    # 1-D inputs promote to single rows, matching NumPy.
    assert kernel.vstack([[1, 2], [3, 4]]) == [[1.0, 2.0], [3.0, 4.0]]
    with pytest.raises(ValueError, match="cannot reshape"):
        kernel.reshape([1, 2, 3], (2, 2))


def test_diag_is_overloaded_both_ways_like_numpy() -> None:
    kernel = _kernel()

    assert kernel.diag([1, 2]) == [[1.0, 0.0], [0.0, 2.0]]
    assert kernel.diag([[1, 2], [3, 4]]) == [1.0, 4.0]
    assert kernel.diag([[1, 2], [3, 4]], k=1) == [2.0]
    with pytest.raises(ValueError, match="1- or 2-dimensional"):
        kernel.diag([[[1]]])


def test_fill_diagonal_returns_a_new_value_instead_of_mutating() -> None:
    """NumPy's `fill_diagonal` mutates in place. A nested list that crossed
    this boundary is a copy, so in-place mutation could not be observed by the
    caller -- returning the result makes that explicit rather than silently
    doing nothing."""

    kernel = _kernel()

    original = [[1.0, 2.0], [3.0, 4.0]]
    result = kernel.fill_diagonal(original, 0.0)

    assert result == [[0.0, 2.0], [3.0, 0.0]]
    assert original == [[1.0, 2.0], [3.0, 4.0]]


def test_diff_and_sort() -> None:
    kernel = _kernel()

    assert kernel.diff([1, 2, 4, 7]) == [1.0, 2.0, 3.0]
    assert kernel.diff([1, 2, 4, 7], n=2) == [1.0, 1.0]
    assert kernel.diff([[1, 3], [2, 8]], axis=0) == [[1.0, 5.0]]
    assert kernel.sort([3, 1, 2]) == [1.0, 2.0, 3.0]
    assert kernel.sort([[3, 1], [2, 0]]) == [[1.0, 3.0], [0.0, 2.0]]
    assert kernel.sort([[3, 1], [2, 0]], axis=0) == [[2.0, 0.0], [3.0, 1.0]]


def test_constants_are_plain_floats() -> None:
    kernel = _kernel()

    import math

    assert kernel.pi == math.pi
    assert kernel.inf == math.inf
    assert math.isnan(kernel.nan)
    for constant in (kernel.pi, kernel.inf, kernel.nan):
        assert type(constant) is float


def test_construction_is_bounded_like_every_other_input() -> None:
    """A shape argument must not be the one input that allocates without a
    ceiling."""

    kernel = _kernel()

    with pytest.raises(ValueError, match="element limit"):
        kernel.zeros((100_000, 100_000))
    with pytest.raises(ValueError, match="non-negative"):
        kernel.zeros(-1)
    with pytest.raises(ValueError, match="rank-8"):
        kernel.zeros((1,) * 9)
    with pytest.raises(ValueError, match="integers"):
        kernel.zeros(("two",))
