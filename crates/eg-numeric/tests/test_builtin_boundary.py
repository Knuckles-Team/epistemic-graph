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
    for name in ("epistemic_graph.numeric", "numeric"):
        try:
            module = importlib.import_module(name)
        except ImportError:
            continue
        if getattr(module, "__kernel__", None) == "eg-numeric":
            return module
    pytest.skip("eg-numeric Surface-A wheel is not installed")


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
