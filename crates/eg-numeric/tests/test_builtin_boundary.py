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
