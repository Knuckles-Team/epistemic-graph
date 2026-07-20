"""CI entry point for the lazy lifecycle static authority gate."""

from __future__ import annotations

import importlib.util
from pathlib import Path


def test_lazy_lifecycle_architecture_gate() -> None:
    root = Path(__file__).resolve().parents[1]
    gate_path = root / "scripts" / "check_lazy_lifecycle_architecture.py"
    spec = importlib.util.spec_from_file_location("lazy_lifecycle_gate", gate_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.main()
