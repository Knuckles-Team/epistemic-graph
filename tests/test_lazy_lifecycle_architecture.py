"""CI entry point for the lazy lifecycle static authority gate."""

from __future__ import annotations

import importlib.util
from pathlib import Path
import pytest

# Pure/static test -- never needs the shared native engine (see
# conftest.py's session-scoped `start_epistemic_graph_server` fixture,
# which this marker exempts this module from triggering).
pytestmark = pytest.mark.no_engine


def test_lazy_lifecycle_architecture_gate() -> None:
    root = Path(__file__).resolve().parents[1]
    gate_path = root / "scripts" / "check_lazy_lifecycle_architecture.py"
    spec = importlib.util.spec_from_file_location("lazy_lifecycle_gate", gate_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.main()
