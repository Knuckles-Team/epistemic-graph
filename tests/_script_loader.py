"""Import a `scripts/*.py` gate as a module, for tests of the gates themselves."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

ROOT = Path(__file__).resolve().parents[1]


def load_script(name: str) -> ModuleType:
    """Load `scripts/<name>.py` under its own module name (registered in
    `sys.modules`, so dataclasses and pickling inside it resolve)."""
    path = ROOT / "scripts" / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module
