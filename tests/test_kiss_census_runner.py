from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

ROOT = Path(__file__).resolve().parents[1]


def _module() -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        "run_kiss_census", ROOT / "scripts" / "run_kiss_census.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_package_root_groups_facade_and_crates() -> None:
    module = _module()
    assert module.package_root(Path("src/lib.rs")) == Path("src")
    assert module.package_root(Path("crates/eg-types/src/lib.rs")) == Path(
        "crates/eg-types"
    )


def test_package_roots_are_derived_from_declared_sources(tmp_path: Path) -> None:
    module = _module()
    repository = tmp_path / "repository"
    paths = [Path("src/lib.rs"), Path("crates/demo/src/lib.rs")]
    for path in paths:
        source = repository / path
        source.parent.mkdir(parents=True, exist_ok=True)
        source.write_text(f"// {path}\n", encoding="utf-8")
    roots = module.package_roots(repository, paths)

    assert roots == [Path("crates/demo"), Path("src")]
