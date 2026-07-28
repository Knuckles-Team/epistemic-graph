"""PEP 517 builds must compose the required numeric native module."""

from __future__ import annotations

import importlib.util
import sys
import zipfile
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
_SPEC = importlib.util.spec_from_file_location(
    "build_backend", REPO / "build_backend.py"
)
assert _SPEC is not None and _SPEC.loader is not None
build_backend = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = build_backend
_SPEC.loader.exec_module(build_backend)


def _wheel(path: Path, member: str, payload: bytes) -> None:
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(member, payload)
        archive.writestr("epistemic_graph-0.dist-info/RECORD", b"")


def test_build_backend_folds_numeric_wheel(monkeypatch, tmp_path: Path) -> None:
    server = tmp_path / "epistemic_graph-0-py3-none-any.whl"
    numeric = tmp_path / "numeric.whl"
    _wheel(server, "epistemic_graph-0.dist-info/METADATA", b"Name: epistemic-graph\n")
    _wheel(numeric, "numeric/numeric.abi3.so", b"native-kernel")

    monkeypatch.setattr(build_backend, "_build_numeric", lambda _: numeric)

    def build(*_args: object) -> str:
        return server.name

    assert build_backend._compose(build, str(tmp_path), None, None) == server.name
    with zipfile.ZipFile(server) as archive:
        assert archive.read("epistemic_graph/numeric.abi3.so") == b"native-kernel"
