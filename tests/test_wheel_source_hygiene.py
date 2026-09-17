"""Maturin source selection must not publish Python interpreter caches."""

from __future__ import annotations

import ast
import base64
import hashlib
import json
import os
import shutil
import subprocess
import zipfile
from pathlib import Path

import pytest
import tomllib

from scripts.check_wheel_completeness import check_wheel
from scripts.check_wheel_privacy import audit_wheel
from scripts.inject_numeric_kernel import inject
from scripts.normalize_wheel_sbom import normalize_wheel

pytestmark = pytest.mark.no_engine

REPO = Path(__file__).resolve().parents[1]
EXPECTED_EXCLUDES = {"**/__pycache__/**", "**/*.pyc", "**/*.pyo"}


def _imported_module_names(node: ast.AST) -> list[str] | None:
    """The module names one ``import``/``from`` statement binds, or ``None``
    if `node` is not an import statement."""
    if isinstance(node, ast.Import):
        return [alias.name for alias in node.names]
    if isinstance(node, ast.ImportFrom):
        return [node.module or ""]
    return None


def _numpy_import_findings(path: Path) -> list[str]:
    """``file:line`` locations in `path` that import ``numpy`` or a
    ``numpy.*`` submodule."""
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    findings = []
    for node in ast.walk(tree):
        if not isinstance(node, (ast.Import, ast.ImportFrom)):
            continue
        names = _imported_module_names(node)
        assert names is not None
        if any(name == "numpy" or name.startswith("numpy.") for name in names):
            findings.append(f"{path.relative_to(REPO)}:{node.lineno}")
    return findings


def test_shipped_python_surface_has_no_numpy_runtime_imports() -> None:
    """The native kernel is the only numeric runtime; Python source stays
    stdlib-only."""

    findings = [
        finding
        for path in sorted((REPO / "epistemic_graph").rglob("*.py"))
        for finding in _numpy_import_findings(path)
    ]
    assert not findings, "NumPy runtime imports in shipped source: " + ", ".join(
        findings
    )


def _recorded_numeric_wheel(path: Path) -> None:
    """Create the smallest component wheel accepted by the real injector."""

    extension = "numeric/numeric.abi3.so"
    payload = b"synthetic numeric payload"
    digest = base64.urlsafe_b64encode(hashlib.sha256(payload).digest())
    record = (
        f"{extension},sha256={digest.decode().rstrip('=')},{len(payload)}\n"
        "numeric-0.dist-info/RECORD,,\n"
    )
    with zipfile.ZipFile(path, "w", zipfile.ZIP_DEFLATED) as archive:
        info = zipfile.ZipInfo(extension)
        info.external_attr = 0o100755 << 16
        archive.writestr(info, payload)
        archive.writestr("numeric-0.dist-info/RECORD", record.encode())


def _seed_python_package(root: Path) -> dict[Path, bytes]:
    package = root / "epistemic_graph"
    source_files = {
        package / "__init__.py": b'"""fixture package"""\n',
        package / "client.py": b"def health() -> str:\n    return 'ok'\n",
        package / "kvcache" / "__init__.py": b"",
        package / "skills" / "kg-modality-sql" / "SKILL.md": b"# fixture\n",
        package / "nested" / "source.py": b"VALUE = 1\n",
    }
    for path, payload in source_files.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(payload)

    caches = (
        package / "__pycache__",
        package / "nested" / "deep" / "__pycache__",
    )
    for cache in caches:
        cache.mkdir(parents=True)
        for version in ("310", "311", "312", "313"):
            for suffix in ("pyc", "pyo"):
                (cache / f"source.cpython-{version}.{suffix}").write_bytes(
                    f"compiled-{version}-{suffix}".encode()
                )
    (package / "root.pyc").write_bytes(b"root-pyc")
    (package / "nested" / "root.pyo").write_bytes(b"root-pyo")
    return {path: path.read_bytes() for path in root.rglob("*") if path.is_file()}


def _fixture_pyproject(root_config: dict[str, object], root: Path) -> None:
    tool = root_config["tool"]
    assert isinstance(tool, dict)
    maturin = tool["maturin"]
    assert isinstance(maturin, dict)
    excludes = maturin["exclude"]
    assert set(excludes) == EXPECTED_EXCLUDES
    root.joinpath("pyproject.toml").write_text(
        "[build-system]\n"
        'requires = ["maturin>=1.0,<2.0"]\n'
        'build-backend = "maturin"\n\n'
        "[project]\n"
        'name = "epistemic-graph"\n'
        'version = "0.0.0"\n'
        'requires-python = ">=3.10"\n'
        'dependencies = ["msgpack>=1.2.1"]\n\n'
        "[tool.maturin]\n"
        'python-source = "."\n'
        'module-name = "epistemic_graph"\n'
        'bindings = "bin"\n'
        "strip = true\n"
        f"exclude = {json.dumps(excludes)}\n",
        encoding="utf-8",
    )


def _skip_unless_maturin_and_cargo_available() -> None:
    if shutil.which("maturin") is None or shutil.which("cargo") is None:
        pytest.skip("Maturin and Cargo are required for the source-selection fixture")


def _seed_fixture_crate(
    fixture: Path, root_config: dict[str, object]
) -> dict[Path, bytes]:
    """Seed the fixture Python package + pyproject + a minimal Cargo bin crate.
    Returns the pre-build byte snapshot used to prove Maturin leaves fixture
    sources untouched."""
    fixture.mkdir()
    before = _seed_python_package(fixture)
    _fixture_pyproject(root_config, fixture)
    (fixture / "Cargo.toml").write_text(
        '[package]\nname = "epistemic-graph"\nversion = "0.0.0"\nedition = "2021"\n\n'
        '[[bin]]\nname = "epistemic-graph-server"\npath = "src/main.rs"\n',
        encoding="utf-8",
    )
    source = fixture / "src" / "main.rs"
    source.parent.mkdir()
    source.write_text("fn main() {}\n", encoding="utf-8")
    return before


def _build_server_wheel(fixture: Path, dist: Path) -> Path:
    environment = {**os.environ, "PYTHONDONTWRITEBYTECODE": "1"}
    subprocess.run(
        [
            "maturin",
            "build",
            "--release",
            "--manifest-path",
            str(fixture / "Cargo.toml"),
            "--out",
            str(dist),
        ],
        cwd=fixture,
        env=environment,
        check=True,
        capture_output=True,
        text=True,
    )
    server_wheels = sorted(dist.glob("epistemic_graph-*.whl"))
    assert len(server_wheels) == 1
    return server_wheels[0]


def _has_bytecode_cache_entry(names: list[str]) -> bool:
    return any(
        "__pycache__" in name or name.endswith((".pyc", ".pyo")) for name in names
    )


def _assert_built_wheel_is_clean_and_complete(server_wheel: Path) -> None:
    with zipfile.ZipFile(server_wheel) as archive:
        names = archive.namelist()
    assert "epistemic_graph/__init__.py" in names
    assert "epistemic_graph/client.py" in names
    assert any(name.endswith("/epistemic-graph-server") for name in names), names
    assert not _has_bytecode_cache_entry(names)


def _inject_numeric_wheel(dist: Path, server_wheel: Path) -> None:
    numeric_wheel = dist / "numeric-0-py3-none-any.whl"
    _recorded_numeric_wheel(numeric_wheel)
    inject(server_wheel, numeric_wheel)


def _assert_injected_wheel_is_clean_and_complete(server_wheel: Path) -> None:
    with zipfile.ZipFile(server_wheel) as archive:
        names = archive.namelist()
        server_entries = [
            info
            for info in archive.infolist()
            if info.filename.endswith("/epistemic-graph-server")
        ]
        assert "epistemic_graph/numeric.abi3.so" in names
        assert "epistemic_graph/client.py" in names
        assert server_entries and (server_entries[0].external_attr >> 16) & 0o100
        assert not _has_bytecode_cache_entry(names)


def _assert_release_composition_gates_pass(
    server_wheel: Path, fixture: Path, before: dict[Path, bytes]
) -> None:
    # Release composition normalizes Maturin's SBOM path references before the
    # privacy gate; this also proves the rewritten archive keeps a valid RECORD.
    assert normalize_wheel(server_wheel, environ={}, checkout=fixture) >= 0
    assert check_wheel(server_wheel) == []
    assert not audit_wheel(server_wheel, environ={}).findings
    assert {path: path.read_bytes() for path in before} == before


def test_maturin_excludes_seeded_bytecode_and_composes_complete_wheel(
    tmp_path: Path,
) -> None:
    """Exercise Maturin selection, then the real fold/privacy/completeness gates."""
    _skip_unless_maturin_and_cargo_available()

    root_config = tomllib.loads((REPO / "pyproject.toml").read_text(encoding="utf-8"))
    fixture = tmp_path / "fixture"
    before = _seed_fixture_crate(fixture, root_config)

    dist = fixture / "dist"
    server_wheel = _build_server_wheel(fixture, dist)
    _assert_built_wheel_is_clean_and_complete(server_wheel)

    _inject_numeric_wheel(dist, server_wheel)
    _assert_injected_wheel_is_clean_and_complete(server_wheel)

    _assert_release_composition_gates_pass(server_wheel, fixture, before)
