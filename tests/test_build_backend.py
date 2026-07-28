"""PEP 517 builds must compose the required numeric native module."""

from __future__ import annotations

import base64
import csv
import hashlib
import importlib.util
import os
import sys
import threading
import time
import zipfile
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))
_SPEC = importlib.util.spec_from_file_location(
    "build_backend", REPO / "build_backend.py"
)
assert _SPEC is not None and _SPEC.loader is not None
build_backend = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = build_backend
_SPEC.loader.exec_module(build_backend)

pytestmark = pytest.mark.no_engine


def _wheel(path: Path, member: str, payload: bytes) -> None:
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(member, payload)
        archive.writestr("epistemic_graph-0.dist-info/RECORD", b"")


def _record(entries: dict[str, bytes]) -> bytes:
    rows: list[tuple[str, str, int | str]] = []
    for name, payload in sorted(entries.items()):
        digest = base64.urlsafe_b64encode(hashlib.sha256(payload).digest()).decode()
        rows.append((name, f"sha256={digest.rstrip('=')}", len(payload)))
    rows.append(("epistemic_graph-0.dist-info/RECORD", "", ""))
    # csv.writer needs a file-like object; its output is deterministic for these
    # plain wheel paths and lets the backend verify the exact RECORD contract.
    from io import StringIO

    output = StringIO(newline="")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerows(rows)
    return output.getvalue().encode()


def _editable_wheel(path: Path) -> None:
    entries = {
        "epistemic_graph.pth": f"{build_backend.ROOT.resolve()}\n".encode(),
        "epistemic_graph/numeric.abi3.so": b"numeric",
        "epistemic_graph-0.data/scripts/epistemic-graph-server": b"server",
        "epistemic_graph-0.dist-info/METADATA": b"Name: epistemic-graph\n",
        "epistemic_graph-0.dist-info/WHEEL": b"Wheel-Version: 1.0\n",
    }
    with zipfile.ZipFile(path, "w") as archive:
        for name, payload in entries.items():
            info = zipfile.ZipInfo(name)
            if name.endswith((".so", "epistemic-graph-server")):
                info.external_attr = 0o100755 << 16
            archive.writestr(info, payload)
        archive.writestr("epistemic_graph-0.dist-info/RECORD", _record(entries))


@pytest.fixture
def editable_cache(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    cache = tmp_path / "cache"
    monkeypatch.setenv("EPISTEMIC_GRAPH_NATIVE_ARTIFACT_CACHE", str(cache))
    monkeypatch.setattr(build_backend, "native_source_digest", lambda: "source-a")
    monkeypatch.setattr(build_backend, "_tool_identity", lambda executable: executable)
    monkeypatch.setattr(build_backend.shutil, "which", lambda _: "/mock/maturin")
    return cache


def _mock_composer(monkeypatch: pytest.MonkeyPatch, *, delay: float = 0.0) -> list[int]:
    calls = [0]

    def compose(
        _build: object,
        wheel_directory: str,
        _settings: object,
        _metadata: object,
    ) -> str:
        calls[0] += 1
        if delay:
            time.sleep(delay)
        filename = "epistemic_graph-0-py3-none-any.whl"
        destination = Path(wheel_directory)
        destination.mkdir(parents=True, exist_ok=True)
        _editable_wheel(destination / filename)
        return filename

    monkeypatch.setattr(build_backend, "_compose", compose)
    return calls


def _cache_entry(cache: Path) -> Path:
    return next(
        path for path in cache.iterdir() if path.is_dir() and path.name != "locks"
    )


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


def test_editable_native_cache_hit_avoids_maturin(
    monkeypatch: pytest.MonkeyPatch, editable_cache: Path, tmp_path: Path
) -> None:
    calls = _mock_composer(monkeypatch)

    first = build_backend.build_editable(str(tmp_path / "first"))
    second = build_backend.build_editable(str(tmp_path / "second"))

    assert calls == [1]
    assert (tmp_path / "first" / first).is_file()
    assert (tmp_path / "second" / second).is_file()
    assert (
        len(
            [
                entry
                for entry in editable_cache.iterdir()
                if entry.is_dir() and entry.name != "locks"
            ]
        )
        == 1
    )


def test_native_cache_key_invalidates_native_inputs_not_venv_path(
    monkeypatch: pytest.MonkeyPatch, editable_cache: Path
) -> None:
    first = build_backend.native_artifact_key({"profile": "release"})
    monkeypatch.setattr(
        build_backend.sys, "executable", "/other/worktree/.venv/bin/python"
    )
    same_abi_different_venv = build_backend.native_artifact_key({"profile": "release"})
    monkeypatch.setattr(build_backend, "native_source_digest", lambda: "source-b")
    changed_source = build_backend.native_artifact_key({"profile": "release"})
    changed_config = build_backend.native_artifact_key({"profile": "debug"})

    assert first == same_abi_different_venv
    assert first != changed_source
    assert changed_source != changed_config


def test_native_source_digest_includes_dirty_and_untracked_inputs(
    tmp_path: Path,
) -> None:
    (tmp_path / "src").mkdir()
    source = tmp_path / "src" / "lib.rs"
    source.write_text("pub fn current() {}\n", encoding="utf-8")
    initial = build_backend.native_source_digest(tmp_path)
    source.write_text("pub fn changed() {}\n", encoding="utf-8")
    changed = build_backend.native_source_digest(tmp_path)
    (tmp_path / "src" / "untracked.rs").write_text(
        "pub fn untracked() {}\n", encoding="utf-8"
    )
    untracked = build_backend.native_source_digest(tmp_path)
    (tmp_path / "docs").mkdir()
    (tmp_path / "docs" / "native-cache.md").write_text("not native\n", encoding="utf-8")
    (tmp_path / "tests").mkdir()
    (tmp_path / "tests" / "test_native_cache.py").write_text(
        "not native\n", encoding="utf-8"
    )

    assert initial != changed
    assert changed != untracked
    assert untracked == build_backend.native_source_digest(tmp_path)


def test_editable_native_cache_rebuilds_corruption(
    monkeypatch: pytest.MonkeyPatch, editable_cache: Path, tmp_path: Path
) -> None:
    calls = _mock_composer(monkeypatch)
    build_backend.build_editable(str(tmp_path / "first"))
    entry = _cache_entry(editable_cache)
    manifest = __import__("json").loads((entry / "manifest.json").read_text())
    cached_wheel = entry / manifest["filename"]
    cached_wheel.chmod(0o600)
    cached_wheel.write_bytes(b"corrupt")

    build_backend.build_editable(str(tmp_path / "second"))

    assert calls == [2]


def test_parallel_editable_builds_publish_once(
    monkeypatch: pytest.MonkeyPatch, editable_cache: Path, tmp_path: Path
) -> None:
    calls = _mock_composer(monkeypatch, delay=0.05)
    results: list[str] = []
    failures: list[BaseException] = []

    def build(index: int) -> None:
        try:
            results.append(build_backend.build_editable(str(tmp_path / str(index))))
        except BaseException as exc:  # pragma: no cover - asserted below
            failures.append(exc)

    threads = [threading.Thread(target=build, args=(index,)) for index in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert failures == []
    assert results == ["epistemic_graph-0-py3-none-any.whl"] * 2
    assert calls == [1]


def test_cached_editable_payload_preserves_source_and_permissions(
    monkeypatch: pytest.MonkeyPatch, editable_cache: Path, tmp_path: Path
) -> None:
    _mock_composer(monkeypatch)
    build_backend.build_editable(str(tmp_path / "wheel"))
    entry = _cache_entry(editable_cache)
    manifest = __import__("json").loads((entry / "manifest.json").read_text())
    wheel = entry / manifest["filename"]

    assert os.stat(wheel).st_mode & 0o777 == 0o444
    with zipfile.ZipFile(wheel) as archive:
        assert (
            archive.read("epistemic_graph.pth")
            == f"{build_backend.ROOT.resolve()}\n".encode()
        )
        assert "direct_url.json" not in "\n".join(archive.namelist())
        assert (
            archive.getinfo("epistemic_graph/numeric.abi3.so").external_attr >> 16
            & 0o111
        )
        assert (
            archive.getinfo(
                "epistemic_graph-0.data/scripts/epistemic-graph-server"
            ).external_attr
            >> 16
            & 0o111
        )
