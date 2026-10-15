"""PEP 517 builds must compose both required native modules."""

from __future__ import annotations

import base64
import csv
import hashlib
import importlib.util
import os
import subprocess
import sys
import threading
import time
import zipfile
from contextlib import contextmanager
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
        info = zipfile.ZipInfo(member)
        info.external_attr = 0o100755 << 16
        archive.writestr(info, payload)
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


def _editable_wheel(
    path: Path,
    *,
    source_root: Path | None = None,
    source_payload: bytes | None = None,
    native_payloads: dict[str, bytes] | None = None,
) -> None:
    source_root = source_root or build_backend.ROOT.resolve()
    entries = {
        "epistemic_graph.pth": (
            source_payload
            if source_payload is not None
            else f"{source_root}\n".encode()
        ),
        "epistemic_graph-0.dist-info/METADATA": b"Name: epistemic-graph\n",
        "epistemic_graph-0.dist-info/WHEEL": b"Wheel-Version: 1.0\n",
    }
    if native_payloads is None:
        native_payloads = {
            "epistemic_graph/numeric.abi3.so": b"numeric",
            "epistemic_graph/engine.abi3.so": b"engine",
        }
    entries.update(native_payloads)
    with zipfile.ZipFile(path, "w") as archive:
        for name, payload in entries.items():
            info = zipfile.ZipInfo(name)
            if name.endswith(".so"):
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


def _test_cache_inputs(label: str) -> dict[str, object]:
    return {
        "schema": build_backend._CACHE_SCHEMA,
        "source_digest": label,
        "environment": {"RUSTFLAGS": None},
    }


@pytest.mark.parametrize("entrypoint", ["build_wheel", "build_editable"])
@pytest.mark.parametrize("suffix", ["abi3.so", "pyd"])
@pytest.mark.spec("EG-CONTRACT-R053.2")
def test_build_backend_folds_numeric_into_engine_base(
    monkeypatch, editable_cache: Path, tmp_path: Path, entrypoint: str, suffix: str
) -> None:
    output = tmp_path / "output"
    output.mkdir()
    server = output / "epistemic_graph-0-py3-none-any.whl"
    # The base wheel is the engine extension wheel (engine module already in it);
    # a stray console executable proves the strip safety net still runs.
    _editable_wheel(
        server,
        native_payloads={
            f"epistemic_graph/engine.{suffix}": b"engine",
            "epistemic_graph-0.data/scripts/epistemic-graph-server": b"server",
        },
    )
    calls = []

    def component(directory, manifest, features):
        calls.append((manifest, features))
        wheel = directory / "numeric.whl"
        _wheel(wheel, f"numeric/numeric.{suffix}", b"numeric")
        return wheel

    monkeypatch.setattr(build_backend, "_build_component", component)
    monkeypatch.setattr(build_backend.maturin, entrypoint, lambda *_: server.name)
    assert getattr(build_backend, entrypoint)(str(output)) == server.name
    # Only the numeric extension is built separately: no server crate, no bin.
    assert calls == [(build_backend.NUMERIC_MANIFEST, "python")]
    assert not hasattr(build_backend, "ENGINE_MANIFEST")
    with zipfile.ZipFile(server) as archive:
        assert archive.read(f"epistemic_graph/numeric.{suffix}") == b"numeric"
        assert archive.read(f"epistemic_graph/engine.{suffix}") == b"engine"
        assert build_backend._record_valid(archive)
        assert not [n for n in archive.namelist() if ".data/scripts/" in n]


def test_regular_and_editable_builds_serialize_complete_maturin_staging(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A shared Cargo target has one staging owner through numeric injection."""

    monkeypatch.setenv("CARGO_TARGET_DIR", str(tmp_path / "shared-target"))
    first_numeric_started = threading.Event()
    release_first_numeric = threading.Event()
    second_lock_attempted = threading.Event()
    second_server_started = threading.Event()
    calls_lock = threading.Lock()
    staging_lock_paths: list[Path] = []
    component_calls = []
    failures: list[BaseException] = []
    original_file_lock = build_backend._exclusive_file_lock

    @contextmanager
    def observed_file_lock(lock_path: Path):
        staging_lock_paths.append(lock_path)
        if threading.current_thread().name == "editable-builder":
            second_lock_attempted.set()
        with original_file_lock(lock_path):
            yield

    def server_builder(label: str):
        def build(
            wheel_directory: str,
            _settings: object,
            _metadata: object,
        ) -> str:
            if label == "editable":
                second_server_started.set()
            destination = Path(wheel_directory)
            destination.mkdir(parents=True, exist_ok=True)
            filename = f"{label}-py3-none-any.whl"
            _wheel(
                destination / filename,
                "epistemic_graph-0.dist-info/METADATA",
                b"Name: epistemic-graph\n",
            )
            return filename

        return build

    def build_component(directory: Path, manifest: Path, _features: str) -> Path:
        with calls_lock:
            component_calls.append(manifest)
        wheel = directory / "numeric.whl"
        _wheel(wheel, "numeric/numeric.abi3.so", b"native-kernel")
        return wheel

    numeric_calls = 0
    original_inject_numeric = build_backend.inject_numeric

    def inject_numeric(server: Path, numeric: Path) -> None:
        nonlocal numeric_calls
        numeric_calls += 1
        if numeric_calls == 1:
            first_numeric_started.set()
            assert release_first_numeric.wait(timeout=5)
        original_inject_numeric(server, numeric)

    monkeypatch.setattr(build_backend, "_build_component", build_component)
    monkeypatch.setattr(build_backend, "inject_numeric", inject_numeric)
    monkeypatch.setattr(build_backend, "_exclusive_file_lock", observed_file_lock)

    def compose(label: str, output: Path) -> None:
        try:
            build_backend._compose(server_builder(label), str(output), None, None)
        except BaseException as exc:  # pragma: no cover - asserted below
            failures.append(exc)

    regular = threading.Thread(target=compose, args=("regular", tmp_path / "regular"))
    editable = threading.Thread(
        target=compose,
        args=("editable", tmp_path / "editable"),
        name="editable-builder",
    )
    regular.start()
    assert first_numeric_started.wait(timeout=5)
    editable.start()
    assert second_lock_attempted.wait(timeout=5)

    # The editable server build must remain outside Maturin while the regular
    # composition is still injecting the numeric module.
    assert not second_server_started.is_set()
    release_first_numeric.set()
    regular.join(timeout=5)
    editable.join(timeout=5)

    assert not regular.is_alive()
    assert not editable.is_alive()
    assert failures == []
    assert second_server_started.is_set()
    assert component_calls == [build_backend.NUMERIC_MANIFEST] * 2
    assert numeric_calls == 2
    assert len(set(staging_lock_paths)) == 1


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


def test_editable_native_cache_manifest_records_only_input_fingerprints(
    monkeypatch: pytest.MonkeyPatch, editable_cache: Path, tmp_path: Path
) -> None:
    sensitive_value = "do-not-persist-this-build-value"
    monkeypatch.setenv("RUSTFLAGS", sensitive_value)
    _mock_composer(monkeypatch)

    build_backend.build_editable(str(tmp_path / "wheel"))

    manifest_text = (_cache_entry(editable_cache) / "manifest.json").read_text()
    manifest = __import__("json").loads(manifest_text)
    assert sensitive_value not in manifest_text
    assert "/environment/RUSTFLAGS" in manifest["input_fingerprints"]
    assert all(len(digest) == 64 for digest in manifest["input_fingerprints"].values())


def test_editable_native_cache_rebuilds_input_provenance_tampering(
    monkeypatch: pytest.MonkeyPatch, editable_cache: Path, tmp_path: Path
) -> None:
    calls = _mock_composer(monkeypatch)
    build_backend.build_editable(str(tmp_path / "first"))
    entry = _cache_entry(editable_cache)
    manifest_path = entry / "manifest.json"
    manifest = __import__("json").loads(manifest_path.read_text())
    entry.chmod(0o700)
    manifest_path.chmod(0o600)
    manifest["input_fingerprints"]["/source_digest"] = "0" * 64
    manifest_path.write_text(__import__("json").dumps(manifest), encoding="utf-8")

    build_backend.build_editable(str(tmp_path / "second"))

    assert calls == [2]


def test_editable_native_cache_rejects_inputs_changed_by_build(
    monkeypatch: pytest.MonkeyPatch, editable_cache: Path, tmp_path: Path
) -> None:
    calls = _mock_composer(monkeypatch)
    snapshots = iter(
        (
            {
                "schema": build_backend._CACHE_SCHEMA,
                "source_digest": "before",
                "environment": {"RUSTFLAGS": None},
            },
            {
                "schema": build_backend._CACHE_SCHEMA,
                "source_digest": "after",
                "environment": {"RUSTFLAGS": "-C target-cpu=native"},
            },
        )
    )
    monkeypatch.setattr(
        build_backend, "_cache_inputs", lambda _settings: next(snapshots)
    )

    with pytest.raises(
        RuntimeError,
        match=(r"inputs changed during build: /environment/RUSTFLAGS, /source_digest"),
    ) as error:
        build_backend.build_editable(str(tmp_path / "wheel"))

    assert calls == [1]
    assert "before" not in str(error.value)
    assert "after" not in str(error.value)
    assert "target-cpu" not in str(error.value)
    assert not [
        entry
        for entry in editable_cache.iterdir()
        if entry.is_dir() and entry.name != "locks"
    ]


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
        assert not [n for n in archive.namelist() if ".data/scripts/" in n]


@pytest.mark.parametrize("trailing_newline", [False, True])
def test_publish_cached_wheel_accepts_symlinked_source_identity(
    tmp_path: Path, trailing_newline: bool
) -> None:
    source_alias = tmp_path / "hermetic-epistemic-graph"
    source_alias.symlink_to(build_backend.ROOT, target_is_directory=True)
    wheel = tmp_path / "epistemic_graph-0-py3-none-any.whl"
    source_payload = str(source_alias).encode()
    if trailing_newline:
        source_payload += b"\n"
    _editable_wheel(wheel, source_payload=source_payload)
    cache = tmp_path / "cache"
    cache.mkdir()
    inputs = _test_cache_inputs("same-source")
    key = build_backend._cache_key(inputs)
    entry = cache / key

    build_backend._publish_cached_wheel(entry, key, wheel, inputs)

    assert build_backend._cached_wheel(entry, key, inputs) is not None
    changed_inputs = _test_cache_inputs("changed-source")
    assert build_backend._cached_wheel(entry, key, changed_inputs) is None


def test_publish_cached_wheel_rejects_different_source_checkout(
    tmp_path: Path,
) -> None:
    different_checkout = tmp_path / "different-epistemic-graph"
    different_checkout.mkdir()
    wheel = tmp_path / "epistemic_graph-0-py3-none-any.whl"
    _editable_wheel(wheel, source_root=different_checkout)
    cache = tmp_path / "cache"
    cache.mkdir()
    inputs = _test_cache_inputs("different-source")
    key = build_backend._cache_key(inputs)

    with pytest.raises(RuntimeError, match="points at another source checkout"):
        build_backend._publish_cached_wheel(cache / key, key, wheel, inputs)


@pytest.mark.parametrize(
    "source_payload",
    [
        b"",
        b"relative/source",
        b"import os",
        f"{build_backend.ROOT}\r\n".encode(),
        f"{build_backend.ROOT}\n{build_backend.ROOT}\n".encode(),
        f"{build_backend.ROOT}\0".encode(),
    ],
)
def test_publish_cached_wheel_rejects_invalid_source_pointer(
    tmp_path: Path, source_payload: bytes
) -> None:
    wheel = tmp_path / "epistemic_graph-0-py3-none-any.whl"
    _editable_wheel(wheel, source_payload=source_payload)
    cache = tmp_path / "cache"
    cache.mkdir()
    inputs = _test_cache_inputs("invalid-source")
    key = build_backend._cache_key(inputs)

    with pytest.raises(RuntimeError, match="does not point at source"):
        build_backend._publish_cached_wheel(cache / key, key, wheel, inputs)


@pytest.mark.parametrize(
    "manifest_name,features",
    [("NUMERIC_MANIFEST", "python")],
)
def test_component_build_uses_same_interpreter_and_release_features(
    monkeypatch, tmp_path, manifest_name, features
):
    manifest = getattr(build_backend, manifest_name)
    calls = []
    monkeypatch.setattr(build_backend.shutil, "which", lambda _: "/tools/maturin")

    def run(command, **kwargs):
        calls.append((command, kwargs))
        (tmp_path / "component.whl").write_bytes(b"wheel")

    monkeypatch.setattr(build_backend.subprocess, "run", run)
    assert (
        build_backend._build_component(tmp_path, manifest, features).name
        == "component.whl"
    )
    assert calls == [
        (
            [
                "/tools/maturin",
                "build",
                "--release",
                "--manifest-path",
                str(manifest),
                "--interpreter",
                build_backend.sys.executable,
                "--features",
                features,
                "--out",
                str(tmp_path),
            ],
            {"check": True, "cwd": build_backend.ROOT},
        )
    ]


@pytest.mark.parametrize("count", [0, 2])
def test_component_build_rejects_ambiguous_output(monkeypatch, tmp_path, count):
    monkeypatch.setattr(build_backend.shutil, "which", lambda _: "/tools/maturin")
    monkeypatch.setattr(build_backend.subprocess, "run", lambda *_, **__: None)
    for index in range(count):
        (tmp_path / f"{index}.whl").write_bytes(b"wheel")
    with pytest.raises(RuntimeError, match="eg-numeric component build"):
        build_backend._build_component(
            tmp_path, build_backend.NUMERIC_MANIFEST, "python"
        )


@pytest.mark.parametrize("entrypoint", ["build_wheel", "build_editable"])
@pytest.mark.parametrize("failure", ["build", "payload"])
def test_numeric_failure_never_returns_or_caches_incomplete_wheel(
    monkeypatch, editable_cache, tmp_path, entrypoint, failure
):
    output = tmp_path / "failed"
    output.mkdir()
    filename = "epistemic_graph-0-py3-none-any.whl"
    _editable_wheel(
        output / filename, native_payloads={"epistemic_graph/engine.abi3.so": b"engine"}
    )
    monkeypatch.setattr(build_backend.maturin, entrypoint, lambda *_: filename)

    def component(directory, manifest, _features):
        wheel = directory / "component.whl"
        if failure == "build":
            raise subprocess.CalledProcessError(1, ["maturin"])
        _wheel(wheel, "numeric/__init__.py", b"")
        return wheel

    monkeypatch.setattr(build_backend, "_build_component", component)
    error = subprocess.CalledProcessError if failure == "build" else SystemExit
    with pytest.raises(error):
        getattr(build_backend, entrypoint)(str(output))
    assert not list(editable_cache.glob("*/manifest.json"))


@pytest.mark.parametrize(
    "engine_names",
    [
        (),
        ("engine.py",),
        ("engine.cache/hidden.so",),
        ("engine.abi3.so", "engine.cp312.so"),
    ],
)
def test_cache_rebuilds_incomplete_engine_with_valid_record_and_digest(
    monkeypatch, editable_cache, tmp_path, engine_names
):
    calls = _mock_composer(monkeypatch)
    build_backend.build_editable(str(tmp_path / "first"))
    entry = _cache_entry(editable_cache)
    manifest_path = entry / "manifest.json"
    manifest = __import__("json").loads(manifest_path.read_text())
    wheel = entry / manifest["filename"]
    wheel.chmod(0o600)
    payloads = {
        "epistemic_graph/numeric.abi3.so": b"numeric",
    }
    payloads.update({f"epistemic_graph/{name}": b"engine" for name in engine_names})
    _editable_wheel(wheel, native_payloads=payloads)
    manifest["sha256"] = build_backend._sha256(wheel)
    manifest_path.chmod(0o600)
    manifest_path.write_text(__import__("json").dumps(manifest))
    with zipfile.ZipFile(wheel) as archive:
        assert build_backend._record_valid(archive)
    with pytest.raises(RuntimeError, match="native payloads"):
        build_backend._validate_editable_wheel(wheel)
    result = build_backend.build_editable(str(tmp_path / "second"))
    assert calls == [2]
    build_backend._validate_editable_wheel(tmp_path / "second" / result)


def test_numeric_injector_changes_invalidate_native_source_digest(tmp_path):
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    injector = scripts / "inject_numeric_kernel.py"
    injector.write_text("# first")
    first = build_backend.native_source_digest(tmp_path)
    injector.write_text("# changed")
    assert first != build_backend.native_source_digest(tmp_path)


def test_windows_editable_native_payloads_accept_pyd(tmp_path):
    wheel = tmp_path / "windows.whl"
    _editable_wheel(
        wheel,
        native_payloads={
            "epistemic_graph/numeric.pyd": b"numeric",
            "epistemic_graph/engine.pyd": b"engine",
        },
    )
    build_backend._validate_editable_wheel(wheel)


def test_editable_cache_rejects_non_executable_unix_engine(tmp_path):
    wheel = tmp_path / "engine-mode.whl"
    _editable_wheel(wheel)
    with zipfile.ZipFile(wheel) as archive:
        entries = [(info, archive.read(info)) for info in archive.infolist()]
    with zipfile.ZipFile(wheel, "w") as archive:
        for info, payload in entries:
            if info.filename == "epistemic_graph/engine.abi3.so":
                info.external_attr = 0o100644 << 16
            archive.writestr(info, payload)
    with pytest.raises(RuntimeError, match="non-executable native payload"):
        build_backend._validate_editable_wheel(wheel)


def test_editable_cache_rejects_wheel_carrying_an_executable(tmp_path):
    wheel = tmp_path / "with-exe.whl"
    _editable_wheel(
        wheel,
        native_payloads={
            "epistemic_graph/numeric.abi3.so": b"numeric",
            "epistemic_graph/engine.abi3.so": b"engine",
            "epistemic_graph-0.data/scripts/epistemic-graph-server": b"server",
        },
    )
    with pytest.raises(RuntimeError, match="executable"):
        build_backend._validate_editable_wheel(wheel)
