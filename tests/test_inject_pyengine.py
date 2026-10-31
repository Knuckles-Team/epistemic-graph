"""Cross-platform contract for folding the medulla-pyengine binding into release wheels.

Mirrors ``tests/test_inject_numeric_kernel.py`` — same wheel-surgery mechanism,
different target module (``engine`` instead of ``numeric``).
"""

from __future__ import annotations

import base64
import csv
import hashlib
import io
import zipfile
from pathlib import Path

import pytest

from scripts.inject_pyengine import inject

pytestmark = pytest.mark.no_engine


def _hash(data: bytes) -> str:
    digest = hashlib.sha256(data).digest()
    return "sha256=" + base64.urlsafe_b64encode(digest).decode().rstrip("=")


def _zip_info(name: str, mode: int = 0o644) -> zipfile.ZipInfo:
    info = zipfile.ZipInfo(name)
    info.external_attr = mode << 16
    return info


def _server_wheel(path: Path) -> None:
    script = b"server"
    metadata = b"Metadata-Version: 2.1\nName: medulla-stem\n"
    record_name = "medulla_stem-0.dist-info/RECORD"
    record = (
        f"medulla_stem-0.data/scripts/medulla-server,{_hash(script)},{len(script)}\n"
        f"medulla_stem-0.dist-info/METADATA,{_hash(metadata)},{len(metadata)}\n"
        f"{record_name},,\n"
    ).encode()
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(
            _zip_info("medulla_stem-0.data/scripts/medulla-server", 0o755),
            script,
        )
        archive.writestr(_zip_info("medulla_stem-0.dist-info/METADATA"), metadata)
        archive.writestr(_zip_info(record_name), record)


def _engine_wheel(path: Path, extension: str) -> bytes:
    payload = b"native-engine-kernel"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(_zip_info(f"engine/{extension}", 0o755), payload)
    return payload


@pytest.mark.parametrize("extension", ["engine.abi3.so", "engine.pyd"])
def test_injects_platform_extension_and_preserves_server_mode(
    tmp_path: Path, extension: str
) -> None:
    server = tmp_path / "server.whl"
    engine = tmp_path / "engine.whl"
    _server_wheel(server)
    payload = _engine_wheel(engine, extension)

    inject(server, engine)

    target = f"medulla/{extension}"
    with zipfile.ZipFile(server) as archive:
        assert archive.read(target) == payload
        server_info = archive.getinfo("medulla_stem-0.data/scripts/medulla-server")
        assert (server_info.external_attr >> 16) & 0o777 == 0o755
        record = archive.read("medulla_stem-0.dist-info/RECORD").decode()
    rows = {row[0]: row[1:] for row in csv.reader(io.StringIO(record))}
    assert rows[target] == [_hash(payload), str(len(payload))]


def test_rejects_kernel_wheel_without_native_extension(tmp_path: Path) -> None:
    server = tmp_path / "server.whl"
    engine = tmp_path / "engine.whl"
    _server_wheel(server)
    with zipfile.ZipFile(engine, "w") as archive:
        archive.writestr("engine/__init__.py", b"")

    with pytest.raises(SystemExit, match="no supported native extension"):
        inject(server, engine)


def test_replaces_stale_kernel_and_rebuilds_record_exactly(tmp_path: Path) -> None:
    server = tmp_path / "server.whl"
    engine = tmp_path / "engine.whl"
    _server_wheel(server)
    target = "medulla/engine.abi3.so"
    stale = b"stale-engine-kernel"

    with zipfile.ZipFile(server, "a") as archive:
        archive.writestr(_zip_info(target, 0o755), stale)
    payload = _engine_wheel(engine, "engine.abi3.so")

    inject(server, engine)

    with zipfile.ZipFile(server) as archive:
        assert archive.namelist().count(target) == 1
        assert archive.read(target) == payload
        record_name = "medulla_stem-0.dist-info/RECORD"
        rows = list(csv.reader(io.StringIO(archive.read(record_name).decode())))
        names = [row[0] for row in rows]
        assert len(names) == len(set(names))
        assert names[-1] == record_name
        assert rows[-1][1:] == ["", ""]
        for name, digest, size in rows[:-1]:
            data = archive.read(name)
            assert digest == _hash(data)
            assert size == str(len(data))


def test_numeric_and_engine_injections_compose_freely(tmp_path: Path) -> None:
    """Both grafts land in the same wheel regardless of application order.

    Proves the design doc's claim (unified-inprocess-engine.md §8): each
    injector only reads/rewrites the target wheel's RECORD and never touches
    the other extension's member, so folding numeric then engine (or the
    reverse) produces the same final artifact.
    """
    from scripts.inject_numeric_kernel import inject as inject_numeric

    server = tmp_path / "server.whl"
    numeric = tmp_path / "numeric.whl"
    engine = tmp_path / "engine.whl"
    _server_wheel(server)
    numeric_payload = b"native-numeric-kernel"
    with zipfile.ZipFile(numeric, "w") as archive:
        archive.writestr(_zip_info("numeric/numeric.abi3.so", 0o755), numeric_payload)
    engine_payload = _engine_wheel(engine, "engine.abi3.so")

    inject_numeric(server, numeric)
    inject(server, engine)

    with zipfile.ZipFile(server) as archive:
        names = archive.namelist()
        assert archive.read("medulla/numeric.abi3.so") == numeric_payload
        assert archive.read("medulla/engine.abi3.so") == engine_payload
        record_name = "medulla_stem-0.dist-info/RECORD"
        rows = {
            row[0]: row[1:]
            for row in csv.reader(io.StringIO(archive.read(record_name).decode()))
        }
        for name in names:
            if name == record_name:
                continue
            data = archive.read(name)
            assert rows[name] == [_hash(data), str(len(data))]
