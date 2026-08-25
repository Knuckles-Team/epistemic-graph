"""Cross-platform contract for folding the numeric kernel into release wheels."""

from __future__ import annotations

import base64
import csv
import hashlib
import io
import zipfile
from pathlib import Path

import pytest

from scripts.inject_numeric_kernel import inject

# Pure zip-file surgery, exactly like sibling test_check_wheel_completeness.py
# -- never needs the shared native engine (see conftest.py's session-scoped
# `start_epistemic_graph_server` fixture, which this marker exempts this
# module from triggering). BUG-PE-018: this was the module that hung a
# combined run by omitting the marker.
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
    metadata = b"Metadata-Version: 2.1\nName: epistemic-graph\n"
    record_name = "epistemic_graph-0.dist-info/RECORD"
    record = (
        f"epistemic_graph-0.data/scripts/epistemic-graph-server,{_hash(script)},{len(script)}\n"
        f"epistemic_graph-0.dist-info/METADATA,{_hash(metadata)},{len(metadata)}\n"
        f"{record_name},,\n"
    ).encode()
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(
            _zip_info("epistemic_graph-0.data/scripts/epistemic-graph-server", 0o755),
            script,
        )
        archive.writestr(_zip_info("epistemic_graph-0.dist-info/METADATA"), metadata)
        archive.writestr(_zip_info(record_name), record)


def _numeric_wheel(path: Path, extension: str) -> bytes:
    payload = b"native-kernel"
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr(_zip_info(f"numeric/{extension}", 0o755), payload)
    return payload


@pytest.mark.parametrize("extension", ["numeric.abi3.so", "numeric.pyd"])
def test_injects_platform_extension_and_preserves_server_mode(
    tmp_path: Path, extension: str
) -> None:
    server = tmp_path / "server.whl"
    numeric = tmp_path / "numeric.whl"
    _server_wheel(server)
    payload = _numeric_wheel(numeric, extension)

    inject(server, numeric)

    target = f"epistemic_graph/{extension}"
    with zipfile.ZipFile(server) as archive:
        assert archive.read(target) == payload
        server_info = archive.getinfo(
            "epistemic_graph-0.data/scripts/epistemic-graph-server"
        )
        assert (server_info.external_attr >> 16) & 0o777 == 0o755
        record = archive.read("epistemic_graph-0.dist-info/RECORD").decode()
    rows = {row[0]: row[1:] for row in csv.reader(io.StringIO(record))}
    assert rows[target] == [_hash(payload), str(len(payload))]


def test_rejects_kernel_wheel_without_native_extension(tmp_path: Path) -> None:
    server = tmp_path / "server.whl"
    numeric = tmp_path / "numeric.whl"
    _server_wheel(server)
    with zipfile.ZipFile(numeric, "w") as archive:
        archive.writestr("numeric/__init__.py", b"")

    with pytest.raises(SystemExit, match="no supported native extension"):
        inject(server, numeric)


def test_replaces_stale_kernel_and_rebuilds_record_exactly(tmp_path: Path) -> None:
    server = tmp_path / "server.whl"
    numeric = tmp_path / "numeric.whl"
    _server_wheel(server)
    target = "epistemic_graph/numeric.abi3.so"
    stale = b"stale-kernel"

    with zipfile.ZipFile(server, "a") as archive:
        archive.writestr(_zip_info(target, 0o755), stale)
    payload = _numeric_wheel(numeric, "numeric.abi3.so")

    inject(server, numeric)

    with zipfile.ZipFile(server) as archive:
        assert archive.namelist().count(target) == 1
        assert archive.read(target) == payload
        record_name = "epistemic_graph-0.dist-info/RECORD"
        rows = list(csv.reader(io.StringIO(archive.read(record_name).decode())))
        names = [row[0] for row in rows]
        assert len(names) == len(set(names))
        assert names[-1] == record_name
        assert rows[-1][1:] == ["", ""]
        for name, digest, size in rows[:-1]:
            data = archive.read(name)
            assert digest == _hash(data)
            assert size == str(len(data))
