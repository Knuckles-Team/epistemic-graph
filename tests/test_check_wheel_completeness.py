"""Tests for the release completeness gate (``scripts/check_wheel_completeness``).

Each test names the shipped regression it locks down. The published
``epistemic-graph`` wheels for 2.14.0-2.23.0 would fail this gate; a correctly
folded wheel passes it.
"""

from __future__ import annotations

import base64
import csv
import hashlib
import io
import sys
import zipfile
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from check_wheel_completeness import check_wheel, main  # noqa: E402

VERSION = "9.9.9"
DIST_INFO = f"epistemic_graph-{VERSION}.dist-info"
DATA_SCRIPTS = f"epistemic_graph-{VERSION}.data/scripts"

METADATA_COMPLETE = f"""Metadata-Version: 2.3
Name: epistemic-graph
Version: {VERSION}
Requires-Dist: msgpack>=1.2.1
Requires-Dist: numpy>=1.22.0
Requires-Dist: pyoxigraph>=0.3.22 ; extra == 'owl'
Provides-Extra: owl
Requires-Python: >=3.10
"""

# The shape every published wheel through 2.23.0 had: numpy reachable only via an
# extra, so the bundled kernel is unimportable on a bare install.
METADATA_NUMPY_BEHIND_EXTRA = f"""Metadata-Version: 2.3
Name: epistemic-graph
Version: {VERSION}
Requires-Dist: msgpack>=1.0.0
Requires-Dist: numpy>=1.22.0 ; extra == 'numeric'
Provides-Extra: numeric
Requires-Python: >=3.10
"""


def _urlsafe_sha256(data: bytes) -> str:
    digest = hashlib.sha256(data).digest()
    return "sha256=" + base64.urlsafe_b64encode(digest).decode().rstrip("=")


def _build_wheel(
    path: Path,
    *,
    members: dict[str, bytes],
    executable: set[str] = frozenset(),
    corrupt_record: bool = False,
) -> Path:
    rows: list[tuple[str, bytes]] = []
    with zipfile.ZipFile(path, "w") as archive:
        for name, payload in members.items():
            info = zipfile.ZipInfo(name)
            info.external_attr = (0o755 if name in executable else 0o644) << 16
            archive.writestr(info, payload)
            rows.append((name, payload))

        buffer = io.StringIO(newline="")
        writer = csv.writer(buffer, lineterminator="\n")
        for name, payload in sorted(rows):
            recorded = payload + b"x" if corrupt_record else payload
            writer.writerow((name, _urlsafe_sha256(recorded), len(recorded)))
        writer.writerow((f"{DIST_INFO}/RECORD", "", ""))
        archive.writestr(f"{DIST_INFO}/RECORD", buffer.getvalue())
    return path


def _complete_members(metadata: str = METADATA_COMPLETE) -> dict[str, bytes]:
    return {
        "epistemic_graph/__init__.py": b"",
        "epistemic_graph/client.py": b"",
        "epistemic_graph/numeric.abi3.so": b"\x7fELF-kernel",
        "epistemic_graph/kvcache/__init__.py": b"",
        "epistemic_graph/skills/kg-modality-sql/SKILL.md": b"# skill",
        f"{DATA_SCRIPTS}/epistemic-graph-server": b"\x7fELF-server",
        f"{DIST_INFO}/METADATA": metadata.encode(),
        f"{DIST_INFO}/WHEEL": b"Wheel-Version: 1.0\n",
    }


@pytest.fixture
def complete_wheel(tmp_path: Path) -> Path:
    return _build_wheel(
        tmp_path / f"epistemic_graph-{VERSION}-py3-none-linux_x86_64.whl",
        members=_complete_members(),
        executable={f"{DATA_SCRIPTS}/epistemic-graph-server"},
    )


def test_a_correctly_folded_wheel_passes(complete_wheel: Path) -> None:
    assert check_wheel(complete_wheel) == []
    assert main([str(complete_wheel)]) == 0


def test_a_wheel_without_the_numeric_kernel_is_rejected(tmp_path: Path) -> None:
    """The 2.14.0-2.23.0 regression: the inject silently did not happen."""
    members = _complete_members()
    del members["epistemic_graph/numeric.abi3.so"]
    wheel = _build_wheel(
        tmp_path / "kernel-less.whl",
        members=members,
        executable={f"{DATA_SCRIPTS}/epistemic-graph-server"},
    )
    failures = check_wheel(wheel)
    assert any("no numeric kernel" in failure for failure in failures)
    assert main([str(wheel)]) == 1


def test_a_windows_wheel_is_accepted(tmp_path: Path) -> None:
    """`.pyd` kernel + `.exe` binary + no POSIX mode bits is a valid wheel."""
    members = _complete_members()
    members["epistemic_graph/numeric.pyd"] = members.pop(
        "epistemic_graph/numeric.abi3.so"
    )
    members[f"{DATA_SCRIPTS}/epistemic-graph-server.exe"] = members.pop(
        f"{DATA_SCRIPTS}/epistemic-graph-server"
    )
    wheel = _build_wheel(tmp_path / "windows.whl", members=members)
    assert check_wheel(wheel) == []


def test_a_non_executable_server_binary_is_rejected(tmp_path: Path) -> None:
    """Re-zipping the wheel to inject the kernel once dropped mode 0755."""
    wheel = _build_wheel(
        tmp_path / "not-executable.whl",
        members=_complete_members(),
        executable=set(),
    )
    failures = check_wheel(wheel)
    assert any("is not executable" in failure for failure in failures)


def test_numpy_behind_an_extra_is_rejected(tmp_path: Path) -> None:
    """Kernel present but unimportable on a bare install — the same bug, restated."""
    wheel = _build_wheel(
        tmp_path / "numpy-extra.whl",
        members=_complete_members(METADATA_NUMPY_BEHIND_EXTRA),
        executable={f"{DATA_SCRIPTS}/epistemic-graph-server"},
    )
    failures = check_wheel(wheel)
    assert any("'numpy' is not an unconditional" in failure for failure in failures)


def test_missing_package_data_is_rejected(tmp_path: Path) -> None:
    members = _complete_members()
    del members["epistemic_graph/kvcache/__init__.py"]
    wheel = _build_wheel(
        tmp_path / "no-kvcache.whl",
        members=members,
        executable={f"{DATA_SCRIPTS}/epistemic-graph-server"},
    )
    assert any("kvcache" in failure for failure in check_wheel(wheel))


def test_an_inconsistent_record_is_rejected(tmp_path: Path) -> None:
    """The inject/normalize steps rebuild RECORD by hand — drift is corruption."""
    wheel = _build_wheel(
        tmp_path / "bad-record.whl",
        members=_complete_members(),
        executable={f"{DATA_SCRIPTS}/epistemic-graph-server"},
        corrupt_record=True,
    )
    failures = check_wheel(wheel)
    assert any("RECORD hash mismatch" in failure for failure in failures)


def test_main_reports_every_incomplete_wheel(tmp_path: Path, capsys) -> None:
    good = _build_wheel(
        tmp_path / "good.whl",
        members=_complete_members(),
        executable={f"{DATA_SCRIPTS}/epistemic-graph-server"},
    )
    members = _complete_members()
    del members["epistemic_graph/numeric.abi3.so"]
    bad = _build_wheel(
        tmp_path / "bad.whl",
        members=members,
        executable={f"{DATA_SCRIPTS}/epistemic-graph-server"},
    )
    assert main([str(good), str(bad)]) == 1
    out = capsys.readouterr().out
    assert "OK: good.whl" in out
    assert "FAIL: bad.whl" in out
