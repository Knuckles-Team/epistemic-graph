"""The strip script removes ``.data/scripts`` members and keeps RECORD valid."""

from __future__ import annotations

import base64
import csv
import hashlib
import io
import zipfile
from pathlib import Path

import pytest

from scripts.strip_wheel_executables import main

pytestmark = pytest.mark.no_engine

RECORD = "pkg-1.dist-info/RECORD"


def _digest(payload: bytes) -> str:
    raw = hashlib.sha256(payload).digest()
    return "sha256=" + base64.urlsafe_b64encode(raw).decode().rstrip("=")


def _synthetic_wheel(path: Path) -> None:
    files = {
        "pkg/__init__.py": b"x = 1\n",
        "pkg-1.dist-info/METADATA": b"Name: pkg\n",
        "pkg-1.data/scripts/x": b"#!/bin/sh\n",
    }
    out = io.StringIO()
    writer = csv.writer(out, lineterminator="\n")
    for name, payload in files.items():
        writer.writerow([name, _digest(payload), len(payload)])
    writer.writerow([RECORD, "", ""])
    with zipfile.ZipFile(path, "w") as archive:
        for name, payload in files.items():
            archive.writestr(name, payload)
        archive.writestr(RECORD, out.getvalue())


def test_strip_removes_scripts_and_regenerates_record(tmp_path: Path) -> None:
    wheel = tmp_path / "pkg-1-py3-none-any.whl"
    _synthetic_wheel(wheel)

    assert main([str(wheel)]) == 0

    with zipfile.ZipFile(wheel) as archive:
        names = archive.namelist()
        assert not [n for n in names if ".data/scripts/" in n]
        assert "pkg/__init__.py" in names
        rows = list(csv.reader(io.StringIO(archive.read(RECORD).decode())))
        listed = {row[0]: row for row in rows}
        assert set(listed) == set(names)
        for name in names:
            if name == RECORD:
                continue
            payload = archive.read(name)
            assert listed[name][1:] == [_digest(payload), str(len(payload))]
