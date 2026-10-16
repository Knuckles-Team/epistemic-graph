from __future__ import annotations

import os
import stat
from pathlib import Path

import epistemic_graph_server as egs
import pytest


def _make_exe(directory: Path) -> Path:
    exe = directory / "epistemic-graph-server"
    exe.write_text("#!/bin/sh\n")
    exe.chmod(exe.stat().st_mode | stat.S_IXUSR)
    return exe


def test_binary_path_in_scripts_dir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    exe = _make_exe(tmp_path)
    monkeypatch.setattr(egs.sysconfig, "get_path", lambda name: str(tmp_path))
    monkeypatch.setattr(egs.os, "name", "posix")
    assert egs.binary_path() == exe


def test_binary_path_falls_back_to_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    empty = tmp_path / "scripts"
    empty.mkdir()
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    exe = _make_exe(bin_dir)
    monkeypatch.setattr(egs.sysconfig, "get_path", lambda name: str(empty))
    monkeypatch.setenv("PATH", str(bin_dir) + os.pathsep + os.environ.get("PATH", ""))
    assert egs.binary_path() == exe


def test_binary_path_absent_raises_typed_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(egs.sysconfig, "get_path", lambda name: str(tmp_path))
    monkeypatch.setenv("PATH", str(tmp_path))
    with pytest.raises(egs.BinaryNotFoundError) as info:
        egs.binary_path()
    assert isinstance(info.value, FileNotFoundError)
    assert "epistemic-graph-server" in str(info.value)
