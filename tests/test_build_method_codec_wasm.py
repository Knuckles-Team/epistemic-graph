"""Rust-only method codec builds retain byte and import verification."""

from __future__ import annotations

import hashlib
import importlib.util
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.no_engine
WASM = b"\x00asm\x01\x00\x00\x00"


@pytest.fixture
def codec(monkeypatch):
    path = ROOT / "scripts" / "build_method_codec_wasm.py"
    spec = importlib.util.spec_from_file_location("build_method_codec_wasm", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, "build_method_codec_wasm", module)
    spec.loader.exec_module(module)
    return module


def test_build_uses_only_cargo_and_returns_the_verified_rust_module(
    codec, monkeypatch, tmp_path
):
    target = tmp_path / "target"
    module_path = target / codec.TARGET / codec.PROFILE / codec.MODULE
    module_path.parent.mkdir(parents=True)
    module_path.write_bytes(WASM + b"\x00\x01x")
    calls = []

    def cargo(command, *, cwd, env, check):
        calls.append((command, cwd, env, check))

    monkeypatch.setattr(codec.subprocess, "run", cargo)
    monkeypatch.setattr(
        codec, "encoded_rustflags", lambda *_args, **_kwargs: ("flags", (), ())
    )
    monkeypatch.setenv("RUSTFLAGS", "-C target-cpu=native")
    assert codec.build(tmp_path, target) == module_path.read_bytes()
    assert len(calls) == 1
    command, cwd, env, check = calls[0]
    assert command[:2] == ["cargo", "build"]
    assert "--locked" in command and "eg-method-codec" in command
    assert cwd == tmp_path and check is True
    assert env["CARGO_TARGET_DIR"] == str(target)
    assert env["CARGO_ENCODED_RUSTFLAGS"] == "flags"
    assert "RUSTFLAGS" not in env


@pytest.mark.parametrize(
    "module",
    [
        b"not wasm",
        WASM + b"\x02\x01\x00",  # even an empty import section is refused
        WASM + b"\x00\x02x",  # truncated custom section
        WASM + b"\x00\x80",  # truncated section size
    ],
)
def test_invalid_or_importing_modules_fail_closed(codec, module):
    with pytest.raises(ValueError, match="WebAssembly|imports"):
        codec.verify_no_imports(module)


def test_check_requires_both_client_copies_to_equal_fresh_bytes(
    codec, tmp_path, capsys
):
    for client in codec.CLIENT_DIRS:
        (tmp_path / client).mkdir(parents=True)
    fresh = WASM + b"\x00\x01x"
    assert codec.write(tmp_path, fresh) == 0
    expected = hashlib.sha256(fresh).hexdigest()
    config = tmp_path / ".config"
    config.mkdir()
    entries = []
    for client in codec.CLIENT_DIRS:
        entries.append(
            "[[artifact]]\n"
            f'path = "{client}/{codec.MODULE}"\n'
            f'sha256 = "{expected}"\n'
            'reproducer = "python3 scripts/build_method_codec_wasm.py --check"\n'
            'proven_by = "language-clients"\n'
            'reason = "Embedded method codec"\n'
        )
    (config / "generated-artifacts.toml").write_text("\n".join(entries))
    assert codec.check(tmp_path, fresh) == 0
    assert f"sha256={expected}" in capsys.readouterr().out
    (tmp_path / codec.CLIENT_DIRS[1] / codec.MODULE).write_bytes(b"stale")
    assert codec.check(tmp_path, fresh) == 1
    assert expected in capsys.readouterr().err
    (tmp_path / codec.CLIENT_DIRS[1] / codec.MODULE).write_bytes(fresh)
    (config / "generated-artifacts.toml").write_text(
        (config / "generated-artifacts.toml").read_text().replace(expected, "0" * 64, 1)
    )
    assert codec.check(tmp_path, fresh) == 1
    assert "does not match its pinned sha256" in capsys.readouterr().err
