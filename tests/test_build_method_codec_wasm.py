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
    for client in codec.CLIENT_DIRS:
        (tmp_path / client / codec.MODULE).write_bytes(fresh)
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


@pytest.fixture
def codec_repo(codec, tmp_path):
    for client in codec.CLIENT_DIRS:
        (tmp_path / client).mkdir(parents=True)
        (tmp_path / client / codec.MODULE).write_bytes(WASM)
    ledger = tmp_path / ".config" / "generated-artifacts.toml"
    ledger.parent.mkdir()
    body = (ROOT / ".config" / "generated-artifacts.toml").read_text()
    for entry in codec.generated_artifacts.load(ROOT).values():
        body = body.replace(entry.sha256, hashlib.sha256(WASM).hexdigest())
    ledger.write_text(body)
    return tmp_path, ledger


def test_write_updates_both_existing_pins_and_preserves_metadata(codec, codec_repo):
    root, ledger = codec_repo
    original = ledger.read_text()
    fresh = WASM + b"\x00\x01x"
    assert codec.write(root, fresh) == 0
    assert ledger.read_text() == original.replace(
        hashlib.sha256(WASM).hexdigest(), hashlib.sha256(fresh).hexdigest()
    )
    assert codec.check(root, fresh) == 0
    assert codec.write(root, fresh) == 0


@pytest.mark.parametrize(
    "change",
    [
        lambda body: "invalid = [",
        lambda body: body.replace("language-clients", "other-job", 1),
        lambda body: body.replace(" --check", " --wrong", 1),
        lambda body: body.replace("clients/go/", "clients/other/", 1),
        lambda body: body + body[body.index("[[artifact]]") :],
        lambda body: body.replace('sha256 = "', 'digest = "', 1),
    ],
)
def test_write_refuses_unexpected_ledger_before_changing_files(
    codec, codec_repo, change
):
    root, ledger = codec_repo
    ledger.write_text(change(ledger.read_text()))
    before = ledger.read_bytes()
    assert codec.write(root, WASM + b"\x00\x01x") == 1
    assert ledger.read_bytes() == before
    for client in codec.CLIENT_DIRS:
        assert (root / client / codec.MODULE).read_bytes() == WASM


def test_write_refuses_missing_ledger_without_creating_artifacts(codec, tmp_path):
    assert codec.write(tmp_path, WASM) == 1
    assert list(tmp_path.iterdir()) == []


def test_write_preserves_unrelated_artifact_with_the_same_old_hash(codec, codec_repo):
    root, ledger = codec_repo
    unrelated = ledger.read_text().split("[[artifact]]")[1]
    unrelated = "[[artifact]]" + unrelated.replace("clients/go/", "clients/unrelated/")
    ledger.write_text(ledger.read_text() + unrelated)
    assert codec.write(root, WASM + b"\x00\x01x") == 0
    assert ledger.read_text().endswith(unrelated)


@pytest.mark.parametrize("target", ["ledger", "codec"])
def test_write_refuses_symlinks_before_changing_files(codec, codec_repo, target):
    root, ledger = codec_repo
    path = ledger if target == "ledger" else root / codec.CLIENT_DIRS[1] / codec.MODULE
    saved = path.with_suffix(".original")
    path.rename(saved)
    path.symlink_to(saved)
    original = saved.read_bytes()
    assert codec.write(root, WASM + b"\x00\x01x") == 1
    assert saved.read_bytes() == original
    assert (root / codec.CLIENT_DIRS[0] / codec.MODULE).read_bytes() == WASM


def test_write_refuses_ambiguous_hash_layout_without_side_effects(codec, codec_repo):
    root, ledger = codec_repo
    original = ledger.read_text().replace('sha256 = "', '"sha256" = "', 1)
    ledger.write_text(original)
    assert codec.write(root, WASM + b"\x00\x01x") == 1
    assert ledger.read_text() == original
    for client in codec.CLIENT_DIRS:
        assert (root / client / codec.MODULE).read_bytes() == WASM


def test_write_refuses_hash_assignment_hidden_in_metadata(codec, codec_repo):
    root, ledger = codec_repo
    original = ledger.read_text().replace('sha256 = "', '"sha256" = "', 1)
    reason = next(line for line in original.splitlines() if line.startswith("reason ="))
    original = original.replace(
        reason,
        "reason = '''\nsha256 = \"" + hashlib.sha256(WASM).hexdigest() + "\"\n'''",
        1,
    )
    ledger.write_text(original)
    assert codec.write(root, WASM + b"\x00\x01x") == 1
    assert ledger.read_text() == original
    for client in codec.CLIENT_DIRS:
        assert (root / client / codec.MODULE).read_bytes() == WASM


def test_write_refuses_invalid_module_without_changing_files(codec, codec_repo):
    root, ledger = codec_repo
    original = ledger.read_bytes()
    with pytest.raises(ValueError, match="WebAssembly"):
        codec.write(root, b"not a module")
    assert ledger.read_bytes() == original
    for client in codec.CLIENT_DIRS:
        assert (root / client / codec.MODULE).read_bytes() == WASM
