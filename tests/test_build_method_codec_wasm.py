"""The pinned binaryen archive: verified on every use, offline-capable (EH-383).

Operator ruling 2026-09-24: a build-time download is allowed only for the exact
pinned version with sha256 verification, cached under a stable directory, with
an environment override naming a local file for offline builds; a checksum
mismatch fails the build. No network is used here.
"""

from __future__ import annotations

import hashlib
import importlib.util
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.no_engine


@pytest.fixture
def codec(monkeypatch):
    path = ROOT / "scripts" / "build_method_codec_wasm.py"
    spec = importlib.util.spec_from_file_location("build_method_codec_wasm", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, "build_method_codec_wasm", module)
    spec.loader.exec_module(module)

    def no_network(*_args, **_kwargs):
        raise AssertionError("the build must not reach the network here")

    monkeypatch.setattr(module.urllib.request, "urlopen", no_network)
    return module


def _pin(monkeypatch, codec, payload: bytes) -> None:
    monkeypatch.setattr(codec, "BINARYEN_SHA256", hashlib.sha256(payload).hexdigest())


def test_the_override_is_used_offline_once_its_checksum_matches(
    codec, monkeypatch, tmp_path
):
    local = tmp_path / "binaryen.tar.gz"
    local.write_bytes(b"pinned archive bytes")
    _pin(monkeypatch, codec, b"pinned archive bytes")
    monkeypatch.setenv(codec.ARCHIVE_OVERRIDE_ENV, str(local))
    assert codec.pinned_archive(tmp_path / "cache") == local


def test_an_override_with_the_wrong_checksum_fails_the_build(
    codec, monkeypatch, tmp_path
):
    local = tmp_path / "binaryen.tar.gz"
    local.write_bytes(b"tampered")
    _pin(monkeypatch, codec, b"pinned archive bytes")
    monkeypatch.setenv(codec.ARCHIVE_OVERRIDE_ENV, str(local))
    with pytest.raises(SystemExit, match="REFUSED"):
        codec.pinned_archive(tmp_path / "cache")


def test_a_cached_archive_is_reverified_and_a_corrupt_one_fails(
    codec, monkeypatch, tmp_path
):
    monkeypatch.delenv(codec.ARCHIVE_OVERRIDE_ENV, raising=False)
    cache = tmp_path / "cache"
    cache.mkdir()
    (cache / codec.BINARYEN_ARCHIVE).write_bytes(b"pinned archive bytes")
    _pin(monkeypatch, codec, b"pinned archive bytes")
    assert codec.pinned_archive(cache) == cache / codec.BINARYEN_ARCHIVE
    (cache / codec.BINARYEN_ARCHIVE).write_bytes(b"corrupted in the cache")
    with pytest.raises(SystemExit, match="REFUSED"):
        codec.pinned_archive(cache)


def test_the_tool_cache_is_stable_and_overridable(codec, monkeypatch, tmp_path):
    monkeypatch.setenv(codec.TOOL_CACHE_ENV, str(tmp_path / "tools"))
    assert codec.tool_cache() == tmp_path / "tools"
    monkeypatch.delenv(codec.TOOL_CACHE_ENV)
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "xdg"))
    assert codec.tool_cache() == tmp_path / "xdg" / "epistemic-graph" / "tools"


def test_the_pin_is_one_exact_release():
    source = (ROOT / "scripts" / "build_method_codec_wasm.py").read_text()
    assert 'BINARYEN_VERSION = "version_133"' in source
    assert "3507aedecef25c46f2889530a7da304677e97122869274125f782b586cb508ab" in source
