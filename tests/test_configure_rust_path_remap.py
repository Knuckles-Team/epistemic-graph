"""Tests for ``scripts/configure_rust_path_remap.py::_resolve_probe_compiler``.

This function had no direct test coverage before it was decomposed into
``_compiler_env_var_for`` / ``_probe_candidate_names`` /
``_compiler_from_env_override`` / ``_cross_compiler_fallback`` /
``_host_native_compiler_fallback``. These tests pin the exact fallback chain
it must preserve: an unrelated *FLAGS variable resolves to no compiler; a
configured environment override wins when its binary is on PATH; a
target-scoped override beats the bare one; a cross build falls back to the
target-triple-prefixed binary and NEVER to a generic host compiler; and a
non-cross (host-native) build falls back to the generic compiler on PATH.
"""

from __future__ import annotations

import pytest

from scripts.configure_rust_path_remap import _resolve_probe_compiler

# Static tests of a pure-Python resolution function with shutil.which
# monkeypatched away -- never touches the compiled engine.
pytestmark = pytest.mark.no_engine


def test_unrelated_variable_resolves_to_no_compiler(monkeypatch):
    monkeypatch.setattr(
        "scripts.configure_rust_path_remap.shutil.which", lambda name: None
    )
    assert _resolve_probe_compiler("LDFLAGS", None, {}) is None


def test_bare_env_override_used_when_binary_on_path(monkeypatch):
    monkeypatch.setattr(
        "scripts.configure_rust_path_remap.shutil.which",
        lambda name: f"/usr/bin/{name}" if name == "clang" else None,
    )
    assert _resolve_probe_compiler("CFLAGS", None, {"CC": "clang -pipe"}) == "clang"


def test_target_scoped_override_wins_over_bare_override(monkeypatch):
    monkeypatch.setattr(
        "scripts.configure_rust_path_remap.shutil.which",
        lambda name: f"/usr/bin/{name}",
    )
    environ = {
        "CC_x86_64-unknown-linux-gnu": "target-cc",
        "CC": "generic-cc",
    }
    assert (
        _resolve_probe_compiler("CFLAGS", "x86_64-unknown-linux-gnu", environ)
        == "target-cc"
    )


def test_override_binary_missing_from_path_is_skipped(monkeypatch):
    monkeypatch.setattr(
        "scripts.configure_rust_path_remap.shutil.which", lambda name: None
    )
    assert _resolve_probe_compiler("CFLAGS", None, {"CC": "ghost-cc"}) is None


def test_cross_build_falls_back_to_target_triple_gcc(monkeypatch):
    monkeypatch.setattr(
        "scripts.configure_rust_path_remap.shutil.which",
        lambda name: (
            "/usr/bin/x86_64-linux-gnu-gcc" if name == "x86_64-linux-gnu-gcc" else None
        ),
    )
    assert (
        _resolve_probe_compiler("CFLAGS", "x86_64-linux-gnu", {})
        == "x86_64-linux-gnu-gcc"
    )


def test_cross_build_never_falls_back_to_generic_host_compiler(monkeypatch):
    # A generic "cc"/"gcc" exists on PATH, but no target-triple-prefixed
    # binary does -- must report unverifiable, never the host's own compiler.
    monkeypatch.setattr(
        "scripts.configure_rust_path_remap.shutil.which",
        lambda name: "/usr/bin/cc" if name == "cc" else None,
    )
    assert _resolve_probe_compiler("CFLAGS", "x86_64-linux-gnu", {}) is None


def test_host_native_build_falls_back_to_generic_compiler(monkeypatch):
    monkeypatch.setattr(
        "scripts.configure_rust_path_remap.shutil.which",
        lambda name: "/usr/bin/cc" if name == "cc" else None,
    )
    assert _resolve_probe_compiler("CFLAGS", None, {}) == "cc"


def test_host_native_build_no_compiler_on_path_is_unverifiable(monkeypatch):
    monkeypatch.setattr(
        "scripts.configure_rust_path_remap.shutil.which", lambda name: None
    )
    assert _resolve_probe_compiler("CXXFLAGS", None, {}) is None
