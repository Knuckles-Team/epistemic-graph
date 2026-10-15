"""Split distribution: one server, an executable-free wheel (EG-CONTRACT-R053)."""

from __future__ import annotations

import re
import sys
import types
from pathlib import Path

import pytest
import tomllib

from epistemic_graph import server_binary

REPO = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.no_engine


def _cargo() -> dict:
    return tomllib.loads((REPO / "Cargo.toml").read_text())


@pytest.mark.spec("EG-CONTRACT-R053.1")
def test_only_server_and_nemesis_binaries_remain_and_tools_are_dispatched() -> None:
    assert {b["name"] for b in _cargo()["bin"]} == {"epistemic-graph-server", "nemesis"}
    dispatcher = (REPO / "src/offline_tools.rs").read_text()
    for name in (
        "restore",
        "migrate-shards",
        "epistemic-graph-rf019-observe",
        "lake-fixture-export",
    ):
        assert f'"{name}" =>' in dispatcher
    main = (REPO / "src/main.rs").read_text()
    assert main.index("offline_tools::run_if_requested()") < main.index("Args::parse()")


@pytest.mark.spec("EG-CONTRACT-R053.3")
def test_server_extra_pins_the_engine_version_and_is_bumped_with_it() -> None:
    version = _cargo()["package"]["version"]
    pyproject = tomllib.loads((REPO / "pyproject.toml").read_text())
    extra = pyproject["project"]["optional-dependencies"]["server"]
    assert extra == [f"epistemic-graph-server=={version}"]
    cfg = (REPO / ".config/bumpversion.cfg").read_text()
    assert "[bumpversion:file:pyproject.toml]" in cfg
    assert 'search = "epistemic-graph-server=={current_version}"' in cfg


@pytest.mark.spec("EG-CONTRACT-R053.4")
def test_server_binary_resolution_order_and_typed_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(sys.modules, "epistemic_graph_server", None)
    monkeypatch.setattr(server_binary.shutil, "which", lambda _: None)
    with pytest.raises(
        server_binary.ServerBinaryNotFoundError, match=r"epistemic-graph\[server\]"
    ):
        server_binary.resolve_server_binary()
    monkeypatch.setattr(server_binary.shutil, "which", lambda _: "/on/path")
    assert server_binary.resolve_server_binary() == "/on/path"
    package = types.SimpleNamespace(binary_path=lambda: "/from/package")
    monkeypatch.setitem(sys.modules, "epistemic_graph_server", package)
    assert server_binary.resolve_server_binary() == "/from/package"


@pytest.mark.spec("EG-CONTRACT-R053.5")
def test_release_server_profile_is_the_size_profile() -> None:
    profile = _cargo()["profile"]["release-server"]
    assert profile == {
        "inherits": "release",
        "opt-level": "s",
        "lto": "fat",
        "codegen-units": 1,
        "panic": "unwind",
        "strip": True,
    }


@pytest.mark.spec("EG-CONTRACT-R053.6")
def test_operator_docs_use_subcommands_not_retired_executables() -> None:
    retired = re.compile(r"^(restore|migrate-shards) --", re.MULTILINE)
    for path in (REPO / "docs").rglob("*.md"):
        assert not retired.search(path.read_text()), path
