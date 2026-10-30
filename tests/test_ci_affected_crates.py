"""Pull-request affected-crate selection (spec EG-CONTRACT-R053.7)."""

from __future__ import annotations

import importlib.util
import pathlib

import pytest
import yaml

ROOT = pathlib.Path(__file__).resolve().parents[1]
_spec = importlib.util.spec_from_file_location(
    "ci_affected_crates", ROOT / "scripts" / "ci_affected_crates.py"
)
assert _spec is not None and _spec.loader is not None
mod = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(mod)


def _meta() -> dict:
    def pkg(name, path, deps=()):
        return {
            "id": name,
            "name": name,
            "manifest_path": f"/ws/{path}Cargo.toml",
            "dependencies": [{"name": d, "path": "/x"} for d in deps]
            + [{"name": "serde"}],
        }

    pkgs = [
        pkg("eg-types", "crates/eg-types/"),
        pkg("eg-storage", "crates/eg-storage/", ["eg-types"]),
        pkg("eg-viz", "crates/eg-viz/"),
        pkg("eg-numeric", "crates/eg-numeric/", ["eg-types"]),
        pkg("facade", "", ["eg-storage", "eg-viz", "eg-numeric"]),
    ]
    return {
        "workspace_root": "/ws",
        "packages": pkgs,
        "workspace_members": [p["id"] for p in pkgs],
    }


@pytest.mark.spec("EG-CONTRACT-R053.7")
@pytest.mark.parametrize(
    ("changed", "expected"),
    [
        (["docs/a.md", "specs/x/requirements.md", "tests/test_a.py"], []),
        (["crates/eg-viz/src/lib.rs"], ["eg-viz", "facade"]),
        (
            ["crates/eg-types/src/lib.rs"],
            ["eg-numeric", "eg-storage", "eg-types", "facade"],
        ),
        (["src/server/mod.rs"], ["facade"]),
        (["Cargo.lock"], ["eg-numeric", "eg-storage", "eg-types", "eg-viz", "facade"]),
        (["Cargo.toml"], ["eg-numeric", "eg-storage", "eg-types", "eg-viz", "facade"]),
    ],
)
def test_affected_is_owners_plus_reverse_dependencies(changed, expected) -> None:
    assert mod.affected(_meta(), changed) == expected


@pytest.mark.spec("EG-CONTRACT-R053.7")
def test_runs_keep_main_feature_profiles() -> None:
    runs = mod.commands(_meta(), ["eg-numeric", "eg-storage", "eg-types", "facade"])
    assert runs == [
        "{tool} -p eg-types --all-features",
        "{tool} -p eg-storage --all-features {serial}",
        f"{{tool}} -p eg-numeric --features {mod.PYO3_FEATURES}",
        "{tool} -p facade --features full",
    ]
    assert mod.commands(_meta(), []) == []


@pytest.mark.spec("EG-CONTRACT-R053.7")
def test_pull_request_runs_one_gate() -> None:
    ci = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8"))
    jobs = ci["jobs"]
    assert jobs["ci"]["name"] == "CI"
    assert jobs["ci"]["if"] == "github.event_name == 'pull_request'"
    assert "ci" in jobs["ci-required"]["needs"]
    for name in ("light-gates", "changes"):
        assert jobs[name]["if"] == "github.event_name != 'pull_request'"
    assert "pull_request" in jobs["security"]["if"]
    script = "\n".join(s.get("run", "") for s in jobs["ci"]["steps"])
    assert "ci_affected_crates.py" in script
    assert "--from-ref" in script and "spec-trailers" in script
    assert "cargo nextest run" in script and "cargo clippy" in script
