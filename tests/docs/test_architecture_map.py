"""EG-T-12.1 to EG-T-12.3: the R052 architecture map in plan.md matches source."""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import tomllib

ROOT = Path(__file__).resolve().parents[2]
PLAN = ROOT / "specs/public-engine-contract-and-release/plan.md"


def _section(heading: str) -> str:
    text = PLAN.read_text(encoding="utf-8")
    start = text.index(heading)
    end = text.find("\n#### ", start + len(heading))
    return text[start : end if end != -1 else len(text)]


def _workspace_packages() -> set[str]:
    manifest = tomllib.loads((ROOT / "Cargo.toml").read_text(encoding="utf-8"))
    members: set[str] = set()
    excluded = {
        (ROOT / pattern).resolve()
        for pattern in manifest["workspace"].get("exclude", [])
    }
    for pattern in manifest["workspace"]["members"]:
        for path in sorted(ROOT.glob(pattern)):
            if path.resolve() in excluded or not (path / "Cargo.toml").is_file():
                continue
            package = tomllib.loads((path / "Cargo.toml").read_text(encoding="utf-8"))
            members.add(package["package"]["name"])
    return members


@pytest.mark.no_engine
@pytest.mark.spec("EG-CONTRACT-R052.1")
def test_module_inventory_matches_workspace_manifests() -> None:
    section = _section("#### Model: module owners and dependencies")
    table_rows = [
        line for line in section.splitlines() if line.startswith("| ") and "`" in line
    ]
    listed = {
        name for row in table_rows for name in re.findall(r"`(eg-[a-z0-9-]+)`", row)
    }
    packages = _workspace_packages()
    assert packages, "workspace members resolve to packages"
    assert packages - listed == set(), "every workspace package has an owner group"
    assert listed - packages == set(), "the map lists no package absent from source"
    assert "Root `src/`" in section, "the root package is included once"

    # The scanner defines no dependency-direction rule, and the map must say so.
    arch_lint = (ROOT / ".config/arch-lint.toml").read_text(encoding="utf-8")
    assert "no Rust dependency-direction rule" in arch_lint
    assert "explicitly defines no dependency-direction rule" in section

    # Sampled dependency observations the map states.
    def deps(crate: str) -> set[str]:
        manifest = tomllib.loads(
            (ROOT / "crates" / crate / "Cargo.toml").read_text(encoding="utf-8")
        )
        return {
            name for name in manifest.get("dependencies", {}) if name.startswith("eg-")
        }

    assert deps("eg-types") == set()
    assert "eg-types" in deps("eg-modality")
    assert "eg-types" in deps("eg-capabilities")
    assert "eg-types" in deps("eg-storage")
    assert {"eg-types", "eg-storage"} <= deps("eg-transaction")


@pytest.mark.no_engine
@pytest.mark.spec("EG-CONTRACT-R052.2")
def test_build_package_and_runtime_boundaries_match_source() -> None:
    section = _section("#### Behavior: builds, packages, and deployments")
    features = tomllib.loads((ROOT / "Cargo.toml").read_text(encoding="utf-8"))[
        "features"
    ]
    assert "full" in features["default"]
    assert "viz" in features["full"]
    assert not any(item.startswith("quantum") for item in features["full"])
    assert "full" in features["cluster"]
    assert "full" in features["full-extras"]
    assert "`default` includes `full`" in section
    assert "Quantum remains optional" in section

    pyproject = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    extras = pyproject["project"]["optional-dependencies"]
    assert extras["full"] == [] and extras["numeric"] == []
    assert "`full = []` and `numeric = []`" in section

    maturin = pyproject["tool"]["maturin"]
    assert maturin["manifest-path"] == "crates/eg-pyengine/Cargo.toml"
    assert set(maturin["features"]) == {"python", "security"}
    action = (ROOT / ".github/actions/folded-wheel/action.yml").read_text(
        encoding="utf-8"
    )
    assert "crates/eg-numeric/Cargo.toml" in action
    assert "--features python" in action
    assert "--require-engine-kernel" in action
    assert "no server crate" in action
    assert "`eg-pyengine/python,security`" in section
    assert "`eg-numeric/python`" in section
    assert "compiles no server crate" in section

    backend = (ROOT / "build_backend.py").read_text(encoding="utf-8")
    assert "def _compose(" in backend
    assert "`_compose`" in section


ANCHOR = re.compile(
    r"`(src/[\w/]+\.rs):(\d+)`, `(\w+)`|`(src/[\w/]+\.rs):(\d+)` exports `(\w+)`"
)


@pytest.mark.no_engine
@pytest.mark.spec("EG-CONTRACT-R052.3")
def test_shared_contract_anchors_resolve_to_source() -> None:
    section = _section("#### Wiring: shared contracts and call sites")
    anchors = [
        (m[0] or m[3], int(m[1] or m[4]), m[2] or m[5]) for m in ANCHOR.findall(section)
    ]
    assert len(anchors) >= 2, (
        "the served mutation and planned query anchors are present"
    )
    for path, line, symbol in anchors:
        lines = (ROOT / path).read_text(encoding="utf-8").splitlines()
        assert symbol in lines[line - 1], f"{path}:{line} names {symbol}"

    symbols = {
        "crates/eg-capabilities/src/lib.rs": "pub fn policy(",
        "crates/eg-transaction/src/kernel.rs": "pub struct MutationKernel",
    }
    for path, needle in symbols.items():
        assert needle in (ROOT / path).read_text(encoding="utf-8"), path
    modality = (ROOT / "crates/eg-modality/src/lib.rs").read_text(encoding="utf-8")
    for exported in ("OpaqueRef", "ArtifactBundle", "EvidenceLocus", "Provenance"):
        assert re.search(rf"pub use [^;]*\b{exported}\b", modality, re.S), exported
    assert "KnowledgeSet" in (ROOT / "crates/eg-plan/src/lib.rs").read_text(
        encoding="utf-8"
    )
    # The map adds no second authority and makes no universal-planner claim.
    assert (
        "adds no planner, identity registry, provenance store, or transaction authority"
        in section
    )
    assert "both directly and through planning" in section
