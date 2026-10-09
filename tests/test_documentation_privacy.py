"""Privacy contracts for public deployment documentation."""

from __future__ import annotations

import re
from pathlib import Path

import check_tracked_privacy as privacy
import pytest

# Pure/static test -- never needs the shared native engine (see
# conftest.py's session-scoped `start_epistemic_graph_server` fixture,
# which this marker exempts this module from triggering).
pytestmark = pytest.mark.no_engine

ROOT = Path(__file__).resolve().parents[1]
PUBLIC_SURFACES = (
    "AGENTS.md",
    "docs/architecture/cluster_deployment.md",
    "docs/deploy/binary_promotion.md",
)
PRIVATE_IPV4 = re.compile(
    r"\b(?:10(?:\.\d{1,3}){3}|192\.168(?:\.\d{1,3}){2}|"
    r"172\.(?:1[6-9]|2\d|3[01])(?:\.\d{1,3}){2})\b"
)
MACHINE_HOME = re.compile(
    r"(?i)(?:[A-Z]:[\\/]Users[\\/][^\\/\s]+|"
    r"/(?:home|Users)/[^/\s]+|/mnt/[A-Z]/Users/[^/\s]+)"
)
ENVIRONMENT_DNS = re.compile(r"(?i)\b(?:[A-Za-z0-9-]+\.)+(?:arpa|local)\b")
# D-EG-PRIVACY-R001-FALSEPOS: mirrors check_tracked_privacy._MACHINE_HOST_ID_RE
# -- a hyphen-preceded match is a requirement-ID citation (`EG-FOO-R001`), not
# a host alias; see that module's own comment for the full rationale.
MACHINE_HOST_ALIAS = re.compile(r"(?i)(?<![a-z0-9-])(?:rw?|host)\d{3,}\b")


def test_cluster_runbook_is_environment_neutral() -> None:
    for relative in PUBLIC_SURFACES:
        content = (ROOT / relative).read_text(encoding="utf-8")
        assert PRIVATE_IPV4.search(content) is None, relative
        assert MACHINE_HOME.search(content) is None, relative
        assert ENVIRONMENT_DNS.search(content) is None, relative
        assert MACHINE_HOST_ALIAS.search(content) is None, relative


@pytest.mark.parametrize("relative", PUBLIC_SURFACES)
@pytest.mark.parametrize(
    "private_text",
    [
        ".".join(("10", "12", "34", "56")),
        "/".join(("", "home", "example", "document")),
        ".".join(("example", "local")),
        "host" + str(123),
    ],
    ids=["private-ip", "home-path", "local-dns", "host-alias"],
)
def test_public_surface_rejects_private_content(
    relative: str,
    private_text: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for surface in PUBLIC_SURFACES:
        path = tmp_path / surface
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("Public deployment documentation.\n", encoding="utf-8")
    (tmp_path / relative).write_text(private_text, encoding="utf-8")
    monkeypatch.setitem(globals(), "ROOT", tmp_path)
    with pytest.raises(AssertionError, match=re.escape(relative)):
        test_cluster_runbook_is_environment_neutral()


@pytest.mark.parametrize("relative", ["docs/guide.md", "src/example.rs"])
@pytest.mark.parametrize("prefix", ["home", "Users", "mnt/c/Users"])
@pytest.mark.parametrize("username", ["李四", "élise", "example李"])
def test_unicode_home_paths_are_rejected(
    relative: str, prefix: str, username: str, tmp_path: Path
) -> None:
    path = tmp_path / relative
    path.parent.mkdir(parents=True)
    path.write_text("/" + prefix + "/" + username + "/state\n", encoding="utf-8")
    findings = privacy.scan(tmp_path)
    assert any("machine-specific home path" in item.category for item in findings)
    path.write_text("/" + prefix + "/example/state\n", encoding="utf-8")
    assert privacy.scan(tmp_path) == []


def test_public_surface_allows_requirement_id_citations(tmp_path: Path) -> None:
    """A hyphen-joined requirement ID (`EG-UNIFIED-DATA-PLANE-R001`) is not a
    host alias; a genuine host alias (`host123`) must still be caught right
    alongside one. Regression for the false-positive flood that broke
    docs/architecture/unified_data_plane_adr.md's own requirement-ID
    citations as "machine-specific host identifier" findings
    (D-EG-PRIVACY-R001-FALSEPOS).
    """
    docs_dir = tmp_path / "docs"
    docs_dir.mkdir()
    (docs_dir / "clean.md").write_text(
        "see `EG-UNIFIED-DATA-PLANE-R001` and `EG-DECISION-ENGINE-R118`\n",
        encoding="utf-8",
    )
    (docs_dir / "leak.md").write_text("reported from host123\n", encoding="utf-8")

    by_path: dict[str, set[str]] = {}
    for item in privacy.scan(tmp_path):
        by_path.setdefault(item.path, set()).add(item.category)

    assert "docs/clean.md" not in by_path
    assert any(
        "machine-specific host identifier" in category
        for category in by_path.get("docs/leak.md", set())
    )
