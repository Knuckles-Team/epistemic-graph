"""Main documentation must deploy without waiting for the release queue."""

from pathlib import Path
from typing import Any

import pytest
import yaml

pytestmark = pytest.mark.no_engine

WORKFLOWS = Path(__file__).resolve().parents[1] / ".github" / "workflows"


def _workflow(name: str) -> dict[str, Any]:
    return yaml.safe_load((WORKFLOWS / name).read_text(encoding="utf-8"))


def test_pages_refreshes_every_main_push_independently_of_release() -> None:
    pages = _workflow("pages.yml")
    release = _workflow("release.yml")
    # BaseLoader keeps GitHub's `on` key rather than YAML 1.1's boolean True.
    triggers = yaml.load(
        (WORKFLOWS / "pages.yml").read_text(encoding="utf-8"), Loader=yaml.BaseLoader
    )["on"]
    assert triggers == {"push": {"branches": ["main"]}}
    assert pages["concurrency"] == {
        "group": "pages-${{ github.ref }}",
        "cancel-in-progress": False,
    }
    assert pages["concurrency"]["group"] != release["concurrency"]["group"]
    assert set(pages["jobs"]) == {"pages"}
    assert "pages" not in release["jobs"]
    assert "needs" not in pages["jobs"]["pages"]


def test_pages_preserves_main_only_native_dashboard_deployment() -> None:
    workflow = _workflow("pages.yml")
    assert workflow["permissions"] == {"contents": "read"}
    # Retain the shared native Pages implementation and its existing access.
    # The explicit guard also protects against future trigger additions: tags
    # and pull requests cannot reach the github-pages deployment environment.
    assert workflow["jobs"]["pages"] == {
        "name": "Deploy GitHub Pages",
        "if": "github.event_name == 'push' && github.ref == 'refs/heads/main'",
        "uses": "Knuckles-Team/pipelines/.github/workflows/pages_pipeline.yml@main",
        "with": {
            "content_source": "docs",
            "shared_theme_enabled": True,
            "spec_dashboard_enabled": True,
        },
        "permissions": {
            "contents": "write",
            "pages": "write",
            "id-token": "write",
        },
    }
