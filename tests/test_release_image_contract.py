"""Static contract for the server image (EG-CONTRACT-R053.11)."""

from __future__ import annotations

from pathlib import Path

import pytest

pytestmark = pytest.mark.no_engine

ROOT = Path(__file__).resolve().parents[1]
DOCKERFILE = ROOT / "docker" / "Dockerfile"
DOCKERIGNORE = ROOT / "docker" / "Dockerfile.dockerignore"


def test_base_images_are_digest_pinned_and_runtime_has_no_interpreter() -> None:
    source = DOCKERFILE.read_text(encoding="utf-8")
    for line in source.splitlines():
        if line.startswith("FROM python:") or "--from=ghcr.io/astral-sh/uv" in line:
            assert "@sha256:" in line, line
    runtime = source.split("AS runtime", 1)[1]
    assert "python" not in runtime.split("COPY --from=builder", 1)[0]
    assert "apt-get" not in runtime


def test_build_context_excludes_credentials_and_build_trees() -> None:
    ignored = DOCKERIGNORE.read_text(encoding="utf-8").splitlines()
    for entry in (".git", ".env", "*.pem", "target/", "crates/", "src/"):
        assert entry in ignored
