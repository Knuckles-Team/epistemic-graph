"""Static contract for the production and exact offline engine images."""

from __future__ import annotations

from pathlib import Path

import pytest

pytestmark = pytest.mark.no_engine

ROOT = Path(__file__).resolve().parents[1]
DOCKERFILE = ROOT / "docker" / "Dockerfile"
DOCKERIGNORE = ROOT / ".dockerignore"
PYTHON_IMAGE = (
    "python:3.14-slim@sha256:"
    "cea0e6040540fb2b965b6e7fb5ffa00871e632eef63719f0ea54bca189ce14a6"
)


def test_release_local_is_python314_hash_locked_and_path_origin_free() -> None:
    source = DOCKERFILE.read_text(encoding="utf-8")
    local_stage = source.split("FROM builder-base AS builder-release-local", 1)[1]
    local_stage = local_stage.split(f"FROM {PYTHON_IMAGE}", 1)[0]
    runtime_stage = source.split(f"FROM {PYTHON_IMAGE} AS runtime-base", 1)[1]

    for required in (
        "--offline",
        "--no-index",
        "--only-binary :all:",
        "--require-hashes",
        "RELEASE_REQUIREMENTS_SHA256",
        'm.version("epistemic-graph")',
        'shutil.which("epistemic-graph-server")',
        'glob("*.dist-info/direct_url.json")',
        "import epistemic_graph.numeric",
        "sys.version_info[:2] == (3, 14)",
    ):
        assert required in local_stage

    assert " @ file:" not in local_stage
    assert "FROM runtime-base AS release-local" in source
    assert source.count(f"FROM {PYTHON_IMAGE}") == 2
    assert "python:3.11" not in source
    assert "python:3.12" not in source
    assert source.rsplit("FROM runtime-base AS ", 1)[1].startswith("runtime\n")
    assert "apt-get" not in runtime_stage
    assert "netcat" not in runtime_stage
    assert "socket.create_connection" in runtime_stage


def test_release_context_admits_only_wheels_and_requirements() -> None:
    ignored = DOCKERIGNORE.read_text(encoding="utf-8").splitlines()

    assert "release-wheelhouse/*" in ignored
    assert "!release-wheelhouse/*.whl" in ignored
    assert "!release-wheelhouse/*.txt" in ignored
