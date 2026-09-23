"""Privacy contracts for public deployment documentation."""

from __future__ import annotations

import re
from pathlib import Path

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
    # Relocated from agent-utilities (RF-ADR-009, EH-366): AU's own
    # equivalent gate (tests/gates/test_public_example_privacy.py)
    # dropped this file from its PUBLIC_EXAMPLES tuple on the assumption
    # this file already covered it -- it did not, until now.
    "docs/architecture/graph-backends.md",
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
# EH-366: agent-utilities' equivalent regex
# (ENVIRONMENT_DERIVED_HOST_ALIAS, tests/gates/test_public_example_privacy.py)
# is `(?:rw?|gr)\d{3,}` -- it catches `gr`-prefixed aliases (e.g. `gr1080`,
# a real homelab host referenced by this lane's own tooling) that this
# pattern's `host` alternative did not. Union both vocabularies rather than
# swap one for the other, so neither repo's alias convention regresses.
MACHINE_HOST_ALIAS = re.compile(r"(?i)\b(?:rw?|gr|host)\d{3,}\b")


def test_cluster_runbook_is_environment_neutral() -> None:
    for relative in PUBLIC_SURFACES:
        content = (ROOT / relative).read_text(encoding="utf-8")
        assert PRIVATE_IPV4.search(content) is None, relative
        assert MACHINE_HOME.search(content) is None, relative
        assert ENVIRONMENT_DNS.search(content) is None, relative
        assert MACHINE_HOST_ALIAS.search(content) is None, relative
