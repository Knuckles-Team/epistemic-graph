"""EG-CONTRACT-R002.3: the installed-wheel / no-source-tree-import check.

The same boundary contract NE-249 (``crates/eg-numeric/tests/test_builtin_
boundary.py``) and BUG-PE-002 (the release workflow's "engine kernel smoke
test" step) already prove for the compiled numeric and engine kernels,
applied to the pure-Python modules EG-CONTRACT-R002.1/.2 exercise against a
live engine: ``epistemic_graph.decision_client``, ``epistemic_graph.
connector_pack`` and ``epistemic_graph.client`` must resolve from the
INSTALLED wheel, not this checkout's source tree, when imported under the
release workflow's ``.gates-wheel-venv``.

This test is deliberately skippable outside that job (it never installs a
wheel itself): running the full root suite from an ordinary source checkout
has no installed ``epistemic-graph`` distribution to prove anything about,
exactly the same opt-in posture NE-249's own boundary test takes for the
folded numeric kernel. The release workflow runs it from a scratch directory
(not the repo root) with the gates venv's interpreter, so a bare `python -m
pytest` cannot let CWD-first sys.path resolve the source tree instead (the
exact shadowing bug BUG-PE-002's comment documents for the numeric/engine
smoke steps); this test also checks that directly, independent of how it is
invoked, by resolving each module's own `__file__`.
"""

from __future__ import annotations

import importlib
import importlib.metadata
from pathlib import Path

import pytest

pytestmark = pytest.mark.no_engine

_REPO_ROOT = Path(__file__).resolve().parent.parent

_BOUNDARY_MODULES = (
    "epistemic_graph.decision_client",
    "epistemic_graph.connector_pack",
    "epistemic_graph.client",
)


def _installed_or_skip() -> None:
    try:
        importlib.metadata.distribution("epistemic-graph")
    except importlib.metadata.PackageNotFoundError:
        pytest.skip(
            "epistemic-graph is not an installed distribution here "
            "(no wheel installed; run under the release workflow's "
            ".gates-wheel-venv to exercise this boundary)"
        )


@pytest.mark.parametrize("module_name", _BOUNDARY_MODULES)
def test_decide_and_pack_client_modules_resolve_outside_the_source_tree(
    module_name: str,
) -> None:
    """Each client module EG-CONTRACT-R002.1/.2's served tests import must
    come from the installed wheel's site-packages, never from this
    checkout -- otherwise a served test against the installed wheel would
    silently exercise uncommitted or unreleased source instead."""
    _installed_or_skip()
    module = importlib.import_module(module_name)
    assert module.__file__ is not None, f"{module_name} has no source file"
    resolved = Path(module.__file__).resolve()
    assert not resolved.is_relative_to(_REPO_ROOT), (
        f"{module_name} resolved to {resolved}, inside the source tree "
        f"{_REPO_ROOT} -- the installed wheel is being shadowed"
    )


def test_installed_distribution_matches_the_wheel_not_an_editable_install() -> None:
    """An editable/develop install would report its `RECORD`-less location
    inside the checkout; a real wheel install reports a site-packages path
    outside it (same check EG-CONTRACT-R015's installed-wheel test already
    makes for the contract/errors/scopes JSON, applied here to the package
    distribution itself)."""
    _installed_or_skip()
    distribution = importlib.metadata.distribution("epistemic-graph")
    location = Path(str(distribution.locate_file(""))).resolve()
    assert not location.is_relative_to(_REPO_ROOT), (
        f"epistemic-graph distribution resolved to {location}, inside the "
        f"source tree {_REPO_ROOT} -- expected an installed wheel, not an "
        f"editable/source-linked install"
    )
