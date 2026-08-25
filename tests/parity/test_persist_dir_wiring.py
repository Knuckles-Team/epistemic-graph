"""Gate A -- config wiring proof (plan §9.2): `EmbeddedTransport` reads the
SAME `GRAPH_SERVICE_PERSIST_DIR` setting `agent_utilities.knowledge_graph
.core.graph_compute:1914` reads today, and an unset value never silently
falls back to an unmounted default path.

Exercises `epistemic_graph.embedded._resolve_persist_dir` directly -- a pure
function needing neither the native `epistemic_graph.engine` extension nor
the shared out-of-process server, so (unlike the rest of `tests/parity/`)
these tests run and pass in THIS session, with no native build. Marked
`no_engine` (this repo's own convention, `tests/test_create_graph_type_
allowlist.py`) so the parent session-scoped server fixture is skipped
when every selected test carries that marker.
"""

from __future__ import annotations

import pytest

from epistemic_graph.embedded import (
    EmbeddedTransportConfigError,
    _resolve_persist_dir,
)

pytestmark = pytest.mark.no_engine


def test_explicit_persist_dir_wins_over_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GRAPH_SERVICE_PERSIST_DIR", "/from/env")
    assert _resolve_persist_dir("/from/argument") == "/from/argument"


def test_reads_the_graph_compute_setting_name_when_unset_explicitly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # Exact name confirmed against `agent_utilities/knowledge_graph/core/
    # graph_compute.py:1914`: `persist_dir = setting("GRAPH_SERVICE_PERSIST_DIR")`.
    monkeypatch.setenv("GRAPH_SERVICE_PERSIST_DIR", "/var/lib/agent-os/engine")
    assert _resolve_persist_dir(None) == "/var/lib/agent-os/engine"


def test_unset_env_and_no_explicit_argument_refuses_to_start(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("GRAPH_SERVICE_PERSIST_DIR", raising=False)
    with pytest.raises(EmbeddedTransportConfigError, match="GRAPH_SERVICE_PERSIST_DIR"):
        _resolve_persist_dir(None)


def test_unset_env_never_falls_back_to_an_unmounted_default_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The exact hazard plan §1.9 identifies: `agent_utilities.knowledge_
    graph.core.graph_compute`'s OWN fallback (`data_dir() / "graph_
    snapshots"`, `graph_compute.py:1915-1921`) resolves inside the pod's
    `emptyDir` on the live deployment, not the PVC. `EmbeddedTransport` must
    never silently choose ANY path when unset -- proven by asserting the
    unset case is an exception, not merely that it differs from one
    specific fallback string."""
    monkeypatch.delenv("GRAPH_SERVICE_PERSIST_DIR", raising=False)
    with pytest.raises(EmbeddedTransportConfigError):
        _resolve_persist_dir(None)


def test_explicit_in_memory_sentinel_opts_out_without_touching_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("GRAPH_SERVICE_PERSIST_DIR", "/from/env")
    assert _resolve_persist_dir(":memory:") is None
