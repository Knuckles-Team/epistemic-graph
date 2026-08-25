"""Differential-parity test fixtures (plan §3.1). FOUNDATION-owned (Wave 0) --
no lane edits this file after Wave 0 lands.

Reuses the shared out-of-process server: `tests/conftest.py`'s autouse,
session-scoped `start_epistemic_graph_server` fixture already starts one and
exports `GRAPH_SERVICE_SOCKET`/`GRAPH_SERVICE_AUTH_SECRET` for the whole
session -- that parent `conftest.py` is loaded automatically for every test
under `tests/`, including this subdirectory, so this file does NOT spin up a
second server (per the task brief: locate and reuse the existing fixture,
don't write a new one).
"""

from __future__ import annotations

import asyncio
import os
import uuid

import pytest

# Plain (non-relative) import: `tests/` has no `__init__.py`, so pytest's
# default "prepend" import mode treats `tests/parity/` as a standalone,
# package-less directory -- it inserts that directory into `sys.path` and
# imports every module in it (including this `conftest.py`) as a top-level
# module, not a package submodule. A relative `from ._harness import ...`
# would fail here with "attempted relative import with no known parent
# package"; every module in this directory imports `_harness` the same way.
from _harness import BoundEmbeddedTransport, TransportPair

from epistemic_graph.client import EpistemicGraphClient
from epistemic_graph.embedded import EmbeddedTransport

# Two distinct principals for the RLS cases (plan §3.1/§4.3's proof-of-concept
# requirement). OWNER reuses this session's already-bootstrapped System
# identity -- the literal values are duplicated from `tests/conftest.py`'s
# `TEST_AGENT_ID`/`TEST_SIGNER_KEY` rather than cross-imported: `tests/` has
# no `__init__.py`, so each subdirectory is its own separate "rootless"
# pytest collection scope (see `tests/test_isolation.py`'s sibling `from
# conftest import ...`, which resolves to `tests/conftest.py` only because
# that test file lives directly in `tests/`, not a subdirectory of it).
# OTHER is a deliberately UNREGISTERED identity -- the simplest, already-
# proven-over-the-wire isolation case (`tests/test_isolation.py::
# test_unregistered_identity_is_denied`), not a row-level ownership scenario
# (which would need an RBAC grant flow out of scope for this Wave-0 proof).
OWNER_AGENT_ID = "service:test-suite"
OTHER_AGENT_ID = "service:parity-unregistered-other"


def _context(agent_id: str) -> dict[str, object]:
    return {
        "principal": agent_id,
        "tenant": "tenant:test",
        "audience": "epistemic-graph-test",
        "agent_id": agent_id,
        "roles": ["test"],
        "scopes": ["*"],
        "policy_version": "policy:test",
        "delegation": [],
    }


@pytest.fixture
def owner_agent_id() -> str:
    """Exposed as a fixture (not imported directly) so test files never need
    `from conftest import ...` -- `tests/` has no `__init__.py`, and pytest's
    per-conftest module naming under nested, package-less directories is not
    worth depending on when a fixture does the same job cleanly."""
    return OWNER_AGENT_ID


@pytest.fixture
def other_agent_id() -> str:
    return OTHER_AGENT_ID


@pytest.fixture
def parity_graph() -> str:
    """A fresh, uniquely-named graph per test -- never the shared
    `__commons__` default (GOC-70 rule 2: no dependence on ambient state a
    sibling test could be concurrently mutating)."""
    return f"agent:parity-{uuid.uuid4().hex[:12]}"


@pytest.fixture
def embedded_persist_dir(tmp_path) -> str:
    """A real, per-test persistence directory for `EmbeddedTransport` -- never
    the ambient `GRAPH_SERVICE_PERSIST_DIR` the shared session server also
    uses (plan §1.9: an explicit, test-owned directory, not a silently-shared
    ambient one)."""
    path = tmp_path / "embedded-persist"
    path.mkdir()
    return str(path)


@pytest.fixture
def pair_factory(embedded_persist_dir):
    """Factory: build a matched `TransportPair` (socket + embedded) for one
    agent_id, against the shared session server (socket) and ONE shared
    in-process engine (embedded) for the whole test.

    BUG-PE-022 (fixed here): the previous shape built a SEPARATE
    `EmbeddedTransport`/native `Engine` per agent_id, all pointed at the same
    `embedded_persist_dir`. That cannot work: `src/persist_lock.rs`'s
    advisory flock is scoped to the OS open-file-description, not the
    process, so a second same-process `Engine(persist_dir=...)` open of one
    `persist_dir` is denied outright (confirmed by the Rust lane's
    throwaway `fs4` probe) -- two principals could never even get far enough
    to compare what they see. `crates/eg-pyengine` (commit `b48ee56c`) closed
    the actual gap instead: `get_node_properties`/`has_node` now accept a
    per-call `agent_id` override (`authority::EmbeddedAuthority`), so ONE
    engine can answer for many principals. This factory now opens exactly
    ONE `EmbeddedTransport` per `embedded_persist_dir` (cached below,
    keyed on that path -- `embedded_persist_dir` is one-per-test, so in
    practice this cache never holds more than one entry) and hands out a
    `BoundEmbeddedTransport` (`_harness.py`) per agent_id, each threading its
    own identity through as the per-call override. The shared engine itself
    is constructed with `agent_id=None` (the trusted-caller default) so no
    single principal is implicitly favored -- every real read goes through
    an explicit per-call override on the bound wrapper.
    """
    socket_clients: list[EpistemicGraphClient] = []
    shared_embedded: dict[str, EmbeddedTransport] = {}

    async def _make(agent_id: str, graph_name: str) -> TransportPair:
        socket_path = os.environ["GRAPH_SERVICE_SOCKET"]
        socket_client = await EpistemicGraphClient.connect(
            socket_path=socket_path,
            graph_name=graph_name,
            verified_context=_context(agent_id),
        )
        socket_clients.append(socket_client)

        shared = shared_embedded.get(embedded_persist_dir)
        if shared is None:
            shared = EmbeddedTransport(
                graph_name=graph_name,
                persist_dir=embedded_persist_dir,
                agent_id=None,
            )
            shared_embedded[embedded_persist_dir] = shared
        embedded = BoundEmbeddedTransport(shared, agent_id)
        return TransportPair(socket=socket_client, embedded=embedded)

    yield _make

    async def _cleanup() -> None:
        for client in socket_clients:
            try:
                await client.close()
            except Exception:
                pass

    asyncio.get_event_loop_policy().new_event_loop().run_until_complete(_cleanup())
