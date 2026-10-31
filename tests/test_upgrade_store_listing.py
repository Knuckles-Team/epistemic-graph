"""A reopened store lists its graphs after a patch upgrade (MEDULLA-DURABLE-KERNEL-R861.2).

A writer engine bootstraps a store, registers an identity with no grants and creates a
`Global` tenant graph the way arbora does. This build then reopens the same store.
Recovery leaves every graph catalog-only until first access, so `ListGraphs` must decide
each graph from its catalog record. Before the fix it read the resident entry, found
none, and dropped every graph without an access decision, on any restart.

The stored policy still decides: the System identity sees both graphs, and the
registered identity with no grants, or an unregistered principal, sees neither.

The writer is this build, and also the previous patch's published engine when
`MEDULLA_PREVIOUS_PATCH_BINARY` names it (`pip install
medulla-stem==<previous>`). A store binds to its files' device and inode, so a
copied store cannot be committed as a fixture; the writer creates it in place. Between
writer and reader the test runs `store-upgrade apply`, the documented offline step. That
upgrade refuses a store under `/tmp`, so run the previous-patch leg with `--basetemp`
outside it.
"""

import asyncio
import json
import os
import subprocess
from pathlib import Path

import pytest
from conftest import find_server_binary, spawn_server, stop_server, wait_for_server

from medulla.client import MedullaClient
from medulla.generated.cluster import (
    decode_list_graphs,
    send_create_graph,
    send_list_graphs,
)

pytestmark = [
    pytest.mark.no_engine,  # Each case owns its engines on its own store.
    pytest.mark.spec("MEDULLA-DURABLE-KERNEL-R861.2"),
]

AGENT = "service:test-suite"
SIGNER_KEY = "test-operation-signer-key"  # sanitizer:ignore — test-only value
AUTH_SECRET = "test-upgrade-listing-secret"  # sanitizer:ignore — test-only value
TENANT_GRAPH = "tenant__homelab____commons__"
UNGRANTED = "service:upgrade-ungranted"
UNREGISTERED = "service:upgrade-unregistered"
BOTH = ["__commons__", TENANT_GRAPH]
PREVIOUS_PATCH_ENV = "MEDULLA_PREVIOUS_PATCH_BINARY"


def _context(agent: str = AGENT, *, roles=None, scopes=None) -> dict[str, object]:
    return {
        "principal": agent,
        "tenant": "tenant:test",
        "audience": "medulla-test",
        "agent_id": agent,
        "roles": list(roles if roles is not None else ["test"]),
        "scopes": list(scopes if scopes is not None else ["*"]),
        "policy_version": "policy:test",
        "delegation": [],
    }


def _env(state_dir: Path) -> dict[str, str]:
    inherited = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("GRAPH_SERVICE_", "MEDULLA_"))
    }
    return {
        **inherited,
        "GRAPH_SERVICE_AUTH_SECRET": AUTH_SECRET,
        "MEDULLA_REQUIRE_OIDC": "false",
        "MEDULLA_AUDIENCE": "medulla-test",
        "MEDULLA_TENANT": "tenant:test",
        "MEDULLA_POLICY_VERSION": "policy:test",
        "MEDULLA_SECURITY_STATE_DIR": str(state_dir),
        "MEDULLA_SIGNER_KEYS_JSON": json.dumps(
            {AGENT: {"key": SIGNER_KEY, "allowed_roles": [], "may_grant_system": True}}
        ),
    }


async def _connect(
    socket_path: str, context: dict[str, object]
) -> MedullaClient:
    return await MedullaClient.connect(
        socket_path=socket_path, auth_secret=AUTH_SECRET, verified_context=context
    )


async def _populate(socket_path: str) -> None:
    client = await _connect(
        socket_path, _context(roles=[], scopes=["security:bootstrap"])
    )
    try:
        await client.consensus.bootstrap_system_identity(
            agent_id=AGENT, signer_id=AGENT, signer_key=SIGNER_KEY
        )
    finally:
        await client.close()
    client = await _connect(socket_path, _context())
    try:
        await client.consensus.register_identity(
            UNGRANTED, "Agent", [], [], signer_id=AGENT, signer_key=SIGNER_KEY
        )
        await send_create_graph(
            client,
            {"graph_name": TENANT_GRAPH, "graph_type": "Global"},
            TENANT_GRAPH,
            idempotency_key=f"arbora:create-graph:{TENANT_GRAPH}",
        )
    finally:
        await client.close()


async def _listed(socket_path: str, agent: str) -> list[str] | str:
    client = await _connect(socket_path, _context(agent))
    try:
        result = await send_list_graphs(client, {}, TENANT_GRAPH)
        return sorted(listing.name for listing in decode_list_graphs(result))
    except Exception as refused:  # an outright refusal also lists nothing
        return f"refused: {refused}"
    finally:
        await client.close()


async def _written(socket_path: str) -> None:
    """Populate a fresh store the way arbora does, and see both graphs."""

    await _populate(socket_path)
    assert await _listed(socket_path, AGENT) == BOTH


async def _lists_both(socket_path: str) -> None:
    assert await _listed(socket_path, AGENT) == BOTH


class _Store:
    """One store on disk, served by a fresh engine process per `run`."""

    def __init__(self, root: Path):
        self.persist = root / "persist"
        self.state = root / "security"
        self.socket = str(root / "engine.sock")
        self.log = root / "engine.log"
        self.persist.mkdir()

    def upgrade(self, binary: str) -> None:
        """The operator's offline step between patches; a no-op on a current store."""

        upgraded = subprocess.run(
            [binary, "store-upgrade", "apply", str(self.persist), "--confirm"],
            capture_output=True,
            text=True,
            timeout=300,
        )
        assert upgraded.returncode == 0, upgraded.stdout + upgraded.stderr

    def run(self, binary: str, check, **extra: str) -> str:
        """Serve the store once with `extra` environment; return the engine log."""

        if os.path.exists(self.socket):
            os.remove(self.socket)
        process = spawn_server(
            [binary, "--socket-path", self.socket, "--persist-dir", str(self.persist)],
            env={**_env(self.state), **extra},
            log_path=self.log,
        )
        try:
            wait_for_server(
                process, self.socket, name="upgrade engine", log_path=self.log
            )
            asyncio.run(check())
        finally:
            stop_server(process, name="upgrade engine", log_path=self.log)
        return self.log.read_text(errors="replace")


def _writers() -> list:
    previous = os.environ.get(PREVIOUS_PATCH_ENV, "").strip()
    return [
        pytest.param("", id="written-by-this-build"),
        pytest.param(
            previous,
            id="written-by-previous-patch",
            marks=pytest.mark.skipif(
                not previous,
                reason=f"set {PREVIOUS_PATCH_ENV} to a previous-patch engine",
            ),
        ),
    ]


@pytest.mark.parametrize("writer", _writers())
def test_reopened_store_lists_graphs_for_the_stored_policy(writer, tmp_path):
    binary = find_server_binary()
    if binary is None:
        pytest.skip("build the full-featured engine binary before this served test")
    store = _Store(tmp_path)

    async def reopened() -> None:
        await _lists_both(store.socket)
        for stranger in (UNGRANTED, UNREGISTERED):
            listed = await _listed(store.socket, stranger)
            assert not isinstance(listed, list) or listed == [], (stranger, listed)

    store.run(writer or binary, lambda: _written(store.socket))
    store.upgrade(binary)
    store.run(binary, reopened)
    # A second restart of the upgraded store still lists both graphs.
    store.run(binary, reopened)
