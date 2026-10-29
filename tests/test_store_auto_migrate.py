"""A previous-patch store migrates when this build opens it (R070.2).

Spec row: MEDULLA-DURABLE-KERNEL-R070.2. A writer engine bootstraps a store and creates a
tenant graph the way arbora does. This build then opens the same store with no offline
step in between. It migrates every older but supported store under its own lock, then
serves: `ListGraphs` returns both graphs for the System identity. A second restart
finds nothing to migrate. With `EPISTEMIC_GRAPH_STORE_AUTO_MIGRATE=false` the engine
keeps the offline refusal. It exits with the store's named format error and the
`store-upgrade apply` command, and that command still works.

The writer is this build, and also the previous patch's published engine when
`EPISTEMIC_GRAPH_PREVIOUS_PATCH_BINARY` names it (`pip install
medulla-stem==<previous>`). A store binds to its files' device and inode, so
the writer creates it in place. The migration stages its scratch copy inside the data
directory, which must not be under `/tmp`. Run the previous-patch leg with `--basetemp`
outside it.
"""

import os
import re
import subprocess

import pytest
from conftest import (
    find_server_binary,
    server_timeout,
)
from test_upgrade_store_listing import (
    PREVIOUS_PATCH_ENV,
    _env,
    _lists_both,
    _Store,
    _writers,
    _written,
)

pytestmark = [
    pytest.mark.no_engine,  # Each case owns its engines on its own store.
    pytest.mark.spec("MEDULLA-DURABLE-KERNEL-R070.2"),
]

SWITCH = "EPISTEMIC_GRAPH_STORE_AUTO_MIGRATE"
ANSI = re.compile(r"\x1b\[[0-9;]*m")


class _MigratingStore(_Store):
    """A `_Store` an engine must also be able to refuse."""

    def refused(self, binary: str, **extra: str) -> tuple[int, str]:
        """Start an engine that must refuse the store; return its exit and output."""

        if os.path.exists(self.socket):
            os.remove(self.socket)
        completed = subprocess.run(
            [binary, "--socket-path", self.socket, "--persist-dir", str(self.persist)],
            env={**_env(self.state), **extra},
            capture_output=True,
            text=True,
            timeout=server_timeout(),
        )
        return completed.returncode, completed.stdout + completed.stderr


def _previous_patch() -> str:
    return os.environ.get(PREVIOUS_PATCH_ENV, "").strip()


def _binary() -> str:
    binary = find_server_binary()
    if binary is None:
        pytest.skip("build the full-featured engine binary before this served test")
    return binary


@pytest.mark.parametrize("writer", _writers())
def test_open_migrates_a_previous_patch_store_and_serves(writer, tmp_path):
    binary = _binary()
    store = _MigratingStore(tmp_path)

    def reopened():
        return _lists_both(store.socket)

    store.run(writer or binary, lambda: _written(store.socket))
    first = ANSI.sub("", store.run(binary, reopened))
    assert "store migration: finished" in first, first
    if writer:
        # A previous-patch store needs at least one registered upgrade.
        assert "store migration: journal written" in first, first
        assert 'outcome="migrated"' in first, first
    assert "FORMAT_UPGRADE_REQUIRED" not in first, first
    # Idempotent: the next open finds nothing to migrate and still serves.
    second = ANSI.sub("", store.run(binary, reopened))
    assert "store migration: journal written" not in second, second
    assert 'outcome="nothing_to_do"' in second, second
    assert not (
        store.persist / ".store-upgrade-staging" / "auto-migrate.journal.json"
    ).exists()


@pytest.mark.skipif(
    not _previous_patch(), reason=f"set {PREVIOUS_PATCH_ENV} to a previous-patch engine"
)
def test_switch_off_keeps_the_offline_refusal_and_the_manual_command(tmp_path):
    binary = _binary()
    store = _MigratingStore(tmp_path)

    store.run(_previous_patch(), lambda: _written(store.socket))
    code, output = store.refused(binary, **{SWITCH: "false"})
    assert code != 0, output
    assert "GRAPH_SHARD_FORMAT_UPGRADE_REQUIRED" in output, output
    assert "store-upgrade apply" in output, output
    # The manual command stays, and the store then serves with the switch off.
    store.upgrade(binary)
    store.run(binary, lambda: _lists_both(store.socket), **{SWITCH: "false"})


def test_an_invalid_switch_value_is_refused_by_name(tmp_path):
    binary = _binary()
    store = _MigratingStore(tmp_path)
    code, output = store.refused(binary, **{SWITCH: "sometimes"})
    assert code != 0, output
    assert "STORE_AUTO_MIGRATE_INVALID_SWITCH" in output, output
