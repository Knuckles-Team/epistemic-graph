"""A short pytest base temp for replicated pytest steps (AF_UNIX path limit).

WHY
---
The engine-backed Python suite binds a Unix socket under pytest's temporary
directory (``<basetemp>/popen-gw<N>/epistemic-graph-runtime<N>/engine.sock``).
Linux caps a socket path at 108 bytes including the terminator.  A hosted
runner's ``/tmp`` leaves plenty of room; a replica host does not: the replica's
``TMPDIR`` sits under a lane's run directory, pytest adds
``pytest-of-<user>/pytest-<N>``, and on 2026-09-24 every engine test of a run on
R710 errored ``OSError: AF_UNIX path too long`` at setup (2849 errors, zero
tests executed).

WHAT
----
Every replicated step gets ``PYTEST_ADDOPTS=--basetemp=<short>`` whose deepest
engine socket fits the limit.  ``<short>`` is one directory per replica process
under a short writable temporary root, so two replica processes on one host
never share -- pytest deletes an explicit basetemp before it starts.  A step's
own ``PYTEST_ADDOPTS`` is kept AFTER ours, so a step that names its own
``--basetemp`` still wins.  Only pytest reads the variable.
"""

from __future__ import annotations

import atexit
import os
import shutil
import tempfile
from pathlib import Path

#: sun_path is 108 bytes including the terminating NUL.
AF_UNIX_PATH_MAX = 107
#: The deepest engine socket the suite creates below the base temp (xdist
#: worker directory and pytest's numbered runtime directory both allow for
#: three-digit counters).
SOCKET_TAIL = "/popen-gw999/epistemic-graph-runtime999/engine.sock"

_BASETEMP: Path | None = None


def _parents() -> tuple[Path, Path]:
    """Try the conventional short roots; existence alone does not imply writable."""
    root = Path(tempfile.gettempdir()).anchor
    return (Path(root) / "var" / "tmp", Path(root) / "tmp")


def short_basetemp() -> Path:
    """This replica process's pytest base temp (created lazily, removed at exit)."""
    global _BASETEMP
    if _BASETEMP is None:
        _BASETEMP = _create_short_basetemp()
    return _BASETEMP


def _create_short_basetemp() -> Path:
    failures = []
    for parent in _parents():
        try:
            directory = Path(
                tempfile.mkdtemp(prefix=f"egr-{os.getuid()}-{os.getpid()}-", dir=parent)
            )
        except OSError as exc:
            failures.append(f"{parent}: {exc}")
            continue
        basetemp = directory / "pt"
        if fits_socket_limit(basetemp):
            atexit.register(shutil.rmtree, directory, True)
            return basetemp
        shutil.rmtree(directory)
        failures.append(f"{parent}: AF_UNIX socket path would exceed 107 bytes")
    raise RuntimeError("no writable short pytest temp root: " + "; ".join(failures))


def fits_socket_limit(basetemp: Path) -> bool:
    """Whether the deepest engine socket under ``basetemp`` fits AF_UNIX."""
    return len(os.fsencode(str(basetemp) + SOCKET_TAIL)) <= AF_UNIX_PATH_MAX


def with_short_basetemp(env: dict[str, str]) -> dict[str, str]:
    """``env`` with ``--basetemp=<short>`` leading ``PYTEST_ADDOPTS``."""
    basetemp = short_basetemp()
    if not fits_socket_limit(basetemp):
        raise RuntimeError(f"pytest base temp {basetemp} is too long for AF_UNIX")
    existing = env.get("PYTEST_ADDOPTS", "").strip()
    option = f"--basetemp={basetemp}"
    env["PYTEST_ADDOPTS"] = f"{option} {existing}" if existing else option
    return env
