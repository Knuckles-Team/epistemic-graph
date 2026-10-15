"""Locate the ``epistemic-graph-server`` executable.

The public ``epistemic-graph`` wheel carries no executables. The server ships in
the separate ``epistemic-graph-server`` package (extra ``server``). Resolution
order: ``epistemic_graph_server.binary_path()`` when importable, then
``shutil.which("epistemic-graph-server")``, else :class:`ServerBinaryNotFoundError`.
"""

from __future__ import annotations

import shutil

SERVER_BINARY_NAME = "epistemic-graph-server"
INSTALL_HINT = 'pip install "epistemic-graph[server]"'


class ServerBinaryNotFoundError(RuntimeError):
    """The epistemic-graph-server executable is not installed."""


def resolve_server_binary() -> str:
    """Return the path of the installed server executable or raise."""
    try:
        from epistemic_graph_server import binary_path
    except ImportError:
        binary_path = None
    if binary_path is not None:
        return str(binary_path())
    found = shutil.which(SERVER_BINARY_NAME)
    if found:
        return found
    raise ServerBinaryNotFoundError(
        f"{SERVER_BINARY_NAME} is not installed; install it with: {INSTALL_HINT}"
    )
