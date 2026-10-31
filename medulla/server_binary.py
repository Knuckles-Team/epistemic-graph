"""Locate the ``medulla-server`` executable.

The public ``medulla`` wheel carries no executables. The server ships in
the separate ``medulla-stem`` package (extra ``server``). Resolution
order: ``medulla_server.binary_path()`` when importable, then
``shutil.which("medulla-server")``, else :class:`ServerBinaryNotFoundError`.
"""

from __future__ import annotations

import shutil

SERVER_BINARY_NAME = "medulla-server"
INSTALL_HINT = 'pip install "medulla[server]"'


class ServerBinaryNotFoundError(RuntimeError):
    """The medulla-server executable is not installed."""


def resolve_server_binary() -> str:
    """Return the path of the installed server executable or raise."""
    try:
        from medulla_server import binary_path
    except ImportError:
        pass
    else:
        return str(binary_path())
    found = shutil.which(SERVER_BINARY_NAME)
    if found:
        return found
    raise ServerBinaryNotFoundError(
        f"{SERVER_BINARY_NAME} is not installed; install it with: {INSTALL_HINT}"
    )
