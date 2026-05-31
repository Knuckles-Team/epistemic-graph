# CONCEPT:KG-2.19 — Epistemic Graph Service CLI
#
# Command-line interface for managing the epistemic-graph Tokio service.
# Handles start/stop/status/ping operations.

from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
import subprocess
import sys

logger = logging.getLogger(__name__)


def _default_socket_path() -> str:
    fallback = os.path.join(
        os.path.expanduser("~"), ".local", "state", "epistemic-graph"
    )
    runtime_dir = os.environ.get("XDG_RUNTIME_DIR", fallback)  # nosec B108
    os.makedirs(runtime_dir, exist_ok=True)
    return os.path.join(runtime_dir, "epistemic-graph.sock")


def _default_pid_path() -> str:
    fallback = os.path.join(
        os.path.expanduser("~"), ".local", "state", "epistemic-graph"
    )
    runtime_dir = os.environ.get("XDG_RUNTIME_DIR", fallback)  # nosec B108
    os.makedirs(runtime_dir, exist_ok=True)
    return os.path.join(runtime_dir, "epistemic-graph.pid")


def _find_server_binary() -> str | None:
    """Locate the epistemic-graph-server binary."""
    # Check common locations.
    candidates = [
        os.path.join(
            os.path.dirname(__file__),
            "..",
            "target",
            "release",
            "epistemic-graph-server",
        ),
        os.path.join(
            os.path.dirname(__file__), "..", "target", "debug", "epistemic-graph-server"
        ),
    ]
    # Also check PATH.
    import shutil

    path_bin = shutil.which("epistemic-graph-server")
    if path_bin:
        candidates.insert(0, path_bin)

    for candidate in candidates:
        if os.path.isfile(candidate) and os.access(candidate, os.X_OK):
            return os.path.abspath(candidate)
    return None


def cmd_start(args: argparse.Namespace) -> None:
    """Start the epistemic-graph service as a background process."""
    binary = _find_server_binary()
    if not binary:
        print("Error: epistemic-graph-server binary not found.", file=sys.stderr)
        print("Build it with: cargo build --release", file=sys.stderr)
        sys.exit(1)

    cmd = [binary, "--socket-path", args.socket_path]
    if args.tcp_addr:
        cmd.extend(["--tcp-addr", args.tcp_addr])
    if args.auth_secret:
        cmd.extend(["--auth-secret", args.auth_secret])
    if args.persist_dir:
        cmd.extend(["--persist-dir", args.persist_dir])

    print("Starting epistemic-graph-server...")
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )

    pid_path = _default_pid_path()
    with open(pid_path, "w") as f:
        f.write(str(proc.pid))

    print(f"Started (PID: {proc.pid}, socket: {args.socket_path})")


def cmd_stop(args: argparse.Namespace) -> None:
    """Stop the epistemic-graph service."""
    pid_path = _default_pid_path()
    if not os.path.exists(pid_path):
        print("No PID file found. Service may not be running.")
        return

    with open(pid_path) as f:
        pid = int(f.read().strip())

    try:
        os.kill(pid, signal.SIGTERM)
        print(f"Sent SIGTERM to PID {pid}")
        os.unlink(pid_path)
    except ProcessLookupError:
        print(f"Process {pid} not found. Cleaning up PID file.")
        os.unlink(pid_path)
    except PermissionError:
        print(f"Permission denied sending signal to PID {pid}", file=sys.stderr)
        sys.exit(1)


def cmd_status(args: argparse.Namespace) -> None:
    """Check if the service is running."""
    pid_path = _default_pid_path()
    if not os.path.exists(pid_path):
        print("Status: NOT RUNNING (no PID file)")
        return

    with open(pid_path) as f:
        pid = int(f.read().strip())

    try:
        os.kill(pid, 0)
        print(f"Status: RUNNING (PID: {pid})")
    except ProcessLookupError:
        print(f"Status: DEAD (stale PID file for {pid})")
    except PermissionError:
        print(f"Status: RUNNING (PID: {pid}, no signal permission)")


def cmd_ping(args: argparse.Namespace) -> None:
    """Send a ping to the service."""
    from .client import EpistemicGraphClient

    async def _ping() -> None:
        client = await EpistemicGraphClient.connect(
            socket_path=args.socket_path,
            auth_secret=args.auth_secret,
        )
        result = await client.ping()
        print(f"Pong: {result}")
        await client.close()

    asyncio.run(_ping())


def cmd_graphs_list(args: argparse.Namespace) -> None:
    """List all registered graphs."""
    from .client import EpistemicGraphClient

    async def _list() -> None:
        client = await EpistemicGraphClient.connect(
            socket_path=args.socket_path,
            auth_secret=args.auth_secret,
        )
        graphs = await client.tenants.list()
        if not graphs:
            print("No graphs registered.")
        else:
            for g in graphs:
                print(f"  {g.get('name', '?')} ({g.get('type', '?')})")
        await client.close()

    asyncio.run(_list())


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        prog="epistemic-graph-service",
        description="Manage the epistemic-graph Tokio service",
    )
    parser.add_argument(
        "--socket-path",
        default=_default_socket_path(),
        help="Unix Domain Socket path",
    )
    parser.add_argument(
        "--auth-secret",
        default=os.environ.get("GRAPH_SERVICE_AUTH_SECRET", ""),
        help="HMAC-SHA256 shared secret",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    # start
    sp_start = subparsers.add_parser("start", help="Start the service")
    sp_start.add_argument("--tcp-addr", help="Optional TCP address (host:port)")
    sp_start.add_argument("--persist-dir", help="Checkpoint persistence directory")
    sp_start.set_defaults(func=cmd_start)

    # stop
    sp_stop = subparsers.add_parser("stop", help="Stop the service")
    sp_stop.set_defaults(func=cmd_stop)

    # status
    sp_status = subparsers.add_parser("status", help="Check service status")
    sp_status.set_defaults(func=cmd_status)

    # ping
    sp_ping = subparsers.add_parser("ping", help="Ping the service")
    sp_ping.set_defaults(func=cmd_ping)

    # graphs list
    sp_graphs = subparsers.add_parser("graphs", help="Graph management")
    graphs_sub = sp_graphs.add_subparsers(dest="graphs_command", required=True)
    sp_gl = graphs_sub.add_parser("list", help="List registered graphs")
    sp_gl.set_defaults(func=cmd_graphs_list)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
