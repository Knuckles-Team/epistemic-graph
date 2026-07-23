"""CONCEPT:EG-KG.compute.ast-parser-fallback — Epistemic Graph AST Parser with Python Fallback."""

import ast
import asyncio
import logging
import os
from typing import Any

from .client import (
    EpistemicGraphClient,
    RequestContextClaims,
    _logical_source_name,
    validate_request_context,
)

logger = logging.getLogger(__name__)


class RustASTParser:
    """CONCEPT:EG-KG.compute.ast-parser-fallback — Epistemic Graph AST Parser with Python Fallback.

    Connects to the epistemic-graph Tokio service via Unix Domain Sockets using
    length-prefixed MessagePack. Performs AST parsing of target source files.
    If the service transport is unavailable, falls back to Python's native `ast`
    parser. Authentication and authorization failures are returned to the caller.
    """

    def __init__(
        self,
        socket_path: str | None = None,
        auth_secret: str | None = None,
        *,
        verified_context: RequestContextClaims | dict[str, Any],
    ) -> None:
        self.socket_path = socket_path or os.environ.get(
            "GRAPH_SERVICE_SOCKET",
            os.path.join(
                os.environ.get("XDG_RUNTIME_DIR", "/tmp"),  # nosec B108 — UDS path, not a temp file
                "epistemic-graph.sock",
            ),
        )
        if not socket_path and not os.path.exists(self.socket_path):
            tmp_socket = "/tmp/epistemic-graph.sock"  # nosec B108 — default UDS socket path
            if os.path.exists(tmp_socket):
                self.socket_path = tmp_socket

        self.auth_secret = auth_secret or os.environ.get(
            "GRAPH_SERVICE_AUTH_SECRET", ""
        )
        if not self.auth_secret:
            raise ValueError("a non-empty authentication secret is required")
        self.verified_context = validate_request_context(verified_context)

    async def parse_file(self, file_path: str, source: bytes) -> dict[str, Any]:
        """Parse source code of a file using the Rust AST service.

        ``file_path`` is a portable logical source name, not a host path. If the
        service transport is unavailable, fall back to Python's native ``ast``
        parser (Python sources only).
        """
        source_name = _logical_source_name(file_path)
        try:
            return await self._parse_file_via_service(source_name, source)
        except (
            FileNotFoundError,
            ConnectionRefusedError,
            ConnectionResetError,
            OSError,
            asyncio.IncompleteReadError,
        ) as exc:
            logger.warning(
                "AST service unavailable (%s); falling back to native Python ast for %s",
                exc,
                source_name,
            )
            return self._parse_file_local(source_name, source)

    def _parse_file_local(self, file_path: str, source: bytes) -> dict[str, Any]:
        """Native Python ``ast`` fallback — extracts class/function symbols.

        Mirrors the service payload shape: a FILE node plus one SYMBOL node per
        class/function, joined by CONTAINS edges.
        """
        text = source.decode("utf-8", errors="replace")
        try:
            tree = ast.parse(text, filename=file_path)
        except SyntaxError:
            return {"nodes": [], "edges": [], "symbols_extracted": 0}

        file_id = f"FILE::{file_path}"
        nodes: list[dict[str, Any]] = [
            {"id": file_id, "node_type": "FILE", "properties": {"path": file_path}}
        ]
        edges: list[dict[str, Any]] = []

        for node in ast.walk(tree):
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                kind = "class" if isinstance(node, ast.ClassDef) else "function"
                sym_id = f"SYMBOL::{file_path}::{node.name}::{node.lineno}"
                nodes.append(
                    {
                        "id": sym_id,
                        "node_type": "SYMBOL",
                        "properties": {
                            "name": node.name,
                            "kind": kind,
                            "line": node.lineno,
                        },
                    }
                )
                edges.append(
                    {"source": file_id, "target": sym_id, "edge_type": "CONTAINS"}
                )

        symbols_extracted = sum(1 for n in nodes if n["node_type"] == "SYMBOL")
        return {
            "nodes": nodes,
            "edges": edges,
            "symbols_extracted": symbols_extracted,
        }

    async def _parse_file_via_service(
        self, file_path: str, source: bytes
    ) -> dict[str, Any]:
        client = await EpistemicGraphClient.connect(
            socket_path=self.socket_path,
            auth_secret=self.auth_secret,
            graph_name="__commons__",
            verified_context=self.verified_context,
        )
        try:
            result = await client._send(
                "ParseFile", {"file_path": file_path, "source": source}
            )
            return result or {}
        finally:
            await client.close()
