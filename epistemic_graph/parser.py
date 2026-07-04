"""CONCEPT:EG-KG.compute.ast-parser-fallback — Epistemic Graph AST Parser with Python Fallback."""

import ast
import asyncio
import hashlib
import hmac
import logging
import os
from typing import Any

import msgpack

logger = logging.getLogger(__name__)


class RustASTParser:
    """CONCEPT:EG-KG.compute.ast-parser-fallback — Epistemic Graph AST Parser with Python Fallback.

    Connects to the epistemic-graph Tokio service via Unix Domain Sockets using
    length-prefixed MessagePack. Performs AST parsing of target source files.
    If the service is unavailable or errors out, falls back to Python's native `ast` parser.
    """

    def __init__(
        self,
        socket_path: str | None = None,
        auth_secret: str | None = None,
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
        self._request_id = 0

    def _next_id(self) -> int:
        self._request_id += 1
        return self._request_id

    def _compute_token(self, request_id: int) -> str:
        if not self.auth_secret:
            return ""
        return hmac.new(
            self.auth_secret.encode(),
            str(request_id).encode(),
            hashlib.sha256,
        ).hexdigest()

    async def parse_file(self, file_path: str, source: bytes) -> dict[str, Any]:
        """Parse source code of a file using the Rust AST service.

        If the service socket is unavailable or the request fails, fall back to
        Python's native ``ast`` parser (Python sources only).
        """
        try:
            return await self._parse_file_via_service(file_path, source)
        except (
            FileNotFoundError,
            ConnectionRefusedError,
            ConnectionResetError,
            OSError,
            RuntimeError,
            asyncio.IncompleteReadError,
        ) as exc:
            logger.warning(
                "AST service unavailable (%s); falling back to native Python ast for %s",
                exc,
                file_path,
            )
            return self._parse_file_local(file_path, source)

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
        reader, writer = await asyncio.open_unix_connection(self.socket_path)
        try:
            req_id = self._next_id()
            # The ParseFile method expects source as a Vec<u8>. In msgpack serialization,
            # bytes are serialized as binary bytes.
            request = {
                "id": req_id,
                "graph": "__commons__",
                "auth_token": self._compute_token(req_id),
                "method": "ParseFile",
                "params": {
                    "file_path": file_path,
                    "source": source,
                },
            }
            payload = msgpack.packb(request, use_bin_type=True)
            length_prefix = len(payload).to_bytes(4, byteorder="big")
            writer.write(length_prefix)
            writer.write(payload)
            await writer.drain()

            len_buf = await reader.readexactly(4)
            msg_len = int.from_bytes(len_buf, byteorder="big")
            resp_bytes = await reader.readexactly(msg_len)
            resp = msgpack.unpackb(resp_bytes, raw=False)
            if resp.get("error") is not None:
                raise RuntimeError(resp.get("error"))
            return resp.get("result", {})
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:  # nosec B110 — best-effort close; errors on teardown are non-fatal
                pass
