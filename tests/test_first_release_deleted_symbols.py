"""First-release source must not regain superseded no-caller symbols."""

from __future__ import annotations

import re
from pathlib import Path

import pytest

# Pure/static source proof -- it greps tracked files and never needs the shared
# native engine, so exempt it from conftest.py's session-scoped
# `start_epistemic_graph_server` fixture (without this marker the module builds
# and starts a server just to read source text).
pytestmark = pytest.mark.no_engine

ROOT = Path(__file__).resolve().parents[1]


def _existing_source(*relative_paths: str) -> str:
    return "\n".join(
        path.read_text(encoding="utf-8")
        for relative_path in relative_paths
        if (path := ROOT / relative_path).is_file()
    )


def test_deleted_first_release_symbols_stay_absent() -> None:
    index_source = _existing_source(
        "crates/eg-core/src/index.rs",
        "crates/eg-core/src/index/model/kinds.rs",
    )
    writer_source = _existing_source(
        "src/server/persistence/redb_backend.rs",
        "src/server/persistence/redb_backend/commands.rs",
        "src/server/persistence/redb_backend/writer/commands.rs",
    )
    network_source = _existing_source(
        "src/raft/network.rs",
        "src/raft/network_rpc.rs",
    )
    autosize_source = (ROOT / "src/autosize.rs").read_text(encoding="utf-8")
    crypto_source = (ROOT / "src/crypto.rs").read_text(encoding="utf-8")
    value_cipher_callers = [
        str(path.relative_to(ROOT))
        for source_root in (ROOT / "src", ROOT / "crates", ROOT / "tests")
        for path in source_root.rglob("*.rs")
        if "ValueCipher::from_env(" in path.read_text(encoding="utf-8")
    ]

    assert not re.search(
        r"pub\s+fn\s+covers\s*\(\s*&self\s*,\s*source_snapshot_version",
        index_source,
    )
    assert not re.search(r"\b(?:Cmd|WriterCommand)::ReadNode\b", writer_source)
    assert not re.search(r"\bReadNode\s*\{", writer_source)
    assert not re.search(
        r"impl\s+GroupNetworkFactory\s*\{.*?pub\s+fn\s+new\s*\(",
        network_source,
        re.DOTALL,
    )
    assert not re.search(
        r"coalescer\s*:\s*Option\s*<\s*Arc\s*<\s*HeartbeatCoalescer\s*>\s*>",
        network_source,
    )
    assert "if let Some(coalescer) = &self.coalescer" not in network_source
    assert not re.search(r"pub\s+fn\s+total_ram_bytes\s*\(", autosize_source)
    assert not re.search(r"pub\s+fn\s+from_env\s*\(\s*\)", crypto_source)
    assert not value_cipher_callers, value_cipher_callers
