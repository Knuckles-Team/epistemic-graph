"""The sprawl bridge exempts only byte-verified generated binary size findings."""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from scripts.check_sprawl import remaining
from scripts.generated_artifacts import verify

pytestmark = pytest.mark.no_engine

BINARY = b"codec" * 300_000
PATH = "clients/go/eg_method_codec.wasm"
SIZE_FINDING = f"tracked binary > 1000000 bytes: {PATH} ({len(BINARY)} bytes)"


def _ledger(root: Path, digest: str) -> None:
    artifact = root / PATH
    artifact.parent.mkdir(parents=True)
    artifact.write_bytes(BINARY)
    config = root / ".config"
    config.mkdir()
    (config / "generated-artifacts.toml").write_text(
        "[[artifact]]\n"
        f'path = "{PATH}"\n'
        f'sha256 = "{digest}"\n'
        'reproducer = "python3 scripts/build_method_codec_wasm.py --check"\n'
        'proven_by = "language-clients"\n'
        'reason = "Go client embeds codec"\n'
    )


def test_matching_pin_exempts_only_its_binary_size_finding(tmp_path: Path) -> None:
    _ledger(tmp_path, hashlib.sha256(BINARY).hexdigest())
    verified, problems = verify(tmp_path)
    assert problems == []
    unrelated = "tracked binary > 1000000 bytes: clients/js/other.wasm (1500000 bytes)"
    marker = "botched-merge marker in: clients/go/codec.go"
    assert remaining([SIZE_FINDING, unrelated, marker], verified) == [unrelated, marker]


def test_stale_pin_keeps_size_finding_and_fails_ledger(tmp_path: Path) -> None:
    _ledger(tmp_path, "0" * 64)
    verified, problems = verify(tmp_path)
    assert verified == set()
    assert len(problems) == 1
    assert remaining([SIZE_FINDING], verified) == [SIZE_FINDING]
