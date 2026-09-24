"""The reviewed generated-artifact ledger (`.config/generated-artifacts.toml`).

A gate skips its binary checks for a listed artifact ONLY while the file's bytes
match the pinned sha256; a changed or missing listed file fails, and an
unlisted binary is still a violation. Every entry's reproducer must run in the
CI job it names, so no entry can outlive the proof that its bytes reproduce.
"""

from __future__ import annotations

import hashlib
import importlib.util
import sys
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.no_engine

BLOB = bytes(range(256)) * 8192  # 2 MiB, not UTF-8: fails text inspection


def _load(name: str):
    spec = importlib.util.spec_from_file_location(name, ROOT / "scripts" / f"{name}.py")
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _repo(tmp_path: Path, pinned: str | None, *, present: bool = True) -> Path:
    if present:
        (tmp_path / "clients").mkdir()
        (tmp_path / "clients" / "codec.wasm").write_bytes(BLOB)
    if pinned is not None:
        (tmp_path / ".config").mkdir()
        (tmp_path / ".config" / "generated-artifacts.toml").write_text(
            "[[artifact]]\n"
            'path = "clients/codec.wasm"\n'
            f'sha256 = "{pinned}"\n'
            'reproducer = "python3 scripts/build.py --check"\n'
            'proven_by = "language-clients"\n'
            'reason = "embedded by the client module"\n',
            encoding="utf-8",
        )
    return tmp_path


def _sanitizer_findings(repo: Path) -> list[str]:
    return _load("security_sanitizer").scan_repository(repo)


def test_listed_binary_with_the_pinned_sha_passes(tmp_path):
    repo = _repo(tmp_path, hashlib.sha256(BLOB).hexdigest())
    assert _sanitizer_findings(repo) == []


def test_listed_binary_with_a_mismatched_sha_fails(tmp_path):
    repo = _repo(tmp_path, "0" * 64)
    findings = _sanitizer_findings(repo)
    assert any("does not match its pinned sha256" in f for f in findings)
    # The changed bytes are NOT exempt: the binary is inspected (and rejected) too.
    assert any("could not be inspected: 'clients/codec.wasm'" in f for f in findings)


def test_an_unlisted_binary_still_fails(tmp_path):
    repo = _repo(tmp_path, None)
    assert _sanitizer_findings(repo) == [
        "Source file could not be inspected: 'clients/codec.wasm'"
    ]


def test_a_listed_artifact_that_is_missing_fails(tmp_path):
    repo = _repo(tmp_path, hashlib.sha256(BLOB).hexdigest(), present=False)
    assert any("is missing: clients/codec.wasm" in f for f in _sanitizer_findings(repo))


@pytest.mark.parametrize(
    "body",
    [
        '[[artifact]]\npath = "x"\n',  # fields missing
        '[[artifact]]\npath = "/abs"\nsha256 = "' + "a" * 64 + '"\n'
        'reproducer = "r"\nproven_by = "j"\nreason = "why"\n',  # absolute path
        '[[artifact]]\npath = "x"\nsha256 = "short"\nreproducer = "r"\n'
        'proven_by = "j"\nreason = "why"\n',  # not a sha256
        'allow = ["everything"]\n',  # anything but [[artifact]] tables
    ],
)
def test_a_malformed_ledger_fails_closed(tmp_path, body):
    (tmp_path / ".config").mkdir()
    (tmp_path / ".config" / "generated-artifacts.toml").write_text(body, "utf-8")
    verified, problems = _load("generated_artifacts").verify(tmp_path)
    assert verified == set() and problems


def test_every_ledger_entry_is_reproduced_by_the_ci_job_it_names():
    entries = _load("generated_artifacts").load(ROOT)
    jobs = yaml.safe_load((ROOT / ".github/workflows/release.yml").read_text())["jobs"]
    for rel, entry in entries.items():
        assert entry.proven_by in jobs, (rel, entry.proven_by)
        runs = [str(step.get("run", "")) for step in jobs[entry.proven_by]["steps"]]
        assert any(entry.reproducer in run for run in runs), (
            f"{rel}: `{entry.reproducer}` runs in no step of job {entry.proven_by}"
        )


def test_the_committed_ledger_matches_the_committed_bytes():
    _, problems = _load("generated_artifacts").verify(ROOT)
    assert problems == []
