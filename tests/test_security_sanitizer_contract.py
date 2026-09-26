"""Regression tests for privacy-safe synthetic credential classification."""

import gzip
import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest

# Pure/static test -- never needs the shared native engine (see
# conftest.py's session-scoped `start_epistemic_graph_server` fixture,
# which this marker exempts this module from triggering).
pytestmark = pytest.mark.no_engine

ROOT = Path(__file__).resolve().parents[1]


def _sanitizer():
    path = ROOT / "scripts" / "security_sanitizer.py"
    spec = importlib.util.spec_from_file_location("eg_security_sanitizer", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_synthetic_invalid_authority_is_a_placeholder() -> None:
    sanitizer = _sanitizer()
    assert sanitizer.is_placeholder('auth_secret="synthetic-invalid-authority"')


def test_unmarked_secret_assignment_is_not_a_placeholder() -> None:
    sanitizer = _sanitizer()
    value = "production" + "-secret-value"
    assert not sanitizer.is_placeholder(f'auth_secret="{value}"')


def test_transient_baseline_notes_are_rejected(tmp_path, monkeypatch) -> None:
    sanitizer = _sanitizer()
    note = tmp_path / "GOC-40-BASELINE-NOTES.md"
    note.write_text("temporary branch marker\n", encoding="utf-8")
    monkeypatch.setattr(
        sanitizer.subprocess,
        "run",
        lambda *args, **kwargs: SimpleNamespace(stdout=f"{note.name}\n"),
    )

    violations = sanitizer.scan_repository(tmp_path)

    assert any("Transient agent note detected" in item for item in violations)


def test_compressed_text_is_inspected_for_secrets(tmp_path) -> None:
    sanitizer = _sanitizer()
    report = tmp_path / "report.json.gz"
    payload = b'token="production-secret-value"\n'  # sanitizer:ignore - synthetic
    report.write_bytes(gzip.compress(payload))

    assert any(
        "Potential unmasked secret (Generic Token Assignment)" in finding
        for finding in sanitizer.secret_violations(report, Path(report.name))
    )


def test_compressed_text_has_a_bounded_expanded_size(tmp_path) -> None:
    sanitizer = _sanitizer()
    report = tmp_path / "report.json.gz"
    report.write_bytes(gzip.compress(b"a" * (sanitizer.MAX_SCAN_BYTES + 1)))

    assert sanitizer.secret_violations(report, Path(report.name)) == [
        f"Source file exceeds security scan boundary: '{report.name}'"
    ]


@pytest.mark.parametrize("contents", [b"not gzip", gzip.compress(b"valid")[:-4]])
def test_invalid_compressed_text_fails_closed(tmp_path, contents: bytes) -> None:
    sanitizer = _sanitizer()
    report = tmp_path / "report.json.gz"
    report.write_bytes(contents)

    assert sanitizer.secret_violations(report, Path(report.name)) == [
        f"Source file could not be inspected: '{report.name}'"
    ]


def test_compressed_text_requires_utf8(tmp_path) -> None:
    sanitizer = _sanitizer()
    report = tmp_path / "report.json.gz"
    report.write_bytes(gzip.compress(b"\xff"))

    assert sanitizer.secret_violations(report, Path(report.name)) == [
        f"Source file could not be inspected: '{report.name}'"
    ]
