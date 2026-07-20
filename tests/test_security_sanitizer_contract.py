"""Regression tests for privacy-safe synthetic credential classification."""

import importlib.util
from pathlib import Path

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
