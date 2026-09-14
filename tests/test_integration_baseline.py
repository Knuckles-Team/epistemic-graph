"""Behavioral contract tests for the integration-failure baseline gate."""

from __future__ import annotations

import subprocess

import pytest

from scripts import check_integration_baseline as gate

pytestmark = pytest.mark.no_engine


def _pytest_result(
    monkeypatch: pytest.MonkeyPatch,
    *,
    returncode: int,
    stdout: str = "",
) -> None:
    """Provide a deterministic child-pytest verdict without starting pytest."""

    def run(*args: object, **kwargs: object) -> subprocess.CompletedProcess[str]:
        del args, kwargs
        return subprocess.CompletedProcess(
            args=["pytest"], returncode=returncode, stdout=stdout, stderr=""
        )

    monkeypatch.setattr(gate.subprocess, "run", run)


def test_valid_baseline_is_loaded_and_successful_run_passes(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    baseline = tmp_path / "integration_failure_baseline.txt"
    baseline.write_text(
        "tests/test_example.py::test_known_failure  # owner=@proof "
        "review-by=2099-01-01\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(gate, "BASELINE_PATH", baseline)
    _pytest_result(
        monkeypatch,
        returncode=1,
        stdout=(
            "================ short test summary info ================\n"
            "FAILED tests/test_example.py::test_known_failure - expected debt\n"
            "=================== 1 failed ===================\n"
        ),
    )

    assert gate.main([]) == 0


def test_missing_baseline_refuses_to_run_pytest(
    tmp_path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    baseline = tmp_path / "does-not-exist.txt"
    monkeypatch.setattr(gate, "BASELINE_PATH", baseline)

    def unexpected_run(*args: object, **kwargs: object) -> None:
        del args, kwargs
        raise AssertionError("pytest must not run without its required baseline")

    monkeypatch.setattr(gate.subprocess, "run", unexpected_run)

    assert gate.command([]) == 1
    assert (
        "REFUSED: cannot read required integration baseline" in capsys.readouterr().err
    )


def test_unreadable_baseline_refuses_to_run_pytest(
    tmp_path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    baseline = tmp_path / "baseline-directory"
    baseline.mkdir()
    monkeypatch.setattr(gate, "BASELINE_PATH", baseline)

    def unexpected_run(*args: object, **kwargs: object) -> None:
        del args, kwargs
        raise AssertionError("pytest must not run with an unreadable baseline")

    monkeypatch.setattr(gate.subprocess, "run", unexpected_run)

    assert gate.command([]) == 1
    assert (
        "REFUSED: cannot read required integration baseline" in capsys.readouterr().err
    )


def test_malformed_baseline_refuses_to_run_pytest(
    tmp_path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    baseline = tmp_path / "integration_failure_baseline.txt"
    baseline.write_text("not a baseline entry\n", encoding="utf-8")
    monkeypatch.setattr(gate, "BASELINE_PATH", baseline)

    def unexpected_run(*args: object, **kwargs: object) -> None:
        del args, kwargs
        raise AssertionError("pytest must not run with malformed baseline data")

    monkeypatch.setattr(gate.subprocess, "run", unexpected_run)

    assert gate.command([]) == 1
    assert "REFUSED: malformed required integration baseline" in capsys.readouterr().err


def test_invalid_review_date_refuses_to_run_pytest(
    tmp_path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    baseline = tmp_path / "integration_failure_baseline.txt"
    baseline.write_text(
        "tests/test_example.py::test_known_failure  "
        "# owner=@proof review-by=2026-02-30\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(gate, "BASELINE_PATH", baseline)

    def unexpected_run(*args: object, **kwargs: object) -> None:
        del args, kwargs
        raise AssertionError("pytest must not run with an invalid baseline date")

    monkeypatch.setattr(gate.subprocess, "run", unexpected_run)

    assert gate.command([]) == 1
    assert "REFUSED: malformed required integration baseline" in capsys.readouterr().err
