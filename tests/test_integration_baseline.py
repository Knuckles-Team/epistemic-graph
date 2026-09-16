"""Behavioral contract tests for the integration-failure baseline gate."""

from __future__ import annotations

import subprocess

import pytest

from scripts import check_integration_baseline as gate

pytestmark = pytest.mark.no_engine

_KNOWN = "tests/test_example.py::test_known_failure"
_SUMMARY = "================ short test summary info ================\n"
_KNOWN_FAILED = f"FAILED {_KNOWN} - expected debt\n"
_ONE_KNOWN_FAILURE = (
    _SUMMARY + _KNOWN_FAILED + "=================== 1 failed ===================\n"
)


def _entry(review_by: str) -> str:
    return f"{_KNOWN}  # owner=@proof review-by={review_by}\n"


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


def _missing(tmp_path):
    return tmp_path / "does-not-exist.txt"


def _directory(tmp_path):
    baseline = tmp_path / "baseline-directory"
    baseline.mkdir()
    return baseline


def _written(text: str):
    def write(tmp_path):
        baseline = tmp_path / "integration_failure_baseline.txt"
        baseline.write_text(text, encoding="utf-8")
        return baseline

    return write


@pytest.mark.parametrize(
    ("make_baseline", "refusal"),
    [
        pytest.param(
            _missing,
            "REFUSED: cannot read required integration baseline",
            id="missing",
        ),
        pytest.param(
            _directory,
            "REFUSED: cannot read required integration baseline",
            id="unreadable",
        ),
        pytest.param(
            _written("not a baseline entry\n"),
            "REFUSED: malformed required integration baseline",
            id="malformed",
        ),
        pytest.param(
            _written(_entry("2026-02-30")),
            "REFUSED: malformed required integration baseline",
            id="invalid-review-date",
        ),
    ],
)
def test_untrustworthy_baseline_refuses_to_run_pytest(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    make_baseline,
    refusal: str,
) -> None:
    monkeypatch.setattr(gate, "BASELINE_PATH", make_baseline(tmp_path))

    def unexpected_run(*args: object, **kwargs: object) -> None:
        del args, kwargs
        raise AssertionError("pytest must not run without a trustworthy baseline")

    monkeypatch.setattr(gate.subprocess, "run", unexpected_run)

    assert gate.command([]) == 1
    assert refusal in capsys.readouterr().err


@pytest.mark.parametrize(
    ("baseline", "argv", "returncode", "stdout", "code", "present", "absent"),
    [
        pytest.param(
            _entry("2099-01-01"),
            [],
            1,
            _ONE_KNOWN_FAILURE,
            0,
            [("out", "1 known failure(s), no regressions")],
            [],
            id="baselined-failure-passes",
        ),
        pytest.param(
            _entry("2099-01-01"),
            [],
            1,
            _SUMMARY
            + _KNOWN_FAILED
            + "FAILED tests/test_other.py::test_new_break - surprise\n"
            + "=================== 2 failed ===================\n",
            1,
            [("err", "REGRESSION"), ("err", "tests/test_other.py::test_new_break")],
            # The already-known failure must not also be reported as a regression.
            [("err", _KNOWN)],
            id="new-unbaselined-failure-is-a-regression",
        ),
        pytest.param(
            _entry("2099-01-01"),
            [],
            0,
            "================ 1 passed ================\n",
            1,
            [("err", "FIXED"), ("err", _KNOWN)],
            [],
            id="baselined-test-now-passing-is-repaired",
        ),
        pytest.param(
            _entry("2020-01-01"),
            ["--today", "2026-01-01"],
            1,
            _ONE_KNOWN_FAILURE,
            1,
            [("err", "OVERDUE"), ("err", _KNOWN)],
            [],
            id="entry-past-review-date-is-overdue",
        ),
        pytest.param(
            _entry("2026-01-01"),
            ["--today", "2026-01-01"],
            1,
            _ONE_KNOWN_FAILURE,
            0,
            [("out", "1 known failure(s), no regressions")],
            [("err", "OVERDUE")],
            id="entry-due-today-is-not-yet-overdue",
        ),
        pytest.param(
            _entry("2099-01-01"),
            [],
            1,
            _SUMMARY
            + f"FAILED {_KNOWN}[case-a] - expected debt\n"
            + "=================== 1 failed ===================\n",
            0,
            [("out", "1 known failure(s), no regressions")],
            [("err", "REGRESSION")],
            id="unbracketed-entry-covers-every-parametrisation",
        ),
        pytest.param(
            _entry("2099-01-01"),
            [],
            2,
            "internal error\n",
            1,
            [
                ("err", "REFUSED: pytest exited 2"),
                ("err", "Nothing was compared to the baseline"),
            ],
            [],
            id="untrustworthy-pytest-exit-code-refuses-to-compare",
        ),
        pytest.param(
            "",
            [],
            0,
            "================ 3 passed ================\n",
            0,
            [("out", "nothing failing at all")],
            [],
            id="nothing-failing-at-all",
        ),
        pytest.param(
            "",
            ["--today", "not-a-date"],
            0,
            "================ 3 passed ================\n",
            0,
            [("out", "nothing failing at all")],
            [],
            id="unparsed-today-without-dated-entries-still-passes",
        ),
        pytest.param(
            "",
            ["--today", "not-a-date"],
            1,
            _ONE_KNOWN_FAILURE,
            1,
            [("err", "REGRESSION"), ("err", _KNOWN)],
            [],
            id="unparsed-today-without-dated-entries-still-regresses",
        ),
    ],
)
def test_gate_verdicts(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    baseline: str,
    argv: list[str],
    returncode: int,
    stdout: str,
    code: int,
    present: list[tuple[str, str]],
    absent: list[tuple[str, str]],
) -> None:
    monkeypatch.setattr(gate, "BASELINE_PATH", _written(baseline)(tmp_path))
    _pytest_result(monkeypatch, returncode=returncode, stdout=stdout)

    assert gate.main(argv) == code
    captured = capsys.readouterr()
    for stream, fragment in present:
        assert fragment in getattr(captured, stream)
    for stream, fragment in absent:
        assert fragment not in getattr(captured, stream)
