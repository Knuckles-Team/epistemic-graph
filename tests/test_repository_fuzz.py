"""Behaviour pins for the bounded repository config-parser fuzz smoke test."""

import importlib.util
import json
from pathlib import Path

import pytest

# Pure/static test -- never needs the shared native engine.
pytestmark = pytest.mark.no_engine

ROOT = Path(__file__).resolve().parents[1]


def _fuzz():
    path = ROOT / ".security" / "repository_fuzz.py"
    spec = importlib.util.spec_from_file_location("eg_repository_fuzz", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _run(module, monkeypatch, tmp_path: Path, corpus_dir: Path) -> tuple[int, dict]:
    output = tmp_path / "results" / "fuzz.json"
    monkeypatch.chdir(corpus_dir)
    monkeypatch.setattr(module.sys, "argv", ["repository_fuzz.py", str(output)])
    status = module.main()
    return status, json.loads(output.read_text(encoding="utf-8"))


def test_usage_error_without_exactly_one_output_argument(monkeypatch) -> None:
    module = _fuzz()
    monkeypatch.setattr(module.sys, "argv", ["repository_fuzz.py"])
    assert module.main() == 2


@pytest.mark.parametrize(
    "files",
    [
        {},
        {"a.json": '{"x": [1, 2, 3]}', "b.toml": 'key = "value"\n'},
    ],
)
def test_runs_exactly_the_bounded_case_count_and_reports_clean(
    monkeypatch, tmp_path, files
) -> None:
    module = _fuzz()
    corpus_dir = tmp_path / "corpus"
    corpus_dir.mkdir()
    for name, text in files.items():
        (corpus_dir / name).write_text(text, encoding="utf-8")
    status, report = _run(module, monkeypatch, tmp_path, corpus_dir)
    assert status == 0
    assert report == {
        "version": 1,
        "kind": "fuzz",
        "passed": True,
        "cases": 64,
        "failures": 0,
        "crashes": 0,
    }


def test_unexpected_parser_exception_counts_as_a_crash(monkeypatch, tmp_path) -> None:
    module = _fuzz()
    exercised: list[tuple[str, bytes]] = []

    def explode(suffix: str, payload: bytes) -> None:
        exercised.append((suffix, payload))
        if len(exercised) % 3 == 0:
            raise RuntimeError("parser bug")
        if len(exercised) % 3 == 1:
            raise ValueError("tolerated? no -- only decode errors are tolerated")
        raise json.JSONDecodeError("expected", "doc", 0)

    monkeypatch.setattr(module, "_exercise", explode)
    status, report = _run(module, monkeypatch, tmp_path, tmp_path)
    assert status == 1
    assert len(exercised) == 64
    # 64 cases: every third raises RuntimeError and every (3k+1)th ValueError;
    # only the JSONDecodeError cases are tolerated.
    assert report["crashes"] == 64 - 21
    assert report["cases"] == 64
    assert report["passed"] is False
