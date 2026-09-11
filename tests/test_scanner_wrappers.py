from __future__ import annotations

import importlib.util
import io
import json
import sys
import tarfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.no_engine


def _load_script(name: str):
    path = ROOT / "scripts" / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def contract():
    scanner_contract = _load_script("scanner_contract")
    return scanner_contract.load_contract(ROOT / "pyproject.toml")


def test_scanner_environment_drops_git_selectors_but_can_keep_staged_index(
    monkeypatch,
):
    scanner_contract = _load_script("scanner_contract")
    monkeypatch.setenv("GIT_DIR", "/tmp/wrong-repository")
    monkeypatch.setenv("GIT_WORK_TREE", "/tmp/wrong-worktree")
    monkeypatch.setenv("GIT_INDEX_FILE", "/tmp/staged-index")

    clean = scanner_contract.sanitized_env()
    staged = scanner_contract.sanitized_env(preserve_index=True)

    assert "GIT_DIR" not in clean
    assert "GIT_WORK_TREE" not in clean
    assert "GIT_INDEX_FILE" not in clean
    assert staged["GIT_INDEX_FILE"] == "/tmp/staged-index"


def test_exclusions_match_root_and_nested_junk_without_erasing_dot_names(contract):
    assert contract.is_excluded(".git/config")
    assert contract.is_excluded("target/debug/app.rs")
    assert contract.is_excluded("nested/target-debug/app.rs")
    assert contract.is_excluded("Cargo.lock")
    assert not contract.is_excluded("src/graph.rs")


def test_directory_exclusions_are_root_anchored_unless_recursive(contract):
    assert contract.is_excluded("contract/methods.json")
    assert contract.is_excluded("epistemic_graph/contract/receipt.json")
    assert not contract.is_excluded("crates/eg-types/src/contract/crypto.rs")
    assert not contract.is_excluded("nested/contract/generated.rs")


def test_scanner_source_manifest_uses_only_supported_census_inputs(contract):
    assert contract.cargo_deny_version == "0.20.2"
    manifest = _load_script("list_scanner_sources")
    paths = [
        "src/main.rs",
        "crates/eg-core/src/lib.rs",
        "src/client.cpp",
        "src/client.cs",
        "src/query.scala",
        "target/generated.rs",
        "docs/README.md",
    ]

    assert manifest.select(paths, "cccc", contract) == [
        "crates/eg-core/src/lib.rs",
        "src/main.rs",
    ]
    assert manifest.select(paths, "kiss", contract) == [
        "crates/eg-core/src/lib.rs",
        "src/main.rs",
    ]


def test_cccc_registry_excludes_unsupported_frontends():
    scanner_contract = _load_script("scanner_contract")
    assert not {"cpp", "cs", "scala"} & set(scanner_contract.CCCC_LANGUAGES)
    assert not {".cc", ".cpp", ".cxx", ".cs", ".scala"} & set(
        scanner_contract.CCCC_SUPPORTED_SUFFIXES
    )


def test_cccc_census_validator_requires_top_level_parse_count(tmp_path):
    validator = _load_script("validate_cccc_census")
    report = tmp_path / "cccc.json"
    valid = {"files": [{}], "summary": {"parse_error_count": 0}}
    report.write_text(json.dumps(valid), encoding="utf-8")
    assert validator.validate_report(report) == 1

    for summary in ({}, {"parse_error_count": 1}, {"parse_error_count": "0"}):
        report.write_text(
            json.dumps({"files": [{}], "summary": summary}), encoding="utf-8"
        )
        with pytest.raises(SystemExit) as raised:
            validator.validate_report(report)
        assert raised.value.code == 2


def test_cccc_census_validator_rejects_file_parse_errors(tmp_path):
    validator = _load_script("validate_cccc_census")
    report = tmp_path / "cccc.json"
    report.write_text(
        json.dumps(
            {
                "files": [{"parse_errors": ["unexpected token"]}],
                "summary": {"parse_error_count": 0},
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(SystemExit) as raised:
        validator.validate_report(report)
    assert raised.value.code == 2


def test_relative_to_root_rejects_escape_and_accepts_absolute_inside(tmp_path):
    scanner_contract = _load_script("scanner_contract")
    root = tmp_path / "repo"
    root.mkdir()
    inside = root / "src" / "main.rs"
    inside.parent.mkdir()
    inside.write_text("fn main() {}\n", encoding="utf-8")

    assert scanner_contract.relative_to_root(inside, root) == "src/main.rs"
    with pytest.raises(ValueError, match="escapes"):
        scanner_contract.relative_to_root("../outside.rs", root)
    with pytest.raises(ValueError):
        scanner_contract.relative_to_root(r"C:\outside.rs", root)


def test_contract_rejects_unknown_scanner_keys(tmp_path):
    scanner_contract = _load_script("scanner_contract")
    source = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    changed = source.replace(
        "[tool.epistemic_graph.scanners]\n",
        "[tool.epistemic_graph.scanners]\nunknown_scanner_key = true\n",
        1,
    )
    path = tmp_path / "pyproject.toml"
    path.write_text(changed, encoding="utf-8")

    with pytest.raises(scanner_contract.ScannerContractError, match="unknown"):
        scanner_contract.load_contract(path)


def test_complexity_parser_keeps_nested_children_and_duplicate_names():
    complexity = _load_script("check_complexity_staged")
    measured = {}
    complexity._walk(
        {
            "name": "outer",
            "cyclomatic": 2,
            "cognitive": 1,
            "line": 4,
            "children": [
                {
                    "name": "inner",
                    "cyclomatic": 11,
                    "cognitive": 2,
                    "line": 6,
                    "children": [],
                }
            ],
        },
        "",
        measured,
    )
    complexity._walk(
        {
            "name": "outer",
            "cyclomatic": 3,
            "cognitive": 4,
            "line": 40,
            "children": [],
        },
        "",
        measured,
    )

    assert measured["outer"] == [(2, 1, 4, False), (3, 4, 40, False)]
    assert measured["outer.inner"] == [(11, 2, 6, False)]


def test_complexity_parser_requires_a_start_line():
    """A row with no line cannot be classified, so it must not be measurable.

    Silently treating it as non-exempt would also make it silently
    unreportable; the gate distinguishes "cannot run" from "found nothing".
    """
    complexity = _load_script("check_complexity_staged")
    with pytest.raises(SystemExit) as raised:
        complexity._walk(
            {"name": "outer", "cyclomatic": 2, "cognitive": 1, "children": []},
            "",
            {},
        )
    assert raised.value.code == 2


def test_complexity_judge_catches_over_cap_duplicate_name():
    complexity = _load_script("check_complexity_staged")
    metrics = complexity.Metrics
    before = {"submit": [metrics(30, 20, 1, False)]}
    after = {"submit": [metrics(30, 20, 1, False), metrics(12, 16, 60, False)]}

    assert complexity.judge(before, after, 10, 15) == [
        ("NEW", "submit", (0, 0), (12, 16))
    ]


def test_complexity_accepts_exhaustive_dispatch_growing_by_a_variant():
    """Adding an enum variant to an accepted dispatcher is not a regression.

    Keeping the match exhaustive is the entire reason the exemption exists; a
    gate that then failed the variant addition would push the author toward the
    lookup table the rule is designed to prevent.
    """
    complexity = _load_script("check_complexity_staged")
    metrics = complexity.Metrics
    before = {"dispatch_kind": [metrics(32, 1, 2076, True)]}
    after = {"dispatch_kind": [metrics(33, 1, 2076, True)]}

    assert complexity.judge(before, after, 10, 15) == []


def test_complexity_fails_when_a_dispatcher_leaves_the_accepted_class():
    """Growing a catch-all arm restores the full cyclomatic value at once."""
    complexity = _load_script("check_complexity_staged")
    metrics = complexity.Metrics
    before = {"dispatch_kind": [metrics(32, 1, 2076, True)]}
    after = {"dispatch_kind": [metrics(32, 1, 2076, False)]}

    assert complexity.judge(before, after, 10, 15) == [
        ("WORSE", "dispatch_kind", (0, 1), (32, 1))
    ]


def test_complexity_never_exempts_cognitive_complexity():
    complexity = _load_script("check_complexity_staged")
    metrics = complexity.Metrics
    after = {"handle": [metrics(32, 16, 10, True)]}

    assert complexity.judge({}, after, 10, 15) == [("NEW", "handle", (0, 0), (32, 16))]


def test_complexity_parser_rejects_missing_report_summary(monkeypatch):
    complexity = _load_script("check_complexity_staged")
    monkeypatch.setattr(complexity, "_resolve_cccc", lambda: "/opt/cccc")

    class Result:
        returncode = 0
        stdout = json.dumps({"files": [{"functions": []}]})
        stderr = ""

    monkeypatch.setattr(complexity.subprocess, "run", lambda *args, **kwargs: Result())
    with pytest.raises(SystemExit) as raised:
        complexity.measure("example.py")
    assert raised.value.code == 2


def test_dupehound_schema_rejects_inconsistent_status_payload():
    dupehound = _load_script("check_dupehound")
    finding = {
        "file": "src/new.py",
        "line": 4,
        "name": "new",
        "similarity": 0.91,
        "original_file": "src/old.py",
        "original_line": 4,
        "original_name": "old",
    }
    payload = json.dumps({"schema_version": 1, "findings": [finding]})
    assert dupehound.finding_document(payload)[0]["name"] == "new"
    with pytest.raises(SystemExit) as raised:
        dupehound.finding_document(
            json.dumps(
                {"schema_version": 1, "findings": [{**finding, "file": "../x.py"}]}
            )
        )
    assert raised.value.code == 2


def _valid_report(root: Path) -> dict:
    return {
        "duplicates": [
            {
                "format": "python",
                "fragment": "return value",
                "lines": 2,
                "tokens": 4,
                "firstFile": {
                    "name": str(root / "src" / "first.py"),
                    "startLoc": {"line": 2},
                    "endLoc": {"line": 3},
                },
                "secondFile": {
                    "name": str(root / "src" / "second.py"),
                    "startLoc": {"line": 4},
                    "endLoc": {"line": 5},
                },
            }
        ],
        "statistics": {
            "total": {
                "clones": 1,
                "sources": 2,
                "duplicatedLines": 4,
                "lines": 8,
                "percentage": 50.0,
            }
        },
    }


def test_jscpd_report_rejects_inconsistent_count_and_root_escape(tmp_path):
    jscpd = _load_script("check_duplication")
    root = tmp_path / "repo"
    root.mkdir()

    report = tmp_path / "report.json"
    document = _valid_report(root)
    report.write_text(json.dumps(document), encoding="utf-8")
    assert jscpd.load_report(report, root)["statistics"]["total"]["clones"] == 1

    document["statistics"]["total"]["clones"] = 0
    report.write_text(json.dumps(document), encoding="utf-8")
    with pytest.raises(SystemExit) as raised:
        jscpd.load_report(report, root)
    assert raised.value.code == 2

    document = _valid_report(root)
    document["duplicates"][0]["secondFile"]["name"] = str(tmp_path / "outside.py")
    report.write_text(json.dumps(document), encoding="utf-8")
    with pytest.raises(SystemExit) as raised:
        jscpd.load_report(report, root)
    assert raised.value.code == 2


def test_archive_extraction_rejects_path_traversal(tmp_path):
    jscpd = _load_script("check_duplication")
    destination = tmp_path / "snapshot"
    destination.mkdir()
    stream = io.BytesIO()
    with tarfile.open(fileobj=stream, mode="w") as archive:
        member = tarfile.TarInfo("../outside.py")
        member.size = 0
        archive.addfile(member)
    stream.seek(0)
    with tarfile.open(fileobj=stream, mode="r:") as archive:
        with pytest.raises(SystemExit) as raised:
            jscpd.safe_extract_archive(archive, destination)
    assert raised.value.code == 2
