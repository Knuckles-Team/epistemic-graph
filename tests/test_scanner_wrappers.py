from __future__ import annotations

import io
import json
import tarfile
from pathlib import Path
from typing import Any

import pytest
from _script_loader import load_script

ROOT = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.no_engine


def _metric_summary(value: int = 0) -> dict[str, int]:
    return {"sum": value, "max": value, "median": value, "p90": value, "p95": value}


def _native_summary(
    file_count: int,
    function_count: int,
    *,
    cognitive: dict[str, int] | None = None,
    cyclomatic: dict[str, int] | None = None,
    parse_error_count: int = 0,
    parse_error_file_count: int = 0,
) -> dict[str, object]:
    return {
        "file_count": file_count,
        "function_count": function_count,
        "parse_error_count": parse_error_count,
        "parse_error_file_count": parse_error_file_count,
        "cognitive": cognitive or _metric_summary(),
        "cyclomatic": cyclomatic or _metric_summary(),
    }


def _native_file(
    path: str,
    *,
    functions: list[dict] | None = None,
    cognitive: int = 0,
    cyclomatic: int = 0,
    parse_errors: list[str] | None = None,
) -> dict[str, object]:
    result: dict[str, object] = {
        "path": path,
        "cognitive": cognitive,
        "cyclomatic": cyclomatic,
        "functions": functions if functions is not None else [],
    }
    if parse_errors is not None:
        result["parse_errors"] = parse_errors
    return result


@pytest.fixture(scope="module")
def contract():
    scanner_contract = load_script("scanner_contract")
    return scanner_contract.load_contract(ROOT / "pyproject.toml")


def test_scanner_environment_drops_git_selectors_but_can_keep_staged_index(
    monkeypatch,
):
    scanner_contract = load_script("scanner_contract")
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
    manifest = load_script("list_scanner_sources")
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


def _load_kiss_census():
    load_script("scanner_contract")
    return load_script("check_kiss_census")


def test_kiss_census_worker_count_is_resource_bounded():
    census = _load_kiss_census()

    assert census.worker_count(None, available_cpus=24) == 4
    assert census.worker_count(None, available_cpus=2) == 2
    assert census.worker_count("3", available_cpus=2) == 2
    with pytest.raises(ValueError, match="between 1 and 4"):
        census.worker_count("5", available_cpus=24)
    with pytest.raises(ValueError, match="must be an integer"):
        census.worker_count("many", available_cpus=24)


def test_kiss_census_passes_one_path_and_caps_child_threads(tmp_path, monkeypatch):
    census = _load_kiss_census()
    log = tmp_path / "calls"
    fake = tmp_path / "kiss"
    fake.write_text(
        "#!/bin/sh\n"
        'printf "%s|%s|%s\\n" "$RAYON_NUM_THREADS" "$#" "$6" >> "$KISS_LOG"\n'
        'printf "VIOLATION:test:%s:1:item: detail\\n" "$6"\n'
        "exit 1\n",
        encoding="utf-8",
    )
    fake.chmod(0o755)
    monkeypatch.setattr(census, "ROOT", tmp_path)
    env = {"KISS_LOG": str(log), "RAYON_NUM_THREADS": "1"}

    result = census.scan_one(str(fake), "src/one.rs", env)

    assert result.path == "src/one.rs"
    assert result.status == 1
    assert census.validate(result) == 1
    assert log.read_text(encoding="utf-8") == "1|6|src/one.rs\n"

    log.write_text("", encoding="utf-8")
    paths = ["src/one.rs", "src/two.rs", "src/three.rs"]
    assert census.scan_paths(str(fake), paths, env, workers=2) == 3
    assert sorted(log.read_text(encoding="utf-8").splitlines()) == [
        "1|6|src/one.rs",
        "1|6|src/three.rs",
        "1|6|src/two.rs",
    ]


@pytest.mark.parametrize(
    ("status", "output", "message"),
    [
        (0, b"VIOLATION:test:file:1:item: detail\n", "status and report disagree"),
        (1, b"Analyzed: 1 files\n", "status and report disagree"),
        (2, b"native failure\n", "failed on src/bad.rs with exit 2"),
        (0, b"Unknown config key: typo\n", "rejected a config key"),
    ],
)
def test_kiss_census_validate_fails_closed(status, output, message, capsys):
    census = _load_kiss_census()
    result = census.ScanResult(
        "src/bad.rs", status, output, output.count(b"VIOLATION:")
    )

    with pytest.raises(SystemExit) as raised:
        census.validate(result)

    assert raised.value.code == 2
    assert message in capsys.readouterr().err


def test_cccc_registry_excludes_unsupported_frontends():
    scanner_contract = load_script("scanner_contract")
    assert not {"cpp", "cs", "scala"} & set(scanner_contract.CCCC_LANGUAGES)
    assert not {".cc", ".cpp", ".cxx", ".cs", ".scala"} & set(
        scanner_contract.CCCC_SUPPORTED_SUFFIXES
    )


def test_cccc_census_validator_requires_native_summary(tmp_path):
    validator = load_script("validate_cccc_census")
    report = tmp_path / "cccc.json"
    valid = {
        "files": [_native_file("src/empty.py", cognitive=1, cyclomatic=1)],
        "summary": _native_summary(1, 0),
    }
    report.write_text(json.dumps(valid), encoding="utf-8")
    assert validator.validate_report(report) == 1

    mismatched = json.loads(json.dumps(valid))
    mismatched["summary"]["function_count"] = 1
    for summary in (
        {},
        {"parse_error_count": 1},
        {"parse_error_count": "0"},
        mismatched["summary"],
    ):
        report.write_text(
            json.dumps({"files": valid["files"], "summary": summary}),
            encoding="utf-8",
        )
        with pytest.raises(SystemExit) as raised:
            validator.validate_report(report)
        assert raised.value.code == 2


def test_cccc_census_validator_rejects_file_parse_errors(tmp_path):
    validator = load_script("validate_cccc_census")
    report = tmp_path / "cccc.json"
    report.write_text(
        json.dumps(
            {
                "files": [
                    _native_file("src/bad.py", parse_errors=["unexpected token"])
                ],
                "summary": _native_summary(
                    1,
                    0,
                    parse_error_count=1,
                    parse_error_file_count=1,
                ),
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(SystemExit) as raised:
        validator.validate_report(report)
    assert raised.value.code == 2


def test_cccc_census_validator_requires_exact_nul_manifest_coverage(tmp_path):
    validator = load_script("validate_cccc_census")
    report = tmp_path / "cccc.json"
    manifest = tmp_path / "sources.nul"

    def write_report(paths):
        report.write_text(
            json.dumps(
                {
                    "files": [_native_file(path) for path in paths],
                    "summary": _native_summary(len(paths), 0),
                }
            ),
            encoding="utf-8",
        )

    write_report(["src/a.rs", "src/b.rs"])
    manifest.write_bytes(b"src/a.rs\0src/b.rs\0")
    assert validator.validate_report(report, manifest) == 2

    invalid_cases = (
        (["src/a.rs"], b"src/a.rs\0src/b.rs\0"),
        (["src/a.rs", "src/b.rs", "src/c.rs"], b"src/a.rs\0src/b.rs\0"),
        (["src/a.rs", "src/a.rs"], b"src/a.rs\0src/b.rs\0"),
        (["src/a.rs"], b"src/a.rs\0src/a.rs\0"),
        (["src/a.rs"], b"src/a.rs\0src/b.rs"),
    )
    for paths, raw_manifest in invalid_cases:
        write_report(paths)
        manifest.write_bytes(raw_manifest)
        with pytest.raises(SystemExit) as raised:
            validator.validate_report(report, manifest)
        assert raised.value.code == 2


def test_cccc_validator_counts_recursive_native_children_without_inventing_rows(
    tmp_path,
):
    validator = load_script("validate_cccc_census")
    report = tmp_path / "cccc.json"
    nested_functions = [
        {
            "name": "outer",
            "kind": "function",
            "line": 1,
            "cognitive": 1,
            "cyclomatic": 2,
            "children": [
                {
                    "name": "inner",
                    "kind": "function",
                    "line": 2,
                    "cognitive": 0,
                    "cyclomatic": 1,
                }
            ],
        }
    ]
    valid_document = {
        "files": [
            _native_file(
                "src/nested.py",
                functions=nested_functions,
                cognitive=1,
                cyclomatic=3,
            )
        ],
        "summary": _native_summary(
            1,
            2,
            cognitive={"sum": 1, "max": 1, "median": 0, "p90": 1, "p95": 1},
            cyclomatic={
                "sum": 3,
                "max": 2,
                "median": 1,
                "p90": 2,
                "p95": 2,
            },
        ),
    }
    report.write_text(json.dumps(valid_document), encoding="utf-8")
    assert validator.validate_report(report) == 1

    for field, value in (("cognitive", 0), ("cyclomatic", 2)):
        invalid = json.loads(json.dumps(valid_document))
        invalid["files"][0][field] = value
        report.write_text(json.dumps(invalid), encoding="utf-8")
        with pytest.raises(SystemExit) as raised:
            validator.validate_report(report)
        assert raised.value.code == 2


def test_complexity_terms_require_zero_is_opt_in_and_fails_actionable_backlog(
    tmp_path, capsys
):
    terms = load_script("report_complexity_terms")
    report = tmp_path / "cccc.json"
    functions = [
        {
            "name": "complex",
            "kind": "function",
            "line": 1,
            "cyclomatic": 11,
            "cognitive": 1,
        }
    ]
    report.write_text(
        json.dumps(
            {
                "files": [
                    _native_file(
                        "src/example.py",
                        functions=functions,
                        cognitive=1,
                        cyclomatic=11,
                    )
                ],
                "summary": _native_summary(
                    1,
                    1,
                    cognitive=_metric_summary(1),
                    cyclomatic=_metric_summary(11),
                ),
            }
        ),
        encoding="utf-8",
    )

    assert terms.main([str(report)]) == 0
    assert "REAL BACKLOG" in capsys.readouterr().out
    assert terms.main(["--require-zero", str(report)]) == 1
    assert "REAL BACKLOG" in capsys.readouterr().out


def test_complexity_terms_keeps_accepted_dispatch_visible_and_zero_gate_passes(
    tmp_path, capsys
):
    terms = load_script("report_complexity_terms")
    report = tmp_path / "cccc.json"
    source_lines = (ROOT / "src/server/wire/mod.rs").read_text().splitlines()
    line = next(
        index
        for index, text in enumerate(source_lines, 1)
        if "fn dispatch_kind" in text
    )
    functions = [
        {
            "name": "dispatch_kind",
            "kind": "method",
            "line": line,
            "cognitive": 1,
            "cyclomatic": 32,
        }
    ]
    report.write_text(
        json.dumps(
            {
                "files": [
                    _native_file(
                        "src/server/wire/mod.rs",
                        functions=functions,
                        cognitive=1,
                        cyclomatic=32,
                    )
                ],
                "summary": _native_summary(
                    1,
                    1,
                    cognitive=_metric_summary(1),
                    cyclomatic=_metric_summary(32),
                ),
            }
        ),
        encoding="utf-8",
    )

    assert terms.main(["--require-zero", str(report)]) == 0
    output = capsys.readouterr().out
    accepted_line = next(
        line for line in output.splitlines() if line.strip().startswith("accepted")
    )
    backlog_line = next(
        line for line in output.splitlines() if line.strip().startswith("REAL BACKLOG")
    )
    assert accepted_line.strip().split()[1] == "1"
    assert "ACCEPTED BY RULE" in output
    assert backlog_line.strip().split()[2] == "0"


def test_complexity_terms_rejects_non_native_function_numbers(tmp_path):
    terms = load_script("report_complexity_terms")
    report = tmp_path / "cccc.json"
    function = {
        "name": "complex",
        "kind": "function",
        "line": 1,
        "cyclomatic": 11,
        "cognitive": 1,
    }
    document: dict[str, Any] = {
        "files": [
            _native_file(
                "src/example.py", functions=[function], cognitive=1, cyclomatic=11
            )
        ],
        "summary": _native_summary(
            1,
            1,
            cognitive=_metric_summary(1),
            cyclomatic=_metric_summary(11),
        ),
    }
    for field, value in (
        ("name", 1),
        ("name", " "),
        ("line", "1"),
        ("line", -1),
        ("line", 2**32),
        ("cyclomatic", "11"),
        ("cyclomatic", -1),
        ("cyclomatic", 2**32),
        ("cognitive", "1"),
        ("cognitive", -1),
        ("cognitive", 2**32),
    ):
        document["files"][0]["functions"][0][field] = value
        report.write_text(json.dumps(document), encoding="utf-8")
        with pytest.raises(SystemExit) as raised:
            terms.main(["--require-zero", str(report)])
        assert raised.value.code == 2
        document["files"][0]["functions"][0][field] = {
            "name": "complex",
            "line": 1,
            "cyclomatic": 11,
            "cognitive": 1,
        }[field]

    document["summary"]["cognitive"]["max"] = 2**32
    report.write_text(json.dumps(document), encoding="utf-8")
    with pytest.raises(SystemExit) as raised:
        terms.main(["--require-zero", str(report)])
    assert raised.value.code == 2


def test_relative_to_root_rejects_escape_and_accepts_absolute_inside(tmp_path):
    scanner_contract = load_script("scanner_contract")
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
    scanner_contract = load_script("scanner_contract")
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
    complexity = load_script("check_complexity_staged")
    measured: dict[str, list[tuple[int, int, int, bool]]] = {}
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
    complexity = load_script("check_complexity_staged")
    with pytest.raises(SystemExit) as raised:
        complexity._walk(
            {"name": "outer", "cyclomatic": 2, "cognitive": 1, "children": []},
            "",
            {},
        )
    assert raised.value.code == 2


def test_complexity_judge_catches_over_cap_duplicate_name():
    complexity = load_script("check_complexity_staged")
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
    complexity = load_script("check_complexity_staged")
    metrics = complexity.Metrics
    before = {"dispatch_kind": [metrics(32, 1, 2076, True)]}
    after = {"dispatch_kind": [metrics(33, 1, 2076, True)]}

    assert complexity.judge(before, after, 10, 15) == []


def test_complexity_fails_when_a_dispatcher_leaves_the_accepted_class():
    """Growing a catch-all arm restores the full cyclomatic value at once."""
    complexity = load_script("check_complexity_staged")
    metrics = complexity.Metrics
    before = {"dispatch_kind": [metrics(32, 1, 2076, True)]}
    after = {"dispatch_kind": [metrics(32, 1, 2076, False)]}

    assert complexity.judge(before, after, 10, 15) == [
        ("WORSE", "dispatch_kind", (0, 1), (32, 1))
    ]


def test_complexity_never_exempts_cognitive_complexity():
    complexity = load_script("check_complexity_staged")
    metrics = complexity.Metrics
    after = {"handle": [metrics(32, 16, 10, True)]}

    assert complexity.judge({}, after, 10, 15) == [("NEW", "handle", (0, 0), (32, 16))]


def test_complexity_parser_rejects_missing_report_summary(monkeypatch):
    complexity = load_script("check_complexity_staged")
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
    dupehound = load_script("check_dupehound")
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
    jscpd = load_script("check_duplication")
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
    jscpd = load_script("check_duplication")
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
