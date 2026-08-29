#!/usr/bin/env python3
"""Fail-closed, changed-source ``dupehound`` gate for epistemic-graph.

Dupehound owns the structural whole-function clone question: does a changed
function reimplement an existing function after identifiers and literals have
been rewritten?  jscpd owns copied blocks, templates, and configuration, and
is intentionally run by the pre-push/CI gate instead.  This wrapper only
launches a locally installed binary, pins its version and thresholds from
``pyproject.toml``, and never writes a baseline or downloads a tool.

Exit codes are 0 (no finding), 1 (changed-source clone), and 2 (the check
could not run).  Existing unrelated clone debt is reported by the scanner but
is not converted into a baseline by this wrapper.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, NoReturn

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(Path(__file__).resolve().parent))

import scanner_contract  # noqa: E402

EXPECTED_JSON_SCHEMA_VERSION = 1

SUPPORTED_SUFFIXES = frozenset(
    {
        ".c",
        ".cc",
        ".cpp",
        ".cjs",
        ".cts",
        ".cs",
        ".cxx",
        ".go",
        ".h",
        ".hh",
        ".hpp",
        ".hxx",
        ".java",
        ".js",
        ".jsx",
        ".mjs",
        ".mts",
        ".php",
        ".py",
        ".pyi",
        ".rb",
        ".rs",
        ".swift",
        ".ts",
        ".tsx",
    }
)
_TEST_DIRECTORY_NAMES = frozenset({"tests", "test", "__tests__", "testdata", "spec"})
_TEST_FILE_SUFFIXES = (
    "_test.go",
    "_test.py",
    "_test.rs",
    "_spec.rb",
    "_test.rb",
    "tests.swift",
    "test.swift",
    "spec.swift",
    "test.php",
    "tests.php",
    "test.java",
    "tests.java",
)
_REQUIRED_FINDING_FIELDS = (
    "file",
    "line",
    "name",
    "similarity",
    "original_file",
    "original_line",
    "original_name",
)


def fail(message: str) -> NoReturn:
    print(f"dupehound gate: CANNOT RUN: {message}", file=sys.stderr)
    raise SystemExit(2)


def contract() -> scanner_contract.ScannerContract:
    try:
        return scanner_contract.load_contract()
    except scanner_contract.ScannerContractError as exc:
        fail(str(exc))


def _resolve_dupehound(
    config: scanner_contract.ScannerContract | None = None,
) -> str:
    """Resolve the locally installed binary using the central contract."""

    try:
        return scanner_contract.resolve_binary("dupehound", "DUPEHOUND_BIN")
    except FileNotFoundError as exc:
        fail(str(exc))


def _check_version(
    executable: str, config: scanner_contract.ScannerContract | None = None
) -> None:
    value = config or contract()
    try:
        scanner_contract.exact_version(executable, value.dupehound_version_output)
    except RuntimeError as exc:
        fail(str(exc))


def git(*args: str, preserve_index: bool = True) -> str:
    """Run git from this checkout without letting a hook re-root its cwd."""

    try:
        result = scanner_contract.run_git(args, cwd=ROOT, preserve_index=preserve_index)
    except RuntimeError as exc:
        fail(str(exc))
    if result.returncode != 0:
        fail(f"git {' '.join(args)} failed: {(result.stderr or '').strip()[:400]}")
    if not isinstance(result.stdout, str):
        fail(f"git {' '.join(args)} returned no text output")
    return result.stdout


def _git_paths(raw: str) -> list[str]:
    """Split Git's NUL-framed output, tolerating simple test doubles."""

    if "\x00" in raw:
        return [path for path in raw.split("\x00") if path]
    return raw.splitlines()


def changed_paths(base_ref: str | None = None) -> list[str]:
    """Return staged/worktree paths or a commit-range path set."""

    if base_ref:
        raw = git(
            "diff",
            "--name-only",
            "-z",
            "--diff-filter=ACMR",
            f"{base_ref}...HEAD",
            preserve_index=False,
        )
        paths = _git_paths(raw)
    else:
        staged = git(
            "diff",
            "--cached",
            "--name-only",
            "-z",
            "--diff-filter=ACMR",
            preserve_index=True,
        )
        paths = _git_paths(staged)
        if not paths:
            paths = git(
                "diff",
                "--name-only",
                "-z",
                "--diff-filter=ACMR",
                "HEAD",
                preserve_index=False,
            )
            paths = _git_paths(paths)
            paths.extend(
                _git_paths(
                    git(
                        "ls-files",
                        "-z",
                        "--others",
                        "--exclude-standard",
                        preserve_index=False,
                    )
                )
            )
    result = []
    for path in paths:
        if not path.strip():
            continue
        try:
            result.append(scanner_contract.relative_to_root(path, ROOT))
        except ValueError as exc:
            fail(f"git reported unsafe changed path: {exc}")
    return sorted(set(result))


def selected_paths(
    paths: list[str], config: scanner_contract.ScannerContract
) -> list[str]:
    selected = []
    for path in paths:
        try:
            normalized = scanner_contract.relative_to_root(path, ROOT)
        except ValueError as exc:
            fail(f"changed path is unsafe: {exc}")
        if (
            Path(normalized).suffix.lower() in SUPPORTED_SUFFIXES
            and not is_dupehound_test_path(normalized)
            and not config.is_excluded(normalized)
        ):
            selected.append(normalized)
    return sorted(set(selected))


# Keep the name used by the first EG candidate and by focused consumers.
select_supported_paths = selected_paths


def is_dupehound_test_path(path: str) -> bool:
    """Mirror dupehound v0.1.2's test-path classifier for scope reporting."""

    normalized = path.replace("\\", "/")
    while normalized.startswith("./"):
        normalized = normalized[2:]
    normalized = normalized.casefold()
    parts = normalized.split("/")
    filename = parts[-1] if parts else normalized
    if filename in {"tests.rs", "test.rs"}:
        return True
    if filename.startswith("test_") or ".test." in filename or ".spec." in filename:
        return True
    if filename.startswith("conftest."):
        return True
    if filename.endswith(_TEST_FILE_SUFFIXES):
        return True
    return any(part in _TEST_DIRECTORY_NAMES for part in parts[:-1])


def command(
    executable: str,
    config: scanner_contract.ScannerContract,
    base_ref: str | None = None,
) -> list[str]:
    arguments = [
        executable,
        "check",
        "--json",
        "--threshold",
        str(config.dupehound_threshold),
        "--min-tokens",
        str(config.dupehound_min_tokens),
        "--exclude-tests",
    ]
    for pattern in config.exclusions:
        arguments.extend(("--exclude", pattern))
    if base_ref:
        arguments.extend(("--diff", base_ref))
    # The default comparison is intentional: without --diff dupehound compares
    # the staged index (or working tree for a manual run) against HEAD.  CI
    # supplies --diff so it compares the requested commit range.
    arguments.append(str(ROOT))
    return arguments


_command = command


def _validate_path_field(index: int, field: str, value: object) -> None:
    if not isinstance(value, str) or not value.strip():
        fail(f"dupehound finding {index} has an invalid {field}")
    try:
        normalized = scanner_contract.relative_to_root(value, ROOT)
    except ValueError as exc:
        fail(f"dupehound finding {index} {field} escapes repository root: {exc}")
    if not normalized:
        fail(f"dupehound finding {index} has an empty {field}")


def finding_document(output: str) -> list[dict[str, Any]]:
    try:
        document = json.loads(output)
    except (TypeError, UnicodeError, json.JSONDecodeError) as exc:
        fail(f"dupehound returned invalid JSON: {exc}")
    if not isinstance(document, dict):
        fail("dupehound JSON result is not an object")
    schema_version = document.get("schema_version")
    if (
        isinstance(schema_version, bool)
        or not isinstance(schema_version, int)
        or schema_version != EXPECTED_JSON_SCHEMA_VERSION
    ):
        fail(
            "dupehound JSON schema drift: expected schema_version "
            f"{EXPECTED_JSON_SCHEMA_VERSION}, got "
            f"{schema_version!r}"
        )
    findings = document.get("findings")
    if not isinstance(findings, list):
        fail("dupehound JSON result has no findings array")
    for index, finding in enumerate(findings):
        _validate_finding(index, finding)
    return findings


def _finding_object(index: int, finding: object) -> dict[str, Any]:
    if not isinstance(finding, dict):
        fail(f"dupehound finding {index} is not an object")
    missing = [key for key in _REQUIRED_FINDING_FIELDS if key not in finding]
    if missing:
        fail(f"dupehound finding {index} is missing {', '.join(missing)}")
    return finding


def _validate_finding_files(index: int, finding: dict[str, Any]) -> None:
    if not isinstance(finding["file"], str) or not isinstance(
        finding["original_file"], str
    ):
        fail(f"dupehound finding {index} has invalid file fields")
    _validate_path_field(index, "file", finding["file"])
    _validate_path_field(index, "original_file", finding["original_file"])


def _validate_finding_names(index: int, finding: dict[str, Any]) -> None:
    for field in ("name", "original_name"):
        if not isinstance(finding[field], str) or not finding[field].strip():
            fail(f"dupehound finding {index} has invalid {field}")


def _validate_finding_lines(index: int, finding: dict[str, Any]) -> None:
    for key in ("line", "original_line"):
        if (
            isinstance(finding[key], bool)
            or not isinstance(finding[key], int)
            or finding[key] <= 0
        ):
            fail(f"dupehound finding {index} has invalid {key}")


def _validate_finding_similarity(index: int, finding: dict[str, Any]) -> None:
    similarity = finding["similarity"]
    if (
        isinstance(similarity, bool)
        or not isinstance(similarity, (int, float))
        or not math.isfinite(float(similarity))
        or not 0.0 <= float(similarity) <= 1.0
    ):
        fail(f"dupehound finding {index} has invalid similarity")


def _validate_finding(index: int, finding: object) -> None:
    value = _finding_object(index, finding)
    _validate_finding_files(index, value)
    _validate_finding_names(index, value)
    _validate_finding_lines(index, value)
    _validate_finding_similarity(index, value)


parse_result = finding_document


def _validated_findings(
    result: subprocess.CompletedProcess[str],
) -> list[dict[str, Any]]:
    if result.returncode not in (0, 1):
        fail(
            f"dupehound exited {result.returncode}: "
            f"{(result.stderr or '').strip()[:500]}"
        )
    findings = finding_document(result.stdout or "")
    if result.returncode == 0 and findings:
        fail("dupehound returned findings with exit 0")
    if result.returncode == 1 and not findings:
        fail("dupehound returned exit 1 without findings")
    return findings


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--base-ref",
        "--diff",
        dest="base_ref",
        default=os.environ.get("CX_DUP_BASE_REF"),
        help="compare the current commit with this revision (CI/PR semantics)",
    )
    args = parser.parse_args(argv)
    config = contract()
    paths = selected_paths(changed_paths(args.base_ref), config)
    if not paths:
        print("dupehound gate: OK: no changed supported-language source")
        return 0

    executable = _resolve_dupehound(config)
    _check_version(executable, config)

    print("dupehound gate: checking changed source (" + ", ".join(paths) + ")")
    try:
        result = subprocess.run(
            command(executable, config, args.base_ref),
            cwd=str(ROOT),
            env=scanner_contract.sanitized_env(preserve_index=args.base_ref is None),
            capture_output=True,
            text=True,
            timeout=900,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        fail(f"dupehound timed out after {exc.timeout}s")
    except (OSError, UnicodeError) as exc:
        fail(f"could not execute dupehound: {exc}")
    findings = _validated_findings(result)
    if not findings:
        print("dupehound gate: OK: no changed function reimplementation")
        return 0
    print(f"dupehound gate: FAIL: {len(findings)} structural clone(s)")
    for finding in findings:
        print(
            f"  {finding['file']}:{finding['line']} {finding['name']} "
            f"reimplements {finding['original_file']}:{finding['original_line']} "
            f"{finding['original_name']} "
            f"(similarity {float(finding['similarity']):.3f})"
        )
    return 1


if __name__ == "__main__":
    sys.exit(main())
