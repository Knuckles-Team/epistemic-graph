#!/usr/bin/env python3
"""Run the BUG-CX-136 jscpd block gate.

``enforce`` compares the clone-pair set in a live base tree with the clone
pair set in ``HEAD`` and fails only for new pairs.  ``census`` scans the whole
tracked tree and reports findings without failing on them; it is the advisory
number used to drive debt down.  Both modes use the same checked-in format and
exclusion contract, include Rust/Python/JavaScript blocks plus configuration
and template formats, and resolve an already-installed exact jscpd version.

The snapshots are materialized with ``git archive`` into a temporary sibling
directory.  This avoids handing jscpd a repository root (and therefore avoids
walking .git/worktree metadata) while retaining hidden configuration such as
``.github``.  No refs, worktrees, stashes, or baseline files are created.

Exit codes: 0 clean (or census, regardless of findings), 1 new clone pairs in
enforce mode, 2 scanner/environment/contract failure.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import subprocess
import sys
import tarfile
import tempfile
from contextlib import contextmanager
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Any, NoReturn

import scanner_contract

ROOT = Path(__file__).resolve().parent.parent

_AMBIENT_CONFIGS = (
    ".jscpd.json",
    ".jscpdrc",
    ".jscpdrc.json",
    ".jscpdrc.yaml",
    ".jscpdrc.yml",
    "jscpd.config.js",
    "jscpd.config.cjs",
)
_REQUIRED_CLONE_FIELDS = (
    "format",
    "fragment",
    "lines",
    "tokens",
    "firstFile",
    "secondFile",
)
_HEX_OBJECT = re.compile(r"^(?:[0-9a-fA-F]{40}|[0-9a-fA-F]{64})$")


@contextmanager
def _temporary_directory(prefix: str, parent: Path):
    try:
        with tempfile.TemporaryDirectory(prefix=prefix, dir=str(parent)) as raw:
            yield Path(raw)
    except (OSError, UnicodeError, ValueError) as exc:
        fail(f"could not create or clean temporary directory: {exc}")


def fail(message: str) -> NoReturn:
    print(f"jscpd gate: CANNOT RUN: {message}", file=sys.stderr)
    raise SystemExit(2)


def config() -> scanner_contract.ScannerContract:
    try:
        return scanner_contract.load_contract()
    except scanner_contract.ScannerContractError as exc:
        fail(str(exc))


def git(*args: str, preserve_index: bool = False) -> str:
    """Run git with repository selectors removed and fail closed."""

    try:
        result = scanner_contract.run_git(args, cwd=ROOT, preserve_index=preserve_index)
    except RuntimeError as exc:
        fail(str(exc))
    if result.returncode != 0:
        fail(f"git {' '.join(args)} failed: {(result.stderr or '').strip()[:500]}")
    if not isinstance(result.stdout, str):
        fail(f"git {' '.join(args)} returned no text output")
    return result.stdout


def _git_paths(raw: str) -> list[str]:
    """Split Git's NUL-framed path output, tolerating simple test doubles."""

    if "\x00" in raw:
        return [path for path in raw.split("\x00") if path]
    return raw.splitlines()


def changed_paths(
    base_ref: str, contract: scanner_contract.ScannerContract
) -> list[str]:
    raw = git(
        "diff",
        "--name-only",
        "-z",
        "--diff-filter=ACMR",
        f"{base_ref}...HEAD",
        preserve_index=False,
    )
    result = []
    for path in _git_paths(raw):
        if not path.strip():
            continue
        try:
            normalized = scanner_contract.relative_to_root(path, ROOT)
        except ValueError as exc:
            fail(f"git reported unsafe changed path: {exc}")
        if contract.is_jscpd_path(normalized):
            result.append(normalized)
    return sorted(set(result))


def guard_ambient_config(directory: Path) -> None:
    """Do not let an unreviewed cwd config silently change jscpd semantics."""

    for name in _AMBIENT_CONFIGS:
        candidate = directory / name
        if candidate.is_symlink() or candidate.exists():
            fail(
                f"{candidate} exists; jscpd auto-loads cwd config files. "
                "Remove it and keep thresholds in pyproject.toml/this command."
            )


def run_version(executable: str, contract: scanner_contract.ScannerContract) -> None:
    try:
        scanner_contract.exact_version(executable, contract.jscpd_version_output)
    except RuntimeError as exc:
        fail(str(exc))


def _resolve_jscpd(
    contract: scanner_contract.ScannerContract | None = None,
) -> str:
    """Resolve the locally installed jscpd binary."""

    try:
        return scanner_contract.resolve_binary("jscpd", "JSCPD_BIN")
    except FileNotFoundError as exc:
        fail(str(exc))


def _check_version(
    executable: str, contract: scanner_contract.ScannerContract | None = None
) -> None:
    run_version(executable, contract or config())


def command(
    executable: str,
    contract: scanner_contract.ScannerContract,
    output: Path,
    target: Path | list[Path],
) -> list[str]:
    # jscpd v5's process exit code is not the finding count.  We deliberately
    # omit --exit-code and treat the validated JSON report as the source of
    # truth (the option's greedy parser also consumes a following path).
    arguments = [
        executable,
        "--min-tokens",
        str(contract.jscpd_min_tokens),
        "--min-lines",
        str(contract.jscpd_min_lines),
        "--mode",
        contract.jscpd_mode,
        "--format",
        contract.jscpd_formats_arg,
        "--ignore",
        ",".join(contract.exclusions),
        "--absolute",
        "-r",
        "json",
        "-o",
        str(output),
        "--formats-exts",
        contract.jscpd_format_exts_arg,
        "--formats-names",
        contract.jscpd_format_names_arg,
    ]
    targets = (
        [Path(target)]
        if isinstance(target, (str, Path))
        else [Path(item) for item in target]
    )
    return [*arguments, *(str(path) for path in targets)]


_jscpd_command = command


def _validate_report_path(
    raw: object,
    report: Path,
    field: str,
    root: Path,
    *,
    require_absolute: bool = True,
) -> None:
    if not isinstance(raw, str) or not raw.strip():
        fail(f"{report} duplicate has invalid {field}")
    if require_absolute and not Path(raw).is_absolute():
        fail(f"{report} duplicate {field} is not absolute")
    try:
        relative = scanner_contract.relative_to_root(raw, root)
    except ValueError as exc:
        fail(f"{report} duplicate {field} escapes scan root: {exc}")
    if not relative:
        fail(f"{report} duplicate {field} names the scan root, not a file")


def _clone_object(index: int, clone: object, report: Path) -> dict[str, Any]:
    if not isinstance(clone, dict):
        fail(f"{report} duplicate {index} is not an object")
    missing = [field for field in _REQUIRED_CLONE_FIELDS if field not in clone]
    if missing:
        fail(f"{report} duplicate {index} is missing {', '.join(missing)}")
    return clone


def _validate_clone_header(
    index: int,
    clone: dict[str, Any],
    report: Path,
    formats: frozenset[str] | None,
) -> None:
    if not isinstance(clone["format"], str) or not clone["format"].strip():
        fail(f"{report} duplicate {index} has an invalid format")
    if formats is not None and clone["format"] not in formats:
        fail(f"{report} duplicate {index} has an unsupported format")
    if not isinstance(clone["fragment"], str) or not clone["fragment"].strip():
        fail(f"{report} duplicate {index} has no fragment")


def _validate_clone_counts(index: int, clone: dict[str, Any], report: Path) -> None:
    for field in ("lines", "tokens"):
        value = clone[field]
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            fail(f"{report} duplicate {index} has invalid {field}")


def _validate_clone_side_path(
    index: int,
    side: str,
    location: dict[str, Any],
    report: Path,
    root: Path | None,
    roots: tuple[Path, ...] | None,
) -> None:
    name = location["name"]
    if roots is not None:
        if not roots:
            fail(f"{report} duplicate {index} has no trusted scan root")
        if not any(_path_is_under_report_root(name, trusted) for trusted in roots):
            fail(f"{report} duplicate {index} {side} escapes scan root")
    elif root is not None:
        _validate_report_path(name, report, side, root)


def _validate_clone_side_lines(
    index: int, side: str, location: dict[str, Any], report: Path
) -> None:
    for point in ("startLoc", "endLoc"):
        position = location.get(point)
        line = position.get("line") if isinstance(position, dict) else None
        if isinstance(line, bool) or not isinstance(line, int) or line <= 0:
            fail(f"{report} duplicate {index} has invalid {side}.{point}.line")
    if location["endLoc"]["line"] < location["startLoc"]["line"]:
        fail(f"{report} duplicate {index} has a reversed {side} range")


def _validate_clone_side(
    index: int,
    side: str,
    location: object,
    report: Path,
    root: Path | None,
    roots: tuple[Path, ...] | None,
) -> None:
    if (
        not isinstance(location, dict)
        or not isinstance(location.get("name"), str)
        or not location["name"].strip()
    ):
        fail(f"{report} duplicate {index} has invalid {side}")
    _validate_clone_side_path(index, side, location, report, root, roots)
    _validate_clone_side_lines(index, side, location, report)


def validate_clone(
    index: int,
    clone: object,
    report: Path,
    root: Path | None = None,
    *,
    roots: tuple[Path, ...] | None = None,
    formats: frozenset[str] | None = None,
) -> None:
    value = _clone_object(index, clone, report)
    _validate_clone_header(index, value, report, formats)
    _validate_clone_counts(index, value, report)
    for side in ("firstFile", "secondFile"):
        _validate_clone_side(index, side, value[side], report, root, roots)


def _path_is_under_report_root(raw: str, root: Path) -> bool:
    if not Path(raw).is_absolute():
        return False
    try:
        relative = scanner_contract.relative_to_root(raw, root)
    except ValueError:
        return False
    return bool(relative)


_validate_clone = validate_clone


def _allowed_report_formats(
    contract: scanner_contract.ScannerContract,
) -> frozenset[str]:
    return frozenset(
        [name for name, _values in contract.jscpd_formats]
        + [name for name, _values in contract.jscpd_filenames]
    )


def _report_duplicates(document: object, report: Path) -> list[object]:
    if not isinstance(document, dict):
        fail(f"{report} is not a JSON object")
    duplicates = document.get("duplicates")
    if not isinstance(duplicates, list):
        fail(f"{report} has no duplicates array")
    return duplicates


def _report_total(document: dict[str, Any], report: Path) -> dict[str, Any]:
    statistics = document.get("statistics")
    total = statistics.get("total") if isinstance(statistics, dict) else None
    if not isinstance(total, dict):
        fail(f"{report} has malformed statistics.total")
    return total


def _validate_report_total(
    total: dict[str, Any], duplicate_count: int, report: Path
) -> None:
    for field in ("clones", "sources", "duplicatedLines", "lines"):
        value = total.get(field)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            fail(f"{report} has invalid total.{field}")
    if total["clones"] != duplicate_count:
        fail(
            f"{report} total.clones={total['clones']} disagrees with "
            f"duplicates length {duplicate_count}"
        )
    if total["duplicatedLines"] > total["lines"]:
        fail(f"{report} has more duplicated lines than total lines")


def _validate_report_percentage(total: dict[str, Any], report: Path) -> None:
    percentage = total.get("percentage")
    if (
        isinstance(percentage, bool)
        or not isinstance(percentage, (int, float))
        or not math.isfinite(float(percentage))
        or not 0.0 <= float(percentage) <= 100.0
    ):
        fail(f"{report} has invalid total.percentage")


def _validate_report_duplicates(
    duplicates: list[object],
    report: Path,
    roots: tuple[Path, ...] | None,
    formats: frozenset[str] | None,
) -> None:
    for index, clone in enumerate(duplicates):
        validate_clone(index, clone, report, roots=roots, formats=formats)


def _validate_report(
    document: object,
    report: Path,
    roots: tuple[Path, ...] | None = None,
    formats: frozenset[str] | None = None,
) -> dict[str, Any]:
    """Validate the complete jscpd report consumed by both gate modes."""

    duplicates = _report_duplicates(document, report)
    if not isinstance(document, dict):
        fail(f"{report} is not a JSON object")
    total = _report_total(document, report)
    _validate_report_total(total, len(duplicates), report)
    _validate_report_percentage(total, report)
    _validate_report_duplicates(duplicates, report, roots, formats)
    return document


def _check_report_file(report: Path) -> None:
    try:
        report_is_link = report.is_symlink()
        report_is_file = report.is_file()
    except (OSError, RuntimeError, ValueError) as exc:
        fail(f"could not inspect jscpd report {report}: {exc}")
    if report_is_link or not report_is_file:
        fail(f"jscpd exited successfully but wrote no report at {report}")


def _check_report_output(report: Path, out_dir: Path | None) -> None:
    if out_dir is not None:
        try:
            scanner_contract.relative_to_root(report, out_dir)
        except ValueError as exc:
            fail(f"jscpd report is outside its output directory: {exc}")


def _read_report(report: Path) -> object:
    try:
        return json.loads(report.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        fail(f"{report} is not valid JSON: {exc}")


def _trusted_roots(
    root: Path | None, roots: tuple[Path, ...] | None
) -> tuple[Path, ...] | None:
    if roots is not None and root is not None:
        fail("jscpd report received both root and roots constraints")
    return roots if roots is not None else ((root,) if root is not None else None)


def load_report(
    report: Path,
    root: Path | None = None,
    *,
    out_dir: Path | None = None,
    formats: frozenset[str] | None = None,
    roots: tuple[Path, ...] | None = None,
) -> dict[str, Any]:
    _check_report_file(report)
    _check_report_output(report, out_dir)
    document = _read_report(report)
    trusted_roots = _trusted_roots(root, roots)
    return _validate_report(document, report, roots=trusted_roots, formats=formats)


def _normalise_targets(target: Path | list[Path]) -> list[Path]:
    if isinstance(target, (str, Path)):
        return [Path(target)]
    return [Path(item) for item in target]


def _target_cwd(targets: list[Path], cwd: Path | None) -> Path:
    if cwd is not None:
        return cwd
    return targets[0].parent if targets[0].is_file() else targets[0]


def _run_jscpd_process(
    executable: str,
    contract: scanner_contract.ScannerContract,
    output: Path,
    targets: list[Path],
    cwd: Path,
) -> None:
    try:
        result = subprocess.run(
            command(executable, contract, output, targets),
            cwd=str(cwd),
            env=scanner_contract.sanitized_env(),
            capture_output=True,
            text=True,
            timeout=900,
            check=False,
        )
    except subprocess.TimeoutExpired:
        fail(f"jscpd timed out while scanning {targets}")
    except (OSError, UnicodeError) as exc:
        fail(f"could not execute jscpd: {exc}")
    if result.returncode != 0:
        fail(
            f"jscpd exited {result.returncode}: "
            f"{(result.stderr or result.stdout or '').strip()[-1000:]}"
        )


def run_jscpd(
    executable: str,
    contract: scanner_contract.ScannerContract,
    target: Path | list[Path],
    *,
    cwd: Path | None = None,
) -> dict[str, Any]:
    targets = _normalise_targets(target)
    if not targets:
        fail("jscpd received no scan target")
    actual_cwd = _target_cwd(targets, cwd)
    guard_ambient_config(actual_cwd)
    with _temporary_directory("cx-jscpd-report-", actual_cwd.parent) as output:
        _run_jscpd_process(executable, contract, output, targets, actual_cwd)
        return load_report(
            output / "jscpd-report.json",
            actual_cwd,
            out_dir=output,
            formats=_allowed_report_formats(contract),
        )


# Compatibility seams for focused wrapper consumers.  Keep the public
# implementation above as the source of truth while retaining the private
# names used by the original scanner candidate and its tests.
def _load_report(
    report: Path,
    out_dir: Path | None = None,
    roots: tuple[Path, ...] | None = None,
    formats: frozenset[str] | None = None,
) -> dict[str, Any]:
    return load_report(report, out_dir=out_dir, formats=formats, roots=roots)


# EH-308: the file-split extraction this program ran consistently rewrote
# destructured locals into field access on a new per-handler context struct,
# always through one of exactly three receiver names -- `req.id` became
# `ctx.req.id`, `state` became `context.state`, `authority` became
# `coordination.authority`.  That rewrite changes a fragment's raw token text
# without changing the duplication it represents.  Nothing else about the
# extraction is mechanical enough to normalise safely: local bindings were
# also renamed and reshaped in the same series (verified by hand against
# JSCPD-TRIAGE.md's samples), and canonalising those away would erase real
# identifier information, not just an added receiver.
_CONTEXT_RECEIVER_PREFIX = re.compile(r"\b(?:ctx|context|coordination)\.(?=[A-Za-z_])")


def _normalise_fragment_identity(fragment: str) -> str:
    """Canonicalise a clone fragment for IDENTITY purposes only.

    This never touches the fragment a human reads in a failure report --
    only the text fed to the identity digest.  Two normalisations, both
    reversing a specific, named side effect of the 2.27.x file-split
    extraction rather than a general code-shape opinion:

    1. Per-line whitespace: a block that moved from a nested match arm/async
       closure to a top-level function changes indentation without changing
       its tokens; jscpd's own clone matching already ignores this, but this
       script hashes the raw captured substring, so indentation alone used
       to change the digest.
    2. The receiver prefix added by the context-struct rewrite described
       above, stripped only for the three known receiver names and only
       when it prefixes a field access -- no other identifier is touched,
       so two genuinely different fragments still hash differently.
    """

    lines = (line.strip() for line in fragment.splitlines())
    text = "\n".join(line for line in lines if line)
    return _CONTEXT_RECEIVER_PREFIX.sub("", text)


def clone_key(clone: dict[str, Any], root: Path) -> tuple[str, str, frozenset[str]]:
    if not isinstance(clone.get("format"), str) or not clone["format"].strip():
        fail("jscpd duplicate has an invalid format")
    if not isinstance(clone.get("fragment"), str) or not clone["fragment"].strip():
        fail("jscpd duplicate has no fragment")
    paths = []
    for side in ("firstFile", "secondFile"):
        try:
            relative = scanner_contract.relative_to_root(clone[side]["name"], root)
        except (KeyError, TypeError, ValueError) as exc:
            fail(f"jscpd duplicate has a path outside scan root: {exc}")
        if not relative:
            fail("jscpd duplicate names the scan root, not a file")
        paths.append(relative)
    digest = hashlib.sha256(
        _normalise_fragment_identity(clone["fragment"]).encode("utf-8", "surrogatepass")
    ).hexdigest()
    return clone["format"], digest, frozenset(paths)


def keys(document: dict[str, Any], root: Path) -> set[tuple[str, str, frozenset[str]]]:
    duplicates = document.get("duplicates")
    if not isinstance(duplicates, list):
        fail("jscpd report has no duplicates array")
    result = set()
    for clone in duplicates:
        if not isinstance(clone, dict):
            fail("jscpd report contains a non-object duplicate")
        result.add(clone_key(clone, root))
    return result


def report_stats(document: dict[str, Any], label: str) -> None:
    total = document["statistics"]["total"]
    print(
        f"jscpd gate [{label}]: {total['clones']} clone(s), "
        f"{total['sources']} source(s), {total['duplicatedLines']} of "
        f"{total['lines']} lines duplicated ({float(total['percentage']):.2f}%)"
    )


def _tree_paths(ref: str) -> list[str]:
    """Return every path in a committed tree, preserving Git's NUL framing."""

    raw = git(
        "ls-tree",
        "-r",
        "-z",
        ref,
        preserve_index=False,
    )
    result = []
    for record in _git_paths(raw):
        # ``git ls-tree -z`` separates the object header and path with a tab.
        # Keep only blobs: a submodule is a tracked tree entry, not a file that
        # can be scanned or truthfully counted as part of the corpus.
        header, separator, raw_path = record.partition("\t")
        if separator:
            fields = header.split()
            if len(fields) < 2 or fields[1] != "blob":
                continue
        else:
            # Tolerate a simple path-only test double; real Git output always
            # takes the metadata branch above.
            raw_path = record
        try:
            result.append(scanner_contract.relative_to_root(raw_path, ROOT))
        except ValueError as exc:
            fail(f"git reported unsafe tracked path: {exc}")
    return result


def tracked_paths(
    ref: str,
    contract: scanner_contract.ScannerContract,
    requested: list[str] | None = None,
) -> list[str]:
    """Select the in-scope files actually present in a committed tree."""

    all_paths = _tree_paths(ref)
    prefixes = []
    for raw_path in requested or []:
        try:
            prefix = scanner_contract.relative_to_root(raw_path, ROOT)
        except ValueError as exc:
            fail(f"census path is unsafe: {exc}")
        if prefix and not any(
            path == prefix or path.startswith(prefix + "/") for path in all_paths
        ):
            fail(f"census path is not present in {ref}: {raw_path}")
        prefixes.append(prefix)

    def selected(path: str) -> bool:
        if not contract.is_jscpd_path(path):
            return False
        if not prefixes:
            return True
        return any(
            not prefix or path == prefix or path.startswith(prefix + "/")
            for prefix in prefixes
        )

    return sorted(path for path in all_paths if selected(path))


def _empty_report() -> dict[str, Any]:
    return {
        "duplicates": [],
        "statistics": {
            "total": {
                "clones": 0,
                "sources": 0,
                "duplicatedLines": 0,
                "lines": 0,
                "percentage": 0.0,
            }
        },
    }


def _archive_relative(member: tarfile.TarInfo) -> PurePosixPath:
    if "\x00" in member.name:
        fail(f"git archive contains NUL in path {member.name!r}")
    normalized = member.name.replace("\\", "/")
    relative = PurePosixPath(normalized)
    windows = PureWindowsPath(normalized)
    unsafe = (
        not relative.parts
        or relative == PurePosixPath(".")
        or relative.is_absolute()
        or windows.is_absolute()
        or windows.drive
        or ".." in relative.parts
    )
    if unsafe:
        fail(f"git archive contains unsafe path {member.name!r}")
    return relative


def _validate_archive_kind(member: tarfile.TarInfo) -> None:
    if member.issym() or member.islnk():
        # Do not let a tracked symlink make the scanner read outside its
        # temporary snapshot.  This is an explicit environment failure,
        # never a reason to silently scan less content.
        fail(f"git archive contains unsupported symlink {member.name!r}")
    if not (member.isdir() or member.isreg()):
        fail(f"git archive contains unsupported entry {member.name!r}")


def _validate_archive_destination(
    relative: PurePosixPath, member: tarfile.TarInfo, destination: Path
) -> None:
    candidate = (destination / Path(*relative.parts)).resolve(strict=False)
    try:
        candidate.relative_to(destination.resolve(strict=False))
    except ValueError:
        fail(f"git archive path escapes destination: {member.name!r}")


def safe_extract_archive(archive: tarfile.TarFile, destination: Path) -> None:
    for member in archive:
        relative = _archive_relative(member)
        _validate_archive_kind(member)
        _validate_archive_destination(relative, member, destination)
        archive.extract(member, path=destination)


def materialize(ref: str, destination: Path, scratch: Path) -> None:
    try:
        destination.mkdir(parents=True, exist_ok=True)
    except (OSError, UnicodeError, ValueError) as exc:
        fail(f"could not create snapshot directory {destination}: {exc}")
    tar_path = scratch / f"{destination.name}.tar"
    try:
        with tar_path.open("wb") as stream:
            result = subprocess.run(
                ["git", "archive", "--format=tar", ref],
                cwd=str(ROOT),
                env=scanner_contract.sanitized_env(),
                stdout=stream,
                stderr=subprocess.PIPE,
                check=False,
                timeout=900,
            )
    except (OSError, subprocess.TimeoutExpired) as exc:
        fail(f"could not materialize {ref}: {exc}")
    if result.returncode != 0:
        stderr = result.stderr or b""
        if isinstance(stderr, bytes):
            stderr = stderr.decode(errors="replace")
        fail(f"git archive {ref} failed: {str(stderr)[:500]}")
    try:
        with tarfile.open(tar_path, mode="r:") as archive:
            safe_extract_archive(archive, destination)
    except (OSError, UnicodeError, RuntimeError, ValueError, tarfile.TarError) as exc:
        fail(f"could not unpack {ref}: {exc}")


def census(
    executable: str,
    contract: scanner_contract.ScannerContract,
    paths: list[str] | None = None,
) -> int:
    # Keep temporary files on the repo's real filesystem rather than a small
    # /tmp tmpfs; an EG snapshot contains thousands of tracked files.
    normalized_paths = []
    for raw_path in paths or []:
        try:
            normalized_paths.append(scanner_contract.relative_to_root(raw_path, ROOT))
        except ValueError as exc:
            fail(f"census path is unsafe: {exc}")
    with _temporary_directory(".cx-jscpd-census-", ROOT.parent) as scratch:
        snapshot = scratch / "tree"
        materialize("HEAD", snapshot, scratch)
        tracked = tracked_paths("HEAD", contract, normalized_paths)
        if not tracked:
            document = _empty_report()
        else:
            # Hand jscpd only the tracked, in-scope paths selected above.  A
            # caller may name a mixture of supported and unsupported paths;
            # forwarding the latter would let a scanner-specific walk widen
            # the census beyond the independently verified Git universe.  For
            # the full census, the archive itself is already the complete
            # tracked tree and avoids an ARG_MAX-sized file list.
            targets = (
                [snapshot / path for path in tracked]
                if normalized_paths
                else [snapshot]
            )
            document = run_jscpd(
                executable,
                contract,
                targets,
                cwd=snapshot,
            )
        report_stats(document, "census")
        print(f"jscpd gate [census]: {len(tracked)} tracked in-scope file(s)")
    print("jscpd gate [census]: advisory only; findings are not a baseline or failure")
    return 0


def _validate_base_ref(base_ref: str) -> None:
    # Resolve the ref before scanning.  A missing base is an environment
    # failure, not permission to compare against an invented empty tree.
    if not base_ref or "\x00" in base_ref or base_ref.startswith("-"):
        fail("base ref must be a non-empty Git revision, not an option")


def _resolve_commit(ref: str, label: str) -> str:
    value = git(
        "rev-parse",
        "--verify",
        "--end-of-options",
        f"{ref}^{{commit}}",
        preserve_index=False,
    ).strip()
    if not _HEX_OBJECT.fullmatch(value):
        fail(f"git returned an invalid {label} commit object id")
    return value


def _merge_tree(base_sha: str, head_sha: str) -> str:
    merged_tree = git(
        "merge-tree",
        "--write-tree",
        base_sha,
        head_sha,
        preserve_index=False,
    ).strip()
    if not _HEX_OBJECT.fullmatch(merged_tree):
        fail("git merge-tree returned an invalid tree object id")
    return merged_tree


def _scan_tree(
    executable: str,
    contract: scanner_contract.ScannerContract,
    ref: str,
    snapshot: Path,
) -> tuple[dict[str, Any], list[str]]:
    paths = tracked_paths(ref, contract)
    document = (
        run_jscpd(executable, contract, snapshot, cwd=snapshot)
        if paths
        else _empty_report()
    )
    return document, paths


def _enforce_snapshots(
    executable: str,
    contract: scanner_contract.ScannerContract,
    base_sha: str,
    merged_tree: str,
) -> tuple[set[tuple[str, str, frozenset[str]]], set[tuple[str, str, frozenset[str]]]]:
    with _temporary_directory(".cx-jscpd-enforce-", ROOT.parent) as scratch:
        before = scratch / "before"
        after = scratch / "after"
        materialize(base_sha, before, scratch)
        materialize(merged_tree, after, scratch)
        before_doc, before_paths = _scan_tree(executable, contract, base_sha, before)
        after_doc, after_paths = _scan_tree(executable, contract, merged_tree, after)
        report_stats(before_doc, "enforce-before")
        report_stats(after_doc, "enforce-after")
        print(
            f"jscpd gate [enforce]: {len(before_paths)} tracked before, "
            f"{len(after_paths)} tracked after"
        )
        return keys(before_doc, before), keys(after_doc, after)


def enforce(
    executable: str, contract: scanner_contract.ScannerContract, base_ref: str
) -> int:
    _validate_base_ref(base_ref)
    base_sha = _resolve_commit(base_ref, "base")
    head_sha = _resolve_commit("HEAD", "head")
    changed = changed_paths(base_ref, contract)
    if not changed:
        print("jscpd gate [enforce]: no changed code/config/template path")
        return 0

    merged_tree = _merge_tree(base_sha, head_sha)
    before_keys, after_keys = _enforce_snapshots(
        executable, contract, base_sha, merged_tree
    )

    new_pairs = after_keys - before_keys
    print(
        f"jscpd gate [enforce]: {len(before_keys)} pre-existing pair(s), "
        f"{len(after_keys)} after-change pair(s), {len(new_pairs)} NEW"
    )
    if not new_pairs:
        print("jscpd gate [enforce]: PASS: no new block duplication")
        return 0
    print("jscpd gate [enforce]: FAIL: new duplicate pairs:")
    for format_name, digest, paths in sorted(
        new_pairs, key=lambda item: sorted(item[2])
    ):
        ordered = sorted(paths)
        left = ordered[0]
        right = ordered[1] if len(ordered) > 1 else ordered[0]
        print(f"  [{format_name}] {left} <-> {right} (fragment {digest[:12]})")
    return 1


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="mode")
    census_parser = subparsers.add_parser(
        "census", help="full tracked-tree advisory census"
    )
    census_parser.add_argument("paths", nargs="*")
    enforce_parser = subparsers.add_parser("enforce", help="new block clone gate")
    enforce_parser.add_argument(
        "--base-ref",
        "--diff",
        dest="base_ref",
        default=os.environ.get("CX_DUP_BASE_REF", "main"),
    )
    return parser


def _uses_enforce_options(arguments: list[str]) -> bool:
    return any(
        argument in {"--base-ref", "--diff"}
        or argument.startswith("--base-ref=")
        or argument.startswith("--diff=")
        for argument in arguments
    )


def _normalise_mode_args(raw_args: list[str]) -> list[str]:
    if not raw_args:
        return ["census"]
    if raw_args[0] == "--mode" and len(raw_args) >= 2:
        return [raw_args[1], *raw_args[2:]]
    if raw_args[0].startswith("--mode="):
        return [raw_args[0].split("=", 1)[1], *raw_args[1:]]
    if raw_args[0] in {"census", "enforce", "-h", "--help"}:
        return raw_args
    if _uses_enforce_options(raw_args):
        return ["enforce", *raw_args]
    return ["census", *raw_args]


def main(argv: list[str] | None = None) -> int:
    parser = _parser()
    raw_args = list(sys.argv[1:] if argv is None else argv)
    # Keep compatibility with callers that expose the mode as an option
    # instead of a subcommand (``--mode census``/``--mode enforce``).
    args = parser.parse_args(_normalise_mode_args(raw_args))
    selected_mode = args.mode

    contract_value = config()
    executable = _resolve_jscpd(contract_value)
    _check_version(executable, contract_value)
    if selected_mode == "census":
        return census(executable, contract_value, args.paths)
    return enforce(executable, contract_value, args.base_ref)


if __name__ == "__main__":
    sys.exit(main())
