#!/usr/bin/env python3
"""Pre-commit complexity gate: NEW and WORSENED functions, at 10/15, both metrics.

WHY THIS EXISTS AND WHY IT IS NOT A RATCHET
-------------------------------------------
`check_complexity.py` enforces ABSOLUTE ceilings over a whole path. That is the
right shape for a census and for a CI job driving the number down, but it cannot
be a pre-commit hook today: this repository holds thousands of functions already
over the caps, so an absolute whole-repo gate would refuse every commit and the
only way to work would be `--no-verify` -- a gate nobody can pass is a gate
nobody runs.

So this hook scopes to the DIFF, and its rule is:

    * a function that does not exist in HEAD and is over either cap  -> FAIL
    * a function that exists in HEAD and got WORSE on either metric  -> FAIL
    * a function that exists in HEAD, is over a cap, and is unchanged -> pass

That is deliberately NOT a baseline (CX MR-11 / the workspace no-ratchet rule).
Nothing is written to disk, no count is frozen, no finding is marked "accepted",
and the REAL absolute numbers for every touched file are printed on every run.
    The comparison is recomputed live from git on each invocation, so the only
property it grants is "the tail cannot grow" -- pre-existing debt stays visible,
stays failing in the census, and must still be burned down deliberately.

    The measurement rule itself is the one `plans/complex/scripts/verify_both.py`
established from measured data: BOTH metrics, EVERY function, INCLUDING nested
children. Extraction moves complexity into the child, so a parent that now looks
clean is not the whole story.

WHAT IS COMPARED
----------------
The INDEX (`git show :path`) against HEAD (`git show HEAD:path`) -- not the
working tree. The index is what the commit will contain, so an unstaged edit can
neither hide a violation nor invent one, independently of whether pre-commit's
own stash ran.

Exit codes: 0 pass, 1 violation, 2 the gate could not run (an ENVIRONMENT fact,
never reported as a clean pass -- a gate that could not run has not found
nothing).
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from scanner_contract import (  # noqa: E402
    CCCC_MAX_COGNITIVE,
    CCCC_MAX_CYCLOMATIC,
    CCCC_SUPPORTED_SUFFIXES,
    ScannerContract,
    ScannerContractError,
    exact_version,
    load_contract,
    relative_to_root,
    resolve_binary,
    sanitized_env,
    sanitized_git_env,
)

DEFAULT_MAX_CYCLOMATIC = CCCC_MAX_CYCLOMATIC
DEFAULT_MAX_COGNITIVE = CCCC_MAX_COGNITIVE

#: Extensions cccc 1.6.0 actually dispatches. A file outside this set is skipped
#: rather than handed to cccc, so an unsupported type cannot look like "0
#: functions, clean". Keep this list aligned with the bundled cccc 1.6.0
#: registry; in particular, cccc has no C++, C#, or Scala front-end.
SUPPORTED_SUFFIXES = CCCC_SUPPORTED_SUFFIXES


def _fail_env(msg: str) -> None:
    print(f"complexity(staged): CANNOT RUN: {msg}", file=sys.stderr)
    raise SystemExit(2)


def _resolve_cccc() -> str:
    """Find cccc WITHOUT consulting any package index.

    A hook that resolves its tool from an index at hook time is how a previous
    fleet sweep shipped a gate that could not pass anywhere. Local paths only.
    """
    try:
        return resolve_binary("cccc", "CCCC_BIN")
    except FileNotFoundError as exc:
        _fail_env(str(exc))


def _check_cccc_version() -> str:
    """Reject a missing or drifted CCCC before measuring any source.

    CCCC's JSON shape and metric names are part of the gate contract.  A
    different binary can silently change both, so treating ``--version`` as
    advisory would turn a green hook into an unreviewed algorithm upgrade.
    """

    try:
        contract = load_contract()
        executable = resolve_binary("cccc", "CCCC_BIN")
        exact_version(executable, f"cccc {contract.cccc_version}")
    except (FileNotFoundError, ScannerContractError, RuntimeError) as exc:
        _fail_env(str(exc))
    return executable


def _run_git(
    *args: str, cwd: str | None = None, preserve_index: bool = False
) -> subprocess.CompletedProcess:
    """Run git with the hook's ambient environment made harmless.

    git exports GIT_DIR / GIT_INDEX_FILE / GIT_WORK_TREE into EVERY hook
    subprocess. Inherited blindly they silently re-root path resolution, which
    is how ~20 copied gate helpers once measured an empty universe and reported
    a confident clean verdict. We strip every GIT_* selector and retain only
    GIT_INDEX_FILE for calls that must read the staged index. We always run from
    the resolved toplevel with repo-relative paths, never `git -C <subdir>`.
    """
    try:
        return subprocess.run(
            ["git", *args],
            cwd=cwd or str(Path(__file__).resolve().parent.parent),
            env=sanitized_git_env(preserve_index=preserve_index),
            capture_output=True,
            text=True,
            timeout=120,
            check=False,
        )
    except (OSError, UnicodeError, subprocess.TimeoutExpired) as exc:
        _fail_env(f"could not execute git: {exc}")


def _git(
    *args: str, cwd: str | None = None, preserve_index: bool = False
) -> subprocess.CompletedProcess:
    return _run_git(*args, cwd=cwd, preserve_index=preserve_index)


def _validated_repo_root(result: subprocess.CompletedProcess) -> str:
    if (
        result.returncode != 0
        or not isinstance(result.stdout, str)
        or not result.stdout.strip()
    ):
        _fail_env(f"not inside a work tree: {(result.stderr or '').strip()[:200]}")
    return result.stdout.strip()


def repo_root() -> str:
    return _validated_repo_root(_git("rev-parse", "--show-toplevel"))


def _staged_contract(contract: ScannerContract | None) -> ScannerContract:
    if contract is not None:
        return contract
    try:
        return load_contract()
    except ScannerContractError as exc:
        _fail_env(str(exc))


def _staged_output(result: subprocess.CompletedProcess) -> str:
    if result.returncode != 0:
        _fail_env(f"git diff --cached failed: {(result.stderr or '').strip()[:200]}")
    if not isinstance(result.stdout, str):
        _fail_env("git diff --cached returned no text output")
    return result.stdout


def _is_supported_staged_path(rel: str, contract: ScannerContract) -> bool:
    return Path(rel).suffix.lower() in SUPPORTED_SUFFIXES and not contract.is_excluded(
        rel
    )


def _select_staged_files(raw: str, root: str, contract: ScannerContract) -> list[str]:
    result = []
    try:
        for value in raw.split("\x00"):
            if not value:
                continue
            rel = relative_to_root(value, Path(root))
            if _is_supported_staged_path(rel, contract):
                result.append(rel)
    except ValueError as exc:
        _fail_env(f"git reported an unsafe staged path: {exc}")
    return sorted(set(result))


def staged_files(root: str, contract: ScannerContract | None = None) -> list[str]:
    """Repo-relative paths staged as Added/Copied/Modified/Renamed."""
    contract = _staged_contract(contract)
    r = _git(
        "diff",
        "--cached",
        "--name-only",
        "-z",
        "--diff-filter=ACMR",
        cwd=root,
        preserve_index=True,
    )
    return _select_staged_files(_staged_output(r), root, contract)


def _head_path_exists(root: str, rel: str) -> bool:
    result = _git(
        "ls-tree",
        "-r",
        "--name-only",
        "-z",
        "HEAD",
        "--",
        rel,
        cwd=root,
        preserve_index=True,
    )
    if result.returncode != 0:
        _fail_env(f"git ls-tree failed: {(result.stderr or '').strip()[:300]}")
    if not isinstance(result.stdout, str):
        _fail_env("git ls-tree returned no text output")
    return rel in result.stdout.split("\x00")


def _blob_is_absent(root: str, rev_path: str, allow_missing: bool) -> bool:
    if not allow_missing or not rev_path.startswith("HEAD:"):
        return False
    rel = rev_path.removeprefix("HEAD:")
    return not _head_path_exists(root, rel)


def _close_fd(fd: int) -> None:
    if fd < 0:
        return
    try:
        os.close(fd)
    except OSError:
        pass


def _materialize_blob(text: str, suffix: str, tmp: str, rev_path: str) -> str:
    fd = -1
    try:
        fd, path = tempfile.mkstemp(suffix=suffix, dir=tmp)
        with os.fdopen(fd, "w", encoding="utf-8", errors="replace") as fh:
            fd = -1
            fh.write(text)
    except (OSError, UnicodeError) as exc:
        _close_fd(fd)
        _fail_env(f"could not materialize {rev_path}: {exc}")
    return path


def _blob_result(
    root: str,
    rev_path: str,
    suffix: str,
    tmp: str,
    allow_missing: bool,
    result: subprocess.CompletedProcess,
) -> str | None:
    if result.returncode != 0:
        if _blob_is_absent(root, rev_path, allow_missing):
            return None
        _fail_env(f"git show {rev_path} failed: {(result.stderr or '').strip()[:300]}")
    if not isinstance(result.stdout, str):
        _fail_env(f"git show {rev_path} returned no text output")
    return _materialize_blob(result.stdout, suffix, tmp, rev_path)


def _blob(
    root: str,
    rev_path: str,
    suffix: str,
    tmp: str,
    *,
    allow_missing: bool = False,
) -> str | None:
    """Materialize a git blob, distinguishing absence from git failure."""
    result = _git("show", rev_path, cwd=root, preserve_index=True)
    return _blob_result(root, rev_path, suffix, tmp, allow_missing, result)


def _function_metrics(fn: dict) -> tuple[int, int]:
    if not isinstance(fn, dict):
        _fail_env("cccc returned a function that is not an object")
    for field in ("name", "cyclomatic", "cognitive"):
        if field not in fn:
            _fail_env(f"cccc function is missing {field}")
    if not isinstance(fn["name"], str) or not fn["name"]:
        _fail_env("cccc function has an invalid name")
    values = (fn["cyclomatic"], fn["cognitive"])
    if any(
        isinstance(value, bool) or not isinstance(value, int) or value < 0
        for value in values
    ):
        _fail_env(f"cccc function {fn['name']!r} has invalid complexity metrics")
    return values


def _function_children(fn: dict) -> list:
    # cccc omits empty children arrays via serde's
    # ``skip_serializing_if = "Vec::is_empty"``; absence is the valid leaf
    # representation, while an explicitly supplied value must still be a list.
    children = fn.get("children", [])
    if not isinstance(children, list):
        _fail_env(f"cccc function {fn['name']!r} has invalid children")
    return children


def _function_details(fn: dict, prefix: str) -> tuple[str, tuple[int, int], list]:
    if not isinstance(fn, dict):
        _fail_env("cccc returned a function that is not an object")
    if not isinstance(fn.get("name"), str) or not fn["name"]:
        _fail_env("cccc function has an invalid name")
    name = f"{prefix}{fn['name']}"
    return name, _function_metrics(fn), _function_children(fn)


def _walk(fn: dict, prefix: str, out: dict) -> None:
    """Collect a function AND its nested children, keeping EVERY row.

    cccc reports nested functions under ``children`` and qualified names can
    collide, so every metric row is retained in a list.
    """
    name, values, children = _function_details(fn, prefix)
    out.setdefault(name, []).append(values)
    for kid in children:
        _walk(kid, f"{name}.", out)


def _run_cccc(path: str) -> str:
    exe = _resolve_cccc()
    try:
        r = subprocess.run(
            [exe, path, "--no-config", "--min", "0"],
            cwd=str(Path(__file__).resolve().parent.parent),
            env=sanitized_env(),
            capture_output=True,
            text=True,
            timeout=600,
            check=False,
        )
    except subprocess.TimeoutExpired:
        _fail_env(f"cccc timed out on {path}")
    except (OSError, UnicodeError) as exc:
        _fail_env(f"could not execute {exe}: {exc}")
    if not isinstance(r.stdout, str):
        _fail_env(f"cccc returned no text output for {path}")
    if r.returncode != 0:
        _fail_env(f"cccc exited {r.returncode}: {(r.stderr or '').strip()[:300]}")
    if not r.stdout.strip():
        _fail_env(f"cccc produced no output for {path}; refusing to call that clean")
    return r.stdout


def _parse_cccc_document(raw: str, path: str) -> dict:
    try:
        doc = json.loads(raw)
    except json.JSONDecodeError as exc:
        _fail_env(f"cccc output was not JSON: {exc}")
    if not isinstance(doc, dict):
        _fail_env(f"cccc output for {path} is not a JSON object")
    return doc


def _validated_measurement_files(doc: dict, path: str) -> list:
    files = doc.get("files")
    if not isinstance(files, list) or not files:
        _fail_env(f"cccc output for {path} has no files array")
    summary = doc.get("summary")
    if not isinstance(summary, dict):
        _fail_env(f"cccc output for {path} has an invalid summary")
    parse_count = summary.get("parse_error_count")
    if (
        isinstance(parse_count, bool)
        or not isinstance(parse_count, int)
        or parse_count < 0
    ):
        _fail_env(f"cccc output for {path} has an invalid parse error count")
    if parse_count:
        _fail_env(f"cccc reported {parse_count} parse error(s) for {path}")
    return files


def _validated_file_functions(file_report: object, path: str) -> list:
    if not isinstance(file_report, dict):
        _fail_env(f"cccc output for {path} has an invalid file report")
    functions = file_report.get("functions")
    if not isinstance(functions, list):
        _fail_env(f"cccc output for {path} has a file without functions")
    parse_errors = file_report.get("parse_errors", [])
    if not isinstance(parse_errors, list) or any(
        not isinstance(error, str) or not error for error in parse_errors
    ):
        _fail_env(f"cccc output for {path} has invalid parse errors")
    if parse_errors:
        _fail_env(f"cccc reported parse errors for {path}")
    return functions


def _measurement_rows(raw: str, path: str) -> dict[str, list[tuple[int, int]]]:
    doc = _parse_cccc_document(raw, path)
    files = _validated_measurement_files(doc, path)
    out: dict[str, list[tuple[int, int]]] = {}
    for file_report in files:
        functions = _validated_file_functions(file_report, path)
        try:
            for fn in functions:
                _walk(fn, "", out)
        except RecursionError as exc:
            _fail_env(f"cccc function tree is too deeply nested for {path}: {exc}")
    return out


def measure(path: str) -> dict[str, list[tuple[int, int]]]:
    """{qualified_name: [(cyclomatic, cognitive), ...]} for one file.

    A list per name, not a pair -- names collide. See `_walk`.
    """
    return _measurement_rows(_run_cccc(path), path)


def _new_findings(name: str, rows: list, max_cyc: int, max_cog: int) -> list:
    """Rows for a name absent from HEAD: each is judged on the caps alone."""
    return [
        ("NEW", name, (0, 0), (cyc, cog))
        for cyc, cog in rows
        if cyc > max_cyc or cog > max_cog
    ]


def _flatten(measured: dict) -> list[tuple[int, int]]:
    """Every (cyclomatic, cognitive) row across every name, duplicates included."""
    return [row for rows in measured.values() for row in rows]


def _worst(rows: list) -> tuple[int, int]:
    """The worst cyclomatic and worst cognitive carried by one name."""
    return max(c for c, _ in rows), max(g for _, g in rows)


def _over_cap(row: tuple[int, int], max_cyc: int, max_cog: int) -> bool:
    return row[0] > max_cyc or row[1] > max_cog


def _new_duplicate_findings(
    name: str,
    prior: list[tuple[int, int]],
    rows: list[tuple[int, int]],
    max_cyc: int = DEFAULT_MAX_CYCLOMATIC,
    max_cog: int = DEFAULT_MAX_COGNITIVE,
) -> list:
    """Report over-cap rows added to a name that already existed in HEAD.

    Qualified names are not unique: cccc can emit two ``submit`` rows at one
    level.  Compare the multiplicity of over-cap rows as well as their worst
    value, so a new row below an older worst row cannot disappear behind it.
    If the count of over-cap rows rose, the lowest after-change rows are the
    conservative candidates for the newly added functions.
    """
    prior_count = sum(_over_cap(row, max_cyc, max_cog) for row in prior)
    after_rows = sorted(
        (row for row in rows if _over_cap(row, max_cyc, max_cog)),
        key=lambda row: (row[0], row[1]),
    )
    added_count = len(after_rows) - prior_count
    if added_count <= 0:
        return []
    return [("NEW", name, (0, 0), row) for row in after_rows[:added_count]]


def _regression(
    name: str,
    prior: list[tuple[int, int]],
    rows: list[tuple[int, int]],
    max_cyc: int = DEFAULT_MAX_CYCLOMATIC,
    max_cog: int = DEFAULT_MAX_COGNITIVE,
) -> list:
    """Find multiplicity additions and worst-value regressions for one name.

    The worst-value comparison remains conservative for changed rows, while
    `_new_duplicate_findings` covers the distinct case where a qualified name
    gains another over-cap function without changing its existing worst row.
    """
    findings = _new_duplicate_findings(name, prior, rows, max_cyc, max_cog)
    before = _worst(prior)
    after = _worst(rows)
    if after[0] > before[0] or after[1] > before[1]:
        findings.append(("WORSE", name, before, after))
    return findings


def judge(
    before: dict[str, list[tuple[int, int]]],
    after: dict[str, list[tuple[int, int]]],
    max_cyc: int,
    max_cog: int,
) -> list[tuple[str, str, tuple[int, int], tuple[int, int]]]:
    """Findings as (kind, function, before, after). Empty means clean.

    NEW = absent from HEAD and over a cap. WORSE = present in HEAD and up on
    either metric. A pre-existing over-cap function left alone yields nothing --
    the module docstring explains why that is scope, not a baseline.
    """
    findings: list = []
    for name, rows in sorted(after.items()):
        prior = before.get(name)
        if prior is None:
            findings.extend(_new_findings(name, rows, max_cyc, max_cog))
        else:
            findings.extend(_regression(name, prior, rows, max_cyc, max_cog))
    return findings


def _report_file(
    rel: str,
    after: dict[str, list[tuple[int, int]]],
    max_cyc: int = DEFAULT_MAX_CYCLOMATIC,
    max_cog: int = DEFAULT_MAX_COGNITIVE,
) -> None:
    """Print the REAL absolute numbers for a touched file, on every run.

    The no-ratchet rule in code: pre-existing debt in a file you touched stays on
    screen even though this hook does not fail on it.
    """
    if not after:
        return
    flat = _flatten(after)
    worst_cyc, worst_cog = _worst(flat)
    over = sum(1 for c, g in flat if c > max_cyc or g > max_cog)
    print(
        f"  {rel}: {len(flat)} fn, worst cyc {worst_cyc}, worst cog {worst_cog}, "
        f"{over} already over {max_cyc}/{max_cog}"
    )


def _print_findings(rel: str, findings: list, max_cyc: int, max_cog: int) -> None:
    for kind, name, prior, now in findings:
        if kind == "NEW":
            flags = []
            if now[0] > max_cyc:
                flags.append(f"cyc {now[0]}")
            if now[1] > max_cog:
                flags.append(f"COG {now[1]}")
            print(f"  NEW    over cap ({', '.join(flags)})   {name}@{rel}")
        else:
            print(
                f"  WORSE  cyc {prior[0]}->{now[0]}  cog {prior[1]}->{now[1]}"
                f"   {name}@{rel}"
            )


def check_file(root: str, rel: str, tmp: str, max_cyc: int, max_cog: int) -> list:
    """Findings for one staged file."""
    suffix = Path(rel).suffix
    after_path = _blob(root, f":{rel}", suffix, tmp)
    if after_path is None:
        _fail_env(f"staged blob {rel} could not be materialized")
    after = measure(after_path)
    _report_file(rel, after, max_cyc, max_cog)
    before_path = _blob(root, f"HEAD:{rel}", suffix, tmp, allow_missing=True)
    before = measure(before_path) if before_path else {}
    findings = judge(before, after, max_cyc, max_cog)
    _print_findings(rel, findings, max_cyc, max_cog)
    return findings


ADVICE = """
Split the function into named parts, or replace the branching with a dict
dispatch table. Measured on real shapes with cccc 1.6.0:

    dict dispatch table          cyclomatic  2   cognitive  1   <- wins BOTH
    extraction (parent)                      2              1   <- wins BOTH...
      ...the extracted CHILD                 5             10   <- ...child inherits
    flat if/elif chain (6 arms)              7              7
    deep nesting                             6             15   <- cognitive killer

Flattening nesting into a longer chain trades one metric for the other and is
not a fix. Recurse until every function you created is under BOTH caps.

Do NOT raise a threshold and do NOT add a suppression comment to pass this --
an in-line suppression is a one-line baseline. If the complexity is genuinely
irreducible, record a time-boxed entry with an owner in scripts/gate_deferrals.tsv.
"""


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.parse_args()

    root = repo_root()
    _check_cccc_version()
    files = staged_files(root)
    if not files:
        print("complexity(staged): OK: no staged file in a cccc-supported language")
        return 0

    print(
        f"complexity(staged): {len(files)} file(s), caps cyclomatic "
        f"{DEFAULT_MAX_CYCLOMATIC} / cognitive {DEFAULT_MAX_COGNITIVE}, both enforced"
    )
    findings: list = []
    with tempfile.TemporaryDirectory(prefix="cx-staged-") as tmp:
        for rel in files:
            findings.extend(
                check_file(
                    root,
                    rel,
                    tmp,
                    DEFAULT_MAX_CYCLOMATIC,
                    DEFAULT_MAX_COGNITIVE,
                )
            )

    if not findings:
        print("\ncomplexity(staged): OK: nothing new over a cap, nothing regressed")
        return 0
    new = sum(1 for f in findings if f[0] == "NEW")
    worse = len(findings) - new
    print(
        f"\ncomplexity(staged): FAIL: {new} new function(s) over a cap, "
        f"{worse} pre-existing function(s) made worse"
    )
    print(ADVICE)
    return 1


if __name__ == "__main__":
    sys.exit(main())
