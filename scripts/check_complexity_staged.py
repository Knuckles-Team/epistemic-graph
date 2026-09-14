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

TERMS OF ACCEPTANCE FOR THE CYCLOMATIC CAP
------------------------------------------
One class of function is exempt from the CYCLOMATIC cap only -- never from the
cognitive cap -- because for it the cyclomatic number measures the wrong thing:
a flat, genuinely exhaustive `match`. `scripts/rust_exhaustive_match.py` states
the rule and the measurement behind it. Membership is recomputed from the
STAGED SOURCE on every run, there is no list of files or functions anywhere,
and the rule is strictly tighter than the absence of a rule it replaces: it
newly exposes every high-cyclomatic dispatcher that ends in a catch-all arm,
which is NOT exhaustive and is therefore ordinary debt. Exempt functions are
counted on screen for every touched file, so the exemption is visible, not
silent.
    The comparison is recomputed live from git on each invocation, so the only
property it grants is "the tail cannot grow" -- pre-existing debt stays visible,
stays failing in the census, and must still be burned down deliberately.

    The measurement rule itself is the one `plans/complex/scripts/verify_both.py`
established from measured data: BOTH metrics, EVERY function, INCLUDING nested
children. Extraction moves complexity into the child, so a parent that now looks
clean is not the whole story.

EXEMPTION STATUS IS NOT PART OF THE COMPARED VALUE
---------------------------------------------------
An earlier version of this gate compared a "graded" cyclomatic number that was
hard-zeroed to 0 for an exempt row before the before/after comparison ran. That
folded EXEMPTION STATUS into the METRIC being compared: a function that started
exempt-over-cap (graded 0) and was simplified below the cap -- ceasing to need
the exemption at all, since `exhaustive_dispatch_exempt` never grants it to a
function at or under the cap -- read as its full raw value appearing from
nowhere (0 -> 9), which the comparison called a regression. Simplifying a
function until it no longer needs the exemption was, perversely, the one thing
this gate could not tell from making it worse.

The fix compares RAW metrics on both sides and treats exemption as a
CLASSIFIER, not a rewrite of the number being classified:

  * a function that is over a cap and NOT exempt on the new side always fails,
    new or pre-existing, exactly the ordinary case;
  * a function that was over a cap and exempt at base, and is at or under the
    cap on the new side (so no longer exempt, because the rule never exempts
    an at-or-under-cap function) -- always passes. This is the transition the
    exemption exists to allow: the exhaustive match got simpler, not worse;
  * an exempt function's cyclomatic axis is judged on its RESIDUAL (measured
    cyclomatic minus its match-arm count) rather than being erased to 0. Adding
    arms to an exhaustive dispatch leaves the residual alone (the whole point
    of keeping the match exhaustive is that arms are free), so that alone never
    reads as a regression. Any OTHER growth -- more branching inside an arm's
    body, more decision points outside the match -- raises the residual and
    fails exactly like ordinary debt would. Cognitive complexity is never
    graded and never exempt on either side.

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
from typing import NamedTuple

sys.path.insert(0, str(Path(__file__).resolve().parent))

from rust_exhaustive_match import (  # noqa: E402
    dispatch_shape,
    exhaustive_dispatch_exempt,
)
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

#: Rust is the only language the exhaustive-dispatch rule can speak about; a
#: Python or JavaScript function has no `match` arms for rustc to check, so it
#: is never exempt. Measured on this tree: 24 of the cyclomatic-only over-cap
#: functions are Python or JavaScript, and all 24 stay in the backlog.
RUST_SUFFIX = ".rs"


class Metrics(NamedTuple):
    """One measured function row, plus whether the dispatch rule accepts it.

    ``exempt`` and ``residual`` are never read from disk and never persisted.
    Both are recomputed from the source under measurement on every run:
    ``exempt`` by ``rust_exhaustive_match.exhaustive_dispatch_exempt``,
    ``residual`` (measured cyclomatic minus the function's match-arm count)
    from ``rust_exhaustive_match.dispatch_shape`` -- only when ``exempt`` is
    True, ``None`` otherwise, since a non-exempt row is judged on its raw
    cyclomatic and has no discounted quantity.
    """

    cyclomatic: int
    cognitive: int
    line: int
    exempt: bool
    residual: int | None = None

    def __eq__(self, other: object) -> bool:
        """Keep the pre-residual four-tuple shape usable by callers.

        ``Metrics`` is consumed directly by the scanner-wrapper tests and by
        small repository-local helpers, which compare parser rows with the
        original four-field tuple.  Measured rows carry the fifth field; an
        old four-tuple comparison must ignore only that derived field.  A
        comparison with another ``Metrics`` (or a five-tuple) remains the
        ordinary tuple comparison.
        """
        if isinstance(other, tuple) and len(other) == 4:
            return tuple.__eq__(self[:4], other)
        return tuple.__eq__(self, other)

    def __hash__(self) -> int:
        """Match the legacy four-field hash kept by ``__eq__`` above."""
        return hash(self[:4])

    @property
    def effective_cyclomatic(self) -> int:
        """The cyclomatic value this gate compares between two rows.

        Raw for a non-exempt row. For an accepted exhaustive dispatcher, the
        RESIDUAL -- cyclomatic minus its match arms -- so that adding arms,
        the whole point of keeping the match exhaustive, does not by itself
        register as growth, while any OTHER branching added to the function
        still does. For source-measured rows this is never zeroed: an exempt
        row's "effective" value is real, just discounted, which is what makes
        an exempt-over-cap function that drops below the cap comparable to
        nothing at all rather than reading as a jump from a fabricated 0.
        """
        if self.exempt:
            # Rows assembled by older callers have no source-derived residual.
            # Preserve their former accepted-dispatch behavior; every row from
            # ``measure`` is stamped with a real residual before it reaches the
            # gate and therefore takes the corrected raw-metric path above.
            return self.residual if self.residual is not None else 0
        return self.cyclomatic


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


def _function_metrics(fn: dict) -> Metrics:
    """One validated row. ``line`` is required: the dispatch rule reads source.

    A row without a usable start line cannot be classified, and an
    unclassifiable row must never be exempt, so a missing or nonsensical line
    is an environment failure rather than a silently unclassifiable row.
    """
    if not isinstance(fn, dict):
        _fail_env("cccc returned a function that is not an object")
    for field in ("name", "cyclomatic", "cognitive", "line"):
        if field not in fn:
            _fail_env(f"cccc function is missing {field}")
    if not isinstance(fn["name"], str) or not fn["name"]:
        _fail_env("cccc function has an invalid name")
    values = (fn["cyclomatic"], fn["cognitive"], fn["line"])
    if any(
        isinstance(value, bool) or not isinstance(value, int) or value < 0
        for value in values
    ):
        _fail_env(f"cccc function {fn['name']!r} has invalid complexity metrics")
    return Metrics(fn["cyclomatic"], fn["cognitive"], fn["line"], False)


def _function_children(fn: dict) -> list:
    # cccc omits empty children arrays via serde's
    # ``skip_serializing_if = "Vec::is_empty"``; absence is the valid leaf
    # representation, while an explicitly supplied value must still be a list.
    children = fn.get("children", [])
    if not isinstance(children, list):
        _fail_env(f"cccc function {fn['name']!r} has invalid children")
    return children


def _function_details(fn: dict, prefix: str) -> tuple[str, Metrics, list]:
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


def _measurement_rows(raw: str, path: str) -> dict[str, list[Metrics]]:
    doc = _parse_cccc_document(raw, path)
    files = _validated_measurement_files(doc, path)
    out: dict[str, list[Metrics]] = {}
    for file_report in files:
        functions = _validated_file_functions(file_report, path)
        try:
            for fn in functions:
                _walk(fn, "", out)
        except RecursionError as exc:
            _fail_env(f"cccc function tree is too deeply nested for {path}: {exc}")
    return out


def _rust_source(path: str) -> str | None:
    """The Rust text of a measured blob, or None when the rule cannot apply.

    None for a non-Rust file and for any file that cannot be read: both make
    every row non-exempt, which is the fail-closed direction.
    """
    if Path(path).suffix.lower() != RUST_SUFFIX:
        return None
    try:
        return Path(path).read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None


def _residual(source: str | None, row: Metrics) -> int | None:
    """Cyclomatic minus match-arm count, for a row already known exempt.

    ``exhaustive_dispatch_exempt`` already proved ``dispatch_shape`` returns a
    usable shape with at least one arm for this exact row -- that is one of
    its four conditions -- so a ``None`` or zero-arm shape here would mean the
    two functions disagree, and this fails closed (no residual) rather than
    dividing by an assumption.
    """
    shape = dispatch_shape(source, row.line) if source is not None else None
    if shape is None or shape.arms <= 0:
        return None
    return row.cyclomatic - shape.arms


def _stamp_row(row: Metrics, source: str | None, max_cyc: int, max_cog: int) -> Metrics:
    """One row with its exhaustive-dispatch verdict, and residual when exempt.

    The residual (see ``Metrics.effective_cyclomatic``) is what lets the
    regression comparison tell "grew because arms were added" from "grew for
    any other reason" instead of erasing the row to 0.
    """
    exempt = exhaustive_dispatch_exempt(
        source, row.line, row.cyclomatic, row.cognitive, max_cyc, max_cog
    )
    residual = _residual(source, row) if exempt else None
    return row._replace(exempt=exempt, residual=residual)


def _graded(
    rows: dict[str, list[Metrics]],
    source: str | None,
    max_cyc: int,
    max_cog: int,
) -> dict[str, list[Metrics]]:
    """Stamp every row with the exhaustive-dispatch verdict for its source."""
    return {
        name: [_stamp_row(row, source, max_cyc, max_cog) for row in measured]
        for name, measured in rows.items()
    }


def measure(
    path: str,
    max_cyc: int = DEFAULT_MAX_CYCLOMATIC,
    max_cog: int = DEFAULT_MAX_COGNITIVE,
) -> dict[str, list[Metrics]]:
    """{qualified_name: [Metrics, ...]} for one file, dispatch rule applied.

    A list per name, not a single row -- names collide. See `_walk`.
    """
    rows = _measurement_rows(_run_cccc(path), path)
    return _graded(rows, _rust_source(path), max_cyc, max_cog)


def _new_findings(name: str, rows: list[Metrics], max_cyc: int, max_cog: int) -> list:
    """Rows for a name absent from HEAD: each is judged on the caps alone."""
    return [
        ("NEW", name, (0, 0), (row.cyclomatic, row.cognitive))
        for row in rows
        if _over_cap(row, max_cyc, max_cog)
    ]


def _flatten(measured: dict) -> list[Metrics]:
    """Every measured row across every name, duplicates included."""
    return [row for rows in measured.values() for row in rows]


def _worst_cognitive(rows: list[Metrics]) -> int:
    """The worst raw cognitive value carried by one name.

    Cognitive complexity is never graded and never exempt, on either side of
    the comparison, so this is always the raw measurement.
    """
    return max(row.cognitive for row in rows)


def _worst_raw_cyclomatic_row(rows: list[Metrics]) -> Metrics:
    """The row with the worst RAW cyclomatic value carried by one name.

    Raw, not effective: this selects WHICH row anchors the cyclomatic
    comparison, and that selection must not itself already be discounted by
    exemption, or a low-residual exempt row could hide a worse non-exempt one
    sharing the same qualified name.
    """
    return max(rows, key=lambda row: row.cyclomatic)


def _cyclomatic_regressed(
    prior: list[Metrics], rows: list[Metrics], max_cyc: int
) -> tuple[bool, int, int]:
    """Whether the cyclomatic axis regressed, plus the raw (before, after) to
    report.

    Compares the RAW-worst row on each side, per the terms of acceptance:

      * if the row was exempt-over-cap at base and is at-or-under the cap now
        (so necessarily not exempt now -- the rule never exempts an
        at-or-under-cap function), this bypasses the value comparison
        entirely and always passes: it is the transition the exemption exists
        to allow, and comparing a raw value against a discounted one would be
        comparing different units;
      * otherwise, compare ``effective_cyclomatic`` on both sides: raw for a
        non-exempt row, residual (arms discounted) for an exempt one. Growth
        purely from added match arms leaves an exempt row's residual alone;
        any other growth raises it and is judged exactly like ordinary debt.
    """
    before = _worst_raw_cyclomatic_row(prior)
    after = _worst_raw_cyclomatic_row(rows)
    # Keep the diagnostic shape emitted for legacy, hand-built Metrics rows.
    # Real ``measure`` rows always carry a residual and report their raw value;
    # a caller that supplies only the historical four fields has no raw-source
    # residual to support the corrected diagnostic.
    reported_before = (
        before.effective_cyclomatic
        if before.exempt and before.residual is None
        else before.cyclomatic
    )
    if before.exempt and after.cyclomatic <= max_cyc:
        return False, reported_before, after.cyclomatic
    regressed = after.effective_cyclomatic > before.effective_cyclomatic
    return regressed, reported_before, after.cyclomatic


def _cyclomatic_over_cap(row: Metrics, max_cyc: int) -> bool:
    """Raw cyclomatic over cap -- discounted to False for an exempt row, since
    the exemption exists precisely to accept that axis for it."""
    return row.cyclomatic > max_cyc and not row.exempt


def _over_cap(row: Metrics, max_cyc: int, max_cog: int) -> bool:
    """True when a row fails on its own terms, independent of any history."""
    return _cyclomatic_over_cap(row, max_cyc) or row.cognitive > max_cog


def _new_duplicate_findings(
    name: str,
    prior: list[Metrics],
    rows: list[Metrics],
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
        key=lambda row: (row.cyclomatic, row.cognitive),
    )
    # A row can also cross a cap without any row being ADDED -- for instance a
    # dispatcher that grew a catch-all arm and so left the accepted class. That
    # is a worsening of an existing row, which `_cyclomatic_regressed` and
    # `_worst_cognitive` report; counting it here as well would report one
    # function twice.
    added_count = min(len(after_rows) - prior_count, len(rows) - len(prior))
    if added_count <= 0:
        return []
    return [
        ("NEW", name, (0, 0), (row.cyclomatic, row.cognitive))
        for row in after_rows[:added_count]
    ]


def _regression(
    name: str,
    prior: list[Metrics],
    rows: list[Metrics],
    max_cyc: int = DEFAULT_MAX_CYCLOMATIC,
    max_cog: int = DEFAULT_MAX_COGNITIVE,
) -> list:
    """Find multiplicity additions and worst-value regressions for one name.

    The worst-value comparison remains conservative for changed rows, while
    `_new_duplicate_findings` covers the distinct case where a qualified name
    gains another over-cap function without changing its existing worst row.
    """
    findings = _new_duplicate_findings(name, prior, rows, max_cyc, max_cog)
    cyc_regressed, cyc_before, cyc_after = _cyclomatic_regressed(prior, rows, max_cyc)
    cog_before = _worst_cognitive(prior)
    cog_after = _worst_cognitive(rows)
    if cyc_regressed or cog_after > cog_before:
        findings.append(
            ("WORSE", name, (cyc_before, cog_before), (cyc_after, cog_after))
        )
    return findings


def judge(
    before: dict[str, list[Metrics]],
    after: dict[str, list[Metrics]],
    max_cyc: int,
    max_cog: int,
) -> list[tuple[str, str, tuple[int, int], tuple[int, int]]]:
    """Findings as (kind, function, before, after). Empty means clean.

    NEW = absent from HEAD and over a cap. WORSE = present in HEAD and up on
    either metric. A pre-existing over-cap function left alone yields nothing --
    the module docstring explains why that is scope, not a baseline. A function
    the exhaustive-dispatch rule accepts is judged on cognitive complexity
    alone; it is still counted and printed by `_report_file`.
    """
    findings: list = []
    for name, rows in sorted(after.items()):
        prior = before.get(name)
        if prior is None:
            findings.extend(_new_findings(name, rows, max_cyc, max_cog))
        else:
            findings.extend(_regression(name, prior, rows, max_cyc, max_cog))
    return findings


def _file_totals(
    flat: list[Metrics], max_cyc: int, max_cog: int
) -> tuple[int, int, int, int]:
    """(worst cyclomatic, worst cognitive, over-cap count, accepted count).

    The first three are the RAW measurement -- never the graded one -- because
    this is the line that keeps pre-existing debt on screen.
    """
    return (
        max(row.cyclomatic for row in flat),
        max(row.cognitive for row in flat),
        sum(1 for row in flat if row.cyclomatic > max_cyc or row.cognitive > max_cog),
        sum(1 for row in flat if row.exempt),
    )


def _report_file(
    rel: str,
    after: dict[str, list[Metrics]],
    max_cyc: int = DEFAULT_MAX_CYCLOMATIC,
    max_cog: int = DEFAULT_MAX_COGNITIVE,
) -> None:
    """Print the REAL absolute numbers for a touched file, on every run.

    The no-ratchet rule in code: pre-existing debt in a file you touched stays
    on screen even though this hook does not fail on it -- and so does the
    count the exhaustive-dispatch rule accepted, because an exemption nobody
    can see is a baseline by another name.
    """
    if not after:
        return
    flat = _flatten(after)
    worst_cyc, worst_cog, over, exempt = _file_totals(flat, max_cyc, max_cog)
    accepted = f", {exempt} accepted as exhaustive dispatch" if exempt else ""
    print(
        f"  {rel}: {len(flat)} fn, worst cyc {worst_cyc}, worst cog {worst_cog}, "
        f"{over} already over {max_cyc}/{max_cog}{accepted}"
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
    after = measure(after_path, max_cyc, max_cog)
    _report_file(rel, after, max_cyc, max_cog)
    before_path = _blob(root, f"HEAD:{rel}", suffix, tmp, allow_missing=True)
    before = measure(before_path, max_cyc, max_cog) if before_path else {}
    findings = judge(before, after, max_cyc, max_cog)
    _print_findings(rel, findings, max_cyc, max_cog)
    return findings


ADVICE = """
If the finding is a cyclomatic-only one on a flat `match`, check it against the
terms of acceptance in scripts/rust_exhaustive_match.py FIRST: a genuinely
exhaustive match (no `_ =>`, no bare binding arm, cognitive within cap, and
nothing much else branching in the body) is already accepted and would not have
been reported. If it WAS reported, one of those four conditions does not hold --
most often a catch-all arm, which means the match is not exhaustive, decomposing
it forfeits no rustc guarantee, and it is ordinary debt.

Do NOT reach for a macro or a lookup table to make an exhaustive match smaller:
the macro only hides the arms from the scanner, and the lookup table trades
compile-time exhaustiveness for a metric. Both are worse code with a better
number.

Otherwise: split the function into named parts, or replace the branching with a
dict dispatch table. Measured on real shapes with cccc 1.6.0:

    dict dispatch table          cyclomatic  2   cognitive  1   <- wins BOTH
    extraction (parent)                      2              1   <- wins BOTH...
      ...the extracted CHILD                 5             10   <- ...child inherits
    flat if/elif chain (6 arms)              7              7
    deep nesting                             6             15   <- cognitive killer

Flattening nesting into a longer chain trades one metric for the other and is
not a fix. Recurse until every function you created is under BOTH caps.

Do NOT raise a threshold and do NOT add a suppression comment to pass this --
an in-line suppression is a one-line baseline, and this repository has no
deferrals ledger to put one in. If you believe the complexity is genuinely
irreducible, the only honest move is to argue for a RULE about the class of code
it belongs to, with the measurement attached, the way
scripts/rust_exhaustive_match.py does -- and to write it down in
docs/quality-gate-terms.md.
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
