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
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

DEFAULT_MAX_CYCLOMATIC = 10
DEFAULT_MAX_COGNITIVE = 15

#: Extensions cccc 1.6.0 scores. A file outside this set is skipped rather than
#: handed to cccc, so an unsupported type cannot look like "0 functions, clean".
SUPPORTED_SUFFIXES = frozenset(
    {
        ".py",
        ".rs",
        ".ts",
        ".tsx",
        ".js",
        ".jsx",
        ".go",
        ".java",
        ".kt",
        ".c",
        ".cc",
        ".cpp",
        ".h",
        ".hpp",
        ".cs",
        ".rb",
        ".php",
        ".swift",
        ".scala",
    }
)


def _fail_env(msg: str) -> None:
    print(f"complexity(staged): CANNOT RUN: {msg}", file=sys.stderr)
    raise SystemExit(2)


def _resolve_cccc() -> str:
    """Find cccc WITHOUT consulting any package index.

    A hook that resolves its tool from an index at hook time is how a previous
    fleet sweep shipped a gate that could not pass anywhere. Local paths only.
    """
    env = os.environ.get("CCCC_BIN")
    if env and Path(env).is_file():
        return env
    for cand in (Path.home() / ".local/bin/cccc", Path("/usr/local/bin/cccc")):
        if cand.is_file():
            return str(cand)
    found = shutil.which("cccc")
    if found:
        return found
    _fail_env(
        "`cccc` not found. Looked at $CCCC_BIN, ~/.local/bin/cccc, "
        "/usr/local/bin/cccc and $PATH. Build it with `cargo build --release` in "
        "open-source-libraries/cccc and copy the binary to ~/.local/bin/. This "
        "gate never installs anything itself -- resolving a gate's tool from a "
        "package index at hook time is how a previous fleet sweep shipped a hook "
        "that could not pass anywhere (69 push failures across 226 repos)."
    )


def _git(*args: str, cwd: str | None = None) -> subprocess.CompletedProcess:
    """Run git with the hook's ambient environment made harmless.

    git exports GIT_DIR / GIT_INDEX_FILE / GIT_WORK_TREE into EVERY hook
    subprocess. Inherited blindly they silently re-root path resolution, which
    is how ~20 copied gate helpers once measured an empty universe and reported
    a confident clean verdict. We keep GIT_DIR/GIT_INDEX_FILE (the index we must
    read IS the one being committed) but always run from the resolved toplevel
    with repo-relative paths, never `git -C <subdir>`.
    """
    return subprocess.run(
        ["git", *args], cwd=cwd, capture_output=True, text=True, check=False
    )


def repo_root() -> str:
    r = _git("rev-parse", "--show-toplevel")
    if r.returncode != 0 or not r.stdout.strip():
        _fail_env(f"not inside a work tree: {(r.stderr or '').strip()[:200]}")
    return r.stdout.strip()


def staged_files(root: str) -> list[str]:
    """Repo-relative paths staged as Added/Copied/Modified/Renamed."""
    r = _git("diff", "--cached", "--name-only", "--diff-filter=ACMR", cwd=root)
    if r.returncode != 0:
        _fail_env(f"git diff --cached failed: {(r.stderr or '').strip()[:200]}")
    return [
        line
        for line in r.stdout.splitlines()
        if line and Path(line).suffix in SUPPORTED_SUFFIXES
    ]


def _blob(root: str, rev_path: str, suffix: str, tmp: str) -> str | None:
    """Materialize `rev_path` (e.g. `HEAD:src/x.py`) to a temp file, or None."""
    r = _git("show", rev_path, cwd=root)
    if r.returncode != 0:
        return None
    fd, path = tempfile.mkstemp(suffix=suffix, dir=tmp)
    with os.fdopen(fd, "w", encoding="utf-8", errors="replace") as fh:
        fh.write(r.stdout)
    return path


def _walk(fn: dict, prefix: str, out: dict) -> None:
    """Collect a function AND its nested children, keeping EVERY row.

    Two traps live here, and both made a gate report clean over dirty code.

    1. cccc reports a nested function under its parent's `children`, not as a
       flat entry. A flat read scores an enclosing `def` at cognitive 0 and
       misses the nested body entirely -- the three worst Python functions in
       this workspace are nested.

    2. ★ A qualified name is NOT unique, so the value is a LIST. This function
       shipped as `out[name] = (cyc, cog)`, which keeps only whichever function
       of that name cccc emitted LAST and discards the others. Two classes in one
       module may each define `submit`; cccc reports both at the same level with
       the same qualified name. Measured when this was found:
       epistemic_graph/client.py collapsed 740 rows to 654 names, hiding 86 --
       including a `submit` at cyclomatic 22 / cognitive 20 sitting behind a
       clean one, over which the sibling gate reported "every function under both
       caps". In au: core/config.py hides 10 rows, multiplexer.py 1,
       source_sync.py 1.

    Do not "simplify" either of these back.
    """
    name = f"{prefix}{fn['name']}"
    out.setdefault(name, []).append((fn["cyclomatic"], fn["cognitive"]))
    for kid in fn.get("children", ()):
        _walk(kid, f"{name}.", out)


def measure(path: str) -> dict[str, list[tuple[int, int]]]:
    """{qualified_name: [(cyclomatic, cognitive), ...]} for one file.

    A list per name, not a pair -- names collide. See `_walk`.
    """
    exe = _resolve_cccc()
    try:
        r = subprocess.run(
            [exe, path, "--min", "0"], capture_output=True, text=True, timeout=600
        )
    except subprocess.TimeoutExpired:
        _fail_env(f"cccc timed out on {path}")
    except OSError as exc:
        _fail_env(f"could not execute {exe}: {exc}")
    if r.returncode > 1:
        _fail_env(f"cccc exited {r.returncode}: {(r.stderr or '').strip()[:300]}")
    if not r.stdout.strip():
        _fail_env(f"cccc produced no output for {path}; refusing to call that clean")
    try:
        doc = json.loads(r.stdout)
    except json.JSONDecodeError as exc:
        _fail_env(f"cccc output was not JSON: {exc}")
    out: dict[str, tuple[int, int]] = {}
    for f in doc.get("files", []):
        for fn in f.get("functions", []):
            _walk(fn, "", out)
    return out


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


def _regression(name: str, prior: list, rows: list) -> list:
    """A name present in HEAD whose worst value rose on either metric.

    Compares WORST-per-name rather than pairing rows up: a name may map to
    several functions and a rename or reorder shuffles them, so they cannot be
    matched across the change. Conservative -- it cannot miss a regression.
    """
    before = _worst(prior)
    after = _worst(rows)
    if after[0] > before[0] or after[1] > before[1]:
        return [("WORSE", name, before, after)]
    return []


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
            findings.extend(_regression(name, prior, rows))
    return findings


def _report_file(rel: str, after: dict[str, tuple[int, int]]) -> None:
    """Print the REAL absolute numbers for a touched file, on every run.

    The no-ratchet rule in code: pre-existing debt in a file you touched stays on
    screen even though this hook does not fail on it.
    """
    if not after:
        return
    flat = _flatten(after)
    worst_cyc, worst_cog = _worst(flat)
    over = sum(
        1 for c, g in flat if c > DEFAULT_MAX_CYCLOMATIC or g > DEFAULT_MAX_COGNITIVE
    )
    print(
        f"  {rel}: {len(flat)} fn, worst cyc {worst_cyc}, worst cog {worst_cog}, "
        f"{over} already over 10/15"
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
    """Findings for one staged file. Missing index blob -> no findings."""
    suffix = Path(rel).suffix
    after_path = _blob(root, f":{rel}", suffix, tmp)
    if after_path is None:
        return []
    after = measure(after_path)
    _report_file(rel, after)
    before_path = _blob(root, f"HEAD:{rel}", suffix, tmp)
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
    ap.add_argument("--max-cyclomatic", type=int, default=DEFAULT_MAX_CYCLOMATIC)
    ap.add_argument("--max-cognitive", type=int, default=DEFAULT_MAX_COGNITIVE)
    args = ap.parse_args()

    root = repo_root()
    files = staged_files(root)
    if not files:
        print("complexity(staged): OK: no staged file in a cccc-supported language")
        return 0

    print(
        f"complexity(staged): {len(files)} file(s), caps cyclomatic "
        f"{args.max_cyclomatic} / cognitive {args.max_cognitive}, both enforced"
    )
    findings: list = []
    with tempfile.TemporaryDirectory(prefix="cx-staged-") as tmp:
        for rel in files:
            findings.extend(
                check_file(root, rel, tmp, args.max_cyclomatic, args.max_cognitive)
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
