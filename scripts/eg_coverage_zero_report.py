#!/usr/bin/env python3
"""Turn one or more ``cargo llvm-cov --lcov`` reports into a ranked,
actionable "what is never executed" list.

This is the analysis half of the coverage instrument
(`scripts/eg_coverage_report.sh` is the collection half). It does not run
any tests itself -- it reads the lcov text ``cargo-llvm-cov`` already wrote
and answers three questions the raw report can't answer by itself:

1. Which *functions* in *covered* files never execute (``FNDA:0,<name>``),
   ranked by crate/module so the worst offenders sort to the top.
2. Which *whole files* have zero executed lines (``LH:0`` with ``LF>0``) --
   the "built but not wired" signature (`resolve_composed_graph` in
   `src/server/persistence/agent_graph.rs` was exactly this shape before it
   was covered).
3. Which source files under the scanned crate roots never appear in the
   lcov output *at all*. This is NOT automatically "zero coverage" -- three
   different situations produce it, reported as three separate buckets so
   they are never silently conflated:
     - **orphaned**: no `mod <stem>;` declaration exists in the file's real
       parent module (checked against Rust's actual module-resolution
       candidates, not a repo-wide stem grep) -- rustc never compiles the
       file under ANY feature combination. Strictly worse than a scope gap.
     - **cfg-gated**: module-declared, but behind a `#[cfg(...)]` this
       build didn't activate -- a build-configuration artifact.
     - **not selected by this run's lane**: module-declared and presumably
       compiled by some build, just outside this run's package/feature
       selection -- a scope gap in this measurement, not the product.

Usage:
    eg_coverage_zero_report.py --lcov OUT/stage-a.lcov [--lcov OUT/stage-b.lcov ...] \
        --root crates/eg-types --root crates/eg-storage --root crates/eg-capabilities \
        --contract-glob '*agent_library*' --contract-glob '*agent_graph*' \
        --contract-glob '*agent_component*' --contract-glob '*agent_template*' \
        --contract-glob '*delegation*' \
        --out OUT/ZERO-COVERAGE-REPORT.md --top 30
"""

from __future__ import annotations

import argparse
import fnmatch
import re
import shutil
import subprocess
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# `cargo llvm-cov` records SF: paths as whatever the build's cwd made them
# (often an absolute build-host path when built from a synced source tree,
# e.g. `/mnt/data/cargo-targets/eg-cov-src/...` on the R820 build host --
# see AGENTS.md build-host-split note). These are stripped back to
# repo-relative so this script's own repo-relative --root cross-check works
# regardless of which host produced the lcov file.
_BUILD_PATH_MARKERS = ("eg-cov-src/", "cargo-targets/eg-cov-src/")


def normalize_sf_path(raw: str) -> str:
    for marker in _BUILD_PATH_MARKERS:
        idx = raw.find(marker)
        if idx != -1:
            return raw[idx + len(marker) :]
    return raw


_DEMANGLE_CACHE: dict[str, str] = {}


def demangle_batch(names: list[str]) -> dict[str, str]:
    """Demangle Rust v0 symbol names via `c++filt` (GNU binutils' c++filt
    understands Rust v0 mangling as of recent binutils; confirmed present
    and working on this workspace's dev/build hosts). Falls back to the
    raw name, unchanged, if c++filt is unavailable -- callers must not
    assume the result is always human-readable.
    """
    todo = [n for n in set(names) if n not in _DEMANGLE_CACHE]
    if not todo:
        return _DEMANGLE_CACHE
    cxxfilt = shutil.which("c++filt")
    if not cxxfilt:
        for n in todo:
            _DEMANGLE_CACHE[n] = n
        return _DEMANGLE_CACHE
    proc = subprocess.run(
        [cxxfilt],
        input="\n".join(todo),
        capture_output=True,
        text=True,
        timeout=60,
    )
    out_lines = proc.stdout.splitlines()
    if len(out_lines) != len(todo):
        # Mismatch (shouldn't happen) -- fail safe to raw names rather than
        # mis-align the mapping.
        for n in todo:
            _DEMANGLE_CACHE[n] = n
    else:
        for n, demangled in zip(todo, out_lines, strict=True):
            _DEMANGLE_CACHE[n] = demangled
    return _DEMANGLE_CACHE


_HASH_BRACKET_RE = re.compile(r"\[[0-9a-f]{16}\]")
_GENERIC_SUFFIX_RE = re.compile(r"::<.*$", re.DOTALL)


def base_function_name(demangled: str) -> str:
    """Collapse a demangled, possibly-monomorphized Rust symbol down to its
    logical (base) function path, e.g.
    `epistemic_graph[c3571a...]::server::mutation::commit_mutation::<...
    ::tests::some_test::{closure#0}>` -> `epistemic_graph::server::
    mutation::commit_mutation`.

    This matters because a generic/async function produces one lcov
    "function" record PER monomorphization (often one per call site / test
    closure), which inflates a raw zero-function count with instantiation
    noise rather than real per-logical-function gaps -- confirmed on this
    codebase's `src/server/mutation.rs::commit_mutation` (972 raw "zero
    functions" collapsing to a handful of base functions once
    monomorphizations are grouped). See the task/report that created this
    script for the specific example.
    """
    stripped = _GENERIC_SUFFIX_RE.sub("", demangled)
    stripped = _HASH_BRACKET_RE.sub("", stripped)
    return stripped


CFG_RE = re.compile(r"#!?\[cfg\(([^)]*)\)\]")


@dataclass
class FileCov:
    path: str
    functions: dict[str, int] = field(default_factory=dict)  # name -> hits
    lines_found: int = 0
    lines_hit: int = 0
    fn_found: int = 0
    fn_hit: int = 0

    @property
    def zero_functions(self) -> list[str]:
        return [n for n, h in self.functions.items() if h == 0]

    @property
    def whole_file_zero(self) -> bool:
        return self.lines_found > 0 and self.lines_hit == 0


def _apply_fn(fc: FileCov, rest: str) -> None:
    # FN:<line>,<name>
    _, _, name = rest.partition(",")
    fc.functions.setdefault(name, 0)


def _apply_fnda(fc: FileCov, rest: str) -> None:
    # FNDA:<hits>,<name>
    hits_s, _, name = rest.partition(",")
    try:
        hits = int(hits_s)
    except ValueError:
        hits = 0
    fc.functions[name] = fc.functions.get(name, 0) + hits


def _apply_fnf(fc: FileCov, rest: str) -> None:
    fc.fn_found = int(rest)


def _apply_fnh(fc: FileCov, rest: str) -> None:
    fc.fn_hit = int(rest)


def _apply_lf(fc: FileCov, rest: str) -> None:
    fc.lines_found = int(rest)


def _apply_lh(fc: FileCov, rest: str) -> None:
    fc.lines_hit = int(rest)


# Dispatch table for lcov tracefile fields that apply to the CURRENT file
# record (everything except SF:, which starts a new record, and
# end_of_record, which closes one -- both handled by the caller since they
# change what "current" points to rather than mutating it).
_LCOV_FIELD_HANDLERS: dict[str, Callable[[FileCov, str], None]] = {
    "FN:": _apply_fn,
    "FNDA:": _apply_fnda,
    "FNF:": _apply_fnf,
    "FNH:": _apply_fnh,
    "LF:": _apply_lf,
    "LH:": _apply_lh,
}


def parse_lcov(path: Path) -> dict[str, FileCov]:
    """Parse one lcov file into {source_path: FileCov}.

    Minimal hand-rolled parser -- lcov's tracefile grammar is a flat
    sequence of ``KEY:value`` lines terminated by ``end_of_record``, no
    library needed for it (deliberately no third-party dep added for a
    ~10-line grammar; see CLAUDE.md "abstraction-first"). Per-field
    handling is a dict-dispatch table (`_LCOV_FIELD_HANDLERS`) rather than
    an if/elif chain -- the same shape this repo's own complexity gate
    recommends for a flat multi-branch prefix match.
    """
    files: dict[str, FileCov] = {}
    current: FileCov | None = None
    with path.open("r", encoding="utf-8", errors="replace") as fh:
        for raw in fh:
            line = raw.rstrip("\n")
            if line.startswith("SF:"):
                src = normalize_sf_path(line[3:])
                current = files.setdefault(src, FileCov(path=src))
                continue
            if line == "end_of_record":
                current = None
                continue
            if current is None:
                continue
            for prefix, handler in _LCOV_FIELD_HANDLERS.items():
                if line.startswith(prefix):
                    handler(current, line[len(prefix) :])
                    break
    return files


def merge(all_files: list[dict[str, FileCov]]) -> dict[str, FileCov]:
    merged: dict[str, FileCov] = {}
    for files in all_files:
        for src, fc in files.items():
            if src not in merged:
                merged[src] = fc
                continue
            dst = merged[src]
            for name, hits in fc.functions.items():
                dst.functions[name] = dst.functions.get(name, 0) + hits
            dst.lines_found = max(dst.lines_found, fc.lines_found)
            dst.lines_hit = max(dst.lines_hit, fc.lines_hit)
            dst.fn_found = max(dst.fn_found, fc.fn_found)
            dst.fn_hit = max(dst.fn_hit, fc.fn_hit)
    return merged


def crate_of(src_path: str) -> str:
    p = Path(src_path)
    parts = p.parts
    if "crates" in parts:
        i = parts.index("crates")
        if i + 1 < len(parts):
            return parts[i + 1]
    return "epistemic-graph (root)"


def leading_cfg(file_path: Path) -> str | None:
    try:
        text = file_path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None
    for m in CFG_RE.finditer(text[:4000]):
        return m.group(1)
    return None


_MOD_DECL_RE = re.compile(
    r"^\s*(?:pub(?:\([^)]*\))?\s+)?mod\s+([A-Za-z0-9_]+)\s*;", re.MULTILINE
)


def is_orphaned(file_path: Path) -> bool:
    """True if no `mod <stem>;` declaration for THIS file's actual parent
    module exists -- i.e. the file is not part of any crate's module tree
    and rustc never compiles it, regardless of feature lane.

    Deliberately restricted to Rust's real module-resolution candidates
    (`<dir>/mod.rs`, the sibling `<dir>.rs`, or `<dir>/lib.rs` /
    `<dir>/main.rs` for a crate root) rather than a repo-wide grep for the
    stem -- a repo-wide grep produces false negatives when an unrelated
    module elsewhere happens to share the same stem. Confirmed on this
    workspace: `crates/eg-types/src/lib.rs` declares `pub mod
    semantic_index;` for EG-TYPES' OWN `semantic_index/` directory, which a
    stem-only grep would wrongly count as "declaring" the unrelated,
    actually-orphaned `src/server/semantic_index.rs`.
    """
    stem = file_path.stem
    # A file named mod.rs or lib.rs/main.rs is never itself "mod-declared"
    # by its own stem -- skip the orphan check for those (their inclusion
    # is implicit / declared by the parent directory name instead).
    if stem in ("mod", "lib", "main"):
        return False
    parent = file_path.parent
    candidates = [
        parent / "mod.rs",
        parent.parent / f"{parent.name}.rs",
        parent / "lib.rs",
        parent / "main.rs",
    ]
    for cand in candidates:
        if not cand.exists() or cand == file_path:
            continue
        try:
            content = cand.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        for m in _MOD_DECL_RE.finditer(content):
            if m.group(1) == stem:
                return False
    return True


def find_source_files(roots: list[str]) -> list[Path]:
    out: list[Path] = []
    for root in roots:
        base = (ROOT / root).resolve()
        if not base.exists():
            continue
        out.extend(sorted(base.rglob("*.rs")))
    return out


def is_contract_surface(src_path: str, globs: list[str]) -> bool:
    return any(
        fnmatch.fnmatch(src_path, g) or fnmatch.fnmatch(Path(src_path).name, g)
        for g in globs
    )


def render_title_block() -> list[str]:
    return [
        "# eg coverage: zero-execution report",
        "",
        "Generated by `scripts/eg_coverage_zero_report.py` from "
        "`cargo llvm-cov --lcov` output. This is a MEASUREMENT artifact, "
        "not a gate: it lists what never executed so a human decides "
        "whether to write a test, delete dead code, or accept the gap.",
        "",
    ]


def render_per_crate_summary(merged: dict[str, FileCov]) -> list[str]:
    by_crate: dict[str, list[FileCov]] = {}
    for fc in merged.values():
        by_crate.setdefault(crate_of(fc.path), []).append(fc)

    out = [
        "## Per-crate summary",
        "",
        "| crate | files | lines hit/found | functions hit/found |",
        "|---|---|---|---|",
    ]
    for crate in sorted(by_crate):
        fcs = by_crate[crate]
        lf = sum(f.lines_found for f in fcs)
        lh = sum(f.lines_hit for f in fcs)
        ff = sum(f.fn_found for f in fcs)
        fh = sum(f.fn_hit for f in fcs)
        pct_l = (100.0 * lh / lf) if lf else 0.0
        pct_f = (100.0 * fh / ff) if ff else 0.0
        out.append(
            f"| {crate} | {len(fcs)} | {lh}/{lf} ({pct_l:.1f}%) | {fh}/{ff} "
            f"({pct_f:.1f}%) |"
        )
    out.append("")
    return out


@dataclass
class BaseFunctionData:
    """Per-file base-function grouping, computed once and shared by the
    ranked-functions and contract-surface sections (see
    `compute_base_function_data`)."""

    zero_bases: dict[str, list[str]]
    base_totals: dict[str, int]


def compute_base_function_data(merged: dict[str, FileCov]) -> BaseFunctionData:
    """Demangle every function name and collapse monomorphizations to their
    base (logical) function, per file. A generic/async function produces
    one lcov FN record PER monomorphization (often one per call site or
    test closure); ranking on raw instantiation counts massively
    over-weights heavily generic dispatch functions (confirmed:
    src/server/mutation.rs::commit_mutation alone contributed ~970 raw
    "zero functions" from instantiation noise). A base function counts as
    zero-coverage only if EVERY one of its monomorphizations is zero -- if
    any instantiation executed, the logical function ran.
    """
    all_names: list[str] = []
    for fc in merged.values():
        all_names.extend(fc.functions.keys())
    demangled = demangle_batch(all_names)

    zero_bases: dict[str, list[str]] = {}
    base_totals: dict[str, int] = {}
    for fc in merged.values():
        groups: dict[str, list[int]] = {}
        for name, hits in fc.functions.items():
            base = base_function_name(demangled.get(name, name))
            groups.setdefault(base, []).append(hits)
        zero_bases[fc.path] = [
            b for b, hitlist in groups.items() if all(h == 0 for h in hitlist)
        ]
        base_totals[fc.path] = len(groups)
    return BaseFunctionData(zero_bases=zero_bases, base_totals=base_totals)


def render_ranked_functions(
    merged: dict[str, FileCov], bf: BaseFunctionData, top_n: int
) -> list[str]:
    ranked = sorted(
        (fc for fc in merged.values() if bf.zero_bases[fc.path]),
        key=lambda fc: (len(bf.zero_bases[fc.path]), fc.lines_found - fc.lines_hit),
        reverse=True,
    )
    out = [
        f"## Ranked zero-coverage functions (top {top_n}, largest first)",
        "",
        "Counts are per **base function** (monomorphizations of the same "
        "generic/async function collapsed together; see script docstring) "
        "-- a base function is listed as zero only if NONE of its "
        "instantiations ever executed.",
        "",
        "| crate | file | zero base fns / total base fns | lines hit/found | zero "
        "function names (first 6) |",
        "|---|---|---|---|---|",
    ]
    for fc in ranked[:top_n]:
        zf = bf.zero_bases[fc.path]
        names = ", ".join(sorted(zf)[:6])
        if len(zf) > 6:
            names += f", … (+{len(zf) - 6} more)"
        out.append(
            f"| {crate_of(fc.path)} | `{fc.path}` | "
            f"{len(zf)}/{bf.base_totals[fc.path]} | "
            f"{fc.lines_hit}/{fc.lines_found} | {names} |"
        )
    out.append("")
    return out


def render_contract_surface(
    merged: dict[str, FileCov], bf: BaseFunctionData, contract_globs: list[str]
) -> list[str]:
    out = [
        "## Agent contract surface (agent_library / agent_graph / agent_component / "
        "agent_template / delegation)",
        "",
    ]
    contract_files = [
        fc for fc in merged.values() if is_contract_surface(fc.path, contract_globs)
    ]
    if not contract_files:
        out.append(
            "No files matching the contract-surface globs appeared in the "
            "lcov input at all -- see 'Source files absent from coverage "
            "output' below; this almost certainly means the contract "
            "surface was not part of this run's package/target selection, "
            "not that it is fully covered."
        )
        out.append("")
        return out
    out.append(
        "| file | zero base fns / total base fns | lines hit/found | zero function "
        "names |"
    )
    out.append("|---|---|---|---|")
    for fc in sorted(
        contract_files, key=lambda f: len(bf.zero_bases[f.path]), reverse=True
    ):
        zf = bf.zero_bases[fc.path]
        names = (
            ", ".join(sorted(zf))
            if zf
            else "(none — every function executed at least once)"
        )
        out.append(
            f"| `{fc.path}` | {len(zf)}/{bf.base_totals[fc.path]} | "
            f"{fc.lines_hit}/{fc.lines_found} | {names} |"
        )
    out.append("")
    return out


def render_whole_file_zero(merged: dict[str, FileCov]) -> list[str]:
    whole_zero = sorted(
        (fc for fc in merged.values() if fc.whole_file_zero),
        key=lambda f: f.lines_found,
        reverse=True,
    )
    out = ["## Whole files at 0% (present in the build, zero lines ever executed)", ""]
    if not whole_zero:
        out.append("None found in this run's scope.")
        out.append("")
        return out
    out.append("| crate | file | lines found |")
    out.append("|---|---|---|")
    for fc in whole_zero:
        out.append(f"| {crate_of(fc.path)} | `{fc.path}` | {fc.lines_found} |")
    out.append("")
    return out


def find_absent_files(
    merged: dict[str, FileCov], roots: list[str]
) -> list[tuple[Path, str | None]]:
    covered_suffixes = {Path(p).as_posix() for p in merged}
    absent: list[tuple[Path, str | None]] = []
    for f in find_source_files(roots):
        rel = f.relative_to(ROOT).as_posix()
        matched = any(rel.endswith(s) or s.endswith(rel) for s in covered_suffixes)
        if not matched:
            absent.append((f, leading_cfg(f)))
    return absent


@dataclass
class AbsentFileGroups:
    orphaned: list[tuple[Path, str | None]]
    cfg_gated: list[tuple[Path, str | None]]
    not_selected: list[tuple[Path, str | None]]


def classify_absent_files(absent: list[tuple[Path, str | None]]) -> AbsentFileGroups:
    """Orphan status is checked FIRST and wins over an incidental leading
    cfg: a file can carry e.g. `#![cfg(feature = "ann-redb")]` while ALSO
    having no `mod` declaration anywhere -- in that state the cfg is dead
    weight inside unreachable code, and the true reason it's absent is
    "orphaned", not "feature off" (confirmed: `semantic_index_service.rs` /
    `semantic_index.rs` both carry such a cfg AND are orphaned; bucketing
    them under cfg-gated alone would have hidden the more severe finding --
    exactly the conflation this report exists to avoid).
    """
    orphaned = [(f, g) for f, g in absent if is_orphaned(f)]
    non_orphaned = [(f, g) for f, g in absent if not is_orphaned(f)]
    cfg_gated = [(f, g) for f, g in non_orphaned if g]
    not_selected = [(f, g) for f, g in non_orphaned if not g]
    return AbsentFileGroups(
        orphaned=orphaned, cfg_gated=cfg_gated, not_selected=not_selected
    )


def render_absent_files_intro() -> list[str]:
    return [
        "## Source files absent from coverage output entirely",
        "",
        "Not automatically a gap -- three different situations produce this, "
        "and conflating them is exactly what makes a coverage report "
        "untrustworthy:\n\n"
        "1. **orphaned** (no `mod <stem>;` declaration exists in the file's "
        "real parent module): rustc never compiles this file under ANY "
        'feature combination, in ANY lane. This is the same "built but not '
        'wired" signature as the motivating `resolve_composed_graph` '
        "defect, one level worse -- the code isn't even reachable, let "
        "alone tested. Confirmed examples in this workspace: "
        "`crates/eg-core/src/compute/semantic_index_service.rs` and "
        "`src/server/semantic_index.rs` (neither has a `mod` declaration "
        "anywhere; `semantic_index.rs` imports from the other orphan, so "
        "the pair references each other while both sit outside every "
        "crate's module tree).\n"
        "2. **cfg-gated** (module-declared, but behind a leading "
        "`#[cfg(...)]` not active in this build): a build-configuration "
        "artifact, not a test gap.\n"
        "3. **not selected by this run's lane** (module-declared and "
        "presumably compiled by some build, just not the package/feature "
        "combination this particular run covered): a scope gap in this "
        "measurement run, not necessarily in the product.",
        "",
    ]


def render_absent_file_group(
    heading: str, group: list[tuple[Path, str | None]], show_cfg_note: bool
) -> list[str]:
    out = [heading, ""]
    for f, g in sorted(group):
        suffix = ""
        if g and show_cfg_note:
            suffix = f" (also carries a leading `cfg({g})`, moot while orphaned)"
        elif g:
            suffix = f" — `cfg({g})`"
        out.append(f"- `{f.relative_to(ROOT).as_posix()}`{suffix}")
    out.append("")
    return out


def render_absent_files(merged: dict[str, FileCov], roots: list[str]) -> list[str]:
    groups = classify_absent_files(find_absent_files(merged, roots))
    out = render_absent_files_intro()
    out += render_absent_file_group(
        f"**orphaned — no `mod` declaration found anywhere in the repo, "
        f"{len(groups.orphaned)} files:**",
        groups.orphaned,
        show_cfg_note=True,
    )
    out += render_absent_file_group(
        f"**cfg-gated (build-configuration artifact), {len(groups.cfg_gated)} files:**",
        groups.cfg_gated,
        show_cfg_note=False,
    )
    out += render_absent_file_group(
        f"**not selected by this run's lane (module-declared, just out of scope here) "
        f"— verify test invocation, {len(groups.not_selected)} files:**",
        groups.not_selected,
        show_cfg_note=False,
    )
    return out


def render_report(
    merged: dict[str, FileCov],
    roots: list[str],
    contract_globs: list[str],
    top_n: int,
) -> str:
    bf = compute_base_function_data(merged)
    lines: list[str] = []
    lines += render_title_block()
    lines += render_per_crate_summary(merged)
    lines += render_ranked_functions(merged, bf, top_n)
    lines += render_contract_surface(merged, bf, contract_globs)
    lines += render_whole_file_zero(merged)
    lines += render_absent_files(merged, roots)
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument(
        "--lcov", action="append", required=True, help="lcov file to read (repeatable)"
    )
    ap.add_argument(
        "--root",
        action="append",
        required=True,
        help=(
            "source root to cross-check for absent files (repeatable, relative "
            "to repo root)"
        ),
    )
    ap.add_argument(
        "--contract-glob",
        action="append",
        default=[
            "*agent_library*",
            "*agent_graph*",
            "*agent_component*",
            "*agent_template*",
            "*delegation*",
        ],
        help="fnmatch glob identifying the agent-contract surface (repeatable)",
    )
    ap.add_argument(
        "--top",
        type=int,
        default=30,
        help="how many ranked zero-function files to list",
    )
    ap.add_argument("--out", required=True, help="path to write the markdown report")
    args = ap.parse_args()

    parsed = [parse_lcov(Path(p)) for p in args.lcov]
    merged = merge(parsed)
    report = render_report(merged, args.root, args.contract_glob, args.top)
    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(report, encoding="utf-8")
    print(f"wrote {out_path} ({len(merged)} files in lcov input)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
