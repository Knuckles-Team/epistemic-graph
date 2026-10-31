#!/usr/bin/env python3
"""Codemod for one crate-split lane (MEDULLA-CONTRACT-R056).

For crate X, the moved files are those `placement.toml` still assigns to X under `src/`.
`--moves` prints the `git mv` commands. The default mode rewrites `crate::<old path>` in
the moved files and in every caller to `crate::<new path>` (same crate) or
`medulla_<x>::<new path>` (other crate); `--check` reports the files that would change
and exits 1 instead of writing. Moved files also lose lib.rs re-export aliases and
report
references to code that stays behind, which the layering forbids.
"""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from crate_split.graph import (
    Index,
    blank_non_code,
    find_use_statements,
    module_path,
    parse_use_tree,
    test_region_start,
)
from crate_split.ownership import (
    DEFAULT_PLACEMENT,
    list_src_files,
    load_manifest,
    load_placement,
)

HERE = Path(__file__).resolve().parent
STRIPS = ("src/server/handlers/", "src/server/persistence/", "src/server/", "src/")
INLINE = re.compile(r"(?<![$\w:])crate((?:::[A-Za-z_]\w*)+)")
HANDLER_PREFIX = "medulla-h-"


@dataclass(frozen=True)
class Move:
    old: str
    new: str
    crate: str
    old_mod: tuple[str, ...]
    new_mod: tuple[str, ...]


def snake(crate: str) -> str:
    return crate.replace("-", "_")


def strip_prefix(rel: str, crate: str) -> str:
    for prefix in STRIPS:
        if prefix == "src/server/handlers/" and not crate.startswith(HANDLER_PREFIX):
            continue
        if prefix == "src/server/persistence/" and crate != "medulla-persistence":
            continue
        if rel.startswith(prefix):
            return rel[len(prefix) :]
    return rel


def plan_moves(files: list[str], placement: dict[str, str], crate: str) -> list[Move]:
    moves = []
    for rel in files:
        if placement.get(rel) != crate:
            continue
        sub = strip_prefix(rel, crate)
        new = f"crates/{crate}/src/{sub}"
        old_mod = tuple(module_path(rel) or ())
        new_mod = tuple(module_path("src/" + sub) or ())
        moves.append(Move(rel, new, crate, old_mod, new_mod))
    return moves


def render_path(move_crate: str, here_crate: str, segs: tuple[str, ...]) -> str:
    head = "crate" if move_crate == here_crate else snake(move_crate)
    return "::".join((head, *segs))


class Rewriter:
    def __init__(
        self,
        root: Path,
        files: list[str],
        moves: list[Move],
        crate: str,
        placement: dict[str, str],
    ):
        self.moves = moves
        self.crate = crate
        self.by_mod = {m.old_mod: m for m in moves if m.old_mod}
        self.moved = {m.old for m in moves}
        self.index = Index(root, files)
        self.placement = placement
        self.level = load_manifest().level
        self.problems: list[str] = []
        self.leaf_map = leaf_reexports(self.index)

    def target(self, segs: tuple[str, ...]) -> tuple[Move, tuple[str, ...]] | None:
        for k in range(len(segs), 0, -1):
            move = self.by_mod.get(segs[:k])
            if move:
                return move, move.new_mod + segs[k:]
        return None

    def path_for(self, rel: str, segs: tuple[str, ...]) -> str | None:
        here = self.crate if rel in self.moved else "old"
        hit = self.target(segs)
        if hit:
            return render_path(hit[0].crate, here, hit[1])
        if rel in self.moved and segs and segs[0] in self.leaf_map:
            return "::".join(self.leaf_map[segs[0]] + segs[1:])
        return None

    def check_stays(self, rel: str, segs: tuple[str, ...]) -> None:
        if rel not in self.moved:
            return
        dst, _ = self.index.locate(segs)
        if not dst.startswith("src/") or dst in self.moved or dst == "src/lib.rs":
            return
        item = f"crate::{'::'.join(segs)}"
        owner = self.placement.get(dst, "?")
        verb = "PREREQ" if self.level(owner) < self.level(self.crate) else "BLOCKED"
        self.problems.append(
            f"{verb}: {rel} references {dst} placed in {owner} ({item})"
        )


def leaf_reexports(index: Index) -> dict[str, tuple[str, ...]]:
    """lib.rs `pub use leaf::{a, b}` aliases: crate-local name -> leaf path."""
    out: dict[str, tuple[str, ...]] = {}
    code = blank_non_code(index.text.get("src/lib.rs", ""))
    for _s, _e, indent, body in find_use_statements(code):
        for segs, _line, alias in parse_use_tree(body):
            if (
                not indent
                and segs[0] not in ("crate", "self", "super")
                and segs[-1] != "*"
            ):
                out[alias or segs[-1]] = tuple(
                    snake(s) if i == 0 else s for i, s in enumerate(segs)
                )
    return out


def rewrite_text(rw: Rewriter, rel: str, text: str) -> str:
    code = blank_non_code(text)
    test_from = test_region_start(text)
    edits: list[tuple[int, int, str]] = []
    spans = []
    for start, end, indent, body in find_use_statements(code):
        spans.append((start, end))
        in_test = text.count("\n", 0, start) + 1 >= test_from
        new = rewrite_use(rw, rel, indent, text[start:end], body, in_test)
        if new is not None:
            edits.append((start, end, new))
    for m in INLINE.finditer(code):
        if any(s <= m.start() < e for s, e in spans):
            continue
        segs = tuple(m.group(1).split("::")[1:])
        new = rw.path_for(rel, segs)
        if new:
            edits.append((m.start(), m.end(), new))
        elif text.count("\n", 0, m.start()) + 1 < test_from:
            rw.check_stays(rel, segs)
    for start, end, repl in sorted(edits, reverse=True):
        text = text[:start] + repl + text[end:]
    return text


def rewrite_use(
    rw: Rewriter, rel: str, indent: str, stmt: str, body: str, in_test: bool
) -> str | None:
    """Rewritten `use` statement, flattened when it is grouped, else None."""
    leaves = parse_use_tree(body)
    lead = re.match(r"([ \t]*(?:pub(?:\([^)]*\))?\s+)?use\s+)", stmt)
    prefix = lead.group(1) if lead else "use "
    out, hit = [], False
    for segs, _line, alias in leaves:
        if not segs or segs[0] != "crate":
            out.append(None)
            continue
        new = rw.path_for(rel, segs[1:]) if segs[1:] else None
        if new is None and not in_test:
            rw.check_stays(rel, segs[1:])
        hit = hit or new is not None
        out.append((new or "::".join(segs), alias))
    if not hit:
        return None
    if "{" not in body:
        item = out[0]
        return f"{prefix}{item[0]}{' as ' + item[1] if item[1] else ''};"
    lines = [
        f"{prefix}{p}{' as ' + a if a else ''};"
        for item in out
        if item
        for p, a in [item]
    ]
    return "\n".join(lines) if lines else None


def run(
    root: Path, crate: str, placement_path: Path, check: bool
) -> tuple[list[str], list[str]]:
    files = list_src_files(root)
    placement = load_placement(placement_path)
    moves = plan_moves(files, placement, crate)
    rw = Rewriter(root, files, moves, crate, placement)
    new_texts = {}
    for rel in files:
        text = (root / rel).read_text(encoding="utf-8")
        new = rewrite_text(rw, rel, text)
        if new != text:
            new_texts[rel] = new
    if not check and not rw.problems:
        for rel, new in new_texts.items():
            (root / rel).write_text(new, encoding="utf-8")
        update_placement(placement_path, moves)
    return sorted(new_texts), rw.problems


def update_placement(path: Path, moves: list[Move]) -> None:
    """Point placement.toml at the new locations of the moved files."""
    text = path.read_text(encoding="utf-8")
    for m in moves:
        text = text.replace(f'"{m.old}"', f'"{m.new}"')
    path.write_text(text, encoding="utf-8")


def print_mods(root: Path, crate: str, placement_path: Path) -> None:
    """Module declarations to add to the new crate root and delete from old parents."""
    files = list_src_files(root)
    moves = plan_moves(files, load_placement(placement_path), crate)
    top = sorted({m.new_mod[0] for m in moves if m.new_mod})
    for name in top:
        print(f"add to crates/{crate}/src/lib.rs: pub mod {name};")
    moved = {m.old for m in moves}
    for m in moves:
        parent = parent_declaring(m.old, set(files))
        if parent and parent not in moved:
            print(f"delete `mod {m.old_mod[-1]};` from {parent}")


def parent_declaring(rel: str, files: set[str]) -> str | None:
    parent = rel.rsplit("/", 1)[0]
    if rel.endswith("/mod.rs"):
        parent = parent.rsplit("/", 1)[0]
    return next(
        (p for p in (parent + ".rs", parent + "/mod.rs") if p in files and p != rel),
        None,
    )


def print_moves(root: Path, crate: str, placement_path: Path) -> None:
    files = list_src_files(root)
    for m in plan_moves(files, load_placement(placement_path), crate):
        print(f"mkdir -p {Path(m.new).parent} && git mv {m.old} {m.new}")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--crate", required=True)
    ap.add_argument("--root", type=Path, default=HERE.parents[1])
    ap.add_argument("--placement", type=Path, default=DEFAULT_PLACEMENT)
    ap.add_argument(
        "--check", action="store_true", help="report, do not write; exit 1 on drift"
    )
    ap.add_argument("--moves", action="store_true", help="print the git mv commands")
    ap.add_argument(
        "--mods", action="store_true", help="print module declarations to add or delete"
    )
    args = ap.parse_args(sys.argv[1:] if argv is None else argv)
    if args.moves:
        print_moves(args.root, args.crate, args.placement)
        return 0
    if args.mods:
        print_mods(args.root, args.crate, args.placement)
        return 0
    changed, problems = run(args.root, args.crate, args.placement, args.check)
    for rel in changed:
        print(("would rewrite " if args.check else "rewrote ") + rel)
    for problem in problems:
        print(problem, file=sys.stderr)
    return 1 if problems or (args.check and changed) else 0


if __name__ == "__main__":
    raise SystemExit(main())
