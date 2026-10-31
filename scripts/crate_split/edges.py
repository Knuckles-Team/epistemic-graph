#!/usr/bin/env python3
"""Inventory the `crate::` edges that violate the crate-split layering.

Parses `use crate::...` trees (including grouped imports) and inline `crate::a::b`
paths in every `.rs` file under `src/`, maps source file and referenced module to
their manifest crate, and reports each edge outside `allowed_deps` as a BACK-EDGE.
Column-0 `use super::`/`use self::` statements are resolved too.

Writes `back_edges.json` and `BACK-EDGES.md` next to this script
(MEDULLA-CONTRACT-R056).
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from crate_split.ownership import (
    DEFAULT_MANIFEST,
    DELETE,
    Manifest,
    build_owner_map,
    list_src_files,
    load_manifest,
)

LEAF = "<leaf>"
BLANK = re.compile(
    r"//[^\n]*|/\*.*?\*/|r(#*)\".*?\"\1|\"(?:\\.|[^\"\\])*\"|'(?:\\.|[^'\\])'", re.S
)
USE_START = re.compile(r"(?m)^([ \t]*)(?:pub(?:\([^)]*\))?\s+)?use\s+")
TOKEN = re.compile(r"::|[{},*]|[A-Za-z_]\w*")
INLINE = re.compile(r"(?<![$\w:])crate((?:::[A-Za-z_]\w*)+)")
TEST_HINTS = ("/tests/", "_tests.rs", "/tests.rs", "test_support", "_fixtures.rs")


@dataclass(frozen=True)
class Edge:
    from_crate: str
    to_crate: str
    file: str
    line: int
    item: str
    is_test: bool


def blank_non_code(text: str) -> str:
    """Replace comments and string/char literals with spaces, keeping newlines."""

    def blank(m: re.Match[str]) -> str:
        return re.sub(r"[^\n]", " ", m.group(0))

    return BLANK.sub(blank, text)


def parse_use_tree(body: str) -> list[tuple[tuple[str, ...], int]]:
    """Expand a use tree into (segments, line offset) leaves."""
    tokens = [
        (m.group(0), body.count("\n", 0, m.start())) for m in TOKEN.finditer(body)
    ]
    leaves: list[tuple[tuple[str, ...], int]] = []
    _parse_items(tokens, 0, (), leaves)
    return leaves


def _parse_items(
    tokens: list[tuple[str, int]],
    i: int,
    prefix: tuple[str, ...],
    leaves: list[tuple[tuple[str, ...], int]],
) -> int:
    segs = list(prefix)
    line = 0
    while i < len(tokens):
        tok, line = tokens[i]
        if tok in (",", "}"):
            break
        if tok == "{":
            return _parse_group(tokens, i + 1, tuple(segs), leaves)
        if tok == "as":
            i += 2
            continue
        if tok not in ("::", "self"):
            segs.append(tok)
        i += 1
    if segs:
        leaves.append((tuple(segs), line))
    return i


def _parse_group(
    tokens: list[tuple[str, int]],
    i: int,
    prefix: tuple[str, ...],
    leaves: list[tuple[tuple[str, ...], int]],
) -> int:
    while i < len(tokens) and tokens[i][0] != "}":
        i = _parse_items(tokens, i, prefix, leaves)
        if i < len(tokens) and tokens[i][0] == ",":
            i += 1
    return i + 1


def find_use_statements(code: str) -> list[tuple[int, int, str, str]]:
    """(start, end, indent, body) for every `use ...;` statement."""
    found = []
    for m in USE_START.finditer(code):
        end = code.find(";", m.end())
        if end != -1:
            found.append((m.start(), end + 1, m.group(1), code[m.end() : end]))
    return found


def module_path(rel: str) -> list[str] | None:
    """Module path of a lib-crate source file, None for bin roots."""
    if rel in ("src/main.rs",) or rel.startswith("src/bin/"):
        return None
    parts = rel[len("src/") : -len(".rs")].split("/")
    if parts[-1] in ("mod", "lib"):
        parts.pop()
    return parts


def absolutize(
    segs: tuple[str, ...], modpath: list[str] | None
) -> tuple[str, ...] | None:
    """Turn crate::/self::/super:: leading forms into crate-relative segments."""
    if not segs:
        return None
    head, rest = segs[0], list(segs[1:])
    if head == "crate":
        return tuple(rest)
    if modpath is None or head not in ("self", "super"):
        return None
    base = list(modpath)
    if head == "super":
        while rest and rest[0] == "super":
            rest.pop(0)
            base = base[:-1]
        base = base[:-1]
    return tuple(base + rest)


class Resolver:
    """Map crate-relative module paths onto the manifest crate that owns them."""

    def __init__(
        self, files: set[str], owners: dict[str, str | None], leaf_names: set[str]
    ):
        self.files = files
        self.owners = owners
        self.leaf_names = leaf_names

    def module_file(self, segs: tuple[str, ...]) -> str | None:
        file = None
        for k in range(1, len(segs) + 1):
            base = "src/" + "/".join(segs[:k])
            if base + ".rs" in self.files:
                file = base + ".rs"
            elif base + "/mod.rs" in self.files:
                file = base + "/mod.rs"
            else:
                break
        return file

    def crate_of(self, segs: tuple[str, ...]) -> str | None:
        file = self.module_file(segs)
        if file is not None:
            return self.owners.get(file)
        if segs and segs[0] in self.leaf_names:
            return LEAF
        return self.owners.get("src/lib.rs")


def lib_reexports(code: str) -> set[str]:
    """Names `src/lib.rs` re-exports or declares as macros: not engine-local modules."""
    names: set[str] = set()
    for _s, _e, _ind, body in find_use_statements(code):
        for segs, _ in parse_use_tree(body):
            names.add(segs[-1] if segs else "")
    return names


def scan_file(rel: str, text: str) -> list[tuple[tuple[str, ...], int]]:
    """All crate-relative referenced paths in a file with their 1-based line."""
    code = blank_non_code(text)
    modpath = module_path(rel)
    refs: list[tuple[tuple[str, ...], int]] = []
    masked = list(code)
    for start, end, indent, body in find_use_statements(code):
        base = code.count("\n", 0, start) + 1
        for segs, off in parse_use_tree(body):
            if indent and segs[:1] in (("self",), ("super",)):
                continue
            absolute = absolutize(segs, modpath)
            if absolute:
                refs.append((absolute, base + off))
        for k in range(start, end):
            masked[k] = " " if masked[k] != "\n" else "\n"
    rest = "".join(masked)
    for m in INLINE.finditer(rest):
        segs = tuple(m.group(1).split("::")[1:])
        refs.append((segs, rest.count("\n", 0, m.start()) + 1))
    return refs


def classify(manifest: Manifest, src: str, dst: str | None) -> bool:
    """True when src -> dst is a back-edge under the manifest."""
    if dst is None or dst == LEAF or dst == src or src == DELETE:
        return False
    if dst == DELETE:
        return True
    return dst not in manifest.crates[src].allowed_deps


def collect_edges(root: Path, manifest: Manifest) -> tuple[list[Edge], dict[str, dict]]:
    files = list_src_files(root)
    owners = build_owner_map(manifest, files)
    leaves = {
        n for n in lib_reexports(blank_non_code((root / "src/lib.rs").read_text()))
    }
    resolver = Resolver(set(files), owners, leaves)
    stats: dict[str, dict] = defaultdict(lambda: {"files": 0, "lines": 0})
    edges: list[Edge] = []
    for rel in files:
        text = (root / rel).read_text(encoding="utf-8", errors="replace")
        src = owners[rel]
        if src is None:
            continue
        stats[src]["files"] += 1
        stats[src]["lines"] += len(text.splitlines())
        edges.extend(file_edges(manifest, resolver, rel, text, src))
    return edges, stats


def file_edges(
    manifest: Manifest, resolver: Resolver, rel: str, text: str, src: str
) -> list[Edge]:
    is_test = any(h in "/" + rel for h in TEST_HINTS)
    out = []
    for segs, line in scan_file(rel, text):
        dst = resolver.crate_of(segs)
        if classify(manifest, src, dst):
            item = "crate::" + "::".join(segs)
            out.append(Edge(src, str(dst), rel, line, item, is_test))
    return out


def summarize(edges: list[Edge], stats: dict[str, dict]) -> dict:
    pair_counts: Counter[str] = Counter(f"{e.from_crate}->{e.to_crate}" for e in edges)
    for name, st in stats.items():
        st["back_edges_out"] = sum(1 for e in edges if e.from_crate == name)
        st["back_edges_in"] = sum(1 for e in edges if e.to_crate == name)
    return {
        "crates": dict(sorted(stats.items())),
        "pairs": dict(pair_counts.most_common()),
        "total_back_edges": len(edges),
        "back_edges": [asdict(e) for e in sorted_edges(edges)],
    }


def sorted_edges(edges: list[Edge]) -> list[Edge]:
    return sorted(
        edges, key=lambda e: (e.from_crate, e.to_crate, e.file, e.line, e.item)
    )


def group_edges(edges: list[Edge]) -> dict[tuple[str, str], dict[str, list[Edge]]]:
    grouped: dict[tuple[str, str], dict[str, list[Edge]]] = defaultdict(
        lambda: defaultdict(list)
    )
    for e in sorted_edges(edges):
        grouped[(e.from_crate, e.to_crate)][e.item].append(e)
    return grouped


def render_group(
    pair: tuple[str, str], items: dict[str, list[Edge]], cap: int
) -> list[str]:
    total = sum(len(v) for v in items.values())
    lines = [f"## {pair[0]} -> {pair[1]}  ({total} refs, {len(items)} items)", ""]
    ranked = sorted(items.items(), key=lambda kv: (-len(kv[1]), kv[0]))
    for item, occ in ranked[:cap]:
        where = ", ".join(f"{e.file}:{e.line}" for e in occ[:3])
        more = f" (+{len(occ) - 3} more)" if len(occ) > 3 else ""
        lines.append(f"- `{item}` x{len(occ)}: {where}{more}")
    if len(ranked) > cap:
        lines.append(f"- ... {len(ranked) - cap} more items in back_edges.json")
    return lines + [""]


def render_markdown(edges: list[Edge], stats: dict[str, dict], cap: int = 60) -> str:
    out = [
        "# Crate-split back-edges",
        "",
        "Generated by `scripts/crate_split/edges.py`.",
        "",
    ]
    out += [
        "| crate | files | lines | back-edges out | back-edges in |",
        "|---|---|---|---|---|",
    ]
    for name, st in sorted(stats.items()):
        out.append(
            f"| {name} | {st['files']} | {st['lines']} | {st['back_edges_out']}"
            f" | {st['back_edges_in']} |"
        )
    out += ["", f"Total back-edge references: {len(edges)}", ""]
    grouped = group_edges(edges)
    order = sorted(grouped, key=lambda p: -sum(len(v) for v in grouped[p].values()))
    for pair in order:
        out += render_group(pair, grouped[pair], cap)
    return "\n".join(out)


def parse_args(argv: list[str]) -> argparse.Namespace:
    here = Path(__file__).resolve().parent
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--root", type=Path, default=here.parents[1])
    ap.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    ap.add_argument("--out-dir", type=Path, default=here)
    return ap.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    manifest = load_manifest(args.manifest)
    edges, stats = collect_edges(args.root, manifest)
    summary = summarize(edges, stats)
    args.out_dir.mkdir(parents=True, exist_ok=True)
    (args.out_dir / "back_edges.json").write_text(
        json.dumps(summary, indent=1, sort_keys=False) + "\n", encoding="utf-8"
    )
    (args.out_dir / "BACK-EDGES.md").write_text(
        render_markdown(edges, summary["crates"]) + "\n", encoding="utf-8"
    )
    print(
        f"{len(edges)} back-edge references across {len(summary['pairs'])} crate pairs"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
