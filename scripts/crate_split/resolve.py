#!/usr/bin/env python3
"""Resolve the file-to-crate placement for the crate split (MEDULLA-CONTRACT-R056).

The module is the unit: a module and its descendants move together and are never
re-rooted. Planned units stay in their planned crate. Unplanned units move down to the
highest crate below-or-equal every crate that references them, provided their own
references stay at or below it. Clusters of unplanned units that block each other
(strongly connected components) move jointly. A reference that still points upward is a
seam, an extracted type or helper, or a `decide` item; there is no `needs-seam` state.
Writes `placement.toml`, `seams.json` and `SEAMS.md` next to this script.
"""

from __future__ import annotations

import argparse
import re
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from crate_split.graph import Ref, collect_refs
from crate_split.ownership import (
    DELETE,
    Manifest,
    build_owner_map,
    entry_matches,
    list_src_files,
    load_manifest,
)
from crate_split.units import HUB_FILES, build_units, is_test_file, tarjan

API = "medulla-server-api"
SERVER = "medulla-server"
FLOOR = 2
STATE_FILE = "src/server/state.rs"
HERE = Path(__file__).resolve().parent
FORCED = {
    "server/sql_catalog_acl": API,
    "mutation_apply": API,
    "server/oidc": API,
    "server/cdc": API,
}
DISPATCH_NAMES = {
    "dispatch",
    "dispatch_authenticated_local_query",
    "dispatch_graph_op",
    "try_handle",
    "TryHandleContext",
    "handlers",
}
TRAITS = {
    "medulla-h-decide": "DecideApi",
    "medulla-h-query": "QueryApi",
    "medulla-h-admin": "AdminApi",
    "medulla-h-txn": "TxnApi",
    "medulla-h-jobs": "JobsApi",
    "medulla-h-ingest": "IngestApi",
    "medulla-h-graph": "GraphApi",
    "medulla-h-compute": "ComputeApi",
    "medulla-wire": "WireApi",
}
STATE_SUFFIX = (
    "Hub",
    "Registry",
    "Surface",
    "Cascade",
    "Coalescer",
    "Authority",
    "Store",
    "State",
)


@dataclass
class Context:
    manifest: Manifest
    files: list[str]
    original: dict[str, str]
    refs: list[Ref]
    unit_of: dict[str, str]
    members: dict[str, list[str]]
    crate: dict[str, str] = field(default_factory=dict)
    movable: set[str] = field(default_factory=set)
    decide: set[str] = field(default_factory=set)
    moves: set[str] = field(default_factory=set)
    placement: dict[str, str] = field(default_factory=dict)

    def ok(self, src_crate: str, dst_crate: str) -> bool:
        return self.manifest.may_depend(src_crate, dst_crate)

    def level(self, crate: str) -> int:
        return self.manifest.level(crate)

    def fcrate(self, rel: str) -> str:
        return SERVER if rel in self.moves else self.crate[self.unit_of[rel]]


@dataclass(frozen=True)
class Classified:
    file: str
    line: int
    item: str
    category: str
    detail: str
    dst: str


def split_item(item: str) -> tuple[str, bool]:
    """(path up to the first capitalised segment, whether it names a type)."""
    segs = item.split("::")
    for i, seg in enumerate(segs):
        if seg[:1].isupper():
            return "::".join(segs[: i + 1]), True
    return item, False


def is_dispatch_target(unit: str) -> bool:
    return unit == "server/dispatch" or unit in HUB_FILES


def label_shape(ctx: Context, ref: Ref, sc: str, dc: str) -> tuple[str, str]:
    """Category and detail for a violating reference, from the item's shape."""
    unit = ctx.unit_of[ref.dst]
    path, is_type = split_item(ref.item)
    last = ref.item.split("::")[-1]
    if ctx.original[ref.dst] == DELETE:
        return "pending-delete", ref.dst
    if ref.src == STATE_FILE:
        return "seam:state-handle", ref.item
    if ctx.unit_of[ref.src] == "mutation_apply":
        return "seam:dispatch", ref.item
    if unit in ctx.decide:
        return "decide", unit
    if is_dispatch_target(unit) and (
        last in DISPATCH_NAMES
        or unit in HUB_FILES
        and not is_type
        and last == "handlers"
    ):
        return "seam:dispatch", ref.dst
    if sc in TRAITS and dc in TRAITS and not is_type:
        return "seam:" + TRAITS[dc], ref.item
    if is_type:
        name = path.split("::")[-1]
        return (
            ("seam:state-handle", path)
            if name.endswith(STATE_SUFFIX)
            else ("extract-type", path)
        )
    return "extract-helper", ref.item


def meet(ctx: Context, crates: set[str]) -> str:
    """Highest crate below-or-equal every crate in the set."""
    cands = [c for c in ctx.manifest.crates if all(ctx.ok(s, c) for s in crates)]
    top = max(ctx.level(c) for c in cands)
    return sorted(c for c in cands if ctx.level(c) == top)[0]


class Placer:
    """Unit placement over the generic (unlabelled, non-test) references."""

    def __init__(self, ctx: Context, generic: list[Ref]):
        self.ctx = ctx
        self.into: dict[str, list[Ref]] = defaultdict(list)
        self.out: dict[str, list[Ref]] = defaultdict(list)
        for ref in generic:
            self.into[ctx.unit_of[ref.dst]].append(ref)
            self.out[ctx.unit_of[ref.src]].append(ref)

    def users(self, unit: str) -> set[str]:
        return {self.ctx.crate[self.ctx.unit_of[r.src]] for r in self.into[unit]}

    def violated(self, unit: str) -> bool:
        c = self.ctx
        return any(
            not c.ok(self.ctx.crate[c.unit_of[r.src]], c.crate[unit])
            for r in self.into[unit]
        )

    def simple(self) -> bool:
        """One pass of single-unit moves; True when something moved."""
        changed = False
        for unit in sorted(self.ctx.movable):
            if not self.violated(unit):
                continue
            want = meet(self.ctx, self.users(unit))
            if self.can_move(unit, want):
                self.ctx.crate[unit] = want
                changed = True
        return changed

    def can_move(self, unit: str, want: str) -> bool:
        c = self.ctx
        if (
            c.level(want) < FLOOR
            or c.level(want) > c.level(c.crate[unit])
            or want == c.crate[unit]
        ):
            return False
        return all(c.ok(want, c.crate[c.unit_of[r.dst]]) for r in self.out[unit])

    def cluster_moves(self) -> bool:
        """Move each blocking cluster of unplanned units jointly."""
        c = self.ctx
        nodes = sorted(c.movable)
        succ = {
            u: {c.unit_of[r.dst] for r in self.out[u] if c.unit_of[r.dst] in c.movable}
            for u in nodes
        }
        changed = False
        for comp in tarjan(nodes, succ):
            if not any(self.violated(u) for u in comp):
                continue
            outside = {
                c.crate[c.unit_of[r.src]]
                for u in comp
                for r in self.into[u]
                if c.unit_of[r.src] not in comp
            }
            if not outside:
                continue
            want = meet(c, outside)
            for unit in comp:
                if (
                    FLOOR <= c.level(want) <= c.level(c.crate[unit])
                    and want != c.crate[unit]
                ):
                    c.crate[unit] = want
                    changed = True
        return changed

    def run(self) -> None:
        for _ in range(8):
            while self.simple():
                pass
            if not self.cluster_moves():
                break

    def stuck(self) -> set[str]:
        return {u for u in self.ctx.movable if self.violated(u)}


def unplanned_files(text: str, manifest: Manifest, files: list[str]) -> set[str]:
    marked = set(re.findall(r'^\s*"([^"]+)",\s*# unplanned', text, re.M))
    result = set()
    for rel in files:
        hits = [(len(e), e) for e, _c in manifest.entries() if entry_matches(e, rel)]
        if hits and max(hits)[1] in marked:
            result.add(rel)
    return result


def is_generic(ctx: Context, ref: Ref) -> bool:
    """References that constrain placement: non-test, unlabelled, between units."""
    if ref.in_test or is_test_file(ref.src) or is_test_file(ref.dst):
        return False
    if ctx.unit_of[ref.src] == ctx.unit_of[ref.dst] or ref.src == STATE_FILE:
        return False
    cs, cd = ctx.original[ref.src], ctx.original[ref.dst]
    return DELETE not in (cs, cd) and not (cs in TRAITS and cd in TRAITS and cs != cd)


def final_labels(ctx: Context) -> list[Classified]:
    """Label every reference that still violates the layering."""
    out = []
    for ref in ctx.refs:
        sc, dc = ctx.fcrate(ref.src), ctx.fcrate(ref.dst)
        if ctx.ok(sc, dc):
            continue
        if ref.in_test or is_test_file(ref.src) or is_test_file(ref.dst):
            cat, detail = "test-move", f"{sc} -> {dc}"
        else:
            cat, detail = label_shape(ctx, ref, sc, dc)
        out.append(Classified(ref.src, ref.line, ref.item, cat, detail, ref.dst))
    return out


def settle_test_moves(ctx: Context) -> list[Classified]:
    """Move test files that reach above their crate into the server crate's tests."""
    while True:
        labels = final_labels(ctx)
        fresh = {
            x.file for x in labels if x.category == "test-move" and is_test_file(x.file)
        }
        fresh -= ctx.moves
        if not fresh:
            return labels
        ctx.moves |= fresh


def build_context(root: Path, manifest_path: Path) -> tuple[Context, set[str]]:
    manifest = load_manifest(manifest_path)
    files = list_src_files(root)
    owners = build_owner_map(manifest, files)
    original = {f: c or SERVER for f, c in owners.items()}
    unit_of, members = build_units(files)
    ctx = Context(
        manifest, files, original, collect_refs(root, files), unit_of, members
    )
    unplanned = unplanned_files(
        manifest_path.read_text(encoding="utf-8"), manifest, files
    )
    for unit, fs in members.items():
        ctx.crate[unit] = FORCED.get(unit) or original[fs[0]]
        srcs = [f for f in fs if not is_test_file(f)]
        free = (
            unit not in FORCED and unit not in HUB_FILES and unit != "server/dispatch"
        )
        if free and srcs and all(f in unplanned for f in srcs):
            ctx.movable.add(unit)
    return ctx, unplanned


def resolve(
    root: Path, manifest_path: Path
) -> tuple[Context, list[Classified], set[str]]:
    ctx, unplanned = build_context(root, manifest_path)
    placer = Placer(ctx, [r for r in ctx.refs if is_generic(ctx, r)])
    placer.run()
    ctx.decide = placer.stuck()
    labels = settle_test_moves(ctx)
    ctx.placement = {f: ctx.fcrate(f) for f in ctx.files}
    return ctx, labels, unplanned


def write_placement(
    path: Path, ctx: Context, moves: list[str], labels: list[Classified]
) -> None:
    placement = dict(ctx.placement)
    for rel in moves:
        placement[rel] = SERVER
    lines = [
        "# Generated by scripts/crate_split/resolve.py. File-to-crate authority.",
        "",
        "[files]",
    ]
    lines += [f'"{f}" = "{c}"' for f, c in sorted(placement.items())]
    quoted = ", ".join(f'"{f}"' for f in moves)
    lines += [
        "",
        "[test_moves]",
        "# land under crates/medulla-server/tests/",
        f"files = [{quoted}]",
    ]
    decide = ", ".join(f'"{u}"' for u in sorted(ctx.decide))
    lines += [
        "",
        "[decide]",
        "# units no placement could settle; see SEAMS.md",
        f"units = [{decide}]",
        "",
    ]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--root", type=Path, default=HERE.parents[1])
    ap.add_argument("--manifest", type=Path, default=HERE / "manifest.toml")
    ap.add_argument("--out-dir", type=Path, default=HERE)
    args = ap.parse_args(sys.argv[1:] if argv is None else argv)
    ctx, labels, unplanned = resolve(args.root, args.manifest)
    write_placement(args.out_dir / "placement.toml", ctx, sorted(ctx.moves), labels)
    from crate_split.seams import write_reports

    write_reports(args.out_dir, ctx, labels, unplanned, sorted(ctx.moves))
    print(f"{len(labels)} labelled references; placement written")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
