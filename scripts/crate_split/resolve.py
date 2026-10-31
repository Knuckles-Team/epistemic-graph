#!/usr/bin/env python3
"""Resolve the file-to-crate placement for the crate split (MEDULLA-CONTRACT-R056).

Start from the manifest assignment and move each referenced file down to the highest
crate below-or-equal every crate that references it, while its own outgoing references
stay at or below the new crate. Repeat to a fixpoint and never move a file up.
References
that no placement can satisfy are classified as seams (traits in server-api), extracted
types or helpers, test moves, or `needs-seam`. Writes `placement.toml`, `seams.json` and
`SEAMS.md` next to this script.
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

API = "medulla-server-api"
SERVER = "medulla-server"
DISPATCH_HUBS = (
    "src/server/mod.rs",
    "src/server/dispatch.rs",
    "src/server/handlers/mod.rs",
    "src/server/dispatch/consensus.rs",
)
DISPATCH_HUB_DIRS = (
    "src/server/dispatch/router/",
    "src/server/dispatch/request_boundary/",
)
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
STATE_FILE = "src/server/state.rs"
HERE = Path(__file__).resolve().parent


@dataclass
class Context:
    manifest: Manifest
    files: list[str]
    original: dict[str, str]
    refs: list[Ref]
    placement: dict[str, str] = field(default_factory=dict)
    moves: set[str] = field(default_factory=set)

    def ok(self, src_crate: str, dst_crate: str) -> bool:
        return self.manifest.may_depend(src_crate, dst_crate)


@dataclass(frozen=True)
class Classified:
    file: str
    line: int
    item: str
    category: str
    detail: str
    dst: str


def is_pinned(rel: str) -> bool:
    """The facade roots stay in the engine crate."""
    return rel in ("src/lib.rs", "src/main.rs") or rel.startswith("src/bin/")


def is_test_file(rel: str) -> bool:
    name = rel.rsplit("/", 1)[-1]
    return (
        name.endswith(("_tests.rs", "tests.rs", "_fixtures.rs"))
        or name == "test_support.rs"
        or "/tests/" in rel
    )


def subject_of(rel: str, files: set[str]) -> str | None:
    """The source file a test file belongs to, when it can be inferred."""
    parent, _, name = rel.rpartition("/")
    candidates = []
    if name.endswith("_tests.rs"):
        stem = name[: -len("_tests.rs")]
        candidates = [f"{parent}/{stem}.rs", f"{parent}/{stem}/mod.rs"]
    else:
        base = parent.split("/tests")[0] if "/tests" in rel else parent
        candidates = [base + ".rs", base + "/mod.rs"]
    return next((c for c in candidates if c in files and c != rel), None)


def split_item(item: str) -> tuple[str, bool]:
    """(path up to the first capitalised segment, whether it names a type)."""
    segs = item.split("::")
    for i, seg in enumerate(segs):
        if seg[:1].isupper():
            return "::".join(segs[: i + 1]), True
    return item, False


def is_hub(dst: str) -> bool:
    return dst in DISPATCH_HUBS or dst.startswith(DISPATCH_HUB_DIRS)


def classify_hub(ref: Ref) -> tuple[str, str]:
    path, is_type = split_item(ref.item)
    last = ref.item.split("::")[-1]
    if last in DISPATCH_NAMES or ref.dst == "src/server/handlers/mod.rs":
        return "seam:dispatch", ref.dst
    if is_type:
        return "extract-type", path
    return "extract-helper", ref.item


def classify_pre(ctx: Context, ref: Ref) -> tuple[str, str] | None:
    """Rule-based label for a reference that must not move a file, else None."""
    cs, cd = ctx.original[ref.src], ctx.original[ref.dst]
    if cs == cd:
        return None
    if cd == DELETE:
        return "pending-delete", ref.dst
    if ref.src == STATE_FILE and ctx.manifest.level(cd) > ctx.manifest.level(API):
        return "seam:state-handle", ref.item
    if is_hub(ref.dst) and ctx.manifest.level(cs) < ctx.manifest.level(SERVER):
        return classify_hub(ref)
    if cs in TRAITS and cd in TRAITS:
        path, is_type = split_item(ref.item)
        return ("extract-type", path) if is_type else ("seam:" + TRAITS[cd], ref.item)
    return None


def meet(ctx: Context, crates: set[str]) -> str:
    """Highest crate below-or-equal every crate in the set."""
    cands = [c for c in ctx.manifest.crates if all(ctx.ok(s, c) for s in crates)]
    top = max(ctx.manifest.level(c) for c in cands)
    return sorted(c for c in cands if ctx.manifest.level(c) == top)[0]


class Placer:
    """Fixpoint file placement over the generic (unlabelled, non-test) references."""

    def __init__(self, ctx: Context, generic: list[Ref]):
        self.ctx = ctx
        self.by_dst: dict[str, list[Ref]] = defaultdict(list)
        self.by_src: dict[str, list[Ref]] = defaultdict(list)
        for ref in generic:
            self.by_dst[ref.dst].append(ref)
            self.by_src[ref.src].append(ref)

    def violations(self, dst: str) -> list[Ref]:
        p = self.ctx.placement
        return [r for r in self.by_dst[dst] if not self.ctx.ok(p[r.src], p[dst])]

    def target(self, dst: str) -> str | None:
        """New crate for `dst`, or None when it must stay."""
        p, ctx = self.ctx.placement, self.ctx
        if is_pinned(dst):
            return None
        want = meet(ctx, {p[r.src] for r in self.by_dst[dst]})
        if ctx.manifest.level(want) > ctx.manifest.level(p[dst]) or want == p[dst]:
            return None
        if all(ctx.ok(want, p[r.dst]) for r in self.by_src[dst]):
            return want
        return None

    def run(self) -> None:
        changed = True
        while changed:
            changed = False
            for dst in sorted(self.by_dst):
                if not self.violations(dst):
                    continue
                want = self.target(dst)
                if want:
                    self.ctx.placement[dst] = want
                    changed = True

    def blockers(self, dst: str) -> list[Ref]:
        p, ctx = self.ctx.placement, self.ctx
        want = meet(ctx, {p[r.src] for r in self.by_dst[dst]})
        return [r for r in self.by_src[dst] if not ctx.ok(want, p[r.dst])]


def unplanned_files(
    manifest_text: str, manifest: Manifest, files: list[str]
) -> set[str]:
    marked = set(re.findall(r'^\s*"([^"]+)",\s*# unplanned', manifest_text, re.M))
    result = set()
    for rel in files:
        hits = [(len(e), e) for e, _c in manifest.entries() if entry_matches(e, rel)]
        if hits and max(hits)[1] in marked:
            result.add(rel)
    return result


def final_labels(
    ctx: Context, pre: dict[int, tuple[str, str]], refs: list[Ref]
) -> list[Classified]:
    """Label each reference that still violates the layering after placement."""
    out = []
    p = ctx.placement
    for i, ref in enumerate(refs):
        if ctx.ok(p[ref.src], p[ref.dst]):
            continue
        if ref.in_test or is_test_file(ref.src) or is_test_file(ref.dst):
            cat, detail = "test-move", f"{p[ref.src]} -> {p[ref.dst]}"
        elif i in pre:
            cat, detail = pre[i]
        else:
            cat, detail = "needs-seam", f"{p[ref.src]} -> {p[ref.dst]}"
        out.append(Classified(ref.src, ref.line, ref.item, cat, detail, ref.dst))
    return out


def follow_tests(ctx: Context) -> None:
    fileset = set(ctx.files)
    for rel in ctx.files:
        if is_test_file(rel):
            subject = subject_of(rel, fileset)
            if subject:
                ctx.placement[rel] = ctx.placement[subject]


def resolve(
    root: Path, manifest_path: Path
) -> tuple[Context, list[Classified], set[str]]:
    manifest = load_manifest(manifest_path)
    files = list_src_files(root)
    owners = build_owner_map(manifest, files)
    ctx = Context(
        manifest,
        files,
        {f: c or SERVER for f, c in owners.items()},
        collect_refs(root, files),
    )
    ctx.placement = dict(ctx.original)
    pre = {}
    generic = []
    for i, ref in enumerate(ctx.refs):
        label = classify_pre(ctx, ref)
        testish = ref.in_test or is_test_file(ref.src) or is_test_file(ref.dst)
        if label:
            pre[i] = label
        elif not testish and ref.src != ref.dst:
            generic.append(ref)
    follow_tests(ctx)
    placer = Placer(ctx, generic)
    placer.run()
    follow_tests(ctx)
    labels = settle_test_moves(ctx, pre)
    text = manifest_path.read_text(encoding="utf-8")
    return ctx, labels, unplanned_files(text, manifest, files)


def settle_test_moves(
    ctx: Context, pre: dict[int, tuple[str, str]]
) -> list[Classified]:
    """Move test files that reach above their crate into the server crate's tests."""
    while True:
        labels = final_labels(ctx, pre, ctx.refs)
        fresh = {
            x.file for x in labels if x.category == "test-move" and is_test_file(x.file)
        }
        fresh -= ctx.moves
        if not fresh:
            return labels
        ctx.moves |= fresh
        for rel in fresh:
            ctx.placement[rel] = SERVER


def write_placement(
    path: Path, ctx: Context, moves: list[str], labels: list[Classified]
) -> None:
    placement = dict(ctx.placement)
    for rel in moves:
        placement[rel] = SERVER
    needs = sorted({x.file for x in labels if x.category == "needs-seam"})
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
    lines += [
        "",
        "[needs_seam]",
        "files = [" + ", ".join(f'"{f}"' for f in needs) + "]",
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--root", type=Path, default=HERE.parents[1])
    ap.add_argument("--manifest", type=Path, default=HERE / "manifest.toml")
    ap.add_argument("--out-dir", type=Path, default=HERE)
    args = ap.parse_args(sys.argv[1:] if argv is None else argv)
    ctx, labels, unplanned = resolve(args.root, args.manifest)
    moves = sorted(ctx.moves)
    write_placement(args.out_dir / "placement.toml", ctx, moves, labels)
    from crate_split.seams import write_reports

    write_reports(args.out_dir, ctx, labels, unplanned, moves)
    print(f"{len(labels)} labelled references; placement written")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
