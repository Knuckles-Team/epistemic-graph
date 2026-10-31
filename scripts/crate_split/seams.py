"""Report writers for resolve.py: seams.json, SEAMS.md (MEDULLA-CONTRACT-R056)."""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from crate_split.resolve import Classified, Context

TRAIT_NOTE = {
    "seam:dispatch": "HandleCtx::dispatch re-entry, MethodHandler, HandlerRegistry",
    "seam:state-handle": "ServerState fields above server-api become trait handles",
}
CAP = 5


def where(entries: list[Classified], cap: int = CAP) -> str:
    shown = ", ".join(f"{e.file}:{e.line}" for e in entries[:cap])
    return shown + (f" (+{len(entries) - cap} more)" if len(entries) > cap else "")


def group_items(entries: list[Classified]) -> list[str]:
    by_item: dict[str, list[Classified]] = defaultdict(list)
    for e in entries:
        by_item[e.item].append(e)
    ranked = sorted(by_item.items(), key=lambda kv: (-len(kv[1]), kv[0]))
    return [f"- `{item}` x{len(occ)}: {where(occ)}" for item, occ in ranked]


def category_section(name: str, entries: list[Classified]) -> list[str]:
    note = TRAIT_NOTE.get(name, "")
    head = f"## {name} ({len(entries)} refs)" + (f" - {note}" if note else "")
    return [head, "", *group_items(entries), ""]


def extract_section(name: str, entries: list[Classified]) -> list[str]:
    """extract-type / extract-helper grouped by the extracted item path."""
    by_item: dict[str, list[Classified]] = defaultdict(list)
    for e in entries:
        by_item[e.detail].append(e)
    lines = [f"## {name} ({len(entries)} refs, {len(by_item)} items)", ""]
    lines.append("Items move down to `medulla-server-api` (file split where needed).")
    lines.append("")
    for item, occ in sorted(by_item.items(), key=lambda kv: (-len(kv[1]), kv[0])):
        lines.append(
            f"- `{item}` x{len(occ)} (defined in `{occ[0].dst}`): {where(occ, 3)}"
        )
    return [*lines, ""]


def needs_section(ctx: Context, entries: list[Classified]) -> list[str]:
    by_dst: dict[str, list[Classified]] = defaultdict(list)
    for e in entries:
        by_dst[e.dst].append(e)
    lines = [f"## needs-seam ({len(entries)} refs, {len(by_dst)} files)", ""]
    lines.append(
        "No placement satisfies the layering. Each file lists referencers and blockers."
    )
    lines.append("")
    for dst, occ in sorted(by_dst.items(), key=lambda kv: (-len(kv[1]), kv[0])):
        crates = Counter(ctx.placement[e.file] for e in occ)
        who = ", ".join(f"{c} x{n}" for c, n in crates.most_common())
        lines.append(
            f"- `{dst}` (in {ctx.placement[dst]}) referenced by {who}: {where(occ, 3)}"
        )
        lines += [f"    - blocked by `{b}`" for b in blockers(ctx, dst, occ)]
    return [*lines, ""]


def blockers(ctx: Context, dst: str, occ: list[Classified]) -> list[str]:
    from crate_split.resolve import meet

    want = meet(ctx, {ctx.placement[e.file] for e in occ})
    seen: Counter[str] = Counter()
    for ref in ctx.refs:
        bad = ref.src == dst and not ctx.ok(want, ctx.placement[ref.dst])
        if bad and not ref.in_test:
            seen[f"{ref.dst} ({ctx.placement[ref.dst]}) via {ref.item}"] += 1
    return [f"{k} x{n}" for k, n in seen.most_common(4)]


def test_section(
    ctx: Context, entries: list[Classified], moves: list[str]
) -> list[str]:
    per_file = Counter(e.file for e in entries)
    lines = [f"## test-move ({len(entries)} refs, {len(per_file)} files)", ""]
    lines.append(
        "Test code that reaches above its crate. Test files move to "
        "`crates/medulla-server/tests/`."
    )
    lines += ["Inline `#[cfg(test)]` modules move out of their file. Files:", ""]
    lines += [
        f"- `{f}` x{n}{' (file moves)' if f in moves else ' (inline)'}"
        for f, n in per_file.most_common()
    ]
    return [*lines, ""]


def unplanned_section(ctx: Context, unplanned: set[str]) -> list[str]:
    by_final: dict[str, list[str]] = defaultdict(list)
    for rel in sorted(unplanned):
        by_final[ctx.placement[rel]].append(rel)
    moved = sum(1 for r in unplanned if ctx.placement[r] != ctx.original[r])
    lines = [
        f"## unplanned placement ({len(unplanned)} files, {moved} moved)",
        "",
    ]
    for crate, fs in sorted(by_final.items()):
        lines.append(f"- {crate}: {len(fs)} files")
        dirs = Counter(f.rsplit("/", 1)[0] if "/" in f else f for f in fs)
        lines += [f"    - `{d}` ({n})" for d, n in dirs.most_common()]
    return [*lines, ""]


def parent_module(rel: str) -> str | None:
    """The file declaring this module (`dir.rs` or `dir/mod.rs`), by path."""
    parent = rel.rsplit("/", 1)[0] if "/" in rel else ""
    stem = rel.rsplit("/", 1)[-1]
    if stem == "mod.rs":
        parent = parent.rsplit("/", 1)[0]
    return parent or None


def reroot_section(ctx: Context) -> list[str]:
    """Moved files whose declaring parent module sits in another crate."""
    files = set(ctx.files)
    rows = []
    for rel in ctx.files:
        base = parent_module(rel)
        parent = next(
            (p for p in (f"{base}.rs", f"{base}/mod.rs") if p in files and p != rel),
            None,
        )
        if parent and ctx.placement[parent] != ctx.placement[rel]:
            rows.append((rel, parent))
    lines = [f"## re-rooted modules ({len(rows)} files)", ""]
    lines.append(
        "Declared in a parent module that lives in another crate. The file becomes "
        "a top-level module of its crate; review its `super::` paths."
    )
    lines.append("")
    return [
        *lines,
        *[f"- `{r}` (parent `{p}` in {ctx.placement[p]})" for r, p in rows],
        "",
    ]


def write_reports(
    out: Path,
    ctx: Context,
    labels: list[Classified],
    unplanned: set[str],
    moves: list[str],
) -> None:
    by_cat: dict[str, list[Classified]] = defaultdict(list)
    for e in labels:
        by_cat[e.category].append(e)
    counts = {c: len(v) for c, v in sorted(by_cat.items())}
    (out / "seams.json").write_text(
        json.dumps({"counts": counts, "refs": [e.__dict__ for e in labels]}, indent=1)
        + "\n",
        encoding="utf-8",
    )
    lines = [
        "# Seams, extractions and blockers",
        "",
        "Generated by `scripts/crate_split/resolve.py`.",
        "",
    ]
    lines += (
        ["| category | refs |", "|---|---|"]
        + [f"| {c} | {n} |" for c, n in counts.items()]
        + [""]
    )
    for name in sorted(c for c in by_cat if c.startswith("seam:")):
        lines += category_section(name, by_cat[name])
    for name in ("extract-type", "extract-helper"):
        if name in by_cat:
            lines += extract_section(name, by_cat[name])
    if "needs-seam" in by_cat:
        lines += needs_section(ctx, by_cat["needs-seam"])
    if "test-move" in by_cat:
        lines += test_section(ctx, by_cat["test-move"], moves)
    if "pending-delete" in by_cat:
        lines += category_section("pending-delete", by_cat["pending-delete"])
    lines += reroot_section(ctx)
    lines += unplanned_section(ctx, unplanned)
    (out / "SEAMS.md").write_text("\n".join(lines), encoding="utf-8")
