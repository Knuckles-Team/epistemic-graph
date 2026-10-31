"""Report writers for resolve.py: seams.json, SEAMS.md (MEDULLA-CONTRACT-R056)."""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from pathlib import Path
from typing import TYPE_CHECKING

from crate_split.units import HUB_FILES

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


def decide_section(ctx: Context, entries: list[Classified]) -> list[str]:
    """Units no placement could settle: one line each, at most 20."""
    by_unit: dict[str, list[Classified]] = defaultdict(list)
    for e in entries:
        by_unit[ctx.unit_of[e.dst]].append(e)
    lines = [f"## decide ({len(by_unit)} units, {len(entries)} refs)", ""]
    lines.append(
        "Unplaceable above the server-api floor. One line per unit; owner decides."
    )
    lines.append("")
    ranked = sorted(by_unit.items(), key=lambda kv: (-len(kv[1]), kv[0]))
    for unit, occ in ranked[:20]:
        users = Counter(ctx.fcrate(e.file) for e in occ)
        who = ", ".join(f"{c} x{n}" for c, n in users.most_common(3))
        lines.append(
            f"- `{unit}` (in {ctx.crate[unit]}) used by {who}: {where(occ, 2)}"
        )
    if len(ranked) > 20:
        lines.append(f"- ... {len(ranked) - 20} more units in seams.json")
    return [*lines, ""]


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
    """Unplanned units whose crate differs from their parent module's crate."""
    files = set(ctx.files)
    rows = []
    for unit, members in sorted(ctx.members.items()):
        if not any(m in unplanned for m in members) or unit in HUB_FILES:
            continue
        root_file = next(
            (f for f in members if f.endswith((f"{unit.split('/')[-1]}.rs", "mod.rs"))),
            members[0],
        )
        base = parent_module(root_file)
        parent = next((p for p in (f"{base}.rs", f"{base}/mod.rs") if p in files), None)
        if (
            parent
            and parent not in HUB_FILES
            and ctx.placement[parent] != ctx.placement[root_file]
        ):
            rows.append(
                f"- `{unit}`: {ctx.placement[root_file]} "
                f"(parent `{parent}` in {ctx.placement[parent]})"
            )
    head = f"## unplanned units placed away from their parent module ({len(rows)})"
    return [head, "", *rows, ""]


def parent_module(rel: str) -> str | None:
    """The file declaring this module (`dir.rs` or `dir/mod.rs`), by path."""
    parent = rel.rsplit("/", 1)[0] if "/" in rel else ""
    stem = rel.rsplit("/", 1)[-1]
    if stem == "mod.rs":
        parent = parent.rsplit("/", 1)[0]
    return parent or None


def reroot_section(ctx: Context, moves: list[str]) -> list[str]:
    """Files whose declaring parent module sits in another crate (must be none)."""
    files = set(ctx.files)
    rows = []
    for rel in ctx.files:
        base = parent_module(rel)
        parent = next(
            (p for p in (f"{base}.rs", f"{base}/mod.rs") if p in files and p != rel),
            None,
        )
        if parent is None or parent in HUB_FILES or rel in moves:
            continue
        if ctx.placement[parent] != ctx.placement[rel]:
            rows.append(f"- `{rel}` (parent `{parent}` in {ctx.placement[parent]})")
    return [f"## re-rooted modules ({len(rows)})", "", *rows, ""]


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
    if "decide" in by_cat:
        lines += decide_section(ctx, by_cat["decide"])
    if "test-move" in by_cat:
        lines += test_section(ctx, by_cat["test-move"], moves)
    if "pending-delete" in by_cat:
        lines += category_section("pending-delete", by_cat["pending-delete"])
    lines += reroot_section(ctx, moves)
    lines += unplanned_section(ctx, unplanned)
    (out / "SEAMS.md").write_text("\n".join(lines).rstrip("\n") + "\n", encoding="utf-8")
