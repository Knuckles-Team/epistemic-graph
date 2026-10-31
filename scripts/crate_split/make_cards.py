# ruff: noqa: E501
#!/usr/bin/env python3
"""Generate the self-contained lane cards in `lanes/` (MEDULLA-CONTRACT-R056).

One card per lane, derived from placement.toml and seams.json so the file lists and the
seam tables cannot drift from the placement. Run after `resolve.py`.
"""

from __future__ import annotations

import json
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from crate_split.ownership import DEFAULT_PLACEMENT, load_placement, load_test_moves

HERE = Path(__file__).resolve().parent
LANES = HERE / "lanes"
# (group, name, crate or None, spec row, predecessor note)
LANE_TABLE = [
    ("G0", "contract-delete", None, 1, "none; first lane"),
    (
        "G0",
        "scaffold",
        None,
        11,
        "none; lands in the same group as the contract delete",
    ),
    (
        "G0",
        "kernel",
        "medulla-kernel",
        3,
        "none; lands in the same group as the scaffold",
    ),
    ("G1", "store", "medulla-store", 2, "G0 landed"),
    ("G1", "server-api", "medulla-server-api", 5, "G0 landed; single lane"),
    ("G2", "persistence", "medulla-persistence", 6, "G1 landed"),
    ("G2", "raft", "medulla-raft", 4, "G1 landed"),
    ("G3", "h-compute", "medulla-h-compute", 7, "G2 landed"),
    ("G3", "h-ingest", "medulla-h-ingest", 7, "G2 landed"),
    ("G3", "h-graph", "medulla-h-graph", 7, "G2 landed"),
    ("G3", "h-jobs", "medulla-h-jobs", 7, "G2 landed"),
    ("G3", "wire", "medulla-wire", 9, "G2 landed"),
    ("G4", "seam-traits", None, 8, "G3 landed; single lane"),
    ("G5", "h-decide", "medulla-h-decide", 8, "G4 landed"),
    ("G5", "h-query", "medulla-h-query", 8, "G4 landed"),
    ("G5", "h-admin", "medulla-h-admin", 8, "G4 landed"),
    ("G5", "h-txn", "medulla-h-txn", 8, "G4 landed"),
    ("G6", "server-facade", "medulla-server", 10, "G5 landed"),
]
EXTRACT_NOTE = (
    "\nThis lane also owns every `extract-type`, `extract-helper` and `seam:state-handle` item "
    "in `SEAMS.md`, whatever file references it: move the item into `medulla-server-api` and "
    "rewrite its callers, so that later lanes find it below them.\n"
)
GENERIC_STOP = (
    "- Any `BLOCKED` or `decide` hit (from `rewrite_paths.py` or the seam table below) means "
    "STOP. Write `BLOCKED.md` in the lane directory (outside the repository) naming the item "
    "and the file:line, then exit. Do not improvise a seam.\n"
    "- Any file outside the list above, other than caller rewrites made by `rewrite_paths.py`, "
    "means STOP.\n"
    "- Cold pull-request CI timing out is not a blocker (MEDULLA-CONTRACT-R056.12).\n"
    "- Never run cargo locally, never `git stash`, never `git add -A` or `git add .`, never "
    "`--no-verify`, never force-push."
)


def load_data() -> tuple[dict[str, str], list[dict]]:
    placement = load_placement(DEFAULT_PLACEMENT)
    seams = json.loads((HERE / "seams.json").read_text(encoding="utf-8"))["refs"]
    return placement, seams


def files_section(files: list[str]) -> str:
    return "\n".join(f"- `{f}`" for f in files) if files else "- none (see Task)"


def seam_table(crate: str, files: set[str], seams: list[dict]) -> str:
    rows: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for s in seams:
        if s["file"] in files and s["category"] != "test-move":
            rows[
                (
                    s["category"],
                    s["detail"] if s["category"].startswith("extract") else s["item"],
                )
            ].append(s)
    if not rows:
        return "No seam, extraction or blocker touches this crate's files."
    out = ["| category | item | refs | first location |", "|---|---|---|---|"]
    for (cat, item), occ in sorted(
        rows.items(), key=lambda kv: (kv[0][0], -len(kv[1]), kv[0][1])
    ):
        out.append(
            f"| {cat} | `{item}` | {len(occ)} | {occ[0]['file']}:{occ[0]['line']} |"
        )
    return "\n".join(out)


def crate_card(
    group: str, name: str, crate: str, row: int, before: str, ctx: dict
) -> str:
    files = sorted(
        f for f, c in ctx["placement"].items() if c == crate and f.startswith("src/")
    )
    moves = [f for f in ctx["test_moves"] if crate == "medulla-server"]
    short = crate.replace("medulla-", "")
    table = seam_table(crate, set(files), ctx["seams"])
    extra = EXTRACT_NOTE if crate == "medulla-server-api" else ""
    return f"""# Lane {group}-{name}: move the files placed in `{crate}`

Spec: `MEDULLA-CONTRACT-R056.{row}`. Start after: {before}.

## Base branch rule
Create the worktree from `main` after the previous group landed
(`git worktree add <wt> -b lane/crate-split-{short} origin/main`). Never branch from another lane.

## Files to move ({len(files)}; exactly these, nothing else)
{files_section(files)}
{"" if not moves else chr(10) + "## Test files that land under `crates/medulla-server/tests/`" + chr(10) + files_section(moves)}

## Codemod (run in this order, from the worktree root)
1. `python3 scripts/crate_split/rewrite_paths.py --crate {crate} --moves > $SCRATCH/moves.sh`
2. `python3 scripts/crate_split/rewrite_paths.py --crate {crate} --mods > $SCRATCH/mods.txt`
3. `python3 scripts/crate_split/rewrite_paths.py --crate {crate}` rewrites `crate::<old path>` in the
   moved files and every caller, and points `placement.toml` at the new paths. Exit 1 prints the
   BLOCKED items and writes nothing.
4. `sh $SCRATCH/moves.sh` runs the `git mv` commands.
5. Apply `$SCRATCH/mods.txt`: add the `pub mod` lines to the new crate root and delete the listed
   `mod` declarations from the old parents. Move the crate's feature forwarding into its own
   `Cargo.toml` (the scaffold created the empty crate).
6. `python3 scripts/crate_split/rewrite_paths.py --crate {crate} --check` must report nothing.

## Seams this lane uses (from SEAMS.md)
{table}

Use the server-api trait or extracted item named in the table. Do not invent another seam.
{extra}
## Files it must not touch
Every file outside the list above, except caller rewrites from step 3. Do not change
`scripts/crate_split/*.toml` by hand; step 3 updates `placement.toml`. Do not copy helpers:
move them. Do not add re-exports. Widen `pub(crate)` to `pub` only where placement requires it.

## Gates (no local cargo)
- `python3 scripts/check_crate_graph.py` passes.
- `python3 scripts/crate_split/edges.py` shows no back-edge out of `{crate}` except the listed seams.
- `python3 scripts/check_spec_paths.py` passes; rewrite the `Spec:` test bindings and spec file paths
  that name moved files in the same commit, then regenerate spec status.
- `ruff check` and `ruff format --check` on touched Python.

## Land
```
eg-lane-run enqueue --lane land-{short} --worktree <wt> --test '-p {crate}::all()'
```
The change lands on a passing build-train run of its changed crates. Link the train result in the
pull request. Commit trailer: `Spec: MEDULLA-CONTRACT-R056.{row}`.

## STOP conditions
{GENERIC_STOP}
"""


def special_card(group: str, name: str, row: int, before: str) -> str:
    bodies = {
        "contract-delete": (
            "Delete `src/server/contract_wave.rs` and every reference to it (module declaration in "
            "`src/server/mod.rs`, uses in `dispatch/router/service_control.rs`, "
            "`handlers/admin/connector_pack/import/skill_files.rs`, "
            "`handlers/admin/connector_pack/import/modality_tests.rs`, `handlers/decide/shape.rs`, "
            "`handlers/work_item/lease_kind_policy.rs`). Do NOT delete `persistence/contract.rs`: it "
            "holds the `PersistenceBackend` trait and moves to server-api. Set `[delete] enforce = true` "
            "in `scripts/crate_split/manifest.toml` and remove the file's key from `placement.toml`."
        ),
        "scaffold": (
            "Create every empty crate listed in `scripts/crate_split/manifest.toml` with its own "
            "`Cargo.toml` (layering from the manifest, workspace member, feature forwarding stubs), an "
            "empty `src/lib.rs`, and facade re-export stubs in `medulla-engine`. One change, before any "
            "parallel lane. Dependencies follow the manifest levels only."
        ),
        "seam-traits": (
            "In `medulla-server-api` add `HandleCtx::dispatch(Method)` re-entry, the `MethodHandler` and "
            "`HandlerRegistry` traits, the `DecideApi`, `QueryApi`, `AdminApi`, `TxnApi`, `JobsApi`, "
            "`IngestApi`, `GraphApi`, `ComputeApi` and `WireApi` traits named in SEAMS.md, the "
            "`seam:state-handle` trait-object handles, and the `extract-type` and `extract-helper` items. "
            "Convert every `seam:*` reference listed in SEAMS.md to the trait call. Behaviour must be "
            "identical, including handler-chain order."
        ),
    }
    return f"""# Lane {group}-{name}

Spec: `MEDULLA-CONTRACT-R056.{row}`. Start after: {before}.

## Base branch rule
Create the worktree from `main` after the previous group landed
(`git worktree add <wt> -b lane/crate-split-{name} origin/main`).

## Task
{bodies[name]}

## Files to touch
Only the files named in the task and in `scripts/crate_split/SEAMS.md` for this lane. Seam table:
see `scripts/crate_split/SEAMS.md`, sections `seam:*`, `extract-type`, `extract-helper`.

## Gates (no local cargo)
- `python3 scripts/check_crate_graph.py` passes.
- `python3 scripts/crate_split/edges.py` shows only listed seams.
- `python3 scripts/check_spec_paths.py` passes.

## Land
```
eg-lane-run enqueue --lane land-{name} --worktree <wt> --test '-p medulla-engine::all()'
```
Link the train result in the pull request. Commit trailer: `Spec: MEDULLA-CONTRACT-R056.{row}`.

## STOP conditions
{GENERIC_STOP}
"""


def main() -> int:
    placement, seams = load_data()
    ctx = {
        "placement": placement,
        "seams": seams,
        "test_moves": load_test_moves(DEFAULT_PLACEMENT),
    }
    LANES.mkdir(exist_ok=True)
    for group, name, crate, row, before in LANE_TABLE:
        card = (
            crate_card(group, name, crate, row, before, ctx)
            if crate
            else special_card(group, name, row, before)
        )
        (LANES / f"{group}-{name}.md").write_text(card, encoding="utf-8")
    print(f"wrote {len(LANE_TABLE)} lane cards")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
