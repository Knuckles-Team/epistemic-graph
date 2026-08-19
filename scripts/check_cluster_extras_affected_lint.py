#!/usr/bin/env python3
"""Scope the heavy `cargo clippy --workspace --all-features` lint to pushes
that can actually reach the `cluster` (Raft/HA) or `full-extras`
(GPU-CUDA/ROS2) opt-in build layers.

WHY: the `cargo-clippy-all-features` pre-push hook was `always_run: true` on
every push, unconditionally compiling the `cluster` layer (`dep:openraft`)
and `full-extras` (`dep:cudarc` CUDA bindings, the CycloneDDS C/cmake build
via `dep:cyclonedds`, plus `dep:dust_dds`/`dep:tokio-tungstenite`) -- 55
crates that exist ONLY under those two features, none of them linked by the
`default`/`full` build every other local gate already lints (confirmed by
`cargo tree`; see AGENTS.md "Cargo Feature Flags"). That is a large, mostly
avoidable tax: MEASURED, this leg alone dwarfs the rest of the pre-push
chain combined, and the overwhelming majority of pushes never touch Raft or
GPU/ROS2 code at all.

AFFECTEDNESS is derived from the real dependency graph, not a hand-maintained
path list, so it self-updates as the crate/feature graph evolves:

  1. Resolve the workspace twice via `cargo metadata --format-version=1`:
     once with `full,ast-extended` (the shipped feature set -- what the
     everyday `cargo-clippy` hook already lints), once with `cluster,
     full-extras` also added.
  2. Diff, per workspace-member crate, the RESOLVED active feature set
     between the two runs. A crate whose active features differ is
     "reachable" from the opt-in layers: only the heavy leg can safely lint
     a change to it.
  3. Map every file the push actually changed (the `PRE_COMMIT_FROM_REF`..
     `PRE_COMMIT_TO_REF` range pre-commit hands a pre-push hook) to its
     owning crate (`crates/<name>/**` -> `<name>`; `src/**`/`build.rs` ->
     the root `epistemic-graph` facade, since `src/raft/`, the ROS2 bridge,
     etc. all live there).
  4. Run the heavy lint if ANY touched crate intersects the reachable set,
     or if `Cargo.toml`/`Cargo.lock` itself changed (the dependency graph
     moved -- too risky to reason about statically). Otherwise, skip it
     with an explicit, auditable rationale printed to stdout.

FAIL CLOSED, always: any failure to determine the changed-file range, run
`cargo metadata`, or parse its output runs the heavy lint. This script only
ever REMOVES the heavy lint when it can affirmatively prove the change
cannot reach `cluster`/`full-extras` code -- never on doubt, never on error.

`PRE_COMMIT_FROM_REF`/`PRE_COMMIT_TO_REF` are ONLY populated when pre-commit
itself is invoked BY git's real `pre-push` hook (it parses the pushed
ref/sha pairs off stdin per git's pre-push protocol -- see
`pre_commit/commands/hook_impl.py::_pre_push_ns`) -- never for a bare
`pre-commit run --hook-stage pre-push`/`--hook-stage manual` from the CLI,
and never for a brand-new branch with no upstream ancestor (pre-commit
itself falls back to `all_files=True` with no ref pair there). So the
fail-closed path above -- not a separate stage check -- already covers
`scripts/ci_parity.sh` (`pre-commit run --all-files --hook-stage
pre-push`/`--hook-stage manual`, both CLI-invoked) and a from-scratch
push: both run the heavy lint unconditionally, matching ci_parity.sh's own
stated purpose, "the one command that answers 'would CI pass?'". Narrowing
only ever activates for the real `git push` path this hook exists to gate.

THE FULL-FAT LEG STILL RUNS, unconditionally, in two places this script does
not touch:

  * `.github/workflows/rust-ci.yml` `lint-and-test` -- the SAME
    `cargo clippy --workspace --all-features --all-targets -- -D warnings`
    on EVERY push and EVERY pull request against `main`, independent of
    what any local hook decided.
  * `pre-commit run --all-files --hook-stage manual` / `--hook-stage
    pre-push` (`scripts/ci_parity.sh`, "would CI pass?") -- both CLI
    invocations lack a ref pair (see above), so this script always runs the
    heavy leg there, unconditionally, ignoring the diff.

So narrowing this hook moves COST off the pre-push developer loop; it does
not remove COVERAGE -- a change that breaks the `cluster`/`full-extras`
build still fails in CI on push, and any operator can still ask for the
exhaustive local check.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import push_gate_evidence  # noqa: E402

# The shipped feature set -- what every other local gate already lints
# (matches the `cargo-clippy` hook's `--features full`, plus `ast-extended`
# since that is also part of the published build, release-build.yml's
# `MATURIN_FEATURES`).
SHIPPED_FEATURES = "full,ast-extended"
# The two opt-in build layers this hook exists to guard. Kept as a single
# named constant (not scattered through the file) so a THIRD opt-in layer,
# if one is ever added, is one line to extend here -- the reachability
# analysis below is otherwise fully mechanical.
EXTRA_FEATURES = "cluster,full-extras"

HEAVY_CLIPPY_CMD = [
    "cargo",
    "clippy",
    "--workspace",
    "--all-features",
    "--all-targets",
    "--",
    "-D",
    "warnings",
]

_TAG = "[check_cluster_extras_affected_lint]"


def _log(msg: str) -> None:
    print(f"{_TAG} {msg}")


def run_heavy(reason: str) -> int:
    _log(f"running the full --all-features clippy -- {reason}")
    try:
        environment = push_gate_evidence.local_build_environment()
    except push_gate_evidence.EvidenceError as exc:
        _log(f"local Cargo environment is invalid ({exc}); running normally")
        environment = None
    return subprocess.run(HEAVY_CLIPPY_CMD, cwd=ROOT, env=environment).returncode


def _cargo_metadata(features: str) -> dict:
    proc = subprocess.run(
        [
            "cargo",
            "metadata",
            "--format-version=1",
            "--no-default-features",
            "--features",
            features,
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(proc.stdout)


def _workspace_member_features(meta: dict) -> dict[str, frozenset[str]]:
    """package name -> its own resolved active feature set (members only)."""

    member_ids = set(meta["workspace_members"])
    nodes = {n["id"]: n for n in meta["resolve"]["nodes"]}
    id_to_name = {p["id"]: p["name"] for p in meta["packages"]}
    result: dict[str, frozenset[str]] = {}
    for member_id in member_ids:
        node = nodes.get(member_id)
        if node is None:
            continue
        result[id_to_name[member_id]] = frozenset(node.get("features", []))
    return result


def reachable_from_extras() -> set[str]:
    """Workspace-member crate names whose resolved feature set changes when
    `cluster,full-extras` is added to the shipped feature set -- i.e. the
    crates a `cluster`/`full-extras`-only change can actually land in."""

    baseline = _workspace_member_features(_cargo_metadata(SHIPPED_FEATURES))
    extended = _workspace_member_features(
        _cargo_metadata(f"{SHIPPED_FEATURES},{EXTRA_FEATURES}")
    )
    affected = set()
    for name, ext_features in extended.items():
        if ext_features != baseline.get(name, frozenset()):
            affected.add(name)
    return affected


def _changed_files() -> list[str] | None:
    """Changed paths for the push range, or None if it cannot be determined
    (the caller must fail closed in that case)."""

    from_ref = os.environ.get("PRE_COMMIT_FROM_REF")
    to_ref = os.environ.get("PRE_COMMIT_TO_REF")
    if not from_ref or not to_ref:
        return None
    try:
        proc = subprocess.run(
            ["git", "diff", "--name-only", f"{from_ref}..{to_ref}"],
            cwd=ROOT,
            capture_output=True,
            text=True,
            check=True,
        )
    except subprocess.CalledProcessError:
        return None
    return [line for line in proc.stdout.splitlines() if line]


def _owning_crate(path: str) -> str | None:
    parts = Path(path).parts
    if not parts:
        return None
    if parts[0] == "crates" and len(parts) > 1:
        return parts[1]
    if parts[0] in ("src", "build.rs"):
        return "epistemic-graph"
    return None


def main() -> int:
    try:
        consumer_environment = push_gate_evidence.local_build_environment()
    except push_gate_evidence.EvidenceError as exc:
        _log(f"local Cargo environment is invalid ({exc}); cache reuse disabled")
        consumer_environment = None
    evidence = push_gate_evidence.EvidenceStore.current()
    try:
        reusable = evidence is not None and evidence.consume(
            push_gate_evidence.Selection.from_argv(
                "cargo-clippy-all-features",
                HEAVY_CLIPPY_CMD,
                kind="cargo",
                environment=consumer_environment,
            )
        )
    except (push_gate_evidence.EvidenceError, OSError):
        reusable = False
    if reusable:
        _log(
            "reusing the successful advisory all-features clippy selection "
            "from this pre-push invocation"
        )
        return 0

    files = _changed_files()
    if files is None:
        return run_heavy(
            "could not determine the push's changed-file range "
            "(PRE_COMMIT_FROM_REF/PRE_COMMIT_TO_REF unset or `git diff` failed) -- fail closed"
        )

    rust_relevant = [
        f
        for f in files
        if f in ("Cargo.toml", "Cargo.lock", "build.rs")
        or f.startswith("crates/")
        or f.startswith("src/")
    ]

    if not rust_relevant:
        _log(
            "no Rust-relevant files changed -- skipping the heavy --all-features clippy"
        )
        _log("the everyday `cargo-clippy` (full,ast-extended) hook already ran above")
        return 0

    if "Cargo.toml" in rust_relevant or "Cargo.lock" in rust_relevant:
        return run_heavy(
            "Cargo.toml/Cargo.lock changed -- the dependency graph itself moved"
        )

    touched_crates = {c for c in (_owning_crate(f) for f in rust_relevant) if c}

    try:
        affected = reachable_from_extras()
    except Exception as exc:  # noqa: BLE001 -- fail closed on ANY metadata/parse failure
        return run_heavy(
            f"could not compute the cluster/full-extras-reachable crate set ({exc!r}) -- fail closed"
        )

    hit = touched_crates & affected
    if hit:
        return run_heavy(
            f"touched crate(s) {sorted(hit)} are reachable from cluster/full-extras "
            f"(reachable set: {sorted(affected)})"
        )

    _log(
        f"touched crate(s) {sorted(touched_crates)} do not reach cluster/full-extras "
        f"(reachable set: {sorted(affected)}) -- skipping the heavy --all-features clippy"
    )
    _log("the everyday `cargo-clippy` (full,ast-extended) hook already ran above")
    _log(
        "the exhaustive leg still runs on every push/PR in rust-ci.yml, "
        "and locally via `pre-commit run --all-files --hook-stage manual`"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
