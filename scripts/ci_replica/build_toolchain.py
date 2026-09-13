"""External build-tool and cargo-feature toolchain resolution for the replica.

Two mirror-image checks that must never be confused (see each banner below):
GAP 2 asks whether the repo's own build config hard-depends on a binary no
workflow installs on the CI runner; GAP 4 asks whether a step's own cargo
features need a binary THIS host happens to lack.
"""

from __future__ import annotations

import re
import shlex
import shutil
from pathlib import Path

import tomllib

from ci_replica.registry import CARGO_TOML_PATH, REPO_ROOT

# ─────────────────────────────────────────────────────────────────────────
# GAP 2 — external build-tool dependency check.
#
# A replica that only checks COMMAND equivalence (does `cargo test ...` run
# the same text locally and in CI) can never catch "you depend on a binary
# the CI runner doesn't have" — the exact class of the sccache/mold break:
# this build host has both installed, so the failing command SUCCEEDS here.
# This check instead parses .cargo/config.toml for anything that makes a
# cargo invocation hard-depend on an external binary (rustc-wrapper, a
# target linker/runner, `-fuse-ld=<x>`/`-C linker=<x>` in rustflags) and
# verifies the binary is mentioned SOMEWHERE in the workflow YAML that would
# run cargo — in practice, an explicit install step. Deliberately not
# special-cased to "sccache": any wrapper/linker/runner binary is checked.
# ─────────────────────────────────────────────────────────────────────────

_TOML_SECTION_RE = re.compile(r"^\[(?P<name>[^\]]+)\]\s*$")
_TOML_KV_RE = re.compile(r"^(?P<key>[A-Za-z0-9_.'\"-]+)\s*=\s*(?P<value>.+?)\s*$")
_FUSE_LD_RE = re.compile(r"-fuse-ld=([A-Za-z0-9_.+-]+)")
_C_LINKER_RE = re.compile(r"-C\s*linker=([^\s\"']+)")


def _parse_cargo_config_sections(text: str) -> list[tuple[str, str, str]]:
    """Deliberately narrow, non-general TOML reader: yields
    (section, key, raw_value) for every `key = value` line found, tagged
    with the `[section]` header it falls under (empty string before the
    first header). NOT a full TOML parser — cargo config files are a
    narrow, well-known dialect (flat key=value under bracketed sections)
    and this only needs to find a handful of well-known keys, not
    round-trip arbitrary TOML. A `#`-comment is stripped outside of a
    multi-line array; a `rustflags = [\n  "...",\n]` array is reassembled
    onto one logical line via a bracket-depth counter before matching."""
    section = ""
    results: list[tuple[str, str, str]] = []
    pending: tuple[str, str] | None = None
    depth = 0
    for raw_line in text.splitlines():
        stripped = raw_line.split("#", 1)[0].strip()
        if pending is not None:
            pending = (pending[0], pending[1] + " " + stripped)
            depth += stripped.count("[") - stripped.count("]")
            if depth <= 0:
                results.append((section, pending[0].strip("'\""), pending[1]))
                pending, depth = None, 0
            continue
        header = _TOML_SECTION_RE.match(stripped)
        if header:
            section = header.group("name").strip("'\" ")
            continue
        kv = _TOML_KV_RE.match(stripped)
        if not kv:
            continue
        key, value = kv.group("key"), kv.group("value")
        depth = value.count("[") - value.count("]")
        if depth > 0:
            pending = (key, value)
            continue
        depth = 0
        results.append((section, key.strip("'\""), value))
    return results


def _wrapper_binary(section: str, key: str, value: str) -> tuple[str, str] | None:
    """The compiler wrapper every cargo invocation hard-depends on, if the
    config declares one — either `[build] rustc-wrapper` or the `[env]`
    `RUSTC_WRAPPER` entry cargo reads as its equivalent."""
    if key == "rustc-wrapper" and section in ("build", ""):
        name = value.strip("'\"")
        return (name, f"[{section or 'build'}] rustc-wrapper") if name else None
    if key.upper() == "RUSTC_WRAPPER" and section == "env":
        quoted = re.search(r"[\"']([^\"']+)[\"']", value)
        return (quoted.group(1), "[env] RUSTC_WRAPPER") if quoted else None
    return None


def _target_toolchain_binaries(
    section: str, key: str, value: str
) -> list[tuple[str, str]]:
    """External binaries one `[target.*]` section's link/run settings require:
    an explicit linker or runner, or a linker named inside rustflags."""
    if not section.startswith("target."):
        return []
    if key == "linker":
        name = value.strip("'\"")
        return [(Path(name).name, f"[{section}] linker")] if name else []
    if key == "runner":
        tokens = value.strip("'\"").split()
        return [(Path(tokens[0]).name, f"[{section}] runner")] if tokens else []
    if key != "rustflags":
        return []
    return [
        (m.group(1), f"[{section}] rustflags -fuse-ld")
        for m in _FUSE_LD_RE.finditer(value)
    ] + [
        (Path(m.group(1)).name, f"[{section}] rustflags -C linker")
        for m in _C_LINKER_RE.finditer(value)
    ]


def find_required_build_binaries(cargo_config_path: Path) -> list[tuple[str, str]]:
    """Scan .cargo/config.toml for external binaries the build hard-depends
    on. Returns (binary_name, human-readable source description) pairs.
    Returns [] if the file does not exist — most repos have no
    .cargo/config.toml at all, which is not a finding, just nothing to
    check."""
    if not cargo_config_path.is_file():
        return []
    found: list[tuple[str, str]] = []
    for section, key, raw_value in _parse_cargo_config_sections(
        cargo_config_path.read_text(encoding="utf-8")
    ):
        value = raw_value.strip()
        wrapper = _wrapper_binary(section, key, value)
        if wrapper is not None:
            found.append(wrapper)
        found.extend(_target_toolchain_binaries(section, key, value))
    return found


def _binary_referenced_in_workflow(binary: str, workflow_text: str) -> bool:
    """True if `binary` appears anywhere in a workflow file's raw YAML text
    — in practice this means an explicit install step (`apt-get install
    <bin>`, a `<bin>-action` marketplace action, `cargo install <bin>`,
    etc.). Deliberately a broad substring-on-word-boundary match rather
    than trying to parse every possible install-step shape: if a build tool
    never appears in a workflow's text AT ALL, nothing in that workflow
    ever installs it, full stop."""
    return re.search(rf"\b{re.escape(binary)}\b", workflow_text) is not None


def check_build_tool_dependencies(
    cargo_config_path: Path, workflow_texts: dict[str, str]
) -> list[str]:
    """Returns a list of human-readable problems (empty = clean). Each
    problem names the missing binary, where the .cargo/config.toml
    dependency comes from, and which workflow file(s) run cargo but never
    mention the binary anywhere in their YAML."""
    binaries = find_required_build_binaries(cargo_config_path)
    if not binaries:
        return []
    problems = []
    for binary, source in binaries:
        missing_in = sorted(
            wf
            for wf, text in workflow_texts.items()
            if re.search(r"\bcargo\b", text)
            and not _binary_referenced_in_workflow(binary, text)
        )
        if missing_in:
            try:
                display_path = cargo_config_path.relative_to(REPO_ROOT)
            except ValueError:
                display_path = cargo_config_path
            problems.append(
                f"'{binary}' (required by {source} in {display_path}) is never "
                f"installed or otherwise referenced in: {', '.join(missing_in)}. cargo "
                f"hard-errors "
                f"(does not soft-fall-back) if a configured "
                f"rustc-wrapper/linker/runner binary is "
                f"not on PATH — add an explicit install step to those workflow(s), or "
                f"remove the "
                f"{source} setting."
            )
    return problems


# ─────────────────────────────────────────────────────────────────────────
# GAP 4 — local-host external-toolchain detection for a `run:` step's own
# cargo invocation.
#
# This is the MIRROR IMAGE of GAP 2 above (check_build_tool_dependencies),
# not a duplicate of it — the two must never be confused or let one weaken
# the other:
#
#   * GAP 2 (runner-side): does this repo's OWN build config (.cargo/
#     config.toml: rustc-wrapper/linker/runner/rustflags) hard-depend on a
#     binary that NONE of the workflow files ever install on the CI RUNNER?
#     That is always a real defect — every runner is an ephemeral, shared,
#     reproducible box; if the workflow never installs the tool, the build
#     WILL break there. It fails `--consistency-check`, i.e. the whole gate,
#     loudly. (The incident this closes: commit 652f91c's `rustc-wrapper =
#     "sccache"` with no install step anywhere.)
#   * GAP 4 (local-side, here): does a `run:` step's cargo invocation name a
#     cargo FEATURE (`--features`/`-F`/`--all-features`) that, per this
#     repo's OWN root Cargo.toml, needs an external binary to COMPILE, that
#     THIS ONE DEV HOST happens not to have on PATH right now? That is never
#     a defect — CI-hosted runners, or a future host, may have it; only the
#     one machine running this replica right now doesn't. It is classified
#     NOT_VALIDATED_LOCALLY (already a NON_BLOCKING_STATUS) — reported
#     loudly, never counted as a pass, never fails the gate.
#
# The detection is deliberately NOT keyed on a job or workflow name (a
# `if job == "feature-matrix"` special-case was explicitly rejected for
# this — a rename, reorder, or a brand new job invoking the same feature
# would silently blind it). Instead it reads the ACTUAL shell text of the
# step being classified, extracts whatever `--features`/`-F`/
# `--all-features` flags cargo itself would see, resolves them through the
# root Cargo.toml's real `[features]` graph (parsed with the stdlib TOML
# parser — this table is standard, well-formed TOML, unlike the narrow
# `.cargo/config.toml` dialect GAP 2 hand-parses above), and checks each
# resolved feature against TOOLCHAIN_FEATURE_REQUIREMENTS.
#
# TOOLCHAIN_FEATURE_REQUIREMENTS is intentionally SHORT and evidence-backed,
# not a guess: every entry is verified against this repo's own documented
# design AND, where practical, empirically. Two verified findings that
# shaped it:
#   * `ros2-rmw` (Cargo.toml ~line 1009-1011) pulls `cyclonedds-rust-sys`,
#     whose build.rs vendors the CycloneDDS C sources and configures+builds
#     them with `cmake` at COMPILE time — a real, unconditional local
#     build-time dependency. Listed below.
#   * `gpu-cuda` is DELIBERATELY NOT listed, even though it is the feature
#     named in this task's own problem statement as needing `nvcc`. Cargo.
#     toml says outright (line ~157-158, ~1020-1021) that it "builds clean
#     everywhere via dynamic-loading" — verified by reading cudarc 0.17.8's
#     own build.rs (only its `cuda-version-from-build-system` feature path
#     shells out to `nvcc --version`; this repo pins the fixed
#     `cuda-12060` feature instead, so that path never runs) AND by
#     actually running `cargo check -p eg-ann --no-default-features
#     --features gpu-cuda` on a host with no nvcc anywhere on PATH, which
#     SUCCEEDED. `ros2-dds` (pure-Rust Dust DDS) and `ros2-bridge`
#     (pure-Rust tokio-tungstenite) build clean everywhere for the same
#     reason cudarc does: no C/GPU toolchain in their dependency graph at
#     all. Listing any of these three would be a FALSE POSITIVE — silently
#     downgrading a step that actually compiles fine here from a real,
#     counted RUN to an uncounted NOT_VALIDATED_LOCALLY is exactly the
#     coverage regression this whole script exists to prevent (see the
#     module docstring: a skip must always be true, never a guess).
# ─────────────────────────────────────────────────────────────────────────

TOOLCHAIN_FEATURE_REQUIREMENTS: dict[str, tuple[tuple[str, str], ...]] = {
    "ros2-rmw": (
        (
            "cmake",
            "ros2-rmw pulls the cyclonedds-rust-sys crate, whose build.rs "
            "vendors the CycloneDDS C sources and configures+builds them "
            "with cmake at compile time (root Cargo.toml, the ros2-rmw "
            "feature's doc comment, ~line 1009-1011) — this is unconditional "
            "at build time, independent of whether a live ROS2/rmw daemon "
            "is present to actually talk to.",
        ),
        (
            "cc",
            "the same vendored CycloneDDS C build cyclonedds-rust-sys "
            "performs for ros2-rmw also needs a C compiler on PATH.",
        ),
    ),
}


def _load_cargo_features(
    cargo_toml_path: Path = CARGO_TOML_PATH,
) -> dict[str, list[str]]:
    """Parse a Cargo.toml's `[features]` table with the stdlib TOML parser
    (this is ordinary, well-formed TOML — unlike .cargo/config.toml's
    narrower dialect, no hand-rolled parser is needed or appropriate here).
    Returns {} if the file is missing or carries no `[features]` table."""
    if not cargo_toml_path.is_file():
        return {}
    with open(cargo_toml_path, "rb") as f:
        doc = tomllib.load(f)
    return doc.get("features", {}) or {}


def _feature_names(value: str) -> set[str]:
    """Comma- AND space-separated feature lists are both cargo-valid."""
    return {v.strip() for v in re.split(r"[,\s]+", value) if v.strip()}


def _extract_requested_features(run_text: str) -> tuple[set[str], bool]:
    """Tokenize a step's shell text the way a shell would (falling back to a
    plain whitespace split if it contains something shlex can't tokenize,
    e.g. an unbalanced quote from a stripped GHA expression) and pull out
    every `--features`/`-F` value plus whether `--all-features` appears
    anywhere. Deliberately tolerant of multiple cargo invocations in one
    step (`&&`-chained) — every occurrence in the whole text is unioned."""
    try:
        tokens = shlex.split(run_text, posix=True)
    except ValueError:
        tokens = run_text.split()

    features: set[str] = set()
    all_features = False
    i = 0
    while i < len(tokens):
        tok = tokens[i]
        if tok == "--all-features":
            all_features = True
        elif tok in ("--features", "-F") and i + 1 < len(tokens):
            # The value token is consumed here so it is never re-read as a flag.
            features |= _feature_names(tokens[i + 1])
            i += 1
        elif tok.startswith("--features="):
            features |= _feature_names(tok[len("--features=") :])
        i += 1
    return features, all_features


def _expand_features(
    requested: set[str], feature_table: dict[str, list[str]], all_features: bool
) -> set[str]:
    """Transitively resolve a set of requested cargo feature names through
    the workspace `[features]` graph, e.g. `full-extras` -> {full-extras,
    full, gpu-cuda, ros2-bridge, ros2-dds, ros2-rmw, ...}. `--all-features`
    seeds the closure with every feature the table defines, matching
    cargo's own semantics for that flag. An implied entry of the form
    `dep:pkg` (optional-dependency activation) or `pkg/feature` /
    `pkg?/feature` (a DIFFERENT crate's feature) is not itself a name in
    THIS table, so it naturally stops the walk there rather than needing
    special-casing — only entries that are themselves keys in this
    workspace's own feature table are followed further."""
    if all_features:
        requested = requested | set(feature_table)
    seen: set[str] = set()
    stack = list(requested)
    while stack:
        f = stack.pop()
        if f in seen:
            continue
        seen.add(f)
        for implied in feature_table.get(f, []):
            name = implied.split("/", 1)[0].rstrip("?")
            if name.startswith("dep:"):
                continue
            if name in feature_table and name not in seen:
                stack.append(name)
    return seen


def check_toolchain_requirements(
    run_text: str, feature_table: dict[str, list[str]]
) -> str | None:
    """Returns a human reason string naming the first missing required tool
    if `run_text` invokes cargo requesting, directly or transitively, a
    workspace feature TOOLCHAIN_FEATURE_REQUIREMENTS documents as needing an
    external build-time tool not currently on PATH — else None (nothing
    required, or everything required is present). Looks only at what the
    step's OWN command line actually names; no job/step name is consulted,
    so this applies uniformly to every RUN step in every workflow."""
    if "cargo" not in run_text:
        return None
    requested, all_features = _extract_requested_features(run_text)
    if not requested and not all_features:
        return None
    for feature in sorted(_expand_features(requested, feature_table, all_features)):
        for tool, why in TOOLCHAIN_FEATURE_REQUIREMENTS.get(feature, ()):
            if shutil.which(tool) is None:
                return (
                    f"{tool!r} not on PATH -- required to compile the {feature!r} "
                    f"feature here "
                    f"({why}). NOT a CI defect: this host lacks the toolchain, the "
                    f"build config "
                    f"does not lack an install step (see check_build_tool_dependencies "
                    f"for that "
                    f"check)."
                )
    return None
