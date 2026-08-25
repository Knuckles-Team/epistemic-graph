#!/usr/bin/env python3
"""Configure identity-neutral Rust source paths without discarding build flags.

Rust can retain source and toolchain paths in release binaries even when debug
information is stripped.  This helper converts any existing ``RUSTFLAGS`` to
Cargo's unambiguous unit-separator representation, preserves an existing
``CARGO_ENCODED_RUSTFLAGS`` verbatim, and appends deterministic
``--remap-path-prefix`` flags for the checkout and build-user directories. It
also appends an equivalent C/C++ prefix-map option to existing CFLAGS/CXXFLAGS
for native dependencies compiled by Cargo build scripts.

``rustc``'s ``--remap-path-prefix`` is always accepted; the C/C++ side is not.
``-ffile-prefix-map`` (remaps both debug info and ``__FILE__``/macro text) is a
GCC 8+ / recent-Clang option -- older or cross toolchains (notably the
manylinux aarch64 cross-gcc used by the release wheel matrix) reject it
outright with "unrecognized command line option", which fails the whole
build rather than merely leaving a path unmapped. This module therefore
feature-detects the target compiler before emitting the flag: it probes for
``-ffile-prefix-map`` support, falls back to the far-older ``-fdebug-prefix-map``
(supported since GCC 4.4; remaps debug info only, NOT ``__FILE__``/macro
text -- a strictly weaker privacy guarantee), and omits the flag entirely
(logging why) if neither is accepted or the target compiler cannot be
located at all. Flags are emitted into a per-target ``CFLAGS_<target>``
variable when a target is known, rather than the generic ``CFLAGS``, so one
toolchain's probe result can never leak into another's build.

The generated values belong in an ephemeral build environment.  The helper
never prints the concrete source prefixes it discovers.
"""

from __future__ import annotations

import argparse
import os
import shlex
import shutil
import subprocess
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path, PurePath

UNIT_SEPARATOR = "\x1f"

# More-specific locations are assigned distinct neutral prefixes.  Any overlap
# is sorted longest-first before flags are emitted so a checkout below HOME does
# not get collapsed into the less useful /build/home mapping.
ENVIRONMENT_ROOTS: tuple[tuple[str, str], ...] = (
    ("GITHUB_WORKSPACE", "/build/source"),
    ("CARGO_MANIFEST_DIR", "/build/source"),
    ("PWD", "/build/source"),
    ("CARGO_TARGET_DIR", "/build/cargo-target"),
    ("RUNNER_WORKSPACE", "/build/runner-workspace"),
    ("CARGO_HOME", "/build/cargo"),
    ("RUSTUP_HOME", "/build/rustup"),
    ("RUNNER_TOOL_CACHE", "/build/tools"),
    ("RUNNER_TEMP", "/build/temp"),
    ("LOCALAPPDATA", "/build/local-data"),
    ("APPDATA", "/build/app-data"),
    ("USERPROFILE", "/build/home"),
    ("HOME", "/build/home"),
)

# The linux-x86_64 / linux-aarch64 release legs build via `PyO3/maturin-action`
# with `manylinux: 2_28`, which does not compile on the outer GitHub runner at
# all: it hands the whole build off to a throwaway `docker run` of the
# manylinux image. That container always runs as root, so its `HOME` is
# unconditionally `/root` and (because Cargo defaults `CARGO_HOME`/`RUSTUP_HOME`
# from `HOME` when unset) its Cargo registry cache is `/root/.cargo/registry`.
# This script only ever runs on the OUTER runner, so no environment variable it
# can read ever names `/root` -- the outer runner's own `HOME` is something
# else entirely (e.g. `/home/runner`). Worse, `maturin-action` explicitly
# excludes `CARGO_HOME` from the env vars it forwards into the container (see
# its `FORBIDDEN_ENVS`), specifically so the container does not try to reuse
# the outer runner's host-path Cargo state -- which means the container's
# `/root/.cargo` is not just unremapped, it is *guaranteed* by the action's own
# design on every manylinux leg, every run, regardless of anything this script
# or its caller does.
#
# A dependency crate fetched fresh into that container's registry cache embeds
# `/root/.cargo/registry/src/<index>/<crate>-<version>/src/....rs` into
# `file!()`/`#[track_caller]`/panic-location strings at compile time. Because
# no `--remap-path-prefix`/`-ffile-prefix-map`/`-fdebug-prefix-map` flag this
# script emits ever targets `/root` -- ``path_remaps()`` is the single source
# of (source, replacement) pairs and every prefix-map emitter below draws from
# it unconditionally, regardless of which native macro flag was selected --
# that literal survives untouched into the compiled
# `epistemic-graph-server` / kernel-tool binaries and the `numeric` cdylib --
# confirmed against a real release wheel's `linux-x86_64` leg, where every one
# of the five compiled/linked members (the four `.data/scripts/*` binaries and
# `epistemic_graph/numeric.abi3.so`) trips `check_wheel_privacy.py`'s
# `privileged-home-prefix` pattern category. That category exists specifically
# to catch `/root` and `/github/home` by broad PATTERN match (not exact-prefix
# match) precisely because no per-run environment variable can describe them
# ahead of time -- this entry closes the corresponding gap on the build side so
# the leak is prevented at compile time instead of merely being caught (but not
# fixable) by that post-hoc audit.
#
# These roots are therefore denied UNCONDITIONALLY, independent of any
# environment variable this process can observe. `/github/home` is included
# alongside `/root` for the same reason: GitHub's own container actions also
# default `HOME` to `/github/home` when they run as a containerized action
# rather than a composite/script step, so it is exactly the same class of
# container convention this script cannot otherwise see. Both reuse the
# existing `/build/home` neutral target rather than minting a new alias, which
# keeps them indistinguishable in the normalized SBOM from an ordinary HOME
# leak -- the correct level of detail to expose.
ALWAYS_DENIED_ROOTS: tuple[tuple[str, str], ...] = (
    ("/root", "/build/home"),
    ("/github/home", "/build/home"),
)


def _safe_root(value: str | None) -> str | None:
    """Return a usable build prefix, excluding roots and malformed values."""

    if not value or "\x00" in value or "\n" in value or "\r" in value:
        return None
    candidate = value.rstrip("/\\")
    if candidate in {"", "/"}:
        return None
    if len(candidate) == 2 and candidate[1:] == ":":
        return None
    return candidate


def _existing_flags(environ: Mapping[str, str]) -> list[str]:
    encoded = environ.get("CARGO_ENCODED_RUSTFLAGS", "")
    if encoded:
        return encoded.split(UNIT_SEPARATOR)
    plain = environ.get("RUSTFLAGS", "")
    return shlex.split(plain, posix=os.name != "nt") if plain else []


def _already_remapped(flags: Sequence[str], source: str) -> bool:
    prefix = "--remap-path-prefix="
    for flag in flags:
        if not flag.startswith(prefix):
            continue
        mapping = flag[len(prefix) :]
        if mapping.partition("=")[0] == source:
            return True
    return False


def path_remaps(
    environ: Mapping[str, str] | None = None,
    *,
    checkout: str | PurePath | None = None,
) -> tuple[tuple[str, str], ...]:
    """Return concrete-to-neutral mappings, ordered most-specific first."""

    env = os.environ if environ is None else environ
    candidates: list[tuple[str, str]] = []

    explicit_checkout = _safe_root(str(checkout)) if checkout is not None else None
    if explicit_checkout:
        candidates.append((explicit_checkout, "/build/source"))

    for name, replacement in ENVIRONMENT_ROOTS:
        source = _safe_root(env.get(name))
        if source:
            candidates.append((source, replacement))

    candidates.extend(ALWAYS_DENIED_ROOTS)

    unique: dict[str, tuple[str, str]] = {}
    for source, replacement in candidates:
        key = source.casefold() if "\\" in source or ":" in source else source
        unique.setdefault(key, (source, replacement))
    return tuple(sorted(unique.values(), key=lambda item: (-len(item[0]), item[0])))


# MSVC `link.exe` stamps a wall-clock `TimeDateStamp` into the PE header and a
# freshly-generated GUID into the CodeView debug directory on EVERY link, so two
# byte-identical inputs still produce two different binaries. `/Brepro` makes both
# content-derived instead, which is what the release workflow's
# "Require byte-identical folded wheels" step needs on the windows-x86_64 leg.
# `strip = true` does NOT cover this: it removes symbols, not the PE timestamp.
# Emitted ONLY for the MSVC target -- these are `link.exe` switches and would be
# rejected by `ld`/`lld` on the Linux and macOS legs.
_MSVC_TARGET = "x86_64-pc-windows-msvc"
_MSVC_DETERMINISTIC_LINK_FLAGS: tuple[str, ...] = ("-Clink-arg=/Brepro",)


def _deterministic_link_flags(target: str | None) -> tuple[str, ...]:
    """Linker flags required for a byte-reproducible link on `target`.

    Only MSVC needs one today; every other target this fleet builds already
    links reproducibly under the existing `SOURCE_DATE_EPOCH` +
    `--remap-path-prefix` + `codegen-units = 1` configuration (proven by the
    linux-x86_64/linux-aarch64/macos-aarch64 legs passing the same gate).
    """

    if target == _MSVC_TARGET:
        return _MSVC_DETERMINISTIC_LINK_FLAGS
    return ()


def encoded_rustflags(
    environ: Mapping[str, str] | None = None,
    *,
    checkout: str | PurePath | None = None,
    target: str | None = None,
) -> tuple[str, int, int]:
    """Return encoded flags plus preserved/remap counts for safe reporting."""

    env = os.environ if environ is None else environ
    existing = _existing_flags(env)
    remaps = [
        f"--remap-path-prefix={source}={replacement}"
        for source, replacement in path_remaps(env, checkout=checkout)
        if not _already_remapped(existing, source)
    ]
    link = [
        flag for flag in _deterministic_link_flags(target) if flag not in existing
    ]
    return (
        UNIT_SEPARATOR.join([*existing, *remaps, *link]),
        len(existing),
        len(remaps),
    )


# Compiler-invocation env var that governs each *FLAGS variable, mirroring the
# `cc` crate's own CC/CXX split (it never looks at a "CFLAGS compiler").
_COMPILER_ENV_VARS: dict[str, str] = {"CFLAGS": "CC", "CXXFLAGS": "CXX"}

# Native prefix-map macros to try, most-capable first. `-ffile-prefix-map`
# (GCC 8+, recent Clang) remaps both debug info AND `__FILE__`/macro text.
# `-fdebug-prefix-map` (GCC 4.4+, all Clang) remaps debug info only.
_PREFIX_MAP_FLAG_NAMES: tuple[str, ...] = ("file-prefix-map", "debug-prefix-map")


def _resolve_probe_compiler(
    variable: str, target: str | None, environ: Mapping[str, str]
) -> str | None:
    """Best-effort emulation of the `cc` crate's own compiler resolution.

    Used ONLY to decide which prefix-map flag (if any) is safe to emit --
    never to invoke a real build. Returns ``None`` when no candidate binary
    can be located on this host's ``PATH``, which callers MUST treat as
    "unverifiable", not as "supported". That happens routinely for the
    manylinux release legs: this script runs on the outer GitHub runner
    *before* `PyO3/maturin-action` spins up the container that owns the real
    target compiler, so a cross-target binary frequently does not exist yet
    at probe time -- see `native_prefix_flags`'s handling of that case.
    """

    # `variable` may be the bare "CFLAGS"/"CXXFLAGS" or a target-scoped
    # "CFLAGS_<target>" (see `_scoped_flags_variable`) -- match by prefix so
    # both resolve to the same compiler-env-var family.
    compiler_var = next(
        (cc for flags, cc in _COMPILER_ENV_VARS.items() if variable.startswith(flags)),
        None,
    )
    if compiler_var is None:
        return None

    candidates: list[str] = []
    if target:
        candidates.append(f"{compiler_var}_{target}")
        candidates.append(f"{compiler_var}_{target.replace('-', '_')}")
        candidates.append("TARGET_" + compiler_var)
    candidates.append(compiler_var)

    for name in candidates:
        value = environ.get(name)
        if not value:
            continue
        parts = shlex.split(value, posix=os.name != "nt")
        if parts and shutil.which(parts[0]):
            return parts[0]

    # No override configured -- fall back to the `cc` crate's own default
    # cross-compiler naming convention (a target-triple-prefixed binary).
    # Deliberately NOT falling back further to a bare "cc"/"gcc" here when a
    # target was requested: that generic binary is the HOST's own compiler,
    # unrelated to the cross toolchain that will actually build this target,
    # and trusting it is exactly the class of bug this function exists to
    # prevent (a probe result from the wrong compiler leaking into a
    # different toolchain's build). Falling through to `return None` instead
    # correctly marks the target compiler as unverifiable.
    if target:
        if compiler_var == "CC" and shutil.which(f"{target}-gcc"):
            return f"{target}-gcc"
        if compiler_var == "CXX" and shutil.which(f"{target}-g++"):
            return f"{target}-g++"
        return None

    # No target given at all -- this is a host-native (non-cross) build, so
    # the platform-generic compiler on PATH genuinely IS the one that will
    # run.
    for generic in (("cc", "gcc") if compiler_var == "CC" else ("c++", "g++")):
        if shutil.which(generic):
            return generic
    return None


def _probe_prefix_map_flag(compiler: str, flag_name: str) -> bool:
    """Return whether ``compiler`` accepts ``-f{flag_name}=a=b``.

    Never raises: a missing binary, a timeout, or any invocation error is
    treated as unsupported (fail closed -- we only emit a flag we have
    positive evidence for).
    """

    probe_flag = f"-f{flag_name}=/probe/a=/probe/b"
    try:
        result = subprocess.run(
            [compiler, probe_flag, "-E", "-x", "c", os.devnull],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            timeout=15,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return False
    if result.returncode != 0:
        return False
    stderr = result.stderr.decode("utf-8", "replace").lower()
    # Belt-and-suspenders: some drivers (notably older MSVC-style front ends)
    # exit 0 while still printing an "unknown option ignored" warning.
    return not any(marker in stderr for marker in ("unrecognized", "unknown option", "ignoring"))


def _select_prefix_map_flag(
    compiler: str | None, *, probe: object = _probe_prefix_map_flag
) -> tuple[str | None, str | None]:
    """Pick the best prefix-map macro this compiler has proven it accepts.

    Returns ``(flag_name, note)``. ``flag_name`` is one of
    ``_PREFIX_MAP_FLAG_NAMES`` or ``None`` (omit the flag entirely); ``note``
    is a human-readable explanation to log whenever the strongest option
    wasn't used, or ``None`` when ``-ffile-prefix-map`` was confirmed.
    """

    if compiler is None:
        # Can't probe what doesn't exist on this host yet (see
        # `_resolve_probe_compiler`'s docstring). Rather than gamble on the
        # newer flag against an unverified toolchain, use the one with the
        # longest support history -- but say so, since this is a judgment
        # call, not a confirmed fact.
        return "debug-prefix-map", (
            "no target compiler could be located to probe (likely a "
            "container-based cross build whose compiler is created by a "
            "later step); conservatively using -fdebug-prefix-map (GCC 4.4+) "
            "instead of the unverified -ffile-prefix-map (GCC 8+) -- this "
            "remaps debug info only, not __FILE__/macro text"
        )
    if probe(compiler, "file-prefix-map"):
        return "file-prefix-map", None
    if probe(compiler, "debug-prefix-map"):
        return "debug-prefix-map", (
            f"target compiler ({compiler!r}) rejects -ffile-prefix-map; "
            "falling back to -fdebug-prefix-map, which remaps debug info "
            "only, not __FILE__/macro text"
        )
    return None, (
        f"target compiler ({compiler!r}) accepts neither -ffile-prefix-map "
        "nor -fdebug-prefix-map; omitting the native prefix-map flag for "
        "this build (rustc's --remap-path-prefix still applies to Rust code)"
    )


def native_prefix_flags(
    variable: str,
    environ: Mapping[str, str] | None = None,
    *,
    checkout: str | PurePath | None = None,
    target: str | None = None,
    probe: object = _probe_prefix_map_flag,
) -> str:
    """Preserve native compiler flags and append source-prefix remapping.

    The prefix-map macro itself is feature-detected against the resolved
    target compiler (see `_select_prefix_map_flag`) so this function can
    never emit a flag known to be rejected by the compiler that will
    actually process it.
    """

    env = os.environ if environ is None else environ
    existing = shlex.split(env.get(variable, ""), posix=os.name != "nt")
    compiler = _resolve_probe_compiler(variable, target, env)
    flag_name, note = _select_prefix_map_flag(compiler, probe=probe)
    if note:
        print(f"NOTE: {variable}: {note}", file=sys.stderr)
    if flag_name is not None:
        for source, replacement in path_remaps(env, checkout=checkout):
            flag = f"-f{flag_name}={source}={replacement}"
            if flag not in existing:
                existing.append(flag)
    return shlex.join(existing)


def _scoped_flags_variable(base: str, target: str | None) -> str:
    """``CFLAGS`` when no target is known, else the `cc`-crate-style
    ``CFLAGS_<target_with_underscores>`` scoped variable.

    Scoping per target keeps one leg's probe result (e.g. an aarch64 cross
    toolchain that only accepts -fdebug-prefix-map) from ever reaching a
    different toolchain in the same job via a shared generic ``CFLAGS`` --
    which is exactly how the aarch64 leg's rejected -ffile-prefix-map flag
    was observed reaching a toolchain it was never probed against.
    """

    if not target:
        return base
    return f"{base}_{target.replace('-', '_')}"


def write_github_environment(
    destination: Path,
    environ: Mapping[str, str] | None = None,
    *,
    checkout: str | PurePath | None = None,
    target: str | None = None,
) -> tuple[int, int]:
    """Append the encoded flags to GitHub Actions' per-step environment file."""

    encoded, preserved, remaps = encoded_rustflags(
        environ, checkout=checkout, target=target
    )
    env = os.environ if environ is None else environ
    cflags_var = _scoped_flags_variable("CFLAGS", target)
    cxxflags_var = _scoped_flags_variable("CXXFLAGS", target)
    cflags = native_prefix_flags(cflags_var, env, checkout=checkout, target=target)
    cxxflags = native_prefix_flags(cxxflags_var, env, checkout=checkout, target=target)
    with destination.open("a", encoding="utf-8", newline="\n") as stream:
        stream.write(f"CARGO_ENCODED_RUSTFLAGS={encoded}\n")
        # Cargo gives the encoded variable precedence.  Clearing the plain form
        # makes that behavior explicit after its values have been preserved above.
        stream.write("RUSTFLAGS=\n")
        stream.write(f"{cflags_var}={cflags}\n")
        stream.write(f"{cxxflags_var}={cxxflags}\n")
    return preserved, remaps


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--github-env",
        type=Path,
        required=True,
        help="ephemeral GitHub Actions environment file",
    )
    parser.add_argument(
        "--target",
        default=None,
        help=(
            "Rust target triple this leg is building (e.g. "
            "aarch64-unknown-linux-gnu). When given, native prefix-map flags "
            "are written to a CFLAGS_<target>/CXXFLAGS_<target> scoped "
            "variable instead of the generic CFLAGS/CXXFLAGS, and the C/C++ "
            "prefix-map macro is feature-detected against that target's "
            "compiler rather than assumed. Omit for host-native (non-cross) "
            "builds."
        ),
    )
    args = parser.parse_args(argv)
    preserved, remaps = write_github_environment(
        args.github_env,
        checkout=Path.cwd(),
        target=args.target,
    )
    print(
        "OK: configured identity-neutral Rust paths "
        f"({remaps} remap(s), {preserved} existing flag(s) preserved)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
