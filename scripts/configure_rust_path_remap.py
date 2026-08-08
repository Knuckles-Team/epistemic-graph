#!/usr/bin/env python3
"""Configure identity-neutral Rust source paths without discarding build flags.

Rust can retain source and toolchain paths in release binaries even when debug
information is stripped.  This helper converts any existing ``RUSTFLAGS`` to
Cargo's unambiguous unit-separator representation, preserves an existing
``CARGO_ENCODED_RUSTFLAGS`` verbatim, and appends deterministic
``--remap-path-prefix`` flags for the checkout and build-user directories. It
also appends the equivalent ``-ffile-prefix-map`` option to existing C/C++ flags
for native dependencies compiled by Cargo build scripts.

The generated values belong in an ephemeral build environment.  The helper
never prints the concrete source prefixes it discovers.
"""

from __future__ import annotations

import argparse
import os
import shlex
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

# The manylinux leg (`maturin-action` with `manylinux: 2_28`) does not compile on
# the runner directly -- it hands off to a throwaway Docker container
# (`ghcr.io/pyo3/maturin`) that always runs as **root**, with its own
# `HOME=/root` (and therefore `CARGO_HOME=/root/.cargo`, `RUSTUP_HOME=/root/.rustup`
# by Cargo's own defaulting) and the checkout mounted at `/io`. This script runs
# BEFORE that handoff, on the *outer* runner, so no environment variable it can
# read ever names those container-side paths -- `ENVIRONMENT_ROOTS` above is
# necessarily blind to them. A dependency crate fetched fresh into the
# container's registry cache embeds
# `/root/.cargo/registry/src/<index>/<crate>-<version>/src/....rs` into
# `file!()`/`#[track_caller]`/panic-location strings at compile time, and
# because that literal was never in the remap list, it survives straight into
# the compiled artifact -- confirmed by inspecting the live `rustc` invocation
# inside a `docker run ghcr.io/pyo3/maturin` reproduction: the forwarded
# `CARGO_ENCODED_RUSTFLAGS` carried only the two *outer*-runner remaps, while
# the crate actually being compiled lived under `/root/.cargo/registry/...`.
#
# These roots are therefore denied UNCONDITIONALLY -- not derived from any
# environment variable -- because they are a container *convention*, not a
# per-run fact this process can observe. They mirror, on purpose,
# `check_wheel_privacy.py`'s own "privileged-home-prefix" category, which
# flags exactly these two literals for the identical reason: a container's
# root-owned build state is sensitive by construction, independent of whatever
# environment variable set it up. Reusing the "/build/home" alias (rather than
# minting a new one) keeps them indistinguishable from an ordinary HOME leak in
# the normalized SBOM, which is the correct level of detail to expose.
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


def encoded_rustflags(
    environ: Mapping[str, str] | None = None,
    *,
    checkout: str | PurePath | None = None,
) -> tuple[str, int, int]:
    """Return encoded flags plus preserved/remap counts for safe reporting."""

    env = os.environ if environ is None else environ
    existing = _existing_flags(env)
    remaps = [
        f"--remap-path-prefix={source}={replacement}"
        for source, replacement in path_remaps(env, checkout=checkout)
        if not _already_remapped(existing, source)
    ]
    return UNIT_SEPARATOR.join([*existing, *remaps]), len(existing), len(remaps)


def native_prefix_flags(
    variable: str,
    environ: Mapping[str, str] | None = None,
    *,
    checkout: str | PurePath | None = None,
) -> str:
    """Preserve native compiler flags and append source-prefix remapping."""

    env = os.environ if environ is None else environ
    existing = shlex.split(env.get(variable, ""), posix=os.name != "nt")
    for source, replacement in path_remaps(env, checkout=checkout):
        flag = f"-ffile-prefix-map={source}={replacement}"
        if flag not in existing:
            existing.append(flag)
    return shlex.join(existing)


def write_github_environment(
    destination: Path,
    environ: Mapping[str, str] | None = None,
    *,
    checkout: str | PurePath | None = None,
) -> tuple[int, int]:
    """Append the encoded flags to GitHub Actions' per-step environment file."""

    encoded, preserved, remaps = encoded_rustflags(environ, checkout=checkout)
    env = os.environ if environ is None else environ
    cflags = native_prefix_flags("CFLAGS", env, checkout=checkout)
    cxxflags = native_prefix_flags("CXXFLAGS", env, checkout=checkout)
    with destination.open("a", encoding="utf-8", newline="\n") as stream:
        stream.write(f"CARGO_ENCODED_RUSTFLAGS={encoded}\n")
        # Cargo gives the encoded variable precedence.  Clearing the plain form
        # makes that behavior explicit after its values have been preserved above.
        stream.write("RUSTFLAGS=\n")
        stream.write(f"CFLAGS={cflags}\n")
        stream.write(f"CXXFLAGS={cxxflags}\n")
    return preserved, remaps


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--github-env",
        type=Path,
        required=True,
        help="ephemeral GitHub Actions environment file",
    )
    args = parser.parse_args(argv)
    preserved, remaps = write_github_environment(
        args.github_env,
        checkout=Path.cwd(),
    )
    print(
        "OK: configured identity-neutral Rust paths "
        f"({remaps} remap(s), {preserved} existing flag(s) preserved)"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
