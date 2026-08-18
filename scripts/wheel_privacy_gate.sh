#!/usr/bin/env bash
# w6-precommit-ci-parity (D-W6PC-2): local reproduction of release-build.yml's
# wheel fold + normalize + audit + completeness sequence ("Normalize and audit
# primary wheel" + "Require a complete primary wheel" steps), so a build-path
# leak or a missing-kernel regression is caught PRE-PUSH instead of only in the
# release workflow. Before this script, `.pre-commit-config.yaml`'s
# `wheel-smoke` hook built the main wheel and stopped — it never ran
# normalize_wheel_sbom.py / normalize_wheel_build_paths.py /
# check_wheel_privacy.py / check_wheel_completeness.py at all, so a
# privileged-home-prefix leak in the wheel (the operator's second live example
# this lane closes) was invisible to every local gate and reached CI first.
#
# DEBUG builds (no --release), same rationale as wheel-smoke's existing
# comment: the release fat-LTO link is ~25 min, far too slow for any local
# stage; every failure mode this script guards against (retained build path,
# missing/incomplete numeric kernel fold, RECORD corruption, or forbidden NumPy
# dependency) reproduces identically in a debug build, the lightest faithful
# check. CI still does the full --release manylinux build and the real audit
# a second time as an independent backstop.
#
# Usage: scripts/wheel_privacy_gate.sh
# Exits non-zero (and prints the failing script's own diagnostic) on the first
# failed step. Never echoes a build-root path itself.
set -euo pipefail
# The local build mirrors release composition from a reused checkout; Python
# tooling must not recreate cache members while Maturin selects package files.
export PYTHONDONTWRITEBYTECODE=1

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

WORKDIR="$(mktemp -d "${TMPDIR:-/tmp}/eg-wheel-privacy-gate.XXXXXX")"
trap 'rm -rf "$WORKDIR"' EXIT

# release-build.yml runs "Configure identity-neutral Rust source paths" BEFORE
# the wheel build, not after — it maps CARGO_MANIFEST_DIR/PWD/HOME etc. to
# neutral /build/... roots via rustc's own --remap-path-prefix, so the local
# checkout/home path never gets embedded in a compiled file!()/panic-location
# string in the first place. First run of this script (w6-precommit-ci-parity,
# D-W6PC-2) skipped this step and built plain — the audit below then found a
# REAL (not planted) posix-home-prefix leak in the compiled server binary that
# survived normalize_wheel_build_paths.py's post-hoc scrub, proving both that
# the audit chain works AND that skipping this step makes the hook fail on
# every local build regardless of any candidate's diff. Configuring the remap
# first, exactly like CI, is what keeps this gate real without being absolute.
echo "=== configure identity-neutral Rust source paths (rust-ci/release-build parity) ==="
REMAP_ENV_FILE="$WORKDIR/rust-path-remap.env"
: > "$REMAP_ENV_FILE"
python3 scripts/configure_rust_path_remap.py --github-env "$REMAP_ENV_FILE"
# NOT `source`d: this is a GitHub Actions per-step-env file (plain KEY=VALUE
# lines, GITHUB_ENV format), not bash syntax — a VALUE containing unquoted
# spaces (CFLAGS/CXXFLAGS carry multiple space-separated -ffile-prefix-map
# flags) would otherwise have its later words parsed as separate shell
# commands. Read and export each line's KEY/VALUE explicitly instead.
while IFS='=' read -r key value; do
  [ -z "$key" ] && continue
  export "$key=$value"
done < "$REMAP_ENV_FILE"

echo "=== build primary server wheel (debug, full+ast-extended) ==="
maturin build --locked --no-default-features --features full,ast-extended \
  --out "$WORKDIR/dist-primary"

echo "=== build eg-numeric kernel wheel (debug, feature python) ==="
maturin build -m crates/eg-numeric/Cargo.toml --features python \
  --out "$WORKDIR/numdist-primary"

SERVER_WHEEL=$(ls "$WORKDIR"/dist-primary/epistemic_graph-*.whl)
NUMERIC_WHEEL=$(ls "$WORKDIR"/numdist-primary/*.whl)

echo "=== fold numeric kernel into server wheel ==="
python3 scripts/inject_numeric_kernel.py "$SERVER_WHEEL" "$NUMERIC_WHEEL"

echo "=== normalize build-local SBOM references ==="
python3 scripts/normalize_wheel_sbom.py "$SERVER_WHEEL"

echo "=== normalize retained native build paths ==="
python3 scripts/normalize_wheel_build_paths.py "$SERVER_WHEEL"

echo "=== audit wheel for retained build/home identity (the w3-wheel-privacy gate) ==="
python3 scripts/check_wheel_privacy.py "$SERVER_WHEEL"

echo "=== require a complete wheel (kernel + executable binary + RECORD) ==="
python3 scripts/check_wheel_completeness.py "$SERVER_WHEEL"

echo "=== wheel privacy/completeness gate PASSED ==="
