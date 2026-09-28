#!/usr/bin/env bash
# Install the pinned native scanners used by the scanner-quality CI job and the
# manual-stage pre-commit hooks (cccc, kiss fork, dupehound, arch-lint, jscpd,
# dependency-cruiser). One definition shared by CI and `scripts/bootstrap.sh
# --scanners`; CI keys its cache on this file's hash, so changing a pin here
# rebuilds the cached toolchain.
#
# Usage: scripts/install_scanners.sh [ROOT]   (default: ~/.local/share/eg-scanners)
# Prints the bin directories to put on PATH, one per line.
set -euo pipefail

root="${1:-$HOME/.local/share/eg-scanners}"
mkdir -p "$root"

install_crate() {
  local name="$1" bin="$2"
  shift 2
  if [[ -x "$root/$name/bin/$bin" ]]; then
    return
  fi
  cargo install --locked "$@" --root "$root/$name"
}

install_crate cccc cccc --git https://github.com/moznion/cccc --rev d728759323be5d9977b7390a27133e8eaf481f26 cccc-cli
# kiss: upstream 0.4.12 + the inline-module fix, from the fork build
# (scripts/kiss_fork.py); crates.io 0.4.12 aborts the census.
install_crate kiss kiss --git https://github.com/Knucklessg1/kiss --rev 7f1c6785697d3fe9a41ceb8b8e5d0f615fb1f3d9 kiss-ai
install_crate dupehound dupehound --version 0.1.2 dupehound
install_crate arch-lint arch-lint --version 0.5.0 arch-lint-cli
if [[ ! -x "$root/npm/node_modules/.bin/jscpd" || ! -x "$root/npm/node_modules/.bin/depcruise" ]]; then
  npm install --prefix "$root/npm" --no-package-lock --ignore-scripts --no-save --no-audit --no-fund \
    "jscpd@5.0.16" "dependency-cruiser@18.2.0"
fi

for bin_dir in cccc/bin kiss/bin dupehound/bin arch-lint/bin npm/node_modules/.bin; do
  echo "$root/$bin_dir"
done
