#!/usr/bin/env bash
# Install the pinned native scanners used by the scanner-quality CI job and the
# manual-stage pre-commit hooks (cccc, kiss fork, dupehound, arch-lint, jscpd,
# dependency-cruiser). One definition shared by CI and `scripts/bootstrap.sh
# --scanners`; CI keys its cache on this file's hash, so changing a pin here
# rebuilds the cached toolchain.
#
# Usage: scripts/install_scanners.sh [--verify] [ROOT]   (default: ~/.local/share/eg-scanners)
# Prints the bin directories to put on PATH, one per line.
set -euo pipefail

verify_only=false
if [[ "${1:-}" == --verify ]]; then
  verify_only=true
  shift
fi
root="${1:-$HOME/.local/share/eg-scanners}"
# The fork probe changes cwd; keep tool paths absolute without creating ROOT.
case "$root" in
  /*) ;;
  *) root="$(pwd -P)/$root" ;;
esac
script_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

version_matches() {
  local bin="$1" expected="$2" actual
  [[ -x "$bin" ]] || return 1
  actual="$("$bin" --version 2>&1)" || return 1
  [[ "$actual" == "$expected" ]] || { [[ "$expected" == "arch-lint 0.5.0" ]] && [[ "$actual" == "0.5.0" ]]; }
}

crate_valid() {
  local name="$1" bin="$2" expected="$3"
  version_matches "$root/$name/bin/$bin" "$expected" || return 1
  if [[ "$name" == kiss ]]; then
    python3 "$script_dir/kiss_fork.py" "$root/$name/bin/$bin" >&2 || return 1
  fi
}

verify_tools() {
  crate_valid cccc cccc "cccc 1.6.0" &&
    crate_valid kiss kiss "kiss 0.4.12" &&
    crate_valid dupehound dupehound "dupehound 0.1.2" &&
    crate_valid arch-lint arch-lint "arch-lint 0.5.0" &&
    version_matches "$root/npm/node_modules/.bin/jscpd" "cpd 5.0.16" &&
    version_matches "$root/npm/node_modules/.bin/depcruise" "18.2.0"
}

if "$verify_only"; then
  verify_tools
  exit
fi
mkdir -p "$root"

install_crate() {
  local name="$1" bin="$2" expected="$3"
  shift 3
  if crate_valid "$name" "$bin" "$expected"; then
    return
  fi
  # Use Cargo's supported Git CLI transport for pinned Git dependencies.
  # This override is scoped to this install; auth/proxy/trust config is unchanged.
  cargo install --config net.git-fetch-with-cli=true --locked --force "$@" --root "$root/$name"
  crate_valid "$name" "$bin" "$expected"
}

install_crate cccc cccc "cccc 1.6.0" --git https://github.com/moznion/cccc --rev d728759323be5d9977b7390a27133e8eaf481f26 cccc-cli
# kiss: upstream 0.4.12 + the inline-module fix, from the fork build
# (scripts/kiss_fork.py); crates.io 0.4.12 aborts the census.
install_crate kiss kiss "kiss 0.4.12" --git https://github.com/Knucklessg1/kiss --rev 7f1c6785697d3fe9a41ceb8b8e5d0f615fb1f3d9 kiss-ai
install_crate dupehound dupehound "dupehound 0.1.2" --version 0.1.2 dupehound
install_crate arch-lint arch-lint "arch-lint 0.5.0" --version 0.5.0 arch-lint-cli
if ! version_matches "$root/npm/node_modules/.bin/jscpd" "cpd 5.0.16" ||
  ! version_matches "$root/npm/node_modules/.bin/depcruise" "18.2.0"; then
  npm install --prefix "$root/npm" --no-package-lock --ignore-scripts --no-save --no-audit --no-fund \
    "jscpd@5.0.16" "dependency-cruiser@18.2.0"
fi

verify_tools

for bin_dir in cccc/bin kiss/bin dupehound/bin arch-lint/bin npm/node_modules/.bin; do
  echo "$root/$bin_dir"
done
