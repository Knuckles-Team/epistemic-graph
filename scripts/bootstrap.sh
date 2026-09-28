#!/usr/bin/env bash
# One-command contributor setup from a fresh clone (local or Claude Code on the web).
#
#   scripts/bootstrap.sh              Python, Rust toolchain, test deps, git hooks
#   scripts/bootstrap.sh --scanners   also the pinned native scanners + cargo-deny
#
# Idempotent and non-interactive. Afterwards:
#   uvx pre-commit run --config .config/pre-commit.yaml --all-files
#   cargo test -p <crate>            # or: cargo check / cargo clippy -p <crate>
set -euo pipefail

cd "$(dirname "$0")/.."

scanners=0
for arg in "$@"; do
  case "$arg" in
    --scanners) scanners=1 ;;
    -h | --help)
      sed -n '2,9p' "$0"
      exit 0
      ;;
    *)
      echo "unknown argument: $arg" >&2
      exit 2
      ;;
  esac
done

export PATH="$HOME/.local/bin:$HOME/.cargo/bin:$PATH"

# uv: older releases cannot download the pinned CPython patch release.
uv_minor() { uv --version 2>/dev/null | awk '{split($2, v, "."); print v[1] * 1000 + v[2]}'; }
if ! command -v uv >/dev/null 2>&1 || [[ "$(uv_minor)" -lt 9 ]]; then
  echo "bootstrap: installing a current uv"
  python3 -m pip install --quiet --user --upgrade "uv>=0.9" \
    || curl -LsSf https://astral.sh/uv/install.sh | sh
fi

echo "bootstrap: Python $(cat .python-version) and locked test dependencies"
uv python install "$(cat .python-version)"
uv sync --frozen --extra test --no-install-project

echo "bootstrap: Rust toolchain from rust-toolchain.toml"
if ! command -v rustup >/dev/null 2>&1; then
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain none
fi
rustup toolchain install

echo "bootstrap: git hooks"
uvx pre-commit install --config .config/pre-commit.yaml --hook-type pre-commit --hook-type pre-push

if [[ "$scanners" == 1 ]]; then
  echo "bootstrap: native scanners (cargo builds; ~15 min cold)"
  scanner_root="$HOME/.local/share/eg-scanners"
  bash scripts/install_scanners.sh "$scanner_root" >/dev/null
  deny_version="$(PYTHONPATH=scripts python3 -c 'from scanner_contract import load_contract; print(load_contract().cargo_deny_version)')"
  command -v cargo-deny >/dev/null 2>&1 || cargo install --locked --version "$deny_version" cargo-deny
  uv tool install --quiet import-linter==2.13
  echo "bootstrap: add the scanners to PATH:"
  bash scripts/install_scanners.sh "$scanner_root" | sed 's/^/  export PATH="/; s/$/:$PATH"/'
fi

echo "bootstrap: done"
