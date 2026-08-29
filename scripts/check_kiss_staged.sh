#!/usr/bin/env bash
# BUG-CX-136 — changed-source KISS gate for epistemic-graph.
#
# KISS has two dangerous defaults for this repository: it can write a
# self-calibrated .kissconfig, and its Rust config only becomes authoritative
# when --config is supplied.  It also reports a false green when multiple
# paths are passed to one `check` invocation.  This wrapper therefore resolves
# only an already-installed, pinned binary, checks one staged source path per
# invocation, and never writes a baseline or config.
#
# Exit 0 = clean/no applicable source, 1 = KISS findings, 2 = cannot run.
set -uo pipefail

# A real git hook inherits repository-selector variables from git itself.  In
# particular, GIT_DIR/GIT_WORK_TREE/GIT_COMMON_DIR can make a command rooted at
# this checkout inspect another worktree.  The helper strips every GIT_* selector
# dynamically. Keep GIT_INDEX_FILE deliberately:
# pre-commit points it at the staged index that this gate must check.
sanitized_env_args() {
  local key snapshot
  snapshot="$(env)" || return 1
  while IFS='=' read -r key _; do
    case "$key" in
      GIT_INDEX_FILE|'' ) ;;
      GIT_* ) printf '%s\n' "-u" "$key" ;;
    esac
  done <<< "$snapshot"
}

git_cmd() {
  local args=()
  local arg sanitized
  sanitized="$(sanitized_env_args)" || return 1
  while IFS= read -r arg; do
    args+=("$arg")
  done <<< "$sanitized"
  env "${args[@]}" git "$@"
}

scanner_cmd() {
  local args=()
  local arg sanitized
  sanitized="$(sanitized_env_args)" || return 1
  while IFS= read -r arg; do
    args+=("$arg")
  done <<< "$sanitized"
  env "${args[@]}" "$@"
}

ROOT="$(git_cmd rev-parse --show-toplevel 2>/dev/null)" || {
  echo "kiss(staged): CANNOT RUN: not inside a git work tree" >&2
  exit 2
}
cd "$ROOT" || {
  echo "kiss(staged): CANNOT RUN: could not enter repository root" >&2
  exit 2
}

die() {
  echo "kiss(staged): CANNOT RUN: $*" >&2
  exit 2
}

read_contract() {
  command -v python3 >/dev/null 2>&1 || die "python3 is required to read pyproject.toml"
  scanner_cmd python3 - <<'PY'
import sys
from pathlib import Path

sys.path.insert(0, str(Path("scripts").resolve()))

try:
    from scanner_contract import load_contract
    value = load_contract().kiss_version
except Exception as exc:
    print(f"invalid [tool.epistemic_graph.scanners] KISS contract: {exc}", file=sys.stderr)
    raise SystemExit(2)
print(value.strip())
PY
}

VERSION="$(read_contract)" || die "could not load the pinned KISS version"
CFG=".kiss/kiss.toml"
KISS="${KISS_BIN:-}"
if [ -z "$KISS" ]; then
  if [ -n "${HOME:-}" ] && [ -x "$HOME/.local/bin/kiss" ]; then
    KISS="$HOME/.local/bin/kiss"
  elif [ -x /usr/local/bin/kiss ]; then
    KISS=/usr/local/bin/kiss
  else
    KISS="$(command -v kiss 2>/dev/null || true)"
  fi
fi
[ -n "$KISS" ] && [ -x "$KISS" ] || die \
  "kiss is not installed. Install the pinned $VERSION binary before running this hook; the hook never downloads it."

GOT="$(scanner_cmd "$KISS" --version 2>/dev/null)" || die "kiss --version failed"
[ "$GOT" = "kiss $VERSION" ] || die \
  "version drift: expected 'kiss $VERSION', got '$GOT'"
[ -f "$CFG" ] || die "missing $CFG (hand-authored KISS thresholds)"
cfg_resolved="$(realpath -- "$CFG" 2>/dev/null)" || die "could not resolve $CFG"
case "$cfg_resolved" in
  "$ROOT"/*) ;;
  *) die "$CFG resolves outside repository root" ;;
esac
[ ! -e .kissconfig ] && [ ! -L .kissconfig ] || die \
  ".kissconfig exists; remove it because bare kiss check self-calibrates and disables rules"

# Pre-commit normally exports GIT_INDEX_FILE.  Read the index because it is
# the tree that will actually be committed; when invoked manually with no
# staged change, fall back to the working-tree diff for useful diagnostics.
paths="$(git_cmd diff --cached --name-only --diff-filter=ACMR 2>/dev/null)" || \
  die "git diff --cached failed"
if [ -z "$paths" ]; then
  paths="$(git_cmd diff --name-only --diff-filter=ACMR HEAD 2>/dev/null)" || \
    die "git diff HEAD failed"
fi

# This repo's checked-in KISS policy is Rust-only.  Python/JS KISS sections
# must be added with measured thresholds before this hook should claim to own
# those languages; dupehound/CCCC still cover their changed functions.
files=()
while IFS= read -r path; do
  case "$path" in
    src/*|crates/*)
      case "$path" in
        *.rs)
      case "/$path" in
        */target/*|*/target-*/*|*/build/*|*/dist/*|*/generated/*|*/vendor/*|*/third_party/*|*/fixtures/*|*/fixture/*|*/Cargo.lock|*.lock)
          continue
          ;;
      esac
      files+=("$path")
        ;;
      esac
      ;;
  esac
done <<< "$paths"

if [ "${#files[@]}" -eq 0 ]; then
  echo "kiss(staged): OK: no staged Rust source covered by .kiss/kiss.toml"
  exit 0
fi

rc=0
total=0
for path in "${files[@]}"; do
  # Never pass more than one path to KISS.  KISS 0.4.10's multi-path check
  # prints NO VIOLATIONS and exits 0 even when either input has violations.
  resolved="$(realpath -- "$path" 2>/dev/null)" || die \
    "could not resolve changed path $path"
  case "$resolved" in
    "$ROOT"/*) ;;
    *) die "changed path $path resolves outside repository root" ;;
  esac
  output="$(scanner_cmd "$KISS" check --config "$CFG" --lang rust "$path" 2>&1)"
  status=$?
  if grep -q "Unknown config key" <<< "$output"; then
    printf '%s\n' "$output" >&2
    die "KISS rejected a config key and fell back to upstream defaults"
  fi
  if [ "$status" -ne 0 ] && [ "$status" -ne 1 ]; then
    printf '%s\n' "$output" >&2
    die "KISS failed on $path with exit $status"
  fi
  [ -n "$output" ] || die "KISS returned no report for $path"
  count="$(grep -c '^VIOLATION:' <<< "$output" || true)"
  if [ "$status" -eq 0 ] && ! grep -q "NO VIOLATIONS" <<< "$output"; then
    printf '%s\n' "$output" >&2
    die "KISS returned exit 0 without a clean report for $path"
  fi
  if [ "$status" -eq 1 ] && grep -q "NO VIOLATIONS" <<< "$output"; then
    printf '%s\n' "$output" >&2
    die "KISS returned findings status with a clean report for $path"
  fi
  if { [ "$status" -eq 0 ] && [ "$count" -ne 0 ]; } || \
    { [ "$status" -eq 1 ] && [ "$count" -eq 0 ]; }; then
    printf '%s\n' "$output" >&2
    die "KISS exit status and violation report disagree for $path"
  fi
  total=$((total + count))
  printf 'kiss(staged): %s violation(s) in %s\n' "$count" "$path"
  grep '^VIOLATION:' <<< "$output" || true
  [ "$count" -eq 0 ] || rc=1
done

printf 'kiss(staged): %s violation(s) across %s changed file(s)\n' "$total" "${#files[@]}"
exit "$rc"
