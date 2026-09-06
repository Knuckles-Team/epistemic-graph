#!/usr/bin/env bash
# BUG-CX-136 — changed-source KISS gate for epistemic-graph.
#
# KISS has two dangerous defaults for this repository: it can write a
# self-calibrated .kissconfig, and its Rust config only becomes authoritative
# when --config is supplied.  It also reports a false green when multiple
# paths are passed to one `check` invocation.  This wrapper therefore resolves
# only an already-installed, pinned binary, checks one staged source root per
# invocation, and never writes a baseline or config. Each root is checked from
# the complete compiler-declared module closure in the staged index; otherwise
# a newly split private child makes KISS report a false missing-module failure.
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
    [ -n "$arg" ] && args+=("$arg")
  done <<< "$sanitized"
  env "${args[@]}" git "$@"
}

scanner_cmd() {
  local args=()
  local arg sanitized
  sanitized="$(sanitized_env_args)" || return 1
  while IFS= read -r arg; do
    [ -n "$arg" ] && args+=("$arg")
  done <<< "$sanitized"
  env "${args[@]}" "$@"
}

die() {
  echo "kiss(staged): CANNOT RUN: $*" >&2
  exit 2
}

# A pre-commit runner normally starts local hooks at the repository root, but
# that is not guaranteed for a direct hook invocation or a remote gate wrapper.
# Resolve the checkout from this script's location first, then ask Git to
# validate that location.  Running the probe with -C keeps root discovery
# independent of the caller's cwd while the sanitized environment still
# prevents ambient GIT_* selectors from re-rooting it.
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" 2>/dev/null && pwd -P)" || \
  die "could not resolve the hook directory"
ROOT="$(cd -- "$SCRIPT_DIR/.." 2>/dev/null && pwd -P)" || \
  die "could not resolve the repository root"
GIT_ROOT="$(git_cmd -C "$ROOT" rev-parse --show-toplevel 2>/dev/null)" || \
  die "not inside a git work tree"
[ "$GIT_ROOT" = "$ROOT" ] || die "Git resolved a different work tree"
cd "$ROOT" || die "could not enter repository root"

# Use a scratch parent so the NUL-delimited changed-path manifest cannot
# collide with a repository path materialized below it.
SCRATCH_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/eg-kiss-staged.XXXXXX")" || \
  die "could not create staged-source directory"
STAGED_ROOT="$SCRATCH_ROOT/index"
CHANGED_PATHS="$SCRATCH_ROOT/changed-paths"
mkdir -- "$STAGED_ROOT" || die "could not create staged-index directory"
cleanup() {
  rm -rf -- "$SCRATCH_ROOT"
}
trap cleanup EXIT HUP INT TERM

# Pre-commit normally exports GIT_INDEX_FILE. Read only that index, and keep
# path records NUL-delimited because newlines are legal Git pathname bytes.
git_cmd diff --cached --name-only --diff-filter=ACMR -z > "$CHANGED_PATHS" \
  2>/dev/null || die "git diff --cached failed"

# This repo's checked-in KISS policy is Rust-only. Python/JS KISS sections
# must be added with measured thresholds before this hook should claim to own
# those languages; dupehound/CCCC still cover their changed functions.
files=()
while IFS= read -r -d '' path; do
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
done < "$CHANGED_PATHS"

if [ "${#files[@]}" -eq 0 ]; then
  echo "kiss(staged): OK: no staged Rust source covered by .kiss/kiss.toml"
  exit 0
fi

# Materialize the complete staged index once. Literal include! inputs are not
# required to end in .rs, so copying a scanner-maintained extension list would
# be another incomplete source authority. checkout-index reads GIT_INDEX_FILE
# when pre-commit supplies one and never observes unrelated unstaged bytes.
git_cmd checkout-index --all --prefix="$STAGED_ROOT/" 2>/dev/null || \
  die "could not materialize the staged index"

for policy_input in pyproject.toml scripts/scanner_contract.py; do
  staged_policy="$STAGED_ROOT/$policy_input"
  [ -f "$staged_policy" ] && [ ! -L "$staged_policy" ] || die \
    "staged policy input is missing or not a regular file: $policy_input"
  policy_resolved="$(realpath -- "$staged_policy" 2>/dev/null)" || die \
    "could not resolve staged policy input: $policy_input"
  case "$policy_resolved" in
    "$STAGED_ROOT"/*) ;;
    *) die "staged policy input resolves outside staged repository: $policy_input" ;;
  esac
done

read_contract() {
  command -v python3 >/dev/null 2>&1 || die "python3 is required to read pyproject.toml"
  scanner_cmd python3 -I - "$STAGED_ROOT" <<'PY'
import sys
from pathlib import Path

root = Path(sys.argv[1]).resolve()
sys.path.insert(0, str(root / "scripts"))

try:
    from scanner_contract import load_contract
    value = load_contract(root / "pyproject.toml").kiss_version
except Exception as exc:
    print(f"invalid [tool.epistemic_graph.scanners] KISS contract: {exc}", file=sys.stderr)
    raise SystemExit(2)
print(value.strip())
PY
}

VERSION="$(read_contract)" || die "could not load the pinned KISS version"
CFG="$STAGED_ROOT/.kiss/kiss.toml"
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
[ -f "$CFG" ] && [ ! -L "$CFG" ] || die \
  "missing staged .kiss/kiss.toml (hand-authored KISS thresholds)"
cfg_resolved="$(realpath -- "$CFG" 2>/dev/null)" || die "could not resolve $CFG"
case "$cfg_resolved" in
  "$STAGED_ROOT"/*) ;;
  *) die "$CFG resolves outside staged repository root" ;;
esac
[ ! -e "$STAGED_ROOT/.kissconfig" ] && [ ! -L "$STAGED_ROOT/.kissconfig" ] || die \
  "staged .kissconfig exists; remove it because bare kiss check self-calibrates and disables rules"

# The shared walker intentionally returns canonical resolved paths. Reject
# staged symlinks under Rust-owned roots before invoking it so a declared child
# or literal include cannot disappear behind resolution outside the index tree.
source_roots=("$STAGED_ROOT/src")
[ ! -d "$STAGED_ROOT/crates" ] || source_roots+=("$STAGED_ROOT/crates")
symlink="$(find "${source_roots[@]}" -type l -print -quit 2>/dev/null)" || die \
  "could not inspect staged Rust roots for symlinks"
[ -z "$symlink" ] || die "staged Rust tree contains a symlink: $symlink"

MODULE_WALKER="$STAGED_ROOT/scripts/rust_module_tree.py"
[ -f "$MODULE_WALKER" ] && [ ! -L "$MODULE_WALKER" ] || die \
  "missing staged scripts/rust_module_tree.py module-closure authority"

validate_module_closure() {
  local path="$1"
  scanner_cmd python3 -I - "$STAGED_ROOT" "$path" <<'PY'
import sys
from pathlib import Path

root = Path(sys.argv[1]).resolve()
relative = sys.argv[2]
scripts = root / "scripts"
sys.path.insert(0, str(scripts))

try:
    from rust_module_tree import read_module_paths

    paths = read_module_paths(relative, root_dir=root, include_tests=True)
except (ImportError, OSError, SystemExit, UnicodeError) as exc:
    print(f"staged Rust module closure failed for {relative}: {exc}", file=sys.stderr)
    raise SystemExit(2)

if not paths:
    print(f"staged Rust module closure is empty for {relative}", file=sys.stderr)
    raise SystemExit(2)
for candidate in paths:
    resolved = Path(candidate).resolve()
    try:
        resolved.relative_to(root)
    except ValueError:
        print(
            f"staged Rust module closure escapes temporary root: {candidate}",
            file=sys.stderr,
        )
        raise SystemExit(2)
    if not resolved.is_file():
        print(f"staged Rust module is not a regular file: {candidate}", file=sys.stderr)
        raise SystemExit(2)
PY
}

rc=0
total=0
for path in "${files[@]}"; do
  # Never pass more than one path to KISS.  KISS 0.4.10's multi-path check
  # prints NO VIOLATIONS and exits 0 even when either input has violations.
  staged_path="$STAGED_ROOT/$path"
  [ -f "$staged_path" ] && [ ! -L "$staged_path" ] || die \
    "staged source is missing or not a regular file: $path"
  resolved="$(realpath -- "$staged_path" 2>/dev/null)" || die \
    "could not resolve staged path $path"
  case "$resolved" in
    "$STAGED_ROOT"/*) ;;
    *) die "staged path $path resolves outside the temporary source root" ;;
  esac
  validate_module_closure "$path" || die \
    "could not prove staged Rust module closure for $path"
  output="$(cd "$STAGED_ROOT" && \
    scanner_cmd "$KISS" check --config "$cfg_resolved" --lang rust "$path" 2>&1)"
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
