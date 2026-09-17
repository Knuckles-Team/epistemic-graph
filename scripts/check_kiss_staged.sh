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
# BUG-CX-136 diff-scoping (F6): a raw `kiss check <file>` re-reports every
# violation the WHOLE file carries, so a one-line comment added to a large
# pre-existing file used to fail the commit for debt the commit never
# touched. This wrapper now also runs KISS on the HEAD blob of each changed
# file (a second ephemeral tree, `HEAD_ROOT`, materialized once via
# `git archive HEAD`) and hands both reports to `scripts/kiss_diff_scope.py`,
# which keeps only the findings the staged diff actually caused: a NEW or
# MODIFIED function/item (matched by the enclosing item's SOURCE CONTENT, not
# by line number or bare symbol name -- both shift under extraction/merges),
# or a file-level/whole-type count the diff newly crosses or makes worse.
# Untouched pre-existing debt elsewhere in the same file no longer fails the
# commit. NO baseline file and NO self-updating count are involved: both
# reports are computed fresh, from the two Git blobs, on every run.
#
# Exit 0 = clean/no applicable source (or every finding was pre-existing and
# untouched), 1 = attributable KISS findings, 2 = cannot run.
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
HEAD_ROOT="$SCRATCH_ROOT/head"
CHANGED_PATHS="$SCRATCH_ROOT/changed-paths"
mkdir -- "$STAGED_ROOT" || die "could not create staged-index directory"
mkdir -- "$HEAD_ROOT" || die "could not create head-tree directory"
cleanup() {
  rm -rf -- "$SCRATCH_ROOT"
}
trap cleanup EXIT HUP INT TERM

# BUG-CX-136 diff-scoping (F6): materialize the complete HEAD tree once, so a
# finding can be compared against what HEAD actually contained -- a commit
# fails only for KISS findings the staged diff is responsible for, never for
# untouched pre-existing debt elsewhere in a changed file. A repository with
# no commits yet (first-ever commit) has no HEAD; every finding is then
# attributable by construction, so HEAD_ROOT is simply left empty.
HAVE_HEAD=0
if git_cmd rev-parse --verify -q HEAD >/dev/null 2>&1; then
  git_cmd archive HEAD | tar -x -C "$HEAD_ROOT" 2>/dev/null || \
    die "could not materialize the HEAD tree"
  HAVE_HEAD=1
fi

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

for authority in scripts/rust_module_tree.py scripts/rust_lexer.py scripts/kiss_diff_scope.py; do
  staged_authority="$STAGED_ROOT/$authority"
  [ -f "$staged_authority" ] && [ ! -L "$staged_authority" ] || die \
    "missing staged $authority module-closure authority"
done

# A cargo TARGET root's `mod x;` declarations resolve relative to the
# directory the root itself sits in, not a subdirectory named after the
# root -- e.g. `crates/<c>/tests/foo.rs` resolves `mod support;` against
# `crates/<c>/tests/support.rs` / `tests/support/mod.rs`, exactly as rustc
# does for an integration-test binary (see rust_module_tree.py's
# `_root_module_dir` / `crate_root` docstring). `lib.rs`/`main.rs`/`mod.rs`
# already get this via their filename; every other *.rs file that is
# itself a cargo target -- one sitting directly in a crate's `tests/`,
# `examples/`, `benches/`, or `src/bin/` directory -- needs `crate_root`
# passed explicitly, or the walker instead treats it as an ordinary module
# file that owns a same-named child directory, fails to find the sibling
# module, and reports the whole file's closure as broken (fails closed) on
# any staged change to it.
# A directory literally named tests/examples/benches/bin is not necessarily cargo's
# own target-root directory for a crate: a crate can just as well have an ordinary
# module directory with that name nested under src/ (e.g. `src/tests/replay.rs`, a
# unit-test-helper module, not a cargo integration-test binary). Cargo's actual
# target-root directories always sit directly beside that crate's own Cargo.toml --
# `<crate>/tests/`, `<crate>/examples/`, `<crate>/benches/` -- or, for a binary
# target, directly under `<crate>/src/bin/`. Require that, not just the bare name,
# so a nested same-named directory is correctly left as an ordinary module file
# (crate_root=0) instead of wrongly promoted to a target root.
is_cargo_target_root() {
  local path="$1" base dir dirname crate_dir
  base="$(basename -- "$path")"
  case "$base" in
    lib.rs|main.rs|mod.rs) return 1 ;;
  esac
  dir="$(dirname -- "$path")"
  dirname="$(basename -- "$dir")"
  case "$dirname" in
    tests|examples|benches)
      crate_dir="$(dirname -- "$dir")"
      ;;
    bin)
      [ "$(basename -- "$(dirname -- "$dir")")" = "src" ] || return 1
      crate_dir="$(dirname -- "$(dirname -- "$dir")")"
      ;;
    *)
      return 1
      ;;
  esac
  [ -f "$STAGED_ROOT/$crate_dir/Cargo.toml" ] && [ ! -L "$STAGED_ROOT/$crate_dir/Cargo.toml" ]
}

validate_module_closure() {
  local path="$1"
  local crate_root=0
  is_cargo_target_root "$path" && crate_root=1
  scanner_cmd python3 -I - "$STAGED_ROOT" "$path" "$crate_root" <<'PY'
import sys
from pathlib import Path

root = Path(sys.argv[1]).resolve()
relative = sys.argv[2]
crate_root = sys.argv[3] == "1"
scripts = root / "scripts"
sys.path.insert(0, str(scripts))

try:
    from rust_module_tree import read_module_paths

    paths = read_module_paths(
        relative, root_dir=root, include_tests=True, crate_root=crate_root
    )
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
  # BUG-CX-136 diff-scoping (F6): a raw KISS report covers the WHOLE file, so
  # it re-reports every pre-existing violation a changed file already
  # carried, not just what this diff touched. Narrow it: re-run KISS on the
  # HEAD blob of the same file (when one exists) and keep only the findings
  # `kiss_diff_scope.py` attributes to the staged diff -- a NEW or MODIFIED
  # function/item, or a file-level (or whole-type methods_per_class) count
  # this diff newly crosses or worsens. Untouched pre-existing debt
  # elsewhere in the same file never fails the commit.
  attributable_count="$count"
  attributable_output="$output"
  if [ "$count" -gt 0 ]; then
    head_file="$HEAD_ROOT/$path"
    head_args=()
    if [ "$HAVE_HEAD" -eq 1 ] && [ -f "$head_file" ] && [ ! -L "$head_file" ]; then
      head_output="$(cd "$HEAD_ROOT" && \
        scanner_cmd "$KISS" check --config "$cfg_resolved" --lang rust "$path" 2>&1)"
      head_status=$?
      if [ "$head_status" -ne 0 ] && [ "$head_status" -ne 1 ]; then
        printf '%s\n' "$head_output" >&2
        die "KISS failed on the HEAD version of $path with exit $head_status"
      fi
      printf '%s' "$head_output" > "$SCRATCH_ROOT/head-report"
      head_args=(--head-source "$head_file" --head-report "$SCRATCH_ROOT/head-report")
    fi
    printf '%s' "$output" > "$SCRATCH_ROOT/staged-report"
    attributable_output="$(scanner_cmd python3 -I "$STAGED_ROOT/scripts/kiss_diff_scope.py" \
      --staged-source "$staged_path" --staged-report "$SCRATCH_ROOT/staged-report" \
      "${head_args[@]}")" || die "kiss_diff_scope.py failed for $path"
    attributable_count="$(grep -c '^VIOLATION:' <<< "$attributable_output" || true)"
    [ -n "$attributable_output" ] || attributable_count=0
  fi
  total=$((total + attributable_count))
  if [ "$attributable_count" -eq "$count" ]; then
    printf 'kiss(staged): %s violation(s) in %s\n' "$attributable_count" "$path"
  else
    printf 'kiss(staged): %s attributable violation(s) in %s (%s pre-existing, untouched by this change, not counted)\n' \
      "$attributable_count" "$path" "$((count - attributable_count))"
  fi
  if [ "$attributable_count" -ne 0 ]; then
    printf '%s\n' "$attributable_output"
    rc=1
  fi
done

printf 'kiss(staged): %s attributable violation(s) across %s changed file(s)\n' "$total" "${#files[@]}"
exit "$rc"
