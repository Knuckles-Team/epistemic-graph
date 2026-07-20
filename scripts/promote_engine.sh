#!/usr/bin/env bash
# Atomically promote an epistemic-graph binary through a deployment-owned hook.
#
# This repository owns artifact validation and atomic replacement. The target
# topology (systemd, Kubernetes, a container scheduler, or another supervisor)
# belongs to an external executable selected with ENGINE_PROMOTION_HOOK. No host,
# service, socket, identity, or filesystem layout is embedded here.
#
# Usage:
#   ENGINE_BIN_DEST=<external destination> scripts/promote_engine.sh [options]
#
# Options:
#   --build               Build the current full CPU release before promotion.
#   --activate            Ask the deployment hook to activate the promoted binary.
#   --activate-consumers  Ask the hook to refresh dependent consumers after activation.
#   --verify              Ask the hook to run its deployment-specific live verification.
#
# Configuration:
#   ENGINE_BIN_DEST        Required destination managed by the deployment.
#   ENGINE_SOURCE_BINARY   Candidate binary; defaults to this checkout's release output.
#   ENGINE_BUILD_FEATURES  Cargo features used by --build (default full,ast-extended).
#   ENGINE_PROMOTION_HOOK  Executable implementing the hook protocol below.
#
# Hook protocol (action followed by bounded path arguments):
#   preflight <candidate> <destination>
#   activate <destination>
#   activate-consumers <destination>
#   verify <destination>
#   rollback <destination>
#
# Hooks are never evaluated through a shell. They remain external configuration and
# may implement any deployment system without coupling this repository to it.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SOURCE_BINARY="${ENGINE_SOURCE_BINARY:-${REPO_ROOT}/target/release/epistemic-graph-server}"
DESTINATION="${ENGINE_BIN_DEST:-}"
PROMOTION_HOOK="${ENGINE_PROMOTION_HOOK:-}"
BUILD_FEATURES="${ENGINE_BUILD_FEATURES:-full,ast-extended}"

DO_BUILD=0
DO_ACTIVATE=0
DO_CONSUMERS=0
DO_VERIFY=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --build) DO_BUILD=1 ;;
    --activate) DO_ACTIVATE=1 ;;
    --activate-consumers) DO_CONSUMERS=1 ;;
    --verify) DO_VERIFY=1 ;;
    *) echo "unknown argument" >&2; exit 2 ;;
  esac
  shift
done

[[ -n "$DESTINATION" ]] || { echo "ENGINE_BIN_DEST is required" >&2; exit 2; }
[[ "$DESTINATION" = /* ]] || { echo "ENGINE_BIN_DEST must be absolute" >&2; exit 2; }
[[ ! -L "$DESTINATION" ]] || { echo "ENGINE_BIN_DEST must not be a symlink" >&2; exit 2; }

if [[ "$DO_CONSUMERS" -eq 1 && "$DO_ACTIVATE" -ne 1 ]]; then
  echo "--activate-consumers requires --activate" >&2
  exit 2
fi
if [[ "$DO_VERIFY" -eq 1 && "$DO_ACTIVATE" -ne 1 ]]; then
  echo "--verify requires --activate" >&2
  exit 2
fi
if [[ "$DO_ACTIVATE" -eq 1 ]]; then
  [[ -n "$PROMOTION_HOOK" && -x "$PROMOTION_HOOK" && ! -L "$PROMOTION_HOOK" ]] || {
    echo "activation requires a regular executable ENGINE_PROMOTION_HOOK" >&2
    exit 2
  }
fi

if [[ "$DO_BUILD" -eq 1 ]]; then
  echo "building full release artifact"
  (
    cd "$REPO_ROOT"
    cargo build --locked --release --features "$BUILD_FEATURES"
  )
fi

[[ -f "$SOURCE_BINARY" && -x "$SOURCE_BINARY" && ! -L "$SOURCE_BINARY" ]] || {
  echo "candidate must be a regular executable" >&2
  exit 1
}
"$SOURCE_BINARY" --help >/dev/null

DESTINATION_DIR="$(dirname "$DESTINATION")"
[[ -d "$DESTINATION_DIR" && -w "$DESTINATION_DIR" ]] || {
  echo "destination directory is unavailable" >&2
  exit 1
}

if [[ -n "$PROMOTION_HOOK" ]]; then
  [[ -x "$PROMOTION_HOOK" && ! -L "$PROMOTION_HOOK" ]] || {
    echo "ENGINE_PROMOTION_HOOK must be a regular executable" >&2
    exit 2
  }
  "$PROMOTION_HOOK" preflight "$SOURCE_BINARY" "$DESTINATION"
fi

LOCK_FILE="${DESTINATION}.promotion.lock"
exec 9>"$LOCK_FILE"
flock -n 9 || { echo "another promotion is active" >&2; exit 1; }

STAGED="$(mktemp "${DESTINATION_DIR}/.epistemic-graph.XXXXXX")"
BACKUP="${DESTINATION}.previous"
cleanup() { rm -f "$STAGED"; }
trap cleanup EXIT

install -m 0755 "$SOURCE_BINARY" "$STAGED"
[[ "$(sha256sum "$SOURCE_BINARY" | cut -d' ' -f1)" = "$(sha256sum "$STAGED" | cut -d' ' -f1)" ]] || {
  echo "staged artifact digest mismatch" >&2
  exit 1
}

HAD_PREVIOUS=0
if [[ -f "$DESTINATION" ]]; then
  [[ ! -L "$DESTINATION" ]] || { echo "destination changed to a symlink" >&2; exit 1; }
  PREVIOUS_STAGED="$(mktemp "${DESTINATION_DIR}/.epistemic-graph.previous.XXXXXX")"
  cp -p "$DESTINATION" "$PREVIOUS_STAGED"
  mv -f "$PREVIOUS_STAGED" "$BACKUP"
  HAD_PREVIOUS=1
fi
mv -f "$STAGED" "$DESTINATION"
trap - EXIT
echo "artifact promoted atomically"

if [[ "$DO_ACTIVATE" -eq 1 ]]; then
  if ! "$PROMOTION_HOOK" activate "$DESTINATION"; then
    if [[ "$HAD_PREVIOUS" -eq 1 ]]; then
      ROLLBACK_STAGED="$(mktemp "${DESTINATION_DIR}/.epistemic-graph.rollback.XXXXXX")"
      cp -p "$BACKUP" "$ROLLBACK_STAGED"
      mv -f "$ROLLBACK_STAGED" "$DESTINATION"
    else
      rm -f "$DESTINATION"
    fi
    "$PROMOTION_HOOK" rollback "$DESTINATION" || true
    echo "activation failed; prior artifact restored" >&2
    exit 1
  fi
fi

if [[ "$DO_CONSUMERS" -eq 1 ]]; then
  "$PROMOTION_HOOK" activate-consumers "$DESTINATION"
fi
if [[ "$DO_VERIFY" -eq 1 ]]; then
  "$PROMOTION_HOOK" verify "$DESTINATION"
fi

echo "promotion complete"
