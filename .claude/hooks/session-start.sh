#!/bin/bash
# Claude Code on the web: provision a fresh container so every gate can run.
set -euo pipefail
if [ "${CLAUDE_CODE_REMOTE:-}" != "true" ]; then
  exit 0
fi
cd "$CLAUDE_PROJECT_DIR"
scripts/bootstrap.sh
# Cloud containers have 4 CPUs, no swap and limited disk: cap parallel rustc
# jobs and skip debuginfo so builds neither OOM nor fill the disk.
{
  echo 'export PATH="$HOME/.local/bin:$HOME/.cargo/bin:$PATH"'
  echo 'export CARGO_BUILD_JOBS=3'
  echo 'export CARGO_PROFILE_DEV_DEBUG=0'
  echo 'export CARGO_PROFILE_TEST_DEBUG=0'
} >> "${CLAUDE_ENV_FILE:-/dev/null}"
