#!/usr/bin/env bash
# Fetch the official SQLite command-line shell that the differential conformance
# tests run against, and print its path.
#
#   export EG_SQLITE3="$(scripts/fetch_sqlite3.sh)"
#   cargo test -p eg-sqlite-format --test differential
#   cargo test -p epistemic-graph --features full test_sqlite_file_roundtrip
#
# Those tests use `$EG_SQLITE3` when it is set, otherwise `sqlite3` on `$PATH`.
# When neither runs they FAIL; they never skip. A distribution package
# (`apt install sqlite3`) works as well as this script. The script exists for
# hosts without one, such as the build hosts.
#
# The archive is the sqlite.org precompiled "sqlite-tools" bundle for linux
# x86_64, checked against a pinned sha256 before use.
#
# Optional argument: cache directory (default: $XDG_CACHE_HOME/eg-sqlite3).
set -euo pipefail

VERSION=3500400
RELEASE_YEAR=2025
ARCHIVE_SHA256=2c61fe3f6bfcf4ab0cad6c4edf1fa1cae93bbb816b744bef52a4bdde5bf7bf38

if [ "$(uname -s)-$(uname -m)" != "Linux-x86_64" ]; then
  echo "fetch_sqlite3.sh: only linux x86_64 is pinned; got $(uname -s)-$(uname -m)" >&2
  exit 1
fi

dest="${1:-${XDG_CACHE_HOME:-$HOME/.cache}/eg-sqlite3}"
name="sqlite-tools-linux-x64-$VERSION"
bin="$dest/$name/sqlite3"

if [ ! -x "$bin" ]; then
  mkdir -p "$dest/$name"
  archive="$dest/$name.zip"
  curl -fsSL --retry 3 -o "$archive.part" \
    "https://www.sqlite.org/$RELEASE_YEAR/$name.zip"
  echo "$ARCHIVE_SHA256  $archive.part" | sha256sum --check --quiet -
  mv "$archive.part" "$archive"
  unzip -o -q "$archive" sqlite3 -d "$dest/$name"
  chmod +x "$bin"
fi

"$bin" --version >/dev/null
printf '%s\n' "$bin"
