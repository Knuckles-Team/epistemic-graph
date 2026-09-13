#!/usr/bin/env bash
# Fetch the ONNX Runtime shared library that eg-tts-piper's synthesis tests load
# under `ort-load-dynamic` (on under `--all-features`), and print its path.
#
#   export ORT_DYLIB_PATH="$(scripts/fetch_onnxruntime.sh)"
#   cargo test -p eg-tts-piper --all-features
#
# The version matches the ONNX Runtime release `ort-sys` 2.0.0-rc.12 pins
# (api-24). An older library such as Debian's `libonnxruntime1.23` cannot
# serve that API version. The archive is checked against a pinned sha256
# before use. The official Microsoft release was measured running these
# tests on a host without AVX2 (Westmere-class R820).
#
# Optional argument: cache directory (default: $XDG_CACHE_HOME/eg-onnxruntime).
set -euo pipefail

VERSION=1.24.2
ARCHIVE_SHA256=43725474ba5663642e17684717946693850e2005efbd724ac72da278fead25e6

if [ "$(uname -s)-$(uname -m)" != "Linux-x86_64" ]; then
  echo "fetch_onnxruntime.sh: only linux x86_64 is pinned; got $(uname -s)-$(uname -m)" >&2
  exit 1
fi

dest="${1:-${XDG_CACHE_HOME:-$HOME/.cache}/eg-onnxruntime}"
name="onnxruntime-linux-x64-$VERSION"
lib="$dest/$name/lib/libonnxruntime.so.$VERSION"

if [ ! -f "$lib" ]; then
  mkdir -p "$dest"
  archive="$dest/$name.tgz"
  curl -fsSL --retry 3 -o "$archive.part" \
    "https://github.com/microsoft/onnxruntime/releases/download/v$VERSION/$name.tgz"
  echo "$ARCHIVE_SHA256  $archive.part" | sha256sum --check --quiet -
  mv "$archive.part" "$archive"
  tar -xzf "$archive" -C "$dest"
fi

test -f "$lib"
printf '%s\n' "$lib"
