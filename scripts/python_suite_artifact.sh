#!/usr/bin/env bash
# Same-run validation artifacts only. Tar retains executable extension/server modes.
set -euo pipefail
mode="${1:?pack or unpack}"
kind="${2:?kernels or server}"
case "$kind" in
  kernels) files=(epistemic_graph/numeric*.so epistemic_graph/engine*.so) ;;
  server) files=(target-isolated/debug/epistemic-graph-server) ;;
  *) echo "Unknown artifact kind: $kind" >&2; exit 1 ;;
esac
bundle="${RUNNER_TEMP:?}/python-$kind"
revision="$(git rev-parse HEAD)"
test "$revision" = "${GITHUB_SHA:?}"
case "$mode" in
  pack)
    for file in "${files[@]}"; do test -s "$file"; done
    mkdir -p "$bundle"
    printf '%s\n' "$revision" > "$bundle/source.sha"
    tar -cf "$bundle/payload.tar" "${files[@]}"
    (cd "$bundle" && sha256sum source.sha payload.tar > SHA256SUMS)
    ;;
  unpack)
    (cd "$bundle" && sha256sum --strict --check SHA256SUMS)
    test "$(cat "$bundle/source.sha")" = "$revision"
    tar -xf "$bundle/payload.tar"
    if [ "$kind" = server ]; then
      test -s "${files[0]}"
      chmod +x "${files[0]}"
      test -x "${files[0]}"
    else
      # Expand after extraction: both extensions must be present and nonempty.
      for module in numeric engine; do
        found=false
        for file in epistemic_graph/"$module"*.so; do
          test -s "$file"
          found=true
        done
        "$found"
      done
    fi
    ;;
  *) echo "Unknown artifact mode: $mode" >&2; exit 1 ;;
esac
