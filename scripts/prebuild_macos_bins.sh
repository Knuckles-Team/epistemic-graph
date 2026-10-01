#!/usr/bin/env bash
# The real Mac prebuild and bounded probe share this command and its telemetry.
set -uo pipefail
pass="${1:?expected primary or reproduction pass}"
case "$pass" in primary|reproduction) ;; *) exit 2 ;; esac
metrics="${RUNNER_TEMP:?}/eg-macos-build-metrics/$pass"
mkdir -p "$metrics" || exit 2
read -r -a jobs <<< "${EG_WHEEL_JOBS_ARG:?}"
# time's maximum RSS is its reported command metric, not aggregate concurrent RAM.
/usr/bin/time -l cargo build --release --locked --bins --timings \
    --target "${EG_WHEEL_TARGET:?}" --features "${MATURIN_FEATURES:?}" \
    "${jobs[@]}" 2>&1 | tee "$metrics/build.log"
status=${PIPESTATUS[0]}
printf '%s\n' "$status" > "$metrics/exit-status.txt"
if [ -d "${CARGO_TARGET_DIR:?}/cargo-timings" ]; then
    cp -R "$CARGO_TARGET_DIR/cargo-timings" "$metrics/" || \
        echo 'Warning: could not retain Cargo timing reports' >&2
fi
exit "$status"
