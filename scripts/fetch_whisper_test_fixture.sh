#!/usr/bin/env bash
# Fetch the model and speech sample that eg-asr-whisper's real-model tests
# (`crates/eg-asr-whisper/tests/real_transcription.rs`) need, and print the two
# environment assignments those tests read.
#
#   export $(scripts/fetch_whisper_test_fixture.sh)
#   cargo test -p eg-asr-whisper --test real_transcription
#
# When the variables are unset the tests FAIL; they never skip.
#
# - Model: whisper.cpp's `ggml-tiny.en.bin` (English, ~75 MB), from a pinned
#   Hugging Face revision of `ggerganov/whisper.cpp`.
# - Audio: whisper.cpp's `samples/jfk.wav` (11 s, 16 kHz mono PCM16), from the
#   `v1.7.6` tag.
#
# Both files are checked against pinned sha256 digests before use. The crate
# itself never downloads a model (GOC-36 owns acquisition); this script is test
# tooling only.
#
# Optional argument: cache directory (default: $XDG_CACHE_HOME/eg-whisper-test).
set -euo pipefail

MODEL_REVISION=5359861c739e955e79d9a303bcbc70fb988958b1
MODEL_SHA256=921e4cf8686fdd993dcd081a5da5b6c365bfde1162e72b08d75ac75289920b1f
WAV_TAG=v1.7.6
WAV_SHA256=59dfb9a4acb36fe2a2affc14bacbee2920ff435cb13cc314a08c13f66ba7860e

dest="${1:-${XDG_CACHE_HOME:-$HOME/.cache}/eg-whisper-test}"
mkdir -p "$dest"

# fetch <url> <sha256> <path>: download to a .part file, verify, then rename.
fetch() {
  local url="$1" sha="$2" path="$3"
  if [ -f "$path" ] && echo "$sha  $path" | sha256sum --check --quiet - 2>/dev/null; then
    return
  fi
  curl -fsSL --retry 3 -o "$path.part" "$url"
  echo "$sha  $path.part" | sha256sum --check --quiet -
  mv "$path.part" "$path"
}

model="$dest/ggml-tiny.en.bin"
wav="$dest/jfk-16k-mono.wav"
fetch "https://huggingface.co/ggerganov/whisper.cpp/resolve/$MODEL_REVISION/ggml-tiny.en.bin" \
  "$MODEL_SHA256" "$model"
fetch "https://raw.githubusercontent.com/ggml-org/whisper.cpp/$WAV_TAG/samples/jfk.wav" \
  "$WAV_SHA256" "$wav"

printf 'EG_ASR_TEST_MODEL_PATH=%s\n' "$model"
printf 'EG_ASR_TEST_WAV_PATH=%s\n' "$wav"
