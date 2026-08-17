"""GOC-33 (`OWNER-VOICE-ASR`): a thin `audio_transcriber.asr_providers`
`TranscriptionProvider` factory backed by this package's own out-of-process
`epistemic_graph.client` (MessagePack/UDS) — the SAME transport
`agent-utilities` already uses, per the sibling lane's coordination note. No
second transport is introduced here.

This module lives in `epistemic-graph`'s own Python surface (not in
`agent-packages/agents/audio-transcriber`, which this lane does not own) and
is registered under `audio_transcriber.asr_providers` via this package's own
`pyproject.toml` entry point (`epistemic-graph = "epistemic_graph.asr_provider:build_provider"`).
`audio-transcriber` discovers it by entry point and construction is
try/except-guarded on that side — if the engine/native dependency is
unreachable, the server still starts and the Faster-Whisper path still serves;
this module does not fight that.

Model acquisition/verification is explicitly NOT this module's job (GOC-36):
the caller (this class) must be told a model path + its declared SHA-256
digest via `EPISTEMIC_GRAPH_ASR_MODEL_PATH`/`EPISTEMIC_GRAPH_ASR_MODEL_SHA256`.
Digest verification and temp-file handling for the INPUT audio happen ABOVE
this seam, in `audio-transcriber`, per the coordination note — this class
receives an already-resolved `Path` and reads it directly.
"""

from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any, ClassVar

from epistemic_graph.client import SyncEpistemicGraphClient

#: How long a successful liveness probe is trusted before re-checking. Keeps
#: `is_available()` genuinely cheap (the engine's own measured ~2.4s p99 on a
#: point read means a *per-call* health round trip would be the dominant cost
#: of every transcription request otherwise).
_HEALTH_TTL_SECONDS = 30.0

#: Bounded WAV upload size this provider will read into memory and send over
#: the wire in one request. `audio-transcriber` is expected to pass a
#: finalized (non-streaming) file for the batch `transcribe()` call; a larger
#: input belongs on the future streaming path, not this convenience wrapper.
_MAX_WAV_BYTES = 64 * 1024 * 1024


class EpistemicGraphAsrProvider:
    """Satisfies `audio_transcriber.asr_providers.TranscriptionProvider`."""

    name: ClassVar[str] = "epistemic-graph"

    def __init__(self) -> None:
        self._client: SyncEpistemicGraphClient | None = None
        self._last_health_ok_at: float = 0.0
        self._last_health_result: bool = False

    # ── connection ──────────────────────────────────────────────────────

    def _build_context(self) -> dict[str, Any]:
        tenant = os.environ.get("EPISTEMIC_GRAPH_TENANT", "")
        audience = os.environ.get("EPISTEMIC_GRAPH_AUDIENCE", "")
        policy_version = os.environ.get("EPISTEMIC_GRAPH_POLICY_VERSION", "")
        if not (tenant and audience and policy_version):
            raise RuntimeError(
                "EPISTEMIC_GRAPH_TENANT/EPISTEMIC_GRAPH_AUDIENCE/"
                "EPISTEMIC_GRAPH_POLICY_VERSION must be set to reach the engine"
            )
        return {
            "principal": "audio-transcriber",
            "tenant": tenant,
            "audience": audience,
            "agent_id": "audio-transcriber",
            "roles": [],
            "scopes": ["asr:transcribe"],
            "policy_version": policy_version,
            "delegation": [],
        }

    def _connect(self) -> SyncEpistemicGraphClient:
        if self._client is not None:
            return self._client
        socket_path = os.environ.get("GRAPH_SERVICE_SOCKET")
        tcp_addr = os.environ.get("GRAPH_SERVICE_ENDPOINTS")
        client = SyncEpistemicGraphClient.connect(
            socket_path=socket_path,
            tcp_addr=None if socket_path else tcp_addr,
            verified_context=self._build_context(),
        )
        self._client = client
        return client

    # ── TranscriptionProvider Protocol ─────────────────────────────────

    def is_available(self) -> bool:
        """Cheap, side-effect-managed, TTL-cached, fails closed.

        Unknown health means unavailable — a missing config or a failed
        connect/health round trip both return `False`, never an assumed
        `True`.
        """
        now = time.monotonic()
        if now - self._last_health_ok_at < _HEALTH_TTL_SECONDS:
            return self._last_health_result
        try:
            client = self._connect()
            client.health()
            self._last_health_result = True
        except Exception:
            self._last_health_result = False
            self._client = None  # force a fresh connect next time
        self._last_health_ok_at = now
        return self._last_health_result

    def transcribe(
        self,
        path: Path,
        *,
        model: str,
        language: str | None = None,
        task: str = "transcribe",
    ) -> dict[str, Any]:
        """Batch, final-only convenience call over the streaming-capable
        engine provider (`eg-asr-whisper`) — one request, one response, the
        Whisper-shaped `{"text", "segments": [...], "language"}` the
        `TranscriptionProvider` Protocol expects. `model` is interpreted as a
        local ggml model FILE PATH (this provider never resolves a bare model
        name against a registry/URL); its declared digest comes from
        `EPISTEMIC_GRAPH_ASR_MODEL_SHA256` (GOC-36 owns real manifest-based
        digest distribution — this env var is today's stand-in).
        """
        model_sha256 = os.environ.get("EPISTEMIC_GRAPH_ASR_MODEL_SHA256", "")
        if not model_sha256:
            raise RuntimeError(
                "EPISTEMIC_GRAPH_ASR_MODEL_SHA256 must be set — this provider "
                "never loads a model without a caller-declared digest"
            )
        audio_bytes = path.read_bytes()
        if len(audio_bytes) > _MAX_WAV_BYTES:
            raise ValueError(
                f"audio file exceeds the {_MAX_WAV_BYTES}-byte bound for this batch call"
            )
        client = self._connect()
        result = client.asr.transcribe_file(
            audio_bytes,
            model_path=model,
            model_sha256=model_sha256,
            language=language,
            translate=(task == "translate"),
            word_timing=False,
        )
        return {
            "text": result.get("text", ""),
            "language": result.get("language", ""),
            "segments": result.get("segments", []),
        }

    def supports_streaming(self) -> bool:
        """Honest `False` today: this class wraps the engine's streaming-
        capable provider (`eg-asr-whisper::WhisperAsrProvider::
        transcribe_streaming`) in a batch call only. A chunk-iterator method
        is not implemented here — GOC-35's full-duplex `VoiceSession` lane
        owns wiring a real streaming RPC surface on top of the same engine
        provider; this class will answer `True` once that iterator method
        exists on the sibling lane's Protocol and is implemented here, not
        before.
        """
        return False


def build_provider() -> EpistemicGraphAsrProvider:
    """Zero-arg factory `audio_transcriber.asr_providers` discovers by entry
    point (`[project.entry-points."audio_transcriber.asr_providers"]`)."""
    return EpistemicGraphAsrProvider()
