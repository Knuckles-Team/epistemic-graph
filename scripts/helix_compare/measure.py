"""Latency, CPU, memory, storage and host probes shared by both engines.

CPU and memory are read from the SERVER process (``/proc/<pid>``), never the
driver, so the Python client's own cost shows up only in latency -- and every
report carries a transport-floor row (a no-op round trip through the same
client) so that cost is visible rather than hidden.
"""

from __future__ import annotations

import asyncio
import math
import os
import platform
import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

CLOCK_TICKS = os.sysconf("SC_CLK_TCK")
MAX_ERROR_SAMPLES = 5


def percentile(sorted_values: Sequence[float], fraction: float) -> float:
    """Nearest-rank percentile of an already sorted sequence."""

    if not sorted_values:
        return float("nan")
    rank = max(1, min(len(sorted_values), math.ceil(fraction * len(sorted_values))))
    return sorted_values[rank - 1]


@dataclass
class Timing:
    latencies_ms: list[float] = field(default_factory=list)
    errors: int = 0
    error_samples: list[str] = field(default_factory=list)
    wall_s: float = 0.0

    def record_error(self, error: BaseException) -> None:
        self.errors += 1
        if len(self.error_samples) < MAX_ERROR_SAMPLES:
            self.error_samples.append(f"{type(error).__name__}: {error}"[:400])

    def summary(self) -> dict[str, Any]:
        ordered = sorted(self.latencies_ms)
        done = len(ordered)
        return {
            "ops": done,
            "errors": self.errors,
            "error_samples": self.error_samples,
            "p50_ms": round(percentile(ordered, 0.50), 3),
            "p95_ms": round(percentile(ordered, 0.95), 3),
            "p99_ms": round(percentile(ordered, 0.99), 3),
            "max_ms": round(ordered[-1], 3) if ordered else None,
            "throughput_ops_s": round(done / self.wall_s, 1) if self.wall_s else None,
        }


async def timed(
    call: Callable[[Any], Awaitable[object]], inputs: Sequence[Any], concurrency: int
) -> Timing:
    """Run ``call`` over ``inputs`` with at most ``concurrency`` in flight."""

    timing = Timing()
    gate = asyncio.Semaphore(concurrency)

    async def one(item: Any) -> None:
        async with gate:
            start = time.perf_counter()
            try:
                await call(item)
            except Exception as error:  # every failure is counted and sampled
                timing.record_error(error)
                return
            timing.latencies_ms.append((time.perf_counter() - start) * 1000.0)

    began = time.perf_counter()
    await asyncio.gather(*(one(item) for item in inputs))
    timing.wall_s = time.perf_counter() - began
    return timing


@dataclass(frozen=True)
class ProcSample:
    cpu_s: float
    rss_kb: int
    hwm_kb: int

    @classmethod
    def of(cls, pid: int) -> ProcSample:
        fields = Path(f"/proc/{pid}/stat").read_text().rsplit(")", 1)[1].split()
        cpu = (int(fields[11]) + int(fields[12])) / CLOCK_TICKS
        status = dict(
            line.split(":", 1)
            for line in Path(f"/proc/{pid}/status").read_text().splitlines()
            if ":" in line
        )
        return cls(cpu, _kb(status.get("VmRSS")), _kb(status.get("VmHWM")))


def _kb(value: str | None) -> int:
    return int(value.split()[0]) if value else 0


def tree_bytes(root: Path) -> int:
    """Allocated bytes under ``root`` (``st_blocks``), like ``du -B1``."""

    return sum(
        (Path(base) / name).lstat().st_blocks * 512
        for base, _, names in os.walk(root)
        for name in names
    )


def loadavg() -> list[float]:
    return [float(value) for value in Path("/proc/loadavg").read_text().split()[:3]]


def _cgroup_limits() -> dict[str, str]:
    relative = Path("/proc/self/cgroup").read_text().strip().split(":", 2)[-1]
    base = Path("/sys/fs/cgroup") / relative.lstrip("/")
    limits = {"path": relative}
    for name in ("cpu.max", "memory.max", "memory.high"):
        path = base / name
        limits[name] = path.read_text().strip() if path.exists() else "absent"
    return limits


def _cpu_model() -> str:
    for line in Path("/proc/cpuinfo").read_text().splitlines():
        if line.startswith("model name"):
            return line.split(":", 1)[1].strip()
    return platform.processor()


def host_probe() -> dict[str, Any]:
    return {
        "hostname": platform.node(),
        "kernel": platform.release(),
        "cpu_model": _cpu_model(),
        "online_cpus": os.cpu_count(),
        "python": platform.python_version(),
        "cgroup": _cgroup_limits(),
        "loadavg": loadavg(),
        "at_unix": int(time.time()),
    }


def parse_cpus(value: str) -> set[int]:
    """``"0-3,6"`` -> ``{0, 1, 2, 3, 6}``."""

    cpus: set[int] = set()
    for part in value.split(","):
        low, _, high = part.partition("-")
        cpus.update(range(int(low), int(high or low) + 1))
    return cpus
