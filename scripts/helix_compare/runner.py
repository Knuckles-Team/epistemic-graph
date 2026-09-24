"""One engine's pass: load, correctness gate, warm and cold timing, footprint.

Order per engine (identical for both): start on an empty data directory; load
the corpus; gate every read workload against the oracle; time the read
workloads warm at each concurrency; restart the server and time the first
queries process-cold; then gate and time the transactional batch LAST, because
it grows the corpus the read oracle describes. A workload whose gate fails is
reported with its failures and never timed.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from .dataset import Dataset, Doc
from .measure import ProcSample, loadavg, timed, tree_bytes
from .oracle import Oracle, check_point_get, check_ranked, check_set, check_text
from .queries import QuerySet

READS = (
    "point_get",
    "one_hop",
    "filtered_hop",
    "text_prefilter",
    "vector_prefilter",
    "mixed",
)
MAX_FAILURE_SAMPLES = 5


class Engine(Protocol):
    name: str
    data_dir: Any

    @property
    def pid(self) -> int: ...

    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    async def load(self, dataset: Dataset) -> int: ...
    async def txn_batch(self, batch: Sequence[tuple[Doc, str]]) -> None: ...
    async def point_get(self, key: str) -> dict[str, Any]: ...
    async def one_hop(self, key: str) -> set[str]: ...


@dataclass(frozen=True)
class Plan:
    gate_ops: int
    warmup_ops: int
    cold_ops: int
    concurrency: tuple[int, ...]


Check = Callable[[Any, Any], "str | None"]


def checks(oracle: Oracle) -> dict[str, Check]:
    return {
        "point_get": lambda item, got: check_point_get(oracle, item, got),
        "one_hop": lambda item, got: check_set(item, oracle.one_hop(item), got),
        "filtered_hop": lambda item, got: check_set(
            item[0], oracle.filtered_hop(*item), got
        ),
        "text_prefilter": lambda item, got: check_text(oracle, item, got),
        "vector_prefilter": lambda item, got: check_ranked(
            oracle, oracle.by_category[item[0]], item[1], item[2], got
        ),
        "mixed": lambda item, got: check_ranked(
            oracle, sorted(oracle.reach(item[0], 2)), item[1], item[2], got
        ),
    }


async def gate(call: Any, items: Sequence[Any], check: Check) -> dict[str, Any]:
    failures: list[str] = []
    for item in items:
        try:
            problem = check(item, await call(item))
        except Exception as error:  # a refused query fails the gate, with its reason
            problem = f"{type(error).__name__}: {error}"[:400]
        if problem is not None:
            failures.append(problem)
    return {
        "checked": len(items),
        "failed": len(failures),
        "failure_samples": failures[:MAX_FAILURE_SAMPLES],
    }


async def gate_txn(
    engine: Engine, oracle: Oracle, batches: Sequence[Sequence[tuple[Doc, str]]]
) -> dict[str, Any]:
    async def apply_and_read_back(batch: Sequence[tuple[Doc, str]]) -> str | None:
        await engine.txn_batch(batch)
        for doc, target in batch:
            oracle.add_doc(doc)
            problem = check_point_get(oracle, doc.key, await engine.point_get(doc.key))
            problem = problem or check_set(
                doc.key, {target}, await engine.one_hop(doc.key)
            )
            if problem:
                return problem
        return None

    return await gate(apply_and_read_back, batches, lambda _item, got: got)


async def timed_with_cpu(
    engine: Engine, call: Any, items: Sequence[Any], concurrency: int
) -> dict[str, Any]:
    before = ProcSample.of(engine.pid)
    timing = await timed(call, items, concurrency)
    after = ProcSample.of(engine.pid)
    summary = timing.summary()
    ops = max(1, summary["ops"])
    summary["server_cpu_ms_per_op"] = round(
        (after.cpu_s - before.cpu_s) * 1000 / ops, 3
    )
    summary["concurrency"] = concurrency
    return summary


async def warm(
    engine: Engine, call: Any, items: Sequence[Any], plan: Plan
) -> dict[str, Any]:
    await timed(call, items[: plan.warmup_ops], 1)
    return {
        f"c{level}": await timed_with_cpu(engine, call, items, level)
        for level in plan.concurrency
    }


def _segment(label: str) -> dict[str, Any]:
    return {"event": label, "loadavg": loadavg(), "at": time.time()}


class EnginePass:
    """Drives one engine through the whole plan and returns its report."""

    def __init__(
        self, engine: Engine, dataset: Dataset, queries: QuerySet, plan: Plan
    ) -> None:
        self.engine, self.dataset, self.queries, self.plan = (
            engine,
            dataset,
            queries,
            plan,
        )
        self.oracle = Oracle.of(dataset)
        self.report: dict[str, Any] = {"engine": engine.name, "timeline": []}
        self.peak_hwm_kb = 0

    def mark(self, label: str) -> None:
        self.report["timeline"].append(_segment(label))

    def sample_memory(self) -> None:
        sample = ProcSample.of(self.engine.pid)
        self.peak_hwm_kb = max(self.peak_hwm_kb, sample.hwm_kb)

    async def run(self) -> dict[str, Any]:
        self.mark("start")
        await self.engine.start()
        self.report["idle_rss_kb"] = ProcSample.of(self.engine.pid).rss_kb
        await self.load()
        read_ok = await self.gate_reads()
        self.report["warm"] = await self.warm_reads(read_ok)
        await self.restart()
        self.report["cold"] = await self.cold_reads(read_ok)
        self.report["txn_batch"] = await self.transactions()
        self.sample_memory()
        self.report["storage_bytes_final"] = tree_bytes(self.engine.data_dir)
        await self.engine.stop()
        self.report["peak_rss_kb"] = self.peak_hwm_kb
        self.mark("end")
        return self.report

    async def load(self) -> None:
        began = time.perf_counter()
        operations = await self.engine.load(self.dataset)
        seconds = time.perf_counter() - began
        stored = tree_bytes(self.engine.data_dir)
        logical = self.dataset.logical_bytes()
        self.report["load"] = {
            "seconds": round(seconds, 2),
            "operations": operations,
            "docs_per_s": round(len(self.dataset.docs) / seconds, 1),
            "storage_bytes": stored,
            "logical_bytes": logical,
            "storage_amplification": round(stored / logical, 3),
        }
        self.sample_memory()
        self.mark("loaded")

    def items(self, name: str) -> Sequence[Any]:
        return getattr(self.queries, name)

    async def gate_reads(self) -> set[str]:
        table = checks(self.oracle)
        self.report["gate"] = {
            name: await gate(
                getattr(self.engine, name),
                self.items(name)[: self.plan.gate_ops],
                table[name],
            )
            for name in READS
        }
        self.mark("gated")
        return {name for name in READS if self.report["gate"][name]["failed"] == 0}

    async def warm_reads(self, passed: set[str]) -> dict[str, Any]:
        results: dict[str, Any] = {
            "transport_floor": await warm(
                self.engine, self.engine_call("ping"), [None] * 2000, self.plan
            )
        }
        for name in READS:
            if name in passed:
                call = self.engine_call(name)
                results[name] = await warm(
                    self.engine, call, self.items(name), self.plan
                )
        self.sample_memory()
        self.mark("warm-timed")
        return results

    def engine_call(self, name: str) -> Any:
        return getattr(self.engine, name)

    async def restart(self) -> None:
        await self.engine.stop()
        await self.engine.start()
        self.mark("restarted")

    async def cold_reads(self, passed: set[str]) -> dict[str, Any]:
        results = {}
        for name in READS:
            if name in passed:
                items = self.items(name)[-self.plan.cold_ops :]
                results[name] = await timed_with_cpu(
                    self.engine, self.engine_call(name), items, 1
                )
        self.sample_memory()
        self.mark("cold-timed")
        return results

    async def transactions(self) -> dict[str, Any]:
        batches = self.queries.txn_batch
        gate_size = self.plan.gate_ops
        report: dict[str, Any] = {
            "gate": await gate_txn(self.engine, self.oracle, batches[:gate_size])
        }
        if report["gate"]["failed"] == 0:
            rest = batches[gate_size:]
            share = len(rest) // len(self.plan.concurrency)
            for index, level in enumerate(self.plan.concurrency):
                chunk = rest[index * share : (index + 1) * share]
                report[f"c{level}"] = await timed_with_cpu(
                    self.engine, self.engine.txn_batch, chunk, level
                )
        self.mark("txn-timed")
        return report
