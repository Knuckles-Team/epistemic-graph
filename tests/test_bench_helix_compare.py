"""EH-353 harness: pinned inputs reproduce, and the oracle rejects wrong answers.

No engine runs here. The comparison itself runs on a build host
(``scripts/bench_helix_compare.py``); these tests keep its inputs and its
correctness gate honest between runs.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from helix_compare import dataset as corpus
from helix_compare import queries as query_set
from helix_compare.eg_engine import cypher_ids, uql_ids
from helix_compare.helix_engine import rows
from helix_compare.measure import Timing, parse_cpus, percentile
from helix_compare.oracle import Oracle, check_ranked, check_set, check_text
from helix_compare.report import render

pytestmark = pytest.mark.no_engine

SPEC = json.loads(
    (
        Path(__file__).resolve().parents[1] / "benches/helix_compare/spec.json"
    ).read_text()
)
TINY = corpus.DatasetSpec(
    seed=7,
    docs=60,
    citations_per_doc=3,
    categories=3,
    vocabulary=40,
    words_per_doc=8,
    dim=4,
    year_min=2000,
    year_max=2010,
)


def _workloads() -> dict[str, query_set.WorkloadSpec]:
    return {
        name: query_set.WorkloadSpec(**values)
        for name, values in SPEC["workloads"].items()
    }


def test_the_pinned_corpus_and_query_set_reproduce() -> None:
    data = corpus.generate(corpus.DatasetSpec.from_json(SPEC["dataset"]))
    assert data.digest() == SPEC["dataset_sha256"]
    queries = query_set.generate(data, Oracle.of(data), _workloads())
    assert queries.digest() == SPEC["queries_sha256"]


def test_the_citation_graph_is_a_dag_so_hops_never_return_to_the_start() -> None:
    data = corpus.generate(TINY)
    oracle = Oracle.of(data)
    for doc in data.docs:
        assert doc.key not in oracle.reach(doc.key, 3)
        assert all(target < doc.key for target in oracle.one_hop(doc.key))


def test_exact_checks_name_what_is_missing_and_extra() -> None:
    assert check_set("d1", {"a", "b"}, {"a", "b"}) is None
    problem = check_set("d1", {"a", "b"}, {"a", "c"})
    assert problem == "d1: missing ['b'] extra ['c']"


def test_text_check_requires_the_term_the_prefilter_and_the_count() -> None:
    data = corpus.generate(TINY)
    oracle = Oracle.of(data)
    group, term = data.docs[0].category, data.docs[0].text.split()[0]
    eligible = oracle.text_eligible(group, term)
    k = len(eligible)
    assert check_text(oracle, (group, term, k), eligible) is None
    assert "hits, expected" in check_text(oracle, (group, term, k), eligible[:-1])
    outsider = next(d.key for d in data.docs if d.category != group)
    assert "fail the prefilter" in check_text(oracle, (group, term, k), [outsider])


def test_vector_check_accepts_the_exact_top_k_and_rejects_a_weaker_hit() -> None:
    data = corpus.generate(TINY)
    oracle = Oracle.of(data)
    group = data.docs[0].category
    candidates = oracle.by_category[group]
    vector = data.docs[0].embedding
    ranked = sorted(
        candidates,
        key=lambda key: (
            -sum(a * b for a, b in zip(oracle.docs[key].embedding, vector, strict=True))
        ),
    )
    assert check_ranked(oracle, candidates, vector, 3, ranked[:3]) is None
    weaker = [ranked[0], ranked[1], ranked[-1]]
    assert "rank below" in check_ranked(oracle, candidates, vector, 3, weaker)
    assert "outside" in check_ranked(oracle, candidates[:1], vector, 1, [ranked[-1]])


def test_percentiles_are_nearest_rank_and_timing_counts_errors() -> None:
    values = [float(v) for v in range(1, 101)]
    assert percentile(values, 0.50) == 50.0
    assert percentile(values, 0.99) == 99.0
    timing = Timing(latencies_ms=[1.0, 2.0], wall_s=1.0)
    timing.record_error(ValueError("boom"))
    summary = timing.summary()
    assert summary["ops"] == 2 and summary["errors"] == 1
    assert summary["error_samples"] == ["ValueError: boom"]


def test_result_shape_adapters() -> None:
    assert uql_ids([["a", 0.5], ["b", None]]) == ["a", "b"]
    assert uql_ids({"kind": "rows", "rows": [{"id": "a", "score": 1.0}]}) == ["a"]
    assert cypher_ids([{"b": "x"}, {"b": {"id": "y"}}], "b") == {"x", "y"}
    assert rows({"r": [{"key": "a"}]}, "r") == [{"key": "a"}]
    assert rows({"r": {"rows": [{"key": "a"}]}}, "r") == [{"key": "a"}]
    assert rows({}, "r") == []
    assert parse_cpus("0-2,5") == {0, 1, 2, 5}


def test_the_report_renders_gates_timings_and_footprint() -> None:
    summary = {
        "ops": 10,
        "errors": 0,
        "p50_ms": 1.0,
        "p95_ms": 2.0,
        "p99_ms": 3.0,
        "throughput_ops_s": 9.0,
        "server_cpu_ms_per_op": 0.5,
        "concurrency": 1,
    }
    engine = {
        "engine": "e",
        "gate": {"point_get": {"checked": 5, "failed": 0, "failure_samples": []}},
        "warm": {"point_get": {"c1": summary}},
        "cold": {"point_get": summary},
        "txn_batch": {"gate": {"checked": 1, "failed": 0}, "c1": summary},
        "load": {"seconds": 1.0, "storage_bytes": 10, "storage_amplification": 2.0},
    }
    markdown = render({"engines": [engine]})
    assert "| point_get | e | 5 | 0 |" in markdown
    assert markdown.count("| point_get | e | 1 | 10 | 0 | 1.0 |") == 2
    assert "| txn_batch | e | 1 | 10 |" in markdown
