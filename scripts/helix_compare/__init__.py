"""Correctness-gated HelixDB / Epistemic Graph comparative benchmark (EH-353).

``scripts/bench_helix_compare.py`` is the entry point. The package splits the
harness by concern: ``dataset`` (seeded corpus + pinned digest), ``queries``
(the seeded query set), ``oracle`` (brute-force expected answers and the
checks both engines must pass before any timing is reported), ``measure``
(latency, CPU, RSS, storage and host-load probes), and one adapter per engine.
"""
