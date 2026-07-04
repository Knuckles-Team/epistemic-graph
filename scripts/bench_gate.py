#!/usr/bin/env python3
"""CONCEPT:EG-423 — the perf/recall regression GATE for the eg-plan hybrid_queries bench.

Runs AFTER `cargo bench -p eg-plan --features …` has produced its criterion output. It
reads two artifacts and fails (exit 1) on a regression:

  1. **recall** — the `eg_plan_recall.json` the bench writes (measured recall@k of the
     vector leg vs the brute-force oracle). Must stay >= `recall_floor`. The bench ALSO
     asserts this internally, so a recall regression already fails the bench binary; this
     is the redundant, reported check.

  2. **latency** — criterion's per-bench `estimates.json`
     (`target/criterion/<bench>/new/estimates.json`), the MEDIAN (p50) point estimate in
     nanoseconds. Must stay <= the committed per-bench ceiling. Ceilings are generous
     (see thresholds.json) so this is a catastrophic-regression net, not a p50 chase — it
     never flakes on machine variance. A bench with no estimates (feature not built) is
     skipped.

Usage:
  python3 scripts/bench_gate.py \
      [--thresholds crates/eg-plan/benches/thresholds.json] \
      [--criterion-dir target/criterion] \
      [--recall-json target/eg_plan_recall.json]
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def load_json(path: Path):
    with path.open() as fh:
        return json.load(fh)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    repo = Path(__file__).resolve().parent.parent
    ap.add_argument(
        "--thresholds",
        type=Path,
        default=repo / "crates" / "eg-plan" / "benches" / "thresholds.json",
    )
    ap.add_argument("--criterion-dir", type=Path, default=repo / "target" / "criterion")
    ap.add_argument("--recall-json", type=Path, default=repo / "target" / "eg_plan_recall.json")
    args = ap.parse_args()

    thresholds = load_json(args.thresholds)
    recall_floor = float(thresholds["recall_floor"])
    ceilings: dict[str, float] = thresholds["latency_p50_ns_max"]

    failures: list[str] = []
    reported: list[str] = []

    # ── recall gate ──────────────────────────────────────────────────────────────
    if args.recall_json.exists():
        rec = load_json(args.recall_json)
        measured = float(rec["recall_at_k"])
        k = rec.get("k", "?")
        ok = measured >= recall_floor
        reported.append(
            f"  recall@{k}: {measured:.4f}  (floor {recall_floor:.4f})  "
            f"{'OK' if ok else 'FAIL'}"
        )
        if not ok:
            failures.append(
                f"recall@{k} {measured:.4f} < floor {recall_floor:.4f}"
            )
    else:
        failures.append(
            f"recall artifact missing: {args.recall_json} "
            "(did the bench run with --features query?)"
        )

    # ── latency gate ─────────────────────────────────────────────────────────────
    for name, ceiling in ceilings.items():
        est = args.criterion_dir / name / "new" / "estimates.json"
        if not est.exists():
            reported.append(f"  {name}: (skipped — no estimates; feature not built)")
            continue
        data = load_json(est)
        p50 = float(data["median"]["point_estimate"])  # nanoseconds
        ok = p50 <= float(ceiling)
        reported.append(
            f"  {name}: p50 {p50 / 1e6:.3f} ms  (ceiling {float(ceiling) / 1e6:.1f} ms)  "
            f"{'OK' if ok else 'FAIL'}"
        )
        if not ok:
            failures.append(
                f"{name} p50 {p50 / 1e6:.3f} ms > ceiling {float(ceiling) / 1e6:.1f} ms"
            )

    print("eg-plan perf/recall gate (CONCEPT:EG-423):")
    print("\n".join(reported))

    if failures:
        print("\nREGRESSION — the gate FAILED:", file=sys.stderr)
        for f in failures:
            print(f"  - {f}", file=sys.stderr)
        return 1
    print("\nall perf/recall thresholds held.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
