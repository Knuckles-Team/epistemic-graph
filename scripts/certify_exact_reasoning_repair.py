#!/usr/bin/env python3
"""Certify restart-safe reasoning, retraction, and fenced repair on one artifact.

The caller supplies the executable and its SHA-256.  This harness never discovers or
builds an engine.  It retains only categorical outcomes, counts, and canonical
digests over synthetic results; graph data, endpoints, paths, and authority material
remain inside an automatically removed runtime directory.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import shutil
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

from certify_exact_fault_restart import (
    AGENT_ID,
    GRAPH,
    CertificationError,
    ExactBinary,
    ExactEngine,
    _fail,
    _new_ephemeral_authority,
    _validate_binary,
    _with_client,
    _write_evidence,
)

SCHEMA_VERSION = 1
PROJECTION_TIMEOUT_SECONDS = 30.0
POLL_SECONDS = 0.05

CASES = (
    "projection_lag",
    "restart",
    "contradiction",
    "retraction",
    "valid_transaction_time_change",
    "causal_recomputation",
    "assumptions",
    "counterexamples",
    "repair",
)


def canonical_digest(value: object) -> str:
    body = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(body).hexdigest()


def _private(**properties: object) -> dict[str, object]:
    return {
        **properties,
        "_owner": AGENT_ID,
        "_visibility": "private",
    }


def _poll_materialization(
    engine: ExactEngine,
    expected_status: str,
    *,
    minimum_version: int = 0,
) -> tuple[dict[str, Any], int]:
    deadline = time.monotonic() + PROJECTION_TIMEOUT_SECONDS
    polls = 0
    while time.monotonic() < deadline:
        polls += 1
        try:
            status = _with_client(
                engine,
                GRAPH,
                lambda client: client.query.materialization_status("derived-result"),
            )
        except RuntimeError:
            time.sleep(POLL_SECONDS)
            continue
        if (
            status.get("status") == expected_status
            and int(status.get("source_graph_version", 0)) >= minimum_version
        ):
            return status, polls
        time.sleep(POLL_SECONDS)
    _fail("reasoning_projection_convergence_timeout")


def _status(engine: ExactEngine, node_id: str) -> dict[str, Any]:
    result = _with_client(
        engine,
        GRAPH,
        lambda client: client.query.epistemic_status(node_id),
    )
    status = result.get("status")
    if not isinstance(status, dict):
        _fail("epistemic_status_shape_invalid")
    return status


def _estimates(result: dict[str, Any]) -> dict[str, dict[str, float]]:
    rows = result.get("estimates")
    if not isinstance(rows, list):
        _fail("causal_estimate_shape_invalid")
    output: dict[str, dict[str, float]] = {}
    for row in rows:
        if not isinstance(row, (list, tuple)) or len(row) != 2:
            _fail("causal_estimate_row_invalid")
        name, estimate = row
        if not isinstance(name, str) or not isinstance(estimate, dict):
            _fail("causal_estimate_row_invalid")
        try:
            mean = float(estimate["mean"])
            variance = float(estimate["variance"])
            level = float(estimate["level"])
            interval = estimate["interval"]
            lower, upper = float(interval[0]), float(interval[1])
        except (KeyError, TypeError, ValueError, IndexError):
            _fail("causal_estimate_row_invalid")
        if (
            not all(
                math.isfinite(value) for value in (mean, variance, level, lower, upper)
            )
            or variance < 0.0
            or not 0.0 < level < 1.0
            or not lower <= mean <= upper
        ):
            _fail("causal_estimate_not_calibrated")
        output[name] = {
            "mean": mean,
            "variance": variance,
            "level": level,
            "lower": lower,
            "upper": upper,
        }
    return output


def _counterfactual_values(result: dict[str, Any]) -> dict[str, float]:
    rows = result.get("values")
    if not isinstance(rows, list):
        _fail("causal_counterfactual_shape_invalid")
    output: dict[str, float] = {}
    for row in rows:
        if not isinstance(row, (list, tuple)) or len(row) != 2:
            _fail("causal_counterfactual_row_invalid")
        name, value = row
        if not isinstance(name, str):
            _fail("causal_counterfactual_row_invalid")
        numeric = float(value)
        if not math.isfinite(numeric):
            _fail("causal_counterfactual_not_finite")
        output[name] = numeric
    return output


def _seed(engine: ExactEngine) -> None:
    _with_client(engine, "__commons__", lambda client: client.tenants.create(GRAPH))

    def write(client: Any) -> None:
        # Durable projection fixture.
        client.nodes.add("base-input", _private(type="Fact", revision=0))
        client.nodes.add("model-activity", _private(type="Model"))
        client.nodes.add(
            "derived-result",
            _private(
                type="Claim",
                confidence=0.8,
                invalidation_deps=["base-input"],
                generating_activity="model-activity",
            ),
        )

        # One supported claim that becomes unbelieved when its evidence is removed.
        client.nodes.add("retractable-claim", _private(type="Claim", confidence=0.3))
        client.nodes.add("support-evidence", _private(type="Evidence", confidence=0.9))
        client.edges.add(
            "support-evidence",
            "retractable-claim",
            _private(relationship="SUPPORTS"),
        )

        # Minimal counterexample: removing the deep defeater flips the TMS verdict.
        client.nodes.add("counter-claim", _private(type="Claim", confidence=0.5))
        client.nodes.add("counter-attacker", _private(type="Claim", confidence=0.5))
        client.nodes.add("counter-evidence", _private(type="Evidence", confidence=0.9))
        client.edges.add(
            "counter-evidence",
            "counter-attacker",
            _private(relationship="ATTACKS"),
        )
        client.edges.add(
            "counter-attacker",
            "counter-claim",
            _private(relationship="ATTACKS"),
        )

        # A textbook non-explosive mutual contradiction.
        client.nodes.add("conflict-left", _private(type="Claim", confidence=0.6))
        client.nodes.add("conflict-right", _private(type="Claim", confidence=0.6))
        client.edges.add(
            "conflict-left",
            "conflict-right",
            _private(relationship="CONTRADICTS"),
        )
        client.edges.add(
            "conflict-right",
            "conflict-left",
            _private(relationship="CONTRADICTS"),
        )

        # The attacker is absent at tx=50 and live at tx=200.
        client.nodes.add("temporal-claim", _private(type="Claim", confidence=0.9))
        client.nodes.add(
            "temporal-attacker",
            _private(
                type="Claim",
                confidence=0.9,
                valid_from=100,
                tx_from=100,
            ),
        )
        client.edges.add(
            "temporal-attacker",
            "temporal-claim",
            _private(relationship="ATTACKS"),
        )

    _with_client(engine, GRAPH, write)


def _causal_cases(engine: ExactEngine) -> dict[str, dict[str, object]]:
    variables = [
        {"id": "z", "parents": [], "bias": 0.0, "noise_var": 1.0},
        {"id": "x", "parents": [["z", 1.0]], "bias": 0.0, "noise_var": 0.25},
        {
            "id": "y",
            "parents": [["z", 1.0], ["x", 0.5]],
            "bias": 0.0,
            "noise_var": 0.25,
        },
    ]

    def queries(client: Any) -> tuple[dict[str, Any], ...]:
        intervene = client.query.causal_estimate(
            variables,
            {"x": 2.0},
            mode="Intervene",
        )
        repeated = client.query.causal_estimate(
            variables,
            {"x": 2.0},
            mode="Intervene",
        )
        observe = client.query.causal_estimate(
            variables,
            {"x": 2.0},
            mode="Observe",
        )
        counterfactual = client.query.causal_counterfactual(
            variables,
            {"z": 1.0, "x": 1.0, "y": 1.5},
            {"x": 2.0},
        )
        counterfactual_repeated = client.query.causal_counterfactual(
            variables,
            {"z": 1.0, "x": 1.0, "y": 1.5},
            {"x": 2.0},
        )
        return intervene, repeated, observe, counterfactual, counterfactual_repeated

    intervene, repeated, observe, counterfactual, counterfactual_repeated = (
        _with_client(engine, GRAPH, queries)
    )
    do_estimates = _estimates(intervene)
    _estimates(repeated)
    observed_estimates = _estimates(observe)
    if set(do_estimates) != {"z", "x", "y"}:
        _fail("causal_variable_inventory_invalid")
    if canonical_digest(intervene) != canonical_digest(repeated):
        _fail("causal_recomputation_not_deterministic")
    if abs(do_estimates["x"]["mean"] - 2.0) > 1e-9:
        _fail("causal_intervention_not_applied")
    if abs(do_estimates["y"]["mean"] - 1.0) > 1e-9:
        _fail("causal_structural_effect_invalid")
    if abs(observed_estimates["z"]["mean"] - do_estimates["z"]["mean"]) < 1e-6:
        _fail("causal_observation_intervention_not_distinct")

    values = _counterfactual_values(counterfactual)
    repeated_values = _counterfactual_values(counterfactual_repeated)
    if canonical_digest(counterfactual) != canonical_digest(counterfactual_repeated):
        _fail("causal_counterfactual_not_deterministic")
    if (
        set(values) != {"z", "x", "y"}
        or abs(values["z"] - 1.0) > 1e-9
        or abs(values["x"] - 2.0) > 1e-9
        or abs(values["y"] - 2.0) > 1e-9
        or values != repeated_values
    ):
        _fail("causal_counterfactual_invalid")

    return {
        "causal_recomputation": {
            "deterministic": True,
            "result_sha256": canonical_digest(intervene),
            "variables": len(do_estimates),
        },
        "assumptions": {
            "calibrated_intervals": len(do_estimates),
            "observe_intervene_distinct": True,
            "result_sha256": canonical_digest(
                {"observe": observe, "intervene": intervene}
            ),
        },
        "counterfactual": {
            "deterministic": True,
            "result_sha256": canonical_digest(counterfactual),
            "variables": len(values),
        },
    }


def _run(binary: ExactBinary, binary_digest: str) -> dict[str, object]:
    authority = _new_ephemeral_authority()
    with tempfile.TemporaryDirectory(prefix="eg-exact-reasoning-") as scratch:
        root = Path(scratch)
        engine = ExactEngine(binary, root, authority)
        try:
            engine.start()
            engine.bootstrap()
            _seed(engine)

            fresh, fresh_polls = _poll_materialization(engine, "Fresh")
            fresh_version = int(fresh["source_graph_version"])

            contradiction = _with_client(
                engine,
                GRAPH,
                lambda client: client.query.resolve_conflict(
                    ["conflict-left", "conflict-right"], "grounded"
                ),
            )
            if (
                sorted(contradiction.get("undecided", []))
                != ["conflict-left", "conflict-right"]
                or contradiction.get("surviving")
                or contradiction.get("defeated")
            ):
                _fail("paraconsistent_conflict_resolution_invalid")

            temporal = _with_client(
                engine,
                GRAPH,
                lambda client: client.query.what_changed(50, 200),
            )
            temporal_rows = temporal.get("changed")
            if not isinstance(temporal_rows, list):
                _fail("temporal_change_shape_invalid")
            temporal_claim = next(
                (
                    row
                    for row in temporal_rows
                    if isinstance(row, dict) and row.get("id") == "temporal-claim"
                ),
                None,
            )
            if (
                temporal_claim is None
                or temporal_claim.get("believed_before") is not True
                or temporal_claim.get("believed_after") is not False
            ):
                _fail("valid_transaction_time_change_not_detected")

            counter_status = _status(engine, "counter-claim")
            minimal_flip = counter_status.get("what_would_invalidate")
            if (
                not isinstance(minimal_flip, dict)
                or minimal_flip.get("evidence_ids") != ["counter-evidence"]
                or minimal_flip.get("believed_now") is not True
                or minimal_flip.get("believed_after") is not False
            ):
                _fail("epistemic_counterexample_not_minimal")

            retractable_before = _status(engine, "retractable-claim")
            if retractable_before.get("believed") is not True:
                _fail("retraction_fixture_not_initially_believed")

            causal = _causal_cases(engine)

            # A source mutation advances the authoritative graph immediately.  The
            # projection may briefly expose its prior watermark, then must converge.
            changed = _with_client(
                engine,
                GRAPH,
                lambda client: client.nodes.compare_and_set(
                    "base-input", {"revision": 0}, {"revision": 1}
                ),
            )
            if changed is not True:
                _fail("projection_source_compare_and_set_failed")
            immediate = _with_client(
                engine,
                GRAPH,
                lambda client: client.query.materialization_status("derived-result"),
            )
            stale, stale_polls = _poll_materialization(
                engine,
                "Stale",
                minimum_version=fresh_version + 1,
            )
            stale_version = int(stale["source_graph_version"])
            if stale_version <= fresh_version:
                _fail("reasoning_projection_watermark_did_not_advance")

            # The exact durable image must survive process replacement unchanged.
            stale_digest = canonical_digest(stale)
            engine.stop()
            engine.start()
            restarted_stale, restart_polls = _poll_materialization(
                engine,
                "Stale",
                minimum_version=stale_version,
            )
            if canonical_digest(restarted_stale) != stale_digest:
                _fail("reasoning_projection_restart_mismatch")

            # A pre-invalidation fence is rejected; the current durable watermark is
            # accepted and writes back a Fresh result before acknowledgement.
            stale_fence_denied = False
            try:
                _with_client(
                    engine,
                    GRAPH,
                    lambda client: client.query.recompute_materialization(
                        "derived-result", fresh_version
                    ),
                )
            except RuntimeError:
                stale_fence_denied = True
            if not stale_fence_denied:
                _fail("stale_recompute_fence_was_accepted")
            repair = _with_client(
                engine,
                GRAPH,
                lambda client: client.query.recompute_materialization(
                    "derived-result", stale_version
                ),
            )
            if (
                repair.get("status") != "Fresh"
                or repair.get("projection_pending") is not False
                or int(repair.get("fence_epoch", 0)) <= 0
                or int(repair.get("source_graph_version", 0)) != stale_version
                or len(repair.get("depends_on", [])) != 1
                or not isinstance(repair.get("generating_activity"), str)
            ):
                _fail("reasoning_materialization_repair_invalid")
            repaired, repair_polls = _poll_materialization(
                engine,
                "Fresh",
                minimum_version=stale_version,
            )

            # Remove the sole support and prove both the belief flip and its durable
            # replay after a second process replacement.
            _with_client(
                engine,
                GRAPH,
                lambda client: client.nodes.remove("support-evidence"),
            )
            retractable_after = _status(engine, "retractable-claim")
            if (
                retractable_after.get("believed") is not False
                or retractable_after.get("why_not") is None
            ):
                _fail("belief_retraction_not_applied")
            post_retraction_projection, retraction_polls = _poll_materialization(
                engine,
                "Fresh",
                minimum_version=stale_version + 1,
            )
            retraction_digest = canonical_digest(retractable_after)
            engine.stop()
            engine.start()
            replayed_retraction = _status(engine, "retractable-claim")
            if canonical_digest(replayed_retraction) != retraction_digest:
                _fail("belief_retraction_restart_mismatch")

            matrix = {
                "projection_lag": {
                    "converged": True,
                    "immediate_prior_watermark": int(
                        immediate.get("source_graph_version", 0)
                    )
                    < stale_version,
                    "polls": stale_polls,
                    "watermark_advanced": True,
                },
                "restart": {
                    "durable_projection_equal": True,
                    "polls": restart_polls,
                    "projection_sha256": stale_digest,
                },
                "contradiction": {
                    "classification_sha256": canonical_digest(contradiction),
                    "undecided": len(contradiction["undecided"]),
                },
                "retraction": {
                    "belief_flipped": True,
                    "durable_replay_equal": True,
                    "result_sha256": retraction_digest,
                },
                "valid_transaction_time_change": {
                    "changed": len(temporal_rows),
                    "claim_flipped": True,
                    "result_sha256": canonical_digest(temporal),
                },
                "causal_recomputation": causal["causal_recomputation"],
                "assumptions": causal["assumptions"],
                "counterexamples": {
                    "causal": causal["counterfactual"],
                    "minimal_flip_cardinality": len(minimal_flip["evidence_ids"]),
                    "status_sha256": canonical_digest(counter_status),
                },
                "repair": {
                    "dependencies": len(repair.get("depends_on", [])),
                    "fence_epoch_positive": True,
                    "fresh": repaired.get("status") == "Fresh",
                    "polls": repair_polls,
                    "post_retraction_projection_polls": retraction_polls,
                    "post_retraction_projection_sha256": canonical_digest(
                        post_retraction_projection
                    ),
                    "stale_fence_denied": True,
                },
            }
            if tuple(matrix) != CASES:
                _fail("reasoning_case_inventory_mismatch")
        finally:
            engine.stop()
            shutil.rmtree(root, ignore_errors=True)

    evidence = {
        "binary": {"sha256": binary_digest},
        "cases": list(CASES),
        "certification": "epistemic-graph-exact-reasoning-repair",
        "matrix": matrix,
        "schema_version": SCHEMA_VERSION,
        "summary": {
            "cases": len(matrix),
            "initial_projection_polls": fresh_polls,
            "passed": len(matrix),
            "status": "pass",
        },
    }
    encoded = json.dumps(evidence, sort_keys=True, ensure_ascii=True)
    if authority.auth_secret in encoded or authority.signer_key in encoded:
        _fail("evidence_contains_ephemeral_authority")
    return evidence


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Certify restart-safe reasoning and repair on one exact artifact."
    )
    parser.add_argument("--binary", required=True)
    parser.add_argument("--binary-sha256", required=True)
    parser.add_argument("--output", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    binary: ExactBinary | None = None
    try:
        output = Path(args.output)
        if output.is_symlink() or output.exists():
            _fail("evidence_destination_must_be_new")
        binary, digest = _validate_binary(args.binary, args.binary_sha256)
        _write_evidence(output, _run(binary, digest))
    except CertificationError as error:
        print(f"exact reasoning certification failed: {error}", file=sys.stderr)
        return 1
    except Exception:
        print(
            "exact reasoning certification failed: unexpected_runtime_failure",
            file=sys.stderr,
        )
        return 1
    finally:
        if binary is not None:
            binary.close()
    print("exact reasoning certification passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
