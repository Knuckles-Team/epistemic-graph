"""Pure absent-versus-hidden verdict for a governed source class."""

from __future__ import annotations

from typing import Literal

HydrationVerdict = Literal["complete", "partial", "not_started", "blocked"]


def fuse_hydration_verdict(
    expected: int | None,
    actual_serving: int | None,
    actual_service: int | None,
    service_attempted: bool,
) -> tuple[HydrationVerdict, str]:
    """The absent-vs-hidden decision rule.

    CONCEPT:AU-KG.audit.hydration-absent-vs-hidden.

    A divergence — 0 under the serving principal, >0 under service authority
    — is ALWAYS ``blocked`` (an RLS visibility issue), never ``not_started``,
    regardless of whether ``expected`` is known.
    """
    if actual_serving is None:
        return (
            "blocked",
            "the serving-principal read failed; completeness cannot be assessed",
        )

    if (
        service_attempted
        and actual_service is not None
        and actual_serving == 0
        and actual_service > 0
    ):
        return (
            "blocked",
            f"0 under the serving principal but {actual_service} under service "
            "authority — an RLS visibility gap, not a hydration failure",
        )

    if expected is None:
        if actual_serving > 0:
            return (
                "complete",
                "no declared-universe count is available for this class; nodes "
                "are present under the serving principal",
            )
        if service_attempted and actual_service == 0:
            return (
                "not_started",
                "0 under both the serving principal and service authority; "
                "genuinely never ingested",
            )
        return (
            "not_started",
            "0 under the serving principal; no declared universe exists to size "
            "an expected count, and service-authority confirmation was "
            + ("unavailable" if not service_attempted else "inconclusive"),
        )

    if expected == 0:
        return "complete", "the declared universe for this class is empty"

    if actual_serving >= expected:
        return (
            "complete",
            f"{actual_serving}/{expected} present under the serving principal",
        )

    if actual_serving == 0:
        if service_attempted and actual_service == 0:
            return (
                "not_started",
                f"expected {expected}, 0 under both the serving principal and "
                "service authority; never ingested",
            )
        return (
            "blocked",
            f"expected {expected}, 0 under the serving principal; "
            + (
                "service-authority read failed too"
                if service_attempted
                else "no service-authority read was available"
            )
            + " — an RLS visibility gap cannot be ruled out",
        )

    return "partial", f"{actual_serving}/{expected} present under the serving principal"
