#!/usr/bin/python
from __future__ import annotations

"""Multi-objective workload placement optimizer (CONCEPT:EG.infra.placement-optimizer / KG-2.49).

Reads the infra subgraph (hosts + services) and produces a placement plan that
balances four objectives, instead of the single efficiency heuristic the
deployment-planner skill documented:

  1. **Efficiency / bin-packing** — the deployment-planner scoring *verbatim*
     (``capacity − density + affinity + storage + network``), kept as the
     canonical objective so efficiency-only weights reproduce its behavior.
  2. **Security / data-residency / blast-radius** — penalize placing a service in
     a region that violates its data-residency, concentrating critical (T0/T1)
     services on one host, or landing on a host with open CIS/PCI findings.
  3. **Cost / power** — prefer already-powered hosts and lower power/TDP draw;
     penalize lighting up an idle host (consolidation bias).
  4. **Resilience** — reward anti-affinity: spread a service's replicas across
     distinct hosts/chassis.

``FINAL = Σ wᵢ·objᵢ`` with efficiency-dominant canonical weights; callers may
select a different objective balance via ``weights=``. The planner core is pure over
dataclasses (deterministic, testable);
:func:`optimize_from_graph` adapts the live KG and writes the plan back
propose-only.
"""

from dataclasses import dataclass, field
from typing import Any

# Service tiers (T0 critical … T6 media), matching the deployment-planner skill.
# Tier → required host role for pinning ("" = no pin).
TIER_PIN: dict[str, str] = {
    "T0": "gateway",
    "T1": "",
    "T2": "",
    "T3": "",
    "T4": "ai",  # GPU workloads
    "T5": "",
    "T6": "nas",
}
_CRITICAL_TIERS = {"T0", "T1"}

DEFAULT_OBJECTIVE_WEIGHTS: dict[str, float] = {
    "efficiency": 1.0,  # dominant → efficiency-only reproduces deployment-planner
    "security": 0.4,
    "cost": 0.3,
    "resilience": 0.3,
}


@dataclass
class HostCapacity:
    """A candidate host's capacity + placement-relevant attributes."""

    host_id: str
    cpu_cores: float
    ram_total_gb: float
    ram_available_gb: float
    disk_total_gb: float
    disk_used_pct: float = 0.0
    gpu: bool = False
    role: str = ""  # gateway | ai | nas | worker | ...
    region: str = ""
    chassis: str = ""
    powered_on: bool = True
    power_watts: float = 100.0
    security_findings: int = 0
    overlays: set[str] = field(default_factory=set)
    volumes: set[str] = field(default_factory=set)
    # Mutated during planning to reflect committed placements.
    placed_services: list[str] = field(default_factory=list)


@dataclass
class ServiceSpec:
    """A service to place."""

    service_id: str
    tier: str = "T3"
    cpu_req: float = 0.5
    ram_req_gb: float = 0.5
    disk_req_gb: float = 1.0
    needs_gpu: bool = False
    replica: int = 0  # replica index (for anti-affinity)
    group: str = ""  # replica group key (service name without replica idx)
    data_residency: str = ""  # required region, "" = any
    depends_on: set[str] = field(default_factory=set)  # co-location affinity
    volumes: set[str] = field(default_factory=set)
    overlays: set[str] = field(default_factory=set)
    current_host: str = ""


@dataclass
class ServicePlacement:
    """A chosen placement for one service, with per-objective scores."""

    service_id: str
    host_id: str
    tier: str
    final_score: float
    scores: dict[str, float]
    reason: str
    migrates_from: str = ""


@dataclass
class PlacementPlan:
    """The full placement plan + golden blueprint."""

    placements: list[ServicePlacement] = field(default_factory=list)
    unplaced: list[str] = field(default_factory=list)
    migrations: list[ServicePlacement] = field(default_factory=list)

    def blueprint(self) -> dict[str, Any]:
        """Golden-blueprint dict (host → services) for export."""
        by_host: dict[str, list[str]] = {}
        for p in self.placements:
            by_host.setdefault(p.host_id, []).append(p.service_id)
        return {
            "hosts": by_host,
            "migrations": [
                {"service": m.service_id, "from": m.migrates_from, "to": m.host_id}
                for m in self.migrations
            ],
            "unplaced": self.unplaced,
        }


def _clamp(v: float, lo: float = 0.0, hi: float = 100.0) -> float:
    return max(lo, min(hi, v))


def _fits(host: HostCapacity, svc: ServiceSpec) -> bool:
    """Hard constraints: GPU, tier role pin, and resource headroom."""
    if svc.needs_gpu and not host.gpu:
        return False
    pin = TIER_PIN.get(svc.tier, "")
    if pin and host.role != pin:
        return False
    if svc.ram_req_gb > host.ram_available_gb:
        return False
    if svc.cpu_req > host.cpu_cores:
        return False
    return True


def _efficiency(host: HostCapacity, svc: ServiceSpec) -> float:
    """Deployment-planner scoring, verbatim (the canonical efficiency objective)."""
    ram_headroom = (
        100.0 * (host.ram_available_gb - svc.ram_req_gb) / host.ram_total_gb
        if host.ram_total_gb
        else 0.0
    )
    cpu_headroom = (
        100.0 * (host.cpu_cores - svc.cpu_req) / host.cpu_cores
        if host.cpu_cores
        else 0.0
    )
    disk_headroom = 100.0 - host.disk_used_pct
    capacity = min(ram_headroom, cpu_headroom, disk_headroom)
    density_penalty = len(host.placed_services) / 20.0
    affinity_bonus = 15.0 * len(svc.depends_on & set(host.placed_services))
    storage_bonus = 20.0 if (svc.volumes & host.volumes) else 0.0
    network_bonus = 5.0 * len(svc.overlays & host.overlays)
    return _clamp(
        capacity - density_penalty + affinity_bonus + storage_bonus + network_bonus
    )


def _security(host: HostCapacity, svc: ServiceSpec) -> float:
    """Residency compliance, critical-tier concentration, CIS findings."""
    score = 100.0
    if svc.data_residency and host.region and svc.data_residency != host.region:
        score -= 80.0  # residency violation is severe
    score -= 10.0 * host.security_findings
    if svc.tier in _CRITICAL_TIERS:
        # Concentrating critical services on one host raises blast radius.
        crit_here = sum(1 for s in host.placed_services if s.startswith("crit:"))
        score -= 15.0 * crit_here
    return _clamp(score)


def _cost(host: HostCapacity, all_hosts: list[HostCapacity]) -> float:
    """Prefer powered-on, lower-power hosts (consolidation bias)."""
    max_w = max((h.power_watts for h in all_hosts), default=1.0) or 1.0
    power_pct = 100.0 * host.power_watts / max_w
    score = 100.0 - power_pct
    if not host.powered_on:
        score -= 40.0  # lighting up an idle host is expensive
    return _clamp(score)


def _resilience(host: HostCapacity, svc: ServiceSpec) -> float:
    """Reward anti-affinity: replicas of the same group on distinct hosts/chassis."""
    if not svc.group:
        return 50.0  # neutral when not a replicated service
    same_group_here = sum(
        1 for s in host.placed_services if s.startswith(f"grp:{svc.group}:")
    )
    return _clamp(100.0 - 50.0 * same_group_here)


def plan_placements(
    hosts: list[HostCapacity],
    services: list[ServiceSpec],
    *,
    weights: dict[str, float] | None = None,
) -> PlacementPlan:
    """Place services across hosts maximizing the weighted objective sum.

    Greedy per-service assignment over a shared (mutating) host state — services
    are placed in tier order (most critical first) so pins/anti-affinity resolve
    deterministically. Pure and deterministic given its inputs.
    """
    w = {**DEFAULT_OBJECTIVE_WEIGHTS, **(weights or {})}
    plan = PlacementPlan()
    # Critical tiers first so their pins/spread win the scarce capacity.
    ordered = sorted(services, key=lambda s: (s.tier, s.group, s.replica))

    for svc in ordered:
        eligible = [h for h in hosts if _fits(h, svc)]
        if not eligible:
            plan.unplaced.append(svc.service_id)
            continue
        best: tuple[float, HostCapacity, dict[str, float]] | None = None
        for host in eligible:
            scores = {
                "efficiency": _efficiency(host, svc),
                "security": _security(host, svc),
                "cost": _cost(host, hosts),
                "resilience": _resilience(host, svc),
            }
            final = sum(w[k] * scores[k] for k in scores)
            if best is None or final > best[0]:
                best = (final, host, scores)
        final, host, scores = best  # type: ignore[misc]
        placement = ServicePlacement(
            service_id=svc.service_id,
            host_id=host.host_id,
            tier=svc.tier,
            final_score=round(final, 4),
            scores={k: round(v, 2) for k, v in scores.items()},
            reason=(
                f"{svc.tier} → {host.host_id} (role={host.role or 'any'}); "
                f"eff={scores['efficiency']:.0f} sec={scores['security']:.0f} "
                f"cost={scores['cost']:.0f} res={scores['resilience']:.0f}"
            ),
            migrates_from=svc.current_host if svc.current_host != host.host_id else "",
        )
        plan.placements.append(placement)
        if placement.migrates_from:
            plan.migrations.append(placement)
        # Commit to host state (markers drive density/anti-affinity/concentration).
        marker = svc.service_id
        host.placed_services.append(marker)
        if svc.group:
            host.placed_services.append(f"grp:{svc.group}:{svc.replica}")
        if svc.tier in _CRITICAL_TIERS:
            host.placed_services.append(f"crit:{svc.service_id}")
        host.ram_available_gb = max(0.0, host.ram_available_gb - svc.ram_req_gb)
    return plan


# ── Live-graph adapters ──────────────────────────────────────────────────────
_HOST_TYPES = {"hardwarenode", "bladeserver", "host"}
_SERVICE_TYPES = {"container", "container_stack", "platform_service", "service"}


def _to_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def load_infra(engine: Any) -> tuple[list[HostCapacity], list[ServiceSpec]]:
    """Read hosts + services from the KG infra subgraph into planner dataclasses."""
    graph = getattr(engine, "graph", None)
    hosts: list[HostCapacity] = []
    services: list[ServiceSpec] = []
    if graph is None:
        return hosts, services
    try:
        node_iter = list(graph.nodes(data=True))
    except TypeError:  # pragma: no cover
        return hosts, services
    for nid, d in node_iter:
        if not isinstance(d, dict):
            continue
        ntype = str(d.get("type", "") or "").lower()
        if ntype in _HOST_TYPES:
            hosts.append(
                HostCapacity(
                    host_id=nid,
                    cpu_cores=_to_float(d.get("cpu_cores"), 1.0),
                    ram_total_gb=_to_float(d.get("ram_total_gb"), 1.0),
                    ram_available_gb=_to_float(
                        d.get("ram_available_gb", d.get("ram_total_gb")), 1.0
                    ),
                    disk_total_gb=_to_float(d.get("disk_total_gb"), 1.0),
                    disk_used_pct=_to_float(d.get("disk_used_pct"), 0.0),
                    gpu=bool(d.get("gpu") or d.get("gpu_count")),
                    role=str(d.get("swarm_role", d.get("role", "")) or ""),
                    region=str(d.get("region", "") or ""),
                    chassis=str(d.get("chassis", "") or ""),
                    powered_on=bool(d.get("powered_on", True)),
                    power_watts=_to_float(d.get("power_watts"), 100.0),
                    security_findings=int(_to_float(d.get("security_findings"), 0)),
                )
            )
        elif ntype in _SERVICE_TYPES:
            services.append(
                ServiceSpec(
                    service_id=nid,
                    tier=str(d.get("tier", "T3") or "T3"),
                    cpu_req=_to_float(d.get("cpu_req"), 0.5),
                    ram_req_gb=_to_float(d.get("ram_req_gb"), 0.5),
                    disk_req_gb=_to_float(d.get("disk_req_gb"), 1.0),
                    needs_gpu=bool(d.get("needs_gpu")),
                    group=str(d.get("group", "") or ""),
                    replica=int(_to_float(d.get("replica"), 0)),
                    data_residency=str(d.get("data_residency", "") or ""),
                    current_host=str(d.get("current_host", "") or ""),
                )
            )
    return hosts, services


def optimize_from_graph(
    engine: Any, *, weights: dict[str, float] | None = None, write: bool = True
) -> dict[str, Any]:
    """Read infra from the KG, plan placements, and write the plan back (propose-only).

    Returns a JSON-able report with the blueprint and per-service placements.
    """
    hosts, services = load_infra(engine)
    plan = plan_placements(hosts, services, weights=weights)
    if write:
        _persist_plan(engine, plan)
    return {
        "hosts": len(hosts),
        "services": len(services),
        "placed": len(plan.placements),
        "unplaced": plan.unplaced,
        "migrations": len(plan.migrations),
        "blueprint": plan.blueprint(),
        "placements": [
            {
                "service": p.service_id,
                "host": p.host_id,
                "tier": p.tier,
                "final_score": p.final_score,
                "scores": p.scores,
                "reason": p.reason,
            }
            for p in plan.placements
        ],
    }


def _persist_plan(engine: Any, plan: PlacementPlan) -> None:
    """Persist DeploymentPlan + ServicePlacement + MigrationTask nodes/edges."""
    add = getattr(engine, "add_node", None)
    link = getattr(engine, "link_nodes", None)
    if not callable(add):
        return
    import hashlib

    plan_id = (
        "deployment_plan:"
        + hashlib.sha256(
            "|".join(
                sorted(p.service_id + ">" + p.host_id for p in plan.placements)
            ).encode()
        ).hexdigest()[:12]
    )
    try:
        add(
            plan_id,
            "deployment_plan",
            properties={
                "status": "proposal",
                "placed": len(plan.placements),
                "migrations": len(plan.migrations),
                "blueprint": plan.blueprint(),
                "concept": "AU-KG.ingest.enterprise-source-extractor",
            },
        )
    except Exception:  # noqa: BLE001
        return
    for p in plan.placements:
        pid = f"service_placement:{p.service_id}"
        try:
            add(
                pid,
                "service_placement",
                properties={
                    "service": p.service_id,
                    "host": p.host_id,
                    "tier": p.tier,
                    "final_score": p.final_score,
                    "scores": p.scores,
                    "reason": p.reason,
                    "status": "proposal",
                    "concept": "AU-KG.ingest.enterprise-source-extractor",
                },
            )
            if callable(link):
                link(pid, p.host_id, "PLACED_ON", properties={"_rel": "PLACED_ON"})
                link(pid, plan_id, "PART_OF", properties={"_rel": "PART_OF"})
        except Exception:  # noqa: BLE001  # nosec B112
            continue
    for m in plan.migrations:
        mid = f"migration_task:{m.service_id}"
        try:
            add(
                mid,
                "migration_task",
                properties={
                    "service": m.service_id,
                    "from": m.migrates_from,
                    "to": m.host_id,
                    "status": "proposal",
                    "concept": "AU-KG.ingest.enterprise-source-extractor",
                },
            )
        except Exception:  # noqa: BLE001  # nosec B112
            continue


__all__ = [
    "HostCapacity",
    "ServiceSpec",
    "ServicePlacement",
    "PlacementPlan",
    "DEFAULT_OBJECTIVE_WEIGHTS",
    "TIER_PIN",
    "plan_placements",
    "load_infra",
    "optimize_from_graph",
]
