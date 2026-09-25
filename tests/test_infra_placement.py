"""EH-503: infrastructure placement authority migrated from Agent Utilities."""

from epistemic_graph.infra_placement import (
    HostCapacity,
    ServiceSpec,
    optimize_from_graph,
    plan_placements,
)

def _host(hid, cores=16, ram=64, role="", region="", power=200, findings=0, gpu=False):
    return HostCapacity(
        host_id=hid,
        cpu_cores=cores,
        ram_total_gb=ram,
        ram_available_gb=ram,
        disk_total_gb=500,
        role=role,
        region=region,
        power_watts=power,
        security_findings=findings,
        gpu=gpu,
    )


# ── tier pinning ─────────────────────────────────────────────────────────────
def test_t0_pins_to_gateway():
    hosts = [_host("gw", role="gateway"), _host("worker", role="worker")]
    svc = ServiceSpec(service_id="caddy", tier="T0", ram_req_gb=1)
    plan = plan_placements(hosts, [svc])
    assert plan.placements[0].host_id == "gw"


def test_gpu_workload_requires_gpu_host():
    hosts = [_host("cpu", role="worker"), _host("ai", role="ai", gpu=True)]
    svc = ServiceSpec(service_id="vllm", tier="T4", needs_gpu=True, ram_req_gb=1)
    plan = plan_placements(hosts, [svc])
    assert plan.placements[0].host_id == "ai"


def test_unplaceable_when_no_capacity():
    hosts = [_host("small", cores=1, ram=1)]
    svc = ServiceSpec(service_id="big", ram_req_gb=999)
    plan = plan_placements(hosts, [svc])
    assert plan.unplaced == ["big"]


# ── objective behavior ───────────────────────────────────────────────────────
def test_efficiency_only_prefers_more_headroom():
    # Two eligible workers; efficiency-only weights pick the emptier/bigger host.
    hosts = [_host("big", cores=64, ram=256), _host("small", cores=8, ram=16)]
    svc = ServiceSpec(service_id="svc", ram_req_gb=2, cpu_req=1)
    plan = plan_placements(
        hosts,
        [svc],
        weights={"efficiency": 1.0, "security": 0.0, "cost": 0.0, "resilience": 0.0},
    )
    assert plan.placements[0].host_id == "big"


def test_security_weight_moves_residency_violator():
    hosts = [
        _host("us", role="worker", region="us"),
        _host("eu", role="worker", region="eu"),
    ]
    svc = ServiceSpec(service_id="pii", ram_req_gb=1, data_residency="eu")
    # With security weighted, the eu host wins despite equal efficiency.
    plan = plan_placements(
        hosts,
        [svc],
        weights={"efficiency": 0.2, "security": 1.0, "cost": 0.0, "resilience": 0.0},
    )
    assert plan.placements[0].host_id == "eu"


def test_resilience_spreads_replicas():
    hosts = [_host("h1", region="a"), _host("h2", region="b")]
    svcs = [
        ServiceSpec(service_id="api-0", group="api", replica=0, ram_req_gb=1),
        ServiceSpec(service_id="api-1", group="api", replica=1, ram_req_gb=1),
    ]
    plan = plan_placements(
        hosts,
        svcs,
        weights={"efficiency": 0.1, "security": 0.0, "cost": 0.0, "resilience": 1.0},
    )
    placed_hosts = {p.host_id for p in plan.placements}
    assert len(placed_hosts) == 2  # replicas spread across distinct hosts


def test_cost_prefers_powered_on_low_power():
    hosts = [
        _host("idle", power=400),
        _host("warm", power=100),
    ]
    hosts[0].powered_on = False
    svc = ServiceSpec(service_id="svc", ram_req_gb=1)
    plan = plan_placements(
        hosts,
        [svc],
        weights={"efficiency": 0.1, "security": 0.0, "cost": 1.0, "resilience": 0.0},
    )
    assert plan.placements[0].host_id == "warm"



class _Graph:
    def __init__(self, nodes):
        self._n = nodes

    def nodes(self, data=False):
        return list(self._n.items()) if data else list(self._n)

    def edges(self, data=False):
        return []


class _Engine:
    def __init__(self, nodes):
        self._nodes = dict(nodes)
        self.graph = _Graph(self._nodes)
        self.backend = None
        self.links = []

    def add_node(self, nid, ntype, properties=None):
        d = dict(properties or {})
        d["type"] = ntype
        self._nodes[nid] = d

    def link_nodes(self, s, d, rel, properties=None):
        self.links.append((s, d, str(rel)))


def test_optimize_from_graph_writes_plan():
    engine = _Engine(
        {
            "gw": {
                "type": "hardwarenode",
                "cpu_cores": 16,
                "ram_total_gb": 64,
                "ram_available_gb": 64,
                "swarm_role": "gateway",
            },
            "w1": {
                "type": "hardwarenode",
                "cpu_cores": 32,
                "ram_total_gb": 128,
                "ram_available_gb": 128,
                "swarm_role": "worker",
            },
            "caddy": {"type": "platform_service", "tier": "T0", "ram_req_gb": 1},
            "app": {"type": "container", "tier": "T3", "ram_req_gb": 2},
        }
    )
    rep = optimize_from_graph(engine)
    assert rep["hosts"] == 2 and rep["services"] == 2
    assert rep["placed"] == 2
    # caddy (T0) pinned to the gateway host.
    caddy = next(p for p in rep["placements"] if p["service"] == "caddy")
    assert caddy["host"] == "gw"
    # DeploymentPlan + ServicePlacement nodes persisted.
    assert any(d.get("type") == "deployment_plan" for d in engine._nodes.values())
    assert any(d.get("type") == "service_placement" for d in engine._nodes.values())
