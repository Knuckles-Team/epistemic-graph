//! The fixed deployment targets offered to placement decisions.
//!
//! This registry defines candidate identity only. Selection and feasibility
//! belong to the placement decision; no target-specific planner lives here.

use serde::{Deserialize, Serialize};

use crate::capacity_lease::LeasePriority;

/// Supported deployment targets. Systemd is not a placement target.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum PlacementTarget {
    InProcess,
    Compose,
    Helm,
    Swarm,
}

impl PlacementTarget {
    /// Every registered target, in a stable order.
    pub const ALL: [PlacementTarget; 4] = [Self::InProcess, Self::Compose, Self::Helm, Self::Swarm];

    /// The declared-option id a decision candidate for this target carries.
    pub fn option_id(self) -> &'static str {
        match self {
            Self::InProcess => "placement:in_process",
            Self::Compose => "placement:compose",
            Self::Helm => "placement:helm",
            Self::Swarm => "placement:swarm",
        }
    }

    /// The target named by a declared-option id, or `None` outside the registry.
    pub fn from_option_id(option_id: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|target| target.option_id() == option_id)
    }
}

/// Resource demand for one replica of the workload to place.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PlacementSubject {
    pub workload_iri: String,
    pub cpu_millis: u64,
    pub memory_bytes: u64,
    pub replicas: u32,
    pub needs_persistent_volume: bool,
}

/// Request-side constraints; feasibility and policy tightening are evaluated separately.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PlacementConstraints {
    pub requires_ha: bool,
    pub forbidden_targets: Vec<PlacementTarget>,
    pub max_relative_cost: Option<u64>,
    pub capacity_cells: Vec<String>,
    pub priority: LeasePriority,
}

/// Typed, evaluate-only placement input. Checking it never acquires capacity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PlacementRequestBody {
    pub subject: PlacementSubject,
    pub constraints: PlacementConstraints,
    /// Declared-option ids, validated against the closed target registry.
    pub candidate_targets: Vec<String>,
}

impl PlacementRequestBody {
    /// Refuse malformed candidates and non-positive replica/resource demand.
    pub fn check(&self) -> Result<(), String> {
        if self.candidate_targets.is_empty() {
            return Err("EMPTY_PLACEMENT_CANDIDATES: at least one target is required".into());
        }
        let mut seen = std::collections::BTreeSet::new();
        for option_id in &self.candidate_targets {
            if PlacementTarget::from_option_id(option_id).is_none() {
                return Err(format!("UNSUPPORTED_PLACEMENT_TARGET: '{option_id}'"));
            }
            if !seen.insert(option_id) {
                return Err(format!("DUPLICATE_PLACEMENT_TARGET: '{option_id}'"));
            }
        }
        if self.subject.replicas == 0 {
            return Err("ZERO_PLACEMENT_REPLICAS: at least one replica is required".into());
        }
        if self.subject.cpu_millis == 0 || self.subject.memory_bytes == 0 {
            return Err("ZERO_PLACEMENT_DEMAND: CPU and memory demand must be positive".into());
        }
        Ok(())
    }
}

/// The declared facts of one target (`placement:Target`, EG-PLACEMENT-R001.5).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct TargetFacts {
    pub supports_ha: bool,
    pub supports_volumes: bool,
    pub max_replicas: u32,
    pub relative_cost: u64,
}

/// Operator policy caps a request may only tighten.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct PlacementPolicy {
    pub max_relative_cost: Option<u64>,
    pub forbidden_targets: Vec<PlacementTarget>,
}

/// Target facts plus the policy in force.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PlacementFacts {
    pub targets: std::collections::BTreeMap<PlacementTarget, TargetFacts>,
    pub policy: PlacementPolicy,
}

/// Free capacity per target at the request priority (read, never leased).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PlacementHeadroom {
    pub by_target: std::collections::BTreeMap<PlacementTarget, (u64, u64)>,
}

/// Why a target was eliminated; the stable string is the why-not reason.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum PlacementReason {
    NoFacts,
    NoHa,
    NoVolumes,
    MaxReplicas { max: u32, requested: u32 },
    Forbidden,
    OverCostCap { cost: u64, cap: u64 },
    NoHeadroom,
}

impl PlacementReason {
    /// The stable why-not string.
    pub fn as_why_not(&self) -> String {
        match self {
            Self::NoFacts => "placement:facts=absent".to_string(),
            Self::NoHa => "placement:supportsHa=false".to_string(),
            Self::NoVolumes => "placement:supportsVolumes=false".to_string(),
            Self::MaxReplicas { max, requested } => {
                format!("placement:maxReplicas={max} < {requested}")
            }
            Self::Forbidden => "placement:forbidden".to_string(),
            Self::OverCostCap { cost, cap } => format!("placement:relativeCost={cost} > {cap}"),
            Self::NoHeadroom => "capacity:headroom<demand".to_string(),
        }
    }
}

/// Feasible targets with their cost (cheapest first, ties by option id) and
/// eliminated targets with their reason.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PlacementVerdict {
    pub feasible: Vec<(PlacementTarget, u64)>,
    pub eliminated: Vec<(PlacementTarget, PlacementReason)>,
}

/// The stable code of a request that loosens a policy cap.
pub const POLICY_LOOSENING: &str = "POLICY_LOOSENING";

fn eliminate(
    subject: &PlacementSubject,
    constraints: &PlacementConstraints,
    facts: &TargetFacts,
    cap: Option<u64>,
    headroom: Option<(u64, u64)>,
) -> Option<PlacementReason> {
    let replicas = u64::from(subject.replicas);
    if constraints.requires_ha && !facts.supports_ha {
        return Some(PlacementReason::NoHa);
    }
    if subject.needs_persistent_volume && !facts.supports_volumes {
        return Some(PlacementReason::NoVolumes);
    }
    if subject.replicas > facts.max_replicas {
        return Some(PlacementReason::MaxReplicas {
            max: facts.max_replicas,
            requested: subject.replicas,
        });
    }
    if let Some(cap) = cap.filter(|cap| facts.relative_cost > *cap) {
        return Some(PlacementReason::OverCostCap {
            cost: facts.relative_cost,
            cap,
        });
    }
    let (cpu, memory) = headroom.unwrap_or((0, 0));
    let fits = subject.cpu_millis.saturating_mul(replicas) <= cpu
        && subject.memory_bytes.saturating_mul(replicas) <= memory;
    (!fits).then_some(PlacementReason::NoHeadroom)
}

/// The tighter of the policy cap and the request cap; a request cap above the
/// policy cap is refused (`POLICY_LOOSENING`).
fn effective_cost_cap(request: Option<u64>, policy: Option<u64>) -> Result<Option<u64>, String> {
    match (request, policy) {
        (Some(asked), Some(cap)) if asked > cap => Err(format!(
            "{POLICY_LOOSENING}: max_relative_cost {asked} exceeds the policy cap {cap}"
        )),
        (Some(asked), _) => Ok(Some(asked)),
        (None, cap) => Ok(cap),
    }
}

/// Pure feasibility (EG-PLACEMENT-R001.6): apply the hard constraints (HA,
/// volumes, max replicas, forbidden targets, cost cap, capacity headroom) to
/// every candidate. Nothing is acquired.
pub fn place(
    body: &PlacementRequestBody,
    facts: &PlacementFacts,
    headroom: &PlacementHeadroom,
) -> Result<PlacementVerdict, String> {
    body.check()?;
    let cap = effective_cost_cap(
        body.constraints.max_relative_cost,
        facts.policy.max_relative_cost,
    )?;
    let mut verdict = PlacementVerdict::default();
    for option_id in &body.candidate_targets {
        let target = PlacementTarget::from_option_id(option_id).expect("checked");
        let forbidden = body.constraints.forbidden_targets.contains(&target)
            || facts.policy.forbidden_targets.contains(&target);
        let reason = match facts.targets.get(&target) {
            _ if forbidden => Some(PlacementReason::Forbidden),
            None => Some(PlacementReason::NoFacts),
            Some(target_facts) => eliminate(
                &body.subject,
                &body.constraints,
                target_facts,
                cap,
                headroom.by_target.get(&target).copied(),
            ),
        };
        match reason {
            Some(reason) => verdict.eliminated.push((target, reason)),
            None => verdict
                .feasible
                .push((target, facts.targets[&target].relative_cost)),
        }
    }
    verdict
        .feasible
        .sort_by(|(a, x), (b, y)| x.cmp(y).then_with(|| a.option_id().cmp(b.option_id())));
    Ok(verdict)
}

/// The closed capability set a host may declare (EG-PLACEMENT-R004.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub enum HeavyCapability {
    Gpu,
    QuantumSimulation,
    LargeMemory,
    Backtest,
    VideoTranscode,
}

/// One host's declared capability and its current headroom.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct HostCapability {
    pub host_iri: String,
    pub capability: HeavyCapability,
    pub headroom: u64,
}

/// The capability a heavy job class requires.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "contract-schema", derive(schemars::JsonSchema))]
pub struct RoutingFact {
    pub job_class: String,
    pub required_capability: HeavyCapability,
}

/// The default host order: most headroom first, ties by host IRI. A named,
/// replaceable default (EG-PLACEMENT-R004.2).
pub fn most_headroom_first(a: &HostCapability, b: &HostCapability) -> std::cmp::Ordering {
    b.headroom
        .cmp(&a.headroom)
        .then_with(|| a.host_iri.cmp(&b.host_iri))
}

/// The hosts that declare the capability `job` requires, in
/// [`most_headroom_first`] order. Hosts without it are absent.
pub fn route_heavy_job<'a>(
    job: &RoutingFact,
    hosts: &'a [HostCapability],
) -> Vec<&'a HostCapability> {
    let mut capable: Vec<_> = hosts
        .iter()
        .filter(|host| host.capability == job.required_capability)
        .collect();
    capable.sort_by(|a, b| most_headroom_first(a, b));
    capable
}

#[cfg(test)]
mod tests {
    use super::*;

    // spec: EG-PLACEMENT-R001.3
    #[test]
    fn every_target_round_trips_through_its_stable_option_id() {
        let expected = [
            (PlacementTarget::InProcess, "placement:in_process"),
            (PlacementTarget::Compose, "placement:compose"),
            (PlacementTarget::Helm, "placement:helm"),
            (PlacementTarget::Swarm, "placement:swarm"),
        ];
        assert_eq!(PlacementTarget::ALL, expected.map(|(target, _)| target));
        for (target, option_id) in expected {
            assert_eq!(target.option_id(), option_id);
            assert_eq!(PlacementTarget::from_option_id(option_id), Some(target));
        }
    }

    // spec: EG-PLACEMENT-R001.3
    #[test]
    fn systemd_and_other_unregistered_option_ids_are_refused() {
        for option_id in [
            "placement:systemd",
            "placement:unknown",
            "placement:InProcess",
            "placement:in-process",
            "compose",
            "placement:compose ",
            "",
        ] {
            assert_eq!(PlacementTarget::from_option_id(option_id), None);
        }
    }

    // spec: EG-PLACEMENT-R001.3
    #[test]
    fn target_wire_names_round_trip_and_systemd_is_refused() {
        for (target, name) in
            PlacementTarget::ALL
                .into_iter()
                .zip(["in_process", "compose", "helm", "swarm"])
        {
            let json = format!("\"{name}\"");
            assert_eq!(serde_json::to_string(&target).unwrap(), json);
            assert_eq!(
                serde_json::from_str::<PlacementTarget>(&json).unwrap(),
                target
            );
        }
        assert!(serde_json::from_str::<PlacementTarget>("\"systemd\"").is_err());
    }

    fn accepted_body() -> PlacementRequestBody {
        PlacementRequestBody {
            subject: PlacementSubject {
                workload_iri: "eg:graphos-core".into(),
                cpu_millis: 2000,
                memory_bytes: 4 * 1024 * 1024 * 1024,
                replicas: 2,
                needs_persistent_volume: false,
            },
            constraints: PlacementConstraints {
                requires_ha: true,
                forbidden_targets: vec![PlacementTarget::InProcess],
                max_relative_cost: Some(3),
                capacity_cells: vec!["cell-a".into()],
                priority: LeasePriority::Orchestration,
            },
            candidate_targets: PlacementTarget::ALL
                .into_iter()
                .map(|target| target.option_id().to_owned())
                .collect(),
        }
    }

    // spec: EG-PLACEMENT-R001.4
    #[test]
    fn a_valid_placement_body_is_accepted_and_round_trips() {
        let body = accepted_body();
        assert_eq!(body.check(), Ok(()));
        let json = serde_json::to_string(&body).unwrap();
        let decoded: PlacementRequestBody = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, body);
        assert_eq!(decoded.check(), Ok(()));
    }

    // spec: EG-PLACEMENT-R001.4
    #[test]
    fn empty_candidates_are_refused() {
        let mut body = accepted_body();
        body.candidate_targets.clear();
        assert!(body
            .check()
            .unwrap_err()
            .starts_with("EMPTY_PLACEMENT_CANDIDATES:"));
    }

    // spec: EG-PLACEMENT-R001.4
    #[test]
    fn duplicate_candidates_are_refused() {
        let mut body = accepted_body();
        body.candidate_targets
            .push(body.candidate_targets[0].clone());
        assert_eq!(
            body.check().unwrap_err(),
            "DUPLICATE_PLACEMENT_TARGET: 'placement:in_process'"
        );
    }

    // spec: EG-PLACEMENT-R001.4
    #[test]
    fn unknown_candidate_option_ids_are_refused() {
        for option_id in ["placement:systemd", "placement:unknown", "compose", ""] {
            let mut body = accepted_body();
            body.candidate_targets.push(option_id.into());
            assert_eq!(
                body.check().unwrap_err(),
                format!("UNSUPPORTED_PLACEMENT_TARGET: '{option_id}'")
            );
        }
    }

    // spec: EG-PLACEMENT-R001.4
    #[test]
    fn zero_replicas_are_refused() {
        let mut body = accepted_body();
        body.subject.replicas = 0;
        assert!(body
            .check()
            .unwrap_err()
            .starts_with("ZERO_PLACEMENT_REPLICAS:"));
    }

    // spec: EG-PLACEMENT-R001.4
    #[test]
    fn zero_cpu_demand_is_refused() {
        let mut body = accepted_body();
        body.subject.cpu_millis = 0;
        assert!(body
            .check()
            .unwrap_err()
            .starts_with("ZERO_PLACEMENT_DEMAND:"));
    }

    // spec: EG-PLACEMENT-R001.4
    #[test]
    fn zero_memory_demand_is_refused() {
        let mut body = accepted_body();
        body.subject.memory_bytes = 0;
        assert!(body
            .check()
            .unwrap_err()
            .starts_with("ZERO_PLACEMENT_DEMAND:"));
    }

    // spec: EG-PLACEMENT-R001.4
    #[test]
    fn unknown_body_subject_and_constraint_fields_are_refused() {
        for path in ["", "subject", "constraints"] {
            let mut json = serde_json::to_value(accepted_body()).unwrap();
            let object = if path.is_empty() {
                &mut json
            } else {
                &mut json[path]
            };
            object["unexpected"] = serde_json::json!(true);
            assert!(serde_json::from_value::<PlacementRequestBody>(json).is_err());
        }
    }

    fn target_facts() -> PlacementFacts {
        let facts = |ha, volumes, max, cost| TargetFacts {
            supports_ha: ha,
            supports_volumes: volumes,
            max_replicas: max,
            relative_cost: cost,
        };
        PlacementFacts {
            targets: [
                (PlacementTarget::InProcess, facts(false, false, 1, 1)),
                (PlacementTarget::Compose, facts(false, true, 4, 2)),
                (PlacementTarget::Swarm, facts(true, true, 16, 4)),
                (PlacementTarget::Helm, facts(true, true, 64, 8)),
            ]
            .into(),
            policy: PlacementPolicy::default(),
        }
    }

    fn roomy() -> PlacementHeadroom {
        PlacementHeadroom {
            by_target: PlacementTarget::ALL
                .into_iter()
                .map(|t| (t, (u64::MAX, u64::MAX)))
                .collect(),
        }
    }

    fn all_candidates() -> PlacementRequestBody {
        let mut body = accepted_body();
        body.subject.replicas = 1;
        body.subject.needs_persistent_volume = false;
        body.constraints.requires_ha = false;
        body.constraints.forbidden_targets = vec![];
        body.constraints.max_relative_cost = None;
        body.candidate_targets = PlacementTarget::ALL
            .iter()
            .map(|t| t.option_id().to_string())
            .collect();
        body
    }

    fn eliminated_by(verdict: &PlacementVerdict) -> Vec<(PlacementTarget, String)> {
        verdict
            .eliminated
            .iter()
            .map(|(t, r)| (*t, r.as_why_not()))
            .collect()
    }

    // spec: EG-PLACEMENT-R001.2
    #[test]
    fn the_placement_question_kind_round_trips() {
        use crate::decision::statistical::QuestionKind;
        let json = serde_json::to_string(&QuestionKind::Placement).unwrap();
        assert_eq!(json, "\"placement\"");
        assert_eq!(
            serde_json::from_str::<QuestionKind>(&json).unwrap(),
            QuestionKind::Placement
        );
        #[cfg(feature = "contract-schema")]
        {
            let schema = serde_json::to_string(&schemars::schema_for!(QuestionKind)).unwrap();
            assert!(schema.contains("\"placement\""));
        }
    }

    // spec: EG-PLACEMENT-R001.6
    #[test]
    fn each_hard_constraint_eliminates_exactly_its_targets() {
        let facts = target_facts();
        let open = place(&all_candidates(), &facts, &roomy()).unwrap();
        assert!(open.eliminated.is_empty());
        assert_eq!(open.feasible[0], (PlacementTarget::InProcess, 1));

        let mut ha = all_candidates();
        ha.constraints.requires_ha = true;
        let verdict = place(&ha, &facts, &roomy()).unwrap();
        assert_eq!(
            eliminated_by(&verdict),
            [
                (
                    PlacementTarget::InProcess,
                    "placement:supportsHa=false".to_string()
                ),
                (
                    PlacementTarget::Compose,
                    "placement:supportsHa=false".to_string()
                ),
            ]
        );

        let mut volume = all_candidates();
        volume.subject.needs_persistent_volume = true;
        let verdict = place(&volume, &facts, &roomy()).unwrap();
        assert_eq!(
            verdict.eliminated,
            [(PlacementTarget::InProcess, PlacementReason::NoVolumes)]
        );

        let mut replicas = all_candidates();
        replicas.subject.replicas = 5;
        let verdict = place(&replicas, &facts, &roomy()).unwrap();
        assert_eq!(verdict.eliminated.len(), 2);
        assert!(verdict
            .eliminated
            .iter()
            .all(|(_, r)| matches!(r, PlacementReason::MaxReplicas { .. })));

        let mut forbidden = all_candidates();
        forbidden.constraints.forbidden_targets = vec![PlacementTarget::Helm];
        let verdict = place(&forbidden, &facts, &roomy()).unwrap();
        assert_eq!(
            verdict.eliminated,
            [(PlacementTarget::Helm, PlacementReason::Forbidden)]
        );

        let mut cost = all_candidates();
        cost.constraints.max_relative_cost = Some(4);
        let verdict = place(&cost, &facts, &roomy()).unwrap();
        assert_eq!(
            verdict.eliminated,
            [(
                PlacementTarget::Helm,
                PlacementReason::OverCostCap { cost: 8, cap: 4 }
            )]
        );

        let mut tight = roomy();
        tight.by_target.insert(PlacementTarget::Swarm, (1, 1));
        let verdict = place(&all_candidates(), &facts, &tight).unwrap();
        assert_eq!(
            verdict.eliminated,
            [(PlacementTarget::Swarm, PlacementReason::NoHeadroom)]
        );
    }

    // spec: EG-PLACEMENT-R001.6
    #[test]
    fn a_request_can_only_tighten_policy_caps() {
        let mut facts = target_facts();
        facts.policy.max_relative_cost = Some(4);
        let mut loosen = all_candidates();
        loosen.constraints.max_relative_cost = Some(8);
        let refused = place(&loosen, &facts, &roomy()).unwrap_err();
        assert!(refused.starts_with(POLICY_LOOSENING));
        let mut tighten = all_candidates();
        tighten.constraints.max_relative_cost = Some(2);
        let verdict = place(&tighten, &facts, &roomy()).unwrap();
        assert_eq!(verdict.feasible.len(), 2);
        // Without a request cap the policy cap still applies.
        let verdict = place(&all_candidates(), &facts, &roomy()).unwrap();
        assert_eq!(verdict.eliminated.len(), 1);
    }

    // spec: EG-PLACEMENT-R004.1
    #[test]
    fn capability_types_round_trip() {
        let host = HostCapability {
            host_iri: "eg:host/gpu-1".into(),
            capability: HeavyCapability::Gpu,
            headroom: 7,
        };
        let fact = RoutingFact {
            job_class: "backtest".into(),
            required_capability: HeavyCapability::Backtest,
        };
        assert_eq!(
            serde_json::from_value::<HostCapability>(serde_json::to_value(&host).unwrap()).unwrap(),
            host
        );
        assert_eq!(
            serde_json::from_value::<RoutingFact>(serde_json::to_value(&fact).unwrap()).unwrap(),
            fact
        );
        let undeclared =
            serde_json::json!({"host_iri": "h", "capability": "teleport", "headroom": 1});
        assert!(serde_json::from_value::<HostCapability>(undeclared).is_err());
    }

    // spec: EG-PLACEMENT-R004.2
    #[test]
    fn route_orders_by_headroom() {
        let host = |iri: &str, capability, headroom| HostCapability {
            host_iri: iri.into(),
            capability,
            headroom,
        };
        let hosts = [
            host("eg:host/a", HeavyCapability::Gpu, 2),
            host("eg:host/b", HeavyCapability::Gpu, 9),
            host("eg:host/c", HeavyCapability::LargeMemory, 50),
        ];
        let job = RoutingFact {
            job_class: "training".into(),
            required_capability: HeavyCapability::Gpu,
        };
        let routed: Vec<_> = route_heavy_job(&job, &hosts)
            .into_iter()
            .map(|h| h.host_iri.as_str())
            .collect();
        assert_eq!(routed, ["eg:host/b", "eg:host/a"]);
    }
}
