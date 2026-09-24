//! Swarm-topology fixtures shared by eg-types' own tests and eg-compute's
//! topology model tests.

use crate::capacity_lease::{CapacityResourceClass, LeasePriority};
use crate::contract::BoundedVec;
use crate::decision::topology::{
    CapacityScope, LeaseDemand, SlotRole, SlotTopology, StopRule, TopologyCaps, TopologyFacts,
    TopologyRequirements,
};

/// The fan-out/join class IRI the fixtures use.
pub const FAN_OUT_JOIN: &str = "http://knuckles.team/kg/swarm#FanOutJoin";
/// The independent-subtasks task class IRI the fixtures use.
pub const INDEPENDENT_SUBTASKS: &str = "http://knuckles.team/kg/swarm#IndependentSubtasks";

/// One slot on template node `node_id` with `role`, widths `min..=max`, one
/// round, a declared p95 and one generator slot per agent.
pub fn slot(node_id: &str, role: SlotRole, widths: (u8, u8), p95_ms: Option<u32>) -> SlotTopology {
    SlotTopology {
        node_id: node_id.to_string(),
        role,
        min_width: widths.0,
        max_width: widths.1,
        max_rounds: 1,
        p95_ms,
        tokens: Some(1_000),
        lease: BoundedVec::new(vec![LeaseDemand {
            class: CapacityResourceClass::LlmGenerator,
            amount: 1,
        }])
        .expect("one lease class fits"),
        harness: None,
    }
}

/// A fan-out/join topology over a one-wide lead and a one-to-four-wide worker.
pub fn facts() -> TopologyFacts {
    TopologyFacts {
        class_iri: FAN_OUT_JOIN.to_string(),
        depth: 1,
        slots: BoundedVec::new(vec![
            slot("lead", SlotRole::Parent, (1, 1), Some(100)),
            slot("worker", SlotRole::Child, (1, 4), Some(200)),
        ])
        .expect("two slots fit"),
        stop: StopRule::MaxRounds { n: 1 },
    }
}

/// A topology request over `subtasks` independent subtasks, one per agent,
/// against cell `cell-llm` at orchestration priority.
pub fn requirements(subtasks: u32) -> TopologyRequirements {
    TopologyRequirements {
        task_classes: BoundedVec::new(vec![INDEPENDENT_SUBTASKS.to_string()])
            .expect("one task class fits"),
        subtasks,
        per_agent_subtasks: 1,
        deadline_ms: None,
        caps: TopologyCaps {
            max_width: 8,
            max_depth: 2,
            max_rounds: 4,
            max_tokens: None,
        },
        capacity: CapacityScope {
            cells: BoundedVec::new(vec!["cell-llm".to_string()]).expect("one cell fits"),
            priority: LeasePriority::Orchestration,
        },
    }
}
