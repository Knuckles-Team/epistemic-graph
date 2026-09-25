use eg_core::graph::GraphCore;

pub fn add_agent_n4(core: &GraphCore) {
    core.add_node(
        "n4".to_string(),
        rmp_serde::to_vec_named(&serde_json::json!({"type": "Agent", "rank": 4})).unwrap(),
    );
    core.mark_dirty();
}
