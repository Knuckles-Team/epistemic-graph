//! Typed **KG projection** of a statechart definition (CONCEPT:INT-P2-2,
//! requirement 1).
//!
//! Requirement 1 asks that a `StatechartDef` be representable ALSO as typed KG
//! nodes+edges (`StatechartDef`/`State`/`Transition`/`Guard`/`Action`) — while the
//! in-memory typed form stays the source of truth. This module is that projection: a
//! pure function from a definition to a set of neutral node/edge DESCRIPTORS. It emits
//! DATA, not graph writes, so it stays a clean leaf (no `eg-core` dependency) and is
//! fully unit-testable; the dispatch handler (which already holds a live `GraphCore`)
//! or any KG ingester can walk [`KgProjection`] and issue the corresponding
//! `add_node`/`add_edge` calls — exactly the "wire-first, project into the KG" shape
//! the rest of the ecosystem uses.
//!
//! Shape (reified transitions/guards/actions so every element is addressable):
//!
//! ```text
//! (:StatechartDef {id,name,initial,finals})
//!    -[:hasState]->        (:State {id, kind: atomic|initial|final, entry_actions, exit_actions})
//!    -[:hasTransition]->   (:Transition {id, event, guarded, label})
//!                              -[:from]->   (:State)
//!                              -[:to]->     (:State)
//!                              -[:hasGuard]->  (:Guard {id, op, spec})
//!                              -[:hasAction]-> (:Action {id, kind, spec})
//! ```

use std::collections::BTreeMap;

use crate::action::Action;
use crate::model::StatechartDef;

/// A neutral typed node descriptor to be materialized into the KG.
#[derive(Clone, Debug, PartialEq)]
pub struct KgNode {
    /// Stable node id (opaque, chart-scoped).
    pub id: String,
    /// The node's type labels (e.g. `["Statechart", "StatechartDef"]`).
    pub labels: Vec<String>,
    /// Scalar/JSON properties.
    pub properties: BTreeMap<String, serde_json::Value>,
}

/// A neutral typed edge descriptor to be materialized into the KG.
#[derive(Clone, Debug, PartialEq)]
pub struct KgEdge {
    /// Source node id.
    pub from: String,
    /// Edge type (e.g. `hasState`, `from`, `to`, `hasGuard`, `hasAction`).
    pub edge_type: String,
    /// Target node id.
    pub to: String,
}

/// The full set of nodes + edges a definition projects into.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct KgProjection {
    pub nodes: Vec<KgNode>,
    pub edges: Vec<KgEdge>,
}

fn state_node_id(def_id: &str, state: &str) -> String {
    format!("{def_id}/state/{state}")
}

fn transition_node_id(def_id: &str, index: usize) -> String {
    format!("{def_id}/transition/{index}")
}

/// Project a definition into typed KG node/edge descriptors (CONCEPT:INT-P2-2). Pure.
pub fn project(def: &StatechartDef) -> KgProjection {
    let def_id = def.def_id();
    let mut projection = KgProjection::default();

    // The chart root.
    let mut root_props = BTreeMap::new();
    root_props.insert("name".into(), serde_json::json!(def.name));
    root_props.insert("initial".into(), serde_json::json!(def.initial));
    root_props.insert("finals".into(), serde_json::json!(def.finals));
    root_props.insert("schema_version".into(), serde_json::json!(def.schema_version));
    root_props.insert("alphabet".into(), serde_json::json!(def.alphabet));
    projection.nodes.push(KgNode {
        id: def_id.clone(),
        labels: vec!["Statechart".into(), "StatechartDef".into()],
        properties: root_props,
    });

    // States.
    for state in &def.states {
        let node_id = state_node_id(&def_id, &state.id);
        let kind = if state.id == def.initial {
            "initial"
        } else if def.is_final(&state.id) {
            "final"
        } else {
            "atomic"
        };
        let mut props = BTreeMap::new();
        props.insert("state_id".into(), serde_json::json!(state.id));
        props.insert("kind".into(), serde_json::json!(kind));
        props.insert("entry_actions".into(), serde_json::json!(state.entry.len()));
        props.insert("exit_actions".into(), serde_json::json!(state.exit.len()));
        props.insert("composite".into(), serde_json::json!(state.is_composite()));
        projection.nodes.push(KgNode {
            id: node_id.clone(),
            labels: vec!["State".into()],
            properties: props,
        });
        projection.edges.push(KgEdge {
            from: def_id.clone(),
            edge_type: "hasState".into(),
            to: node_id.clone(),
        });
        // Entry/exit actions reified under the state.
        for (i, action) in state.entry.iter().enumerate() {
            emit_action(&mut projection, &node_id, "hasEntryAction", i, action);
        }
        for (i, action) in state.exit.iter().enumerate() {
            emit_action(&mut projection, &node_id, "hasExitAction", i, action);
        }
    }

    // Transitions (reified), each wired from/to its states + guard + actions.
    for (index, t) in def.transitions.iter().enumerate() {
        let node_id = transition_node_id(&def_id, index);
        let mut props = BTreeMap::new();
        props.insert("event".into(), serde_json::json!(t.event));
        props.insert("guarded".into(), serde_json::json!(t.guard.is_some()));
        props.insert("label".into(), serde_json::json!(t.label));
        projection.nodes.push(KgNode {
            id: node_id.clone(),
            labels: vec!["Transition".into()],
            properties: props,
        });
        projection.edges.push(KgEdge {
            from: def_id.clone(),
            edge_type: "hasTransition".into(),
            to: node_id.clone(),
        });
        projection.edges.push(KgEdge {
            from: node_id.clone(),
            edge_type: "from".into(),
            to: state_node_id(&def_id, &t.from),
        });
        projection.edges.push(KgEdge {
            from: node_id.clone(),
            edge_type: "to".into(),
            to: state_node_id(&def_id, &t.to),
        });
        if let Some(guard) = &t.guard {
            let guard_id = format!("{node_id}/guard");
            let mut gprops = BTreeMap::new();
            gprops.insert("spec".into(), serde_json::to_value(guard).unwrap_or_default());
            gprops.insert("depth".into(), serde_json::json!(guard.depth()));
            projection.nodes.push(KgNode {
                id: guard_id.clone(),
                labels: vec!["Guard".into()],
                properties: gprops,
            });
            projection.edges.push(KgEdge {
                from: node_id.clone(),
                edge_type: "hasGuard".into(),
                to: guard_id,
            });
        }
        for (i, action) in t.actions.iter().enumerate() {
            emit_action(&mut projection, &node_id, "hasAction", i, action);
        }
    }

    projection
}

fn emit_action(
    projection: &mut KgProjection,
    parent_id: &str,
    edge_type: &str,
    index: usize,
    action: &Action,
) {
    let action_id = format!("{parent_id}/{edge_type}/{index}");
    let mut props = BTreeMap::new();
    props.insert(
        "context_mutation".into(),
        serde_json::json!(action.is_context_mutation()),
    );
    props.insert("spec".into(), serde_json::to_value(action).unwrap_or_default());
    projection.nodes.push(KgNode {
        id: action_id.clone(),
        labels: vec!["Action".into()],
        properties: props,
    });
    projection.edges.push(KgEdge {
        from: parent_id.to_string(),
        edge_type: edge_type.to_string(),
        to: action_id,
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::action::{Action, ActionValue};
    use crate::guard::Guard;
    use crate::model::{State, Transition};

    fn def() -> StatechartDef {
        StatechartDef {
            name: "door".into(),
            schema_version: 1,
            states: vec![
                State::new("open"),
                State::new("closed").with_entry(vec![Action::Assign {
                    key: "shut".into(),
                    value: ActionValue::Const { value: serde_json::json!(true) },
                }]),
            ],
            alphabet: vec!["shut".into(), "open".into()],
            transitions: vec![
                Transition::new("open", "shut", "closed").with_guard(Guard::Always),
                Transition::new("closed", "open", "open"),
            ],
            initial: "open".into(),
            finals: vec!["closed".into()],
            meta: Default::default(),
        }
    }

    #[test]
    fn projects_root_states_and_transitions() {
        let p = project(&def());
        // 1 root + 2 states + 1 entry action + 2 transitions + 1 guard = 7 nodes.
        assert_eq!(p.nodes.iter().filter(|n| n.labels.contains(&"StatechartDef".to_string())).count(), 1);
        assert_eq!(p.nodes.iter().filter(|n| n.labels.contains(&"State".to_string())).count(), 2);
        assert_eq!(p.nodes.iter().filter(|n| n.labels.contains(&"Transition".to_string())).count(), 2);
        assert_eq!(p.nodes.iter().filter(|n| n.labels.contains(&"Guard".to_string())).count(), 1);
        assert_eq!(p.nodes.iter().filter(|n| n.labels.contains(&"Action".to_string())).count(), 1);
    }

    #[test]
    fn every_edge_endpoint_resolves_to_a_node() {
        let p = project(&def());
        let ids: std::collections::BTreeSet<&str> = p.nodes.iter().map(|n| n.id.as_str()).collect();
        for edge in &p.edges {
            assert!(ids.contains(edge.from.as_str()), "dangling edge source {}", edge.from);
            assert!(ids.contains(edge.to.as_str()), "dangling edge target {}", edge.to);
        }
    }

    #[test]
    fn state_kinds_reflect_initial_and_final() {
        let p = project(&def());
        let kind = |state: &str| {
            p.nodes
                .iter()
                .find(|n| n.properties.get("state_id") == Some(&serde_json::json!(state)))
                .and_then(|n| n.properties.get("kind"))
                .cloned()
        };
        assert_eq!(kind("open"), Some(serde_json::json!("initial")));
        assert_eq!(kind("closed"), Some(serde_json::json!("final")));
    }
}
