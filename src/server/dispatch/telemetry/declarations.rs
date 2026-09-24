//! What the request graph declares about the entities telemetry binds to
//! (EH-408 / EH-409).
//!
//! An individual takes part when its node `type` names a class the ontology
//! subsumes under a bindable class (see [`super::classes`]) AND the verified
//! caller can see its row. It declares, as node properties:
//!
//! * [`RESOLUTION_KEYS_PROPERTY`] -- an object of attribute name → value, the
//!   resolution-key values telemetry from it carries (for example
//!   `{"service.name": "checkout"}`);
//! * [`DECLARED_HEALTH_PROPERTY`] -- optional, a declared health such as
//!   `{"health": "healthy", "max_error_ratio": 0.05}` or `{"health": "retired"}`.
//!
//! An individual declaring neither takes no part. One whose declaration does
//! not parse is skipped and counted, never guessed at.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use eg_core::graph::GraphCore;
use eg_stream::telemetry::{
    Declarations, DeclaredEntity, DeclaredHealth, DeclaredState, EntityClass, EntityRef,
};

use super::classes::{bindable_types, BindableTypes};
use serde_json::Value;
use tokio::sync::RwLock;

use super::super::timed_read;
use crate::isolation::AccessLevel;
use crate::server::access::{check_graph_access, GraphReadAuthority};
use crate::server::auth::VerifiedRequestContext;
use crate::server::state::ServerState;

pub(super) const RESOLUTION_KEYS_PROPERTY: &str = "resolution_keys";
pub(super) const DECLARED_HEALTH_PROPERTY: &str = "declared_health";

/// The most individuals of one node type a derivation reads.
const MAX_INDIVIDUALS_PER_TYPE: usize = 100_000;

/// The declarations read from one graph, and how many were unusable.
#[derive(Debug, Default)]
pub(super) struct ReadDeclarations {
    pub(super) declarations: Declarations,
    pub(super) invalid: usize,
}

/// Read the declarations of `graph` visible to the verified caller.
pub(super) async fn read(
    state: &Arc<RwLock<ServerState>>,
    graph: &str,
    verified: &VerifiedRequestContext,
) -> Result<ReadDeclarations, String> {
    let (core, authority) = {
        let current = timed_read(state).await;
        let entry = current
            .registry
            .get(graph)
            .ok_or_else(|| format!("GRAPH_NOT_FOUND: {graph}"))?;
        check_graph_access(
            &current.isolation,
            Some(verified.agent_id()),
            graph,
            entry.graph_type,
            entry.owner.as_deref(),
            AccessLevel::Read,
        )?;
        let authority = GraphReadAuthority::from_verified(verified, &current.isolation)?;
        (entry.core.clone(), authority)
    };
    let sources = core.schema_sources();
    let types = tokio::task::spawn_blocking(move || bindable_types(&sources))
        .await
        .map_err(|_| "telemetry schema classification worker failed".to_string())??;
    Ok(declarations_from(&core, &types, |node_id, properties| {
        authority.can_see_node(properties, core.is_schema_node(node_id))
    }))
}

/// The declarations of every individual of a bindable type that `visible`
/// admits. A node found under several type labels is read once, under the
/// union of their target classes.
pub(super) fn declarations_from(
    core: &GraphCore,
    types: &BindableTypes,
    visible: impl Fn(&str, &[u8]) -> bool,
) -> ReadDeclarations {
    let mut individuals: BTreeMap<String, (BTreeSet<EntityClass>, Vec<u8>)> = BTreeMap::new();
    for node_type in types.types() {
        let targets = types.targets(node_type).cloned().unwrap_or_default();
        for (node_id, properties) in core.get_nodes_by_label(node_type, MAX_INDIVIDUALS_PER_TYPE) {
            if visible(&node_id, &properties) {
                let entry = individuals
                    .entry(node_id)
                    .or_insert_with(|| (BTreeSet::new(), properties));
                entry.0.extend(targets.iter().copied());
            }
        }
    }
    let mut read = ReadDeclarations::default();
    for (node_id, (classes, properties)) in individuals {
        for class in classes {
            read.push(class, node_id.clone(), &properties);
        }
    }
    read
}

impl ReadDeclarations {
    fn push(&mut self, class: EntityClass, node_id: String, properties: &[u8]) {
        let Ok(node) = eg_types::msgpack::decode_property_value(properties) else {
            self.invalid += 1;
            return;
        };
        let entity = EntityRef { class, id: node_id };
        match declared_keys(&node) {
            Declared::Valid(keys) => self.declarations.entities.push(DeclaredEntity {
                entity: entity.clone(),
                keys,
            }),
            Declared::Invalid => self.invalid += 1,
            Declared::Absent => {}
        }
        match declared_health(&node) {
            Declared::Valid(health) => self.declarations.health.push(DeclaredState {
                declared_by: entity.id.clone(),
                entity,
                health,
            }),
            Declared::Invalid => self.invalid += 1,
            Declared::Absent => {}
        }
    }
}

/// One optional declaration read from a node.
enum Declared<T> {
    Absent,
    Invalid,
    Valid(T),
}

fn declared_keys(node: &Value) -> Declared<BTreeMap<String, String>> {
    let Some(keys) = node.get(RESOLUTION_KEYS_PROPERTY) else {
        return Declared::Absent;
    };
    let parsed: Option<BTreeMap<String, String>> = keys.as_object().and_then(|object| {
        object
            .iter()
            .map(|(name, value)| Some((name.clone(), value.as_str()?.to_string())))
            .collect()
    });
    match parsed {
        Some(keys) if !keys.is_empty() => Declared::Valid(keys),
        _ => Declared::Invalid,
    }
}

fn declared_health(node: &Value) -> Declared<DeclaredHealth> {
    let Some(health) = node.get(DECLARED_HEALTH_PROPERTY) else {
        return Declared::Absent;
    };
    match serde_json::from_value(health.clone()) {
        Ok(health) => Declared::Valid(health),
        Err(_) => Declared::Invalid,
    }
}
