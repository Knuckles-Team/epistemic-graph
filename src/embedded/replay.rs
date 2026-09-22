//! Boot-time replay of the durable store into a fresh registry
//! (CONCEPT:EG-KG.backend.engine-modes).
//!
//! The SAME reconstruction the server's redb `load_all` performs, expressed
//! over the embedded transport's synchronous registry lock.

use std::sync::Arc;

use parking_lot::RwLock;

use crate::compute::semantic::SemanticStore;
use crate::graph::{GraphSnapshot, GRAPH_SNAPSHOT_SCHEMA_VERSION};
use crate::redb_store::GraphDump;
use crate::registry::GraphRegistry;

const COMMONS: &str = "__commons__";

/// Replay every durable graph dump into `registry`.
pub(super) fn replay_durable_dumps(
    registry: &RwLock<GraphRegistry>,
    dumps: Vec<GraphDump>,
) -> Result<(), String> {
    let mut reg = registry.write();
    for dump in dumps {
        if dump.name == COMMONS {
            install_commons_dump(&mut reg, dump)?;
        } else {
            replay_graph_dump(&mut reg, dump);
        }
    }
    Ok(())
}

/// Install the durable `__commons__` image as a committed graph.
///
/// `GraphRegistry::new` seeds an in-memory `__commons__` placeholder so
/// in-memory callers can use it immediately. A durable commons dump is a
/// committed image, however, and must replace that placeholder so its
/// authoritative Graph(version) is adopted before publication. Replaying rows
/// into the bootstrap core leaves its version at zero and makes the next
/// checkpoint look stale.
fn install_commons_dump(reg: &mut GraphRegistry, dump: GraphDump) -> Result<(), String> {
    let semantic_store = if dump.semantic.is_empty() {
        SemanticStore::new()
    } else {
        rmp_serde::from_slice::<SemanticStore>(&dump.semantic).map_err(|error| {
            format!("failed to decode durable __commons__ semantic store: {error}")
        })?
    };
    let GraphDump {
        graph_type,
        incarnation_id,
        source_snapshot_version,
        schema_sources,
        nodes,
        edges,
        ledger,
        ..
    } = dump;
    let snapshot = GraphSnapshot {
        nodes: nodes
            .into_iter()
            .map(|(id, properties)| (id, Arc::new(properties)))
            .collect(),
        edges: edges
            .into_iter()
            .map(|(source, target, properties)| (source, target, Arc::new(properties)))
            .collect(),
        schema_version: GRAPH_SNAPSHOT_SCHEMA_VERSION,
        schema_sources,
        ledger,
        semantic_store,
    };
    reg.install_committed_graph(
        COMMONS,
        graph_type,
        None,
        incarnation_id,
        snapshot,
        source_snapshot_version,
    )?;
    Ok(())
}

/// Recreate (when absent) and repopulate one non-commons graph from its rows.
///
/// A semantic store that no longer decodes is skipped, leaving the graph's
/// fresh store in place, exactly as boot recovery always has.
fn replay_graph_dump(reg: &mut GraphRegistry, dump: GraphDump) {
    if !reg.exists(&dump.name) {
        let _ = reg.create_graph_with_incarnation(
            &dump.name,
            dump.graph_type,
            None,
            dump.incarnation_id.clone(),
            dump.source_snapshot_version,
        );
    }
    let Some(core) = reg.get(&dump.name).map(|entry| entry.core.clone()) else {
        return;
    };
    let GraphDump {
        schema_sources,
        nodes,
        edges,
        semantic,
        ..
    } = dump;
    core.install_schema_sources(schema_sources);
    nodes
        .into_iter()
        .for_each(|(id, properties)| core.add_node(id, properties));
    edges.into_iter().for_each(|(source, target, properties)| {
        let _ = core.add_edge(source, target, properties);
    });
    let decoded = (!semantic.is_empty())
        .then(|| rmp_serde::from_slice::<SemanticStore>(&semantic).ok())
        .flatten();
    if let Some(store) = decoded {
        *core.semantic_store.write() = store;
    }
}
