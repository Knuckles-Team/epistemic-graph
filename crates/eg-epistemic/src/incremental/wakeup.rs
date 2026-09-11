use std::collections::{BTreeMap, BTreeSet};

use eg_core::graph::GraphView;

use super::{
    opaque_identity, IncrementalDelta, IncrementalReasoningEvent, IncrementalReasoningIndex,
    ProjectionInvalidationKind, ProjectionPosition, ReasoningProjectionWakeup,
};

impl IncrementalReasoningIndex {
    pub fn apply_wakeup(
        &mut self,
        position: ProjectionPosition,
        wakeup: &ReasoningProjectionWakeup,
        view: &GraphView,
    ) -> Result<IncrementalDelta, String> {
        wakeup.validate()?;
        let Some(position) = self.prepare_position(position)? else {
            return Ok(IncrementalDelta::default());
        };
        let mut touched_nodes = BTreeSet::new();
        let mut touched_edges = BTreeSet::new();
        let mut delta = IncrementalDelta::default();
        {
            let mut context = WakeupContext {
                committed_source_graph_version: position.source_graph_version,
                view,
                touched_nodes: &mut touched_nodes,
                touched_edges: &mut touched_edges,
                delta: &mut delta,
            };
            for event in &wakeup.events {
                apply_event(self, event, &mut context)?;
            }
        }
        let delta = complete_wakeup(self, view, touched_nodes, touched_edges, delta);
        self.position = Some(position);
        Ok(delta)
    }
}

struct WakeupContext<'a> {
    committed_source_graph_version: u64,
    view: &'a GraphView,
    touched_nodes: &'a mut BTreeSet<String>,
    touched_edges: &'a mut BTreeSet<(String, String)>,
    delta: &'a mut IncrementalDelta,
}

fn apply_event(
    index: &mut IncrementalReasoningIndex,
    event: &IncrementalReasoningEvent,
    context: &mut WakeupContext<'_>,
) -> Result<(), String> {
    match event {
        IncrementalReasoningEvent::NodeUpserted {
            node_ref,
            dependency_refs,
            generator_ref,
            is_materialization,
        } => apply_node_upserted(
            index,
            node_ref,
            dependency_refs,
            generator_ref,
            *is_materialization,
            context,
        ),
        IncrementalReasoningEvent::NodeRemoved { node_ref } => {
            apply_node_removed(index, node_ref, context)
        }
        IncrementalReasoningEvent::NodeChanged { node_ref } => {
            index.stale_dependents(node_ref, context.delta);
            context.touched_nodes.insert(node_ref.clone());
            Ok(())
        }
        IncrementalReasoningEvent::EdgeUpserted {
            source_ref,
            target_ref,
            relationship,
        } => apply_edge_upserted(index, source_ref, target_ref, relationship, context),
        IncrementalReasoningEvent::EdgeRemoved {
            source_ref,
            target_ref,
        } => apply_edge_removed(index, source_ref, target_ref, context),
        IncrementalReasoningEvent::Invalidate {
            invalidation,
            subject_ref,
        } => apply_invalidation(index, invalidation, subject_ref, context.delta),
        IncrementalReasoningEvent::Recompute {
            materialization_ref,
            expected_source_graph_version,
        } => {
            index.recompute_from_ref(
                materialization_ref,
                *expected_source_graph_version,
                context.committed_source_graph_version,
                context.view,
            )?;
            Ok(())
        }
        IncrementalReasoningEvent::InvalidateAll => apply_invalidate_all(index, context),
    }
}

fn apply_node_upserted(
    index: &mut IncrementalReasoningIndex,
    node_ref: &str,
    dependency_refs: &BTreeSet<String>,
    generator_ref: &Option<String>,
    is_materialization: bool,
    context: &mut WakeupContext<'_>,
) -> Result<(), String> {
    index.stale_dependents(node_ref, context.delta);
    if is_materialization {
        index.register_materialization_refs(
            node_ref,
            dependency_refs.clone(),
            generator_ref.clone(),
        );
    } else {
        index.clear_materialization_state(node_ref);
    }
    context.touched_nodes.insert(node_ref.to_string());
    Ok(())
}

fn apply_node_removed(
    index: &mut IncrementalReasoningIndex,
    node_ref: &str,
    context: &mut WakeupContext<'_>,
) -> Result<(), String> {
    let was_materialization = index.materializations.contains(node_ref);
    index.remove_incident(node_ref, context.delta);
    index.materialization_deps.remove(node_ref);
    index.remove_materialization_generator(node_ref);
    index.stale_materializations.remove(node_ref);
    if was_materialization {
        index
            .retracted_materializations
            .insert(node_ref.to_string());
    } else {
        index.materializations.remove(node_ref);
        index.retracted_materializations.remove(node_ref);
    }
    index.stale_dependents(node_ref, context.delta);
    context.touched_nodes.insert(node_ref.to_string());
    Ok(())
}

fn apply_edge_upserted(
    index: &mut IncrementalReasoningIndex,
    source_ref: &str,
    target_ref: &str,
    relationship: &Option<String>,
    context: &mut WakeupContext<'_>,
) -> Result<(), String> {
    if let Some(relationship) = relationship {
        index.add_edge_relationship(source_ref, target_ref, relationship);
    }
    index.stale_dependents(source_ref, context.delta);
    index.stale_dependents(target_ref, context.delta);
    context
        .touched_edges
        .insert((source_ref.to_string(), target_ref.to_string()));
    Ok(())
}

fn apply_edge_removed(
    index: &mut IncrementalReasoningIndex,
    source_ref: &str,
    target_ref: &str,
    context: &mut WakeupContext<'_>,
) -> Result<(), String> {
    if index.remove_edge_refs(source_ref, target_ref) {
        context.delta.edge_changes += 1;
    }
    index.stale_dependents(source_ref, context.delta);
    index.stale_dependents(target_ref, context.delta);
    context
        .touched_edges
        .insert((source_ref.to_string(), target_ref.to_string()));
    Ok(())
}

fn apply_invalidation(
    index: &mut IncrementalReasoningIndex,
    invalidation: &str,
    subject_ref: &str,
    delta: &mut IncrementalDelta,
) -> Result<(), String> {
    let kind = match invalidation {
        "policy_changed" => ProjectionInvalidationKind::PolicyChanged,
        "model_retired" => ProjectionInvalidationKind::ModelRetired,
        "ontology_evolved" => ProjectionInvalidationKind::OntologyEvolved,
        _ => return Err("reasoning invalidation event is invalid".to_string()),
    };
    delta
        .newly_stale
        .extend(index.invalidate(kind, subject_ref));
    Ok(())
}

fn apply_invalidate_all(
    index: &mut IncrementalReasoningIndex,
    context: &mut WakeupContext<'_>,
) -> Result<(), String> {
    let prior_epoch = index.recompute_epoch;
    let mut rebuilt = IncrementalReasoningIndex::from_graph_view(context.view);
    rebuilt.recompute_epoch = prior_epoch;
    for materialization in &rebuilt.materializations {
        if rebuilt
            .stale_materializations
            .insert(materialization.clone())
        {
            context.delta.newly_stale.insert(materialization.clone());
        }
    }
    *index = rebuilt;
    context.touched_nodes.clear();
    context.touched_edges.clear();
    Ok(())
}

fn complete_wakeup(
    index: &mut IncrementalReasoningIndex,
    view: &GraphView,
    touched_nodes: BTreeSet<String>,
    touched_edges: BTreeSet<(String, String)>,
    delta: IncrementalDelta,
) -> IncrementalDelta {
    let source_nodes = view
        .node_properties
        .keys()
        .map(|node_id| (opaque_identity(node_id), node_id.as_str()))
        .collect::<BTreeMap<_, _>>();
    for node_ref in touched_nodes {
        if let Some(node_id) = source_nodes.get(&node_ref) {
            index.refresh_from_graph_view(view, node_id);
        }
    }
    for (source_ref, target_ref) in touched_edges {
        index.remove_edge_refs(&source_ref, &target_ref);
        let Some(source_id) = source_nodes.get(&source_ref) else {
            continue;
        };
        let Some(target_id) = source_nodes.get(&target_ref) else {
            continue;
        };
        if let Some(properties) = view
            .edge_properties
            .get(&(source_id.to_string(), target_id.to_string()))
            .and_then(|versions| versions.last())
        {
            index.add_edge(source_id, target_id, properties);
        }
    }
    delta
}
