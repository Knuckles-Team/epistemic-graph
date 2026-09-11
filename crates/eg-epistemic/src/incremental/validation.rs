use std::collections::BTreeSet;

use super::REASONING_PROJECTION_VERSION;
use super::{opaque_identity, IncrementalReasoningIndex, ProjectionPosition};

impl ProjectionPosition {
    pub fn validate(&self) -> Result<(), String> {
        if self.batch_id.trim().is_empty() {
            return Err("projection position requires a batch identity".to_string());
        }
        if self.source_graph_version == 0 {
            return Err(
                "reasoning projection requires a non-zero source graph version".to_string(),
            );
        }
        Ok(())
    }
}

impl IncrementalReasoningIndex {
    pub fn validate(&self) -> Result<(), String> {
        validate_schema_and_position(self)?;
        validate_materialization_state(self)?;
        validate_opaque_references(self)?;
        validate_generator_index(self)?;
        validate_canonical_generators(self)?;
        validate_provenance_dependencies(self)?;
        validate_relationships(self)?;
        validate_recompute_fences(self)
    }
}

fn validate_schema_and_position(index: &IncrementalReasoningIndex) -> Result<(), String> {
    if index.schema_version != REASONING_PROJECTION_VERSION {
        return Err(format!(
            "unsupported reasoning projection version {} (expected {})",
            index.schema_version, REASONING_PROJECTION_VERSION
        ));
    }
    if let Some(position) = &index.position {
        position.validate()?;
        if opaque_identity(&position.batch_id) != position.batch_id {
            return Err("reasoning projection position is not opaque".to_string());
        }
    }
    Ok(())
}

fn validate_materialization_state(index: &IncrementalReasoningIndex) -> Result<(), String> {
    let materialization_state_is_known = index
        .materialization_deps
        .keys()
        .chain(index.materialization_generators.keys())
        .chain(
            index
                .generator_materializations
                .values()
                .flat_map(|values| values.iter()),
        )
        .chain(index.stale_materializations.iter())
        .chain(index.retracted_materializations.iter())
        .all(|materialization| index.materializations.contains(materialization));
    if !materialization_state_is_known {
        return Err(
            "reasoning projection contains state for an unknown materialization".to_string(),
        );
    }
    if index
        .stale_materializations
        .iter()
        .any(|materialization| index.retracted_materializations.contains(materialization))
    {
        return Err(
            "reasoning projection materialization cannot be stale and retracted".to_string(),
        );
    }
    Ok(())
}

fn valid_ref(value: &str) -> bool {
    opaque_identity(value) == value
}

fn validate_opaque_references(index: &IncrementalReasoningIndex) -> Result<(), String> {
    let all_refs_valid = index.materializations.iter().all(|value| valid_ref(value))
        && index
            .materialization_deps
            .iter()
            .all(|(materialization, dependencies)| {
                valid_ref(materialization) && dependencies.iter().all(|value| valid_ref(value))
            })
        && index
            .materialization_generators
            .iter()
            .all(|(materialization, generator)| valid_ref(materialization) && valid_ref(generator))
        && index
            .generator_materializations
            .iter()
            .all(|(generator, materializations)| {
                valid_ref(generator) && materializations.iter().all(|value| valid_ref(value))
            })
        && index
            .epistemic_edges
            .keys()
            .chain(index.provenance_edges.keys())
            .all(|(source, target)| valid_ref(source) && valid_ref(target))
        && index
            .causal_out
            .iter()
            .chain(index.conflicts.iter())
            .all(|(source, targets)| {
                valid_ref(source) && targets.iter().all(|target| valid_ref(target))
            })
        && index
            .stale_materializations
            .iter()
            .chain(index.retracted_materializations.iter())
            .chain(index.recompute_fences.keys())
            .all(|value| valid_ref(value));
    if !all_refs_valid {
        return Err("reasoning projection contains a non-opaque identity".to_string());
    }
    Ok(())
}

fn validate_generator_index(index: &IncrementalReasoningIndex) -> Result<(), String> {
    let reverse_index_is_consistent = !index
        .generator_materializations
        .values()
        .any(BTreeSet::is_empty)
        && !index
            .materialization_generators
            .iter()
            .any(|(materialization, generator)| {
                !index
                    .generator_materializations
                    .get(generator)
                    .is_some_and(|values| values.contains(materialization))
            })
        && !index
            .generator_materializations
            .iter()
            .any(|(generator, materializations)| {
                materializations.iter().any(|materialization| {
                    index.materialization_generators.get(materialization) != Some(generator)
                })
            });
    if !reverse_index_is_consistent {
        return Err("reasoning projection generator reverse index is inconsistent".to_string());
    }
    Ok(())
}

fn validate_canonical_generators(index: &IncrementalReasoningIndex) -> Result<(), String> {
    for (materialization, expected_generator) in index.canonical_edge_generators() {
        if index.materialization_generators.get(&materialization) != Some(&expected_generator) {
            return Err(
                "reasoning projection canonical generator edge is inconsistent".to_string(),
            );
        }
    }
    Ok(())
}

fn validate_provenance_dependencies(index: &IncrementalReasoningIndex) -> Result<(), String> {
    let dependencies_are_indexed = !index
        .provenance_edges
        .iter()
        .filter(|(_, relationship)| {
            matches!(relationship.as_str(), "DERIVED_FROM" | "GENERATED_BY")
        })
        .any(|((materialization, dependency), _)| {
            !index
                .materialization_deps
                .get(materialization)
                .is_some_and(|dependencies| dependencies.contains(dependency))
        });
    if !dependencies_are_indexed {
        return Err("reasoning projection provenance dependency index is inconsistent".to_string());
    }
    Ok(())
}

fn validate_relationships(index: &IncrementalReasoningIndex) -> Result<(), String> {
    let relationships_are_valid = !index
        .epistemic_edges
        .values()
        .chain(index.provenance_edges.values())
        .any(|relationship| {
            !matches!(
                relationship.as_str(),
                "SUPPORTS"
                    | "CONTRADICTS"
                    | "ATTACKS"
                    | "CAUSES"
                    | "ENABLES"
                    | "DERIVED_FROM"
                    | "GENERATED_BY"
            )
        });
    if !relationships_are_valid {
        return Err("reasoning projection contains an invalid relationship".to_string());
    }
    Ok(())
}

fn validate_recompute_fences(index: &IncrementalReasoningIndex) -> Result<(), String> {
    let fences_are_valid = !index
        .recompute_fences
        .iter()
        .any(|(materialization, epoch)| {
            !index.materializations.contains(materialization)
                || *epoch == 0
                || *epoch > index.recompute_epoch
        });
    if !fences_are_valid {
        return Err("reasoning projection contains an invalid recompute fence".to_string());
    }
    Ok(())
}
