//! Pure KG projection of a [`DurableExecutionUnitMirror`] (DE1,
//! CONCEPT:AU-KG.storage.durable-execution-unit).
//!
//! Same shape and posture as `eg-statechart::kg::project` (a `StatechartDef` ->
//! typed node/edge descriptors, pure data, no `GraphCore`/engine dependency, fully
//! unit-testable, wired by whichever caller already holds a live graph handle): this
//! is that same pattern for a [`DurableExecutionUnitMirror`] instead of a chart
//! definition. `eg-durable` deliberately links none of `eg-statechart`/`eg-jobs`/
//! `eg-mutation-store` (DE0's own crate doc), so [`KgNode`]/[`KgEdge`] here are a
//! small, local, neutral descriptor pair rather than a re-export of
//! `eg_statechart::kg`'s types -- the two crates independently agree on the same
//! shape because it is the right shape, not because one depends on the other.
//!
//! **Not wired to a live caller yet** -- exactly the same honest posture
//! `eg_statechart::kg::project` itself has carried since it shipped (confirmed: no
//! caller anywhere in this workspace). A real call-through (the server dispatch
//! handler for a `eg-statechart`/`eg-jobs`/`eg-mutation-store` commit path calling
//! this after each transition) is tracked, not silently claimed done here — see
//! `docs/architecture/durable-execution.md`'s parity sweep in `agent-utilities`.

use std::collections::BTreeMap;

use crate::mirror::DurableExecutionUnitMirror;

/// A neutral typed node descriptor to be materialized into the KG (mirrors
/// `eg_statechart::kg::KgNode`'s shape).
#[derive(Clone, Debug, PartialEq)]
pub struct KgNode {
    pub id: String,
    pub labels: Vec<String>,
    pub properties: BTreeMap<String, serde_json::Value>,
}

/// A neutral typed edge descriptor to be materialized into the KG.
#[derive(Clone, Debug, PartialEq)]
pub struct KgEdge {
    pub from: String,
    pub edge_type: String,
    pub to: String,
}

/// The full set of nodes + edges one mirror projects into.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct KgProjection {
    pub nodes: Vec<KgNode>,
    pub edges: Vec<KgEdge>,
}

/// Stable, opaque `:backendRef`-derived node id: `<unit_label>:<backend_ref>`.
#[must_use]
pub fn unit_node_id(mirror: &DurableExecutionUnitMirror) -> String {
    format!("{}:{}", mirror.kind.unit_label(), mirror.backend_ref)
}

/// Project one [`DurableExecutionUnitMirror`] into typed KG node/edge descriptors
/// (DE1). Pure -- no I/O, no engine dependency.
///
/// Always emits exactly one node (labeled `["DurableExecutionUnit", <subclass>]`,
/// e.g. `["DurableExecutionUnit", "AnalyticsJob"]` -- both the abstract superclass
/// and the concrete subclass, so a caller materializing multi-label nodes gets
/// ontology-aware queries for free; a caller whose store only accepts one label
/// should prefer `labels[1]`, the concrete subclass). `produced_run_trace_id`, when
/// given, additionally emits the DE0 `:produced` edge to that `RunTrace` id
/// (`DurableExecutionUnit -[:PRODUCED]-> RunTrace`).
#[must_use]
pub fn project(
    mirror: &DurableExecutionUnitMirror,
    produced_run_trace_id: Option<&str>,
) -> KgProjection {
    let node_id = unit_node_id(mirror);
    let mut properties = BTreeMap::new();
    properties.insert("backend_ref".into(), serde_json::json!(mirror.backend_ref));
    properties.insert(
        "durable_status".into(),
        serde_json::json!(mirror.durable_status),
    );
    if let Some(checkpoint_ref) = &mirror.checkpoint_ref {
        properties.insert("checkpoint_ref".into(), serde_json::json!(checkpoint_ref));
    }
    if let Some(definition_version) = &mirror.definition_version {
        properties.insert(
            "definition_version".into(),
            serde_json::json!(definition_version),
        );
    }
    if let Some(lease_epoch) = mirror.lease_epoch {
        properties.insert("lease_epoch".into(), serde_json::json!(lease_epoch));
    }
    if let Some(idempotency_key) = &mirror.idempotency_key {
        properties.insert("idempotency_key".into(), serde_json::json!(idempotency_key));
    }

    let mut projection = KgProjection {
        nodes: vec![KgNode {
            id: node_id.clone(),
            labels: vec![
                "DurableExecutionUnit".to_string(),
                mirror.kind.unit_label().to_string(),
            ],
            properties,
        }],
        edges: Vec::new(),
    };
    if let Some(run_trace_id) = produced_run_trace_id {
        projection.edges.push(KgEdge {
            from: node_id,
            edge_type: "produced".to_string(),
            to: run_trace_id.to_string(),
        });
    }
    projection
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::route::DurableBackendKind;

    fn mirror() -> DurableExecutionUnitMirror {
        DurableExecutionUnitMirror {
            backend_ref: "job-7".to_string(),
            kind: DurableBackendKind::Jobs,
            durable_status: "running".to_string(),
            checkpoint_ref: Some("stage=fetch".to_string()),
            definition_version: Some("v1".to_string()),
            lease_epoch: Some(3),
            idempotency_key: Some("idem-abc".to_string()),
        }
    }

    #[test]
    fn projects_one_node_labeled_abstract_and_concrete() {
        let p = project(&mirror(), None);
        assert_eq!(p.nodes.len(), 1);
        assert_eq!(p.edges.len(), 0);
        let node = &p.nodes[0];
        assert_eq!(node.id, "AnalyticsJob:job-7");
        assert_eq!(
            node.labels,
            vec![
                "DurableExecutionUnit".to_string(),
                "AnalyticsJob".to_string()
            ]
        );
    }

    #[test]
    fn projects_every_present_optional_field() {
        let p = project(&mirror(), None);
        let props = &p.nodes[0].properties;
        assert_eq!(props["backend_ref"], serde_json::json!("job-7"));
        assert_eq!(props["durable_status"], serde_json::json!("running"));
        assert_eq!(props["checkpoint_ref"], serde_json::json!("stage=fetch"));
        assert_eq!(props["definition_version"], serde_json::json!("v1"));
        assert_eq!(props["lease_epoch"], serde_json::json!(3));
        assert_eq!(props["idempotency_key"], serde_json::json!("idem-abc"));
    }

    #[test]
    fn absent_optionals_are_omitted_not_null() {
        let mut m = mirror();
        m.checkpoint_ref = None;
        m.definition_version = None;
        m.lease_epoch = None;
        m.idempotency_key = None;
        let p = project(&m, None);
        let props = &p.nodes[0].properties;
        assert!(!props.contains_key("checkpoint_ref"));
        assert!(!props.contains_key("definition_version"));
        assert!(!props.contains_key("lease_epoch"));
        assert!(!props.contains_key("idempotency_key"));
    }

    #[test]
    fn produced_run_trace_id_emits_the_de0_produced_edge() {
        let p = project(&mirror(), Some("trace:abc"));
        assert_eq!(p.edges.len(), 1);
        assert_eq!(p.edges[0].from, "AnalyticsJob:job-7");
        assert_eq!(p.edges[0].edge_type, "produced");
        assert_eq!(p.edges[0].to, "trace:abc");
    }

    #[test]
    fn node_id_is_stable_and_kind_scoped() {
        let statechart = DurableExecutionUnitMirror {
            kind: DurableBackendKind::Statechart,
            ..mirror()
        };
        // Same backend_ref, different backend kind -> different node id (no
        // cross-backend collision even if two backends reuse an id string).
        assert_ne!(unit_node_id(&mirror()), unit_node_id(&statechart));
        assert_eq!(unit_node_id(&statechart), "StatechartInstance:job-7");
    }
}
