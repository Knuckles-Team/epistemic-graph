//! Shape-checked `RunTrace` writes (EG-ORCH-VOCAB-R009.15).
//!
//! A direct `AddNode` of a `RunTrace` node is validated against the
//! orchestration SHACL shapes at the same pre-commit point as the harness
//! admission. A violation is a typed refusal naming the shape; nothing is
//! written. Every other node passes through untouched.

use crate::protocol::Method;

/// Properties that link a trace to another node; they are IRIs, not literals.
const LINKS: [&str; 3] = ["runModel", "runTaskClass", "appliedHarnessConfig"];
/// Properties the shapes constrain; others are not part of the RDF view.
const SHAPED: [&str; 12] = [
    "status",
    "terminationReason",
    "peakContextTokens",
    "contextBudgetTokens",
    "elisionCount",
    "summarizationCount",
    "turnCount",
    "toolCallCount",
    "costUsd",
    "tokenCost",
    "runModel",
    "runTaskClass",
];

fn is_run_trace(props: &serde_json::Map<String, serde_json::Value>) -> bool {
    ["type", "node_type", "label"]
        .iter()
        .any(|key| props.get(*key).and_then(|v| v.as_str()) == Some("RunTrace"))
}

fn literal(value: &serde_json::Value) -> Option<String> {
    use serde_json::Value;
    match value {
        Value::Number(n) if n.is_i64() || n.is_u64() => Some(n.to_string()),
        Value::Number(n) => Some(format!("\"{n}\"^^xsd:decimal")),
        Value::String(s) => Some(format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\""))),
        Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

/// The node as Turtle: the shaped properties and the three links.
fn turtle(props: &serde_json::Map<String, serde_json::Value>) -> String {
    let mut out = String::from(
        "@prefix : <http://knuckles.team/kg#> .\n@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n<urn:eg:run-trace> a :RunTrace",
    );
    for key in SHAPED.iter().chain(LINKS.iter()) {
        let Some(value) = props.get(*key) else { continue };
        let object = if LINKS.contains(key) {
            value.as_str().map(|id| {
                let encoded: String = id
                    .bytes()
                    .map(|b| match b {
                        b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'-' | b'_' | b'.' => {
                            (b as char).to_string()
                        }
                        _ => format!("%{b:02X}"),
                    })
                    .collect();
                format!("<urn:eg:{encoded}>")
            })
        } else if *key == "costUsd" || *key == "tokenCost" {
            // A decimal is carried as text or number on the wire.
            match value {
                serde_json::Value::String(s) => Some(format!("\"{s}\"^^xsd:decimal")),
                other => literal(other).map(|l| {
                    if l.contains("^^") { l } else { format!("\"{l}\"^^xsd:decimal") }
                }),
            }
        } else {
            literal(value)
        };
        if let Some(object) = object {
            out.push_str(&format!(" ;\n  :{key} {object}"));
        }
    }
    out.push_str(" .\n");
    out
}

/// An admitted write: the node blob without its link declarations, and the
/// edges that carry those links.
pub struct Admitted {
    pub method: Method,
    pub links: Vec<Method>,
}

/// Refuse a shape-invalid or dangling-link `RunTrace` write; any other write
/// passes unchanged. The links the caller declares (`runModel`,
/// `runTaskClass`, `appliedHarnessConfig`) are validated as present for the
/// shapes, must name existing nodes, and are stored as edges from the trace.
pub fn admit(method: Method, core: &crate::graph::GraphCore) -> Result<Admitted, String> {
    let passthrough = |method| Admitted {
        method,
        links: Vec::new(),
    };
    let Method::AddNode {
        node_id,
        properties_msgpack,
    } = &method
    else {
        return Ok(passthrough(method));
    };
    let Ok(mut props) = eg_types::msgpack::decode_property_object(properties_msgpack) else {
        return Ok(passthrough(method));
    };
    if !is_run_trace(&props) {
        return Ok(passthrough(method));
    }
    validate_shapes(&props)?;
    let mut links = Vec::new();
    for link in LINKS {
        let Some(value) = props.remove(link) else {
            continue;
        };
        let target = value.as_str().ok_or_else(|| {
            format!("SHAPE_VIOLATION: RunTrace link {link} must name a node id")
        })?;
        if core.get_node_properties(target).is_none() {
            return Err(format!(
                "SHAPE_VIOLATION: RunTrace link {link} names unknown node {target}"
            ));
        }
        links.push(Method::AddEdge {
            source_id: node_id.clone(),
            target_id: target.to_string(),
            properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({ "relationship": link }))
                .map_err(|error| format!("run trace link encoding failed: {error}"))?,
        });
    }
    let stripped = rmp_serde::to_vec_named(&serde_json::Value::Object(props))
        .map_err(|error| format!("run trace encoding failed: {error}"))?;
    Ok(Admitted {
        method: Method::AddNode {
            node_id: node_id.clone(),
            properties_msgpack: stripped,
        },
        links,
    })
}

#[cfg(feature = "shacl")]
fn validate_shapes(props: &serde_json::Map<String, serde_json::Value>) -> Result<(), String> {
    let report = eg_shacl::validate::validate_turtle(
        crate::graph::ORCHESTRATION_SHAPES,
        &turtle(props),
    )
    .map_err(|error| format!("SHAPE_VIOLATION: run trace could not be validated: {error}"))?;
    if report.conforms {
        return Ok(());
    }
    let named: Vec<String> = report
        .results
        .iter()
        .map(|r| {
            format!(
                "{} ({})",
                r.source_shape,
                r.message.as_deref().unwrap_or(&r.constraint_component)
            )
        })
        .collect();
    Err(format!(
        "SHAPE_VIOLATION: RunTrace refused by shape {}",
        named.join("; ")
    ))
}

/// Without SHACL no shape can be evaluated, so the write is admitted.
#[cfg(not(feature = "shacl"))]
fn validate_shapes(_props: &serde_json::Map<String, serde_json::Value>) -> Result<(), String> {
    Ok(())
}
