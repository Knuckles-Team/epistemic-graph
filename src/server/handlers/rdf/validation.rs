use std::sync::Arc;

use crate::graph::GraphCore;
use crate::protocol::{Response, ResultPayload};

/// Validate the request graph (or an inline `data_graph` Turtle document) against
/// SHACL constraints (CONCEPT:EG-KG.ontology.concept-6), returning a `Json`
/// `sh:ValidationReport`. Omitted/empty `shapes` means the request graph's
/// composed GraphSchema shapes. Read-only: an empty `data_graph` exports the
/// LIVE graph's RDF (the same triples `GetRdf` serializes) and validates that.
#[cfg(feature = "shacl")]
pub(super) async fn handle_shacl_validate(
    req_id: u64,
    graph_name: &str,
    core: &Arc<GraphCore>,
    shapes: Option<String>,
    data_graph: String,
) -> Response {
    let shapes = match load_shacl_shapes(core, shapes.as_deref()) {
        Ok(shapes) => shapes,
        Err(error) => return Response::err(req_id, error),
    };
    let data = match load_shacl_data(graph_name, core, &data_graph) {
        Ok(graph) => graph,
        Err(error) => return Response::err(req_id, error),
    };
    let report = match eg_shacl::validate(&shapes.graph, &data) {
        Ok(report) => report,
        Err(error) => return Response::err(req_id, format!("ShaclValidate: {error}")),
    };
    Response::ok(
        req_id,
        ResultPayload::of::<eg_types::result_contract::reasoning::ShaclValidate>(
            shacl_report_wire(report, shapes.schema_digests, shapes.composed_digest),
        ),
    )
}

#[cfg(feature = "shacl")]
struct LoadedShaclShapes {
    graph: eg_shacl::Graph,
    schema_digests: Vec<String>,
    composed_digest: Option<String>,
}

#[cfg(feature = "shacl")]
fn load_shacl_shapes(core: &GraphCore, shapes: Option<&str>) -> Result<LoadedShaclShapes, String> {
    if let Some(shapes) = shapes.filter(|document| !document.trim().is_empty()) {
        return eg_shacl::graph_from_turtle(shapes)
            .map(|graph| LoadedShaclShapes {
                graph,
                schema_digests: Vec::new(),
                composed_digest: None,
            })
            .map_err(|error| format!("ShaclValidate: bad shapes graph: {error}"));
    }
    let sources = core.schema_sources();
    let composed_digest = sources.composed_digest().to_hex();
    let mut schema_digests: Vec<String> = sources
        .all()
        .filter_map(|(_, source)| source.shapes_sha256.map(|digest| digest.to_hex()))
        .collect();
    schema_digests.sort_unstable();
    schema_digests.dedup();
    crate::server::graph_schema::compose::validate_and_compose(&sources)
        .and_then(|composed| {
            if composed.shapes.is_empty() {
                return Err("no committed GraphSchema SHACL sources".to_string());
            }
            Ok(LoadedShaclShapes {
                graph: composed.shapes,
                schema_digests,
                composed_digest: Some(composed_digest),
            })
        })
        .map_err(|error| format!("ShaclValidate: composed GraphSchema is invalid: {error}"))
}

#[cfg(feature = "shacl")]
fn load_shacl_data(
    graph_name: &str,
    core: &Arc<GraphCore>,
    data_graph: &str,
) -> Result<eg_shacl::Graph, String> {
    if !data_graph.trim().is_empty() {
        return eg_shacl::graph_from_turtle(data_graph)
            .map_err(|error| format!("ShaclValidate: bad data graph: {error}"));
    }
    let triples = eg_rdf::mapping::export_triples(core, graph_name)
        .map_err(|error| format!("ShaclValidate: export live graph: {error}"))?;
    let mut graph = eg_shacl::Graph::new();
    for triple in &triples {
        graph.insert(triple);
    }
    Ok(graph)
}

/// The `ShaclValidate` wire body of an engine SHACL report.
#[cfg(feature = "shacl")]
fn shacl_report_wire(
    report: eg_shacl::ValidationReport,
    schema_digests: Vec<String>,
    composed_digest: Option<String>,
) -> eg_types::rdf_report::ShaclValidationReport {
    use eg_types::rdf_report::{ShaclSeverity, ShaclValidationReport, ShaclValidationResult};
    let results = report
        .results
        .into_iter()
        .map(|result| ShaclValidationResult {
            focus_node: result.focus_node,
            path: result.path,
            value: result.value,
            source_shape: result.source_shape,
            constraint_component: result.constraint_component,
            message: result.message,
            severity: match result.severity {
                eg_shacl::Severity::Violation => ShaclSeverity::Violation,
                eg_shacl::Severity::Warning => ShaclSeverity::Warning,
                eg_shacl::Severity::Info => ShaclSeverity::Info,
            },
        })
        .collect();
    ShaclValidationReport {
        schema_digests,
        composed_digest,
        conforms: report.conforms,
        results,
    }
}

/// Validate the request graph (or an inline `data_graph` Turtle document) against a
/// **ShExJ** `schema` for a `shape_map` (`[node_iri, shape_label]` pairs) (CONCEPT:EG-KG.compute.concept-2),
/// returning a `Json` `ShexReport`. Read-only: an empty `data_graph` exports the LIVE
/// graph's RDF (the same triples `GetRdf` serializes) and validates that.
#[cfg(feature = "shex")]
pub(super) async fn handle_shex_validate(
    req_id: u64,
    graph_name: &str,
    core: &Arc<GraphCore>,
    schema: String,
    data_graph: String,
    shape_map: Vec<[String; 2]>,
) -> Response {
    let schema = match eg_shex::Schema::from_shexj(&schema) {
        Ok(s) => s,
        Err(e) => return Response::err(req_id, format!("ShexValidate: bad schema: {e}")),
    };
    // Data graph: an inline Turtle document, else the live graph's exported RDF.
    let data = if data_graph.trim().is_empty() {
        let exported = eg_rdf::mapping::export_triples(core, graph_name);
        match exported {
            Ok(triples) => {
                let mut g = eg_shex::Graph::new();
                for t in &triples {
                    g.insert(t);
                }
                g
            }
            Err(e) => {
                return Response::err(req_id, format!("ShexValidate: export live graph: {e}"))
            }
        }
    } else {
        match eg_shex::graph_from_turtle(&data_graph) {
            Ok(g) => g,
            Err(e) => return Response::err(req_id, format!("ShexValidate: bad data graph: {e}")),
        }
    };
    let pairs: Vec<(&str, &str)> = shape_map
        .iter()
        .map(|p| (p[0].as_str(), p[1].as_str()))
        .collect();
    let map = eg_shex::ShapeMap::from_iri_pairs(&pairs);
    let report = eg_shex::validate(&schema, &data, &map);
    Response::ok(
        req_id,
        ResultPayload::of::<eg_types::result_contract::reasoning::ShexValidate>(shex_report_wire(
            report,
        )),
    )
}

/// The `ShexValidate` wire body of an engine ShEx report.
#[cfg(feature = "shex")]
fn shex_report_wire(report: eg_shex::ShexReport) -> eg_types::rdf_report::ShexValidationReport {
    use eg_types::rdf_report::{ShexNodeResult, ShexValidationReport};
    let results = report
        .results
        .into_iter()
        .map(|result| ShexNodeResult {
            node: result.node,
            shape: result.shape,
            conforms: result.conforms,
            reason: result.reason,
        })
        .collect();
    ShexValidationReport {
        conforms: report.conforms,
        results,
    }
}

#[cfg(all(test, feature = "shacl"))]
mod graph_schema_shapes_tests {
    use std::sync::Arc;

    use super::*;
    use crate::graph::{GraphSchemaSource, SchemaSourceOrigin};

    #[test]
    fn omitted_and_empty_shapes_use_the_same_composed_graph_schema() {
        let core = GraphCore::new();
        let omitted = load_shacl_shapes(&core, None).unwrap();
        let empty = load_shacl_shapes(&core, Some(" \n\t")).unwrap();

        assert!(!omitted.graph.is_empty());
        assert_eq!(omitted.graph.len(), empty.graph.len());
        assert!(omitted
            .graph
            .iter()
            .all(|triple| empty.graph.contains(triple)));
        assert_eq!(omitted.schema_digests, empty.schema_digests);
        assert_eq!(omitted.composed_digest, empty.composed_digest);
    }

    #[test]
    fn explicit_shapes_remain_an_ad_hoc_validation_graph() {
        let core = GraphCore::new();
        let explicit = load_shacl_shapes(
            &core,
            Some(
                "@prefix sh: <http://www.w3.org/ns/shacl#> .\n\
                 @prefix ex: <http://example/> .\n\
                 ex:Only a sh:NodeShape ; sh:targetClass ex:Thing .\n",
            ),
        )
        .unwrap();

        assert_eq!(explicit.graph.len(), 2);
        assert!(explicit.schema_digests.is_empty());
        assert_eq!(explicit.composed_digest, None);
    }

    #[test]
    fn validation_receipt_pins_the_snapshot_even_when_live_schema_changes() {
        let live = GraphCore::new();
        let snapshot = live.fork();
        let loaded = load_shacl_shapes(&snapshot, None).unwrap();
        let used_digest = loaded.composed_digest.clone().unwrap();

        let mut updated = (*live.schema_sources()).clone();
        updated
            .attach_dynamic(
                "admin:race".to_string(),
                GraphSchemaSource::new(
                    SchemaSourceOrigin::Admin {
                        name: "race".to_string(),
                    },
                    Some(Arc::from(
                        "@prefix sh: <http://www.w3.org/ns/shacl#> .\n\
                         @prefix ex: <http://example/> .\n\
                         ex:Race a sh:NodeShape ; sh:targetClass ex:RaceTarget .\n",
                    )),
                    None,
                    0,
                )
                .unwrap(),
            )
            .unwrap();
        live.install_schema_sources(Arc::new(updated));
        let later_digest = live.schema_sources().composed_digest().to_hex();

        let report = eg_shacl::validate(&loaded.graph, &eg_shacl::Graph::new()).unwrap();
        let receipt = shacl_report_wire(report, loaded.schema_digests, loaded.composed_digest);
        assert_ne!(used_digest, later_digest);
        assert_eq!(
            receipt.composed_digest.as_deref(),
            Some(used_digest.as_str())
        );
        assert!(!receipt.schema_digests.is_empty());
        assert!(receipt
            .schema_digests
            .iter()
            .all(|digest| digest.len() == 64));
    }
}
