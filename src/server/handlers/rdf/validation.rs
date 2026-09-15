use std::sync::Arc;

use crate::graph::GraphCore;
use crate::protocol::{Response, ResultPayload};

/// Validate the request graph (or an inline `data_graph` Turtle document) against a
/// SHACL `shapes` Turtle document (CONCEPT:EG-KG.ontology.concept-6), returning a `Json`
/// `sh:ValidationReport`. Read-only: an empty `data_graph` exports the LIVE graph's RDF
/// (the same triples `GetRdf` serializes) and validates that.
#[cfg(feature = "shacl")]
pub(super) async fn handle_shacl_validate(
    req_id: u64,
    graph_name: &str,
    core: &Arc<GraphCore>,
    shapes: String,
    data_graph: String,
) -> Response {
    let shapes_graph = match parse_shacl_shapes(&shapes) {
        Ok(graph) => graph,
        Err(error) => return Response::err(req_id, error),
    };
    let data = match load_shacl_data(graph_name, core, &data_graph) {
        Ok(graph) => graph,
        Err(error) => return Response::err(req_id, error),
    };
    let report = match eg_shacl::validate(&shapes_graph, &data) {
        Ok(report) => report,
        Err(error) => return Response::err(req_id, format!("ShaclValidate: {error}")),
    };
    Response::ok(
        req_id,
        ResultPayload::of::<eg_types::result_contract::reasoning::ShaclValidate>(
            shacl_report_wire(report),
        ),
    )
}

#[cfg(feature = "shacl")]
fn parse_shacl_shapes(shapes: &str) -> Result<eg_shacl::Graph, String> {
    eg_shacl::graph_from_turtle(shapes)
        .map_err(|error| format!("ShaclValidate: bad shapes graph: {error}"))
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
