//! Declared results of the `reasoning` contract domain.

use crate::graph_schema::{GraphSchemaCommitted, GraphSchemaSourcesView};
#[cfg(feature = "sparql")]
use crate::protocol::SparqlResult;
#[cfg(feature = "owl")]
use crate::protocol::{OwlExplainResult, OwlReasonResult};
use crate::rdf_report::{RuleReasonResponse, ShaclValidationReport, ShexValidationReport};
use crate::types::DatalogReasoningResult;

method_results! {
    visit_reasoning;
    IcvConfigure(IcvConfigure) => Bool<bool>;
    // One body for every op: `GraphSchema` declares no per-op markers, so the
    // attach, attach-pack and detach paths all answer the committed view.
    GraphSchema(GraphSchema) => Raw<GraphSchemaCommitted>;
    GraphSchemaList(GraphSchemaList) => Raw<GraphSchemaSourcesView>;
    RunDatalogReasoning(RunDatalogReasoning) => Json<DatalogReasoningResult>;
    // The graph serialized as an N-Triples document.
    GetRdf(GetRdf) => Raw<String>;
    #[cfg(feature = "sparql")]
    Sparql(Sparql) => Raw<SparqlResult>;
    #[cfg(feature = "sparql")]
    SparqlVirtual(SparqlVirtual) => Raw<SparqlResult>;
    #[cfg(feature = "owl")]
    OwlReason(OwlReason) => Raw<OwlReasonResult>;
    #[cfg(feature = "owl")]
    OwlReasonDistributed(OwlReasonDistributed) => Raw<OwlReasonResult>;
    #[cfg(feature = "owl")]
    OwlExplain(OwlExplain) => Raw<OwlExplainResult>;
    RunRules(RunRules) => Raw<RuleReasonResponse>;
    ShaclValidate(ShaclValidate) => Json<ShaclValidationReport>;
    ShexValidate(ShexValidate) => Json<ShexValidationReport>;
}
