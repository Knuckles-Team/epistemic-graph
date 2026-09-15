//! Declared results of the `reasoning` contract domain.

#[cfg(feature = "sparql")]
use crate::protocol::SparqlResult;
#[cfg(feature = "owl")]
use crate::protocol::{OwlExplainResult, OwlReasonResult};
use crate::rdf_report::{RuleReasonResponse, ShaclValidationReport, ShexValidationReport};
use crate::types::DatalogReasoningResult;

method_results! {
    visit_reasoning;
    IcvConfigure(IcvConfigure) => Bool<bool>;
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
