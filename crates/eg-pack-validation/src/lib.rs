//! Shared connector-pack ontology and SHACL orchestration, without a server.
//!
//! Uses the engine's RDF parser, class/ABox checker and SHACL evaluator. Input
//! allocation guards are enforced before parsing. This is NOT yet a fail-closed
//! offline profile: SHACL supported-construct and evaluation-work hardening is
//! still required before a versioned CLI can advertise a complete verdict.

use eg_types::connector_pack::PackViolationCode;

mod limits;
pub use limits::{MAX_DOCUMENTS, MAX_DOCUMENT_BYTES, MAX_INPUT_BYTES, MAX_RENDERED_BYTES};

/// The existing engine budget for each class/ABox reasoning phase.
pub const REASONING_STEPS: u64 = 10_000_000;
/// The existing SHACL graph-size product admission bound (not an evaluator meter).
pub const SHACL_STEPS: usize = 10_000_000;
/// Existing engine refusal code and bounded static diagnostic.
pub type Refusal = (PackViolationCode, &'static str);

/// Validate ontology and shape unions using the same orchestration as admission.
/// Documents must already have passed identity/import/base and term validation.
/// This call does not admit, attach, authorize or fetch any source.
pub fn validate_unions(
    ontologies: &[String],
    shapes: &[String],
) -> Result<(), (PackViolationCode, &'static str)> {
    limits::preflight(ontologies, shapes)?;
    if ontologies.is_empty() && shapes.is_empty() {
        return Ok(());
    }
    let ontology = parse_union(
        ontologies,
        PackViolationCode::OntologyInvalid,
        "an ontology file is not valid Turtle",
    )?;
    let triples = ontology.triples;
    // G14 (EH-119, EH-355 ruling (c)): the EL+/RL classification and then the
    // full-ABox tableau, each inside one deterministic step budget -- the same
    // verdict on every host, and the same budget the schema attach uses.
    match eg_rdf::tableau::check_pack_ontology(&triples, REASONING_STEPS) {
        Ok(()) => {}
        Err(eg_rdf::tableau::BoundedCheckRefusal::Inconsistent { .. }) => {
            return Err((
                PackViolationCode::OntologyInconsistent,
                "ontology union is inconsistent: an unsatisfiable class or individual",
            ));
        }
        Err(eg_rdf::tableau::BoundedCheckRefusal::BudgetExceeded { .. }) => {
            return Err((
                PackViolationCode::ValidationBudgetExceeded,
                "ontology reasoning exceeds the deterministic step budget",
            ));
        }
    }
    if shapes.is_empty() {
        return Ok(());
    }
    validate_shapes_union(shapes, &ontology.ntriples, triples.len())
}

/// G15/G16 over the file-scoped shapes union, against the ontology union.
fn validate_shapes_union(
    shapes: &[String],
    ontology: &str,
    ontology_triples: usize,
) -> Result<(), (PackViolationCode, &'static str)> {
    let shape_union = parse_union(
        shapes,
        PackViolationCode::ShapesInvalid,
        "a shapes file is not valid Turtle",
    )?;
    let shape_triples = shape_union.triples;
    let shape_graph = shape_union.ntriples;
    // The ICV parser represents unsupported paths but ignores them during
    // evaluation. A pack must refuse one rather than silently drop a declared
    // constraint (G15): this build supports only predicate paths.
    const SH_PATH: &str = "http://www.w3.org/ns/shacl#path";
    if shape_triples.iter().any(|triple| {
        triple.predicate.as_str() == SH_PATH
            && !matches!(&triple.object, eg_rdf::oxrdf::Term::NamedNode(_))
    }) {
        return Err((
            PackViolationCode::ShapesInvalid,
            "SHACL property paths must be predicate IRIs",
        ));
    }
    if shape_triples.len().saturating_mul(ontology_triples.max(1)) > SHACL_STEPS {
        return Err((
            PackViolationCode::ValidationBudgetExceeded,
            "SHACL validation exceeds the deterministic evaluation budget",
        ));
    }
    if shapes
        .iter()
        .any(|document| document.to_ascii_uppercase().contains("SERVICE"))
    {
        return Err((
            PackViolationCode::ShapesInvalid,
            "SHACL SPARQL SERVICE constraints are forbidden",
        ));
    }
    eg_shacl::IcvPolicy::from_turtle(&shape_graph).map_err(|_| {
        (
            PackViolationCode::ShapesInvalid,
            "shapes union is not a supported ICV policy",
        )
    })?;
    let report = eg_shacl::validate_icv_turtle(&shape_graph, ontology).map_err(|_| {
        (
            PackViolationCode::ShapesInvalid,
            "SHACL validation could not evaluate the shapes union",
        )
    })?;
    if !report.conforms {
        return Err((
            PackViolationCode::ShaclViolation,
            "ontology union does not conform to the shapes union",
        ));
    }
    Ok(())
}

fn parse_union(
    documents: &[String],
    invalid: PackViolationCode,
    detail: &'static str,
) -> Result<eg_rdf::pack::RdfUnion, Refusal> {
    use eg_rdf::pack::UnionLimitError;
    eg_rdf::pack::scoped_union_with_limits(
        documents,
        eg_rdf::pack::MAX_RDF_TRIPLES,
        MAX_RENDERED_BYTES,
    )
    .map_err(|error| match error {
        UnionLimitError::Parse { .. } => (invalid, detail),
        UnionLimitError::Triples { .. } => (
            PackViolationCode::ValidationBudgetExceeded,
            if invalid == PackViolationCode::OntologyInvalid {
                "ontology union exceeds the triple validation budget"
            } else {
                "shapes union exceeds the triple validation budget"
            },
        ),
        UnionLimitError::RenderedBytes { .. } => (
            PackViolationCode::ValidationBudgetExceeded,
            "RDF union rendering exceeds the byte validation budget",
        ),
    })
}

#[cfg(test)]
mod tests;
