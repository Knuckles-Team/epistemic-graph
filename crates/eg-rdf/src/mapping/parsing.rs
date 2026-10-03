use super::*;

/// Drain one oxrdf streaming parser into an owned vector, naming the syntax in every
/// error it reports.
///
/// The concrete-syntax readers differ only in their parser and that name; keeping the
/// drain here means a new syntax cannot report its errors in a different shape.
fn collect_parsed<T, E: std::fmt::Display>(
    syntax: &str,
    parsed: impl IntoIterator<Item = Result<T, E>>,
) -> Result<Vec<T>, String> {
    parsed
        .into_iter()
        .map(|item| item.map_err(|error| format!("{syntax} parse: {error}")))
        .collect()
}

/// Parse a Turtle document into oxrdf triples.
pub fn parse_turtle(doc: &str) -> Result<Vec<Triple>, String> {
    collect_parsed("turtle", TurtleParser::new().for_reader(doc.as_bytes()))
}

/// Parse Turtle while refusing to retain more than `max_triples` emitted triples.
/// The caller must enforce its document byte budget before calling this helper.
pub fn parse_turtle_bounded(doc: &str, max_triples: usize) -> Result<Vec<Triple>, String> {
    let mut triples = Vec::new();
    for (index, triple) in TurtleParser::new().for_reader(doc.as_bytes()).enumerate() {
        if index == max_triples {
            return Err(format!("turtle exceeds maximum of {max_triples} triples"));
        }
        triples.push(triple.map_err(|error| format!("turtle parse: {error}"))?);
    }
    Ok(triples)
}

/// Parse an N-Triples document into oxrdf triples.
pub fn parse_ntriples(doc: &str) -> Result<Vec<Triple>, String> {
    collect_parsed("ntriples", NTriplesParser::new().for_reader(doc.as_bytes()))
}

/// Parse an N-Quads document into oxrdf quads (subject/predicate/object + graph name).
pub fn parse_nquads(doc: &str) -> Result<Vec<Quad>, String> {
    collect_parsed("nquads", NQuadsParser::new().for_reader(doc.as_bytes()))
}

/// Parse a TriG document into oxrdf quads.
pub fn parse_trig(doc: &str) -> Result<Vec<Quad>, String> {
    collect_parsed("trig", TriGParser::new().for_reader(doc.as_bytes()))
}

/// Parse an RDF/XML document into oxrdf triples (feature `rdf-xml`).
#[cfg(feature = "rdf-xml")]
pub fn parse_rdfxml(doc: &str) -> Result<Vec<Triple>, String> {
    crate::rdfxml::parse(doc)
}

/// Parse a JSON-LD 1.1 document into oxrdf quads (feature `json-ld`).
#[cfg(feature = "json-ld")]
pub fn parse_jsonld(doc: &str) -> Result<Vec<Quad>, String> {
    use oxjsonld::JsonLdParser;
    collect_parsed("jsonld", JsonLdParser::new().for_slice(doc.as_bytes()))
}

// ── EG-137: named-graph-aware `from_*` reader surface ────────────────────────────
//
// CONCEPT:EG-KG.ontology.completes-rdf-concrete-syntax completes the RDF 1.1 concrete-syntax matrix — TriG + N-Quads (quad,
// named-graph-aware) and RDF/XML — alongside Turtle/N-Triples (EG-050). The writers
// (`to_trig`/`to_nquads`/`to_rdfxml`) sit above; these `from_*` readers give the matrix
// a first-class, uniformly-named reader surface (delegating to the same oxttl/quick-xml
// parsers). Named-graph awareness: `from_trig`/`from_nquads` yield `Quad`s carrying the
// per-statement graph term; RDF/XML is a single-graph syntax so `from_rdfxml` yields
// triples.

/// EG-137: parse N-Quads into quads (subject/predicate/object + graph term). The
/// canonically-named reader for the `application/n-quads` form.
pub fn from_nquads(doc: &str) -> Result<Vec<Quad>, String> {
    parse_nquads(doc)
}

/// EG-137: parse TriG (named-graph-aware Turtle) into quads. The canonically-named
/// reader for the `application/trig` form.
pub fn from_trig(doc: &str) -> Result<Vec<Quad>, String> {
    parse_trig(doc)
}

/// EG-137: parse RDF/XML into triples (a single-graph syntax). The canonically-named
/// reader for the `application/rdf+xml` form (feature `rdf-xml`).
#[cfg(feature = "rdf-xml")]
pub fn from_rdfxml(doc: &str) -> Result<Vec<Triple>, String> {
    parse_rdfxml(doc)
}

#[cfg(test)]
mod bounded_tests {
    use super::*;

    #[test]
    fn bounded_turtle_accepts_the_limit_and_preserves_syntax_errors() {
        let document = "<urn:s> <urn:p> <urn:o> .";
        assert_eq!(
            parse_turtle_bounded(document, 1).unwrap(),
            parse_turtle(document).unwrap()
        );
        assert!(parse_turtle_bounded("", 0).unwrap().is_empty());
        assert!(parse_turtle_bounded(document, 0)
            .unwrap_err()
            .contains("maximum of 0 triples"));
        assert!(parse_turtle_bounded("not Turtle .", 1)
            .unwrap_err()
            .starts_with("turtle parse:"));
    }

    #[test]
    fn bounded_turtle_refuses_before_draining_the_rest_of_the_document() {
        let document = "<urn:s> <urn:p> <urn:o> .\n<urn:s> <urn:p> <urn:o> .\nnot Turtle .";
        assert!(parse_turtle_bounded(document, 1)
            .unwrap_err()
            .contains("maximum of 1 triples"));
    }
}
