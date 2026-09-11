//! SPARQL result media negotiation and RDF graph serialization.

/// Candidate SELECT/ASK output media types, DEFAULT (SPARQL-results JSON) first.
pub(super) const SELECT_FORMS: &[&str] = &[
    "application/sparql-results+json",
    "application/sparql-results+xml",
    "text/csv",
    "text/tab-separated-values",
];

/// Candidate CONSTRUCT/DESCRIBE output media types, DEFAULT (N-Triples) first.
#[cfg(feature = "rdf-xml")]
pub(super) const GRAPH_FORMS: &[&str] = &[
    "application/n-triples",
    "text/turtle",
    "application/n-quads",
    "application/trig",
    "application/ld+json",
    "application/rdf+xml",
];

#[cfg(not(feature = "rdf-xml"))]
pub(super) const GRAPH_FORMS: &[&str] = &[
    "application/n-triples",
    "text/turtle",
    "application/n-quads",
    "application/trig",
    "application/ld+json",
];

pub(super) fn serialize_graph(
    ct: &str,
    triples: &[eg_rdf::oxrdf::Triple],
) -> Result<String, String> {
    match ct {
        "text/turtle" => eg_rdf::mapping::to_turtle(triples),
        "application/n-quads" => eg_rdf::mapping::to_nquads(triples, None),
        "application/trig" => eg_rdf::mapping::to_trig(triples, None),
        "application/ld+json" => eg_rdf::jsonld::to_jsonld(triples, None, None),
        #[cfg(feature = "rdf-xml")]
        "application/rdf+xml" => eg_rdf::mapping::to_rdfxml(triples),
        _ => eg_rdf::mapping::to_ntriples(triples),
    }
}

/// Resolve an output/format override, constrained to the current form's candidates.
pub(super) fn choose_ct(
    accept: &str,
    fmt_override: Option<&str>,
    forms: &[&'static str],
) -> &'static str {
    if let Some(tok) = fmt_override {
        if let Some(ct) = override_ct(tok, forms) {
            return ct;
        }
    }
    negotiate(accept, forms)
}

/// Map a short output token (or a full media type) to a supported candidate.
fn override_ct(token: &str, forms: &[&'static str]) -> Option<&'static str> {
    const SHORT_FORMS: &[(&str, &str)] = &[
        ("json", "application/sparql-results+json"),
        ("srj", "application/sparql-results+json"),
        ("xml", "application/sparql-results+xml"),
        ("srx", "application/sparql-results+xml"),
        ("csv", "text/csv"),
        ("tsv", "text/tab-separated-values"),
        ("nt", "application/n-triples"),
        ("ntriples", "application/n-triples"),
        ("n-triples", "application/n-triples"),
        ("ttl", "text/turtle"),
        ("turtle", "text/turtle"),
        ("nq", "application/n-quads"),
        ("nquads", "application/n-quads"),
        ("n-quads", "application/n-quads"),
        ("trig", "application/trig"),
        ("jsonld", "application/ld+json"),
        ("json-ld", "application/ld+json"),
        ("ld+json", "application/ld+json"),
        ("rdfxml", "application/rdf+xml"),
        ("rdf+xml", "application/rdf+xml"),
        ("rdf/xml", "application/rdf+xml"),
    ];
    let t = token.trim().to_ascii_lowercase();
    let want = SHORT_FORMS
        .iter()
        .find_map(|(short, media)| (*short == t.as_str()).then_some(*media))
        .unwrap_or(t.as_str());
    forms.iter().copied().find(|&f| f == want)
}

/// Pick the best media type among `forms`, honoring q-values and wildcards.
pub(super) fn negotiate(accept: &str, forms: &[&'static str]) -> &'static str {
    let accept = accept.trim();
    if accept.is_empty() {
        return forms[0];
    }
    let mut best: Option<(&'static str, f32)> = None;
    for part in accept.split(',') {
        if let Some((form, quality)) = accepted_part(part, forms) {
            if best.map(|(_, bq)| quality > bq).unwrap_or(true) {
                best = Some((form, quality));
            }
        }
    }
    best.map(|(f, _)| f).unwrap_or(forms[0])
}

fn accepted_part(part: &str, forms: &[&'static str]) -> Option<(&'static str, f32)> {
    let mut segs = part.split(';');
    let media = segs.next().unwrap_or("").trim().to_ascii_lowercase();
    let mut quality = 1.0f32;
    for seg in segs {
        if let Some(value) = seg.trim().strip_prefix("q=") {
            quality = value.parse().unwrap_or(1.0);
        }
    }
    if quality <= 0.0 {
        return None;
    }
    forms
        .iter()
        .copied()
        .find(|form| media_matches(&media, form))
        .map(|form| (form, quality))
}

fn media_matches(media: &str, form: &str) -> bool {
    media == form
        || media == "*/*"
        || (media.ends_with("/*") && form.starts_with(&media[..media.len() - 1]))
}
