//! Native parser contract for the media and content vocabulary added by
//! media vocab lane B (second half of the media-and-personal-vocabulary spec).
//!
//! One test per delivered row: the classes and properties each connector's
//! mapping table (specs/media-and-personal-vocabulary/plan.md, Connector
//! mapping tables) names, asserted by type, subClassOf path and domain/range.
//! Reused terms are asserted where their owning module declares them
//! (retrieval-v1 for the search and paper terms, core-foundation for the
//! shared parents).

use medulla_rdf::mapping::parse_turtle;
use medulla_rdf::oxrdf::{NamedNode, Triple};

const KG: &str = "http://knuckles.team/kg#";
const OWL: &str = "http://www.w3.org/2002/07/owl#";
const RDF: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
const RDFS: &str = "http://www.w3.org/2000/01/rdf-schema#";
const XSD: &str = "http://www.w3.org/2001/XMLSchema#";

fn kg(local: &str) -> String {
    format!("{KG}{local}")
}

fn has_iri(triples: &[Triple], subject: &str, predicate: &str, object: &str) -> bool {
    triples.contains(&Triple::new(
        NamedNode::new(subject).unwrap(),
        NamedNode::new(predicate).unwrap(),
        NamedNode::new(object).unwrap(),
    ))
}

fn assert_typed(triples: &[Triple], local: &str, kind: &str) {
    assert!(
        has_iri(
            triples,
            &kg(local),
            &format!("{RDF}type"),
            &format!("{OWL}{kind}")
        ),
        "{local} is an owl:{kind}"
    );
}

fn assert_subclass_of(triples: &[Triple], local: &str, parent: &str) {
    assert!(
        has_iri(
            triples,
            &kg(local),
            &format!("{RDFS}subClassOf"),
            &kg(parent)
        ),
        "{local} is a subclass of {parent}"
    );
}

fn assert_domain_range(triples: &[Triple], local: &str, domain: &str, range: &str) {
    assert!(
        has_iri(triples, &kg(local), &format!("{RDFS}domain"), &kg(domain)),
        "{local} domain is {domain}"
    );
    let range_iri = if let Some(data_type) = range.strip_prefix("xsd:") {
        format!("{XSD}{data_type}")
    } else {
        kg(range)
    };
    assert!(
        has_iri(triples, &kg(local), &format!("{RDFS}range"), &range_iri),
        "{local} range is {range}"
    );
}

/// The modules that own the terms these rows map through: media (the lane B
/// block), retrieval (search and paper terms) and core-foundation (the shared
/// parents), plus personal-v1 for :Tag.
fn load_vocabulary() -> Vec<Triple> {
    parse_turtle(include_str!("../ontology/media-v1.ttl"))
        .expect("the media vocabulary parses")
        .into_iter()
        .chain(
            parse_turtle(include_str!("../ontology/retrieval-v1.ttl"))
                .expect("the retrieval vocabulary parses"),
        )
        .chain(
            parse_turtle(include_str!("../ontology/core-foundation-v1.ttl"))
                .expect("the core foundation vocabulary parses"),
        )
        .chain(
            parse_turtle(include_str!("../ontology/personal-v1.ttl"))
                .expect("the personal vocabulary parses"),
        )
        .collect()
}

// spec: MEDULLA-MEDIA-VOCAB-R014.1
#[test]
fn scholarx_mapping_terms_present() {
    let triples = load_vocabulary();
    // The paper class reuses retrieval-v1's :ScholarlyPaper (a :Document).
    assert_typed(&triples, "ScholarlyPaper", "Class");
    assert_subclass_of(&triples, "ScholarlyPaper", "Document");
    assert_typed(&triples, "ResearchCategory", "Class");
    assert_subclass_of(&triples, "ResearchCategory", "Tag");
    assert_typed(&triples, "PaperSource", "Class");
    assert_subclass_of(&triples, "PaperSource", "SourceBackend");
    assert_domain_range(&triples, "doi", "ScholarlyPaper", "xsd:string");
    assert_domain_range(&triples, "arxivId", "ScholarlyPaper", "xsd:string");
    assert_domain_range(&triples, "publishedAt", "Document", "xsd:dateTime");
    assert_domain_range(&triples, "citationCount", "ScholarlyPaper", "xsd:integer");
}

// spec: MEDULLA-MEDIA-VOCAB-R015.1
#[test]
fn searxng_mapping_terms_present() {
    let triples = load_vocabulary();
    // Query, result and engine classes reuse retrieval-v1's terms.
    assert_typed(&triples, "SearchQuery", "Class");
    assert_typed(&triples, "SearchResult", "Class");
    assert_typed(&triples, "SearchEngine", "Class");
    assert_subclass_of(&triples, "SearchResult", "Document");
    // The result URL reuses retrieval-v1's :resourceUrl (:Document domain
    // covers search results).
    assert_domain_range(&triples, "resourceUrl", "Document", "xsd:string");
    assert_domain_range(&triples, "rank", "SearchResult", "xsd:integer");
    assert_domain_range(&triples, "servedByEngine", "SearchResult", "SearchEngine");
}

// spec: MEDULLA-MEDIA-VOCAB-R017.1
#[test]
fn postiz_mapping_terms_present() {
    let triples = load_vocabulary();
    assert_typed(&triples, "SocialPost", "Class");
    assert_subclass_of(&triples, "SocialPost", "Document");
    assert_typed(&triples, "PublishingChannel", "Class");
    assert_subclass_of(&triples, "PublishingChannel", "BroadcastChannel");
    assert_typed(&triples, "EngagementMetric", "Class");
    assert_subclass_of(&triples, "EngagementMetric", "Observation");
    assert_domain_range(&triples, "postState", "SocialPost", "xsd:string");
    assert_domain_range(&triples, "socialScheduledAt", "SocialPost", "xsd:dateTime");
    assert_domain_range(&triples, "platform", "PublishingChannel", "xsd:string");
}

// spec: MEDULLA-MEDIA-VOCAB-R018.1
#[test]
fn listmonk_mapping_terms_present() {
    let triples = load_vocabulary();
    assert_typed(&triples, "EmailCampaign", "Class");
    assert_subclass_of(&triples, "EmailCampaign", "Event");
    assert_typed(&triples, "MailingList", "Class");
    assert_subclass_of(&triples, "MailingList", "Collection");
    assert_typed(&triples, "Subscriber", "Class");
    assert_subclass_of(&triples, "Subscriber", "Person");
    assert_domain_range(&triples, "sentCount", "EmailCampaign", "xsd:integer");
    assert_domain_range(&triples, "viewCount", "EmailCampaign", "xsd:integer");
    assert_domain_range(&triples, "clickCount", "EmailCampaign", "xsd:integer");
    assert_domain_range(&triples, "campaignStatus", "EmailCampaign", "xsd:string");
    assert_domain_range(&triples, "subscriptionStatus", "Subscriber", "xsd:string");
    // Subscriber identity is the keyed hash; no raw-address property exists.
    assert_domain_range(&triples, "subscriberKey", "Subscriber", "xsd:string");
    for forbidden in ["emailAddress", "rawEmail", "address"] {
        assert!(
            !has_iri(
                &triples,
                &kg(forbidden),
                &format!("{RDF}type"),
                &format!("{OWL}DatatypeProperty")
            ),
            "{forbidden} must not be a property of subscribers"
        );
    }
}

// spec: MEDULLA-MEDIA-VOCAB-R019.1
#[test]
fn mattermost_mapping_terms_present() {
    let triples = load_vocabulary();
    // The team reuses core-foundation's :Team; only the channel and message
    // terms are new.
    assert_typed(&triples, "Team", "Class");
    assert_typed(&triples, "Person", "Class");
    assert_typed(&triples, "MessageChannel", "Class");
    assert_subclass_of(&triples, "MessageChannel", "Collection");
    assert_typed(&triples, "ChatMessage", "Class");
    assert_subclass_of(&triples, "ChatMessage", "Document");
    assert_domain_range(&triples, "memberCount", "MessageChannel", "xsd:integer");
    assert_domain_range(&triples, "lastPostAt", "MessageChannel", "xsd:dateTime");
    assert_domain_range(&triples, "postedAt", "ChatMessage", "xsd:dateTime");
    assert_domain_range(&triples, "postedBy", "ChatMessage", "Person");
}
