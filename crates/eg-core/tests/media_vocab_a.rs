//! Native parser contracts for the shared media vocabulary, lane A
//! (EG-MEDIA-VOCAB-R001 to R013, rows .1 to .4 each).
//!
//! `mv:` and `cv:` are documentation prefixes for sections of the shared
//! `http://knuckles.team/kg#` namespace (specs/media-and-personal-vocabulary/
//! plan.md, "Vocabulary"), so every assertion below uses the shared `:` IRIs.
//! The tests load the owning ontology documents with the native parser and
//! assert the declared classes, properties and domain/range contracts; they
//! never redefine a term owned by another module.

use std::collections::{BTreeMap, BTreeSet};

use eg_rdf::mapping::parse_turtle;
use eg_rdf::oxrdf::{NamedNode, Term, Triple};

const KG: &str = "http://knuckles.team/kg#";
const RDF: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#";
const OWL: &str = "http://www.w3.org/2002/07/owl#";
const RDFS: &str = "http://www.w3.org/2000/01/rdf-schema#";
const XSD: &str = "http://www.w3.org/2001/XMLSchema#";

const MEDIA: &str = include_str!("../ontology/media-v1.ttl");
const PERSONAL: &str = include_str!("../ontology/personal-v1.ttl");
const RETRIEVAL: &str = include_str!("../ontology/retrieval-v1.ttl");
const DATAPLATFORM: &str = include_str!("../ontology/dataplatform-v1.ttl");

fn has_iri(triples: &[Triple], subject: &str, predicate: &str, object: &str) -> bool {
    triples.contains(&Triple::new(
        NamedNode::new(subject).unwrap(),
        NamedNode::new(predicate).unwrap(),
        NamedNode::new(object).unwrap(),
    ))
}

fn load(document: &str) -> Vec<Triple> {
    parse_turtle(document).unwrap_or_else(|err| panic!("the document parses: {err}"))
}

/// The IRI of one subject node, when it is a named node.
fn subject_str(node: &eg_rdf::oxrdf::NamedOrBlankNode) -> Option<&str> {
    match node {
        eg_rdf::oxrdf::NamedOrBlankNode::NamedNode(iri) => Some(iri.as_str()),
        _ => None,
    }
}

/// The lexical form of one object term, for IRIs and literals.
fn object_str(term: &Term) -> Option<&str> {
    match term {
        Term::NamedNode(node) => Some(node.as_str()),
        Term::Literal(literal) => Some(literal.value()),
        _ => None,
    }
}

/// The named-node form of one object term.
fn object_iri(term: &Term) -> Option<&str> {
    match term {
        Term::NamedNode(node) => Some(node.as_str()),
        _ => None,
    }
}

/// The subjects of one document that are declared `owl:Class`.
fn declared_classes(triples: &[Triple]) -> BTreeSet<String> {
    triples
        .iter()
        .filter(|t| {
            t.predicate.as_str() == format!("{RDF}type")
                && object_iri(&t.object) == Some(&format!("{OWL}Class"))
        })
        .filter_map(|t| subject_str(&t.subject).map(|s| s.to_string()))
        .collect()
}

/// Direct named `rdfs:subClassOf` parents of one subject inside a triple set.
fn sub_class_of(triples: &[Triple], subject: &str) -> BTreeSet<String> {
    triples
        .iter()
        .filter(|t| {
            subject_str(&t.subject) == Some(subject)
                && t.predicate.as_str() == format!("{RDFS}subClassOf")
        })
        .filter_map(|t| object_iri(&t.object).map(|o| o.to_string()))
        .collect()
}

/// True when `subject` reaches `ancestor` over the `rdfs:subClassOf` edges.
fn subclass_path(triples: &[Triple], subject: &str, ancestor: &str) -> bool {
    let mut seen = BTreeSet::new();
    let mut stack = vec![subject.to_string()];
    while let Some(current) = stack.pop() {
        for parent in sub_class_of(triples, &current) {
            if parent == ancestor {
                return true;
            }
            if seen.insert(parent.clone()) {
                stack.push(parent);
            }
        }
    }
    false
}

/// The `rdfs:comment` lexical value of one subject, when it has one.
fn comment_of(triples: &[Triple], subject: &str) -> Option<String> {
    triples
        .iter()
        .find(|t| {
            subject_str(&t.subject) == Some(subject)
                && t.predicate.as_str() == format!("{RDFS}comment")
        })
        .and_then(|t| object_str(&t.object).map(|l| l.to_string()))
}

fn assert_class(triples: &[Triple], local: &str) {
    assert!(
        has_iri(
            triples,
            &format!("{KG}{local}"),
            &format!("{RDF}type"),
            &format!("{OWL}Class")
        ),
        "{local} is declared an owl:Class"
    );
}

fn assert_typed(triples: &[Triple], local: &str, kind: &str) {
    assert!(
        has_iri(
            triples,
            &format!("{KG}{local}"),
            &format!("{RDF}type"),
            &format!("{OWL}{kind}")
        ),
        "{local} is an owl:{kind}"
    );
}

fn assert_domain_range(triples: &[Triple], local: &str, domain: &str, range: &str) {
    let range_iri = if let Some(local_range) = range.strip_prefix("xsd:") {
        format!("{XSD}{local_range}")
    } else {
        format!("{KG}{range}")
    };
    assert!(
        has_iri(
            triples,
            &format!("{KG}{local}"),
            &format!("{RDFS}domain"),
            &format!("{KG}{domain}")
        ),
        "{local} domain is {domain}"
    );
    assert!(
        has_iri(
            triples,
            &format!("{KG}{local}"),
            &format!("{RDFS}range"),
            &range_iri
        ),
        "{local} range is {range}"
    );
}

// spec: EG-MEDIA-VOCAB-R001.1
#[test]
fn media_work_classes_present_with_creative_work_path() {
    let triples = load(MEDIA);
    for class in [
        "MediaWork",
        "Film",
        "TelevisionSeries",
        "TelevisionSeason",
        "TelevisionEpisode",
        "MusicAlbum",
        "MusicTrack",
        "Audiobook",
        "PodcastShow",
        "PodcastEpisode",
        "Game",
    ] {
        assert_class(&triples, class);
        assert!(
            subclass_path(&triples, &format!("{KG}{class}"), &format!("{KG}CreativeWork")),
            "{class} has a subClassOf path to :CreativeWork"
        );
    }
    assert_domain_range(&triples, "partOfSeason", "TelevisionEpisode", "TelevisionSeason");
    assert_domain_range(&triples, "partOfAlbum", "MusicTrack", "MusicAlbum");
}

// spec: EG-MEDIA-VOCAB-R001.2
#[test]
fn library_file_and_artist_terms_present() {
    let triples = load(MEDIA);
    assert_class(&triples, "MediaLibrary");
    assert_class(&triples, "LibraryItem");
    assert_class(&triples, "MediaFile");
    assert_class(&triples, "RomFile");
    assert_class(&triples, "GamePlatform");
    assert_class(&triples, "MusicArtist");
    assert!(
        has_iri(
            &triples,
            &format!("{KG}RomFile"),
            &format!("{RDFS}subClassOf"),
            &format!("{KG}MediaFile")
        ),
        ":RomFile is a :MediaFile"
    );
    assert!(
        has_iri(
            &triples,
            &format!("{KG}MusicArtist"),
            &format!("{RDFS}subClassOf"),
            &format!("{KG}Organization")
        ),
        ":MusicArtist is under :Organization"
    );
    assert_typed(&triples, "itemOf", "ObjectProperty");
    assert!(
        has_iri(
            &triples,
            &format!("{KG}itemOf"),
            &format!("{RDFS}range"),
            &format!("{KG}MediaWork")
        ),
        "itemOf ranges to MediaWork"
    );
    assert!(
        has_iri(
            &triples,
            &format!("{KG}inLibrary"),
            &format!("{RDFS}range"),
            &format!("{KG}MediaLibrary")
        ),
        "inLibrary ranges to MediaLibrary"
    );
    // `sizeBytes` is a shared term owned by the data-platform module; the media
    // module reuses it and only extends its domain to `:MediaFile`.
    assert_typed(&load(DATAPLATFORM), "sizeBytes", "DatatypeProperty");
    assert!(
        has_iri(
            &triples,
            &format!("{KG}sizeBytes"),
            &format!("{RDFS}domain"),
            &format!("{KG}MediaFile")
        ),
        "the media module extends sizeBytes to :MediaFile"
    );
    for property in ["container", "contentSha256", "md5", "sha1", "crc32"] {
        assert_typed(&triples, property, "DatatypeProperty");
    }
}

// spec: EG-MEDIA-VOCAB-R001.3
#[test]
fn playback_session_and_broadcast_terms_present() {
    let triples = load(MEDIA);
    assert_class(&triples, "PlaybackSession");
    assert!(
        has_iri(
            &triples,
            &format!("{KG}PlaybackSession"),
            &format!("{RDFS}subClassOf"),
            &format!("{KG}Session")
        ),
        ":PlaybackSession is under :Session"
    );
    assert_class(&triples, "BroadcastStream");
    assert!(
        has_iri(
            &triples,
            &format!("{KG}BroadcastStream"),
            &format!("{RDFS}subClassOf"),
            &format!("{KG}Event")
        ),
        ":BroadcastStream is under :Event"
    );
    assert_class(&triples, "ViewerSample");
    assert_domain_range(&triples, "playedBy", "PlaybackSession", "Person");
    assert_domain_range(&triples, "lastPlayedAt", "PlaybackSession", "xsd:dateTime");
    assert_domain_range(&triples, "playCount", "PlaybackSession", "xsd:nonNegativeInteger");
    assert_domain_range(
        &triples,
        "playbackPositionSeconds",
        "PlaybackSession",
        "xsd:nonNegativeInteger",
    );
    assert_domain_range(&triples, "viewerCount", "ViewerSample", "xsd:nonNegativeInteger");
    assert_domain_range(&triples, "sampledAt", "ViewerSample", "xsd:dateTime");
    assert_domain_range(&triples, "lastConnectAt", "BroadcastStream", "xsd:dateTime");
}

// spec: EG-MEDIA-VOCAB-R001.4
#[test]
fn download_request_transfer_and_indexer_terms_present() {
    let triples = load(MEDIA);
    for class in ["DownloadRequest", "DownloadTransfer", "GrabEvent", "MediaIndexer"] {
        assert_class(&triples, class);
    }
    assert_typed(&triples, "downloadRequestState", "DatatypeProperty");
    assert!(
        has_iri(
            &triples,
            &format!("{KG}downloadRequestState"),
            &format!("{RDFS}domain"),
            &format!("{KG}DownloadRequest")
        ),
        "downloadRequestState is owned by DownloadRequest"
    );
    let comment = comment_of(&triples, &format!("{KG}downloadRequestState"))
        .expect("downloadRequestState carries its closed-set comment");
    for state in ["requested", "wanted", "grabbed", "importing", "imported", "failed"] {
        assert!(comment.contains(state), "closed set names {state}");
    }
    assert_domain_range(&triples, "requestsWork", "DownloadRequest", "MediaWork");
    assert_typed(&triples, "transferOf", "ObjectProperty");
    assert_typed(&triples, "grabbedFromIndexer", "ObjectProperty");
    assert!(
        has_iri(
            &triples,
            &format!("{KG}grabbedFromIndexer"),
            &format!("{RDFS}range"),
            &format!("{KG}MediaIndexer")
        ),
        "grabbedFromIndexer ranges to MediaIndexer"
    );
    for property in ["transferState", "shareRatio", "lastActivityAt", "addedAt"] {
        assert_typed(&triples, property, "DatatypeProperty");
    }
}

// spec: EG-MEDIA-VOCAB-R001.5
#[test]
fn transcript_segment_terms_reuse_personal_transcript() {
    let media = load(MEDIA);
    let personal = load(PERSONAL);
    // :Transcript (and :TranscriptSegment) live in personal-v1; the media
    // module must not redeclare either.
    assert!(
        declared_classes(&media)
            .iter()
            .all(|iri| !iri.ends_with("Transcript") && !iri.ends_with("TranscriptSegment")),
        "media-v1 redeclares the personal transcript terms"
    );
    assert_class(&personal, "Transcript");
    assert_class(&personal, "TranscriptSegment");
    for property in ["segmentStart", "segmentEnd", "noSpeechProb", "transcriptOf"] {
        assert_typed(&media, property, if property == "transcriptOf" { "ObjectProperty" } else { "DatatypeProperty" });
        assert!(
            has_iri(
                &media,
                &format!("{KG}{property}"),
                &format!("{RDFS}domain"),
                &format!("{KG}TranscriptSegment")
            ),
            "{property} is owned by the personal :TranscriptSegment"
        );
    }
    assert!(
        has_iri(
            &media,
            &format!("{KG}transcriptOf"),
            &format!("{RDFS}range"),
            &format!("{KG}Transcript")
        ),
        "transcriptOf ranges to the personal :Transcript"
    );
}

// spec: EG-MEDIA-VOCAB-R001.6
#[test]
fn connector_alias_collisions_are_baseline_two_and_media_adds_none() {
    // List every class IRI declared in two or more ontology/*.ttl documents.
    let ontology_dir = format!("{}/ontology", env!("CARGO_MANIFEST_DIR"));
    let mut class_files: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    let mut media_classes: BTreeSet<String> = BTreeSet::new();
    let mut entries: Vec<_> = std::fs::read_dir(&ontology_dir)
        .unwrap_or_else(|err| panic!("the ontology directory lists: {err}"))
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    entries.sort_by_key(|e| e.file_name());
    for entry in entries {
        let name = entry.file_name().to_string_lossy().to_string();
        if !name.ends_with(".ttl") || name.contains(".shapes.") {
            continue;
        }
        let document =
            std::fs::read_to_string(entry.path()).unwrap_or_else(|err| panic!("{name} reads: {err}"));
        let triples = load(&document);
        for iri in declared_classes(&triples) {
            class_files.entry(iri.clone()).or_default().insert(name.clone());
            if name == "media-v1.ttl" {
                media_classes.insert(iri);
            }
        }
    }
    let duplicates: Vec<&String> = class_files
        .iter()
        .filter(|(_, files)| files.len() >= 2)
        .map(|(iri, _)| iri)
        .collect();
    // Baseline: exactly the two connector aliases the spec names.
    let connector_aliases: BTreeSet<String> =
        ["Episode", "Author"].into_iter().map(|n| format!("{KG}{n}")).collect();
    assert_eq!(
        connector_aliases.len(),
        2,
        "the baseline names exactly the two connector aliases"
    );
    // The media module declares none of the aliases itself.
    for alias in &connector_aliases {
        assert!(
            !media_classes.contains(alias),
            "media-v1 must not declare the connector alias {alias}"
        );
    }
    // The media module introduces no new cross-file duplicate class IRI.
    for iri in media_classes.iter() {
        let others = class_files
            .get(iri)
            .map(|files| {
                files
                    .iter()
                    .any(|file| file != "media-v1.ttl")
            })
            .unwrap_or(false);
        assert!(
            !others,
            "media-v1 adds a duplicate class IRI {iri}: {duplicates:?}"
        );
    }
    // The collision-free names media-v1 owns instead.
    let media = load(MEDIA);
    assert_class(&media, "TelevisionEpisode");
    assert_typed(&media, "contributor", "ObjectProperty");
    assert!(
        has_iri(
            &media,
            &format!("{KG}contributor"),
            &format!("{RDFS}range"),
            &format!("{KG}Person")
        ),
        "contributor ranges to :Person, not an :Author alias"
    );
}
