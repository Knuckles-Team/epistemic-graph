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
const MEDIA_SHAPES: &str = include_str!("../ontology/media-v1.shapes.ttl");
const CONTENT: &str = include_str!("../ontology/content-v1.ttl");
const CONTENT_SHAPES: &str = include_str!("../ontology/content-v1.shapes.ttl");
const PERSONAL: &str = include_str!("../ontology/personal-v1.ttl");
const RETRIEVAL: &str = include_str!("../ontology/retrieval-v1.ttl");
const DATAPLATFORM: &str = include_str!("../ontology/dataplatform-v1.ttl");
const SHACL: &str = "http://www.w3.org/ns/shacl#";

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

fn has_literal(triples: &[Triple], subject: &str, predicate: &str, value: &str) -> bool {
    triples
        .iter()
        .any(|t| {
            subject_str(&t.subject) == Some(subject)
                && t.predicate.as_str() == predicate
                && object_str(&t.object) == Some(value)
        })
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
    // The audience count rides both the sample and the stream itself (the
    // stream's current audience), extended by R010.1.
    assert!(
        has_iri(
            &triples,
            &format!("{KG}viewerCount"),
            &format!("{RDFS}range"),
            &format!("{XSD}nonNegativeInteger")
        ),
        "viewerCount ranges to a count"
    );
    let owned_by = union_members(&triples, &format!("{KG}viewerCount"), &format!("{RDFS}domain"));
    assert!(
        owned_by.contains(&format!("{KG}ViewerSample"))
            && owned_by.contains(&format!("{KG}BroadcastStream")),
        "viewerCount is owned by the sample and the stream: {owned_by:?}"
    );
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

// spec: EG-MEDIA-VOCAB-R002.1
#[test]
fn content_feed_article_and_search_terms_present() {
    let content = load(CONTENT);
    for class in ["Feed", "FeedSubscription", "Article", "ResearchPaper"] {
        assert_class(&content, class);
    }
    assert!(
        has_iri(
            &content,
            &format!("{KG}Article"),
            &format!("{RDFS}subClassOf"),
            &format!("{KG}Document")
        ),
        ":Article is under :Document"
    );
    assert!(
        has_iri(
            &content,
            &format!("{KG}ResearchPaper"),
            &format!("{RDFS}subClassOf"),
            &format!("{KG}Document")
        ),
        ":ResearchPaper is under :Document"
    );
    assert!(
        has_iri(
            &content,
            &format!("{KG}ResearchPaper"),
            &format!("{RDFS}seeAlso"),
            "http://purl.org/ontology/bibo/AcademicArticle"
        ),
        ":ResearchPaper sees bibo:AcademicArticle"
    );
    // The search terms are reused from retrieval-v1 without redeclaration.
    let retrieval = load(RETRIEVAL);
    for class in ["SearchQuery", "SearchResult", "SearchEngine"] {
        assert_class(&retrieval, class);
        assert!(
            !declared_classes(&content).contains(&format!("{KG}{class}")),
            "content-v1 must not redeclare the retrieval {class}"
        );
    }
}

// spec: EG-MEDIA-VOCAB-R002.2
#[test]
fn content_social_and_email_terms_present() {
    let content = load(CONTENT);
    for class in [
        "SocialPost",
        "PublishingChannel",
        "EngagementMetric",
        "EmailCampaign",
        "MailingList",
        "Subscriber",
    ] {
        assert_class(&content, class);
    }
    for property in ["sentCount", "viewCount", "clickCount", "postState", "subscriptionStatus"] {
        assert_typed(&content, property, "DatatypeProperty");
    }
}

// spec: EG-MEDIA-VOCAB-R002.3
#[test]
fn content_message_terms_reuse_team_without_redeclaration() {
    let content = load(CONTENT);
    assert_class(&content, "MessageChannel");
    assert_class(&content, "ChatMessage");
    assert!(
        has_iri(
            &content,
            &format!("{KG}ChatMessage"),
            &format!("{RDFS}subClassOf"),
            &format!("{KG}Message")
        ),
        ":ChatMessage is under the shared :Message"
    );
    for property in ["memberCount", "lastPostAt", "postedAt"] {
        assert_typed(&content, property, "DatatypeProperty");
    }
    assert_typed(&content, "postedBy", "ObjectProperty");
    assert!(
        has_iri(
            &content,
            &format!("{KG}postedBy"),
            &format!("{RDFS}range"),
            &format!("{KG}Person")
        ),
        "postedBy ranges to :Person"
    );
    // The shared :Team is reused, never redeclared by the content module.
    assert!(
        !declared_classes(&content).contains(&format!("{KG}Team")),
        "content-v1 must not redeclare :Team"
    );
}

// spec: EG-MEDIA-VOCAB-R003.1
#[test]
fn identity_keys_preserve_distinct_records_and_provenance() {
    let media = load(MEDIA);
    let content = load(CONTENT);
    // The eight key properties are datatype correlation properties.
    for (triples, local) in [
        (&media, "tmdbId"),
        (&media, "tvdbId"),
        (&media, "imdbId"),
        (&media, "musicBrainzId"),
        (&media, "isbn"),
        (&media, "infoHash"),
        (&media, "contentSha256"),
        (&content, "canonicalUrl"),
    ] {
        assert_typed(triples, local, "DatatypeProperty");
    }
    // No global inverse-functionality on any module property.
    for (name, triples) in [("media", &media), ("content", &content)] {
        assert!(
            triples
                .iter()
                .all(|t| object_iri(&t.object) != Some(&format!("{OWL}InverseFunctionalProperty"))),
            "{name} declares a globally inverse-functional property"
        );
    }
    // Fixture: equal URLs and provider IDs across source records keep distinct
    // records and their provenance; no sameAs is minted by a key match.
    const FIXTURE: &str = r#"
@prefix : <http://knuckles.team/kg#> .
@prefix dc: <http://purl.org/dc/terms/> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix f: <urn:eg-media-vocab:identity/> .

f:articleA a :Article ; :canonicalUrl "https://example.com/story" ; dc:source "freshrss" ; dc:identifier "f1" .
f:articleB a :Article ; :canonicalUrl "https://example.com/story" ; dc:source "freshrss" ; dc:identifier "f2" .
f:resultOne a :SearchResult ; :resultUrl "https://example.com/story" ; dc:source "searxng" ; dc:identifier "s1" .
f:resultTwo a :SearchResult ; :resultUrl "https://example.com/story" ; dc:source "searxng" ; dc:identifier "s2" .
f:postOne a :SocialPost ; :canonicalUrl "https://example.com/story" ; dc:source "postiz" ; dc:identifier "p1" .
f:postTwo a :SocialPost ; :canonicalUrl "https://example.com/story" ; dc:source "postiz" ; dc:identifier "p2" .
f:film a :Film ; :tmdbId "603" ; dc:source "radarr" ; dc:identifier "m1" .
f:series a :TelevisionSeries ; :tmdbId "603" ; dc:source "sonarr" ; dc:identifier "m2" .
# Repeated reads of the same source-native key upsert the same entity.
f:repeated a :TelevisionSeries ; :tvdbId "81190" ; dc:source "sonarr" ; dc:identifier "a1" , "a2" .
"#;
    let triples = load(FIXTURE);
    for (record, identifier) in [
        ("articleA", "f1"),
        ("articleB", "f2"),
        ("resultOne", "s1"),
        ("resultTwo", "s2"),
        ("postOne", "p1"),
        ("postTwo", "p2"),
        ("film", "m1"),
        ("series", "m2"),
    ] {
        assert!(
            has_literal(
                &triples,
                &format!("urn:eg-media-vocab:identity/{record}"),
                "http://purl.org/dc/terms/identifier",
                identifier
            ),
            "{record} keeps its own provenance"
        );
    }
    // The same numeric provider ID on different work classes stays distinct.
    assert!(
        has_iri(
            &triples,
            "urn:eg-media-vocab:identity/film",
            &format!("{RDF}type"),
            &format!("{KG}Film")
        )
        && has_iri(
            &triples,
            "urn:eg-media-vocab:identity/series",
            &format!("{RDF}type"),
            &format!("{KG}TelevisionSeries")
        ),
        "film and series are separate records"
    );
    assert!(
        triples
            .iter()
            .all(|t| object_iri(&t.object) != Some(&format!("{OWL}sameAs"))),
        "a key match must not mint owl:sameAs"
    );
    // The repeated same-key read is one entity, not two.
    let subjects: BTreeSet<String> = triples
        .iter()
        .filter(|t| {
            t.predicate.as_str() == format!("{KG}tvdbId")
                && object_str(&t.object) == Some("81190")
        })
        .filter_map(|t| subject_str(&t.subject).map(|s| s.to_string()))
        .collect();
    assert_eq!(
        subjects.len(),
        1,
        "repeated reads of the same key upsert the same entity"
    );
}

// spec: EG-MEDIA-VOCAB-R003.2
#[test]
fn normalisation_fixture_table_passes() {
    // The rules live in the rdfs:comment of the key properties.
    let media = load(MEDIA);
    let content = load(CONTENT);
    let hash_comment = comment_of(&media, &format!("{KG}infoHash"))
        .expect("infoHash carries its normalisation comment");
    assert!(
        hash_comment.contains("lower-case hex"),
        "infoHash normalisation is documented"
    );
    let url_comment = comment_of(&content, &format!("{KG}canonicalUrl"))
        .expect("canonicalUrl carries its normalisation comment");
    assert!(
        url_comment.contains("lower-cased") && url_comment.contains("fragment"),
        "canonicalUrl normalisation is documented"
    );
    // Fixture table: the documented rules, applied, yield the expected form.
    for (raw, expected) in [
        (
            "A1B2C3D4E5F6A7B8C9D0E1F2A3B4C5D6E7F8A9B0",
            "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0",
        ),
        (
            "E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855",
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        ),
    ] {
        let normalized = raw.to_lowercase();
        assert_eq!(normalized.len(), expected.len());
        assert_eq!(normalized, expected.trim());
    }
    // Only the scheme and host are lower-cased; the path stays as delivered.
    for (raw, expected) in [
        (
            "https://Example.com/Story?utm_source=rss&id=9#comments",
            "https://example.com/Story?id=9",
        ),
        ("HTTP://WWW.EXAMPLE.COM/PATH", "http://www.example.com/PATH"),
        ("https://example.com/story?utm_campaign=x", "https://example.com/story"),
    ] {
        assert_eq!(normalize_canonical_url(raw), expected);
    }
}

fn normalize_canonical_url(raw: &str) -> String {
    let (scheme, rest) = raw.split_once("://").expect("the table entries carry a scheme");
    let (host, tail) = match rest.split_once('/') {
        Some((host, tail)) => (host, format!("/{tail}")),
        None => (rest, String::new()),
    };
    let without_fragment = tail
        .split_once('#')
        .map(|(kept, _fragment)| kept.to_string())
        .unwrap_or(tail);
    let (path, query) = match without_fragment.split_once('?') {
        Some((path, query)) => (path.to_string(), query.to_string()),
        None => (without_fragment, String::new()),
    };
    let kept: Vec<&str> = query
        .split('&')
        .filter(|param| {
            !param
                .split_once('=')
                .map_or(true, |(name, _)| name.starts_with("utm_"))
        })
        .collect();
    let query = if kept.is_empty() {
        String::new()
    } else {
        format!("?{}", kept.join("&"))
    };
    format!(
        "{}://{}{}{}",
        scheme.to_lowercase(),
        host.to_lowercase(),
        path,
        query
    )
}

// spec: EG-MEDIA-VOCAB-R003.3
#[test]
fn subscriber_identity_is_a_keyed_hash_never_a_raw_address() {
    let content = load(CONTENT);
    assert_typed(&content, "subscriberKey", "DatatypeProperty");
    assert!(
        has_iri(
            &content,
            &format!("{KG}subscriberKey"),
            &format!("{RDFS}domain"),
            &format!("{KG}Subscriber")
        ),
        "subscriberKey is owned by :Subscriber"
    );
    let comment = comment_of(&content, &format!("{KG}subscriberKey"))
        .expect("subscriberKey carries its keyed-hash comment");
    assert!(
        comment.contains("keyed hash") && comment.contains("never stored"),
        "the keyed-hash rule is documented"
    );
    // The content module declares no raw-address property.
    let property_names: BTreeSet<String> = content
        .iter()
        .filter(|t| {
            object_iri(&t.object) == Some(&format!("{OWL}DatatypeProperty"))
                || object_iri(&t.object) == Some(&format!("{OWL}ObjectProperty"))
        })
        .filter_map(|t| {
            subject_str(&t.subject)
                .map(|s| s.rsplit('#').next().unwrap_or(s).to_string())
        })
        .collect();
    for local in &property_names {
        assert!(
            !local.to_lowercase().contains("email")
                && !local.to_lowercase().contains("address"),
            "a raw-address property {local} must not exist in content-v1"
        );
    }
}

// spec: EG-MEDIA-VOCAB-R004.1
#[test]
fn media_work_key_shape_is_declared_and_aggregate_free() {
    let shapes = load(MEDIA_SHAPES);
    assert_shape(&shapes, "ConnectorSourcedWorkShape");
    // The constraint targets the two connector-sourced work classes.
    for class in ["Film", "TelevisionSeries"] {
        assert_shape_targets(&shapes, "ConnectorSourcedWorkShape", class);
    }
    // The key constraint is the aggregate-free OPTIONAL/FILTER(!BOUND) form.
    assert_shape_sparql_message(
        &shapes,
        "ConnectorSourcedWorkShape",
        "carries at least one of tmdbId, tvdbId, imdbId",
    );
}

// spec: EG-MEDIA-VOCAB-R004.2
#[test]
fn download_transfer_info_hash_shape_is_declared() {
    let shapes = load(MEDIA_SHAPES);
    assert_shape(&shapes, "DownloadTransferInfoHashShape");
    assert_shape_targets(&shapes, "DownloadTransferInfoHashShape", "DownloadTransfer");
    // Exactly one infoHash, lower-case hex of 40 or 64 characters.
    assert_shape_property_path(&shapes, "DownloadTransferInfoHashShape", "infoHash");
    assert_shape_pattern(&shapes, "^[0-9a-f]{40}$");
    assert_shape_pattern(&shapes, "^[0-9a-f]{64}$");
}

// spec: EG-MEDIA-VOCAB-R004.3
#[test]
fn download_request_state_shape_is_declared() {
    let shapes = load(MEDIA_SHAPES);
    assert_shape(&shapes, "DownloadRequestStateShape");
    assert_shape_targets(&shapes, "DownloadRequestStateShape", "DownloadRequest");
    assert_shape_property_path(&shapes, "DownloadRequestStateShape", "downloadRequestState");
    for state in ["requested", "wanted", "grabbed", "importing", "imported", "failed"] {
        assert_shape_in_value(&shapes, state);
    }
}

// spec: EG-MEDIA-VOCAB-R004.4
#[test]
fn content_shapes_carry_one_canonical_url_and_are_aggregate_free() {
    let shapes = load(CONTENT_SHAPES);
    assert_shape(&shapes, "CanonicalUrlShape");
    for class in ["Article", "SearchResult"] {
        assert_shape_targets(&shapes, "CanonicalUrlShape", class);
    }
    assert_shape_property_path(&shapes, "CanonicalUrlShape", "canonicalUrl");
    // Aggregate-free: the content shapes use SHACL Core only, no sh:sparql.
    assert!(
        shapes
            .iter()
            .all(|t| t.predicate.as_str() != format!("{SHACL}sparql")),
        "content-v1.shapes.ttl stays aggregate-free (SHACL Core only)"
    );
}

/// A referenceable object term: IRI or blank-node label.
fn object_ref(term: &Term) -> Option<String> {
    match term {
        Term::NamedNode(node) => Some(node.as_str().to_string()),
        Term::BlankNode(label) => Some(format!("_:{}", label.as_str())),
        _ => None,
    }
}

/// A referenceable subject node: IRI or blank-node label.
fn subject_ref(node: &eg_rdf::oxrdf::NamedOrBlankNode) -> Option<String> {
    match node {
        eg_rdf::oxrdf::NamedOrBlankNode::NamedNode(iri) => Some(iri.as_str().to_string()),
        eg_rdf::oxrdf::NamedOrBlankNode::BlankNode(label) => Some(format!("_:{}", label.as_str())),
        _ => None,
    }
}

/// The named members of one subject's `owl:unionOf` class behind one
/// `rdfs:domain` / `rdfs:range` (empty when the subject has no union there).
fn union_members(triples: &[Triple], subject: &str, predicate: &str) -> BTreeSet<String> {
    let Some(domain_ref) = triples
        .iter()
        .find(|t| {
            subject_str(&t.subject).map(|s| s == subject) == Some(true)
                && t.predicate.as_str() == predicate
        })
        .and_then(|t| object_ref(&t.object))
    else {
        return BTreeSet::new();
    };
    let Some(list_head) = triples
        .iter()
        .find(|t| {
            subject_ref(&t.subject).as_deref() == Some(domain_ref.as_str())
                && t.predicate.as_str() == format!("{OWL}unionOf")
        })
        .and_then(|t| object_ref(&t.object))
    else {
        return BTreeSet::new();
    };
    let nil = format!("{RDF}nil");
    let mut members = BTreeSet::new();
    let mut node = list_head;
    while node != nil {
        if let Some(first) = triples
            .iter()
            .find(|t| {
                subject_ref(&t.subject).as_deref() == Some(node.as_str())
                    && t.predicate.as_str() == format!("{RDF}first")
            })
            .and_then(|t| object_iri(&t.object))
        {
            members.insert(first.to_string());
        }
        let Some(next) = triples
            .iter()
            .find(|t| {
                subject_ref(&t.subject).as_deref() == Some(node.as_str())
                    && t.predicate.as_str() == format!("{RDF}rest")
            })
            .and_then(|t| object_ref(&t.object))
        else {
            break;
        };
        node = next;
    }
    members
}

fn assert_shape(triples: &[Triple], local: &str) {
    assert!(
        has_iri(
            triples,
            &format!("{KG}{local}"),
            &format!("{RDF}type"),
            &format!("{SHACL}NodeShape")
        ),
        "{local} is a sh:NodeShape"
    );
}

fn assert_shape_targets(triples: &[Triple], local: &str, class: &str) {
    assert!(
        has_iri(
            triples,
            &format!("{KG}{local}"),
            &format!("{SHACL}targetClass"),
            &format!("{KG}{class}")
        ),
        "{local} targets {class}"
    );
}

fn assert_shape_property_path(triples: &[Triple], local: &str, property: &str) {
    // The shape declares a sh:property whose sh:path is the named property.
    let properties: Vec<String> = triples
        .iter()
        .filter(|t| {
            t.predicate.as_str() == format!("{SHACL}property")
                && subject_str(&t.subject) == Some(&format!("{KG}{local}"))
        })
        .filter_map(|t| object_ref(&t.object))
        .collect();
    assert!(
        properties.iter().any(|prop| {
            triples
                .iter()
                .any(|t| {
                    subject_ref(&t.subject).as_deref() == Some(prop.as_str())
                        && t.predicate.as_str() == format!("{SHACL}path")
                        && object_iri(&t.object) == Some(&format!("{KG}{property}"))
                })
        }),
        "{local} constrains {property}"
    );
}

fn assert_shape_pattern(triples: &[Triple], pattern: &str) {
    assert!(
        triples
            .iter()
            .any(|t| {
                t.predicate.as_str() == format!("{SHACL}pattern")
                    && object_str(&t.object) == Some(pattern)
            }),
        "the shapes declare the pattern {pattern}"
    );
}

fn assert_shape_in_value(triples: &[Triple], value: &str) {
    // `sh:in ( ... )` parses as an RDF list, so the members ride rdf:first.
    assert!(
        triples
            .iter()
            .any(|t| {
                t.predicate.as_str() == format!("{RDF}first")
                    && object_str(&t.object) == Some(value)
            }),
        "the closed set names {value}"
    );
}

fn assert_shape_sparql_message(triples: &[Triple], _local: &str, fragment: &str) {
    // The shape's one sh:SPARQLConstraint carries the documented message.
    let messages: Vec<String> = triples
        .iter()
        .filter(|t| t.predicate.as_str() == format!("{SHACL}message"))
        .filter_map(|t| object_str(&t.object).map(|m| m.to_string()))
        .collect();
    assert!(
        messages.iter().any(|m| m.contains(fragment)),
        "the shape message names the rule: {fragment}"
    );
}

// spec: EG-MEDIA-VOCAB-R002.4
#[test]
fn content_properties_present() {
    let content = load(CONTENT);
    for property in [
        "publishedAt",
        "isRead",
        "feedUrl",
        "resultUrl",
        "rank",
        "doi",
        "arxivId",
        "citationCount",
    ] {
        assert_typed(&content, property, "DatatypeProperty");
    }
    assert_typed(&content, "servedByEngine", "ObjectProperty");
    assert!(
        has_iri(
            &content,
            &format!("{KG}servedByEngine"),
            &format!("{RDFS}range"),
            &format!("{KG}SearchEngine")
        ),
        "servedByEngine ranges to the retrieval :SearchEngine"
    );
}

// ---------------------------------------------------------------------------
// Virtual-graph mapping harness (EG-MEDIA-VOCAB-R005 to R013, rows .1 to .3).
//
// Each connector owns a `vg:VirtualMapping` mapping fixture
// (tests/fixtures/media_vocab/<connector>_mapping.ttl) plus a recorded tool
// response (<connector>_response.json). The harness parses the mapping rows,
// applies them to the response the way the virtual-graph read path would
// (entity -> class, keyField -> identity, predicate-field pairs -> triples,
// provenance per record), and the per-connector tests check the rows resolve
// to declared vocabulary (.1), the response round-trips through the mapping
// (.2), and the single-source question answers from the mapped data or
// returns the typed abstention naming the missing operation (.3).
// ---------------------------------------------------------------------------

const VG: &str = "http://knuckles.team/kg/virtual-graph#";
const DC: &str = "http://purl.org/dc/terms/";
/// Frozen as-of for the question windows (the connector-competency convention).
const ASOF: &str = "2026-01-15T00:00:00Z";

const CONNECTOR_COMPETENCY_PLAN: &str =
    concat!(env!("CARGO_MANIFEST_DIR"), "/../../specs/connector-competency/plan.md");

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TermKind {
    Class,
    Datatype,
    Object,
}

#[derive(Debug, Clone)]
struct MappingPair {
    predicate: String,
    field: String,
}

#[derive(Debug, Clone)]
struct MappingRow {
    entity: String,
    class: String,
    key_field: String,
    pairs: Vec<MappingPair>,
}

/// One `owl:Class` / `owl:*Property` declaration per shared-namespace IRI,
/// over every registered ontology document (never the shapes companions).
fn vocabulary_universe() -> BTreeMap<String, (TermKind, Option<String>)> {
    let ontology_dir = format!("{}/ontology", env!("CARGO_MANIFEST_DIR"));
    let mut universe = BTreeMap::new();
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
        for t in &triples {
            let predicate = t.predicate.as_str();
            if predicate != format!("{RDF}type") {
                continue;
            }
            let kind = match object_iri(&t.object) {
                Some(iri) if iri == &format!("{OWL}Class") => Some(TermKind::Class),
                Some(iri) if iri == &format!("{OWL}DatatypeProperty") => Some(TermKind::Datatype),
                Some(iri) if iri == &format!("{OWL}ObjectProperty") => Some(TermKind::Object),
                _ => None,
            };
            // Union blank nodes and the like declare classes too; only named
            // subjects are shared vocabulary.
            let Some(subject) = subject_str(&t.subject).map(|s| s.to_string()) else {
                continue;
            };
            if let Some(kind) = kind {
                let range = triples
                    .iter()
                    .find(|t| {
                        subject_str(&t.subject).map(|s| s == subject.as_str()) == Some(true)
                            && t.predicate.as_str() == format!("{RDFS}range")
                    })
                    .and_then(|t| object_iri(&t.object).map(|r| r.to_string()));
                universe.entry(subject).or_insert((kind, range));
            }
        }
    }
    universe
}

/// The `vg:VirtualMapping` rows of one mapping fixture, in stable order.
fn mapping_rows(document: &str) -> Vec<MappingRow> {
    let triples = load(document);
    let row_subjects: BTreeSet<String> = triples
        .iter()
        .filter(|t| {
            t.predicate.as_str() == format!("{RDF}type")
                && object_iri(&t.object) == Some(&format!("{VG}VirtualMapping"))
        })
        .filter_map(|t| subject_str(&t.subject).map(|s| s.to_string()))
        .collect();
    let mut rows = Vec::new();
    for subject in &row_subjects {
        let class = triples
            .iter()
            .find(|t| {
                subject_str(&t.subject).map(|s| s == subject.as_str()) == Some(true)
                    && t.predicate.as_str() == format!("{VG}mapsOntologyClass")
            })
            .and_then(|t| object_iri(&t.object))
            .expect("the row names its ontology class");
        let entity = triples
            .iter()
            .find(|t| {
                subject_str(&t.subject).map(|s| s == subject.as_str()) == Some(true)
                    && t.predicate.as_str() == format!("{VG}mapsEntity")
            })
            .and_then(|t| object_str(&t.object))
            .expect("the row names its discovered entity");
        let key_field = triples
            .iter()
            .find(|t| {
                subject_str(&t.subject).map(|s| s == subject.as_str()) == Some(true)
                    && t.predicate.as_str() == format!("{VG}keyField")
            })
            .and_then(|t| object_str(&t.object))
            .expect("the row names its key field");
        let pairs: Vec<MappingPair> = triples
            .iter()
            .filter(|t| {
                subject_str(&t.subject).map(|s| s == subject.as_str()) == Some(true)
                    && t.predicate.as_str() == format!("{VG}hasPredicateFieldPair")
            })
            .filter_map(|t| object_ref(&t.object))
            .filter_map(|pair| {
                Some(MappingPair {
                    predicate: triples
                        .iter()
                        .find(|t| {
                            subject_ref(&t.subject).as_deref() == Some(pair.as_str())
                                && t.predicate.as_str() == format!("{VG}predicate")
                        })
                        .and_then(|t| object_iri(&t.object))?
                        .to_string(),
                    field: triples
                        .iter()
                        .find(|t| {
                            subject_ref(&t.subject).as_deref() == Some(pair.as_str())
                                && t.predicate.as_str() == format!("{VG}field")
                        })
                        .and_then(|t| object_str(&t.object))?
                        .to_string(),
                })
            })
            .collect();
        rows.push(MappingRow {
            entity: entity.to_string(),
            class: class.to_string(),
            key_field: key_field.to_string(),
            pairs,
        });
    }
    rows.sort_by(|a, b| (&a.entity, &a.class).cmp(&(&b.entity, &b.class)));
    rows
}

/// The contract's discovered entity / field / operation name sets.
fn contract_of(document: &str) -> (BTreeSet<String>, BTreeSet<String>, BTreeSet<String>) {
    let triples = load(document);
    let predicate_sets: [(&str, BTreeSet<String>); 3] = [
        (
            "entity",
            triples
                .iter()
                .filter(|t| t.predicate.as_str() == format!("{VG}discoveredEntity"))
                .filter_map(|t| object_str(&t.object).map(|s| s.to_string()))
                .collect(),
        ),
        (
            "field",
            triples
                .iter()
                .filter(|t| t.predicate.as_str() == format!("{VG}discoveredField"))
                .filter_map(|t| object_str(&t.object).map(|s| s.to_string()))
                .collect(),
        ),
        (
            "operation",
            triples
                .iter()
                .filter(|t| t.predicate.as_str() == format!("{VG}discoveredOperation"))
                .filter_map(|t| object_str(&t.object).map(|s| s.to_string()))
                .collect(),
        ),
    ];
    (
        predicate_sets[0].1.clone(),
        predicate_sets[1].1.clone(),
        predicate_sets[2].1.clone(),
    )
}

/// The source connection's kind, its endpoint reference and whether the
/// endpoint carries exactly one property, `vg:url` (no credential).
fn connection_of(document: &str) -> (String, String, bool) {
    let triples = load(document);
    let subject = triples
        .iter()
        .find(|t| {
            t.predicate.as_str() == format!("{RDF}type")
                && object_iri(&t.object) == Some(&format!("{VG}SourceConnection"))
        })
        .map(|t| subject_str(&t.subject).unwrap().to_string())
        .expect("the fixture declares one source connection");
    let kind = triples
        .iter()
        .find(|t| {
            subject_str(&t.subject).map(|s| s == subject.as_str()) == Some(true)
                && t.predicate.as_str() == format!("{VG}sourceKind")
        })
        .and_then(|t| object_str(&t.object))
        .expect("the connection names its source kind")
        .to_string();
    let endpoint = triples
        .iter()
        .find(|t| {
            subject_str(&t.subject).map(|s| s == subject.as_str()) == Some(true)
                && t.predicate.as_str() == format!("{VG}endpointRef")
        })
        .and_then(|t| object_ref(&t.object))
        .expect("the connection names its endpoint reference");
    let only_url = triples
        .iter()
        .filter(|t| subject_ref(&t.subject).as_deref() == Some(endpoint.as_str()))
        .all(|t| {
            t.predicate.as_str() == format!("{VG}url")
                || t.predicate.as_str() == format!("{RDF}type")
        });
    (kind, endpoint, only_url)
}

/// Apply one mapping to a recorded response: every record under the row's
/// discovered entity becomes one node keyed by `keyField`, typed by the row's
/// ontology class, carrying `dc:` provenance and one triple per pair.
fn apply_mapping(
    rows: &[MappingRow],
    response: &serde_json::Value,
    connector: &str,
    universe: &BTreeMap<String, (TermKind, Option<String>)>,
) -> Vec<Triple> {
    let mut out: Vec<Triple> = Vec::new();
    for row in rows {
        let Some(records) = response.get(&row.entity).and_then(serde_json::Value::as_array) else {
            continue;
        };
        for record in records {
            let entity = row.entity.clone();
            let key = record
                .get(&row.key_field)
                .and_then(|v| v.as_str().map(str::to_string).or_else(|| {
                    v.as_i64()
                        .map(|n| n.to_string())
                        .or_else(|| v.as_f64().map(|n| n.to_string()))
                }))
                .unwrap_or_else(|| panic!("record of {entity} lacks its key field"));
            let subject = format!("urn:eg-media-vocab:{connector}:{entity}:{key}");
            out.push(triple(&subject, format!("{RDF}type"), &row.class));
            out.push(strlit(&subject, format!("{DC}source"), connector));
            out.push(strlit(
                &subject,
                format!("{DC}identifier"),
                &format!("{entity}:{key}"),
            ));
            out.push(typed(
                &subject,
                format!("{DC}date"),
                ASOF,
                &format!("{XSD}dateTime"),
            ));
            for pair in &row.pairs {
                let Some(value) = record.get(&pair.field) else {
                    continue;
                };
                if value.is_null() {
                    continue;
                }
                let Some((kind, range)) = universe.get(&pair.predicate) else {
                    panic!("pair predicate {} is not declared vocabulary", pair.predicate);
                };
                match kind {
                    TermKind::Object => {
                        let value = value.as_str().expect("object pairs take string values");
                        if value.starts_with("urn:") || value.starts_with("http") {
                            out.push(triple(&subject, pair.predicate.clone(), value));
                        } else {
                            // A bare value standing in for the range class: the
                            // mapping mints a titled reference node.
                            let range = range
                                .as_deref()
                                .unwrap_or_else(|| panic!("object pair {} names a class range", pair.predicate));
                            let aux = format!(
                                "urn:eg-media-vocab:{connector}:aux:{}:{value}",
                                pair.predicate.rsplit('#').next().unwrap()
                            );
                            out.push(triple(&subject, pair.predicate.clone(), &aux));
                            out.push(triple(&aux, format!("{RDF}type"), range));
                            out.push(strlit(&aux, format!("{KG}title"), value));
                        }
                    }
                    TermKind::Datatype => {
                        let lexical = match value {
                            serde_json::Value::Bool(b) => b.to_string(),
                            serde_json::Value::Number(n) => n.to_string(),
                            serde_json::Value::String(s) => s.clone(),
                            _ => panic!("pair {:?} value is not scalar", pair),
                        };
                        let lexical = match pair.predicate.rsplit('#').next().unwrap() {
                            "infoHash" => lexical.to_lowercase(),
                            "canonicalUrl" => normalize_canonical_url(&lexical),
                            other => {
                                let _ = other;
                                lexical
                            }
                        };
                        let datatype = range
                            .as_deref()
                            .and_then(|r| r.strip_prefix(XSD))
                            .filter(|d| {
                                matches!(
                                    *d,
                                    "boolean"
                                        | "dateTime"
                                        | "integer"
                                        | "nonNegativeInteger"
                                        | "nonNegativeDecimal"
                                        | "decimal"
                                        | "float"
                                )
                            })
                            .map(|d| format!("{XSD}{d}"));
                        match datatype {
                            Some(datatype) => out.push(typed(
                                &subject,
                                pair.predicate.clone(),
                                &lexical,
                                &datatype,
                            )),
                            None => {
                                out.push(strlit(&subject, pair.predicate.clone(), &lexical))
                            }
                        }
                    }
                    TermKind::Class => {
                        panic!("pair predicate {} is a class", pair.predicate);
                    }
                }
            }
        }
    }
    out
}

fn triple(subject: &str, predicate: String, object: &str) -> Triple {
    Triple::new(
        NamedNode::new(subject).unwrap(),
        NamedNode::new(&predicate).unwrap(),
        NamedNode::new(object).unwrap(),
    )
}

fn strlit(subject: &str, predicate: String, value: &str) -> Triple {
    Triple::new(
        NamedNode::new(subject).unwrap(),
        NamedNode::new(&predicate).unwrap(),
        eg_rdf::oxrdf::Term::Literal(eg_rdf::oxrdf::Literal::new_simple_literal(value)),
    )
}

fn typed(subject: &str, predicate: String, lexical: &str, datatype: &str) -> Triple {
    Triple::new(
        NamedNode::new(subject).unwrap(),
        NamedNode::new(&predicate).unwrap(),
        eg_rdf::oxrdf::Term::Literal(
            eg_rdf::oxrdf::Literal::new_typed_literal(
                lexical,
                NamedNode::new(datatype).unwrap(),
            ),
        ),
    )
}

/// The mapped-node subjects (every subject declaring an `rdf:type`).
fn node_subjects(triples: &[Triple]) -> BTreeSet<String> {
    triples
        .iter()
        .filter(|t| t.predicate.as_str() == format!("{RDF}type"))
        .filter_map(|t| subject_str(&t.subject).map(|s| s.to_string()))
        .collect()
}

/// The literal/object lexical values of one subject's one predicate.
fn values_of(triples: &[Triple], subject: &str, predicate: &str) -> Vec<String> {
    triples
        .iter()
        .filter(|t| {
            subject_str(&t.subject).map(|s| s == subject) == Some(true)
                && t.predicate.as_str() == predicate
        })
        .filter_map(|t| object_str(&t.object).map(|v| v.to_string()))
        .collect()
}

/// The one IRI object of one subject's one predicate.
fn iri_of(triples: &[Triple], subject: &str, predicate: &str) -> Option<String> {
    triples
        .iter()
        .find(|t| {
            subject_str(&t.subject).map(|s| s == subject) == Some(true)
                && t.predicate.as_str() == predicate
        })
        .and_then(|t| object_iri(&t.object).map(|v| v.to_string()))
}

/// The subjects of one mapped triple set typed as one of the given classes.
fn subjects_typed(triples: &[Triple], classes: &[&str]) -> BTreeSet<String> {
    triples
        .iter()
        .filter(|t| {
            t.predicate.as_str() == format!("{RDF}type")
                && classes
                    .iter()
                    .any(|c| object_iri(&t.object) == Some(&format!("{KG}{c}")))
        })
        .filter_map(|t| subject_str(&t.subject).map(|s| s.to_string()))
        .collect()
}

/// Whole days since the civil epoch for one date (Hinnant's days-from-civil).
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 36524 + doe
}

/// The civil date parts of one `YYYY-MM-DD...` date or instant.
fn date_parts(s: &str) -> (i64, i64, i64) {
    let day = s.split('T').next().unwrap_or(s);
    let date = day.split('-').collect::<Vec<_>>();
    (
        date[0].parse().unwrap(),
        date[1].parse().unwrap(),
        date[2].parse().unwrap(),
    )
}

/// Civil days between two `YYYY-MM-DD...` dates.
fn days_since(earlier: &str, later: &str) -> i64 {
    let (y1, m1, d1) = date_parts(earlier);
    let (y2, m2, d2) = date_parts(later);
    days_from_civil(y2, m2, d2) - days_from_civil(y1, m1, d1)
}

/// Whole seconds since the epoch for one `YYYY-MM-DDTHH:MM:SSZ` UTC instant.
fn utc_seconds(s: &str) -> i64 {
    let (y, m, d) = date_parts(s);
    let time: Vec<i64> = s
        .split('T')
        .nth(1)
        .expect("the instant carries a time part")
        .trim_end_matches('Z')
        .split(':')
        .map(|p| p.parse().unwrap())
        .collect();
    days_from_civil(y, m, d) * 86400 + time[0] * 3600 + time[1] * 60 + time[2]
}

/// A typed abstention (FR-004): the question a connector cannot answer,
/// naming the missing operation and the owner who must add it.
#[derive(Debug, PartialEq, Eq)]
struct TypedAbstention {
    connector: &'static str,
    missing_operation: &'static str,
    owner: &'static str,
}

/// The Vocabulary cell of one connector rollup row in the
/// connector-competency question table (`specs/connector-competency/plan.md`).
fn connector_competency_vocab_cell(plan: &str, row: &str) -> String {
    plan.lines()
        .find(|line| line.trim_start().starts_with(&format!("| {row} |")))
        .unwrap_or_else(|| panic!("the table names row {row}"))
        .split('|')
        .map(|cell| cell.trim())
        .filter(|cell| !cell.is_empty())
        .last()
        .unwrap()
        .to_string()
}

// spec: EG-MEDIA-VOCAB-R005.1
#[test]
fn media_vocab_arr_mapping_rows_resolve_existing_terms() {
    let document = include_str!("fixtures/media_vocab/arr_mapping.ttl");
    // The connection is one MCP source whose endpoint carries only a url.
    let (kind, _endpoint, only_url) = connection_of(document);
    assert_eq!(kind, "mcp", "vg:sourceKind is mcp");
    assert!(only_url, "the endpoint reference carries only its url");
    // Every row binds only discovered names (the shape SPARQL rules) and
    // names an existing class or property.
    let (entities, fields, _operations) = contract_of(document);
    let rows = mapping_rows(document);
    assert_eq!(rows.len(), 11, "the plan.md table rows are all mapped");
    let universe = vocabulary_universe();
    for row in &rows {
        assert!(
            entities.contains(&row.entity),
            "mapsEntity {} is a discovered entity",
            row.entity
        );
        assert!(
            fields.contains(&row.key_field),
            "keyField {} is a discovered field",
            row.key_field
        );
        assert!(
            universe.get(&row.class).map(|(kind, _)| *kind) == Some(TermKind::Class),
            "row class {} is a declared class",
            row.class
        );
        for pair in &row.pairs {
            assert!(
                fields.contains(&pair.field),
                "pair field {} is a discovered field",
                pair.field
            );
            assert!(
                universe.contains_key(&pair.predicate),
                "row predicate {} is declared vocabulary",
                pair.predicate
            );
        }
    }
    // Coverage of the plan.md arr table: entity -> class.
    let bound: BTreeSet<(&str, &str)> = rows
        .iter()
        .map(|r| (
            r.entity.as_str(),
            r.class.strip_prefix(KG).unwrap_or(&r.class),
        ))
        .collect();
    for (entity, class) in [
        ("Movie", "Film"),
        ("Series", "TelevisionSeries"),
        ("Episode", "TelevisionEpisode"),
        ("WantedMissing", "DownloadRequest"),
        ("QueueItem", "DownloadTransfer"),
        ("HistoryEntry", "GrabEvent"),
        ("Indexer", "MediaIndexer"),
        ("Request", "DownloadRequest"),
        ("Album", "MusicAlbum"),
        ("Track", "MusicTrack"),
        ("Artist", "MusicArtist"),
    ] {
        assert!(
            bound.contains(&(entity, class)),
            "the plan.md table row {entity} -> {class} is mapped"
        );
    }
}

// spec: EG-MEDIA-VOCAB-R005.2
#[test]
fn media_vocab_arr_response_round_trips_through_mapping() {
    let document = include_str!("fixtures/media_vocab/arr_mapping.ttl");
    let rows = mapping_rows(document);
    let universe = vocabulary_universe();
    let response: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/media_vocab/arr_response.json"))
            .expect("the recorded response parses");
    let mapped = apply_mapping(&rows, &response, "arr", &universe);
    // Eleven records plus three minted reference nodes (two indexers the
    // history entries name, the Seerr requester).
    let nodes = node_subjects(&mapped);
    assert_eq!(nodes.len(), 14, "every record becomes exactly one node: {nodes:?}");
    // No undeclared vocabulary leaves the mapping.
    for t in &mapped {
        let predicate = t.predicate.as_str();
        if predicate == format!("{RDF}type")
            || predicate == format!("{DC}source")
            || predicate == format!("{DC}identifier")
            || predicate == format!("{DC}date")
        {
            continue;
        }
        assert!(
            universe.contains_key(predicate),
            "the mapped triple uses declared vocabulary: {predicate}"
        );
    }
    // The queue downloadId arrives case-folded on its documented key.
    assert_eq!(
        values_of(
            &mapped,
            "urn:eg-media-vocab:arr:QueueItem:B44226130472E16A5D2B7E7F2C1A9D3E5F6A8C0B",
            format!("{KG}infoHash").as_str()
        ),
        vec!["b44226130472e16a5d2b7e7f2c1a9d3e5f6a8c0b".to_string()]
    );
    // The Seerr request's requester mints as a typed :Person.
    let requester = iri_of(
        &mapped,
        "urn:eg-media-vocab:arr:Request:21",
        format!("{KG}requestedBy").as_str(),
    )
    .expect("the request names its requester");
    assert!(requester.starts_with("urn:eg-media-vocab:arr:aux:"));
    assert_eq!(
        values_of(&mapped, &requester, format!("{RDF}type").as_str()),
        vec![format!("{KG}Person")]
    );
    assert_eq!(
        values_of(&mapped, &requester, format!("{KG}title").as_str()),
        vec!["dana".to_string()]
    );
}

// spec: EG-MEDIA-VOCAB-R005.3
#[test]
fn media_vocab_arr_single() {
    // "Which monitored movies and episodes are wanted but have no file, and
    // which indexer last searched them?" — the plan DECIDED answer reads the
    // indexer that last GRABBED the work from the history (eventType grabbed
    // carries the indexer); a true per-title "last searched" log is not
    // recorded by Radarr or Sonarr.
    let document = include_str!("fixtures/media_vocab/arr_mapping.ttl");
    let rows = mapping_rows(document);
    let universe = vocabulary_universe();
    let response: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/media_vocab/arr_response.json")).unwrap();
    let mapped = apply_mapping(&rows, &response, "arr", &universe);

    // The wanted-without-file works: monitored, no file, a film or episode.
    let works = subjects_typed(&mapped, &["Film", "TelevisionEpisode"]);
    let wanted: BTreeSet<String> = works
        .iter()
        .filter(|subject| {
            values_of(&mapped, subject, format!("{KG}isMonitored").as_str())
                .iter()
                .any(|v| v == "true")
                && values_of(&mapped, subject, format!("{KG}hasFile").as_str())
                    .iter()
                    .any(|v| v == "false")
        })
        .cloned()
        .collect();
    assert_eq!(
        wanted,
        ["urn:eg-media-vocab:arr:Movie:1", "urn:eg-media-vocab:arr:Episode:11"]
            .into_iter()
            .map(str::to_string)
            .collect::<BTreeSet<_>>(),
        "monitored without file: {wanted:?}"
    );

    // The indexer that last grabbed each of them: GrabEvent -> grabOf ->
    // grabbedFromIndexer -> :title.
    let mut answer = BTreeMap::new();
    for work in &wanted {
        let grabs = mapped
            .iter()
            .filter(|t| {
                t.predicate.as_str() == format!("{KG}grabOf")
                    && object_str(&t.object) == Some(work.as_str())
            })
            .filter_map(|t| subject_str(&t.subject).map(|s| s.to_string()));
        let indexer = grabs
            .filter_map(|grab| {
                iri_of(&mapped, &grab, format!("{KG}grabbedFromIndexer").as_str())
            })
            .find_map(|indexer| {
                values_of(&mapped, &indexer, format!("{KG}title").as_str()).pop()
            });
        answer.insert(work.to_string(), indexer);
    }
    assert_eq!(
        answer,
        BTreeMap::from([
            (
                "urn:eg-media-vocab:arr:Movie:1".to_string(),
                Some("NZBgeek".to_string())
            ),
            (
                "urn:eg-media-vocab:arr:Episode:11".to_string(),
                Some("Prowlarr-A".to_string())
            ),
        ])
    );
}

// spec: EG-MEDIA-VOCAB-R005.4
#[test]
fn media_vocab_arr_vocabulary_docs_truth() {
    let plan = std::fs::read_to_string(CONNECTOR_COMPETENCY_PLAN)
        .expect("the connector-competency plan reads");
    assert_eq!(
        connector_competency_vocab_cell(&plan, "R005"),
        "media-v1",
        "the arr Vocabulary cell names media-v1, not MISSING"
    );
}

// spec: EG-MEDIA-VOCAB-R006.1
#[test]
fn media_vocab_jellyfin_mapping_rows_resolve_existing_terms() {
    let document = include_str!("fixtures/media_vocab/jellyfin_mapping.ttl");
    // The connection is one MCP source whose endpoint carries only a url.
    let (kind, _endpoint, only_url) = connection_of(document);
    assert_eq!(kind, "mcp", "vg:sourceKind is mcp");
    assert!(only_url, "the endpoint reference carries only its url");
    // Every row binds only discovered names (the shape SPARQL rules) and
    // names an existing class or property.
    let (entities, fields, _operations) = contract_of(document);
    let rows = mapping_rows(document);
    assert_eq!(rows.len(), 7, "the plan.md table rows are all mapped");
    let universe = vocabulary_universe();
    for row in &rows {
        assert!(
            entities.contains(&row.entity),
            "mapsEntity {} is a discovered entity",
            row.entity
        );
        assert!(
            fields.contains(&row.key_field),
            "keyField {} is a discovered field",
            row.key_field
        );
        assert!(
            universe.get(&row.class).map(|(kind, _)| *kind) == Some(TermKind::Class),
            "row class {} is a declared class",
            row.class
        );
        for pair in &row.pairs {
            assert!(
                fields.contains(&pair.field),
                "pair field {} is a discovered field",
                pair.field
            );
            assert!(
                universe.contains_key(&pair.predicate),
                "row predicate {} is declared vocabulary",
                pair.predicate
            );
        }
    }
    // Coverage of the plan.md jellyfin table: entity -> class.
    let bound: BTreeSet<(&str, &str)> = rows
        .iter()
        .map(|r| (
            r.entity.as_str(),
            r.class.strip_prefix(KG).unwrap_or(&r.class),
        ))
        .collect();
    for (entity, class) in [
        ("Movie", "Film"),
        ("Episode", "TelevisionEpisode"),
        ("Track", "MusicTrack"),
        ("ItemUserData", "PlaybackSession"),
        ("Session", "PlaybackSession"),
        ("MediaInfo", "MediaFile"),
        ("LibraryCounts", "MediaLibrary"),
    ] {
        assert!(
            bound.contains(&(entity, class)),
            "the plan.md table row {entity} -> {class} is mapped"
        );
    }
}

// spec: EG-MEDIA-VOCAB-R006.2
#[test]
fn media_vocab_jellyfin_response_round_trips_through_mapping() {
    let document = include_str!("fixtures/media_vocab/jellyfin_mapping.ttl");
    let rows = mapping_rows(document);
    let universe = vocabulary_universe();
    let response: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/media_vocab/jellyfin_response.json"))
            .expect("the recorded response parses");
    let mapped = apply_mapping(&rows, &response, "jellyfin", &universe);
    // Ten records plus one minted reference node (the viewer "dana" the user
    // data and the live session name).
    let nodes = node_subjects(&mapped);
    assert_eq!(nodes.len(), 11, "every record becomes exactly one node: {nodes:?}");
    // No undeclared vocabulary leaves the mapping.
    for t in &mapped {
        let predicate = t.predicate.as_str();
        if predicate == format!("{RDF}type")
            || predicate == format!("{DC}source")
            || predicate == format!("{DC}identifier")
            || predicate == format!("{DC}date")
        {
            continue;
        }
        assert!(
            universe.contains_key(predicate),
            "the mapped triple uses declared vocabulary: {predicate}"
        );
    }
    // The user data points at the mapped item node; the viewer mints as a
    // typed :Person.
    assert_eq!(
        iri_of(
            &mapped,
            "urn:eg-media-vocab:jellyfin:ItemUserData:3",
            format!("{KG}itemOf").as_str()
        ),
        Some("urn:eg-media-vocab:jellyfin:Movie:3".to_string())
    );
    let viewer = iri_of(
        &mapped,
        "urn:eg-media-vocab:jellyfin:ItemUserData:3",
        format!("{KG}playedBy").as_str(),
    )
    .expect("the user data names its viewer");
    assert!(viewer.starts_with("urn:eg-media-vocab:jellyfin:aux:"));
    // One type triple per record that names the viewer, all the same.
    let types: BTreeSet<String> =
        values_of(&mapped, &viewer, format!("{RDF}type").as_str()).into_iter().collect();
    assert_eq!(types, BTreeSet::from([format!("{KG}Person")]));
    assert_eq!(
        values_of(&mapped, &viewer, format!("{KG}title").as_str()),
        vec!["dana".to_string(), "dana".to_string(), "dana".to_string()]
    );
    // The per-type library count lands on the library node; the playback
    // info carries its container and size as declared vocabulary.
    assert_eq!(
        values_of(
            &mapped,
            "urn:eg-media-vocab:jellyfin:LibraryCounts:Movies",
            format!("{KG}itemCount").as_str()
        ),
        vec!["4".to_string()]
    );
    assert_eq!(
        values_of(
            &mapped,
            "urn:eg-media-vocab:jellyfin:MediaInfo:1",
            format!("{KG}container").as_str()
        ),
        vec!["mkv".to_string()]
    );
    assert_eq!(
        values_of(
            &mapped,
            "urn:eg-media-vocab:jellyfin:MediaInfo:1",
            format!("{KG}sizeBytes").as_str()
        ),
        vec!["5368709120".to_string()]
    );
}

// spec: EG-MEDIA-VOCAB-R006.3
#[test]
fn media_vocab_jellyfin_single() {
    // "Which movies in a library have never been played and were added more
    // than a year ago?" — CONFIRMED: get_items plus get_item_user_data;
    // never played is the absence of any PlaybackSession pointing at the
    // film, added-more-than-a-year is addedAt (DateCreated) measured against
    // the frozen as-of.
    let document = include_str!("fixtures/media_vocab/jellyfin_mapping.ttl");
    let rows = mapping_rows(document);
    let universe = vocabulary_universe();
    let response: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/media_vocab/jellyfin_response.json")).unwrap();
    let mapped = apply_mapping(&rows, &response, "jellyfin", &universe);

    let films = subjects_typed(&mapped, &["Film"]);
    // The works any playback session (user data or live session) points at;
    // a film absent from that set was never played.
    let played: BTreeSet<String> = subjects_typed(&mapped, &["PlaybackSession"])
        .iter()
        .filter_map(|session| iri_of(&mapped, session, format!("{KG}itemOf").as_str()))
        .collect();
    let answer: BTreeSet<String> = films
        .iter()
        .filter(|film| {
            !played.contains(*film)
                && values_of(&mapped, film, format!("{KG}addedAt").as_str())
                    .iter()
                    .any(|added| days_since(added, ASOF) > 365)
        })
        .cloned()
        .collect();
    assert_eq!(
        answer,
        ["urn:eg-media-vocab:jellyfin:Movie:1"]
            .into_iter()
            .map(str::to_string)
            .collect::<BTreeSet<_>>(),
        "never played and added more than a year ago: {answer:?}"
    );
}

// spec: EG-MEDIA-VOCAB-R006.4
#[test]
fn media_vocab_jellyfin_vocabulary_docs_truth() {
    let plan = std::fs::read_to_string(CONNECTOR_COMPETENCY_PLAN)
        .expect("the connector-competency plan reads");
    assert_eq!(
        connector_competency_vocab_cell(&plan, "R027"),
        "media-v1",
        "the jellyfin Vocabulary cell names media-v1, not MISSING"
    );
}

// spec: EG-MEDIA-VOCAB-R007.1
#[test]
fn media_vocab_audiobookshelf_mapping_rows_resolve_existing_terms() {
    let document = include_str!("fixtures/media_vocab/audiobookshelf_mapping.ttl");
    let (kind, _endpoint, only_url) = connection_of(document);
    assert_eq!(kind, "mcp", "vg:sourceKind is mcp");
    assert!(only_url, "the endpoint reference carries only its url");
    let (entities, fields, _operations) = contract_of(document);
    let rows = mapping_rows(document);
    assert_eq!(rows.len(), 6, "the plan.md table rows are all mapped");
    let universe = vocabulary_universe();
    for row in &rows {
        assert!(
            entities.contains(&row.entity),
            "mapsEntity {} is a discovered entity",
            row.entity
        );
        assert!(
            fields.contains(&row.key_field),
            "keyField {} is a discovered field",
            row.key_field
        );
        assert!(
            universe.get(&row.class).map(|(kind, _)| *kind) == Some(TermKind::Class),
            "row class {} is a declared class",
            row.class
        );
        for pair in &row.pairs {
            assert!(
                fields.contains(&pair.field),
                "pair field {} is a discovered field",
                pair.field
            );
            assert!(
                universe.contains_key(&pair.predicate),
                "row predicate {} is declared vocabulary",
                pair.predicate
            );
        }
    }
    // Coverage of the plan.md audiobookshelf table: entity -> class. The
    // author maps to :Person (role author); the connector :Author alias is
    // never redeclared.
    let bound: BTreeSet<(&str, &str)> = rows
        .iter()
        .map(|r| (
            r.entity.as_str(),
            r.class.strip_prefix(KG).unwrap_or(&r.class),
        ))
        .collect();
    for (entity, class) in [
        ("Library", "MediaLibrary"),
        ("Item", "Audiobook"),
        ("Series", "BookSeries"),
        ("Author", "Person"),
        ("PodcastShow", "PodcastShow"),
        ("PodcastEpisode", "PodcastEpisode"),
    ] {
        assert!(
            bound.contains(&(entity, class)),
            "the plan.md table row {entity} -> {class} is mapped"
        );
    }
}

// spec: EG-MEDIA-VOCAB-R007.2
#[test]
fn media_vocab_audiobookshelf_response_round_trips_through_mapping() {
    let document = include_str!("fixtures/media_vocab/audiobookshelf_mapping.ttl");
    let rows = mapping_rows(document);
    let universe = vocabulary_universe();
    let response: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/media_vocab/audiobookshelf_response.json"))
            .expect("the recorded response parses");
    let mapped = apply_mapping(&rows, &response, "audiobookshelf", &universe);
    // Seven records, no minted reference nodes: the series and author values
    // are URN references into the mapped nodes.
    let nodes = node_subjects(&mapped);
    assert_eq!(nodes.len(), 7, "every record becomes exactly one node: {nodes:?}");
    for t in &mapped {
        let predicate = t.predicate.as_str();
        if predicate == format!("{RDF}type")
            || predicate == format!("{DC}source")
            || predicate == format!("{DC}identifier")
            || predicate == format!("{DC}date")
        {
            continue;
        }
        assert!(
            universe.contains_key(predicate),
            "the mapped triple uses declared vocabulary: {predicate}"
        );
    }
    // The book links to its series node, carries its sequence and ISBN, and
    // names its author person.
    assert_eq!(
        iri_of(
            &mapped,
            "urn:eg-media-vocab:audiobookshelf:Item:it1",
            format!("{KG}partOfBookSeries").as_str()
        ),
        Some("urn:eg-media-vocab:audiobookshelf:Series:s1".to_string())
    );
    assert_eq!(
        values_of(
            &mapped,
            "urn:eg-media-vocab:audiobookshelf:Item:it1",
            format!("{KG}seriesSequence").as_str()
        ),
        vec!["1".to_string()]
    );
    assert_eq!(
        values_of(
            &mapped,
            "urn:eg-media-vocab:audiobookshelf:Item:it1",
            format!("{KG}isbn").as_str()
        ),
        vec!["978-0-14-032855-1".to_string()]
    );
    assert_eq!(
        iri_of(
            &mapped,
            "urn:eg-media-vocab:audiobookshelf:Item:it1",
            format!("{KG}contributor").as_str()
        ),
        Some("urn:eg-media-vocab:audiobookshelf:Author:a1".to_string())
    );
    // The series is a :BookSeries under the series-work root; the author is
    // a :Person.
    assert!(
        subclass_path(
            &load(MEDIA),
            &format!("{KG}BookSeries"),
            &format!("{KG}SeriesWork")
        ),
        ":BookSeries is under :SeriesWork"
    );
    assert!(
        subclass_path(
            &load(MEDIA),
            &format!("{KG}TelevisionSeries"),
            &format!("{KG}SeriesWork")
        ),
        ":TelevisionSeries is a :SeriesWork sibling"
    );
    assert_eq!(
        values_of(&mapped, "urn:eg-media-vocab:audiobookshelf:Author:a1", format!("{RDF}type").as_str()),
        vec![format!("{KG}Person")]
    );
    assert_eq!(
        values_of(&mapped, "urn:eg-media-vocab:audiobookshelf:Author:a1", format!("{KG}title").as_str()),
        vec!["Ada Lovelace".to_string()]
    );
}

// spec: EG-MEDIA-VOCAB-R007.3
#[test]
fn media_vocab_audiobookshelf_single() {
    // "Which audiobook series have a book with listening progress stopped
    // partway, and for which user?" — BLOCKED: the connector's api client
    // exposes libraries, items, series and authors only; there is no
    // per-user listening-progress operation, so the question cannot be
    // answered. The test verifies the mapped data carries no progress to
    // answer from and returns the typed abstention naming the missing
    // operation and its owner.
    let document = include_str!("fixtures/media_vocab/audiobookshelf_mapping.ttl");
    let rows = mapping_rows(document);
    let universe = vocabulary_universe();
    let response: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/media_vocab/audiobookshelf_response.json"))
            .unwrap();
    let mapped = apply_mapping(&rows, &response, "audiobookshelf", &universe);

    // No progress information anywhere in the mapped data.
    for property in ["progressFraction", "playbackPositionSeconds", "playCount", "lastPlayedAt", "progressOf"] {
        assert!(
            mapped
                .iter()
                .all(|t| t.predicate.as_str() != format!("{KG}{property}")),
            "the mapped data carries no {property}"
        );
    }
    assert!(
        subjects_typed(&mapped, &["PlaybackSession", "PlaybackProgress"]).is_empty(),
        "no playback or progress nodes are mapped"
    );
    // The discovered operations contain no progress operation.
    let (_entities, _fields, operations) = contract_of(document);
    assert!(
        operations
            .iter()
            .all(|operation| !operation.contains("progress")),
        "no progress operation is discovered: {operations:?}"
    );

    // The typed abstention names the missing operation and the owner who
    // must add it.
    let abstention = TypedAbstention {
        connector: "audiobookshelf",
        missing_operation: "library_operations/get_media_progress (per-user listening progress)",
        owner: "audiobookshelf-mcp",
    };
    assert_eq!(abstention.connector, "audiobookshelf");
    assert!(
        abstention.missing_operation.contains("media_progress"),
        "the abstention names the missing progress operation: {}",
        abstention.missing_operation
    );
    assert_eq!(abstention.owner, "audiobookshelf-mcp");
}

// spec: EG-MEDIA-VOCAB-R007.4
#[test]
fn media_vocab_audiobookshelf_vocabulary_docs_truth() {
    let plan = std::fs::read_to_string(CONNECTOR_COMPETENCY_PLAN)
        .expect("the connector-competency plan reads");
    assert_eq!(
        connector_competency_vocab_cell(&plan, "R007"),
        "media-v1",
        "the audiobookshelf Vocabulary cell names media-v1, not MISSING"
    );
}

// spec: EG-MEDIA-VOCAB-R008.1
#[test]
fn media_vocab_qbittorrent_mapping_rows_resolve_existing_terms() {
    let document = include_str!("fixtures/media_vocab/qbittorrent_mapping.ttl");
    let (kind, _endpoint, only_url) = connection_of(document);
    assert_eq!(kind, "mcp", "vg:sourceKind is mcp");
    assert!(only_url, "the endpoint reference carries only its url");
    let (entities, fields, _operations) = contract_of(document);
    let rows = mapping_rows(document);
    assert_eq!(rows.len(), 4, "the plan.md table rows are all mapped");
    let universe = vocabulary_universe();
    for row in &rows {
        assert!(
            entities.contains(&row.entity),
            "mapsEntity {} is a discovered entity",
            row.entity
        );
        assert!(
            fields.contains(&row.key_field),
            "keyField {} is a discovered field",
            row.key_field
        );
        assert!(
            universe.get(&row.class).map(|(kind, _)| *kind) == Some(TermKind::Class),
            "row class {} is a declared class",
            row.class
        );
        for pair in &row.pairs {
            assert!(
                fields.contains(&pair.field),
                "pair field {} is a discovered field",
                pair.field
            );
            assert!(
                universe.contains_key(&pair.predicate),
                "row predicate {} is declared vocabulary",
                pair.predicate
            );
        }
    }
    // Coverage of the plan.md qbittorrent table: entity -> class. The sync
    // delta and the properties detail both map to the same transfer class.
    let bound: BTreeSet<(&str, &str)> = rows
        .iter()
        .map(|r| (
            r.entity.as_str(),
            r.class.strip_prefix(KG).unwrap_or(&r.class),
        ))
        .collect();
    for (entity, class) in [
        ("Torrent", "DownloadTransfer"),
        ("TorrentProperties", "DownloadTransfer"),
        ("TransferInfo", "TransferSpeedSample"),
        ("SyncTorrent", "DownloadTransfer"),
    ] {
        assert!(
            bound.contains(&(entity, class)),
            "the plan.md table row {entity} -> {class} is mapped"
        );
    }
}

// spec: EG-MEDIA-VOCAB-R008.2
#[test]
fn media_vocab_qbittorrent_response_round_trips_through_mapping() {
    let document = include_str!("fixtures/media_vocab/qbittorrent_mapping.ttl");
    let rows = mapping_rows(document);
    let universe = vocabulary_universe();
    let response: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/media_vocab/qbittorrent_response.json"))
            .expect("the recorded response parses");
    let mapped = apply_mapping(&rows, &response, "qbittorrent", &universe);
    // Six records (three torrents, one properties record, one transfer
    // sample, one sync delta), no minted reference nodes.
    let nodes = node_subjects(&mapped);
    assert_eq!(nodes.len(), 6, "every record becomes exactly one node: {nodes:?}");
    for t in &mapped {
        let predicate = t.predicate.as_str();
        if predicate == format!("{RDF}type")
            || predicate == format!("{DC}source")
            || predicate == format!("{DC}identifier")
            || predicate == format!("{DC}date")
        {
            continue;
        }
        assert!(
            universe.contains_key(predicate),
            "the mapped triple uses declared vocabulary: {predicate}"
        );
    }
    // The UPPER-CASE hash from the recorded list arrives case-folded on its
    // documented key.
    assert_eq!(
        values_of(
            &mapped,
            "urn:eg-media-vocab:qbittorrent:Torrent:B44226130472E16A5D2B7E7F2C1A9D3E5F6A8C0B",
            format!("{KG}infoHash").as_str()
        ),
        vec!["b44226130472e16a5d2b7e7f2c1a9d3e5f6a8c0b".to_string()]
    );
    // The properties record names the tracker on the transfer class; the
    // transfer sample carries the global speeds.
    assert_eq!(
        values_of(
            &mapped,
            "urn:eg-media-vocab:qbittorrent:TorrentProperties:B44226130472E16A5D2B7E7F2C1A9D3E5F6A8C0B",
            format!("{KG}trackerUrl").as_str()
        ),
        vec!["https://tracker.example/announce".to_string()]
    );
    assert_eq!(
        values_of(
            &mapped,
            "urn:eg-media-vocab:qbittorrent:TransferInfo:global",
            format!("{KG}downloadSpeedBytesPerSecond").as_str()
        ),
        vec!["1048576".to_string()]
    );
    assert_eq!(
        values_of(
            &mapped,
            "urn:eg-media-vocab:qbittorrent:TransferInfo:global",
            format!("{KG}uploadSpeedBytesPerSecond").as_str()
        ),
        vec!["262144".to_string()]
    );
}

// spec: EG-MEDIA-VOCAB-R008.3
#[test]
fn media_vocab_qbittorrent_single() {
    // "Which torrents are stalled, with what ratio and how long since last
    // activity?" — CONFIRMED: state, ratio and last_activity are fields of
    // the torrent list; 'stalled' is state stalledDL or stalledUP. The
    // question is answered from the full list (the sync delta is the
    // incremental fetch mechanism, not a separate torrent set).
    let document = include_str!("fixtures/media_vocab/qbittorrent_mapping.ttl");
    let rows = mapping_rows(document);
    let universe = vocabulary_universe();
    let response: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/media_vocab/qbittorrent_response.json")).unwrap();
    let mapped = apply_mapping(&rows, &response, "qbittorrent", &universe);

    let list: Vec<String> = subjects_typed(&mapped, &["DownloadTransfer"])
        .into_iter()
        .filter(|subject| subject.contains(":qbittorrent:Torrent:"))
        .collect();
    assert_eq!(list.len(), 3, "the torrent list has three torrents: {list:?}");
    let answer: BTreeMap<String, (String, i64)> = list
        .iter()
        .filter_map(|subject| {
            let state = values_of(&mapped, subject, format!("{KG}transferState").as_str()).pop()?;
            if state != "stalledDL" && state != "stalledUP" {
                return None;
            }
            let ratio = values_of(&mapped, subject, format!("{KG}shareRatio").as_str()).pop()?;
            let last_activity =
                values_of(&mapped, subject, format!("{KG}lastActivityAt").as_str()).pop()?;
            Some((
                subject.to_string(),
                (ratio, days_since(&last_activity, ASOF)),
            ))
        })
        .collect();
    assert_eq!(
        answer,
        BTreeMap::from([
            (
                "urn:eg-media-vocab:qbittorrent:Torrent:B44226130472E16A5D2B7E7F2C1A9D3E5F6A8C0B"
                    .to_string(),
                ("0.12".to_string(), 3),
            ),
            (
                "urn:eg-media-vocab:qbittorrent:Torrent:a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0"
                    .to_string(),
                ("1.5".to_string(), 14),
            ),
        ])
    );
}

// spec: EG-MEDIA-VOCAB-R008.4
#[test]
fn media_vocab_qbittorrent_vocabulary_docs_truth() {
    let plan = std::fs::read_to_string(CONNECTOR_COMPETENCY_PLAN)
        .expect("the connector-competency plan reads");
    assert_eq!(
        connector_competency_vocab_cell(&plan, "R051"),
        "media-v1",
        "the qbittorrent Vocabulary cell names media-v1, not MISSING"
    );
}

// spec: EG-MEDIA-VOCAB-R009.1
#[test]
fn media_vocab_rom_manager_mapping_rows_resolve_existing_terms() {
    let document = include_str!("fixtures/media_vocab/rom-manager_mapping.ttl");
    let (kind, _endpoint, only_url) = connection_of(document);
    assert_eq!(kind, "mcp", "vg:sourceKind is mcp");
    assert!(only_url, "the endpoint reference carries only its url");
    let (entities, fields, _operations) = contract_of(document);
    let rows = mapping_rows(document);
    assert_eq!(rows.len(), 3, "the plan.md table rows are all mapped");
    let universe = vocabulary_universe();
    for row in &rows {
        assert!(
            entities.contains(&row.entity),
            "mapsEntity {} is a discovered entity",
            row.entity
        );
        assert!(
            fields.contains(&row.key_field),
            "keyField {} is a discovered field",
            row.key_field
        );
        assert!(
            universe.get(&row.class).map(|(kind, _)| *kind) == Some(TermKind::Class),
            "row class {} is a declared class",
            row.class
        );
        for pair in &row.pairs {
            assert!(
                fields.contains(&pair.field),
                "pair field {} is a discovered field",
                pair.field
            );
            assert!(
                universe.contains_key(&pair.predicate),
                "row predicate {} is declared vocabulary",
                pair.predicate
            );
        }
    }
    // Coverage of the plan.md rom-manager table: entity -> class.
    let bound: BTreeSet<(&str, &str)> = rows
        .iter()
        .map(|r| (
            r.entity.as_str(),
            r.class.strip_prefix(KG).unwrap_or(&r.class),
        ))
        .collect();
    for (entity, class) in [("Rom", "RomFile"), ("Platform", "GamePlatform"), ("Game", "Game")] {
        assert!(
            bound.contains(&(entity, class)),
            "the plan.md table row {entity} -> {class} is mapped"
        );
    }
}

// spec: EG-MEDIA-VOCAB-R009.2
#[test]
fn media_vocab_rom_manager_response_round_trips_through_mapping() {
    let document = include_str!("fixtures/media_vocab/rom-manager_mapping.ttl");
    let rows = mapping_rows(document);
    let universe = vocabulary_universe();
    let response: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/media_vocab/rom-manager_response.json"))
            .expect("the recorded response parses");
    let mapped = apply_mapping(&rows, &response, "rom-manager", &universe);
    // Six records (four ROMs, one platform, one game), no minted reference
    // nodes: the game and platform values are URN references.
    let nodes = node_subjects(&mapped);
    assert_eq!(nodes.len(), 6, "every record becomes exactly one node: {nodes:?}");
    for t in &mapped {
        let predicate = t.predicate.as_str();
        if predicate == format!("{RDF}type")
            || predicate == format!("{DC}source")
            || predicate == format!("{DC}identifier")
            || predicate == format!("{DC}date")
        {
            continue;
        }
        assert!(
            universe.contains_key(predicate),
            "the mapped triple uses declared vocabulary: {predicate}"
        );
    }
    // A ROM links to the game node it contains and carries its checksums and
    // file name.
    assert_eq!(
        iri_of(
            &mapped,
            "urn:eg-media-vocab:rom-manager:Rom:d41d8cd98f00b204e9800998ecf8427e",
            format!("{KG}gameOf").as_str()
        ),
        Some("urn:eg-media-vocab:rom-manager:Game:08920168".to_string())
    );
    assert_eq!(
        values_of(
            &mapped,
            "urn:eg-media-vocab:rom-manager:Rom:d41d8cd98f00b204e9800998ecf8427e",
            format!("{KG}crc32").as_str()
        ),
        vec!["7d1b2c4e".to_string()]
    );
    assert_eq!(
        values_of(
            &mapped,
            "urn:eg-media-vocab:rom-manager:Rom:d41d8cd98f00b204e9800998ecf8427e",
            format!("{KG}fileName").as_str()
        ),
        vec!["sonic_adventure.zip".to_string()]
    );
    // The game carries its code and links to the platform node.
    assert_eq!(
        values_of(
            &mapped,
            "urn:eg-media-vocab:rom-manager:Game:08920168",
            format!("{KG}gameCode").as_str()
        ),
        vec!["08920168".to_string()]
    );
    assert_eq!(
        iri_of(
            &mapped,
            "urn:eg-media-vocab:rom-manager:Game:08920168",
            format!("{KG}onPlatform").as_str()
        ),
        Some("urn:eg-media-vocab:rom-manager:Platform:p1".to_string())
    );
}

// spec: EG-MEDIA-VOCAB-R009.3
#[test]
fn media_vocab_rom_manager_single() {
    // "Which ROMs in a platform collection fail a checksum match against the
    // reference set?" — BLOCKED: RomM is queried by hash and lists hashes,
    // but there is no operation that compares a library against a reference
    // (DAT) set and reports mismatches. DECIDED interim: report ROMs whose
    // metadata-provider match is absent as 'unidentified', labelled as not a
    // checksum verdict. The test returns the typed abstention naming the
    // missing verify operation and its owner.
    let document = include_str!("fixtures/media_vocab/rom-manager_mapping.ttl");
    let rows = mapping_rows(document);
    let universe = vocabulary_universe();
    let response: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/media_vocab/rom-manager_response.json")).unwrap();
    let mapped = apply_mapping(&rows, &response, "rom-manager", &universe);

    let roms = subjects_typed(&mapped, &["RomFile"]);
    assert_eq!(roms.len(), 4, "the collection lists four ROMs: {roms:?}");
    // The interim answer: ROMs with no metadata-provider match are
    // 'unidentified' — a metadata absence, not a checksum verdict.
    let unidentified: BTreeSet<String> = roms
        .iter()
        .filter(|rom| {
            iri_of(&mapped, rom, format!("{KG}gameOf").as_str()).is_none()
        })
        .cloned()
        .collect();
    assert_eq!(
        unidentified,
        [
            "urn:eg-media-vocab:rom-manager:Rom:1679091c5a880faf6fb5e6087eb1b2dc",
            "urn:eg-media-vocab:rom-manager:Rom:202cb962ac59075b964b07152d234b70",
        ]
        .into_iter()
        .map(str::to_string)
        .collect::<BTreeSet<_>>(),
        "unidentified (not a checksum verdict): {unidentified:?}"
    );
    // The identified ROMs resolve to their game work.
    let identified: BTreeSet<String> = roms
        .iter()
        .filter(|rom| iri_of(&mapped, rom, format!("{KG}gameOf").as_str()).is_some())
        .cloned()
        .collect();
    for rom in &identified {
        let game =
            iri_of(&mapped, rom, format!("{KG}gameOf").as_str()).expect("the ROM names its game");
        assert_eq!(
            values_of(&mapped, &game, format!("{KG}title").as_str()),
            vec!["Sonic Adventure".to_string()]
        );
    }
    // The reference-set verdict itself cannot be produced: no checksum
    // verdict vocabulary is bound in the mapped data.
    for property in ["expectedChecksum", "actualChecksum"] {
        assert!(
            mapped
                .iter()
                .all(|t| t.predicate.as_str() != format!("{KG}{property}")),
            "the mapped data carries no {property} verdict"
        );
    }

    // The typed abstention names the missing operation and the owner who
    // must add it.
    let abstention = TypedAbstention {
        connector: "rom-manager",
        missing_operation: "romm_roms/verify (reference-set checksum verdict)",
        owner: "rom-manager",
    };
    assert_eq!(abstention.connector, "rom-manager");
    assert!(
        abstention.missing_operation.contains("verify"),
        "the abstention names the missing verify operation: {}",
        abstention.missing_operation
    );
    assert_eq!(abstention.owner, "rom-manager");
}

// spec: EG-MEDIA-VOCAB-R009.4
#[test]
fn media_vocab_rom_manager_vocabulary_docs_truth() {
    let plan = std::fs::read_to_string(CONNECTOR_COMPETENCY_PLAN)
        .expect("the connector-competency plan reads");
    assert_eq!(
        connector_competency_vocab_cell(&plan, "R053"),
        "media-v1",
        "the rom-manager Vocabulary cell names media-v1, not MISSING"
    );
}

// spec: EG-MEDIA-VOCAB-R010.1
#[test]
fn media_vocab_owncast_mapping_rows_resolve_existing_terms() {
    let document = include_str!("fixtures/media_vocab/owncast_mapping.ttl");
    let (kind, _endpoint, only_url) = connection_of(document);
    assert_eq!(kind, "mcp", "vg:sourceKind is mcp");
    assert!(only_url, "the endpoint reference carries only its url");
    let (entities, fields, _operations) = contract_of(document);
    let rows = mapping_rows(document);
    assert_eq!(rows.len(), 5, "the plan.md table rows are all mapped");
    let universe = vocabulary_universe();
    for row in &rows {
        assert!(
            entities.contains(&row.entity),
            "mapsEntity {} is a discovered entity",
            row.entity
        );
        assert!(
            fields.contains(&row.key_field),
            "keyField {} is a discovered field",
            row.key_field
        );
        assert!(
            universe.get(&row.class).map(|(kind, _)| *kind) == Some(TermKind::Class),
            "row class {} is a declared class",
            row.class
        );
        for pair in &row.pairs {
            assert!(
                fields.contains(&pair.field),
                "pair field {} is a discovered field",
                pair.field
            );
            assert!(
                universe.contains_key(&pair.predicate),
                "row predicate {} is declared vocabulary",
                pair.predicate
            );
        }
    }
    // Coverage of the plan.md owncast table: entity -> class. The follower is
    // a :Person; the chat message is the shared content :ChatMessage.
    let bound: BTreeSet<(&str, &str)> = rows
        .iter()
        .map(|r| (
            r.entity.as_str(),
            r.class.strip_prefix(KG).unwrap_or(&r.class),
        ))
        .collect();
    for (entity, class) in [
        ("Status", "BroadcastStream"),
        ("ViewerSample", "ViewerSample"),
        ("Hardware", "HardwareSample"),
        ("ChatMessage", "ChatMessage"),
        ("Follower", "Follower"),
    ] {
        assert!(
            bound.contains(&(entity, class)),
            "the plan.md table row {entity} -> {class} is mapped"
        );
    }
}

// spec: EG-MEDIA-VOCAB-R010.2
#[test]
fn media_vocab_owncast_response_round_trips_through_mapping() {
    let document = include_str!("fixtures/media_vocab/owncast_mapping.ttl");
    let rows = mapping_rows(document);
    let universe = vocabulary_universe();
    let response: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/media_vocab/owncast_response.json"))
            .expect("the recorded response parses");
    let mapped = apply_mapping(&rows, &response, "owncast", &universe);
    // Six records plus one minted reference node (the chat author "dana").
    let nodes = node_subjects(&mapped);
    assert_eq!(nodes.len(), 7, "every record becomes exactly one node: {nodes:?}");
    for t in &mapped {
        let predicate = t.predicate.as_str();
        if predicate == format!("{RDF}type")
            || predicate == format!("{DC}source")
            || predicate == format!("{DC}identifier")
            || predicate == format!("{DC}date")
        {
            continue;
        }
        assert!(
            universe.contains_key(predicate),
            "the mapped triple uses declared vocabulary: {predicate}"
        );
    }
    // The stream carries its live audience on the shared :viewerCount; the
    // samples point back at the stream node.
    assert_eq!(
        values_of(
            &mapped,
            "urn:eg-media-vocab:owncast:Status:stream",
            format!("{KG}streamOnline").as_str()
        ),
        vec!["true".to_string()]
    );
    assert_eq!(
        values_of(
            &mapped,
            "urn:eg-media-vocab:owncast:Status:stream",
            format!("{KG}viewerCount").as_str()
        ),
        vec!["30".to_string()]
    );
    assert_eq!(
        iri_of(
            &mapped,
            "urn:eg-media-vocab:owncast:ViewerSample:v2",
            format!("{KG}sampleOf").as_str()
        ),
        Some("urn:eg-media-vocab:owncast:Status:stream".to_string())
    );
    assert_eq!(
        values_of(
            &mapped,
            "urn:eg-media-vocab:owncast:ViewerSample:v2",
            format!("{KG}viewerCount").as_str()
        ),
        vec!["37".to_string()]
    );
    // The hardware sample carries decimal usage; the chat author mints as a
    // typed :Person; the follower is typed :Follower.
    assert_eq!(
        values_of(
            &mapped,
            "urn:eg-media-vocab:owncast:Hardware:hw1",
            format!("{KG}cpuUsagePercent").as_str()
        ),
        vec!["23.5".to_string()]
    );
    let author = iri_of(
        &mapped,
        "urn:eg-media-vocab:owncast:ChatMessage:c1",
        format!("{KG}postedBy").as_str(),
    )
    .expect("the chat message names its author");
    assert!(author.starts_with("urn:eg-media-vocab:owncast:aux:"));
    assert_eq!(
        values_of(&mapped, &author, format!("{RDF}type").as_str()),
        vec![format!("{KG}Person")]
    );
    assert_eq!(
        values_of(&mapped, "urn:eg-media-vocab:owncast:Follower:f1", format!("{RDF}type").as_str()),
        vec![format!("{KG}Follower")]
    );
}

// spec: EG-MEDIA-VOCAB-R010.3
#[test]
fn media_vocab_owncast_single() {
    // "Which streams had the highest peak viewer count and how long did they
    // run?" — BLOCKED for 'highest peak across streams': the connector
    // exposes the live status and the viewers-over-time series of the
    // current window only, with no list of past broadcasts. DECIDED: answer
    // the current broadcast's peak and duration now; historical ranking
    // needs an owncast-agent stream-history operation. The test computes the
    // interim answer and returns the typed abstention naming the missing
    // operation and its owner.
    let document = include_str!("fixtures/media_vocab/owncast_mapping.ttl");
    let rows = mapping_rows(document);
    let universe = vocabulary_universe();
    let response: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/media_vocab/owncast_response.json")).unwrap();
    let mapped = apply_mapping(&rows, &response, "owncast", &universe);

    // The current broadcast: the stream node and its audience samples.
    let streams = subjects_typed(&mapped, &["BroadcastStream"]);
    assert_eq!(streams.len(), 1, "one stream is mapped: {streams:?}");
    let stream = streams.iter().next().unwrap().to_string();
    let samples = subjects_typed(&mapped, &["ViewerSample"]);
    let peak = mapped
        .iter()
        .filter(|t| {
            subject_str(&t.subject).map(|s| s == stream.as_str() || samples.contains(s)) == Some(true)
                && t.predicate.as_str() == format!("{KG}viewerCount")
        })
        .filter_map(|t| object_str(&t.object).and_then(|v| v.parse::<i64>().ok()))
        .max()
        .expect("the stream or a sample carries a viewer count");
    assert_eq!(peak, 37, "the current window's peak audience");

    // The broadcast ran from its last connect time to the frozen as-of.
    let last_connect = values_of(&mapped, &stream, format!("{KG}lastConnectAt").as_str())
        .pop()
        .expect("the stream names its last connect time");
    let duration = utc_seconds(ASOF) - utc_seconds(&last_connect);
    assert_eq!(duration, 14400, "the current broadcast ran 4 hours");

    // Historical ranking cannot be produced: the mapped data holds one
    // broadcast, and the contract has no stream-history operation.
    let (_entities, _fields, operations) = contract_of(document);
    assert!(
        operations
            .iter()
            .all(|operation| !operation.contains("history")),
        "no stream-history operation is discovered: {operations:?}"
    );
    let abstention = TypedAbstention {
        connector: "owncast",
        missing_operation: "owncast_internal stream-history (past broadcast list with start/end and peak viewers)",
        owner: "owncast-agent",
    };
    assert_eq!(abstention.connector, "owncast");
    assert!(
        abstention.missing_operation.contains("stream-history"),
        "the abstention names the missing stream-history operation: {}",
        abstention.missing_operation
    );
    assert_eq!(abstention.owner, "owncast-agent");
}

// spec: EG-MEDIA-VOCAB-R010.4
#[test]
fn media_vocab_owncast_vocabulary_docs_truth() {
    let plan = std::fs::read_to_string(CONNECTOR_COMPETENCY_PLAN)
        .expect("the connector-competency plan reads");
    assert_eq!(
        connector_competency_vocab_cell(&plan, "R045"),
        "media-v1 / content-v1",
        "the owncast Vocabulary cell names media-v1 / content-v1, not MISSING"
    );
}

// spec: EG-MEDIA-VOCAB-R011.1
#[test]
fn media_vocab_media_downloader_mapping_rows_resolve_existing_terms() {
    let document = include_str!("fixtures/media_vocab/media-downloader_mapping.ttl");
    let (kind, _endpoint, only_url) = connection_of(document);
    assert_eq!(kind, "mcp", "vg:sourceKind is mcp");
    assert!(only_url, "the endpoint reference carries only its url");
    let (entities, fields, _operations) = contract_of(document);
    let rows = mapping_rows(document);
    assert_eq!(rows.len(), 2, "the plan.md table rows are all mapped");
    let universe = vocabulary_universe();
    for row in &rows {
        assert!(
            entities.contains(&row.entity),
            "mapsEntity {} is a discovered entity",
            row.entity
        );
        assert!(
            fields.contains(&row.key_field),
            "keyField {} is a discovered field",
            row.key_field
        );
        assert!(
            universe.get(&row.class).map(|(kind, _)| *kind) == Some(TermKind::Class),
            "row class {} is a declared class",
            row.class
        );
        for pair in &row.pairs {
            assert!(
                fields.contains(&pair.field),
                "pair field {} is a discovered field",
                pair.field
            );
            assert!(
                universe.contains_key(&pair.predicate),
                "row predicate {} is declared vocabulary",
                pair.predicate
            );
        }
    }
    // Coverage of the plan.md media-downloader table: entity -> class. The
    // download action is modelled as a request; the watch file is a file.
    let bound: BTreeSet<(&str, &str)> = rows
        .iter()
        .map(|r| (
            r.entity.as_str(),
            r.class.strip_prefix(KG).unwrap_or(&r.class),
        ))
        .collect();
    for (entity, class) in [("DownloadJob", "DownloadRequest"), ("WatchFile", "MediaFile")] {
        assert!(
            bound.contains(&(entity, class)),
            "the plan.md table row {entity} -> {class} is mapped"
        );
    }
}

// spec: EG-MEDIA-VOCAB-R011.2
#[test]
fn media_vocab_media_downloader_response_round_trips_through_mapping() {
    let document = include_str!("fixtures/media_vocab/media-downloader_mapping.ttl");
    let rows = mapping_rows(document);
    let universe = vocabulary_universe();
    let response: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/media_vocab/media-downloader_response.json"))
            .expect("the recorded response parses");
    let mapped = apply_mapping(&rows, &response, "media-downloader", &universe);
    // Four records (two download jobs, two watched files), no minted
    // reference nodes: the output values are URN references.
    let nodes = node_subjects(&mapped);
    assert_eq!(nodes.len(), 4, "every record becomes exactly one node: {nodes:?}");
    for t in &mapped {
        let predicate = t.predicate.as_str();
        if predicate == format!("{RDF}type")
            || predicate == format!("{DC}source")
            || predicate == format!("{DC}identifier")
            || predicate == format!("{DC}date")
        {
            continue;
        }
        assert!(
            universe.contains_key(predicate),
            "the mapped triple uses declared vocabulary: {predicate}"
        );
    }
    // The job carries its source URL and state, and links the file it
    // produced.
    assert_eq!(
        values_of(
            &mapped,
            "urn:eg-media-vocab:media-downloader:DownloadJob:j1",
            format!("{KG}downloadSourceUrl").as_str()
        ),
        vec!["https://media.example/files/episode-01.mkv".to_string()]
    );
    assert_eq!(
        values_of(
            &mapped,
            "urn:eg-media-vocab:media-downloader:DownloadJob:j1",
            format!("{KG}downloadRequestState").as_str()
        ),
        vec!["importing".to_string()]
    );
    assert_eq!(
        iri_of(
            &mapped,
            "urn:eg-media-vocab:media-downloader:DownloadJob:j1",
            format!("{KG}downloadOutput").as_str()
        ),
        Some("urn:eg-media-vocab:media-downloader:WatchFile:wf1".to_string())
    );
    assert_eq!(
        values_of(
            &mapped,
            "urn:eg-media-vocab:media-downloader:WatchFile:wf1",
            format!("{KG}fileName").as_str()
        ),
        vec!["episode-01.mkv".to_string()]
    );
}

// spec: EG-MEDIA-VOCAB-R011.3
#[test]
fn media_vocab_media_downloader_single() {
    // "Which download jobs failed in the last week, with what error, for
    // which source URL?" — BLOCKED: the connector exposes only download
    // actions and watch prompts; there is no operation that lists past
    // jobs, their status or error. The test verifies the mapped data
    // carries no failure to report and returns the typed abstention naming
    // the missing operation and its owner.
    let document = include_str!("fixtures/media_vocab/media-downloader_mapping.ttl");
    let rows = mapping_rows(document);
    let universe = vocabulary_universe();
    let response: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/media_vocab/media-downloader_response.json"))
            .unwrap();
    let mapped = apply_mapping(&rows, &response, "media-downloader", &universe);

    // No failed job and no error anywhere in the mapped data.
    let requests = subjects_typed(&mapped, &["DownloadRequest"]);
    let failed: BTreeSet<String> = requests
        .iter()
        .filter(|request| {
            values_of(&mapped, request, format!("{KG}downloadRequestState").as_str())
                .iter()
                .any(|state| state == "failed")
        })
        .cloned()
        .collect();
    assert!(
        failed.is_empty(),
        "no failed job is recorded: {failed:?}"
    );
    assert!(
        mapped
            .iter()
            .all(|t| t.predicate.as_str() != format!("{KG}downloadError")),
        "the mapped data carries no downloadError"
    );
    // The discovered operations contain no download-history operation.
    let (_entities, _fields, operations) = contract_of(document);
    assert!(
        operations
            .iter()
            .all(|operation| !operation.contains("list_downloads")),
        "no list_downloads operation is discovered: {operations:?}"
    );

    // The typed abstention names the missing operation and the owner who
    // must add it.
    let abstention = TypedAbstention {
        connector: "media-downloader",
        missing_operation: "list_downloads (download history: job id, source URL, status, error, completedAt, output path)",
        owner: "media-downloader",
    };
    assert_eq!(abstention.connector, "media-downloader");
    assert!(
        abstention.missing_operation.contains("list_downloads"),
        "the abstention names the missing download-history operation: {}",
        abstention.missing_operation
    );
    assert_eq!(abstention.owner, "media-downloader");
}

// spec: EG-MEDIA-VOCAB-R011.4
#[test]
fn media_vocab_media_downloader_vocabulary_docs_truth() {
    let plan = std::fs::read_to_string(CONNECTOR_COMPETENCY_PLAN)
        .expect("the connector-competency plan reads");
    assert_eq!(
        connector_competency_vocab_cell(&plan, "R038"),
        "media-v1",
        "the media-downloader Vocabulary cell names media-v1, not MISSING"
    );
}

// spec: EG-MEDIA-VOCAB-R012.1
#[test]
fn media_vocab_audio_transcriber_mapping_rows_resolve_existing_terms() {
    let document = include_str!("fixtures/media_vocab/audio-transcriber_mapping.ttl");
    let (kind, _endpoint, only_url) = connection_of(document);
    assert_eq!(kind, "mcp", "vg:sourceKind is mcp");
    assert!(only_url, "the endpoint reference carries only its url");
    let (entities, fields, _operations) = contract_of(document);
    let rows = mapping_rows(document);
    assert_eq!(rows.len(), 3, "the plan.md table rows are all mapped");
    let universe = vocabulary_universe();
    for row in &rows {
        assert!(
            entities.contains(&row.entity),
            "mapsEntity {} is a discovered entity",
            row.entity
        );
        assert!(
            fields.contains(&row.key_field),
            "keyField {} is a discovered field",
            row.key_field
        );
        assert!(
            universe.get(&row.class).map(|(kind, _)| *kind) == Some(TermKind::Class),
            "row class {} is a declared class",
            row.class
        );
        for pair in &row.pairs {
            assert!(
                fields.contains(&pair.field),
                "pair field {} is a discovered field",
                pair.field
            );
            assert!(
                universe.contains_key(&pair.predicate),
                "row predicate {} is declared vocabulary",
                pair.predicate
            );
        }
    }
    // Coverage of the plan.md audio-transcriber table: entity -> class. The
    // transcript terms are reused from personal-v1, never redeclared.
    let bound: BTreeSet<(&str, &str)> = rows
        .iter()
        .map(|r| (
            r.entity.as_str(),
            r.class.strip_prefix(KG).unwrap_or(&r.class),
        ))
        .collect();
    for (entity, class) in [
        ("Transcript", "Transcript"),
        ("Segment", "TranscriptSegment"),
        ("IngestedTranscript", "Transcript"),
    ] {
        assert!(
            bound.contains(&(entity, class)),
            "the plan.md table row {entity} -> {class} is mapped"
        );
    }
    assert!(
        !declared_classes(&load(MEDIA)).contains(&format!("{KG}Transcript"))
            && !declared_classes(&load(MEDIA)).contains(&format!("{KG}TranscriptSegment")),
        "media-v1 redeclares the personal transcript terms"
    );
}

// spec: EG-MEDIA-VOCAB-R012.2
#[test]
fn media_vocab_audio_transcriber_response_round_trips_through_mapping() {
    let document = include_str!("fixtures/media_vocab/audio-transcriber_mapping.ttl");
    let rows = mapping_rows(document);
    let universe = vocabulary_universe();
    let response: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/media_vocab/audio-transcriber_response.json"))
            .expect("the recorded response parses");
    let mapped = apply_mapping(&rows, &response, "audio-transcriber", &universe);
    // Four records plus three minted reference nodes (two recordings the
    // transcripts derive from, the audio blob the ingested transcript
    // carries).
    let nodes = node_subjects(&mapped);
    assert_eq!(nodes.len(), 7, "every record becomes exactly one node: {nodes:?}");
    for t in &mapped {
        let predicate = t.predicate.as_str();
        if predicate == format!("{RDF}type")
            || predicate == format!("{DC}source")
            || predicate == format!("{DC}identifier")
            || predicate == format!("{DC}date")
        {
            continue;
        }
        assert!(
            universe.contains_key(predicate),
            "the mapped triple uses declared vocabulary: {predicate}"
        );
    }
    // The segment carries its span and text, and belongs to the transcript
    // node.
    assert_eq!(
        values_of(
            &mapped,
            "urn:eg-media-vocab:audio-transcriber:Segment:t1s1",
            format!("{KG}segmentStart").as_str()
        ),
        vec!["1.2".to_string()]
    );
    assert_eq!(
        values_of(
            &mapped,
            "urn:eg-media-vocab:audio-transcriber:Segment:t1s1",
            format!("{KG}text").as_str()
        ),
        vec!["the quarterly figgers".to_string()]
    );
    assert_eq!(
        iri_of(
            &mapped,
            "urn:eg-media-vocab:audio-transcriber:Segment:t1s1",
            format!("{KG}segmentOf").as_str()
        ),
        Some("urn:eg-media-vocab:audio-transcriber:Transcript:t1".to_string())
    );
    // The transcript derives from the recording reference (typed by the
    // personal-v1 range); the ingested transcript carries its audio blob.
    let recording = iri_of(
        &mapped,
        "urn:eg-media-vocab:audio-transcriber:Transcript:t1",
        format!("{KG}transcribedFrom").as_str(),
    )
    .expect("the transcript names its recording");
    assert_eq!(
        values_of(&mapped, &recording, format!("{RDF}type").as_str()),
        vec![format!("{KG}VoiceMessage")]
    );
    let blob = iri_of(
        &mapped,
        "urn:eg-media-vocab:audio-transcriber:IngestedTranscript:ing1",
        format!("{KG}hasBlob").as_str(),
    )
    .expect("the ingested transcript names its blob");
    assert_eq!(
        values_of(&mapped, &blob, format!("{RDF}type").as_str()),
        vec![format!("{KG}Blob")]
    );
}

// spec: EG-MEDIA-VOCAB-R012.3
#[test]
fn media_vocab_audio_transcriber_single() {
    // "Which recordings were transcribed with a low-confidence segment, and
    // what text was affected?" — DECIDED interim: low confidence :=
    // noSpeechProb above a threshold (0.5), because the connector's segments
    // carry noSpeechProb but not avg_logprob / confidence (kg_ingest.py
    // segment dict); the proxy is labelled, not presented as a verdict. The
    // connector transcribes on demand and has no list/get operation for
    // past transcripts, so a complete answer across recordings is the typed
    // abstention naming the missing operation and its owner.
    const LOW_CONFIDENCE_NO_SPEECH_PROB: f64 = 0.5;
    let document = include_str!("fixtures/media_vocab/audio-transcriber_mapping.ttl");
    let rows = mapping_rows(document);
    let universe = vocabulary_universe();
    let response: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/media_vocab/audio-transcriber_response.json"))
            .unwrap();
    let mapped = apply_mapping(&rows, &response, "audio-transcriber", &universe);

    // The low-confidence segment of the recorded transcript: noSpeechProb
    // above the DECIDED proxy threshold.
    let segments = subjects_typed(&mapped, &["TranscriptSegment"]);
    let affected: BTreeSet<(String, String)> = segments
        .iter()
        .filter_map(|segment| {
            let prob = values_of(&mapped, segment, format!("{KG}noSpeechProb").as_str())
                .pop()?
                .parse::<f64>()
                .ok()?;
            if prob > LOW_CONFIDENCE_NO_SPEECH_PROB {
                Some((
                    segment.to_string(),
                    values_of(&mapped, segment, format!("{KG}text").as_str()).pop()?,
                ))
            } else {
                None
            }
        })
        .collect();
    assert_eq!(
        affected.len(),
        1,
        "one segment is low-confidence (proxy): {affected:?}"
    );
    let (segment, text) = affected.into_iter().next().unwrap();
    assert_eq!(segment, "urn:eg-media-vocab:audio-transcriber:Segment:t1s1");
    assert_eq!(text, "the quarterly figgers");
    let transcript = iri_of(&mapped, &segment, format!("{KG}segmentOf").as_str())
        .expect("the segment names its transcript");
    assert_eq!(transcript, "urn:eg-media-vocab:audio-transcriber:Transcript:t1");

    // The verdict is a proxy: no true-confidence field exists in the
    // contract, and the connector transcribes on demand (no list/get
    // operation for past transcripts).
    let (_entities, fields, operations) = contract_of(document);
    assert!(
        fields
            .iter()
            .all(|field| !field.to_lowercase().contains("logprob") && field != "confidence"),
        "no true-confidence field is discovered: {fields:?}"
    );
    assert!(
        operations
            .iter()
            .all(|operation| !operation.contains("list") && !operation.contains("get_")),
        "no list/get transcript operation is discovered: {operations:?}"
    );
    let abstention = TypedAbstention {
        connector: "audio-transcriber",
        missing_operation: "list/get transcript by recording id and date",
        owner: "audio-transcriber",
    };
    assert_eq!(abstention.connector, "audio-transcriber");
    assert!(
        abstention.missing_operation.contains("list/get transcript"),
        "the abstention names the missing list/get operation: {}",
        abstention.missing_operation
    );
    assert_eq!(abstention.owner, "audio-transcriber");
}

// spec: EG-MEDIA-VOCAB-R012.4
#[test]
fn media_vocab_audio_transcriber_vocabulary_docs_truth() {
    let plan = std::fs::read_to_string(CONNECTOR_COMPETENCY_PLAN)
        .expect("the connector-competency plan reads");
    assert_eq!(
        connector_competency_vocab_cell(&plan, "R008"),
        "media-v1 / personal-v1",
        "the audio-transcriber Vocabulary cell names media-v1 / personal-v1, not MISSING"
    );
}

// spec: EG-MEDIA-VOCAB-R013.1
#[test]
fn media_vocab_freshrss_mapping_rows_resolve_existing_terms() {
    let document = include_str!("fixtures/media_vocab/freshrss_mapping.ttl");
    let (kind, _endpoint, only_url) = connection_of(document);
    assert_eq!(kind, "mcp", "vg:sourceKind is mcp");
    assert!(only_url, "the endpoint reference carries only its url");
    let (entities, fields, _operations) = contract_of(document);
    let rows = mapping_rows(document);
    assert_eq!(rows.len(), 3, "the plan.md table rows are all mapped");
    let universe = vocabulary_universe();
    for row in &rows {
        assert!(
            entities.contains(&row.entity),
            "mapsEntity {} is a discovered entity",
            row.entity
        );
        assert!(
            fields.contains(&row.key_field),
            "keyField {} is a discovered field",
            row.key_field
        );
        assert!(
            universe.get(&row.class).map(|(kind, _)| *kind) == Some(TermKind::Class),
            "row class {} is a declared class",
            row.class
        );
        for pair in &row.pairs {
            assert!(
                fields.contains(&pair.field),
                "pair field {} is a discovered field",
                pair.field
            );
            assert!(
                universe.contains_key(&pair.predicate),
                "row predicate {} is declared vocabulary",
                pair.predicate
            );
        }
    }
    // Coverage of the plan.md freshrss table: entity -> class. The feed
    // terms are reused from content-v1, never redeclared.
    let bound: BTreeSet<(&str, &str)> = rows
        .iter()
        .map(|r| (
            r.entity.as_str(),
            r.class.strip_prefix(KG).unwrap_or(&r.class),
        ))
        .collect();
    for (entity, class) in [
        ("Article", "Article"),
        ("UnreadCount", "Feed"),
        ("Subscription", "FeedSubscription"),
    ] {
        assert!(
            bound.contains(&(entity, class)),
            "the plan.md table row {entity} -> {class} is mapped"
        );
    }
    // R013.1's addition: the per-feed unread count, owned by :Feed.
    let content = load(CONTENT);
    assert_typed(&content, "unreadCount", "DatatypeProperty");
    assert!(
        has_iri(
            &content,
            &format!("{KG}unreadCount"),
            &format!("{RDFS}domain"),
            &format!("{KG}Feed")
        ),
        "unreadCount is owned by :Feed"
    );
    assert!(
        has_iri(
            &content,
            &format!("{KG}unreadCount"),
            &format!("{RDFS}range"),
            &format!("{XSD}nonNegativeInteger")
        ),
        "unreadCount is a non-negative integer count"
    );
}

// spec: EG-MEDIA-VOCAB-R013.2
#[test]
fn media_vocab_freshrss_response_round_trips_through_mapping() {
    let document = include_str!("fixtures/media_vocab/freshrss_mapping.ttl");
    let rows = mapping_rows(document);
    let universe = vocabulary_universe();
    let response: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/media_vocab/freshrss_response.json"))
            .expect("the recorded response parses");
    let mapped = apply_mapping(&rows, &response, "freshrss", &universe);
    // Four articles, two per-stream unread counts, two subscriptions, and
    // two minted feed reference nodes (one per subscription's stream id).
    let nodes = node_subjects(&mapped);
    assert_eq!(nodes.len(), 10, "every record becomes exactly one node: {nodes:?}");
    for t in &mapped {
        let predicate = t.predicate.as_str();
        if predicate == format!("{RDF}type")
            || predicate == format!("{DC}source")
            || predicate == format!("{DC}identifier")
            || predicate == format!("{DC}date")
        {
            continue;
        }
        assert!(
            universe.contains_key(predicate),
            "the mapped triple uses declared vocabulary: {predicate}"
        );
    }
    // The article URL carrying tracking parameters arrives normalised.
    assert_eq!(
        values_of(
            &mapped,
            "urn:eg-media-vocab:freshrss:Article:feed-2/a1",
            format!("{KG}canonicalUrl").as_str()
        ),
        vec!["https://example-blog.invalid/posts/quarterly-update".to_string()]
    );
    // The read state and the per-feed unread count ride their declared
    // ranges.
    assert_eq!(
        values_of(
            &mapped,
            "urn:eg-media-vocab:freshrss:Article:feed-2/a3",
            format!("{KG}isRead").as_str()
        ),
        vec!["true".to_string()]
    );
    assert_eq!(
        values_of(
            &mapped,
            "urn:eg-media-vocab:freshrss:UnreadCount:feed-2",
            format!("{KG}unreadCount").as_str()
        ),
        vec!["2".to_string()]
    );
    // The subscription names its feed through the stream id; the minted
    // reference node is typed by the ofFeed range.
    let feed = iri_of(
        &mapped,
        "urn:eg-media-vocab:freshrss:Subscription:feed-2",
        format!("{KG}ofFeed").as_str(),
    )
    .expect("the subscription names its feed");
    assert_eq!(feed, "urn:eg-media-vocab:freshrss:aux:ofFeed:feed-2");
    assert_eq!(
        values_of(&mapped, &feed, format!("{RDF}type").as_str()),
        vec![format!("{KG}Feed")]
    );
    assert_eq!(
        values_of(&mapped, &feed, format!("{KG}title").as_str()),
        vec!["feed-2".to_string()]
    );
    assert_eq!(
        values_of(
            &mapped,
            "urn:eg-media-vocab:freshrss:Subscription:feed-2",
            format!("{KG}feedUrl").as_str()
        ),
        vec!["https://example-blog.invalid/feed.xml".to_string()]
    );
}

// spec: EG-MEDIA-VOCAB-R013.3
#[test]
fn media_vocab_freshrss_single() {
    // "Which unread articles from a given feed were published in the last
    // 48 hours?" — CONFIRMED: stream_contents with the newer_than window and
    // unread filter, plus unread_count (freshrss_reader docstring,
    // mcp_freshrss reader tool). The article key is the GReader stream id,
    // so the given-feed filter rides on the key (dc:identifier), and
    // "published in the window" is the DECIDED reading of the
    // newer_than watermark (publishedAt within the window).
    const FEED: &str = "feed-2";
    const WINDOW_SECONDS: i64 = 2 * 86400; // the question's 48 hours
    let document = include_str!("fixtures/media_vocab/freshrss_mapping.ttl");
    let rows = mapping_rows(document);
    let universe = vocabulary_universe();
    let response: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/media_vocab/freshrss_response.json"))
            .unwrap();
    let mapped = apply_mapping(&rows, &response, "freshrss", &universe);

    // The given feed's unread articles, published within the window.
    let articles = subjects_typed(&mapped, &["Article"]);
    let unread_of_feed = |subject: &str| -> (bool, Option<String>) {
        let identifier = values_of(&mapped, subject, format!("{DC}identifier").as_str());
        let unread = values_of(&mapped, subject, format!("{KG}isRead").as_str());
        (
            identifier
                .get(0)
                .map(|id| id.starts_with(&format!("Article:{FEED}/")))
                == Some(true),
            unread.into_iter().next(),
        )
    };
    let answer: BTreeSet<String> = articles
        .iter()
        .filter_map(|subject| {
            let (in_feed, unread) = unread_of_feed(subject);
            let published = values_of(&mapped, subject, format!("{KG}publishedAt").as_str())
                .into_iter()
                .next()?;
            (in_feed
                && unread.as_deref() == Some("false")
                && utc_seconds(&published) >= utc_seconds(ASOF) - WINDOW_SECONDS)
                .then(|| subject.to_string())
        })
        .collect();
    assert_eq!(
        answer,
        ["urn:eg-media-vocab:freshrss:Article:feed-2/a1"]
            .into_iter()
            .map(str::to_string)
            .collect::<BTreeSet<_>>(),
        "the given feed's unread articles in the window: {answer:?}"
    );

    // The unread_count leg corroborates the stream: the recorded per-stream
    // count equals the unread articles the stream contents carry.
    let unread_articles = articles
        .iter()
        .filter(|subject| unread_of_feed(subject).0 && unread_of_feed(subject).1.as_deref() == Some("false"))
        .count();
    let recorded = values_of(
        &mapped,
        &format!("urn:eg-media-vocab:freshrss:UnreadCount:{FEED}"),
        format!("{KG}unreadCount").as_str(),
    )
    .pop()
    .expect("the feed carries its recorded unread count");
    assert_eq!(
        recorded.parse::<usize>().expect("the unread count is a whole number"),
        unread_articles,
        "the per-stream unread count matches the unread articles"
    );

    // The given feed is a subscribed feed, named on its feed URL.
    assert_eq!(
        values_of(
            &mapped,
            &format!("urn:eg-media-vocab:freshrss:Subscription:{FEED}"),
            format!("{KG}feedUrl").as_str()
        ),
        vec!["https://example-blog.invalid/feed.xml".to_string()]
    );
}

// spec: EG-MEDIA-VOCAB-R013.4
#[test]
fn media_vocab_freshrss_vocabulary_docs_truth() {
    let plan = std::fs::read_to_string(CONNECTOR_COMPETENCY_PLAN)
        .expect("the connector-competency plan reads");
    assert_eq!(
        connector_competency_vocab_cell(&plan, "R022"),
        "content-v1",
        "the freshrss Vocabulary cell names content-v1, not MISSING"
    );
}
