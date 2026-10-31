//! SHACL pass and fail fixtures for the media and content vocabulary shapes
//! (MEDULLA-MEDIA-VOCAB-R004.1 to R004.4, plus the R003.3 subscriber identity
//! shape). Every fixture is validated through the native SHACL Core engine;
//! the shapes are aggregate-free per the virtual-graph shapes convention.

use medulla_shacl::validate_turtle;

const MEDIA_SHAPES: &str = include_str!("../../medulla-core/ontology/media-v1.shapes.ttl");
const CONTENT_SHAPES: &str = include_str!("../../medulla-core/ontology/content-v1.shapes.ttl");

const PREFIXES: &str =
    "@prefix : <http://knuckles.team/kg#> . @prefix dc: <http://purl.org/dc/terms/> . ";

fn conforms(shapes: &str, data: &str) -> bool {
    validate_turtle(shapes, &format!("{PREFIXES}{data}"))
        .expect("the fixtures parse")
        .conforms
}

// spec: MEDULLA-MEDIA-VOCAB-R004.1
#[test]
fn connector_sourced_works_carry_a_provider_key() {
    // A connector-sourced film with one key conforms...
    assert!(conforms(
        MEDIA_SHAPES,
        "<urn:f:1> a :Film ; :tmdbId \"603\" ; dc:source \"radarr\" ."
    ));
    // ...as does a series carrying only the TVDB key...
    assert!(conforms(
        MEDIA_SHAPES,
        "<urn:s:1> a :TelevisionSeries ; :tvdbId \"81190\" ; dc:source \"sonarr\" ."
    ));
    // ...and a hand-entered work without provenance stays exempt.
    assert!(conforms(
        MEDIA_SHAPES,
        "<urn:h:1> a :Film ; :imdbId \"tt0111161\" ."
    ));
    // A connector-sourced work with no provider key at all is refused.
    assert!(!conforms(
        MEDIA_SHAPES,
        "<urn:f:2> a :Film ; dc:source \"radarr\" ."
    ));
    assert!(!conforms(
        MEDIA_SHAPES,
        "<urn:s:2> a :TelevisionSeries ; dc:source \"sonarr\" ."
    ));
}

// spec: MEDULLA-MEDIA-VOCAB-R004.2
#[test]
fn download_transfer_carries_one_lower_case_hex_info_hash() {
    assert!(conforms(
        MEDIA_SHAPES,
        "<urn:t:1> a :DownloadTransfer ; :infoHash \"a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0\" ."
    ));
    assert!(conforms(
        MEDIA_SHAPES,
        "<urn:t:2> a :DownloadTransfer ; :infoHash \"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\" ."
    ));
    // Uppercase, a wrong length, and a duplicate hash are all refused.
    assert!(!conforms(
        MEDIA_SHAPES,
        "<urn:t:3> a :DownloadTransfer ; :infoHash \"A1B2C3D4E5F6A7B8C9D0E1F2A3B4C5D6E7F8A9B0\" ."
    ));
    assert!(!conforms(
        MEDIA_SHAPES,
        "<urn:t:4> a :DownloadTransfer ; :infoHash \"a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b\" ."
    ));
    assert!(!conforms(
        MEDIA_SHAPES,
        "<urn:t:5> a :DownloadTransfer ; :infoHash \"a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0\", \"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855\" ."
    ));
    assert!(!conforms(MEDIA_SHAPES, "<urn:t:6> a :DownloadTransfer ."));
}

// spec: MEDULLA-MEDIA-VOCAB-R004.3
#[test]
fn download_request_state_is_a_closed_set() {
    for state in [
        "requested",
        "wanted",
        "grabbed",
        "importing",
        "imported",
        "failed",
    ] {
        assert!(
            conforms(
                MEDIA_SHAPES,
                &format!(
                    "<urn:r:{state}> a :DownloadRequest ; :downloadRequestState \"{state}\" ."
                )
            ),
            "state {state} is in the closed set"
        );
    }
    assert!(!conforms(
        MEDIA_SHAPES,
        "<urn:r:other> a :DownloadRequest ; :downloadRequestState \"archived\" ."
    ));
    assert!(!conforms(
        MEDIA_SHAPES,
        "<urn:r:two> a :DownloadRequest ; :downloadRequestState \"wanted\", \"imported\" ."
    ));
}

// spec: MEDULLA-MEDIA-VOCAB-R004.4
#[test]
fn article_and_result_carry_exactly_one_canonical_url() {
    assert!(conforms(
        CONTENT_SHAPES,
        "<urn:a:1> a :Article ; :canonicalUrl \"https://example.com/story\" ."
    ));
    assert!(conforms(
        CONTENT_SHAPES,
        "<urn:r:1> a :SearchResult ; :canonicalUrl \"https://example.com/story\" ."
    ));
    assert!(!conforms(
        CONTENT_SHAPES,
        "<urn:a:2> a :Article ; :canonicalUrl \"https://example.com/story\", \"https://example.com/story/\" ."
    ));
    assert!(!conforms(CONTENT_SHAPES, "<urn:a:3> a :Article ."));
    assert!(!conforms(
        CONTENT_SHAPES,
        "<urn:r:2> a :SearchResult ; :canonicalUrl \"https://example.com/story\", \"https://example.com/other\" ."
    ));
    assert!(!conforms(CONTENT_SHAPES, "<urn:r:3> a :SearchResult ."));
}

// spec: MEDULLA-MEDIA-VOCAB-R003.3
#[test]
fn subscriber_carries_only_its_keyed_hash() {
    assert!(conforms(
        CONTENT_SHAPES,
        "<urn:s:1> a :Subscriber ; :subscriberKey \"sha256:ab12\" ; :subscriptionStatus \"subscribed\" ."
    ));
    // A raw-address property is refused by the closed shape...
    assert!(!conforms(
        CONTENT_SHAPES,
        "<urn:s:2> a :Subscriber ; :subscriberKey \"sha256:ab12\" ; :emailAddress \"user@example.com\" ."
    ));
    // ...as is a subscriber without its key.
    assert!(!conforms(
        CONTENT_SHAPES,
        "<urn:s:3> a :Subscriber ; :subscriptionStatus \"subscribed\" ."
    ));
}
