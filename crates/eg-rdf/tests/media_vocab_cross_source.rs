//! Cross-source worked examples for the media and personal vocabulary
//! (EG-MEDIA-VOCAB-R020). Each test loads the registered vocabulary modules
//! with a disposable fixture (no live credentials, no network), runs the
//! question through the native SPARQL surface and asserts the answer rows
//! carry provenance (source, record id, as-of) per leg. The `dc:` triples
//! stand in for the fields each connector's virtual mapping attaches to a
//! fetched record at query time. Frozen as-of: 2026-01-15T00:00:00Z.
#![cfg(feature = "sparql")]

mod common;

use std::collections::BTreeMap;

use common::parse_all;
use eg_core::graph::{GraphCore, GraphView};
use eg_rdf::mapping::{load_triples, parse_turtle, IriStore};
use eg_rdf::sparql::{execute, Dataset, Projection, QueryOutcome};

const MEDIA: &str = include_str!("../../eg-core/ontology/media-v1.ttl");
const RETRIEVAL: &str = include_str!("../../eg-core/ontology/retrieval-v1.ttl");
const CONTENT: &str = include_str!("../../eg-core/ontology/content-v1.ttl");

const ASOF: &str = "2026-01-15T00:00:00Z";

const FIXTURE_PREFIXES: &str = r#"
@prefix : <http://knuckles.team/kg#> .
@prefix dc: <http://purl.org/dc/terms/> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix f: <urn:eg-media-vocab-r020:fixture/> .
"#;

const QUERY_PREFIXES: &str = r#"
PREFIX : <http://knuckles.team/kg#>
PREFIX dc: <http://purl.org/dc/terms/>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
"#;

type Row = BTreeMap<String, String>;

/// The provenance triples a virtual mapping attaches to a fetched record.
fn prov(source: &str, record: &str) -> String {
    format!(
        "dc:source \"{source}\" ; dc:identifier \"{record}\" ; dc:date \"{ASOF}\"^^xsd:dateTime"
    )
}

fn graph(schema: &[&str], fixture: &str) -> GraphView {
    let core = GraphCore::new();
    let mut iris = IriStore::default();
    load_triples(&core, &mut iris, "schema", parse_all(schema)).unwrap();
    let doc = format!("{FIXTURE_PREFIXES}\n{fixture}");
    load_triples(&core, &mut iris, "fixture", parse_turtle(&doc).unwrap()).unwrap();
    core.analysis_snapshot()
}

/// Run a SELECT and return its rows keyed by variable, sorted for stability.
fn select(view: &GraphView, query: &str) -> Vec<Row> {
    let outcome = execute(
        &Dataset::new(view, Vec::new()),
        &format!("{QUERY_PREFIXES}\n{query}"),
        &Projection::raw(),
        None,
    )
    .expect("query executes");
    let table = match outcome {
        QueryOutcome::Solutions(table) => table,
        other => panic!("expected a SELECT solution table, got {other:?}"),
    };
    let (vars, rows) = table.to_rows();
    let mut keyed: Vec<Row> = rows
        .into_iter()
        .map(|row| {
            vars.iter()
                .cloned()
                .zip(
                    row.into_iter()
                        .map(|cell| cell.expect("every selected variable binds")),
                )
                .collect()
        })
        .collect();
    keyed.sort();
    keyed
}

fn local(cell: &str) -> &str {
    cell.trim_start_matches('<')
        .trim_end_matches('>')
        .rsplit('/')
        .next()
        .unwrap()
}

fn locals(rows: &[Row], column: &str) -> Vec<String> {
    rows.iter().map(|r| local(&r[column]).to_string()).collect()
}

fn assert_leg(row: &Row, source_col: &str, source: &str, record_col: &str, record: &str) {
    assert_eq!(row[source_col], source, "{source_col}");
    assert_eq!(row[record_col], record, "{record_col}");
}

// spec: EG-MEDIA-VOCAB-R020.1
#[test]
fn media_vocab_films_downloaded_not_in_library() {
    // "Which requested films are downloaded but not yet in the library?"
    // across arr (requested), qbittorrent (downloaded), jellyfin (library).
    // The film is the join entity; a Jellyfin item is the same film through
    // owl:sameAs (the identity-resolution output), never a copied row.
    let fixture = format!(
        r#"
f:film-1 a :Video ; :title "Film One" ; :isMonitored true ; :hasFile false ; {r1} .
f:film-2 a :Video ; :title "Film Two" ; :isMonitored true ; :hasFile false ; {r2} .
f:film-3 a :Video ; :title "Film Three" ; :isMonitored true ; :hasFile false ; {r3} .
f:film-4 a :Video ; :title "Film Four" ; :isMonitored false ; :hasFile false ; {r4} .
f:torrent-1 a :Torrent ; :transfersMedia f:film-1 ; :transferState "stalledUP" ; {q1} .
f:torrent-2 a :Torrent ; :transfersMedia f:film-2 ; :transferState "uploading" ; {q2} .
f:torrent-3 a :Torrent ; :transfersMedia f:film-3 ; :transferState "downloading" ; {q3} .
f:torrent-4 a :Torrent ; :transfersMedia f:film-4 ; :transferState "stalledUP" ; {q4} .
f:jf-2 a :Video ; owl:sameAs f:film-2 ; {j2} .
"#,
        r1 = prov("radarr", "radarr:movie:1"),
        r2 = prov("radarr", "radarr:movie:2"),
        r3 = prov("radarr", "radarr:movie:3"),
        r4 = prov("radarr", "radarr:movie:4"),
        q1 = prov("qbittorrent", "qb:hash:aaa"),
        q2 = prov("qbittorrent", "qb:hash:bbb"),
        q3 = prov("qbittorrent", "qb:hash:ccc"),
        q4 = prov("qbittorrent", "qb:hash:ddd"),
        j2 = prov("jellyfin", "jf:item:2"),
    );
    let view = graph(&[MEDIA], &fixture);
    let rows = select(
        &view,
        r#"SELECT ?film ?title ?arrSource ?arrRecord ?torrentSource ?torrentRecord ?asof WHERE {
  ?film a :Video ; :title ?title ; :isMonitored true ;
        dc:source "radarr" ; dc:source ?arrSource ; dc:identifier ?arrRecord .
  ?torrent a :Torrent ; :transfersMedia ?film ; :transferState ?state ;
           dc:source ?torrentSource ; dc:identifier ?torrentRecord ; dc:date ?asof .
  FILTER(?state IN ("uploading", "stalledUP", "pausedUP", "queuedUP", "forcedUP"))
  FILTER NOT EXISTS { ?visible owl:sameAs ?film ; dc:source "jellyfin" }
}"#,
    );
    // Film Two is already visible in Jellyfin, Film Three is still
    // downloading and Film Four was never requested.
    assert_eq!(locals(&rows, "film"), ["film-1"], "{rows:?}");
    assert_eq!(rows[0]["title"], "Film One");
    assert_leg(
        &rows[0],
        "arrSource",
        "radarr",
        "arrRecord",
        "radarr:movie:1",
    );
    assert_leg(
        &rows[0],
        "torrentSource",
        "qbittorrent",
        "torrentRecord",
        "qb:hash:aaa",
    );
    assert_eq!(rows[0]["asof"], ASOF);
}

// spec: EG-MEDIA-VOCAB-R020.2
#[test]
fn media_vocab_articles_unarchived() {
    // "Which feed articles are not archived and have no matching search
    // result?" across freshrss (articles), archivebox (captures) and searxng
    // (results), joined on :canonicalUrl. A URL join does not create
    // owl:sameAs (content-v1 R003.1), so the match is on the shared string.
    let fixture = format!(
        r#"
f:art-new a :Article, :FeedItem ; :canonicalUrl "https://example.org/new" ; {a1} .
f:art-archived a :Article, :FeedItem ; :canonicalUrl "https://example.org/archived" ; {a2} .
f:art-searched a :Article, :FeedItem ; :canonicalUrl "https://example.org/searched" ; {a3} .
f:page-archived a :WebResource ; :canonicalUrl "https://example.org/archived" .
f:capture-archived a :WebCapture ; :captureOf f:page-archived ; {c2} .
f:hit-searched a :SearchResult ; :canonicalUrl "https://example.org/searched" ; {s3} .
f:hit-other a :SearchResult ; :canonicalUrl "https://example.org/other" ; {s4} .
"#,
        a1 = prov("freshrss", "freshrss:entry:1"),
        a2 = prov("freshrss", "freshrss:entry:2"),
        a3 = prov("freshrss", "freshrss:entry:3"),
        c2 = prov("archivebox", "archivebox:snapshot:2"),
        s3 = prov("searxng", "searxng:result:3"),
        s4 = prov("searxng", "searxng:result:4"),
    );
    let view = graph(&[RETRIEVAL, CONTENT], &fixture);
    let rows = select(
        &view,
        r#"SELECT ?article ?url ?source ?record ?asof WHERE {
  ?article a :Article, :FeedItem ; :canonicalUrl ?url ;
           dc:source "freshrss" ; dc:source ?source ; dc:identifier ?record ; dc:date ?asof .
  FILTER NOT EXISTS {
    ?capture a :WebCapture ; :captureOf ?page .
    ?page :canonicalUrl ?url .
  }
  FILTER NOT EXISTS { ?hit a :SearchResult ; :canonicalUrl ?url }
}"#,
    );
    assert_eq!(locals(&rows, "article"), ["art-new"], "{rows:?}");
    assert_eq!(rows[0]["url"], "https://example.org/new");
    assert_leg(&rows[0], "source", "freshrss", "record", "freshrss:entry:1");
    assert_eq!(rows[0]["asof"], ASOF);
}
