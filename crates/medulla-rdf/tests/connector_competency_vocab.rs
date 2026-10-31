//! Connector competency questions (MEDULLA-CQ-CONN) for the connectors whose
//! vocabulary `specs/connector-competency/plan.md` marked MISSING.
//!
//! The terms these questions need were added to the owning ontology modules
//! (`media-v1`, `dataplatform-v1`, `retrieval-v1`, `personal-v1`). Every test
//! loads those registered modules with a disposable fixture (no live
//! credentials, no network), runs the question through the native SPARQL
//! surface, and asserts a non-empty answer whose rows carry provenance (source,
//! record id, as-of). The `dc:` triples stand in for the fields the connector's
//! virtual mapping attaches to fetched records at query time. Cross-source
//! questions join through `owl:sameAs`, the output of the approved
//! identity-resolution contract, never through copied source data.
//!
//! Time windows use the `YEAR()/MONTH()/DAY()` accessors: the ordering
//! comparator parses operands as f64, so a dateTime lexical comparison drops
//! rows. Frozen as-of: 2026-01-15T00:00:00Z.
#![cfg(feature = "sparql")]

mod common;

use std::collections::BTreeMap;

use common::{assert_acceptance, has_iri, parse_all, AcceptanceQuestion};
use medulla_core::graph::{GraphCore, GraphView};
use medulla_rdf::mapping::{load_triples, parse_turtle, IriStore};
use medulla_rdf::sparql::{execute, Dataset, Projection, QueryOutcome};

const MEDIA: &str = include_str!("../../medulla-core/ontology/media-v1.ttl");
const DATAPLATFORM: &str = include_str!("../../medulla-core/ontology/dataplatform-v1.ttl");
const RETRIEVAL: &str = include_str!("../../medulla-core/ontology/retrieval-v1.ttl");
const PERSONAL: &str = include_str!("../../medulla-core/ontology/personal-v1.ttl");

const ASOF: &str = "2026-01-15T00:00:00Z";

const FIXTURE_PREFIXES: &str = r#"
@prefix : <http://knuckles.team/kg#> .
@prefix dc: <http://purl.org/dc/terms/> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
@prefix f: <urn:eg-cq-conn-vocab:fixture/> .
"#;

const QUERY_PREFIXES: &str = r#"
PREFIX : <http://knuckles.team/kg#>
PREFIX dc: <http://purl.org/dc/terms/>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
"#;

/// The provenance triples a virtual mapping attaches to a fetched record.
fn prov(source: &str, record: &str) -> String {
    format!(
        "dc:source \"{source}\" ; dc:identifier \"{record}\" ; dc:date \"{ASOF}\"^^xsd:dateTime"
    )
}

type Row = BTreeMap<String, String>;

/// Load the vocabulary modules and the fixture into one graph.
fn graph(schema: &[&str], fixture: &str) -> GraphView {
    let core = GraphCore::new();
    let mut iris = IriStore::default();
    load_triples(&core, &mut iris, "schema", parse_all(schema)).unwrap();
    let doc = format!("{FIXTURE_PREFIXES}\n{fixture}");
    load_triples(&core, &mut iris, "fixture", parse_turtle(&doc).unwrap()).unwrap();
    core.analysis_snapshot()
}

/// Execute a SELECT and return its rows keyed by variable, sorted by the cells
/// in selection order so assertions do not depend on scan order.
fn select(view: &GraphView, query: &str) -> Vec<Row> {
    let (vars, mut rows) = run(view, query);
    rows.sort();
    keyed(&vars, rows)
}

/// Execute a SELECT and keep the engine's row order (for ORDER BY / LIMIT).
fn select_ordered(view: &GraphView, query: &str) -> Vec<Row> {
    let (vars, rows) = run(view, query);
    keyed(&vars, rows)
}

fn keyed(vars: &[String], rows: Vec<Vec<String>>) -> Vec<Row> {
    rows.into_iter()
        .map(|row| vars.iter().cloned().zip(row).collect())
        .collect()
}

fn run(view: &GraphView, query: &str) -> (Vec<String>, Vec<Vec<String>>) {
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
    let rows = rows
        .into_iter()
        .map(|row| row.into_iter().map(Option::unwrap_or_default).collect())
        .collect();
    (vars, rows)
}

/// The local name of a bracketed node-id cell (`<urn:…/m1>` → `m1`).
fn local(cell: &str) -> &str {
    cell.trim_start_matches('<')
        .trim_end_matches('>')
        .rsplit('/')
        .next()
        .unwrap()
}

/// One column of every row, reduced to local names, in row order.
fn locals(rows: &[Row], column: &str) -> Vec<String> {
    rows.iter()
        .map(|row| local(&row[column]).to_owned())
        .collect()
}

/// Assert the row carries `source`, `record` and the frozen as-of.
fn assert_prov(row: &Row, source_col: &str, source: &str, record_col: &str, record: &str) {
    assert_eq!(row[source_col], source, "source: {row:?}");
    assert_eq!(row[record_col], record, "record id: {row:?}");
}

fn assert_asof(row: &Row, asof_col: &str) {
    assert_eq!(row[asof_col], ASOF, "as-of: {row:?}");
}

// ---------------------------------------------------------------------------
// media-v1: arr, audiobookshelf, jellyfin, media-downloader, owncast,
// qbittorrent, rom-manager
// ---------------------------------------------------------------------------

// spec: MEDULLA-CQ-CONN-R005.1
#[test]
fn arr_wanted_titles_without_file_answer() {
    // "Which monitored movies and episodes are wanted but have no file, and
    // which indexer last searched them?" — arr connector alone.
    let fixture = format!(
        r#"
f:movie-1 a :Video ; :title "Movie One" ; :isMonitored true ; :hasFile false ;
    :lastSearchedBy "NZBgeek" ; {p1} .
f:episode-1 a :MediaEpisode ; :title "Show S01E02" ; :isMonitored true ; :hasFile false ;
    :lastSearchedBy "Prowlarr-A" ; {p2} .
f:movie-2 a :Video ; :title "Movie Two" ; :isMonitored true ; :hasFile true ;
    :lastSearchedBy "NZBgeek" ; {p3} .
f:movie-3 a :Video ; :title "Movie Three" ; :isMonitored false ; :hasFile false ;
    :lastSearchedBy "NZBgeek" ; {p4} .
"#,
        p1 = prov("radarr", "radarr:movie:1"),
        p2 = prov("sonarr", "sonarr:episode:1"),
        p3 = prov("radarr", "radarr:movie:2"),
        p4 = prov("radarr", "radarr:movie:3"),
    );
    let view = graph(&[MEDIA], &fixture);
    let rows = select(
        &view,
        r#"SELECT ?item ?title ?indexer ?source ?record ?asof WHERE {
  ?item a ?cls ; :title ?title ; :isMonitored true ; :hasFile false ; :lastSearchedBy ?indexer ;
        dc:source ?source ; dc:identifier ?record ; dc:date ?asof .
  VALUES ?cls { :Video :MediaEpisode }
}"#,
    );
    assert_eq!(locals(&rows, "item"), ["episode-1", "movie-1"], "{rows:?}");
    assert_eq!(rows[0]["indexer"], "Prowlarr-A");
    assert_prov(&rows[0], "source", "sonarr", "record", "sonarr:episode:1");
    assert_eq!(rows[1]["indexer"], "NZBgeek");
    assert_prov(&rows[1], "source", "radarr", "record", "radarr:movie:1");
    rows.iter().for_each(|row| assert_asof(row, "asof"));
}

// spec: MEDULLA-CQ-CONN-R005.2
#[test]
fn arr_wanted_downloading_not_in_jellyfin_answers() {
    // "Which wanted Radarr titles are currently downloading in qBittorrent and
    // not yet visible in Jellyfin?" — media item (title) is the link.
    let fixture = format!(
        r#"
f:movie-1 a :Video ; :title "Movie One" ; :isMonitored true ; :hasFile false ; {r1} .
f:movie-2 a :Video ; :title "Movie Two" ; :isMonitored true ; :hasFile false ; {r2} .
f:movie-3 a :Video ; :title "Movie Three" ; :isMonitored true ; :hasFile false ; {r3} .
f:torrent-1 a :Torrent ; :transfersMedia f:movie-1 ; :transferState "downloading" ; {q1} .
f:torrent-2 a :Torrent ; :transfersMedia f:movie-2 ; :transferState "downloading" ; {q2} .
f:torrent-3 a :Torrent ; :transfersMedia f:movie-3 ; :transferState "pausedDL" ; {q3} .
f:jf-2 a :Video ; owl:sameAs f:movie-2 ; {j2} .
"#,
        r1 = prov("radarr", "radarr:movie:1"),
        r2 = prov("radarr", "radarr:movie:2"),
        r3 = prov("radarr", "radarr:movie:3"),
        q1 = prov("qbittorrent", "qb:hash:aaa"),
        q2 = prov("qbittorrent", "qb:hash:bbb"),
        q3 = prov("qbittorrent", "qb:hash:ccc"),
        j2 = prov("jellyfin", "jf:item:2"),
    );
    let view = graph(&[MEDIA], &fixture);
    let rows = select(
        &view,
        r#"SELECT ?movie ?title ?arrSource ?arrRecord ?torrentSource ?torrentRecord ?asof WHERE {
  ?movie a :Video ; :title ?title ; :isMonitored true ; :hasFile false ;
         dc:source "radarr" ; dc:source ?arrSource ; dc:identifier ?arrRecord .
  ?torrent a :Torrent ; :transfersMedia ?movie ; :transferState "downloading" ;
           dc:source ?torrentSource ; dc:identifier ?torrentRecord ; dc:date ?asof .
  FILTER NOT EXISTS { ?visible owl:sameAs ?movie ; dc:source "jellyfin" }
}"#,
    );
    assert_eq!(locals(&rows, "movie"), ["movie-1"], "{rows:?}");
    assert_prov(
        &rows[0],
        "arrSource",
        "radarr",
        "arrRecord",
        "radarr:movie:1",
    );
    assert_prov(
        &rows[0],
        "torrentSource",
        "qbittorrent",
        "torrentRecord",
        "qb:hash:aaa",
    );
    assert_asof(&rows[0], "asof");
}

// spec: MEDULLA-CQ-CONN-R007.1
#[test]
fn audiobookshelf_partial_progress_by_user_answers() {
    // "Which audiobook series have a book with listening progress stopped
    // partway, and for which user?" — audiobookshelf alone.
    let fixture = format!(
        r#"
f:series-1 a :Series ; :title "Series One" ; :includesItem f:book-1, f:book-2 .
f:series-2 a :Series ; :title "Series Two" ; :includesItem f:book-3 .
f:book-1 a :Audiobook ; :title "Book One" .
f:book-2 a :Audiobook ; :title "Book Two" .
f:book-3 a :Audiobook ; :title "Book Three" .
f:user-ann a :Viewer .
f:user-bo a :Viewer .
f:progress-1 a :PlaybackProgress ; :progressOf f:book-1 ; :progressViewer f:user-ann ;
    :progressFraction 0.4 ; {p1} .
f:progress-2 a :PlaybackProgress ; :progressOf f:book-2 ; :progressViewer f:user-bo ;
    :progressFraction 1.0 ; {p2} .
f:progress-3 a :PlaybackProgress ; :progressOf f:book-3 ; :progressViewer f:user-bo ;
    :progressFraction 0.0 ; {p3} .
"#,
        p1 = prov("audiobookshelf", "abs:progress:1"),
        p2 = prov("audiobookshelf", "abs:progress:2"),
        p3 = prov("audiobookshelf", "abs:progress:3"),
    );
    let view = graph(&[MEDIA], &fixture);
    let rows = select(
        &view,
        r#"SELECT ?series ?book ?user ?fraction ?source ?record ?asof WHERE {
  ?series a :Series ; :includesItem ?book .
  ?book a :Audiobook .
  ?p a :PlaybackProgress ; :progressOf ?book ; :progressViewer ?user ; :progressFraction ?fraction ;
     dc:source ?source ; dc:identifier ?record ; dc:date ?asof .
  FILTER(?fraction > 0 && ?fraction < 1)
}"#,
    );
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(local(&rows[0]["series"]), "series-1");
    assert_eq!(local(&rows[0]["book"]), "book-1");
    assert_eq!(local(&rows[0]["user"]), "user-ann");
    assert_prov(
        &rows[0],
        "source",
        "audiobookshelf",
        "record",
        "abs:progress:1",
    );
    assert_asof(&rows[0], "asof");
}

// spec: MEDULLA-CQ-CONN-R007.2
#[test]
fn audiobookshelf_versus_downloads_one_side_answers() {
    // "Which audiobooks exist both in Audiobookshelf and as a downloaded file
    // from media-downloader, and which are only one side?"
    let fixture = format!(
        r#"
f:abs-both a :Audiobook ; :title "Both" ; {a1} .
f:abs-only a :Audiobook ; :title "Shelf Only" ; {a2} .
f:dl-both a :MediaItem ; owl:sameAs f:abs-both ; {d1} .
f:dl-only a :MediaItem ; :title "Download Only" ; {d2} .
f:job-1 a :DownloadJob ; :downloadStatus "completed" ; :downloadOutput f:dl-both .
f:job-2 a :DownloadJob ; :downloadStatus "completed" ; :downloadOutput f:dl-only .
"#,
        a1 = prov("audiobookshelf", "abs:item:1"),
        a2 = prov("audiobookshelf", "abs:item:2"),
        d1 = prov("media-downloader", "md:file:1"),
        d2 = prov("media-downloader", "md:file:2"),
    );
    let view = graph(&[MEDIA], &fixture);
    let both = select(
        &view,
        r#"SELECT ?book ?source ?record ?asof WHERE {
  ?book a :Audiobook ; dc:source ?source ; dc:identifier ?record ; dc:date ?asof .
  ?file owl:sameAs ?book . ?job :downloadOutput ?file .
}"#,
    );
    assert_eq!(locals(&both, "book"), ["abs-both"], "{both:?}");
    assert_prov(&both[0], "source", "audiobookshelf", "record", "abs:item:1");
    let shelf_only = select(
        &view,
        r#"SELECT ?book ?source ?record ?asof WHERE {
  ?book a :Audiobook ; dc:source ?source ; dc:identifier ?record ; dc:date ?asof .
  FILTER NOT EXISTS { ?file owl:sameAs ?book . ?job :downloadOutput ?file }
}"#,
    );
    assert_eq!(locals(&shelf_only, "book"), ["abs-only"], "{shelf_only:?}");
    let download_only = select(
        &view,
        r#"SELECT ?file ?source ?record ?asof WHERE {
  ?job a :DownloadJob ; :downloadStatus "completed" ; :downloadOutput ?file .
  ?file dc:source "media-downloader" ; dc:source ?source ; dc:identifier ?record ; dc:date ?asof .
  FILTER NOT EXISTS { ?file owl:sameAs ?book . ?book a :Audiobook }
}"#,
    );
    assert_eq!(
        locals(&download_only, "file"),
        ["dl-only"],
        "{download_only:?}"
    );
    assert_prov(
        &download_only[0],
        "source",
        "media-downloader",
        "record",
        "md:file:2",
    );
    assert_asof(&download_only[0], "asof");
}

// spec: MEDULLA-CQ-CONN-R027.2
#[test]
fn jellyfin_items_without_arr_entry_answer() {
    // "Which Jellyfin items have no matching Radarr or Sonarr entry, and so
    // cannot be re-fetched?"
    let fixture = format!(
        r#"
f:jf-1 a :Video ; :title "Fetched Movie" ; {j1} .
f:jf-2 a :Video ; :title "Orphan Movie" ; {j2} .
f:jf-3 a :MediaEpisode ; :title "Orphan Episode" ; {j3} .
f:jf-4 a :MediaEpisode ; :title "Tracked Episode" ; {j4} .
f:radarr-1 a :Video ; owl:sameAs f:jf-1 ; dc:source "radarr" ; dc:identifier "radarr:movie:1" .
f:sonarr-4 a :MediaEpisode ; owl:sameAs f:jf-4 ; dc:source "sonarr" ; dc:identifier "sonarr:episode:4" .
"#,
        j1 = prov("jellyfin", "jf:item:1"),
        j2 = prov("jellyfin", "jf:item:2"),
        j3 = prov("jellyfin", "jf:item:3"),
        j4 = prov("jellyfin", "jf:item:4"),
    );
    let view = graph(&[MEDIA], &fixture);
    let rows = select(
        &view,
        r#"SELECT ?item ?title ?source ?record ?asof WHERE {
  ?item a ?cls ; :title ?title ; dc:source "jellyfin" ;
        dc:source ?source ; dc:identifier ?record ; dc:date ?asof .
  VALUES ?cls { :Video :MediaEpisode }
  FILTER NOT EXISTS { ?entry owl:sameAs ?item ; dc:source ?arrSource . FILTER(?arrSource = "radarr" || ?arrSource = "sonarr") }
}"#,
    );
    assert_eq!(locals(&rows, "item"), ["jf-2", "jf-3"], "{rows:?}");
    assert_prov(&rows[0], "source", "jellyfin", "record", "jf:item:2");
    assert_prov(&rows[1], "source", "jellyfin", "record", "jf:item:3");
    rows.iter().for_each(|row| assert_asof(row, "asof"));
}

// spec: MEDULLA-CQ-CONN-R038.1
#[test]
fn media_downloader_failed_jobs_answer() {
    // "Which download jobs failed in the last week, with what error, for which
    // source URL?" — frozen as-of 2026-01-15, so the window starts 2026-01-08.
    let fixture = format!(
        r#"
f:job-1 a :DownloadJob ; :downloadStatus "failed" ; :downloadError "HTTP 403" ;
    :downloadSourceUrl "https://example.invalid/a" ;
    :downloadFinishedAt "2026-01-12T08:00:00Z"^^xsd:dateTime ; {p1} .
f:job-2 a :DownloadJob ; :downloadStatus "failed" ; :downloadError "timeout" ;
    :downloadSourceUrl "https://example.invalid/old" ;
    :downloadFinishedAt "2025-12-20T08:00:00Z"^^xsd:dateTime ; {p2} .
f:job-3 a :DownloadJob ; :downloadStatus "completed" ;
    :downloadSourceUrl "https://example.invalid/ok" ;
    :downloadFinishedAt "2026-01-13T08:00:00Z"^^xsd:dateTime ; {p3} .
"#,
        p1 = prov("media-downloader", "md:job:1"),
        p2 = prov("media-downloader", "md:job:2"),
        p3 = prov("media-downloader", "md:job:3"),
    );
    let view = graph(&[MEDIA], &fixture);
    let rows = select(
        &view,
        r#"SELECT ?job ?error ?url ?source ?record ?asof WHERE {
  ?job a :DownloadJob ; :downloadStatus "failed" ; :downloadError ?error ;
       :downloadSourceUrl ?url ; :downloadFinishedAt ?t ;
       dc:source ?source ; dc:identifier ?record ; dc:date ?asof .
  FILTER(YEAR(?t) = 2026 && MONTH(?t) = 1 && DAY(?t) >= 8)
}"#,
    );
    assert_eq!(locals(&rows, "job"), ["job-1"], "{rows:?}");
    assert_eq!(rows[0]["error"], "HTTP 403");
    assert_eq!(rows[0]["url"], "https://example.invalid/a");
    assert_prov(&rows[0], "source", "media-downloader", "record", "md:job:1");
    assert_asof(&rows[0], "asof");
}

// spec: MEDULLA-CQ-CONN-R038.2
#[test]
fn media_downloader_completed_never_added_answers() {
    // "Which completed downloads were never added to Jellyfin or Audiobookshelf?"
    let fixture = format!(
        r#"
f:file-1 a :MediaItem ; {d1} .
f:file-2 a :MediaItem ; {d2} .
f:file-3 a :MediaItem ; {d3} .
f:job-1 a :DownloadJob ; :downloadStatus "completed" ; :downloadSourceUrl "https://example.invalid/1" ; :downloadOutput f:file-1 .
f:job-2 a :DownloadJob ; :downloadStatus "completed" ; :downloadSourceUrl "https://example.invalid/2" ; :downloadOutput f:file-2 .
f:job-3 a :DownloadJob ; :downloadStatus "completed" ; :downloadSourceUrl "https://example.invalid/3" ; :downloadOutput f:file-3 .
f:jf-1 a :Video ; owl:sameAs f:file-1 ; dc:source "jellyfin" ; dc:identifier "jf:item:1" .
f:abs-3 a :Audiobook ; owl:sameAs f:file-3 ; dc:source "audiobookshelf" ; dc:identifier "abs:item:3" .
"#,
        d1 = prov("media-downloader", "md:file:1"),
        d2 = prov("media-downloader", "md:file:2"),
        d3 = prov("media-downloader", "md:file:3"),
    );
    let view = graph(&[MEDIA], &fixture);
    let rows = select(
        &view,
        r#"SELECT ?job ?url ?source ?record ?asof WHERE {
  ?job a :DownloadJob ; :downloadStatus "completed" ; :downloadSourceUrl ?url ; :downloadOutput ?file .
  ?file dc:source ?source ; dc:identifier ?record ; dc:date ?asof .
  FILTER NOT EXISTS { ?added owl:sameAs ?file ; dc:source ?addedSource .
                      FILTER(?addedSource = "jellyfin" || ?addedSource = "audiobookshelf") }
}"#,
    );
    assert_eq!(locals(&rows, "job"), ["job-2"], "{rows:?}");
    assert_eq!(rows[0]["url"], "https://example.invalid/2");
    assert_prov(
        &rows[0],
        "source",
        "media-downloader",
        "record",
        "md:file:2",
    );
    assert_asof(&rows[0], "asof");
}

// spec: MEDULLA-CQ-CONN-R045.1
#[test]
fn owncast_highest_peak_viewers_answer() {
    // "Which streams had the highest peak viewer count and how long did they run?"
    let fixture = format!(
        r#"
f:stream-1 a :LiveStream ; :title "Morning" ; :peakViewerCount 42 ; :streamDurationSeconds 3600 ; {p1} .
f:stream-2 a :LiveStream ; :title "Evening" ; :peakViewerCount 120 ; :streamDurationSeconds 7200 ; {p2} .
f:stream-3 a :LiveStream ; :title "Night" ; :peakViewerCount 7 ; :streamDurationSeconds 600 ; {p3} .
"#,
        p1 = prov("owncast", "oc:stream:1"),
        p2 = prov("owncast", "oc:stream:2"),
        p3 = prov("owncast", "oc:stream:3"),
    );
    let view = graph(&[MEDIA], &fixture);
    let rows = select(
        &view,
        r#"SELECT ?stream ?peak ?seconds ?source ?record ?asof WHERE {
  ?stream a :LiveStream ; :peakViewerCount ?peak ; :streamDurationSeconds ?seconds ;
          dc:source ?source ; dc:identifier ?record ; dc:date ?asof .
  FILTER(?peak >= 42)
}
ORDER BY DESC(?peak)"#,
    );
    // Rows keep ORDER BY order only through the engine; compare as a set here.
    let mut by_peak: Vec<(String, String, String)> = rows
        .iter()
        .map(|row| {
            (
                local(&row["stream"]).to_owned(),
                row["peak"].clone(),
                row["seconds"].clone(),
            )
        })
        .collect();
    by_peak.sort();
    assert_eq!(
        by_peak,
        [
            ("stream-1".to_owned(), "42".to_owned(), "3600".to_owned()),
            ("stream-2".to_owned(), "120".to_owned(), "7200".to_owned()),
        ]
    );
    let top = rows
        .iter()
        .find(|row| local(&row["stream"]) == "stream-2")
        .unwrap();
    assert_prov(top, "source", "owncast", "record", "oc:stream:2");
    assert_asof(top, "asof");
}

// spec: MEDULLA-CQ-CONN-R051.1
#[test]
fn qbittorrent_stalled_torrents_answer() {
    // "Which torrents are stalled, with what ratio and how long since last
    // activity?" — the last-activity time is returned next to the as-of.
    let fixture = format!(
        r#"
f:torrent-1 a :Torrent ; :transferState "stalledDL" ; :shareRatio 0.25 ;
    :lastActivityAt "2026-01-02T10:00:00Z"^^xsd:dateTime ; {p1} .
f:torrent-2 a :Torrent ; :transferState "stalledUP" ; :shareRatio 1.5 ;
    :lastActivityAt "2025-12-01T10:00:00Z"^^xsd:dateTime ; {p2} .
f:torrent-3 a :Torrent ; :transferState "downloading" ; :shareRatio 0.0 ;
    :lastActivityAt "2026-01-14T10:00:00Z"^^xsd:dateTime ; {p3} .
"#,
        p1 = prov("qbittorrent", "qb:hash:aaa"),
        p2 = prov("qbittorrent", "qb:hash:bbb"),
        p3 = prov("qbittorrent", "qb:hash:ccc"),
    );
    let view = graph(&[MEDIA], &fixture);
    let rows = select(
        &view,
        r#"SELECT ?torrent ?state ?ratio ?last ?source ?record ?asof WHERE {
  ?torrent a :Torrent ; :transferState ?state ; :shareRatio ?ratio ; :lastActivityAt ?last ;
           dc:source ?source ; dc:identifier ?record ; dc:date ?asof .
  FILTER(STRSTARTS(?state, "stalled"))
}"#,
    );
    assert_eq!(
        locals(&rows, "torrent"),
        ["torrent-1", "torrent-2"],
        "{rows:?}"
    );
    assert_eq!(rows[0]["ratio"], "0.25");
    assert_eq!(rows[0]["last"], "2026-01-02T10:00:00Z");
    assert_prov(&rows[0], "source", "qbittorrent", "record", "qb:hash:aaa");
    assert_eq!(rows[1]["last"], "2025-12-01T10:00:00Z");
    rows.iter().for_each(|row| assert_asof(row, "asof"));
}

// spec: MEDULLA-CQ-CONN-R051.2
#[test]
fn qbittorrent_stalled_blocking_wanted_titles_answers() {
    // "Which stalled torrents belong to a wanted Radarr or Sonarr title, and so
    // block it from import?"
    let fixture = format!(
        r#"
f:movie-1 a :Video ; :title "Wanted Movie" ; :isMonitored true ; :hasFile false ; {r1} .
f:movie-2 a :Video ; :title "Owned Movie" ; :isMonitored true ; :hasFile true ; {r2} .
f:episode-1 a :MediaEpisode ; :title "Wanted Episode" ; :isMonitored true ; :hasFile false ; {r3} .
f:torrent-1 a :Torrent ; :transfersMedia f:movie-1 ; :transferState "stalledDL" ; {q1} .
f:torrent-2 a :Torrent ; :transfersMedia f:movie-2 ; :transferState "stalledDL" ; {q2} .
f:torrent-3 a :Torrent ; :transfersMedia f:episode-1 ; :transferState "downloading" ; {q3} .
"#,
        r1 = prov("radarr", "radarr:movie:1"),
        r2 = prov("radarr", "radarr:movie:2"),
        r3 = prov("sonarr", "sonarr:episode:1"),
        q1 = prov("qbittorrent", "qb:hash:aaa"),
        q2 = prov("qbittorrent", "qb:hash:bbb"),
        q3 = prov("qbittorrent", "qb:hash:ccc"),
    );
    let view = graph(&[MEDIA], &fixture);
    let rows = select(
        &view,
        r#"SELECT ?title ?torrentSource ?torrentRecord ?arrSource ?arrRecord ?asof WHERE {
  ?torrent a :Torrent ; :transferState "stalledDL" ; :transfersMedia ?item ;
           dc:source ?torrentSource ; dc:identifier ?torrentRecord ; dc:date ?asof .
  ?item :title ?title ; :isMonitored true ; :hasFile false ;
        dc:source ?arrSource ; dc:identifier ?arrRecord .
  FILTER(?arrSource = "radarr" || ?arrSource = "sonarr")
}"#,
    );
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(rows[0]["title"], "Wanted Movie");
    assert_prov(
        &rows[0],
        "torrentSource",
        "qbittorrent",
        "torrentRecord",
        "qb:hash:aaa",
    );
    assert_prov(
        &rows[0],
        "arrSource",
        "radarr",
        "arrRecord",
        "radarr:movie:1",
    );
    assert_asof(&rows[0], "asof");
}

// spec: MEDULLA-CQ-CONN-R053.1
#[test]
fn rom_manager_checksum_mismatch_answers() {
    // "Which ROMs in a platform collection fail a checksum match against the
    // reference set?"
    let fixture = format!(
        r#"
f:snes a :GameSystem ; :title "SNES" .
f:game-1 a :GameTitle ; :title "Game One" ; :playsOn f:snes .
f:game-2 a :GameTitle ; :title "Game Two" ; :playsOn f:snes .
f:rom-1 a :RomFile ; :romOf f:game-1 ; :expectedChecksum "aaa111" ; :actualChecksum "aaa111" ; {p1} .
f:rom-2 a :RomFile ; :romOf f:game-2 ; :expectedChecksum "bbb222" ; :actualChecksum "zzz999" ; {p2} .
"#,
        p1 = prov("rom-manager", "rm:rom:1"),
        p2 = prov("rom-manager", "rm:rom:2"),
    );
    let view = graph(&[MEDIA], &fixture);
    let rows = select(
        &view,
        r#"SELECT ?rom ?game ?expected ?actual ?source ?record ?asof WHERE {
  ?rom a :RomFile ; :romOf ?game ; :expectedChecksum ?expected ; :actualChecksum ?actual ;
       dc:source ?source ; dc:identifier ?record ; dc:date ?asof .
  ?game :playsOn ?system . ?system :title "SNES" .
  FILTER(?expected != ?actual)
}"#,
    );
    assert_eq!(locals(&rows, "rom"), ["rom-2"], "{rows:?}");
    assert_eq!(local(&rows[0]["game"]), "game-2");
    assert_eq!(rows[0]["expected"], "bbb222");
    assert_eq!(rows[0]["actual"], "zzz999");
    assert_prov(&rows[0], "source", "rom-manager", "record", "rm:rom:2");
    assert_asof(&rows[0], "asof");
}

// spec: MEDULLA-CQ-CONN-R053.2
#[test]
fn rom_manager_downloaded_unconverted_answers() {
    // "Which ROMs were obtained by a media-downloader job and which are still
    // unconverted?"
    let fixture = format!(
        r#"
f:rom-1 a :RomFile ; :conversionState "unconverted" ; {p1} .
f:rom-2 a :RomFile ; :conversionState "converted" ; {p2} .
f:rom-3 a :RomFile ; :conversionState "unconverted" ; {p3} .
f:job-1 a :DownloadJob ; :downloadStatus "completed" ; :downloadOutput f:rom-1 ; {d1} .
f:job-2 a :DownloadJob ; :downloadStatus "completed" ; :downloadOutput f:rom-2 ; {d2} .
"#,
        p1 = prov("rom-manager", "rm:rom:1"),
        p2 = prov("rom-manager", "rm:rom:2"),
        p3 = prov("rom-manager", "rm:rom:3"),
        d1 = prov("media-downloader", "md:job:1"),
        d2 = prov("media-downloader", "md:job:2"),
    );
    let view = graph(&[MEDIA], &fixture);
    let rows = select(
        &view,
        r#"SELECT ?rom ?state ?romSource ?romRecord ?jobSource ?jobRecord ?asof WHERE {
  ?job a :DownloadJob ; :downloadStatus "completed" ; :downloadOutput ?rom ;
       dc:source ?jobSource ; dc:identifier ?jobRecord .
  ?rom a :RomFile ; :conversionState ?state ;
       dc:source ?romSource ; dc:identifier ?romRecord ; dc:date ?asof .
  FILTER(?state = "unconverted")
}"#,
    );
    assert_eq!(locals(&rows, "rom"), ["rom-1"], "{rows:?}");
    assert_prov(
        &rows[0],
        "romSource",
        "rom-manager",
        "romRecord",
        "rm:rom:1",
    );
    assert_prov(
        &rows[0],
        "jobSource",
        "media-downloader",
        "jobRecord",
        "md:job:1",
    );
    assert_asof(&rows[0], "asof");
}

// ---------------------------------------------------------------------------
// dataplatform-v1: data-science, documentdb, objectstore, sql, vector
// ---------------------------------------------------------------------------

// spec: MEDULLA-CQ-CONN-R058.1
#[test]
fn sql_tables_without_primary_key_and_dangling_references_answer() {
    // "Which tables have no primary key, and which columns reference tables
    // that do not exist?" — sql connector alone.
    let fixture = format!(
        r#"
f:orders a :Table ; :hasSchema f:orders-schema ; {t1} .
f:audit a :Table ; :hasSchema f:audit-schema ; {t2} .
f:orders-schema a :Schema ; :hasColumn f:orders-id, f:orders-customer .
f:audit-schema a :Schema ; :hasColumn f:audit-note .
f:orders-id a :Column ; :isPrimaryKey true .
f:orders-customer a :Column ; :isPrimaryKey false ; :referencesTable f:customers ; {c1} .
f:audit-note a :Column ; :isPrimaryKey false ; :referencesTable f:orders ; {c2} .
"#,
        t1 = prov("sql", "sql:table:orders"),
        t2 = prov("sql", "sql:table:audit"),
        c1 = prov("sql", "sql:column:orders.customer"),
        c2 = prov("sql", "sql:column:audit.note"),
    );
    let view = graph(&[DATAPLATFORM], &fixture);
    let no_pk = select(
        &view,
        r#"SELECT ?table ?source ?record ?asof WHERE {
  ?table a :Table ; :hasSchema ?schema ; dc:source ?source ; dc:identifier ?record ; dc:date ?asof .
  FILTER NOT EXISTS { ?schema :hasColumn ?col . ?col :isPrimaryKey true }
}"#,
    );
    assert_eq!(locals(&no_pk, "table"), ["audit"], "{no_pk:?}");
    assert_prov(&no_pk[0], "source", "sql", "record", "sql:table:audit");
    assert_asof(&no_pk[0], "asof");
    let dangling = select(
        &view,
        r#"SELECT ?column ?target ?source ?record ?asof WHERE {
  ?column a :Column ; :referencesTable ?target ; dc:source ?source ; dc:identifier ?record ; dc:date ?asof .
  FILTER NOT EXISTS { ?target a :Table }
}"#,
    );
    assert_eq!(
        locals(&dangling, "column"),
        ["orders-customer"],
        "{dangling:?}"
    );
    assert_eq!(local(&dangling[0]["target"]), "customers");
    assert_prov(
        &dangling[0],
        "source",
        "sql",
        "record",
        "sql:column:orders.customer",
    );
}

// spec: MEDULLA-CQ-CONN-R058.2
#[test]
fn sql_tables_versus_egeria_catalog_answers() {
    // "Which SQL tables are not cataloged in Egeria, and which cataloged
    // tables no longer exist?"
    let fixture = format!(
        r#"
f:orders a :Table ; {t1} .
f:audit a :Table ; {t2} .
f:asset-orders a :DataAsset ; owl:sameAs f:orders ; {a1} .
f:asset-legacy a :DataAsset ; {a2} .
"#,
        t1 = prov("sql", "sql:table:orders"),
        t2 = prov("sql", "sql:table:audit"),
        a1 = prov("egeria", "egeria:asset:orders"),
        a2 = prov("egeria", "egeria:asset:legacy"),
    );
    let view = graph(&[DATAPLATFORM], &fixture);
    let uncataloged = select(
        &view,
        r#"SELECT ?table ?source ?record ?asof WHERE {
  ?table a :Table ; dc:source ?source ; dc:identifier ?record ; dc:date ?asof .
  FILTER NOT EXISTS { ?asset a :DataAsset ; owl:sameAs ?table }
}"#,
    );
    assert_eq!(locals(&uncataloged, "table"), ["audit"], "{uncataloged:?}");
    assert_prov(
        &uncataloged[0],
        "source",
        "sql",
        "record",
        "sql:table:audit",
    );
    assert_asof(&uncataloged[0], "asof");
    let missing = select(
        &view,
        r#"SELECT ?asset ?source ?record ?asof WHERE {
  ?asset a :DataAsset ; dc:source ?source ; dc:identifier ?record ; dc:date ?asof .
  FILTER NOT EXISTS { ?asset owl:sameAs ?table . ?table a :Table }
}"#,
    );
    assert_eq!(locals(&missing, "asset"), ["asset-legacy"], "{missing:?}");
    assert_prov(
        &missing[0],
        "source",
        "egeria",
        "record",
        "egeria:asset:legacy",
    );
}

// spec: MEDULLA-CQ-CONN-R016.1
#[test]
fn documentdb_largest_collections_and_sparse_fields_answer() {
    // "Which collections have the most documents and which fields are missing
    // in more than half of them?"
    let fixture = format!(
        r#"
f:users a :DocumentCollection ; :documentCount 1000 ; {c1} .
f:events a :DocumentCollection ; :documentCount 10 ; {c2} .
f:users-email a :FieldProfile ; :profiledCollection f:users ; :fieldName "email" ; :fieldPresenceCount 300 .
f:users-name a :FieldProfile ; :profiledCollection f:users ; :fieldName "name" ; :fieldPresenceCount 990 .
f:events-kind a :FieldProfile ; :profiledCollection f:events ; :fieldName "kind" ; :fieldPresenceCount 10 .
"#,
        c1 = prov("documentdb", "ddb:collection:users"),
        c2 = prov("documentdb", "ddb:collection:events"),
    );
    let view = graph(&[DATAPLATFORM], &fixture);
    let ranked = select_ordered(
        &view,
        r#"SELECT ?collection ?count ?source ?record ?asof WHERE {
  ?collection a :DocumentCollection ; :documentCount ?count ;
              dc:source ?source ; dc:identifier ?record ; dc:date ?asof .
} ORDER BY DESC(?count)"#,
    );
    assert_eq!(
        locals(&ranked, "collection"),
        ["users", "events"],
        "{ranked:?}"
    );
    assert_prov(
        &ranked[0],
        "source",
        "documentdb",
        "record",
        "ddb:collection:users",
    );
    assert_asof(&ranked[0], "asof");
    let sparse = select(
        &view,
        r#"SELECT ?collection ?field ?present ?count WHERE {
  ?profile a :FieldProfile ; :profiledCollection ?collection ; :fieldName ?field ; :fieldPresenceCount ?present .
  ?collection :documentCount ?count .
  FILTER(?present * 2 < ?count)
}"#,
    );
    assert_eq!(sparse.len(), 1, "{sparse:?}");
    assert_eq!(local(&sparse[0]["collection"]), "users");
    assert_eq!(sparse[0]["field"], "email");
}

// spec: MEDULLA-CQ-CONN-R016.2
#[test]
fn documentdb_collections_duplicating_sql_tables_answer() {
    // "Which documentdb collections duplicate a table in the SQL database, and
    // which side is newer?" — newer is decided on the calendar date because
    // the engine's comparator does not order dateTime lexicals.
    let fixture = format!(
        r#"
f:users-collection a :DocumentCollection ; :duplicateOf f:users-table ;
    :lastWriteAt "2026-01-10T00:00:00Z"^^xsd:dateTime ; {c1} .
f:orders-collection a :DocumentCollection ; :duplicateOf f:orders-table ;
    :lastWriteAt "2025-11-01T00:00:00Z"^^xsd:dateTime ; {c2} .
f:events-collection a :DocumentCollection ; {c3} .
f:users-table a :Table ; :lastWriteAt "2026-01-05T00:00:00Z"^^xsd:dateTime ; {t1} .
f:orders-table a :Table ; :lastWriteAt "2025-12-20T00:00:00Z"^^xsd:dateTime ; {t2} .
"#,
        c1 = prov("documentdb", "ddb:collection:users"),
        c2 = prov("documentdb", "ddb:collection:orders"),
        c3 = prov("documentdb", "ddb:collection:events"),
        t1 = prov("sql", "sql:table:users"),
        t2 = prov("sql", "sql:table:orders"),
    );
    let view = graph(&[DATAPLATFORM], &fixture);
    let rows = select(
        &view,
        r#"SELECT ?collection ?newer ?source ?record ?tableSource ?tableRecord ?asof WHERE {
  ?collection a :DocumentCollection ; :duplicateOf ?table ; :lastWriteAt ?cw ;
              dc:source ?source ; dc:identifier ?record ; dc:date ?asof .
  ?table :lastWriteAt ?tw ; dc:source ?tableSource ; dc:identifier ?tableRecord .
  BIND(YEAR(?cw) * 10000 + MONTH(?cw) * 100 + DAY(?cw) AS ?cKey)
  BIND(YEAR(?tw) * 10000 + MONTH(?tw) * 100 + DAY(?tw) AS ?tKey)
  BIND(IF(?cKey > ?tKey, "documentdb", "sql") AS ?newer)
}"#,
    );
    assert_eq!(
        locals(&rows, "collection"),
        ["orders-collection", "users-collection"],
        "{rows:?}"
    );
    assert_eq!(rows[0]["newer"], "sql");
    assert_eq!(rows[1]["newer"], "documentdb");
    assert_prov(
        &rows[1],
        "source",
        "documentdb",
        "record",
        "ddb:collection:users",
    );
    assert_prov(
        &rows[1],
        "tableSource",
        "sql",
        "tableRecord",
        "sql:table:users",
    );
    assert_asof(&rows[1], "asof");
}

// spec: MEDULLA-CQ-CONN-R041.1
#[test]
fn objectstore_largest_buckets_and_unread_objects_answer() {
    // "Which buckets hold the most data and which objects have not been read in
    // a year?" — as-of 2026-01-15, so unread means last read before 2025-01-15.
    let fixture = format!(
        r#"
f:bucket-big a :StorageContainer ; :totalSizeBytes 9000000 ; {b1} .
f:bucket-small a :StorageContainer ; :totalSizeBytes 1000 ; {b2} .
f:object-old a :Blob ; :storedInContainer f:bucket-big ; :sizeBytes 500 ;
    :lastReadAt "2024-06-01T00:00:00Z"^^xsd:dateTime ; {o1} .
f:object-new a :Blob ; :storedInContainer f:bucket-big ; :sizeBytes 700 ;
    :lastReadAt "2025-12-01T00:00:00Z"^^xsd:dateTime ; {o2} .
"#,
        b1 = prov("objectstore", "os:bucket:big"),
        b2 = prov("objectstore", "os:bucket:small"),
        o1 = prov("objectstore", "os:object:old"),
        o2 = prov("objectstore", "os:object:new"),
    );
    let view = graph(&[DATAPLATFORM], &fixture);
    let ranked = select_ordered(
        &view,
        r#"SELECT ?bucket ?bytes ?source ?record ?asof WHERE {
  ?bucket a :StorageContainer ; :totalSizeBytes ?bytes ;
          dc:source ?source ; dc:identifier ?record ; dc:date ?asof .
} ORDER BY DESC(?bytes)"#,
    );
    assert_eq!(
        locals(&ranked, "bucket"),
        ["bucket-big", "bucket-small"],
        "{ranked:?}"
    );
    assert_prov(
        &ranked[0],
        "source",
        "objectstore",
        "record",
        "os:bucket:big",
    );
    let unread = select(
        &view,
        r#"SELECT ?object ?bucket ?source ?record ?asof WHERE {
  ?object a :Blob ; :storedInContainer ?bucket ; :lastReadAt ?t ;
          dc:source ?source ; dc:identifier ?record ; dc:date ?asof .
  FILTER(YEAR(?t) < 2025 || (YEAR(?t) = 2025 && MONTH(?t) = 1 && DAY(?t) < 15))
}"#,
    );
    assert_eq!(locals(&unread, "object"), ["object-old"], "{unread:?}");
    assert_prov(
        &unread[0],
        "source",
        "objectstore",
        "record",
        "os:object:old",
    );
    assert_asof(&unread[0], "asof");
}

// spec: MEDULLA-CQ-CONN-R041.2
#[test]
fn objectstore_artifacts_unreferenced_by_models_answer() {
    // "Which object-store artifacts are not referenced by any model or dataset
    // in the data-science connector?"
    let fixture = format!(
        r#"
f:blob-model a :Blob ; {o1} .
f:blob-dataset a :Blob ; {o2} .
f:blob-orphan a :Blob ; {o3} .
f:artifact-1 a :ModelArtifact ; :backedByBlob f:blob-model ; {d1} .
f:distribution-1 a :Distribution ; :backedByBlob f:blob-dataset ; {d2} .
"#,
        o1 = prov("objectstore", "os:object:model"),
        o2 = prov("objectstore", "os:object:dataset"),
        o3 = prov("objectstore", "os:object:orphan"),
        d1 = prov("data-science", "ds:artifact:1"),
        d2 = prov("data-science", "ds:distribution:1"),
    );
    let view = graph(&[DATAPLATFORM], &fixture);
    let rows = select(
        &view,
        r#"SELECT ?blob ?source ?record ?asof WHERE {
  ?blob a :Blob ; dc:source "objectstore" ; dc:source ?source ; dc:identifier ?record ; dc:date ?asof .
  FILTER NOT EXISTS { ?holder :backedByBlob ?blob }
}"#,
    );
    assert_eq!(locals(&rows, "blob"), ["blob-orphan"], "{rows:?}");
    assert_prov(
        &rows[0],
        "source",
        "objectstore",
        "record",
        "os:object:orphan",
    );
    assert_asof(&rows[0], "asof");
}

// spec: MEDULLA-CQ-CONN-R014.1
#[test]
fn data_science_models_below_baseline_answer() {
    // "Which trained models have validation metrics below their baseline run,
    // and on which dataset?"
    let fixture = format!(
        r#"
f:model-a a :ModelVersion ; :title "model-a" .
f:model-b a :ModelVersion ; :title "model-b" .
f:dataset-1 a :Dataset ; :title "dataset-1" .
f:eval-a a :ModelEvaluation ; :evaluatesModel f:model-a ; :evaluatedOn f:dataset-1 ;
    :validationMetric 0.71 ; :baselineMetric 0.80 ; {e1} .
f:eval-b a :ModelEvaluation ; :evaluatesModel f:model-b ; :evaluatedOn f:dataset-1 ;
    :validationMetric 0.91 ; :baselineMetric 0.80 ; {e2} .
"#,
        e1 = prov("data-science", "ds:eval:a"),
        e2 = prov("data-science", "ds:eval:b"),
    );
    let view = graph(&[DATAPLATFORM], &fixture);
    let rows = select(
        &view,
        r#"SELECT ?model ?dataset ?metric ?baseline ?source ?record ?asof WHERE {
  ?eval a :ModelEvaluation ; :evaluatesModel ?model ; :evaluatedOn ?dataset ;
        :validationMetric ?metric ; :baselineMetric ?baseline ;
        dc:source ?source ; dc:identifier ?record ; dc:date ?asof .
  FILTER(?metric < ?baseline)
}"#,
    );
    assert_eq!(locals(&rows, "model"), ["model-a"], "{rows:?}");
    assert_eq!(local(&rows[0]["dataset"]), "dataset-1");
    assert_eq!(rows[0]["metric"], "0.71");
    assert_prov(&rows[0], "source", "data-science", "record", "ds:eval:a");
    assert_asof(&rows[0], "asof");
}

// spec: MEDULLA-CQ-CONN-R014.2
#[test]
fn data_science_fine_tuned_models_without_evidence_answer() {
    // "Which fine-tuned models have no evaluation dataset trace in the trace
    // store and no stored artifact in the object store?"
    let fixture = format!(
        r#"
f:base a :ModelVersion .
f:tuned-bare a :ModelVersion ; :fineTunedFrom f:base ; {m1} .
f:tuned-evaluated a :ModelVersion ; :fineTunedFrom f:base ; {m2} .
f:tuned-stored a :ModelVersion ; :fineTunedFrom f:base ; :hasModelArtifact f:artifact-1 ; {m3} .
f:dataset-1 a :Dataset .
f:eval-1 a :ModelEvaluation ; :evaluatesModel f:tuned-evaluated ; :evaluatedOn f:dataset-1 .
f:artifact-1 a :ModelArtifact ; :backedByBlob f:blob-1 .
f:blob-1 a :Blob .
"#,
        m1 = prov("data-science", "ds:model:bare"),
        m2 = prov("data-science", "ds:model:evaluated"),
        m3 = prov("data-science", "ds:model:stored"),
    );
    let view = graph(&[DATAPLATFORM], &fixture);
    let rows = select(
        &view,
        r#"SELECT ?model ?source ?record ?asof WHERE {
  ?model a :ModelVersion ; :fineTunedFrom ?base ;
         dc:source ?source ; dc:identifier ?record ; dc:date ?asof .
  FILTER NOT EXISTS { ?eval :evaluatesModel ?model ; :evaluatedOn ?dataset }
  FILTER NOT EXISTS { ?model :hasModelArtifact ?artifact . ?artifact :backedByBlob ?blob }
}"#,
    );
    assert_eq!(locals(&rows, "model"), ["tuned-bare"], "{rows:?}");
    assert_prov(
        &rows[0],
        "source",
        "data-science",
        "record",
        "ds:model:bare",
    );
    assert_asof(&rows[0], "asof");
}

// spec: MEDULLA-CQ-CONN-R065.1
#[test]
fn vector_largest_collections_and_dimension_mismatch_answer() {
    // "Which collections have the most vectors and which have an embedding
    // dimension that mismatches the model?"
    let fixture = format!(
        r#"
f:model-1024 a :EmbeddingModel ; :embeddingDimension 1024 .
f:index-docs a :VectorIndex ; :vectorCount 5000 ; :indexDimension 768 ; :indexedWithModel f:model-1024 ; {i1} .
f:index-notes a :VectorIndex ; :vectorCount 100 ; :indexDimension 1024 ; :indexedWithModel f:model-1024 ; {i2} .
"#,
        i1 = prov("vector", "vec:collection:docs"),
        i2 = prov("vector", "vec:collection:notes"),
    );
    let view = graph(&[DATAPLATFORM], &fixture);
    let ranked = select_ordered(
        &view,
        r#"SELECT ?index ?count ?source ?record ?asof WHERE {
  ?index a :VectorIndex ; :vectorCount ?count ;
         dc:source ?source ; dc:identifier ?record ; dc:date ?asof .
} ORDER BY DESC(?count)"#,
    );
    assert_eq!(
        locals(&ranked, "index"),
        ["index-docs", "index-notes"],
        "{ranked:?}"
    );
    assert_prov(
        &ranked[0],
        "source",
        "vector",
        "record",
        "vec:collection:docs",
    );
    let mismatch = select(
        &view,
        r#"SELECT ?index ?dim ?modelDim ?source ?record ?asof WHERE {
  ?index a :VectorIndex ; :indexDimension ?dim ; :indexedWithModel ?model ;
         dc:source ?source ; dc:identifier ?record ; dc:date ?asof .
  ?model :embeddingDimension ?modelDim .
  FILTER(?dim != ?modelDim)
}"#,
    );
    assert_eq!(locals(&mismatch, "index"), ["index-docs"], "{mismatch:?}");
    assert_eq!(mismatch[0]["dim"], "768");
    assert_eq!(mismatch[0]["modelDim"], "1024");
    assert_asof(&mismatch[0], "asof");
}

// spec: MEDULLA-CQ-CONN-R065.2
#[test]
fn vector_documents_without_embedding_answer() {
    // "Which Paperless-ngx documents have no embedding stored in the vector
    // database?"
    let fixture = format!(
        r#"
f:doc-1 a :Document ; {d1} .
f:doc-2 a :Document ; {d2} .
f:index-docs a :VectorIndex ; {i1} .
f:embedding-1 a :Embedding ; :embeddingOf f:doc-1 ; :embeddingIn f:index-docs .
"#,
        d1 = prov("paperless-ngx", "pl:doc:1"),
        d2 = prov("paperless-ngx", "pl:doc:2"),
        i1 = prov("vector", "vec:collection:docs"),
    );
    let view = graph(&[DATAPLATFORM], &fixture);
    let rows = select(
        &view,
        r#"SELECT ?doc ?source ?record ?asof WHERE {
  ?doc a :Document ; dc:source "paperless-ngx" ;
       dc:source ?source ; dc:identifier ?record ; dc:date ?asof .
  FILTER NOT EXISTS { ?embedding :embeddingOf ?doc ; :embeddingIn ?index }
}"#,
    );
    assert_eq!(locals(&rows, "doc"), ["doc-2"], "{rows:?}");
    assert_prov(&rows[0], "source", "paperless-ngx", "record", "pl:doc:2");
    assert_asof(&rows[0], "asof");
}

// ---------------------------------------------------------------------------
// retrieval-v1 and personal-v1: audio-transcriber, clarity, freshrss,
// pulselink, scholarx, searxng
// ---------------------------------------------------------------------------

// spec: MEDULLA-CQ-CONN-R008.1
#[test]
fn audio_transcriber_low_confidence_segments_answer() {
    // "Which recordings were transcribed with a low-confidence segment, and
    // what text was affected?"
    let fixture = format!(
        r#"
f:recording-1 a :VoiceMessage ; :hasTranscript f:transcript-1 ; {r1} .
f:recording-2 a :VoiceMessage ; :hasTranscript f:transcript-2 ; {r2} .
f:transcript-1 a :Transcript .
f:transcript-2 a :Transcript .
f:segment-1 a :TranscriptSegment ; :segmentOf f:transcript-1 ; :segmentConfidence 0.31 ; :text "the quarterly figgers" .
f:segment-2 a :TranscriptSegment ; :segmentOf f:transcript-1 ; :segmentConfidence 0.97 ; :text "were reviewed" .
f:segment-3 a :TranscriptSegment ; :segmentOf f:transcript-2 ; :segmentConfidence 0.92 ; :text "all clear" .
"#,
        r1 = prov("audio-transcriber", "at:recording:1"),
        r2 = prov("audio-transcriber", "at:recording:2"),
    );
    let view = graph(&[PERSONAL], &fixture);
    let rows = select(
        &view,
        r#"SELECT ?recording ?text ?confidence ?source ?record ?asof WHERE {
  ?recording a :VoiceMessage ; :hasTranscript ?transcript ;
             dc:source ?source ; dc:identifier ?record ; dc:date ?asof .
  ?segment a :TranscriptSegment ; :segmentOf ?transcript ; :segmentConfidence ?confidence ; :text ?text .
  FILTER(?confidence < 0.5)
}"#,
    );
    assert_eq!(locals(&rows, "recording"), ["recording-1"], "{rows:?}");
    assert_eq!(rows[0]["text"], "the quarterly figgers");
    assert_eq!(rows[0]["confidence"], "0.31");
    assert_prov(
        &rows[0],
        "source",
        "audio-transcriber",
        "record",
        "at:recording:1",
    );
    assert_asof(&rows[0], "asof");
}

// spec: MEDULLA-CQ-CONN-R008.2
#[test]
fn audio_transcriber_topics_shared_with_pulselink_answer() {
    // "Which transcribed recordings mention a topic that also appears in
    // PulseLink social or video transcripts?"
    let fixture = format!(
        r#"
f:topic-gpu a :Topic ; :title "gpu pricing" .
f:topic-garden a :Topic ; :title "gardening" .
f:recording-1 a :VoiceMessage ; :hasTranscript f:transcript-1 ; {r1} .
f:recording-2 a :VoiceMessage ; :hasTranscript f:transcript-2 ; {r2} .
f:transcript-1 a :Transcript ; :mentionsTopic f:topic-gpu .
f:transcript-2 a :Transcript ; :mentionsTopic f:topic-garden .
f:video-transcript-1 a :Transcript ; :mentionsTopic f:topic-gpu ; {p1} .
"#,
        r1 = prov("audio-transcriber", "at:recording:1"),
        r2 = prov("audio-transcriber", "at:recording:2"),
        p1 = prov("pulselink", "pl:transcript:1"),
    );
    let view = graph(&[PERSONAL, RETRIEVAL], &fixture);
    let rows = select(
        &view,
        r#"SELECT ?recording ?topic ?source ?record ?pulseSource ?pulseRecord ?asof WHERE {
  ?recording a :VoiceMessage ; :hasTranscript ?transcript ;
             dc:source ?source ; dc:identifier ?record ; dc:date ?asof .
  ?transcript :mentionsTopic ?topic .
  ?other a :Transcript ; :mentionsTopic ?topic ;
         dc:source ?pulseSource ; dc:identifier ?pulseRecord .
  FILTER(?pulseSource = "pulselink")
}"#,
    );
    assert_eq!(locals(&rows, "recording"), ["recording-1"], "{rows:?}");
    assert_eq!(local(&rows[0]["topic"]), "topic-gpu");
    assert_prov(
        &rows[0],
        "source",
        "audio-transcriber",
        "record",
        "at:recording:1",
    );
    assert_prov(
        &rows[0],
        "pulseSource",
        "pulselink",
        "pulseRecord",
        "pl:transcript:1",
    );
    assert_asof(&rows[0], "asof");
}

// spec: MEDULLA-CQ-CONN-R050.1
#[test]
fn pulselink_videos_with_term_in_transcript_answer() {
    // "Which videos on a topic had transcripts mentioning a given term this
    // week?" — as-of 2026-01-15, so this week starts 2026-01-08.
    let fixture = format!(
        r#"
f:topic-gpu a :Topic ; :title "gpu pricing" .
f:video-1 a :Video ; :title "GPU market update" ; :hasTranscript f:transcript-1 ; {v1} .
f:video-2 a :Video ; :title "Older GPU video" ; :hasTranscript f:transcript-2 ; {v2} .
f:video-3 a :Video ; :title "Cooking" ; :hasTranscript f:transcript-3 ; {v3} .
f:transcript-1 a :Transcript ; :mentionsTopic f:topic-gpu ; :text "prices for the blackwell cards dropped" ;
    :firstSeenAt "2026-01-12T00:00:00Z"^^xsd:dateTime .
f:transcript-2 a :Transcript ; :mentionsTopic f:topic-gpu ; :text "blackwell launch recap" ;
    :firstSeenAt "2025-12-01T00:00:00Z"^^xsd:dateTime .
f:transcript-3 a :Transcript ; :text "blackwell is also a surname" ;
    :firstSeenAt "2026-01-12T00:00:00Z"^^xsd:dateTime .
"#,
        v1 = prov("pulselink", "pl:video:1"),
        v2 = prov("pulselink", "pl:video:2"),
        v3 = prov("pulselink", "pl:video:3"),
    );
    let view = graph(&[PERSONAL, RETRIEVAL, MEDIA], &fixture);
    let rows = select(
        &view,
        r#"SELECT ?video ?title ?source ?record ?asof WHERE {
  ?video a :Video ; :title ?title ; :hasTranscript ?transcript ;
         dc:source ?source ; dc:identifier ?record ; dc:date ?asof .
  ?transcript :mentionsTopic f:topic-gpu ; :text ?text ; :firstSeenAt ?seen .
  FILTER(CONTAINS(LCASE(?text), "blackwell"))
  FILTER(YEAR(?seen) = 2026 && MONTH(?seen) = 1 && DAY(?seen) >= 8)
}"#
        .replace("f:topic-gpu", "<urn:eg-cq-conn-vocab:fixture/topic-gpu>")
        .as_str(),
    );
    assert_eq!(locals(&rows, "video"), ["video-1"], "{rows:?}");
    assert_eq!(rows[0]["title"], "GPU market update");
    assert_prov(&rows[0], "source", "pulselink", "record", "pl:video:1");
    assert_asof(&rows[0], "asof");
}

// spec: MEDULLA-CQ-CONN-R050.2
#[test]
fn pulselink_topics_missing_from_archive_answer() {
    // "Which topics appear in PulseLink video transcripts and SearXNG news but
    // not in any archived page?"
    let fixture = format!(
        r#"
f:topic-a a :Topic ; :title "topic archived" .
f:topic-b a :Topic ; :title "topic unarchived" .
f:topic-c a :Topic ; :title "topic video only" .
f:transcript-1 a :Transcript ; :mentionsTopic f:topic-a, f:topic-b, f:topic-c ; {t1} .
f:hit-1 a :SearchHit ; :mentionsTopic f:topic-a, f:topic-b ; {h1} .
f:page-1 a :WebResource ; :mentionsTopic f:topic-a .
f:capture-1 a :WebCapture ; :captureOf f:page-1 .
"#,
        t1 = prov("pulselink", "pl:transcript:1"),
        h1 = prov("searxng", "sx:hit:1"),
    );
    let view = graph(&[PERSONAL, RETRIEVAL], &fixture);
    let rows = select(
        &view,
        r#"SELECT ?topic ?title ?pulseSource ?pulseRecord ?searchSource ?searchRecord ?asof WHERE {
  ?transcript a :Transcript ; :mentionsTopic ?topic ;
              dc:source ?pulseSource ; dc:identifier ?pulseRecord ; dc:date ?asof .
  ?hit a :SearchHit ; :mentionsTopic ?topic ; dc:source ?searchSource ; dc:identifier ?searchRecord .
  ?topic :title ?title .
  FILTER NOT EXISTS { ?capture :captureOf ?page . ?page :mentionsTopic ?topic }
}"#,
    );
    assert_eq!(locals(&rows, "topic"), ["topic-b"], "{rows:?}");
    assert_prov(
        &rows[0],
        "pulseSource",
        "pulselink",
        "pulseRecord",
        "pl:transcript:1",
    );
    assert_prov(
        &rows[0],
        "searchSource",
        "searxng",
        "searchRecord",
        "sx:hit:1",
    );
    assert_asof(&rows[0], "asof");
}

// spec: MEDULLA-CQ-CONN-R012.1
#[test]
fn clarity_highest_rage_click_pages_answer() {
    // "Which pages have the highest rage-click or dead-click rate over the last
    // week?" — the connector returns the pages of the requested window.
    let fixture = format!(
        r#"
f:page-checkout a :WebResource ; :resourceUrl "https://example.invalid/checkout" ;
    :rageClickRate 0.12 ; :deadClickRate 0.03 ; {p1} .
f:page-home a :WebResource ; :resourceUrl "https://example.invalid/" ;
    :rageClickRate 0.01 ; :deadClickRate 0.02 ; {p2} .
f:page-search a :WebResource ; :resourceUrl "https://example.invalid/search" ;
    :rageClickRate 0.04 ; :deadClickRate 0.09 ; {p3} .
"#,
        p1 = prov("clarity", "clarity:page:checkout"),
        p2 = prov("clarity", "clarity:page:home"),
        p3 = prov("clarity", "clarity:page:search"),
    );
    let view = graph(&[RETRIEVAL], &fixture);
    let rows = select_ordered(
        &view,
        r#"SELECT ?url ?rage ?dead ?source ?record ?asof WHERE {
  ?page a :WebResource ; :resourceUrl ?url ; :rageClickRate ?rage ; :deadClickRate ?dead ;
        dc:source ?source ; dc:identifier ?record ; dc:date ?asof .
  FILTER(?rage >= 0.04 || ?dead >= 0.05)
} ORDER BY DESC(?rage)"#,
    );
    let urls: Vec<&str> = rows.iter().map(|row| row["url"].as_str()).collect();
    assert_eq!(
        urls,
        [
            "https://example.invalid/checkout",
            "https://example.invalid/search"
        ],
        "{rows:?}"
    );
    assert_prov(
        &rows[0],
        "source",
        "clarity",
        "record",
        "clarity:page:checkout",
    );
    assert_asof(&rows[0], "asof");
}

// spec: MEDULLA-CQ-CONN-R022.2
#[test]
fn freshrss_articles_also_in_searxng_answer() {
    // "Which FreshRSS articles also appear in SearXNG results for the same
    // query, and which are new?" — new means first seen in the last 48 hours
    // before the frozen as-of 2026-01-15.
    let fixture = format!(
        r#"
f:query-1 a :SearchQuery ; :text "homelab" .
f:query-2 a :SearchQuery ; :text "other" .
f:item-1 a :FeedItem ; :resourceUrl "https://example.invalid/a" ;
    :firstSeenAt "2026-01-14T10:00:00Z"^^xsd:dateTime ; {i1} .
f:item-2 a :FeedItem ; :resourceUrl "https://example.invalid/b" ;
    :firstSeenAt "2026-01-02T10:00:00Z"^^xsd:dateTime ; {i2} .
f:item-3 a :FeedItem ; :resourceUrl "https://example.invalid/c" ;
    :firstSeenAt "2026-01-14T11:00:00Z"^^xsd:dateTime ; {i3} .
f:hit-1 a :SearchHit ; :resourceUrl "https://example.invalid/a" ; :answersQuery f:query-1 ; {h1} .
f:hit-2 a :SearchHit ; :resourceUrl "https://example.invalid/b" ; :answersQuery f:query-1 ; {h2} .
f:hit-3 a :SearchHit ; :resourceUrl "https://example.invalid/c" ; :answersQuery f:query-2 ; {h3} .
"#,
        i1 = prov("freshrss", "fr:item:1"),
        i2 = prov("freshrss", "fr:item:2"),
        i3 = prov("freshrss", "fr:item:3"),
        h1 = prov("searxng", "sx:hit:1"),
        h2 = prov("searxng", "sx:hit:2"),
        h3 = prov("searxng", "sx:hit:3"),
    );
    let view = graph(&[RETRIEVAL], &fixture);
    let rows = select(
        &view,
        r#"SELECT ?item ?url ?freshness ?source ?record ?hitSource ?hitRecord ?asof WHERE {
  ?item a :FeedItem ; :resourceUrl ?url ; :firstSeenAt ?seen ;
        dc:source ?source ; dc:identifier ?record ; dc:date ?asof .
  ?hit a :SearchHit ; :resourceUrl ?url ; :answersQuery ?query ;
       dc:source ?hitSource ; dc:identifier ?hitRecord .
  ?query :text "homelab" .
  BIND(IF(YEAR(?seen) = 2026 && MONTH(?seen) = 1 && DAY(?seen) >= 13, "new", "known") AS ?freshness)
}"#,
    );
    assert_eq!(locals(&rows, "item"), ["item-1", "item-2"], "{rows:?}");
    assert_eq!(rows[0]["freshness"], "new");
    assert_eq!(rows[1]["freshness"], "known");
    assert_prov(&rows[0], "source", "freshrss", "record", "fr:item:1");
    assert_prov(&rows[0], "hitSource", "searxng", "hitRecord", "sx:hit:1");
    assert_asof(&rows[0], "asof");
}

// spec: MEDULLA-CQ-CONN-R055.1
#[test]
fn scholarx_recent_citing_papers_answer() {
    // "Which papers cite a given paper and were published in the last two
    // years?" — as-of 2026, so 2024 or later.
    let fixture = format!(
        r#"
f:target a :ScholarlyPaper ; :title "Target paper" ; {t} .
f:paper-1 a :ScholarlyPaper ; :title "Recent citer" ; :cites f:target ; :publicationYear 2025 ; {p1} .
f:paper-2 a :ScholarlyPaper ; :title "Old citer" ; :cites f:target ; :publicationYear 2019 ; {p2} .
f:paper-3 a :ScholarlyPaper ; :title "Recent non-citer" ; :publicationYear 2025 ; {p3} .
"#,
        t = prov("scholarx", "sx:paper:target"),
        p1 = prov("scholarx", "sx:paper:1"),
        p2 = prov("scholarx", "sx:paper:2"),
        p3 = prov("scholarx", "sx:paper:3"),
    );
    let view = graph(&[RETRIEVAL], &fixture);
    let rows = select(
        &view,
        r#"SELECT ?paper ?title ?year ?source ?record ?asof WHERE {
  ?paper a :ScholarlyPaper ; :title ?title ; :cites ?target ; :publicationYear ?year ;
         dc:source ?source ; dc:identifier ?record ; dc:date ?asof .
  ?target :title "Target paper" .
  FILTER(?year >= 2024)
}"#,
    );
    assert_eq!(locals(&rows, "paper"), ["paper-1"], "{rows:?}");
    assert_eq!(rows[0]["year"], "2025");
    assert_prov(&rows[0], "source", "scholarx", "record", "sx:paper:1");
    assert_asof(&rows[0], "asof");
}

// spec: MEDULLA-CQ-CONN-R055.2
#[test]
fn scholarx_papers_never_surfaced_by_searxng_answer() {
    // "Which papers on a topic are in ScholarX but were never surfaced by a
    // SearXNG query for that topic?"
    let fixture = format!(
        r#"
f:topic-rag a :Topic ; :title "retrieval augmented generation" .
f:paper-1 a :ScholarlyPaper ; :aboutTopic f:topic-rag ; :resourceUrl "https://example.invalid/p1" ; {p1} .
f:paper-2 a :ScholarlyPaper ; :aboutTopic f:topic-rag ; :resourceUrl "https://example.invalid/p2" ; {p2} .
f:query-1 a :SearchQuery ; :text "retrieval augmented generation" .
f:hit-1 a :SearchHit ; :answersQuery f:query-1 ; :resourceUrl "https://example.invalid/p1" ; {h1} .
"#,
        p1 = prov("scholarx", "sx:paper:1"),
        p2 = prov("scholarx", "sx:paper:2"),
        h1 = prov("searxng", "sx:hit:1"),
    );
    let view = graph(&[RETRIEVAL], &fixture);
    let rows = select(
        &view,
        r#"SELECT ?paper ?url ?source ?record ?asof WHERE {
  ?paper a :ScholarlyPaper ; :aboutTopic ?topic ; :resourceUrl ?url ;
         dc:source ?source ; dc:identifier ?record ; dc:date ?asof .
  ?topic :title "retrieval augmented generation" .
  FILTER NOT EXISTS { ?hit a :SearchHit ; :resourceUrl ?url ; :answersQuery ?query . ?query :text "retrieval augmented generation" }
}"#,
    );
    assert_eq!(locals(&rows, "paper"), ["paper-2"], "{rows:?}");
    assert_prov(&rows[0], "source", "scholarx", "record", "sx:paper:2");
    assert_asof(&rows[0], "asof");
}

// spec: MEDULLA-CQ-CONN-R056.1
#[test]
fn searxng_engines_by_result_count_answer() {
    // "Which engines returned no results for a given query, and which returned
    // the most?" — one SearchQuery per engine execution of the same terms.
    let fixture = format!(
        r#"
f:engine-a a :SearchEngine ; :title "duckduckgo" .
f:engine-b a :SearchEngine ; :title "brave" .
f:engine-c a :SearchEngine ; :title "bing" .
f:run-a a :SearchQuery ; :text "homelab" ; :runsOnEngine f:engine-a ; {q1} .
f:run-b a :SearchQuery ; :text "homelab" ; :runsOnEngine f:engine-b ; {q2} .
f:run-c a :SearchQuery ; :text "homelab" ; :runsOnEngine f:engine-c ; {q3} .
f:result-1 a :SearchResult ; :answersQuery f:run-a .
f:result-2 a :SearchResult ; :answersQuery f:run-a .
f:result-3 a :SearchResult ; :answersQuery f:run-a .
f:result-4 a :SearchResult ; :answersQuery f:run-b .
"#,
        q1 = prov("searxng", "sx:run:a"),
        q2 = prov("searxng", "sx:run:b"),
        q3 = prov("searxng", "sx:run:c"),
    );
    let view = graph(&[RETRIEVAL], &fixture);
    let silent = select(
        &view,
        r#"SELECT ?engineName ?source ?record ?asof WHERE {
  ?run a :SearchQuery ; :text "homelab" ; :runsOnEngine ?engine ;
       dc:source ?source ; dc:identifier ?record ; dc:date ?asof .
  ?engine :title ?engineName .
  FILTER NOT EXISTS { ?result :answersQuery ?run }
}"#,
    );
    assert_eq!(silent.len(), 1, "{silent:?}");
    assert_eq!(silent[0]["engineName"], "bing");
    assert_prov(&silent[0], "source", "searxng", "record", "sx:run:c");
    assert_asof(&silent[0], "asof");
    let most = select_ordered(
        &view,
        r#"SELECT ?engineName (COUNT(?result) AS ?results) WHERE {
  ?run a :SearchQuery ; :text "homelab" ; :runsOnEngine ?engine .
  ?engine :title ?engineName .
  ?result :answersQuery ?run .
} GROUP BY ?engineName ORDER BY DESC(?results)"#,
    );
    assert_eq!(most[0]["engineName"], "duckduckgo", "{most:?}");
    assert_eq!(most[0]["results"], "3");
}

// spec: MEDULLA-CQ-CONN-R056.2
#[test]
fn searxng_urls_not_archived_answer() {
    // "Which URLs from a SearXNG query are not yet archived in ArchiveBox?"
    let fixture = format!(
        r#"
f:query-1 a :SearchQuery ; :text "homelab" .
f:hit-1 a :SearchHit ; :answersQuery f:query-1 ; :resourceUrl "https://example.invalid/a" ; {h1} .
f:hit-2 a :SearchHit ; :answersQuery f:query-1 ; :resourceUrl "https://example.invalid/b" ; {h2} .
f:page-a a :WebResource ; :resourceUrl "https://example.invalid/a" .
f:capture-a a :WebCapture ; :captureOf f:page-a ; {c1} .
"#,
        h1 = prov("searxng", "sx:hit:1"),
        h2 = prov("searxng", "sx:hit:2"),
        c1 = prov("archivebox", "ab:snapshot:a"),
    );
    let view = graph(&[RETRIEVAL], &fixture);
    let rows = select(
        &view,
        r#"SELECT ?url ?source ?record ?asof WHERE {
  ?hit a :SearchHit ; :answersQuery ?query ; :resourceUrl ?url ;
       dc:source ?source ; dc:identifier ?record ; dc:date ?asof .
  ?query :text "homelab" .
  FILTER NOT EXISTS { ?capture :captureOf ?page . ?page :resourceUrl ?url }
}"#,
    );
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(rows[0]["url"], "https://example.invalid/b");
    assert_prov(&rows[0], "source", "searxng", "record", "sx:hit:2");
    assert_asof(&rows[0], "asof");
}

// ---------------------------------------------------------------------------
// Vocabulary: every term these questions rely on is declared by the module
// that owns it, with its parent class, domain and range.
// ---------------------------------------------------------------------------
// `.3` acceptance rows: both questions of a connector through its declared
// access contract, or the typed abstention naming the unconnected source.
// ---------------------------------------------------------------------------

// spec: MEDULLA-CQ-CONN-R005.3
#[test]
fn arr_acceptance_through_declared_contracts() {
    assert_acceptance(&[
        AcceptanceQuestion {
            id: "MEDULLA-CQ-CONN-R005.1",
            required: &[("radarr", "Video"), ("sonarr", "MediaEpisode")],
            answer: arr_wanted_titles_without_file_answer,
        },
        AcceptanceQuestion {
            id: "MEDULLA-CQ-CONN-R005.2",
            required: &[
                ("radarr", "Video"),
                ("qbittorrent", "Torrent"),
                ("jellyfin", "Video"),
            ],
            answer: arr_wanted_downloading_not_in_jellyfin_answers,
        },
    ]);
}

// spec: MEDULLA-CQ-CONN-R007.3
#[test]
fn audiobookshelf_acceptance_through_declared_contracts() {
    assert_acceptance(&[
        AcceptanceQuestion {
            id: "MEDULLA-CQ-CONN-R007.1",
            required: &[("audiobookshelf", "Audiobook")],
            answer: audiobookshelf_partial_progress_by_user_answers,
        },
        AcceptanceQuestion {
            id: "MEDULLA-CQ-CONN-R007.2",
            required: &[
                ("audiobookshelf", "Audiobook"),
                ("media-downloader", "DownloadJob"),
            ],
            answer: audiobookshelf_versus_downloads_one_side_answers,
        },
    ]);
}

// spec: MEDULLA-CQ-CONN-R008.3
#[test]
fn audio_transcriber_acceptance_through_declared_contracts() {
    assert_acceptance(&[
        AcceptanceQuestion {
            id: "MEDULLA-CQ-CONN-R008.1",
            required: &[("audio-transcriber", "Transcript")],
            answer: audio_transcriber_low_confidence_segments_answer,
        },
        AcceptanceQuestion {
            id: "MEDULLA-CQ-CONN-R008.2",
            required: &[
                ("audio-transcriber", "Transcript"),
                ("pulselink", "Transcript"),
            ],
            answer: audio_transcriber_topics_shared_with_pulselink_answer,
        },
    ]);
}

// spec: MEDULLA-CQ-CONN-R014.3
#[test]
fn data_science_acceptance_through_declared_contracts() {
    assert_acceptance(&[
        AcceptanceQuestion {
            id: "MEDULLA-CQ-CONN-R014.1",
            required: &[("data-science", "ModelVersion")],
            answer: data_science_models_below_baseline_answer,
        },
        AcceptanceQuestion {
            id: "MEDULLA-CQ-CONN-R014.2",
            required: &[("data-science", "ModelVersion")],
            answer: data_science_fine_tuned_models_without_evidence_answer,
        },
    ]);
}

// spec: MEDULLA-CQ-CONN-R016.3
#[test]
fn documentdb_acceptance_through_declared_contracts() {
    assert_acceptance(&[
        AcceptanceQuestion {
            id: "MEDULLA-CQ-CONN-R016.1",
            required: &[("documentdb", "DocumentCollection")],
            answer: documentdb_largest_collections_and_sparse_fields_answer,
        },
        AcceptanceQuestion {
            id: "MEDULLA-CQ-CONN-R016.2",
            required: &[("documentdb", "DocumentCollection"), ("sql", "Table")],
            answer: documentdb_collections_duplicating_sql_tables_answer,
        },
    ]);
}

// spec: MEDULLA-CQ-CONN-R038.3
#[test]
fn media_downloader_acceptance_through_declared_contracts() {
    assert_acceptance(&[
        AcceptanceQuestion {
            id: "MEDULLA-CQ-CONN-R038.1",
            required: &[("media-downloader", "DownloadJob")],
            answer: media_downloader_failed_jobs_answer,
        },
        AcceptanceQuestion {
            id: "MEDULLA-CQ-CONN-R038.2",
            required: &[
                ("media-downloader", "DownloadJob"),
                ("audiobookshelf", "Audiobook"),
                ("jellyfin", "Video"),
            ],
            answer: media_downloader_completed_never_added_answers,
        },
    ]);
}

// spec: MEDULLA-CQ-CONN-R041.3
#[test]
fn objectstore_acceptance_through_declared_contracts() {
    assert_acceptance(&[
        AcceptanceQuestion {
            id: "MEDULLA-CQ-CONN-R041.1",
            required: &[("objectstore", "StorageContainer")],
            answer: objectstore_largest_buckets_and_unread_objects_answer,
        },
        AcceptanceQuestion {
            id: "MEDULLA-CQ-CONN-R041.2",
            required: &[("objectstore", "Blob"), ("data-science", "ModelArtifact")],
            answer: objectstore_artifacts_unreferenced_by_models_answer,
        },
    ]);
}

// spec: MEDULLA-CQ-CONN-R050.3
#[test]
fn pulselink_acceptance_through_declared_contracts() {
    assert_acceptance(&[
        AcceptanceQuestion {
            id: "MEDULLA-CQ-CONN-R050.1",
            required: &[("pulselink", "Video")],
            answer: pulselink_videos_with_term_in_transcript_answer,
        },
        AcceptanceQuestion {
            id: "MEDULLA-CQ-CONN-R050.2",
            required: &[("pulselink", "Transcript"), ("searxng", "SearchHit")],
            answer: pulselink_topics_missing_from_archive_answer,
        },
    ]);
}

// spec: MEDULLA-CQ-CONN-R051.3
#[test]
fn qbittorrent_acceptance_through_declared_contracts() {
    assert_acceptance(&[
        AcceptanceQuestion {
            id: "MEDULLA-CQ-CONN-R051.1",
            required: &[("qbittorrent", "Torrent")],
            answer: qbittorrent_stalled_torrents_answer,
        },
        AcceptanceQuestion {
            id: "MEDULLA-CQ-CONN-R051.2",
            required: &[
                ("qbittorrent", "Torrent"),
                ("radarr", "Video"),
                ("sonarr", "MediaEpisode"),
            ],
            answer: qbittorrent_stalled_blocking_wanted_titles_answers,
        },
    ]);
}

// spec: MEDULLA-CQ-CONN-R053.3
#[test]
fn rom_manager_acceptance_through_declared_contracts() {
    assert_acceptance(&[
        AcceptanceQuestion {
            id: "MEDULLA-CQ-CONN-R053.1",
            required: &[("rom-manager", "RomFile")],
            answer: rom_manager_checksum_mismatch_answers,
        },
        AcceptanceQuestion {
            id: "MEDULLA-CQ-CONN-R053.2",
            required: &[
                ("rom-manager", "RomFile"),
                ("media-downloader", "DownloadJob"),
            ],
            answer: rom_manager_downloaded_unconverted_answers,
        },
    ]);
}

// spec: MEDULLA-CQ-CONN-R055.3
#[test]
fn scholarx_acceptance_through_declared_contracts() {
    assert_acceptance(&[
        AcceptanceQuestion {
            id: "MEDULLA-CQ-CONN-R055.1",
            required: &[("scholarx", "ScholarlyPaper")],
            answer: scholarx_recent_citing_papers_answer,
        },
        AcceptanceQuestion {
            id: "MEDULLA-CQ-CONN-R055.2",
            required: &[("scholarx", "ScholarlyPaper"), ("searxng", "SearchHit")],
            answer: scholarx_papers_never_surfaced_by_searxng_answer,
        },
    ]);
}

// spec: MEDULLA-CQ-CONN-R056.3
#[test]
fn searxng_acceptance_through_declared_contracts() {
    assert_acceptance(&[
        AcceptanceQuestion {
            id: "MEDULLA-CQ-CONN-R056.1",
            required: &[("searxng", "SearchResult")],
            answer: searxng_engines_by_result_count_answer,
        },
        AcceptanceQuestion {
            id: "MEDULLA-CQ-CONN-R056.2",
            required: &[("searxng", "SearchHit"), ("archivebox", "WebCapture")],
            answer: searxng_urls_not_archived_answer,
        },
    ]);
}

// spec: MEDULLA-CQ-CONN-R058.3
#[test]
fn sql_acceptance_through_declared_contracts() {
    assert_acceptance(&[
        AcceptanceQuestion {
            id: "MEDULLA-CQ-CONN-R058.1",
            required: &[("sql", "Table")],
            answer: sql_tables_without_primary_key_and_dangling_references_answer,
        },
        AcceptanceQuestion {
            id: "MEDULLA-CQ-CONN-R058.2",
            required: &[("sql", "Table"), ("egeria", "DataAsset")],
            answer: sql_tables_versus_egeria_catalog_answers,
        },
    ]);
}

// spec: MEDULLA-CQ-CONN-R065.3
#[test]
fn vector_acceptance_through_declared_contracts() {
    assert_acceptance(&[
        AcceptanceQuestion {
            id: "MEDULLA-CQ-CONN-R065.1",
            required: &[("vector", "VectorIndex")],
            answer: vector_largest_collections_and_dimension_mismatch_answer,
        },
        AcceptanceQuestion {
            id: "MEDULLA-CQ-CONN-R065.2",
            required: &[("vector", "Embedding"), ("paperless-ngx", "Document")],
            answer: vector_documents_without_embedding_answer,
        },
    ]);
}

// ---------------------------------------------------------------------------

const KG: &str = "http://knuckles.team/kg#";
const XSD: &str = "http://www.w3.org/2001/XMLSchema#";
const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const RDFS_DOMAIN: &str = "http://www.w3.org/2000/01/rdf-schema#domain";
const RDFS_RANGE: &str = "http://www.w3.org/2000/01/rdf-schema#range";
const RDFS_SUBCLASS: &str = "http://www.w3.org/2000/01/rdf-schema#subClassOf";
const OWL: &str = "http://www.w3.org/2002/07/owl#";

/// (module source, term, kind, parent class or domain, range; empty when the
/// domain or range is a union or a datatype listed in the range column).
const TERMS: &[(&str, &str, &str, &str, &str)] = &[
    (MEDIA, "PlaybackProgress", "Class", "Observation", ""),
    (MEDIA, "DownloadJob", "Class", "Task", ""),
    (MEDIA, "RomFile", "Class", "File", ""),
    (
        MEDIA,
        "isMonitored",
        "DatatypeProperty",
        "MediaItem",
        "xsd:boolean",
    ),
    (
        MEDIA,
        "hasFile",
        "DatatypeProperty",
        "MediaItem",
        "xsd:boolean",
    ),
    (
        MEDIA,
        "lastSearchedBy",
        "DatatypeProperty",
        "MediaItem",
        "xsd:string",
    ),
    (
        MEDIA,
        "transferState",
        "DatatypeProperty",
        "Torrent",
        "xsd:string",
    ),
    (
        MEDIA,
        "shareRatio",
        "DatatypeProperty",
        "Torrent",
        "xsd:decimal",
    ),
    (
        MEDIA,
        "lastActivityAt",
        "DatatypeProperty",
        "Torrent",
        "xsd:dateTime",
    ),
    (
        MEDIA,
        "progressOf",
        "ObjectProperty",
        "PlaybackProgress",
        "MediaItem",
    ),
    (
        MEDIA,
        "progressViewer",
        "ObjectProperty",
        "PlaybackProgress",
        "Viewer",
    ),
    (
        MEDIA,
        "progressFraction",
        "DatatypeProperty",
        "PlaybackProgress",
        "xsd:decimal",
    ),
    (
        MEDIA,
        "downloadStatus",
        "DatatypeProperty",
        "DownloadJob",
        "xsd:string",
    ),
    (
        MEDIA,
        "downloadSourceUrl",
        "DatatypeProperty",
        "DownloadJob",
        "xsd:string",
    ),
    (
        MEDIA,
        "downloadError",
        "DatatypeProperty",
        "DownloadJob",
        "xsd:string",
    ),
    (
        MEDIA,
        "downloadFinishedAt",
        "DatatypeProperty",
        "DownloadJob",
        "xsd:dateTime",
    ),
    (MEDIA, "downloadOutput", "ObjectProperty", "DownloadJob", ""),
    (
        MEDIA,
        "peakViewerCount",
        "DatatypeProperty",
        "LiveStream",
        "xsd:integer",
    ),
    (
        MEDIA,
        "streamDurationSeconds",
        "DatatypeProperty",
        "LiveStream",
        "xsd:integer",
    ),
    (MEDIA, "romOf", "ObjectProperty", "RomFile", "GameTitle"),
    (
        MEDIA,
        "expectedChecksum",
        "DatatypeProperty",
        "RomFile",
        "xsd:string",
    ),
    (
        MEDIA,
        "actualChecksum",
        "DatatypeProperty",
        "RomFile",
        "xsd:string",
    ),
    (
        MEDIA,
        "conversionState",
        "DatatypeProperty",
        "RomFile",
        "xsd:string",
    ),
    (DATAPLATFORM, "DocumentCollection", "Class", "Dataset", ""),
    (DATAPLATFORM, "FieldProfile", "Class", "Observation", ""),
    (
        DATAPLATFORM,
        "isPrimaryKey",
        "DatatypeProperty",
        "Column",
        "xsd:boolean",
    ),
    (
        DATAPLATFORM,
        "referencesTable",
        "ObjectProperty",
        "Column",
        "Table",
    ),
    (
        DATAPLATFORM,
        "documentCount",
        "DatatypeProperty",
        "DocumentCollection",
        "xsd:integer",
    ),
    (
        DATAPLATFORM,
        "profiledCollection",
        "ObjectProperty",
        "FieldProfile",
        "DocumentCollection",
    ),
    (
        DATAPLATFORM,
        "fieldName",
        "DatatypeProperty",
        "FieldProfile",
        "xsd:string",
    ),
    (
        DATAPLATFORM,
        "fieldPresenceCount",
        "DatatypeProperty",
        "FieldProfile",
        "xsd:integer",
    ),
    (
        DATAPLATFORM,
        "lastWriteAt",
        "DatatypeProperty",
        "Dataset",
        "xsd:dateTime",
    ),
    (
        DATAPLATFORM,
        "duplicateOf",
        "ObjectProperty",
        "DocumentCollection",
        "Table",
    ),
    (
        DATAPLATFORM,
        "totalSizeBytes",
        "DatatypeProperty",
        "StorageContainer",
        "xsd:integer",
    ),
    (
        DATAPLATFORM,
        "storedInContainer",
        "ObjectProperty",
        "Blob",
        "StorageContainer",
    ),
    (
        DATAPLATFORM,
        "sizeBytes",
        "DatatypeProperty",
        "Blob",
        "xsd:integer",
    ),
    (
        DATAPLATFORM,
        "lastReadAt",
        "DatatypeProperty",
        "Blob",
        "xsd:dateTime",
    ),
    (DATAPLATFORM, "backedByBlob", "ObjectProperty", "", "Blob"),
    (
        DATAPLATFORM,
        "validationMetric",
        "DatatypeProperty",
        "ModelEvaluation",
        "xsd:decimal",
    ),
    (
        DATAPLATFORM,
        "baselineMetric",
        "DatatypeProperty",
        "ModelEvaluation",
        "xsd:decimal",
    ),
    (
        DATAPLATFORM,
        "fineTunedFrom",
        "ObjectProperty",
        "ModelVersion",
        "ModelVersion",
    ),
    (
        DATAPLATFORM,
        "vectorCount",
        "DatatypeProperty",
        "VectorIndex",
        "xsd:integer",
    ),
    (
        DATAPLATFORM,
        "indexDimension",
        "DatatypeProperty",
        "VectorIndex",
        "xsd:integer",
    ),
    (
        DATAPLATFORM,
        "indexedWithModel",
        "ObjectProperty",
        "VectorIndex",
        "EmbeddingModel",
    ),
    (
        DATAPLATFORM,
        "embeddingDimension",
        "DatatypeProperty",
        "EmbeddingModel",
        "xsd:integer",
    ),
    (RETRIEVAL, "ScholarlyPaper", "Class", "Document", ""),
    (
        RETRIEVAL,
        "resourceUrl",
        "DatatypeProperty",
        "Document",
        "xsd:string",
    ),
    (
        RETRIEVAL,
        "firstSeenAt",
        "DatatypeProperty",
        "Document",
        "xsd:dateTime",
    ),
    (
        RETRIEVAL,
        "mentionsTopic",
        "ObjectProperty",
        "Document",
        "Topic",
    ),
    (
        RETRIEVAL,
        "aboutTopic",
        "ObjectProperty",
        "ScholarlyPaper",
        "Topic",
    ),
    (
        RETRIEVAL,
        "cites",
        "ObjectProperty",
        "ScholarlyPaper",
        "ScholarlyPaper",
    ),
    (
        RETRIEVAL,
        "publicationYear",
        "DatatypeProperty",
        "ScholarlyPaper",
        "xsd:integer",
    ),
    (
        RETRIEVAL,
        "rageClickRate",
        "DatatypeProperty",
        "WebResource",
        "xsd:decimal",
    ),
    (
        RETRIEVAL,
        "deadClickRate",
        "DatatypeProperty",
        "WebResource",
        "xsd:decimal",
    ),
    (PERSONAL, "TranscriptSegment", "Class", "Fragment", ""),
    (
        PERSONAL,
        "segmentOf",
        "ObjectProperty",
        "TranscriptSegment",
        "Transcript",
    ),
    (
        PERSONAL,
        "segmentConfidence",
        "DatatypeProperty",
        "TranscriptSegment",
        "xsd:decimal",
    ),
    (
        PERSONAL,
        "hasTranscript",
        "ObjectProperty",
        "",
        "Transcript",
    ),
];

#[test]
fn connector_vocabulary_terms_are_declared_by_their_owning_module() {
    for (source, name, kind, parent_or_domain, range) in TERMS {
        let triples = parse_all(&[source]);
        let subject = format!("{KG}{name}");
        assert!(
            has_iri(&triples, &subject, RDF_TYPE, &format!("{OWL}{kind}")),
            "{name} must be declared as owl:{kind} in its owning module"
        );
        if parent_or_domain.is_empty() {
            continue;
        }
        let predicate = if *kind == "Class" {
            RDFS_SUBCLASS
        } else {
            RDFS_DOMAIN
        };
        assert!(
            has_iri(
                &triples,
                &subject,
                predicate,
                &format!("{KG}{parent_or_domain}")
            ),
            "{name} must name {parent_or_domain} as its {}",
            if *kind == "Class" { "parent" } else { "domain" }
        );
        if range.is_empty() {
            continue;
        }
        let range_iri = match range.strip_prefix("xsd:") {
            Some(local) => format!("{XSD}{local}"),
            None => format!("{KG}{range}"),
        };
        assert!(
            has_iri(&triples, &subject, RDFS_RANGE, &range_iri),
            "{name} must have range {range}"
        );
    }
}
