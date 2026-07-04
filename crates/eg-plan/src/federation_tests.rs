//! Federation proofs (CONCEPT:EG-KG.query.query-federation, Lane P): a FOREIGN source composes with the
//! LOCAL graph in ONE plan, and the fused result equals the manual join.
//!
//! The foreign source here is a MOCK HTTP/JSON API served by a tiny in-test TCP server
//! (no external network), exercising the `HttpJsonSource` kind end-to-end through the
//! real `ureq` rustls client. The 2nd-in-process-ENGINE proof (the `RemoteEngine` kind)
//! lives in the facade crate where `ServerState` exists. Both prove the same property:
//! `ForeignScan -> Filter/Join-with-local -> Rank -> Limit` == the manual join.

use std::collections::HashSet;
use std::io::{Read, Write};
use std::net::TcpListener;

use eg_types::wire::{ForeignSourceSpec, HttpFieldMap};

use crate::algebra::Op;
use crate::exec::{execute, PlanCtx};
use crate::rowset::RowSet;
use crate::Plan;

/// Spawn a one-shot HTTP server on an ephemeral port that returns `body` (a JSON
/// string) to the first GET, then exits. Returns its `http://host:port/` URL.
fn spawn_mock_json(body: &'static str) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock http");
    let addr = listener.local_addr().unwrap();
    std::thread::spawn(move || {
        if let Ok((mut stream, _)) = listener.accept() {
            // Drain the request line/headers (we don't need them).
            let mut buf = [0u8; 1024];
            let _ = stream.read(&mut buf);
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = stream.write_all(resp.as_bytes());
        }
    });
    format!("http://{addr}/")
}

#[test]
fn foreign_scan_reads_an_http_json_source_as_a_rowset() {
    // The external API returns a list of records; field_map says id="ref", score="w".
    let addr = spawn_mock_json(r#"{"data":[{"ref":"d2","w":0.7},{"ref":"d4","w":0.4}]}"#);
    let spec = ForeignSourceSpec::HttpJson {
        url: addr,
        json_path: "data".into(),
        field_map: HttpFieldMap {
            id: "ref".into(),
            score: Some("w".into()),
        },
    };
    let rs = crate::federation::source_for(&spec).fetch().unwrap();
    assert_eq!(
        rs.ids(),
        vec!["d2", "d4"],
        "foreign ids from json_path array"
    );
    // The score field is projected too.
    assert_eq!(rs.rows()[0].score, Some(0.7));
}

/// THE compose proof: a plan that JOINS a foreign HTTP source with the local graph
/// (`Scan -> Filter(year>2023) -> ForeignScan(join) -> Rank -> Limit`) equals the
/// MANUAL join (local-filter ids ∩ foreign ids, vector-ranked, top-k) done by hand.
///
/// Fixture data (from `crate::fixture`): Doc years d1=2025, d2=2025, d3=2023, d4=2024;
/// the query vec `[1,0,0,0]` ranks d2 > d4 > d3 among the candidates.
#[test]
fn foreign_join_with_local_equals_manual_join() {
    let fx = crate::fixture::build();
    let query = crate::fixture::query_vec(); // [1,0,0,0]

    // The foreign source returns {d1, d2, d4} (note: d3 is year 2023 — the LOCAL filter
    // `year > 2023` drops it anyway; d1 IS in the foreign set but the JOIN keeps it only
    // if it also survives the local filter, which it does — d1 is year 2025).
    let addr = spawn_mock_json(r#"[{"k":"d2"},{"k":"d4"}]"#);
    let foreign_spec = ForeignSourceSpec::HttpJson {
        url: addr,
        json_path: "".into(), // the response IS the array
        field_map: HttpFieldMap {
            id: "k".into(),
            score: None,
        },
    };

    // ── the federated plan ──
    let plan = Plan::new(vec![
        Op::Scan {
            label: "Doc".into(),
        },
        Op::Filter {
            preds: vec![eg_types::wire::Pred::GtNum {
                prop: "year".into(),
                n: 2023.0,
            }],
        },
        // Intersect the local-filtered set with the foreign source's ids (foreign∩local).
        Op::ForeignScan {
            source: foreign_spec,
            join: true,
        },
        Op::Rank {
            query: query.clone(),
        },
        Op::Limit { k: 10 },
    ]);
    let ctx = PlanCtx::new(&fx.view, &fx.semantic);
    let fused = execute(&plan, &ctx).unwrap();

    // ── the manual join (the oracle) ──
    // Local filter: Docs with year > 2023 → {d1, d2, d4} (d3 is 2023, d5 not a year>2023
    // Doc relevant here; the fixture also has d5=2025 — include it to be faithful).
    let local_filtered: HashSet<&str> = ["d1", "d2", "d4", "d5"].into_iter().collect();
    // Foreign ids: {d2, d4}.
    let foreign: HashSet<&str> = ["d2", "d4"].into_iter().collect();
    // Join = intersection → {d2, d4}; rank by the SAME query vector, top-k.
    let joined: HashSet<String> = local_filtered
        .intersection(&foreign)
        .map(|s| s.to_string())
        .collect();
    let ranked = fx.semantic.semantic_search(&query, 32);
    let manual: Vec<String> = ranked
        .into_iter()
        .map(|(id, _)| id)
        .filter(|id| joined.contains(id))
        .collect();

    assert_eq!(
        fused.ids(),
        manual,
        "federated foreign∩local plan must equal the manual join"
    );
    assert_eq!(
        fused.ids(),
        vec!["d2", "d4"],
        "ranked: d2 (closest) then d4"
    );
}

/// A `ForeignScan` with `join=false` is a pure SOURCE: it REPLACES the input, exactly
/// like `Scan` — so a foreign source can SEED a plan that then ranks/limits locally.
#[test]
fn foreign_scan_as_a_pure_source_replaces_the_input() {
    let fx = crate::fixture::build();
    let addr = spawn_mock_json(r#"[{"k":"d3"},{"k":"d4"}]"#);
    let spec = ForeignSourceSpec::HttpJson {
        url: addr,
        json_path: "".into(),
        field_map: HttpFieldMap {
            id: "k".into(),
            score: None,
        },
    };
    // Scan would seed all Docs, but the non-join ForeignScan replaces it with {d3,d4}.
    let plan = Plan::new(vec![
        Op::Scan {
            label: "Doc".into(),
        },
        Op::ForeignScan {
            source: spec,
            join: false,
        },
        Op::Limit { k: 10 },
    ]);
    let ctx = PlanCtx::new(&fx.view, &fx.semantic);
    let out: RowSet = execute(&plan, &ctx).unwrap();
    let mut ids = out.ids();
    ids.sort();
    assert_eq!(
        ids,
        vec!["d3", "d4"],
        "foreign source replaced the local scan"
    );
}
