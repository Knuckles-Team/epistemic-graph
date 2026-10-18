//! EG-DECISION-ENGINE-R802.6: select from 50 search candidates, 10 of them
//! held and fresh; JSON on stdout. Budget: p99 3 ms in the optimized profile.
//! Run with `cargo bench -p eg-numeric --features decision --bench web_fetch_selection`.

use std::collections::{BTreeMap, BTreeSet};
use std::hint::black_box;
use std::time::Instant;

use eg_numeric::decision::web_fetch_selection::{select_web_pages, HeldPage};
use eg_types::decision::web_fetch_selection::{SelectWebPagesRequest, WebCandidate};

const NOW: u64 = 1_800_000_000_000;
const SAMPLES: usize = 2_000;

fn main() {
    let candidates: Vec<WebCandidate> = (0..50)
        .map(|i| WebCandidate {
            url: format!("https://site{}.example/page/{}?utm_source=feed", i % 45, i),
            title: format!("Result {i} about web ingest and citation offsets"),
            snippet: "A page that explains chunking, embeddings and retrieval.".into(),
            engine: ["searxng", "bing", "duckduckgo"][i % 3].into(),
            rank: (i / 3 + 1) as u32,
            content_digest: (i % 7 == 0).then(|| format!("sha256:{:064x}", i % 14)),
            published_at_ms: Some(NOW - (i as u64) * 86_400_000),
        })
        .collect();
    let held: BTreeMap<String, HeldPage> = (0..10)
        .map(|i| {
            let url =
                eg_numeric::decision::web_fetch_selection::canonical_url(&candidates[i * 4].url)
                    .unwrap();
            (
                url,
                HeldPage {
                    retrieved_at_ms: Some(NOW - 3_600_000),
                    content_digest: None,
                },
            )
        })
        .collect();
    let request = SelectWebPagesRequest {
        query: "web ingest citation offsets".into(),
        candidates,
        max_fetches: 8,
        freshness_window_s: None,
        now_ms: Some(NOW),
    };
    let digests = BTreeSet::new();
    let mut micros = Vec::with_capacity(SAMPLES);
    let mut selected = 0;
    for _ in 0..SAMPLES {
        let start = Instant::now();
        let answer = black_box(
            select_web_pages(black_box(&request), &held, &digests, NOW, "q".into()).unwrap(),
        );
        micros.push(start.elapsed().as_micros());
        selected = answer.selected.len();
        assert!(answer
            .selected
            .iter()
            .all(|p| !held.contains_key(&p.canonical_url)));
    }
    micros.sort_unstable();
    let (p50, p99) = (micros[SAMPLES / 2], micros[SAMPLES * 99 / 100]);
    println!(
        "{{\"bench\":\"web_fetch_selection\",\"candidates\":50,\"selected\":{selected},\
         \"p50_us\":{p50},\"p99_us\":{p99},\"budget_p99_us\":3000,\"optimized\":{}}}",
        !cfg!(debug_assertions)
    );
    if !cfg!(debug_assertions) {
        assert!(p99 <= 3_000, "selection exceeded its budget");
    }
}
