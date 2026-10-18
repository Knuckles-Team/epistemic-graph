//! Deterministic web fetch selection (EG-DECISION-ENGINE-R802.2).
//!
//! Pure: the answer depends only on the request, the held pages and the
//! clock passed in. The cost is O(c log c) in candidates.
//!
//! Score, in millionths: `0.5 * relevance + 0.3 * rank + 0.2 * freshness`.
//! - relevance: the share of distinct query terms found in the title (weight
//!   2), the snippet (1) and the URL (1);
//! - rank: reciprocal rank fusion over the engines that returned the URL,
//!   `sum(1 / (60 + rank))`, scaled so one first place is 1;
//! - freshness: `30 / (30 + age_days)` from the publication time, 0.5 when
//!   unknown.

use std::collections::{BTreeMap, BTreeSet};

use eg_types::decision::web_fetch_selection::{
    SelectWebPagesAnswer, SelectWebPagesRequest, SelectedPage, SkipReason, SkippedPage,
    DEFAULT_FRESHNESS_WINDOW_S,
};

const RRF_K: f64 = 60.0;
const MICROS: f64 = 1_000_000.0;
const DAY_MS: f64 = 86_400_000.0;

/// What the request graph already holds for one canonical URL.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct HeldPage {
    pub retrieved_at_ms: Option<u64>,
    pub content_digest: Option<String>,
}

/// Canonical form of an absolute http(s) URL: lower-case scheme and host, no
/// default port, no fragment, no tracking parameters, sorted query, and no
/// trailing slash except at the root. `None` for anything else.
pub fn canonical_url(raw: &str) -> Option<String> {
    let raw = raw.trim();
    let (scheme, rest) = raw.split_once("://")?;
    let scheme = scheme.to_ascii_lowercase();
    if scheme != "http" && scheme != "https" {
        return None;
    }
    let rest = rest.split('#').next().unwrap_or_default();
    let (authority, path_query) = match rest.find(['/', '?']) {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, ""),
    };
    let host = authority
        .rsplit('@')
        .next()
        .unwrap_or_default()
        .to_ascii_lowercase();
    let default_port = if scheme == "http" { ":80" } else { ":443" };
    let host = host.strip_suffix(default_port).unwrap_or(&host);
    if host.is_empty() || host.contains(char::is_whitespace) {
        return None;
    }
    let (path, query) = path_query.split_once('?').unwrap_or((path_query, ""));
    let path = path.trim_end_matches('/');
    let mut params: Vec<&str> = query
        .split('&')
        .filter(|p| !p.is_empty() && !is_tracking_param(p))
        .collect();
    params.sort_unstable();
    let mut out = format!(
        "{scheme}://{host}{}",
        if path.is_empty() { "/" } else { path }
    );
    if !params.is_empty() {
        out.push('?');
        out.push_str(&params.join("&"));
    }
    Some(out)
}

fn is_tracking_param(param: &str) -> bool {
    let key = param
        .split('=')
        .next()
        .unwrap_or_default()
        .to_ascii_lowercase();
    key.starts_with("utm_") || matches!(key.as_str(), "fbclid" | "gclid" | "msclkid" | "ref_src")
}

fn terms(text: &str) -> BTreeSet<String> {
    text.split(|c: char| !c.is_alphanumeric())
        .filter(|t| t.chars().count() >= 2)
        .map(str::to_lowercase)
        .collect()
}

fn micros(value: f64) -> u64 {
    (value.clamp(0.0, 1.0) * MICROS).round() as u64
}

/// One canonical URL after fusing its duplicate rows.
struct Fused {
    first: usize,
    rrf: f64,
    engines: BTreeSet<String>,
}

fn skip(url: &str, canonical: &str, reason: SkipReason, detail: String) -> SkippedPage {
    SkippedPage {
        url: url.to_string(),
        canonical_url: canonical.to_string(),
        reason,
        detail,
    }
}

/// Fuse rows by canonical URL; invalid URLs and fused duplicates are skipped.
fn fuse(request: &SelectWebPagesRequest, skipped: &mut Vec<SkippedPage>) -> Vec<(String, Fused)> {
    let mut fused: BTreeMap<String, Fused> = BTreeMap::new();
    for (index, row) in request.candidates.iter().enumerate() {
        let Some(canonical) = canonical_url(&row.url) else {
            skipped.push(skip(
                &row.url,
                "",
                SkipReason::InvalidUrl,
                "not an absolute http(s) URL".into(),
            ));
            continue;
        };
        let contribution = 1.0 / (RRF_K + f64::from(row.rank.max(1)));
        match fused.get_mut(&canonical) {
            Some(entry) => {
                entry.rrf += contribution;
                entry.engines.insert(row.engine.clone());
                let detail = format!("rank fused into row {}", entry.first);
                skipped.push(skip(&row.url, &canonical, SkipReason::DuplicateUrl, detail));
            }
            None => {
                let engines = BTreeSet::from([row.engine.clone()]);
                fused.insert(
                    canonical,
                    Fused {
                        first: index,
                        rrf: contribution,
                        engines,
                    },
                );
            }
        }
    }
    let mut ordered: Vec<_> = fused.into_iter().collect();
    ordered.sort_by_key(|(_, entry)| entry.first);
    ordered
}

/// Select pages to fetch. `held` maps canonical URL to what the graph holds;
/// `held_digests` is every content digest the graph holds.
pub fn select_web_pages(
    request: &SelectWebPagesRequest,
    held: &BTreeMap<String, HeldPage>,
    held_digests: &BTreeSet<String>,
    now_ms: u64,
    query_digest: String,
) -> Result<SelectWebPagesAnswer, String> {
    request.validate()?;
    let window_ms = request
        .freshness_window_s
        .unwrap_or(DEFAULT_FRESHNESS_WINDOW_S)
        .saturating_mul(1_000);
    let query_terms = terms(&request.query);
    let mut skipped = Vec::new();
    let mut seen_digests = BTreeSet::new();
    let mut scored = Vec::new();
    for (canonical, entry) in fuse(request, &mut skipped) {
        let row = &request.candidates[entry.first];
        let held_page = held.get(&canonical);
        if let Some(digest) = row.content_digest.as_deref() {
            if held_digests.contains(digest) || !seen_digests.insert(digest.to_string()) {
                let detail = format!("content digest {digest} already held or listed");
                skipped.push(skip(
                    &row.url,
                    &canonical,
                    SkipReason::DuplicateDigest,
                    detail,
                ));
                continue;
            }
        }
        let age_ms = held_page
            .and_then(|h| h.retrieved_at_ms)
            .map(|t| now_ms.saturating_sub(t));
        if age_ms.is_some_and(|age| age < window_ms) {
            let detail = format!(
                "held copy is {}s old, window {}s",
                age_ms.unwrap_or(0) / 1_000,
                window_ms / 1_000
            );
            skipped.push(skip(&row.url, &canonical, SkipReason::HeldFresh, detail));
            continue;
        }
        scored.push(score(
            row,
            canonical,
            entry,
            &query_terms,
            held_page.is_some(),
            now_ms,
        ));
    }
    scored.sort_by(|a, b| {
        b.score_micros
            .cmp(&a.score_micros)
            .then_with(|| a.canonical_url.cmp(&b.canonical_url))
    });
    let budget = request.max_fetches as usize;
    let mut selected = Vec::new();
    for (position, mut page) in scored.into_iter().enumerate() {
        if position < budget {
            page.order = position as u32;
            selected.push(page);
        } else {
            let detail = format!("position {} is past the budget of {budget}", position + 1);
            skipped.push(skip(
                &page.url,
                &page.canonical_url,
                SkipReason::OverBudget,
                detail,
            ));
        }
    }
    Ok(SelectWebPagesAnswer {
        query_digest,
        selected,
        skipped,
        max_fetches: request.max_fetches,
        considered: request.candidates.len() as u32,
        now_ms,
    })
}

fn score(
    row: &eg_types::decision::web_fetch_selection::WebCandidate,
    canonical: String,
    entry: Fused,
    query_terms: &BTreeSet<String>,
    refresh: bool,
    now_ms: u64,
) -> SelectedPage {
    let (title, snippet, url) = (terms(&row.title), terms(&row.snippet), terms(&canonical));
    let hits: usize = query_terms
        .iter()
        .map(|t| {
            2 * usize::from(title.contains(t))
                + usize::from(snippet.contains(t))
                + usize::from(url.contains(t))
        })
        .sum();
    let relevance = hits as f64 / (4 * query_terms.len().max(1)) as f64;
    let rank = entry.rrf * (RRF_K + 1.0);
    let freshness = row.published_at_ms.map_or(0.5, |published| {
        let age_days = now_ms.saturating_sub(published) as f64 / DAY_MS;
        30.0 / (30.0 + age_days)
    });
    let (relevance, rank, freshness) = (micros(relevance), micros(rank), micros(freshness));
    let score_micros = (5 * relevance + 3 * rank + 2 * freshness) / 10;
    let mut reasons = vec![
        format!(
            "relevance {:.3}: {hits} weighted query-term hits",
            relevance as f64 / MICROS
        ),
        format!(
            "rank {:.3} from {} engine(s)",
            rank as f64 / MICROS,
            entry.engines.len()
        ),
        format!("freshness {:.3}", freshness as f64 / MICROS),
    ];
    if refresh {
        reasons.push("refresh: the held copy is older than the freshness window".into());
    }
    SelectedPage {
        order: 0,
        url: row.url.clone(),
        canonical_url: canonical,
        title: row.title.clone(),
        score_micros,
        relevance_micros: relevance,
        rank_micros: rank,
        freshness_micros: freshness,
        engines: entry.engines.into_iter().collect(),
        refresh,
        reasons,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use eg_types::decision::web_fetch_selection::WebCandidate;

    const NOW: u64 = 1_800_000_000_000;

    fn row(url: &str, title: &str, engine: &str, rank: u32) -> WebCandidate {
        WebCandidate {
            url: url.into(),
            title: title.into(),
            snippet: String::new(),
            engine: engine.into(),
            rank,
            content_digest: None,
            published_at_ms: None,
        }
    }

    fn request(candidates: Vec<WebCandidate>, max_fetches: u32) -> SelectWebPagesRequest {
        SelectWebPagesRequest {
            query: "rust document chunking".into(),
            candidates,
            max_fetches,
            freshness_window_s: None,
            now_ms: None,
        }
    }

    #[test]
    fn canonical_urls() {
        assert_eq!(
            canonical_url("HTTPS://Example.ORG:443/a/b/?utm_source=x&z=1&a=2#frag").as_deref(),
            Some("https://example.org/a/b?a=2&z=1")
        );
        assert_eq!(
            canonical_url("http://example.org").as_deref(),
            Some("http://example.org/")
        );
        assert_eq!(canonical_url("ftp://example.org/x"), None);
        assert_eq!(canonical_url("not a url"), None);
    }

    // spec: EG-DECISION-ENGINE-R802.2
    #[test]
    fn ranks_dedupes_and_skips_fresh() {
        let mut digest_row = row("https://d.example/x", "rust chunking", "bing", 4);
        digest_row.content_digest = Some("sha256:held".into());
        let mut fresh_pub = row("https://e.example/new", "chunking", "bing", 5);
        fresh_pub.published_at_ms = Some(NOW - 1_000);
        let candidates = vec![
            row(
                "https://a.example/rust-chunking",
                "Rust document chunking guide",
                "google",
                1,
            ),
            row("https://b.example/other", "Unrelated", "google", 2),
            row(
                "https://a.example/rust-chunking/?utm_source=x",
                "dup",
                "bing",
                1,
            ),
            row(
                "https://held.example/page",
                "Rust document chunking",
                "google",
                3,
            ),
            digest_row,
            fresh_pub,
            row("mailto:x", "bad", "google", 6),
        ];
        let held = BTreeMap::from([(
            "https://held.example/page".to_string(),
            HeldPage {
                retrieved_at_ms: Some(NOW - 3_600_000),
                content_digest: None,
            },
        )]);
        let digests = BTreeSet::from(["sha256:held".to_string()]);
        let answer = select_web_pages(
            &request(candidates.clone(), 2),
            &held,
            &digests,
            NOW,
            "q".into(),
        )
        .unwrap();
        let reasons: BTreeMap<_, _> = answer
            .skipped
            .iter()
            .map(|s| (s.url.as_str(), s.reason))
            .collect();
        assert_eq!(
            reasons["https://a.example/rust-chunking/?utm_source=x"],
            SkipReason::DuplicateUrl
        );
        assert_eq!(reasons["https://held.example/page"], SkipReason::HeldFresh);
        assert_eq!(reasons["https://d.example/x"], SkipReason::DuplicateDigest);
        assert_eq!(reasons["mailto:x"], SkipReason::InvalidUrl);
        assert_eq!(answer.selected.len(), 2, "the budget holds");
        assert_eq!(
            answer.selected[0].canonical_url,
            "https://a.example/rust-chunking"
        );
        assert_eq!(
            answer.selected[0].engines,
            vec!["bing", "google"],
            "duplicate rows fuse"
        );
        assert!(answer
            .skipped
            .iter()
            .any(|s| s.reason == SkipReason::OverBudget));
        assert_eq!(
            answer.selected.len() + answer.skipped.len(),
            candidates.len()
        );
        // A stale held copy is selectable again, marked as a refresh.
        let stale = BTreeMap::from([(
            "https://held.example/page".to_string(),
            HeldPage {
                retrieved_at_ms: Some(NOW - 2 * 86_400_000),
                content_digest: None,
            },
        )]);
        let again = select_web_pages(
            &request(candidates.clone(), 8),
            &stale,
            &digests,
            NOW,
            "q".into(),
        )
        .unwrap();
        assert!(again
            .selected
            .iter()
            .any(|p| p.canonical_url == "https://held.example/page" && p.refresh));
        // Deterministic: the same inputs give the same answer.
        assert_eq!(
            again,
            select_web_pages(&request(candidates, 8), &stale, &digests, NOW, "q".into()).unwrap()
        );
    }

    // spec: EG-DECISION-ENGINE-R802.4
    #[test]
    fn web_fetch_refusals() {
        let none = BTreeMap::new();
        let digests = BTreeSet::new();
        let empty = request(Vec::new(), 1);
        assert!(select_web_pages(&empty, &none, &digests, NOW, "q".into())
            .unwrap_err()
            .contains("NO_CANDIDATES"));
        let over = request(vec![row("https://a.example/", "a", "g", 1)], 33);
        assert!(select_web_pages(&over, &none, &digests, NOW, "q".into())
            .unwrap_err()
            .contains("BUDGET_INVALID"));
    }

    // spec: EG-DECISION-ENGINE-R802.5
    #[test]
    fn premises_per_candidate() {
        let candidates: Vec<_> = (0..40)
            .map(|i| {
                row(
                    &format!("https://site{}.example/p", i % 30),
                    "rust chunking",
                    "searxng",
                    i + 1,
                )
            })
            .collect();
        let answer = select_web_pages(
            &request(candidates, 10),
            &BTreeMap::new(),
            &BTreeSet::new(),
            NOW,
            "q".into(),
        )
        .unwrap();
        assert_eq!(
            answer.selected.len() + answer.skipped.len(),
            40,
            "one premise per candidate"
        );
        assert_eq!(answer.selected.len(), 10);
        for (i, page) in answer.selected.iter().enumerate() {
            assert_eq!(page.order as usize, i);
            assert_eq!(page.reasons.len(), 3);
        }
        assert!(answer
            .selected
            .windows(2)
            .all(|w| w[0].score_micros >= w[1].score_micros));
    }
}
