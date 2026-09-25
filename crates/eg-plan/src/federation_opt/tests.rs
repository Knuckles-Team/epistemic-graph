//! EH-563 phase-1 proofs. Oracles: (E) the optimized plan returns exactly the rows of the
//! naive full fetch; (B) what crossed the network — requests and items the mock source
//! served; (S) typed refusals. The HTTP proofs drive the real executor and the real
//! `HttpJsonSource` against a recording in-test API server (loopback, exact allow-list).

use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use eg_types::wire::{ForeignSourceSpec, HttpFieldMap};

use super::capability::{KeyLookup, LimitPushdown, RemoteRequest, SourceCapabilities, SourceRate};
use super::remote::{Identity, RemoteFetch};
use super::run::Fragment;
use super::stats::fingerprint;
use super::{FederationBudget, FederationSession, FetchStrategy, BUDGET_EXCEEDED, REQUIRES_KEYS};
use crate::algebra::Op;
use crate::exec::{execute, PlanCtx};
use crate::federation_tests::MockHttpAllowGuard;
use crate::rowset::RowSet;
use crate::Plan;

// ── a recording JSON API ─────────────────────────────────────────────────────────

/// What the mock API has served.
#[derive(Default)]
struct Served {
    requests: AtomicUsize,
    items: AtomicUsize,
    queries: Mutex<Vec<String>>,
}

/// A loopback JSON API over `catalog`: `ids=` selects by id (comma list), `offset=`/`page=`
/// + `limit=` page it, `limit=` alone truncates; page size is capped at `cap`.
struct MockApi {
    base: String,
    served: Arc<Served>,
}

fn param<'q>(query: &'q str, name: &str) -> Option<&'q str> {
    query
        .split('&')
        .find_map(|kv| kv.strip_prefix(name)?.strip_prefix('='))
}

fn decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let hex = if bytes[i] == b'%' {
            value
                .get(i + 1..i + 3)
                .and_then(|h| u8::from_str_radix(h, 16).ok())
        } else {
            None
        };
        match hex {
            Some(b) => {
                out.push(b);
                i += 3;
            }
            None => {
                out.push(bytes[i]);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// The items one request selects.
fn select(catalog: &[String], query: &str, cap: usize) -> Vec<String> {
    if let Some(ids) = param(query, "ids") {
        let wanted: Vec<String> = ids.split(',').map(decode).collect();
        return catalog
            .iter()
            .filter(|c| wanted.contains(c))
            .cloned()
            .collect();
    }
    let limit = param(query, "limit")
        .and_then(|v| v.parse().ok())
        .unwrap_or(cap)
        .min(cap);
    let offset = match (param(query, "offset"), param(query, "page")) {
        (Some(o), _) => o.parse().unwrap_or(0),
        (None, Some(p)) => p.parse::<usize>().unwrap_or(1).saturating_sub(1) * limit,
        (None, None) => 0,
    };
    catalog.iter().skip(offset).take(limit).cloned().collect()
}

impl MockApi {
    fn spawn(catalog: Vec<String>, cap: usize) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock api");
        let base = format!("http://{}", listener.local_addr().unwrap());
        let served = Arc::new(Served::default());
        let record = served.clone();
        std::thread::spawn(move || {
            for stream in listener.incoming().take(64) {
                let Ok(mut stream) = stream else { continue };
                let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
                let mut buf = [0u8; 8192];
                let n = stream.read(&mut buf).unwrap_or(0);
                let head = String::from_utf8_lossy(&buf[..n]).into_owned();
                let target = head.split_whitespace().nth(1).unwrap_or("/");
                let query = target.split_once('?').map_or("", |(_, q)| q).to_string();
                let items = select(&catalog, &query, cap);
                record.requests.fetch_add(1, Ordering::SeqCst);
                record.items.fetch_add(items.len(), Ordering::SeqCst);
                record.queries.lock().unwrap().push(query);
                let body = serde_json::json!({
                    "data": items.iter().map(|id| serde_json::json!({"ref": id})).collect::<Vec<_>>()
                })
                .to_string();
                let _ = stream.write_all(
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    )
                    .as_bytes(),
                );
            }
        });
        Self { base, served }
    }

    fn spec(&self, query: &str) -> ForeignSourceSpec {
        ForeignSourceSpec::HttpJson {
            url: format!("{}/items{query}", self.base),
            json_path: "data".into(),
            field_map: HttpFieldMap {
                id: "ref".into(),
                score: None,
            },
        }
    }

    fn requests(&self) -> usize {
        self.served.requests.load(Ordering::SeqCst)
    }

    fn items(&self) -> usize {
        self.served.items.load(Ordering::SeqCst)
    }
}

/// `n` catalog ids; the fixture's `d2` and `d4` are among them.
fn catalog(n: usize) -> Vec<String> {
    let mut ids = vec!["d2".to_string(), "d4".to_string()];
    ids.extend((0..n.saturating_sub(2)).map(|i| format!("x{i}")));
    ids
}

fn ids(rows: &RowSet) -> Vec<String> {
    rows.ids()
}

// ── (E)+(B): HTTP through the executor ──────────────────────────────────────────

#[test]
fn a_foreign_join_ships_local_ids_instead_of_fetching_the_source() {
    let fx = crate::fixture::build();
    let api = MockApi::spawn(catalog(1000), 1000);
    let _allow = MockHttpAllowGuard::new(&api.base);
    let spec = api.spec("?ids={keys}");
    let local = execute(
        &Plan::new(vec![Op::Scan {
            label: "Doc".into(),
        }]),
        &PlanCtx::new(&fx.view, &fx.semantic),
    )
    .unwrap();
    let expected: Vec<String> = ids(&local)
        .into_iter()
        .filter(|id| id == "d2" || id == "d4")
        .collect();

    let session = FederationSession::from_env();
    let ctx = PlanCtx::new(&fx.view, &fx.semantic).with_federation(&session);
    let plan = Plan::new(vec![
        Op::Scan {
            label: "Doc".into(),
        },
        Op::ForeignScan {
            source: Box::new(spec),
            join: true,
        },
    ]);
    let joined = execute(&plan, &ctx).unwrap();

    assert_eq!(
        ids(&joined),
        expected,
        "(E) the bind join equals local ∩ catalog"
    );
    assert_eq!(api.requests(), 1, "(B) one batch carried every local id");
    assert_eq!(
        api.items(),
        2,
        "(B) two items moved, not the 1000-item catalog"
    );
    let trace = session.trace();
    assert_eq!(trace[0].strategy, FetchStrategy::BindJoin);
    assert_eq!(trace[0].keys_pushed, local.len());
}

#[test]
fn a_paged_source_is_read_to_its_end_not_truncated_to_page_one() {
    let api = MockApi::spawn(catalog(250), 40);
    let _allow = MockHttpAllowGuard::new(&api.base);
    let spec = api.spec("?offset={offset}&limit={limit}");
    let naive = super::foreign_source_rows(&spec, None).unwrap();
    assert_eq!(naive.len(), 40, "the naive single GET sees page one only");

    let fx = crate::fixture::build();
    let session = FederationSession::from_env();
    let ctx = PlanCtx::new(&fx.view, &fx.semantic).with_federation(&session);
    let plan = Plan::new(vec![Op::ForeignScan {
        source: Box::new(spec),
        join: false,
    }]);
    let rows = execute(&plan, &ctx).unwrap();
    assert_eq!(ids(&rows), catalog(250), "(E) every page, in source order");
    assert_eq!(session.trace()[0].strategy, FetchStrategy::Paged);
}

#[test]
fn a_following_limit_is_pushed_into_the_source() {
    let api = MockApi::spawn(catalog(500), 1000);
    let _allow = MockHttpAllowGuard::new(&api.base);
    let spec = api.spec("?limit={limit}");
    let fx = crate::fixture::build();
    let session = FederationSession::from_env();
    let ctx = PlanCtx::new(&fx.view, &fx.semantic).with_federation(&session);
    let plan = Plan::new(vec![
        Op::ForeignScan {
            source: Box::new(spec),
            join: false,
        },
        Op::Limit { k: 5 },
    ]);
    let rows = execute(&plan, &ctx).unwrap();
    assert_eq!(
        ids(&rows),
        catalog(500)[..5].to_vec(),
        "(E) the first five, as the naive plan"
    );
    assert_eq!(api.items(), 5, "(B) five items moved");
    assert!(api
        .served
        .queries
        .lock()
        .unwrap()
        .iter()
        .all(|q| q == "limit=5"));
    assert_eq!(session.trace()[0].limit_pushed, Some(5));
}

#[test]
fn a_key_only_source_refuses_a_scan_without_keys() {
    let api = MockApi::spawn(catalog(10), 10);
    let _allow = MockHttpAllowGuard::new(&api.base);
    let fx = crate::fixture::build();
    let ctx = PlanCtx::new(&fx.view, &fx.semantic);
    let plan = Plan::new(vec![Op::ForeignScan {
        source: Box::new(api.spec("?ids={keys}")),
        join: false,
    }]);
    let err = execute(&plan, &ctx).unwrap_err();
    assert!(err.starts_with(REQUIRES_KEYS), "{err}");
    assert_eq!(api.requests(), 0, "(S) refused before any request");
}

#[test]
fn the_request_budget_refuses_instead_of_returning_partial_rows() {
    let api = MockApi::spawn(catalog(250), 40);
    let _allow = MockHttpAllowGuard::new(&api.base);
    let fx = crate::fixture::build();
    let session = FederationSession::new(FederationBudget {
        max_requests: 2,
        ..FederationBudget::default()
    });
    let ctx = PlanCtx::new(&fx.view, &fx.semantic).with_federation(&session);
    let plan = Plan::new(vec![Op::ForeignScan {
        source: Box::new(api.spec("?page={page}&limit={limit}")),
        join: false,
    }]);
    let err = execute(&plan, &ctx).unwrap_err();
    assert!(
        err.starts_with(&format!("{BUDGET_EXCEEDED}:requests")),
        "{err}"
    );
    assert_eq!(api.requests(), 2);
}

#[test]
fn uql_profile_reports_remote_fragments_and_caller_hint_refuses_partial_results() {
    use eg_types::wire::UqlResult;

    let api = MockApi::spawn(catalog(250), 40);
    let _allow = MockHttpAllowGuard::new(&api.base);
    let fx = crate::fixture::build();
    let spec = api.spec("?page={page}&limit={limit}");
    let mut source = crate::federation::ForeignSourceRegistry::default();
    source.register_spec("catalog", spec);
    let session = FederationSession::new(FederationBudget::default());
    let ctx = PlanCtx::new(&fx.view, &fx.semantic)
        .with_foreign(&source)
        .with_federation(&session);

    let explain =
        crate::uql::parse_statement("EXPLAIN FOREIGN 'catalog' |> LIMIT 2", &Default::default())
            .unwrap();
    let UqlResult::Explain { federation, .. } =
        crate::uql::serve::run_statement(&explain, &ctx).unwrap()
    else {
        panic!("EXPLAIN expected")
    };
    assert_eq!(federation, ["remote registered source catalog"]);
    assert_eq!(api.requests(), 0, "EXPLAIN never fetches");

    let profile = crate::uql::parse_statement(
        "PROFILE FEDERATION BUDGET (REQUESTS 2) FOREIGN 'catalog'",
        &Default::default(),
    )
    .unwrap();
    let err = crate::uql::serve::run_statement(&profile, &ctx).unwrap_err();
    assert!(
        err.starts_with(&format!("{BUDGET_EXCEEDED}:requests")),
        "{err}"
    );
    assert_eq!(api.requests(), 2);
    assert_eq!(session.budget().max_requests, 2);

    let session = FederationSession::new(FederationBudget {
        max_requests: 5,
        ..FederationBudget::default()
    });
    let ctx = PlanCtx::new(&fx.view, &fx.semantic)
        .with_foreign(&source)
        .with_federation(&session);
    let profile = crate::uql::parse_statement(
        "PROFILE FEDERATION BUDGET (REQUESTS 20) FOREIGN 'catalog' |> LIMIT 2",
        &Default::default(),
    )
    .unwrap();
    let UqlResult::Profile {
        federation, rows, ..
    } = crate::uql::serve::run_statement(&profile, &ctx).unwrap()
    else {
        panic!("PROFILE expected")
    };
    assert_eq!(rows.len(), 2);
    assert_eq!(
        session.budget().max_requests,
        5,
        "a caller cannot raise the server ceiling"
    );
    assert_eq!(federation.len(), 1);
    assert!(federation[0].contains("requests="), "{}", federation[0]);
    assert!(federation[0].contains("fetched="), "{}", federation[0]);
    assert!(
        !federation[0].contains(&api.base),
        "trace must not expose the URL"
    );
}

#[test]
fn placeholders_outside_the_query_string_never_template_the_destination() {
    let fx = crate::fixture::build();
    let ctx = PlanCtx::new(&fx.view, &fx.semantic);
    let spec = ForeignSourceSpec::HttpJson {
        url: "http://{keys}.example.invalid/items".into(),
        json_path: String::new(),
        field_map: HttpFieldMap {
            id: "ref".into(),
            score: None,
        },
    };
    let err = execute(
        &Plan::new(vec![Op::ForeignScan {
            source: Box::new(spec),
            join: false,
        }]),
        &ctx,
    )
    .unwrap_err();
    assert!(err.contains("federation"), "{err}");
}

// ── (E)+(B): the bind-join engine against scripted sources ──────────────────────

/// A key-lookup source over `catalog` that fails any batch larger than `max_ok` keys (or
/// every key batch when `max_ok == 0`) and counts its round trips.
struct Scripted {
    catalog: Vec<String>,
    max_ok: usize,
    calls: AtomicUsize,
    identity: Identity,
}

impl Scripted {
    fn new(tag: &str, catalog: Vec<String>, max_ok: usize) -> Self {
        Self {
            catalog,
            max_ok,
            calls: AtomicUsize::new(0),
            identity: Identity {
                label: "scripted".into(),
                fingerprint: fingerprint(tag.as_bytes()),
                cache_name: None,
            },
        }
    }
}

impl RemoteFetch for Scripted {
    fn capabilities(&self) -> SourceCapabilities {
        SourceCapabilities::single_full_fetch(
            KeyLookup::Batched { max_keys: 1000 },
            LimitPushdown::Unsupported,
        )
    }
    fn identity(&self) -> &Identity {
        &self.identity
    }
    fn fetch(&self, request: &RemoteRequest) -> Result<RowSet, String> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if !request.keys.is_empty() && request.keys.len() > self.max_ok {
            return Err("scripted: batch too large".into());
        }
        let keep = |id: &String| request.keys.is_empty() || request.keys.contains(id);
        Ok(RowSet::from_ids(
            self.catalog.iter().filter(|id| keep(id)).cloned(),
        ))
    }
}

fn local(n: usize) -> RowSet {
    RowSet::from_ids((0..n).map(|i| format!("x{i}")))
}

#[test]
fn an_oversized_batch_is_split_until_the_source_accepts_it() {
    let source = Scripted::new("split-test", catalog(5000), 10);
    let session = FederationSession::from_env();
    let input = local(40);
    let read = Fragment::new(&source, &session).join(&input).unwrap();
    let naive = RowSet::from_ids(catalog(5000));
    let expected = input.intersect_keep_order(&naive.id_set());
    assert_eq!(
        input.intersect_keep_order(&read.rows.id_set()),
        expected,
        "(E) the split batches return the same join"
    );
    assert_eq!(read.trace.strategy, FetchStrategy::BindJoin);
    assert_eq!(
        read.trace.requests, 6,
        "(B) 40 and 20 refused, then four batches of 10"
    );
}

#[test]
fn failing_key_lookups_fall_back_to_the_naive_fetch() {
    let source = Scripted::new("fallback-test", catalog(50), 0);
    let session = FederationSession::from_env();
    let input = local(20);
    let read = Fragment::new(&source, &session).join(&input).unwrap();
    assert_eq!(read.trace.strategy, FetchStrategy::FallbackFullFetch);
    assert_eq!(
        read.rows,
        RowSet::from_ids(catalog(50)),
        "(E) the naive rows"
    );
    let learned =
        super::stats::lookup(&source.identity.fingerprint).expect("the failure is remembered");
    assert_eq!(learned.key_lookup_failures, 1);
    assert_eq!(learned.full_samples, 1);
}

struct Concurrent {
    identity: Identity,
    active: AtomicUsize,
    peak: AtomicUsize,
}

impl Concurrent {
    fn new() -> Self {
        Self {
            identity: Identity {
                label: "concurrent".into(),
                fingerprint: fingerprint(b"fo09-concurrency"),
                cache_name: None,
            },
            active: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
        }
    }
}

impl RemoteFetch for Concurrent {
    fn capabilities(&self) -> SourceCapabilities {
        let mut caps = SourceCapabilities::single_full_fetch(
            KeyLookup::Batched { max_keys: 32 },
            LimitPushdown::Unsupported,
        );
        caps.rate = SourceRate::new(2, 0);
        caps
    }

    fn identity(&self) -> &Identity {
        &self.identity
    }

    fn parallel_safe(&self) -> Option<&(dyn RemoteFetch + Sync)> {
        Some(self)
    }

    fn fetch(&self, request: &RemoteRequest) -> Result<RowSet, String> {
        let current = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak.fetch_max(current, Ordering::SeqCst);
        std::thread::sleep(Duration::from_millis(15));
        self.active.fetch_sub(1, Ordering::SeqCst);
        Ok(RowSet::from_ids(request.keys.clone()))
    }
}

#[test]
fn parallel_batches_share_a_source_cap_across_queries() {
    let source = Concurrent::new();
    let sessions = [FederationSession::from_env(), FederationSession::from_env()];
    let input = local(128);
    std::thread::scope(|scope| {
        let handles: Vec<_> = sessions
            .iter()
            .map(|session| {
                let source = &source;
                let input = &input;
                scope.spawn(move || Fragment::new(source, session).join(input).unwrap())
            })
            .collect();
        for handle in handles {
            let read = handle.join().unwrap();
            assert_eq!(read.rows, input, "parallel batches retain exact join rows");
            assert_eq!(read.trace.requests, 4);
        }
    });
    let peak = source.peak.load(Ordering::SeqCst);
    assert_eq!(
        peak, 2,
        "parallel batches use but never exceed the source cap"
    );
}

#[test]
fn a_small_learned_source_is_fetched_whole_next_time() {
    let source = Scripted::new("learned-small-test", catalog(20), 1000);
    super::stats::observe_full_fetch(&source.identity.fingerprint, 20);
    let session = FederationSession::from_env();
    let read = Fragment::new(&source, &session).join(&local(300)).unwrap();
    assert_eq!(
        read.trace.strategy,
        FetchStrategy::FullFetch,
        "20 rows beat 300 keys + a round trip"
    );
    assert!(matches!(
        read.trace.estimate,
        super::EstimateProvenance::Learned { samples: 1, .. }
    ));
}

// ── LIMIT-hint soundness ─────────────────────────────────────────────────────────

#[test]
fn a_limit_hint_is_dropped_when_occurrences_disagree() {
    let src = || Op::ForeignScan {
        source: Box::new(ForeignSourceSpec::Named { name: "s".into() }),
        join: false,
    };
    let session = FederationSession::from_env();
    session.prepare(&[src(), Op::Limit { k: 3 }]);
    assert_eq!(session.limit_hint(&src()), Some(3));
    session.prepare(&[src(), Op::Limit { k: 3 }, src(), Op::Limit { k: 100 }]);
    assert_eq!(session.limit_hint(&src()), None, "two different limits");
    session.prepare(&[
        src(),
        Op::Limit { k: 3 },
        Op::Scan {
            label: "Doc".into(),
        },
        src(),
    ]);
    assert_eq!(
        session.limit_hint(&src()),
        None,
        "an occurrence without a limit"
    );
}
