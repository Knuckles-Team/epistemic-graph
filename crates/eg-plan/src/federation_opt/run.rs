//! Executing foreign leaves through the optimizer: the `Op::ForeignScan` / `Op::Foreign`
//! arms of the executor land here.

use std::collections::HashSet;
use std::time::Instant;

use eg_types::wire::ForeignSourceSpec;

use super::budget::{BUDGET_EXCEEDED, REQUIRES_KEYS, RESULT_INCOMPLETE};
use super::capability::{
    FullFetch, LimitPushdown, PageRequest, Paging, RemoteRequest, SourceCapabilities,
};
use super::http::PAGE_SIZE;
use super::limiter;
use super::remote::RemoteFetch;
use super::session::FederationSession;
use super::strategy::{choose_join, BatchSizer, JoinInputs, JoinStrategy};
use super::trace::{FetchStrategy, FragmentTrace};
use super::{fuse_foreign, stats};
use crate::algebra::Op;
use crate::exec::PlanCtx;
use crate::rowset::RowSet;

/// `Op::ForeignScan { source, join }`: a source read (`join == false`, or an empty input) or
/// a foreign∩local join, optimized unless `EPISTEMIC_GRAPH_FEDERATION_OPT=0`.
pub(crate) fn foreign_scan(
    op: &Op,
    input: RowSet,
    source: &ForeignSourceSpec,
    join: bool,
    ctx: &PlanCtx,
) -> Result<RowSet, String> {
    let remote = super::enabled()
        .then(|| super::remote::resolve(source, ctx.foreign))
        .flatten();
    let Some(remote) = remote else {
        return Ok(fuse_foreign(
            input,
            super::foreign_source_rows(source, ctx.foreign)?,
            join,
        ));
    };
    with_session(ctx, |session| {
        let fragment = Fragment::new(remote.as_ref(), session);
        if join && !input.is_empty() {
            let fetched = fragment.join(&input)?;
            let kept = input.intersect_keep_order(&fetched.rows.id_set());
            return Ok(fetched.record(session, kept));
        }
        Ok(fragment.source(session.limit_hint(op))?.finish(session))
    })
}

/// `Op::Foreign { name }` (UQL `FOREIGN "<name>"`): the named source's rows replace the
/// input, with a following `Limit` pushed when the source allows it.
pub(crate) fn foreign_named(op: &Op, name: &str, ctx: &PlanCtx) -> Result<RowSet, String> {
    let registry = ctx
        .foreign
        .ok_or_else(|| "FOREIGN requires a bound foreign-source registry".to_string())?;
    let remote = super::enabled()
        .then(|| super::remote::resolve_named(name, registry))
        .flatten();
    let Some(remote) = remote else {
        return registry.resolve(name);
    };
    with_session(ctx, |session| {
        let fragment = Fragment::new(remote.as_ref(), session);
        Ok(fragment.source(session.limit_hint(op))?.finish(session))
    })
}

/// Run `f` under the ctx's session, or a fresh server-budget session when none is bound.
fn with_session<R>(ctx: &PlanCtx, f: impl FnOnce(&FederationSession) -> R) -> R {
    match ctx.federation {
        Some(session) => f(session),
        None => f(&FederationSession::from_env()),
    }
}

/// Is `error` one of the optimizer's typed refusals (never retried or fallen back from)?
fn is_refusal(error: &str) -> bool {
    [BUDGET_EXCEEDED, REQUIRES_KEYS, RESULT_INCOMPLETE]
        .iter()
        .any(|code| error.starts_with(code))
}

/// The rows a fragment read and its trace, before the residual decides what is kept.
pub(super) struct Read {
    pub(super) rows: RowSet,
    pub(super) trace: FragmentTrace,
    started: Instant,
}

impl Read {
    /// A source read keeps every row it read.
    fn finish(mut self, session: &FederationSession) -> RowSet {
        let rows = std::mem::take(&mut self.rows);
        self.record(session, rows)
    }

    /// Finish the trace with what the plan keeps, record it, and hand the kept rows on.
    fn record(mut self, session: &FederationSession, kept: RowSet) -> RowSet {
        self.trace.rows_kept = kept.len();
        self.trace.elapsed_ms =
            u64::try_from(self.started.elapsed().as_millis()).unwrap_or(u64::MAX);
        session.record(self.trace);
        kept
    }
}

/// One remote fragment of one query.
pub(super) struct Fragment<'r, 's> {
    remote: &'r dyn RemoteFetch,
    session: &'s FederationSession,
    caps: SourceCapabilities,
}

impl<'r, 's> Fragment<'r, 's> {
    pub(super) fn new(remote: &'r dyn RemoteFetch, session: &'s FederationSession) -> Self {
        let caps = remote.capabilities();
        Self {
            remote,
            session,
            caps,
        }
    }

    fn trace(&self, strategy: FetchStrategy) -> FragmentTrace {
        FragmentTrace::new(self.remote.label(), strategy)
    }

    /// A source read: every row, or the first `hint` rows when the source takes a limit.
    pub(super) fn source(&self, hint: Option<usize>) -> Result<Read, String> {
        if self.caps.full_fetch == FullFetch::RequiresKeys {
            return Err(format!(
                "{REQUIRES_KEYS}: this source answers key lookups only; join it with a local candidate set"
            ));
        }
        let started = Instant::now();
        let limit = hint.filter(|_| self.caps.limit == LimitPushdown::Native);
        let mut trace = self.trace(source_strategy(limit, self.caps.paging));
        trace.limit_pushed = limit;
        let request = RemoteRequest {
            limit,
            ..RemoteRequest::full()
        };
        let rows = match self.read_all(&request, &mut trace) {
            Ok(rows) if needs_refill(limit, self.caps.paging, &rows) => {
                trace.strategy = FetchStrategy::LimitRefill;
                self.read_full(&mut trace)?
            }
            Ok(rows) => self.observed(limit, rows),
            Err(e) if limit.is_none() || is_refusal(&e) => return Err(e),
            Err(_) => {
                trace.strategy = FetchStrategy::FallbackFullFetch;
                self.read_full(&mut trace)?
            }
        };
        Ok(Read {
            rows,
            trace,
            started,
        })
    }

    /// Record an unlimited read's size in the source's statistics.
    fn observed(&self, limit: Option<usize>, rows: RowSet) -> RowSet {
        if limit.is_none() {
            stats::observe_full_fetch(&self.remote.fingerprint(), rows.len());
        }
        rows
    }

    /// Every row of the source (the naive request), recorded in the statistics.
    fn read_full(&self, trace: &mut FragmentTrace) -> Result<RowSet, String> {
        let rows = self.read_all(&RemoteRequest::full(), trace)?;
        Ok(self.observed(None, rows))
    }

    /// A foreign∩local join over `input`: bind join or full fetch, as the cost model says.
    pub(super) fn join(&self, input: &RowSet) -> Result<Read, String> {
        let started = Instant::now();
        let keys = input.ids();
        let (strategy, estimate) = choose_join(&JoinInputs {
            caps: &self.caps,
            keys: keys.len(),
            keys_expressible: keys.iter().all(|k| self.remote.key_expressible(k)),
            max_bind_keys: self.session.budget().max_bind_keys,
            stats: stats::lookup(&self.remote.fingerprint()),
        });
        let (rows, mut trace) = match strategy {
            JoinStrategy::Refuse(error) => return Err(error),
            JoinStrategy::BindJoin => self.bind_or_fallback(keys)?,
            JoinStrategy::FullFetch => {
                let mut trace = self.trace(FetchStrategy::FullFetch);
                (self.read_full(&mut trace)?, trace)
            }
        };
        trace.estimate = estimate;
        Ok(Read {
            rows,
            trace,
            started,
        })
    }

    /// Bind join; when key lookups keep failing, fall back to a full fetch (or fail for a
    /// key-only source).
    fn bind_or_fallback(&self, keys: Vec<String>) -> Result<(RowSet, FragmentTrace), String> {
        let mut trace = self.trace(FetchStrategy::BindJoin);
        trace.keys_pushed = keys.len();
        if let Some(rows) = self.bind(&keys, &mut trace)? {
            return Ok((rows, trace));
        }
        stats::observe_key_failure(&self.remote.fingerprint());
        if self.caps.full_fetch == FullFetch::RequiresKeys {
            return Err("federation: key lookups against a key-only source failed".to_string());
        }
        trace.strategy = FetchStrategy::FallbackFullFetch;
        let rows = self.read_full(&mut trace)?;
        Ok((rows, trace))
    }

    /// Ship `keys` in AIMD-sized batches. `Ok(None)` when the batches kept failing.
    fn bind(&self, keys: &[String], trace: &mut FragmentTrace) -> Result<Option<RowSet>, String> {
        if self.caps.paging == Paging::Single && keys.len() > 64 {
            if let Some(remote) = self.remote.parallel_safe() {
                if self.caps.rate.max_concurrent > 1 {
                    match self.bind_parallel(remote, keys, trace)? {
                        Some(rows) => return Ok(Some(rows)),
                        None => return self.bind_sequential(keys, trace),
                    }
                }
            }
        }
        self.bind_sequential(keys, trace)
    }

    /// Fixed windows are submitted in parallel. If a source rejects any window,
    /// retry the whole join with AIMD so the adaptive split and fallback semantics
    /// remain identical; successful speculative rows are never returned partially.
    fn bind_parallel(
        &self,
        remote: &(dyn RemoteFetch + Sync),
        keys: &[String],
        trace: &mut FragmentTrace,
    ) -> Result<Option<RowSet>, String> {
        let batch_size = self.caps.max_keys().unwrap_or(1).min(32);
        let mut out = Vec::new();
        let mut next = 0;
        while next < keys.len() {
            let mut requests = Vec::new();
            for _ in 0..self.caps.rate.max_concurrent {
                if next >= keys.len() {
                    break;
                }
                let window = &keys[next..keys.len().min(next + batch_size)];
                let fit = remote.fit_keys(window).clamp(1, window.len());
                requests.push(RemoteRequest::keys(window[..fit].to_vec()));
                next += fit;
            }
            let results = fetch_round(remote, self.session, self.caps, &requests);
            account(trace, &results);
            for attempt in results {
                match attempt.result {
                    Ok(rows) => {
                        out.extend(rows.rows().iter().map(|r| (r.id.clone(), r.score)));
                    }
                    Err(e) if is_refusal(&e) => return Err(e),
                    Err(_) => return Ok(None),
                }
            }
        }
        Ok(Some(RowSet::from_rows(out)))
    }

    fn bind_sequential(
        &self,
        keys: &[String],
        trace: &mut FragmentTrace,
    ) -> Result<Option<RowSet>, String> {
        let mut sizer = BatchSizer::new(self.caps.max_keys().unwrap_or(1));
        let mut out: Vec<(String, Option<f32>)> = Vec::new();
        let mut next = 0;
        while next < keys.len() {
            let window = &keys[next..keys.len().min(next + sizer.size())];
            let batch = &window[..self.remote.fit_keys(window).clamp(1, window.len())];
            let sent = Instant::now();
            match self.read_all(&RemoteRequest::keys(batch.to_vec()), trace) {
                Ok(rows) => {
                    sizer.success(sent.elapsed());
                    out.extend(rows.rows().iter().map(|r| (r.id.clone(), r.score)));
                    next += batch.len();
                }
                Err(e) if is_refusal(&e) => return Err(e),
                Err(_) if sizer.failure(batch.len()) => {}
                Err(_) => return Ok(None),
            }
        }
        Ok(Some(RowSet::from_rows(out)))
    }

    /// Answer one request, page by page when the source pages.
    fn read_all(
        &self,
        request: &RemoteRequest,
        trace: &mut FragmentTrace,
    ) -> Result<RowSet, String> {
        match self.caps.paging {
            Paging::Single => self.round_trip(request, trace),
            Paging::Offset | Paging::Page => self.read_pages(request, trace),
        }
    }

    /// Pages until an empty page, or until `request.limit` distinct rows arrived. A source
    /// that never runs dry is stopped by the request budget (a typed refusal).
    fn read_pages(
        &self,
        request: &RemoteRequest,
        trace: &mut FragmentTrace,
    ) -> Result<RowSet, String> {
        let mut rows: Vec<(String, Option<f32>)> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();
        let mut page = PageRequest {
            index: 0,
            offset: 0,
            size: PAGE_SIZE,
        };
        loop {
            let paged = RemoteRequest {
                page: Some(page),
                ..request.clone()
            };
            let got = self.round_trip(&paged, trace)?;
            if got.is_empty() {
                break;
            }
            page.index += 1;
            page.offset += got.len();
            for row in got.rows() {
                seen.insert(row.id.clone());
                rows.push((row.id.clone(), row.score));
            }
            if request.limit.is_some_and(|k| seen.len() >= k) {
                break;
            }
        }
        Ok(RowSet::from_rows(rows))
    }

    /// One budgeted round trip.
    fn round_trip(
        &self,
        request: &RemoteRequest,
        trace: &mut FragmentTrace,
    ) -> Result<RowSet, String> {
        let attempt = fetch_one(self.remote, self.session, self.caps, request);
        trace.requests += attempt.requests;
        trace.rows_fetched += attempt.rows;
        attempt.result
    }
}

/// One charged and source-admitted network call. The permit is held only during fetch,
/// and the wait for it ends at the query's wall deadline.
struct Attempt {
    requests: u32,
    rows: usize,
    result: Result<RowSet, String>,
}

impl Attempt {
    fn refused(error: String) -> Self {
        Self {
            requests: 0,
            rows: 0,
            result: Err(error),
        }
    }
}

/// Send one round of requests concurrently; every attempt has finished when this returns.
fn fetch_round(
    remote: &(dyn RemoteFetch + Sync),
    session: &FederationSession,
    caps: SourceCapabilities,
    requests: &[RemoteRequest],
) -> Vec<Attempt> {
    std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(requests.len());
        for request in requests {
            handles.push(scope.spawn(move || fetch_one(remote, session, caps, request)));
        }
        handles
            .into_iter()
            .map(|handle| handle.join().expect("foreign request panicked"))
            .collect()
    })
}

/// Add every attempt of one parallel round to the trace. The whole round ran before any
/// result is read, so a failed batch does not hide the requests and rows of the others.
fn account(trace: &mut FragmentTrace, attempts: &[Attempt]) {
    trace.requests += attempts.iter().map(|a| a.requests).sum::<u32>();
    trace.rows_fetched += attempts.iter().map(|a| a.rows).sum::<usize>();
}

fn fetch_one(
    remote: &dyn RemoteFetch,
    session: &FederationSession,
    caps: SourceCapabilities,
    request: &RemoteRequest,
) -> Attempt {
    let cache_scope = session.cache_scope();
    let cache_name = remote.identity().cache_name.as_deref();
    if let (Some(scope), Some(name)) = (cache_scope.as_deref(), cache_name) {
        if let Some(rows) = scope.get(name, &remote.fingerprint(), request) {
            if let Err(error) = session.with_meter(|meter| {
                meter.check_wall()?;
                meter.charge_rows(rows.len())
            }) {
                return Attempt::refused(error);
            }
            return Attempt {
                requests: 0,
                rows: 0,
                result: Ok(rows),
            };
        }
    }
    if let Err(e) = session.with_meter(|m| m.charge_request()) {
        return Attempt::refused(e);
    }
    let deadline = session.with_meter(|m| m.deadline());
    let Some(_permit) = limiter::acquire(remote.fingerprint(), caps.rate, deadline) else {
        return Attempt::refused(session.with_meter(|m| m.wall_refusal()));
    };
    let sent = Instant::now();
    let result = remote.fetch(request);
    let ms = u64::try_from(sent.elapsed().as_millis()).unwrap_or(u64::MAX);
    stats::observe_request(&remote.fingerprint(), ms);
    let rows = result.as_ref().map_or(0, RowSet::len);
    let result = result.and_then(|rows| {
        session.with_meter(|m| m.charge_rows(rows.len()))?;
        if let (Some(scope), Some(name)) = (cache_scope.as_deref(), cache_name) {
            scope.insert(name, &remote.fingerprint(), request, &rows);
        }
        Ok(rows)
    });
    Attempt {
        requests: 1,
        rows,
        result,
    }
}

/// The trace label of a source read.
fn source_strategy(limit: Option<usize>, paging: Paging) -> FetchStrategy {
    match (limit, paging) {
        (_, Paging::Offset | Paging::Page) => FetchStrategy::Paged,
        (Some(_), Paging::Single) => FetchStrategy::LimitPushdown,
        (None, Paging::Single) => FetchStrategy::FullFetch,
    }
}

/// A single-request limited read that de-duplicated below `k` rows may have cut rows the
/// naive read keeps; it is refilled from a full read.
fn needs_refill(limit: Option<usize>, paging: Paging, rows: &RowSet) -> bool {
    paging == Paging::Single && limit.is_some_and(|k| rows.len() < k)
}

#[cfg(test)]
mod cache_tests {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use super::*;
    use crate::federation_opt::cache::{now_ms, FragmentCacheScope, SourceWatermark};
    use crate::federation_opt::remote::Identity;

    struct Counted {
        identity: Identity,
        calls: AtomicUsize,
    }

    impl RemoteFetch for Counted {
        fn capabilities(&self) -> SourceCapabilities {
            SourceCapabilities::fetch_only()
        }

        fn identity(&self) -> &Identity {
            &self.identity
        }

        fn fetch(&self, _request: &RemoteRequest) -> Result<RowSet, String> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Ok(RowSet::from_rows(vec![("row".into(), None)]))
        }
    }

    // spec: EG-FEDERATED-QUERY-R049
    #[test]
    fn served_fragment_reuses_only_fresh_named_owner_scope() {
        let remote = Counted {
            identity: Identity {
                label: "counted:crm".into(),
                fingerprint: [19; 32],
                cache_name: Some("crm".into()),
            },
            calls: AtomicUsize::new(0),
        };
        let scope = |owner: &str, watermark: &str, expiry| {
            Arc::new(FragmentCacheScope::new(
                owner.into(),
                HashMap::from([(
                    "crm".into(),
                    SourceWatermark {
                        watermark: watermark.into(),
                        valid_through_ms: expiry,
                    },
                )]),
            ))
        };
        let session = FederationSession::new(crate::federation_opt::FederationBudget::default());
        session.set_cache_scope(Some(scope("owner-one", "lsn-1", now_ms() + 30_000)));
        let request = RemoteRequest::keys(vec!["needle".into()]);
        let caps = remote.capabilities();
        let first = fetch_one(&remote, &session, caps, &request);
        assert_eq!(first.requests, 1);
        assert!(first.result.is_ok());
        let hit = fetch_one(&remote, &session, caps, &request);
        assert_eq!(hit.requests, 0);
        assert_eq!(remote.calls.load(Ordering::SeqCst), 1);

        session.set_cache_scope(Some(scope("owner-two", "lsn-1", now_ms() + 30_000)));
        assert_eq!(fetch_one(&remote, &session, caps, &request).requests, 1);
        session.set_cache_scope(Some(scope("owner-one", "lsn-2", now_ms() + 30_000)));
        assert_eq!(fetch_one(&remote, &session, caps, &request).requests, 1);
        session.set_cache_scope(Some(scope("owner-one", "lsn-1", now_ms() - 1)));
        assert_eq!(fetch_one(&remote, &session, caps, &request).requests, 1);
        session.set_cache_scope(None);
        assert_eq!(fetch_one(&remote, &session, caps, &request).requests, 1);
        assert_eq!(remote.calls.load(Ordering::SeqCst), 5);
    }
}
