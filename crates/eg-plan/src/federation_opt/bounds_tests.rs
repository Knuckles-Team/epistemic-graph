//! Bounds of the optimizer against scripted sources: a request waiting for a source's
//! permit gives up at its query's wall deadline.

use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use super::capability::{RemoteRequest, SourceCapabilities, SourceRate};
use super::remote::{Identity, RemoteFetch};
use super::run::Fragment;
use super::stats::fingerprint;
use super::{FederationBudget, FederationSession, BUDGET_EXCEEDED};
use crate::rowset::RowSet;

fn identity(tag: &str) -> Identity {
    Identity {
        label: tag.to_string(),
        fingerprint: fingerprint(tag.as_bytes()),
        cache_name: None,
    }
}

// ── the admission wait ends at the query's deadline ─────────────────────────────────

/// A source with ONE permit whose fetch announces itself and then blocks until released
/// (or, failing that, for `HOLD`).
struct Held {
    identity: Identity,
    entered: SyncSender<()>,
    release: Mutex<Receiver<()>>,
}

/// How long the holder keeps the permit when nothing releases it.
const HOLD: Duration = Duration::from_secs(8);

impl RemoteFetch for Held {
    fn capabilities(&self) -> SourceCapabilities {
        let mut caps = SourceCapabilities::fetch_only();
        caps.rate = SourceRate::new(1, 0);
        caps
    }

    fn identity(&self) -> &Identity {
        &self.identity
    }

    fn fetch(&self, _request: &RemoteRequest) -> Result<RowSet, String> {
        let _ = self.entered.try_send(());
        let _ = self
            .release
            .lock()
            .expect("release channel lock")
            .recv_timeout(HOLD);
        Ok(RowSet::from_ids(["held".to_string()]))
    }
}

#[test]
fn a_short_budget_query_does_not_wait_out_a_held_permit() {
    let (entered, has_entered) = sync_channel(2);
    let (release, released) = sync_channel(2);
    let source = Held {
        identity: identity("bounds-held-permit"),
        entered,
        release: Mutex::new(released),
    };
    let holder = FederationSession::from_env();
    let short = FederationSession::new(FederationBudget {
        max_wall_ms: 100,
        ..FederationBudget::default()
    });
    std::thread::scope(|scope| {
        let holding = scope.spawn(|| Fragment::new(&source, &holder).source(None).map(|_| ()));
        has_entered
            .recv_timeout(Duration::from_secs(10))
            .expect("the holder's fetch started and holds the only permit");

        let started = Instant::now();
        let refused = Fragment::new(&source, &short).source(None).map(|_| ());
        let waited = started.elapsed();
        release.send(()).expect("release the holder");

        let error = refused.expect_err("the permit was never free within the budget");
        assert!(
            error.starts_with(&format!("{BUDGET_EXCEEDED}:wall_ms")),
            "{error}"
        );
        assert!(
            waited < HOLD / 2,
            "the wait ended at the 100 ms deadline, not when the holder let go: {waited:?}"
        );
        assert!(holding.join().expect("holder thread").is_ok());
    });
}
