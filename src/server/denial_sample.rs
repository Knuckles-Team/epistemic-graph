//! The durable denial sample (IDM-03).
//!
//! Every authorization denial already counts in metrics. The denial paths
//! also OFFER a sample here -- an in-memory, bounded, rate-limited sampler,
//! cheap on the hot path -- and the engine periodically writes what it kept
//! into the identity store's hash-chained audit trail in ONE durable write.
#![cfg(feature = "security")]

use std::sync::{Arc, Mutex, OnceLock};

use eg_types::identity::{DenialSample, DenialSampler};
use tokio::sync::RwLock;

use crate::server::ServerState;

/// How often kept samples are written.
const FLUSH_EVERY: std::time::Duration = std::time::Duration::from_secs(60);

fn sampler() -> &'static Mutex<DenialSampler> {
    static SAMPLER: OnceLock<Mutex<DenialSampler>> = OnceLock::new();
    SAMPLER.get_or_init(|| Mutex::new(DenialSampler::default()))
}

/// Offer one denial. Never blocks on durable state and never fails the
/// denial it describes.
pub(crate) fn offer(principal: &str, action: &str, reason: &str) {
    let sample = DenialSample {
        at_ms: crate::isolation::access_clock_ms(),
        principal: principal.to_string(),
        action: action.to_string(),
        reason: reason.to_string(),
    };
    if let Ok(mut sampler) = sampler().lock() {
        sampler.offer(sample);
    }
}

/// Write every kept sample into the identity audit trail.
pub(crate) async fn flush(state: &Arc<RwLock<ServerState>>) {
    let (samples, dropped) = match sampler().lock() {
        Ok(mut sampler) => sampler.drain(),
        Err(_) => return,
    };
    if samples.is_empty() && dropped == 0 {
        return;
    }
    let mut guard = super::dispatch::timed_write(state).await;
    if let Err(error) = guard.isolation.try_record_denials(samples, dropped) {
        tracing::warn!(%error, "denial sample could not be written; dropped");
    }
}

/// The periodic writer, one per engine.
pub fn spawn_flusher(state: Arc<RwLock<ServerState>>) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(FLUSH_EVERY);
        loop {
            ticker.tick().await;
            flush(&state).await;
        }
    });
}
