//! Bounded, no-row source reachability probes. No request body or credential is sent.

use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::time::{Duration, Instant};

use eg_types::wire::ForeignSourceSpec;

use super::{limiter, remote};

const PROBE_BUDGET: Duration = Duration::from_secs(3);

/// Diagnostic strings are fixed; no endpoint, DSN or secret enters the result.
pub struct ProbeOutcome {
    pub reachable: bool,
    pub code: &'static str,
}

/// Reuse FO-09's process-wide source admission and FO-16's DNS-pinned destination
/// gate. The probe only connects a socket; it never requests or exposes rows.
pub fn probe_source(spec: &ForeignSourceSpec) -> ProbeOutcome {
    let Some(source) = remote::resolve(spec, None) else {
        return outcome(false, "PROBE_UNSUPPORTED");
    };
    let rate = source.capabilities().rate;
    let Some(_permit) = limiter::try_acquire(source.fingerprint(), rate, PROBE_BUDGET) else {
        return outcome(false, "SOURCE_BUSY");
    };
    match spec {
        ForeignSourceSpec::HttpJson { url, .. } => probe_http(url),
        ForeignSourceSpec::RemoteEngine { endpoint, .. } => probe_remote(endpoint),
        // SQL drivers currently re-resolve after validation and cannot pin the
        // selected address. Refuse a probe rather than open that SSRF window.
        ForeignSourceSpec::Sql { .. } | ForeignSourceSpec::Named { .. } => {
            outcome(false, "PROBE_UNSUPPORTED")
        }
    }
}

fn probe_http(url: &str) -> ProbeOutcome {
    let allow =
        std::env::var(crate::federation_ssrf::HTTP_JSON_FEDERATION_ALLOW_ENV).unwrap_or_default();
    if allow.len() > 64 * 1024 {
        return outcome(false, "TARGET_REFUSED");
    }
    let allow: Vec<String> = allow.split(',').take(1024).map(str::to_string).collect();
    let target = crate::federation_ssrf::validate_outbound_http_target(
        url,
        &allow,
        crate::federation_ssrf::OutboundAllowPolicy::PublicHttps,
    );
    let Ok(target) = target else {
        return outcome(false, "TARGET_REFUSED");
    };
    connect(&target.addresses)
}

fn probe_remote(endpoint: &str) -> ProbeOutcome {
    if endpoint.is_empty() || endpoint.len() > 1_024 {
        return outcome(false, "TARGET_REFUSED");
    }
    let Ok(addresses) = endpoint.to_socket_addrs() else {
        return outcome(false, "TARGET_REFUSED");
    };
    let addresses: Vec<_> = addresses.take(8).collect();
    if addresses.is_empty() || addresses.iter().any(|address| !address.ip().is_loopback()) {
        return outcome(false, "TARGET_REFUSED");
    }
    connect(&addresses)
}

fn connect(addresses: &[SocketAddr]) -> ProbeOutcome {
    let deadline = Instant::now() + PROBE_BUDGET;
    for address in addresses {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            break;
        }
        if TcpStream::connect_timeout(address, remaining).is_ok() {
            return outcome(true, "TCP_CONNECTED");
        }
    }
    outcome(false, "CONNECT_FAILED")
}

fn outcome(reachable: bool, code: &'static str) -> ProbeOutcome {
    ProbeOutcome { reachable, code }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn remote_probe_uses_loopback_only_and_returns_fixed_diagnostics() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let endpoint = listener.local_addr().unwrap().to_string();
        let reachable = probe_remote(&endpoint);
        assert!(reachable.reachable);
        assert_eq!(reachable.code, "TCP_CONNECTED");
        let refused = probe_remote("192.0.2.1:1234");
        assert!(!refused.reachable);
        assert_eq!(refused.code, "TARGET_REFUSED");
    }

    #[test]
    fn invalid_http_probe_never_discloses_target() {
        let outcome = probe_http("http://user:secret@127.0.0.1/private");
        assert!(!outcome.reachable);
        assert_eq!(outcome.code, "TARGET_REFUSED");
    }
}
