//! The ONE outbound-destination gate for federation (CONCEPT:EG-KG.query.query-federation,
//! EH-563 FO-05). Every destination a foreign source dials — an HTTP/JSON URL or an external
//! SQL DSN host — is resolved, vetted against the SSRF-sensitive ranges, and admitted only
//! when public (HTTPS for HTTP) or explicitly allow-listed by exact host / `host:port` /
//! origin in [`HTTP_JSON_FEDERATION_ALLOW_ENV`]. Errors never reflect the destination or its
//! credentials. The SQL gate resolves and vets but cannot pin the address inside the SQL
//! driver (residual resolve-then-connect window, documented in the federation design).

/// Existing federation SSRF opt-in, shared with the server's peer fan-out. Values are
/// comma-separated exact bare hosts, `host:port` authorities, or origins. Wildcards and
/// suffix matching are deliberately unsupported. A bare host is an explicit opt-in for
/// that exact host on any port, preserving the established server-side configuration.
pub const HTTP_JSON_FEDERATION_ALLOW_ENV: &str = "EPISTEMIC_GRAPH_FEDERATION_ALLOW";

const MAX_HTTP_JSON_URL_BYTES: usize = 2 * 1024;
const MAX_HTTP_JSON_ALLOWLIST_BYTES: usize = 64 * 1024;
const MAX_HTTP_JSON_ADDRESSES: usize = 8;

/// The validated, DNS-pinned transport target. No URL/host is retained here, keeping it
/// out of `Debug`/error paths and reducing the chance that embedded query credentials are
/// reflected by a future caller.
pub struct ValidatedHttpJsonTarget {
    /// Every DNS answer passed the destination gate. Pin this list in the HTTP resolver.
    pub addresses: Vec<std::net::SocketAddr>,
    /// `false` is only possible for an exact-allowlisted internal HTTP target.
    pub https_only: bool,
}

/// Whether the caller requires an explicit destination grant, even for public HTTPS.
#[derive(Debug, Clone, Copy)]
pub enum OutboundAllowPolicy {
    /// Public HTTPS is admitted; sensitive addresses need an exact allowlist entry.
    PublicHttps,
    /// Every destination needs an exact allowlist entry (SPARQL SERVICE).
    ExplicitOnly,
}

/// Parse and validate a federation URL without reflecting it in any error. Public
/// destinations are HTTPS-only. An internal/special-use resolution is accepted only
/// when the exact host, authority, or origin is present in the established federation
/// allowlist. Explicitly allowlisted internal endpoints may use HTTP for a local trusted
/// tunnel/test service; HTTP to a public destination remains forbidden.
pub(crate) fn validate_http_json_target(url: &str) -> Result<ValidatedHttpJsonTarget, String> {
    let allow = std::env::var(HTTP_JSON_FEDERATION_ALLOW_ENV).unwrap_or_default();
    if allow.len() > MAX_HTTP_JSON_ALLOWLIST_BYTES {
        return Err("federation: destination allowlist is too large".to_string());
    }
    let allow: Vec<_> = allow.split(',').take(1024).map(str::to_string).collect();
    validate_outbound_http_target(url, &allow, OutboundAllowPolicy::PublicHttps)
}

/// The shared destination gate for HTTP/JSON, SPARQL SERVICE, and peer fan-out.
/// Callers must disable implicit proxies and redirects and pin `addresses` in their
/// HTTP client's resolver; validating a URL and then resolving it again is unsafe.
pub fn validate_outbound_http_target(
    url: &str,
    allow: &[String],
    policy: OutboundAllowPolicy,
) -> Result<ValidatedHttpJsonTarget, String> {
    validate_http_json_url_shape(url)?;

    let (is_https, rest) = split_http_scheme(url)?;
    let authority = rest.split(['/', '?']).next().unwrap_or_default();
    if authority.is_empty() || authority.contains('@') || authority.contains('%') {
        return Err("federation: invalid HTTP JSON URL authority".to_string());
    }

    let default_port = if is_https { 443 } else { 80 };
    let (host, port) = parse_http_authority(authority, default_port)?;
    let host = normalize_http_host(host)?;
    let allowlisted = target_allowlisted(&host, port, is_https, allow);
    if matches!(policy, OutboundAllowPolicy::ExplicitOnly) && !allowlisted {
        return Err("federation: destination is not allowed".to_string());
    }

    let addresses = resolve_http_json_addresses(&host, port)?;
    check_http_json_ssrf(&addresses, allowlisted, is_https)?;

    Ok(ValidatedHttpJsonTarget {
        addresses,
        https_only: is_https,
    })
}

/// Reject a URL with disallowed bytes/shape before any scheme/authority
/// parsing runs.
fn validate_http_json_url_shape(url: &str) -> Result<(), String> {
    if url.is_empty()
        || url.len() > MAX_HTTP_JSON_URL_BYTES
        || url.trim() != url
        || url
            .bytes()
            .any(|byte| byte.is_ascii_control() || byte == b'\\')
        || url.contains('#')
    {
        return Err("federation: invalid HTTP JSON URL".to_string());
    }
    Ok(())
}

/// Split a validated URL into `(is_https, rest-after-scheme)`; any scheme
/// other than `http://`/`https://` is rejected.
fn split_http_scheme(url: &str) -> Result<(bool, &str), String> {
    if let Some(rest) = url.strip_prefix("https://") {
        Ok((true, rest))
    } else if let Some(rest) = url.strip_prefix("http://") {
        Ok((false, rest))
    } else {
        Err("federation: HTTP JSON source requires HTTPS".to_string())
    }
}

/// Resolve `host:port`, bounding and deduplicating the address set.
fn resolve_http_json_addresses(host: &str, port: u16) -> Result<Vec<std::net::SocketAddr>, String> {
    use std::net::ToSocketAddrs;
    let mut addresses: Vec<_> = (host, port)
        .to_socket_addrs()
        .map_err(|_| "federation: unable to resolve HTTP JSON source".to_string())?
        .take(MAX_HTTP_JSON_ADDRESSES + 1)
        .collect();
    addresses.sort_unstable();
    addresses.dedup();
    if addresses.is_empty() || addresses.len() > MAX_HTTP_JSON_ADDRESSES {
        return Err("federation: invalid HTTP JSON source resolution".to_string());
    }
    Ok(addresses)
}

/// Enforce the SSRF policy: a sensitive resolved address requires the
/// destination be explicitly allowlisted, and a non-HTTPS source is only
/// ever permitted for an allowlisted sensitive destination.
fn check_http_json_ssrf(
    addresses: &[std::net::SocketAddr],
    allowlisted: bool,
    is_https: bool,
) -> Result<(), String> {
    let has_sensitive_address = addresses
        .iter()
        .any(|address| is_ssrf_sensitive_ip(address.ip()));
    if has_sensitive_address && !allowlisted {
        return Err("federation: HTTP JSON destination is not allowed".to_string());
    }
    if !(is_https || has_sensitive_address && allowlisted) {
        return Err("federation: HTTP JSON source requires HTTPS".to_string());
    }
    Ok(())
}

fn parse_http_authority(authority: &str, default_port: u16) -> Result<(&str, u16), String> {
    if let Some(bracketed) = authority.strip_prefix('[') {
        let close = bracketed
            .find(']')
            .ok_or_else(|| "federation: invalid HTTP JSON URL authority".to_string())?;
        let host = &bracketed[..close];
        let suffix = &bracketed[close + 1..];
        let port = if suffix.is_empty() {
            default_port
        } else {
            let raw_port = suffix
                .strip_prefix(':')
                .ok_or_else(|| "federation: invalid HTTP JSON URL authority".to_string())?;
            parse_http_port(raw_port)?
        };
        if host.parse::<std::net::Ipv6Addr>().is_err() {
            return Err("federation: invalid HTTP JSON URL authority".to_string());
        }
        return Ok((host, port));
    }

    if authority.matches(':').count() > 1 {
        return Err("federation: invalid HTTP JSON URL authority".to_string());
    }
    match authority.rsplit_once(':') {
        Some((host, raw_port)) => Ok((host, parse_http_port(raw_port)?)),
        None => Ok((authority, default_port)),
    }
}

fn parse_http_port(raw: &str) -> Result<u16, String> {
    if raw.is_empty() || !raw.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err("federation: invalid HTTP JSON URL port".to_string());
    }
    raw.parse::<u16>()
        .ok()
        .filter(|port| *port != 0)
        .ok_or_else(|| "federation: invalid HTTP JSON URL port".to_string())
}

fn normalize_http_host(host: &str) -> Result<String, String> {
    let host = host.trim_end_matches('.').to_ascii_lowercase();
    if host.is_empty() || host.len() > 253 || !host.is_ascii() {
        return Err("federation: invalid HTTP JSON URL host".to_string());
    }
    if host.parse::<std::net::IpAddr>().is_ok() {
        return Ok(host);
    }
    let valid = host.split('.').all(|label| {
        !label.is_empty()
            && label.len() <= 63
            && !label.starts_with('-')
            && !label.ends_with('-')
            && label
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    });
    if !valid {
        return Err("federation: invalid HTTP JSON URL host".to_string());
    }
    Ok(host)
}

/// Exact allowlist matching only: no substrings, suffixes, globs, regexes, or CIDRs.
fn http_json_target_allowlisted(host: &str, port: u16, is_https: bool) -> bool {
    let Ok(raw) = std::env::var(HTTP_JSON_FEDERATION_ALLOW_ENV) else {
        return false;
    };
    if raw.len() > MAX_HTTP_JSON_ALLOWLIST_BYTES {
        return false;
    }
    let allow: Vec<_> = raw.split(',').take(1024).map(str::to_string).collect();
    target_allowlisted(host, port, is_https, &allow)
}

fn target_allowlisted(host: &str, port: u16, is_https: bool, allow: &[String]) -> bool {
    let scheme = if is_https { "https" } else { "http" };
    let authority_host = if host.contains(':') {
        format!("[{host}]")
    } else {
        host.to_string()
    };
    let authority = format!("{authority_host}:{port}");
    let origin = format!("{scheme}://{authority}");
    let scheme_host = format!("{scheme}://{authority_host}");
    allow.iter().take(1024).any(|entry| {
        let entry = entry.trim().to_ascii_lowercase();
        entry == host
            || entry == authority_host
            || entry == authority
            || entry == scheme_host
            || entry == origin
    })
}

/// Reject every local/private/link-local/multicast/documentation/transition range that
/// can address a non-public service. Public HTTPS is allowed; an operator must explicitly
/// opt an exact host/origin into this set's exceptions.
fn is_ssrf_sensitive_ip(ip: std::net::IpAddr) -> bool {
    match ip {
        std::net::IpAddr::V4(address) => is_ssrf_sensitive_ipv4(address),
        std::net::IpAddr::V6(address) => is_ssrf_sensitive_ipv6(address),
    }
}

fn is_ssrf_sensitive_ipv4(address: std::net::Ipv4Addr) -> bool {
    is_ssrf_std_range_v4(address) || is_ssrf_custom_range_v4(address)
}

/// The IPv4 ranges the standard library already classifies as non-public.
fn is_ssrf_std_range_v4(address: std::net::Ipv4Addr) -> bool {
    address.is_loopback()
        || address.is_unspecified()
        || address.is_private()
        || address.is_link_local()
        || address.is_multicast()
        || address.is_broadcast()
        || address.is_documentation()
}

/// IPv4 ranges the standard library does NOT classify as non-public but that
/// still can address a non-public/transition service.
fn is_ssrf_custom_range_v4(address: std::net::Ipv4Addr) -> bool {
    let [a, b, c, _] = address.octets();
    is_ssrf_custom_range_v4_reserved(a, b) || is_ssrf_custom_range_v4_special(a, b, c)
}

/// The all-zero, reserved-future-use, and carrier-grade-NAT ranges.
fn is_ssrf_custom_range_v4_reserved(a: u8, b: u8) -> bool {
    a == 0 || a >= 240 || (a == 100 && (64..=127).contains(&b)) // carrier-grade NAT
}

/// The IETF-protocol-assignment, deprecated-6to4-relay, and benchmark ranges.
fn is_ssrf_custom_range_v4_special(a: u8, b: u8, c: u8) -> bool {
    (a == 192 && b == 0 && c == 0) // IETF protocol assignments
        || (a == 192 && b == 88 && c == 99) // deprecated 6to4 relay anycast
        || (a == 198 && (b == 18 || b == 19)) // benchmark networks
}

fn is_ssrf_sensitive_ipv6(address: std::net::Ipv6Addr) -> bool {
    if let Some(mapped) = address.to_ipv4() {
        return is_ssrf_sensitive_ipv4(mapped);
    }
    is_ssrf_std_range_v6(address) || is_ssrf_custom_range_v6(address)
}

/// The IPv6 ranges the standard library already classifies as non-public.
fn is_ssrf_std_range_v6(address: std::net::Ipv6Addr) -> bool {
    address.is_loopback() || address.is_unspecified() || address.is_multicast()
}

/// IPv6 ranges the standard library does NOT classify as non-public but that
/// still can address a non-public/transition/documentation service.
fn is_ssrf_custom_range_v6(address: std::net::Ipv6Addr) -> bool {
    let segments = address.segments();
    is_ssrf_custom_range_v6_local(segments) || is_ssrf_custom_range_v6_transition(segments)
}

/// unique-local / link-local / deprecated site-local / discard-only ranges.
fn is_ssrf_custom_range_v6_local(segments: [u16; 8]) -> bool {
    (segments[0] & 0xfe00) == 0xfc00 // unique-local fc00::/7
        || (segments[0] & 0xffc0) == 0xfe80 // link-local fe80::/10
        || (segments[0] & 0xffc0) == 0xfec0 // deprecated site-local fec0::/10
        || (segments[0] == 0x0100 && segments[1..4] == [0, 0, 0]) // discard-only
}

/// NAT64 / 6to4 / Teredo / documentation / ORCHID transition ranges.
fn is_ssrf_custom_range_v6_transition(segments: [u16; 8]) -> bool {
    (segments[0] == 0x0064 && segments[1] == 0xff9b) // NAT64 translation
        || segments[0] == 0x2002 // 6to4 embeds an IPv4 target
        || (segments[0] == 0x2001 && segments[1] == 0x0000) // Teredo
        || (segments[0] == 0x2001 && segments[1] == 0x0db8) // documentation
        || (segments[0] == 0x2001 && (segments[1] & 0xfff0) == 0x0020) // ORCHID
}

/// The default port of a SQL DSN scheme.
#[cfg(feature = "federation-sql")]
fn sql_default_port(dsn: &str) -> Option<u16> {
    match dsn.split(':').next().unwrap_or("") {
        "postgres" | "postgresql" => Some(5432),
        "mysql" | "mariadb" => Some(3306),
        _ => None,
    }
}

/// FO-05 — admit a Postgres/MySQL DSN only when every host it names passes the same gate as
/// an HTTP destination: a public address, or an exact allow-list entry for an internal one.
/// A DSN with no host (a local socket default) or a `host=`/`hostaddr=` override is refused.
#[cfg(feature = "federation-sql")]
pub(crate) fn check_sql_dsn(dsn: &str) -> Result<(), String> {
    const REFUSED: &str = "federation: SQL destination is not allowed";
    let port = sql_default_port(dsn).ok_or_else(|| REFUSED.to_string())?;
    let rest = dsn
        .split_once("://")
        .map(|(_, r)| r)
        .ok_or_else(|| REFUSED.to_string())?;
    let (authority, tail) = rest.split_at(rest.find(['/', '?']).unwrap_or(rest.len()));
    let hosts = authority.rsplit_once('@').map_or(authority, |(_, h)| h);
    let overrides = tail.contains("host=") || tail.contains("hostaddr=");
    if hosts.is_empty() || overrides {
        return Err(REFUSED.to_string());
    }
    hosts
        .split(',')
        .try_for_each(|host| check_sql_host(host, port))
        .map_err(|_| REFUSED.to_string())
}

/// One `host[:port]` of a SQL DSN through the shared resolve + sensitive-range + allow-list gate.
#[cfg(feature = "federation-sql")]
fn check_sql_host(host_port: &str, default_port: u16) -> Result<(), String> {
    let (host, port) = parse_http_authority(host_port, default_port)?;
    let host = normalize_http_host(host)?;
    let allowlisted = http_json_target_allowlisted(&host, port, true)
        || http_json_target_allowlisted(&host, port, false);
    let addresses = resolve_http_json_addresses(&host, port)?;
    let sensitive = addresses.iter().any(|a| is_ssrf_sensitive_ip(a.ip()));
    if sensitive && !allowlisted {
        return Err("federation: SQL destination is not allowed".to_string());
    }
    Ok(())
}

#[cfg(test)]
mod http_json_security_tests {
    #[cfg(feature = "federation-sql")]
    use super::check_sql_dsn;
    use super::{
        is_ssrf_sensitive_ip, parse_http_authority, validate_http_json_target,
        validate_outbound_http_target, OutboundAllowPolicy,
    };
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

    #[test]
    fn internal_and_transition_addresses_are_sensitive() {
        for address in [
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)),
            IpAddr::V4(Ipv4Addr::new(100, 64, 0, 1)),
            IpAddr::V4(Ipv4Addr::new(100, 127, 255, 254)),
            IpAddr::V6(Ipv6Addr::LOCALHOST),
            "2002:c000:0201::1".parse().unwrap(),
            "2001:0:c000:201::1".parse().unwrap(),
            "2001:db8::1".parse().unwrap(),
            "64:ff9b::1".parse().unwrap(),
        ] {
            assert!(is_ssrf_sensitive_ip(address));
        }
        assert!(!is_ssrf_sensitive_ip(IpAddr::V4(Ipv4Addr::new(
            93, 184, 216, 34
        ))));
    }

    #[test]
    fn authority_parser_rejects_ambiguous_or_invalid_ports() {
        assert!(parse_http_authority("::1:443", 443).is_err());
        assert!(parse_http_authority("example.invalid:0", 443).is_err());
        assert!(parse_http_authority("[::1]:443", 80).is_ok());
    }

    #[test]
    fn destination_errors_do_not_reflect_url_secrets() {
        let secret = "test-sensitive-token-value"; // sanitizer:ignore
        let error = validate_http_json_target(&format!("ftp://example.invalid/?token={secret}"))
            .err()
            .expect("unsupported scheme must fail");
        assert!(!error.contains(secret));
        assert!(!error.contains("example.invalid"));
    }

    #[test]
    fn explicit_service_grant_is_exact_and_pins_every_resolution() {
        let allow = vec!["127.0.0.1:7900".to_string()];
        let target = validate_outbound_http_target(
            "http://127.0.0.1:7900/sparql",
            &allow,
            OutboundAllowPolicy::ExplicitOnly,
        )
        .expect("explicit internal endpoint");
        assert!(!target.https_only);
        assert_eq!(target.addresses.len(), 1);
        for endpoint in [
            "http://127.0.0.1:7901/sparql",
            "http://100.64.0.1:7900/sparql",
            "http://[2002::1]:7900/sparql",
        ] {
            assert!(
                validate_outbound_http_target(endpoint, &allow, OutboundAllowPolicy::ExplicitOnly)
                    .is_err(),
                "{endpoint}"
            );
        }
    }

    #[test]
    fn service_and_peer_policies_refuse_transition_ranges_without_a_grant() {
        for endpoint in [
            "https://100.64.0.1:7900/sparql",           // CGNAT lower boundary
            "https://100.127.255.254:7900/sparql",      // CGNAT upper boundary
            "https://[2002:c000:0201::1]:7900/sparql",  // 6to4
            "https://[2001:0:c000:201::1]:7900/sparql", // Teredo
        ] {
            for policy in [
                OutboundAllowPolicy::ExplicitOnly,
                OutboundAllowPolicy::PublicHttps,
            ] {
                assert!(
                    validate_outbound_http_target(endpoint, &[], policy).is_err(),
                    "{policy:?} admitted {endpoint} without an exact grant"
                );
            }
        }
    }

    #[cfg(feature = "federation-sql")]
    #[test]
    fn sql_dsn_hosts_are_vetted_like_http_destinations() {
        // Holds the allow-list lock so a concurrent test cannot widen it mid-assertion.
        let _guard = crate::federation_tests::MockHttpAllowGuard::new("unused.invalid");
        for dsn in [
            "postgres://u:p@127.0.0.1:5432/db", // sanitizer:ignore
            "postgresql://u@[::1]/db",
            "mysql://u@10.1.2.3/db",
            "postgres:///db",
            "postgres://u@db.invalid/db?host=/var/run/postgresql",
            "postgres://u@db.invalid:0/db",
        ] {
            assert!(check_sql_dsn(dsn).is_err(), "{dsn}");
        }
        let err = check_sql_dsn("postgres://user:hunter2@127.0.0.1/db").unwrap_err(); // sanitizer:ignore
        assert!(
            !err.contains("hunter2") && !err.contains("127.0.0.1"),
            "{err}"
        );
    }

    #[cfg(feature = "federation-sql")]
    #[test]
    fn an_allowlisted_internal_sql_host_is_accepted() {
        let _guard = crate::federation_tests::MockHttpAllowGuard::new("127.0.0.1:5432");
        assert!(check_sql_dsn("postgres://u@127.0.0.1:5432/db").is_ok());
        assert!(check_sql_dsn("postgres://u@127.0.0.1:5433/db").is_err());
    }
}
