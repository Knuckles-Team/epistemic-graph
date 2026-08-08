//! Minimal AWS Signature Version 4 request signing, for the Braket adapter's outbound
//! calls to Braket's AWS API Gateway control plane (`CreateQuantumTask`,
//! `GetQuantumTask`, `CancelQuantumTask`).
//!
//! Not vendored from an `aws-sigv4`/`aws-sdk-*` crate (neither is in this workspace's
//! `Cargo.lock`, and adding either would be exactly the kind of new third-party
//! dependency this program's charter asks to avoid where a small hand-rolled
//! implementation suffices). It IS, however, the same well-known algorithm and the
//! same `hmac`+`sha2` crates (at the SAME pinned versions) `src/server/s3/mod.rs`
//! already uses to *verify* inbound SigV4 requests for this repo's own S3-compatible
//! gateway -- this module is that algorithm's client (signing) side, independently
//! implemented against the public AWS spec since the S3 module's helpers are
//! `fn`-private to a binary crate this library crate cannot depend on (doing so would
//! invert the workspace's dependency direction).
//!
//! Reference: <https://docs.aws.amazon.com/general/latest/gr/sigv4-signing-examples.html>

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone)]
pub struct AwsCredentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    /// SigV4 requires a session token header (`x-amz-security-token`) when the
    /// caller is using temporary (STS-issued) credentials; `None` for a long-lived
    /// IAM user access key pair.
    pub session_token: Option<String>,
    pub region: String,
}

/// The fully-signed headers a caller must attach to the outbound request, in
/// addition to whatever it already set. `authorization` is the complete
/// `AWS4-HMAC-SHA256 Credential=...` header value.
#[derive(Debug, Clone)]
pub struct SignedHeaders {
    pub authorization: String,
    pub x_amz_date: String,
    pub x_amz_content_sha256: String,
    pub x_amz_security_token: Option<String>,
}

fn uri_encode(value: &str, encode_slash: bool) -> String {
    let mut out = String::new();
    for byte in value.as_bytes() {
        if byte.is_ascii_alphanumeric()
            || matches!(*byte, b'-' | b'_' | b'.' | b'~')
            || (!encode_slash && *byte == b'/')
        {
            out.push(*byte as char);
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

fn hmac_bytes(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts a key of any length");
    mac.update(value);
    mac.finalize().into_bytes().to_vec()
}

/// Format the current instant as an AWS-style `YYYYMMDDTHHMMSSZ` timestamp plus the
/// bare `YYYYMMDD` date stamp, from Unix epoch seconds -- Howard Hinnant's
/// days-from-civil algorithm (the exact civil-calendar conversion AWS's own
/// examples use), so this module needs no `chrono`/`time` dependency.
fn amz_date_and_scope(now: std::time::SystemTime) -> (String, String) {
    let secs = now
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock is after the Unix epoch")
        .as_secs() as i64;
    let days = secs.div_euclid(86_400);
    let secs_of_day = secs.rem_euclid(86_400);
    let (hour, minute, second) = (
        secs_of_day / 3600,
        (secs_of_day % 3600) / 60,
        secs_of_day % 60,
    );
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (
        format!("{y:04}{m:02}{d:02}T{hour:02}{minute:02}{second:02}Z"),
        format!("{y:04}{m:02}{d:02}"),
    )
}

/// Sign one request. `path` is already URL-path-encoded per AWS's rules by the
/// caller (Braket paths here are always a literal `/quantum-task` or
/// `/quantum-task/{id}`, no dynamic path-segment encoding needed). `query` is the
/// raw (unsorted, unencoded) query string, `""` for the POST calls this adapter
/// makes. `body` is the exact bytes that will be sent -- the payload hash MUST match
/// what actually goes over the wire.
#[allow(clippy::too_many_arguments)]
pub fn sign(
    creds: &AwsCredentials,
    service: &str,
    method: &str,
    host: &str,
    path: &str,
    query: &str,
    body: &[u8],
    now: std::time::SystemTime,
) -> SignedHeaders {
    let (amz_date, date_stamp) = amz_date_and_scope(now);
    let payload_hash = hex::encode(Sha256::digest(body));

    let mut signed_header_names = vec!["host", "x-amz-content-sha256", "x-amz-date"];
    let mut canonical_headers =
        format!("host:{host}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amz_date}\n");
    if creds.session_token.is_some() {
        signed_header_names.push("x-amz-security-token");
    }
    signed_header_names.sort_unstable();
    if let Some(token) = &creds.session_token {
        // Re-derive canonical_headers in sorted-header order including the token.
        let mut pairs = vec![
            ("host".to_string(), host.to_string()),
            ("x-amz-content-sha256".to_string(), payload_hash.clone()),
            ("x-amz-date".to_string(), amz_date.clone()),
            ("x-amz-security-token".to_string(), token.clone()),
        ];
        pairs.sort_by(|a, b| a.0.cmp(&b.0));
        canonical_headers = pairs
            .into_iter()
            .map(|(k, v)| format!("{k}:{v}\n"))
            .collect::<String>();
    }
    let signed_headers_raw = signed_header_names.join(";");

    let canonical_request = format!(
        "{method}\n{}\n{}\n{canonical_headers}\n{signed_headers_raw}\n{payload_hash}",
        uri_encode(path, false),
        canonical_query(query),
    );
    let scope = format!("{date_stamp}/{}/{service}/aws4_request", creds.region);
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
        hex::encode(Sha256::digest(canonical_request.as_bytes())),
    );

    let date_key = hmac_bytes(
        format!("AWS4{}", creds.secret_access_key).as_bytes(),
        date_stamp.as_bytes(),
    );
    let region_key = hmac_bytes(&date_key, creds.region.as_bytes());
    let service_key = hmac_bytes(&region_key, service.as_bytes());
    let signing_key = hmac_bytes(&service_key, b"aws4_request");
    let signature = hex::encode(hmac_bytes(&signing_key, string_to_sign.as_bytes()));

    let authorization = format!(
        "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_headers_raw}, Signature={signature}",
        creds.access_key_id,
    );

    SignedHeaders {
        authorization,
        x_amz_date: amz_date,
        x_amz_content_sha256: payload_hash,
        x_amz_security_token: creds.session_token.clone(),
    }
}

fn canonical_query(raw: &str) -> String {
    if raw.is_empty() {
        return String::new();
    }
    let mut fields: Vec<(String, String)> = raw
        .split('&')
        .filter(|part| !part.is_empty())
        .map(|part| {
            let (key, value) = part.split_once('=').unwrap_or((part, ""));
            (uri_encode(key, true), uri_encode(value, true))
        })
        .collect();
    fields.sort();
    fields
        .into_iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A fixed, well-known AWS SigV4 test vector's spirit (not the literal published
    /// vector, since this signs against a synthetic Braket-shaped request) -- what
    /// this test actually pins is DETERMINISM and STRUCTURE: signing the same request
    /// twice at the same instant produces the identical signature, and the
    /// authorization header has the expected `AWS4-HMAC-SHA256 Credential=...`
    /// shape, both necessary properties for a correct signer even without a
    /// reference vector.
    #[test]
    fn signing_is_deterministic_and_well_formed() {
        let creds = AwsCredentials {
            access_key_id: "AKIDEXAMPLE".to_string(),
            secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
            session_token: None,
            region: "us-east-1".to_string(),
        };
        let now = std::time::UNIX_EPOCH + std::time::Duration::from_secs(1_700_000_000);
        let body = br#"{"deviceArn":"arn:aws:braket:::device/quantum-simulator/amazon/sv1"}"#;
        let a = sign(
            &creds,
            "braket",
            "POST",
            "braket.us-east-1.amazonaws.com",
            "/quantum-task",
            "",
            body,
            now,
        );
        let b = sign(
            &creds,
            "braket",
            "POST",
            "braket.us-east-1.amazonaws.com",
            "/quantum-task",
            "",
            body,
            now,
        );
        assert_eq!(
            a.authorization, b.authorization,
            "signing must be deterministic"
        );
        assert!(a.authorization.starts_with(
            "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20231114/us-east-1/braket/aws4_request"
        ));
        assert!(a
            .authorization
            .contains("SignedHeaders=host;x-amz-content-sha256;x-amz-date"));
        assert_eq!(
            a.x_amz_content_sha256.len(),
            64,
            "sha256 hex digest is 64 chars"
        );
    }

    #[test]
    fn different_bodies_produce_different_signatures() {
        let creds = AwsCredentials {
            access_key_id: "AKIDEXAMPLE".to_string(),
            secret_access_key: "secret".to_string(),
            session_token: None,
            region: "us-west-2".to_string(),
        };
        let now = std::time::SystemTime::now();
        let a = sign(
            &creds,
            "braket",
            "POST",
            "h",
            "/quantum-task",
            "",
            b"{\"a\":1}",
            now,
        );
        let b = sign(
            &creds,
            "braket",
            "POST",
            "h",
            "/quantum-task",
            "",
            b"{\"a\":2}",
            now,
        );
        assert_ne!(a.authorization, b.authorization);
    }

    #[test]
    fn session_token_is_included_when_present() {
        let creds = AwsCredentials {
            access_key_id: "AKIDEXAMPLE".to_string(),
            secret_access_key: "secret".to_string(),
            session_token: Some("FQoGZXIvYXdzE...".to_string()),
            region: "us-east-1".to_string(),
        };
        let signed = sign(
            &creds,
            "braket",
            "POST",
            "h",
            "/quantum-task",
            "",
            b"{}",
            std::time::SystemTime::now(),
        );
        assert!(signed
            .authorization
            .contains("SignedHeaders=host;x-amz-content-sha256;x-amz-date;x-amz-security-token"));
        assert_eq!(
            signed.x_amz_security_token.as_deref(),
            Some("FQoGZXIvYXdzE...")
        );
    }
}
