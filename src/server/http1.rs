//! One HTTP/1.1 request-framing authority for the hand-rolled listeners.
//!
//! Every serving surface in `src/server` speaks HTTP/1.1 over a raw
//! [`TcpStream`] (the dependency-free "Pi contract" — no axum/hyper). Before
//! this module each of them carried its own reader, and the ten copies
//! DISAGREED about framing: `federation`, `viz_interactive` and
//! `policy_export` accepted a `Transfer-Encoding` header next to
//! `Content-Length` (request smuggling), never bounded the header count or
//! the body, and let control bytes through header values; `sparql_http`
//! rejected a repeated `Accept` but silently kept the first of two repeated
//! `Cookie`s; `lake::rest` let a later duplicate overwrite an earlier header.
//! Framing is one contract, so it has one owner here — the strictest of the
//! nine, which was `obs`'s.
//!
//! What a caller gets is a syntactically valid, unambiguously framed request:
//! a well-formed request line, uniquely named headers with RFC 7230 token
//! names and control-free values, and exactly `Content-Length` body bytes.
//! Everything above that — which methods and paths a surface serves, whether
//! it requires `Host`, how it decodes its target — is the surface's own
//! routing contract and stays with the surface. So is the READ DEADLINE: this
//! reader awaits its peer, and a caller that does not wrap it in a timeout is
//! open to a slowloris.
//!
//! `Expect: 100-continue` is handled nowhere in this tree, before or after the
//! consolidation: a client that waits for the interim response stalls until
//! its surface's read timeout.
//!
//! One serving endpoint deliberately does NOT come through here:
//! [`crate::metrics::serve`] parses no request at all — it drains one read and
//! answers the whole registry whatever was sent — so it has no framing to
//! share. Every endpoint that does parse a request uses this reader.

use std::collections::HashMap;

use tokio::io::AsyncReadExt;
use tokio::net::TcpStream;

/// Bounds a single header line may not exceed.
const MAX_HEADER_LINE_BYTES: usize = 16 * 1024;
/// Most headers one request may carry.
const MAX_HEADERS: usize = 128;
/// Longest request target (path + query) accepted.
const MAX_TARGET_BYTES: usize = 8 * 1024;
/// Socket read granularity, the page size every prior copy of this reader used.
const CHUNK_BYTES: usize = 4096;

/// The two byte budgets that genuinely differ between surfaces: a metrics
/// scrape and a 64 MiB object PUT are not the same request.
#[derive(Clone, Copy)]
pub(crate) struct RequestLimits {
    /// Cap on the bytes read before the `\r\n\r\n` header terminator.
    pub max_head_bytes: usize,
    /// Cap on `Content-Length`, and therefore on the body actually read.
    pub max_body_bytes: usize,
}

/// One framed HTTP/1.1 request.
pub(crate) struct HttpMessage {
    /// The request method, verbatim (`GET`, `POST`, …).
    pub method: String,
    /// The origin-form request target, verbatim (`/v1/logs?since=1`).
    pub target: String,
    /// `HTTP/1.0` or `HTTP/1.1`.
    pub version: String,
    /// Every header, names lowercased. A repeated name is rejected by the
    /// reader, so this map loses nothing.
    pub headers: HashMap<String, String>,
    /// Exactly `Content-Length` body bytes (empty when the header is absent).
    pub body: Vec<u8>,
}

impl HttpMessage {
    /// The value of `name` (lowercase), or `""` when the request omitted it.
    pub fn header(&self, name: &str) -> &str {
        self.headers.get(name).map_or("", String::as_str)
    }

    /// The body decoded lossily as UTF-8, for the surfaces whose payload is text.
    pub fn text(&self) -> std::borrow::Cow<'_, str> {
        String::from_utf8_lossy(&self.body)
    }

    /// The target split at its first `?`; the query is `""` when absent.
    pub fn path_and_query(&self) -> (&str, &str) {
        self.target.split_once('?').unwrap_or((&self.target, ""))
    }
}

/// Read and frame one HTTP/1.1 request from `stream`. `None` means the request
/// is unframeable — malformed, ambiguous, or over `limits` — and the caller
/// must close the connection rather than guess at a response.
pub(crate) async fn read_request(
    stream: &mut TcpStream,
    limits: RequestLimits,
) -> Option<HttpMessage> {
    let mut buf = Vec::new();
    let header_end = read_head(stream, &mut buf, limits.max_head_bytes).await?;

    let head = std::str::from_utf8(&buf[..header_end]).ok()?;
    let mut lines = head.split("\r\n");
    let (method, target, version) = parse_request_line(lines.next()?)?;
    let (headers, content_length) = parse_headers(lines, limits.max_body_bytes)?;
    let body = read_body(stream, &buf[header_end + 4..], content_length).await?;

    Some(HttpMessage {
        method,
        target,
        version,
        headers,
        body,
    })
}

/// Read into `buf` until the `\r\n\r\n` head/body boundary appears, bounded by
/// `max_head_bytes`. Returns the boundary offset.
async fn read_head(
    stream: &mut TcpStream,
    buf: &mut Vec<u8>,
    max_head_bytes: usize,
) -> Option<usize> {
    let mut tmp = [0u8; CHUNK_BYTES];
    loop {
        if let Some(pos) = find_subslice(buf, b"\r\n\r\n") {
            return (pos <= max_head_bytes).then_some(pos);
        }
        let n = stream.read(&mut tmp).await.ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&tmp[..n]);
        if buf.len() > max_head_bytes {
            return None;
        }
    }
}

/// Parse and validate `METHOD SP target SP VERSION`: an RFC 7230 token method,
/// an origin-form target, and HTTP/1.0 or HTTP/1.1.
fn parse_request_line(request_line: &str) -> Option<(String, String, String)> {
    if !is_well_formed_request_line(request_line) {
        return None;
    }
    let mut parts = request_line.split(' ');
    let method = parts.next()?;
    let target = parts.next()?;
    let version = parts.next()?;
    if !matches!(version, "HTTP/1.0" | "HTTP/1.1")
        || method.is_empty()
        || !method.bytes().all(is_token_byte)
        || target.len() > MAX_TARGET_BYTES
        || !target.starts_with('/')
    {
        return None;
    }
    Some((method.to_string(), target.to_string(), version.to_string()))
}

/// RFC 7230 §3.1.1 request-line shape: ASCII, no control bytes, and EXACTLY two
/// single spaces. `split_whitespace` would have accepted a leading space, a run
/// of spaces, a TAB or a vertical tab as the separator, each of which a
/// fronting proxy may split differently from this server.
fn is_well_formed_request_line(request_line: &str) -> bool {
    request_line.is_ascii()
        && !request_line.bytes().any(|byte| byte.is_ascii_control())
        && request_line.bytes().filter(|byte| *byte == b' ').count() == 2
}

/// Validate and fold every header line, returning the lowercase-keyed map and
/// the framed body length. A repeated name, a `Transfer-Encoding` (chunked
/// framing is deliberately unsupported: accepting it beside a `Content-Length`
/// is the request-smuggling ambiguity), or a `Content-Length` over
/// `max_body_bytes` rejects the request.
fn parse_headers<'a>(
    lines: impl Iterator<Item = &'a str>,
    max_body_bytes: usize,
) -> Option<(HashMap<String, String>, usize)> {
    let mut headers: HashMap<String, String> = HashMap::new();
    let mut content_length: Option<usize> = None;
    for (index, line) in lines.enumerate() {
        if index >= MAX_HEADERS || line.len() > MAX_HEADER_LINE_BYTES {
            return None;
        }
        let (name, value) = line.split_once(':')?;
        if !is_valid_header_line(name, value) {
            return None;
        }
        let name = name.to_ascii_lowercase();
        let value = value.trim().to_string();
        if name == "transfer-encoding" {
            return None;
        }
        if name == "content-length" {
            content_length = Some(value.parse().ok().filter(|n| *n <= max_body_bytes)?);
        }
        if headers.insert(name, value).is_some() {
            return None;
        }
    }
    Some((headers, content_length.unwrap_or(0)))
}

/// RFC 7230 `field-name` / `field-value` syntax for one header line. The name
/// is checked VERBATIM — never trimmed — because a space is not a `tchar`:
/// `Host : evil` and an obs-fold continuation line (` Authorization: …`, which
/// `split("\r\n")` hands us as its own line whose "name" begins with a space)
/// are both MUST-rejects under RFC 7230 §3.2.4, and both are front-proxy
/// divergence primitives — a header a fronting proxy folded into the previous
/// value must not re-materialize here as a header of its own. Only the VALUE
/// carries legal surrounding whitespace.
fn is_valid_header_line(name: &str, value: &str) -> bool {
    !name.is_empty()
        && name.bytes().all(is_token_byte)
        && !value
            .bytes()
            .any(|byte| byte.is_ascii_control() && byte != b'\t')
}

/// RFC 7230 `tchar`: the byte set legal in a method name or a header name.
fn is_token_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
        || matches!(
            byte,
            b'!' | b'#'
                | b'$'
                | b'%'
                | b'&'
                | b'\''
                | b'*'
                | b'+'
                | b'-'
                | b'.'
                | b'^'
                | b'_'
                | b'`'
                | b'|'
                | b'~'
        )
}

/// Read exactly `content_length` body bytes, starting from `already_read` —
/// whatever arrived past the head boundary in the same syscall. A peer that
/// sends more than it framed (a pipelined second request, or a smuggled one)
/// is rejected rather than truncated.
async fn read_body(
    stream: &mut TcpStream,
    already_read: &[u8],
    content_length: usize,
) -> Option<Vec<u8>> {
    if already_read.len() > content_length {
        return None;
    }
    let mut body = already_read.to_vec();
    let mut tmp = [0u8; CHUNK_BYTES];
    while body.len() < content_length {
        let n = stream.read(&mut tmp).await.ok()?;
        if n == 0 || body.len() + n > content_length {
            return None;
        }
        body.extend_from_slice(&tmp[..n]);
    }
    Some(body)
}

/// The offset of `needle` in `haystack`, the one copy of a search eight
/// listeners each carried.
pub(crate) fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}
