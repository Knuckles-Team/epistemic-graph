//! HTTP/JSON source capabilities (EH-563 FO-03), declared by placeholders in the query
//! string of the registered URL:
//!
//! | placeholder | capability | substituted with |
//! |---|---|---|
//! | `{keys}` | batched key lookup; the source REQUIRES keys | comma-joined, percent-encoded ids |
//! | `{limit}` | limit pushdown / page size | the pushed limit, the page size, or 10 000 |
//! | `{offset}` | offset paging | the page's row offset |
//! | `{page}` | page-number paging (1-based) | the page number |
//!
//! Placeholders are recognised only after the `?` — the scheme, host and path can never be
//! templated, so a substituted value cannot change the destination; every concrete URL
//! still goes through the DNS-pinned SSRF gate. A key containing `,` cannot be expressed in
//! a comma list and is never pushed. A `{limit}` without paging and without a pushed limit
//! requests 10 000 rows; a response that fills it is refused as
//! [`super::RESULT_INCOMPLETE`] rather than silently truncated.

use eg_types::wire::{ForeignSourceSpec, HttpFieldMap};

use super::budget::RESULT_INCOMPLETE;
use super::capability::{
    FullFetch, KeyLookup, LimitPushdown, Paging, RemoteRequest, SourceCapabilities,
};
use super::remote::{Identity, RemoteFetch};
use super::stats::Fingerprint;
use crate::rowset::RowSet;

/// The longest concrete URL (mirrors the HTTP/JSON source's own cap).
const MAX_URL_BYTES: usize = 2 * 1024;
/// Keys per request before the URL-length fit.
const MAX_HTTP_KEYS: usize = 100;
/// Rows requested by an unpaged `{limit}` with no pushed limit.
const UNPAGED_LIMIT: usize = 10_000;
/// Rows per page when paging.
pub(crate) const PAGE_SIZE: usize = 100;
/// Bytes reserved for the other substituted placeholders when fitting keys into a URL.
const PLACEHOLDER_SLACK: usize = 64;

const KEYS: &str = "{keys}";
const LIMIT: &str = "{limit}";
const OFFSET: &str = "{offset}";
const PAGE: &str = "{page}";

/// Which placeholders a URL's query string carries.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Placeholders {
    keys: bool,
    limit: bool,
    offset: bool,
    page: bool,
}

impl Placeholders {
    /// Parse the placeholders of `url`; `Err` when one appears before the query string.
    fn of(url: &str) -> Result<Self, String> {
        let (head, query) = url.split_once('?').unwrap_or((url, ""));
        if [KEYS, LIMIT, OFFSET, PAGE].iter().any(|p| head.contains(p)) {
            return Err(
                "federation: HTTP JSON placeholders are only allowed in the query string"
                    .to_string(),
            );
        }
        Ok(Self {
            keys: query.contains(KEYS),
            limit: query.contains(LIMIT),
            offset: query.contains(OFFSET),
            page: query.contains(PAGE),
        })
    }

    fn paging(self) -> Paging {
        if self.offset {
            Paging::Offset
        } else if self.page {
            Paging::Page
        } else {
            Paging::Single
        }
    }
}

/// An HTTP/JSON foreign source whose URL declares its pushdown capabilities.
pub(crate) struct HttpRemote<'a> {
    url: &'a str,
    json_path: &'a str,
    field_map: &'a HttpFieldMap,
    placeholders: Result<Placeholders, String>,
    identity: Identity,
}

impl<'a> HttpRemote<'a> {
    pub(crate) fn new(
        url: &'a str,
        json_path: &'a str,
        field_map: &'a HttpFieldMap,
        identity: Identity,
    ) -> Self {
        Self {
            url,
            json_path,
            field_map,
            placeholders: Placeholders::of(url),
            identity,
        }
    }

    /// The concrete URL for `request`.
    fn render(&self, p: Placeholders, request: &RemoteRequest) -> String {
        let mut url = self.url.to_string();
        if p.keys {
            url = url.replace(KEYS, &encode_keys(&request.keys));
        }
        if p.limit {
            url = url.replace(LIMIT, &limit_value(p, request).to_string());
        }
        let page = request.page.unwrap_or(super::capability::PageRequest {
            index: 0,
            offset: 0,
            size: PAGE_SIZE,
        });
        url.replace(OFFSET, &page.offset.to_string())
            .replace(PAGE, &(page.index + 1).to_string())
    }

    fn get(&self, url: String) -> Result<RowSet, String> {
        let spec = ForeignSourceSpec::HttpJson {
            url,
            json_path: self.json_path.to_string(),
            field_map: self.field_map.clone(),
        };
        let source = crate::federation::source_for(&spec);
        source.fetch()
    }
}

/// The `{limit}` value: the page size when paging, else the pushed limit, else the unpaged
/// ceiling.
fn limit_value(p: Placeholders, request: &RemoteRequest) -> usize {
    match (request.page, request.limit) {
        (Some(page), _) => page.size,
        (None, Some(k)) => k,
        (None, None) if p.paging() == Paging::Single => UNPAGED_LIMIT,
        (None, None) => PAGE_SIZE,
    }
}

impl RemoteFetch for HttpRemote<'_> {
    fn capabilities(&self) -> SourceCapabilities {
        let Ok(&p) = self.placeholders.as_ref() else {
            return SourceCapabilities::fetch_only();
        };
        SourceCapabilities {
            key_lookup: if p.keys {
                KeyLookup::Batched {
                    max_keys: MAX_HTTP_KEYS,
                }
            } else {
                KeyLookup::Unsupported
            },
            limit: if p.limit {
                LimitPushdown::Native
            } else {
                LimitPushdown::Unsupported
            },
            paging: p.paging(),
            full_fetch: if p.keys {
                FullFetch::RequiresKeys
            } else {
                FullFetch::Allowed
            },
        }
    }

    fn label(&self) -> String {
        self.identity.label.clone()
    }

    fn fingerprint(&self) -> Fingerprint {
        self.identity.fingerprint
    }

    fn fetch(&self, request: &RemoteRequest) -> Result<RowSet, String> {
        let p = self.placeholders.clone()?;
        let rows = self.get(self.render(p, request))?;
        let unpaged_ceiling = p.limit && request.page.is_none() && request.limit.is_none();
        if unpaged_ceiling && p.paging() == Paging::Single && rows.len() >= UNPAGED_LIMIT {
            return Err(format!(
                "{RESULT_INCOMPLETE}: the HTTP JSON source returned its full {UNPAGED_LIMIT}-row page \
                 and declares no paging placeholder"
            ));
        }
        Ok(rows)
    }

    fn fit_keys(&self, keys: &[String]) -> usize {
        let base = self.url.len().saturating_sub(KEYS.len()) + PLACEHOLDER_SLACK;
        let mut used = base;
        let fitted = keys
            .iter()
            .take_while(|k| {
                used += encoded_len(k) + 1;
                used <= MAX_URL_BYTES
            })
            .count();
        fitted.max(usize::from(!keys.is_empty()))
    }

    fn key_expressible(&self, key: &str) -> bool {
        !key.is_empty() && !key.contains(',')
    }
}

/// Is `b` an RFC 3986 unreserved byte (never percent-encoded)?
fn unreserved(b: u8) -> bool {
    b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_' | b'~')
}

/// Percent-encode one value (everything but unreserved bytes).
fn encode(value: &str) -> String {
    use std::fmt::Write;
    let mut out = String::with_capacity(value.len());
    for b in value.bytes() {
        if unreserved(b) {
            out.push(char::from(b));
        } else {
            let _ = write!(out, "%{b:02X}");
        }
    }
    out
}

fn encoded_len(value: &str) -> usize {
    value
        .bytes()
        .map(|b| if unreserved(b) { 1 } else { 3 })
        .sum()
}

fn encode_keys(keys: &[String]) -> String {
    keys.iter().map(|k| encode(k)).collect::<Vec<_>>().join(",")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placeholders_are_only_recognised_in_the_query_string() {
        assert!(Placeholders::of("https://api.example/{keys}/x").is_err());
        assert!(Placeholders::of("https://{keys}.example/x").is_err());
        let p = Placeholders::of("https://api.example/x?ids={keys}&n={limit}").unwrap();
        assert!(p.keys && p.limit && !p.offset && !p.page);
    }

    #[test]
    fn keys_are_percent_encoded_and_comma_joined() {
        let keys = vec!["a b".to_string(), "é/&".to_string(), "plain-1".to_string()];
        assert_eq!(encode_keys(&keys), "a%20b,%C3%A9%2F%26,plain-1");
        assert_eq!(encoded_len("é/&"), encode("é/&").len());
    }
}
