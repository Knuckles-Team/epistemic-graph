use super::*;

// ── EG-127 helpers: hashing, non-deterministic sources, date-time, URI encoding ──

/// Hex-encoded digest of `input` for the SPARQL hash built-ins (CONCEPT:EG-KG.ontology.concept-4). The
/// `sha2::Digest` bound is the shared RustCrypto `digest::Digest` trait (re-exported by
/// `md-5`/`sha1`/`sha2` at the same 0.10 line), so it accepts `Md5`/`Sha1`/`Sha2*`.
#[cfg(feature = "sparql-hash")]
pub(super) fn hash_hex<D: sha2::Digest>(input: &str) -> String {
    use std::fmt::Write as _;
    let out = D::digest(input.as_bytes());
    let mut s = String::with_capacity(out.len() * 2);
    for b in out {
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// Global PRNG state for the non-deterministic built-ins (`RAND`/`UUID`/`STRUUID`/
/// arg-less `BNODE`). NOT cryptographic and deliberately kept off any cached path.
pub(super) static RNG_STATE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// SplitMix64 mixed with the wall clock + a monotonic counter — a cheap, dependency-free
/// non-deterministic source. Sufficient for SPARQL RAND/UUID (which need no crypto grade).
pub(super) fn next_rand_u64() -> u64 {
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let mut x =
        RNG_STATE.fetch_add(0x9E37_79B9_7F4A_7C15, std::sync::atomic::Ordering::Relaxed) ^ t;
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

/// A 53-bit-mantissa double in `[0, 1)` for `RAND()`.
pub(super) fn rand_f64() -> f64 {
    (next_rand_u64() >> 11) as f64 / ((1u64 << 53) as f64)
}

/// A fresh opaque id for arg-less `BNODE()`.
pub(super) fn fresh_id() -> u64 {
    next_rand_u64()
}

/// A blank-node label reduced to `[A-Za-z0-9_]` so `BNODE(str)` yields a legal label.
pub(super) fn sanitize_bnode_label(s: &str) -> String {
    let cleaned: String = s
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    if cleaned.is_empty() {
        format!("b{:x}", fresh_id())
    } else {
        cleaned
    }
}

/// An RFC-4122 v4 UUID string (used by `UUID()`/`STRUUID()`).
pub(super) fn fresh_uuid() -> String {
    let (a, b) = (next_rand_u64(), next_rand_u64());
    let bytes = [
        (a >> 56) as u8,
        (a >> 48) as u8,
        (a >> 40) as u8,
        (a >> 32) as u8,
        (a >> 24) as u8,
        (a >> 16) as u8,
        (((a >> 8) as u8) & 0x0f) | 0x40, // version 4
        a as u8,
        (((b >> 56) as u8) & 0x3f) | 0x80, // variant 10xx
        (b >> 48) as u8,
        (b >> 40) as u8,
        (b >> 32) as u8,
        (b >> 24) as u8,
        (b >> 16) as u8,
        (b >> 8) as u8,
        b as u8,
    ];
    let mut s = String::with_capacity(36);
    use std::fmt::Write as _;
    for (i, byte) in bytes.iter().enumerate() {
        if matches!(i, 4 | 6 | 8 | 10) {
            s.push('-');
        }
        let _ = write!(s, "{byte:02x}");
    }
    s
}

/// Percent-encode per SPARQL `ENCODE_FOR_URI` (unreserved set `A-Za-z0-9-_.~` pass through).
pub(super) fn encode_for_uri(s: &str) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => {
                let _ = write!(out, "%{b:02X}");
            }
        }
    }
    out
}

/// Decomposed xsd:dateTime / xsd:date fields for the accessor built-ins.
pub(super) struct DateTimeParts {
    year: i64,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
    second: f64,
    /// The timezone lexical exactly as written (`""`, `"Z"`, `"+01:00"`, `"-05:00"`).
    tz: String,
}

/// Parse an xsd:dateTime (or xsd:date) lexical enough to serve YEAR..SECONDS/TZ/TIMEZONE.
pub(super) fn parse_datetime(s: &str) -> Option<DateTimeParts> {
    let s = s.trim();
    let (neg, rest) = match s.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, s),
    };
    let (date_part, time_part) = match rest.split_once('T') {
        Some((d, t)) => (d, Some(t)),
        None => (rest, None),
    };
    let mut di = date_part.split('-');
    let year: i64 = di.next()?.parse().ok()?;
    let month: u32 = di.next()?.parse().ok()?;
    let day: u32 = di.next()?.parse().ok()?;
    let (mut hour, mut minute, mut second, mut tz) = (0u32, 0u32, 0.0f64, String::new());
    if let Some(tp) = time_part {
        // Split off the timezone: `Z`/`+..` is unambiguous; a trailing `-..` is the tz.
        let (hms, tzs) = if let Some(i) = tp.find(['Z', '+']) {
            (&tp[..i], tp[i..].to_string())
        } else if let Some(i) = tp.rfind('-') {
            (&tp[..i], tp[i..].to_string())
        } else {
            (tp, String::new())
        };
        tz = tzs;
        let mut ti = hms.split(':');
        hour = ti.next()?.parse().ok()?;
        minute = ti.next()?.parse().ok()?;
        second = ti.next().unwrap_or("0").parse().ok()?;
    }
    Some(DateTimeParts {
        year: if neg { -year } else { year },
        month,
        day,
        hour,
        minute,
        second,
        tz,
    })
}

/// The `xsd:dayTimeDuration` form of a timezone lexical, per SPARQL `TIMEZONE`
/// (`Z`/empty → `PT0S`, `+01:00` → `PT1H`, `-05:30` → `-PT5H30M`).
pub(super) fn tz_to_duration(tz: &str) -> String {
    if tz.is_empty() || tz == "Z" {
        return "PT0S".to_string();
    }
    let (sign, rest) = match tz.strip_prefix('+') {
        Some(r) => ("", r),
        None => match tz.strip_prefix('-') {
            Some(r) => ("-", r),
            None => return "PT0S".to_string(),
        },
    };
    let mut it = rest.split(':');
    let h: i64 = it.next().and_then(|x| x.parse().ok()).unwrap_or(0);
    let m: i64 = it.next().and_then(|x| x.parse().ok()).unwrap_or(0);
    let mut out = format!("{sign}PT");
    if h != 0 {
        out.push_str(&format!("{h}H"));
    }
    if m != 0 {
        out.push_str(&format!("{m}M"));
    }
    if h == 0 && m == 0 {
        out.push_str("0S");
    }
    out
}

/// The current UTC instant as an `xsd:dateTime` lexical (`NOW()`), without a date crate:
/// seconds-since-epoch → civil date via Howard Hinnant's `days_from_civil` inverse.
pub(super) fn now_xsd_datetime() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400);
    let (y, m, d) = civil_from_days(days);
    let (hh, mm, ss) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    format!("{y:04}-{m:02}-{d:02}T{hh:02}:{mm:02}:{ss:02}Z")
}

/// Civil (proleptic Gregorian) `(year, month, day)` from a days-since-1970-01-01 count.
pub(super) fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = (if mp < 10 { mp + 3 } else { mp - 9 }) as u32; // [1, 12]
    (if m <= 2 { y + 1 } else { y }, m, d)
}

// ── EG-130 helpers: RDF-star quoted-triple term (string-encoded first-class term) ──

/// Render a `Binding` as a term inside a quoted triple: nodes keep their `<iri>`/`_:b`
/// form; literals are quoted (`"lit"`).
#[cfg(feature = "sparql-star")]
pub(super) fn render_star_term(b: &Binding) -> String {
    match b {
        Binding::Node(s) => s.clone(),
        Binding::Literal(s) => format!("{s:?}"),
    }
}

/// Encode a quoted triple from its three component bindings as `<< s p o >>`.
#[cfg(feature = "sparql-star")]
pub(super) fn encode_quoted(s: &Binding, p: &Binding, o: &Binding) -> String {
    format!(
        "<< {} {} {} >>",
        render_star_term(s),
        render_star_term(p),
        render_star_term(o)
    )
}

/// Whether a binding is a quoted-triple term (`<< … >>`).
#[cfg(feature = "sparql-star")]
pub(super) fn is_quoted(b: &Binding) -> bool {
    matches!(b, Binding::Node(s) if s.starts_with("<<") && s.ends_with(">>"))
}

/// Project component `idx` (0=subject, 1=predicate, 2=object) of a quoted-triple term.
/// Components are whitespace-delimited canonical terms (IRIs/bnodes/simple literals);
/// space-bearing or nested-quoted components are a documented follow-up.
#[cfg(feature = "sparql-star")]
pub(super) fn quoted_component(b: &Binding, idx: usize) -> Option<Binding> {
    if !is_quoted(b) {
        return None;
    }
    let Binding::Node(s) = b else { return None };
    let inner = s.strip_prefix("<<")?.strip_suffix(">>")?.trim();
    let tok = inner.split_whitespace().nth(idx)?;
    Some(if tok.starts_with('<') || tok.starts_with("_:") {
        Binding::Node(tok.to_string())
    } else {
        Binding::Literal(tok.trim_matches('"').to_string())
    })
}

#[cfg(test)]
pub(super) fn view_of_turtle(ttl: &str) -> GraphView {
    let core = eg_core::graph::GraphCore::new();
    let mut iris = crate::mapping::IriStore::default();
    crate::mapping::load_triples(
        &core,
        &mut iris,
        "g",
        crate::mapping::parse_turtle(ttl).unwrap(),
    )
    .unwrap();
    core.analysis_snapshot()
}
