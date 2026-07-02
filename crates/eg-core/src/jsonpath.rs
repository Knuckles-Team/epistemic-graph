//! CONCEPT:EG-084 — document/JSON deep indexing: a pure-Rust JSONPath evaluator +
//! Postgres-style JSON containment, over the SAME `serde_json::Value` the node
//! property blobs already decode to. ZERO new dependency (serde_json is already an
//! eg-core dep) — a small hand-rolled path walk, deliberately NOT a heavy JSONPath
//! crate, so it stays Pi-safe (this module is unconditionally compiled in eg-core).
//!
//! Supported deep-access subset (what the engine's JSON operators lower onto):
//!   * `$`            — the document root
//!   * `.key`         — object field access
//!   * `['key']` / `["key"]` — quoted object field (keys with dots/brackets)
//!   * `[n]`          — array index
//!   * `[*]` / `.*`   — wildcard (every immediate child of an object or array)
//!
//! Plus the three predicate primitives the `Pred::JsonPath` filter (eg-plan) and the
//! eg-core inverted path-index build on: existence, equality, and Postgres-`@>` JSON
//! containment.

use serde_json::Value;

/// One parsed step of a JSONPath (CONCEPT:EG-084).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Segment {
    /// `.key` / `['key']` — descend into an object field.
    Key(String),
    /// `[n]` — descend into an array element by index.
    Index(usize),
    /// `[*]` / `.*` — every immediate child of an object or array.
    Wildcard,
}

/// Parse a JSONPath string into its [`Segment`]s (CONCEPT:EG-084). A leading `$` is
/// optional; `$` alone (or the empty string) is the root and yields no segments.
/// Returns `None` on a malformed path (the caller then treats it as "matches nothing").
pub fn parse_path(path: &str) -> Option<Vec<Segment>> {
    let mut chars = path.trim().chars().peekable();
    if chars.peek() == Some(&'$') {
        chars.next();
    }
    let mut segs = Vec::new();
    while let Some(&c) = chars.peek() {
        match c {
            '.' => {
                chars.next();
                if chars.peek() == Some(&'*') {
                    chars.next();
                    segs.push(Segment::Wildcard);
                } else {
                    let key = take_bare_key(&mut chars);
                    if key.is_empty() {
                        return None;
                    }
                    segs.push(Segment::Key(key));
                }
            }
            '[' => {
                chars.next();
                match chars.peek() {
                    Some('*') => {
                        chars.next();
                        if chars.next() != Some(']') {
                            return None;
                        }
                        segs.push(Segment::Wildcard);
                    }
                    Some('\'') | Some('"') => {
                        let quote = chars.next()?;
                        let mut key = String::new();
                        loop {
                            match chars.next() {
                                Some(c) if c == quote => break,
                                Some(c) => key.push(c),
                                None => return None,
                            }
                        }
                        if chars.next() != Some(']') {
                            return None;
                        }
                        segs.push(Segment::Key(key));
                    }
                    Some(c) if c.is_ascii_digit() => {
                        let mut num = String::new();
                        while let Some(&c) = chars.peek() {
                            if c.is_ascii_digit() {
                                num.push(c);
                                chars.next();
                            } else {
                                break;
                            }
                        }
                        if chars.next() != Some(']') {
                            return None;
                        }
                        segs.push(Segment::Index(num.parse().ok()?));
                    }
                    _ => return None,
                }
            }
            _ => {
                // A bare leading key without a `.` (e.g. `a.b` with no `$`); only valid
                // at the very start of the path.
                if !segs.is_empty() {
                    return None;
                }
                let key = take_bare_key(&mut chars);
                if key.is_empty() {
                    return None;
                }
                segs.push(Segment::Key(key));
            }
        }
    }
    Some(segs)
}

/// Consume a run of key characters up to the next `.` or `[`.
fn take_bare_key(chars: &mut std::iter::Peekable<std::str::Chars<'_>>) -> String {
    let mut key = String::new();
    while let Some(&c) = chars.peek() {
        if c == '.' || c == '[' {
            break;
        }
        key.push(c);
        chars.next();
    }
    key
}

/// Evaluate parsed `segments` against `root`, returning every matched value
/// (CONCEPT:EG-084). A wildcard fans out, so more than one value can match; a missing
/// key/index simply drops that branch (⇒ empty result = "path absent").
pub fn eval<'a>(root: &'a Value, segments: &[Segment]) -> Vec<&'a Value> {
    let mut cur = vec![root];
    for seg in segments {
        let mut next = Vec::new();
        for v in cur {
            match seg {
                Segment::Key(k) => {
                    if let Some(child) = v.get(k) {
                        next.push(child);
                    }
                }
                Segment::Index(i) => {
                    if let Some(child) = v.get(*i) {
                        next.push(child);
                    }
                }
                Segment::Wildcard => match v {
                    Value::Object(m) => next.extend(m.values()),
                    Value::Array(a) => next.extend(a.iter()),
                    _ => {}
                },
            }
        }
        cur = next;
    }
    cur
}

/// Parse `path` and evaluate it against `root` in one call (CONCEPT:EG-084). A
/// malformed path yields an empty result (matches nothing).
pub fn eval_path<'a>(root: &'a Value, path: &str) -> Vec<&'a Value> {
    match parse_path(path) {
        Some(segs) => eval(root, &segs),
        None => Vec::new(),
    }
}

/// Does `path` resolve to at least one value in `root`? (existence — the `@?` /
/// `jsonb_path_query` existence primitive, CONCEPT:EG-084).
pub fn path_exists(root: &Value, path: &str) -> bool {
    !eval_path(root, path).is_empty()
}

/// Does any value at `path` equal `wanted`? (CONCEPT:EG-084). Direct JSON equality,
/// with a Postgres-`->>`-style fallback: when `wanted` is a JSON string, a matched
/// scalar also compares equal if its canonical text form equals the string (so
/// `props->>'age' = '30'` matches a numeric `30`).
pub fn path_eq(root: &Value, path: &str, wanted: &Value) -> bool {
    let matches = eval_path(root, path);
    matches.iter().any(|v| {
        if *v == wanted {
            return true;
        }
        if let Value::String(s) = wanted {
            return canonical_scalar(v).as_deref() == Some(s.as_str());
        }
        false
    })
}

/// Does the value at `path` CONTAIN `needle` per Postgres `@>` JSON containment
/// (CONCEPT:EG-084)? When `path` is the root (`$`) this is `props @> needle`. If the
/// path fans out (wildcard), containment holds if ANY matched value contains `needle`.
pub fn path_contains(root: &Value, path: &str, needle: &Value) -> bool {
    eval_path(root, path)
        .iter()
        .any(|v| json_contains(v, needle))
}

/// Postgres `@>` containment (CONCEPT:EG-084): does `haystack` contain `needle`?
///   * object ⊇ object — every needle key present with a contained value;
///   * array ⊇ array   — every needle element is contained by some haystack element;
///   * array ⊇ scalar  — the scalar is contained by some element (`'[1,2]' @> '2'`);
///   * scalar          — plain equality.
pub fn json_contains(haystack: &Value, needle: &Value) -> bool {
    match (haystack, needle) {
        (Value::Object(h), Value::Object(n)) => n
            .iter()
            .all(|(k, nv)| h.get(k).is_some_and(|hv| json_contains(hv, nv))),
        (Value::Array(h), Value::Array(n)) => {
            n.iter().all(|ne| h.iter().any(|he| json_contains(he, ne)))
        }
        (Value::Array(h), ne) => h.iter().any(|he| json_contains(he, ne)),
        (a, b) => a == b,
    }
}

/// Canonical string form of a SCALAR leaf value, used as the inverted path-index key
/// (CONCEPT:EG-084). Strings index under their text, bools/numbers under their
/// `to_string()`. Arrays/objects/null are NOT scalar-indexable (return `None`).
/// Mirrors `graph::GraphCore::property_value_key` so a JSONPath equality index agrees
/// with the flat property index.
pub fn canonical_scalar(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.clone()),
        Value::Bool(b) => Some(b.to_string()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn eg084_parse_dotted_indexed_wildcard_paths() {
        assert_eq!(parse_path("$"), Some(vec![]));
        assert_eq!(
            parse_path("$.a.b"),
            Some(vec![Segment::Key("a".into()), Segment::Key("b".into())])
        );
        assert_eq!(
            parse_path("$.a[0]"),
            Some(vec![Segment::Key("a".into()), Segment::Index(0)])
        );
        assert_eq!(
            parse_path("$.a[*]"),
            Some(vec![Segment::Key("a".into()), Segment::Wildcard])
        );
        assert_eq!(
            parse_path("$.a.*"),
            Some(vec![Segment::Key("a".into()), Segment::Wildcard])
        );
        // leading `$` optional + quoted key with a dot in it
        assert_eq!(parse_path("a"), Some(vec![Segment::Key("a".into())]));
        assert_eq!(
            parse_path("$['a.b']"),
            Some(vec![Segment::Key("a.b".into())])
        );
    }

    #[test]
    fn eg084_parse_rejects_malformed() {
        assert_eq!(parse_path("$.a["), None);
        assert_eq!(parse_path("$.a[x]"), None);
        assert_eq!(parse_path("$..a"), None);
    }

    #[test]
    fn eg084_eval_deep_and_indexed() {
        let doc = json!({"a": {"b": 7}, "xs": [10, 20, 30]});
        assert_eq!(eval_path(&doc, "$.a.b"), vec![&json!(7)]);
        assert_eq!(eval_path(&doc, "$.xs[1]"), vec![&json!(20)]);
        assert!(eval_path(&doc, "$.a.missing").is_empty());
    }

    #[test]
    fn eg084_eval_wildcard_fans_out() {
        let doc = json!({"tags": ["x", "y", "z"], "m": {"p": 1, "q": 2}});
        let got = eval_path(&doc, "$.tags[*]");
        assert_eq!(got.len(), 3);
        let got_obj = eval_path(&doc, "$.m.*");
        assert_eq!(got_obj.len(), 2);
    }

    #[test]
    fn eg084_path_exists_and_eq() {
        let doc = json!({"user": {"age": 30, "name": "ada"}});
        assert!(path_exists(&doc, "$.user.age"));
        assert!(!path_exists(&doc, "$.user.email"));
        // direct + ->>-style string coercion
        assert!(path_eq(&doc, "$.user.age", &json!(30)));
        assert!(path_eq(&doc, "$.user.age", &json!("30")));
        assert!(path_eq(&doc, "$.user.name", &json!("ada")));
        assert!(!path_eq(&doc, "$.user.name", &json!("bob")));
    }

    #[test]
    fn eg084_json_containment_semantics() {
        // object ⊇ object
        assert!(json_contains(
            &json!({"k": "v", "n": 1}),
            &json!({"k": "v"})
        ));
        assert!(!json_contains(&json!({"k": "v"}), &json!({"k": "x"})));
        // array ⊇ array + array ⊇ scalar
        assert!(json_contains(&json!([1, 2, 3]), &json!([2, 3])));
        assert!(json_contains(&json!([1, 2, 3]), &json!(2)));
        assert!(!json_contains(&json!([1, 2, 3]), &json!(9)));
        // nested object containment at the root
        let doc = json!({"meta": {"lang": "rust", "year": 2024}});
        assert!(path_contains(&doc, "$", &json!({"meta": {"lang": "rust"}})));
        assert!(!path_contains(&doc, "$", &json!({"meta": {"lang": "go"}})));
    }
}
