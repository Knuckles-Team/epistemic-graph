//! The hand-written scanner for `@name(args)` federation directives in SDL text
//! (CONCEPT:EG-KG.query.apollo-federation-subgraph).
//!
//! It reads one segment at a time — a type header or a single field line — because that is
//! the granularity the SDL flattener hands out. Only the three arguments federation
//! actually carries are understood (`fields:`, `from:`, and `@key`'s bare `resolvable:`);
//! any other argument is REJECTED rather than dropped, so an unsupported directive
//! argument can never be silently ignored.

use std::collections::BTreeMap;

use super::ScannedDir;

/// Scan `@name(args)` federation directives out of one SDL text segment (a type header or
/// a field line). String args are captured for `fields:`/`from:`; the `@key`
/// `resolvable:` flag is read as a bare `true`/`false`.
pub(super) fn scan_directives(seg: &str) -> Result<Vec<ScannedDir>, String> {
    let bytes = seg.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        if i == bytes.len() {
            break;
        }
        if bytes[i] != b'@' {
            return Err(format!(
                "GraphQL federation SDL: unexpected text in directive list `{}`",
                &seg[i..]
            ));
        }
        let (name, after_name) = read_directive_name(seg, i + 1)?;
        let mut open = after_name;
        while open < bytes.len() && bytes[open].is_ascii_whitespace() {
            open += 1;
        }
        if open < bytes.len() && bytes[open] == b'(' {
            let close = directive_args_end(bytes, open + 1)
                .ok_or_else(|| format!("GraphQL federation SDL: unterminated @{name} arguments"))?;
            out.push(scan_directive_args(name, &seg[open + 1..close])?);
            i = close + 1;
        } else {
            out.push(ScannedDir {
                name,
                fields: None,
                from: None,
                resolvable: None,
            });
            i = after_name;
        }
    }
    Ok(out)
}

/// Read the directive name starting at `start` (just past the `@`), returning it and the
/// index just past it. A GraphQL directive name is `[A-Za-z_][A-Za-z0-9_]*` and may not be
/// in the reserved `__` namespace.
fn read_directive_name(seg: &str, start: usize) -> Result<(String, usize), String> {
    let bytes = seg.as_bytes();
    let mut j = start;
    while j < bytes.len() && (bytes[j].is_ascii_alphanumeric() || bytes[j] == b'_') {
        j += 1;
    }
    let name = seg[start..j].to_string();
    if name.is_empty()
        || !bytes[start].is_ascii_alphabetic() && bytes[start] != b'_'
        || name.starts_with("__")
    {
        return Err("GraphQL federation SDL: malformed directive name".to_string());
    }
    Ok((name, j))
}

/// The index of the `)` closing an argument list whose contents start at `open`,
/// respecting quoted strings and their backslash escapes. `None` when the input runs out
/// or ends inside a string, both of which are an unterminated argument list.
fn directive_args_end(bytes: &[u8], open: usize) -> Option<usize> {
    let mut cursor = open;
    let mut quoted = false;
    let mut escaped = false;
    while cursor < bytes.len() {
        match bytes[cursor] {
            b'\\' if quoted => escaped = !escaped,
            b'"' if !escaped => quoted = !quoted,
            b')' if !quoted => break,
            _ => escaped = false,
        }
        cursor += 1;
    }
    if cursor == bytes.len() || quoted {
        None
    } else {
        Some(cursor)
    }
}

/// Parse a directive's argument text into the three federation arguments this scanner
/// understands — `fields:`, `from:` and `@key`'s bare `resolvable:` flag — and reject any
/// other argument rather than silently dropping it.
fn scan_directive_args(name: String, raw_args: &str) -> Result<ScannedDir, String> {
    if raw_args.trim().is_empty() {
        return Err(format!(
            "GraphQL federation SDL: @{name} has an empty argument list"
        ));
    }
    let mut args = parse_directive_args(raw_args)?;
    let fields = args
        .remove("fields")
        .map(|value| parse_quoted_arg("fields", &value))
        .transpose()?;
    let from = args
        .remove("from")
        .map(|value| parse_quoted_arg("from", &value))
        .transpose()?;
    let resolvable = args
        .remove("resolvable")
        .map(|value| match value.as_str() {
            "true" => Ok(true),
            "false" => Ok(false),
            _ => Err("GraphQL federation SDL: `resolvable` must be true or false".to_string()),
        })
        .transpose()?;
    if let Some(unexpected) = args.keys().next() {
        return Err(format!(
            "GraphQL federation SDL: unsupported directive argument `{unexpected}`"
        ));
    }
    Ok(ScannedDir {
        name,
        fields,
        from,
        resolvable,
    })
}

fn parse_directive_args(args: &str) -> Result<BTreeMap<String, String>, String> {
    let mut parsed = BTreeMap::new();
    let mut start = 0usize;
    let mut quoted = false;
    let mut escaped = false;
    for (index, byte) in args.bytes().enumerate() {
        match byte {
            b'\\' if quoted => escaped = !escaped,
            b'"' if !escaped => quoted = !quoted,
            b',' if !quoted => {
                insert_directive_arg(&mut parsed, &args[start..index])?;
                start = index + 1;
            }
            _ => escaped = false,
        }
    }
    if quoted {
        return Err("GraphQL federation SDL: unterminated quoted argument".to_string());
    }
    if !args.trim().is_empty() {
        insert_directive_arg(&mut parsed, &args[start..])?;
    }
    Ok(parsed)
}

fn insert_directive_arg(parsed: &mut BTreeMap<String, String>, raw: &str) -> Result<(), String> {
    let (key, value) = raw
        .split_once(':')
        .ok_or_else(|| "GraphQL federation SDL: malformed directive argument".to_string())?;
    let key = key.trim();
    let value = value.trim();
    if key.is_empty() || value.is_empty() {
        return Err("GraphQL federation SDL: empty directive argument".to_string());
    }
    if parsed.insert(key.to_string(), value.to_string()).is_some() {
        return Err(format!(
            "GraphQL federation SDL: duplicate directive argument `{key}`"
        ));
    }
    Ok(())
}

fn parse_quoted_arg(key: &str, value: &str) -> Result<String, String> {
    if value.len() < 2
        || !value.starts_with('"')
        || !value.ends_with('"')
        || value[1..value.len() - 1]
            .chars()
            .any(|ch| matches!(ch, '"' | '\\'))
    {
        return Err(format!(
            "GraphQL federation SDL: `{key}` must be one unescaped quoted string"
        ));
    }
    let value = &value[1..value.len() - 1];
    if value.trim().is_empty() {
        return Err(format!("GraphQL federation SDL: `{key}` must not be empty"));
    }
    Ok(value.to_string())
}
