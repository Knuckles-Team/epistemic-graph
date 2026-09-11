//! Dependency-free JSON parsing and deterministic emission for pipeline values.

use std::collections::BTreeMap;

use super::PipeValue;

/// Render a pipeline value as compact JSON with deterministic object ordering.
pub(super) fn to_json(value: &PipeValue) -> String {
    let mut out = String::new();
    write_value(value, &mut out);
    out
}

fn write_value(value: &PipeValue, out: &mut String) {
    match value {
        PipeValue::Null => out.push_str("null"),
        PipeValue::Bool(true) => out.push_str("true"),
        PipeValue::Bool(false) => out.push_str("false"),
        PipeValue::I64(number) => out.push_str(&number.to_string()),
        PipeValue::F64(number) => out.push_str(&number.to_string()),
        PipeValue::Str(text) => write_json_str(text, out),
        PipeValue::Array(_) | PipeValue::Object(_) => write_container(value, out),
    }
}

fn write_container(value: &PipeValue, out: &mut String) {
    match value {
        PipeValue::Array(items) => write_array(items, out),
        PipeValue::Object(map) => write_object(map, out),
        _ => unreachable!("scalar value routed to JSON container emission"),
    }
}

fn write_array(items: &[PipeValue], out: &mut String) {
    out.push('[');
    for (index, item) in items.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        write_value(item, out);
    }
    out.push(']');
}

fn write_object(map: &BTreeMap<String, PipeValue>, out: &mut String) {
    out.push('{');
    for (index, (key, value)) in map.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        write_json_str(key, out);
        out.push(':');
        write_value(value, out);
    }
    out.push('}');
}

// ---------------------------------------------------------------------------
// minimal hand-rolled JSON reader (no serde_json — the zero-new-dep contract)
// ---------------------------------------------------------------------------

/// Parse a JSON document into a [`PipeValue`] (CONCEPT:EG-KG.enrichment.cross-modal-enrichment-hook). A tiny
/// recursive-descent reader over `&[u8]` covering objects, arrays, strings (with the
/// standard escapes), numbers (int vs float), and `true`/`false`/`null`. Deliberately
/// dependency-free to hold the Pi contract the rest of eg-tsdb keeps.
pub(super) fn parse_json_value(s: &str) -> Result<PipeValue, String> {
    let bytes = s.as_bytes();
    let mut p = JsonParser { b: bytes, i: 0 };
    p.skip_ws();
    let v = p.parse_value()?;
    p.skip_ws();
    if p.i != bytes.len() {
        return Err(format!("trailing bytes at offset {}", p.i));
    }
    Ok(v)
}

struct JsonParser<'a> {
    b: &'a [u8],
    i: usize,
}

impl JsonParser<'_> {
    fn skip_ws(&mut self) {
        while self.i < self.b.len() && matches!(self.b[self.i], b' ' | b'\t' | b'\n' | b'\r') {
            self.i += 1;
        }
    }

    fn peek(&self) -> Option<u8> {
        self.b.get(self.i).copied()
    }

    fn parse_value(&mut self) -> Result<PipeValue, String> {
        self.skip_ws();
        match self.peek().ok_or("unexpected end of JSON")? {
            b'{' => self.parse_object(),
            b'[' => self.parse_array(),
            b'"' => Ok(PipeValue::Str(self.parse_string()?)),
            b't' | b'f' => self.parse_bool(),
            b'n' => self.parse_null(),
            _ => self.parse_number(),
        }
    }

    fn parse_object(&mut self) -> Result<PipeValue, String> {
        self.i += 1; // '{'
        let mut map = BTreeMap::new();
        self.skip_ws();
        if self.peek() == Some(b'}') {
            self.i += 1;
            return Ok(PipeValue::Object(map));
        }
        loop {
            self.skip_ws();
            if self.peek() != Some(b'"') {
                return Err("expected string key in object".into());
            }
            let key = self.parse_string()?;
            self.skip_ws();
            if self.peek() != Some(b':') {
                return Err("expected ':' after object key".into());
            }
            self.i += 1;
            let val = self.parse_value()?;
            map.insert(key, val);
            self.skip_ws();
            match self.peek() {
                Some(b',') => {
                    self.i += 1;
                }
                Some(b'}') => {
                    self.i += 1;
                    break;
                }
                _ => return Err("expected ',' or '}' in object".into()),
            }
        }
        Ok(PipeValue::Object(map))
    }

    fn parse_array(&mut self) -> Result<PipeValue, String> {
        self.i += 1; // '['
        let mut items = Vec::new();
        self.skip_ws();
        if self.peek() == Some(b']') {
            self.i += 1;
            return Ok(PipeValue::Array(items));
        }
        loop {
            let val = self.parse_value()?;
            items.push(val);
            self.skip_ws();
            match self.peek() {
                Some(b',') => {
                    self.i += 1;
                }
                Some(b']') => {
                    self.i += 1;
                    break;
                }
                _ => return Err("expected ',' or ']' in array".into()),
            }
        }
        Ok(PipeValue::Array(items))
    }

    fn parse_string(&mut self) -> Result<String, String> {
        self.i += 1; // opening quote
        let mut out = String::new();
        while let Some(c) = self.peek() {
            self.i += 1;
            match c {
                b'"' => return Ok(out),
                b'\\' => self.parse_escape(&mut out)?,
                _ => {
                    // Copy the raw UTF-8 byte(s). `c` is one byte of a (possibly multi-
                    // byte) char; push it through a 1-byte buffer-safe path.
                    out.push(c as char);
                    // Fix up multi-byte UTF-8: if the byte was a lead byte, the naive
                    // `c as char` above is wrong, so handle >=0x80 via the source slice.
                    if c >= 0x80 {
                        out.pop();
                        let start = self.i - 1;
                        let width = utf8_width(c);
                        let end = (start + width).min(self.b.len());
                        let chunk = std::str::from_utf8(&self.b[start..end])
                            .map_err(|_| "invalid UTF-8 in string")?;
                        out.push_str(chunk);
                        self.i = end;
                    }
                }
            }
        }
        Err("unterminated string".into())
    }

    fn parse_escape(&mut self, out: &mut String) -> Result<(), String> {
        let esc = self.peek().ok_or("unterminated escape")?;
        self.i += 1;
        match esc {
            b'"' | b'\\' | b'/' => out.push(esc as char),
            b'n' => out.push('\n'),
            b't' => out.push('\t'),
            b'r' => out.push('\r'),
            b'b' => out.push('\u{0008}'),
            b'f' => out.push('\u{000C}'),
            b'u' => {
                let hex = self
                    .b
                    .get(self.i..self.i + 4)
                    .ok_or("truncated \\u escape")?;
                let code =
                    u32::from_str_radix(std::str::from_utf8(hex).map_err(|_| "bad \\u hex")?, 16)
                        .map_err(|_| "bad \\u hex")?;
                self.i += 4;
                out.push(char::from_u32(code).unwrap_or('\u{FFFD}'));
            }
            other => return Err(format!("bad escape '\\{}'", other as char)),
        }
        Ok(())
    }

    fn parse_bool(&mut self) -> Result<PipeValue, String> {
        if self.b[self.i..].starts_with(b"true") {
            self.i += 4;
            Ok(PipeValue::Bool(true))
        } else if self.b[self.i..].starts_with(b"false") {
            self.i += 5;
            Ok(PipeValue::Bool(false))
        } else {
            Err("invalid literal (expected true/false)".into())
        }
    }

    fn parse_null(&mut self) -> Result<PipeValue, String> {
        if self.b[self.i..].starts_with(b"null") {
            self.i += 4;
            Ok(PipeValue::Null)
        } else {
            Err("invalid literal (expected null)".into())
        }
    }

    fn parse_number(&mut self) -> Result<PipeValue, String> {
        let start = self.i;
        let mut is_float = false;
        while let Some(c) = self.peek() {
            match c {
                b'0'..=b'9' | b'-' | b'+' => self.i += 1,
                b'.' | b'e' | b'E' => {
                    is_float = true;
                    self.i += 1;
                }
                _ => break,
            }
        }
        let tok = std::str::from_utf8(&self.b[start..self.i]).map_err(|_| "bad number")?;
        if tok.is_empty() {
            return Err(format!("unexpected byte at offset {start}"));
        }
        if is_float {
            tok.parse::<f64>()
                .map(PipeValue::F64)
                .map_err(|_| format!("bad float '{tok}'"))
        } else {
            match tok.parse::<i64>() {
                Ok(n) => Ok(PipeValue::I64(n)),
                Err(_) => tok
                    .parse::<f64>()
                    .map(PipeValue::F64)
                    .map_err(|_| format!("bad number '{tok}'")),
            }
        }
    }
}

/// UTF-8 byte-width from a lead byte (CONCEPT:EG-KG.enrichment.cross-modal-enrichment-hook JSON reader helper).
fn utf8_width(lead: u8) -> usize {
    if lead < 0x80 {
        1
    } else if lead >> 5 == 0b110 {
        2
    } else if lead >> 4 == 0b1110 {
        3
    } else if lead >> 3 == 0b11110 {
        4
    } else {
        1
    }
}

/// Write a JSON string literal (quoted + escaped) into `out` (CONCEPT:EG-KG.enrichment.cross-modal-enrichment-hook).
fn write_json_str(s: &str, out: &mut String) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\t' => out.push_str("\\t"),
            '\r' => out.push_str("\\r"),
            '\u{0008}' => out.push_str("\\b"),
            '\u{000C}' => out.push_str("\\f"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
}
