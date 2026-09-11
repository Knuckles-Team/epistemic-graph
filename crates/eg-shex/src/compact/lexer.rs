//! Lexical stages for the bounded ShExC grammar.
//!
//! The scanner keeps the source cursor as a character offset.  That is the
//! position convention used by the original compact parser's diagnostics and
//! is deliberately retained while the token classes are handled by named
//! grammar stages.

use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq)]
pub(super) enum Tok {
    /// A fully-resolved absolute IRI (from `<iri>`, prefix-expansion, or `a`).
    Iri(String),
    /// A bare word with no `:` — a keyword candidate (`CLOSED`, `AND`, `IRI`, …).
    Word(String),
    Str(String, StrSuffix),
    /// A bare numeric literal's lexical form (facet arguments).
    Num(String),
    /// `{m,n}` / `{m,}` / `{m}` — lexed as one token because a bare `{` alone
    /// starts a shape definition (disambiguated by "digit right after `{`").
    RepeatRange(u64, Option<u64>),
    Punct(char),
}

#[derive(Debug, Clone, PartialEq)]
pub(super) enum StrSuffix {
    None,
    Datatype(String),
    Lang(String),
}

/// Tokenize a ShExC document while expanding prefixes and the active base.
pub(super) fn lex(text: &str) -> Result<Vec<Tok>, String> {
    CompactLexer::new(text).tokenize()
}

struct CompactLexer {
    chars: Vec<char>,
    cursor: usize,
    prefixes: HashMap<String, String>,
    base: Option<String>,
    tokens: Vec<Tok>,
}

impl CompactLexer {
    fn new(text: &str) -> Self {
        Self {
            chars: text.chars().collect(),
            cursor: 0,
            prefixes: HashMap::new(),
            base: None,
            tokens: Vec::new(),
        }
    }

    fn tokenize(mut self) -> Result<Vec<Tok>, String> {
        while !self.at_end() {
            if self.scan_trivia() {
                continue;
            }
            if self.scan_delimited()? || self.scan_punctuation() || self.scan_number() {
                continue;
            }
            if self.scan_name()? {
                continue;
            }
            return Err(format!(
                "ShExC: unexpected character `{}` at offset {}",
                self.current().unwrap_or('\0'),
                self.cursor
            ));
        }
        self.tokens.push(Tok::Punct('\0'));
        Ok(self.tokens)
    }

    fn at_end(&self) -> bool {
        self.cursor >= self.chars.len()
    }

    fn current(&self) -> Option<char> {
        self.chars.get(self.cursor).copied()
    }

    fn scan_trivia(&mut self) -> bool {
        if self.current().is_some_and(|c| c.is_whitespace()) {
            self.cursor += 1;
            return true;
        }
        if self.current() == Some('#') {
            while self.current().is_some_and(|c| c != '\n') {
                self.cursor += 1;
            }
            return true;
        }
        false
    }

    fn scan_delimited(&mut self) -> Result<bool, String> {
        match self.current() {
            Some('%') => Err("ShExC: semantic actions (%...%) are not supported".to_string()),
            Some('<') => {
                let (iri, next) = read_iriref(&self.chars, self.cursor)?;
                self.cursor = next;
                self.tokens
                    .push(Tok::Iri(resolve_base(&iri, self.base.as_deref())));
                Ok(true)
            }
            Some(quote @ ('"' | '\'')) => {
                let (value, next) = read_string(&self.chars, self.cursor, quote)?;
                self.cursor = next;
                let (suffix, next) = read_str_suffix(&self.chars, self.cursor, &self.prefixes)?;
                self.cursor = next;
                self.tokens.push(Tok::Str(value, suffix));
                Ok(true)
            }
            Some('{') => {
                if let Some((min, max, next)) = try_read_repeat_range(&self.chars, self.cursor) {
                    self.tokens.push(Tok::RepeatRange(min, max));
                    self.cursor = next;
                } else {
                    self.tokens.push(Tok::Punct('{'));
                    self.cursor += 1;
                }
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    fn scan_punctuation(&mut self) -> bool {
        let Some(c) = self.current() else {
            return false;
        };
        if !"}()[]|;,.?*+^$~=@".contains(c) {
            return false;
        }
        self.tokens.push(Tok::Punct(c));
        self.cursor += 1;
        true
    }

    fn scan_number(&mut self) -> bool {
        let Some(c) = self.current() else {
            return false;
        };
        if !(c.is_ascii_digit()
            || ((c == '-' || c == '+') && peek_digit(&self.chars, self.cursor + 1)))
        {
            return false;
        }
        let (number, next) = read_number(&self.chars, self.cursor);
        self.cursor = next;
        self.tokens.push(Tok::Num(number));
        true
    }

    fn scan_name(&mut self) -> Result<bool, String> {
        let Some(c) = self.current() else {
            return Ok(false);
        };
        if !is_pn_char_start(c) {
            return Ok(false);
        }
        let (word, next) = read_pn(&self.chars, self.cursor);
        self.cursor = next;
        self.scan_name_value(word)
    }

    fn scan_name_value(&mut self, word: String) -> Result<bool, String> {
        if let Some(local) = word.strip_prefix(':') {
            let namespace = self.prefixes.get("").cloned().unwrap_or_default();
            self.tokens.push(Tok::Iri(format!("{namespace}{local}")));
            return Ok(true);
        }
        if let Some((prefix, local)) = word.split_once(':') {
            let namespace = self.prefixes.get(prefix).ok_or_else(|| {
                format!("ShExC: undeclared prefix `{prefix}:` (missing a PREFIX directive)")
            })?;
            self.tokens.push(Tok::Iri(format!("{namespace}{local}")));
            return Ok(true);
        }
        match word.as_str() {
            "PREFIX" => {
                let (prefix, next) =
                    read_pname_ns(&self.chars, next_non_ws(&self.chars, self.cursor))?;
                let (iri, next) = expect_iriref(&self.chars, next_non_ws(&self.chars, next))?;
                self.prefixes
                    .insert(prefix, resolve_base(&iri, self.base.as_deref()));
                self.cursor = next;
                Ok(true)
            }
            "BASE" => {
                let (iri, next) =
                    expect_iriref(&self.chars, next_non_ws(&self.chars, self.cursor))?;
                self.base = Some(resolve_base(&iri, self.base.as_deref()));
                self.cursor = next;
                Ok(true)
            }
            _ => {
                self.tokens.push(Tok::Word(word));
                Ok(true)
            }
        }
    }
}

fn next_non_ws(chars: &[char], mut i: usize) -> usize {
    while i < chars.len() && chars[i].is_whitespace() {
        i += 1;
    }
    i
}

fn peek_digit(chars: &[char], i: usize) -> bool {
    chars.get(i).is_some_and(|c| c.is_ascii_digit())
}

fn resolve_base(iri: &str, base: Option<&str>) -> String {
    if iri.contains("://") || iri.starts_with("urn:") || base.is_none() || iri.is_empty() {
        return iri.to_string();
    }
    format!("{}{iri}", base.unwrap_or_default())
}

fn read_iriref(chars: &[char], start: usize) -> Result<(String, usize), String> {
    debug_assert_eq!(chars[start], '<');
    let mut i = start + 1;
    let mut value = String::new();
    while i < chars.len() {
        match chars[i] {
            '>' => return Ok((value, i + 1)),
            '\\' if i + 1 < chars.len() => {
                value.push(chars[i + 1]);
                i += 2;
            }
            c => {
                value.push(c);
                i += 1;
            }
        }
    }
    Err("ShExC: unterminated IRIREF (missing `>`)".to_string())
}

fn expect_iriref(chars: &[char], i: usize) -> Result<(String, usize), String> {
    if chars.get(i) != Some(&'<') {
        return Err("ShExC: expected an IRIREF (`<...>`)".to_string());
    }
    read_iriref(chars, i)
}

/// A `PNAME_NS` for a `PREFIX` directive: `word:` (the prefix name, possibly empty).
fn read_pname_ns(chars: &[char], start: usize) -> Result<(String, usize), String> {
    let mut i = start;
    let mut value = String::new();
    while i < chars.len() && chars[i] != ':' && !chars[i].is_whitespace() {
        value.push(chars[i]);
        i += 1;
    }
    if chars.get(i) != Some(&':') {
        return Err("ShExC: expected `prefix:` in a PREFIX directive".to_string());
    }
    Ok((value, i + 1))
}

fn read_string(chars: &[char], start: usize, quote: char) -> Result<(String, usize), String> {
    let triple = is_triple_quote(chars, start, quote);
    let mut i = start + if triple { 3 } else { 1 };
    let mut value = String::new();
    loop {
        if i >= chars.len() {
            return Err("ShExC: unterminated string literal".to_string());
        }
        if consume_escape(chars, &mut i, &mut value) {
            continue;
        }
        if let Some(next) = closing_quote(chars, i, quote, triple) {
            return Ok((value, next));
        }
        value.push(chars[i]);
        i += 1;
    }
}

fn is_triple_quote(chars: &[char], start: usize, quote: char) -> bool {
    chars.get(start + 1) == Some(&quote) && chars.get(start + 2) == Some(&quote)
}

fn consume_escape(chars: &[char], cursor: &mut usize, value: &mut String) -> bool {
    if chars.get(*cursor) != Some(&'\\') || chars.get(*cursor + 1).is_none() {
        return false;
    }
    value.push(match chars[*cursor + 1] {
        'n' => '\n',
        't' => '\t',
        'r' => '\r',
        other => other,
    });
    *cursor += 2;
    true
}

fn closing_quote(chars: &[char], cursor: usize, quote: char, triple: bool) -> Option<usize> {
    if chars.get(cursor) != Some(&quote) {
        return None;
    }
    if !triple {
        return Some(cursor + 1);
    }
    (chars.get(cursor + 1) == Some(&quote) && chars.get(cursor + 2) == Some(&quote))
        .then_some(cursor + 3)
}

fn read_str_suffix(
    chars: &[char],
    i: usize,
    prefixes: &HashMap<String, String>,
) -> Result<(StrSuffix, usize), String> {
    if chars.get(i) == Some(&'^') && chars.get(i + 1) == Some(&'^') {
        let mut j = i + 2;
        if chars.get(j) == Some(&'<') {
            let (iri, next) = read_iriref(chars, j)?;
            return Ok((StrSuffix::Datatype(iri), next));
        }
        let (word, next) = read_pn(chars, j);
        j = next;
        let (prefix, local) = word.split_once(':').ok_or_else(|| {
            "ShExC: expected a datatype IRI or prefixed name after `^^`".to_string()
        })?;
        let namespace = prefixes
            .get(prefix)
            .ok_or_else(|| format!("ShExC: undeclared prefix `{prefix}:` in a datatype"))?;
        return Ok((StrSuffix::Datatype(format!("{namespace}{local}")), j));
    }
    if chars.get(i) == Some(&'@') {
        let mut j = i + 1;
        let mut tag = String::new();
        while j < chars.len() && (chars[j].is_alphanumeric() || chars[j] == '-') {
            tag.push(chars[j]);
            j += 1;
        }
        if !tag.is_empty() {
            return Ok((StrSuffix::Lang(tag), j));
        }
    }
    Ok((StrSuffix::None, i))
}

fn read_number(chars: &[char], start: usize) -> (String, usize) {
    let mut i = start;
    let mut value = String::new();
    if matches!(chars[i], '-' | '+') {
        value.push(chars[i]);
        i += 1;
    }
    while i < chars.len() && (chars[i].is_ascii_digit() || chars[i] == '.') {
        value.push(chars[i]);
        i += 1;
    }
    let (exponent, next) = read_exponent(chars, i);
    value.push_str(&exponent);
    (value, next)
}

fn read_exponent(chars: &[char], start: usize) -> (String, usize) {
    let Some(marker @ ('e' | 'E')) = chars.get(start).copied() else {
        return (String::new(), start);
    };
    let mut i = start + 1;
    let mut exponent = marker.to_string();
    if chars.get(i).is_some_and(|c| matches!(c, '+' | '-')) {
        exponent.push(chars[i]);
        i += 1;
    }
    if !peek_digit(chars, i) {
        return (String::new(), start);
    }
    while i < chars.len() && chars[i].is_ascii_digit() {
        exponent.push(chars[i]);
        i += 1;
    }
    (exponent, i)
}

fn is_pn_char_start(c: char) -> bool {
    c.is_alphabetic() || c == '_' || c == ':'
}

fn is_pn_char(c: char) -> bool {
    c.is_alphanumeric() || matches!(c, '_' | '-' | '.' | ':')
}

/// A bare word or `prefix:local` / `prefix:` / `:local` token (a keyword, a
/// directive name, or a prefixed name — disambiguated by the caller).
fn read_pn(chars: &[char], start: usize) -> (String, usize) {
    let mut i = start;
    let mut value = String::new();
    while i < chars.len() && is_pn_char(chars[i]) {
        value.push(chars[i]);
        i += 1;
    }
    // A trailing `.` is very likely end-of-statement punctuation, not part of a
    // local name (ShExC local names don't typically end in `.`); back off ONE
    // trailing `.` so `ex:Foo .` and `ex:Foo.` (no space) both lex sanely.
    if value.ends_with('.') && !value.ends_with("..") {
        value.pop();
        i -= 1;
    }
    (value, i)
}

/// `{` immediately followed (no whitespace) by a digit or `,` is a repeat-range
/// cardinality, not a shape-definition's opening brace.
fn try_read_repeat_range(chars: &[char], start: usize) -> Option<(u64, Option<u64>, usize)> {
    let mut i = start + 1;
    if !chars.get(i).is_some_and(|c| c.is_ascii_digit()) {
        return None;
    }
    let mut min_s = String::new();
    while chars.get(i).is_some_and(|c| c.is_ascii_digit()) {
        min_s.push(chars[i]);
        i += 1;
    }
    let min: u64 = min_s.parse().ok()?;
    let max = if chars.get(i) == Some(&',') {
        i += 1;
        let mut max_s = String::new();
        while chars.get(i).is_some_and(|c| c.is_ascii_digit()) {
            max_s.push(chars[i]);
            i += 1;
        }
        if max_s.is_empty() {
            None
        } else {
            Some(max_s.parse().ok()?)
        }
    } else {
        Some(min)
    };
    if chars.get(i) != Some(&'}') {
        return None;
    }
    Some((min, max, i + 1))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokens_keep_unicode_and_escape_decoding() {
        let tokens = lex("PREFIX ex: <http://example.org/> ex:S { ex:p \"café\\nπ\"@en }")
            .expect("fixture should tokenize");
        assert_eq!(
            tokens,
            vec![
                Tok::Iri("http://example.org/S".into()),
                Tok::Punct('{'),
                Tok::Iri("http://example.org/p".into()),
                Tok::Str("café\nπ".into(), StrSuffix::Lang("en".into())),
                Tok::Punct('}'),
                Tok::Punct('\0'),
            ]
        );
    }

    #[test]
    fn unexpected_character_keeps_character_offset_after_unicode() {
        let error = lex("π §").expect_err("section sign is outside the grammar");
        assert_eq!(error, "ShExC: unexpected character `§` at offset 2");
    }

    #[test]
    fn malformed_delimited_tokens_keep_original_errors() {
        assert_eq!(
            lex("<unterminated").unwrap_err(),
            "ShExC: unterminated IRIREF (missing `>`)",
        );
        assert_eq!(
            lex("\"unterminated").unwrap_err(),
            "ShExC: unterminated string literal",
        );
    }
}
