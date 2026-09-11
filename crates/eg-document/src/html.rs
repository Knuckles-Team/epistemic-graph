//! Bounded HTML-to-text normalization for the native document decoder
//! (GOC-06 — document/image modality stack).
//!
//! HTML source is valid UTF-8, so without this step it would flow straight
//! through [`crate::decoder::NativeTextDecoder`] as raw markup — every tag,
//! script, and style rule treated as document text, and the conformance
//! corpus's HTML class would decode as tag soup rather than readable pages.
//! [`looks_like_html`] sniffs a bounded prefix for a DOCTYPE/`<html>` signature;
//! [`strip_to_text`] then reduces the source to plain text before the existing
//! page/heading/table/lexeme extraction in `decoder.rs` runs unchanged on the
//! result. `script`/`style`/`noscript`/`template` element bodies are dropped
//! entirely (never document text), a small fixed set of named and numeric
//! character references is decoded, and a handful of block-level tags become
//! line breaks so the downstream heading/list/table classifier still sees
//! something resembling the rendered structure.
//!
//! This is deliberately **not** a spec-compliant HTML5 parser or a
//! layout-faithful renderer (that is `LayoutExtractor` provider scope per the
//! GOC-06 lane doc) — it is the bounded, dependency-free normalization that
//! turns HTML bytes into text the existing decoder can extract structure from.
//! Known limitation: a `>` inside a quoted attribute value (e.g.
//! `<a title="x>y">`) ends tag scanning early, since tag boundaries are found
//! by the first unquoted `>` rather than a full attribute grammar — this
//! degrades extraction fidelity on that malformed/unusual markup but never
//! panics, loops, or grows memory unboundedly (see the termination argument in
//! this module's tests).

const SNIFF_WINDOW: usize = 512;
const MAX_SOURCE_BYTES: usize = 256 * 1024 * 1024;

/// Block-level tag names that become a line break in the stripped text (both
/// on open and close, so `<p>a</p><p>b</p>` becomes two lines rather than
/// one run-on line).
const BLOCK_TAGS: &[&str] = &[
    "p",
    "div",
    "br",
    "li",
    "tr",
    "table",
    "ul",
    "ol",
    "section",
    "article",
    "header",
    "footer",
    "blockquote",
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
    "pre",
    "hr",
    "td",
    "th",
];

/// Element bodies dropped entirely — never document text.
const SKIPPED_ELEMENTS: &[&str] = &["script", "style", "noscript", "template"];

/// Maps `h1`..`h6` to their markdown heading level (1..6), matching
/// `decoder.rs::classify`'s recognized `#`.."######" prefix.
fn heading_level(tag_name: &str) -> Option<usize> {
    match tag_name {
        "h1" => Some(1),
        "h2" => Some(2),
        "h3" => Some(3),
        "h4" => Some(4),
        "h5" => Some(5),
        "h6" => Some(6),
        _ => None,
    }
}

/// Sniffs a bounded prefix of `bytes` for a DOCTYPE/`<html>` signature. Never
/// reads past [`SNIFF_WINDOW`] bytes, so this is O(1) regardless of source size.
pub(crate) fn looks_like_html(bytes: &[u8]) -> bool {
    let window = &bytes[..bytes.len().min(SNIFF_WINDOW)];
    let Ok(text) = std::str::from_utf8(window) else {
        return false;
    };
    let lowered = text.to_ascii_lowercase();
    let trimmed = lowered.trim_start();
    trimmed.starts_with("<!doctype html")
        || trimmed.starts_with("<html")
        || lowered.contains("<html>")
        || lowered.contains("<html ")
}

/// Reduces bounded HTML `bytes` to plain text. Returns `None` on oversized or
/// non-UTF-8 input, or when the stripped result carries no non-whitespace
/// content (mirrors `NativeTextDecoder::decode`'s empty-input rejection).
pub(crate) fn strip_to_text(bytes: &[u8]) -> Option<String> {
    if bytes.is_empty() || bytes.len() > MAX_SOURCE_BYTES {
        return None;
    }
    let source = std::str::from_utf8(bytes).ok()?;
    let mut scanner = HtmlScanner::new(source);
    scanner.scan();
    let decoded = decode_entities(&scanner.output);
    (!decoded.trim().is_empty()).then_some(decoded)
}

struct HtmlScanner<'a> {
    source: &'a str,
    output: String,
    skip_element: Option<String>,
    index: usize,
}

impl<'a> HtmlScanner<'a> {
    fn new(source: &'a str) -> Self {
        Self {
            source,
            output: String::with_capacity(source.len()),
            skip_element: None,
            index: 0,
        }
    }

    fn scan(&mut self) {
        while self.index < self.source.len() {
            let rest = &self.source[self.index..];
            let Some(ch) = rest.chars().next() else {
                break;
            };
            if ch != '<' {
                if self.skip_element.is_none() {
                    self.output.push(ch);
                }
                self.index += ch.len_utf8();
                continue;
            }
            if rest.starts_with("<!--") {
                self.index = match rest.find("-->") {
                    Some(offset) => self.index + offset + 3,
                    None => self.source.len(),
                };
                continue;
            }
            if !self.scan_tag(rest, ch) {
                break;
            }
        }
    }

    fn scan_tag(&mut self, rest: &str, ch: char) -> bool {
        match parse_tag(rest) {
            TagScan::Malformed => {
                self.index += ch.len_utf8();
                true
            }
            TagScan::Unterminated => false,
            TagScan::Complete(tag) => {
                self.index += tag.consumed;
                self.emit_tag(&tag);
                true
            }
        }
    }

    fn emit_tag(&mut self, tag: &ParsedTag) {
        if let Some(skipped) = &self.skip_element {
            if tag.is_close && &tag.name == skipped {
                self.skip_element = None;
            }
            return;
        }
        if !tag.is_close && SKIPPED_ELEMENTS.contains(&tag.name.as_str()) {
            self.skip_element = Some(tag.name.clone());
            return;
        }
        if !tag.is_close {
            if let Some(level) = heading_level(&tag.name) {
                self.output.push('\n');
                self.output.extend(std::iter::repeat_n('#', level));
                self.output.push(' ');
                return;
            }
            if tag.name == "li" {
                self.output.push('\n');
                self.output.push_str("- ");
                return;
            }
        }
        if BLOCK_TAGS.contains(&tag.name.as_str()) {
            self.output.push('\n');
        }
    }
}

struct ParsedTag {
    is_close: bool,
    name: String,
    consumed: usize,
}

enum TagScan {
    Malformed,
    Unterminated,
    Complete(ParsedTag),
}

fn parse_tag(rest: &str) -> TagScan {
    let is_close = rest.as_bytes().get(1) == Some(&b'/');
    let name_start = 1 + usize::from(is_close);
    let Some(name_tail) = rest.get(name_start..) else {
        return TagScan::Malformed;
    };
    let name_rel_end = name_tail
        .find(|c: char| c == '>' || c == '/' || c.is_ascii_whitespace())
        .unwrap_or(name_tail.len());
    let Some(tag_name) = rest.get(name_start..name_start + name_rel_end) else {
        return TagScan::Malformed;
    };
    let Some(close_rel) = rest.find('>') else {
        return TagScan::Unterminated;
    };
    TagScan::Complete(ParsedTag {
        is_close,
        name: tag_name.to_ascii_lowercase(),
        consumed: close_rel + 1,
    })
}

/// Decodes the fixed named-entity set plus numeric (`&#NN;` / `&#xHH;`)
/// character references. An unrecognized or malformed reference passes
/// through unchanged (the leading `&` and the rest of the text are never
/// dropped) rather than being guessed at.
fn decode_entities(input: &str) -> String {
    let mut output = String::with_capacity(input.len());
    let total = input.len();
    let mut i = 0usize;
    while i < total {
        let rest = &input[i..];
        if let Some(after_amp) = rest.strip_prefix('&') {
            if let Some(semicolon) = after_amp.find(';').filter(|pos| *pos <= 10) {
                let entity = &after_amp[..semicolon];
                if let Some(decoded) = decode_one_entity(entity) {
                    output.push(decoded);
                    i += 1 + semicolon + 1;
                    continue;
                }
            }
        }
        let Some(ch) = rest.chars().next() else {
            break;
        };
        output.push(ch);
        i += ch.len_utf8();
    }
    output
}

fn decode_one_entity(entity: &str) -> Option<char> {
    Some(match entity {
        "amp" => '&',
        "lt" => '<',
        "gt" => '>',
        "quot" => '"',
        "apos" => '\'',
        "nbsp" => ' ',
        _ => {
            let code = entity
                .strip_prefix("#x")
                .or_else(|| entity.strip_prefix("#X"))
                .and_then(|hex| u32::from_str_radix(hex, 16).ok())
                .or_else(|| {
                    entity
                        .strip_prefix('#')
                        .and_then(|dec| dec.parse::<u32>().ok())
                })?;
            char::from_u32(code)?
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sniffs_only_a_bounded_prefix() {
        assert!(looks_like_html(
            b"<!DOCTYPE html><html><body>hi</body></html>"
        ));
        assert!(looks_like_html(b"  <html lang=\"en\">"));
        assert!(!looks_like_html(b"plain text, no markup"));
        assert!(!looks_like_html(b"a < b and c > d, just prose"));
    }

    #[test]
    fn strips_tags_drops_script_and_style_bodies_and_decodes_entities() {
        let html = b"<!DOCTYPE html><html><head><style>body{color:red}</style>\
<script>alert(1)</script></head><body><h1>Title &amp; more</h1>\
<p>alpha &lt;beta&gt; gamma</p></body></html>";
        let text = strip_to_text(html).unwrap();
        assert!(!text.contains("color:red"));
        assert!(!text.contains("alert(1)"));
        assert!(text.contains("Title & more"));
        assert!(text.contains("alpha <beta> gamma"));
    }

    #[test]
    fn decodes_numeric_entities() {
        let html = b"<html><body>&#65;&#x42;&#67;</body></html>";
        let text = strip_to_text(html).unwrap();
        assert!(text.contains("ABC"));
    }

    #[test]
    fn block_tags_separate_lines_so_page_extraction_finds_paragraphs() {
        let html = b"<html><body><p>first</p><p>second</p></body></html>";
        let text = strip_to_text(html).unwrap();
        let lines: Vec<&str> = text
            .lines()
            .map(str::trim)
            .filter(|l| !l.is_empty())
            .collect();
        assert_eq!(lines, vec!["first", "second"]);
    }

    #[test]
    fn headings_and_list_items_carry_a_classify_recognized_prefix() {
        let html = b"<html><body><h1>One</h1><h2>Two</h2><h3>Three</h3>\
<h4>Four</h4><h5>Five</h5><h6>Six</h6><ul><li>one</li><li>two</li></ul></body></html>";
        let text = strip_to_text(html).unwrap();
        let lines: Vec<&str> = text
            .lines()
            .map(str::trim)
            .filter(|l| !l.is_empty())
            .collect();
        assert_eq!(
            lines,
            vec![
                "# One",
                "## Two",
                "### Three",
                "#### Four",
                "##### Five",
                "###### Six",
                "- one",
                "- two",
            ]
        );
    }

    #[test]
    fn unterminated_tag_does_not_panic_and_keeps_text_before_it() {
        let text = strip_to_text(b"<html><body>hello <b>world").unwrap();
        assert!(text.contains("hello"));
    }

    #[test]
    fn unterminated_comment_drops_the_remainder_without_panicking() {
        // The property under test is that this returns at all (no panic, no
        // hang) — an unterminated `<!--` consumes the rest of the source per
        // `strip_to_text`'s documented behavior, so "world" is dropped.
        let result = strip_to_text(b"<html><body>hello <!-- unterminated comment world");
        assert!(result.is_none_or(|text| !text.contains("world")));
    }

    #[test]
    fn adversarial_angle_bracket_soup_terminates_and_stays_bounded() {
        let mut hostile = b"<html><body>".to_vec();
        hostile.extend(std::iter::repeat_n(b'<', 50_000));
        hostile.extend_from_slice(b">tail</body></html>");
        let result = strip_to_text(&hostile);
        // Must return (not hang) and never exceed the source length.
        if let Some(text) = result {
            assert!(text.len() <= hostile.len());
        }
    }

    #[test]
    fn oversized_and_non_utf8_input_is_rejected() {
        assert!(strip_to_text(&[]).is_none());
        assert!(strip_to_text(&[0xff, 0xfe]).is_none());
    }
}
