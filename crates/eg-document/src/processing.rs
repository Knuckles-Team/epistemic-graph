//! Deterministic document chunking (EG-REPO-INGEST-R652).
//!
//! One pass over the text splits it into sections at Markdown ATX headings
//! (`#` to `######`), then packs each section into windows of at most
//! [`ChunkingConfig::max_bytes`] UTF-8 bytes. A window ends at the last
//! paragraph break, else the last sentence end, else the last whitespace
//! before the limit. Consecutive windows of one section overlap by about
//! [`ChunkingConfig::overlap_bytes`]; windows never cross a heading. Every
//! chunk carries byte and character offsets into the source text, so a
//! citation can address the exact passage. The output depends only on the
//! text and the config: the same input always yields the same chunks.

use serde::{Deserialize, Serialize};

/// The chunker's version tag, recorded with every enrichment.
pub const CHUNKER_VERSION: &str = "eg-document/heading-paragraph-v1";

/// The largest window the chunker accepts.
pub const MAX_CHUNK_BYTES: usize = 64 * 1024;
/// The smallest window the chunker accepts.
pub const MIN_CHUNK_BYTES: usize = 64;

/// Window size and overlap, in UTF-8 bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkingConfig {
    pub max_bytes: usize,
    pub overlap_bytes: usize,
}

impl Default for ChunkingConfig {
    fn default() -> Self {
        Self {
            max_bytes: 1_200,
            overlap_bytes: 150,
        }
    }
}

impl ChunkingConfig {
    /// Refuse a window outside the accepted range, or an overlap of half the
    /// window or more (which would make no forward progress worth the name).
    pub fn validate(&self) -> Result<(), String> {
        if !(MIN_CHUNK_BYTES..=MAX_CHUNK_BYTES).contains(&self.max_bytes) {
            return Err(format!(
                "CHUNK_CONFIG_INVALID: max_bytes must be in {MIN_CHUNK_BYTES}..={MAX_CHUNK_BYTES}"
            ));
        }
        if self.overlap_bytes * 2 >= self.max_bytes {
            return Err(
                "CHUNK_CONFIG_INVALID: overlap_bytes must be under half of max_bytes".into(),
            );
        }
        Ok(())
    }
}

/// One chunk: its position, its section and its offsets into the source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkSpan {
    pub ordinal: u32,
    /// Headings enclosing the chunk, outermost first.
    pub heading_path: Vec<String>,
    pub start_byte: usize,
    pub end_byte: usize,
    /// Unicode scalar offsets, for citations.
    pub start_char: usize,
    pub end_char: usize,
}

impl ChunkSpan {
    /// The chunk's text, borrowed from the source it was cut from.
    pub fn text<'a>(&self, source: &'a str) -> &'a str {
        &source[self.start_byte..self.end_byte]
    }
}

struct Section {
    heading_path: Vec<String>,
    start: usize,
    end: usize,
}

/// The heading level and title of an ATX heading line, if it is one.
fn heading(line: &str) -> Option<(usize, &str)> {
    let trimmed = line.trim_start_matches(' ');
    let level = trimmed.bytes().take_while(|b| *b == b'#').count();
    if !(1..=6).contains(&level) || line.len() - trimmed.len() > 3 {
        return None;
    }
    let rest = &trimmed[level..];
    if !(rest.is_empty() || rest.starts_with(' ') || rest.starts_with('\t')) {
        return None;
    }
    Some((level, rest.trim().trim_end_matches('#').trim_end()))
}

/// Split the text into heading-delimited sections. A heading line belongs to
/// the section it opens, so its title is part of that section's first chunk.
fn sections(text: &str) -> Vec<Section> {
    let mut out = Vec::new();
    let mut stack: Vec<(usize, String)> = Vec::new();
    let mut start = 0;
    let mut offset = 0;
    for line in text.split_inclusive('\n') {
        if let Some((level, title)) = heading(line.trim_end_matches(['\n', '\r'])) {
            if offset > start {
                out.push(Section {
                    heading_path: stack.iter().map(|(_, t)| t.clone()).collect(),
                    start,
                    end: offset,
                });
            }
            while stack.last().is_some_and(|(l, _)| *l >= level) {
                stack.pop();
            }
            stack.push((level, title.to_string()));
            start = offset;
        }
        offset += line.len();
    }
    if text.len() > start {
        out.push(Section {
            heading_path: stack.into_iter().map(|(_, t)| t).collect(),
            start,
            end: text.len(),
        });
    }
    out
}

fn floor_boundary(text: &str, mut index: usize) -> usize {
    while !text.is_char_boundary(index) {
        index -= 1;
    }
    index
}

/// The best cut in `text[from..limit]`: after the last paragraph break, else
/// after the last sentence end, else at the last whitespace, else `limit`.
/// The cut lies in the window's second half, so every window makes progress.
fn cut_point(text: &str, from: usize, limit: usize) -> usize {
    let floor = from + (limit - from) / 2;
    let window = &text.as_bytes()[floor..limit];
    if let Some(i) = window.windows(2).rposition(|w| w == b"\n\n") {
        return floor + i + 2;
    }
    if let Some(i) = window
        .windows(2)
        .rposition(|w| matches!(w[0], b'.' | b'!' | b'?') && w[1].is_ascii_whitespace())
    {
        return floor + i + 2;
    }
    if let Some(i) = window.iter().rposition(u8::is_ascii_whitespace) {
        return floor + i + 1;
    }
    limit
}

/// Counts characters up to a byte offset. Offsets move forward, and back by
/// at most one overlap, so the total work stays linear in the text.
struct CharCursor<'a> {
    text: &'a str,
    byte: usize,
    chars: usize,
}

impl CharCursor<'_> {
    fn at(&mut self, byte: usize) -> usize {
        if byte >= self.byte {
            self.chars += self.text[self.byte..byte].chars().count();
        } else {
            self.chars -= self.text[byte..self.byte].chars().count();
        }
        self.byte = byte;
        self.chars
    }
}

/// Trim surrounding whitespace from `text[start..end]`, returning the new
/// bounds, or `None` when nothing but whitespace remains.
fn trimmed(text: &str, start: usize, end: usize) -> Option<(usize, usize)> {
    let slice = &text[start..end];
    let lead = slice.len() - slice.trim_start().len();
    let body = slice.trim();
    (!body.is_empty()).then(|| (start + lead, start + lead + body.len()))
}

/// Chunk `text` under `config`. Refuses an invalid config; an empty or
/// whitespace-only text yields no chunks.
pub fn chunk_text(text: &str, config: &ChunkingConfig) -> Result<Vec<ChunkSpan>, String> {
    config.validate()?;
    let mut cursor = CharCursor {
        text,
        byte: 0,
        chars: 0,
    };
    let mut chunks = Vec::new();
    for section in sections(text) {
        let mut from = section.start;
        while from < section.end {
            let limit = floor_boundary(text, (from + config.max_bytes).min(section.end));
            let cut = if limit == section.end {
                limit
            } else {
                floor_boundary(text, cut_point(text, from, limit))
            };
            if let Some((start, end)) = trimmed(text, from, cut) {
                let start_char = cursor.at(start);
                let end_char = cursor.at(end);
                chunks.push(ChunkSpan {
                    ordinal: chunks.len() as u32,
                    heading_path: section.heading_path.clone(),
                    start_byte: start,
                    end_byte: end,
                    start_char,
                    end_char,
                });
            }
            if cut >= section.end {
                break;
            }
            from = next_start(text, from, cut, config.overlap_bytes);
        }
    }
    Ok(chunks)
}

/// Where the next window starts: `overlap` bytes before `cut`, moved forward
/// to a word start, and always after `from`.
fn next_start(text: &str, from: usize, cut: usize, overlap: usize) -> usize {
    if overlap == 0 {
        return cut;
    }
    let back = floor_boundary(text, cut.saturating_sub(overlap).max(from + 1));
    let word = text.as_bytes()[back..cut]
        .iter()
        .position(u8::is_ascii_whitespace)
        .map_or(back, |i| back + i + 1);
    if word > from && word < cut {
        word
    } else {
        cut
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(max_bytes: usize, overlap_bytes: usize) -> ChunkingConfig {
        ChunkingConfig {
            max_bytes,
            overlap_bytes,
        }
    }

    // spec: EG-UNIFIED-DATA-PLANE-R651.7.2
    #[test]
    fn chunks_respect_headings_size_and_offsets() {
        let text = "# Title\n\nIntro paragraph one.\n\n## Part A\n\nAlpha text here.\n\n## Part B\n\nBeta.\n";
        let chunks = chunk_text(text, &ChunkingConfig::default()).unwrap();
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].heading_path, vec!["Title"]);
        assert_eq!(chunks[1].heading_path, vec!["Title", "Part A"]);
        assert_eq!(chunks[2].heading_path, vec!["Title", "Part B"]);
        assert!(chunks[1].text(text).starts_with("## Part A"));
        for chunk in &chunks {
            let by_chars: String = text
                .chars()
                .skip(chunk.start_char)
                .take(chunk.end_char - chunk.start_char)
                .collect();
            assert_eq!(by_chars, chunk.text(text));
        }
    }

    // spec: EG-UNIFIED-DATA-PLANE-R651.7.2
    #[test]
    fn long_sections_split_with_overlap_under_the_limit() {
        let sentence = "Ünïcode sentence number with some words in it. ";
        let text = sentence.repeat(200);
        let chunks = chunk_text(&text, &config(300, 60)).unwrap();
        assert!(chunks.len() > 20);
        for pair in chunks.windows(2) {
            assert!(pair[0].end_byte - pair[0].start_byte <= 300);
            assert!(
                pair[1].start_byte < pair[0].end_byte,
                "consecutive windows overlap"
            );
            assert!(pair[1].start_byte > pair[0].start_byte, "windows advance");
        }
        assert_eq!(chunks.last().unwrap().end_byte, text.trim_end().len());
        for chunk in &chunks {
            let by_chars: String = text
                .chars()
                .skip(chunk.start_char)
                .take(chunk.end_char - chunk.start_char)
                .collect();
            assert_eq!(by_chars, chunk.text(&text));
        }
    }

    // spec: EG-UNIFIED-DATA-PLANE-R651.7.2
    #[test]
    fn chunking_is_deterministic_and_paragraph_aware() {
        let text = format!("{}\n\n{}", "a".repeat(500), "b ".repeat(400));
        let first = chunk_text(&text, &config(800, 100)).unwrap();
        assert_eq!(first, chunk_text(&text, &config(800, 100)).unwrap());
        assert_eq!(
            first[0].text(&text),
            "a".repeat(500),
            "cut at the paragraph break"
        );
    }

    // spec: EG-UNIFIED-DATA-PLANE-R651.7.4
    #[test]
    fn invalid_configs_and_blank_text() {
        assert!(chunk_text("x", &config(10, 0)).is_err());
        assert!(chunk_text("x", &config(200, 100)).is_err());
        assert!(chunk_text(" \n\n ", &ChunkingConfig::default())
            .unwrap()
            .is_empty());
        let unbroken = "x".repeat(5_000);
        let chunks = chunk_text(&unbroken, &config(1_000, 0)).unwrap();
        assert_eq!(chunks.len(), 5, "a text with no break cuts at the limit");
    }
}
