//! Bounded native UTF-8 document extraction.
//!
//! The decoder extracts pages, line-level layout, table shape, exact spans, and an
//! authority-keyed lexical posting list. It never stores source text. Binary office
//! and rendered-page formats enter through governed renditions produced by their
//! owning decoder; the native runtime accepts the current UTF-8 contract directly.

use crate::document::{
    BlockKind, DocumentData, LayoutBlock, LexicalPosting, Page, Span, Table, TableCell,
};

const MAX_PAGES: usize = 4_096;
const MAX_BLOCKS: usize = 250_000;
const MAX_POSTINGS: usize = 1_000_000;
const MAX_LEXEME_BYTES: usize = 128;
const MAX_SOURCE_BYTES: usize = 256 * 1024 * 1024;

/// Issues an irreversible reference for a normalized lexeme. Production uses an
/// authority-derived HMAC key; the decoder never receives or retains that key.
pub trait LexemeEncoder {
    fn encode_lexeme(&self, normalized: &str) -> Option<String>;
}

impl<F> LexemeEncoder for F
where
    F: Fn(&str) -> Option<String>,
{
    fn encode_lexeme(&self, normalized: &str) -> Option<String> {
        self(normalized)
    }
}

pub trait DocumentDecoder {
    fn decode(&self, bytes: &[u8], lexemes: &dyn LexemeEncoder) -> Option<DocumentData>;
}

/// Current dependency-light decoder for UTF-8, Markdown-like headings, form-feed
/// pages, and pipe-delimited table shape.
#[derive(Clone, Copy, Debug, Default)]
pub struct NativeTextDecoder;

impl DocumentDecoder for NativeTextDecoder {
    fn decode(&self, bytes: &[u8], lexemes: &dyn LexemeEncoder) -> Option<DocumentData> {
        if bytes.is_empty() || bytes.len() > MAX_SOURCE_BYTES {
            return None;
        }
        let text = std::str::from_utf8(bytes).ok()?;
        let mut pages = Vec::new();
        let mut postings = Vec::new();
        let mut total_blocks = 0usize;
        let mut page_start = 0usize;

        for (page_index, raw_page) in text.split_inclusive('\u{000c}').enumerate() {
            if page_index >= MAX_PAGES {
                return None;
            }
            let page_text = raw_page.strip_suffix('\u{000c}').unwrap_or(raw_page);
            let mut blocks = Vec::new();
            let mut line_start = page_start;
            for raw_line in page_text.split_inclusive('\n') {
                let line = raw_line.strip_suffix('\n').unwrap_or(raw_line);
                let leading_bytes = line.len() - line.trim_start().len();
                let leading = line.get(..leading_bytes)?.chars().count();
                let trimmed = line.trim();
                if !trimmed.is_empty() {
                    total_blocks += 1;
                    if total_blocks > MAX_BLOCKS {
                        return None;
                    }
                    let start = line_start.checked_add(leading)?;
                    let end = start.checked_add(trimmed.chars().count())?;
                    let kind = classify(trimmed);
                    let block = if kind == BlockKind::Table {
                        let columns = trimmed
                            .split('|')
                            .filter(|cell| !cell.trim().is_empty())
                            .count()
                            .max(1);
                        let cells = (0..columns).map(|col| TableCell { row: 0, col }).collect();
                        let mut block =
                            LayoutBlock::table(Table::new(1, columns).with_cells(cells));
                        block.spans.push(Span::new(start, end));
                        block
                    } else {
                        LayoutBlock::paragraph(vec![Span::new(start, end)]).with_kind(kind)
                    };
                    let block_number = blocks.len() as u32;
                    blocks.push(block);
                    index_lexemes(
                        trimmed,
                        start,
                        page_index as u32 + 1,
                        block_number,
                        lexemes,
                        &mut postings,
                    )?;
                }
                line_start = line_start.checked_add(raw_line.chars().count())?;
            }
            pages.push(Page::new(page_index as u32 + 1, blocks));
            page_start = page_start.checked_add(raw_page.chars().count())?;
        }

        if pages.is_empty() || postings.is_empty() {
            return None;
        }
        Some(
            DocumentData::new(crate::header::content_hash(bytes))
                .with_pages(pages)
                .with_lexical_postings(postings),
        )
    }
}

fn classify(line: &str) -> BlockKind {
    let heading_prefix = line.bytes().take_while(|byte| *byte == b'#').count();
    if (1..=6).contains(&heading_prefix)
        && line
            .as_bytes()
            .get(heading_prefix)
            .is_some_and(u8::is_ascii_whitespace)
    {
        BlockKind::Heading
    } else if line
        .split('|')
        .filter(|cell| !cell.trim().is_empty())
        .count()
        >= 2
    {
        BlockKind::Table
    } else if line.starts_with("- ") || line.starts_with("* ") {
        BlockKind::ListItem
    } else {
        BlockKind::Paragraph
    }
}

fn index_lexemes(
    text: &str,
    absolute_start: usize,
    page: u32,
    block: u32,
    encoder: &dyn LexemeEncoder,
    output: &mut Vec<LexicalPosting>,
) -> Option<()> {
    let mut token_start = None;
    let mut finish =
        |byte_start: usize, byte_end: usize, char_start: usize, char_end: usize| -> Option<()> {
            if byte_end <= byte_start
                || byte_end - byte_start > MAX_LEXEME_BYTES
                || output.len() >= MAX_POSTINGS
            {
                return None;
            }
            let normalized = text.get(byte_start..byte_end)?.to_lowercase();
            let token_ref = encoder.encode_lexeme(&normalized)?;
            output.push(LexicalPosting {
                token_ref,
                page,
                block,
                start: absolute_start.checked_add(char_start)?,
                end: absolute_start.checked_add(char_end)?,
            });
            Some(())
        };

    let mut char_offset = 0usize;
    for (byte_offset, character) in text.char_indices() {
        if character.is_alphanumeric() {
            token_start.get_or_insert((byte_offset, char_offset));
        } else if let Some((byte_start, char_start)) = token_start.take() {
            finish(byte_start, byte_offset, char_start, char_offset)?;
        }
        char_offset = char_offset.checked_add(1)?;
    }
    if let Some((byte_start, char_start)) = token_start {
        finish(byte_start, text.len(), char_start, char_offset)?;
    }
    Some(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lexeme(value: &str) -> Option<String> {
        Some(format!(
            "eg:lexeme:{}",
            crate::content_hash(value.as_bytes())
        ))
    }

    #[test]
    fn native_decoder_extracts_pages_layout_tables_and_private_postings() {
        let source = b"# Heading\nalpha beta\nleft | right\x0csecond page";
        let doc = NativeTextDecoder.decode(source, &lexeme).unwrap();
        assert_eq!(doc.pages.len(), 2);
        assert_eq!(doc.pages[0].blocks[0].kind, BlockKind::Heading);
        assert_eq!(doc.pages[0].blocks[2].kind, BlockKind::Table);
        assert_eq!(doc.pages[0].blocks[2].table.as_ref().unwrap().cols, 2);
        assert_eq!(doc.pages[0].blocks[2].spans.len(), 1);
        assert!(doc.lexical_postings.len() >= 7);
        let encoded = serde_json::to_string(&doc).unwrap();
        assert!(!encoded.contains("Heading"));
        assert!(!encoded.contains("alpha"));
        assert!(doc.lexical_postings.iter().any(|posting| posting.page == 2));
    }

    #[test]
    fn unicode_spans_are_character_ranges_not_utf8_byte_ranges() {
        let doc = NativeTextDecoder
            .decode("école".as_bytes(), &lexeme)
            .unwrap();
        let span = &doc.pages[0].blocks[0].spans[0];
        assert_eq!((span.start, span.end), (0, 5));
        assert_eq!(
            (doc.lexical_postings[0].start, doc.lexical_postings[0].end),
            (0, 5)
        );
    }

    #[test]
    fn decoder_rejects_invalid_empty_and_tokenless_input() {
        let decoder = NativeTextDecoder;
        assert!(decoder.decode(&[0xff, 0xfe], &lexeme).is_none());
        assert!(decoder.decode(b"", &lexeme).is_none());
        assert!(decoder.decode(b"---", &lexeme).is_none());
    }
}
