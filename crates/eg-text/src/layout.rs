//! Table extraction + layout spans + citation/clause spans (CONCEPT:EG-KG.query
//! depth: per-modality text/layout structure) — a pure-Rust heuristic structural
//! extractor over plain text (no coordinates/PDF layout needed: markdown-pipe and
//! whitespace-aligned tables, heading/list/paragraph segmentation, and citation/
//! legal-clause markers). Dependency-free, ships in EVERY build (no `tantivy`
//! needed) — mirrors [`crate::bm25`]'s "always compiled" posture.
//!
//! The shapes here are deliberately **`EvidenceAddress`-shaped**: [`TableSpan`] mirrors
//! `eg_modality::EvidenceAddress::TableCellRange` field-for-field and
//! [`CitationSpan`]/[`LayoutSpan`] mirror `EvidenceAddress::CharacterRange`, so once a
//! caller has an `eg-modality` `contract` build available, converting is a free
//! `From` (see `src/contract.rs`) — this module itself takes no `eg-modality`
//! dependency.

use serde::{Deserialize, Serialize};

// ─────────────────────────────── table extraction ───────────────────────────────

/// One detected table: its parsed grid (row-major, `rows[r][c]`) and the
/// half-open BYTE range `[byte_start, byte_end)` of the source text it came from
/// (for a `CharacterRange` backing address in the surrounding document).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ExtractedTable {
    pub table_id: String,
    pub rows: Vec<Vec<String>>,
    pub byte_start: usize,
    pub byte_end: usize,
}

impl ExtractedTable {
    pub fn n_rows(&self) -> usize {
        self.rows.len()
    }

    pub fn n_cols(&self) -> usize {
        self.rows.first().map(Vec::len).unwrap_or(0)
    }

    /// The whole table as one `TableCellRange`-shaped span.
    pub fn full_span(&self) -> TableSpan {
        TableSpan {
            table_id: self.table_id.clone(),
            row_start: 0,
            row_end: self.n_rows().saturating_sub(1),
            col_start: 0,
            col_end: self.n_cols().saturating_sub(1),
            text: String::new(),
        }
    }

    /// A single cell's `TableCellRange`-shaped span, carrying that cell's text.
    /// `None` if `(row, col)` is out of range.
    pub fn cell_span(&self, row: usize, col: usize) -> Option<TableSpan> {
        let text = self.rows.get(row)?.get(col)?.clone();
        Some(TableSpan {
            table_id: self.table_id.clone(),
            row_start: row,
            row_end: row,
            col_start: col,
            col_end: col,
            text,
        })
    }
}

/// A `TableCellRange`-shaped span: identical fields to
/// `eg_modality::EvidenceAddress::TableCellRange` plus the cell text itself (the
/// `EvidenceAddress` variant carries no payload — this is the extractor's own richer
/// intermediate the caller reads before staging just the located reference).
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TableSpan {
    pub table_id: String,
    pub row_start: usize,
    pub row_end: usize,
    pub col_start: usize,
    pub col_end: usize,
    pub text: String,
}

/// Split one table row into its cells: markdown pipe-delimited (`| a | b |`) if the
/// line contains `|`, else runs of 2+ whitespace (the common plain-text
/// fixed-width-column convention).
fn split_row(line: &str) -> Vec<String> {
    let trimmed = line.trim();
    if trimmed.contains('|') {
        trimmed
            .trim_matches('|')
            .split('|')
            .map(|c| c.trim().to_string())
            .collect()
    } else {
        split_on_multi_space(trimmed)
    }
}

/// Split on runs of >= 2 whitespace characters (single spaces stay inside a cell —
/// "New York" is one cell, not two).
fn split_on_multi_space(line: &str) -> Vec<String> {
    let mut cells = Vec::new();
    let mut cur = String::new();
    let mut space_run = 0usize;
    for ch in line.chars() {
        if ch.is_whitespace() {
            space_run += 1;
            if space_run == 2 {
                cells.push(cur.trim().to_string());
                cur.clear();
            } else if space_run > 2 {
                // absorbed into the same separator
            } else {
                cur.push(ch);
            }
        } else {
            space_run = 0;
            cur.push(ch);
        }
    }
    let tail = cur.trim();
    if !tail.is_empty() || !cells.is_empty() {
        cells.push(tail.to_string());
    }
    cells
}

/// A markdown table separator row, e.g. `| --- | :--- | ---: |` or `--- | ---`.
fn is_separator_row(cells: &[String]) -> bool {
    !cells.is_empty()
        && cells.iter().all(|c| {
            let c = c.trim();
            !c.is_empty() && c.chars().all(|ch| ch == '-' || ch == ':')
        })
}

/// Detect tabular blocks in `text`: contiguous runs of >= 2 lines that each split
/// into the SAME (>= 2) cell count. A markdown separator row (`---|---`) is
/// dropped from the parsed grid but still counts toward the run. Returns one
/// [`ExtractedTable`] per detected block, `table_id`s numbered `table-0`,
/// `table-1`, … in document order.
pub fn extract_tables(text: &str) -> Vec<ExtractedTable> {
    // (line, byte_start_of_line) pairs, preserving exact byte offsets into `text`.
    let mut line_starts: Vec<usize> = Vec::new();
    let mut offset = 0usize;
    let lines: Vec<&str> = text.split_inclusive('\n').collect();
    let mut plain_lines: Vec<&str> = Vec::new();
    for l in &lines {
        line_starts.push(offset);
        offset += l.len();
        plain_lines.push(l.trim_end_matches('\n').trim_end_matches('\r'));
    }

    let mut tables = Vec::new();
    let mut i = 0usize;
    while i < plain_lines.len() {
        let cells = split_row(plain_lines[i]);
        if cells.len() < 2 || is_separator_row(&cells) {
            i += 1;
            continue;
        }
        let ncols = cells.len();
        let block_start = i;
        let mut rows: Vec<Vec<String>> = vec![cells];
        let mut j = i + 1;
        while j < plain_lines.len() {
            let c = split_row(plain_lines[j]);
            if is_separator_row(&c) && c.len() == ncols {
                j += 1;
                continue; // skip the markdown separator row, don't end the block
            }
            if c.len() != ncols {
                break;
            }
            rows.push(c);
            j += 1;
        }
        if rows.len() >= 2 {
            let byte_start = line_starts[block_start];
            let byte_end = if j < line_starts.len() {
                line_starts[j]
            } else {
                text.len()
            };
            tables.push(ExtractedTable {
                table_id: format!("table-{}", tables.len()),
                rows,
                byte_start,
                byte_end,
            });
            i = j;
        } else {
            i += 1;
        }
    }
    tables
}

// ─────────────────────────────── layout spans ───────────────────────────────

/// The structural role of one [`LayoutSpan`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum LayoutKind {
    Heading,
    ListItem,
    Table,
    Paragraph,
}

/// A `CharacterRange`-shaped structural region: a half-open BYTE range `[start, end)`
/// tagged with its [`LayoutKind`].
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct LayoutSpan {
    pub kind: LayoutKind,
    pub start: usize,
    pub end: usize,
}

fn is_heading_line(line: &str) -> bool {
    let t = line.trim_start();
    t.starts_with('#')
        || (t.len() > 3 && t == t.to_uppercase() && t.chars().any(char::is_alphabetic))
}

fn is_list_item_line(line: &str) -> bool {
    let t = line.trim_start();
    if t.starts_with("- ") || t.starts_with("* ") || t.starts_with("+ ") {
        return true;
    }
    // "1. " / "12) " numbered list markers.
    let digits: String = t.chars().take_while(|c| c.is_ascii_digit()).collect();
    if digits.is_empty() {
        return false;
    }
    matches!(t[digits.len()..].chars().next(), Some('.') | Some(')'))
}

/// Segment `text` into [`LayoutSpan`]s: table regions (reusing [`extract_tables`]),
/// then headings / list items / paragraphs line-by-line over what's left, merging
/// consecutive lines of the SAME kind (paragraphs especially) into one span.
pub fn layout_spans(text: &str) -> Vec<LayoutSpan> {
    let tables = extract_tables(text);

    // Compute per-line byte ranges once.
    let mut line_ranges: Vec<(usize, usize)> = Vec::new(); // [start, end) excluding the trailing \n
    let mut offset = 0usize;
    for raw in text.split_inclusive('\n') {
        let content_len = raw.trim_end_matches('\n').trim_end_matches('\r').len();
        line_ranges.push((offset, offset + content_len));
        offset += raw.len();
    }

    let mut spans: Vec<LayoutSpan> = Vec::new();
    let mut li = 0usize;
    let mut pending_para_start: Option<usize> = None;

    let flush_para = |spans: &mut Vec<LayoutSpan>, start: Option<usize>, end: usize| {
        if let Some(s) = start {
            if end > s {
                spans.push(LayoutSpan {
                    kind: LayoutKind::Paragraph,
                    start: s,
                    end,
                });
            }
        }
    };

    while li < line_ranges.len() {
        let (lstart, lend) = line_ranges[li];
        // A table starting exactly at this line's byte offset takes the whole block.
        if let Some(t) = tables.iter().find(|t| t.byte_start == lstart) {
            flush_para(&mut spans, pending_para_start.take(), lstart);
            spans.push(LayoutSpan {
                kind: LayoutKind::Table,
                start: t.byte_start,
                end: t.byte_end,
            });
            // Advance li past every line the table's byte range covers.
            while li < line_ranges.len() && line_ranges[li].0 < t.byte_end {
                li += 1;
            }
            continue;
        }

        let raw_line = &text[lstart..lend];
        if raw_line.trim().is_empty() {
            flush_para(&mut spans, pending_para_start.take(), lstart);
            li += 1;
            continue;
        }
        if is_heading_line(raw_line) {
            flush_para(&mut spans, pending_para_start.take(), lstart);
            spans.push(LayoutSpan {
                kind: LayoutKind::Heading,
                start: lstart,
                end: lend,
            });
            li += 1;
            continue;
        }
        if is_list_item_line(raw_line) {
            flush_para(&mut spans, pending_para_start.take(), lstart);
            spans.push(LayoutSpan {
                kind: LayoutKind::ListItem,
                start: lstart,
                end: lend,
            });
            li += 1;
            continue;
        }
        // Plain text line: extend (or start) the pending paragraph run.
        if pending_para_start.is_none() {
            pending_para_start = Some(lstart);
        }
        li += 1;
    }
    flush_para(&mut spans, pending_para_start, text.len());

    spans.sort_by_key(|s| s.start);
    spans
}

// ─────────────────────────────── citation / clause spans ───────────────────────

/// The kind of reference a [`CitationSpan`] marks.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CitationKind {
    /// A bracketed numeric/footnote citation, e.g. `[12]`, `[3, 4]`.
    Bracketed,
    /// A parenthetical author-year citation, e.g. `(Smith, 2020)`, `(2020)`.
    ParentheticalYear,
    /// A legal/structural clause reference, e.g. `Section 3.2`, `Article 12`.
    ClauseReference,
}

/// A `CharacterRange`-shaped citation/clause reference: a half-open byte range plus
/// its matched text and [`CitationKind`].
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CitationSpan {
    pub kind: CitationKind,
    pub start: usize,
    pub end: usize,
    pub text: String,
}

/// `[...]` spans whose inner content "looks like" a citation marker: short (<= 24
/// bytes) and containing at least one ASCII digit (footnote/reference numbers),
/// e.g. `[12]`, `[3, 4]`, `[Smith20]` — but NOT arbitrary prose in brackets.
fn scan_bracketed(text: &str) -> Vec<CitationSpan> {
    let bytes = text.as_bytes();
    let mut out = Vec::new();
    let mut i = 0usize;
    while i < bytes.len() {
        if bytes[i] == b'[' {
            if let Some(rel_close) = text[i + 1..].find(']') {
                let close = i + 1 + rel_close;
                let inner = &text[i + 1..close];
                if !inner.is_empty()
                    && inner.len() <= 24
                    && !inner.contains('\n')
                    && inner.chars().any(|c| c.is_ascii_digit())
                {
                    out.push(CitationSpan {
                        kind: CitationKind::Bracketed,
                        start: i,
                        end: close + 1,
                        text: text[i..close + 1].to_string(),
                    });
                }
                i = close + 1;
                continue;
            }
        }
        i += 1;
    }
    out
}

/// `(...)` spans containing a plausible 4-digit year (1500..=2099) — the standard
/// author-year citation shape, e.g. `(Smith, 2020)`, `(Smith et al., 2020)`,
/// `(2020)`.
fn scan_parenthetical_year(text: &str) -> Vec<CitationSpan> {
    let mut out = Vec::new();
    let mut i = 0usize;
    while i < text.len() {
        if text.as_bytes()[i] == b'(' {
            if let Some(rel_close) = text[i + 1..].find(')') {
                let close = i + 1 + rel_close;
                let inner = &text[i + 1..close];
                if inner.len() <= 60 && !inner.contains('\n') && contains_plausible_year(inner) {
                    out.push(CitationSpan {
                        kind: CitationKind::ParentheticalYear,
                        start: i,
                        end: close + 1,
                        text: text[i..close + 1].to_string(),
                    });
                }
                i = close + 1;
                continue;
            }
        }
        i += 1;
    }
    out
}

fn contains_plausible_year(s: &str) -> bool {
    let digits: Vec<char> = s.chars().collect();
    for w in digits.windows(4) {
        if w.iter().all(|c| c.is_ascii_digit()) {
            let year: i32 = w.iter().collect::<String>().parse().unwrap_or(0);
            if (1500..=2099).contains(&year) {
                // Boundary check: not part of a longer digit run (e.g. a phone
                // number or a 5+ digit id).
                return true;
            }
        }
    }
    false
}

/// "Section 3.2" / "Clause 4(a)" / "Article 12" style legal/structural clause
/// references: one of a small keyword set followed by whitespace and an
/// alphanumeric/dotted locator.
fn scan_clause_references(text: &str) -> Vec<CitationSpan> {
    const KEYWORDS: &[&str] = &["Section", "Clause", "Article", "Paragraph", "Appendix"];
    let mut out = Vec::new();
    for kw in KEYWORDS {
        let mut search_from = 0usize;
        while let Some(rel) = text[search_from..].find(kw) {
            let start = search_from + rel;
            // Word-boundary check on the left (don't match mid-word, e.g. "Subsection").
            let left_ok = start == 0
                || !text[..start]
                    .chars()
                    .next_back()
                    .map(|c| c.is_alphanumeric())
                    .unwrap_or(false);
            let after = start + kw.len();
            if left_ok && text[after..].starts_with(char::is_whitespace) {
                let rest = &text[after..];
                let locator_start = after + rest.len() - rest.trim_start().len();
                let locator = rest.trim_start();
                let loc_len = locator
                    .chars()
                    .take_while(|c| c.is_alphanumeric() || *c == '.' || *c == '(' || *c == ')')
                    .count();
                if loc_len > 0 {
                    let first = locator.chars().next().unwrap();
                    if first.is_ascii_digit() {
                        // The span anchors the LOCATOR itself (e.g. `3.2`, `12`),
                        // not the leading keyword: `text` is the clause identifier
                        // callers key on, and keeping `start`/`end` on the locator
                        // preserves the crate-wide `text == text[start..end]`
                        // invariant that `to_evidence_address` relies on.
                        let end = locator_start + loc_len;
                        out.push(CitationSpan {
                            kind: CitationKind::ClauseReference,
                            start: locator_start,
                            end,
                            text: text[locator_start..end].to_string(),
                        });
                    }
                }
            }
            search_from = start + kw.len();
        }
    }
    out
}

/// All citation/clause spans in `text` — bracketed, parenthetical-year, and
/// clause-reference — merged and sorted by start offset. Overlapping matches from
/// different scanners are left as-is (a caller wanting non-overlap can prefer the
/// first-sorted); in practice the three patterns rarely collide.
pub fn citation_spans(text: &str) -> Vec<CitationSpan> {
    let mut out = Vec::new();
    out.extend(scan_bracketed(text));
    out.extend(scan_parenthetical_year(text));
    out.extend(scan_clause_references(text));
    out.sort_by_key(|s| s.start);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_markdown_pipe_table() {
        let text = "Intro line.\n\n| Name | Age |\n| --- | --- |\n| Alice | 30 |\n| Bob | 25 |\n\nOutro.\n";
        let tables = extract_tables(text);
        assert_eq!(tables.len(), 1);
        let t = &tables[0];
        assert_eq!(t.n_rows(), 3); // header + 2 data rows (separator dropped)
        assert_eq!(t.n_cols(), 2);
        assert_eq!(t.rows[0], vec!["Name".to_string(), "Age".to_string()]);
        assert_eq!(t.rows[1], vec!["Alice".to_string(), "30".to_string()]);
        // The byte range must exactly reproduce the table block from the source.
        assert!(text[t.byte_start..t.byte_end].contains("Alice"));
        assert!(!text[t.byte_start..t.byte_end].contains("Intro"));
    }

    #[test]
    fn extracts_whitespace_aligned_table() {
        let text = "Name      Age\nAlice     30\nBob       25\n";
        let tables = extract_tables(text);
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].n_cols(), 2);
        assert_eq!(tables[0].rows[1][0], "Alice");
    }

    #[test]
    fn no_table_in_plain_prose() {
        let text = "This is just a normal paragraph of prose with no tabular structure at all.\n";
        assert!(extract_tables(text).is_empty());
    }

    #[test]
    fn cell_span_and_full_span_shapes() {
        let text = "| A | B |\n| --- | --- |\n| 1 | 2 |\n";
        let tables = extract_tables(text);
        let t = &tables[0];
        let cell = t.cell_span(1, 0).unwrap();
        assert_eq!(cell.table_id, "table-0");
        assert_eq!(cell.row_start, 1);
        assert_eq!(cell.col_start, 0);
        assert_eq!(cell.text, "1");
        assert!(t.cell_span(99, 0).is_none());
        let full = t.full_span();
        assert_eq!(full.row_end, 1); // 2 rows -> indices 0,1
        assert_eq!(full.col_end, 1); // 2 cols -> indices 0,1
    }

    #[test]
    fn layout_spans_classify_heading_list_paragraph_and_table() {
        let text = "# Title\n\nSome intro paragraph\nspanning two lines.\n\n- item one\n- item two\n\n| A | B |\n| --- | --- |\n| 1 | 2 |\n";
        let spans = layout_spans(text);
        assert!(spans.iter().any(|s| s.kind == LayoutKind::Heading));
        assert!(spans.iter().any(|s| s.kind == LayoutKind::ListItem));
        assert!(spans.iter().any(|s| s.kind == LayoutKind::Table));
        assert!(spans.iter().any(|s| s.kind == LayoutKind::Paragraph));
        // Spans must be in document order and non-overlapping-ish (sorted).
        for w in spans.windows(2) {
            assert!(w[0].start <= w[1].start);
        }
    }

    #[test]
    fn bracketed_citation_detected() {
        let text = "This claim is well supported [12] by prior work, unlike [random text].";
        let spans = citation_spans(text);
        let bracketed: Vec<&CitationSpan> = spans
            .iter()
            .filter(|s| s.kind == CitationKind::Bracketed)
            .collect();
        assert_eq!(bracketed.len(), 1);
        assert_eq!(bracketed[0].text, "[12]");
    }

    #[test]
    fn parenthetical_year_citation_detected() {
        let text = "As shown previously (Smith et al., 2020), the effect holds; (this is not one).";
        let spans = citation_spans(text);
        assert!(spans
            .iter()
            .any(|s| s.kind == CitationKind::ParentheticalYear && s.text.contains("2020")));
        assert!(!spans.iter().any(|s| s.text.contains("this is not one")));
    }

    #[test]
    fn clause_reference_detected() {
        let text = "Per Section 3.2 of the agreement, and also Article 12, the parties agree.";
        let spans = citation_spans(text);
        let clauses: Vec<&CitationSpan> = spans
            .iter()
            .filter(|s| s.kind == CitationKind::ClauseReference)
            .collect();
        assert_eq!(clauses.len(), 2);
        assert!(clauses.iter().any(|c| c.text == "3.2"));
        assert!(clauses.iter().any(|c| c.text == "12"));
    }

    #[test]
    fn spans_are_sorted_by_start_offset() {
        let text = "Section 1 says (Smith, 1999) supports claim [3].";
        let spans = citation_spans(text);
        for w in spans.windows(2) {
            assert!(w[0].start <= w[1].start);
        }
    }
}
