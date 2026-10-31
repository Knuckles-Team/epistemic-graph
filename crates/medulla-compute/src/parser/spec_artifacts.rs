//! Deterministic parser for spec artifacts (MEDULLA-SDLC-R014.1).
//!
//! Reads the Markdown files of one spec directory and emits typed RDF triples
//! for requirement table rows, headings, task lines, `Spec:` trailers and test
//! bindings. The parser makes no model call and touches no state beyond
//! reading the given directory. Files are visited in sorted path order and the
//! output is sorted and deduplicated, so the same bytes give the same triples.
//!
//! A task line in a `tasks.md` file that matches no known form refuses with
//! `SPEC_ARTIFACT_UNPARSEABLE`, naming the file and the line.

use std::fmt;
use std::path::{Path, PathBuf};

use medulla_types::contract::EngineErrorCode;
use medulla_types::ontology_inspection::{RdfObject, RdfTriple};

/// Namespace of the spec-driven development vocabulary.
pub const SDD_NS: &str = "urn:medulla:sdd:";
const RDF_TYPE: &str = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type";
const XSD_BOOLEAN: &str = "http://www.w3.org/2001/XMLSchema#boolean";
const XSD_INTEGER: &str = "http://www.w3.org/2001/XMLSchema#integer";

/// Why a spec directory did not parse. Nothing is emitted for a refused directory.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SpecArtifactRefusal {
    /// A line matched no artifact form.
    Unparseable { file: String, line: usize },
    /// The directory or one of its files could not be read.
    Unreadable { path: String, message: String },
}

impl SpecArtifactRefusal {
    /// The refusal code this refusal is reported under.
    pub fn code(&self) -> EngineErrorCode {
        match self {
            Self::Unparseable { .. } => EngineErrorCode::SpecArtifactUnparseable,
            Self::Unreadable { .. } => EngineErrorCode::InvalidArgument,
        }
    }
}

impl fmt::Display for SpecArtifactRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unparseable { file, line } => write!(f, "{}: {file}:{line}", self.code()),
            Self::Unreadable { path, message } => write!(f, "{}: {path}: {message}", self.code()),
        }
    }
}

impl std::error::Error for SpecArtifactRefusal {}

/// One parsed task line.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TaskLine {
    pub task_id: Option<String>,
    pub parallel: bool,
    pub story: Option<String>,
    pub row: Option<String>,
    pub ticked: bool,
    pub text: String,
}

/// The kind of one Markdown line.
enum LineKind<'a> {
    Heading(usize, &'a str),
    TableRow(&'a str),
    Task(bool, &'a str),
    Trailer(&'a str),
    Other,
}

/// Prefix dispatch table: the first matching prefix decides the line kind.
const PREFIXES: &[(&str, fn(&str) -> LineKind<'_>)] = &[
    ("- [ ] ", open_task),
    ("- [x] ", ticked_task),
    ("- [X] ", ticked_task),
    ("Spec: ", trailer),
    ("|", table_row),
    ("#", heading),
];

fn trailer(rest: &str) -> LineKind<'_> {
    LineKind::Trailer(rest)
}

fn table_row(rest: &str) -> LineKind<'_> {
    LineKind::TableRow(rest)
}

fn open_task(rest: &str) -> LineKind<'_> {
    LineKind::Task(false, rest)
}

fn ticked_task(rest: &str) -> LineKind<'_> {
    LineKind::Task(true, rest)
}

fn heading(rest: &str) -> LineKind<'_> {
    let hashes = 1 + rest.chars().take_while(|c| *c == '#').count();
    match rest[hashes - 1..].strip_prefix(' ') {
        Some(text) if hashes <= 6 => LineKind::Heading(hashes, text.trim()),
        _ => LineKind::Other,
    }
}

fn classify(line: &str) -> LineKind<'_> {
    PREFIXES
        .iter()
        .find_map(|(prefix, make)| line.strip_prefix(prefix).map(make))
        .unwrap_or(LineKind::Other)
}

/// Parse every `*.md` file under `dir` (recursively) into sorted, unique triples.
pub fn parse_spec_dir(dir: &Path) -> Result<Vec<RdfTriple>, SpecArtifactRefusal> {
    let mut files = Vec::new();
    collect_markdown(dir, &mut files)?;
    files.sort();
    let mut triples = Vec::new();
    for path in files {
        let rel = relative_name(dir, &path);
        let text = std::fs::read_to_string(&path).map_err(|e| unreadable(&path, &e))?;
        triples.extend(parse_spec_file(&rel, &text)?);
    }
    sort_triples(&mut triples);
    Ok(triples)
}

/// Parse one artifact file's text. `rel` names the file in subjects and refusals.
pub fn parse_spec_file(rel: &str, text: &str) -> Result<Vec<RdfTriple>, SpecArtifactRefusal> {
    let file = format!("{SDD_NS}file:{rel}");
    let is_tasks = rel.rsplit('/').next() == Some("tasks.md");
    let mut out = vec![typed(&file, "ArtifactFile")];
    for (index, raw) in text.lines().enumerate() {
        let line_no = index + 1;
        match classify(raw.trim_end()) {
            LineKind::Heading(level, title) => emit_heading(&mut out, &file, line_no, level, title),
            LineKind::TableRow(row) => emit_table_row(&mut out, &file, row),
            LineKind::Task(ticked, rest) if is_tasks => {
                let task = parse_task_line(ticked, rest).ok_or_else(|| {
                    SpecArtifactRefusal::Unparseable {
                        file: rel.to_string(),
                        line: line_no,
                    }
                })?;
                emit_task(&mut out, &file, line_no, &task);
            }
            LineKind::Trailer(ids) => emit_trailer(&mut out, &file, ids),
            LineKind::Task(..) | LineKind::Other => {}
        }
    }
    sort_triples(&mut out);
    Ok(out)
}

/// Parse the text after a task checkbox. `None` when it matches no task form.
///
/// Forms: `T001 [P] [US1] <ROW-ID> text` (flags and row optional) and
/// `<ROW-ID> text` (a row-only task).
pub fn parse_task_line(ticked: bool, rest: &str) -> Option<TaskLine> {
    let mut tokens = rest.split_whitespace().peekable();
    let task_id = tokens.next_if(|t| is_task_id(t)).map(str::to_string);
    let parallel = tokens.next_if_eq(&"[P]").is_some();
    let story = tokens
        .next_if(|t| t.starts_with("[US") && t.ends_with(']'))
        .map(|t| t[1..t.len() - 1].to_string());
    let row = tokens
        .next_if(|t| is_row_id(strip_ticks(t)))
        .map(|t| strip_ticks(t).to_string());
    if task_id.is_none() && row.is_none() {
        return None;
    }
    let text = tokens.collect::<Vec<_>>().join(" ");
    let text = text.trim_start_matches(['—', '-', ':', ' ']).to_string();
    Some(TaskLine {
        task_id,
        parallel,
        story,
        row,
        ticked,
        text,
    })
}

fn is_task_id(token: &str) -> bool {
    token.len() >= 2 && token.starts_with('T') && token[1..].bytes().all(|b| b.is_ascii_digit())
}

/// `PREFIX-...-R<digits>(.<digits>)*`, upper-case prefix segments.
fn is_row_id(token: &str) -> bool {
    let Some((prefix, number)) = token.rsplit_once("-R") else {
        return false;
    };
    let prefix_ok = !prefix.is_empty()
        && prefix.starts_with(|c: char| c.is_ascii_uppercase())
        && prefix
            .bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit() || b == b'-');
    let number_ok = !number.is_empty()
        && number
            .split('.')
            .all(|part| !part.is_empty() && part.bytes().all(|b| b.is_ascii_digit()));
    prefix_ok && number_ok
}

fn strip_ticks(token: &str) -> &str {
    token.trim_matches('`')
}

fn emit_heading(out: &mut Vec<RdfTriple>, file: &str, line: usize, level: usize, title: &str) {
    let subject = format!("{file}#L{line}");
    out.push(typed(&subject, "Heading"));
    out.push(iri(&subject, "inFile", file));
    out.push(literal(
        &subject,
        "level",
        &level.to_string(),
        Some(XSD_INTEGER),
    ));
    out.push(literal(&subject, "title", title, None));
}

/// `| \`ID\` | **Title.** Body | Acceptance |`; other cells carry test bindings.
fn emit_table_row(out: &mut Vec<RdfTriple>, file: &str, row: &str) {
    let cells: Vec<&str> = row
        .trim_end_matches('|')
        .split('|')
        .map(str::trim)
        .collect();
    let Some(id) = cells
        .first()
        .and_then(|c| backticked(c))
        .filter(|id| is_row_id(id))
    else {
        return;
    };
    let subject = format!("{SDD_NS}req:{id}");
    out.push(typed(&subject, "Requirement"));
    out.push(literal(&subject, "rowId", id, None));
    out.push(iri(&subject, "declaredIn", file));
    if let Some((title, body)) = cells.get(1).and_then(|c| bold_title(c)) {
        out.push(literal(&subject, "title", title, None));
        out.push(literal(&subject, "body", body, None));
        if let Some(acceptance) = cells.get(2) {
            out.push(literal(&subject, "acceptance", acceptance, None));
        }
    }
    for binding in cells.iter().skip(1).flat_map(|c| ticked_spans(c)) {
        if binding.contains("::") {
            out.push(literal(&subject, "testBinding", binding, None));
        }
    }
}

fn emit_task(out: &mut Vec<RdfTriple>, file: &str, line: usize, task: &TaskLine) {
    let key = task.task_id.clone().unwrap_or_else(|| format!("L{line}"));
    let subject = format!("{file}#{key}");
    out.push(typed(&subject, "Task"));
    out.push(iri(&subject, "inFile", file));
    out.push(literal(
        &subject,
        "line",
        &line.to_string(),
        Some(XSD_INTEGER),
    ));
    out.push(literal(
        &subject,
        "parallel",
        bool_lex(task.parallel),
        Some(XSD_BOOLEAN),
    ));
    out.push(literal(
        &subject,
        "ticked",
        bool_lex(task.ticked),
        Some(XSD_BOOLEAN),
    ));
    out.push(literal(&subject, "text", &task.text, None));
    if let Some(id) = &task.task_id {
        out.push(literal(&subject, "taskId", id, None));
    }
    if let Some(story) = &task.story {
        out.push(literal(&subject, "story", story, None));
    }
    if let Some(row) = &task.row {
        out.push(iri(&subject, "refersTo", &format!("{SDD_NS}req:{row}")));
    }
}

fn emit_trailer(out: &mut Vec<RdfTriple>, file: &str, ids: &str) {
    for id in ids
        .split([',', ' '])
        .map(strip_ticks)
        .filter(|id| is_row_id(id))
    {
        out.push(iri(file, "citesRequirement", &format!("{SDD_NS}req:{id}")));
    }
}

fn backticked(cell: &str) -> Option<&str> {
    cell.strip_prefix('`')?.strip_suffix('`')
}

fn ticked_spans(cell: &str) -> impl Iterator<Item = &str> {
    cell.split('`').skip(1).step_by(2)
}

/// `**Title.** Body` → (`Title`, `Body`).
fn bold_title(cell: &str) -> Option<(&str, &str)> {
    let (title, body) = cell.strip_prefix("**")?.split_once("**")?;
    Some((title.trim_end_matches('.').trim(), body.trim()))
}

fn bool_lex(value: bool) -> &'static str {
    if value {
        "true"
    } else {
        "false"
    }
}

fn typed(subject: &str, class: &str) -> RdfTriple {
    RdfTriple {
        subject: subject.to_string(),
        predicate: RDF_TYPE.to_string(),
        object: RdfObject::Iri {
            iri: format!("{SDD_NS}{class}"),
        },
    }
}

fn iri(subject: &str, property: &str, object: &str) -> RdfTriple {
    RdfTriple {
        subject: subject.to_string(),
        predicate: format!("{SDD_NS}{property}"),
        object: RdfObject::Iri {
            iri: object.to_string(),
        },
    }
}

fn literal(subject: &str, property: &str, lexical: &str, datatype: Option<&str>) -> RdfTriple {
    RdfTriple {
        subject: subject.to_string(),
        predicate: format!("{SDD_NS}{property}"),
        object: RdfObject::Literal {
            lexical: lexical.to_string(),
            datatype: datatype.map(str::to_string),
            language: None,
        },
    }
}

fn object_key(object: &RdfObject) -> (u8, &str, &str, &str) {
    match object {
        RdfObject::Iri { iri } => (0, iri.as_str(), "", ""),
        RdfObject::Literal {
            lexical,
            datatype,
            language,
        } => (
            1,
            lexical.as_str(),
            datatype.as_deref().unwrap_or(""),
            language.as_deref().unwrap_or(""),
        ),
    }
}

fn sort_triples(triples: &mut Vec<RdfTriple>) {
    triples.sort_by(|a, b| {
        (&a.subject, &a.predicate, object_key(&a.object)).cmp(&(
            &b.subject,
            &b.predicate,
            object_key(&b.object),
        ))
    });
    triples.dedup();
}

fn collect_markdown(dir: &Path, files: &mut Vec<PathBuf>) -> Result<(), SpecArtifactRefusal> {
    let entries = std::fs::read_dir(dir).map_err(|e| unreadable(dir, &e))?;
    for entry in entries {
        let path = entry.map_err(|e| unreadable(dir, &e))?.path();
        if path.is_dir() {
            collect_markdown(&path, files)?;
        } else if path.extension().is_some_and(|ext| ext == "md") {
            files.push(path);
        }
    }
    Ok(())
}

fn relative_name(dir: &Path, path: &Path) -> String {
    let rel = path.strip_prefix(dir).unwrap_or(path);
    rel.components()
        .map(|c| c.as_os_str().to_string_lossy())
        .collect::<Vec<_>>()
        .join("/")
}

fn unreadable(path: &Path, error: &std::io::Error) -> SpecArtifactRefusal {
    SpecArtifactRefusal::Unreadable {
        path: path.display().to_string(),
        message: error.to_string(),
    }
}
