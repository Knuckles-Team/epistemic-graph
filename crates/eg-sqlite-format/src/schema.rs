//! `sqlite_schema` row shape + a minimal `CREATE TABLE (…)` column-list scanner.
//! This is NOT a SQL parser: it splits the top-level paren-delimited, comma-separated
//! column list (respecting nested parens for `DECIMAL(10,2)` and quoted identifiers),
//! taking each entry's first token as the column name and the remainder as its declared
//! type + constraints. Only the type token feeds SQLite affinity mapping downstream.

use crate::error::{Error, Result};
use crate::value::{ColumnDef, Value};

/// One decoded `sqlite_schema` row.
#[derive(Debug, Clone)]
pub struct SchemaRow {
    pub kind: String, // "table" | "index" | "view" | "trigger"
    pub name: String,
    #[allow(dead_code)] // part of the sqlite_schema row shape; not needed by the importer
    pub tbl_name: String,
    pub root_page: i64,
    pub sql: String,
}

impl SchemaRow {
    /// Build a `SchemaRow` from a decoded `sqlite_schema` record's 5 columns.
    pub fn from_record(cols: &[Value]) -> Result<Self> {
        if cols.len() < 5 {
            return Err(Error::corrupt("sqlite_schema row has < 5 columns"));
        }
        let text = |v: &Value| -> String {
            match v {
                Value::Text(s) => s.clone(),
                _ => String::new(),
            }
        };
        let root_page = match &cols[3] {
            Value::Integer(i) => *i,
            _ => 0,
        };
        Ok(SchemaRow {
            kind: text(&cols[0]),
            name: text(&cols[1]),
            tbl_name: text(&cols[2]),
            root_page,
            sql: text(&cols[4]),
        })
    }
}

/// True if the stored DDL declares a `WITHOUT ROWID` table (unsupported — index-b-tree
/// storage with a different cell shape and key-ordering rule).
pub fn is_without_rowid(sql: &str) -> bool {
    // Case-insensitive search for the `WITHOUT ROWID` table option (outside quotes).
    let upper = sql.to_ascii_uppercase();
    // Collapse whitespace so "WITHOUT   ROWID" still matches.
    let collapsed: String = upper.split_whitespace().collect::<Vec<_>>().join(" ");
    collapsed.contains("WITHOUT ROWID")
}

/// Parse the column list out of a `CREATE TABLE name (col decl, …)` statement.
pub fn parse_columns(sql: &str) -> Result<Vec<ColumnDef>> {
    let bytes = sql.as_bytes();
    // Find the first top-level '(' after CREATE TABLE.
    let open = sql
        .find('(')
        .ok_or_else(|| Error::corrupt("CREATE TABLE has no column list"))?;
    // Find the MATCHING closing paren for that open paren (track depth + quote state).
    let mut depth = 0i32;
    let mut close = None;
    let mut i = open;
    let mut quote: Option<u8> = None;
    while i < bytes.len() {
        let c = bytes[i];
        match quote {
            Some(q) => {
                if c == q {
                    // Doubled quote is an escape inside the same string.
                    if i + 1 < bytes.len() && bytes[i + 1] == q {
                        i += 1;
                    } else {
                        quote = None;
                    }
                }
            }
            None => match c {
                b'\'' | b'"' | b'`' => quote = Some(c),
                b'[' => quote = Some(b']'),
                b'(' => depth += 1,
                b')' => {
                    depth -= 1;
                    if depth == 0 {
                        close = Some(i);
                        break;
                    }
                }
                _ => {}
            },
        }
        i += 1;
    }
    let close = close.ok_or_else(|| Error::corrupt("unbalanced CREATE TABLE parens"))?;
    let inner = &sql[open + 1..close];

    // Split on top-level commas.
    let entries = split_top_level(inner);
    let mut cols = Vec::new();
    for entry in entries {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        // A table-level constraint (PRIMARY KEY / UNIQUE / CHECK / FOREIGN KEY / CONSTRAINT)
        // is not a column definition — skip it.
        let first = first_token(entry);
        let upper_first = first.0.to_ascii_uppercase();
        if matches!(
            upper_first.as_str(),
            "PRIMARY" | "UNIQUE" | "CHECK" | "FOREIGN" | "CONSTRAINT"
        ) {
            continue;
        }
        cols.push(ColumnDef {
            name: unquote(&first.0),
            decl_type: type_token(first.1),
        });
    }
    if cols.is_empty() {
        return Err(Error::corrupt("CREATE TABLE has no columns"));
    }
    Ok(cols)
}

/// Split `s` on commas that are not inside parens or quotes.
fn split_top_level(s: &str) -> Vec<String> {
    let bytes = s.as_bytes();
    let mut out = Vec::new();
    let mut start = 0;
    let mut depth = 0i32;
    let mut quote: Option<u8> = None;
    let mut i = 0;
    while i < bytes.len() {
        let c = bytes[i];
        match quote {
            Some(q) => {
                if c == q {
                    if i + 1 < bytes.len() && bytes[i + 1] == q {
                        i += 1;
                    } else {
                        quote = None;
                    }
                }
            }
            None => match c {
                b'\'' | b'"' | b'`' => quote = Some(c),
                b'[' => quote = Some(b']'),
                b'(' => depth += 1,
                b')' => depth -= 1,
                b',' if depth == 0 => {
                    out.push(s[start..i].to_string());
                    start = i + 1;
                }
                _ => {}
            },
        }
        i += 1;
    }
    out.push(s[start..].to_string());
    out
}

/// Return `(first_token, rest)` splitting on the first top-level whitespace, but a quoted
/// identifier ('...', "...", `...`, [...]) counts as one token.
fn first_token(s: &str) -> (String, &str) {
    let s = s.trim_start();
    let bytes = s.as_bytes();
    if bytes.is_empty() {
        return (String::new(), "");
    }
    let opener = bytes[0];
    let closer = match opener {
        b'\'' => Some(b'\''),
        b'"' => Some(b'"'),
        b'`' => Some(b'`'),
        b'[' => Some(b']'),
        _ => None,
    };
    if let Some(closer) = closer {
        // Consume the quoted identifier.
        let mut i = 1;
        while i < bytes.len() {
            if bytes[i] == closer {
                if opener != b'[' && i + 1 < bytes.len() && bytes[i + 1] == closer {
                    i += 2;
                    continue;
                }
                let token = &s[..=i];
                let rest = &s[i + 1..];
                return (token.to_string(), rest);
            }
            i += 1;
        }
        return (s.to_string(), "");
    }
    // Unquoted: token runs until whitespace.
    match s.find(char::is_whitespace) {
        Some(pos) => (s[..pos].to_string(), &s[pos..]),
        None => (s.to_string(), ""),
    }
}

/// Extract the declared-type token(s) from the remainder after a column name — everything
/// up to the first column-constraint keyword. We only need it for affinity, so returning
/// the leading type words (or the whole remainder) is sufficient.
fn type_token(rest: &str) -> String {
    let rest = rest.trim();
    if rest.is_empty() {
        return String::new();
    }
    // The type name is the leading run of identifier chars / parenthesised size, ending at
    // the first constraint keyword. Grab up to the first constraint token.
    let mut result = String::new();
    for tok in rest.split_whitespace() {
        let up = tok.to_ascii_uppercase();
        if matches!(
            up.as_str(),
            "PRIMARY"
                | "NOT"
                | "NULL"
                | "UNIQUE"
                | "CHECK"
                | "DEFAULT"
                | "REFERENCES"
                | "COLLATE"
                | "GENERATED"
                | "AS"
                | "CONSTRAINT"
                | "AUTOINCREMENT"
        ) {
            break;
        }
        if !result.is_empty() {
            result.push(' ');
        }
        result.push_str(tok);
    }
    result
}

/// Strip surrounding SQL identifier quotes and unescape doubled quotes.
fn unquote(name: &str) -> String {
    let bytes = name.as_bytes();
    if bytes.len() >= 2 {
        let (open, close) = (bytes[0], bytes[bytes.len() - 1]);
        let matched = matches!(
            (open, close),
            (b'"', b'"') | (b'\'', b'\'') | (b'`', b'`') | (b'[', b']')
        );
        if matched {
            let inner = &name[1..name.len() - 1];
            if open == b'[' {
                return inner.to_string();
            }
            let esc = [open, open];
            let esc = std::str::from_utf8(&esc).unwrap();
            return inner.replace(esc, std::str::from_utf8(&[open]).unwrap());
        }
    }
    name.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_basic_columns() {
        let cols =
            parse_columns("CREATE TABLE people (id INTEGER, name TEXT, score REAL, blob_col BLOB)")
                .unwrap();
        let names: Vec<_> = cols.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, ["id", "name", "score", "blob_col"]);
        assert_eq!(cols[0].decl_type, "INTEGER");
        assert_eq!(cols[2].decl_type, "REAL");
    }

    #[test]
    fn parse_quoted_and_sized_and_constraints() {
        let cols = parse_columns(
            "CREATE TABLE \"t\" (\"a b\" DECIMAL(10,2) NOT NULL, c VARCHAR(20) DEFAULT 'x', PRIMARY KEY(c))",
        )
        .unwrap();
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[0].name, "a b");
        assert_eq!(cols[0].decl_type, "DECIMAL(10,2)");
        assert_eq!(cols[1].name, "c");
        assert_eq!(cols[1].decl_type, "VARCHAR(20)");
    }

    #[test]
    fn detects_without_rowid() {
        assert!(is_without_rowid("CREATE TABLE t (a, b) WITHOUT ROWID"));
        assert!(is_without_rowid("CREATE TABLE t (a, b) without   rowid"));
        assert!(!is_without_rowid("CREATE TABLE t (a INTEGER, b TEXT)"));
    }
}
