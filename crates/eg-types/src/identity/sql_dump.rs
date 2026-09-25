//! The administrator's SQL dump of the identity store: standard Postgres DDL
//! plus one INSERT per row, over the SAME redacted relations the SQL
//! projection serves -- so a dump never carries a password hash, a token or
//! session hash, a sealed secret or a recovery code. Sessions and the
//! throttle are volatile and are not dumped.
//!
//! [`parse_dump`] reads back exactly this dialect (and nothing else): DDL and
//! transaction statements are skipped, INSERTs into `identity.<relation>` are
//! decoded, anything else is refused.

use serde_json::Value;

use super::relations::SqlRelation;
use super::IdentityRefusal;

/// Relations a dump leaves out (volatile runtime state).
const NOT_DUMPED: [&str; 2] = ["sessions", "throttle"];
/// Largest dump accepted for import.
pub const MAX_DUMP_BYTES: usize = 16 * 1024 * 1024;

fn literal(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::Bool(true) => "TRUE".to_string(),
        Value::Bool(false) => "FALSE".to_string(),
        Value::Number(number) => number.to_string(),
        Value::String(text) => format!("'{}'", text.replace('\'', "''")),
        Value::Array(_) | Value::Object(_) => {
            format!("'{}'", value.to_string().replace('\'', "''"))
        }
    }
}

fn create_table(relation: &SqlRelation) -> String {
    let columns: Vec<String> = relation
        .columns
        .iter()
        .map(|(name, kind)| format!("  {name} {}", kind.postgres()))
        .collect();
    format!(
        "CREATE TABLE identity.{} (\n{}\n);\n",
        relation.name,
        columns.join(",\n")
    )
}

fn inserts(relation: &SqlRelation, out: &mut String) {
    let names: Vec<&str> = relation.columns.iter().map(|(name, _)| *name).collect();
    for row in &relation.rows {
        let values: Vec<String> = row.iter().map(literal).collect();
        out.push_str(&format!(
            "INSERT INTO identity.{} ({}) VALUES ({});\n",
            relation.name,
            names.join(", "),
            values.join(", ")
        ));
    }
}

/// Render `relations` as a Postgres dump.
pub fn render_dump(relations: &[SqlRelation]) -> String {
    let mut out = String::from(
        "-- epistemic-graph identity store dump (redacted: no password hashes, tokens,\n\
         -- session ids or sealed secrets). Restored humans must reset their password.\n\
         BEGIN;\nCREATE SCHEMA IF NOT EXISTS identity;\n",
    );
    let dumped = relations
        .iter()
        .filter(|relation| !NOT_DUMPED.contains(&relation.name));
    for relation in dumped {
        out.push_str(&create_table(relation));
        inserts(relation, &mut out);
    }
    out.push_str("COMMIT;\n");
    out
}

/// One decoded INSERT: relation, column names, values.
#[derive(Debug, Clone, PartialEq)]
pub struct DumpRow {
    pub relation: String,
    pub columns: Vec<String>,
    pub values: Vec<Value>,
}

impl DumpRow {
    /// The value of `column`, or `Null`.
    pub fn get(&self, column: &str) -> &Value {
        self.columns
            .iter()
            .position(|name| name == column)
            .and_then(|index| self.values.get(index))
            .unwrap_or(&Value::Null)
    }

    pub fn text(&self, column: &str) -> Option<String> {
        self.get(column).as_str().map(str::to_string)
    }
}

/// Split `text` into statements at `;` outside single quotes, dropping `--`
/// comment lines.
fn statements(text: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();
    let mut quoted = false;
    for line in text
        .lines()
        .filter(|line| !line.trim_start().starts_with("--"))
    {
        for character in line.chars() {
            if character == '\'' {
                quoted = !quoted;
            }
            if character == ';' && !quoted {
                out.push(std::mem::take(&mut current));
            } else {
                current.push(character);
            }
        }
        current.push('\n');
    }
    out.push(current);
    out.into_iter()
        .map(|statement| statement.trim().to_string())
        .collect()
}

/// Split a parenthesised list at commas outside single quotes.
fn split_list(body: &str) -> Vec<String> {
    let mut items = Vec::new();
    let mut current = String::new();
    let mut quoted = false;
    for character in body.chars() {
        if character == '\'' {
            quoted = !quoted;
        }
        if character == ',' && !quoted {
            items.push(std::mem::take(&mut current).trim().to_string());
        } else {
            current.push(character);
        }
    }
    items.push(current.trim().to_string());
    items
}

fn parse_literal(token: &str) -> Result<Value, IdentityRefusal> {
    match token {
        "NULL" => Ok(Value::Null),
        "TRUE" => Ok(Value::Bool(true)),
        "FALSE" => Ok(Value::Bool(false)),
        quoted if quoted.len() >= 2 && quoted.starts_with('\'') && quoted.ends_with('\'') => Ok(
            Value::String(quoted[1..quoted.len() - 1].replace("''", "'")),
        ),
        number => number
            .parse::<u64>()
            .map(Value::from)
            .map_err(|_| IdentityRefusal::InvalidRequest),
    }
}

/// `(<inner>)` → `inner`.
fn parenthesised(text: &str) -> Option<&str> {
    text.trim().strip_prefix('(')?.strip_suffix(')')
}

fn parse_insert(statement: &str) -> Result<DumpRow, IdentityRefusal> {
    let invalid = IdentityRefusal::InvalidRequest;
    let rest = statement
        .strip_prefix("INSERT INTO identity.")
        .ok_or(invalid)?;
    let (relation, rest) = rest.split_once(' ').ok_or(invalid)?;
    let (columns, values) = rest.split_once(" VALUES ").ok_or(invalid)?;
    let columns = split_list(parenthesised(columns).ok_or(invalid)?);
    let values = split_list(parenthesised(values).ok_or(invalid)?)
        .iter()
        .map(|token| parse_literal(token))
        .collect::<Result<Vec<_>, _>>()?;
    if columns.len() != values.len() {
        return Err(invalid);
    }
    Ok(DumpRow {
        relation: relation.to_string(),
        columns,
        values,
    })
}

/// Whether a statement is dump framing the importer skips.
fn is_framing(statement: &str) -> bool {
    statement.is_empty()
        || statement == "BEGIN"
        || statement == "COMMIT"
        || statement.starts_with("CREATE SCHEMA ")
        || statement.starts_with("CREATE TABLE identity.")
}

/// Decode every INSERT of a dump produced by [`render_dump`].
pub fn parse_dump(text: &str) -> Result<Vec<DumpRow>, IdentityRefusal> {
    if text.len() > MAX_DUMP_BYTES {
        return Err(IdentityRefusal::InvalidRequest);
    }
    statements(text)
        .iter()
        .filter(|statement| !is_framing(statement))
        .map(|statement| parse_insert(statement))
        .collect()
}
