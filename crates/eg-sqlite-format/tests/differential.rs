//! Differential conformance tests against the REAL `sqlite3` CLI (the non-optional bar):
//! the Reader must decode `sqlite3`-authored fixtures identically, and the Writer's output
//! must pass `PRAGMA integrity_check` and read back byte-for-byte through the real CLI.
//! Every test that needs the CLI skips (with a message) when `sqlite3` is not on `$PATH`.

use std::path::Path;
use std::process::Command;

use eg_sqlite_format::{ColumnDef, Reader, Row, Value, Writer};

fn sqlite3_available() -> bool {
    Command::new("sqlite3")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

fn run_sqlite(db: &Path, sql: &str) -> String {
    let out = Command::new("sqlite3")
        .arg(db)
        .arg(sql)
        .output()
        .expect("spawn sqlite3");
    assert!(
        out.status.success(),
        "sqlite3 failed for {sql:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8_lossy(&out.stdout).to_string()
}

fn cd(name: &str, ty: &str) -> ColumnDef {
    ColumnDef {
        name: name.to_string(),
        decl_type: ty.to_string(),
    }
}

// ── Reader: decode a real sqlite3-authored fixture ────────────────────────────────

#[test]
fn reader_matches_sqlite3_fixture() {
    if !sqlite3_available() {
        eprintln!("SKIP reader_matches_sqlite3_fixture: sqlite3 not on PATH");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("fixture.db");
    run_sqlite(
        &db,
        "CREATE TABLE people (id INTEGER, name TEXT, score REAL, flag INTEGER, data BLOB);
         INSERT INTO people VALUES (1,'alice',9.5,1,x'0102');
         INSERT INTO people VALUES (2,'bob',NULL,0,NULL);
         CREATE TABLE big (id INTEGER, txt TEXT);
         INSERT INTO big VALUES (1, hex(randomblob(8000)));
         CREATE TABLE nums (n INTEGER, label TEXT);
         WITH RECURSIVE c(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM c WHERE n<3000)
           INSERT INTO nums SELECT n, 'row'||n FROM c;",
    );

    let r = Reader::open(&db).unwrap();
    assert_eq!(r.list_tables().unwrap(), vec!["big", "nums", "people"]);

    let cols: Vec<String> = r
        .table_columns("people")
        .unwrap()
        .into_iter()
        .map(|c| c.name)
        .collect();
    assert_eq!(cols, ["id", "name", "score", "flag", "data"]);

    let rows = r.scan_table("people").unwrap();
    assert_eq!(
        rows[0],
        vec![
            Value::Integer(1),
            Value::Text("alice".into()),
            Value::Real(9.5),
            Value::Integer(1),
            Value::Blob(vec![1, 2]),
        ]
    );
    assert_eq!(
        rows[1],
        vec![
            Value::Integer(2),
            Value::Text("bob".into()),
            Value::Null,
            Value::Integer(0),
            Value::Null,
        ]
    );

    // Overflow: a 16000-char TEXT must reconstruct across its overflow chain.
    assert_eq!(r.table_row_count("big").unwrap(), 1);
    let big = r.scan_table("big").unwrap();
    match &big[0][1] {
        Value::Text(t) => assert_eq!(t.len(), 16_000),
        other => panic!("expected large text, got {other:?}"),
    }

    // Multi-leaf + interior b-tree.
    assert_eq!(r.table_row_count("nums").unwrap(), 3000);
    let nums = r.scan_table("nums").unwrap();
    assert_eq!(nums.len(), 3000);
    assert_eq!(nums[0], vec![Value::Integer(1), Value::Text("row1".into())]);
    assert_eq!(
        nums[2999],
        vec![Value::Integer(3000), Value::Text("row3000".into())]
    );
}

#[test]
fn reader_rejects_without_rowid() {
    if !sqlite3_available() {
        eprintln!("SKIP reader_rejects_without_rowid: sqlite3 not on PATH");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("wr.db");
    run_sqlite(
        &db,
        "CREATE TABLE t (k TEXT PRIMARY KEY, v INTEGER) WITHOUT ROWID;
         INSERT INTO t VALUES ('a', 1);",
    );
    let r = Reader::open(&db).unwrap();
    // Listed, but any decode must be a typed Unsupported error, not garbage.
    assert_eq!(r.list_tables().unwrap(), vec!["t"]);
    assert!(r.scan_table("t").is_err());
    assert!(r.table_columns("t").is_err());
}

// ── Writer: differential integrity_check + round-trip ─────────────────────────────

#[test]
fn writer_passes_sqlite3_integrity_check() {
    if !sqlite3_available() {
        eprintln!("SKIP writer_passes_sqlite3_integrity_check: sqlite3 not on PATH");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("out.db");

    let mut w = Writer::create(&db, 4096).unwrap();
    w.add_table(
        "people",
        &[
            cd("id", "INTEGER"),
            cd("name", "TEXT"),
            cd("score", "REAL"),
            cd("data", "BLOB"),
        ],
    )
    .unwrap();
    w.insert_rows(
        "people",
        &[
            vec![
                Value::Integer(1),
                Value::Text("alice".into()),
                Value::Real(9.5),
                Value::Blob(vec![1, 2]),
            ],
            vec![
                Value::Integer(2),
                Value::Text("bob".into()),
                Value::Null,
                Value::Null,
            ],
        ],
    )
    .unwrap();

    // Overflow-forcing table.
    w.add_table("big", &[cd("id", "INTEGER"), cd("txt", "TEXT")])
        .unwrap();
    let big_text = "x".repeat(20_000);
    w.insert_rows(
        "big",
        &[vec![Value::Integer(1), Value::Text(big_text.clone())]],
    )
    .unwrap();

    // Multi-leaf + interior-level table.
    w.add_table("nums", &[cd("n", "INTEGER"), cd("label", "TEXT")])
        .unwrap();
    let many: Vec<Row> = (1..=5000)
        .map(|i| vec![Value::Integer(i), Value::Text(format!("row{i}"))])
        .collect();
    w.insert_rows("nums", &many).unwrap();

    w.finish().unwrap();

    // THE conformance bar: real sqlite3 integrity_check must say exactly "ok".
    assert_eq!(
        run_sqlite(&db, "PRAGMA integrity_check;").trim(),
        "ok",
        "writer produced a file failing sqlite3 integrity_check"
    );

    // Schema and SELECTs, diffed against the real CLI. (`.schema` reformats with an
    // injected `IF NOT EXISTS`; compare the exact stored DDL from sqlite_master instead.)
    assert_eq!(
        run_sqlite(&db, "SELECT sql FROM sqlite_master WHERE name='people';").trim(),
        "CREATE TABLE \"people\" (\"id\" INTEGER, \"name\" TEXT, \"score\" REAL, \"data\" BLOB)"
    );
    let people = run_sqlite(
        &db,
        "SELECT id,name,ifnull(score,'NULL'),quote(data) FROM people ORDER BY id;",
    );
    let people: Vec<&str> = people.lines().collect();
    assert_eq!(people[0], "1|alice|9.5|X'0102'");
    assert_eq!(people[1], "2|bob|NULL|NULL");

    assert_eq!(
        run_sqlite(&db, "SELECT length(txt) FROM big;").trim(),
        "20000"
    );
    assert_eq!(
        run_sqlite(&db, "SELECT count(*),min(n),max(n) FROM nums;").trim(),
        "5000|1|5000"
    );
    assert_eq!(
        run_sqlite(&db, "SELECT label FROM nums WHERE n=2500;").trim(),
        "row2500"
    );

    // And our own Reader round-trips its own Writer's output.
    let r = Reader::open(&db).unwrap();
    assert_eq!(r.scan_table("nums").unwrap().len(), 5000);
    match &r.scan_table("big").unwrap()[0][1] {
        Value::Text(t) => assert_eq!(t.len(), 20_000),
        other => panic!("expected large text, got {other:?}"),
    }
}

#[test]
fn writer_empty_table_and_single_row() {
    if !sqlite3_available() {
        eprintln!("SKIP writer_empty_table_and_single_row: sqlite3 not on PATH");
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("edge.db");
    let mut w = Writer::create(&db, 4096).unwrap();
    w.add_table("empty", &[cd("a", "INTEGER")]).unwrap();
    w.add_table("one", &[cd("a", "INTEGER"), cd("b", "TEXT")])
        .unwrap();
    w.insert_rows("one", &[vec![Value::Integer(42), Value::Text("hi".into())]])
        .unwrap();
    w.finish().unwrap();

    assert_eq!(run_sqlite(&db, "PRAGMA integrity_check;").trim(), "ok");
    assert_eq!(run_sqlite(&db, "SELECT count(*) FROM empty;").trim(), "0");
    assert_eq!(run_sqlite(&db, "SELECT a,b FROM one;").trim(), "42|hi");
}
