//! `iceberg('namespace.table')` end to end over a real partitioned table
//! (EG-FEDERATED-QUERY-R051): the provider is built by a sync caller, then scanned by
//! DataFusion on a different runtime, and the manifest planner prunes data files from the
//! filter DataFusion pushes at scan time.
//!
//! The table is written with the `iceberg` crate's own writers — a table partitioned by
//! `region`, one Parquet data file per partition, committed through a transaction — into
//! an in-memory store under `s3://` locations. An in-process server then plays the two
//! remote parties the provider talks to: the Iceberg REST catalog (`/v1/config` and the
//! table load) and the S3-compatible object store holding the table's files (`HEAD` and
//! ranged `GET`). Nothing is mocked inside the provider: it uses its real REST client and
//! its real object-store client against that server.
#![cfg(all(feature = "sql", feature = "iceberg-federation"))]

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion::catalog::TableProvider;
use datafusion::prelude::SessionContext;
use eg_query::sql::{build_iceberg_provider, ICEBERG_FEDERATION_CATALOG_URI_ENV};
use iceberg::io::{
    FileIO, MemoryStorageFactory, S3_ACCESS_KEY_ID, S3_DISABLE_CONFIG_LOAD,
    S3_DISABLE_EC2_METADATA, S3_ENDPOINT, S3_PATH_STYLE_ACCESS, S3_REGION, S3_SECRET_ACCESS_KEY,
};
use iceberg::memory::{MemoryCatalogBuilder, MEMORY_CATALOG_WAREHOUSE};
use iceberg::spec::{
    DataFile, DataFileFormat, Literal, NestedField, PartitionKey, PrimitiveType, Schema, Struct,
    Transform, Type, UnboundPartitionSpec,
};
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent, TableCreation};

const BUCKET: &str = "lake";
const TABLE_ROUTE: &str = "/v1/namespaces/sales/tables/facts";
/// One partition, and so one data file, per region; three rows each.
const REGIONS: [&str; 3] = ["east", "north", "west"];

// ── the fixture table ───────────────────────────────────────────────────────────────

/// Rows `(region, id, amount)` of one partition: ids `10 * index + 1..=3`, amount `100 * id`.
fn region_rows(index: usize) -> Vec<(i64, i64)> {
    (1..=3)
        .map(|n| {
            let id = 10 * index as i64 + n;
            (id, 100 * id)
        })
        .collect()
}

fn region_batch(schema: &SchemaRef, region: &str, index: usize) -> RecordBatch {
    let rows = region_rows(index);
    let columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(vec![region; rows.len()])),
        Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.0))),
        Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.1))),
    ];
    RecordBatch::try_new(schema.clone(), columns).expect("a batch in the table's schema")
}

/// One Parquet data file per region, each under its own partition key.
async fn write_partitions(table: &Table) -> Vec<DataFile> {
    let metadata = table.metadata();
    let schema = metadata.current_schema().clone();
    let arrow_schema: SchemaRef = Arc::new(
        iceberg::arrow::schema_to_arrow_schema(&schema).expect("the table's Arrow schema"),
    );
    let parquet = ParquetWriterBuilder::from_table_properties(
        &metadata.table_properties().expect("table properties"),
        schema.clone(),
    );
    let rolling = RollingFileWriterBuilder::new_with_default_file_size(
        parquet,
        table.file_io().clone(),
        DefaultLocationGenerator::new(metadata).expect("data location"),
        DefaultFileNameGenerator::new("part".to_string(), None, DataFileFormat::Parquet),
    );
    let builder = DataFileWriterBuilder::new(rolling);
    let mut files = Vec::new();
    for (index, region) in REGIONS.iter().enumerate() {
        let key = PartitionKey::new(
            metadata.default_partition_spec().as_ref().clone(),
            schema.clone(),
            Struct::from_iter([Some(Literal::string(region))]),
        );
        let mut writer = builder.build(Some(key)).await.expect("partition writer");
        writer
            .write(region_batch(&arrow_schema, region, index))
            .await
            .expect("write the partition's rows");
        files.extend(writer.close().await.expect("close the data file"));
    }
    files
}

/// `sales.facts(region, id, amount)` partitioned by `region`, with one committed snapshot
/// of three data files.
async fn write_fixture() -> Table {
    let catalog = MemoryCatalogBuilder::default()
        .with_storage_factory(Arc::new(MemoryStorageFactory))
        .load(
            "fixture",
            HashMap::from([(
                MEMORY_CATALOG_WAREHOUSE.to_string(),
                format!("s3://{BUCKET}/warehouse"),
            )]),
        )
        .await
        .expect("in-memory catalog");
    let namespace = NamespaceIdent::new("sales".to_string());
    catalog
        .create_namespace(&namespace, HashMap::new())
        .await
        .expect("namespace");
    let long = || Type::Primitive(PrimitiveType::Long);
    let schema = Schema::builder()
        .with_fields(vec![
            NestedField::required(1, "region", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::required(2, "id", long()).into(),
            NestedField::required(3, "amount", long()).into(),
        ])
        .build()
        .expect("schema");
    let by_region = UnboundPartitionSpec::builder()
        .add_partition_field(1, "region", Transform::Identity)
        .expect("partition field")
        .build();
    let creation = TableCreation::builder()
        .name("facts".to_string())
        .schema(schema)
        .partition_spec(by_region)
        .build();
    let table = catalog
        .create_table(&namespace, creation)
        .await
        .expect("create the table");
    let files = write_partitions(&table).await;
    let transaction = Transaction::new(&table);
    let append = transaction.fast_append().add_data_files(files);
    append
        .apply(transaction)
        .expect("stage the append")
        .commit(&catalog)
        .await
        .expect("commit the snapshot")
}

// ── the REST catalog and object store the provider talks to ─────────────────────────

/// One HTTP request as the server needs it.
struct Asked {
    method: String,
    path: String,
    range: Option<(usize, usize)>,
}

fn percent_decode(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut at = 0;
    while at < bytes.len() {
        let escaped = (bytes[at] == b'%')
            .then(|| text.get(at + 1..at + 3))
            .flatten()
            .and_then(|hex| u8::from_str_radix(hex, 16).ok());
        out.push(escaped.unwrap_or(bytes[at]));
        at += if escaped.is_some() { 3 } else { 1 };
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// `bytes=<first>-<last>` (both inclusive) or the open-ended `bytes=<first>-`.
fn parse_range(value: &str) -> Option<(usize, usize)> {
    let (first, last) = value.trim().strip_prefix("bytes=")?.split_once('-')?;
    Some((first.parse().ok()?, last.parse().unwrap_or(usize::MAX)))
}

fn read_request(stream: &TcpStream) -> Option<Asked> {
    let mut lines = BufReader::new(stream).lines();
    let request_line = lines.next()?.ok()?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next()?.to_string();
    let target = parts.next()?;
    let path = percent_decode(target.split('?').next()?);
    let mut range = None;
    for line in lines {
        let line = line.ok()?;
        if line.is_empty() {
            break;
        }
        if let Some((name, value)) = line.split_once(':') {
            if name.eq_ignore_ascii_case("range") {
                range = parse_range(value);
            }
        }
    }
    Some(Asked {
        method,
        path,
        range,
    })
}

/// What the server answers with.
struct Answer {
    status: &'static str,
    content_range: Option<String>,
    body: Vec<u8>,
}

impl Answer {
    fn ok(body: Vec<u8>) -> Self {
        Self {
            status: "200 OK",
            content_range: None,
            body,
        }
    }

    fn not_found() -> Self {
        Self {
            status: "404 Not Found",
            content_range: None,
            body: Vec::new(),
        }
    }

    /// The whole object, or the requested inclusive byte range of it.
    fn object(bytes: Vec<u8>, range: Option<(usize, usize)>) -> Self {
        let Some((first, last)) = range else {
            return Self::ok(bytes);
        };
        let last = last.min(bytes.len().saturating_sub(1));
        Self {
            status: "206 Partial Content",
            content_range: Some(format!("bytes {first}-{last}/{}", bytes.len())),
            body: bytes[first..=last].to_vec(),
        }
    }

    fn send(self, mut stream: &TcpStream, head_only: bool) {
        let mut head = format!(
            "HTTP/1.1 {}\r\nContent-Length: {}\r\nConnection: close\r\n",
            self.status,
            self.body.len()
        );
        if let Some(range) = &self.content_range {
            head.push_str(&format!("Content-Range: {range}\r\n"));
        }
        head.push_str("\r\n");
        let _ = stream.write_all(head.as_bytes());
        if !head_only {
            let _ = stream.write_all(&self.body);
        }
    }
}

/// The catalog and store of the fixture table.
struct Lake {
    base: String,
    /// Every object key the store was asked for.
    objects: Arc<Mutex<Vec<String>>>,
}

/// The table-load response: the committed metadata, its location, and the object-store
/// settings a catalog vends with a table. The credentials are placeholders the server
/// never checks.
fn load_table_response(table: &Table, endpoint: &str) -> Vec<u8> {
    let placeholder = |part: &str| format!("fixture-{part}-placeholder");
    let config = HashMap::from([
        (S3_ENDPOINT, endpoint.to_string()),
        (S3_PATH_STYLE_ACCESS, "true".to_string()),
        (S3_REGION, "us-east-1".to_string()),
        (S3_ACCESS_KEY_ID, placeholder("key-id")),
        (S3_SECRET_ACCESS_KEY, placeholder("key")),
        (S3_DISABLE_CONFIG_LOAD, "true".to_string()),
        (S3_DISABLE_EC2_METADATA, "true".to_string()),
    ]);
    serde_json::to_vec(&serde_json::json!({
        "metadata-location": table.metadata_location(),
        "metadata": table.metadata(),
        "config": config,
    }))
    .expect("table-load response")
}

impl Lake {
    fn serve(table: Table) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind the lake");
        let base = format!("http://{}", listener.local_addr().expect("lake address"));
        let objects = Arc::new(Mutex::new(Vec::new()));
        let (endpoint, asked) = (base.clone(), objects.clone());
        std::thread::spawn(move || {
            let reader = tokio::runtime::Builder::new_current_thread()
                .build()
                .expect("object reader runtime");
            let file_io = table.file_io().clone();
            for stream in listener.incoming() {
                let Ok(stream) = stream else { continue };
                let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
                let Some(request) = read_request(&stream) else {
                    continue;
                };
                let answer = if request.path == "/v1/config" {
                    Answer::ok(br#"{"defaults":{},"overrides":{}}"#.to_vec())
                } else if request.path == TABLE_ROUTE {
                    Answer::ok(load_table_response(&table, &endpoint))
                } else {
                    let object = reader.block_on(read_object(&file_io, &request.path, &asked));
                    object.map_or_else(Answer::not_found, |bytes| {
                        Answer::object(bytes, request.range)
                    })
                };
                answer.send(&stream, request.method == "HEAD");
            }
        });
        Self { base, objects }
    }

    /// The Parquet data files the store was asked for.
    fn data_files_asked(&self) -> Vec<String> {
        let asked = self.objects.lock().expect("lake record lock");
        asked
            .iter()
            .filter(|key| key.ends_with(".parquet"))
            .cloned()
            .collect()
    }
}

/// The bytes of `/<bucket>/<key>` from the fixture's store, recording the key.
async fn read_object(file_io: &FileIO, path: &str, asked: &Mutex<Vec<String>>) -> Option<Vec<u8>> {
    let key = path.strip_prefix(&format!("/{BUCKET}/"))?;
    asked
        .lock()
        .expect("lake record lock")
        .push(key.to_string());
    let input = file_io.new_input(format!("s3://{BUCKET}/{key}")).ok()?;
    input.read().await.ok().map(|bytes| bytes.to_vec())
}

// ── the scan ────────────────────────────────────────────────────────────────────────

/// `(id, amount)` rows of `sql`, in result order.
async fn id_amount(ctx: &SessionContext, sql: &str) -> Vec<(i64, i64)> {
    let batches = ctx
        .sql(sql)
        .await
        .expect("plan the query")
        .collect()
        .await
        .expect("run the query");
    let column = |batch: &RecordBatch, index: usize| -> Vec<i64> {
        let values = batch.column(index).as_any().downcast_ref::<Int64Array>();
        values.expect("an Int64 column").values().to_vec()
    };
    batches
        .iter()
        .flat_map(|batch| column(batch, 0).into_iter().zip(column(batch, 1)))
        .collect()
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(2)
        .build()
        .expect("runtime")
}

// spec: EG-FEDERATED-QUERY-R051
#[test]
fn a_partitioned_table_is_scanned_through_the_provider_with_files_pruned() {
    let writer = runtime();
    let lake = Lake::serve(writer.block_on(write_fixture()));
    std::env::set_var(ICEBERG_FEDERATION_CATALOG_URI_ENV, &lake.base);

    // Built by a sync caller outside any runtime, as the table function is at plan time.
    let provider = build_iceberg_provider("sales.facts", None).expect("load the table");
    let before = provider.pushdown_stats();
    assert_eq!(before.total_data_files, 3, "one data file per region");
    assert_eq!(before.columns_total, 3);
    assert!(
        lake.data_files_asked().is_empty(),
        "loading reads metadata only"
    );

    // Scanned on DataFusion's runtime: a different runtime, started after the load.
    let ctx = SessionContext::new();
    ctx.register_table("facts", provider.clone() as Arc<dyn TableProvider>)
        .expect("register the provider");
    let queries = runtime();
    let east = queries.block_on(id_amount(
        &ctx,
        "SELECT id, amount FROM facts WHERE region = 'east' ORDER BY id",
    ));
    assert_eq!(east, vec![(1, 100), (2, 200), (3, 300)]);
    let pruned = provider.pushdown_stats();
    assert_eq!(
        pruned.files_scanned, 1,
        "the pushed filter leaves one partition"
    );
    assert_eq!(pruned.files_skipped(), 2, "two data files were pruned");
    let asked = lake.data_files_asked();
    assert!(!asked.is_empty(), "the east data file was read");
    assert!(
        asked.iter().all(|key| key.contains("region=east")),
        "no pruned data file was requested from the store: {asked:?}"
    );

    // No filter: every file, and only the projected columns.
    let all = queries.block_on(id_amount(&ctx, "SELECT id, amount FROM facts ORDER BY id"));
    let expected: Vec<(i64, i64)> = (0..REGIONS.len()).flat_map(region_rows).collect();
    assert_eq!(all, expected, "nine rows over three partitions");
    let full = provider.pushdown_stats();
    assert_eq!((full.files_scanned, full.files_skipped()), (3, 0));
    assert_eq!(
        (full.columns_projected, full.columns_total),
        (2, 3),
        "the projection reached the scan"
    );

    // The provider outlives the runtime that first scanned it, and may be dropped on one.
    drop(queries);
    let later = runtime();
    let west = later.block_on(id_amount(
        &ctx,
        "SELECT id, amount FROM facts WHERE region = 'west' AND id > 21 ORDER BY id LIMIT 5",
    ));
    assert_eq!(west, vec![(22, 2200), (23, 2300)]);
    assert_eq!(provider.pushdown_stats().files_skipped(), 2);
    later.block_on(async move {
        drop(ctx);
        drop(provider);
    });
}
