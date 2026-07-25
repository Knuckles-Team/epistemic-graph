//! S3-compatible object-storage REST surface (CONCEPT:EG-KG.ontology.object-put-get-head) — an Amazon-S3 /
//! MinIO-shaped HTTP API so any S3 SDK, `aws s3` CLI, or `mc` client talks to the
//! engine's content-addressed BLOB store DIRECTLY.
//!
//! ## What this is (and is NOT)
//!
//! An ADAPTER, not a re-implemented object store. Object BYTES land in the engine's
//! content-addressed [`ChunkStore`](crate::server::blob::store::ChunkStore) BLOB
//! substrate (CONCEPT:EG-KG.storage.blob-namespace) — so identical objects dedup by sha256 — while the
//! bucket / object index (the listing metadata) lives in the durable KV surface
//! (CONCEPT:EG-KG.storage.namespaced-kv-surface), keyed for prefix-scan. Both are the engine's own durable
//! stores; nothing new is invented here.
//!
//! It is a HAND-ROLLED HTTP/1.1 listener over `tokio::net` — the SAME
//! dependency-free idiom as [`crate::server::obs`] / [`crate::server::sparql_http`]
//! / [`crate::metrics`] (NO axum/hyper/warp, so the Pi contract holds) — but its
//! own parser keeps the body as raw BYTES (S3 objects are binary) and captures the
//! request headers (for the auth guard). XML responses are hand-built strings in
//! the S3 shape.
//!
//! ## API subset (CONCEPT:EG-KG.ontology.object-put-get-head)
//!
//! LANDED: `PutObject` / `GetObject` / `HeadObject` / `DeleteObject`,
//! `ListObjectsV2` (+ v1 fallback, `prefix`), `CreateBucket` / `DeleteBucket` /
//! `HeadBucket` / `ListBuckets`, and a mandatory SigV4 auth guard. Requests verify
//! the canonical request, signed headers, payload digest,
//! credential scope, and timestamp freshness with the secret access key.
//!
//! ## Multipart upload + ranged reads (CONCEPT:EG-KG.txn.pubsub-transactions)
//!
//! LANDED (EG-307): the multipart-upload lifecycle — `CreateMultipartUpload`
//! (`POST /b/k?uploads` → an `UploadId`), `UploadPart`
//! (`PUT /b/k?partNumber=N&uploadId=…`), `CompleteMultipartUpload`
//! (`POST /b/k?uploadId=…`, parts concatenated in ascending part-number order into
//! one CAS object), `AbortMultipartUpload` (`DELETE …?uploadId=…`), and `ListParts`
//! (`GET …?uploadId=…`). Each uploaded part's bytes land in the SAME
//! content-addressed BLOB CAS (so an identical part dedups), tracked in an in-memory
//! per-upload registry until completion/abort. Plus `Range:` support on
//! `GetObject` — `bytes=start-end` / `bytes=start-` / `bytes=-suffix` → a
//! `206 Partial Content` reply carrying the requested slice + a `Content-Range`
//! header. Object versioning, ACLs / bucket policies, and multipart ETag
//! MD5-of-MD5 semantics are outside this adapter's intentionally narrow API.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use hmac::{Hmac, Mac};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::RwLock;

use crate::server::blob::store::{ChunkStore, RedbChunkStore};
use crate::server::kv::KvStore;
use crate::server::ServerState;

/// Env var: when set (and built `--features s3-api`) the S3 REST listener binds
/// this address (documented loopback default `127.0.0.1:9000`, the MinIO default).
/// Unset ⇒ no listener.
pub const S3_ADDR_ENV: &str = "EPISTEMIC_GRAPH_S3_ADDR";
/// Env var: the access-key id. When set (with the secret), the SigV4 guard is
/// armed and anonymous access is refused.
pub const S3_ACCESS_KEY_ENV: &str = "EPISTEMIC_GRAPH_S3_ACCESS_KEY";
/// Env var: the secret access key (armed together with the access key).
pub const S3_SECRET_KEY_ENV: &str = "EPISTEMIC_GRAPH_S3_SECRET_KEY";
const MAX_HTTP_HEADER_BYTES: usize = 64 * 1024;
const MAX_HTTP_HEADERS: usize = 256;
const MAX_HTTP_QUERY_FIELDS: usize = 256;
const MAX_S3_BODY_BYTES: usize = 64 * 1024 * 1024;
const MAX_S3_META_BYTES: usize = 16 * 1024;
const MAX_S3_META_ITEMS: usize = 128;
const MAX_S3_BUCKET_BYTES: usize = 63;
const MAX_S3_KEY_BYTES: usize = 4_000;
const MAX_S3_CONTENT_TYPE_BYTES: usize = 1024;
const MAX_S3_UPLOADS: usize = 1_024;
const MAX_S3_MULTIPART_PARTS: usize = 1_000;
const MAX_S3_LIST_RESULTS: usize = 1_000;
const HTTP_READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(15);

const BUCKET_NS: &str = "s3:buckets";
const OBJECT_NS: &str = "s3:objects";

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Configured credentials for the mandatory SigV4 guard.
#[derive(Clone, Debug)]
pub struct S3Auth {
    pub access_key: String,
    pub secret_key: String,
}

/// Object listing metadata (the bytes live in the CAS, referenced by `digest`).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
struct ObjectMeta {
    digest: String,
    size: u64,
    etag: String,
    last_modified_ms: u64,
    content_type: String,
}

fn validate_bucket(bucket: &str) -> Result<(), String> {
    if bucket.is_empty()
        || bucket.len() > MAX_S3_BUCKET_BYTES
        || bucket.chars().any(char::is_control)
        || bucket.contains('/')
    {
        Err("invalid S3 bucket identifier".to_string())
    } else {
        Ok(())
    }
}

fn validate_object_key(key: &str) -> Result<(), String> {
    if key.is_empty() || key.len() > MAX_S3_KEY_BYTES || key.chars().any(char::is_control) {
        Err("invalid S3 object key".to_string())
    } else {
        Ok(())
    }
}

fn validate_content_type(content_type: &str) -> Result<(), String> {
    if content_type.is_empty()
        || content_type.len() > MAX_S3_CONTENT_TYPE_BYTES
        || content_type.chars().any(char::is_control)
    {
        Err("invalid S3 content type".to_string())
    } else {
        Ok(())
    }
}

fn validate_object_meta(meta: &ObjectMeta) -> Result<(), String> {
    if meta.digest.len() != 64
        || !meta.digest.bytes().all(|byte| byte.is_ascii_hexdigit())
        || meta.etag != format!("\"{}\"", meta.digest)
        || meta.size > MAX_S3_BODY_BYTES as u64
    {
        return Err("stored S3 metadata is invalid or exceeds resource limits".to_string());
    }
    validate_content_type(&meta.content_type)
        .map_err(|_| "stored S3 metadata is invalid or exceeds resource limits".to_string())
}

fn decode_object_meta(bytes: &[u8]) -> Result<ObjectMeta, String> {
    let meta: ObjectMeta = eg_types::msgpack::decode_bounded(
        bytes,
        eg_types::msgpack::MsgpackLimits::new(
            MAX_S3_META_BYTES,
            MAX_S3_META_ITEMS,
            eg_types::msgpack::DEFAULT_MAX_DEPTH,
        ),
    )
    .map_err(|_| "stored S3 metadata is invalid or exceeds resource limits".to_string())?;
    validate_object_meta(&meta)?;
    Ok(meta)
}

// ── the store (CONCEPT:EG-KG.ontology.object-put-get-head) — objects over the BLOB CAS + KV index ────────────

/// One uploaded part of an in-progress multipart upload (CONCEPT:EG-KG.txn.pubsub-transactions). The bytes
/// live in the CAS, referenced by `digest`.
#[derive(Clone, Debug)]
struct PartInfo {
    digest: String,
    size: u64,
    etag: String,
}

/// An in-progress multipart upload (CONCEPT:EG-KG.txn.pubsub-transactions): the target bucket/key, its
/// content-type, and the parts received so far keyed by part number (so completion
/// concatenates them in ascending order regardless of arrival order).
#[derive(Clone, Debug)]
struct MultipartUpload {
    bucket: String,
    key: String,
    content_type: String,
    parts: BTreeMap<u32, PartInfo>,
    total_size: u64,
}

/// The S3 backing store: a content-addressed [`ChunkStore`] for object bytes + a
/// durable KV index for buckets and object metadata. In-progress multipart uploads
/// are held in an in-memory registry until completed or aborted (CONCEPT:EG-KG.txn.pubsub-transactions).
pub struct S3Store {
    kv: Arc<KvStore>,
    blob: Arc<dyn ChunkStore>,
    /// `uploadId` → the in-progress multipart upload state (CONCEPT:EG-KG.txn.pubsub-transactions).
    uploads: Mutex<HashMap<String, MultipartUpload>>,
}

impl S3Store {
    /// Open the S3 store. `Some(dir)` ⇒ durable (`{dir}/s3-index/kv.redb` +
    /// `{dir}/s3-blob`); `None` ⇒ an ephemeral in-memory index over a temp-dir CAS
    /// (the BLOB substrate has no in-memory backend — mirrors its philosophy).
    pub fn open(persist_dir: Option<&str>) -> Result<Self, String> {
        let (kv_dir, blob_dir) = match persist_dir {
            Some(d) => (Some(format!("{d}/s3-index")), format!("{d}/s3-blob")),
            None => {
                // Process-unique CAS dir: pid + a monotonic counter, so two ephemeral
                // stores in the same process (e.g. parallel tests) never collide on the
                // redb file lock.
                static EPHEMERAL_SEQ: AtomicU64 = AtomicU64::new(0);
                let seq = EPHEMERAL_SEQ.fetch_add(1, Ordering::Relaxed);
                let tmp = std::env::temp_dir().join(format!(
                    "eg-s3-{}-{}-{}",
                    std::process::id(),
                    now_ms(),
                    seq
                ));
                (None, tmp.to_string_lossy().into_owned())
            }
        };
        let kv = Arc::new(KvStore::open(kv_dir.as_deref())?);
        let blob: Arc<dyn ChunkStore> = Arc::new(RedbChunkStore::open(&blob_dir)?);
        Ok(Self {
            kv,
            blob,
            uploads: Mutex::new(HashMap::new()),
        })
    }

    pub fn is_durable(&self) -> bool {
        self.kv.is_durable()
    }

    fn bucket_exists(&self, bucket: &str) -> Result<bool, String> {
        validate_bucket(bucket)?;
        Ok(self.kv.get(BUCKET_NS, bucket)?.is_some())
    }

    fn create_bucket(&self, bucket: &str) -> Result<(), String> {
        validate_bucket(bucket)?;
        self.kv.put(BUCKET_NS, bucket, b"1".to_vec())
    }

    /// Delete a bucket. Errors with `BucketNotEmpty` if it still holds objects.
    fn delete_bucket(&self, bucket: &str) -> Result<bool, String> {
        validate_bucket(bucket)?;
        if !self.list_objects(bucket, "")?.is_empty() {
            return Err("BucketNotEmpty".into());
        }
        self.kv.delete(BUCKET_NS, bucket)
    }

    fn list_buckets(&self) -> Result<Vec<String>, String> {
        let rows = self.kv.scan(BUCKET_NS, "", MAX_S3_LIST_RESULTS)?;
        let mut buckets = Vec::with_capacity(rows.len());
        for (bucket, _) in rows {
            validate_bucket(&bucket)?;
            buckets.push(bucket);
        }
        Ok(buckets)
    }

    fn obj_key(bucket: &str, key: &str) -> String {
        format!("{bucket}/{key}")
    }

    /// Store an object's bytes in the CAS + record its metadata. Returns the etag.
    fn put_object(
        &self,
        bucket: &str,
        key: &str,
        body: &[u8],
        content_type: &str,
    ) -> Result<String, String> {
        validate_bucket(bucket)?;
        validate_object_key(key)?;
        validate_content_type(content_type)?;
        if body.len() > MAX_S3_BODY_BYTES {
            return Err("S3 object exceeds resource limits".to_string());
        }
        let (digest, _) = self.blob.put_chunk(body)?;
        let etag = format!("\"{digest}\"");
        let meta = ObjectMeta {
            digest,
            size: body.len() as u64,
            etag: etag.clone(),
            last_modified_ms: now_ms(),
            content_type: content_type.to_string(),
        };
        let bytes = rmp_serde::to_vec_named(&meta).map_err(|e| e.to_string())?;
        eg_types::msgpack::validate_single_value(
            &bytes,
            eg_types::msgpack::MsgpackLimits::new(
                MAX_S3_META_BYTES,
                MAX_S3_META_ITEMS,
                eg_types::msgpack::DEFAULT_MAX_DEPTH,
            ),
        )
        .map_err(|_| "S3 metadata exceeds resource limits".to_string())?;
        self.kv.put(OBJECT_NS, &Self::obj_key(bucket, key), bytes)?;
        Ok(etag)
    }

    fn object_meta(&self, bucket: &str, key: &str) -> Result<Option<ObjectMeta>, String> {
        validate_bucket(bucket)?;
        validate_object_key(key)?;
        match self.kv.get(OBJECT_NS, &Self::obj_key(bucket, key))? {
            Some(bytes) => Ok(Some(decode_object_meta(&bytes)?)),
            None => Ok(None),
        }
    }

    fn get_object(&self, bucket: &str, key: &str) -> Result<Option<(ObjectMeta, Vec<u8>)>, String> {
        match self.object_meta(bucket, key)? {
            Some(meta) => {
                let bytes = self
                    .blob
                    .get_chunk(&meta.digest)?
                    .ok_or_else(|| "stored S3 object chunk is missing".to_string())?;
                if bytes.len() as u64 != meta.size {
                    return Err("stored S3 object size does not match metadata".to_string());
                }
                Ok(Some((meta, bytes)))
            }
            None => Ok(None),
        }
    }

    fn delete_object(&self, bucket: &str, key: &str) -> Result<bool, String> {
        validate_bucket(bucket)?;
        validate_object_key(key)?;
        // The CAS chunk is left in place (content-addressed / shareable; reclaimed by
        // the blob sweep, not on the S3 delete path).
        self.kv.delete(OBJECT_NS, &Self::obj_key(bucket, key))
    }

    /// List objects in `bucket` whose key starts with `prefix`, sorted, with meta.
    fn list_objects(
        &self,
        bucket: &str,
        prefix: &str,
    ) -> Result<Vec<(String, ObjectMeta)>, String> {
        validate_bucket(bucket)?;
        if prefix.len() > MAX_S3_KEY_BYTES || prefix.chars().any(char::is_control) {
            return Err("invalid S3 object prefix".to_string());
        }
        let scan_prefix = format!("{bucket}/{prefix}");
        let bucket_prefix = format!("{bucket}/");
        let mut out = Vec::new();
        for (k, v) in self.kv.scan(OBJECT_NS, &scan_prefix, MAX_S3_LIST_RESULTS)? {
            let key = k.strip_prefix(&bucket_prefix).unwrap_or(&k).to_string();
            validate_object_key(&key)?;
            let meta = decode_object_meta(&v)?;
            out.push((key, meta));
        }
        out.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(out)
    }

    // ── multipart upload (CONCEPT:EG-KG.txn.pubsub-transactions) ─────────────────────────────────────────

    /// Begin a multipart upload, returning a fresh, process-unique `UploadId`. The
    /// upload accumulates parts in memory until completed/aborted.
    fn create_multipart(
        &self,
        bucket: &str,
        key: &str,
        content_type: &str,
    ) -> Result<String, String> {
        validate_bucket(bucket)?;
        validate_object_key(key)?;
        validate_content_type(content_type)?;
        let upload_id = uuid::Uuid::new_v4().simple().to_string();
        let mut uploads = self.uploads.lock();
        if uploads.len() >= MAX_S3_UPLOADS {
            return Err("S3 multipart upload limit exceeded".to_string());
        }
        uploads.insert(
            upload_id.clone(),
            MultipartUpload {
                bucket: bucket.to_string(),
                key: key.to_string(),
                content_type: content_type.to_string(),
                parts: BTreeMap::new(),
                total_size: 0,
            },
        );
        Ok(upload_id)
    }

    /// Store one part's bytes in the CAS and record it under `part_number`. Returns
    /// the part's etag. Errors with `NoSuchUpload` for an unknown upload id.
    fn upload_part(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
        part_number: u32,
        body: &[u8],
    ) -> Result<String, String> {
        validate_bucket(bucket)?;
        validate_object_key(key)?;
        if !(1..=MAX_S3_MULTIPART_PARTS as u32).contains(&part_number)
            || body.len() > MAX_S3_BODY_BYTES
        {
            return Err("S3 multipart part exceeds resource limits".to_string());
        }
        {
            let uploads = self.uploads.lock();
            let upload = uploads.get(upload_id).ok_or("NoSuchUpload")?;
            if upload.bucket != bucket || upload.key != key {
                return Err("NoSuchUpload".to_string());
            }
            let replaced_size = upload
                .parts
                .get(&part_number)
                .map(|part| part.size)
                .unwrap_or(0);
            upload
                .total_size
                .checked_sub(replaced_size)
                .and_then(|size| size.checked_add(body.len() as u64))
                .filter(|size| *size <= MAX_S3_BODY_BYTES as u64)
                .ok_or_else(|| "S3 multipart object exceeds resource limits".to_string())?;
        }
        let (digest, _) = self.blob.put_chunk(body)?;
        let etag = format!("\"{digest}\"");
        let mut uploads = self.uploads.lock();
        let up = uploads.get_mut(upload_id).ok_or("NoSuchUpload")?;
        if up.bucket != bucket || up.key != key {
            return Err("NoSuchUpload".to_string());
        }
        let replaced_size = up
            .parts
            .get(&part_number)
            .map(|part| part.size)
            .unwrap_or(0);
        let next_size = up
            .total_size
            .checked_sub(replaced_size)
            .and_then(|size| size.checked_add(body.len() as u64))
            .filter(|size| *size <= MAX_S3_BODY_BYTES as u64)
            .ok_or_else(|| "S3 multipart object exceeds resource limits".to_string())?;
        up.parts.insert(
            part_number,
            PartInfo {
                digest,
                size: body.len() as u64,
                etag: etag.clone(),
            },
        );
        up.total_size = next_size;
        Ok(etag)
    }

    /// List the parts received so far for an in-progress upload (ascending part
    /// number). Errors with `NoSuchUpload` for an unknown id.
    fn list_parts(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<Vec<(u32, PartInfo)>, String> {
        let uploads = self.uploads.lock();
        let up = uploads.get(upload_id).ok_or("NoSuchUpload")?;
        if up.bucket != bucket || up.key != key {
            return Err("NoSuchUpload".to_string());
        }
        Ok(up.parts.iter().map(|(n, p)| (*n, p.clone())).collect())
    }

    /// Complete a multipart upload: concatenate the parts (ascending part number)
    /// into one object stored in the CAS + KV index, drop the in-progress state, and
    /// return `(bucket, key, etag)`. Errors with `NoSuchUpload` for an unknown id.
    fn complete_multipart(
        &self,
        bucket: &str,
        key: &str,
        upload_id: &str,
    ) -> Result<(String, String, String), String> {
        let up = self
            .uploads
            .lock()
            .remove(upload_id)
            .ok_or("NoSuchUpload")?;
        if up.bucket != bucket || up.key != key {
            self.uploads.lock().insert(upload_id.to_string(), up);
            return Err("NoSuchUpload".to_string());
        }
        let result = (|| -> Result<(String, String, String), String> {
            if up.parts.is_empty()
                || up.parts.len() > MAX_S3_MULTIPART_PARTS
                || up.total_size > MAX_S3_BODY_BYTES as u64
            {
                return Err("S3 multipart object exceeds resource limits".to_string());
            }
            // Concatenate the parts in ascending part-number order (BTreeMap is ordered).
            let capacity = usize::try_from(up.total_size)
                .map_err(|_| "S3 multipart object exceeds resource limits".to_string())?;
            let mut body = Vec::with_capacity(capacity);
            for part in up.parts.values() {
                let bytes = self
                    .blob
                    .get_chunk(&part.digest)?
                    .ok_or_else(|| "stored S3 multipart chunk is missing".to_string())?;
                if bytes.len() as u64 != part.size {
                    return Err("stored S3 multipart chunk size is invalid".to_string());
                }
                body.extend_from_slice(&bytes);
            }
            if body.len() != capacity {
                return Err("stored S3 multipart aggregate size is invalid".to_string());
            }
            let etag = self.put_object(&up.bucket, &up.key, &body, &up.content_type)?;
            Ok((up.bucket.clone(), up.key.clone(), etag))
        })();
        if result.is_err() {
            self.uploads.lock().insert(upload_id.to_string(), up);
        }
        result
    }

    /// Abort a multipart upload, discarding its in-progress state. `true` if an
    /// upload with that id existed. (CAS part chunks are content-addressed and
    /// reclaimed by the blob sweep, not on this path — mirrors `delete_object`.)
    fn abort_multipart(&self, bucket: &str, key: &str, upload_id: &str) -> bool {
        let mut uploads = self.uploads.lock();
        if uploads
            .get(upload_id)
            .is_some_and(|upload| upload.bucket == bucket && upload.key == key)
        {
            uploads.remove(upload_id);
            true
        } else {
            false
        }
    }
}

// ── SigV4 auth guard (CONCEPT:EG-KG.ontology.object-put-get-head) ─────────────────────────────────────────────

type HmacSha256 = Hmac<Sha256>;

fn aws_uri_encode(value: &str, encode_slash: bool) -> String {
    let mut out = String::new();
    for byte in value.as_bytes() {
        if byte.is_ascii_alphanumeric()
            || matches!(*byte, b'-' | b'_' | b'.' | b'~')
            || (!encode_slash && *byte == b'/')
        {
            out.push(*byte as char);
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

fn canonical_query(raw: &str) -> String {
    let mut fields: Vec<(String, String)> = raw
        .split('&')
        .filter(|part| !part.is_empty())
        .map(|part| {
            let (key, value) = part.split_once('=').unwrap_or((part, ""));
            (
                aws_uri_encode(&percent_decode(key), true),
                aws_uri_encode(&percent_decode(value), true),
            )
        })
        .collect();
    fields.sort();
    fields
        .into_iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("&")
}

fn hmac_bytes(key: &[u8], value: &[u8]) -> Option<Vec<u8>> {
    let mut mac = HmacSha256::new_from_slice(key).ok()?;
    mac.update(value);
    Some(mac.finalize().into_bytes().to_vec())
}

fn valid_amz_date(value: &str, scope_date: &str) -> bool {
    if value.len() != 16
        || !value.ends_with('Z')
        || value.get(0..8) != Some(scope_date)
        || value.as_bytes().get(8) != Some(&b'T')
    {
        return false;
    }
    let number = |range: std::ops::Range<usize>| value.get(range)?.parse::<i64>().ok();
    let (year, month, day, hour, minute, second) = match (
        number(0..4),
        number(4..6),
        number(6..8),
        number(9..11),
        number(11..13),
        number(13..15),
    ) {
        (Some(y), Some(m), Some(d), Some(h), Some(mi), Some(s)) => (y, m, d, h, mi, s),
        _ => return false,
    };
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let month_days = [
        31,
        if leap { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    if !(1..=12).contains(&month)
        || day < 1
        || day > month_days[(month - 1) as usize]
        || hour > 23
        || minute > 59
        || second > 59
    {
        return false;
    }
    // Howard Hinnant's civil-date conversion, yielding days from Unix epoch.
    let adjusted_year = year - if month <= 2 { 1 } else { 0 };
    let era = if adjusted_year >= 0 {
        adjusted_year
    } else {
        adjusted_year - 399
    } / 400;
    let yoe = adjusted_year - era * 400;
    let shifted_month = month + if month > 2 { -3 } else { 9 };
    let doy = (153 * shifted_month + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    let timestamp = days
        .saturating_mul(86_400)
        .saturating_add(hour * 3_600 + minute * 60 + second);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs() as i64)
        .unwrap_or(0);
    now.abs_diff(timestamp) <= 900
}

fn verify_sigv4(auth: &S3Auth, req: &S3Request, header: &str) -> bool {
    if req
        .query
        .split('&')
        .filter(|field| !field.is_empty())
        .count()
        > MAX_HTTP_QUERY_FIELDS
    {
        return false;
    }
    let Some(fields) = header.strip_prefix("AWS4-HMAC-SHA256 ") else {
        return false;
    };
    let mut parsed_fields = HashMap::with_capacity(3);
    for field in fields.split(',') {
        let Some((name, value)) = field.trim().split_once('=') else {
            return false;
        };
        if !matches!(name, "Credential" | "SignedHeaders" | "Signature")
            || value.is_empty()
            || parsed_fields.insert(name, value).is_some()
        {
            return false;
        }
    }
    if parsed_fields.len() != 3 {
        return false;
    }
    let fields = parsed_fields;
    let Some(credential) = fields.get("Credential") else {
        return false;
    };
    let Some(signed_headers_raw) = fields.get("SignedHeaders") else {
        return false;
    };
    let Some(signature) = fields.get("Signature") else {
        return false;
    };
    let credential: Vec<&str> = credential.split('/').collect();
    if credential.len() != 5
        || credential[0] != auth.access_key
        || credential[3] != "s3"
        || credential[4] != "aws4_request"
    {
        return false;
    }
    let signed_headers: Vec<&str> = signed_headers_raw.split(';').collect();
    if signed_headers.is_empty()
        || signed_headers.len() > MAX_HTTP_HEADERS
        || signed_headers.windows(2).any(|pair| pair[0] >= pair[1])
        || signed_headers.iter().any(|name| {
            name.is_empty()
                || !name
                    .bytes()
                    .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        })
        || !["host", "x-amz-content-sha256", "x-amz-date"]
            .iter()
            .all(|required| signed_headers.contains(required))
    {
        return false;
    }
    let Some(amz_date) = req.headers.get("x-amz-date") else {
        return false;
    };
    if !valid_amz_date(amz_date, credential[1]) {
        return false;
    }
    let Some(payload_hash) = req.headers.get("x-amz-content-sha256") else {
        return false;
    };
    let actual_payload_hash = hex::encode(Sha256::digest(&req.body));
    if payload_hash != &actual_payload_hash {
        return false;
    }
    let mut canonical_headers = Vec::with_capacity(signed_headers.len());
    for name in &signed_headers {
        let Some(value) = req.headers.get(*name) else {
            return false;
        };
        canonical_headers.push(format!(
            "{name}:{}",
            value.split_whitespace().collect::<Vec<_>>().join(" ")
        ));
    }
    let canonical_request = format!(
        "{}\n{}\n{}\n{}\n{}\n{}",
        req.method,
        aws_uri_encode(&req.path, false),
        canonical_query(&req.query),
        canonical_headers.join("\n"),
        signed_headers_raw,
        payload_hash,
    );
    let scope = credential[1..].join("/");
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{}\n{}\n{}",
        amz_date,
        scope,
        hex::encode(Sha256::digest(canonical_request.as_bytes())),
    );
    let Some(date_key) = hmac_bytes(
        format!("AWS4{}", auth.secret_key).as_bytes(),
        credential[1].as_bytes(),
    ) else {
        return false;
    };
    let Some(region_key) = hmac_bytes(&date_key, credential[2].as_bytes()) else {
        return false;
    };
    let Some(service_key) = hmac_bytes(&region_key, credential[3].as_bytes()) else {
        return false;
    };
    let Some(signing_key) = hmac_bytes(&service_key, credential[4].as_bytes()) else {
        return false;
    };
    let Ok(got_signature) = hex::decode(signature) else {
        return false;
    };
    let Ok(mut verifier) = HmacSha256::new_from_slice(&signing_key) else {
        return false;
    };
    verifier.update(string_to_sign.as_bytes());
    verifier.verify_slice(&got_signature).is_ok()
}

/// Verify a complete body-bound SigV4 request.
fn authorized(auth: &S3Auth, req: &S3Request) -> bool {
    let hdr = match req.headers.get("authorization") {
        Some(h) => h,
        None => return false,
    };
    verify_sigv4(auth, req, hdr)
}

// ── request / response + routing ───────────────────────────────────────────────

/// A parsed S3 HTTP request: method, decoded path, raw query, headers (lowercased
/// keys), and the raw (binary) body.
struct S3Request {
    method: String,
    path: String,
    query: String,
    headers: HashMap<String, String>,
    body: Vec<u8>,
}

/// A response ready to serialize: HTTP status line, extra headers, body bytes, and
/// whether to elide the body (a HEAD reply carries the headers only).
struct S3Response {
    status: &'static str,
    content_type: String,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
    head_only: bool,
}

impl S3Response {
    fn xml(status: &'static str, body: String) -> Self {
        S3Response {
            status,
            content_type: "application/xml".into(),
            headers: Vec::new(),
            body: body.into_bytes(),
            head_only: false,
        }
    }
    fn empty(status: &'static str) -> Self {
        S3Response {
            status,
            content_type: "application/xml".into(),
            headers: Vec::new(),
            body: Vec::new(),
            head_only: false,
        }
    }
    fn error(status: &'static str, code: &str, message: &str) -> Self {
        let body = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>{}</Code><Message>{}</Message></Error>",
            xml_escape(code),
            xml_escape(message)
        );
        S3Response::xml(status, body)
    }
}

/// XML-escape a text value for an S3 XML body.
fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

/// Percent-decode a path/key (`%20` → space, etc.). Invalid escapes pass through.
fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hi = (bytes[i + 1] as char).to_digit(16);
            let lo = (bytes[i + 2] as char).to_digit(16);
            if let (Some(h), Some(l)) = (hi, lo) {
                out.push((h * 16 + l) as u8);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Extract a query parameter's value (`k=v`), or `Some("")` for a bare flag.
fn query_param(query: &str, name: &str) -> Option<String> {
    for pair in query.split('&') {
        match pair.split_once('=') {
            Some((k, v)) if k == name => return Some(percent_decode(v)),
            None if pair == name => return Some(String::new()),
            _ => {}
        }
    }
    None
}

/// Split a decoded path `/bucket/key/with/slashes` into `(bucket, key)`. The key is
/// empty for a bucket-level path.
fn split_bucket_key(path: &str) -> (String, String) {
    let trimmed = path.trim_start_matches('/');
    match trimmed.split_once('/') {
        Some((b, k)) => (b.to_string(), k.to_string()),
        None => (trimmed.to_string(), String::new()),
    }
}

/// Format epoch-ms as an ISO-8601 UTC instant (S3 `LastModified` shape). Minimal
/// hand-rolled formatter (no chrono — the Pi contract).
fn iso8601(ms: u64) -> String {
    // Days since epoch → civil date via Howard Hinnant's algorithm.
    let secs = ms / 1000;
    let days = (secs / 86_400) as i64;
    let rem = secs % 86_400;
    let (h, mi, s) = (rem / 3600, (rem % 3600) / 60, rem % 60);
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{y:04}-{m:02}-{d:02}T{h:02}:{mi:02}:{s:02}.000Z")
}

/// Route + execute one S3 request → an [`S3Response`]. Pure (sync) so it is fully
/// unit-testable without a socket (CONCEPT:EG-KG.ontology.object-put-get-head).
fn handle(store: &S3Store, auth: &S3Auth, req: &S3Request) -> S3Response {
    // A18: SigV4 IS this surface's own carrier proof — mint the engine-owned
    // authority only after it succeeds, then gate through the SAME shared check
    // every other surface uses (real now: denies iff no carrier was minted).
    let carrier = authorized(auth, req)
        .then(crate::server::auth::VerifiedRequestContext::authenticated_s3_actor)
        .and_then(Result::ok)
        .and_then(|context| crate::server::access::CarrierAuthority::from_verified(&context).ok());
    if crate::server::access::unauthenticated_carrier_denied(carrier.as_ref()) {
        return S3Response::error("403 Forbidden", "AccessDenied", "Access Denied");
    }
    handle_authorized(store, req)
}

/// Route a request after the network boundary verified SigV4.
fn handle_authorized(store: &S3Store, req: &S3Request) -> S3Response {
    let (bucket, key) = split_bucket_key(&req.path);

    // Service-level: `GET /` → ListBuckets.
    if bucket.is_empty() {
        return match req.method.as_str() {
            "GET" => match store.list_buckets() {
                Ok(buckets) => S3Response::xml("200 OK", list_buckets_xml(&buckets)),
                Err(e) => internal(&e),
            },
            _ => S3Response::error("405 Method Not Allowed", "MethodNotAllowed", "unsupported"),
        };
    }

    // Bucket-level (no object key).
    if key.is_empty() {
        return match req.method.as_str() {
            "PUT" => match store.create_bucket(&bucket) {
                Ok(()) => {
                    let mut r = S3Response::empty("200 OK");
                    r.headers.push(("Location".into(), format!("/{bucket}")));
                    r
                }
                Err(e) => internal(&e),
            },
            "DELETE" => match store.delete_bucket(&bucket) {
                Ok(_) => S3Response::empty("204 No Content"),
                Err(e) if e == "BucketNotEmpty" => S3Response::error(
                    "409 Conflict",
                    "BucketNotEmpty",
                    "The bucket you tried to delete is not empty",
                ),
                Err(e) => internal(&e),
            },
            "HEAD" => match store.bucket_exists(&bucket) {
                Ok(true) => S3Response::empty("200 OK"),
                Ok(false) => S3Response::error("404 Not Found", "NoSuchBucket", "no such bucket"),
                Err(e) => internal(&e),
            },
            "GET" => {
                // ListObjects(V2) — `list-type=2` or the v1 default.
                let prefix = query_param(&req.query, "prefix").unwrap_or_default();
                match store.list_objects(&bucket, &prefix) {
                    Ok(objs) => {
                        S3Response::xml("200 OK", list_objects_xml(&bucket, &prefix, &objs))
                    }
                    Err(e) => internal(&e),
                }
            }
            _ => S3Response::error("405 Method Not Allowed", "MethodNotAllowed", "unsupported"),
        };
    }

    // Object-level. Multipart-upload sub-resources (CONCEPT:EG-KG.txn.pubsub-transactions) are selected by
    // query parameters (`?uploads`, `?uploadId=…`, `?partNumber=…`) and take
    // precedence over the plain object verbs.
    let has_uploads = query_param(&req.query, "uploads").is_some();
    let upload_id = query_param(&req.query, "uploadId");
    let part_number = query_param(&req.query, "partNumber").and_then(|s| s.parse::<u32>().ok());

    if has_uploads && req.method == "POST" {
        // CreateMultipartUpload.
        return match store.bucket_exists(&bucket) {
            Ok(false) => S3Response::error("404 Not Found", "NoSuchBucket", "no such bucket"),
            Err(e) => internal(&e),
            Ok(true) => {
                let ctype = req
                    .headers
                    .get("content-type")
                    .cloned()
                    .unwrap_or_else(|| "application/octet-stream".to_string());
                match store.create_multipart(&bucket, &key, &ctype) {
                    Ok(uid) => {
                        S3Response::xml("200 OK", initiate_multipart_xml(&bucket, &key, &uid))
                    }
                    Err(error) => internal(&error),
                }
            }
        };
    }
    if let Some(uid) = upload_id {
        return handle_multipart(store, &bucket, &key, &uid, part_number, req);
    }

    match req.method.as_str() {
        "PUT" => {
            match store.bucket_exists(&bucket) {
                Ok(false) => {
                    return S3Response::error("404 Not Found", "NoSuchBucket", "no such bucket")
                }
                Err(e) => return internal(&e),
                Ok(true) => {}
            }
            let ctype = req
                .headers
                .get("content-type")
                .cloned()
                .unwrap_or_else(|| "application/octet-stream".to_string());
            match store.put_object(&bucket, &key, &req.body, &ctype) {
                Ok(etag) => {
                    let mut r = S3Response::empty("200 OK");
                    r.headers.push(("ETag".into(), etag));
                    r
                }
                Err(e) => internal(&e),
            }
        }
        "GET" => match store.get_object(&bucket, &key) {
            Ok(Some((meta, bytes))) => match req.headers.get("range") {
                // Ranged read (CONCEPT:EG-KG.txn.pubsub-transactions) → 206 Partial Content.
                Some(range) => range_response(meta, bytes, range),
                None => object_response(meta, bytes, false),
            },
            Ok(None) => S3Response::error("404 Not Found", "NoSuchKey", "no such key"),
            Err(e) => internal(&e),
        },
        "HEAD" => match store.object_meta(&bucket, &key) {
            Ok(Some(meta)) => object_response(meta, Vec::new(), true),
            Ok(None) => S3Response::error("404 Not Found", "NoSuchKey", "no such key"),
            Err(e) => internal(&e),
        },
        "DELETE" => match store.delete_object(&bucket, &key) {
            Ok(_) => S3Response::empty("204 No Content"),
            Err(e) => internal(&e),
        },
        _ => S3Response::error("405 Method Not Allowed", "MethodNotAllowed", "unsupported"),
    }
}

/// Route the multipart sub-resource verbs for a known `uploadId` (CONCEPT:EG-KG.txn.pubsub-transactions):
/// `PUT …&partNumber=N` (UploadPart), `POST` (CompleteMultipartUpload),
/// `DELETE` (AbortMultipartUpload), `GET` (ListParts).
fn handle_multipart(
    store: &S3Store,
    bucket: &str,
    key: &str,
    upload_id: &str,
    part_number: Option<u32>,
    req: &S3Request,
) -> S3Response {
    let no_such = || S3Response::error("404 Not Found", "NoSuchUpload", "no such upload");
    match req.method.as_str() {
        "PUT" => {
            let pn = match part_number {
                Some(n) if (1..=MAX_S3_MULTIPART_PARTS as u32).contains(&n) => n,
                _ => {
                    return S3Response::error(
                        "400 Bad Request",
                        "InvalidArgument",
                        "partNumber is outside the supported range",
                    )
                }
            };
            match store.upload_part(bucket, key, upload_id, pn, &req.body) {
                Ok(etag) => {
                    let mut r = S3Response::empty("200 OK");
                    r.headers.push(("ETag".into(), etag));
                    r
                }
                Err(e) if e == "NoSuchUpload" => no_such(),
                Err(e) => internal(&e),
            }
        }
        "POST" => match store.complete_multipart(bucket, key, upload_id) {
            Ok((b, k, etag)) => S3Response::xml("200 OK", complete_multipart_xml(&b, &k, &etag)),
            Err(e) if e == "NoSuchUpload" => no_such(),
            Err(e) => internal(&e),
        },
        "DELETE" => {
            if store.abort_multipart(bucket, key, upload_id) {
                S3Response::empty("204 No Content")
            } else {
                no_such()
            }
        }
        "GET" => match store.list_parts(bucket, key, upload_id) {
            Ok(parts) => S3Response::xml("200 OK", list_parts_xml(bucket, key, upload_id, &parts)),
            Err(e) if e == "NoSuchUpload" => no_such(),
            Err(e) => internal(&e),
        },
        _ => S3Response::error("405 Method Not Allowed", "MethodNotAllowed", "unsupported"),
    }
}

fn internal(msg: &str) -> S3Response {
    S3Response::error("500 Internal Server Error", "InternalError", msg)
}

/// Build the object GET/HEAD reply headers (content-type/length/etag/last-modified).
fn object_response(meta: ObjectMeta, bytes: Vec<u8>, head_only: bool) -> S3Response {
    S3Response {
        status: "200 OK",
        content_type: meta.content_type.clone(),
        headers: vec![
            ("ETag".into(), meta.etag.clone()),
            ("Last-Modified".into(), iso8601(meta.last_modified_ms)),
        ],
        body: bytes,
        head_only,
    }
}

/// Parse an HTTP `Range` header value against a body of `total` bytes, returning the
/// inclusive `(start, end)` byte offsets (CONCEPT:EG-KG.txn.pubsub-transactions). Supports `bytes=start-end`,
/// `bytes=start-` (to end), and `bytes=-suffix` (last N bytes). Returns `None` when
/// the range is malformed or unsatisfiable (→ a `416` reply).
fn parse_range(range: &str, total: u64) -> Option<(u64, u64)> {
    let spec = range.trim().strip_prefix("bytes=")?;
    // Only the first range of a possible list is honored.
    let spec = spec.split(',').next()?.trim();
    let (start_s, end_s) = spec.split_once('-')?;
    if total == 0 {
        return None;
    }
    let last = total - 1;
    let (start, end) = if start_s.is_empty() {
        // Suffix range: the last `n` bytes.
        let n: u64 = end_s.parse().ok()?;
        if n == 0 {
            return None;
        }
        (total.saturating_sub(n), last)
    } else {
        let start: u64 = start_s.parse().ok()?;
        let end = if end_s.is_empty() {
            last
        } else {
            end_s.parse::<u64>().ok()?.min(last)
        };
        (start, end)
    };
    if start > last || start > end {
        return None;
    }
    Some((start, end))
}

/// Build a `206 Partial Content` reply carrying the requested byte slice + the
/// `Content-Range` / `Accept-Ranges` headers (CONCEPT:EG-KG.txn.pubsub-transactions). An unsatisfiable
/// range yields `416 Range Not Satisfiable`.
fn range_response(meta: ObjectMeta, bytes: Vec<u8>, range: &str) -> S3Response {
    let total = bytes.len() as u64;
    match parse_range(range, total) {
        Some((start, end)) => {
            let slice = bytes[start as usize..=end as usize].to_vec();
            S3Response {
                status: "206 Partial Content",
                content_type: meta.content_type.clone(),
                headers: vec![
                    ("ETag".into(), meta.etag.clone()),
                    ("Last-Modified".into(), iso8601(meta.last_modified_ms)),
                    ("Accept-Ranges".into(), "bytes".into()),
                    (
                        "Content-Range".into(),
                        format!("bytes {start}-{end}/{total}"),
                    ),
                ],
                body: slice,
                head_only: false,
            }
        }
        None => {
            let mut r = S3Response::error(
                "416 Range Not Satisfiable",
                "InvalidRange",
                "The requested range is not satisfiable",
            );
            r.headers
                .push(("Content-Range".into(), format!("bytes */{total}")));
            r
        }
    }
}

/// `<InitiateMultipartUploadResult>` — the CreateMultipartUpload reply (EG-307).
fn initiate_multipart_xml(bucket: &str, key: &str, upload_id: &str) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><InitiateMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Bucket>{}</Bucket><Key>{}</Key><UploadId>{}</UploadId></InitiateMultipartUploadResult>",
        xml_escape(bucket),
        xml_escape(key),
        xml_escape(upload_id),
    )
}

/// `<CompleteMultipartUploadResult>` — the CompleteMultipartUpload reply (EG-307).
fn complete_multipart_xml(bucket: &str, key: &str, etag: &str) -> String {
    format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><CompleteMultipartUploadResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Location>/{}/{}</Location><Bucket>{}</Bucket><Key>{}</Key><ETag>{}</ETag></CompleteMultipartUploadResult>",
        xml_escape(bucket),
        xml_escape(key),
        xml_escape(bucket),
        xml_escape(key),
        xml_escape(etag),
    )
}

/// `<ListPartsResult>` — the ListParts reply (EG-307).
fn list_parts_xml(bucket: &str, key: &str, upload_id: &str, parts: &[(u32, PartInfo)]) -> String {
    let mut b = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListPartsResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Bucket>{}</Bucket><Key>{}</Key><UploadId>{}</UploadId>",
        xml_escape(bucket),
        xml_escape(key),
        xml_escape(upload_id),
    );
    for (n, part) in parts {
        b.push_str(&format!(
            "<Part><PartNumber>{}</PartNumber><ETag>{}</ETag><Size>{}</Size></Part>",
            n,
            xml_escape(&part.etag),
            part.size,
        ));
    }
    b.push_str("</ListPartsResult>");
    b
}

fn list_buckets_xml(buckets: &[String]) -> String {
    let mut b = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListAllMyBucketsResult><Owner><ID>epistemic-graph</ID><DisplayName>epistemic-graph</DisplayName></Owner><Buckets>",
    );
    for name in buckets {
        b.push_str(&format!(
            "<Bucket><Name>{}</Name><CreationDate>1970-01-01T00:00:00.000Z</CreationDate></Bucket>",
            xml_escape(name)
        ));
    }
    b.push_str("</Buckets></ListAllMyBucketsResult>");
    b
}

fn list_objects_xml(bucket: &str, prefix: &str, objs: &[(String, ObjectMeta)]) -> String {
    let mut b = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><ListBucketResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\"><Name>{}</Name><Prefix>{}</Prefix><KeyCount>{}</KeyCount><MaxKeys>1000</MaxKeys><IsTruncated>false</IsTruncated>",
        xml_escape(bucket),
        xml_escape(prefix),
        objs.len()
    );
    for (key, meta) in objs {
        b.push_str(&format!(
            "<Contents><Key>{}</Key><LastModified>{}</LastModified><ETag>{}</ETag><Size>{}</Size><StorageClass>STANDARD</StorageClass></Contents>",
            xml_escape(key),
            iso8601(meta.last_modified_ms),
            xml_escape(&meta.etag),
            meta.size
        ));
    }
    b.push_str("</ListBucketResult>");
    b
}

// ── the HTTP listener (hand-rolled, no axum/hyper — the Pi contract) ─────────────

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

/// Read one HTTP/1.1 request, keeping the body as RAW BYTES (objects are binary)
/// and capturing headers (lowercased keys) for the auth guard.
async fn read_request(stream: &mut tokio::net::TcpStream) -> Option<S3Request> {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 8192];
    let header_end = loop {
        if let Some(pos) = find_subslice(&buf, b"\r\n\r\n") {
            break pos;
        }
        let n = stream.read(&mut tmp).await.ok()?;
        if n == 0 {
            return None;
        }
        buf.extend_from_slice(&tmp[..n]);
        if buf.len() > MAX_HTTP_HEADER_BYTES {
            return None; // header flood guard
        }
    };
    let head = std::str::from_utf8(&buf[..header_end]).ok()?.to_string();
    let mut lines = head.split("\r\n");
    let request_line = lines.next()?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next()?.to_string();
    let target = parts.next()?.to_string();
    let version = parts.next()?;
    if !matches!(version, "HTTP/1.0" | "HTTP/1.1") || parts.next().is_some() {
        return None;
    }
    let (raw_path, query) = match target.split_once('?') {
        Some((p, q)) => (p, q.to_string()),
        None => (target.as_str(), String::new()),
    };
    if query.split('&').filter(|field| !field.is_empty()).count() > MAX_HTTP_QUERY_FIELDS {
        return None;
    }
    let path = percent_decode(raw_path);

    let mut headers = HashMap::new();
    let mut content_length: Option<usize> = None;
    for line in lines {
        if headers.len() >= MAX_HTTP_HEADERS {
            return None;
        }
        let (k, v) = line.split_once(':')?;
        let key = k.trim().to_ascii_lowercase();
        let val = v.trim().to_string();
        if key.is_empty()
            || !key
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
            || headers.contains_key(&key)
        {
            return None;
        }
        if key == "content-length" {
            if content_length.is_some() {
                return None;
            }
            content_length = Some(val.parse().ok()?);
        } else if key == "transfer-encoding" {
            return None;
        }
        headers.insert(key, val);
    }
    let content_length = content_length.unwrap_or(0);
    if content_length > MAX_S3_BODY_BYTES {
        return None;
    }
    let mut body = buf[header_end + 4..].to_vec();
    if body.len() > content_length || body.len() > MAX_S3_BODY_BYTES {
        return None;
    }
    while body.len() < content_length {
        let n = stream.read(&mut tmp).await.ok()?;
        if n == 0 {
            break;
        }
        body.extend_from_slice(&tmp[..n]);
    }
    if body.len() != content_length {
        return None;
    }
    Some(S3Request {
        method,
        path,
        query,
        headers,
        body,
    })
}

/// Serve the S3 REST surface on `addr` until the process exits. Spawned by
/// `main.rs` only when built `--features s3-api` AND `EPISTEMIC_GRAPH_S3_ADDR` is
/// set (CONCEPT:EG-KG.ontology.object-put-get-head). One task per connection, one response per request,
/// connection: close — the SAME idiom as the obs / SPARQL listeners.
pub async fn serve(addr: &str, state: Arc<RwLock<ServerState>>) -> std::io::Result<()> {
    let persist_dir = { state.read().await.persist_dir.clone() };
    let store = Arc::new(S3Store::open(persist_dir.as_deref()).map_err(std::io::Error::other)?);
    let auth = resolve_auth()?;
    serve_with_store_inner(addr, store, auth).await
}

/// Resolve mandatory SigV4 credentials from the environment.
fn resolve_auth() -> std::io::Result<S3Auth> {
    let access = std::env::var(S3_ACCESS_KEY_ENV)
        .ok()
        .filter(|s| !s.is_empty());
    let secret = std::env::var(S3_SECRET_KEY_ENV)
        .ok()
        .filter(|s| !s.is_empty());
    match (access, secret) {
        (Some(access_key), Some(secret_key)) => Ok(S3Auth {
            access_key,
            secret_key,
        }),
        _ => Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "S3 access and secret credentials are required",
        )),
    }
}

async fn serve_with_store_inner(
    addr: &str,
    store: Arc<S3Store>,
    auth: S3Auth,
) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    crate::server::require_loopback_listener(&listener)?;
    tracing::info!(
        "s3-api: serving authenticated S3-compatible REST surface on {} (durable={})",
        addr,
        store.is_durable()
    );
    loop {
        let (mut stream, _peer) = listener.accept().await?;
        let store = store.clone();
        let auth = auth.clone();
        tokio::spawn(async move {
            let resp =
                match tokio::time::timeout(HTTP_READ_TIMEOUT, read_request(&mut stream)).await {
                    Ok(Some(req)) => handle(&store, &auth, &req),
                    _ => S3Response::error("400 Bad Request", "InvalidRequest", "malformed"),
                };
            let head = format!(
                "HTTP/1.1 {}\r\ncontent-type: {}\r\ncontent-length: {}\r\nconnection: close\r\n{}\r\n",
                resp.status,
                resp.content_type,
                if resp.head_only { 0 } else { resp.body.len() },
                resp.headers
                    .iter()
                    .map(|(k, v)| format!("{k}: {v}\r\n"))
                    .collect::<String>(),
            );
            let _ = stream.write_all(head.as_bytes()).await;
            if !resp.head_only {
                let _ = stream.write_all(&resp.body).await;
            }
            let _ = stream.shutdown().await;
        });
    }
}

#[cfg(test)]
mod tests {
    //! CONCEPT:EG-KG.ontology.object-put-get-head — object PUT/GET/HEAD/List/Delete round-trip over the CAS+KV
    //! store, bucket lifecycle, the SigV4 auth accept/reject, and helper
    //! coverage (path split, query parse, XML shape).
    use super::*;

    fn mem_store() -> Arc<S3Store> {
        Arc::new(S3Store::open(None).unwrap())
    }

    fn req(method: &str, path: &str, body: &[u8], headers: &[(&str, &str)]) -> S3Request {
        let (path, query) = match path.split_once('?') {
            Some((p, q)) => (p.to_string(), q.to_string()),
            None => (path.to_string(), String::new()),
        };
        S3Request {
            method: method.to_string(),
            path,
            query,
            headers: headers
                .iter()
                .map(|(k, v)| (k.to_ascii_lowercase(), v.to_string()))
                .collect(),
            body: body.to_vec(),
        }
    }

    #[test]
    fn eg176_helpers_split_query_iso() {
        assert_eq!(split_bucket_key("/b/k/x"), ("b".into(), "k/x".into()));
        assert_eq!(split_bucket_key("/b"), ("b".into(), "".into()));
        assert_eq!(split_bucket_key("/"), ("".into(), "".into()));
        assert_eq!(
            query_param("list-type=2&prefix=docs%2F", "prefix").as_deref(),
            Some("docs/")
        );
        assert_eq!(percent_decode("a%20b"), "a b");
        assert_eq!(iso8601(0), "1970-01-01T00:00:00.000Z");
    }

    #[test]
    fn eg176_object_put_get_list_delete() {
        let store = mem_store();
        // CreateBucket.
        assert_eq!(
            handle_authorized(&store, &req("PUT", "/mybucket", b"", &[])).status,
            "200 OK"
        );
        // PutObject.
        let put = handle_authorized(
            &store,
            &req(
                "PUT",
                "/mybucket/hello.txt",
                b"hello world",
                &[("content-type", "text/plain")],
            ),
        );
        assert_eq!(put.status, "200 OK");
        assert!(put.headers.iter().any(|(k, _)| k == "ETag"));
        // GetObject → bytes + content-type.
        let get = handle_authorized(&store, &req("GET", "/mybucket/hello.txt", b"", &[]));
        assert_eq!(get.status, "200 OK");
        assert_eq!(get.body, b"hello world");
        assert_eq!(get.content_type, "text/plain");
        // HeadObject → headers, no body.
        let head = handle_authorized(&store, &req("HEAD", "/mybucket/hello.txt", b"", &[]));
        assert_eq!(head.status, "200 OK");
        assert!(head.head_only);
        // ListObjectsV2 → the key appears in the XML.
        let list = handle_authorized(&store, &req("GET", "/mybucket?list-type=2", b"", &[]));
        assert_eq!(list.status, "200 OK");
        let xml = String::from_utf8(list.body).unwrap();
        assert!(xml.contains("<Key>hello.txt</Key>"), "{xml}");
        assert!(xml.contains("<Size>11</Size>"), "{xml}");
        // DeleteObject → 204, then GET → 404.
        assert_eq!(
            handle_authorized(&store, &req("DELETE", "/mybucket/hello.txt", b"", &[])).status,
            "204 No Content"
        );
        assert_eq!(
            handle_authorized(&store, &req("GET", "/mybucket/hello.txt", b"", &[])).status,
            "404 Not Found"
        );
    }

    #[test]
    fn eg176_missing_bucket_and_key_errors() {
        let store = mem_store();
        // PUT object into a non-existent bucket → NoSuchBucket.
        let r = handle_authorized(&store, &req("PUT", "/ghost/x", b"data", &[]));
        assert_eq!(r.status, "404 Not Found");
        assert!(String::from_utf8_lossy(&r.body).contains("NoSuchBucket"));
        // GET a missing key → NoSuchKey.
        handle_authorized(&store, &req("PUT", "/b", b"", &[]));
        let r = handle_authorized(&store, &req("GET", "/b/missing", b"", &[]));
        assert!(String::from_utf8_lossy(&r.body).contains("NoSuchKey"));
    }

    #[test]
    fn eg176_list_buckets_and_bucket_lifecycle() {
        let store = mem_store();
        handle_authorized(&store, &req("PUT", "/b1", b"", &[]));
        handle_authorized(&store, &req("PUT", "/b2", b"", &[]));
        let xml =
            String::from_utf8(handle_authorized(&store, &req("GET", "/", b"", &[])).body).unwrap();
        assert!(
            xml.contains("<Name>b1</Name>") && xml.contains("<Name>b2</Name>"),
            "{xml}"
        );
        // A non-empty bucket refuses deletion.
        handle_authorized(&store, &req("PUT", "/b1/obj", b"x", &[]));
        assert_eq!(
            handle_authorized(&store, &req("DELETE", "/b1", b"", &[])).status,
            "409 Conflict"
        );
        // An empty bucket deletes.
        assert_eq!(
            handle_authorized(&store, &req("DELETE", "/b2", b"", &[])).status,
            "204 No Content"
        );
    }

    #[test]
    fn eg176_sigv4_rejects_unverified_or_stale_signatures() {
        let store = mem_store();
        let auth = S3Auth {
            access_key: "AKIA_TEST".into(),
            secret_key: "shh".into(),
        };
        // No Authorization header → 403.
        let r = handle(&store, &auth, &req("GET", "/", b"", &[]));
        assert_eq!(r.status, "403 Forbidden");
        // Merely naming the correct access key and adding any Signature is not
        // authentication; the former incomplete signature path accepted this forgery.
        let good = "AWS4-HMAC-SHA256 Credential=AKIA_TEST/20130524/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=abc123";
        let r = handle(
            &store,
            &auth,
            &req("GET", "/", b"", &[("authorization", good)]),
        );
        assert_eq!(r.status, "403 Forbidden");
        // Wrong access key → 403.
        let bad =
            "AWS4-HMAC-SHA256 Credential=WRONG/20130524/us-east-1/s3/aws4_request, Signature=abc";
        let r = handle(
            &store,
            &auth,
            &req("GET", "/", b"", &[("authorization", bad)]),
        );
        assert_eq!(r.status, "403 Forbidden");
    }

    /// Sign a request exactly as a real SigV4 client would, reusing the SAME
    /// canonical-request helpers `verify_sigv4` reconstructs from
    /// (`aws_uri_encode`/`canonical_query`/`hmac_bytes`) so this cannot drift
    /// from what the server independently re-derives. Returns the headers a
    /// caller must send: `(authorization, host, x-amz-content-sha256, x-amz-date)`.
    fn sign_sigv4(
        auth: &S3Auth,
        method: &str,
        path: &str,
        query: &str,
        body: &[u8],
    ) -> (String, String, String, String) {
        let host = "s3.example.test".to_string();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        // Epoch seconds -> civil Y/M/D H:M:S (Howard Hinnant's algorithm; the
        // exact inverse of `valid_amz_date`'s forward conversion above).
        let days = now.div_euclid(86_400);
        let secs_of_day = now.rem_euclid(86_400);
        let (hour, minute, second) = (
            secs_of_day / 3600,
            (secs_of_day % 3600) / 60,
            secs_of_day % 60,
        );
        let z = days + 719_468;
        let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
        let doe = z - era * 146_097;
        let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
        let y = yoe + era * 400;
        let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
        let mp = (5 * doy + 2) / 153;
        let d = doy - (153 * mp + 2) / 5 + 1;
        let m = if mp < 10 { mp + 3 } else { mp - 9 };
        let y = if m <= 2 { y + 1 } else { y };
        let amz_date = format!("{y:04}{m:02}{d:02}T{hour:02}{minute:02}{second:02}Z");
        let date_stamp = format!("{y:04}{m:02}{d:02}");
        let region = "us-east-1";
        let payload_hash = hex::encode(Sha256::digest(body));
        let signed_headers_raw = "host;x-amz-content-sha256;x-amz-date";
        let canonical_headers =
            format!("host:{host}\nx-amz-content-sha256:{payload_hash}\nx-amz-date:{amz_date}");
        let canonical_request = format!(
            "{method}\n{}\n{}\n{canonical_headers}\n{signed_headers_raw}\n{payload_hash}",
            aws_uri_encode(path, false),
            canonical_query(query),
        );
        let scope = format!("{date_stamp}/{region}/s3/aws4_request");
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{}",
            hex::encode(Sha256::digest(canonical_request.as_bytes())),
        );
        let date_key = hmac_bytes(
            format!("AWS4{}", auth.secret_key).as_bytes(),
            date_stamp.as_bytes(),
        )
        .unwrap();
        let region_key = hmac_bytes(&date_key, region.as_bytes()).unwrap();
        let service_key = hmac_bytes(&region_key, b"s3").unwrap();
        let signing_key = hmac_bytes(&service_key, b"aws4_request").unwrap();
        let signature = hex::encode(hmac_bytes(&signing_key, string_to_sign.as_bytes()).unwrap());
        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed_headers_raw}, Signature={signature}",
            auth.access_key,
        );
        (authorization, host, payload_hash, amz_date)
    }

    #[test]
    fn eg18_authenticated_carrier_allowed_unauthenticated_denied() {
        // A18: the carrier-check stub used to deny EVERY request unconditionally.
        // A genuinely authenticated (valid SigV4) carrier must now be let through;
        // an unauthenticated one must still be denied — proving both directions of
        // the fix, not just the fail-closed half the stub trivially satisfied.
        let store = mem_store();
        let auth = S3Auth {
            access_key: "AKIA_TEST".into(),
            secret_key: "shh".into(),
        };

        // Unauthenticated: no Authorization header at all → still denied.
        let denied = handle(&store, &auth, &req("GET", "/", b"", &[]));
        assert_eq!(denied.status, "403 Forbidden");

        // Authenticated: a REAL SigV4 signature over this exact request → allowed
        // (reaches ListBuckets and returns a well-formed bucket-listing body, not
        // the AccessDenied error the stub always returned).
        let (authorization, host, payload_hash, amz_date) = sign_sigv4(&auth, "GET", "/", "", b"");
        let allowed = handle(
            &store,
            &auth,
            &req(
                "GET",
                "/",
                b"",
                &[
                    ("host", &host),
                    ("x-amz-content-sha256", &payload_hash),
                    ("x-amz-date", &amz_date),
                    ("authorization", &authorization),
                ],
            ),
        );
        assert_ne!(
            allowed.status,
            "403 Forbidden",
            "an authenticated carrier must be allowed through; got body: {}",
            String::from_utf8_lossy(&allowed.body)
        );
        assert_eq!(allowed.status, "200 OK");
    }

    #[test]
    fn eg176_etag_is_content_addressed() {
        // Identical bytes across two keys share the CAS digest (dedup) → same etag.
        let store = mem_store();
        handle_authorized(&store, &req("PUT", "/b", b"", &[]));
        let e1 = handle_authorized(&store, &req("PUT", "/b/a", b"same", &[]))
            .headers
            .into_iter()
            .find(|(k, _)| k == "ETag")
            .unwrap()
            .1;
        let e2 = handle_authorized(&store, &req("PUT", "/b/c", b"same", &[]))
            .headers
            .into_iter()
            .find(|(k, _)| k == "ETag")
            .unwrap()
            .1;
        assert_eq!(e1, e2);
        // And a GET returns the stored bytes.
        assert_eq!(
            handle_authorized(&store, &req("GET", "/b/a", b"", &[])).body,
            b"same"
        );
    }

    #[test]
    fn stored_object_metadata_rejects_declared_allocation_bombs() {
        let store = mem_store();
        store.create_bucket("bucket").unwrap();
        store
            .kv
            .put(
                OBJECT_NS,
                &S3Store::obj_key("bucket", "object"),
                vec![0xdd, 0xff, 0xff, 0xff, 0xff],
            )
            .unwrap();
        assert!(store.object_meta("bucket", "object").is_err());
    }

    // ── multipart upload + ranged reads (CONCEPT:EG-KG.txn.pubsub-transactions) ─────────────────────────

    /// Pull the `<UploadId>` out of an InitiateMultipartUpload XML reply.
    fn upload_id_of(xml: &str) -> String {
        let start = xml.find("<UploadId>").unwrap() + "<UploadId>".len();
        let end = xml.find("</UploadId>").unwrap();
        xml[start..end].to_string()
    }

    #[test]
    fn eg307_multipart_create_upload_complete_roundtrips_object() {
        let store = mem_store();
        handle_authorized(&store, &req("PUT", "/b", b"", &[]));
        // CreateMultipartUpload → an UploadId.
        let init = handle_authorized(
            &store,
            &req(
                "POST",
                "/b/big.txt?uploads",
                b"",
                &[("content-type", "text/plain")],
            ),
        );
        assert_eq!(init.status, "200 OK");
        let xml = String::from_utf8(init.body).unwrap();
        assert!(xml.contains("<InitiateMultipartUploadResult"), "{xml}");
        let uid = upload_id_of(&xml);

        // UploadPart 1 + 2 (out of order arrival is fine — completion sorts).
        let p2 = handle_authorized(
            &store,
            &req(
                "PUT",
                &format!("/b/big.txt?partNumber=2&uploadId={uid}"),
                b"world!",
                &[],
            ),
        );
        assert_eq!(p2.status, "200 OK");
        assert!(p2.headers.iter().any(|(k, _)| k == "ETag"));
        let p1 = handle_authorized(
            &store,
            &req(
                "PUT",
                &format!("/b/big.txt?partNumber=1&uploadId={uid}"),
                b"Hello, ",
                &[],
            ),
        );
        assert_eq!(p1.status, "200 OK");

        // ListParts shows both, ascending.
        let lp = handle_authorized(
            &store,
            &req("GET", &format!("/b/big.txt?uploadId={uid}"), b"", &[]),
        );
        let lpx = String::from_utf8(lp.body).unwrap();
        assert!(lpx.contains("<PartNumber>1</PartNumber>"), "{lpx}");
        assert!(lpx.contains("<PartNumber>2</PartNumber>"), "{lpx}");

        // CompleteMultipartUpload concatenates → one object.
        let done = handle_authorized(
            &store,
            &req("POST", &format!("/b/big.txt?uploadId={uid}"), b"", &[]),
        );
        assert_eq!(done.status, "200 OK");
        assert!(String::from_utf8_lossy(&done.body).contains("<CompleteMultipartUploadResult"));

        // The assembled object round-trips the concatenated bytes.
        let get = handle_authorized(&store, &req("GET", "/b/big.txt", b"", &[]));
        assert_eq!(get.status, "200 OK");
        assert_eq!(get.body, b"Hello, world!");
        assert_eq!(get.content_type, "text/plain");

        // The upload id is consumed — a second complete is NoSuchUpload.
        let again = handle_authorized(
            &store,
            &req("POST", &format!("/b/big.txt?uploadId={uid}"), b"", &[]),
        );
        assert_eq!(again.status, "404 Not Found");
        assert!(String::from_utf8_lossy(&again.body).contains("NoSuchUpload"));
    }

    #[test]
    fn eg307_multipart_abort_discards_upload() {
        let store = mem_store();
        handle_authorized(&store, &req("PUT", "/b", b"", &[]));
        let init = handle_authorized(&store, &req("POST", "/b/k?uploads", b"", &[]));
        let uid = upload_id_of(&String::from_utf8(init.body).unwrap());
        handle_authorized(
            &store,
            &req(
                "PUT",
                &format!("/b/k?partNumber=1&uploadId={uid}"),
                b"data",
                &[],
            ),
        );
        // Abort → 204, then the upload id is gone.
        let abort = handle_authorized(
            &store,
            &req("DELETE", &format!("/b/k?uploadId={uid}"), b"", &[]),
        );
        assert_eq!(abort.status, "204 No Content");
        let list = handle_authorized(
            &store,
            &req("GET", &format!("/b/k?uploadId={uid}"), b"", &[]),
        );
        assert_eq!(list.status, "404 Not Found");
        // And no object was ever materialized.
        assert_eq!(
            handle_authorized(&store, &req("GET", "/b/k", b"", &[])).status,
            "404 Not Found"
        );
    }

    #[test]
    fn multipart_upload_id_is_scoped_to_its_object() {
        let store = mem_store();
        handle_authorized(&store, &req("PUT", "/bucket", b"", &[]));
        let init = handle_authorized(&store, &req("POST", "/bucket/original?uploads", b"", &[]));
        let upload_id = upload_id_of(&String::from_utf8(init.body).unwrap());
        let response = handle_authorized(
            &store,
            &req(
                "PUT",
                &format!("/bucket/different?partNumber=1&uploadId={upload_id}"),
                b"data",
                &[],
            ),
        );
        assert_eq!(response.status, "404 Not Found");
    }

    #[test]
    fn eg307_range_get_returns_206_partial_content() {
        let store = mem_store();
        handle_authorized(&store, &req("PUT", "/b", b"", &[]));
        handle_authorized(&store, &req("PUT", "/b/data", b"0123456789", &[]));

        // bytes=2-5 → the inclusive slice "2345".
        let r = handle_authorized(
            &store,
            &req("GET", "/b/data", b"", &[("range", "bytes=2-5")]),
        );
        assert_eq!(r.status, "206 Partial Content");
        assert_eq!(r.body, b"2345");
        assert!(r
            .headers
            .iter()
            .any(|(k, v)| k == "Content-Range" && v == "bytes 2-5/10"));
        assert!(r
            .headers
            .iter()
            .any(|(k, v)| k == "Accept-Ranges" && v == "bytes"));

        // Open-ended bytes=5- → tail "56789".
        let r = handle_authorized(
            &store,
            &req("GET", "/b/data", b"", &[("range", "bytes=5-")]),
        );
        assert_eq!(r.status, "206 Partial Content");
        assert_eq!(r.body, b"56789");

        // Suffix bytes=-3 → the last 3 bytes "789".
        let r = handle_authorized(
            &store,
            &req("GET", "/b/data", b"", &[("range", "bytes=-3")]),
        );
        assert_eq!(r.status, "206 Partial Content");
        assert_eq!(r.body, b"789");

        // A GET with no Range header is still a whole-object 200.
        let full = handle_authorized(&store, &req("GET", "/b/data", b"", &[]));
        assert_eq!(full.status, "200 OK");
        assert_eq!(full.body, b"0123456789");
    }

    #[test]
    fn eg307_range_get_unsatisfiable_returns_416() {
        let store = mem_store();
        handle_authorized(&store, &req("PUT", "/b", b"", &[]));
        handle_authorized(&store, &req("PUT", "/b/data", b"abc", &[]));
        // Start past the end → 416.
        let r = handle_authorized(
            &store,
            &req("GET", "/b/data", b"", &[("range", "bytes=10-20")]),
        );
        assert_eq!(r.status, "416 Range Not Satisfiable");
        assert!(r
            .headers
            .iter()
            .any(|(k, v)| k == "Content-Range" && v == "bytes */3"));
    }

    #[test]
    fn eg307_parse_range_helper() {
        assert_eq!(parse_range("bytes=0-4", 10), Some((0, 4)));
        assert_eq!(parse_range("bytes=5-", 10), Some((5, 9)));
        assert_eq!(parse_range("bytes=-3", 10), Some((7, 9)));
        // End clamps to the last byte.
        assert_eq!(parse_range("bytes=8-100", 10), Some((8, 9)));
        // Unsatisfiable / malformed.
        assert_eq!(parse_range("bytes=10-12", 10), None);
        assert_eq!(parse_range("items=0-1", 10), None);
        assert_eq!(parse_range("bytes=0-4", 0), None);
    }
}
