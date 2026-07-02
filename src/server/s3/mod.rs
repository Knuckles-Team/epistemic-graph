//! S3-compatible object-storage REST surface (CONCEPT:EG-176) — an Amazon-S3 /
//! MinIO-shaped HTTP API so any S3 SDK, `aws s3` CLI, or `mc` client talks to the
//! engine's content-addressed BLOB store DIRECTLY.
//!
//! ## What this is (and is NOT)
//!
//! An ADAPTER, not a re-implemented object store. Object BYTES land in the engine's
//! content-addressed [`ChunkStore`](crate::server::blob::store::ChunkStore) BLOB
//! substrate (CONCEPT:KG-2.206) — so identical objects dedup by sha256 — while the
//! bucket / object index (the listing metadata) lives in the durable KV surface
//! (CONCEPT:EG-022), keyed for prefix-scan. Both are the engine's own durable
//! stores; nothing new is invented here.
//!
//! It is a HAND-ROLLED HTTP/1.1 listener over `tokio::net` — the SAME
//! dependency-free idiom as [`crate::server::obs`] / [`crate::server::sparql_http`]
//! / [`crate::metrics`] (NO axum/hyper/warp, so the Pi contract holds) — but its
//! own parser keeps the body as raw BYTES (S3 objects are binary) and captures the
//! request headers (for the auth guard). XML responses are hand-built strings in
//! the S3 shape.
//!
//! ## API subset (CONCEPT:EG-176)
//!
//! LANDED: `PutObject` / `GetObject` / `HeadObject` / `DeleteObject`,
//! `ListObjectsV2` (+ v1 fallback, `prefix`), `CreateBucket` / `DeleteBucket` /
//! `HeadBucket` / `ListBuckets`, and a **SigV4-lite** auth guard (anonymous when no
//! credentials are configured; when configured, the `Authorization:
//! AWS4-HMAC-SHA256` header's `Credential=<access-key>` must match and a
//! `Signature=` must be present).
//!
//! ## Multipart upload + ranged reads (CONCEPT:EG-307)
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
//! header. DEFERRED: object versioning, ACLs / bucket policies, multipart ETag
//! MD5-of-MD5 semantics, and full canonical-request HMAC signature verification.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
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
/// Env var: the access-key id. When set (with the secret), the SigV4-lite guard is
/// armed and anonymous access is refused.
pub const S3_ACCESS_KEY_ENV: &str = "EPISTEMIC_GRAPH_S3_ACCESS_KEY";
/// Env var: the secret access key (armed together with the access key).
pub const S3_SECRET_KEY_ENV: &str = "EPISTEMIC_GRAPH_S3_SECRET_KEY";

const BUCKET_NS: &str = "s3:buckets";
const OBJECT_NS: &str = "s3:objects";

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Configured credentials for the SigV4-lite guard. `None` ⇒ anonymous access.
#[derive(Clone, Debug)]
pub struct S3Auth {
    pub access_key: String,
    #[allow(dead_code)]
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

// ── the store (CONCEPT:EG-176) — objects over the BLOB CAS + KV index ────────────

/// One uploaded part of an in-progress multipart upload (CONCEPT:EG-307). The bytes
/// live in the CAS, referenced by `digest`.
#[derive(Clone, Debug)]
struct PartInfo {
    digest: String,
    size: u64,
    etag: String,
}

/// An in-progress multipart upload (CONCEPT:EG-307): the target bucket/key, its
/// content-type, and the parts received so far keyed by part number (so completion
/// concatenates them in ascending order regardless of arrival order).
#[derive(Clone, Debug)]
struct MultipartUpload {
    bucket: String,
    key: String,
    content_type: String,
    parts: BTreeMap<u32, PartInfo>,
}

/// The S3 backing store: a content-addressed [`ChunkStore`] for object bytes + a
/// durable KV index for buckets and object metadata. In-progress multipart uploads
/// are held in an in-memory registry until completed or aborted (CONCEPT:EG-307).
pub struct S3Store {
    kv: Arc<KvStore>,
    blob: Arc<dyn ChunkStore>,
    /// `uploadId` → the in-progress multipart upload state (CONCEPT:EG-307).
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
        Ok(self.kv.get(BUCKET_NS, bucket)?.is_some())
    }

    fn create_bucket(&self, bucket: &str) -> Result<(), String> {
        self.kv.put(BUCKET_NS, bucket, b"1".to_vec())
    }

    /// Delete a bucket. Errors with `BucketNotEmpty` if it still holds objects.
    fn delete_bucket(&self, bucket: &str) -> Result<bool, String> {
        if !self.list_objects(bucket, "")?.is_empty() {
            return Err("BucketNotEmpty".into());
        }
        self.kv.delete(BUCKET_NS, bucket)
    }

    fn list_buckets(&self) -> Result<Vec<String>, String> {
        Ok(self
            .kv
            .scan(BUCKET_NS, "", 0)?
            .into_iter()
            .map(|(k, _)| k)
            .collect())
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
        self.kv.put(OBJECT_NS, &Self::obj_key(bucket, key), bytes)?;
        Ok(etag)
    }

    fn object_meta(&self, bucket: &str, key: &str) -> Result<Option<ObjectMeta>, String> {
        match self.kv.get(OBJECT_NS, &Self::obj_key(bucket, key))? {
            Some(b) => Ok(Some(rmp_serde::from_slice(&b).map_err(|e| e.to_string())?)),
            None => Ok(None),
        }
    }

    fn get_object(&self, bucket: &str, key: &str) -> Result<Option<(ObjectMeta, Vec<u8>)>, String> {
        match self.object_meta(bucket, key)? {
            Some(meta) => {
                let bytes = self.blob.get_chunk(&meta.digest)?.unwrap_or_default();
                Ok(Some((meta, bytes)))
            }
            None => Ok(None),
        }
    }

    fn delete_object(&self, bucket: &str, key: &str) -> Result<bool, String> {
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
        let scan_prefix = format!("{bucket}/{prefix}");
        let bucket_prefix = format!("{bucket}/");
        let mut out = Vec::new();
        for (k, v) in self.kv.scan(OBJECT_NS, &scan_prefix, 0)? {
            let key = k.strip_prefix(&bucket_prefix).unwrap_or(&k).to_string();
            let meta: ObjectMeta = rmp_serde::from_slice(&v).map_err(|e| e.to_string())?;
            out.push((key, meta));
        }
        out.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(out)
    }

    // ── multipart upload (CONCEPT:EG-307) ─────────────────────────────────────────

    /// Begin a multipart upload, returning a fresh, process-unique `UploadId`. The
    /// upload accumulates parts in memory until completed/aborted.
    fn create_multipart(&self, bucket: &str, key: &str, content_type: &str) -> String {
        static UPLOAD_SEQ: AtomicU64 = AtomicU64::new(0);
        let seq = UPLOAD_SEQ.fetch_add(1, Ordering::Relaxed);
        let upload_id = format!("{:x}-{:x}-{:x}", std::process::id(), now_ms(), seq);
        self.uploads.lock().insert(
            upload_id.clone(),
            MultipartUpload {
                bucket: bucket.to_string(),
                key: key.to_string(),
                content_type: content_type.to_string(),
                parts: BTreeMap::new(),
            },
        );
        upload_id
    }

    /// Store one part's bytes in the CAS and record it under `part_number`. Returns
    /// the part's etag. Errors with `NoSuchUpload` for an unknown upload id.
    fn upload_part(
        &self,
        upload_id: &str,
        part_number: u32,
        body: &[u8],
    ) -> Result<String, String> {
        let (digest, _) = self.blob.put_chunk(body)?;
        let etag = format!("\"{digest}\"");
        let mut uploads = self.uploads.lock();
        let up = uploads.get_mut(upload_id).ok_or("NoSuchUpload")?;
        up.parts.insert(
            part_number,
            PartInfo {
                digest,
                size: body.len() as u64,
                etag: etag.clone(),
            },
        );
        Ok(etag)
    }

    /// List the parts received so far for an in-progress upload (ascending part
    /// number). Errors with `NoSuchUpload` for an unknown id.
    fn list_parts(&self, upload_id: &str) -> Result<Vec<(u32, PartInfo)>, String> {
        let uploads = self.uploads.lock();
        let up = uploads.get(upload_id).ok_or("NoSuchUpload")?;
        Ok(up
            .parts
            .iter()
            .map(|(n, p)| (*n, p.clone()))
            .collect())
    }

    /// Complete a multipart upload: concatenate the parts (ascending part number)
    /// into one object stored in the CAS + KV index, drop the in-progress state, and
    /// return `(bucket, key, etag)`. Errors with `NoSuchUpload` for an unknown id.
    fn complete_multipart(&self, upload_id: &str) -> Result<(String, String, String), String> {
        let up = self
            .uploads
            .lock()
            .remove(upload_id)
            .ok_or("NoSuchUpload")?;
        // Concatenate the parts in ascending part-number order (BTreeMap is ordered).
        let mut body = Vec::new();
        for (_n, part) in up.parts.iter() {
            let bytes = self.blob.get_chunk(&part.digest)?.unwrap_or_default();
            body.extend_from_slice(&bytes);
        }
        let etag = self.put_object(&up.bucket, &up.key, &body, &up.content_type)?;
        Ok((up.bucket, up.key, etag))
    }

    /// Abort a multipart upload, discarding its in-progress state. `true` if an
    /// upload with that id existed. (CAS part chunks are content-addressed and
    /// reclaimed by the blob sweep, not on this path — mirrors `delete_object`.)
    fn abort_multipart(&self, upload_id: &str) -> bool {
        self.uploads.lock().remove(upload_id).is_some()
    }
}

// ── SigV4-lite auth guard (CONCEPT:EG-176) ────────────────────────────────────────

/// SigV4-lite: with credentials configured, require an `Authorization:
/// AWS4-HMAC-SHA256` header whose `Credential=<access-key>` matches AND that
/// carries a `Signature=`. Without credentials, everything is anonymous-allowed.
/// (Full canonical-request HMAC verification is a documented follow-up.)
fn authorized(auth: &Option<S3Auth>, headers: &HashMap<String, String>) -> bool {
    let cfg = match auth {
        None => return true,
        Some(c) => c,
    };
    let hdr = match headers.get("authorization") {
        Some(h) => h,
        None => return false,
    };
    if !hdr.starts_with("AWS4-HMAC-SHA256") {
        return false;
    }
    let cred = hdr
        .split("Credential=")
        .nth(1)
        .and_then(|s| s.split('/').next())
        .map(|s| s.trim());
    let has_sig = hdr.contains("Signature=");
    cred == Some(cfg.access_key.as_str()) && has_sig
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
/// unit-testable without a socket (CONCEPT:EG-176).
fn handle(store: &S3Store, auth: &Option<S3Auth>, req: &S3Request) -> S3Response {
    if !authorized(auth, &req.headers) {
        return S3Response::error("403 Forbidden", "AccessDenied", "Access Denied");
    }
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

    // Object-level. Multipart-upload sub-resources (CONCEPT:EG-307) are selected by
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
                let uid = store.create_multipart(&bucket, &key, &ctype);
                S3Response::xml("200 OK", initiate_multipart_xml(&bucket, &key, &uid))
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
                // Ranged read (CONCEPT:EG-307) → 206 Partial Content.
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

/// Route the multipart sub-resource verbs for a known `uploadId` (CONCEPT:EG-307):
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
                Some(n) if n >= 1 => n,
                _ => {
                    return S3Response::error(
                        "400 Bad Request",
                        "InvalidArgument",
                        "partNumber must be >= 1",
                    )
                }
            };
            match store.upload_part(upload_id, pn, &req.body) {
                Ok(etag) => {
                    let mut r = S3Response::empty("200 OK");
                    r.headers.push(("ETag".into(), etag));
                    r
                }
                Err(e) if e == "NoSuchUpload" => no_such(),
                Err(e) => internal(&e),
            }
        }
        "POST" => match store.complete_multipart(upload_id) {
            Ok((b, k, etag)) => {
                S3Response::xml("200 OK", complete_multipart_xml(&b, &k, &etag))
            }
            Err(e) if e == "NoSuchUpload" => no_such(),
            Err(e) => internal(&e),
        },
        "DELETE" => {
            if store.abort_multipart(upload_id) {
                S3Response::empty("204 No Content")
            } else {
                no_such()
            }
        }
        "GET" => match store.list_parts(upload_id) {
            Ok(parts) => S3Response::xml(
                "200 OK",
                list_parts_xml(bucket, key, upload_id, &parts),
            ),
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
/// inclusive `(start, end)` byte offsets (CONCEPT:EG-307). Supports `bytes=start-end`,
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
/// `Content-Range` / `Accept-Ranges` headers (CONCEPT:EG-307). An unsatisfiable
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
            r.headers.push(("Content-Range".into(), format!("bytes */{total}")));
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
        if buf.len() > 8 * 1024 * 1024 {
            return None; // header flood guard
        }
    };
    let head = String::from_utf8_lossy(&buf[..header_end]).to_string();
    let mut lines = head.split("\r\n");
    let request_line = lines.next()?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next()?.to_string();
    let target = parts.next()?.to_string();
    let (raw_path, query) = match target.split_once('?') {
        Some((p, q)) => (p, q.to_string()),
        None => (target.as_str(), String::new()),
    };
    let path = percent_decode(raw_path);

    let mut headers = HashMap::new();
    let mut content_length = 0usize;
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            let key = k.trim().to_ascii_lowercase();
            let val = v.trim().to_string();
            if key == "content-length" {
                content_length = val.parse().unwrap_or(0);
            }
            headers.insert(key, val);
        }
    }
    let mut body = buf[header_end + 4..].to_vec();
    while body.len() < content_length {
        let n = stream.read(&mut tmp).await.ok()?;
        if n == 0 {
            break;
        }
        body.extend_from_slice(&tmp[..n]);
    }
    if content_length > 0 && body.len() > content_length {
        body.truncate(content_length);
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
/// set (CONCEPT:EG-176). One task per connection, one response per request,
/// connection: close — the SAME idiom as the obs / SPARQL listeners.
pub async fn serve(addr: &str, state: Arc<RwLock<ServerState>>) -> std::io::Result<()> {
    let persist_dir = { state.read().await.persist_dir.clone() };
    let store = Arc::new(S3Store::open(persist_dir.as_deref()).map_err(std::io::Error::other)?);
    let auth = resolve_auth();
    serve_with_store(addr, store, auth).await
}

/// Resolve the SigV4-lite credentials from the env (both must be set to arm the
/// guard; else anonymous).
fn resolve_auth() -> Option<S3Auth> {
    let access = std::env::var(S3_ACCESS_KEY_ENV)
        .ok()
        .filter(|s| !s.is_empty());
    let secret = std::env::var(S3_SECRET_KEY_ENV)
        .ok()
        .filter(|s| !s.is_empty());
    match (access, secret) {
        (Some(access_key), Some(secret_key)) => Some(S3Auth {
            access_key,
            secret_key,
        }),
        _ => None,
    }
}

/// `serve` with an EXPLICIT store + auth (CONCEPT:EG-176) — tests bind an ephemeral
/// store on a random port without process env / `ServerState`.
pub async fn serve_with_store(
    addr: &str,
    store: Arc<S3Store>,
    auth: Option<S3Auth>,
) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    tracing::info!(
        "s3-api: serving S3-compatible REST surface on {} (durable={}, auth={})",
        addr,
        store.is_durable(),
        auth.is_some()
    );
    loop {
        let (mut stream, _peer) = listener.accept().await?;
        let store = store.clone();
        let auth = auth.clone();
        tokio::spawn(async move {
            let resp = match read_request(&mut stream).await {
                Some(req) => handle(&store, &auth, &req),
                None => S3Response::error("400 Bad Request", "InvalidRequest", "malformed"),
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
    //! CONCEPT:EG-176 — object PUT/GET/HEAD/List/Delete round-trip over the CAS+KV
    //! store, bucket lifecycle, the SigV4-lite auth accept/reject, and helper
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
        let none = None;
        // CreateBucket.
        assert_eq!(
            handle(&store, &none, &req("PUT", "/mybucket", b"", &[])).status,
            "200 OK"
        );
        // PutObject.
        let put = handle(
            &store,
            &none,
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
        let get = handle(&store, &none, &req("GET", "/mybucket/hello.txt", b"", &[]));
        assert_eq!(get.status, "200 OK");
        assert_eq!(get.body, b"hello world");
        assert_eq!(get.content_type, "text/plain");
        // HeadObject → headers, no body.
        let head = handle(&store, &none, &req("HEAD", "/mybucket/hello.txt", b"", &[]));
        assert_eq!(head.status, "200 OK");
        assert!(head.head_only);
        // ListObjectsV2 → the key appears in the XML.
        let list = handle(
            &store,
            &none,
            &req("GET", "/mybucket?list-type=2", b"", &[]),
        );
        assert_eq!(list.status, "200 OK");
        let xml = String::from_utf8(list.body).unwrap();
        assert!(xml.contains("<Key>hello.txt</Key>"), "{xml}");
        assert!(xml.contains("<Size>11</Size>"), "{xml}");
        // DeleteObject → 204, then GET → 404.
        assert_eq!(
            handle(
                &store,
                &none,
                &req("DELETE", "/mybucket/hello.txt", b"", &[])
            )
            .status,
            "204 No Content"
        );
        assert_eq!(
            handle(&store, &none, &req("GET", "/mybucket/hello.txt", b"", &[])).status,
            "404 Not Found"
        );
    }

    #[test]
    fn eg176_missing_bucket_and_key_errors() {
        let store = mem_store();
        let none = None;
        // PUT object into a non-existent bucket → NoSuchBucket.
        let r = handle(&store, &none, &req("PUT", "/ghost/x", b"data", &[]));
        assert_eq!(r.status, "404 Not Found");
        assert!(String::from_utf8_lossy(&r.body).contains("NoSuchBucket"));
        // GET a missing key → NoSuchKey.
        handle(&store, &none, &req("PUT", "/b", b"", &[]));
        let r = handle(&store, &none, &req("GET", "/b/missing", b"", &[]));
        assert!(String::from_utf8_lossy(&r.body).contains("NoSuchKey"));
    }

    #[test]
    fn eg176_list_buckets_and_bucket_lifecycle() {
        let store = mem_store();
        let none = None;
        handle(&store, &none, &req("PUT", "/b1", b"", &[]));
        handle(&store, &none, &req("PUT", "/b2", b"", &[]));
        let xml =
            String::from_utf8(handle(&store, &none, &req("GET", "/", b"", &[])).body).unwrap();
        assert!(
            xml.contains("<Name>b1</Name>") && xml.contains("<Name>b2</Name>"),
            "{xml}"
        );
        // A non-empty bucket refuses deletion.
        handle(&store, &none, &req("PUT", "/b1/obj", b"x", &[]));
        assert_eq!(
            handle(&store, &none, &req("DELETE", "/b1", b"", &[])).status,
            "409 Conflict"
        );
        // An empty bucket deletes.
        assert_eq!(
            handle(&store, &none, &req("DELETE", "/b2", b"", &[])).status,
            "204 No Content"
        );
    }

    #[test]
    fn eg176_sigv4_lite_auth_accept_reject() {
        let store = mem_store();
        let auth = Some(S3Auth {
            access_key: "AKIA_TEST".into(),
            secret_key: "shh".into(),
        });
        // No Authorization header → 403.
        let r = handle(&store, &auth, &req("GET", "/", b"", &[]));
        assert_eq!(r.status, "403 Forbidden");
        // Correct access key + a Signature → accepted.
        let good = "AWS4-HMAC-SHA256 Credential=AKIA_TEST/20130524/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=abc123";
        let r = handle(
            &store,
            &auth,
            &req("GET", "/", b"", &[("authorization", good)]),
        );
        assert_eq!(r.status, "200 OK");
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

    #[test]
    fn eg176_authorized_helper_anonymous_when_unconfigured() {
        // No configured credentials ⇒ anonymous access is allowed.
        assert!(authorized(&None, &HashMap::new()));
    }

    #[test]
    fn eg176_etag_is_content_addressed() {
        // Identical bytes across two keys share the CAS digest (dedup) → same etag.
        let store = mem_store();
        let none = None;
        handle(&store, &none, &req("PUT", "/b", b"", &[]));
        let e1 = handle(&store, &none, &req("PUT", "/b/a", b"same", &[]))
            .headers
            .into_iter()
            .find(|(k, _)| k == "ETag")
            .unwrap()
            .1;
        let e2 = handle(&store, &none, &req("PUT", "/b/c", b"same", &[]))
            .headers
            .into_iter()
            .find(|(k, _)| k == "ETag")
            .unwrap()
            .1;
        assert_eq!(e1, e2);
        // And a GET returns the stored bytes.
        assert_eq!(
            handle(&store, &none, &req("GET", "/b/a", b"", &[])).body,
            b"same"
        );
    }

    // ── multipart upload + ranged reads (CONCEPT:EG-307) ─────────────────────────

    /// Pull the `<UploadId>` out of an InitiateMultipartUpload XML reply.
    fn upload_id_of(xml: &str) -> String {
        let start = xml.find("<UploadId>").unwrap() + "<UploadId>".len();
        let end = xml.find("</UploadId>").unwrap();
        xml[start..end].to_string()
    }

    #[test]
    fn eg307_multipart_create_upload_complete_roundtrips_object() {
        let store = mem_store();
        let none = None;
        handle(&store, &none, &req("PUT", "/b", b"", &[]));
        // CreateMultipartUpload → an UploadId.
        let init = handle(
            &store,
            &none,
            &req("POST", "/b/big.txt?uploads", b"", &[("content-type", "text/plain")]),
        );
        assert_eq!(init.status, "200 OK");
        let xml = String::from_utf8(init.body).unwrap();
        assert!(xml.contains("<InitiateMultipartUploadResult"), "{xml}");
        let uid = upload_id_of(&xml);

        // UploadPart 1 + 2 (out of order arrival is fine — completion sorts).
        let p2 = handle(
            &store,
            &none,
            &req(
                "PUT",
                &format!("/b/big.txt?partNumber=2&uploadId={uid}"),
                b"world!",
                &[],
            ),
        );
        assert_eq!(p2.status, "200 OK");
        assert!(p2.headers.iter().any(|(k, _)| k == "ETag"));
        let p1 = handle(
            &store,
            &none,
            &req(
                "PUT",
                &format!("/b/big.txt?partNumber=1&uploadId={uid}"),
                b"Hello, ",
                &[],
            ),
        );
        assert_eq!(p1.status, "200 OK");

        // ListParts shows both, ascending.
        let lp = handle(
            &store,
            &none,
            &req("GET", &format!("/b/big.txt?uploadId={uid}"), b"", &[]),
        );
        let lpx = String::from_utf8(lp.body).unwrap();
        assert!(lpx.contains("<PartNumber>1</PartNumber>"), "{lpx}");
        assert!(lpx.contains("<PartNumber>2</PartNumber>"), "{lpx}");

        // CompleteMultipartUpload concatenates → one object.
        let done = handle(
            &store,
            &none,
            &req("POST", &format!("/b/big.txt?uploadId={uid}"), b"", &[]),
        );
        assert_eq!(done.status, "200 OK");
        assert!(String::from_utf8_lossy(&done.body).contains("<CompleteMultipartUploadResult"));

        // The assembled object round-trips the concatenated bytes.
        let get = handle(&store, &none, &req("GET", "/b/big.txt", b"", &[]));
        assert_eq!(get.status, "200 OK");
        assert_eq!(get.body, b"Hello, world!");
        assert_eq!(get.content_type, "text/plain");

        // The upload id is consumed — a second complete is NoSuchUpload.
        let again = handle(
            &store,
            &none,
            &req("POST", &format!("/b/big.txt?uploadId={uid}"), b"", &[]),
        );
        assert_eq!(again.status, "404 Not Found");
        assert!(String::from_utf8_lossy(&again.body).contains("NoSuchUpload"));
    }

    #[test]
    fn eg307_multipart_abort_discards_upload() {
        let store = mem_store();
        let none = None;
        handle(&store, &none, &req("PUT", "/b", b"", &[]));
        let init = handle(&store, &none, &req("POST", "/b/k?uploads", b"", &[]));
        let uid = upload_id_of(&String::from_utf8(init.body).unwrap());
        handle(
            &store,
            &none,
            &req(
                "PUT",
                &format!("/b/k?partNumber=1&uploadId={uid}"),
                b"data",
                &[],
            ),
        );
        // Abort → 204, then the upload id is gone.
        let abort = handle(
            &store,
            &none,
            &req("DELETE", &format!("/b/k?uploadId={uid}"), b"", &[]),
        );
        assert_eq!(abort.status, "204 No Content");
        let list = handle(
            &store,
            &none,
            &req("GET", &format!("/b/k?uploadId={uid}"), b"", &[]),
        );
        assert_eq!(list.status, "404 Not Found");
        // And no object was ever materialized.
        assert_eq!(
            handle(&store, &none, &req("GET", "/b/k", b"", &[])).status,
            "404 Not Found"
        );
    }

    #[test]
    fn eg307_range_get_returns_206_partial_content() {
        let store = mem_store();
        let none = None;
        handle(&store, &none, &req("PUT", "/b", b"", &[]));
        handle(&store, &none, &req("PUT", "/b/data", b"0123456789", &[]));

        // bytes=2-5 → the inclusive slice "2345".
        let r = handle(
            &store,
            &none,
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
        let r = handle(
            &store,
            &none,
            &req("GET", "/b/data", b"", &[("range", "bytes=5-")]),
        );
        assert_eq!(r.status, "206 Partial Content");
        assert_eq!(r.body, b"56789");

        // Suffix bytes=-3 → the last 3 bytes "789".
        let r = handle(
            &store,
            &none,
            &req("GET", "/b/data", b"", &[("range", "bytes=-3")]),
        );
        assert_eq!(r.status, "206 Partial Content");
        assert_eq!(r.body, b"789");

        // A GET with no Range header is still a whole-object 200.
        let full = handle(&store, &none, &req("GET", "/b/data", b"", &[]));
        assert_eq!(full.status, "200 OK");
        assert_eq!(full.body, b"0123456789");
    }

    #[test]
    fn eg307_range_get_unsatisfiable_returns_416() {
        let store = mem_store();
        let none = None;
        handle(&store, &none, &req("PUT", "/b", b"", &[]));
        handle(&store, &none, &req("PUT", "/b/data", b"abc", &[]));
        // Start past the end → 416.
        let r = handle(
            &store,
            &none,
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
