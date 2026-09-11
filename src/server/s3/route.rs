//! What an S3 request addresses, and the verbs each address answers.
//!
//! S3's data model is three levels — the service, a bucket, an object — with
//! multipart uploads as a sub-resource selected by query parameter. Resolving
//! which of them a request names is one step; serving its verb is another.
//! Before the split both were one function of cyclomatic 46 / cognitive 43,
//! where every verb re-derived where it sat in that hierarchy.

use super::{
    complete_multipart_xml, initiate_multipart_xml, internal, list_buckets_xml, list_objects_xml,
    list_parts_xml, object_response, query_param, range_response, split_bucket_key, S3Request,
    S3Response, S3Store, MAX_S3_MULTIPART_PARTS,
};

/// What a request's path and query address.
pub(super) enum S3Target {
    /// `/` — the service itself.
    Service,
    /// `/bucket` — no object key.
    Bucket(String),
    /// `POST /bucket/key?uploads` — start a multipart upload.
    CreateUpload { bucket: String, key: String },
    /// `/bucket/key?uploadId=…` — an in-flight multipart upload.
    Upload {
        bucket: String,
        key: String,
        upload_id: String,
        part_number: Option<u32>,
    },
    /// `/bucket/key` — the object itself.
    Object { bucket: String, key: String },
}

/// Resolve what this request addresses. The multipart sub-resources are
/// selected by query parameter and take precedence over the plain object
/// verbs, which is why they are resolved before `Object`.
pub(super) fn target(req: &S3Request) -> S3Target {
    let (bucket, key) = split_bucket_key(&req.path);
    if bucket.is_empty() {
        return S3Target::Service;
    }
    if key.is_empty() {
        return S3Target::Bucket(bucket);
    }
    if query_param(&req.query, "uploads").is_some() && req.method == "POST" {
        return S3Target::CreateUpload { bucket, key };
    }
    match query_param(&req.query, "uploadId") {
        Some(upload_id) => S3Target::Upload {
            bucket,
            key,
            upload_id,
            part_number: query_param(&req.query, "partNumber").and_then(|n| n.parse().ok()),
        },
        None => S3Target::Object { bucket, key },
    }
}

impl S3Target {
    /// Serve the verb this request carries against the address it names.
    pub(super) fn serve(self, store: &S3Store, req: &S3Request) -> S3Response {
        match self {
            Self::Service => service(store, req),
            Self::Bucket(bucket) => bucket_verb(store, req, &bucket),
            Self::CreateUpload { bucket, key } => create_upload(store, req, &bucket, &key),
            Self::Upload {
                bucket,
                key,
                upload_id,
                part_number,
            } => upload_verb(store, req, &bucket, &key, &upload_id, part_number),
            Self::Object { bucket, key } => object_verb(store, req, &bucket, &key),
        }
    }
}

/// The one `405` this surface answers.
pub(super) fn method_not_allowed() -> S3Response {
    S3Response::error("405 Method Not Allowed", "MethodNotAllowed", "unsupported")
}

/// The one `NoSuchBucket` this surface answers.
fn no_such_bucket() -> S3Response {
    S3Response::error("404 Not Found", "NoSuchBucket", "no such bucket")
}

/// The one `NoSuchKey` this surface answers.
fn no_such_key() -> S3Response {
    S3Response::error("404 Not Found", "NoSuchKey", "no such key")
}

/// `None` when the bucket exists; otherwise the response the caller must
/// answer with instead of touching the object store.
fn absent_bucket(store: &S3Store, bucket: &str) -> Option<S3Response> {
    match store.bucket_exists(bucket) {
        Ok(true) => None,
        Ok(false) => Some(no_such_bucket()),
        Err(error) => Some(internal(&error)),
    }
}

/// The object content type the request declared, or S3's default.
fn declared_content_type(req: &S3Request) -> String {
    req.headers
        .get("content-type")
        .cloned()
        .unwrap_or_else(|| "application/octet-stream".to_string())
}

/// Service-level: `GET /` is ListBuckets.
fn service(store: &S3Store, req: &S3Request) -> S3Response {
    if req.method != "GET" {
        return method_not_allowed();
    }
    match store.list_buckets() {
        Ok(buckets) => S3Response::xml("200 OK", list_buckets_xml(&buckets)),
        Err(error) => internal(&error),
    }
}

/// Bucket-level verbs.
fn bucket_verb(store: &S3Store, req: &S3Request, bucket: &str) -> S3Response {
    match req.method.as_str() {
        "PUT" => create_bucket(store, bucket),
        "DELETE" => delete_bucket(store, bucket),
        "HEAD" => head_bucket(store, bucket),
        "GET" => list_objects(store, req, bucket),
        _ => method_not_allowed(),
    }
}

/// `PUT /bucket` — CreateBucket, answering the bucket's own location.
fn create_bucket(store: &S3Store, bucket: &str) -> S3Response {
    match store.create_bucket(bucket) {
        Ok(()) => {
            let mut response = S3Response::empty("200 OK");
            response
                .headers
                .push(("Location".into(), format!("/{bucket}")));
            response
        }
        Err(error) => internal(&error),
    }
}

/// `DELETE /bucket` — DeleteBucket. A non-empty bucket is a `409`, never a
/// silent partial delete.
fn delete_bucket(store: &S3Store, bucket: &str) -> S3Response {
    match store.delete_bucket(bucket) {
        Ok(_) => S3Response::empty("204 No Content"),
        Err(error) if error == "BucketNotEmpty" => S3Response::error(
            "409 Conflict",
            "BucketNotEmpty",
            "The bucket you tried to delete is not empty",
        ),
        Err(error) => internal(&error),
    }
}

/// `HEAD /bucket` — HeadBucket.
fn head_bucket(store: &S3Store, bucket: &str) -> S3Response {
    match store.bucket_exists(bucket) {
        Ok(true) => S3Response::empty("200 OK"),
        Ok(false) => no_such_bucket(),
        Err(error) => internal(&error),
    }
}

/// `GET /bucket` — ListObjects(V2): `list-type=2` and the v1 default answer
/// the same listing.
fn list_objects(store: &S3Store, req: &S3Request, bucket: &str) -> S3Response {
    let prefix = query_param(&req.query, "prefix").unwrap_or_default();
    match store.list_objects(bucket, &prefix) {
        Ok(objects) => S3Response::xml("200 OK", list_objects_xml(bucket, &prefix, &objects)),
        Err(error) => internal(&error),
    }
}

/// `POST /bucket/key?uploads` — CreateMultipartUpload.
fn create_upload(store: &S3Store, req: &S3Request, bucket: &str, key: &str) -> S3Response {
    if let Some(refusal) = absent_bucket(store, bucket) {
        return refusal;
    }
    match store.create_multipart(bucket, key, &declared_content_type(req)) {
        Ok(upload_id) => S3Response::xml("200 OK", initiate_multipart_xml(bucket, key, &upload_id)),
        Err(error) => internal(&error),
    }
}

/// Object-level verbs.
fn object_verb(store: &S3Store, req: &S3Request, bucket: &str, key: &str) -> S3Response {
    match req.method.as_str() {
        "PUT" => put_object(store, req, bucket, key),
        "GET" => get_object(store, req, bucket, key),
        "HEAD" => head_object(store, bucket, key),
        "DELETE" => match store.delete_object(bucket, key) {
            Ok(_) => S3Response::empty("204 No Content"),
            Err(error) => internal(&error),
        },
        _ => method_not_allowed(),
    }
}

/// `PUT /bucket/key` — PutObject.
fn put_object(store: &S3Store, req: &S3Request, bucket: &str, key: &str) -> S3Response {
    if let Some(refusal) = absent_bucket(store, bucket) {
        return refusal;
    }
    match store.put_object(bucket, key, &req.body, &declared_content_type(req)) {
        Ok(etag) => {
            let mut response = S3Response::empty("200 OK");
            response.headers.push(("ETag".into(), etag));
            response
        }
        Err(error) => internal(&error),
    }
}

/// `GET /bucket/key` — GetObject, or a `206` when the request carries a
/// `Range`.
fn get_object(store: &S3Store, req: &S3Request, bucket: &str, key: &str) -> S3Response {
    match store.get_object(bucket, key) {
        Ok(Some((meta, bytes))) => match req.headers.get("range") {
            Some(range) => range_response(meta, bytes, range),
            None => object_response(meta, bytes, false),
        },
        Ok(None) => no_such_key(),
        Err(error) => internal(&error),
    }
}

/// `HEAD /bucket/key` — HeadObject: the object's headers, never its bytes.
fn head_object(store: &S3Store, bucket: &str, key: &str) -> S3Response {
    match store.object_meta(bucket, key) {
        Ok(Some(meta)) => object_response(meta, Vec::new(), true),
        Ok(None) => no_such_key(),
        Err(error) => internal(&error),
    }
}

/// The one `NoSuchUpload` this surface answers.
fn no_such_upload() -> S3Response {
    S3Response::error("404 Not Found", "NoSuchUpload", "no such upload")
}

/// The verbs an in-flight multipart upload answers: `PUT` (UploadPart), `POST`
/// (CompleteMultipartUpload), `DELETE` (AbortMultipartUpload), `GET`
/// (ListParts).
fn upload_verb(
    store: &S3Store,
    req: &S3Request,
    bucket: &str,
    key: &str,
    upload_id: &str,
    part_number: Option<u32>,
) -> S3Response {
    match req.method.as_str() {
        "PUT" => upload_part(store, req, bucket, key, upload_id, part_number),
        "POST" => complete_upload(store, bucket, key, upload_id),
        "DELETE" => {
            if store.abort_multipart(bucket, key, upload_id) {
                S3Response::empty("204 No Content")
            } else {
                no_such_upload()
            }
        }
        "GET" => list_parts(store, bucket, key, upload_id),
        _ => method_not_allowed(),
    }
}

/// `PUT …?uploadId=…&partNumber=n` — UploadPart. A part number outside the
/// supported range is refused before the store is touched.
fn upload_part(
    store: &S3Store,
    req: &S3Request,
    bucket: &str,
    key: &str,
    upload_id: &str,
    part_number: Option<u32>,
) -> S3Response {
    let Some(part_number) = part_number.filter(|n| (1..=MAX_S3_MULTIPART_PARTS as u32).contains(n))
    else {
        return S3Response::error(
            "400 Bad Request",
            "InvalidArgument",
            "partNumber is outside the supported range",
        );
    };
    match store.upload_part(bucket, key, upload_id, part_number, &req.body) {
        Ok(etag) => {
            let mut response = S3Response::empty("200 OK");
            response.headers.push(("ETag".into(), etag));
            response
        }
        Err(error) if error == "NoSuchUpload" => no_such_upload(),
        Err(error) => internal(&error),
    }
}

/// `POST …?uploadId=…` — CompleteMultipartUpload.
fn complete_upload(store: &S3Store, bucket: &str, key: &str, upload_id: &str) -> S3Response {
    match store.complete_multipart(bucket, key, upload_id) {
        Ok((bucket, key, etag)) => {
            S3Response::xml("200 OK", complete_multipart_xml(&bucket, &key, &etag))
        }
        Err(error) if error == "NoSuchUpload" => no_such_upload(),
        Err(error) => internal(&error),
    }
}

/// `GET …?uploadId=…` — ListParts.
fn list_parts(store: &S3Store, bucket: &str, key: &str, upload_id: &str) -> S3Response {
    match store.list_parts(bucket, key, upload_id) {
        Ok(parts) => S3Response::xml("200 OK", list_parts_xml(bucket, key, upload_id, &parts)),
        Err(error) if error == "NoSuchUpload" => no_such_upload(),
        Err(error) => internal(&error),
    }
}
