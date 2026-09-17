use super::*;

fn frame_test_limits() -> ConnectionLimits {
    ConnectionLimits {
        request_bytes: 4_096,
        response_bytes: 4_096,
        msgpack_items: 64,
        io_timeout: std::time::Duration::from_secs(1),
        dispatch_deadline: std::time::Duration::from_secs(1),
    }
}

fn frame_test_request(id: u64) -> Request {
    Request {
        id,
        graph: "tenant__local__transport".to_string(),
        auth_token: String::new(),
        agent_id: Some("unsigned-agent-assertion".to_string()),
        method: Method::Ping,
    }
}

fn frame_test_payload(payload: &[u8]) -> Vec<u8> {
    let mut frame = (payload.len() as u32).to_be_bytes().to_vec();
    frame.extend_from_slice(payload);
    frame
}

#[tokio::test]
async fn frame_decode_error_answers_its_id_and_preserves_next_request() {
    let invalid = rmp_serde::to_vec_named(&serde_json::json!({
        "id": 71,
        "graph": "tenant__local__transport",
        "auth_token": "",
        "agent_id": "unsigned-agent-assertion",
        "method": "NoSuchMethod",
    }))
    .expect("encode invalid request fixture");
    assert!(rmp_serde::from_slice::<Request>(&invalid).is_err());
    let valid = rmp_serde::to_vec_named(&frame_test_request(72)).expect("encode valid request");
    let mut wire = frame_test_payload(&invalid);
    wire.extend_from_slice(&frame_test_payload(&valid));
    let mut reader = wire.as_slice();
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);

    let request = next_request(&mut reader, &tx, frame_test_limits())
        .await
        .expect("the next bounded request remains readable");
    assert_eq!(request.id, 72);
    let frame = rx.recv().await.expect("malformed request receives a reply");
    let response: Response = rmp_serde::from_slice(&frame[4..]).expect("decode error reply");
    assert_eq!(response.id, 71);
    assert_eq!(
        response.error.as_deref(),
        Some("INVALID_ARGUMENT: invalid request encoding")
    );
    assert!(reader.is_empty());
}

#[tokio::test]
async fn invalid_prefix_closes_without_consuming_its_body() {
    let mut wire = 4_097_u32.to_be_bytes().to_vec();
    wire.extend_from_slice(&[9, 8, 7, 6]);
    let mut reader = wire.as_slice();
    let response = match read_request_frame(&mut reader, frame_test_limits()).await {
        Err(FrameReadError::Closed(Some(response))) => response,
        _ => panic!("an oversized prefix must terminate before its body is read"),
    };
    assert_eq!(response.id, 0);
    assert_eq!(reader, &[9, 8, 7, 6]);
}

#[tokio::test(start_paused = true)]
async fn body_read_deadline_closes_without_dispatching() {
    use tokio::io::AsyncWriteExt;

    let (mut client, mut reader) = tokio::io::duplex(64);
    client
        .write_all(&8_u32.to_be_bytes())
        .await
        .expect("write prefix");
    client.write_all(&[0]).await.expect("write incomplete body");
    let start = tokio::time::Instant::now();
    assert!(matches!(
        read_request_frame(&mut reader, frame_test_limits()).await,
        Err(FrameReadError::Closed(None))
    ));
    assert!(start.elapsed() >= frame_test_limits().io_timeout);
    drop(client);
}

#[tokio::test]
async fn response_writer_drains_last_sender_and_preserves_frame_order() {
    use tokio::io::AsyncReadExt;

    let (mut client, server) = tokio::io::duplex(1_024);
    let (tx, rx) = tokio::sync::mpsc::channel(2);
    let last_sender = tx.clone();
    let first = encode_frame(&Response::err(1, "first"));
    let second = encode_frame(&Response::err(2, "second"));
    let writer = tokio::spawn(write_responses(server, rx, frame_test_limits().io_timeout));
    tx.send(first.clone()).await.expect("queue first reply");
    drop(tx);
    last_sender
        .send(second.clone())
        .await
        .expect("queue last reply");
    drop(last_sender);
    tokio::time::timeout(std::time::Duration::from_secs(5), writer)
        .await
        .expect("all sender drops must let the writer drain")
        .expect("writer task");
    let mut bytes = Vec::new();
    client
        .read_to_end(&mut bytes)
        .await
        .expect("read drained replies");
    assert_eq!(bytes, [first, second].concat());
}

#[tokio::test]
async fn qos_verification_rejects_unsigned_agent_before_admission() {
    let scheduler = Arc::new(super::super::qos::QosScheduler::new(
        super::super::qos::QosConfig::auto(8),
    ));
    let state = RwLock::new(ServerState::new_for_test(
        "transport-test-secret",
        ServerState::test_isolation("transport-test-agent"),
    ));
    let response = match admit_qos_request(&frame_test_request(81), &state, Some(&scheduler)).await
    {
        Err(response) => response,
        Ok(_) => panic!("an unsigned agent assertion must never enter QoS accounting"),
    };
    assert_eq!(response.id, 81);
    assert_eq!(response.error.as_deref(), Some("Authentication failed"));
    assert_eq!(scheduler.stats().in_flight, 0);
}

#[test]
fn conn_guard_refcounts() {
    let coord = ShutdownCoordinator::new();
    assert_eq!(coord.active_connections(), 0);
    let g1 = ConnGuard::new(coord.clone());
    let g2 = ConnGuard::new(coord.clone());
    assert_eq!(coord.active_connections(), 2);
    drop(g1);
    assert_eq!(coord.active_connections(), 1);
    drop(g2);
    assert_eq!(coord.active_connections(), 0);
}

#[test]
fn encode_frame_is_len_prefixed_and_decodes() {
    // CONCEPT:EG-KG.backend.framed-response — a framed response is `4-byte BE len ++ MessagePack body`,
    // and the body round-trips back to the same id/result so the client can
    // demux it out of order.
    let resp = Response::ok(42, crate::protocol::ResultPayload::String("pong".into()));
    let frame = encode_frame(&resp);
    assert!(frame.len() > 4);
    let declared = u32::from_be_bytes([frame[0], frame[1], frame[2], frame[3]]) as usize;
    assert_eq!(
        declared,
        frame.len() - 4,
        "len prefix must match body length"
    );
    let decoded: Response = rmp_serde::from_slice(&frame[4..]).expect("decode body");
    assert_eq!(decoded.id, 42, "id preserved so the client demuxes by it");
}

#[test]
fn socket_mode_default_matches_prior_hardcoded_value() {
    // The shipped clap default ("0600") must parse to exactly the value this
    // code used to hardcode, so upgrading changes no deployment's behavior.
    assert_eq!(parse_unix_socket_mode("0600").unwrap(), 0o600);
}

#[test]
fn socket_mode_accepts_group_readable_variants() {
    assert_eq!(parse_unix_socket_mode("0660").unwrap(), 0o660);
    assert_eq!(parse_unix_socket_mode("660").unwrap(), 0o660);
    assert_eq!(parse_unix_socket_mode("0o660").unwrap(), 0o660);
    assert_eq!(parse_unix_socket_mode(" 0640 ").unwrap(), 0o640);
}

#[test]
fn socket_mode_refuses_world_bits() {
    // Never a path to "just make it writable" — any nonzero "other" bit is
    // refused outright, whether read, write, or execute.
    for world_open in ["0601", "0604", "0606", "0607", "0777"] {
        let err = parse_unix_socket_mode(world_open)
            .expect_err(&format!("{world_open} should be refused"));
        assert!(
            err.contains("world"),
            "error for {world_open} should explain the world-bit refusal: {err}"
        );
    }
}

#[test]
fn socket_mode_refuses_garbage_and_out_of_range() {
    assert!(parse_unix_socket_mode("").is_err());
    assert!(parse_unix_socket_mode("not-octal").is_err());
    assert!(
        parse_unix_socket_mode("999").is_err(),
        "9 is not a valid octal digit"
    );
    assert!(
        parse_unix_socket_mode("07777").is_err(),
        "exceeds a file mode's 0777 range"
    );
}

#[cfg(any(unix, feature = "server-tls"))]
fn unique_socket_path(label: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "epistemic-graph-{label}-{}-{}.sock",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos()
    ))
}

/// A short, test-owned base directory for a UDS path that itself needs a
/// missing parent underneath it. `sockaddr_un` caps `sun_path` at 108 bytes
/// on Linux; build hosts set a long, deeply nested `TMPDIR` for lane
/// isolation, and `unique_socket_path` under that `TMPDIR` plus this test's
/// extra `<dir>/graph.sock` component can exceed the limit, so `bind` fails
/// with `EINVAL` before the missing-parent `ENOENT` this test asserts.
/// Anchoring at `/tmp` directly (not `std::env::temp_dir()`) keeps the whole
/// path well under the limit regardless of `TMPDIR`.
#[cfg(unix)]
fn short_socket_dir(label: &str) -> std::path::PathBuf {
    std::path::PathBuf::from("/tmp").join(format!(
        "eg-uds-{label}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock")
            .as_nanos()
    ))
}

#[cfg(unix)]
fn uds_test_state() -> Arc<RwLock<ServerState>> {
    Arc::new(RwLock::new(ServerState::new_for_test(
        "transport-test-secret",
        ServerState::test_isolation("transport-test-agent"),
    )))
}

#[cfg(unix)]
#[tokio::test(flavor = "current_thread")]
async fn uds_setup_keeps_current_thread_responsive_and_shutdown_cancels_accept() {
    use std::os::unix::fs::PermissionsExt;

    let path = unique_socket_path("responsive");
    std::fs::write(&path, b"stale socket placeholder").expect("write stale path");
    let socket_path = path.to_string_lossy().into_owned();
    let coord = ShutdownCoordinator::new();
    let server_coord = coord.clone();
    let server_socket_path = socket_path.clone();
    let server = tokio::spawn(async move {
        serve_uds(&server_socket_path, 0o640, uds_test_state(), server_coord).await
    });

    let stream = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        loop {
            match tokio::net::UnixStream::connect(&socket_path).await {
                Ok(stream) => break stream,
                Err(error)
                    if matches!(
                        error.kind(),
                        std::io::ErrorKind::NotFound | std::io::ErrorKind::ConnectionRefused
                    ) =>
                {
                    tokio::task::yield_now().await;
                }
                Err(error) => panic!("unexpected UDS setup failure: {error}"),
            }
        }
    })
    .await
    .expect("UDS setup must not block the current-thread executor");
    assert_eq!(
        std::fs::metadata(&path)
            .expect("socket metadata")
            .permissions()
            .mode()
            & 0o777,
        0o640
    );

    drop(stream);
    coord.trigger();
    tokio::time::timeout(std::time::Duration::from_secs(5), server)
        .await
        .expect("shutdown must cancel the pending accept")
        .expect("server task")
        .expect("serve UDS");
    let _ = std::fs::remove_file(path);
}

#[cfg(unix)]
#[tokio::test(flavor = "current_thread")]
async fn uds_setup_propagates_bind_errors_without_entering_accept_loop() {
    let missing_parent = short_socket_dir("missing-parent");
    let socket_path = missing_parent.join("graph.sock");
    let socket_path_text = socket_path.to_string_lossy().into_owned();
    let error = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        serve_uds(
            &socket_path_text,
            0o600,
            uds_test_state(),
            ShutdownCoordinator::new(),
        ),
    )
    .await
    .expect("failed setup must return without blocking")
    .expect_err("binding below a missing parent must fail");
    assert_eq!(error.kind(), std::io::ErrorKind::NotFound);
    assert!(!socket_path.exists());
}

#[cfg(all(unix, feature = "server-tls"))]
#[test]
fn tls_prepare_keeps_a_current_thread_runtime_responsive() {
    use crate::test_rendezvous::{join_bounded, meet};

    let root = unique_socket_path("tls-offload");
    std::fs::create_dir(&root).expect("create TLS test directory");
    let cert_path = root.join("certificate.pipe");
    let key_path = root.join("private-key.pem");
    let status = std::process::Command::new("mkfifo")
        .arg(&cert_path)
        .status()
        .expect("run mkfifo");
    assert!(status.success(), "mkfifo must create the blocking fixture");

    let progress = Arc::new(AtomicUsize::new(0));
    let rendezvous = Arc::new(std::sync::Barrier::new(2));
    let watchdog_released = Arc::new(AtomicBool::new(false));
    let runtime_progress = progress.clone();
    let runtime_rendezvous = rendezvous.clone();
    let runtime_cert_path = cert_path.clone();
    let runtime_key_path = key_path.clone();
    let runtime = std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build current-thread runtime")
            .block_on(async move {
                let preparation = tokio::spawn(prepare_tcp_tls(TcpTlsConfig {
                    cert_path: runtime_cert_path.to_string_lossy().into_owned(),
                    key_path: runtime_key_path.to_string_lossy().into_owned(),
                    client_ca_path: None,
                }));
                tokio::task::yield_now().await;
                runtime_progress.fetch_add(1, Ordering::SeqCst);
                meet(
                    &runtime_rendezvous,
                    "TLS-preparation runtime at the rendezvous",
                );
                preparation.await.expect("TLS preparation task")
            })
    });

    // If the FIFO read regresses onto the current-thread runtime, release it
    // after a bounded interval so this proof fails instead of hanging CI.
    let watchdog_cert_path = cert_path.clone();
    let watchdog_progress = progress.clone();
    let watchdog_flag = watchdog_released.clone();
    let (watchdog_cancel, watchdog_cancelled) = std::sync::mpsc::sync_channel(1);
    let watchdog = std::thread::spawn(move || {
        if watchdog_cancelled
            .recv_timeout(std::time::Duration::from_secs(2))
            .is_err()
            && watchdog_progress.load(Ordering::SeqCst) == 0
        {
            watchdog_flag.store(true, Ordering::SeqCst);
            std::fs::write(watchdog_cert_path, b"invalid certificate")
                .expect("release blocked certificate read");
        }
    });

    meet(
        &rendezvous,
        "the test joining the TLS-preparation rendezvous",
    );
    let _ = watchdog_cancel.send(());
    if !watchdog_released.load(Ordering::SeqCst) {
        std::fs::write(&cert_path, b"invalid certificate").expect("complete certificate read");
    }
    let error = match join_bounded(runtime, "the current-thread TLS runtime") {
        Ok(_) => panic!("invalid certificate must fail closed"),
        Err(error) => error,
    };
    join_bounded(watchdog, "the TLS-read watchdog thread");

    assert_eq!(progress.load(Ordering::SeqCst), 1);
    assert!(
        !watchdog_released.load(Ordering::SeqCst),
        "TLS material read blocked the current-thread runtime"
    );
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    assert_eq!(error.to_string(), "server TLS certificate invalid");
    assert!(!error.to_string().contains(root.to_string_lossy().as_ref()));
    std::fs::remove_dir_all(root).expect("remove TLS test directory");
}

#[cfg(feature = "server-tls")]
#[tokio::test(flavor = "current_thread")]
async fn tls_prepare_keeps_missing_material_errors_private() {
    let missing = unique_socket_path("private-tls-error");
    let missing_text = missing.to_string_lossy().into_owned();
    let error = match prepare_tcp_tls(TcpTlsConfig {
        cert_path: missing_text.clone(),
        key_path: missing_text.clone(),
        client_ca_path: None,
    })
    .await
    {
        Ok(_) => panic!("missing TLS material must fail closed"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    assert_eq!(error.to_string(), "server TLS certificate unavailable");
    assert!(!error.to_string().contains(&missing_text));
}

#[test]
fn per_connection_limit_is_bounded_and_positive() {
    // CONCEPT:EG-KG.backend.framed-response — the per-connection in-flight cap auto-sizes from cores
    // but is always clamped so one connection can neither stall (floor) nor
    // spawn unbounded work (ceiling).
    let n = per_connection_inflight_limit();
    assert!((8..=1024).contains(&n), "per-conn cap {n} out of [8,1024]");
}

#[test]
fn dispatch_deadline_is_bounded_and_positive() {
    // CONCEPT:EG-KG.coordination.backpressure-busy-signal — the hard dispatch ceiling
    // always resolves to a finite, positive duration, so "no bound at all" is not a
    // reachable configuration.
    let d = dispatch_deadline();
    assert!(d > std::time::Duration::ZERO);
    assert!(d <= std::time::Duration::from_secs(86_400));
}

#[tokio::test]
async fn hung_dispatch_is_abandoned_with_a_typed_error() {
    // A dispatch that never completes must still produce a reply for its id, so the
    // caller returns (and its permits drop) instead of parking forever.
    let resp = dispatch_within_deadline(
        std::future::pending::<Response>(),
        std::time::Duration::from_millis(20),
        77,
    )
    .await;
    assert_eq!(resp.id, 77, "the abandoned request is still answered by id");
    assert!(
        resp.error
            .as_deref()
            .unwrap_or_default()
            .starts_with("TIMEOUT:"),
        "expected a typed timeout error, got {:?}",
        resp.error
    );
}

/// D-HYD-2 defect pin. THE livelock, reproduced in miniature.
///
/// The live incident: the shard-3 durable writer thread wedged in an unbounded
/// userspace loop, so every dispatch that needed it parked forever on its completion
/// oneshot. Each of those tasks holds a `QosPermit`, and — because the deployment
/// resolves EVERY caller (server, host daemon, scheduler, MCP, external agents) to
/// ONE verified principal scope — they all draw on ONE per-principal in-flight quota.
/// 96 stranded dispatches pinned that principal at its quota (`capacity/4` of 384)
/// permanently, so for 2.5 days the engine shed 100% of requests, INCLUDING reads
/// that never touch the wedged shard, with `BUSY: QoS per-principal quota exhausted`.
///
/// The invariant this pins: a dispatch that never completes must not permanently
/// retain its admission slot. Revert `dispatch_within_deadline`'s use at the dispatch
/// site (await the raw future) and this goes RED — the joins time out and the final
/// admit is still `Reject(Quota)`.
#[tokio::test]
async fn a_hung_dispatch_does_not_permanently_exhaust_the_principal_quota() {
    use crate::server::qos::{
        QosClass, QosConfig, QosDecision, QosReject, QosRequest, QosScheduler,
    };

    let mut cfg = QosConfig::auto(8);
    cfg.per_principal_quota = 2; // the live value was 96; 2 keeps the test fast
    cfg.bucket_refill_per_sec = 0.0; // isolate the QUOTA rule from the token bucket
    let sched = QosScheduler::new(cfg);
    let req = QosRequest {
        class: QosClass::Orch,
        principal: "one-shared-principal".to_string(),
        deadline_micros: None,
    };

    let deadline = std::time::Duration::from_millis(50);
    let mut stranded = Vec::new();
    for _ in 0..2 {
        let permit = match sched.try_admit(&req) {
            QosDecision::Admit(permit) => permit,
            QosDecision::Reject(why) => panic!("expected Admit, got Reject({why:?})"),
        };
        // EXACTLY the production shape: the permit rides the dispatch task and is
        // released only when that task returns.
        stranded.push(tokio::spawn(async move {
            let resp =
                dispatch_within_deadline(std::future::pending::<Response>(), deadline, 1).await;
            drop(permit);
            resp
        }));
    }

    // The observed live state: at quota, every further request is shed `Quota`.
    assert!(
        matches!(sched.try_admit(&req), QosDecision::Reject(QosReject::Quota)),
        "a principal at its in-flight quota must be shed while the work is live"
    );

    // Bounded join: with the fix reverted these never resolve, so this FAILS rather
    // than hanging the suite.
    for task in stranded {
        let resp = tokio::time::timeout(deadline * 20, task)
            .await
            .expect("a stranded dispatch must be abandoned at the deadline")
            .expect("dispatch task must not panic");
        assert!(
            resp.error.is_some(),
            "an abandoned dispatch answers with an error"
        );
    }

    // The invariant: the quota recovered on its own, with no restart.
    assert!(
        matches!(sched.try_admit(&req), QosDecision::Admit(_)),
        "a hung dispatch must not permanently retain its admission slot"
    );
}

#[test]
fn request_frame_allocation_has_a_hard_ceiling() {
    let limit = max_request_frame_bytes();
    assert!(limit > 0);
    assert!(limit <= HARD_MAX_REQUEST_FRAME_BYTES);
}

#[test]
fn msgpack_preflight_rejects_declared_allocation_bombs() {
    // array32 declares 2^32-1 values while carrying no body. The preflight
    // rejects it without allocating from the untrusted hint.
    assert!(validate_msgpack_frame(&[0xdd, 0xff, 0xff, 0xff, 0xff], 1_000).is_err());
    assert!(validate_msgpack_frame(&[0xdc, 0x00, 0x02, 0xc0], 1_000).is_err());

    let three_nils = [0x93, 0xc0, 0xc0, 0xc0];
    assert!(validate_msgpack_frame(&three_nils, 3).is_err());
    assert!(validate_msgpack_frame(&three_nils, 4).is_ok());
}

#[test]
fn msgpack_preflight_bounds_depth_and_requires_exact_frame() {
    let mut nested = vec![0x91; MAX_MSGPACK_NESTING_DEPTH + 1];
    nested.push(0xc0);
    assert!(validate_msgpack_frame(&nested, 1_000).is_err());

    let valid = rmp_serde::to_vec(&serde_json::json!({
        "method": "Ping",
        "values": [1, 2, 3]
    }))
    .unwrap();
    assert!(validate_msgpack_frame(&valid, 1_000).is_ok());
    let mut trailing = valid;
    trailing.push(0xc0);
    assert!(validate_msgpack_frame(&trailing, 1_000).is_err());
}

#[test]
fn recover_request_id_reads_the_id_of_an_otherwise_undecodable_request() {
    // U-96/U-98: `graph_type: "Ontology"` is not one of the closed
    // `GraphType` wire values (`Agent`/`Team`/`Global`/`Commons`), so the
    // full `Request` fails to decode. Before the fix, that failure always
    // answered under a synthetic id `0`, which the Python client's
    // `_pending` map never has a future for — the response is dropped and
    // the caller starves out its whole timeout/retry budget instead of
    // seeing the error immediately (see `epistemic_graph/client.py`
    // `_read_loop`). The id must still be recoverable from the same bytes
    // that failed to decode as a full `Request`.
    let payload = rmp_serde::to_vec_named(&serde_json::json!({
        "id": 555_555_u64,
        "graph": "tenant__local__ontology",
        "auth_token": "",
        "agent_id": "system",
        "method": "CreateGraph",
        "params": {
            "graph_name": "tenant__local__ontology",
            "graph_type": "Ontology",
        },
    }))
    .unwrap();

    // The full request genuinely fails to decode (proves the test reproduces
    // the actual defect, not a strawman).
    assert!(
        rmp_serde::from_slice::<Request>(&payload).is_err(),
        "an unsupported GraphType value must fail full Request decode"
    );

    // But the id is still recoverable from the very same bytes.
    assert_eq!(recover_request_id(&payload), 555_555);
}

#[test]
fn recover_request_id_falls_back_to_zero_only_when_the_id_itself_is_unreadable() {
    // A frame with no readable `id` field at all (e.g. a bare nil, or a
    // completely non-map payload) has nothing to recover — id 0 is the
    // documented last resort, not a silent success case.
    assert_eq!(recover_request_id(&[0xc0]), 0); // msgpack nil
    assert_eq!(recover_request_id(b"not msgpack at all"), 0);

    // A well-formed map missing `id` entirely also falls back.
    let no_id = rmp_serde::to_vec_named(&serde_json::json!({"graph": "g"})).unwrap();
    assert_eq!(recover_request_id(&no_id), 0);
}

#[test]
fn trigger_latches() {
    let coord = ShutdownCoordinator::new();
    assert!(!coord.is_requested());
    coord.trigger();
    assert!(coord.is_requested());
    // Idempotent.
    coord.trigger();
    assert!(coord.is_requested());
}

// ── CONCEPT:EG-KG.coordination.reserved-read-lane — reserved read-lane admission guarantee ──────────────────

/// A read MUST stay admittable when the global pool AND the per-graph cap are
/// fully saturated by writes — it falls back to the reserved read lane — while a
/// write in the same saturated state is correctly shed BUSY. This is the core
/// "an interactive read is never starved behind ingestion" guarantee, proven
/// deterministically against the pure admission function.
#[test]
fn read_is_admitted_when_write_pool_is_saturated() {
    let sem = Arc::new(Semaphore::new(4)); // tiny global pool
    let read_sem = Arc::new(Semaphore::new(2)); // small reserved read lane
    let pg_map = dashmap::DashMap::new();
    let pg_limit = 2;
    let graph = "__commons__";

    // Saturate the global pool: hold all of its permits (an in-flight ingestion
    // write firehose). With the pool drained, the NORMAL admission path fails.
    let _writers: Vec<_> = (0..sem.available_permits())
        .map(|_| sem.clone().try_acquire_owned().unwrap())
        .collect();
    assert_eq!(sem.available_permits(), 0, "global pool saturated");

    // A WRITE now sheds BUSY (back-pressured, not dropped).
    assert!(
        matches!(
            admit_request(&sem, &read_sem, &pg_map, pg_limit, graph, true),
            Admission::Busy
        ),
        "write must be shed BUSY when the global pool is full"
    );

    // A READ is STILL admitted via the reserved read lane.
    let r = admit_request(&sem, &read_sem, &pg_map, pg_limit, graph, false);
    match r {
        Admission::Granted { global, read, .. } => {
            assert!(
                global.is_none(),
                "read used the reserved lane, not the global pool"
            );
            assert!(read.is_some(), "read holds a reserved-lane permit");
        }
        Admission::Busy => panic!("read must NOT be shed BUSY while the read lane has slots"),
    }
}

/// The reserved read lane is itself bounded: a genuine read FLOOD that fills it is
/// shed BUSY so memory stays bounded — the reservation guarantees availability, not
/// unbounded admission.
#[test]
fn read_lane_is_bounded_under_a_read_flood() {
    let sem = Arc::new(Semaphore::new(1));
    let read_sem = Arc::new(Semaphore::new(2));
    let pg_map = dashmap::DashMap::new();
    let pg_limit = 1;
    let graph = "g";

    // Saturate the global pool so reads must use the reserved lane.
    let _g = sem.clone().try_acquire_owned().unwrap();
    assert_eq!(sem.available_permits(), 0);

    // Hold both reserved read permits.
    let mut reads = Vec::new();
    for _ in 0..2 {
        match admit_request(&sem, &read_sem, &pg_map, pg_limit, graph, false) {
            Admission::Granted { read, .. } => reads.push(read.expect("reserved permit")),
            Admission::Busy => panic!("reserved read lane should admit up to its size"),
        }
    }
    // The third read floods the lane → BUSY.
    assert!(
        matches!(
            admit_request(&sem, &read_sem, &pg_map, pg_limit, graph, false),
            Admission::Busy
        ),
        "a read flood that fills the reserved lane is shed BUSY (bounded memory)"
    );
    drop(reads);
    // Once a reserved slot frees, reads admit again.
    assert!(
        matches!(
            admit_request(&sem, &read_sem, &pg_map, pg_limit, graph, false),
            Admission::Granted { .. }
        ),
        "read admits again after a reserved slot frees"
    );
}

/// Concurrency stress: while many WRITE permits saturate the global pool, a burst
/// of concurrent READS on the SAME hot graph must ALL be admitted (never BUSY),
/// proving an interactive read survives under maximum write load on the firehose
/// graph. Mirrors the live K=4 ingestion symptom at the admission layer.
#[tokio::test(flavor = "multi_thread")]
async fn reads_survive_under_max_write_load_on_hot_graph() {
    let sem = Arc::new(Semaphore::new(8));
    let read_sem = Arc::new(Semaphore::new(8));
    let pg_map = Arc::new(dashmap::DashMap::new());
    let pg_limit = 4; // a quarter, like the live default
    let graph = "__commons__";

    // Saturate the global pool: hold all 8 write permits for the test duration.
    let writers: Vec<_> = (0..8)
        .map(|_| sem.clone().try_acquire_owned().unwrap())
        .collect();
    assert_eq!(sem.available_permits(), 0, "writers saturate the pool");

    // Fire a burst of concurrent reads on the SAME hot graph; every one must be
    // admitted (via the reserved lane), holding its permit briefly then releasing.
    let mut tasks = Vec::new();
    for _ in 0..200usize {
        let sem = sem.clone();
        let read_sem = read_sem.clone();
        let pg_map = pg_map.clone();
        tasks.push(tokio::spawn(async move {
            // Retry briefly: the reserved lane is small, so concurrent reads share
            // it — but each holds its slot only momentarily, so all make progress
            // without ever being permanently starved.
            for _ in 0..1000 {
                match admit_request(&sem, &read_sem, &pg_map, pg_limit, graph, false) {
                    Admission::Granted { read, .. } => {
                        assert!(read.is_some(), "served by reserved lane under saturation");
                        tokio::task::yield_now().await; // hold briefly, then drop
                        return true;
                    }
                    Admission::Busy => tokio::task::yield_now().await,
                }
            }
            false
        }));
    }
    let mut ok = 0usize;
    for t in tasks {
        if t.await.unwrap() {
            ok += 1;
        }
    }
    assert_eq!(
        ok, 200,
        "every interactive read completed under max write load"
    );
    drop(writers);
}

#[tokio::test(start_paused = true)]
async fn idle_watcher_triggers_when_idle() {
    let coord = ShutdownCoordinator::new();
    let c = coord.clone();
    let h = tokio::spawn(async move { run_idle_watcher(c, 1).await });
    // No connections ⇒ after the grace window the watcher must trigger.
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    assert!(coord.is_requested(), "idle watcher did not trigger");
    h.await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn idle_watcher_resets_on_active_connection() {
    let coord = ShutdownCoordinator::new();
    // Hold a live connection the whole time ⇒ the watcher never fires.
    let _g = ConnGuard::new(coord.clone());
    let c = coord.clone();
    tokio::spawn(async move { run_idle_watcher(c, 1).await });
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    assert!(
        !coord.is_requested(),
        "idle watcher fired despite an active connection"
    );
}
