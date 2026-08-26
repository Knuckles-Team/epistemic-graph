// CONCEPT:EG-KG.query.tokio-service-server — Tokio Service Server
//
// Long-running Tokio server that holds the GraphRegistry in memory
// and serves requests over UDS or TCP with HMAC-SHA256 authentication.

use hmac::Mac as _;

pub(crate) mod access;
pub(crate) mod auth;

/// Verified minimum stack for engine Tokio workers.
///
/// Full multimodal/job publication dispatch can exceed Tokio's 2 MiB default in
/// debug and instrumented builds. Keep one explicit product value shared by the
/// production runtime and exact integration runtimes; environment variables are
/// not part of this safety contract.
pub const ENGINE_WORKER_STACK_BYTES: usize = 4 * 1024 * 1024;

const _: () = assert!(ENGINE_WORKER_STACK_BYTES >= 4 * 1024 * 1024);

/// Start the engine's async driver on an explicitly sized stack.
///
/// Tokio's `thread_stack_size` applies only to runtime-owned workers; calling
/// `Runtime::block_on` from a default-sized launcher would still execute the
/// outer driver future on that launcher's stack. Production and exact tests use
/// this helper so both the outer driver and Tokio workers share the same verified
/// minimum. Spawn diagnostics are deliberately normalized and contain no host,
/// path, or identity data.
pub fn spawn_engine_driver<F, T>(driver: F) -> std::io::Result<std::thread::JoinHandle<T>>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    std::thread::Builder::new()
        .name("epistemic-graph-runtime-driver".to_string())
        .stack_size(ENGINE_WORKER_STACK_BYTES)
        .spawn(driver)
        .map_err(|error| {
            std::io::Error::new(error.kind(), "engine runtime driver thread could not start")
        })
}

/// Join the engine driver without reflecting a panic payload into logs or errors.
pub fn join_engine_driver<T>(driver: std::thread::JoinHandle<T>) -> std::io::Result<T> {
    driver
        .join()
        .map_err(|_| std::io::Error::other("engine runtime driver terminated unexpectedly"))
}

fn direct_wire_addr_is_loopback(addr: &str) -> bool {
    addr.parse::<std::net::SocketAddr>()
        .map(|socket| socket.ip().is_loopback())
        .unwrap_or_else(|_| {
            addr.rsplit_once(':')
                .map(|(host, port)| {
                    host.trim_matches(|character| character == '[' || character == ']')
                        .eq_ignore_ascii_case("localhost")
                        && !port.is_empty()
                        && port.chars().all(|character| character.is_ascii_digit())
                })
                .unwrap_or(false)
        })
}

/// Validate a plaintext database-compatibility listener before it binds.
///
/// PGWire, MySQL, MSSQL TDS, Bolt, AMQP, MQTT and STOMP do not terminate TLS themselves. Their direct
/// listeners therefore remain loopback-only even when the broader auxiliary
/// two-key ingress exception is enabled. A routable deployment terminates TLS
/// (preferably mTLS) in an identity-aware sidecar/gateway and connects that
/// gateway to this loopback backend. The protocol login must be
/// cryptographically verified before its principal becomes an engine ACL actor;
/// there is no anonymous or trust compatibility mode.
pub fn validate_direct_wire_security(
    addr: &str,
    surface: &str,
    verified_identity_binding: bool,
) -> std::io::Result<()> {
    if !direct_wire_addr_is_loopback(addr) {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            format!(
                "{surface} direct listener is plaintext and must bind loopback; expose it only through an authenticated TLS identity-binding gateway"
            ),
        ));
    }
    if !verified_identity_binding {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            format!(
                "{surface} listener requires cryptographically verified login-to-actor binding"
            ),
        ));
    }
    Ok(())
}

/// Convert a successfully authenticated broker principal into a stable, bounded,
/// non-reversible actor reference before it reaches engine authorization or durable
/// audit state. The same principal maps to the same actor across AMQP/MQTT/STOMP,
/// while the deployment secret prevents offline recovery of low-entropy usernames.
pub(crate) fn pseudonymous_broker_actor(secret: &str, principal: &str) -> std::io::Result<String> {
    if secret.is_empty() || principal.is_empty() || principal.len() > 4 * 1024 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "broker identity cannot be pseudonymized",
        ));
    }
    let mut mac = hmac::Hmac::<sha2::Sha256>::new_from_slice(secret.as_bytes())
        .expect("HMAC accepts any key length");
    mac.update(b"broker-actor:");
    mac.update(principal.as_bytes());
    Ok(format!(
        "broker:actor:hmac-sha256:{}",
        hex::encode(mac.finalize().into_bytes())
    ))
}

#[cfg(test)]
mod direct_wire_security_tests {
    use super::{
        join_engine_driver, pseudonymous_broker_actor, spawn_engine_driver,
        validate_direct_wire_security, ENGINE_WORKER_STACK_BYTES,
    };

    #[test]
    fn engine_driver_uses_the_verified_stack_contract_and_joins_exactly() {
        assert_eq!(ENGINE_WORKER_STACK_BYTES, 4 * 1024 * 1024);
        let driver = spawn_engine_driver(|| 41_u64).unwrap();
        assert_eq!(join_engine_driver(driver).unwrap(), 41);
    }

    #[test]
    fn engine_driver_join_failure_does_not_reflect_the_panic_payload() {
        let driver =
            spawn_engine_driver(|| -> u64 { panic!("opaque-driver-test-failure") }).unwrap();
        let error = join_engine_driver(driver).unwrap_err();
        assert_eq!(
            error.to_string(),
            "engine runtime driver terminated unexpectedly"
        );
        assert!(!error.to_string().contains("opaque-driver-test-failure"));
    }

    #[test]
    fn nonloopback_direct_wire_is_always_rejected() {
        assert!(validate_direct_wire_security("0.0.0.0:5433", "pgwire", true).is_err());
        assert!(validate_direct_wire_security("192.0.2.10:3306", "mysql-wire", true).is_err());
    }

    #[test]
    fn anonymous_loopback_wire_is_rejected_in_every_profile() {
        assert!(validate_direct_wire_security("127.0.0.1:5433", "pgwire", false).is_err());
        assert!(validate_direct_wire_security("localhost:7687", "bolt-wire", false).is_err());
    }

    #[test]
    fn verified_loopback_wire_is_accepted() {
        assert!(validate_direct_wire_security("[::1]:5433", "pgwire", true).is_ok());
        assert!(validate_direct_wire_security("127.0.0.1:7687", "bolt-wire", true).is_ok());
    }

    #[test]
    fn broker_actor_reference_is_stable_opaque_and_secret_bound() {
        let actor = pseudonymous_broker_actor("secret", "human-readable-name").unwrap();
        assert_eq!(
            actor,
            pseudonymous_broker_actor("secret", "human-readable-name").unwrap()
        );
        assert!(!actor.contains("human-readable-name"));
        assert_ne!(
            actor,
            pseudonymous_broker_actor("other", "human-readable-name").unwrap()
        );
        assert!(pseudonymous_broker_actor("", "principal").is_err());
    }
}

/// Refuse an auxiliary listener that is reachable off-host. Auxiliary HTTP and
/// database-wire protocols terminate only on loopback; remote deployments place
/// an authenticated TLS identity-binding gateway in front of that local socket.
pub fn require_loopback_listener(listener: &tokio::net::TcpListener) -> std::io::Result<()> {
    let address = listener.local_addr()?;
    if address.ip().is_loopback() {
        return Ok(());
    }
    Err(std::io::Error::new(
        std::io::ErrorKind::PermissionDenied,
        "auxiliary listeners must bind loopback",
    ))
}

/// Validate secure request-context configuration and initialize its durable
/// replay adapter before listeners are opened.
pub fn validate_verified_request_context_startup(
    secret: &str,
    state_dir: Option<&str>,
) -> Result<(), String> {
    auth::validate_verified_context_startup(secret, state_dir)
}
/// A temp dir guaranteed unique even across concurrent tests in one process.
///
/// `pid + nanos` is NOT sufficient: two threads can observe the same nanosecond,
/// and redb takes an EXCLUSIVE per-file lock, so the loser fails with
/// `Database already open. Cannot acquire lock.` That is exactly how
/// `server::lake::rest::tests` began failing once the suite ran on a 64-core
/// host -- more tests in flight at once, so the collision window is actually
/// hit. The monotonic counter removes the race rather than narrowing it.
///
/// Gated on `redb` (not `blob`): every current caller (this module's
/// `test_state`, `server::blob::store`, `server::lake::rest`) needs a durable
/// redb-backed temp dir, and `blob`/`lake` both imply `redb` transitively, so
/// this is the widest feature that still keeps the fn used (never dead code)
/// in every build that calls it.
#[cfg(feature = "redb")]
pub(crate) fn unique_temp_dir(prefix: &str) -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static SEQ: AtomicU64 = AtomicU64::new(0);
    std::env::temp_dir().join(format!(
        "{prefix}-{}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0),
        SEQ.fetch_add(1, Ordering::Relaxed),
    ))
}

// Streamed content-addressed BLOB substrate (CONCEPT:EG-KG.storage.blob-namespace). Facade-only,
// behind the `blob` cargo feature. Default/server-only builds compile NONE of it;
// the Blob* methods then fall to the dispatch "not available" catch-all.
//
// BUG (pre-existing, not caused by any recent merge): a 2026-08-09 commit
// (02f70963, GOC-59-W09/BUG-027) inserted `unique_temp_dir` between this
// `#[cfg(feature = "blob")]` attribute and its intended target, so the
// attribute silently re-attached to the new function instead of gating this
// `mod` declaration. That made `server::blob` (and therefore `blob/store.rs`'s
// unconditional `redb`/`eg_mutation_store` imports) compile in EVERY build,
// including `--no-default-features --features server`, breaking the slim
// server. It went unnoticed because this repo has not pushed in a long time,
// so CI never ran that feature-matrix row. Restoring the attribute here is
// the fix (AGENTS.md "Feature-gating gates three sites").
#[cfg(feature = "blob")]
pub mod blob;
// Generic namespaced Key→Value surface (CONCEPT:EG-KG.storage.namespaced-kv-surface). Self-routing (NOT graph-
// scoped) like blob/tsdb, behind the `kv` cargo feature. A build without it compiles
// none of it; the Kv* methods then fall to the dispatch "not available" catch-all.
#[cfg(feature = "kv")]
pub mod kv;
// Change-Data-Capture hub + continuous queries + subscriptions/triggers
// (CONCEPT:EG-KG.query.streaming-cdc-subscriptions/230). Facade-only, behind the `streaming` cargo feature (no heavy
// dep, folds into pi/node/cluster/full). A build without it compiles none of it.
#[cfg(feature = "streaming")]
pub mod cdc;
// CDC -> Kafka sink (CA-11, feature `cdc-kafka`, off by default). Reserved and EMPTY
// until CA-11 implements it; landed by CA-17's single feature-stub commit so CA-11 never
// edits this shared module list.
#[cfg(feature = "cdc-kafka")]
pub mod cdc_sink;
// Live CEP standing-query surface (CONCEPT:EG-KG.query.protocol-types): the PUSH half of the event-stream +
// CEP modality (CONCEPT:EG-KG.query.pipelined-execution). Owns an `eg_stream::live::CepEngine` (feature `stream`)
// adapted onto the CDC feed (feature `streaming`) — register a CEP pattern once, then
// poll pushed matches. Needs BOTH: the CDC hub to feed it (`streaming`) AND the live NFA
// engine (`stream`, the only thing that pulls eg-stream's tokio). A build with one but
// not the other compiles none of it.
#[cfg(all(feature = "streaming", feature = "stream"))]
pub mod cep;
// Plan-backed materialized views (CONCEPT:EG-KG.storage.plan-backed-matview): a NAMED,
// DURABLE `wire::Plan` whose result rides the version-keyed, RLS-aware result cache. The
// manager subscribes to the CDC feed (`CdcHub::emit`) to mark views over a changed graph
// stale. Gated behind `matview` (which pulls `compute-dist`/`query`/`result-cache`/
// `streaming`); its Method variants route through the `compute-dist` dispatch line.
#[cfg(feature = "matview")]
pub mod matview;
// Distributed result-cache coherence over the CDC feed (CONCEPT:EG-KG.coordination.distributed-cache-coherence): a replica
// tailing CDC invalidates its local version-keyed result cache on a remote write.
// Needs BOTH the cache (`result-cache`) and the CDC feed (`streaming`).
#[cfg(all(feature = "result-cache", feature = "streaming"))]
pub mod cache_coherence;
// Facade-side ColdTier impls (CONCEPT:EG-KG.coordination.distributed-cache-coherence): redb-durable default + S3 behind
// `cold-tier-s3`. The seam + in-memory impl live in eg-core; this needs redb.
#[cfg(all(feature = "cold-tier", feature = "redb"))]
pub mod cold_tier_impl;
// Warm-on-demand for the semantic ANN index (W0.4, CONCEPT:EG-KG.storage.semantic-index-directory): the
// boot-time warm task only covers graphs resident at startup, so this module
// supplies the post-write trigger + periodic backstop for a graph created — or
// crossing `ANN_BUILD_THRESHOLD` — after boot. Gated with the `ann` feature the
// warm mechanism itself requires; a non-`ann` build compiles none of it.
#[cfg(feature = "ann")]
pub mod ann_warm;
// Native visualization engine-side state (D-VZ-1 lane V4, "engine integration"):
// a persistent (process-lifetime, not fresh-per-request) ColumnStore plus a
// content-addressed render cache and durable render provenance. Gated the SAME
// as `handlers::viz` (`viz-static-export`) -- this state exists only where that
// handler exists to use it. `viz_provenance` is a separate module (not under
// `persistence/`, which is themed around the authoritative GRAPH store this is
// NOT -- a render is explicitly not graph-scoped) so it can compile durability
// support conditionally on `redb` internally while `viz_engine` itself never
// requires `redb` (matches `viz-static-export` not implying `redb`).
// `pub` (not `pub(crate)`): `main.rs` (a separate crate from this facade lib)
// constructs `viz_engine::VizEngineState` directly to eagerly share it between
// the RPC render path and the V3b interactive listener — see that struct's
// own doc.
#[cfg(feature = "viz-static-export")]
pub mod viz_engine;
#[cfg(feature = "viz-static-export")]
pub(crate) mod viz_provenance;
// D-VZ-1 lane V3b: the interactive HTTP listener (WebGPU/WebGL2 reference
// client + binary viewport-tile protocol). `pub` (not `pub(crate)`) so
// `main.rs` can call `serve` directly, mirroring `obs`/`lake::rest`'s own
// top-level HTTP-surface modules.
mod compute;
mod dispatch;
#[cfg(feature = "viz-interactive")]
pub mod viz_interactive;
// VIZ-2: the binary tile/streaming protocol for GRAPH payloads (nodes + edges
// together, edges by node index rather than repeated string ids), served over
// the SAME loopback listener as `viz_interactive` above (see that module's own
// entry point calling into this one, and this module's doc for why it is a
// separate file rather than more routes inline in `viz_interactive::route`:
// genuine HTTP chunked-transfer streaming needs `async` socket writes a plain
// synchronous `(status, content_type, Vec<u8>)` return cannot express).
#[cfg(feature = "viz-graph-tiles")]
pub mod graph_tile_server;
// Fleet server registry stale-lease reaper (CONCEPT:EG-KG.sharding.server-registry, W2.5): periodic
// sweep that expires a `:Server` node whose `Method::RegisterServer`-issued
// lease has lapsed. Always declared (mirrors `ann_warm` above) — the sweep is a
// no-op when nothing has registered.
pub(crate) mod handlers;
pub mod registry_reaper;
// MutationPlan + the single commit gateway (CONCEPT:EG-P0-2): consumes
// `eg-capabilities`' MethodPolicy to drive authz + durable-commit + audit + CDC for
// the GATEWAY_ROUTED mutation set from ONE call site. See its module docs for scope.
pub(crate) mod mutation;
pub(crate) mod mutation_batch;
// Per-graph batching worker for the five coalescable GATEWAY_ROUTED structural
// writes (CONCEPT:EG-KG.sharding.per-graph-write-coalescer, L18 rewrite). See its module
// docs and `mutation::commit_coalescable_mutation` for why the durable commit, not
// just the RAM publish, must run inside the worker's ONE `lock_graph` hold per batch.
// `pub` (not `pub(crate)`) so `ServerState::routed_write_coalescer`'s field type is
// nameable from the `epistemic-graph-server` binary crate, mirroring `write_coalescer`.
pub mod routed_write_coalescer;
// Durable incremental reasoning authority. It tails the committed MutationBatch
// outbox, fsyncs each per-graph projection before cursor acknowledgement, and serves
// status/recompute directly from that projection.
#[cfg(feature = "epistemic-tms")]
pub mod reasoning_projection;
// Reasoning auto-cascade (W3.6/E16, opt-in via `REASON_ON_WRITE`): CDC-triggered,
// debounced OWL/RL closure re-materialization per graph. Wires the EXISTING
// `eg_rdf::owl::Reasoner` incremental delta re-seed (`add_axioms`) into the write
// path via the CDC hub's `emit` choke point (see `cdc.rs`). Gated on `owl` alone
// (this module's real dependency) — a `streaming`-less build compiles it, but its
// only call site (the `owl`-gated hook inside `cdc.rs`, itself `streaming`-gated)
// then has nothing to reach it, so nothing spawns.
#[cfg(feature = "owl")]
pub mod reasoning_cascade;
// X5-enforce (CONCEPT:EG-KG.ontology.rdf-update-guard): wires the EXISTING eg-shacl ICV
// commit guard onto the live RDF write path (AddTriples/RemoveTriples/ApplyMutation).
// Pure-Rust (no new dep — `eg-shacl` + `std::sync::OnceLock`), gated `shacl`; a build
// without it compiles none of it and every write path stays byte-identical.
#[cfg(feature = "shacl")]
pub(crate) mod icv_guard;
pub mod persistence;
// Wire-agnostic SQL execution core (CONCEPT:EG-KG.compute.subsystems-reference) — the multi-wire keystone. The
// wire-NEUTRAL `classify → dispatch → exec` pipeline + per-connection session/txn
// state that EVERY wire (Postgres today; SQLite/MySQL/MSSQL Phase J; AMQP Phase Y)
// reuses. Behind the `wire` facade feature (pulled in by `pgwire`; a future wire's
// feature pulls it in too). Kept OUT of `node`/`full` — the orchestrator folds it in.
#[cfg(feature = "wire")]
pub mod wire;
// Postgres wire-protocol shim (CONCEPT:AU-KG.query.raw-python). Facade-only, behind the `pgwire`
// cargo feature (cluster tier). The FIRST `wire::WireProtocol` adapter (CONCEPT:EG-KG.compute.subsystems-reference).
// Default/pi/node builds compile NONE of it.
#[cfg(feature = "pgwire")]
pub mod pgwire;
// SQLite-compatible served surface (CONCEPT:EG-KG.query.concept-3) — Phase J. SQLite has NO client/
// server wire protocol, so this is a lightweight NDJSON-over-TCP endpoint that accepts
// SQLite-dialect SQL, rewrites the SQLite-isms (AUTOINCREMENT / INTEGER PRIMARY KEY /
// PRAGMA no-ops) and runs them through the shared `WireSession` (CONCEPT:EG-KG.compute.subsystems-reference). The
// SECOND `wire` consumer after pgwire; behind the `sqlite-wire` feature (pulls in
// `wire`). Pure-Rust — NO C-linked sqlite. Kept OUT of node/full — the orchestrator folds it.
#[cfg(feature = "sqlite-wire")]
pub mod sqlite_wire;
// MySQL / MariaDB wire-protocol listener (CONCEPT:EG-KG.query.kg-2). A hand-rolled MySQL
// client/server protocol (handshake v10 + `mysql_native_password` + `COM_QUERY`)
// behind the `mysql-wire` cargo feature. The SECOND `wire::WireProtocol` adapter
// (CONCEPT:EG-KG.compute.subsystems-reference), reusing the shared `WireSession` execution core. Default/pi/node/
// full builds compile NONE of it.
#[cfg(feature = "mysql-wire")]
pub mod mysql_wire;
// MSSQL TDS wire-protocol listener (CONCEPT:EG-KG.query.hand-rolled-tds-server). A hand-rolled TDS server — the
// MSSQL `wire::WireProtocol` adapter (CONCEPT:EG-KG.compute.subsystems-reference), sibling of `pgwire`. Behind the
// `mssql-wire` cargo feature (which pulls `wire`); DELIBERATELY kept OUT of
// `node`/`full`/`pi` — the orchestrator folds it into a tier. Default builds compile
// NONE of it.
#[cfg(feature = "mssql-wire")]
pub mod mssql_wire;
// AMQP 0.9.1 wire-protocol listener (CONCEPT:EG-KG.compute.message-broker-exchanges). A hand-rolled AMQP 0.9.1 server
// mapping exchange/queue/basic.* frames onto the `broker` primitives via the engine
// dispatch. Behind the `amqp-wire` cargo feature; links NO AMQP crate (Pi contract).
// Default/pi/node/full builds compile NONE of it.
#[cfg(feature = "amqp-wire")]
pub mod amqp_wire;
// Neo4j Bolt wire-protocol listener (CONCEPT:EG-KG.query.bolt-wire-protocol). A native Bolt v4.4 server — a
// hand-rolled PackStream v2 codec + chunked framing + message state machine over
// `tokio::net`, routing RUN's Cypher straight to the eg-query cypher engine. Behind the
// `bolt-wire` cargo feature (which pulls `cypher` + `server`); it does NOT use the SQL
// `wire`/`WireSession` core (Bolt speaks Cypher). Default/pi builds compile NONE of it.
#[cfg(feature = "bolt-wire")]
pub mod bolt_wire;
// Redis RESP wire-protocol listener (CONCEPT:EG-KG.ontology.resp2-resp3-codec-round). A native, hand-rolled Redis
// server — a RESP2 + RESP3 codec + the core command set (string/list/hash/set/zset,
// GET/SET/EXPIRE/SCAN/…) over the engine's durable KV surface (CONCEPT:EG-KG.storage.namespaced-kv-surface).
// Behind the `redis-wire` cargo feature (pulls `kv`); links NO redis crate (Pi
// contract). Default/pi builds compile NONE of it.
#[cfg(feature = "redis-wire")]
pub mod redis_wire;
// MQTT 3.1.1 (+ basic 5.0) wire-protocol listener (CONCEPT:EG-KG.query.mqtt-packet-codec). A hand-rolled MQTT
// broker front-end mapping CONNECT/PUBLISH/SUBSCRIBE/… onto the `broker` topic exchange
// + per-session queues via the engine dispatch. Behind the `mqtt-wire` cargo feature
// (pulls `broker` + `server`); links NO MQTT crate (Pi contract). Default/pi builds
// compile NONE of it.
#[cfg(feature = "mqtt-wire")]
pub mod mqtt_wire;
// STOMP 1.2 wire-protocol listener (CONCEPT:EG-KG.ontology.stomp-frame-codec-unit). A hand-rolled STOMP text-frame
// server mapping CONNECT/SEND/SUBSCRIBE/ACK/… onto the `broker` primitives (destinations
// → exchange + per-subscription queues) via the engine dispatch. Behind the `stomp-wire`
// cargo feature (pulls `broker` + `server`); links NO STOMP crate (Pi contract).
// Default/pi builds compile NONE of it.
#[cfg(feature = "stomp-wire")]
pub mod stomp_wire;
// S3-compatible object-storage REST surface (CONCEPT:EG-KG.ontology.object-put-get-head). A hand-rolled HTTP/1.1
// listener exposing PutObject/GetObject/ListObjectsV2/CreateBucket/… over the
// content-addressed BLOB CAS (bytes) + the durable KV index (listing), with a
// SigV4-lite auth guard. Behind the `s3-api` cargo feature (pulls `blob` + `kv`).
// Default/pi builds compile NONE of it.
/// Current GraphQL subscriptions over authenticated Server-Sent Events
/// (CONCEPT:EG-KG.compute.cdc-event-emit, feature `graphql`): eg2 request binding,
/// graph ACL, and default-deny RLS guard every caller-visible re-resolution.
#[cfg(feature = "graphql")]
pub mod graphql_sub;
/// Remote KV-cache HTTP surface (CONCEPT:EG-KG.backend.is-configured-so-co, feature `kvcache-server`): a
/// hand-rolled HTTP listener exposing the `eg-kvcache` shared, content-addressed
/// backend (EG-186) so parallel vLLM/LMCache instances SHARE KV blocks by token-hash
/// over GET/PUT/HEAD /kv/<hash> + /kv/stats, with a bearer-token guard. Default/pi
/// builds compile NONE of it (no eg-kvcache/ureq in pi).
#[cfg(feature = "kvcache-server")]
pub mod kvcache_http;
/// Observability log ingestion + Parquet segment substrate (CONCEPT:AU-KG.ingest.self-ingest/161,
/// feature `obs`): a hand-rolled HTTP listener accepting OTLP/HTTP, Elasticsearch
/// `_bulk`/`_doc` and JSON-lines log records, landing them in eg-tsdb series +
/// eg-text full-text indices and rolling Parquet-on-blob-CAS segments — the first
/// slice of Phase T (surpass OpenObserve). Self-contained (its own `ObsState`), not
/// tied to the graph `ServerState`.
#[cfg(feature = "obs")]
pub mod obs;
/// Shared OIDC/JWKS bearer-token verification core (feature `oidc`, pulled in by
/// both `security` and `kvcache-server`): one RSA-signature-against-JWKS
/// verifier reused by the KV-cache HTTP bearer guard AND `auth`'s primary
/// `eg2.` identity binding. See the module doc for the full split.
#[cfg(feature = "oidc")]
pub(crate) mod oidc;
#[cfg(feature = "s3-api")]
pub mod s3;
/// W3C SPARQL 1.1 Protocol HTTP endpoint (CONCEPT:EG-KG.query.named-graph-support, feature `sparql-http`).
#[cfg(feature = "sparql-http")]
pub mod sparql_http;
// SPARQL graph publication to an external Fuseki endpoint (CA-12, feature
// `sparql-fuseki`, off by default). Reserved and EMPTY until CA-12 implements it; landed
// by CA-17's single feature-stub commit so CA-12 never edits this shared module list.
// Named for its own lane's surface -- NOT the pre-existing `sparql-service` FEATURE,
// which gates the inbound SERVICE federation client inside `sparql_http`.
#[cfg(feature = "sparql-fuseki")]
pub mod sparql_service;
// OBDA wire method + Python client seam (CA-13, feature `obda-wire`, off by default).
// Reserved and EMPTY until CA-13 implements it; landed by CA-17's single feature-stub
// commit so CA-13 never edits this shared module list. The OBDA mapping/rewrite engine
// itself already exists behind the `obda` feature this one implies.
#[cfg(feature = "obda-wire")]
pub mod obda;
// RLS marking-predicate export (CA-16, feature `policy_export`, off by default).
// Reserved and EMPTY until CA-16 implements it; landed by CA-17's single feature-stub
// commit so CA-16 never edits this shared module list.
#[cfg(feature = "policy_export")]
pub mod policy_export;

/// LTAP lakehouse materialization tier (CONCEPT:EG-KG.storage.lsn-as-snapshot-returns engine-side seam, feature
/// `lake`, INT-P2-3): converts real `eg_tsdb` series into `eg_lake::LakeBatch`es,
/// materializes them to Parquet on the blob CAS with Delta/Iceberg logs + an
/// Iceberg-REST catalog + OpenLineage run events. The `rest` submodule (feature
/// `lake-rest`) serves the standards Iceberg-REST catalog surface over it.
#[cfg(feature = "lake")]
pub mod lake;

/// PromQL + the Prometheus-compatible HTTP query API (CONCEPT:EG-KG.query.prometheus-http-query-api, feature
/// `promql`): `/api/v1/query[_range]` + `/labels` served on the obs listener over the
/// durable eg-tsdb series, backed by the pure-Rust `eg_tsdb::promql` engine.
#[cfg(feature = "promql")]
pub mod promql;

/// Distributed traces: OTLP-JSON span ingest + Jaeger/OpenObserve trace-search,
/// assembly and service-dependency-graph API (CONCEPT:EG-OS.observability.trace-assembly, feature `traces`),
/// served on the obs listener over the pure-Rust `eg_tsdb::traces` span store —
/// completing the observability trilogy (logs + metrics + traces).
#[cfg(feature = "traces")]
pub mod traces;
// Owner-scoped user-defined relational catalogs (CONCEPT:EG-KG.query.register-user-tables-alongside/EG-023): one
// `eg_query::TableStore` per verified tenant+actor, shared across Method::Sql and every
// native SQL/SQLite/OBDA surface for that owner. Opaque files live under the configured
// persistence directory; there is no global or temporary fallback.
#[cfg(feature = "query")]
pub mod sql_tables;
// Tenant-scoped SQL table ownership, grants, and row-level security layered on top
// of `sql_tables`'s tenant-shared catalog (CONCEPT:NE-003). Crate-private: not yet
// wired into any live request path — see the module doc for why.
#[cfg(feature = "query")]
pub(crate) mod sql_catalog_acl;
// Natural-language query planner resolution (CONCEPT:EG-KG.query.fence-stripper, feature `nl-query`): owns
// which `eg_plan::NlPlanner` the facade uses (injected vs standalone-config default).
#[cfg(feature = "nl-query")]
pub mod nl;
// Super-cluster federated search (CONCEPT:EG-KG.ontology.federation-client, feature `federation-search`): fans a
// read query across a registry of peer engine instances (SSRF-vetted, per-peer timeouts)
// AND the local store, then unions/de-dups + RRF-re-ranks the partials — a slow/dead peer
// degrades to `partial: true` + `failed_peers` rather than failing the query. Behind the
// `federation-search` cargo feature (pulls the same pure-Rust `ureq`/rustls stack the
// other federation surfaces link — NO new HTTP dep, kept OUT of the Pi tier).
#[cfg(feature = "federation-search")]
pub mod federation;
// Cross-region async read-replica tier (CONCEPT:EG-KG.sharding.follower-pull-loop) + capacity guardrails
// (CONCEPT:EG-KG.coordination.circuit-breaker): a bounded LSN replication log the primary ships over `/replicate`, an
// async follower pull-loop that applies the tail via the canonical `mutation_apply::apply` path, and
// the pure circuit-breaker / per-tenant-quota / backpressure guards the transport + the
// follower consult. Behind `federation-search` (reuses the same pure-Rust `ureq` stack —
// NO new dep, kept OUT of the Pi tier).
#[cfg(feature = "federation-search")]
pub mod replica;
// ROS2 bridge over the rosbridge-WebSocket JSON protocol (CONCEPT:EG-KG.domains.robotics-gpu-distribution): bridges engine
// CDC events ↔ ROS2 topics by talking `rosbridge_suite` JSON-over-WebSocket to a
// `rosbridge_server` — NO CycloneDDS/rmw/DDS C stack, just a pure-Rust tokio-tungstenite
// client. Behind the `ros2-bridge` cargo feature; kept OUT of the Pi tier (a slim build
// links no tokio-tungstenite). Also compiled behind `ros2-dds`/`ros2-rmw` (CONCEPT:EG-KG.ingest.dds-transport), which
// reuse this module's PURE CDC↔ROS2 message mapping (`cdc_to_publish`/`publish_to_method`)
// as the shared shaping for the native DDS legs — only the tungstenite driver
// (`run_ros2_bridge`) is `ros2-bridge`-specific.
#[cfg(any(feature = "ros2-bridge", feature = "ros2-dds", feature = "ros2-rmw"))]
pub mod ros2_bridge;
// Native DDS/RTPS ROS2 transport seam (CONCEPT:EG-KG.ingest.dds-transport): the `DdsTransport` trait that
// unifies the EG-325 rosbridge-WebSocket leg and TWO native DDS legs behind ONE
// interface — the pure-Rust `rustdds`-backed `NativeDdsTransport` (feature `ros2-dds`)
// and the CycloneDDS-C-backed `CycloneDdsTransport` (feature `ros2-rmw`, S5). Kept OUT of
// pi/default/node/full — only the opt-in `full-extras` bundle (a default/pi/full build
// links no rustdds/cyclonedds).
#[cfg(any(feature = "ros2-bridge", feature = "ros2-dds", feature = "ros2-rmw"))]
pub mod dds;
// Real-time QoS / SLO-aware admission scheduler (CONCEPT:EG-KG.coordination.backpressure-busy-signal). An additive, opt-in
// gate (enabled by `EPISTEMIC_GRAPH_QOS`) the transport runs BEFORE the baseline
// admission: priority-class preemption + per-tenant fair-share + hard quotas +
// deadline-aware ordering + typed backpressure. With QoS unconfigured the transport never
// touches it and the baseline path is byte-for-byte unchanged.
pub mod qos;
// Server-layer secondary indexes (text/temporal/derived-OWL) wired into the per-graph
// IndexManager seam so a committed write batch maintains them incrementally
// (CONCEPT:EG-KG.storage.incremental-text / .incremental-temporal / .incremental-derived-owl).
#[cfg(any(feature = "text", feature = "tsdb", feature = "owl", feature = "geo"))]
pub mod secondary_indexes;
// Request-scoped cancellation registry (CONCEPT:EG-KG.query.streaming-spillable-collect, L36): threads
// a REAL `CancellationToken` from a served `Method::Sql` down to the SQL streaming
// collect path, tripped by an explicit `Method::CancelRequest` or a per-request
// timeout, instead of the always-fresh never-cancelled token the collect path built
// internally before this. Needs `eg_query::CancellationToken`, gated behind `query`
// (which implies `eg-query/sql`).
#[cfg(feature = "query")]
pub mod request_cancel;
mod state;
mod transport;
// Server-staged OCC ACID transactions (CONCEPT:EG-KG.txn.multi-op-occ-acid). `txn` holds the staged
// transaction state + id source; `handlers::txn` owns the Txn* methods.
pub mod txn;
// Durable cross-store commit-intent log for a mixed graph+user-table SQL
// transaction (CONCEPT:EG-TXN.mixed-commit-intent, NE-004) — see its module doc.
pub(crate) mod txn_intent;

// External path surface — `server::ServerState`, `server::MAX_BATCH_IDS`,
// `server::dispatch`, and
// `server::{handle_connection,serve_uds,serve_tcp}` — used by main.rs/persist.rs/tests.
pub use auth::{compute_verified_envelope_token, VerifiedEnvelopeParams};
pub(crate) use dispatch::authoritative_now_secs;
pub use dispatch::dispatch;
#[cfg(all(feature = "raft", feature = "jobs"))]
pub(crate) use dispatch::{
    apply_replicated_job_publication_commit, apply_replicated_job_publication_finalize,
};
#[cfg(feature = "raft")]
pub(crate) use dispatch::{
    apply_replicated_native, apply_replicated_transaction_decision,
    apply_replicated_transaction_finalize, apply_replicated_transaction_participant,
    apply_replicated_transaction_prepare, ReplicatedParticipantRef,
};
// NL planner injection (CONCEPT:EG-KG.query.fence-stripper): an embedder opts into engine-driven NL→query.
#[cfg(feature = "nl-query")]
pub use nl::{resolve_planner as resolve_nl_planner, set_nl_planner};
// Distributed-compute materialized-view boot reload (CONCEPT:EG-KG.storage.feature): the binary
// calls this on startup to repopulate the in-RAM matview index from redb.
#[cfg(any(feature = "compute-dist", feature = "matview"))]
pub use handlers::dist_compute::reload_matviews;
pub use persistence::PersistenceBackend;
pub use state::{txn_limits_from_env, ServerState, MAX_BATCH_IDS};
pub use transport::{
    handle_connection, run_idle_watcher, serve_tcp, validate_tcp_tls_config, ShutdownCoordinator,
    TcpTlsConfig,
};
// serve_uds is Unix-only (UnixListener); on Windows the server uses serve_tcp,
// so gate the re-export to keep the windows-msvc wheel building (main.rs already
// guards its serve_uds call with #[cfg(unix)]).
#[cfg(unix)]
pub use transport::serve_uds;
// parse_unix_socket_mode is pure parsing/validation (no fs calls) and is exported
// unconditionally so main.rs can validate --socket-mode/GRAPH_SERVICE_SOCKET_MODE
// up front on every platform, even though the mode itself is only applied by
// serve_uds on unix.
pub use transport::parse_unix_socket_mode;

/// CA-17-W00 feature-stub contract.
///
/// Six features are declared empty and default-off so that lanes CA-11..CA-16 each edit
/// only their own `src/server/<feature>/` directory and their owning crate's manifest,
/// never the root `Cargo.toml` `[features]` table or this file's module list -- both of
/// which a dozen concurrent eg lanes share. These tests pin the two properties that make
/// that safe: each reserved feature keeps the parent its owning lane builds on, and none
/// of them leaks into a release bundle before its lane has implemented anything.
///
/// The check is textual against the manifest (rather than `cfg!(feature = ..)`) so that
/// it reports the same verdict no matter which `--features` the test run itself was
/// built with.
#[cfg(test)]
mod ca17_feature_stub_contract {
    const CARGO_TOML: &str = include_str!("../../Cargo.toml");

    /// `(feature name, the exact dependency list its owning lane builds on)`.
    const RESERVED: [(&str, &str); 6] = [
        ("cdc-kafka", "[\"streaming\"]"),
        ("sparql-fuseki", "[\"sparql\"]"),
        ("obda-wire", "[\"obda\"]"),
        ("federation-opensearch", "[\"federation-search\"]"),
        ("lineage-transport", "[\"lake\"]"),
        ("policy_export", "[\"security\"]"),
    ];

    /// Release/aggregate bundles a reserved feature must not appear in. Each is an
    /// existing `[features]` key whose value transitively defines a shipped artifact.
    const BUNDLES: [&str; 5] = ["default", "full", "all", "full-extras", "cluster"];

    fn declaration(feature: &str) -> &'static str {
        let prefix = format!("{feature} = ");
        let mut found = CARGO_TOML.lines().filter(|line| line.starts_with(&prefix));
        let line = found.next().unwrap_or_else(|| {
            panic!("reserved feature {feature:?} is not declared in Cargo.toml")
        });
        assert!(
            found.next().is_none(),
            "reserved feature {feature:?} is declared more than once"
        );
        line
    }

    #[test]
    fn every_reserved_feature_keeps_the_parent_its_lane_builds_on() {
        for (feature, parent) in RESERVED {
            let line = declaration(feature);
            assert_eq!(
                line,
                format!("{feature} = {parent}"),
                "reserved feature {feature:?} no longer implies exactly {parent}; a lane \
                 changing its own feature's parents must say so in its report"
            );
        }
    }

    #[test]
    fn no_reserved_feature_leaks_into_a_release_bundle() {
        for bundle in BUNDLES {
            let line = declaration(bundle);
            for (feature, _) in RESERVED {
                assert!(
                    !line.contains(&format!("\"{feature}\"")),
                    "bundle {bundle:?} enables reserved feature {feature:?}, which is still \
                     an empty placeholder -- a lane adding it to a bundle must land its \
                     implementation in the same commit"
                );
            }
        }
    }

    /// The point of the stub commit is that the shared module lists already carry every
    /// gate, so no later lane has to touch them. Pin that the gate lines are present.
    #[test]
    fn every_reserved_module_gate_is_already_declared_in_its_shared_module_list() {
        for (file, source, gate, module) in [
            (
                "src/server/mod.rs",
                include_str!("mod.rs"),
                "cdc-kafka",
                "pub mod cdc_sink;",
            ),
            (
                "src/server/mod.rs",
                include_str!("mod.rs"),
                "sparql-fuseki",
                "pub mod sparql_service;",
            ),
            (
                "src/server/mod.rs",
                include_str!("mod.rs"),
                "obda-wire",
                "pub mod obda;",
            ),
            (
                "src/server/mod.rs",
                include_str!("mod.rs"),
                "policy_export",
                "pub mod policy_export;",
            ),
            (
                "src/server/federation/mod.rs",
                include_str!("federation/mod.rs"),
                "federation-opensearch",
                "pub mod opensearch;",
            ),
            (
                "src/server/lake/mod.rs",
                include_str!("lake/mod.rs"),
                "lineage-transport",
                "pub mod lineage_transport;",
            ),
        ] {
            let expected = format!("#[cfg(feature = \"{gate}\")]\n{module}");
            assert!(
                source.contains(&expected),
                "{file} is missing the CA-17-W00 gate for {gate:?}: {expected:?}"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::compute::weight_semantic_results;
    use super::*;
    use crate::acl::RequestContextClaims;
    use crate::channels::ChannelManager;
    use crate::isolation::{AgentIdentity, AgentRole, IsolationLayer};
    use crate::protocol::{GraphType, Method, Request, Response, ResultPayload};
    use crate::registry::GraphRegistry;
    use dashmap::DashMap;
    use std::sync::Arc;
    use tokio::sync::{RwLock, Semaphore};

    const SECRET: &str = "dispatch-test-secret";

    /// BUG-044: keep the full (`--features full`) dispatcher's state machine behind
    /// one heap indirection. `dispatch()` bottoms out in `dispatch_inner`
    /// (`src/server/dispatch.rs`), a single very large async fn whose generated
    /// future is enormous; nesting it (a helper awaiting `dispatch` inside a test
    /// awaiting the helper) can exhaust the test harness thread's 8 MiB stack before
    /// the first request is even polled, aborting the WHOLE test binary with SIGABRT
    /// and hiding every subsequent result.
    ///
    /// Same fix as `result_cache_dispatch_tests` (ae64cfd) and `redb_backend`'s tests
    /// (92586a7), and mirrors `src/cost.rs`'s `dispatch_on_heap`. Route every call in
    /// this module through here -- raising the thread stack size via a process
    /// environment override would only mask the class (see
    /// `check_current_only_architecture.py`'s gate against exactly that).
    fn dispatch_on_heap<'a>(
        state: &'a Arc<RwLock<ServerState>>,
        request: Request,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Response> + Send + 'a>> {
        Box::pin(dispatch(state, request))
    }

    /// Provision `EPISTEMIC_GRAPH_ENCRYPTION_KEY` for every test in this module that
    /// reaches a multi-op transaction `Commit` (`handlers::txn::seal_txn_recovery_plan`
    /// fails closed without a configured key — see `crypto::TXN_RECOVERY_KEY_ENV`'s
    /// doc). Every caller MUST already hold `crate::crypto::acquire_test_env_lock()`
    /// for its entire test body before calling this (see each call site) — this
    /// function does NOT acquire it itself (`std::sync::Mutex` is not reentrant).
    ///
    /// Before this was centralized, several of these tests silently depended on
    /// EXECUTION-ORDER LUCK: some OTHER test in the binary (e.g. `redb_backend::
    /// tests::cm_dir`'s own `Once`) happening to have set this process-global var
    /// FIRST. That is not something a test may rely on — running it filtered/alone,
    /// or simply losing the race to be scheduled first, reproduces "transaction
    /// durability requires EPISTEMIC_GRAPH_ENCRYPTION_KEY to be configured" without
    /// this. Encryption is transparent to every one of these tests' assertions
    /// (they never inspect raw on-disk bytes), so provisioning it here is harmless.
    #[cfg(feature = "security")]
    fn ensure_txn_recovery_key() {
        static ENCRYPTION_KEY: std::sync::Once = std::sync::Once::new();
        ENCRYPTION_KEY.call_once(|| {
            std::env::set_var(
                crate::crypto::ENCRYPTION_KEY_ENV,
                "server-mod-txn-test-recovery-key",
            );
        });
    }

    fn test_state() -> Arc<RwLock<ServerState>> {
        let mut isolation = IsolationLayer::new();
        isolation.register_agent(AgentIdentity {
            agent_id: "system".into(),
            role: AgentRole::System,
            teams: Vec::new(),
            roles: Vec::new(),
        });
        Arc::new(RwLock::new(ServerState {
            #[cfg(feature = "redb")]
            cold_tracker: std::sync::Arc::new(
                crate::server::persistence::cold_offload::ColdTenantTracker::new(),
            ),
            registry: GraphRegistry::new(),
            isolation,
            channels: ChannelManager::new(),
            #[cfg(feature = "viz-static-export")]
            viz_engine: None,
            auth_secret: SECRET.to_string(),
            #[cfg(feature = "query")]
            persist_dir: Some(
                crate::server::sql_tables::test_persist_dir()
                    .to_string_lossy()
                    .into_owned(),
            ),
            #[cfg(not(feature = "query"))]
            persist_dir: None,
            // A REAL durable backend on its own uniquely-named temp dir (same
            // reason the series store below is per-test: redb takes an exclusive
            // per-process file lock).
            //
            // This was `None`, which made 31 of this module's tests fail closed
            // the moment they touched anything durable -- 25 with "authoritative
            // MutationBatch commit requires a persistence backend", 5 with
            // "session control mutation requires durable redb coordination", and
            // 1 with "graph creation requires durable persistence". Those errors
            // are CORRECT: the dispatch path is deliberately fail-closed for
            // durable-domain methods, so a state with no backend genuinely cannot
            // exercise them. The TESTS were wrong, and it stayed invisible for as
            // long as the facade-full suite aborted on a stack overflow before it
            // ever reached them.
            //
            // Fixed by gating: the 22 durable-domain tests in this module that
            // relied on the real backend built here now carry their own
            // `#[cfg(feature = "redb")]` (AGENTS.md rule 4, the `ast`
            // precedent) so they simply do not run in a build without `redb` --
            // matched by 9 more in the `edge_pagination` / `multi_graph_batch_write`
            // / `node_binding_envelope` integration targets, gated on
            // `security` (which implies `redb`) because their dispatch calls go
            // through the real, non-`cfg(test)` secure-envelope path.
            #[cfg(feature = "redb")]
            persistence: Some(std::sync::Arc::new(
                crate::server::persistence::redb_backend::RedbBackend::open(
                    unique_temp_dir("eg-server-test")
                        .to_string_lossy()
                        .into_owned(),
                    crate::durability::DurabilityPolicy::Each,
                    256,
                )
                .expect("open test redb backend"),
            )),
            #[cfg(not(feature = "redb"))]
            persistence: None,
            max_in_flight: Arc::new(Semaphore::new(16)),
            read_admission: Arc::new(Semaphore::new(16)),
            per_graph_inflight: Arc::new(dashmap::DashMap::new()),
            per_graph_inflight_limit: 8,
            write_coalescer: Arc::new(crate::write_coalescer::WriteCoalescerRegistry::new()),
            routed_write_coalescer: Arc::new(
                crate::server::routed_write_coalescer::RoutedWriteCoalescerRegistry::new(),
            ),
            open_txns: Arc::new(DashMap::new()),
            txn_id_gen: Arc::new(crate::server::txn::TxnIdGen),
            txn_ttl_secs: 300,
            txn_max_per_graph: 256,
            txn_max_per_agent: 256,
            #[cfg(feature = "blob")]
            blob: None,
            #[cfg(feature = "blob")]
            blob_cursor_ttl_secs: 300,
            #[cfg(feature = "raft")]
            raft: None,
            #[cfg(feature = "raft")]
            multi_raft: None,
            // A real per-test temp series store so the `Ts*` handler round-trips
            // exercise the actual store (a fresh, uniquely-named redb file — redb
            // holds an exclusive per-process file lock, so each test gets its own).
            #[cfg(feature = "tsdb")]
            tsdb_store: Some(Arc::new(
                eg_tsdb::store::SeriesStore::open(&std::env::temp_dir().join(format!(
                        "eg-tsdb-test-{}-{}.redb",
                        std::process::id(),
                        std::sync::atomic::AtomicU64::new(0)
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                            + std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .map(|d| d.as_nanos() as u64)
                                .unwrap_or(0)
                    )))
                .expect("open test series store"),
            )),
            #[cfg(feature = "streaming")]
            cdc: Some(std::sync::Arc::new(crate::server::cdc::CdcHub::new())),
            #[cfg(feature = "wasm-udf")]
            udf_registry: std::sync::Arc::new(eg_wasm::UdfRegistry::new()),
            #[cfg(feature = "compute-dist")]
            matviews: std::sync::Arc::new(parking_lot::Mutex::new(
                crate::raft::pregel::MatViewStore::new(),
            )),
            #[cfg(feature = "federation")]
            foreign_sources: std::sync::Arc::new(dashmap::DashMap::new()),
            #[cfg(feature = "kv")]
            kv: None,
            #[cfg(feature = "lake")]
            lake: std::sync::Arc::new(crate::server::lake::LakeManager::new()),
        }))
    }

    /// State with worker1/worker2 (team alpha) + their manager registered, and
    /// per-agent + team graphs created with real owners.
    ///
    /// RBAC (CONCEPT:EG-KG.compute.feature) is the mandatory current access decision
    /// under `feature = "security"` (`isolation.rs::check_access`) — there is no
    /// pre-RBAC owner/manager/team ACL fall-through any more. This fixture used to
    /// register worker1/worker2/manager with `roles: vec![]`, which meant the
    /// RBAC evaluator always returned default-deny for them regardless of the
    /// owner/manager/team relationships still asserted below; the relationships
    /// have to be expressed as explicit RBAC grants now, replicating the retired
    /// ACL semantics one for one (owner full R/W on their own graph, manager
    /// full R/W on subordinate agent graphs, team members read/manager write on
    /// the team graph, every registered agent read-only on `global:*` and R/W on
    /// the open `__commons__` bus).
    async fn multi_tenant_state() -> Arc<RwLock<ServerState>> {
        let state = test_state();
        {
            let mut s = state.write().await;
            #[cfg(feature = "security")]
            {
                use crate::acl::{Grant, GrantEffect, RbacAction, ResourceSelector, Role};
                s.isolation.add_role(Role::new("owner-worker1"));
                s.isolation.add_role(Role::new("owner-worker2"));
                s.isolation.add_role(Role::new("manager-of-alpha"));
                s.isolation.add_role(Role::new("team-alpha-member"));
                s.isolation.add_role(Role::new("team-alpha-manager"));
                s.isolation.add_role(Role::new("global-reader"));
                s.isolation.add_role(Role::new("commons-user"));

                let grant = |role: &str, resource: ResourceSelector, action: RbacAction| Grant {
                    role: role.to_string(),
                    resource,
                    action,
                    effect: GrantEffect::Allow,
                };
                // Owner: full R/W on their own agent graph.
                for (role, graph) in [
                    ("owner-worker1", "agent:worker1"),
                    ("owner-worker2", "agent:worker2"),
                ] {
                    s.isolation.add_grant(grant(
                        role,
                        ResourceSelector::Graph(graph.to_string()),
                        RbacAction::Read,
                    ));
                    s.isolation.add_grant(grant(
                        role,
                        ResourceSelector::Graph(graph.to_string()),
                        RbacAction::Write,
                    ));
                }
                // Manager: full R/W on every subordinate's agent graph.
                s.isolation.add_grant(grant(
                    "manager-of-alpha",
                    ResourceSelector::Pattern("agent:*".to_string()),
                    RbacAction::Read,
                ));
                s.isolation.add_grant(grant(
                    "manager-of-alpha",
                    ResourceSelector::Pattern("agent:*".to_string()),
                    RbacAction::Write,
                ));
                // Team: members read, manager R/W.
                s.isolation.add_grant(grant(
                    "team-alpha-member",
                    ResourceSelector::Graph("team:alpha".to_string()),
                    RbacAction::Read,
                ));
                s.isolation.add_grant(grant(
                    "team-alpha-manager",
                    ResourceSelector::Graph("team:alpha".to_string()),
                    RbacAction::Read,
                ));
                s.isolation.add_grant(grant(
                    "team-alpha-manager",
                    ResourceSelector::Graph("team:alpha".to_string()),
                    RbacAction::Write,
                ));
                // Global: read-only for every agent.
                s.isolation.add_grant(grant(
                    "global-reader",
                    ResourceSelector::Pattern("global:*".to_string()),
                    RbacAction::Read,
                ));
                // Bus: all authenticated agents have full access.
                s.isolation.add_grant(grant(
                    "commons-user",
                    ResourceSelector::Graph("__commons__".to_string()),
                    RbacAction::Read,
                ));
                s.isolation.add_grant(grant(
                    "commons-user",
                    ResourceSelector::Graph("__commons__".to_string()),
                    RbacAction::Write,
                ));
            }
            s.isolation.register_agent(AgentIdentity {
                agent_id: "manager".into(),
                role: AgentRole::Manager {
                    subordinates: vec!["worker1".into(), "worker2".into()],
                },
                teams: vec!["alpha".into()],
                #[cfg(feature = "security")]
                roles: vec![
                    "manager-of-alpha".into(),
                    "team-alpha-manager".into(),
                    "global-reader".into(),
                    "commons-user".into(),
                ],
                #[cfg(not(feature = "security"))]
                roles: vec![],
            });
            for w in ["worker1", "worker2"] {
                s.isolation.register_agent(AgentIdentity {
                    agent_id: w.into(),
                    role: AgentRole::Agent,
                    teams: vec!["alpha".into()],
                    #[cfg(feature = "security")]
                    roles: vec![
                        format!("owner-{w}"),
                        "team-alpha-member".into(),
                        "global-reader".into(),
                        "commons-user".into(),
                    ],
                    #[cfg(not(feature = "security"))]
                    roles: vec![],
                });
            }
            s.registry
                .create_graph("agent:worker1", GraphType::Agent, Some("worker1".into()))
                .unwrap();
            s.registry
                .create_graph("team:alpha", GraphType::Team, Some("manager".into()))
                .unwrap();
            s.registry
                .create_graph("global:ontology", GraphType::Global, None)
                .unwrap();

            // Under the `security` feature, `IsolationLayer::check_access` defers
            // entirely to the RBAC evaluator (CONCEPT:EG-KG.compute.feature) -- the
            // `GraphType`/`graph_owner`-derived rules below are IGNORED for any
            // non-System identity. These grants reproduce, via RBAC, the EXACT same
            // access shape the graph-type rules describe (owner full access to their
            // own agent graph incl. one they create dynamically in tests, team
            // members read / manager read+write on the team graph, all-agents
            // read+write on `__commons__`, all-agents read-only on `global:*`, and a
            // manager's read+write reach onto a subordinate's agent graph) so the
            // ACL-shape tests below exercise the SAME intended policy through the
            // now-mandatory RBAC path instead of failing closed on an empty policy.
            #[cfg(feature = "security")]
            {
                use crate::acl::{Grant, GrantEffect, RbacAction, ResourceSelector, Role};
                let mut grant = |role: &str, resource: ResourceSelector, action: RbacAction| {
                    s.isolation.add_role(Role::new(role));
                    s.isolation.add_grant(Grant {
                        role: role.to_string(),
                        resource,
                        action,
                        effect: GrantEffect::Allow,
                    });
                };
                // Owners: full (read+write) access to their own agent graph.
                // worker2's "agent:worker2" doesn't exist yet at fixture time --
                // `test_create_graph_records_caller_as_owner` creates it -- but an
                // RBAC grant is independent of graph existence.
                for (agent, own_graph) in
                    [("worker1", "agent:worker1"), ("worker2", "agent:worker2")]
                {
                    let role = format!("{agent}-self");
                    grant(
                        &role,
                        ResourceSelector::Graph(own_graph.into()),
                        RbacAction::Read,
                    );
                    grant(
                        &role,
                        ResourceSelector::Graph(own_graph.into()),
                        RbacAction::Write,
                    );
                }
                // Team members: read-only on their team graph.
                grant(
                    "team-alpha-member",
                    ResourceSelector::Graph("team:alpha".into()),
                    RbacAction::Read,
                );
                // Manager: read+write on the team graph, and reaches (read+write)
                // into a subordinate's agent graph.
                for action in [RbacAction::Read, RbacAction::Write] {
                    grant(
                        "manager",
                        ResourceSelector::Graph("team:alpha".into()),
                        action,
                    );
                    grant(
                        "manager",
                        ResourceSelector::Graph("agent:worker1".into()),
                        action,
                    );
                }
                // Global graphs: read-only for every agent.
                for role in ["worker1-self", "worker2-self", "manager"] {
                    grant(
                        role,
                        ResourceSelector::Graph("global:ontology".into()),
                        RbacAction::Read,
                    );
                }
                // Commons: read+write for every authenticated agent.
                for role in ["worker1-self", "worker2-self", "manager"] {
                    for action in [RbacAction::Read, RbacAction::Write] {
                        grant(role, ResourceSelector::Graph("__commons__".into()), action);
                    }
                }
            }
        }
        state
    }

    fn request(id: u64, graph: &str, agent_id: Option<&str>, method: Method) -> Request {
        static NONCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let effective_agent = agent_id.unwrap_or("system");
        let claims = RequestContextClaims {
            principal: effective_agent.to_string(),
            tenant: "tenant-shared".to_string(),
            audience: "epistemic-graph-test".to_string(),
            agent_id: effective_agent.to_string(),
            roles: vec!["test".to_string()],
            scopes: vec!["*".to_string()],
            policy_version: "policy-test".to_string(),
            delegation: Vec::new(),
            node: None,
            priority: None,
        };
        let mut request = Request {
            id,
            graph: graph.to_string(),
            auth_token: String::new(),
            agent_id: Some(effective_agent.to_string()),
            method,
        };
        let nonce = format!(
            "server-mod-{}-{}",
            std::process::id(),
            NONCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        );
        request.auth_token = compute_verified_envelope_token(
            SECRET,
            &request,
            &VerifiedEnvelopeParams {
                context: &claims,
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .expect("system clock")
                    .as_secs(),
                nonce: &nonce,
                idempotency_key: &format!("server-mod-request-{id}"),
            },
        );
        request
    }

    fn add_node(node_id: &str) -> Method {
        Method::AddNode {
            node_id: node_id.to_string(),
            properties_msgpack: rmp_serde::to_vec(&serde_json::json!({})).unwrap(),
        }
    }

    fn assert_denied(resp: &Response) {
        let err = resp.error.as_deref().unwrap_or("");
        assert!(
            err.starts_with("ACCESS_DENIED"),
            "expected ACCESS_DENIED, got: ok={:?} err={:?}",
            resp.result,
            resp.error
        );
    }

    fn assert_ok(resp: &Response) {
        assert!(
            resp.error.is_none(),
            "expected success, got error: {:?}",
            resp.error
        );
    }

    // ── Cross-graph union reads (CONCEPT:EG-KG.query.cross-graph-union) ──────────────────────

    #[cfg(feature = "redb")]
    #[tokio::test]
    async fn test_union_read_across_graphs() {
        let state = test_state();
        {
            let mut s = state.write().await;
            s.registry
                .create_graph("__ingest__", GraphType::Global, None)
                .unwrap();
        }
        // Node A lives in __commons__, node B lives ONLY in __ingest__.
        let mk = |id: &str, name: &str| Method::AddNode {
            node_id: id.to_string(),
            properties_msgpack: rmp_serde::to_vec(
                &serde_json::json!({"type": "Doc", "name": name}),
            )
            .unwrap(),
        };
        assert_ok(
            &dispatch_on_heap(&state, request(1, "__commons__", None, mk("A", "alpha"))).await,
        );
        assert_ok(&dispatch_on_heap(&state, request(2, "__ingest__", None, mk("B", "beta"))).await);

        let graphs = vec!["__commons__".to_string(), "__ingest__".to_string()];

        // A single-graph read of __commons__ does NOT see B (proves the union does work).
        let single = dispatch_on_heap(
            &state,
            request(
                3,
                "__commons__",
                None,
                Method::GetNodeProperties {
                    node_id: "B".into(),
                },
            ),
        )
        .await;
        assert!(
            matches!(
                single.result,
                Some(ResultPayload::Json(serde_json::Value::Null))
            ),
            "B must be absent from __commons__ alone, got {:?}",
            single.result
        );

        // Union point read finds B (which lives only in __ingest__).
        let up = dispatch_on_heap(
            &state,
            request(
                4,
                "__commons__",
                None,
                Method::UnionGetNodeProperties {
                    graphs: graphs.clone(),
                    node_id: "B".into(),
                },
            ),
        )
        .await;
        assert_ok(&up);
        assert!(
            matches!(up.result, Some(ResultPayload::PropertiesMsgpack(_))),
            "union point read must find B across graphs, got {:?}",
            up.result
        );

        // Union label scan sees BOTH graphs, deduped by id.
        let ul = dispatch_on_heap(
            &state,
            request(
                5,
                "__commons__",
                None,
                Method::UnionGetNodesByLabel {
                    graphs: graphs.clone(),
                    label: "Doc".into(),
                    limit: 0,
                },
            ),
        )
        .await;
        assert_ok(&ul);
        match ul.result {
            Some(ResultPayload::NodeList(nodes)) => {
                let ids: std::collections::HashSet<String> =
                    nodes.iter().map(|(k, _)| k.clone()).collect();
                assert!(
                    ids.contains("A") && ids.contains("B"),
                    "union label scan must union both graphs, got {:?}",
                    ids
                );
            }
            other => panic!("expected NodeList, got {:?}", other),
        }

        // A missing lane graph in the set is skipped (no error), still returns __commons__'s A.
        let with_missing = vec![
            "__commons__".to_string(),
            "__ingest_does_not_exist__".to_string(),
        ];
        let um = dispatch_on_heap(
            &state,
            request(
                6,
                "__commons__",
                None,
                Method::UnionGetNodeProperties {
                    graphs: with_missing,
                    node_id: "A".into(),
                },
            ),
        )
        .await;
        assert_ok(&um);
        assert!(matches!(
            um.result,
            Some(ResultPayload::PropertiesMsgpack(_))
        ));
    }

    // ── SQL query surface (CONCEPT:EG-KG.query.read-only-sql-query) ────────────────────────────

    /// End-to-end: add nodes, then route `Method::Sql` through the full dispatch
    /// chain and decode the `Raw(QueryResult)` payload back to rows. Proves the
    /// query handler is wired before graph_ops and returns rows. (query feature)
    #[cfg(feature = "query")]
    #[tokio::test]
    async fn test_sql_select_returns_rows() {
        let state = test_state();
        let mk = |id: &str, ty: &str, rank: i64| Method::AddNode {
            node_id: id.to_string(),
            properties_msgpack: rmp_serde::to_vec_named(
                &serde_json::json!({"type": ty, "rank": rank}),
            )
            .unwrap(),
        };
        for (i, (id, ty, rank)) in [("n1", "Agent", 1), ("n2", "Agent", 2), ("n3", "Tool", 3)]
            .iter()
            .enumerate()
        {
            assert_ok(
                &dispatch_on_heap(
                    &state,
                    request(i as u64 + 1, "__commons__", None, mk(id, ty, *rank)),
                )
                .await,
            );
        }

        let resp = dispatch_on_heap(
            &state,
            request(
                10,
                "__commons__",
                None,
                Method::Sql {
                    query: "SELECT id FROM nodes WHERE rank >= 2 ORDER BY id LIMIT 5".into(),
                    params_msgpack: Vec::new(),
                },
            ),
        )
        .await;
        assert_ok(&resp);
        let raw = match resp.result {
            Some(ResultPayload::Raw(bytes)) => bytes,
            other => panic!("expected Raw(QueryResult), got {:?}", other),
        };
        let qr: crate::protocol::QueryResult = rmp_serde::from_slice(&raw).unwrap();
        assert_eq!(qr.columns, vec!["id".to_string()]);
        let ids: Vec<String> = qr
            .rows
            .iter()
            .map(|blob| {
                let cells: Vec<serde_json::Value> = rmp_serde::from_slice(blob).unwrap();
                cells[0].as_str().unwrap().to_string()
            })
            .collect();
        assert_eq!(ids, vec!["n2".to_string(), "n3".to_string()]);
    }

    // ── Unified cross-modal query (CONCEPT:AU-KG.compute.vector/209) ────────────────

    /// Build the canonical cross-modal fixture in `__commons__` via the full
    /// dispatch chain: Doc nodes with a `year`, CITES/MENTIONS edges, and an
    /// embedding per Doc. Mirrors eg-plan's test fixture so the dispatched plan and
    /// the in-crate oracle operate on identical data.
    #[cfg(feature = "query")]
    async fn build_unified_fixture(state: &Arc<RwLock<ServerState>>) {
        let mut id = 1u64;
        let mut send = |m: Method| {
            let r = request(id, "__commons__", None, m);
            id += 1;
            r
        };
        for (nid, ty, year) in [
            ("d1", "Doc", 2025),
            ("d2", "Doc", 2025),
            ("d3", "Doc", 2023),
            ("d4", "Doc", 2024),
            ("d5", "Doc", 2025),
            ("old", "Doc", 2020),
            ("t1", "Tool", 2025),
        ] {
            let m = Method::AddNode {
                node_id: nid.into(),
                properties_msgpack: rmp_serde::to_vec_named(
                    &serde_json::json!({"type": ty, "year": year}),
                )
                .unwrap(),
            };
            assert_ok(&dispatch_on_heap(state, send(m)).await);
        }
        for (s, t, rel) in [
            ("d1", "d2", "CITES"),
            ("d2", "d3", "CITES"),
            ("d1", "d4", "CITES"),
            ("d2", "d5", "MENTIONS"),
        ] {
            let m = Method::AddEdge {
                source_id: s.into(),
                target_id: t.into(),
                properties_msgpack: rmp_serde::to_vec_named(
                    &serde_json::json!({"relationship": rel}),
                )
                .unwrap(),
            };
            assert_ok(&dispatch_on_heap(state, send(m)).await);
        }
        for (nid, emb) in [
            ("d1", vec![0.2f32, 0.9, 0.0, 0.0]),
            ("d2", vec![0.98, 0.20, 0.0, 0.0]),
            ("d3", vec![0.80, 0.60, 0.0, 0.0]),
            ("d4", vec![0.90, 0.44, 0.0, 0.0]),
            ("d5", vec![0.0, 0.0, 1.0, 0.0]),
            ("old", vec![0.0, 1.0, 0.0, 0.0]),
        ] {
            let m = Method::AddEmbedding {
                node_id: nid.into(),
                embedding: emb,
            };
            assert_ok(&dispatch_on_heap(state, send(m)).await);
        }
    }

    /// Decode a `UnifiedQuery` response (`Raw([(id, score?)])`) to its id list.
    #[cfg(feature = "query")]
    fn unified_ids(resp: &crate::protocol::Response) -> Vec<String> {
        let raw = match &resp.result {
            Some(ResultPayload::Raw(bytes)) => bytes,
            other => panic!("expected Raw rows, got {:?}", other),
        };
        let rows: Vec<(String, Option<f32>)> = rmp_serde::from_slice(raw).unwrap();
        rows.into_iter().map(|(id, _)| id).collect()
    }

    /// THE oracle proof, end-to-end through the SERVED surface: run the unified plan
    /// `Method::UnifiedQuery` over the full dispatch chain, then run the SAME query
    /// the siloed way via `eg_plan::oracle::separate_surfaces` over the graph's
    /// snapshot, and assert the served result is byte-identical. (CONCEPT:AU-KG.compute.vector)
    #[cfg(feature = "query")]
    #[tokio::test]
    async fn test_unified_query_matches_separate_surfaces_oracle() {
        use eg_plan::{Op, Pred};
        let state = test_state();
        build_unified_fixture(&state).await;

        let plan = vec![
            Op::Scan {
                label: "Doc".into(),
            },
            Op::Filter {
                preds: vec![Pred::GtNum {
                    prop: "year".into(),
                    n: 2024.0,
                }],
            },
            Op::Traverse {
                rel: "CITES".into(),
                min: 1,
                max: 2,
            },
            Op::Rank {
                query: vec![1.0, 0.0, 0.0, 0.0],
            },
            Op::Limit { k: 10 },
        ];
        let resp = dispatch_on_heap(
            &state,
            request(
                100,
                "__commons__",
                None,
                Method::UnifiedQuery {
                    plan: eg_plan::Plan::new(plan),
                },
            ),
        )
        .await;
        assert_ok(&resp);
        let served_ids = unified_ids(&resp);

        // The siloed oracle over the same snapshot.
        let core = {
            let s = state.read().await;
            s.registry.get("__commons__").unwrap().core.clone()
        };
        let view = core.analysis_snapshot();
        let semantic = core.semantic_store.read().clone();
        // The oracle's FILTER leg drives DataFusion via a current-thread runtime
        // (eg_query::exec_sql), which cannot nest inside this #[tokio::test] reactor —
        // run it off-reactor, exactly as the served handler does via spawn_blocking.
        let oracle = tokio::task::spawn_blocking(move || {
            eg_plan::oracle::separate_surfaces(
                &view,
                &semantic,
                "Doc",
                &[Pred::GtNum {
                    prop: "year".into(),
                    n: 2024.0,
                }],
                "CITES",
                1,
                2,
                &[1.0, 0.0, 0.0, 0.0],
                10,
            )
        })
        .await
        .unwrap()
        .unwrap();

        assert_eq!(
            served_ids,
            oracle.ids(),
            "served unified plan must equal the separate-surfaces oracle"
        );
        assert_eq!(
            served_ids,
            vec!["d2", "d4", "d3"],
            "expected ranked order d2 > d4 > d3"
        );
    }

    /// UQL e2e (CONCEPT:AU-KG.query.top-nodes-by-degree): the SAME query written as a UQL TEXT string, served
    /// via `Method::UnifiedQueryText`, returns the BYTE-IDENTICAL result to (a) the
    /// hand-built structured `Method::UnifiedQuery` plan AND (b) the separate-surfaces
    /// oracle. This is the proof the text front-end is faithful: text → Plan → the
    /// SAME run_unified executor, no new execution path.
    #[cfg(feature = "query")]
    #[tokio::test]
    async fn test_uql_text_equals_structured_plan_and_oracle() {
        use eg_plan::{Op, Pred};
        let state = test_state();
        build_unified_fixture(&state).await;

        // (1) The structured plan (the existing surface).
        let plan = vec![
            Op::Scan {
                label: "Doc".into(),
            },
            Op::Filter {
                preds: vec![Pred::GtNum {
                    prop: "year".into(),
                    n: 2024.0,
                }],
            },
            Op::Traverse {
                rel: "CITES".into(),
                min: 1,
                max: 2,
            },
            Op::Rank {
                query: vec![1.0, 0.0, 0.0, 0.0],
            },
            Op::Limit { k: 10 },
        ];
        let structured = dispatch_on_heap(
            &state,
            request(
                300,
                "__commons__",
                None,
                Method::UnifiedQuery {
                    plan: eg_plan::Plan::new(plan),
                },
            ),
        )
        .await;
        assert_ok(&structured);
        let structured_ids = unified_ids(&structured);

        // (2) The SAME query as a UQL text string, served via the text surface.
        let uql = "MATCH (:Doc) WHERE year > 2024 \
                   |> TRAVERSE -[:CITES]->{1,2} \
                   |> RANK BY ~[1.0, 0.0, 0.0, 0.0] \
                   |> LIMIT 10";
        let textq = dispatch_on_heap(
            &state,
            request(
                301,
                "__commons__",
                None,
                Method::UnifiedQueryText { text: uql.into() },
            ),
        )
        .await;
        assert_ok(&textq);
        let text_ids = unified_ids(&textq);

        // (3) The siloed oracle over the same snapshot.
        let core = {
            let s = state.read().await;
            s.registry.get("__commons__").unwrap().core.clone()
        };
        let view = core.analysis_snapshot();
        let semantic = core.semantic_store.read().clone();
        let oracle = tokio::task::spawn_blocking(move || {
            eg_plan::oracle::separate_surfaces(
                &view,
                &semantic,
                "Doc",
                &[Pred::GtNum {
                    prop: "year".into(),
                    n: 2024.0,
                }],
                "CITES",
                1,
                2,
                &[1.0, 0.0, 0.0, 0.0],
                10,
            )
        })
        .await
        .unwrap()
        .unwrap();

        assert_eq!(
            text_ids, structured_ids,
            "UQL text result must equal the structured plan result"
        );
        assert_eq!(
            text_ids,
            oracle.ids(),
            "UQL text result must equal the separate-surfaces oracle"
        );
    }

    /// A malformed UQL string returns a CLEAR error Response (caret diagnostic), not a
    /// panic and not a wrong result. (CONCEPT:AU-KG.query.top-nodes-by-degree)
    #[cfg(feature = "query")]
    #[tokio::test]
    async fn test_uql_text_bad_syntax_is_clear_error() {
        let state = test_state();
        build_unified_fixture(&state).await;
        let resp = dispatch_on_heap(
            &state,
            request(
                302,
                "__commons__",
                None,
                Method::UnifiedQueryText {
                    text: "MATCH (:Doc) |> FROBNICATE".into(),
                },
            ),
        )
        .await;
        let err = resp.error.expect("malformed UQL must error");
        assert!(
            err.contains("UQL parse error") && err.contains("pipeline stage"),
            "expected a clear UQL parse error, got: {err}"
        );
    }

    // ── Query federation / foreign sources (CONCEPT:EG-KG.query.query-federation, Lane P) ───────

    /// Stand up the REMOTE engine (B) the query-federation tests read from: a
    /// `test_state()` carrying the `agent:federation-test` identity, the shared unified
    /// fixture, and its TCP listener served in the background. Returns the live state
    /// handle plus its `host:port`. The accept loop is UNBOUNDED (not the single
    /// `accept()` this setup started as), so one remote can serve MANY foreign
    /// round-trips — what the by-name federation proof below needs.
    #[cfg(feature = "federation")]
    async fn spawn_federation_remote() -> (Arc<RwLock<ServerState>>, String) {
        let remote = test_state();
        // The federated sub-query's `RequestContextClaims` (below) authenticates as
        // "agent:federation-test". Two independent gates require this identity be
        // registered on `remote`'s isolation layer, and `test_state()` only ever
        // registers "system":
        //
        //  1. Coarse graph-level RBAC (CONCEPT:EG-KG.compute.feature) —
        //     `check_graph_access` denies outright when the caller isn't a
        //     registered identity at all (`isolation.agents.get(agent_id)` misses).
        //  2. Row-level security (CONCEPT:EG-KG.sharding.row-level-security) —
        //     `rls.filter_view` hides every row an identity's OWN visibility check
        //     (`can_see_row`) rejects, and an identity's default posture for an
        //     UNTAGGED row (no `_owner`/`_visibility`/`_grants` property — exactly
        //     what `build_unified_fixture`'s Doc/Tool nodes are) is fail-closed
        //     (`RowVisibility::tagged == false` ⇒ hidden) unless the identity is
        //     `AgentRole::System`. A merely-registered `Agent`-role identity clears
        //     gate 1 but still gets an EMPTY `GraphView` back from gate 2 — the
        //     remote's own `UnifiedQueryText` handler then builds a SQL scan over
        //     zero rows, and DataFusion's schema inference (which promotes JSON
        //     property keys to real columns ONLY from rows it actually sees) infers
        //     no columns at all, so `WHERE year > 2024` fails with "No field named
        //     year" — the genuine root cause behind the generic "federation: remote
        //     engine returned an error" this test previously saw.
        //
        // A signed, HMAC-authenticated remote-engine federation channel (this
        // whole seam) is itself the trust boundary — the equivalent of a
        // privileged service account, not an end-user subject to per-row
        // ownership — so `System` is the correct role here, not a narrowly RBAC-
        // scoped one. The identity ALSO needs the `federation-reader` RBAC role
        // (under the `security` feature) so the coarse graph-level grant check
        // passes independently of RLS. Both must land in ONE `register_agent`
        // call: `IsolationLayer::register_agent` documents itself as "register OR
        // UPDATE" — a second call for the same `agent_id` overwrites the whole
        // identity record (including `role`), it does not merge fields. A prior
        // version of this test registered `role: System` and then immediately
        // re-registered the same agent as `role: Agent` to add the RBAC role list,
        // silently discarding the System role and reintroducing the exact
        // RLS-fail-closed "No field named year" symptom this comment describes.
        {
            let mut s = remote.write().await;
            s.isolation.register_agent(AgentIdentity {
                agent_id: "agent:federation-test".into(),
                role: AgentRole::System,
                teams: Vec::new(),
                #[cfg(feature = "security")]
                roles: vec!["federation-reader".into()],
                #[cfg(not(feature = "security"))]
                roles: vec![],
            });
            #[cfg(feature = "security")]
            {
                use crate::acl::{Grant, GrantEffect, RbacAction, ResourceSelector, Role};
                s.isolation.add_role(Role::new("federation-reader"));
                s.isolation.add_grant(Grant {
                    role: "federation-reader".into(),
                    resource: ResourceSelector::Graph("__commons__".into()),
                    action: RbacAction::Read,
                    effect: GrantEffect::Allow,
                });
            }
        }
        build_unified_fixture(&remote).await;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let remote_addr = listener.local_addr().unwrap().to_string();
        let remote_for_serve = remote.clone();
        tokio::spawn(async move {
            // UNBOUNDED accept loop: a by-name federation proof issues SEVERAL foreign
            // round-trips against the same remote (inline spec, `Named` spec, and the
            // `Op::Foreign` marker), so the single-`accept()` form this started as would
            // hang the second one. Each connection is served on its own task.
            while let Ok((stream, _)) = listener.accept().await {
                let serve = remote_for_serve.clone();
                tokio::spawn(async move { handle_connection(stream, serve).await });
            }
        });

        (remote, remote_addr)
    }

    /// The `RemoteEngine` [`eg_types::wire::ForeignSourceSpec`] pointed at
    /// [`spawn_federation_remote`]'s engine. Its UQL
    /// (`MATCH (:Doc) WHERE year > 2024 |> TRAVERSE -[:CITES]->{1,2}`) returns
    /// `{d2, d3, d4}` over the shared fixture. ONE spec, used BOTH inline (as an
    /// `Op::ForeignScan { source }`) and BY NAME (registered with
    /// `Method::RegisterForeignSource`, then referenced as a `Named` spec /
    /// `Op::Foreign`) — the two reach the SAME federation machinery.
    #[cfg(feature = "federation")]
    fn federation_remote_spec(endpoint: String) -> eg_types::wire::ForeignSourceSpec {
        eg_types::wire::ForeignSourceSpec::RemoteEngine {
            endpoint,
            graph: "__commons__".into(),
            secret: SECRET.into(),
            context: Box::new(eg_types::acl::RequestContextClaims {
                principal: "agent:federation-test".into(),
                tenant: "tenant-shared".into(),
                audience: "epistemic-graph-test".into(),
                agent_id: "agent:federation-test".into(),
                roles: vec!["federation-reader".into()],
                scopes: vec!["kg:read".into()],
                policy_version: "policy-test".into(),
                delegation: vec![],
                node: None,
                priority: None,
            }),
            uql: "MATCH (:Doc) WHERE year > 2024 |> TRAVERSE -[:CITES]->{1,2}".into(),
            cypher: String::new(),
            id_field: String::new(),
        }
    }

    /// THE federation compose proof through TWO in-process engines: a LOCAL engine A
    /// runs a `UnifiedQuery` whose plan `ForeignScan`s a REMOTE engine B (served over
    /// TCP, queried with the engine's own length-prefixed-MessagePack + HMAC transport),
    /// JOINS B's rows with A's local graph, ranks, and limits. The fused result equals
    /// the MANUAL join done by hand. This is the cross-engine federation seam: ONE plan,
    /// TWO engines, no Python round-trip. (CONCEPT:EG-KG.query.query-federation)
    #[cfg(feature = "federation")]
    #[tokio::test]
    async fn test_federated_query_two_engines_equals_manual_join() {
        use eg_plan::{Op, Pred};

        // ── engine B (the REMOTE), served over TCP ──
        let (_remote, remote_addr) = spawn_federation_remote().await;

        // ── engine A (the LOCAL) ──
        let local = test_state();
        build_unified_fixture(&local).await;

        // The remote returns the ids of Docs it CITES-reaches from the year>2024 seed (a
        // remote graph traversal): UQL `MATCH (:Doc) WHERE year>2024 |> TRAVERSE
        // -[:CITES]->{1,2}` → over B's fixture the seed is {d1,d2,d5} (years 2025) and
        // CITES-reaching 1..2 hops gives {d2,d3,d4} (d1→d2→d3, d1→d4). The foreign source
        // pulls those ids; A then JOINS them with its OWN local filter `year > 2023`.
        let foreign = federation_remote_spec(remote_addr);

        let query = vec![1.0f32, 0.0, 0.0, 0.0]; // ranks d2 > d4 > d3
        let plan = vec![
            Op::Scan {
                label: "Doc".into(),
            },
            Op::Filter {
                preds: vec![Pred::GtNum {
                    prop: "year".into(),
                    n: 2023.0,
                }],
            },
            Op::ForeignScan {
                source: Box::new(foreign),
                join: true,
            },
            Op::Rank {
                query: query.clone(),
            },
            Op::Limit { k: 10 },
        ];
        let resp = dispatch_on_heap(
            &local,
            request(
                500,
                "__commons__",
                None,
                Method::UnifiedQuery {
                    plan: eg_plan::Plan::new(plan),
                },
            ),
        )
        .await;
        assert_ok(&resp);
        let fused = unified_ids(&resp);

        // ── the manual join (the oracle) ──
        // A's local filter `year > 2023` → {d1, d2, d4, d5}. The remote's
        // CITES-traversal set → {d2, d3, d4}. Join (intersection) → {d2, d4}; ranked by
        // `[1,0,0,0]` → d2 then d4.
        let local_filtered: std::collections::HashSet<&str> =
            ["d1", "d2", "d4", "d5"].into_iter().collect();
        let remote_reached: std::collections::HashSet<&str> =
            ["d2", "d3", "d4"].into_iter().collect();
        let joined: std::collections::HashSet<String> = local_filtered
            .intersection(&remote_reached)
            .map(|s| s.to_string())
            .collect();
        let core = {
            let s = local.read().await;
            s.registry.get("__commons__").unwrap().core.clone()
        };
        let semantic = core.semantic_store.read().clone();
        let ranked = tokio::task::spawn_blocking(move || semantic.semantic_search(&query, 32))
            .await
            .unwrap();
        let manual: Vec<String> = ranked
            .into_iter()
            .map(|(id, _)| id)
            .filter(|id| joined.contains(id))
            .collect();

        assert_eq!(
            fused, manual,
            "federated two-engine plan must equal the manual join"
        );
        assert_eq!(fused, vec!["d2", "d4"], "ranked: d2 (closest) then d4");
    }

    /// `RegisterForeignSource` is served and recorded on `ServerState`. (CONCEPT:EG-KG.query.query-federation)
    #[cfg(feature = "federation")]
    #[tokio::test]
    async fn test_register_foreign_source_served() {
        let state = test_state();
        let resp = dispatch_on_heap(
            &state,
            request(
                600,
                "__commons__",
                None,
                Method::RegisterForeignSource {
                    name: "papers_api".into(),
                    source: eg_types::wire::ForeignSourceSpec::HttpJson {
                        url: "http://example.invalid/papers".into(),
                        json_path: "data".into(),
                        field_map: eg_types::wire::HttpFieldMap {
                            id: "id".into(),
                            score: None,
                        },
                    },
                },
            ),
        )
        .await;
        assert_ok(&resp);
        match resp.result {
            Some(ResultPayload::String(name)) => assert_eq!(name, "papers_api"),
            other => panic!("expected the registered name, got {other:?}"),
        }
        let s = state.read().await;
        assert!(
            s.foreign_sources.contains_key("papers_api"),
            "the source must be recorded on ServerState"
        );
    }

    /// CONCEPT:EG-KG.query.closure-backed-source — the READ side of `RegisterForeignSource`.
    ///
    /// `Method::RegisterForeignSource` has always WRITTEN `ServerState::foreign_sources`
    /// (proven by `test_register_foreign_source_served` above), but until `run_unified`
    /// bound that map into the executor's `eg_plan::federation::ForeignSourceRegistry`
    /// NOTHING in `src/` ever READ it: a caller could register a source successfully,
    /// exactly as the client/MCP surface and the agent-utilities skill reference
    /// document, and then have EVERY query naming it fail with "no ForeignSourceRegistry
    /// is attached to the PlanCtx". This test closes that loop end to end, through the
    /// FULL served dispatch chain:
    ///
    ///   1. register a `RemoteEngine` source under the name `remote_docs`;
    ///   2. run a `UnifiedQuery` whose plan carries `Op::ForeignScan { Named, join }`,
    ///      and assert it equals the byte-identical INLINE-spec federated result
    ///      (`["d2", "d4"]`, the manual-join oracle
    ///      `test_federated_query_two_engines_equals_manual_join` proves) — the
    ///      by-name and inline surfaces reach the SAME federation machinery, one
    ///      mechanism, not two;
    ///   3. run the UQL `FOREIGN "<name>"` marker (`Op::Foreign`) against the same
    ///      registered name and get the remote's rows.
    #[cfg(feature = "federation")]
    #[tokio::test]
    async fn test_registered_foreign_source_is_queryable_by_name() {
        use eg_plan::{Op, Pred};

        let (_remote, remote_addr) = spawn_federation_remote().await;
        let local = test_state();
        build_unified_fixture(&local).await;

        // (1) REGISTER — the surface the client/MCP/skill docs expose.
        let resp = dispatch_on_heap(
            &local,
            request(
                700,
                "__commons__",
                None,
                Method::RegisterForeignSource {
                    name: "remote_docs".into(),
                    source: federation_remote_spec(remote_addr),
                },
            ),
        )
        .await;
        assert_ok(&resp);

        // (2) QUERY BY NAME — a `Named` spec carries ONLY the registry key; resolving it
        // is exactly the read that did not exist before.
        let named_plan = vec![
            Op::Scan {
                label: "Doc".into(),
            },
            Op::Filter {
                preds: vec![Pred::GtNum {
                    prop: "year".into(),
                    n: 2023.0,
                }],
            },
            Op::ForeignScan {
                source: Box::new(eg_types::wire::ForeignSourceSpec::Named {
                    name: "remote_docs".into(),
                }),
                join: true,
            },
            Op::Rank {
                query: vec![1.0f32, 0.0, 0.0, 0.0],
            },
            Op::Limit { k: 10 },
        ];
        let resp = dispatch_on_heap(
            &local,
            request(
                701,
                "__commons__",
                None,
                Method::UnifiedQuery {
                    plan: eg_plan::Plan::new(named_plan),
                },
            ),
        )
        .await;
        assert_ok(&resp);
        assert_eq!(
            unified_ids(&resp),
            vec!["d2".to_string(), "d4".to_string()],
            "a REGISTERED foreign source must be queryable BY NAME, and must return the \
             byte-identical result the inline-spec ForeignScan returns"
        );

        // (3) the UQL `FOREIGN "<name>"` marker (`Op::Foreign`) — a pure SOURCE: the
        // remote's CITES-traversal set {d2, d3, d4} REPLACES the seed.
        let resp = dispatch_on_heap(
            &local,
            request(
                702,
                "__commons__",
                None,
                Method::UnifiedQuery {
                    plan: eg_plan::Plan::new(vec![
                        Op::Foreign {
                            name: "remote_docs".into(),
                        },
                        Op::Limit { k: 10 },
                    ]),
                },
            ),
        )
        .await;
        assert_ok(&resp);
        let mut marker_ids = unified_ids(&resp);
        marker_ids.sort();
        assert_eq!(
            marker_ids,
            vec!["d2".to_string(), "d3".to_string(), "d4".to_string()],
            "the UQL FOREIGN \"<name>\" marker must resolve through the same registry"
        );
    }

    /// CONCEPT:EG-KG.query.closure-backed-source — the NEGATIVE half: naming a source that
    /// was never registered must still fail, CLEANLY and loudly. Binding the registry
    /// must not turn an unbound name into silently-local rows (a `Named` `ForeignScan`
    /// with `join: true` degrading to "just the local candidate set" would be a silent
    /// correctness hole), and the error must name BOTH the missing source and what IS
    /// registered so the caller can see the typo.
    #[cfg(feature = "federation")]
    #[tokio::test]
    async fn test_unregistered_foreign_source_errors_cleanly() {
        use eg_plan::{Op, Pred};

        let (_remote, remote_addr) = spawn_federation_remote().await;
        let local = test_state();
        build_unified_fixture(&local).await;
        let resp = dispatch_on_heap(
            &local,
            request(
                710,
                "__commons__",
                None,
                Method::RegisterForeignSource {
                    name: "remote_docs".into(),
                    source: federation_remote_spec(remote_addr),
                },
            ),
        )
        .await;
        assert_ok(&resp);

        // A `Named` `ForeignScan` for a name nobody registered.
        let resp = dispatch_on_heap(
            &local,
            request(
                711,
                "__commons__",
                None,
                Method::UnifiedQuery {
                    plan: eg_plan::Plan::new(vec![
                        Op::Scan {
                            label: "Doc".into(),
                        },
                        Op::Filter {
                            preds: vec![Pred::GtNum {
                                prop: "year".into(),
                                n: 2023.0,
                            }],
                        },
                        Op::ForeignScan {
                            source: Box::new(eg_types::wire::ForeignSourceSpec::Named {
                                name: "typo_docs".into(),
                            }),
                            join: true,
                        },
                    ]),
                },
            ),
        )
        .await;
        let err = resp.error.expect(
            "an unregistered foreign source must ERROR, never silently degrade to the \
             local candidate set",
        );
        assert!(
            err.contains("typo_docs") && err.contains("remote_docs"),
            "the error must name the missing source AND list what IS registered, got: {err}"
        );

        // The same for the UQL `FOREIGN "<name>"` marker.
        let resp = dispatch_on_heap(
            &local,
            request(
                712,
                "__commons__",
                None,
                Method::UnifiedQuery {
                    plan: eg_plan::Plan::new(vec![Op::Foreign {
                        name: "typo_docs".into(),
                    }]),
                },
            ),
        )
        .await;
        let err = resp
            .error
            .expect("an unregistered FOREIGN \"<name>\" marker must error");
        assert!(
            err.contains("typo_docs"),
            "the marker's error must name the missing source, got: {err}"
        );
    }

    // ── Cypher query surface (CONCEPT:EG-KG.query.dep-free-behind) ─────────────────────────

    /// End-to-end: add nodes + a KNOWS edge, route `Method::CypherQuery` through
    /// the FULL dispatch chain, and decode the `Raw(QueryResult)` rows. Proves the
    /// dep-free Cypher handler is wired before graph_ops in a no-DataFusion build.
    /// (cypher feature)
    #[cfg(feature = "cypher")]
    #[tokio::test]
    async fn test_cypher_match_returns_rows() {
        let state = test_state();
        // `node_type`, not `type`: `eg_query::cypher::exec::node_has_label`
        // deliberately treats a bare `type`/`label` property as a legacy payload
        // that must never satisfy a Cypher node label (its `CREATE`/`MERGE` path
        // canonicalizes onto `node_type` only) — a documented, intentional
        // divergence from `GraphCore::labels_of`'s broader `type`/`node_type`/
        // `label` convention most OTHER fixtures in this file use.
        let add = |id: u64, node_id: &str, ty: &str, name: &str| {
            request(
                id,
                "__commons__",
                None,
                Method::AddNode {
                    node_id: node_id.to_string(),
                    properties_msgpack: rmp_serde::to_vec_named(
                        &serde_json::json!({"node_type": ty, "name": name}),
                    )
                    .unwrap(),
                },
            )
        };
        assert_ok(&dispatch_on_heap(&state, add(1, "alice", "Person", "Alice")).await);
        assert_ok(&dispatch_on_heap(&state, add(2, "bob", "Person", "Bob")).await);
        assert_ok(
            &dispatch_on_heap(
                &state,
                request(
                    3,
                    "__commons__",
                    None,
                    Method::AddEdge {
                        source_id: "alice".into(),
                        target_id: "bob".into(),
                        properties_msgpack: rmp_serde::to_vec_named(
                            &serde_json::json!({"relationship": "KNOWS"}),
                        )
                        .unwrap(),
                    },
                ),
            )
            .await,
        );

        // Single-node label MATCH → label index.
        let resp = dispatch_on_heap(
            &state,
            request(
                10,
                "__commons__",
                None,
                Method::CypherQuery {
                    query: "MATCH (a:Person) WHERE a.name = 'Alice' RETURN a".into(),
                    mode: crate::protocol::CypherMode::Read,
                },
            ),
        )
        .await;
        assert_ok(&resp);
        let qr = match resp.result {
            Some(ResultPayload::Raw(bytes)) => {
                rmp_serde::from_slice::<crate::protocol::QueryResult>(&bytes).unwrap()
            }
            other => panic!("expected Raw(QueryResult), got {:?}", other),
        };
        assert_eq!(qr.columns, vec!["a".to_string()]);
        let cells: Vec<serde_json::Value> = rmp_serde::from_slice(&qr.rows[0]).unwrap();
        // A bare node variable (`RETURN a`) projects as a canonical MAP (id +
        // node_type + properties), not a plain id string — see eg-query's own
        // `bare_node_projection_is_a_canonical_map` test.
        assert_eq!(cells[0]["id"].as_str(), Some("alice"));

        // 2-node typed-edge MATCH → VF2.
        let resp2 = dispatch_on_heap(
            &state,
            request(
                11,
                "__commons__",
                None,
                Method::CypherQuery {
                    query: "MATCH (a:Person)-[:KNOWS]->(b:Person) RETURN a, b".into(),
                    mode: crate::protocol::CypherMode::Read,
                },
            ),
        )
        .await;
        assert_ok(&resp2);
        let qr2 = match resp2.result {
            Some(ResultPayload::Raw(bytes)) => {
                rmp_serde::from_slice::<crate::protocol::QueryResult>(&bytes).unwrap()
            }
            other => panic!("expected Raw(QueryResult), got {:?}", other),
        };
        assert_eq!(qr2.columns, vec!["a".to_string(), "b".to_string()]);
        assert_eq!(qr2.rows.len(), 1);
        let pair: Vec<serde_json::Value> = rmp_serde::from_slice(&qr2.rows[0]).unwrap();
        assert_eq!(pair[0]["id"].as_str(), Some("alice"));
        assert_eq!(pair[1]["id"].as_str(), Some("bob"));
    }

    /// Feature-gating contract for the Cypher surface (CONCEPT:EG-KG.query.dep-free-behind): with the
    /// `cypher` feature off, `Method::CypherQuery`'s handler arm is compiled away
    /// and the request must hit the not-built catch-all. (Compiled out when
    /// `cypher` is on, where the real handler answers instead.)
    #[cfg(not(feature = "cypher"))]
    #[tokio::test]
    async fn test_cypher_gated_out_returns_not_built() {
        let state = test_state();
        let method = Method::CypherQuery {
            query: "MATCH (a:Person) RETURN a".into(),
            mode: crate::protocol::CypherMode::Read,
        };
        let resp = dispatch_on_heap(&state, request(1, "__commons__", None, method)).await;
        let err = resp.error.as_deref().unwrap_or("");
        assert!(
            err.contains("not available in this server build"),
            "expected the not-built catch-all, got: ok={:?} err={:?}",
            resp.result,
            resp.error
        );
    }

    /// End-to-end (CONCEPT:EG-KG.query.sparql-completeness): add the SAME alice-KNOWS->bob graph the Cypher
    /// test builds, route `Method::GraphQl` through the FULL dispatch chain, and PROVE
    /// the GraphQL result is the expected node/field set. When `cypher` is ALSO built,
    /// cross-check that the GraphQL KNOWS traversal equals the served Cypher result for
    /// the same question — the GraphQL==Cypher equivalence over the served surface.
    /// (graphql feature)
    #[cfg(feature = "graphql")]
    #[tokio::test]
    async fn test_graphql_routes_and_equals_cypher() {
        let state = test_state();
        // Both `type` AND `node_type`: GraphQL's own type resolution accepts either
        // (`eg_graphql::schema` checks `type`/`node_type`/`label`/`labels`), but
        // Cypher's `node_has_label` deliberately does NOT — a bare `type` is
        // documented as "a legacy payload [that] must never satisfy a Cypher node
        // label" (its `CREATE`/`MERGE` path canonicalizes onto `node_type` only).
        // This test's whole point is proving GraphQL == Cypher over the SAME
        // served data, so the fixture has to satisfy both surfaces' label
        // conventions at once.
        let add = |id: u64, node_id: &str, ty: &str, name: &str| {
            request(
                id,
                "__commons__",
                None,
                Method::AddNode {
                    node_id: node_id.to_string(),
                    properties_msgpack: rmp_serde::to_vec_named(
                        &serde_json::json!({"type": ty, "node_type": ty, "name": name}),
                    )
                    .unwrap(),
                },
            )
        };
        assert_ok(&dispatch_on_heap(&state, add(1, "alice", "Person", "Alice")).await);
        assert_ok(&dispatch_on_heap(&state, add(2, "bob", "Person", "Bob")).await);
        assert_ok(
            &dispatch_on_heap(
                &state,
                request(
                    3,
                    "__commons__",
                    None,
                    Method::AddEdge {
                        source_id: "alice".into(),
                        target_id: "bob".into(),
                        properties_msgpack: rmp_serde::to_vec_named(
                            &serde_json::json!({"relationship": "KNOWS"}),
                        )
                        .unwrap(),
                    },
                ),
            )
            .await,
        );

        // GraphQL: Alice + her KNOWS targets' names.
        let gql = dispatch_on_heap(
            &state,
            request(
                10,
                "__commons__",
                None,
                Method::GraphQl {
                    query: r#"{ Person(name: "Alice") { name KNOWS { name } } }"#.into(),
                    variables: None,
                },
            ),
        )
        .await;
        assert_ok(&gql);
        let value: serde_json::Value = match gql.result {
            Some(ResultPayload::Raw(bytes)) => rmp_serde::from_slice(&bytes).unwrap(),
            other => panic!("expected Raw(json), got {:?}", other),
        };
        let alice = &value["data"]["Person"][0];
        assert_eq!(alice["name"].as_str(), Some("Alice"));
        let gql_knows: Vec<String> = alice["KNOWS"]
            .as_array()
            .unwrap()
            .iter()
            .map(|n| n["name"].as_str().unwrap().to_string())
            .collect();
        // alice KNOWS bob.
        assert_eq!(gql_knows, vec!["Bob".to_string()]);

        // When cypher is ALSO built: prove GraphQL == Cypher over the SAME served
        // dispatch for the same question (the equivalence proof, served form).
        #[cfg(feature = "cypher")]
        {
            let cy = dispatch_on_heap(
                &state,
                request(
                    11,
                    "__commons__",
                    None,
                    Method::CypherQuery {
                        query: "MATCH (a:Person)-[:KNOWS]->(b:Person) WHERE a.name = 'Alice' \
                                RETURN b.name"
                            .into(),
                        mode: crate::protocol::CypherMode::Read,
                    },
                ),
            )
            .await;
            assert_ok(&cy);
            let qr = match cy.result {
                Some(ResultPayload::Raw(bytes)) => {
                    rmp_serde::from_slice::<crate::protocol::QueryResult>(&bytes).unwrap()
                }
                other => panic!("expected Raw(QueryResult), got {:?}", other),
            };
            let cy_knows: Vec<String> = qr
                .rows
                .iter()
                .map(|b| {
                    let cells: Vec<serde_json::Value> = rmp_serde::from_slice(b).unwrap();
                    cells[0].as_str().unwrap().to_string()
                })
                .collect();
            assert_eq!(
                gql_knows, cy_knows,
                "GraphQL KNOWS traversal must equal the served Cypher result"
            );
        }
    }

    #[tokio::test]
    async fn test_bad_auth_token_rejected() {
        let state = test_state();
        let mut req = request(1, "__commons__", None, Method::Ping);
        req.auth_token = "bogus".to_string();
        let resp = dispatch_on_heap(&state, req).await;
        assert_eq!(resp.error.as_deref(), Some("Authentication failed"));
    }

    /// Feature-gating contract: a gated-out domain's Method variant still exists
    /// in the wire enum, but with the feature off its handler arm is compiled
    /// away — the request must hit the explicit "not available in this build"
    /// catch-all, never panic or silently route elsewhere. `reasoning` is off in
    /// this build, so `RunDatalogReasoning` exercises the gate. (Compiled out
    /// when `reasoning` is enabled, where the real handler answers instead.)
    #[cfg(not(feature = "reasoning"))]
    #[tokio::test]
    async fn test_gated_out_method_returns_not_built() {
        let state = multi_tenant_state().await;
        let method = Method::RunDatalogReasoning {
            subclass_relations: vec![],
            subproperty_relations: vec![],
            symmetric_properties: vec![],
            transitive_properties: vec![],
            inverse_properties: vec![],
            domain_rules: vec![],
            range_rules: vec![],
            property_chains: vec![],
        };
        let resp =
            dispatch_on_heap(&state, request(1, "agent:worker1", Some("worker1"), method)).await;
        let err = resp.error.as_deref().unwrap_or("");
        assert!(
            err.contains("not available in this server build"),
            "expected the not-built catch-all, got: ok={:?} err={:?}",
            resp.result,
            resp.error
        );
    }

    /// Same feature-gating contract for the SQL surface (CONCEPT:EG-KG.query.read-only-sql-query): with
    /// the `query` feature off, `Method::Sql`'s handler arm is compiled away and
    /// the request must hit the not-built catch-all. (Compiled out when `query` is
    /// on, where the real handler answers instead.)
    #[cfg(not(feature = "query"))]
    #[tokio::test]
    async fn test_sql_gated_out_returns_not_built() {
        let state = test_state();
        let method = Method::Sql {
            query: "SELECT id FROM nodes".into(),
            params_msgpack: Vec::new(),
        };
        let resp = dispatch_on_heap(&state, request(1, "__commons__", None, method)).await;
        let err = resp.error.as_deref().unwrap_or("");
        assert!(
            err.contains("not available in this server build"),
            "expected the not-built catch-all, got: ok={:?} err={:?}",
            resp.result,
            resp.error
        );
    }

    /// Feature-gating contract for X-1 (CONCEPT:EG-X1): `Method::ExplainEvidence`
    /// exists in the wire enum whenever `epistemic` is on (see its doc comment), but
    /// its handler arm additionally requires `evidence-graph` — a build with
    /// `epistemic` on and `evidence-graph` off must hit the "not available in this
    /// server build" catch-all, never a panic or mis-route (mirrors
    /// `test_gated_out_method_returns_not_built` above).
    #[cfg(all(feature = "epistemic", not(feature = "evidence-graph")))]
    #[tokio::test]
    async fn explain_evidence_gated_out_returns_not_built() {
        let state = multi_tenant_state().await;
        let method = Method::ExplainEvidence {
            node_id: "claim1".into(),
        };
        let resp =
            dispatch_on_heap(&state, request(1, "agent:worker1", Some("worker1"), method)).await;
        let err = resp.error.as_deref().unwrap_or("");
        assert!(
            err.contains("not available in this server build"),
            "expected the not-built catch-all, got: ok={:?} err={:?}",
            resp.result,
            resp.error
        );
    }

    /// Feature-gating contract for EPI-P3-7 (gap-fill): `Method::ResolveConflict`
    /// exists in the wire enum whenever `epistemic` is on, but its handler arm
    /// additionally requires `epistemic-tms` — a build with `epistemic` on and
    /// `epistemic-tms` off must hit the "not available in this server build"
    /// catch-all, never a panic or mis-route (mirrors `explain_evidence_gated_out_returns_not_built`).
    #[cfg(all(feature = "epistemic", not(feature = "epistemic-tms")))]
    #[tokio::test]
    async fn resolve_conflict_gated_out_returns_not_built() {
        let state = multi_tenant_state().await;
        let method = Method::ResolveConflict {
            node_ids: vec!["claim1".into()],
            semantics: "grounded".into(),
        };
        let resp =
            dispatch_on_heap(&state, request(1, "agent:worker1", Some("worker1"), method)).await;
        let err = resp.error.as_deref().unwrap_or("");
        assert!(
            err.contains("not available in this server build"),
            "expected the not-built catch-all, got: ok={:?} err={:?}",
            resp.result,
            resp.error
        );
    }

    /// Same feature-gating contract for EPI-P3-3/P3-6: `Method::CausalEstimate`/
    /// `Method::CausalCounterfactual`/`Method::RankByProvenance` exist whenever
    /// `epistemic` is on, but their handler arms additionally require
    /// `epistemic-causal`.
    #[cfg(all(feature = "epistemic", not(feature = "epistemic-causal")))]
    #[tokio::test]
    async fn causal_estimate_and_rank_by_provenance_gated_out_return_not_built() {
        let state = multi_tenant_state().await;

        let method = Method::CausalEstimate {
            variables: vec![],
            do_values: std::collections::BTreeMap::new(),
            mode: crate::protocol::CausalQueryModeWire::Intervene,
        };
        let resp =
            dispatch_on_heap(&state, request(1, "agent:worker1", Some("worker1"), method)).await;
        let err = resp.error.as_deref().unwrap_or("");
        assert!(
            err.contains("not available in this server build"),
            "CausalEstimate: expected the not-built catch-all, got: ok={:?} err={:?}",
            resp.result,
            resp.error
        );

        let method = Method::CausalCounterfactual {
            variables: vec![],
            actual: std::collections::BTreeMap::new(),
            do_values: std::collections::BTreeMap::new(),
        };
        let resp =
            dispatch_on_heap(&state, request(2, "agent:worker1", Some("worker1"), method)).await;
        let err = resp.error.as_deref().unwrap_or("");
        assert!(
            err.contains("not available in this server build"),
            "CausalCounterfactual: expected the not-built catch-all, got: ok={:?} err={:?}",
            resp.result,
            resp.error
        );

        let method = Method::RankByProvenance {
            candidates: vec![],
            weights: Default::default(),
        };
        let resp =
            dispatch_on_heap(&state, request(3, "agent:worker1", Some("worker1"), method)).await;
        let err = resp.error.as_deref().unwrap_or("");
        assert!(
            err.contains("not available in this server build"),
            "RankByProvenance: expected the not-built catch-all, got: ok={:?} err={:?}",
            resp.result,
            resp.error
        );
    }

    #[cfg(feature = "redb")]
    #[tokio::test]
    async fn memory_cap_evicts_graphs_over_cap() {
        // E3: a graph above the per-graph cap is evicted (LRU) back down to it;
        // under the cap is a no-op. A cap of 0 is NOT a no-op — `evict_oversized_all`
        // (via `GraphCore::lru_eviction_candidates`) treats `max_nodes` literally:
        // "at or below the cap" for a cap of 0 means every durably-confirmed
        // resident node is a candidate, so it evicts everything still resident.
        let state = test_state();
        for i in 0..6 {
            assert_ok(
                &dispatch_on_heap(
                    &state,
                    request(1, "__commons__", None, add_node(&format!("n{i}"))),
                )
                .await,
            );
        }
        assert_eq!(
            crate::persist::evict_oversized_all(&state, 4).await,
            2,
            "6 nodes capped at 4 -> evict 2"
        );
        assert_eq!(crate::persist::evict_oversized_all(&state, 4).await, 0);
        assert_eq!(crate::persist::evict_oversized_all(&state, 100).await, 0);
        assert_eq!(
            crate::persist::evict_oversized_all(&state, 0).await,
            4,
            "cap 0 evicts every durably-confirmed resident node, not a no-op"
        );
    }

    #[cfg(feature = "redb")]
    #[tokio::test]
    async fn batch_node_reads_collapse_round_trips() {
        // A2: GetNodePropertiesBatch / HasNodesBatch fetch N nodes in one request.
        let state = test_state();
        for (id, k) in [("a", 1), ("b", 2)] {
            let m = Method::AddNode {
                node_id: id.into(),
                properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({ "k": k }))
                    .unwrap(),
            };
            assert_ok(&dispatch_on_heap(&state, request(1, "__commons__", None, m)).await);
        }

        let resp = dispatch_on_heap(
            &state,
            request(
                2,
                "__commons__",
                None,
                Method::GetNodePropertiesBatch {
                    node_ids: vec!["a".into(), "missing".into(), "b".into()],
                },
            ),
        )
        .await;
        let raw = match resp.result {
            Some(ResultPayload::Raw(b)) => b,
            other => panic!("expected Raw, got {other:?} (err={:?})", resp.error),
        };
        let rows: Vec<(String, Option<serde_bytes::ByteBuf>)> =
            rmp_serde::from_slice(&raw).unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].0, "a");
        assert!(rows[0].1.is_some(), "present node returns properties");
        assert_eq!(rows[1].0, "missing");
        assert!(rows[1].1.is_none(), "absent id returns nil");
        let a_props: serde_json::Value =
            rmp_serde::from_slice(rows[0].1.as_ref().unwrap()).unwrap();
        assert_eq!(a_props["k"], 1);

        let resp = dispatch_on_heap(
            &state,
            request(
                3,
                "__commons__",
                None,
                Method::HasNodesBatch {
                    node_ids: vec!["a".into(), "missing".into()],
                },
            ),
        )
        .await;
        let raw = match resp.result {
            Some(ResultPayload::Raw(b)) => b,
            other => panic!("expected Raw, got {other:?}"),
        };
        let flags: Vec<bool> = rmp_serde::from_slice(&raw).unwrap();
        assert_eq!(flags, vec![true, false]);

        // Oversize batches are rejected, not truncated (OOM guard).
        let resp = dispatch_on_heap(
            &state,
            request(
                4,
                "__commons__",
                None,
                Method::GetNodePropertiesBatch {
                    node_ids: vec!["x".to_string(); MAX_BATCH_IDS + 1],
                },
            ),
        )
        .await;
        assert!(resp.error.is_some(), "oversize batch must be rejected");
    }

    #[tokio::test]
    async fn get_neighbors_batch_collapses_round_trips() {
        // D-DPF-1: GetNeighborsBatch fetches neighbor ids for N nodes in one
        // request/one topo-lock acquisition instead of N GetNeighbors round-trips.
        //
        // Seeds via the GraphCore directly (not Method::AddNode/AddEdge through
        // dispatch): this test is about the READ path, and the durable
        // gateway-routed mutation path this repo's OWN
        // `batch_node_reads_collapse_round_trips` test also uses is independently
        // broken against `test_state()`'s `persistence: None` on unmodified main
        // (verified: `cargo test --features server
        // batch_node_reads_collapse_round_trips` fails identically on a fresh
        // main checkout with "authoritative MutationBatch commit requires a
        // persistence backend" — a pre-existing gap, not something this change
        // introduces or needs to fix). Going through the core's own `add_node`/
        // `add_edge` (used the same way `multi_tenant_state`/`test_union_read_across_graphs`
        // seed non-`__commons__` graphs) exercises the exact same
        // `GraphCore::get_neighbors`/`get_neighbors_batch` data this test cares
        // about without depending on that unrelated durable-write path at all.
        let state = test_state();
        let core = {
            let s = state.read().await;
            s.registry.get("__commons__").unwrap().core.clone()
        };
        for id in ["a", "b", "c"] {
            core.add_node(
                id.to_string(),
                rmp_serde::to_vec_named(&serde_json::json!({})).unwrap(),
            );
        }
        for (source, target) in [("a", "b"), ("b", "c")] {
            core.add_edge(
                source.to_string(),
                target.to_string(),
                rmp_serde::to_vec_named(&serde_json::json!({})).unwrap(),
            )
            .unwrap();
        }

        let resp = dispatch_on_heap(
            &state,
            request(
                6,
                "__commons__",
                None,
                Method::GetNeighborsBatch {
                    node_ids: vec!["a".into(), "b".into(), "missing".into(), "c".into()],
                },
            ),
        )
        .await;
        let raw = match resp.result {
            Some(ResultPayload::Raw(b)) => b,
            other => panic!("expected Raw, got {other:?} (err={:?})", resp.error),
        };
        let mut rows: Vec<(String, Vec<String>)> = rmp_serde::from_slice(&raw).unwrap();
        for (_, neighbors) in rows.iter_mut() {
            neighbors.sort();
        }
        assert_eq!(
            rows,
            vec![
                ("a".to_string(), vec!["b".to_string()]),
                ("b".to_string(), vec!["a".to_string(), "c".to_string()]),
                ("missing".to_string(), Vec::<String>::new()),
                ("c".to_string(), vec!["b".to_string()]),
            ],
            "one batched call returns every node's neighbors, in input order, \
             absent id -> empty list rather than failing the batch"
        );

        // Oversize batches are rejected, not truncated (OOM guard) — same
        // contract as GetNodePropertiesBatch/HasNodesBatch.
        let resp = dispatch_on_heap(
            &state,
            request(
                7,
                "__commons__",
                None,
                Method::GetNeighborsBatch {
                    node_ids: vec!["x".to_string(); MAX_BATCH_IDS + 1],
                },
            ),
        )
        .await;
        assert!(resp.error.is_some(), "oversize batch must be rejected");
    }

    #[cfg(feature = "redb")]
    #[tokio::test]
    async fn per_graph_backpressure_isolates_tenants() {
        // A hot graph that has exhausted its per-graph in-flight cap sheds WRITES with
        // BUSY, but OTHER graphs keep being served from the (ample) global pool — one
        // tenant cannot starve the rest. Per-graph backpressure is a WRITE property:
        // reads bypass the per-graph cap via the reserved read lane (CONCEPT:EG-KG.coordination.reserved-read-lane),
        // so both probes here are writes.
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        async fn round_trip(s: &mut tokio::io::DuplexStream, req: &Request) -> Response {
            let payload = rmp_serde::to_vec_named(req).unwrap();
            s.write_all(&(payload.len() as u32).to_be_bytes())
                .await
                .unwrap();
            s.write_all(&payload).await.unwrap();
            let mut lb = [0u8; 4];
            s.read_exact(&mut lb).await.unwrap();
            let n = u32::from_be_bytes(lb) as usize;
            let mut buf = vec![0u8; n];
            s.read_exact(&mut buf).await.unwrap();
            rmp_serde::from_slice(&buf).unwrap()
        }

        // RBAC (CONCEPT:EG-KG.compute.feature) is mandatory for every non-System
        // identity under `feature = "security"` — `check_graph_access` denies with
        // "a provisioned identity/RBAC policy is required" the instant
        // `isolation.has_rules()` is false (i.e. ZERO agents registered), which an
        // `IsolationLayer::new()` with nothing registered always is. `g_cold`'s write
        // reaches dispatch (unlike `g_hot`'s, shed BUSY at the per-graph admission
        // cap before any access check), so it needs a resolvable identity. `request()`
        // defaults `agent_id` to `"system"`, so register exactly that — the same
        // System-role bypass `test_state()` relies on everywhere else in this module.
        let mut isolation = IsolationLayer::new();
        isolation.register_agent(AgentIdentity {
            agent_id: "system".into(),
            role: AgentRole::System,
            teams: Vec::new(),
            roles: Vec::new(),
        });
        let state = Arc::new(RwLock::new(ServerState {
            #[cfg(feature = "redb")]
            cold_tracker: std::sync::Arc::new(
                crate::server::persistence::cold_offload::ColdTenantTracker::new(),
            ),
            registry: GraphRegistry::new(),
            isolation,
            channels: ChannelManager::new(),
            #[cfg(feature = "viz-static-export")]
            viz_engine: None,
            auth_secret: SECRET.to_string(),
            persist_dir: None,
            // AddNode is a durable, `GraphRedb`-domain GATEWAY_ROUTED method
            // (CONCEPT:EG-P0-2): now that `g_cold`'s write clears the RBAC gate
            // above, it reaches `commit_mutation_inner`'s durable-commit branch,
            // which fails closed ("authoritative MutationBatch commit requires a
            // persistence backend") without a REAL backend — the same
            // authoritative-commit flip `test_state()` documents. A real backend
            // on its own uniquely-named temp dir, same pattern as `test_state()`.
            #[cfg(feature = "redb")]
            persistence: Some(std::sync::Arc::new(
                crate::server::persistence::redb_backend::RedbBackend::open(
                    unique_temp_dir("eg-per-graph-backpressure")
                        .to_string_lossy()
                        .into_owned(),
                    crate::durability::DurabilityPolicy::Each,
                    256,
                )
                .expect("open test redb backend"),
            )),
            #[cfg(not(feature = "redb"))]
            persistence: None,
            max_in_flight: Arc::new(Semaphore::new(64)), // global: ample
            read_admission: Arc::new(Semaphore::new(64)),
            per_graph_inflight: Arc::new(DashMap::new()),
            per_graph_inflight_limit: 1, // any one graph: a single slot
            write_coalescer: Arc::new(crate::write_coalescer::WriteCoalescerRegistry::new()),
            routed_write_coalescer: Arc::new(
                crate::server::routed_write_coalescer::RoutedWriteCoalescerRegistry::new(),
            ),
            open_txns: Arc::new(DashMap::new()),
            txn_id_gen: Arc::new(crate::server::txn::TxnIdGen),
            txn_ttl_secs: 300,
            txn_max_per_graph: 256,
            txn_max_per_agent: 256,
            #[cfg(feature = "blob")]
            blob: None,
            #[cfg(feature = "blob")]
            blob_cursor_ttl_secs: 300,
            #[cfg(feature = "raft")]
            raft: None,
            #[cfg(feature = "raft")]
            multi_raft: None,
            #[cfg(feature = "tsdb")]
            tsdb_store: None,
            #[cfg(feature = "streaming")]
            cdc: Some(std::sync::Arc::new(crate::server::cdc::CdcHub::new())),
            #[cfg(feature = "wasm-udf")]
            udf_registry: std::sync::Arc::new(eg_wasm::UdfRegistry::new()),
            #[cfg(feature = "compute-dist")]
            matviews: std::sync::Arc::new(parking_lot::Mutex::new(
                crate::raft::pregel::MatViewStore::new(),
            )),
            #[cfg(feature = "federation")]
            foreign_sources: std::sync::Arc::new(dashmap::DashMap::new()),
            #[cfg(feature = "kv")]
            kv: None,
            #[cfg(feature = "lake")]
            lake: std::sync::Arc::new(crate::server::lake::LakeManager::new()),
        }));

        // Pre-seed g_hot's per-graph semaphore and hold its only permit, simulating
        // an op already in flight on that graph.
        let hot_sem = Arc::new(Semaphore::new(1));
        state
            .read()
            .await
            .per_graph_inflight
            .insert("g_hot".into(), hot_sem.clone());
        let _held = hot_sem.try_acquire_owned().unwrap();

        // g_cold's write must reach dispatch, so the graph has to exist. (g_hot's write
        // is shed at admission, BEFORE dispatch, so g_hot needs no registry entry.)
        state
            .write()
            .await
            .registry
            .create_graph("g_cold", GraphType::Agent, None)
            .unwrap();

        let (mut client, server) = tokio::io::duplex(64 * 1024);
        let st = state.clone();
        let handle = tokio::spawn(async move { handle_connection(server, st).await });

        // g_hot is saturated → its WRITE is shed BUSY at the per-graph cap.
        let r_hot = round_trip(&mut client, &request(1, "g_hot", None, add_node("h1"))).await;
        assert!(
            r_hot.error.as_deref().unwrap_or("").contains("at capacity"),
            "hot graph write must be shed BUSY, got {:?}",
            r_hot
        );

        // g_cold is independent → its WRITE is served normally despite g_hot saturation.
        let r_cold = round_trip(&mut client, &request(2, "g_cold", None, add_node("c1"))).await;
        assert!(
            r_cold.error.is_none(),
            "cold graph must NOT be starved by the hot graph, got {:?}",
            r_cold
        );

        drop(client);
        let _ = handle.await;
    }

    #[cfg(feature = "redb")]
    #[tokio::test]
    async fn test_owner_can_write_own_graph() {
        let state = multi_tenant_state().await;
        let resp = dispatch_on_heap(
            &state,
            request(1, "agent:worker1", Some("worker1"), add_node("n1")),
        )
        .await;
        assert_ok(&resp);
    }

    /// BUG-193 regression: a node written through the REAL production write
    /// path (`Method::AddNode`, dispatched exactly like every other wire
    /// caller — the mutation gateway's `try_handle_gateway`) by a non-System
    /// caller who supplies no explicit ownership key comes back stamped
    /// `_owner_id` = that caller's `agent_id` (`isolation::
    /// stamp_owner_id_if_absent`) and is visible to its writer under
    /// row-level RLS. Directly contrasted, in the SAME graph, with an
    /// ownerless LEGACY-shaped row (`_visibility: "public"`, no owner, no
    /// grants, no au tag — byte-identical to the 21,064-row BUG-064 incident
    /// population) written straight against `GraphCore`, bypassing the
    /// gateway entirely the way the incident's out-of-band writer did — that
    /// row MUST stay hidden. Proves BUG-193's fix and BUG-192's protection
    /// compose correctly rather than one silently undoing the other.
    /// `agent:worker1` is graph-OWNED by `worker1` at the graph-ACL level,
    /// but `can_see_row`/row-level RLS never consults graph ownership, so
    /// the hidden/visible split below is a genuine per-row result, not a
    /// graph-ACL side effect.
    #[cfg(feature = "redb")]
    #[tokio::test]
    async fn bug193_native_write_is_owner_stamped_and_visible_legacy_shape_stays_hidden() {
        let state = multi_tenant_state().await;

        // A real write by worker1 carrying NO ownership key of either
        // convention in the caller-supplied blob (`add_node` sends `{}`).
        let resp = dispatch_on_heap(
            &state,
            request(
                1,
                "agent:worker1",
                Some("worker1"),
                add_node("native-write"),
            ),
        )
        .await;
        assert_ok(&resp);

        let (core, isolation) = {
            let s = state.read().await;
            (
                s.registry.get("agent:worker1").unwrap().core.clone(),
                s.isolation.clone(),
            )
        };

        // The chokepoint stamped `_owner_id` from the caller — never present
        // in the caller-supplied blob.
        let stamped = eg_types::msgpack::decode_property_value(
            &core.get_node_properties("native-write").unwrap(),
        )
        .unwrap();
        assert_eq!(
            stamped["_owner_id"], "worker1",
            "a natively-written row with no caller-supplied owner must be \
             stamped with the writer's agent_id (BUG-193)"
        );

        // The exact BUG-064 incident shape, written directly against
        // `GraphCore` — bypassing the gateway the way the out-of-band
        // `engine_lifecycle_batch_update` incident did.
        core.add_node(
            "legacy-incident-shape".to_string(),
            rmp_serde::to_vec_named(&serde_json::json!({ "_visibility": "public" })).unwrap(),
        );
        core.mark_dirty();

        let context = super::auth::VerifiedRequestContext::verified_for_test("worker1");
        let authority = super::access::GraphReadAuthority::from_verified(&context, &isolation)
            .expect("worker1 is a registered non-System identity");
        let projected = authority.project_core(&core);
        assert!(
            projected.has_node("native-write"),
            "the natively-written, owner-stamped row must be visible to its writer"
        );
        assert!(
            !projected.has_node("legacy-incident-shape"),
            "the ownerless legacy-shaped row (BUG-064 incident population) must stay hidden"
        );
    }

    #[tokio::test]
    async fn test_peer_denied_read_and_write() {
        let state = multi_tenant_state().await;
        let resp = dispatch_on_heap(
            &state,
            request(1, "agent:worker1", Some("worker2"), add_node("n1")),
        )
        .await;
        assert_denied(&resp);
        let resp = dispatch_on_heap(
            &state,
            request(2, "agent:worker1", Some("worker2"), Method::GetNodes),
        )
        .await;
        assert_denied(&resp);
    }

    #[tokio::test]
    async fn test_anonymous_denied_when_rules_exist() {
        // Deliberately NOT `request(.., None, ..)`: that helper's `agent_id`
        // defaults `None` to `"system"` (`agent_id.unwrap_or("system")`), which
        // is the one identity `check_access` unconditionally bypasses
        // (`AgentRole::System`) — so it silently resolves an "anonymous" caller
        // to the single MOST privileged identity in the fixture, structurally
        // making this test unable to observe a real denial. ~30 other call
        // sites rely on that same `None` default to reach `__commons__`/
        // `__ingest__` as "some already-registered, already-permitted agent",
        // so the default can't change without breaking them (this repo's own
        // "there is no anonymous/trust fallback" posture — see
        // `src/server/mod.rs:73`, `mysql_wire/auth.rs`, `pgwire/auth.rs` —
        // means a *truly* unauthenticated caller is rejected before it ever
        // reaches dispatch, so there is nothing generic to fix in `request()`
        // itself). What this test needs is an agent id that is well-formed and
        // signs a valid envelope but was never provisioned via
        // `register_agent`/RBAC — i.e. genuinely anonymous from
        // `check_access`'s point of view (`self.agents.get(agent_id)` misses,
        // first line, before any RBAC evaluation).
        let state = multi_tenant_state().await;
        let resp = dispatch_on_heap(
            &state,
            request(
                1,
                "agent:worker1",
                Some("unregistered-anonymous-caller"),
                Method::GetNodes,
            ),
        )
        .await;
        assert_denied(&resp);
    }

    #[cfg(feature = "redb")]
    #[tokio::test]
    async fn test_manager_reaches_subordinate_graph() {
        let state = multi_tenant_state().await;
        let resp = dispatch_on_heap(
            &state,
            request(1, "agent:worker1", Some("manager"), add_node("n1")),
        )
        .await;
        assert_ok(&resp);
    }

    #[cfg(feature = "redb")]
    #[tokio::test]
    async fn test_team_member_read_only() {
        let state = multi_tenant_state().await;
        let resp = dispatch_on_heap(
            &state,
            request(1, "team:alpha", Some("worker1"), Method::GetNodes),
        )
        .await;
        assert_ok(&resp);
        let resp = dispatch_on_heap(
            &state,
            request(2, "team:alpha", Some("worker1"), add_node("n1")),
        )
        .await;
        assert_denied(&resp);
        let resp = dispatch_on_heap(
            &state,
            request(3, "team:alpha", Some("manager"), add_node("n1")),
        )
        .await;
        assert_ok(&resp);
    }

    #[tokio::test]
    async fn test_global_graph_read_only() {
        let state = multi_tenant_state().await;
        let resp = dispatch_on_heap(
            &state,
            request(1, "global:ontology", Some("worker1"), Method::GetNodes),
        )
        .await;
        assert_ok(&resp);
        let resp = dispatch_on_heap(
            &state,
            request(2, "global:ontology", Some("worker1"), add_node("n1")),
        )
        .await;
        assert_denied(&resp);
    }

    #[cfg(feature = "redb")]
    #[tokio::test]
    async fn test_bus_stays_open_to_all() {
        let state = multi_tenant_state().await;
        for (id, agent) in [(1, Some("worker1")), (2, Some("worker2")), (3, None)] {
            let resp = dispatch_on_heap(
                &state,
                request(id, "__commons__", agent, add_node(&format!("n{}", id))),
            )
            .await;
            assert_ok(&resp);
        }
    }

    #[cfg(feature = "redb")]
    #[tokio::test]
    async fn test_create_graph_records_caller_as_owner() {
        let state = multi_tenant_state().await;
        let resp = dispatch_on_heap(
            &state,
            request(
                1,
                "__commons__",
                Some("worker2"),
                Method::CreateGraph {
                    graph_name: "agent:worker2".to_string(),
                    graph_type: GraphType::Agent,
                },
            ),
        )
        .await;
        assert_ok(&resp);
        {
            let s = state.read().await;
            assert_eq!(
                s.registry.get("agent:worker2").unwrap().owner.as_deref(),
                Some("worker2")
            );
        }
        // Owner writes fine; the peer is denied.
        let resp = dispatch_on_heap(
            &state,
            request(2, "agent:worker2", Some("worker2"), add_node("n1")),
        )
        .await;
        assert_ok(&resp);
        let resp = dispatch_on_heap(
            &state,
            request(3, "agent:worker2", Some("worker1"), add_node("n2")),
        )
        .await;
        assert_denied(&resp);
    }

    #[cfg(all(feature = "redb", feature = "security"))]
    #[tokio::test]
    async fn test_create_graph_auto_provisions_tenant_rbac_for_a_different_principal() {
        // P0 root-cause regression test: `tenant__homelab____commons__` was
        // durably unreadable/unwritable by every ordinary principal because
        // CreateGraph never provisioned an RBAC grant for anyone
        // (`plans/au-eg-program/HANDOFF-2026-07-22.md` §7-8). This proves the
        // fix end-to-end through the REAL dispatch entrypoint (not just the
        // isolation.rs unit tests): creating a tenant graph must make it
        // reachable for a SEPARATE registered principal that merely carries
        // the tenant's role — the exact "N webui end-users share one tenant"
        // shape the live incident hit — with no manual grant ever issued.
        let state = multi_tenant_state().await;

        // A distinct principal, registered ahead of time (mirrors Tier-1
        // Keycloak provisioning already having run for this end-user), that
        // carries ONLY the tenant role — never the graph's creator, and never
        // System.
        {
            let mut s = state.write().await;
            s.isolation.register_agent(AgentIdentity {
                agent_id: "webui-end-user".to_string(),
                role: AgentRole::Agent,
                teams: vec![],
                roles: vec!["tenant:acme".to_string()],
            });
        }

        // A DIFFERENT identity (the tenant's provisioning/bootstrap actor)
        // creates the tenant's graph.
        let resp = dispatch_on_heap(
            &state,
            request(
                1,
                "__commons__",
                Some("worker1"),
                Method::CreateGraph {
                    graph_name: "tenant__acme____commons__".to_string(),
                    graph_type: GraphType::Agent,
                },
            ),
        )
        .await;
        assert_ok(&resp);

        // The end-user who never created anything, and was never individually
        // granted anything, can now read AND write it — purely because it
        // carries the auto-provisioned `tenant:acme` role.
        let resp = dispatch_on_heap(
            &state,
            request(
                2,
                "tenant__acme____commons__",
                Some("webui-end-user"),
                Method::GetNodes,
            ),
        )
        .await;
        assert_ok(&resp);
        let resp = dispatch_on_heap(
            &state,
            request(
                3,
                "tenant__acme____commons__",
                Some("webui-end-user"),
                add_node("n1"),
            ),
        )
        .await;
        assert_ok(&resp);

        // A SIBLING graph of the SAME tenant, created afterward by yet a THIRD
        // identity, is covered by the SAME tenant-wide grant the FIRST
        // CreateGraph provisioned -- no second manual grant needed.
        let resp = dispatch_on_heap(
            &state,
            request(
                4,
                "__commons__",
                Some("worker2"),
                Method::CreateGraph {
                    graph_name: "tenant__acme__default".to_string(),
                    graph_type: GraphType::Agent,
                },
            ),
        )
        .await;
        assert_ok(&resp);
        let resp = dispatch_on_heap(
            &state,
            request(
                5,
                "tenant__acme__default",
                Some("webui-end-user"),
                Method::GetNodes,
            ),
        )
        .await;
        assert_ok(&resp);

        // A principal from a DIFFERENT tenant is still denied.
        {
            let mut s = state.write().await;
            s.isolation.register_agent(AgentIdentity {
                agent_id: "other-tenant-user".to_string(),
                role: AgentRole::Agent,
                teams: vec![],
                roles: vec!["tenant:other".to_string()],
            });
        }
        let resp = dispatch_on_heap(
            &state,
            request(
                6,
                "tenant__acme____commons__",
                Some("other-tenant-user"),
                Method::GetNodes,
            ),
        )
        .await;
        assert_denied(&resp);
    }

    #[cfg(feature = "redb")]
    #[tokio::test]
    async fn test_delete_graph_requires_write_access() {
        let state = multi_tenant_state().await;
        let del = || Method::DeleteGraph {
            graph_name: "agent:worker1".to_string(),
        };
        let resp =
            dispatch_on_heap(&state, request(1, "__commons__", Some("worker2"), del())).await;
        assert_denied(&resp);
        let resp =
            dispatch_on_heap(&state, request(2, "__commons__", Some("worker1"), del())).await;
        assert_ok(&resp);
    }

    #[cfg(feature = "redb")]
    #[tokio::test]
    async fn test_channel_operations_unaffected_by_rules() {
        let state = multi_tenant_state().await;
        let resp = dispatch_on_heap(
            &state,
            request(
                1,
                "__commons__",
                Some("worker1"),
                Method::CreateChannel {
                    channel_id: "channel:p2p:worker1:worker2".to_string(),
                    channel_type: crate::protocol::ChannelType::PeerToPeer,
                    creator: "worker1".to_string(),
                    initial_members: vec!["worker1".to_string(), "worker2".to_string()],
                },
            ),
        )
        .await;
        assert_ok(&resp);
        let resp = dispatch_on_heap(
            &state,
            request(
                2,
                "__commons__",
                Some("worker2"),
                Method::SendMessage {
                    channel_id: "channel:p2p:worker1:worker2".to_string(),
                    sender: "worker2".to_string(),
                    payload: "hello".to_string(),
                },
            ),
        )
        .await;
        assert_ok(&resp);
    }

    // ── Lock-free compute (CONCEPT:EG-KG.txn.per-graph-write-isolation) ─────────────────────────────

    fn msgpack_props(val: serde_json::Value) -> Option<Vec<u8>> {
        Some(rmp_serde::to_vec_named(&val).expect("encode property object"))
    }

    #[test]
    fn test_weight_semantic_results_orders_decays_and_truncates() {
        let now = 100_000_000u64;
        let thirty_days = 30 * 86_400u64;
        let candidates = vec![
            // Fresh fact: confidence 1.0, no decay → keeps raw similarity.
            (
                "fresh".to_string(),
                0.8f32,
                msgpack_props(serde_json::json!({"type": "Fact", "valid_from": now})),
            ),
            // One half-life old: 0.9 similarity decays to ~0.45 → ranks below.
            (
                "aged".to_string(),
                0.9f32,
                msgpack_props(serde_json::json!({"type": "Fact", "valid_from": now - thirty_days})),
            ),
            // Validity window closed → filtered out entirely.
            (
                "stale".to_string(),
                0.99f32,
                msgpack_props(serde_json::json!({"type": "Fact", "valid_until": now - 1})),
            ),
            // No properties → similarity passes through unweighted.
            ("bare".to_string(), 0.5f32, None),
        ];

        let out = weight_semantic_results(candidates, now, 10);
        let ids: Vec<&str> = out.iter().map(|(id, _)| id.as_str()).collect();
        assert_eq!(ids, vec!["fresh", "bare", "aged"]);
        let aged_score = out[2].1;
        assert!(
            (aged_score - 0.45).abs() < 0.01,
            "expected ~0.45 after one half-life, got {aged_score}"
        );

        // Truncation honors n_results after re-ranking.
        let top1 = weight_semantic_results(
            vec![
                ("a".to_string(), 0.4f32, None),
                ("b".to_string(), 0.7f32, None),
            ],
            now,
            1,
        );
        assert_eq!(top1.len(), 1);
        assert_eq!(top1[0].0, "b");
    }

    /// Writers must keep making progress while a large semantic search (HNSW
    /// path, index rebuilt per query) runs concurrently on the same graph.
    /// Before KG-2.51 the search held the graph read lock for its whole
    /// duration; now it only memcpys the embedding store under the lock.
    #[cfg(feature = "redb")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_writers_not_starved_by_large_semantic_search() {
        let state = test_state();
        {
            let mut s = state.write().await;
            s.registry
                .create_graph("agent:busy", GraphType::Agent, None)
                .unwrap();
        }
        // Seed enough embeddings to take the HNSW path (>= brute-force
        // threshold) and make the search non-trivial.
        {
            let s = state.read().await;
            let core = s.registry.get("agent:busy").unwrap().core.clone();
            drop(s);
            let g = &*core;
            for i in 0..2_000u32 {
                let id = format!("n{}", i);
                g.add_node(
                    id.clone(),
                    rmp_serde::to_vec(&serde_json::json!({})).unwrap(),
                );
                let emb: Vec<f32> = (0..64).map(|d| ((i + d) % 97) as f32 / 97.0).collect();
                g.semantic_store.write().add_embedding(id, emb).unwrap();
            }
        }

        let search_state = state.clone();
        let search = tokio::spawn(async move {
            dispatch_on_heap(
                &search_state,
                request(
                    1,
                    "agent:busy",
                    None,
                    Method::SemanticSearch {
                        query_embedding: vec![0.5f32; 64],
                        n_results: 25,
                    },
                ),
            )
            .await
        });

        // GOC-70: this used to dispatch the 50 writes SEQUENTIALLY, each racing
        // its own tight 5s timeout, so a single scheduler hiccup on any one
        // write (unrelated to whether it was actually blocked behind the
        // search's lock) failed the whole test -- and on a many-core host
        // running the ~1000-test `--lib` suite at full host parallelism (every
        // test spinning its own multi-thread tokio runtime), that hiccup was
        // common enough to make this test flaky-fail there while it passed
        // reliably at CI's 2-core scale (confirmed: this test is 100% green
        // under `taskset -c 0,1`, the CI-equivalent constrained-parallelism
        // gate). The property under test is "writers are not BLOCKED behind the
        // search's lock", a deadlock/liveness property -- not "each write beats
        // a tight per-op deadline", which is a latency assertion sensitive to
        // ambient host load having nothing to do with the code path under test.
        // Fix: launch all 50 writes CONCURRENTLY and await the whole batch
        // against ONE generous timeout used purely as a hang/deadlock guard, so
        // ordinary scheduler jitter on any single write no longer fails the
        // test -- it only fails if the batch is genuinely stuck. This also
        // stresses the per-graph lock path more realistically than one write at
        // a time.
        // `tokio::spawn` starts each writer running CONCURRENTLY the instant it
        // is called (they do not wait for `.await` on the handle to begin
        // executing), so collecting the handles first and awaiting them in
        // order below retrieves already-in-flight results -- it does not
        // serialize their actual execution.
        let mut writers = Vec::with_capacity(50);
        for i in 0..50u64 {
            let st = state.clone();
            writers.push(tokio::spawn(async move {
                dispatch_on_heap(
                    &st,
                    request(100 + i, "agent:busy", None, add_node(&format!("w{}", i))),
                )
                .await
            }));
        }
        let batch = async {
            let mut results = Vec::with_capacity(writers.len());
            for h in writers {
                results.push(h.await.expect("writer task panicked"));
            }
            results
        };
        // 30s was still not always enough purely as a hang guard: this host
        // also runs unrelated production workloads (k8s/jellyfin/etc.)
        // alongside the ~1000-test suite, adding scheduling noise that has
        // nothing to do with this test's own code path. 120s remains many
        // multiples below what an actual deadlock looks like (indefinite),
        // so it does not weaken the guard's ability to catch a real one -- the
        // correctness fix is the concurrent-dispatch restructuring above, not
        // this bound.
        let results = tokio::time::timeout(std::time::Duration::from_secs(120), batch)
            .await
            .expect("writers starved (deadlocked) during semantic search");
        for r in &results {
            assert_ok(r);
        }

        let resp = search.await.expect("search task panicked");
        assert_ok(&resp);
        // Compact encoding (Phase C-D): the weighted result is a Raw msgpack blob.
        assert!(matches!(resp.result, Some(ResultPayload::Raw(_))));
    }

    /// W0.4 — a graph that crosses `ANN_BUILD_THRESHOLD` AFTER "boot" (this
    /// harness never runs `main.rs`'s boot-time warm task at all) must still
    /// reach `is_ready()` WITHOUT a restart: the post-write dispatch-tail trigger
    /// (`ann_warm::maybe_warm_after_write`) must spawn the warm the moment a
    /// write on the graph observes the threshold crossed. Brute-force search
    /// stays exactly correct both before the warm and after.
    #[cfg(feature = "ann")]
    #[tokio::test]
    async fn test_ann_warms_on_demand_after_threshold_crossing_post_boot() {
        let state = test_state();
        {
            let mut s = state.write().await;
            s.registry
                .create_graph("agent:grows-post-boot", GraphType::Agent, None)
                .unwrap();
        }
        let core = {
            let s = state.read().await;
            s.registry
                .get("agent:grows-post-boot")
                .unwrap()
                .core
                .clone()
        };

        // Simulate embeddings accumulated before this process's "boot", seeded
        // directly (bypassing per-request dispatch overhead so the test stays
        // fast) — exactly the state a graph is in the instant it crosses the
        // threshold, with no boot-time warm task ever having run for it (there is
        // none in this harness).
        let dim = 8;
        let n = crate::compute::semantic_ann::ANN_BUILD_THRESHOLD + 50;
        let mut target = vec![0.0f32; dim];
        {
            let mut store = core.semantic_store.write();
            for i in 0..n {
                let mut v = vec![0.0f32; dim];
                v[i % dim] = 1.0;
                // A bare one-hot on `i % dim` repeats EXACTLY every `dim` steps — with
                // `n` in the thousands and `dim == 8`, ~n/8 nodes end up byte-identical
                // to n42's vector (cosine similarity 1.0 to `target`, an exact tie).
                // `semantic_search`'s top-k over that many exact ties is not required to
                // include n42 specifically (brute-force AND the ANN index are both free
                // to return any tied candidate), so asserting n42 is in the top 3 was
                // non-deterministic by construction. A tiny per-`i`-unique perturbation
                // on a DIFFERENT coordinate keeps every vector distinguishable — n42
                // then has the single, unique closest match to its own (unperturbed)
                // `target` copy, deterministically ranking it #1 in both the brute-force
                // "before" and the (possibly-ANN) "after" search.
                v[(i + 1) % dim] += 1e-4 * (i as f32 + 1.0);
                if i == 42 {
                    target = v.clone();
                }
                store.add_embedding(format!("n{i}"), v).unwrap();
            }
        }
        assert!(
            !core.semantic_store.read().is_ready(),
            "nothing has triggered a warm yet"
        );
        // Brute force must already be exact even before any warm.
        let before = core.semantic_store.read().semantic_search(&target, 3);
        assert!(before.iter().any(|(id, _)| id == "n42"));

        // The ONE mechanism under test: a normal write through the real dispatch
        // path (any Write-classified method on this graph) must trigger the
        // post-write warm hook — no restart, no explicit warm() call from the
        // test itself.
        let resp = dispatch_on_heap(
            &state,
            request(1, "agent:grows-post-boot", None, add_node("trigger")),
        )
        .await;
        assert_ok(&resp);

        // 30s, not 10s: building the ANN index over ~4.1K vectors in an unoptimized
        // `cargo test` debug binary measured ~10.3s wall-clock on the CI build host
        // — right at the edge of the original 10s budget (this path was never
        // actually exercised before: the test used to panic earlier, at the
        // brute-force "before" assertion, on a fundamentally non-deterministic
        // target vector — see the seeding loop above). This is purely a debug-build
        // timing margin, not a correctness change: the loop condition
        // (`is_ready()`) is untouched.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
        while !core.semantic_store.read().is_ready() {
            assert!(
                std::time::Instant::now() < deadline,
                "semantic ANN index never warmed after crossing the threshold post-write"
            );
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }

        // Still exact after warming — the ANN path must agree with brute force.
        let after = core.semantic_store.read().semantic_search(&target, 3);
        assert!(after.iter().any(|(id, _)| id == "n42"));
    }

    /// One-round-trip hybrid discovery (CONCEPT:EG-KG.retrieval.one-round-trip-discovery): a single `Discover`
    /// blends dense (HNSW) similarity with lexical keyword overlap and returns the
    /// top-k hydrated with `name`/`description`/`type` text. The keyword signal
    /// must be able to promote a lexically-strong hit above a slightly-closer pure
    /// vector neighbour, and an empty embedding must degrade to keyword-only.
    #[tokio::test]
    async fn test_discover_blends_keyword_and_semantic() {
        let state = test_state();
        {
            let mut s = state.write().await;
            s.registry
                .create_graph("agent:disc", GraphType::Agent, None)
                .unwrap();
        }
        let core = {
            let s = state.read().await;
            s.registry.get("agent:disc").unwrap().core.clone()
        };
        // Three nodes with text + embeddings. `deployer` is the exact query
        // direction; `deploy_runbook` is slightly farther in vector space BUT its
        // text matches the "deploy" keyword; `unrelated` matches neither.
        let seed = |id: &str, props: serde_json::Value, emb: Vec<f32>| {
            core.add_node(id.to_string(), rmp_serde::to_vec(&props).unwrap());
            core.semantic_store
                .write()
                .add_embedding(id.to_string(), emb)
                .unwrap();
        };
        seed(
            "deployer",
            serde_json::json!({"type": "agent", "name": "Release Bot",
                "description": "ships builds"}),
            vec![1.0, 0.0, 0.0],
        );
        seed(
            "deploy_runbook",
            serde_json::json!({"type": "doc", "name": "Deploy Runbook",
                "description": "how to deploy a service"}),
            vec![0.92, 0.39, 0.0],
        );
        seed(
            "unrelated",
            serde_json::json!({"type": "doc", "name": "Kitchen Sink",
                "description": "nothing to see"}),
            vec![0.0, 1.0, 0.0],
        );

        // Hybrid: keyword "deploy" + an embedding closest to `deployer`.
        let resp = dispatch_on_heap(
            &state,
            request(
                1,
                "agent:disc",
                None,
                Method::Discover {
                    keywords: vec!["deploy".into()],
                    query_embedding: vec![1.0, 0.0, 0.0],
                    k: 3,
                },
            ),
        )
        .await;
        assert_ok(&resp);
        let Some(ResultPayload::Json(serde_json::Value::Array(rows))) = resp.result else {
            panic!("expected Json array discover payload");
        };
        assert_eq!(rows.len(), 3, "all three candidates returned, ranked");
        // `deploy_runbook` wins: keyword overlap (0.4) + strong sim beats the
        // marginally-closer keyword-less `deployer`.
        assert_eq!(rows[0]["id"], "deploy_runbook");
        assert_eq!(rows[0]["name"], "Deploy Runbook");
        assert_eq!(rows[0]["type"], "doc");
        assert_eq!(rows[0]["description"], "how to deploy a service");
        assert!(rows[0]["score"].as_f64().unwrap() > rows[1]["score"].as_f64().unwrap());
        // `unrelated` (neither signal) ranks last.
        assert_eq!(rows[2]["id"], "unrelated");

        // Embedding-absent fallback: keyword-only still finds the matching nodes.
        let kw_only = dispatch_on_heap(
            &state,
            request(
                2,
                "agent:disc",
                None,
                Method::Discover {
                    keywords: vec!["deploy".into()],
                    query_embedding: vec![],
                    k: 5,
                },
            ),
        )
        .await;
        assert_ok(&kw_only);
        let Some(ResultPayload::Json(serde_json::Value::Array(rows))) = kw_only.result else {
            panic!("expected Json array discover payload");
        };
        // Only `deploy_runbook` has "deploy" in its text (name+description).
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["id"], "deploy_runbook");
    }

    #[cfg(feature = "redb")]
    #[tokio::test]
    async fn test_offloaded_algorithms_round_trip() {
        // Snapshot+spawn_blocking arms must preserve result semantics.
        let state = test_state();
        {
            let mut s = state.write().await;
            s.registry
                .create_graph("agent:algo", GraphType::Agent, None)
                .unwrap();
        }
        for (id, m) in [
            (1, add_node("a")),
            (2, add_node("b")),
            (3, add_node("c")),
            (
                4,
                Method::AddEdge {
                    source_id: "a".into(),
                    target_id: "b".into(),
                    properties_msgpack: rmp_serde::to_vec(&serde_json::json!({"weight": 2.0}))
                        .unwrap(),
                },
            ),
            (
                5,
                Method::AddEdge {
                    source_id: "b".into(),
                    target_id: "c".into(),
                    properties_msgpack: rmp_serde::to_vec(&serde_json::json!({})).unwrap(),
                },
            ),
        ] {
            assert_ok(&dispatch_on_heap(&state, request(id, "agent:algo", None, m)).await);
        }

        let pagerank = dispatch_on_heap(
            &state,
            request(
                10,
                "agent:algo",
                None,
                Method::PageRank {
                    damping: 0.85,
                    iterations: 20,
                },
            ),
        )
        .await;
        assert_ok(&pagerank);
        // Compact encoding (Phase C-D): pagerank returns a Raw msgpack blob that
        // decodes to the exact same typed result.
        let Some(ResultPayload::Raw(bytes)) = pagerank.result else {
            panic!("expected Raw pagerank result");
        };
        let scores: Vec<(String, f64)> = rmp_serde::from_slice(&bytes).unwrap();
        assert_eq!(scores.len(), 3);

        let communities = dispatch_on_heap(
            &state,
            request(
                11,
                "agent:algo",
                None,
                Method::CommunityDetection { resolution: 1.0 },
            ),
        )
        .await;
        assert_ok(&communities);

        let metrics =
            dispatch_on_heap(&state, request(12, "agent:algo", None, Method::Metrics)).await;
        assert_ok(&metrics);
        let Some(ResultPayload::Json(m)) = metrics.result else {
            panic!("expected JSON metrics result");
        };
        assert_eq!(m.get("node_count").and_then(|v| v.as_u64()), Some(3));
        assert_eq!(m.get("edge_count").and_then(|v| v.as_u64()), Some(2));
        // total_mutations comes from the ledger length captured under-lock
        // (3 adds + 2 edges) — the snapshot itself carries no ledger.
        assert_eq!(m.get("total_mutations").and_then(|v| v.as_u64()), Some(5));
    }

    #[tokio::test]
    async fn test_diff_against_gates_other_graph() {
        let state = multi_tenant_state().await;
        // worker2 owns nothing here; create their graph for the diff source.
        {
            let mut s = state.write().await;
            s.registry
                .create_graph("agent:worker2", GraphType::Agent, Some("worker2".into()))
                .unwrap();
        }
        // worker2 may read its own graph but NOT diff it against worker1's.
        let resp = dispatch_on_heap(
            &state,
            request(
                1,
                "agent:worker2",
                Some("worker2"),
                Method::DiffAgainst {
                    other_graph: "agent:worker1".to_string(),
                },
            ),
        )
        .await;
        assert_denied(&resp);
    }

    /// CONCEPT:EG-KG.txn.per-graph-write-isolation — Per-graph write isolation (parallel writers).
    ///
    /// Writers to DIFFERENT graphs must never serialize on a global/registry lock:
    /// `dispatch_graph_op` only takes the global `ServerState` lock as a SHARED
    /// reader, clones the target graph's `Arc<GraphCore>`, and releases it before
    /// any mutation — so the only write lock taken is `GraphCore::topo`, which is
    /// per-graph. This reproduces the starvation scenario: a long-running write
    /// txn on graph A (a stand-in for sustained ingestion holding A's write lock)
    /// must NOT block writers targeting graph B.
    #[cfg(feature = "redb")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_writers_to_distinct_graphs_do_not_serialize() {
        let state = test_state();
        {
            let mut s = state.write().await;
            s.registry
                .create_graph("agent:ingest", GraphType::Agent, None)
                .unwrap();
            s.registry
                .create_graph("agent:control", GraphType::Agent, None)
                .unwrap();
        }

        // Grab graph A's core and open a write txn, HOLDING its topology write lock
        // for the duration — exactly what sustained ingestion does to one graph.
        let ingest_core = {
            let s = state.read().await;
            s.registry.get("agent:ingest").unwrap().core.clone()
        };
        let _held_txn = ingest_core.txn(); // holds agent:ingest topo.write()

        // With A's write lock held, writers to B (the control plane) must still
        // complete. If anything serialized writes across graphs (a global write
        // lock, or lazy-create under a registry write lock), every one of these
        // would deadlock against `_held_txn` and the timeout below would fire.
        //
        // GOC-70: this used to dispatch the 25 writes SEQUENTIALLY, each racing
        // its own tight 5s timeout -- a latency assertion sensitive to ambient
        // host load, not to the lock-granularity property actually under test
        // (confirmed: 100% green under `taskset -c 0,1`, the CI-equivalent
        // constrained-parallelism gate; only flaky-failed on a many-core host
        // running the ~1000-test `--lib` suite at full host parallelism). Fixed
        // the same way as `test_writers_not_starved_by_large_semantic_search`
        // above: launch all 25 writes CONCURRENTLY and await the whole batch
        // against ONE generous timeout used purely as a deadlock guard, so
        // ordinary scheduler jitter on any single write no longer fails the
        // test -- and concurrent dispatch stresses the cross-graph lock path
        // more realistically than one write at a time.
        let mut writers = Vec::with_capacity(25);
        for i in 0..25u64 {
            let st = state.clone();
            writers.push(tokio::spawn(async move {
                dispatch_on_heap(
                    &st,
                    request(200 + i, "agent:control", None, add_node(&format!("c{}", i))),
                )
                .await
            }));
        }
        let batch = async {
            let mut results = Vec::with_capacity(writers.len());
            for h in writers {
                results.push(h.await.expect("writer task panicked"));
            }
            results
        };
        // See test_writers_not_starved_by_large_semantic_search's comment above
        // for why 120s (this host also runs unrelated production workloads
        // alongside the ~1000-test suite): still many multiples below what an
        // actual deadlock looks like (indefinite), and not the correctness
        // mechanism -- that's the concurrent-dispatch restructuring above.
        let results = tokio::time::timeout(std::time::Duration::from_secs(120), batch)
            .await
            .expect("control-plane writers starved (deadlocked) by ingestion holding another graph's lock");
        for r in &results {
            assert_ok(r);
        }

        // The held graph took no control-plane writes; the control graph took all.
        drop(_held_txn);
        assert_eq!(ingest_core.node_count(), 0);
        let control_core = {
            let s = state.read().await;
            s.registry.get("agent:control").unwrap().core.clone()
        };
        assert_eq!(control_core.node_count(), 25);
    }

    /// CONCEPT:EG-KG.sharding.per-graph-write-coalescer — per-graph write coalescer, end-to-end through dispatch.
    ///
    /// Many concurrent writers to ONE hot graph (the `__commons__` firehose) must ALL
    /// land via the dispatch path — no lost writes among admitted requests — and
    /// every admitted op must be accounted for in the writer's stats. A saturated
    /// bounded queue rejects new requests with BUSY before they can mutate RAM or
    /// durable state; it never uses an unordered inline fallback.
    ///
    /// L18 rewrite (2026-08-11): `mutation::commit_coalescable_mutation` no longer
    /// holds `mutation_batch::lock_graph` itself around the enqueue+await for the
    /// five coalescable structural writes — it hands the WHOLE prepare→durable-
    /// commit→RAM-publish sequence to `server::routed_write_coalescer`'s per-graph
    /// worker, which acquires `lock_graph` ONCE per flushed batch and runs every
    /// queued job's sequence inside that one hold (see that module's docs and
    /// `commit_coalescable_mutation`'s doc for why batching the RAM publish ALONE,
    /// the previous attempt, was unsafe). So the worker's queue genuinely CAN hold
    /// more than one job at a time now: 200 concurrent dispatches race to enqueue
    /// onto the SAME per-graph worker without each needing its own `lock_graph`
    /// acquisition first, so this proves a REAL batching win, not just a 1:1
    /// accounting identity.
    ///
    /// Portability fix (2026-08-13, D-EG-CI-2core): this test used to spawn its
    /// 200 dispatches and just hope enough of them piled up concurrently for a
    /// meaningful batching win to occur AND for all of them to land via the
    /// worker's queue. That is exactly what a small CI runner breaks:
    /// `write_coalescer::CoalescerConfig::auto` sizes `queue_capacity` from
    /// the cgroup-aware `Capacity::writer_queue()` floor of 256 at constrained
    /// CPU budgets. N=200 stays within that bounded admission window on
    /// constrained runners, so this test proves accepted FIFO work rather than
    /// relying on a scheduler-dependent mixture of BUSY responses.
    /// The remaining assertion — that batching actually happened — is a
    /// genuinely timing-dependent property (it requires real concurrent pile-up
    /// against the worker), so instead of trusting the scheduler to provide that
    /// pile-up on whatever host runs this, this test now FORCES it
    /// deterministically: it holds `mutation_batch::lock_graph("__commons__")` —
    /// the exact lock the worker's `flush_batch` must acquire to make ANY
    /// progress — before firing off all 200 dispatches, and only releases it
    /// once every spawned task has actually started running (an atomic barrier,
    /// not a sleep). Every writer is thus forced to queue up in the coalescer's
    /// channel before ANY of them can
    /// be applied, on any host, at any core count. This does not just make the
    /// win "more likely" — see the assertion's own message for why it fails
    /// loudly, with a diagnosis, if the environment somehow still could not
    /// produce it, rather than silently passing.
    #[cfg(feature = "redb")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn dispatch_coalesces_concurrent_writes_to_one_graph() {
        let state = test_state();

        const N: u64 = 200;

        // Force genuine contention deterministically (see doc above): hold the
        // per-graph lock every landing path needs before any of the N writers
        // can be spawned.
        let hold = crate::server::mutation_batch::lock_graph("__commons__").await;

        let started = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let mut handles = Vec::with_capacity(N as usize);
        for i in 0..N {
            let st = state.clone();
            let started = started.clone();
            handles.push(tokio::spawn(async move {
                started.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                dispatch_on_heap(
                    &st,
                    request(i, "__commons__", None, add_node(&format!("n{i}"))),
                )
                .await
            }));
        }
        // Deterministic barrier, not a sleep: wait for every spawned task to
        // have actually begun running (bounded — this is "the scheduler ran
        // them," never a hang, since nothing here can block before the
        // `fetch_add`) before releasing the lock they are about to contend on.
        while started.load(std::sync::atomic::Ordering::SeqCst) < N {
            tokio::task::yield_now().await;
        }
        // A few more yields so tasks that have started get to actually reach
        // their first await point (enqueue, or block behind `hold`) while we
        // still hold the lock, maximizing genuine pile-up.
        for _ in 0..64 {
            tokio::task::yield_now().await;
        }
        drop(hold);

        for h in handles {
            assert_ok(&h.await.unwrap());
        }

        // INVARIANT — true on any host: every dispatched write lands exactly
        // once on the live path.
        let core = {
            let s = state.read().await;
            s.registry.get("__commons__").unwrap().core.clone()
        };
        assert_eq!(
            core.node_count() as u64,
            N,
            "all {N} dispatched writes land"
        );

        let (batches, ops) = {
            let s = state.read().await;
            let w = s.routed_write_coalescer.writer_for("__commons__");
            (w.stats().batches(), w.stats().ops())
        };
        // INVARIANT — true on any host, any core count: every admitted write is
        // accounted for by the ordered worker. If this regresses, the drain
        // stopped recording a committed job — an accounting bug, not an
        // environment issue.
        assert_eq!(ops, N, "stats account for every admitted dispatched write");
        // TIMING-DEPENDENT, but FORCED deterministic above: the coalescer
        // applied them in fewer lock acquisitions than ops. Because every
        // writer was made to queue behind `hold` before any could land, this
        // failing means the environment genuinely could not produce ANY
        // multi-op batch even under forced contention (`CoalescerConfig::
        // auto`'s `max_batch` floor is 16, so a single successful batch is
        // enough to satisfy this) — i.e. a real regression in the coalescer's
        // batching, not a machine too small to exercise it.
        assert!(
            batches < ops,
            "coalescer should genuinely batch: {ops} ops landed in {batches} \
             lock_graph acquisitions (expected < {ops}) — batches == ops here \
             would mean NO batch of size > 1 formed even with every writer \
             forced to queue behind the SAME held lock before any could run, \
             which points at a real regression in the coalescer, not host \
             core count (queue_capacity/max_batch are auto-sized but floored \
             well above 1 on any host)",
        );
    }

    /// Regression test for the L18 rewrite (2026-08-11): a coalescable
    /// structural write's ENTIRE prepare→durable-commit→RAM-publish sequence
    /// and a Transaction Commit's ENTIRE validate→durable-commit→RAM-publish
    /// sequence contend for the IDENTICAL per-graph `mutation_batch::lock_graph`
    /// lane, so NEITHER can make any progress — not even reach its own durable
    /// commit — while the other holds it.
    ///
    /// This directly targets the interleaving a first (rejected) fix attempt
    /// permitted: releasing the coalescable write's lock right after its
    /// durable commit but before its RAM publish would let a concurrent
    /// Transaction Commit acquire the lock in THAT gap and validate → durably
    /// commit → RAM-publish entirely ahead of the still-pending coalesced
    /// write — producing a RAM apply order that diverges from the durable
    /// commit order (the earlier caller's write landing in `core` AFTER the
    /// later one, even though it committed durably first). Proven here by
    /// holding `lock_graph` EXTERNALLY (a deterministic probe, mirroring
    /// `mutation_batch::tests::work_item_commit_waits_for_shared_graph_mutation_lane`,
    /// not a timing-dependent race) and showing neither op's `JoinHandle`
    /// finishes while it is held, then that BOTH land correctly once released.
    ///
    /// `dispatch::ApplyChangeEnvelope` (`server/dispatch.rs` ~6023) acquires
    /// the exact same `lock_graph` function with no special-casing, so this
    /// same mutual-exclusion proof covers it transitively; a dedicated test
    /// would need to hand-construct a fully valid signed `ChangeEnvelope`
    /// (privacy attestation, digest-verified principal, …) with no existing
    /// test helper to build on across this codebase, and would not exercise a
    /// different code path than this test already does (both go through
    /// `mutation_batch::lock_graph`, unconditionally, with no per-caller
    /// carve-out).
    // This test drives `handlers::txn::seal_txn_recovery_plan`'s REAL
    // `#[cfg(all(feature = "redb", feature = "security"))]` arm (it configures
    // `crate::crypto::ENCRYPTION_KEY_ENV` and asserts a committed transaction),
    // so it needs the same two features that arm does. `crypto` itself is only
    // compiled behind `#[cfg(any(feature = "security", feature = "raft"))]`
    // (`src/lib.rs`), so referencing `crate::crypto::ENCRYPTION_KEY_ENV`
    // unconditionally broke every build without `security` — including the
    // slim `server`-only profile. `security` already implies `redb` (see its
    // feature definition in Cargo.toml), so gating on `security` alone covers
    // both preconditions. Pre-existing gap in the 2026-08-11 coalescer merge
    // (e38df149), not something the slim-build fix here introduced.
    #[cfg(feature = "security")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn coalesced_write_and_transaction_commit_share_one_graph_lane() {
        // Transaction commit fail-closed requires a configured recovery-plan
        // seal key (`handlers::txn::seal_txn_recovery_plan`); an unfiltered
        // full-suite run happens to have it set by the time this test runs
        // (some OTHER test in this binary sets it first, e.g.
        // `persistence::redb_backend::tests::cm_dir`'s `Once`), but that is an
        // execution-order accident, not something this test may rely on —
        // running it filtered/alone reliably reproduces "transaction
        // durability requires EPISTEMIC_GRAPH_ENCRYPTION_KEY to be
        // configured" without this. Set it the same way `cm_dir` does: once,
        // process-global, harmless for every other test since encryption is
        // transparent to a durable round-trip's assertions.
        //
        // Held for the WHOLE test (not just the key provisioning): a `RedbBackend`
        // resolves its cipher once at `open()`, so `EPISTEMIC_GRAPH_ENCRYPTION_KEY`
        // must stay stable for the rest of this test too — a concurrent `crypto::
        // tests::EnvGuard`-protected test transiently clearing/changing it mid-flight
        // would otherwise reproduce the "sealed framing"/"wrong key" failures. See
        // `crate::crypto::acquire_test_env_lock`'s doc for the full mechanism.
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        ensure_txn_recovery_key();

        let state = test_state();
        const GRAPH: &str = "coalesce-txn-race";
        assert_ok(
            &dispatch_on_heap(
                &state,
                request(
                    0,
                    GRAPH,
                    None,
                    Method::CreateGraph {
                        graph_name: GRAPH.to_string(),
                        graph_type: GraphType::Global,
                    },
                ),
            )
            .await,
        );

        // Seed + open/stage a txn BEFORE taking the probe: begin/stage never
        // touch `lock_graph` ("no lock is held during client think-time; it
        // begins only after Commit consumes the staged txn" — handlers::txn),
        // so doing this first keeps the probe focused on the commit step.
        assert_ok(&dispatch_on_heap(&state, request(1, GRAPH, None, add_node("seed"))).await);
        let txn = begin_txn(&state, 2, GRAPH).await;
        let staged = dispatch_on_heap(
            &state,
            request(
                3,
                GRAPH,
                None,
                Method::TxnAddNode {
                    txn_id: txn.clone(),
                    node_id: "txn-node".into(),
                    properties_msgpack: node_props(serde_json::json!({"v": 1})),
                    graph: None,
                },
            ),
        )
        .await;
        assert!(
            matches!(staged.result, Some(ResultPayload::Bool(true))),
            "stage: {:?}",
            staged.error
        );

        // Hold the SAME per-graph lane externally.
        let held = crate::server::mutation_batch::lock_graph(GRAPH).await;

        let st1 = state.clone();
        let coalesced = tokio::spawn(async move {
            dispatch_on_heap(&st1, request(4, GRAPH, None, add_node("coalesced-node"))).await
        });
        let st2 = state.clone();
        let committed = tokio::spawn(async move {
            dispatch_on_heap(
                &st2,
                request(
                    5,
                    GRAPH,
                    None,
                    Method::Commit {
                        txn_id: txn,
                        idempotency_key: None,
                    },
                ),
            )
            .await
        });

        // Neither can complete ANY part of its sequence while the lane is
        // held — including its own durable commit — so there is no window in
        // which one's durably-committed-but-unpublished state could ever be
        // exposed to the other.
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        assert!(
            !coalesced.is_finished(),
            "coalescable write must NOT complete while lock_graph is held externally"
        );
        assert!(
            !committed.is_finished(),
            "transaction commit must NOT complete while lock_graph is held externally"
        );

        drop(held);

        let coalesced_resp = tokio::time::timeout(std::time::Duration::from_secs(5), coalesced)
            .await
            .expect("coalesced write must complete promptly once the lane is free")
            .unwrap();
        let committed_resp = tokio::time::timeout(std::time::Duration::from_secs(5), committed)
            .await
            .expect("transaction commit must complete promptly once the lane is free")
            .unwrap();
        assert_ok(&coalesced_resp);
        assert!(
            matches!(committed_resp.result, Some(ResultPayload::Bool(true))),
            "commit: {:?}",
            committed_resp.error
        );

        let core = {
            let s = state.read().await;
            s.registry.get(GRAPH).unwrap().core.clone()
        };
        assert!(core.has_node("coalesced-node"), "coalesced write landed");
        assert!(core.has_node("txn-node"), "committed txn write landed");
    }

    /// Regression test (CDC pre-image staleness): concurrent coalescable
    /// writes to the SAME node, all queued onto the SAME per-graph
    /// `routed_write_coalescer` worker, must never let a later write's CDC
    /// "before" image miss an earlier write's already-applied effect. The
    /// worker's `flush_batch` runs every queued op's full
    /// `commit_mutation_body` sequence — CDC pre-image capture included — in a
    /// plain sequential `for` loop with no concurrent spawning (`run.await`
    /// inside the loop body), so ops touching the same graph can never
    /// interleave their CDC captures regardless of batch composition. Proven
    /// here structurally, not by timing: read the CDC feed back and verify
    /// each event's `before` exactly equals the immediately preceding event's
    /// `after` — the chain a stale read would break (an event whose `before`
    /// missed a concurrently-applied predecessor would show an older value
    /// than what that predecessor's `after` established).
    #[cfg(feature = "streaming")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn coalesced_writes_cdc_before_image_never_stale() {
        let state = test_state();
        const GRAPH: &str = "coalesce-cdc-chain";
        const K: u64 = 40;

        assert_ok(
            &dispatch_on_heap(
                &state,
                request(
                    0,
                    GRAPH,
                    None,
                    Method::CreateGraph {
                        graph_name: GRAPH.to_string(),
                        graph_type: GraphType::Global,
                    },
                ),
            )
            .await,
        );
        assert_ok(
            &dispatch_on_heap(&state, request(1, GRAPH, None, doc_node("chain", "Chain"))).await,
        );

        // K concurrent overwrites of the SAME node — every one a coalescable
        // AddNode (add_node upserts an existing id's properties), fired
        // together so they race to enqueue onto the same worker.
        let mut handles = Vec::with_capacity(K as usize);
        for i in 1..=K {
            let st = state.clone();
            handles.push(tokio::spawn(async move {
                dispatch_on_heap(
                    &st,
                    request(
                        100 + i,
                        GRAPH,
                        None,
                        Method::AddNode {
                            node_id: "chain".into(),
                            properties_msgpack: rmp_serde::to_vec_named(
                                &serde_json::json!({"type": "Chain", "v": i}),
                            )
                            .unwrap(),
                        },
                    ),
                )
                .await
            }));
        }
        for h in handles {
            assert_ok(&h.await.unwrap());
        }

        let r = dispatch_on_heap(
            &state,
            request(
                200,
                GRAPH,
                None,
                Method::CdcRead {
                    graph: GRAPH.into(),
                    from_seq: 0,
                    limit: 0,
                },
            ),
        )
        .await;
        let events: Vec<_> = cdc_events(&r)
            .into_iter()
            .filter(|e| e.node_id == "chain")
            .collect();
        assert_eq!(
            events.len(),
            (K + 1) as usize,
            "the seed AddNode + K overwrites must each produce exactly one event"
        );
        for w in events.windows(2) {
            assert_eq!(
                w[0].after, w[1].before,
                "event {}'s before-image must exactly equal event {}'s \
                 after-image (seq {} -> {}) — a mismatch means a CDC \
                 pre-image was captured before an earlier concurrent write's \
                 effect had landed in RAM",
                w[0].seq, w[1].seq, w[0].seq, w[1].seq,
            );
        }

        // The graph's live state must match the CHAIN's own last link, not
        // some other write that raced ahead of it undetected.
        let core = {
            let s = state.read().await;
            s.registry.get(GRAPH).unwrap().core.clone()
        };
        let live = core.get_node_properties("chain").unwrap();
        assert_eq!(
            live,
            events.last().unwrap().after,
            "live RAM state must match the CDC chain's final after-image"
        );
    }

    /// CAS exactly-once is preserved through the dispatch coalescer: concurrent
    /// claimers of one node via `CompareAndSetNodeFields` yield exactly one winner.
    #[cfg(feature = "redb")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn dispatch_cas_exactly_once_under_coalescing() {
        let state = test_state();

        // Seed the task node with owner=null.
        let seed = Method::AddNode {
            node_id: "task".into(),
            properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({"owner": null}))
                .unwrap(),
        };
        assert_ok(&dispatch_on_heap(&state, request(0, "__commons__", None, seed)).await);

        const C: u64 = 40;
        let mut handles = Vec::with_capacity(C as usize);
        for i in 0..C {
            let st = state.clone();
            handles.push(tokio::spawn(async move {
                let conditions_msgpack =
                    rmp_serde::to_vec_named(&serde_json::json!({"owner": null})).unwrap();
                // Distinct owner label prefix (not "w{i}") so this test's per-`i`
                // CAS payloads never collide with another test's literal
                // `CompareAndSetNodeFields` payload on the SAME `node_id`/graph name
                // under `mutation`'s process-global idempotency-replay cache
                // (CONCEPT:EG-P0-2) -- `CompareAndSetNodeFields` is policy-idempotent,
                // so an identical (graph_name, method) tuple from a DIFFERENT test
                // sharing this process would otherwise return THIS test's cached
                // response instead of really executing (see
                // `standalone_cas_still_works`, which used to collide with `w1`/`w2`
                // here before this rename).
                let updates_msgpack = rmp_serde::to_vec_named(
                    &serde_json::json!({"owner": format!("coalescing-claimer-{i}")}),
                )
                .unwrap();
                let m = Method::CompareAndSetNodeFields {
                    node_id: "task".into(),
                    conditions_msgpack,
                    updates_msgpack,
                };
                let resp = dispatch_on_heap(&st, request(100 + i, "__commons__", None, m)).await;
                matches!(resp.result, Some(ResultPayload::Bool(true)))
            }));
        }
        let mut winners = 0;
        for h in handles {
            if h.await.unwrap() {
                winners += 1;
            }
        }
        assert_eq!(winners, 1, "exactly one CAS claimer wins through dispatch");
    }

    // ── Multi-op OCC ACID transactions (CONCEPT:EG-KG.txn.multi-op-occ-acid) ───────────────

    /// Open a txn on `graph` and return its server-issued id.
    async fn begin_txn(state: &Arc<RwLock<ServerState>>, id: u64, graph: &str) -> String {
        begin_txn_iso(state, id, graph, None).await
    }

    /// Open a txn on `graph` with an explicit isolation hint (CONCEPT:EG-KG.txn.serializable-zero-cost) and
    /// return its server-issued id.
    async fn begin_txn_iso(
        state: &Arc<RwLock<ServerState>>,
        id: u64,
        graph: &str,
        isolation: Option<&str>,
    ) -> String {
        let resp = dispatch_on_heap(
            state,
            request(
                id,
                graph,
                None,
                Method::BeginTxn {
                    graph: None,
                    isolation: isolation.map(str::to_string),
                },
            ),
        )
        .await;
        match resp.result {
            Some(ResultPayload::String(txn_id)) => txn_id,
            other => panic!(
                "BeginTxn must return a txn id, got {other:?} (err={:?})",
                resp.error
            ),
        }
    }

    fn node_props(v: serde_json::Value) -> Vec<u8> {
        rmp_serde::to_vec_named(&v).unwrap()
    }

    /// (a) Happy path: begin → stage two nodes + one edge → commit → all present.
    #[cfg(feature = "redb")]
    #[tokio::test]
    async fn txn_commit_applies_staged_writes() {
        // Held for the whole test: this test's `Commit` reaches
        // `seal_txn_recovery_plan`, which fails closed without a configured
        // `EPISTEMIC_GRAPH_ENCRYPTION_KEY`. See `ensure_txn_recovery_key`'s doc.
        #[cfg(feature = "security")]
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        #[cfg(feature = "security")]
        ensure_txn_recovery_key();
        let state = test_state();
        let txn = begin_txn(&state, 1, "__commons__").await;

        for (i, nid) in ["a", "b"].iter().enumerate() {
            let r = dispatch_on_heap(
                &state,
                request(
                    10 + i as u64,
                    "__commons__",
                    None,
                    Method::TxnAddNode {
                        txn_id: txn.clone(),
                        node_id: nid.to_string(),
                        properties_msgpack: node_props(serde_json::json!({"type": "Doc"})),
                        graph: None,
                    },
                ),
            )
            .await;
            assert!(
                matches!(r.result, Some(ResultPayload::Bool(true))),
                "stage node {nid}"
            );
        }
        let r = dispatch_on_heap(
            &state,
            request(
                20,
                "__commons__",
                None,
                Method::TxnAddEdge {
                    txn_id: txn.clone(),
                    source_id: "a".into(),
                    target_id: "b".into(),
                    properties_msgpack: node_props(serde_json::json!({})),
                    graph: None,
                },
            ),
        )
        .await;
        assert!(
            matches!(r.result, Some(ResultPayload::Bool(true))),
            "stage edge"
        );

        // Nothing applied until commit: the nodes are absent pre-commit.
        let core = {
            let s = state.read().await;
            s.registry.get("__commons__").unwrap().core.clone()
        };
        assert!(
            !core.has_node("a"),
            "staged node must NOT exist before commit"
        );

        // Commit → Bool(true), all present.
        let r = dispatch_on_heap(
            &state,
            request(
                30,
                "__commons__",
                None,
                Method::Commit {
                    txn_id: txn.clone(),
                    idempotency_key: None,
                },
            ),
        )
        .await;
        assert!(
            matches!(r.result, Some(ResultPayload::Bool(true))),
            "commit ok: {:?}",
            r.error
        );
        assert!(
            core.has_node("a") && core.has_node("b"),
            "committed nodes present"
        );
        assert!(core.has_edge("a", "b"), "committed edge present");
        // The txn id is consumed.
        let s = state.read().await;
        assert!(s.open_txns.get(&txn).is_none(), "committed txn removed");
    }

    /// B-9 (2026-08-13): a `Commit` with NO `idempotency_key` returns the SAME
    /// bare `Bool` wire shape it always has (the VERIFY contract's "without the
    /// key the behaviour is unchanged"). `txn_commit_applies_staged_writes`
    /// above already exercises this path end to end; this test pins the exact
    /// response SHAPE so a future change to the keyed path cannot silently leak
    /// into the unkeyed one.
    #[cfg(feature = "redb")]
    #[tokio::test]
    async fn commit_without_idempotency_key_returns_bare_bool() {
        #[cfg(feature = "security")]
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        #[cfg(feature = "security")]
        ensure_txn_recovery_key();
        let state = test_state();
        let txn = begin_txn(&state, 1, "__commons__").await;
        let r = dispatch_on_heap(
            &state,
            request(
                10,
                "__commons__",
                None,
                Method::TxnAddNode {
                    txn_id: txn.clone(),
                    node_id: "unkeyed".to_string(),
                    properties_msgpack: node_props(serde_json::json!({"type": "Doc"})),
                    graph: None,
                },
            ),
        )
        .await;
        assert!(matches!(r.result, Some(ResultPayload::Bool(true))));
        let r = dispatch_on_heap(
            &state,
            request(
                11,
                "__commons__",
                None,
                Method::Commit {
                    txn_id: txn,
                    idempotency_key: None,
                },
            ),
        )
        .await;
        assert!(
            matches!(r.result, Some(ResultPayload::Bool(true))),
            "no idempotency_key -> bare Bool, unchanged wire shape: {:?}",
            r.result
        );
    }

    /// B-9 (2026-08-13): the gap this closes -- a caller loses track of its
    /// `txn_id` (e.g. `BeginTxn`'s own response never arrived, or the process
    /// holding it died) and must `BeginTxn` + re-stage from scratch to retry.
    /// Without a caller idempotency key, a fresh `txn_id` is indistinguishable
    /// from a genuinely new transaction, so the SAME logical write could be
    /// applied twice. Proves the fix directly: two INDEPENDENTLY staged
    /// transactions (fresh `txn_id`s, both begun before either commits so they
    /// share one OCC `begin_version` and therefore stage byte-identical
    /// recovery plans) that reuse ONE caller `idempotency_key` at `Commit`
    /// apply exactly once -- the second call reports `replayed: true` and the
    /// graph's OCC version does not advance a second time.
    #[cfg(feature = "redb")]
    #[tokio::test]
    async fn commit_with_same_idempotency_key_applies_once_and_reports_replay() {
        #[cfg(feature = "security")]
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        #[cfg(feature = "security")]
        ensure_txn_recovery_key();
        let state = test_state();
        let key = "b9-caller-idempotency-key";

        // Both txns begin BEFORE either commits, so both see the SAME OCC
        // begin_version -- the realistic "lost track, must re-stage" shape,
        // where the retry reconstructs the identical intended write.
        let txn_a = begin_txn(&state, 1, "__commons__").await;
        let txn_b = begin_txn(&state, 2, "__commons__").await;
        for (id, txn) in [(10u64, &txn_a), (20u64, &txn_b)] {
            let r = dispatch_on_heap(
                &state,
                request(
                    id,
                    "__commons__",
                    None,
                    Method::TxnAddNode {
                        txn_id: txn.clone(),
                        node_id: "kept".to_string(),
                        properties_msgpack: node_props(serde_json::json!({"type": "Doc"})),
                        graph: None,
                    },
                ),
            )
            .await;
            assert!(
                matches!(r.result, Some(ResultPayload::Bool(true))),
                "stage {txn}"
            );
        }

        let core = {
            let s = state.read().await;
            s.registry.get("__commons__").unwrap().core.clone()
        };
        let v_before = core.version();

        // First attempt (txn_a): applies fresh.
        let r = dispatch_on_heap(
            &state,
            request(
                11,
                "__commons__",
                None,
                Method::Commit {
                    txn_id: txn_a,
                    idempotency_key: Some(key.to_string()),
                },
            ),
        )
        .await;
        let first = r
            .result
            .clone()
            .unwrap_or_else(|| panic!("first commit must succeed: {:?}", r.error));
        let ResultPayload::Json(first_value) = &first else {
            panic!("a keyed Commit must return Json {{committed, replayed}}, got {first:?}");
        };
        assert_eq!(first_value["committed"], true, "{first_value:?}");
        assert_eq!(
            first_value["replayed"], false,
            "the FIRST attempt under this key must NOT be reported as a replay: {first_value:?}"
        );
        assert!(core.has_node("kept"), "first attempt's write landed");
        let v_after_first = core.version();
        assert!(
            v_after_first > v_before,
            "the first attempt must advance the graph's OCC version"
        );

        // Second attempt (txn_b): a DIFFERENT txn_id, but the SAME caller key
        // and byte-identical staged content -- this is the retry B-9 makes safe.
        let r = dispatch_on_heap(
            &state,
            request(
                21,
                "__commons__",
                None,
                Method::Commit {
                    txn_id: txn_b,
                    idempotency_key: Some(key.to_string()),
                },
            ),
        )
        .await;
        let second = r
            .result
            .unwrap_or_else(|| panic!("replayed commit must still succeed: {:?}", r.error));
        let ResultPayload::Json(second_value) = &second else {
            panic!("a keyed Commit must return Json {{committed, replayed}}, got {second:?}");
        };
        // The CACHED outcome (`committed`) must match the original attempt's --
        // this is the "same cached result" proof. `replayed` is EXPECTED to
        // differ (false on the original, true on the replay); that flag IS the
        // signal B-9 adds, so it is asserted separately, not folded into an
        // object-level equality that would spuriously fail on the very field
        // under test.
        assert_eq!(
            second_value["committed"], first_value["committed"],
            "a replay must report the SAME cached `committed` outcome as the original commit"
        );
        assert_eq!(
            second_value["replayed"], true,
            "B-9: the SECOND attempt under the SAME key must be reported as a replay: {second_value:?}"
        );
        let v_after_second = core.version();
        assert_eq!(
            v_after_second, v_after_first,
            "B-9: a replayed Commit must NOT re-apply the write-set -- the graph's \
             OCC version must not advance a second time"
        );
    }

    /// (b) Rollback: begin → stage → rollback → graph unchanged, nothing persisted.
    #[tokio::test]
    async fn txn_rollback_applies_nothing() {
        let state = test_state();
        let core = {
            let s = state.read().await;
            s.registry.get("__commons__").unwrap().core.clone()
        };
        let v0 = core.version();
        let txn = begin_txn(&state, 1, "__commons__").await;
        let r = dispatch_on_heap(
            &state,
            request(
                10,
                "__commons__",
                None,
                Method::TxnAddNode {
                    txn_id: txn.clone(),
                    node_id: "ghost".into(),
                    properties_msgpack: node_props(serde_json::json!({})),
                    graph: None,
                },
            ),
        )
        .await;
        assert!(matches!(r.result, Some(ResultPayload::Bool(true))));

        let r = dispatch_on_heap(
            &state,
            request(
                20,
                "__commons__",
                None,
                Method::Rollback {
                    txn_id: txn.clone(),
                },
            ),
        )
        .await;
        assert!(
            matches!(r.result, Some(ResultPayload::Bool(true))),
            "rollback ok"
        );
        assert!(!core.has_node("ghost"), "rolled-back node must be absent");
        assert_eq!(
            core.version(),
            v0,
            "rollback bumps no version (nothing applied)"
        );
        assert_eq!(core.node_count(), 0, "graph unchanged after rollback");
    }

    /// (c) OCC conflict: two txns read-modify the SAME node; the first commits, the
    /// second's commit returns Bool(false) (a true rollback — nothing applied).
    #[cfg(feature = "redb")]
    #[tokio::test]
    async fn txn_occ_conflict_second_commit_fails() {
        // See `txn_commit_applies_staged_writes` above: held for the whole test.
        #[cfg(feature = "security")]
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        #[cfg(feature = "security")]
        ensure_txn_recovery_key();
        let state = test_state();
        // Seed the contended node.
        assert_ok(
            &dispatch_on_heap(
                &state,
                request(
                    1,
                    "__commons__",
                    None,
                    Method::AddNode {
                        node_id: "k".into(),
                        properties_msgpack: node_props(serde_json::json!({"v": 0})),
                    },
                ),
            )
            .await,
        );

        // Both transactions open and stage a CAS-like overwrite of node "k"; both
        // fingerprint "k" at its CURRENT (v=0) state.
        let t1 = begin_txn(&state, 2, "__commons__").await;
        let t2 = begin_txn(&state, 3, "__commons__").await;
        for (rid, txn, val) in [(10u64, &t1, 1), (11, &t2, 2)] {
            let r = dispatch_on_heap(
                &state,
                request(
                    rid,
                    "__commons__",
                    None,
                    Method::TxnAddNode {
                        txn_id: txn.clone(),
                        node_id: "k".into(),
                        properties_msgpack: node_props(serde_json::json!({"v": val})),
                        graph: None,
                    },
                ),
            )
            .await;
            assert!(matches!(r.result, Some(ResultPayload::Bool(true))));
        }

        // First commit wins.
        let r1 = dispatch_on_heap(
            &state,
            request(
                20,
                "__commons__",
                None,
                Method::Commit {
                    txn_id: t1.clone(),
                    idempotency_key: None,
                },
            ),
        )
        .await;
        assert!(
            matches!(r1.result, Some(ResultPayload::Bool(true))),
            "t1 commits"
        );

        // Second commit conflicts (node "k" changed since t2 began) → Bool(false).
        let r2 = dispatch_on_heap(
            &state,
            request(
                21,
                "__commons__",
                None,
                Method::Commit {
                    txn_id: t2.clone(),
                    idempotency_key: None,
                },
            ),
        )
        .await;
        assert!(
            matches!(r2.result, Some(ResultPayload::Bool(false))),
            "t2 must conflict, got {:?} err={:?}",
            r2.result,
            r2.error
        );

        // t1's value won; t2 applied nothing.
        let core = {
            let s = state.read().await;
            s.registry.get("__commons__").unwrap().core.clone()
        };
        let props: serde_json::Value =
            rmp_serde::from_slice(&core.get_node_properties("k").unwrap()).unwrap();
        assert_eq!(
            props["v"], 1,
            "first committer's write survives, not the conflicted one"
        );
    }

    /// (d) Abandoned txn auto-rolls-back after the TTL (drive the sweep directly).
    #[tokio::test]
    async fn txn_ttl_sweep_reclaims_idle() {
        use crate::server::txn::{now_ms, sweep_expired_txns};
        let state = test_state();
        let txn = begin_txn(&state, 1, "__commons__").await;
        assert!(state.read().await.open_txns.get(&txn).is_some());

        // A sweep with a future "now" (TTL elapsed) reclaims the idle txn.
        let future = now_ms() + 10 * 60 * 1000; // 10 min later
        let reclaimed = sweep_expired_txns(&state, 300, future);
        assert_eq!(reclaimed, 1, "idle txn past TTL is swept");
        assert!(
            state.read().await.open_txns.get(&txn).is_none(),
            "swept txn removed"
        );
        // Committing a swept txn is now an unknown-id error (true rollback occurred).
        let r = dispatch_on_heap(
            &state,
            request(
                2,
                "__commons__",
                None,
                Method::Commit {
                    txn_id: txn,
                    idempotency_key: None,
                },
            ),
        )
        .await;
        assert!(r.error.is_some(), "committing a swept txn errors");
    }

    /// (e) Regression: standalone single-op CAS still works (degenerate 1-op
    /// auto-commit) and is untouched by the txn machinery.
    #[cfg(feature = "redb")]
    #[tokio::test]
    async fn standalone_cas_still_works() {
        let state = test_state();
        assert_ok(
            &dispatch_on_heap(
                &state,
                request(
                    1,
                    "__commons__",
                    None,
                    Method::AddNode {
                        node_id: "task".into(),
                        properties_msgpack: node_props(serde_json::json!({"owner": null})),
                    },
                ),
            )
            .await,
        );
        let r = dispatch_on_heap(
            &state,
            request(
                2,
                "__commons__",
                None,
                Method::CompareAndSetNodeFields {
                    node_id: "task".into(),
                    conditions_msgpack: node_props(serde_json::json!({"owner": null})),
                    updates_msgpack: node_props(serde_json::json!({"owner": "w1"})),
                },
            ),
        )
        .await;
        assert!(
            matches!(r.result, Some(ResultPayload::Bool(true))),
            "CAS claims"
        );
        // A second CAS with the same condition fails (already claimed).
        let r = dispatch_on_heap(
            &state,
            request(
                3,
                "__commons__",
                None,
                Method::CompareAndSetNodeFields {
                    node_id: "task".into(),
                    conditions_msgpack: node_props(serde_json::json!({"owner": null})),
                    updates_msgpack: node_props(serde_json::json!({"owner": "w2"})),
                },
            ),
        )
        .await;
        assert!(
            matches!(r.result, Some(ResultPayload::Bool(false))),
            "second CAS rejected"
        );
    }

    // ── Transaction isolation levels (CONCEPT:EG-KG.txn.serializable-zero-cost — M6b) ────────────

    /// Commit `txn` and return the Bool payload (true=committed, false=conflict).
    async fn commit_bool(
        state: &Arc<RwLock<ServerState>>,
        id: u64,
        graph: &str,
        txn: &str,
    ) -> bool {
        let r = dispatch_on_heap(
            state,
            request(
                id,
                graph,
                None,
                Method::Commit {
                    txn_id: txn.to_string(),
                    idempotency_key: None,
                },
            ),
        )
        .await;
        match r.result {
            Some(ResultPayload::Bool(b)) => b,
            other => panic!("Commit must return Bool, got {other:?} (err={:?})", r.error),
        }
    }

    /// Stage an AddNode into `txn` (used by the phantom scenarios).
    async fn stage_add(
        state: &Arc<RwLock<ServerState>>,
        id: u64,
        graph: &str,
        txn: &str,
        node_id: &str,
        props: serde_json::Value,
    ) {
        let r = dispatch_on_heap(
            state,
            request(
                id,
                graph,
                None,
                Method::TxnAddNode {
                    txn_id: txn.to_string(),
                    node_id: node_id.to_string(),
                    properties_msgpack: node_props(props),
                    graph: None,
                },
            ),
        )
        .await;
        assert!(matches!(r.result, Some(ResultPayload::Bool(true))), "stage");
    }

    /// (a-serializable) Phantom: under `serializable:label=Doc`, txn A declares a
    /// label-scan read-set, txn B inserts a matching `Doc` and commits, then A's
    /// commit returns Bool(false) — the phantom is rejected.
    #[cfg(feature = "redb")]
    #[tokio::test]
    async fn txn_serializable_rejects_phantom() {
        // See `txn_commit_applies_staged_writes` above: held for the whole test.
        #[cfg(feature = "security")]
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        #[cfg(feature = "security")]
        ensure_txn_recovery_key();
        let state = test_state();
        // Seed one Doc so the label set is non-empty at begin (not required, but
        // mirrors a real range read).
        assert_ok(
            &dispatch_on_heap(
                &state,
                request(
                    1,
                    "__commons__",
                    None,
                    Method::AddNode {
                        node_id: "d0".into(),
                        properties_msgpack: node_props(serde_json::json!({"type": "Doc"})),
                    },
                ),
            )
            .await,
        );

        // Txn A reads the `Doc` label set (declared via the isolation hint) and stages
        // an unrelated write so it has something to commit.
        let a = begin_txn_iso(&state, 2, "__commons__", Some("serializable:label=Doc")).await;
        stage_add(
            &state,
            3,
            "__commons__",
            &a,
            "a_marker",
            serde_json::json!({"type": "Marker"}),
        )
        .await;

        // Txn B inserts a NEW matching Doc and commits — a phantom for A's read-set.
        let b = begin_txn_iso(&state, 4, "__commons__", Some("snapshot")).await;
        stage_add(
            &state,
            5,
            "__commons__",
            &b,
            "d_phantom",
            serde_json::json!({"type": "Doc"}),
        )
        .await;
        assert!(
            commit_bool(&state, 6, "__commons__", &b).await,
            "B (phantom inserter) commits"
        );

        // A's commit must now conflict: the Doc label set changed under it.
        assert!(
            !commit_bool(&state, 7, "__commons__", &a).await,
            "serializable A must reject the phantom (Bool(false))"
        );
        // A applied nothing — its unrelated marker is absent.
        let core = {
            let s = state.read().await;
            s.registry.get("__commons__").unwrap().core.clone()
        };
        assert!(!core.has_node("a_marker"), "conflicted A applied nothing");
    }

    /// (a-snapshot) The SAME phantom scenario under `snapshot` ALLOWS A to commit —
    /// proving the levels differ. A touches no node B touched, so the per-node OCC
    /// read-set sees no conflict and snapshot does not watch the label predicate.
    #[cfg(feature = "redb")]
    #[tokio::test]
    async fn txn_snapshot_allows_phantom() {
        // See `txn_commit_applies_staged_writes` above: held for the whole test.
        #[cfg(feature = "security")]
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        #[cfg(feature = "security")]
        ensure_txn_recovery_key();
        let state = test_state();
        assert_ok(
            &dispatch_on_heap(
                &state,
                request(
                    1,
                    "__commons__",
                    None,
                    Method::AddNode {
                        node_id: "d0".into(),
                        properties_msgpack: node_props(serde_json::json!({"type": "Doc"})),
                    },
                ),
            )
            .await,
        );

        // A under SNAPSHOT (the default). A label predicate is meaningless here and
        // omitted; A simply stages an unrelated write.
        let a = begin_txn_iso(&state, 2, "__commons__", Some("snapshot")).await;
        stage_add(
            &state,
            3,
            "__commons__",
            &a,
            "a_marker",
            serde_json::json!({"type": "Marker"}),
        )
        .await;

        // B inserts a matching Doc and commits (the phantom).
        let b = begin_txn(&state, 4, "__commons__").await;
        stage_add(
            &state,
            5,
            "__commons__",
            &b,
            "d_phantom",
            serde_json::json!({"type": "Doc"}),
        )
        .await;
        assert!(commit_bool(&state, 6, "__commons__", &b).await, "B commits");

        // Under snapshot, A commits successfully despite the phantom.
        assert!(
            commit_bool(&state, 7, "__commons__", &a).await,
            "snapshot A is allowed to commit through the phantom (Bool(true))"
        );
        let core = {
            let s = state.read().await;
            s.registry.get("__commons__").unwrap().core.clone()
        };
        assert!(core.has_node("a_marker"), "snapshot A applied its write");
    }

    /// A serializable txn whose predicate set is UNCHANGED still commits (the level
    /// rejects only real anomalies, not every concurrent write).
    #[cfg(feature = "redb")]
    #[tokio::test]
    async fn txn_serializable_commits_when_predicate_unchanged() {
        // See `txn_commit_applies_staged_writes` above: held for the whole test.
        #[cfg(feature = "security")]
        let _env_lock = crate::crypto::acquire_test_env_lock().await;
        #[cfg(feature = "security")]
        ensure_txn_recovery_key();
        let state = test_state();
        let a = begin_txn_iso(&state, 1, "__commons__", Some("serializable:label=Doc")).await;
        stage_add(
            &state,
            2,
            "__commons__",
            &a,
            "m1",
            serde_json::json!({"type": "Marker"}),
        )
        .await;
        // A concurrent commit that does NOT touch the Doc label set.
        let b = begin_txn(&state, 3, "__commons__").await;
        stage_add(
            &state,
            4,
            "__commons__",
            &b,
            "m2",
            serde_json::json!({"type": "Other"}),
        )
        .await;
        assert!(commit_bool(&state, 5, "__commons__", &b).await, "B commits");
        assert!(
            commit_bool(&state, 6, "__commons__", &a).await,
            "serializable A commits when its Doc predicate set is unchanged"
        );
    }

    /// (c) An unknown isolation value is rejected at BeginTxn (no txn opened).
    #[tokio::test]
    async fn txn_unknown_isolation_rejected() {
        let state = test_state();
        let resp = dispatch_on_heap(
            &state,
            request(
                1,
                "__commons__",
                None,
                Method::BeginTxn {
                    graph: None,
                    isolation: Some("read-committed".into()),
                },
            ),
        )
        .await;
        assert!(
            resp.error.is_some() && resp.result.is_none(),
            "unknown isolation must error, got ok={:?} err={:?}",
            resp.result,
            resp.error
        );
        assert!(
            resp.error.as_deref().unwrap_or("").contains("isolation"),
            "error should name the isolation problem: {:?}",
            resp.error
        );
        // No transaction was registered.
        assert_eq!(
            state.read().await.open_txns.len(),
            0,
            "rejected BeginTxn opens no txn"
        );
    }

    // ── Time-series (CONCEPT:AU-KG.retrieval.god-nodes-communities/211) round-trips through full dispatch ──
    #[cfg(feature = "tsdb")]
    const TS_NS: i64 = 1_000_000_000;

    #[cfg(feature = "tsdb")]
    fn ts_points(pts: &[(i64, Vec<f64>)]) -> Vec<u8> {
        rmp_serde::to_vec(&pts.to_vec()).unwrap()
    }

    #[cfg(feature = "tsdb")]
    #[tokio::test]
    async fn ts_append_then_range_via_dispatch() {
        let state = test_state();
        let pts: Vec<(i64, Vec<f64>)> = (0..10).map(|i| (i * TS_NS, vec![i as f64])).collect();
        let r = dispatch_on_heap(
            &state,
            request(
                1,
                "__commons__",
                None,
                Method::TsAppend {
                    series_id: "s".into(),
                    n_fields: 1,
                    bucket_ns: 100 * TS_NS as u64,
                    field_names: vec!["v".into()],
                    points_msgpack: ts_points(&pts),
                },
            ),
        )
        .await;
        assert!(
            matches!(r.result, Some(ResultPayload::Count(10))),
            "{:?}",
            r
        );

        let r = dispatch_on_heap(
            &state,
            request(
                2,
                "__commons__",
                None,
                Method::TsRange {
                    series_id: "s".into(),
                    from: 2 * TS_NS,
                    to: 5 * TS_NS,
                },
            ),
        )
        .await;
        let raw = match r.result {
            Some(ResultPayload::Raw(b)) => b,
            other => panic!("expected Raw, got {other:?}"),
        };
        let got: Vec<(i64, Vec<f64>)> = rmp_serde::from_slice(&raw).unwrap();
        assert_eq!(got.len(), 3);
        assert_eq!(got[0].0, 2 * TS_NS);
        assert_eq!(got[2].1[0], 4.0);
    }

    #[cfg(feature = "tsdb")]
    #[tokio::test]
    async fn ts_asof_window_gapfill_via_dispatch() {
        let state = test_state();
        let ticks: Vec<(i64, Vec<f64>)> = (0..20)
            .map(|i| (i * TS_NS, vec![100.0 + i as f64]))
            .collect();
        assert_ok(
            &dispatch_on_heap(
                &state,
                request(
                    1,
                    "__commons__",
                    None,
                    Method::TsAppend {
                        series_id: "px".into(),
                        n_fields: 1,
                        bucket_ns: 100 * TS_NS as u64,
                        field_names: vec!["px".into()],
                        points_msgpack: ts_points(&ticks),
                    },
                ),
            )
            .await,
        );

        // ASOF: out-of-order left events, results returned in caller order.
        let left_ts: Vec<i64> = vec![7 * TS_NS, 3 * TS_NS];
        let r = dispatch_on_heap(
            &state,
            request(
                2,
                "__commons__",
                None,
                Method::TsAsofJoin {
                    series_id: "px".into(),
                    left_ts_msgpack: rmp_serde::to_vec(&left_ts).unwrap(),
                    tolerance: -1,
                },
            ),
        )
        .await;
        let raw = match r.result {
            Some(ResultPayload::Raw(b)) => b,
            other => panic!("expected Raw, got {other:?}"),
        };
        let got: Vec<Option<f64>> = rmp_serde::from_slice(&raw).unwrap();
        assert_eq!(got, vec![Some(107.0), Some(103.0)]);

        // WINDOW: 10s mean buckets.
        let r = dispatch_on_heap(
            &state,
            request(
                3,
                "__commons__",
                None,
                Method::TsWindow {
                    series_id: "px".into(),
                    from: 0,
                    to: 20 * TS_NS,
                    width: 10 * TS_NS,
                    agg: "mean".into(),
                },
            ),
        )
        .await;
        let raw = match r.result {
            Some(ResultPayload::Raw(b)) => b,
            other => panic!("expected Raw, got {other:?}"),
        };
        let bars: Vec<(i64, f64, usize)> = rmp_serde::from_slice(&raw).unwrap();
        assert_eq!(bars.len(), 2);
        assert!((bars[0].1 - 104.5).abs() < 1e-9);

        // GAP-FILL on a 5s grid.
        let r = dispatch_on_heap(
            &state,
            request(
                4,
                "__commons__",
                None,
                Method::TsGapFill {
                    series_id: "px".into(),
                    from: 0,
                    to: 20 * TS_NS,
                    step: 5 * TS_NS,
                },
            ),
        )
        .await;
        let raw = match r.result {
            Some(ResultPayload::Raw(b)) => b,
            other => panic!("expected Raw, got {other:?}"),
        };
        let grid: Vec<(i64, f64, bool)> = rmp_serde::from_slice(&raw).unwrap();
        assert_eq!(grid.len(), 4);
    }

    // ── RDF/SPARQL Method round-trips through dispatch (CONCEPT:EG-KG.ontology.kg-native-rdf-sparql/218) ──

    /// Register a minimal but VALID SHACL shapes document (no constraints) on
    /// `graph` -- satisfies the mandatory rdf-update-guard integrity-policy
    /// requirement (CONCEPT:EG-KG.ontology.rdf-update-guard): once the `shacl` feature is compiled in,
    /// `icv_guard::CoreIcvGuard::check_graph` fails EVERY `AddTriples`/
    /// `RemoveTriples` on a graph closed ("no integrity policy is registered")
    /// until that graph has one registered via `IcvConfigure`, even a permissive
    /// empty one. Real tests of the guard's ENFORCEMENT behavior configure their
    /// own non-empty shapes; this is for the many round-trip tests that exist to
    /// prove something else and just need the write to be let through.
    ///
    /// `IcvConfigure` is policy-idempotent (CONCEPT:EG-P0-2 `MutationPlan::idempotent`),
    /// and its replay-dedup cache (`mutation::idempotency_store`) is a PROCESS-GLOBAL
    /// `OnceLock`, keyed only by `(graph_name, method content)` — it has no notion of
    /// "which `test_state()`" issued the call. Every `#[tokio::test]` in this module
    /// gets its own fresh, independent `ServerState`/`GraphCore`, but they all run in
    /// the SAME process and therefore share ONE idempotency cache: a second test
    /// calling this helper with byte-identical `graph`/shapes content would replay the
    /// FIRST test's cached response instead of actually registering a policy on the
    /// second test's own core — an order-dependent false pass/fail entirely internal
    /// to this test module, not a product defect. A per-call nonce embedded as a
    /// harmless Turtle comment keeps every call's cache key unique (same convention
    /// `test_owl_reason_distributed_two_graphs` already uses per-graph, below).
    #[cfg(all(feature = "rdf", feature = "shacl"))]
    async fn configure_icv_enforce(state: &Arc<RwLock<ServerState>>, req_id: u64, graph: &str) {
        static NONCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let nonce = NONCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let r = dispatch_on_heap(
            state,
            request(
                req_id,
                graph,
                None,
                Method::IcvConfigure {
                    graph: None,
                    mode: "enforce".into(),
                    shapes: format!(
                        "@prefix sh: <http://www.w3.org/ns/shacl#> .\n# configure_icv_enforce:{nonce}"
                    ),
                },
            ),
        )
        .await;
        assert_ok(&r);
    }

    /// AddTriples → GetRdf round-trips through the dispatch chain: Turtle in, the
    /// graph populated, N-Triples out reparses to the same triple set (xsd + @lang).
    #[cfg(feature = "rdf")]
    #[tokio::test]
    async fn test_add_triples_then_get_rdf_round_trips() {
        let state = test_state();
        #[cfg(feature = "shacl")]
        configure_icv_enforce(&state, 0, "__commons__").await;
        let ttl = r#"
@prefix ex: <http://example.org/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
ex:alice a ex:Person ; ex:name "Alice" ; ex:age "30"^^xsd:integer ; ex:knows ex:bob .
ex:bob   a ex:Person ; ex:name "Bob"@en .
"#;
        let r = dispatch_on_heap(
            &state,
            request(
                1,
                "__commons__",
                None,
                Method::AddTriples {
                    turtle: ttl.into(),
                    ntriples: String::new(),
                },
            ),
        )
        .await;
        assert_ok(&r);
        let report: eg_rdf::mapping::LoadReport = match r.result {
            Some(ResultPayload::Raw(b)) => rmp_serde::from_slice(&b).unwrap(),
            other => panic!("expected Raw(LoadReport), got {other:?}"),
        };
        assert_eq!(report.triples, 6);
        assert_eq!(report.multivalue, 0);

        let r2 = dispatch_on_heap(&state, request(2, "__commons__", None, Method::GetRdf)).await;
        assert_ok(&r2);
        let nt: String = match r2.result {
            Some(ResultPayload::Raw(b)) => rmp_serde::from_slice(&b).unwrap(),
            other => panic!("expected Raw(String), got {other:?}"),
        };
        let parsed_in = eg_rdf::mapping::parse_turtle(ttl).unwrap();
        let parsed_out = eg_rdf::mapping::parse_ntriples(&nt).unwrap();
        assert_eq!(
            eg_rdf::mapping::triple_set_key(&parsed_in),
            eg_rdf::mapping::triple_set_key(&parsed_out),
            "AddTriples→GetRdf must round-trip the triple set"
        );
    }

    /// Sparql Method round-trips through dispatch: a BGP+FILTER over a loaded graph.
    #[cfg(feature = "sparql")]
    #[tokio::test]
    async fn test_sparql_method_round_trips() {
        let state = test_state();
        #[cfg(feature = "shacl")]
        configure_icv_enforce(&state, 0, "__commons__").await;
        let ttl = r#"
@prefix ex: <http://example.org/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
ex:alice a ex:Person ; ex:name "Alice" ; ex:age "30"^^xsd:integer ; ex:knows ex:bob .
ex:bob   a ex:Person ; ex:name "Bob"   ; ex:age "25"^^xsd:integer .
ex:carol a ex:Person ; ex:name "Carol" ; ex:age "40"^^xsd:integer ; ex:knows ex:alice .
"#;
        assert_ok(
            &dispatch_on_heap(
                &state,
                request(
                    1,
                    "__commons__",
                    None,
                    Method::AddTriples {
                        turtle: ttl.into(),
                        ntriples: String::new(),
                    },
                ),
            )
            .await,
        );
        let r = dispatch_on_heap(
            &state,
            request(
                2,
                "__commons__",
                None,
                Method::Sparql {
                    query: r#"
                        PREFIX ex: <http://example.org/>
                        SELECT ?name WHERE {
                          ?p a ex:Person . ?p ex:name ?name . ?p ex:age ?age .
                          ?p ex:knows ?o . FILTER (?age > 28)
                        }"#
                    .into(),
                    base_iri: String::new(),
                    type_convention: String::new(),
                },
            ),
        )
        .await;
        assert_ok(&r);
        let res: crate::protocol::SparqlResult = match r.result {
            Some(ResultPayload::Raw(b)) => rmp_serde::from_slice(&b).unwrap(),
            other => panic!("expected Raw(SparqlResult), got {other:?}"),
        };
        let name_idx = res.vars.iter().position(|v| v == "name").unwrap();
        let mut names: Vec<String> = res
            .rows
            .iter()
            .filter_map(|row| row[name_idx].clone())
            .collect();
        names.sort();
        assert_eq!(names, vec!["Alice".to_string(), "Carol".to_string()]);
    }

    /// OwlReason Method round-trips through dispatch: an EL existential-restriction
    /// subsumption + an inferred instance membership the property-graph stored no
    /// explicit type edge for, plus a consistency verdict (CONCEPT:EG-KG.ontology.incremental-materialization).
    #[cfg(feature = "owl")]
    #[tokio::test]
    async fn test_owl_reason_method_round_trips() {
        let state = test_state();
        #[cfg(feature = "shacl")]
        configure_icv_enforce(&state, 0, "__commons__").await;
        // TBox + one individual, loaded as RDF; HumanHeart ⊑ HumanComponent is derived
        // through ∃partOf.Body on the LHS — RL cannot reach it.
        let ttl = r#"
@prefix ex:  <http://example.org/> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdfs:<http://www.w3.org/2000/01/rdf-schema#> .
ex:Heart rdfs:subClassOf [ a owl:Restriction ; owl:onProperty ex:partOf ; owl:someValuesFrom ex:Body ] .
[ a owl:Restriction ; owl:onProperty ex:partOf ; owl:someValuesFrom ex:Body ] rdfs:subClassOf ex:HumanComponent .
ex:HumanHeart rdfs:subClassOf ex:Heart .
ex:myHeart a ex:HumanHeart .
"#;
        assert_ok(
            &dispatch_on_heap(
                &state,
                request(
                    1,
                    "__commons__",
                    None,
                    Method::AddTriples {
                        turtle: ttl.into(),
                        ntriples: String::new(),
                    },
                ),
            )
            .await,
        );
        let r = dispatch_on_heap(
            &state,
            request(
                2,
                "__commons__",
                None,
                Method::OwlReason {
                    ontology: String::new(),
                    target_class: "http://example.org/HumanComponent".into(),
                    class_base: String::new(),
                    min_confidence: 0.0,
                },
            ),
        )
        .await;
        assert_ok(&r);
        let res: crate::protocol::OwlReasonResult = match r.result {
            Some(ResultPayload::Raw(b)) => rmp_serde::from_slice(&b).unwrap(),
            other => panic!("expected Raw(OwlReasonResult), got {other:?}"),
        };
        assert!(res.consistent, "ontology is consistent");
        // Confidence is aligned + present (hard ontology ⇒ all 1.0).
        assert_eq!(res.subclasses.len(), res.subclass_conf.len());
        assert_eq!(res.instances.len(), res.instance_conf.len());
        // The EL-derived subsumption is in the hierarchy.
        assert!(res.subclasses.contains(&(
            "<http://example.org/HumanHeart>".into(),
            "<http://example.org/HumanComponent>".into()
        )));
        // myHeart is an INFERRED HumanComponent (no explicit type edge for it).
        assert!(res.instances.contains(&(
            "<http://example.org/myHeart>".into(),
            "<http://example.org/HumanComponent>".into()
        )));
    }

    /// BUG-281 regression: `Method::OwlReason` with an EMPTY `target_class` (its own
    /// documented "all classes" contract) over bare-string-typed nodes (`AddNode`'s
    /// `type` property, the property-graph's normal convention — no `AddTriples`/RDF
    /// TBox at all) must NOT error, and must classify every asserted type once an
    /// explicit `class_base` is supplied. Before the fix this always raised "OwlReason
    /// requires an absolute target class with a current class namespace", because
    /// `class_base` was derived ONLY from `target_class` — so a caller with no filter
    /// had no way to also supply a bridging namespace.
    #[cfg(feature = "owl")]
    #[tokio::test]
    async fn test_owl_reason_empty_target_class_uses_explicit_class_base() {
        let state = test_state();
        #[cfg(feature = "shacl")]
        configure_icv_enforce(&state, 0, "__commons__").await;
        for (id, name) in [("alice", "Alice"), ("bob", "Bob")] {
            assert_ok(
                &dispatch_on_heap(
                    &state,
                    request(
                        1,
                        "__commons__",
                        None,
                        Method::AddNode {
                            node_id: id.into(),
                            properties_msgpack: rmp_serde::to_vec(&serde_json::json!({
                                "type": "Agent",
                                "name": name,
                            }))
                            .unwrap(),
                        },
                    ),
                )
                .await,
            );
        }
        let r = dispatch_on_heap(
            &state,
            request(
                2,
                "__commons__",
                None,
                Method::OwlReason {
                    ontology: String::new(),
                    target_class: String::new(),
                    class_base: "http://example.org/ns#".into(),
                    min_confidence: 0.0,
                },
            ),
        )
        .await;
        assert_ok(&r);
        let res: crate::protocol::OwlReasonResult = match r.result {
            Some(ResultPayload::Raw(b)) => rmp_serde::from_slice(&b).unwrap(),
            other => panic!("expected Raw(OwlReasonResult), got {other:?}"),
        };
        assert!(
            res.consistent,
            "no ontology declared -> trivially consistent"
        );
        // `class_base` bridges the CLASS (the bare `type` value `"Agent"` ->
        // `<http://example.org/ns#Agent>`); the INSTANCE stays the graph's own node id
        // (`alice`), which is not part of the class vocabulary and is never rewritten.
        assert!(
            res.instances
                .contains(&("alice".into(), "<http://example.org/ns#Agent>".into())),
            "alice must be classified via the explicit class_base with an empty \
             target_class; instances={:?}",
            res.instances
        );
        assert!(
            res.instances
                .contains(&("bob".into(), "<http://example.org/ns#Agent>".into())),
            "bob must be classified via the explicit class_base with an empty \
             target_class; instances={:?}",
            res.instances
        );

        // BUG-281, the other half: BOTH fields empty is also legitimate ("reason over
        // everything using my graph's own local vocabulary"). The bare `type` then
        // classifies under its own bare label — identity mode, mirroring
        // `sparql::Projection::raw()` — rather than erroring or fabricating a namespace.
        let r2 = dispatch_on_heap(
            &state,
            request(
                3,
                "__commons__",
                None,
                Method::OwlReason {
                    ontology: String::new(),
                    target_class: String::new(),
                    class_base: String::new(),
                    min_confidence: 0.0,
                },
            ),
        )
        .await;
        assert_ok(&r2);
        let res2: crate::protocol::OwlReasonResult = match r2.result {
            Some(ResultPayload::Raw(b)) => rmp_serde::from_slice(&b).unwrap(),
            other => panic!("expected Raw(OwlReasonResult), got {other:?}"),
        };
        assert!(
            res2.instances.contains(&("alice".into(), "Agent".into())),
            "with NO class_base the bare type stays bare (identity mode); \
             instances={:?}",
            res2.instances
        );
    }

    /// OwlExplain Method round-trips through dispatch (CONCEPT:EG-KG.ontology.owl-proof-tree-explanation): the
    /// classic transitive-subClassOf chain `Dog ⊑ Animal ⊑ LivingThing` reconstructs a
    /// FULL proof tree down to the asserted leaf, over the wire exactly like the
    /// in-process `eg_rdf::owl` unit test proves.
    #[cfg(feature = "owl")]
    #[tokio::test]
    async fn test_owl_explain_method_round_trips() {
        let state = test_state();
        #[cfg(feature = "shacl")]
        configure_icv_enforce(&state, 0, "__commons__").await;
        let ttl = r#"
@prefix ex:  <http://example.org/> .
@prefix rdfs:<http://www.w3.org/2000/01/rdf-schema#> .
ex:Dog rdfs:subClassOf ex:Animal .
ex:Animal rdfs:subClassOf ex:LivingThing .
"#;
        assert_ok(
            &dispatch_on_heap(
                &state,
                request(
                    1,
                    "__commons__",
                    None,
                    Method::AddTriples {
                        turtle: ttl.into(),
                        ntriples: String::new(),
                    },
                ),
            )
            .await,
        );
        let r = dispatch_on_heap(
            &state,
            request(
                2,
                "__commons__",
                None,
                Method::OwlExplain {
                    ontology: String::new(),
                    sub: "http://example.org/Dog".into(),
                    sup: "http://example.org/LivingThing".into(),
                },
            ),
        )
        .await;
        assert_ok(&r);
        let res: crate::protocol::OwlExplainResult = match r.result {
            Some(ResultPayload::Raw(b)) => rmp_serde::from_slice(&b).unwrap(),
            other => panic!("expected Raw(OwlExplainResult), got {other:?}"),
        };
        assert!(res.found, "Dog ⊑ LivingThing must be entailed");
        assert!(res.consistent);
        let tree = res.tree.expect("found ⇒ a tree");
        assert_eq!(tree.sub, "<http://example.org/Dog>");
        assert_eq!(tree.sup, "<http://example.org/LivingThing>");
        assert_ne!(tree.rule, "asserted", "the root is derived");
        assert_eq!(tree.premises.len(), 1);
        let mid = &tree.premises[0];
        assert_eq!(mid.sub, "<http://example.org/Dog>");
        assert_eq!(mid.sup, "<http://example.org/Animal>");
        assert_eq!(mid.premises.len(), 1);
        let leaf = &mid.premises[0];
        assert_eq!(leaf.rule, "asserted");
        assert!(leaf.premises.is_empty());

        // A non-entailed pair explains to `found: false`, no tree.
        let r2 = dispatch_on_heap(
            &state,
            request(
                3,
                "__commons__",
                None,
                Method::OwlExplain {
                    ontology: String::new(),
                    sub: "http://example.org/Cat".into(),
                    sup: "http://example.org/LivingThing".into(),
                },
            ),
        )
        .await;
        assert_ok(&r2);
        let res2: crate::protocol::OwlExplainResult = match r2.result {
            Some(ResultPayload::Raw(b)) => rmp_serde::from_slice(&b).unwrap(),
            other => panic!("expected Raw(OwlExplainResult), got {other:?}"),
        };
        assert!(!res2.found);
        assert!(res2.tree.is_none());
    }

    /// SparqlVirtual Method round-trips through dispatch (CONCEPT:EG-KG.query.r2rml-virtual-graph /
    /// CONCEPT:EG-KG.query.obda-query-rewrite): a real user SQL table (created via
    /// `Method::Sql` DDL/DML — the SAME owner-scoped `eg_query::TableStore`
    /// `ImportSqliteFile` and the pgwire shim resolve for this caller) is exposed as RDF via a compact R2RML-style mapping and queried
    /// with SPARQL — WITHOUT any `AddTriples`/materialize step ever touching this graph.
    #[cfg(feature = "obda")]
    #[tokio::test]
    async fn test_sparql_virtual_method_round_trips() {
        let state = test_state();
        let table = format!("eg_obda_people_{}", std::process::id());
        let sql = |q: String| Method::Sql {
            query: q,
            params_msgpack: Vec::new(),
        };

        let d = dispatch_on_heap(
            &state,
            request(
                1,
                "__commons__",
                None,
                sql(format!("DROP TABLE IF EXISTS {table}")),
            ),
        )
        .await;
        assert!(d.error.is_none(), "DROP failed: {:?}", d.error);

        let c = dispatch_on_heap(
            &state,
            request(
                2,
                "__commons__",
                None,
                sql(format!(
                    "CREATE TABLE {table} (id TEXT, name TEXT, age BIGINT)"
                )),
            ),
        )
        .await;
        assert!(c.error.is_none(), "CREATE TABLE failed: {:?}", c.error);

        let i = dispatch_on_heap(
            &state,
            request(
                3,
                "__commons__",
                None,
                sql(format!(
                    "INSERT INTO {table} (id, name, age) VALUES ('1', 'Alice', 30), ('2', 'Bob', 25)"
                )),
            ),
        )
        .await;
        assert!(i.error.is_none(), "INSERT failed: {:?}", i.error);

        let mapping = format!(
            "SOURCE  {table}\nSUBJECT http://example.org/person/{{id}}\nCLASS   http://example.org/Person\nCOLUMN  http://example.org/name name\nCOLUMN  http://example.org/age  age\n"
        );
        let r = dispatch_on_heap(
            &state,
            request(
                4,
                "__commons__",
                None,
                Method::SparqlVirtual {
                    query: "PREFIX ex: <http://example.org/> SELECT ?name WHERE { ?p a ex:Person ; ex:name ?name }".into(),
                    mapping,
                    tables: vec![table.clone()],
                    external_sources: vec![],
                },
            ),
        )
        .await;
        assert_ok(&r);
        let res: crate::protocol::SparqlResult = match r.result {
            Some(ResultPayload::Raw(b)) => rmp_serde::from_slice(&b).unwrap(),
            other => panic!("expected Raw(SparqlResult), got {other:?}"),
        };
        assert_eq!(res.vars, vec!["name".to_string()]);
        let name_idx = 0;
        let mut names: Vec<Option<String>> =
            res.rows.iter().map(|row| row[name_idx].clone()).collect();
        names.sort();
        assert_eq!(
            names,
            vec![Some("Alice".to_string()), Some("Bob".to_string())],
            "the virtual graph's rows came from the SQL table, not from AddTriples"
        );

        // The request's OWN graph (__commons__) stays untouched — GetRdf sees no triples
        // from the virtual query (proves it never materialized into a real graph).
        let get = dispatch_on_heap(&state, request(5, "__commons__", None, Method::GetRdf)).await;
        assert_ok(&get);
        let nt: String = match get.result {
            Some(ResultPayload::Raw(b)) => rmp_serde::from_slice(&b).unwrap(),
            other => panic!("expected Raw(String), got {other:?}"),
        };
        assert!(
            !nt.contains("Alice") && !nt.contains("Bob"),
            "the virtual query must not have materialized into the request's graph"
        );

        let _ = dispatch_on_heap(
            &state,
            request(
                6,
                "__commons__",
                None,
                sql(format!("DROP TABLE IF EXISTS {table}")),
            ),
        )
        .await;
    }

    /// DISTRIBUTED OwlReason over TWO graphs derives the SAME entailment a single graph
    /// would (CONCEPT:EG-KG.ontology.concept-13). The shared TBox + p1 live in graph A; p2 lives in graph
    /// B; `OwlReasonDistributed{[A,B]}` unions them and infers p2 ⊑ ScholarlyWork — an
    /// entailment NEITHER shard alone reaches (B has no axioms).
    #[cfg(feature = "owl")]
    #[tokio::test]
    async fn test_owl_reason_distributed_two_graphs() {
        let state = test_state();
        // Graph A: the TBox + individual p1.
        let tbox = r#"
@prefix ex:  <http://example.org/> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdfs:<http://www.w3.org/2000/01/rdf-schema#> .
ex:Paper rdfs:subClassOf [ a owl:Restriction ; owl:onProperty ex:about ; owl:someValuesFrom ex:Topic ] .
[ a owl:Restriction ; owl:onProperty ex:about ; owl:someValuesFrom ex:Topic ] rdfs:subClassOf ex:ScholarlyWork .
ex:Article rdfs:subClassOf ex:Paper .
ex:p1 a ex:Paper .
"#;
        for (g, doc) in [
            ("__commons__", tbox),
            (
                "shard:b",
                "@prefix ex: <http://example.org/> .\nex:p2 a ex:Article .\n",
            ),
        ] {
            if g == "shard:b" {
                assert_ok(
                    &dispatch_on_heap(
                        &state,
                        request(
                            10,
                            "__commons__",
                            None,
                            Method::CreateGraph {
                                graph_name: "shard:b".into(),
                                graph_type: GraphType::Commons,
                            },
                        ),
                    )
                    .await,
                );
            }
            #[cfg(feature = "shacl")]
            configure_icv_enforce(&state, 0, g).await;
            assert_ok(
                &dispatch_on_heap(
                    &state,
                    request(
                        9,
                        g,
                        None,
                        Method::IcvConfigure {
                            graph: None,
                            mode: "enforce".to_string(),
                            shapes: format!(
                                "@prefix sh: <http://www.w3.org/ns/shacl#> .\n# test:owl-distributed:{g}"
                            ),
                        },
                    ),
                )
                .await,
            );
            assert_ok(
                &dispatch_on_heap(
                    &state,
                    request(
                        11,
                        g,
                        None,
                        Method::AddTriples {
                            turtle: doc.into(),
                            ntriples: String::new(),
                        },
                    ),
                )
                .await,
            );
        }

        let r = dispatch_on_heap(
            &state,
            request(
                12,
                "__commons__",
                None,
                Method::OwlReasonDistributed {
                    graphs: vec!["__commons__".into(), "shard:b".into()],
                    ontology: String::new(),
                    target_class: "http://example.org/ScholarlyWork".into(),
                    class_base: String::new(),
                    min_confidence: 0.0,
                },
            ),
        )
        .await;
        assert_ok(&r);
        let res: crate::protocol::OwlReasonResult = match r.result {
            Some(ResultPayload::Raw(b)) => rmp_serde::from_slice(&b).unwrap(),
            other => panic!("expected Raw(OwlReasonResult), got {other:?}"),
        };
        // p2 (Article, on shard B) is inferred ScholarlyWork via the TBox on shard A —
        // ONLY the union reaches it.
        assert!(
            res.instances.contains(&(
                "<http://example.org/p2>".into(),
                "<http://example.org/ScholarlyWork>".into()
            )),
            "distributed union must infer p2 ⊑ ScholarlyWork; instances={:?}",
            res.instances
        );
        assert_eq!(res.instances.len(), res.instance_conf.len());
        assert!(res.consistent);
    }

    // ── Streaming / CDC / subscriptions / triggers (CONCEPT:EG-KG.query.streaming-cdc-subscriptions/230) ──
    // End-to-end through the FULL dispatch path (the emit hook fires from the
    // write-side-effect block, NOT a direct hub call).

    #[cfg(feature = "streaming")]
    fn doc_node(id: &str, label: &str) -> Method {
        Method::AddNode {
            node_id: id.to_string(),
            properties_msgpack: rmp_serde::to_vec_named(&serde_json::json!({"type": label}))
                .unwrap(),
        }
    }

    /// B-8 (2026-08-13): `CdcRead` now returns a typed `CdcReadResult`
    /// (`{events, gap, watermark, head_seq, epoch}`), not a bare `Vec<CdcEvent>`
    /// — see that struct's doc. Every existing caller of this helper reads a
    /// cursor that is legitimately in range (never behind the ring, never past
    /// the head), so asserting `!gap` here converts a wrongly-behaving fix into
    /// a loud test failure instead of a silently wrong `events` list.
    #[cfg(feature = "streaming")]
    fn cdc_events(resp: &Response) -> Vec<crate::wire::CdcEvent> {
        let result: crate::wire::CdcReadResult = match &resp.result {
            Some(ResultPayload::Raw(b)) => rmp_serde::from_slice(b).unwrap(),
            other => panic!("expected Raw(CdcReadResult), got {other:?}"),
        };
        assert!(
            !result.gap,
            "test cursor was expected to be in range, but the engine reported a gap: {result:?}"
        );
        result.events
    }

    /// A write through dispatch lands in the CDC feed in order; re-reading from a
    /// later cursor skips what was already seen (CONCEPT:EG-KG.query.streaming-cdc-subscriptions).
    #[cfg(feature = "streaming")]
    #[tokio::test]
    async fn test_cdc_ordered_read_from_cursor() {
        let state = test_state();
        assert_ok(
            &dispatch_on_heap(
                &state,
                request(1, "__commons__", None, doc_node("n1", "Doc")),
            )
            .await,
        );
        assert_ok(
            &dispatch_on_heap(
                &state,
                request(2, "__commons__", None, doc_node("n2", "Doc")),
            )
            .await,
        );

        // Read from the start: both, in order.
        let r = dispatch_on_heap(
            &state,
            request(
                3,
                "__commons__",
                None,
                Method::CdcRead {
                    graph: "__commons__".into(),
                    from_seq: 0,
                    limit: 0,
                },
            ),
        )
        .await;
        let events = cdc_events(&r);
        assert_eq!(events.len(), 2, "two writes → two CDC events");
        assert_eq!(events[0].seq, 0);
        assert_eq!(events[0].node_id, "n1");
        assert_eq!(events[0].label, "Doc");
        assert!(matches!(events[0].kind, crate::wire::CdcKind::AddNode));
        assert_eq!(events[1].seq, 1);
        assert_eq!(events[1].node_id, "n2");

        // Re-read from one past the last seen cursor → empty (skips seen).
        let cursor = events.last().unwrap().seq + 1;
        let r2 = dispatch_on_heap(
            &state,
            request(
                4,
                "__commons__",
                None,
                Method::CdcRead {
                    graph: "__commons__".into(),
                    from_seq: cursor,
                    limit: 0,
                },
            ),
        )
        .await;
        assert!(cdc_events(&r2).is_empty(), "cursor past head sees nothing");

        // A new write then read-from-cursor returns ONLY it.
        assert_ok(
            &dispatch_on_heap(
                &state,
                request(
                    5,
                    "__commons__",
                    None,
                    Method::RemoveNode {
                        node_id: "n1".into(),
                    },
                ),
            )
            .await,
        );
        let r3 = dispatch_on_heap(
            &state,
            request(
                6,
                "__commons__",
                None,
                Method::CdcRead {
                    graph: "__commons__".into(),
                    from_seq: cursor,
                    limit: 0,
                },
            ),
        )
        .await;
        let tail = cdc_events(&r3);
        assert_eq!(tail.len(), 1);
        assert!(matches!(tail[0].kind, crate::wire::CdcKind::RemoveNode));
        assert_eq!(tail[0].node_id, "n1");

        // ClearGraph through dispatch RESETS the feed (CONCEPT:EG-KG.query.streaming-cdc-subscriptions): the seq
        // rewinds to 0 and the ring empties, so a consumer re-seeds from 0. (This is
        // what gives a wiped/cleared graph a clean change feed.)
        assert_ok(
            &dispatch_on_heap(&state, request(7, "__commons__", None, Method::ClearGraph)).await,
        );
        let after_clear = dispatch_on_heap(
            &state,
            request(
                8,
                "__commons__",
                None,
                Method::CdcRead {
                    graph: "__commons__".into(),
                    from_seq: 0,
                    limit: 0,
                },
            ),
        )
        .await;
        assert!(
            cdc_events(&after_clear).is_empty(),
            "ClearGraph resets the CDC feed to empty"
        );
        // A post-clear write is seq 0 again.
        assert_ok(
            &dispatch_on_heap(
                &state,
                request(9, "__commons__", None, doc_node("fresh", "Doc")),
            )
            .await,
        );
        let reseeded = dispatch_on_heap(
            &state,
            request(
                10,
                "__commons__",
                None,
                Method::CdcRead {
                    graph: "__commons__".into(),
                    from_seq: 0,
                    limit: 0,
                },
            ),
        )
        .await;
        let ev = cdc_events(&reseeded);
        assert_eq!(ev.len(), 1);
        assert_eq!(
            ev[0].seq, 0,
            "feed rewound — first post-clear change is seq 0"
        );
        assert_eq!(ev[0].node_id, "fresh");
    }

    /// A continuous query maintained incrementally off the CDC feed equals a full
    /// re-run over the final graph state (CONCEPT:EG-KG.query.streaming-cdc-subscriptions).
    #[cfg(feature = "streaming")]
    #[tokio::test]
    async fn test_continuous_query_incremental_equals_full_rerun() {
        let state = test_state();
        // Register a Count CQ over label "Doc" BEFORE any writes.
        let spec = crate::wire::ContinuousQuerySpec {
            graph: "__commons__".into(),
            label: "Doc".into(),
            agg: crate::wire::ContinuousAgg::Count,
        };
        assert_ok(
            &dispatch_on_heap(
                &state,
                request(
                    1,
                    "__commons__",
                    None,
                    Method::RegisterContinuousQuery {
                        name: "doc_count".into(),
                        spec_msgpack: rmp_serde::to_vec_named(&spec).unwrap(),
                    },
                ),
            )
            .await,
        );

        // Mutations: 3 Doc adds, 1 Other add, 1 Doc remove → 2 live Docs.
        let mut id = 10u64;
        for n in ["a", "b", "c"] {
            assert_ok(
                &dispatch_on_heap(&state, request(id, "__commons__", None, doc_node(n, "Doc")))
                    .await,
            );
            id += 1;
        }
        assert_ok(
            &dispatch_on_heap(
                &state,
                request(id, "__commons__", None, doc_node("x", "Other")),
            )
            .await,
        );
        id += 1;
        assert_ok(
            &dispatch_on_heap(
                &state,
                request(
                    id,
                    "__commons__",
                    None,
                    Method::RemoveNode {
                        node_id: "a".into(),
                    },
                ),
            )
            .await,
        );
        id += 1;

        // Read the incrementally-maintained CQ value.
        let r = dispatch_on_heap(
            &state,
            request(
                id,
                "__commons__",
                None,
                Method::ReadContinuousQuery {
                    name: "doc_count".into(),
                },
            ),
        )
        .await;
        let cq: crate::wire::ContinuousQueryResult = match r.result {
            Some(ResultPayload::Raw(b)) => rmp_serde::from_slice(&b).unwrap(),
            other => panic!("expected Raw(ContinuousQueryResult), got {other:?}"),
        };

        // ORACLE: full re-run = count nodes with label "Doc" in the final graph.
        let full_rerun = {
            let s = state.read().await;
            let core = s.registry.get("__commons__").unwrap().core.clone();
            core.get_nodes_by_label("Doc", 0).len() as f64
        };
        assert_eq!(
            cq.value, full_rerun,
            "incremental CQ must equal the full re-run"
        );
        assert_eq!(cq.value, 2.0);
    }

    /// A `Watch` long-poll delivers a change to a subscriber, and a registered trigger
    /// fires its action on a matching change (CONCEPT:EG-KG.query.wire-codec).
    #[cfg(feature = "streaming")]
    #[tokio::test]
    async fn test_watch_and_trigger_delivery() {
        let state = test_state();

        // Register a trigger: any "Alert"-labelled node add records an action.
        assert_ok(
            &dispatch_on_heap(
                &state,
                request(
                    1,
                    "__commons__",
                    None,
                    Method::RegisterTrigger {
                        name: "on_alert".into(),
                        graph: "__commons__".into(),
                        label: "Alert".into(),
                        op: "add".into(),
                        action_msgpack: rmp_serde::to_vec_named(
                            &serde_json::json!({"topic": "ops"}),
                        )
                        .unwrap(),
                    },
                ),
            )
            .await,
        );

        // A non-matching write (Doc) does NOT fire the trigger.
        assert_ok(
            &dispatch_on_heap(
                &state,
                request(2, "__commons__", None, doc_node("d1", "Doc")),
            )
            .await,
        );
        // A matching write (Alert) DOES — and is delivered to a Watch subscriber.
        assert_ok(
            &dispatch_on_heap(
                &state,
                request(3, "__commons__", None, doc_node("a1", "Alert")),
            )
            .await,
        );

        // Watch from the start, filtered to "Alert": must see exactly the Alert change.
        let w = dispatch_on_heap(
            &state,
            request(
                4,
                "__commons__",
                None,
                Method::Watch {
                    graph: "__commons__".into(),
                    from_seq: 0,
                    label: "Alert".into(),
                    timeout_ms: 0,
                },
            ),
        )
        .await;
        let batch: crate::wire::WatchBatch = match w.result {
            Some(ResultPayload::Raw(b)) => rmp_serde::from_slice(&b).unwrap(),
            other => panic!("expected Raw(WatchBatch), got {other:?}"),
        };
        assert_eq!(
            batch.events.len(),
            1,
            "watch delivers only the Alert change"
        );
        assert_eq!(batch.events[0].node_id, "a1");
        assert_eq!(batch.next_seq, batch.events[0].seq + 1);

        // The trigger fired exactly once; poll the fired log for its action.
        let f = dispatch_on_heap(
            &state,
            request(
                5,
                "__commons__",
                None,
                Method::FiredTriggers {
                    graph: "__commons__".into(),
                    from_seq: 0,
                    limit: 0,
                },
            ),
        )
        .await;
        // B-8 follow-up: `FiredTriggers` now returns a typed `FiredTriggersResult`
        // (`{fired, gap, watermark, head_seq, epoch}`), not a bare `Vec<FiredAction>`.
        let fired_result: crate::wire::FiredTriggersResult = match f.result {
            Some(ResultPayload::Raw(b)) => rmp_serde::from_slice(&b).unwrap(),
            other => panic!("expected Raw(FiredTriggersResult), got {other:?}"),
        };
        assert!(
            !fired_result.gap,
            "cursor 0 must not be a gap: {fired_result:?}"
        );
        let fired = fired_result.fired;
        assert_eq!(fired.len(), 1, "exactly one firing (only the Alert add)");
        assert_eq!(fired[0].trigger, "on_alert");
        assert_eq!(fired[0].node_id, "a1");
        let action: serde_json::Value = rmp_serde::from_slice(&fired[0].action).unwrap();
        assert_eq!(action["topic"], "ops");
    }

    /// `Watch` long-poll wakes when a change lands DURING the poll window
    /// (subscription push semantics over the long-poll transport).
    ///
    /// GOC-70: this used to spawn the write behind a fixed `sleep(20ms)`,
    /// racing it against a concurrently-dispatched `Method::Watch` call, on the
    /// theory that 20ms was enough real time for the watch dispatch task to have
    /// reached its own internal `Notify` registration first. That assumption
    /// held at CI's 2-core scale but not on a many-core dev host running the
    /// ~1000-test `--lib` suite at full host parallelism (each test spinning its
    /// own multi-thread tokio runtime): the watch dispatch TASK ITSELF is not
    /// guaranteed to be scheduled within any fixed wall-clock bound under that
    /// contention, so even a prior widening from a 2s to a 20s long-poll timeout
    /// (see git history) did not fully close the race -- it only made the
    /// failure rarer, which is exactly the "defective timing-dependent test"
    /// shape GOC-70 exists to catch, not a real product bug (the failing test's
    /// OWN write-side assertion always passed: the write itself never failed,
    /// only its arrival relative to the watch's registration was ever in
    /// question).
    ///
    /// Fix: inline the exact sequence `handlers::streaming::Watch` performs
    /// (`CdcHub::notifier` → an empty first-pass `watch_batch` → await
    /// `notified`) using the SAME hub primitives the handler calls, so this test
    /// controls the "arm, confirm nothing pending yet, THEN write" ordering with
    /// `.await` boundaries instead of hoping a sleep wins a real-time race. This
    /// still proves the real underlying wakeup mechanism (the full
    /// `Method::Watch` request/response path, including auth + wire encoding,
    /// is covered separately by the "already pending" `Watch` test above) --
    /// it is deterministic on any core count, including 1, because every step
    /// is ordered by `.await`, not by scheduling luck.
    #[cfg(feature = "streaming")]
    #[tokio::test]
    async fn test_watch_long_poll_wakes_on_write() {
        let state = test_state();
        let hub = state
            .read()
            .await
            .cdc
            .clone()
            .expect("streaming hub configured");

        // Arm the per-graph `Notify` BEFORE checking for anything pending --
        // `Notify::notified()` captures any `notify_waiters()` call made after
        // this point, per its own doc, exactly mirroring the production
        // handler's lost-wakeup-safe "arm before check" ordering.
        let notify = hub.notifier("__commons__");
        let notified = notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        let pre = hub.watch_batch("__commons__", 0, "", 0);
        assert!(
            !pre.gap && pre.events.is_empty(),
            "nothing must be pending before the write: {pre:?}"
        );

        // Now perform the write -- deterministically AFTER the barrier above is
        // armed and confirmed empty, so the wakeup this test proves can only be
        // the one triggered BY this write, not a pre-existing pending change.
        let resp = dispatch_on_heap(
            &state,
            request(9, "__commons__", None, doc_node("late", "Doc")),
        )
        .await;
        assert!(
            resp.error.is_none(),
            "the write itself failed, so there was nothing to wake on: {:?}",
            resp.error
        );

        // `dispatch_on_heap` awaited the write (and its `CdcHub::emit` ->
        // `notify_waiters()`) to completion above, so `notified` has already
        // fired or resolves immediately -- this timeout is a hang guard against
        // a genuine product regression, not part of the ordering argument.
        tokio::time::timeout(std::time::Duration::from_secs(20), notified)
            .await
            .expect("notify did not fire after the write completed");

        let batch = hub.watch_batch("__commons__", 0, "", 0);
        assert_eq!(
            batch.events.len(),
            1,
            "long-poll woke on the in-window write"
        );
        assert_eq!(batch.events[0].node_id, "late");
    }

    /// End-to-end WASM UDF through the SERVER dispatch (CONCEPT:EG-KG.query.rowset-execution): RegisterUdf
    /// compiles+caches a sandboxed module, then RunUdf runs it over a payload and the
    /// output round-trips — AND an infinite-loop UDF registered the same way is
    /// FUEL-KILLED (a trap error response), never a hang. Proves the Method surface +
    /// the off-reactor sandboxed execution path, not just the eg-wasm unit tests.
    #[cfg(feature = "wasm-udf")]
    #[tokio::test]
    async fn run_udf_through_dispatch_runs_sandboxed_and_fuel_kills_infinite_loop() {
        let state = test_state();

        // An identity UDF (echoes its input bytes) and an infinite-loop UDF.
        let identity = wat::parse_str(
            r#"(module
                (memory (export "memory") 1)
                (global $n (mut i32) (i32.const 1024))
                (func (export "alloc") (param $l i32) (result i32)
                    (local $p i32) (local.set $p (global.get $n))
                    (global.set $n (i32.add (global.get $n) (local.get $l))) (local.get $p))
                (func (export "udf") (param $p i32) (param $l i32) (result i64)
                    (i64.or (i64.shl (i64.extend_i32_u (local.get $p)) (i64.const 32))
                            (i64.extend_i32_u (local.get $l)))))"#,
        )
        .unwrap();
        let infinite = wat::parse_str(
            r#"(module
                (memory (export "memory") 1)
                (func (export "alloc") (param $l i32) (result i32) (i32.const 1024))
                (func (export "udf") (param $p i32) (param $l i32) (result i64)
                    (loop $f (br $f)) (i64.const 0)))"#,
        )
        .unwrap();

        // Register both (process-global; the request graph is just the ACL anchor).
        assert_ok(
            &dispatch_on_heap(
                &state,
                request(
                    1,
                    "__commons__",
                    None,
                    Method::RegisterUdf {
                        id: "echo".into(),
                        wasm: identity,
                    },
                ),
            )
            .await,
        );
        assert_ok(
            &dispatch_on_heap(
                &state,
                request(
                    2,
                    "__commons__",
                    None,
                    Method::RegisterUdf {
                        id: "spin".into(),
                        wasm: infinite,
                    },
                ),
            )
            .await,
        );

        // RunUdf "echo" over a payload → the SAME bytes back (sandboxed round-trip).
        let payload = b"rows-over-the-wire".to_vec();
        let resp = dispatch_on_heap(
            &state,
            request(
                3,
                "__commons__",
                None,
                Method::RunUdf {
                    id: "echo".into(),
                    input: payload.clone(),
                },
            ),
        )
        .await;
        assert_ok(&resp);
        match resp.result {
            Some(ResultPayload::Raw(out)) => assert_eq!(out, payload, "identity UDF echoes input"),
            other => panic!("expected Raw output, got {other:?}"),
        }

        // RunUdf "spin" → the infinite loop is FUEL-KILLED: an error response, not a hang.
        let start = std::time::Instant::now();
        let resp = dispatch_on_heap(
            &state,
            request(
                4,
                "__commons__",
                None,
                Method::RunUdf {
                    id: "spin".into(),
                    input: b"x".to_vec(),
                },
            ),
        )
        .await;
        assert!(
            resp.error.is_some(),
            "infinite-loop UDF must be killed (error), got ok: {:?}",
            resp.result
        );
        assert!(
            start.elapsed() < std::time::Duration::from_secs(10),
            "fuel kill must be fast, not a hang"
        );
    }
}
