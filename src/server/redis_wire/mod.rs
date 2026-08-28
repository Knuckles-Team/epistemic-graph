//! Redis RESP wire-protocol listener (CONCEPT:EG-KG.ontology.resp2-resp3-codec-round) — a native, HAND-ROLLED
//! Redis server so a Redis client, an ORM cache layer, or `redis-cli` connects
//! DIRECTLY to the engine and runs the core Redis command set.
//!
//! ## What this is (and is NOT)
//!
//! Like the SQL wire shims (pgwire / mysql-wire, CONCEPT:EG-KG.compute.subsystems-reference) and the Bolt
//! adapter (CONCEPT:EG-KG.query.bolt-wire-protocol), this is an ADAPTER — NOT a re-implemented Redis. The
//! bytes are stored on the engine's own durable Key→Value substrate
//! ([`crate::server::kv::KvStore`], CONCEPT:EG-KG.storage.namespaced-kv-surface): every Redis key lives as a
//! single msgpack [`Entry`] envelope in one `redis` namespace, so a `SET` that
//! returned `OK` survives a `kill -9` exactly like a `KvPut`. The five Redis data
//! types (string / list / hash / set / zset) are modeled as variants of one
//! serialized [`RedisData`] value, and every read-modify-write op is serialized
//! under one coarse process mutex (a serving surface, not a perf-critical inner
//! loop).
//!
//! It links NO server-side redis protocol crate — the RESP2 + RESP3 codec is
//! hand-rolled against the documented RESP spec (the Pi-contract idiom pgwire /
//! mysql-wire / bolt-wire follow). PURE-RUST, so it stays outside the Pi forbidden
//! set; it is deliberately OUT of the `pi` tier (a network listener).
//!
//! ## Protocol subset (CONCEPT:EG-KG.ontology.resp2-resp3-codec-round)
//!
//! LANDED: the RESP2 + RESP3 codec (simple strings, errors, integers, bulk
//! strings, arrays, and the RESP3 map/set/double/boolean/null types) + inline
//! commands, the `HELLO`/`COMMAND` handshake so `redis-cli` connects, and the core
//! command set: `PING`, `ECHO`, `GET`/`SET` (`EX`/`PX`/`NX`/`XX`), `DEL`, `EXISTS`,
//! `EXPIRE`/`TTL`, `INCR`/`DECR`, `MGET`/`MSET`, `HSET`/`HGET`/`HGETALL`/`HDEL`,
//! `LPUSH`/`RPUSH`/`LRANGE`/`LLEN`, `SADD`/`SMEMBERS`/`SREM`,
//! `ZADD`/`ZRANGE`/`ZSCORE`, `SCAN`, `TYPE`, plus `AUTH`/`SELECT`/`QUIT`/`CONFIG`/
//! `CLIENT` niceties.
//!
//! ## Pub/sub + transactions (CONCEPT:EG-KG.txn.pubsub-transactions)
//!
//! LANDED (EG-307): the publish/subscribe surface — `SUBSCRIBE`/`UNSUBSCRIBE`,
//! `PSUBSCRIBE`/`PUNSUBSCRIBE` (glob-pattern channels), and `PUBLISH`, backed by a
//! per-listener [`PubSub`] registry (an mpsc channel per connection; the connection
//! driver `select!`s between the socket and its subscriber mailbox so published
//! messages are pushed out as they arrive). Plus `MULTI`/`EXEC`/`DISCARD`
//! transactions: commands after `MULTI` are queued (`+QUEUED`) and executed
//! back-to-back on `EXEC` (no other connection interleaves), returning the array of
//! replies; `DISCARD` drops the queue; a malformed queued command aborts the whole
//! transaction with `EXECABORT`. Lua, streams, optimistic locking, and Redis
//! cluster commands are outside this adapter's declared protocol and return an
//! explicit unknown-command error; unsupported operations are never accepted as
//! successful no-ops.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use hmac::{Hmac, Mac};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio::sync::RwLock;

use crate::server::kv::KvStore;
use crate::server::ServerState;

/// Env var: when set (and built `--features redis-wire`) the Redis RESP listener
/// binds this address (documented loopback default `127.0.0.1:6379`, the Redis
/// default port). Unset ⇒ no listener.
pub const REDIS_ADDR_ENV: &str = "EPISTEMIC_GRAPH_REDIS_ADDR";
type HmacSha256 = Hmac<Sha256>;

/// Derive the Redis credential for a principal from the deployment auth secret.
/// Clients authenticate with `AUTH <principal> <credential>`; the principal is
/// pseudonymized before it is used as a durable keyspace or pub/sub scope.
pub fn derive_redis_password(secret: &str, principal: &str) -> String {
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key length");
    mac.update(b"redis:");
    mac.update(principal.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

fn verify_redis_password(secret: &str, principal: &str, password: &[u8]) -> bool {
    if secret.is_empty()
        || principal.is_empty()
        || principal.len() > MAX_REDIS_COMMAND_BYTES
        || password.len() != 64
    {
        return false;
    }
    let Ok(candidate) = hex::decode(password) else {
        return false;
    };
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key length");
    mac.update(b"redis:");
    mac.update(principal.as_bytes());
    mac.verify_slice(&candidate).is_ok()
}

/// Wall-clock milliseconds since the epoch (TTL clock; same tolerance as the
/// blob/txn TTL sweeps).
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

// ── the stored value model (CONCEPT:EG-KG.ontology.resp2-resp3-codec-round) ─────────────────────────────────────

/// One Redis value, tagged by type. Serialized (msgpack) inside an [`Entry`] and
/// stored as ONE KV value per key — read-modify-write under the store mutex.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
enum RedisData {
    Str(Vec<u8>),
    List(Vec<Vec<u8>>),
    /// Insertion-ordered `(field, value)` pairs (Redis hashes are unordered; we
    /// keep insertion order for a stable `HGETALL`).
    Hash(Vec<(Vec<u8>, Vec<u8>)>),
    Set(Vec<Vec<u8>>),
    /// `(member, score)` pairs kept sorted by `(score, member)`.
    ZSet(Vec<(Vec<u8>, f64)>),
}

impl RedisData {
    /// The Redis `TYPE` name for this value.
    fn type_name(&self) -> &'static str {
        match self {
            RedisData::Str(_) => "string",
            RedisData::List(_) => "list",
            RedisData::Hash(_) => "hash",
            RedisData::Set(_) => "set",
            RedisData::ZSet(_) => "zset",
        }
    }
}

/// The stored envelope: the value plus an optional absolute expiry (epoch ms).
#[derive(Serialize, Deserialize, Clone, Debug)]
struct Entry {
    data: RedisData,
    expire_at_ms: Option<u64>,
}

impl Entry {
    fn expired(&self, now: u64) -> bool {
        matches!(self.expire_at_ms, Some(t) if now >= t)
    }
}

/// A set of `(field, value)` byte pairs (hash entries / bulk arg pairs).
type BytePairs = Vec<(Vec<u8>, Vec<u8>)>;
/// The result of parsing one command out of the read buffer: `Ok(None)` ⇒
/// incomplete (read more), `Ok(Some((args, consumed)))` ⇒ a complete command.
type CommandParse = Result<Option<(Vec<Vec<u8>>, usize)>, String>;

/// The WRONGTYPE error text (verbatim Redis wording, so clients that match on it
/// still work).
const WRONGTYPE: &str = "WRONGTYPE Operation against a key holding the wrong kind of value";
const NOT_INT: &str = "ERR value is not an integer or out of range";
const MAX_REDIS_REQUEST_BYTES: usize = 64 * 1024 * 1024;
const MAX_REDIS_COMMAND_BYTES: usize = 64;
const MAX_REDIS_KEY_BYTES: usize = 4 * 1024;
const MAX_RESP_ARGUMENTS: usize = 1_024;
const MAX_RESP_BULK_BYTES: usize = MAX_REDIS_REQUEST_BYTES;
const MAX_RESP_LINE_BYTES: usize = 64 * 1024;
const MAX_REDIS_RESPONSE_BYTES: usize = 64 * 1024 * 1024;
const MAX_STORED_ENTRY_BYTES: usize = 64 * 1024 * 1024;
const MAX_STORED_ENTRY_ITEMS: usize = 1_000_000;
const MAX_REDIS_SUBSCRIPTIONS: usize = 1_024;
const MAX_REDIS_SUBSCRIPTION_BYTES: usize = 1024 * 1024;
const MAX_REDIS_PUBSUB_CONNECTIONS: usize = 4_096;
const MAX_REDIS_PUBSUB_LINKS: usize = 65_536;
const MAX_REDIS_PUBSUB_KEY_BYTES: usize = 64 * 1024 * 1024;
const MAX_REDIS_CHANNEL_BYTES: usize = 4 * 1024;
const MAX_MULTI_COMMANDS: usize = 1_024;
const MAX_MULTI_BYTES: usize = 64 * 1024 * 1024;
const PUBSUB_MAILBOX_CAPACITY: usize = 1_024;

fn invalid_data(message: &'static str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message)
}

fn entry_limits() -> eg_types::msgpack::MsgpackLimits {
    eg_types::msgpack::MsgpackLimits::new(
        MAX_STORED_ENTRY_BYTES,
        MAX_STORED_ENTRY_ITEMS,
        eg_types::msgpack::DEFAULT_MAX_DEPTH,
    )
}

fn decode_entry(bytes: &[u8]) -> Result<Entry, String> {
    eg_types::msgpack::decode_bounded(bytes, entry_limits())
        .map_err(|_| "ERR stored value is invalid or exceeds resource limits".to_string())
}

fn utf8_argument(bytes: &[u8], max_bytes: usize) -> Result<String, String> {
    if bytes.len() > max_bytes {
        return Err("ERR argument exceeds resource limits".to_string());
    }
    std::str::from_utf8(bytes)
        .map(str::to_owned)
        .map_err(|_| "ERR argument must be valid UTF-8".to_string())
}

// ── the store (CONCEPT:EG-KG.ontology.resp2-resp3-codec-round) — Redis types over the engine KV surface ──────────

/// A Redis keyspace layered over one [`KvStore`] namespace. Durable when the KV
/// store is (a persist dir), else in-memory scratch — exactly the KV contract.
struct RedisStore {
    kv: Arc<KvStore>,
    /// Serializes read-modify-write ops so multi-step mutations (e.g. `HSET`,
    /// `INCR`, `ZADD`) are atomic against concurrent connections.
    lock: Arc<Mutex<()>>,
    /// Secret-keyed pseudonymous namespace. A served connection receives its own
    /// scope only after successful authentication, so principals cannot enumerate
    /// or mutate one another's Redis data.
    namespace: String,
}

impl RedisStore {
    /// Open the Redis keyspace. `Some(dir)` ⇒ durable `{dir}/redis-kv/kv.redb`;
    /// `None` ⇒ in-memory scratch (CONCEPT:EG-KG.ontology.resp2-resp3-codec-round).
    fn open(persist_dir: Option<&str>) -> Result<Self, String> {
        let sub = persist_dir.map(|d| format!("{d}/redis-kv"));
        let kv = KvStore::open(sub.as_deref())?;
        Ok(Self {
            kv: Arc::new(kv),
            lock: Arc::new(Mutex::new(())),
            namespace: "redis:unbound".to_string(),
        })
    }

    fn scoped(&self, actor_ref: &str) -> Self {
        Self {
            kv: Arc::clone(&self.kv),
            lock: Arc::clone(&self.lock),
            namespace: format!("redis:{actor_ref}"),
        }
    }

    /// `true` if writes land durably on disk.
    fn is_durable(&self) -> bool {
        self.kv.is_durable()
    }

    // ── internal (no-lock) primitives ────────────────────────────────────────

    /// Load the live entry for `key`, transparently expiring (and deleting) a
    /// stale one. MUST be called with the store lock held.
    fn load(&self, key: &str) -> Result<Option<Entry>, String> {
        match self.kv.get(&self.namespace, key)? {
            Some(bytes) => {
                let entry = decode_entry(&bytes)?;
                if entry.expired(now_ms()) {
                    self.kv.delete(&self.namespace, key)?;
                    Ok(None)
                } else {
                    Ok(Some(entry))
                }
            }
            None => Ok(None),
        }
    }

    fn store(&self, key: &str, entry: &Entry) -> Result<(), String> {
        let bytes = rmp_serde::to_vec_named(entry)
            .map_err(|_| "ERR value could not be encoded".to_string())?;
        eg_types::msgpack::validate_single_value(&bytes, entry_limits())
            .map_err(|_| "ERR value exceeds resource limits".to_string())?;
        self.kv.put(&self.namespace, key, bytes)
    }

    // ── string ops ────────────────────────────────────────────────────────────

    /// `SET key value` with optional `EX`/`PX` expiry + `NX`/`XX` conditions.
    /// Returns `true` if written (a failed `NX`/`XX` returns `false` ⇒ null reply).
    #[allow(clippy::too_many_arguments)]
    fn set(
        &self,
        key: &str,
        value: Vec<u8>,
        expire_ms: Option<u64>,
        nx: bool,
        xx: bool,
    ) -> Result<bool, String> {
        let _g = self.lock.lock();
        let existing = self.load(key)?;
        if nx && existing.is_some() {
            return Ok(false);
        }
        if xx && existing.is_none() {
            return Ok(false);
        }
        let entry = Entry {
            data: RedisData::Str(value),
            expire_at_ms: expire_ms.map(|ms| now_ms() + ms),
        };
        self.store(key, &entry)?;
        Ok(true)
    }

    fn get(&self, key: &str) -> Result<Option<Vec<u8>>, String> {
        let _g = self.lock.lock();
        match self.load(key)? {
            Some(Entry {
                data: RedisData::Str(v),
                ..
            }) => Ok(Some(v)),
            Some(_) => Err(WRONGTYPE.into()),
            None => Ok(None),
        }
    }

    fn incr_by(&self, key: &str, delta: i64) -> Result<i64, String> {
        let _g = self.lock.lock();
        let (cur, expire) = match self.load(key)? {
            Some(Entry {
                data: RedisData::Str(v),
                expire_at_ms,
            }) => {
                let s = std::str::from_utf8(&v).map_err(|_| NOT_INT.to_string())?;
                let n: i64 = s.trim().parse().map_err(|_| NOT_INT.to_string())?;
                (n, expire_at_ms)
            }
            Some(_) => return Err(WRONGTYPE.into()),
            None => (0, None),
        };
        let next = cur.checked_add(delta).ok_or_else(|| NOT_INT.to_string())?;
        let entry = Entry {
            data: RedisData::Str(next.to_string().into_bytes()),
            expire_at_ms: expire,
        };
        self.store(key, &entry)?;
        Ok(next)
    }

    /// Delete `keys`, returning how many existed.
    fn del(&self, keys: &[&str]) -> Result<i64, String> {
        let _g = self.lock.lock();
        let mut n = 0;
        for k in keys {
            // Expire-aware existence: a stale key does not count.
            if self.load(k)?.is_some() && self.kv.delete(&self.namespace, k)? {
                n += 1;
            }
        }
        Ok(n)
    }

    fn exists(&self, keys: &[&str]) -> Result<i64, String> {
        let _g = self.lock.lock();
        let mut n = 0;
        for k in keys {
            if self.load(k)?.is_some() {
                n += 1;
            }
        }
        Ok(n)
    }

    /// `EXPIRE key seconds` → `true` if the key existed and the TTL was set.
    fn expire(&self, key: &str, seconds: i64) -> Result<bool, String> {
        let _g = self.lock.lock();
        match self.load(key)? {
            Some(mut entry) => {
                if seconds <= 0 {
                    self.kv.delete(&self.namespace, key)?;
                } else {
                    entry.expire_at_ms = Some(now_ms() + (seconds as u64) * 1000);
                    self.store(key, &entry)?;
                }
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// `TTL key` → remaining seconds, `-1` (no expiry), or `-2` (missing).
    fn ttl(&self, key: &str) -> Result<i64, String> {
        let _g = self.lock.lock();
        match self.load(key)? {
            Some(Entry {
                expire_at_ms: Some(t),
                ..
            }) => {
                let now = now_ms();
                Ok(if t > now {
                    ((t - now) / 1000) as i64
                } else {
                    -2
                })
            }
            Some(_) => Ok(-1),
            None => Ok(-2),
        }
    }

    fn type_of(&self, key: &str) -> Result<&'static str, String> {
        let _g = self.lock.lock();
        Ok(match self.load(key)? {
            Some(e) => e.data.type_name(),
            None => "none",
        })
    }

    /// `SCAN` — returns every key in the namespace whose name matches `pattern`
    /// (glob, `*`/`?` only). One-shot: the returned cursor is always `0`.
    fn scan(&self, pattern: Option<&str>) -> Result<Vec<String>, String> {
        let _g = self.lock.lock();
        let now = now_ms();
        let mut out = Vec::new();
        for (k, v) in self.kv.scan(&self.namespace, "", 0)? {
            // Drop expired entries lazily as we walk.
            let entry = decode_entry(&v)?;
            if entry.expired(now) {
                continue;
            }
            if pattern.map(|p| glob_match(p, &k)).unwrap_or(true) {
                out.push(k);
            }
        }
        Ok(out)
    }

    // ── hash ops ────────────────────────────────────────────────────────────────

    fn hset(&self, key: &str, pairs: &[(Vec<u8>, Vec<u8>)]) -> Result<i64, String> {
        let _g = self.lock.lock();
        let (mut h, expire) = match self.load(key)? {
            Some(Entry {
                data: RedisData::Hash(h),
                expire_at_ms,
            }) => (h, expire_at_ms),
            Some(_) => return Err(WRONGTYPE.into()),
            None => (Vec::new(), None),
        };
        // Preserve the serialized insertion order while using a transient field
        // directory for batch updates. `entry(...).or_insert(...)` retains the
        // deterministic first-field behavior if a corrupt value contains a duplicate.
        let mut positions: HashMap<Vec<u8>, usize> = HashMap::with_capacity(h.len() + pairs.len());
        for (index, (field, _)) in h.iter().enumerate() {
            positions.entry(field.clone()).or_insert(index);
        }
        let mut added = 0;
        for (f, val) in pairs {
            if let Some(index) = positions.get(f.as_slice()).copied() {
                h[index].1 = val.clone();
            } else {
                let index = h.len();
                h.push((f.clone(), val.clone()));
                positions.insert(f.clone(), index);
                added += 1;
            }
        }
        self.store(
            key,
            &Entry {
                data: RedisData::Hash(h),
                expire_at_ms: expire,
            },
        )?;
        Ok(added)
    }

    fn hget(&self, key: &str, field: &[u8]) -> Result<Option<Vec<u8>>, String> {
        let _g = self.lock.lock();
        match self.load(key)? {
            Some(Entry {
                data: RedisData::Hash(h),
                ..
            }) => Ok(h.into_iter().find(|(f, _)| f == field).map(|(_, v)| v)),
            Some(_) => Err(WRONGTYPE.into()),
            None => Ok(None),
        }
    }

    fn hgetall(&self, key: &str) -> Result<BytePairs, String> {
        let _g = self.lock.lock();
        match self.load(key)? {
            Some(Entry {
                data: RedisData::Hash(h),
                ..
            }) => Ok(h),
            Some(_) => Err(WRONGTYPE.into()),
            None => Ok(Vec::new()),
        }
    }

    fn hdel(&self, key: &str, fields: &[Vec<u8>]) -> Result<i64, String> {
        let _g = self.lock.lock();
        match self.load(key)? {
            Some(Entry {
                data: RedisData::Hash(mut h),
                expire_at_ms,
            }) => {
                let fields: HashSet<&[u8]> = fields.iter().map(Vec::as_slice).collect();
                let before = h.len();
                h.retain(|(f, _)| !fields.contains(f.as_slice()));
                let removed = (before - h.len()) as i64;
                if h.is_empty() {
                    self.kv.delete(&self.namespace, key)?;
                } else {
                    self.store(
                        key,
                        &Entry {
                            data: RedisData::Hash(h),
                            expire_at_ms,
                        },
                    )?;
                }
                Ok(removed)
            }
            Some(_) => Err(WRONGTYPE.into()),
            None => Ok(0),
        }
    }

    // ── list ops ──────────────────────────────────────────────────────────────

    /// `LPUSH`/`RPUSH` — push `values` at the head (`left`) or tail; returns the
    /// new length. `LPUSH a b c` yields `c b a` at the head (Redis order).
    fn push(&self, key: &str, values: &[Vec<u8>], left: bool) -> Result<i64, String> {
        let _g = self.lock.lock();
        let (mut list, expire) = match self.load(key)? {
            Some(Entry {
                data: RedisData::List(l),
                expire_at_ms,
            }) => (l, expire_at_ms),
            Some(_) => return Err(WRONGTYPE.into()),
            None => (Vec::new(), None),
        };
        if left && !values.is_empty() {
            // Repeated `insert(0, ...)` shifts the complete tail for every value.
            // Build the reversed prefix once, then move the old tail once.
            let new_len = list
                .len()
                .checked_add(values.len())
                .ok_or_else(|| "ERR list exceeds resource limits".to_string())?;
            let mut prefixed = Vec::with_capacity(new_len);
            prefixed.extend(values.iter().rev().cloned());
            prefixed.append(&mut list);
            list = prefixed;
        } else {
            list.extend(values.iter().cloned());
        }
        let len = list.len() as i64;
        self.store(
            key,
            &Entry {
                data: RedisData::List(list),
                expire_at_ms: expire,
            },
        )?;
        Ok(len)
    }

    fn lrange(&self, key: &str, start: i64, stop: i64) -> Result<Vec<Vec<u8>>, String> {
        let _g = self.lock.lock();
        let list = match self.load(key)? {
            Some(Entry {
                data: RedisData::List(l),
                ..
            }) => l,
            Some(_) => return Err(WRONGTYPE.into()),
            None => return Ok(Vec::new()),
        };
        Ok(range_slice(&list, start, stop).to_vec())
    }

    fn llen(&self, key: &str) -> Result<i64, String> {
        let _g = self.lock.lock();
        match self.load(key)? {
            Some(Entry {
                data: RedisData::List(l),
                ..
            }) => Ok(l.len() as i64),
            Some(_) => Err(WRONGTYPE.into()),
            None => Ok(0),
        }
    }

    // ── set ops ───────────────────────────────────────────────────────────────

    fn sadd(&self, key: &str, members: &[Vec<u8>]) -> Result<i64, String> {
        let _g = self.lock.lock();
        let (mut set, expire) = match self.load(key)? {
            Some(Entry {
                data: RedisData::Set(s),
                expire_at_ms,
            }) => (s, expire_at_ms),
            Some(_) => return Err(WRONGTYPE.into()),
            None => (Vec::new(), None),
        };
        let mut present: HashSet<Vec<u8>> = HashSet::with_capacity(set.len() + members.len());
        present.extend(set.iter().cloned());
        let mut added = 0;
        for m in members {
            if present.insert(m.clone()) {
                set.push(m.clone());
                added += 1;
            }
        }
        self.store(
            key,
            &Entry {
                data: RedisData::Set(set),
                expire_at_ms: expire,
            },
        )?;
        Ok(added)
    }

    fn smembers(&self, key: &str) -> Result<Vec<Vec<u8>>, String> {
        let _g = self.lock.lock();
        match self.load(key)? {
            Some(Entry {
                data: RedisData::Set(s),
                ..
            }) => Ok(s),
            Some(_) => Err(WRONGTYPE.into()),
            None => Ok(Vec::new()),
        }
    }

    fn srem(&self, key: &str, members: &[Vec<u8>]) -> Result<i64, String> {
        let _g = self.lock.lock();
        match self.load(key)? {
            Some(Entry {
                data: RedisData::Set(mut s),
                expire_at_ms,
            }) => {
                let members: HashSet<&[u8]> = members.iter().map(Vec::as_slice).collect();
                let before = s.len();
                s.retain(|e| !members.contains(e.as_slice()));
                let removed = (before - s.len()) as i64;
                if s.is_empty() {
                    self.kv.delete(&self.namespace, key)?;
                } else {
                    self.store(
                        key,
                        &Entry {
                            data: RedisData::Set(s),
                            expire_at_ms,
                        },
                    )?;
                }
                Ok(removed)
            }
            Some(_) => Err(WRONGTYPE.into()),
            None => Ok(0),
        }
    }

    // ── sorted-set ops ──────────────────────────────────────────────────────────

    /// `ZADD` — insert/update `(member, score)` pairs; returns the count of NEW
    /// members. The vector is kept sorted by `(score, member)`.
    fn zadd(&self, key: &str, pairs: &[(f64, Vec<u8>)]) -> Result<i64, String> {
        let _g = self.lock.lock();
        let (mut z, expire) = match self.load(key)? {
            Some(Entry {
                data: RedisData::ZSet(z),
                expire_at_ms,
            }) => (z, expire_at_ms),
            Some(_) => return Err(WRONGTYPE.into()),
            None => (Vec::new(), None),
        };
        let mut positions: HashMap<Vec<u8>, usize> = HashMap::with_capacity(z.len() + pairs.len());
        for (index, (member, _)) in z.iter().enumerate() {
            positions.entry(member.clone()).or_insert(index);
        }
        let mut added = 0;
        for (score, member) in pairs {
            if let Some(index) = positions.get(member.as_slice()).copied() {
                z[index].1 = *score;
            } else {
                let index = z.len();
                z.push((member.clone(), *score));
                positions.insert(member.clone(), index);
                added += 1;
            }
        }
        z.sort_by(|a, b| {
            a.1.partial_cmp(&b.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then(a.0.cmp(&b.0))
        });
        self.store(
            key,
            &Entry {
                data: RedisData::ZSet(z),
                expire_at_ms: expire,
            },
        )?;
        Ok(added)
    }

    /// `ZRANGE key start stop` → members in rank order (already sorted on store).
    fn zrange(&self, key: &str, start: i64, stop: i64) -> Result<Vec<(Vec<u8>, f64)>, String> {
        let _g = self.lock.lock();
        let z = match self.load(key)? {
            Some(Entry {
                data: RedisData::ZSet(z),
                ..
            }) => z,
            Some(_) => return Err(WRONGTYPE.into()),
            None => return Ok(Vec::new()),
        };
        Ok(range_slice(&z, start, stop).to_vec())
    }

    fn zscore(&self, key: &str, member: &[u8]) -> Result<Option<f64>, String> {
        let _g = self.lock.lock();
        match self.load(key)? {
            Some(Entry {
                data: RedisData::ZSet(z),
                ..
            }) => Ok(z.into_iter().find(|(m, _)| m == member).map(|(_, s)| s)),
            Some(_) => Err(WRONGTYPE.into()),
            None => Ok(None),
        }
    }
}

/// Resolve a Redis-style `[start, stop]` (negative = from-the-end, inclusive)
/// against a slice, returning the sub-slice (empty if the window is empty).
fn range_slice<T>(items: &[T], start: i64, stop: i64) -> &[T] {
    let len = items.len() as i64;
    if len == 0 {
        return &[];
    }
    let norm = |i: i64| -> i64 {
        if i < 0 {
            (len + i).max(0)
        } else {
            i.min(len)
        }
    };
    let s = norm(start);
    // `stop` is inclusive: clamp to len-1 then +1 for an exclusive upper bound.
    let e = if stop < 0 {
        (len + stop + 1).max(0)
    } else {
        (stop + 1).min(len)
    };
    if s >= e {
        &[]
    } else {
        &items[s as usize..e as usize]
    }
}

/// A minimal glob matcher for `SCAN MATCH` — supports `*` (any run) and `?` (one
/// char). Sufficient for the common `prefix:*` patterns; anything fancier degrades
/// to a literal comparison.
fn glob_match(pat: &str, text: &str) -> bool {
    let (p, t): (Vec<char>, Vec<char>) = (pat.chars().collect(), text.chars().collect());
    // Classic two-pointer glob with backtracking on `*`.
    let (mut pi, mut ti, mut star, mut mark) = (0usize, 0usize, None, 0usize);
    while ti < t.len() {
        if pi < p.len() && (p[pi] == '?' || p[pi] == t[ti]) {
            pi += 1;
            ti += 1;
        } else if pi < p.len() && p[pi] == '*' {
            star = Some(pi);
            mark = ti;
            pi += 1;
        } else if let Some(sp) = star {
            pi = sp + 1;
            mark += 1;
            ti = mark;
        } else {
            return false;
        }
    }
    while pi < p.len() && p[pi] == '*' {
        pi += 1;
    }
    pi == p.len()
}

// ── pub/sub registry (CONCEPT:EG-KG.txn.pubsub-transactions) ────────────────────────────────────────────

/// One message delivered to a subscriber's mailbox (CONCEPT:EG-KG.txn.pubsub-transactions). A `Channel`
/// message is rendered as a RESP `message` push; a `Pattern` message (from a glob
/// `PSUBSCRIBE`) as a `pmessage` push carrying the originating pattern.
#[derive(Clone, Debug)]
enum PubMessage {
    Channel {
        channel: Arc<str>,
        payload: Arc<[u8]>,
    },
    Pattern {
        pattern: Arc<str>,
        channel: Arc<str>,
        payload: Arc<[u8]>,
    },
}

impl PubMessage {
    /// Render this delivery as the RESP push frame Redis clients expect.
    fn to_resp(&self) -> Resp {
        match self {
            PubMessage::Channel { channel, payload } => Resp::Push(vec![
                Resp::bulk_str("message"),
                Resp::bulk_str(channel.as_bytes()),
                Resp::Bulk(Some(payload.as_ref().to_vec())),
            ]),
            PubMessage::Pattern {
                pattern,
                channel,
                payload,
            } => Resp::Push(vec![
                Resp::bulk_str("pmessage"),
                Resp::bulk_str(pattern.as_bytes()),
                Resp::bulk_str(channel.as_bytes()),
                Resp::Bulk(Some(payload.as_ref().to_vec())),
            ]),
        }
    }
}

#[derive(Default)]
struct PubSubInner {
    next_id: u64,
    subscription_links: usize,
    subscription_key_bytes: usize,
    /// conn-id → the mailbox sender for that connection.
    conns: HashMap<u64, mpsc::Sender<PubMessage>>,
    /// exact channel → the set of conn-ids subscribed to it.
    channels: HashMap<String, HashSet<u64>>,
    /// glob pattern → the set of conn-ids subscribed to it.
    patterns: HashMap<String, HashSet<u64>>,
}

/// The per-listener publish/subscribe registry (CONCEPT:EG-KG.txn.pubsub-transactions). Shared (via `Arc`)
/// across every connection the listener accepts; each connection registers an
/// bounded mpsc mailbox on connect and drops it on disconnect. `PUBLISH` fans a
/// payload out to every exact-channel subscriber plus every glob-pattern subscriber
/// whose pattern matches, returning the delivery count. All state lives under one
/// `parking_lot::Mutex` — `try_send` is non-blocking, so the lock is
/// never held across an `.await`.
#[derive(Default)]
struct PubSub {
    inner: Mutex<PubSubInner>,
}

impl PubSub {
    /// Register a fresh connection mailbox, returning its unique connection id.
    fn register(&self, tx: mpsc::Sender<PubMessage>) -> Option<u64> {
        let mut g = self.inner.lock();
        if g.conns.len() >= MAX_REDIS_PUBSUB_CONNECTIONS {
            return None;
        }
        g.next_id = g.next_id.checked_add(1)?;
        let id = g.next_id;
        g.conns.insert(id, tx);
        Some(id)
    }

    /// Drop a connection: remove its mailbox and prune it from every channel /
    /// pattern subscription (garbage-collecting now-empty entries).
    fn unregister(&self, id: u64) {
        let mut g = self.inner.lock();
        g.conns.remove(&id);
        let mut removed_links = 0usize;
        let mut removed_key_bytes = 0usize;
        g.channels.retain(|channel, ids| {
            if ids.remove(&id) {
                removed_links += 1;
            }
            let keep = !ids.is_empty();
            if !keep {
                removed_key_bytes = removed_key_bytes.saturating_add(channel.len());
            }
            keep
        });
        g.patterns.retain(|pattern, ids| {
            if ids.remove(&id) {
                removed_links += 1;
            }
            let keep = !ids.is_empty();
            if !keep {
                removed_key_bytes = removed_key_bytes.saturating_add(pattern.len());
            }
            keep
        });
        g.subscription_links = g.subscription_links.saturating_sub(removed_links);
        g.subscription_key_bytes = g.subscription_key_bytes.saturating_sub(removed_key_bytes);
    }

    fn subscribe(&self, id: u64, scope: &str, channel: &str) -> bool {
        let mut g = self.inner.lock();
        if !g.conns.contains_key(&id) {
            return false;
        }
        let channel = scoped_pubsub_key(scope, channel);
        let new_key = !g.channels.contains_key(&channel);
        let next_key_bytes = g.subscription_key_bytes.checked_add(channel.len());
        if g.subscription_links >= MAX_REDIS_PUBSUB_LINKS
            || (new_key && next_key_bytes.is_none_or(|bytes| bytes > MAX_REDIS_PUBSUB_KEY_BYTES))
        {
            return false;
        }
        if g.channels.entry(channel).or_default().insert(id) {
            g.subscription_links += 1;
            if new_key {
                g.subscription_key_bytes = next_key_bytes.unwrap_or(g.subscription_key_bytes);
            }
        }
        true
    }

    fn unsubscribe(&self, id: u64, scope: &str, channel: &str) {
        let mut g = self.inner.lock();
        let channel = scoped_pubsub_key(scope, channel);
        let (removed, empty) = match g.channels.get_mut(&channel) {
            Some(ids) => {
                let removed = ids.remove(&id);
                (removed, ids.is_empty())
            }
            None => (false, false),
        };
        if removed {
            g.subscription_links = g.subscription_links.saturating_sub(1);
        }
        if empty {
            g.channels.remove(&channel);
            g.subscription_key_bytes = g.subscription_key_bytes.saturating_sub(channel.len());
        }
    }

    fn psubscribe(&self, id: u64, scope: &str, pattern: &str) -> bool {
        let mut g = self.inner.lock();
        if !g.conns.contains_key(&id) {
            return false;
        }
        let pattern = scoped_pubsub_key(scope, pattern);
        let new_key = !g.patterns.contains_key(&pattern);
        let next_key_bytes = g.subscription_key_bytes.checked_add(pattern.len());
        if g.subscription_links >= MAX_REDIS_PUBSUB_LINKS
            || (new_key && next_key_bytes.is_none_or(|bytes| bytes > MAX_REDIS_PUBSUB_KEY_BYTES))
        {
            return false;
        }
        if g.patterns.entry(pattern).or_default().insert(id) {
            g.subscription_links += 1;
            if new_key {
                g.subscription_key_bytes = next_key_bytes.unwrap_or(g.subscription_key_bytes);
            }
        }
        true
    }

    fn punsubscribe(&self, id: u64, scope: &str, pattern: &str) {
        let mut g = self.inner.lock();
        let pattern = scoped_pubsub_key(scope, pattern);
        let (removed, empty) = match g.patterns.get_mut(&pattern) {
            Some(ids) => {
                let removed = ids.remove(&id);
                (removed, ids.is_empty())
            }
            None => (false, false),
        };
        if removed {
            g.subscription_links = g.subscription_links.saturating_sub(1);
        }
        if empty {
            g.patterns.remove(&pattern);
            g.subscription_key_bytes = g.subscription_key_bytes.saturating_sub(pattern.len());
        }
    }

    /// Fan `payload` out to every exact subscriber of `channel` and every pattern
    /// subscriber whose glob matches it. Returns the number of deliveries (the
    /// integer `PUBLISH` replies with). A dropped receiver (a connection that has
    /// gone away but not yet unregistered) simply isn't counted.
    fn publish(&self, scope: &str, channel: &str, payload: &[u8]) -> i64 {
        let g = self.inner.lock();
        // Share one immutable payload allocation across every bounded subscriber
        // mailbox.  Cloning a full payload per subscriber lets one large PUBLISH
        // amplify memory linearly with fan-out even when each mailbox is bounded.
        let payload: Arc<[u8]> = Arc::from(payload);
        let wire_channel: Arc<str> = Arc::from(channel);
        let scoped_channel = scoped_pubsub_key(scope, channel);
        publish_to_channel_subscribers(&g, &scoped_channel, &wire_channel, &payload)
            + publish_to_pattern_subscribers(&g, scope, &scoped_channel, &wire_channel, &payload)
    }
}

/// Fan `payload` out to every EXACT subscriber of `scoped_channel`.
fn publish_to_channel_subscribers(
    g: &PubSubInner,
    scoped_channel: &str,
    wire_channel: &Arc<str>,
    payload: &Arc<[u8]>,
) -> i64 {
    let mut count = 0i64;
    if let Some(ids) = g.channels.get(scoped_channel) {
        for id in ids {
            if let Some(tx) = g.conns.get(id) {
                let msg = PubMessage::Channel {
                    channel: Arc::clone(wire_channel),
                    payload: Arc::clone(payload),
                };
                if tx.try_send(msg).is_ok() {
                    count += 1;
                }
            }
        }
    }
    count
}

/// Fan `payload` out to every glob-PATTERN subscriber whose pattern matches
/// `scoped_channel`.
fn publish_to_pattern_subscribers(
    g: &PubSubInner,
    scope: &str,
    scoped_channel: &str,
    wire_channel: &Arc<str>,
    payload: &Arc<[u8]>,
) -> i64 {
    let mut count = 0i64;
    let scope_prefix = format!("{scope}\0");
    for (pat, ids) in g.patterns.iter() {
        if !glob_match(pat, scoped_channel) {
            continue;
        }
        let Some(wire_pattern) = pat.strip_prefix(&scope_prefix) else {
            continue;
        };
        let pattern: Arc<str> = Arc::from(wire_pattern);
        for id in ids {
            if let Some(tx) = g.conns.get(id) {
                let msg = PubMessage::Pattern {
                    pattern: Arc::clone(&pattern),
                    channel: Arc::clone(wire_channel),
                    payload: Arc::clone(payload),
                };
                if tx.try_send(msg).is_ok() {
                    count += 1;
                }
            }
        }
    }
    count
}

fn scoped_pubsub_key(scope: &str, name: &str) -> String {
    format!("{scope}\0{name}")
}

// ── RESP2 / RESP3 codec (CONCEPT:EG-KG.ontology.resp2-resp3-codec-round) ─────────────────────────────────────────

/// A RESP reply value. Version-aware: [`encode`](Resp::encode) renders the RESP3
/// extended types (map/set/double/boolean/null) natively when the connection has
/// upgraded via `HELLO 3`, and downgrades them to their RESP2 equivalents
/// (array/bulk/integer/null-bulk) otherwise.
#[derive(Clone, Debug, PartialEq)]
enum Resp {
    Simple(String),
    Error(String),
    Int(i64),
    Bulk(Option<Vec<u8>>),
    Array(Option<Vec<Resp>>),
    Map(Vec<(Resp, Resp)>),
    Set(Vec<Resp>),
    /// A RESP3 push message (`>`, used for pub/sub delivery + subscribe confirms,
    /// CONCEPT:EG-KG.txn.pubsub-transactions). Downgrades to a plain array (`*`) on RESP2 — the RESP2
    /// wire has no distinct push type, exactly how real Redis behaves.
    Push(Vec<Resp>),
    Double(f64),
    Bool(bool),
    Null,
}

impl Resp {
    fn bulk_str(s: impl Into<Vec<u8>>) -> Resp {
        Resp::Bulk(Some(s.into()))
    }

    /// Serialize into RESP bytes for protocol version `proto` (2 or 3).
    fn encode(&self, proto: u8, out: &mut Vec<u8>) {
        match self {
            Resp::Simple(s) => {
                out.push(b'+');
                out.extend_from_slice(s.as_bytes());
                out.extend_from_slice(b"\r\n");
            }
            Resp::Error(s) => {
                out.push(b'-');
                out.extend_from_slice(s.as_bytes());
                out.extend_from_slice(b"\r\n");
            }
            Resp::Int(i) => {
                out.push(b':');
                out.extend_from_slice(i.to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
            }
            Resp::Bulk(Some(b)) => {
                out.push(b'$');
                out.extend_from_slice(b.len().to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
                out.extend_from_slice(b);
                out.extend_from_slice(b"\r\n");
            }
            Resp::Bulk(None) | Resp::Null => encode_null_bulk(proto, out),
            Resp::Array(Some(items)) => encode_aggregate(b'*', items, proto, out),
            Resp::Array(None) => encode_null_array(proto, out),
            Resp::Map(pairs) => encode_map(pairs, proto, out),
            Resp::Set(items) => encode_aggregate(b'~', items, proto, out),
            Resp::Push(items) => encode_aggregate(b'>', items, proto, out),
            Resp::Double(d) => encode_double(*d, proto, out),
            Resp::Bool(b) => encode_bool(*b, proto, out),
        }
    }
}

fn encode_null_bulk(proto: u8, out: &mut Vec<u8>) {
    if proto >= 3 {
        out.extend_from_slice(b"_\r\n");
    } else {
        out.extend_from_slice(b"$-1\r\n");
    }
}

fn encode_null_array(proto: u8, out: &mut Vec<u8>) {
    if proto >= 3 {
        out.extend_from_slice(b"_\r\n");
    } else {
        out.extend_from_slice(b"*-1\r\n");
    }
}

fn encode_map(pairs: &[(Resp, Resp)], proto: u8, out: &mut Vec<u8>) {
    if proto >= 3 {
        out.push(b'%');
        out.extend_from_slice(pairs.len().to_string().as_bytes());
        out.extend_from_slice(b"\r\n");
    } else {
        // RESP2: a flat array of 2N elements.
        out.push(b'*');
        out.extend_from_slice((pairs.len() * 2).to_string().as_bytes());
        out.extend_from_slice(b"\r\n");
    }
    for (k, v) in pairs {
        k.encode(proto, out);
        v.encode(proto, out);
    }
}

/// `Set`/`Push` share the same shape on the wire: a RESP3-typed aggregate
/// (`~`/`>`) that downgrades to a plain array (`*`) on RESP2 — the RESP2 wire
/// has no distinct set/push type, exactly how real Redis behaves.
fn encode_aggregate(resp3_prefix: u8, items: &[Resp], proto: u8, out: &mut Vec<u8>) {
    out.push(if proto >= 3 { resp3_prefix } else { b'*' });
    out.extend_from_slice(items.len().to_string().as_bytes());
    out.extend_from_slice(b"\r\n");
    for it in items {
        it.encode(proto, out);
    }
}

fn encode_double(d: f64, proto: u8, out: &mut Vec<u8>) {
    if proto >= 3 {
        out.push(b',');
        out.extend_from_slice(fmt_double(d).as_bytes());
        out.extend_from_slice(b"\r\n");
    } else {
        Resp::bulk_str(fmt_double(d)).encode(proto, out);
    }
}

fn encode_bool(b: bool, proto: u8, out: &mut Vec<u8>) {
    if proto >= 3 {
        out.extend_from_slice(if b { b"#t\r\n" } else { b"#f\r\n" });
    } else {
        Resp::Int(if b { 1 } else { 0 }).encode(proto, out);
    }
}

/// Conservative pre-encoding budget. It counts retained payload bytes plus fixed
/// framing/headroom per nested value, preventing a small MULTI request from
/// materializing or encoding an arbitrarily large aggregate response.
fn response_cost(resp: &Resp, total: &mut usize, items: &mut usize) -> bool {
    *items = match (*items).checked_add(1) {
        Some(value) if value <= MAX_STORED_ENTRY_ITEMS => value,
        _ => return false,
    };
    *total = match (*total).checked_add(32) {
        Some(value) if value <= MAX_REDIS_RESPONSE_BYTES => value,
        _ => return false,
    };
    let payload_bytes = match resp {
        Resp::Simple(value) | Resp::Error(value) => value.len(),
        Resp::Bulk(Some(value)) => value.len(),
        Resp::Array(Some(values)) | Resp::Set(values) | Resp::Push(values) => {
            return values
                .iter()
                .all(|value| response_cost(value, total, items));
        }
        Resp::Map(values) => {
            return values.iter().all(|(key, value)| {
                response_cost(key, total, items) && response_cost(value, total, items)
            });
        }
        Resp::Int(_)
        | Resp::Bulk(None)
        | Resp::Array(None)
        | Resp::Double(_)
        | Resp::Bool(_)
        | Resp::Null => 0,
    };
    *total = match (*total).checked_add(payload_bytes) {
        Some(value) if value <= MAX_REDIS_RESPONSE_BYTES => value,
        _ => return false,
    };
    true
}

fn responses_within_budget(responses: &[Resp]) -> bool {
    let mut total = 0usize;
    let mut items = 0usize;
    responses
        .iter()
        .all(|response| response_cost(response, &mut total, &mut items))
}

/// Format an `f64` the Redis way (bare integer when whole, else the shortest
/// round-trippable decimal).
fn fmt_double(d: f64) -> String {
    if d.fract() == 0.0 && d.abs() < 1e15 {
        format!("{}", d as i64)
    } else {
        format!("{d}")
    }
}

/// Try to parse ONE complete command from `buf`. Supports the RESP array-of-bulk
/// form (`redis-cli`) and the inline (whitespace) form (telnet). Returns
/// `Ok(None)` when the buffer holds only a partial command (read more),
/// `Ok(Some((args, consumed)))` on a complete one, and `Err` on a protocol error.
fn try_parse_command(buf: &[u8]) -> CommandParse {
    if buf.is_empty() {
        return Ok(None);
    }
    if buf[0] == b'*' {
        parse_array_command(buf)
    } else {
        parse_inline_command(buf)
    }
}

/// Find the byte index just past the next CRLF, returning `(line_bytes, next)`.
fn read_crlf_line(buf: &[u8], from: usize) -> Option<(&[u8], usize)> {
    let mut i = from;
    while i + 1 < buf.len() {
        if buf[i] == b'\r' && buf[i + 1] == b'\n' {
            return Some((&buf[from..i], i + 2));
        }
        i += 1;
    }
    None
}

fn parse_inline_command(buf: &[u8]) -> CommandParse {
    match read_crlf_line(buf, 0) {
        Some((line, next)) => {
            if line.len() > MAX_RESP_LINE_BYTES {
                return Err("ERR Protocol error: too big inline request".into());
            }
            let mut args = Vec::new();
            for argument in line
                .split(|b| *b == b' ' || *b == b'\t')
                .filter(|argument| !argument.is_empty())
            {
                if args.len() >= MAX_RESP_ARGUMENTS {
                    return Err("ERR Protocol error: too many arguments".into());
                }
                args.push(argument.to_vec());
            }
            Ok(Some((args, next)))
        }
        None => {
            // Guard against an unbounded inline line with no terminator.
            if buf.len() > MAX_RESP_LINE_BYTES {
                Err("ERR Protocol error: too big inline request".into())
            } else {
                Ok(None)
            }
        }
    }
}

fn parse_array_command(buf: &[u8]) -> CommandParse {
    let (header, mut pos) = match read_crlf_line(buf, 0) {
        Some(v) => v,
        None if buf.len() > MAX_RESP_LINE_BYTES => {
            return Err("ERR Protocol error: multibulk header too large".into());
        }
        None => return Ok(None),
    };
    if header.len() > MAX_RESP_LINE_BYTES {
        return Err("ERR Protocol error: multibulk header too large".into());
    }
    let count: i64 = std::str::from_utf8(&header[1..])
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| "ERR Protocol error: invalid multibulk length".to_string())?;
    if count == -1 {
        return Ok(Some((Vec::new(), pos)));
    }
    if count < -1 {
        return Err("ERR Protocol error: invalid multibulk length".into());
    }
    let count = usize::try_from(count)
        .map_err(|_| "ERR Protocol error: invalid multibulk length".to_string())?;
    if count > MAX_RESP_ARGUMENTS {
        return Err("ERR Protocol error: too many arguments".into());
    }
    let mut args = Vec::with_capacity(count);
    let mut aggregate_bytes = 0usize;
    for _ in 0..count {
        match parse_one_bulk_argument(buf, pos, &mut aggregate_bytes)? {
            Some((bytes, new_pos)) => {
                args.push(bytes);
                pos = new_pos;
            }
            None => return Ok(None), // bytes + trailing CRLF not all here yet
        }
    }
    Ok(Some((args, pos)))
}

/// Parse one `$<len>\r\n<bytes>\r\n` bulk argument starting at `pos`, tracking
/// the running `aggregate_bytes` across the whole multibulk command against
/// `MAX_REDIS_REQUEST_BYTES`. `Ok(None)` means the buffer doesn't yet hold the
/// complete argument (the connection driver should read more and retry).
fn parse_one_bulk_argument(
    buf: &[u8],
    pos: usize,
    aggregate_bytes: &mut usize,
) -> Result<Option<(Vec<u8>, usize)>, String> {
    let Some((blen, after_len)) = parse_bulk_length(buf, pos)? else {
        return Ok(None);
    };
    if blen == -1 {
        return Ok(Some((Vec::new(), after_len)));
    }
    if blen < -1 {
        return Err("ERR Protocol error: invalid bulk length".into());
    }
    let blen =
        usize::try_from(blen).map_err(|_| "ERR Protocol error: invalid bulk length".to_string())?;
    read_bulk_payload(buf, after_len, blen, aggregate_bytes)
}

/// Parse a bulk argument's `$<len>\r\n` length header. `Ok(None)` means the
/// header itself hasn't fully arrived yet.
fn parse_bulk_length(buf: &[u8], pos: usize) -> Result<Option<(i64, usize)>, String> {
    let (blen_line, after_len) = match read_crlf_line(buf, pos) {
        Some(v) => v,
        None if buf.len().saturating_sub(pos) > MAX_RESP_LINE_BYTES => {
            return Err("ERR Protocol error: bulk header too large".into());
        }
        None => return Ok(None),
    };
    if blen_line.len() > MAX_RESP_LINE_BYTES {
        return Err("ERR Protocol error: bulk header too large".into());
    }
    if blen_line.first() != Some(&b'$') {
        return Err("ERR Protocol error: expected '$'".into());
    }
    let blen: i64 = std::str::from_utf8(&blen_line[1..])
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| "ERR Protocol error: invalid bulk length".to_string())?;
    Ok(Some((blen, after_len)))
}

/// Read a bulk argument's `blen`-byte payload plus its trailing CRLF, starting
/// at `start`, and fold `blen` into the running `aggregate_bytes` bound.
/// `Ok(None)` means the payload + terminator hasn't fully arrived yet.
fn read_bulk_payload(
    buf: &[u8],
    start: usize,
    blen: usize,
    aggregate_bytes: &mut usize,
) -> Result<Option<(Vec<u8>, usize)>, String> {
    if blen > MAX_RESP_BULK_BYTES {
        return Err("ERR Protocol error: bulk request too large".into());
    }
    *aggregate_bytes = aggregate_bytes
        .checked_add(blen)
        .filter(|total| *total <= MAX_REDIS_REQUEST_BYTES)
        .ok_or_else(|| "ERR Protocol error: request too large".to_string())?;
    let end = start
        .checked_add(blen)
        .ok_or_else(|| "ERR Protocol error: bulk length overflow".to_string())?;
    let framed_end = end
        .checked_add(2)
        .ok_or_else(|| "ERR Protocol error: bulk length overflow".to_string())?;
    if buf.len() < framed_end {
        return Ok(None);
    }
    if &buf[end..framed_end] != b"\r\n" {
        return Err("ERR Protocol error: invalid bulk terminator".into());
    }
    Ok(Some((buf[start..end].to_vec(), end + 2)))
}

// ── command dispatch (CONCEPT:EG-KG.ontology.resp2-resp3-codec-round) ─────────────────────────────────────────────

/// Per-connection mutable state threaded through command execution. Carries the
/// RESP version + auth flag, the pub/sub subscription sets, and the `MULTI`
/// transaction queue (CONCEPT:EG-KG.ontology.resp2-resp3-codec-round core; CONCEPT:EG-KG.txn.pubsub-transactions pub/sub + transactions).
struct ConnState {
    proto: u8,
    /// Present only after HMAC credential verification. The value is a
    /// secret-keyed pseudonym, never the supplied principal.
    actor_scope: Option<String>,
    quit: bool,
    /// This connection's unique id in the [`PubSub`] registry (0 until registered;
    /// the pure-`execute` unit tests never register, which is fine).
    id: u64,
    /// Channels this connection is `SUBSCRIBE`d to (CONCEPT:EG-KG.txn.pubsub-transactions).
    sub_channels: HashSet<String>,
    /// Glob patterns this connection is `PSUBSCRIBE`d to (CONCEPT:EG-KG.txn.pubsub-transactions).
    sub_patterns: HashSet<String>,
    /// Total retained channel/pattern bytes across both subscription sets.
    sub_bytes: usize,
    /// `true` between `MULTI` and `EXEC`/`DISCARD`: commands are queued not run.
    in_multi: bool,
    /// The queued commands awaiting `EXEC` (CONCEPT:EG-KG.txn.pubsub-transactions).
    queued: Vec<Vec<Vec<u8>>>,
    /// Aggregate bytes retained by `queued`; commands have already left the
    /// socket buffer, so this independent accounting prevents an unbounded
    /// transaction from accumulating across otherwise-small requests.
    queued_bytes: usize,
    /// Set when a queued command was malformed → `EXEC` aborts with `EXECABORT`.
    multi_dirty: bool,
}

impl ConnState {
    fn new(proto: u8) -> Self {
        ConnState {
            proto,
            actor_scope: None,
            quit: false,
            id: 0,
            sub_channels: HashSet::new(),
            sub_patterns: HashSet::new(),
            sub_bytes: 0,
            in_multi: false,
            queued: Vec::new(),
            queued_bytes: 0,
            multi_dirty: false,
        }
    }

    /// Total live subscriptions (channels + patterns) — the count Redis echoes in
    /// every subscribe/unsubscribe confirmation.
    fn sub_count(&self) -> i64 {
        (self.sub_channels.len() + self.sub_patterns.len()) as i64
    }

    fn authenticated_scope(&self) -> Result<&str, Resp> {
        self.actor_scope
            .as_deref()
            .ok_or_else(|| Resp::Error("NOAUTH Authentication required.".into()))
    }
}

/// Uppercase an argument for case-insensitive command / option matching.
fn upper(b: &[u8]) -> String {
    if b.len() > MAX_REDIS_COMMAND_BYTES {
        return String::new();
    }
    std::str::from_utf8(b)
        .map(str::to_ascii_uppercase)
        .unwrap_or_default()
}

/// Execute ONE parsed command against the store, returning the reply. Handshake /
/// session commands (`HELLO`/`AUTH`/`QUIT`/`SELECT`) mutate `conn`.
fn execute(store: &RedisStore, args: &[Vec<u8>], conn: &mut ConnState, auth_secret: &str) -> Resp {
    if args.is_empty() {
        return Resp::Error("ERR empty command".into());
    }
    let cmd = upper(&args[0]);

    // Only authentication and disconnect are available before identity binding.
    if let Some(resp) = execute_preauth(&cmd, args, conn, auth_secret) {
        return resp;
    }

    let scope = match conn.authenticated_scope() {
        Ok(scope) => scope,
        Err(error) => return error,
    };

    if let Some(resp) = execute_quick_verb(&cmd, args) {
        return resp;
    }

    let scoped_store = store.scoped(scope);
    match execute_data(&scoped_store, &cmd, args, conn.proto) {
        Ok(r) => r,
        Err(e) => Resp::Error(e),
    }
}

/// The verbs available before identity binding: `QUIT`, `HELLO`, and `AUTH`.
/// `None` means the caller should fall through to the authenticated path.
fn execute_preauth(
    cmd: &str,
    args: &[Vec<u8>],
    conn: &mut ConnState,
    auth_secret: &str,
) -> Option<Resp> {
    match cmd {
        "QUIT" => {
            conn.quit = true;
            Some(Resp::Simple("OK".into()))
        }
        "HELLO" => Some(hello(args, conn, auth_secret)),
        "AUTH" => {
            if args.len() != 3 {
                return Some(Resp::Error(
                    "ERR AUTH requires principal and credential".into(),
                ));
            }
            Some(
                match authenticate_redis_principal(auth_secret, &args[1], &args[2]) {
                    Some(scope) => {
                        conn.actor_scope = Some(scope);
                        Resp::Simple("OK".into())
                    }
                    None => Resp::Error("WRONGPASS invalid username-password pair".into()),
                },
            )
        }
        _ => None,
    }
}

/// Verbs answered directly without touching the data store. `None` means the
/// caller should fall through to [`execute_data`].
fn execute_quick_verb(cmd: &str, args: &[Vec<u8>]) -> Option<Resp> {
    match cmd {
        "PING" => Some(match args.get(1) {
            Some(msg) => Resp::Bulk(Some(msg.clone())),
            None => Resp::Simple("PONG".into()),
        }),
        "ECHO" => Some(match args.get(1) {
            Some(msg) => Resp::Bulk(Some(msg.clone())),
            None => Resp::Error("ERR wrong number of arguments for 'echo'".into()),
        }),
        "SELECT" => Some(Resp::Simple("OK".into())),
        "COMMAND" | "CONFIG" => Some(Resp::Array(Some(Vec::new()))),
        "CLIENT" => Some(Resp::Simple("OK".into())),
        _ => None,
    }
}

fn authenticate_redis_principal(
    auth_secret: &str,
    principal: &[u8],
    credential: &[u8],
) -> Option<String> {
    let principal = std::str::from_utf8(principal).ok()?;
    if !verify_redis_password(auth_secret, principal, credential) {
        return None;
    }
    crate::server::pseudonymous_broker_actor(auth_secret, principal).ok()
}

/// If `args[1]` parses as a protocol version number, validate and apply it
/// (advancing `*i` past it). A non-numeric or absent `args[1]` is not a
/// protover at all — leaves `*i` untouched so it's parsed as a `HELLO` option
/// instead, matching real Redis's argument-shape sniffing.
fn hello_apply_protover(args: &[Vec<u8>], conn: &mut ConnState, i: &mut usize) -> Option<Resp> {
    let value = args.get(1)?;
    let p: u8 = std::str::from_utf8(value)
        .unwrap_or_default()
        .parse()
        .ok()?;
    if p != 2 && p != 3 {
        return Some(Resp::Error("NOPROTO unsupported protocol version".into()));
    }
    conn.proto = p;
    *i = 2;
    None
}

/// Apply every remaining `HELLO` option starting at `i` (today, only
/// `AUTH <principal> <credential>`). `None` means all options were valid.
fn hello_apply_options(
    args: &[Vec<u8>],
    mut i: usize,
    conn: &mut ConnState,
    auth_secret: &str,
) -> Option<Resp> {
    while i < args.len() {
        if upper(&args[i]) == "AUTH" && i + 2 < args.len() {
            match authenticate_redis_principal(auth_secret, &args[i + 1], &args[i + 2]) {
                Some(scope) => conn.actor_scope = Some(scope),
                None => {
                    return Some(Resp::Error(
                        "WRONGPASS invalid username-password pair".into(),
                    ))
                }
            }
            i += 3;
        } else {
            return Some(Resp::Error("ERR invalid HELLO option".into()));
        }
    }
    None
}

/// The `HELLO` handshake upgrades RESP and performs mandatory identity binding
/// before returning server metadata.
fn hello(args: &[Vec<u8>], conn: &mut ConnState, auth_secret: &str) -> Resp {
    let mut i = 1;
    if let Some(resp) = hello_apply_protover(args, conn, &mut i) {
        return resp;
    }
    // `HELLO ... AUTH <principal> <credential>` binds a fresh connection. An
    // already-authenticated connection may renegotiate only the RESP version.
    if let Some(resp) = hello_apply_options(args, i, conn, auth_secret) {
        return resp;
    }
    if conn.actor_scope.is_none() {
        return Resp::Error("NOAUTH Authentication required.".into());
    }
    Resp::Map(vec![
        (Resp::bulk_str("server"), Resp::bulk_str("epistemic-graph")),
        (Resp::bulk_str("version"), Resp::bulk_str("2.1.0")),
        (Resp::bulk_str("proto"), Resp::Int(conn.proto as i64)),
        (Resp::bulk_str("id"), Resp::Int(1)),
        (Resp::bulk_str("mode"), Resp::bulk_str("standalone")),
        (Resp::bulk_str("role"), Resp::bulk_str("master")),
        (Resp::bulk_str("modules"), Resp::Array(Some(Vec::new()))),
    ])
}

/// Execute a data command (auth already checked). `Err(msg)` becomes a RESP error.
// CXA-EG-03 refactor: `execute_data` was CCN 90 as one `match cmd { .. }`
// with ~24 arms, several containing their own inner loops/matches. Same
// technique as the sibling `try_handle`/`apply_txn_op` dispatchers in this
// lane: a `match` costs lizard ~1 regardless of arm count, so the fix is
// giving every substantive arm a single tail-call to a named `cmd_*`
// helper (verbatim original arm body, just moved) and leaving only the
// handful of already-single-expression arms (TTL/INCR/DECR/LLEN/TYPE)
// inline. The `key` closure (captured `args`/`cmd`) becomes a bare fn
// taking both explicitly, since a helper fn can't share its closure.
fn redis_key(args: &[Vec<u8>], cmd: &str, n: usize) -> Result<String, String> {
    args.get(n)
        .map(|bytes| utf8_argument(bytes, MAX_REDIS_KEY_BYTES))
        .transpose()?
        .ok_or_else(|| {
            format!(
                "ERR wrong number of arguments for '{}'",
                cmd.to_ascii_lowercase()
            )
        })
}

fn cmd_set(store: &RedisStore, cmd: &str, args: &[Vec<u8>]) -> Result<Resp, String> {
    let k = redis_key(args, cmd, 1)?;
    let v = args
        .get(2)
        .cloned()
        .ok_or_else(|| "ERR wrong number of arguments for 'set'".to_string())?;
    let (mut expire, mut nx, mut xx) = (None, false, false);
    let mut i = 3;
    while i < args.len() {
        match upper(&args[i]).as_str() {
            "EX" => {
                let s: u64 = parse_num(args.get(i + 1))?;
                expire = Some(s * 1000);
                i += 2;
            }
            "PX" => {
                let ms: u64 = parse_num(args.get(i + 1))?;
                expire = Some(ms);
                i += 2;
            }
            "NX" => {
                nx = true;
                i += 1;
            }
            "XX" => {
                xx = true;
                i += 1;
            }
            other => return Err(format!("ERR syntax error near '{other}'")),
        }
    }
    if store.set(&k, v, expire, nx, xx)? {
        Ok(Resp::Simple("OK".into()))
    } else {
        Ok(Resp::Null)
    }
}

fn redis_keys_from_args(args: &[Vec<u8>]) -> Result<Vec<String>, String> {
    args[1..]
        .iter()
        .map(|bytes| utf8_argument(bytes, MAX_REDIS_KEY_BYTES))
        .collect::<Result<_, _>>()
}

fn cmd_expire(store: &RedisStore, cmd: &str, args: &[Vec<u8>]) -> Result<Resp, String> {
    let k = redis_key(args, cmd, 1)?;
    let secs: i64 = parse_num(args.get(2))?;
    Ok(Resp::Int(store.expire(&k, secs)? as i64))
}

fn cmd_mget(store: &RedisStore, args: &[Vec<u8>]) -> Result<Resp, String> {
    let mut out = Vec::new();
    for a in &args[1..] {
        let k = utf8_argument(a, MAX_REDIS_KEY_BYTES)?;
        out.push(Resp::Bulk(store.get(&k)?));
    }
    Ok(Resp::Array(Some(out)))
}

fn cmd_mset(store: &RedisStore, args: &[Vec<u8>]) -> Result<Resp, String> {
    if args.len() < 3 || !(args.len() - 1).is_multiple_of(2) {
        return Err("ERR wrong number of arguments for 'mset'".into());
    }
    let mut i = 1;
    while i + 1 < args.len() {
        let k = utf8_argument(&args[i], MAX_REDIS_KEY_BYTES)?;
        store.set(&k, args[i + 1].clone(), None, false, false)?;
        i += 2;
    }
    Ok(Resp::Simple("OK".into()))
}

fn cmd_hset(store: &RedisStore, cmd: &str, args: &[Vec<u8>]) -> Result<Resp, String> {
    let k = redis_key(args, cmd, 1)?;
    if args.len() < 4 || !(args.len() - 2).is_multiple_of(2) {
        return Err("ERR wrong number of arguments for 'hset'".into());
    }
    let mut pairs = Vec::new();
    let mut i = 2;
    while i + 1 < args.len() {
        pairs.push((args[i].clone(), args[i + 1].clone()));
        i += 2;
    }
    Ok(Resp::Int(store.hset(&k, &pairs)?))
}

fn cmd_hget(store: &RedisStore, cmd: &str, args: &[Vec<u8>]) -> Result<Resp, String> {
    let k = redis_key(args, cmd, 1)?;
    let field = args
        .get(2)
        .ok_or_else(|| "ERR wrong number of arguments for 'hget'".to_string())?;
    Ok(Resp::Bulk(store.hget(&k, field)?))
}

fn cmd_hgetall(store: &RedisStore, cmd: &str, args: &[Vec<u8>]) -> Result<Resp, String> {
    let pairs = store.hgetall(&redis_key(args, cmd, 1)?)?;
    let map = pairs
        .into_iter()
        .map(|(f, v)| (Resp::Bulk(Some(f)), Resp::Bulk(Some(v))))
        .collect();
    Ok(Resp::Map(map))
}

fn cmd_hdel(store: &RedisStore, cmd: &str, args: &[Vec<u8>]) -> Result<Resp, String> {
    let k = redis_key(args, cmd, 1)?;
    let fields: Vec<Vec<u8>> = args[2..].to_vec();
    Ok(Resp::Int(store.hdel(&k, &fields)?))
}

fn cmd_push(store: &RedisStore, cmd: &str, args: &[Vec<u8>]) -> Result<Resp, String> {
    let k = redis_key(args, cmd, 1)?;
    let vals: Vec<Vec<u8>> = args[2..].to_vec();
    if vals.is_empty() {
        return Err(format!(
            "ERR wrong number of arguments for '{}'",
            cmd.to_ascii_lowercase()
        ));
    }
    Ok(Resp::Int(store.push(&k, &vals, cmd == "LPUSH")?))
}

fn cmd_lrange(store: &RedisStore, cmd: &str, args: &[Vec<u8>]) -> Result<Resp, String> {
    let k = redis_key(args, cmd, 1)?;
    let start: i64 = parse_num(args.get(2))?;
    let stop: i64 = parse_num(args.get(3))?;
    let items = store
        .lrange(&k, start, stop)?
        .into_iter()
        .map(|v| Resp::Bulk(Some(v)))
        .collect();
    Ok(Resp::Array(Some(items)))
}

fn cmd_sadd(store: &RedisStore, cmd: &str, args: &[Vec<u8>]) -> Result<Resp, String> {
    let k = redis_key(args, cmd, 1)?;
    let members: Vec<Vec<u8>> = args[2..].to_vec();
    if members.is_empty() {
        return Err("ERR wrong number of arguments for 'sadd'".into());
    }
    Ok(Resp::Int(store.sadd(&k, &members)?))
}

fn cmd_smembers(store: &RedisStore, cmd: &str, args: &[Vec<u8>]) -> Result<Resp, String> {
    let items = store
        .smembers(&redis_key(args, cmd, 1)?)?
        .into_iter()
        .map(|v| Resp::Bulk(Some(v)))
        .collect();
    Ok(Resp::Set(items))
}

fn cmd_srem(store: &RedisStore, cmd: &str, args: &[Vec<u8>]) -> Result<Resp, String> {
    let k = redis_key(args, cmd, 1)?;
    let members: Vec<Vec<u8>> = args[2..].to_vec();
    Ok(Resp::Int(store.srem(&k, &members)?))
}

fn cmd_zadd(store: &RedisStore, cmd: &str, args: &[Vec<u8>]) -> Result<Resp, String> {
    let k = redis_key(args, cmd, 1)?;
    if args.len() < 4 || !(args.len() - 2).is_multiple_of(2) {
        return Err("ERR wrong number of arguments for 'zadd'".into());
    }
    let mut pairs = Vec::new();
    let mut i = 2;
    while i + 1 < args.len() {
        let score: f64 = std::str::from_utf8(&args[i])
            .ok()
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| "ERR value is not a valid float".to_string())?;
        pairs.push((score, args[i + 1].clone()));
        i += 2;
    }
    Ok(Resp::Int(store.zadd(&k, &pairs)?))
}

fn zrange_member_resp(score: f64, proto: u8) -> Resp {
    if proto >= 3 {
        Resp::Double(score)
    } else {
        Resp::bulk_str(fmt_double(score))
    }
}

fn cmd_zrange(store: &RedisStore, cmd: &str, args: &[Vec<u8>], proto: u8) -> Result<Resp, String> {
    let k = redis_key(args, cmd, 1)?;
    let start: i64 = parse_num(args.get(2))?;
    let stop: i64 = parse_num(args.get(3))?;
    let withscores = args
        .get(4)
        .map(|b| upper(b) == "WITHSCORES")
        .unwrap_or(false);
    let z = store.zrange(&k, start, stop)?;
    let mut out = Vec::new();
    for (m, s) in z {
        out.push(Resp::Bulk(Some(m)));
        if withscores {
            out.push(zrange_member_resp(s, proto));
        }
    }
    Ok(Resp::Array(Some(out)))
}

fn cmd_zscore(store: &RedisStore, cmd: &str, args: &[Vec<u8>]) -> Result<Resp, String> {
    let k = redis_key(args, cmd, 1)?;
    let member = args
        .get(2)
        .ok_or_else(|| "ERR wrong number of arguments for 'zscore'".to_string())?;
    match store.zscore(&k, member)? {
        Some(s) => Ok(Resp::bulk_str(fmt_double(s))),
        None => Ok(Resp::Null),
    }
}

fn cmd_scan(store: &RedisStore, args: &[Vec<u8>]) -> Result<Resp, String> {
    // SCAN cursor [MATCH pattern] [COUNT n] [TYPE t] — one-shot (cursor 0).
    let mut pattern = None;
    let mut i = 2;
    while i < args.len() {
        match upper(&args[i]).as_str() {
            "MATCH" => {
                pattern = args
                    .get(i + 1)
                    .map(|bytes| utf8_argument(bytes, MAX_REDIS_KEY_BYTES))
                    .transpose()?;
                i += 2;
            }
            "COUNT" | "TYPE" => i += 2, // accepted, ignored (one-shot scan)
            _ => i += 1,
        }
    }
    let keys = store.scan(pattern.as_deref())?;
    let items = keys.into_iter().map(Resp::bulk_str).collect();
    Ok(Resp::Array(Some(vec![
        Resp::bulk_str("0"),
        Resp::Array(Some(items)),
    ])))
}

fn cmd_get(store: &RedisStore, cmd: &str, args: &[Vec<u8>]) -> Result<Resp, String> {
    Ok(Resp::Bulk(store.get(&redis_key(args, cmd, 1)?)?))
}

fn cmd_del_exec(store: &RedisStore, args: &[Vec<u8>]) -> Result<Resp, String> {
    let keys = redis_keys_from_args(args)?;
    let refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
    Ok(Resp::Int(store.del(&refs)?))
}

fn cmd_exists(store: &RedisStore, args: &[Vec<u8>]) -> Result<Resp, String> {
    let keys = redis_keys_from_args(args)?;
    let refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
    Ok(Resp::Int(store.exists(&refs)?))
}

fn cmd_ttl(store: &RedisStore, cmd: &str, args: &[Vec<u8>]) -> Result<Resp, String> {
    Ok(Resp::Int(store.ttl(&redis_key(args, cmd, 1)?)?))
}

fn cmd_incr(store: &RedisStore, cmd: &str, args: &[Vec<u8>]) -> Result<Resp, String> {
    Ok(Resp::Int(store.incr_by(&redis_key(args, cmd, 1)?, 1)?))
}

fn cmd_decr(store: &RedisStore, cmd: &str, args: &[Vec<u8>]) -> Result<Resp, String> {
    Ok(Resp::Int(store.incr_by(&redis_key(args, cmd, 1)?, -1)?))
}

fn cmd_llen(store: &RedisStore, cmd: &str, args: &[Vec<u8>]) -> Result<Resp, String> {
    Ok(Resp::Int(store.llen(&redis_key(args, cmd, 1)?)?))
}

fn cmd_type(store: &RedisStore, cmd: &str, args: &[Vec<u8>]) -> Result<Resp, String> {
    Ok(Resp::Simple(
        store.type_of(&redis_key(args, cmd, 1)?)?.into(),
    ))
}

fn execute_data(
    store: &RedisStore,
    cmd: &str,
    args: &[Vec<u8>],
    proto: u8,
) -> Result<Resp, String> {
    match cmd {
        "SET" => cmd_set(store, cmd, args),
        "GET" => cmd_get(store, cmd, args),
        "DEL" => cmd_del_exec(store, args),
        "EXISTS" => cmd_exists(store, args),
        "EXPIRE" => cmd_expire(store, cmd, args),
        "TTL" => cmd_ttl(store, cmd, args),
        "INCR" => cmd_incr(store, cmd, args),
        "DECR" => cmd_decr(store, cmd, args),
        "MGET" => cmd_mget(store, args),
        "MSET" => cmd_mset(store, args),
        "HSET" => cmd_hset(store, cmd, args),
        "HGET" => cmd_hget(store, cmd, args),
        "HGETALL" => cmd_hgetall(store, cmd, args),
        "HDEL" => cmd_hdel(store, cmd, args),
        "LPUSH" | "RPUSH" => cmd_push(store, cmd, args),
        "LRANGE" => cmd_lrange(store, cmd, args),
        "LLEN" => cmd_llen(store, cmd, args),
        "SADD" => cmd_sadd(store, cmd, args),
        "SMEMBERS" => cmd_smembers(store, cmd, args),
        "SREM" => cmd_srem(store, cmd, args),
        "ZADD" => cmd_zadd(store, cmd, args),
        "ZRANGE" => cmd_zrange(store, cmd, args, proto),
        "ZSCORE" => cmd_zscore(store, cmd, args),
        "SCAN" => cmd_scan(store, args),
        "TYPE" => cmd_type(store, cmd, args),
        other => Err(format!(
            "ERR unknown command '{}'",
            other.to_ascii_lowercase()
        )),
    }
}

fn parse_num<T: std::str::FromStr>(arg: Option<&Vec<u8>>) -> Result<T, String> {
    arg.and_then(|b| std::str::from_utf8(b).ok())
        .and_then(|s| s.trim().parse().ok())
        .ok_or_else(|| "ERR value is not an integer or out of range".to_string())
}

// ── pub/sub + transaction dispatch (CONCEPT:EG-KG.txn.pubsub-transactions) ───────────────────────────────

/// Commands that are ALWAYS run immediately, never queued, even inside a `MULTI`
/// block (they steer the transaction / session itself).
fn is_multi_control(cmd: &str) -> bool {
    matches!(cmd, "MULTI" | "EXEC" | "DISCARD" | "RESET" | "QUIT")
}

/// Is `cmd` a command this shim recognizes? Used at queue time so an unknown
/// command taints the transaction and `EXEC` returns `EXECABORT` (Redis semantics).
fn is_known_command(cmd: &str) -> bool {
    matches!(
        cmd,
        "PING"
            | "ECHO"
            | "SET"
            | "GET"
            | "DEL"
            | "EXISTS"
            | "EXPIRE"
            | "TTL"
            | "INCR"
            | "DECR"
            | "MGET"
            | "MSET"
            | "HSET"
            | "HGET"
            | "HGETALL"
            | "HDEL"
            | "LPUSH"
            | "RPUSH"
            | "LRANGE"
            | "LLEN"
            | "SADD"
            | "SMEMBERS"
            | "SREM"
            | "ZADD"
            | "ZRANGE"
            | "ZSCORE"
            | "SCAN"
            | "TYPE"
            | "PUBLISH"
    )
}

/// Commands permitted while a RESP2 connection is in subscriber mode. Everything
/// else is refused with the exact Redis error until the client unsubscribes.
fn allowed_in_subscribe(cmd: &str) -> bool {
    matches!(
        cmd,
        "SUBSCRIBE"
            | "UNSUBSCRIBE"
            | "PSUBSCRIBE"
            | "PUNSUBSCRIBE"
            | "PING"
            | "QUIT"
            | "RESET"
            | "HELLO"
    )
}

/// Top-level command dispatch used by the connection driver (CONCEPT:EG-KG.txn.pubsub-transactions).
/// Unlike [`execute`] (one reply) this returns a VECTOR of replies — subscribe /
/// unsubscribe emit one confirmation per channel — and threads the [`PubSub`]
/// registry plus the connection's transaction/subscription state. Non-pub/sub,
/// non-transaction commands delegate to [`execute`] for their single reply.
fn dispatch(
    store: &RedisStore,
    pubsub: &PubSub,
    args: &[Vec<u8>],
    conn: &mut ConnState,
    auth_secret: &str,
) -> Vec<Resp> {
    if args.is_empty() {
        return vec![Resp::Error("ERR empty command".into())];
    }
    let cmd = upper(&args[0]);
    if conn.actor_scope.is_none() && !matches!(cmd.as_str(), "AUTH" | "HELLO" | "QUIT") {
        return vec![Resp::Error("NOAUTH Authentication required.".into())];
    }

    // Queue everything (except control verbs) while inside MULTI.
    if conn.in_multi && !is_multi_control(&cmd) {
        return queue_in_multi(&cmd, args, conn);
    }

    // Enforce the RESP2 subscriber-mode command gate.
    if conn.proto < 3
        && (!conn.sub_channels.is_empty() || !conn.sub_patterns.is_empty())
        && !allowed_in_subscribe(&cmd)
    {
        return vec![Resp::Error(format!(
            "ERR Can't execute '{}': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context",
            cmd.to_ascii_lowercase()
        ))];
    }

    dispatch_command(store, pubsub, &cmd, args, conn, auth_secret)
}

/// The per-command dispatch table [`dispatch`] falls into once the
/// auth/MULTI-queue/subscriber-mode gates have all passed. A plain exhaustive
/// match (not a lookup table) so the compiler keeps proving every handled verb
/// routes somewhere and an unrecognized one falls through to [`execute`].
fn dispatch_command(
    store: &RedisStore,
    pubsub: &PubSub,
    cmd: &str,
    args: &[Vec<u8>],
    conn: &mut ConnState,
    auth_secret: &str,
) -> Vec<Resp> {
    match cmd {
        "MULTI" => handle_multi_cmd(conn),
        "DISCARD" => handle_discard_cmd(conn),
        "EXEC" => exec_transaction(store, conn, auth_secret),
        "SUBSCRIBE" | "PSUBSCRIBE" => subscribe(pubsub, conn, &args[1..], cmd == "PSUBSCRIBE"),
        "UNSUBSCRIBE" | "PUNSUBSCRIBE" => {
            unsubscribe(pubsub, conn, &args[1..], cmd == "PUNSUBSCRIBE")
        }
        "PUBLISH" => handle_publish_cmd(pubsub, conn, args),
        "RESET" => reset_connection(pubsub, conn),
        _ => vec![execute(store, args, conn, auth_secret)],
    }
}

/// Queue one command inside an open `MULTI` block (CONCEPT:EG-KG.txn.pubsub-transactions), rejecting an
/// unknown command, a SUBSCRIBE-family command, or a queue that would exceed
/// resource limits — all of which taint the transaction (`multi_dirty`) so the
/// eventual `EXEC` aborts.
fn queue_in_multi(cmd: &str, args: &[Vec<u8>], conn: &mut ConnState) -> Vec<Resp> {
    if !is_known_command(cmd) && !allowed_in_subscribe(cmd) {
        conn.multi_dirty = true;
        return vec![Resp::Error(format!(
            "ERR unknown command '{}'",
            cmd.to_ascii_lowercase()
        ))];
    }
    // SUBSCRIBE-family commands are not allowed inside a transaction.
    if matches!(
        cmd,
        "SUBSCRIBE" | "UNSUBSCRIBE" | "PSUBSCRIBE" | "PUNSUBSCRIBE"
    ) {
        conn.multi_dirty = true;
        return vec![Resp::Error(format!(
            "ERR {} is not allowed in transactions",
            cmd
        ))];
    }
    let command_bytes = args
        .iter()
        .try_fold(0usize, |total, value| total.checked_add(value.len()));
    let next_bytes = command_bytes.and_then(|size| conn.queued_bytes.checked_add(size));
    if conn.queued.len() >= MAX_MULTI_COMMANDS
        || next_bytes.is_none_or(|size| size > MAX_MULTI_BYTES)
    {
        conn.multi_dirty = true;
        return vec![Resp::Error(
            "ERR transaction exceeds resource limits".into(),
        )];
    }
    conn.queued_bytes = next_bytes.unwrap_or(0);
    conn.queued.push(args.to_vec());
    vec![Resp::Simple("QUEUED".into())]
}

fn handle_multi_cmd(conn: &mut ConnState) -> Vec<Resp> {
    if conn.in_multi {
        return vec![Resp::Error("ERR MULTI calls can not be nested".into())];
    }
    conn.in_multi = true;
    conn.queued.clear();
    conn.queued_bytes = 0;
    conn.multi_dirty = false;
    vec![Resp::Simple("OK".into())]
}

fn handle_discard_cmd(conn: &mut ConnState) -> Vec<Resp> {
    if !conn.in_multi {
        return vec![Resp::Error("ERR DISCARD without MULTI".into())];
    }
    conn.in_multi = false;
    conn.queued.clear();
    conn.queued_bytes = 0;
    conn.multi_dirty = false;
    vec![Resp::Simple("OK".into())]
}

fn handle_publish_cmd(pubsub: &PubSub, conn: &ConnState, args: &[Vec<u8>]) -> Vec<Resp> {
    let Some(scope) = conn.actor_scope.as_deref() else {
        return vec![Resp::Error("NOAUTH Authentication required.".into())];
    };
    match (args.get(1), args.get(2)) {
        (Some(chan), Some(payload)) => match utf8_argument(chan, MAX_REDIS_CHANNEL_BYTES) {
            Ok(channel) => vec![Resp::Int(pubsub.publish(scope, &channel, payload))],
            Err(error) => vec![Resp::Error(error)],
        },
        _ => vec![Resp::Error(
            "ERR wrong number of arguments for 'publish'".into(),
        )],
    }
}

fn reset_connection(pubsub: &PubSub, conn: &mut ConnState) -> Vec<Resp> {
    let Some(scope) = conn.actor_scope.clone() else {
        return vec![Resp::Error("NOAUTH Authentication required.".into())];
    };
    for c in conn.sub_channels.drain().collect::<Vec<_>>() {
        pubsub.unsubscribe(conn.id, &scope, &c);
    }
    for p in conn.sub_patterns.drain().collect::<Vec<_>>() {
        pubsub.punsubscribe(conn.id, &scope, &p);
    }
    conn.in_multi = false;
    conn.queued.clear();
    conn.queued_bytes = 0;
    conn.multi_dirty = false;
    conn.sub_bytes = 0;
    conn.proto = 2;
    conn.actor_scope = None;
    vec![Resp::Simple("RESET".into())]
}

/// Execute a queued `MULTI` transaction atomically (CONCEPT:EG-KG.txn.pubsub-transactions): run every
/// queued command in order with no other connection interleaving, returning the
/// array of their replies. A prior malformed queued command aborts with
/// `EXECABORT`; `EXEC` outside a transaction is an error.
fn exec_transaction(store: &RedisStore, conn: &mut ConnState, auth_secret: &str) -> Vec<Resp> {
    if !conn.in_multi {
        return vec![Resp::Error("ERR EXEC without MULTI".into())];
    }
    conn.in_multi = false;
    let queued = std::mem::take(&mut conn.queued);
    conn.queued_bytes = 0;
    if std::mem::take(&mut conn.multi_dirty) {
        return vec![Resp::Error(
            "EXECABORT Transaction discarded because of previous errors.".into(),
        )];
    }
    let mut results = Vec::with_capacity(queued.len());
    let mut result_bytes = 0usize;
    let mut result_items = 0usize;
    for qargs in queued {
        let result = execute(store, &qargs, conn, auth_secret);
        if !response_cost(&result, &mut result_bytes, &mut result_items) {
            return vec![Resp::Error(
                "ERR transaction response exceeds resource limits".into(),
            )];
        }
        results.push(result);
    }
    vec![Resp::Array(Some(results))]
}

/// `SUBSCRIBE` / `PSUBSCRIBE` (CONCEPT:EG-KG.txn.pubsub-transactions): add each channel/pattern to the
/// registry + the connection's set, emitting one confirmation push per channel with
/// the running total subscription count.
fn subscribe(pubsub: &PubSub, conn: &mut ConnState, chans: &[Vec<u8>], pattern: bool) -> Vec<Resp> {
    let kind = if pattern { "psubscribe" } else { "subscribe" };
    let Some(scope) = conn.actor_scope.clone() else {
        return vec![Resp::Error("NOAUTH Authentication required.".into())];
    };
    if chans.is_empty() {
        return vec![Resp::Error(format!(
            "ERR wrong number of arguments for '{kind}'"
        ))];
    }
    let mut out = Vec::with_capacity(chans.len());
    for c in chans {
        let name = match utf8_argument(c, MAX_REDIS_CHANNEL_BYTES) {
            Ok(name) => name,
            Err(error) => {
                out.push(Resp::Error(error));
                continue;
            }
        };
        out.push(subscribe_one(pubsub, conn, &scope, kind, pattern, name));
    }
    out
}

/// Subscribe `conn` to one already-decoded channel/pattern `name`, enforcing
/// the per-connection and global subscription resource limits, and return
/// its confirmation push (or the resource-limit error in its place).
fn subscribe_one(
    pubsub: &PubSub,
    conn: &mut ConnState,
    scope: &str,
    kind: &str,
    pattern: bool,
    name: String,
) -> Resp {
    let already_subscribed = if pattern {
        conn.sub_patterns.contains(&name)
    } else {
        conn.sub_channels.contains(&name)
    };
    let next_sub_bytes = conn.sub_bytes.checked_add(name.len());
    if !already_subscribed
        && (conn.sub_count() as usize >= MAX_REDIS_SUBSCRIPTIONS
            || next_sub_bytes.is_none_or(|bytes| bytes > MAX_REDIS_SUBSCRIPTION_BYTES))
    {
        return Resp::Error("ERR subscription resource limit exceeded".into());
    }
    if pattern {
        if !already_subscribed {
            if !pubsub.psubscribe(conn.id, scope, &name) {
                return Resp::Error("ERR global subscription resource limit exceeded".into());
            }
            conn.sub_patterns.insert(name.clone());
            conn.sub_bytes = next_sub_bytes.unwrap_or(conn.sub_bytes);
        }
    } else if !already_subscribed {
        if !pubsub.subscribe(conn.id, scope, &name) {
            return Resp::Error("ERR global subscription resource limit exceeded".into());
        }
        conn.sub_channels.insert(name.clone());
        conn.sub_bytes = next_sub_bytes.unwrap_or(conn.sub_bytes);
    }
    Resp::Push(vec![
        Resp::bulk_str(kind),
        Resp::bulk_str(name.into_bytes()),
        Resp::Int(conn.sub_count()),
    ])
}

/// `UNSUBSCRIBE` / `PUNSUBSCRIBE` (CONCEPT:EG-KG.txn.pubsub-transactions): drop the named channels (or ALL
/// of this kind when none are named), one confirmation push each. Unsubscribing from
/// nothing still emits a single null-channel confirmation, matching Redis.
fn unsubscribe(
    pubsub: &PubSub,
    conn: &mut ConnState,
    chans: &[Vec<u8>],
    pattern: bool,
) -> Vec<Resp> {
    let kind = if pattern {
        "punsubscribe"
    } else {
        "unsubscribe"
    };
    let Some(scope) = conn.actor_scope.clone() else {
        return vec![Resp::Error("NOAUTH Authentication required.".into())];
    };
    let targets = match unsubscribe_targets(conn, chans, pattern) {
        Ok(targets) => targets,
        Err(resp) => return vec![resp],
    };
    if targets.is_empty() {
        return vec![Resp::Push(vec![
            Resp::bulk_str(kind),
            Resp::Null,
            Resp::Int(conn.sub_count()),
        ])];
    }
    let mut out = Vec::with_capacity(targets.len());
    for name in targets {
        out.push(unsubscribe_one(pubsub, conn, &scope, kind, pattern, name));
    }
    out
}

/// Resolve which channels/patterns an `UNSUBSCRIBE`/`PUNSUBSCRIBE` targets:
/// every currently-subscribed one of that kind when `chans` is empty,
/// otherwise the explicitly named (and UTF-8-decoded) ones.
fn unsubscribe_targets(
    conn: &ConnState,
    chans: &[Vec<u8>],
    pattern: bool,
) -> Result<Vec<String>, Resp> {
    if chans.is_empty() {
        return Ok(if pattern {
            conn.sub_patterns.iter().cloned().collect()
        } else {
            conn.sub_channels.iter().cloned().collect()
        });
    }
    let mut targets = Vec::with_capacity(chans.len());
    for channel in chans {
        match utf8_argument(channel, MAX_REDIS_CHANNEL_BYTES) {
            Ok(channel) => targets.push(channel),
            Err(error) => return Err(Resp::Error(error)),
        }
    }
    Ok(targets)
}

/// Unsubscribe `conn` from one already-resolved channel/pattern `name` and
/// return its confirmation push.
fn unsubscribe_one(
    pubsub: &PubSub,
    conn: &mut ConnState,
    scope: &str,
    kind: &str,
    pattern: bool,
    name: String,
) -> Resp {
    let removed = if pattern {
        let removed = conn.sub_patterns.remove(&name);
        pubsub.punsubscribe(conn.id, scope, &name);
        removed
    } else {
        let removed = conn.sub_channels.remove(&name);
        pubsub.unsubscribe(conn.id, scope, &name);
        removed
    };
    if removed {
        conn.sub_bytes = conn.sub_bytes.saturating_sub(name.len());
    }
    Resp::Push(vec![
        Resp::bulk_str(kind),
        Resp::bulk_str(name.into_bytes()),
        Resp::Int(conn.sub_count()),
    ])
}

// ── the per-connection driver + listener ──────────────────────────────────────────

/// Drive ONE Redis connection: parse commands from the socket, execute, reply,
/// until the client quits or the socket closes. Generic over the byte stream so an
/// in-process test can drive it over any duplex transport (CONCEPT:EG-KG.ontology.resp2-resp3-codec-round).
async fn handle_connection<S>(
    s: &mut S,
    store: Arc<RedisStore>,
    pubsub: Arc<PubSub>,
    auth_secret: String,
) -> std::io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut conn = ConnState::new(2);
    // Register this connection's pub/sub mailbox so PUBLISH can reach it (EG-307).
    let (tx, mut rx) = mpsc::channel::<PubMessage>(PUBSUB_MAILBOX_CAPACITY);
    conn.id = pubsub
        .register(tx)
        .ok_or_else(|| invalid_data("Redis connection limit exceeded"))?;
    let result = drive_connection(s, &store, &pubsub, &mut conn, &mut rx, &auth_secret).await;
    // Always release the registry slot + all subscriptions on the way out.
    pubsub.unregister(conn.id);
    result
}

/// Parse and execute every complete command already in `buf`, writing each
/// reply out. Parses by offset and compacts `buf` once so a pipeline of tiny
/// commands cannot force a full-tail memmove after every command (quadratic
/// CPU work). Returns `Ok(true)` if the connection should close now (a
/// protocol error was already written, or the client sent `QUIT`).
async fn drain_and_execute<S>(
    s: &mut S,
    store: &Arc<RedisStore>,
    pubsub: &Arc<PubSub>,
    conn: &mut ConnState,
    buf: &mut Vec<u8>,
    auth_secret: &str,
) -> std::io::Result<bool>
where
    S: AsyncWrite + Unpin,
{
    let mut consumed_total = 0usize;
    loop {
        let (args, consumed) = match try_parse_command(&buf[consumed_total..]) {
            Ok(Some(v)) => v,
            Ok(None) => break,
            Err(e) => {
                let mut out = Vec::new();
                Resp::Error(e).encode(conn.proto, &mut out);
                s.write_all(&out).await?;
                return Ok(true);
            }
        };
        consumed_total = consumed_total
            .checked_add(consumed)
            .filter(|consumed| *consumed <= buf.len())
            .ok_or_else(|| invalid_data("invalid RESP command length"))?;
        if args.is_empty() {
            continue;
        }
        let replies = dispatch(store, pubsub, &args, conn, auth_secret);
        let mut out = Vec::new();
        if responses_within_budget(&replies) {
            for reply in &replies {
                reply.encode(conn.proto, &mut out);
            }
        } else {
            Resp::Error("ERR response exceeds resource limits".into()).encode(conn.proto, &mut out);
        }
        s.write_all(&out).await?;
        if conn.quit {
            let _ = s.shutdown().await;
            return Ok(true);
        }
    }
    if consumed_total > 0 {
        buf.drain(..consumed_total);
    }
    Ok(false)
}

/// The inner connection loop (split out so [`handle_connection`] can guarantee the
/// [`PubSub`] unregister runs on every exit path). `select!`s between the socket and
/// this connection's subscriber mailbox: buffered client commands are executed
/// first, then it awaits either more bytes or a published message to push out
/// (CONCEPT:EG-KG.txn.pubsub-transactions).
async fn drive_connection<S>(
    s: &mut S,
    store: &Arc<RedisStore>,
    pubsub: &Arc<PubSub>,
    conn: &mut ConnState,
    rx: &mut mpsc::Receiver<PubMessage>,
    auth_secret: &str,
) -> std::io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut buf: Vec<u8> = Vec::new();
    let mut tmp = [0u8; 8192];
    loop {
        // Drain and execute every complete command already in the buffer.
        if drain_and_execute(s, store, pubsub, conn, &mut buf, auth_secret).await? {
            return Ok(());
        }
        // Nothing more to parse: wait for either new bytes or a published message.
        tokio::select! {
            read = s.read(&mut tmp) => {
                let n = read?;
                if n == 0 {
                    return Ok(()); // client closed
                }
                buf.extend_from_slice(&tmp[..n]);
                if buf.len() > MAX_REDIS_REQUEST_BYTES {
                    return Ok(()); // runaway request guard
                }
            }
            msg = rx.recv() => {
                match msg {
                    Some(m) => {
                        let mut out = Vec::new();
                        let response = m.to_resp();
                        if responses_within_budget(std::slice::from_ref(&response)) {
                            response.encode(conn.proto, &mut out);
                        } else {
                            Resp::Error("ERR response exceeds resource limits".into())
                                .encode(conn.proto, &mut out);
                        }
                        s.write_all(&out).await?;
                    }
                    None => return Ok(()), // mailbox closed (shouldn't happen)
                }
            }
        }
    }
}

/// Bind `addr` and serve Redis RESP connections until the process exits. Spawned by
/// `main.rs` only when built `--features redis-wire` AND `EPISTEMIC_GRAPH_REDIS_ADDR`
/// is set (CONCEPT:EG-KG.ontology.resp2-resp3-codec-round). The Redis keyspace is durable when a persist dir is
/// configured on [`ServerState`], else in-memory scratch. The direct listener is
/// loopback-only and requires a non-empty engine auth secret before bind.
pub async fn serve(addr: &str, state: Arc<RwLock<ServerState>>) -> std::io::Result<()> {
    let (persist_dir, auth_secret) = {
        let state = state.read().await;
        (state.persist_dir.clone(), state.auth_secret.clone())
    };
    crate::server::validate_direct_wire_security(addr, "redis-wire", !auth_secret.is_empty())?;
    let store = Arc::new(RedisStore::open(persist_dir.as_deref()).map_err(std::io::Error::other)?);
    serve_listener(addr, store, auth_secret).await
}

/// Private authenticated listener seam used by the production boundary and its
/// socket-level tests. There is no public unowned or unauthenticated server entry.
async fn serve_listener(
    addr: &str,
    store: Arc<RedisStore>,
    auth_secret: String,
) -> std::io::Result<()> {
    if auth_secret.is_empty() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "redis-wire requires non-empty authentication key material",
        ));
    }
    let listener = TcpListener::bind(addr).await?;
    // One pub/sub registry per listener, shared across every accepted connection
    // (CONCEPT:EG-KG.txn.pubsub-transactions).
    let pubsub = Arc::new(PubSub::default());
    tracing::info!(
        "redis-wire: serving authenticated, principal-scoped Redis RESP protocol on {} (durable={})",
        addr,
        store.is_durable()
    );
    loop {
        let (mut socket, peer) = listener.accept().await?;
        let store = store.clone();
        let pubsub = pubsub.clone();
        let auth_secret = auth_secret.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_connection(&mut socket, store, pubsub, auth_secret).await {
                tracing::debug!("redis-wire connection from {peer} ended: {e}");
            }
        });
    }
}

#[cfg(test)]
mod tests {
    //! CONCEPT:EG-KG.ontology.resp2-resp3-codec-round — RESP2/RESP3 codec round-trips, command-parse coverage, the
    //! Redis data-type command execution over the KV store, plus an in-process
    //! listener smoke test driving the private authenticated listener over a TCP socket
    //! with hand-built RESP frames (no redis client crate).
    use super::*;
    use tokio::net::TcpStream;

    const TEST_SECRET: &str = "redis-test-auth-secret";
    const TEST_PRINCIPAL: &str = "synthetic-test-principal";

    fn mem_store() -> Arc<RedisStore> {
        Arc::new(RedisStore::open(None).unwrap())
    }

    fn authenticated_connection(proto: u8, principal: &str) -> ConnState {
        let mut connection = ConnState::new(3);
        connection.actor_scope =
            Some(crate::server::pseudonymous_broker_actor(TEST_SECRET, principal).unwrap());
        connection.proto = proto;
        connection
    }

    fn conn3() -> ConnState {
        authenticated_connection(3, TEST_PRINCIPAL)
    }

    fn a(parts: &[&str]) -> Vec<Vec<u8>> {
        parts.iter().map(|s| s.as_bytes().to_vec()).collect()
    }

    fn enc(r: &Resp, proto: u8) -> Vec<u8> {
        let mut out = Vec::new();
        r.encode(proto, &mut out);
        out
    }

    // ── codec round-trips ──────────────────────────────────────────────────────

    #[test]
    fn eg174_resp2_encodes_core_types() {
        assert_eq!(enc(&Resp::Simple("OK".into()), 2), b"+OK\r\n");
        assert_eq!(enc(&Resp::Error("ERR x".into()), 2), b"-ERR x\r\n");
        assert_eq!(enc(&Resp::Int(42), 2), b":42\r\n");
        assert_eq!(enc(&Resp::bulk_str("hi"), 2), b"$2\r\nhi\r\n");
        assert_eq!(enc(&Resp::Bulk(None), 2), b"$-1\r\n");
        assert_eq!(enc(&Resp::Null, 2), b"$-1\r\n");
        assert_eq!(
            enc(
                &Resp::Array(Some(vec![Resp::Int(1), Resp::bulk_str("a")])),
                2
            ),
            b"*2\r\n:1\r\n$1\r\na\r\n"
        );
    }

    #[test]
    fn eg174_resp3_extended_types_downgrade_on_resp2() {
        // Map: RESP3 uses '%', RESP2 flattens to a 2N array.
        let map = Resp::Map(vec![(Resp::bulk_str("f"), Resp::bulk_str("v"))]);
        assert_eq!(enc(&map, 3), b"%1\r\n$1\r\nf\r\n$1\r\nv\r\n");
        assert_eq!(enc(&map, 2), b"*2\r\n$1\r\nf\r\n$1\r\nv\r\n");
        // Set: '~' on RESP3, '*' on RESP2.
        let set = Resp::Set(vec![Resp::bulk_str("m")]);
        assert_eq!(enc(&set, 3), b"~1\r\n$1\r\nm\r\n");
        assert_eq!(enc(&set, 2), b"*1\r\n$1\r\nm\r\n");
        // Double + Bool + Null native on RESP3, downgraded on RESP2.
        assert_eq!(enc(&Resp::Double(1.5), 3), b",1.5\r\n");
        assert_eq!(enc(&Resp::Double(1.5), 2), b"$3\r\n1.5\r\n");
        assert_eq!(enc(&Resp::Bool(true), 3), b"#t\r\n");
        assert_eq!(enc(&Resp::Bool(true), 2), b":1\r\n");
        assert_eq!(enc(&Resp::Null, 3), b"_\r\n");
    }

    #[test]
    fn eg174_parse_array_and_inline_commands() {
        // Array form (redis-cli): *2 $3 GET $1 k
        let wire = b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n";
        let (args, consumed) = try_parse_command(wire).unwrap().unwrap();
        assert_eq!(args, a(&["GET", "k"]));
        assert_eq!(consumed, wire.len());
        // Inline form (telnet).
        let (args, consumed) = try_parse_command(b"PING hello\r\n").unwrap().unwrap();
        assert_eq!(args, a(&["PING", "hello"]));
        assert_eq!(consumed, "PING hello\r\n".len());
        // Partial array → None (need more bytes).
        assert!(try_parse_command(b"*2\r\n$3\r\nGE").unwrap().is_none());
        assert!(try_parse_command(b"*1\r\n$3\r\nGETxx").is_err());
    }

    #[test]
    fn stored_entry_preflight_rejects_declared_allocation_bomb() {
        let allocation_bomb = [0xdd, 0xff, 0xff, 0xff, 0xff];
        assert!(decode_entry(&allocation_bomb).is_err());
    }

    #[test]
    fn eg174_glob_match_and_range_slice() {
        assert!(glob_match("user:*", "user:42"));
        assert!(glob_match("*", "anything"));
        assert!(glob_match("a?c", "abc"));
        assert!(!glob_match("user:*", "post:1"));
        let v = vec![10, 20, 30, 40];
        assert_eq!(range_slice(&v, 0, -1), &[10, 20, 30, 40]);
        assert_eq!(range_slice(&v, 1, 2), &[20, 30]);
        assert_eq!(range_slice(&v, -2, -1), &[30, 40]);
        assert_eq!(range_slice(&v, 5, 10), &[] as &[i32]);
    }

    // ── command execution over the KV store ─────────────────────────────────────

    #[test]
    fn eg174_set_get_del_incr_expire() {
        let store = mem_store();
        let mut c = conn3();
        assert_eq!(
            execute(&store, &a(&["SET", "k", "v"]), &mut c, TEST_SECRET),
            Resp::Simple("OK".into())
        );
        assert_eq!(
            execute(&store, &a(&["GET", "k"]), &mut c, TEST_SECRET),
            Resp::Bulk(Some(b"v".to_vec()))
        );
        assert_eq!(
            execute(&store, &a(&["TYPE", "k"]), &mut c, TEST_SECRET),
            Resp::Simple("string".into())
        );
        assert_eq!(
            execute(&store, &a(&["EXISTS", "k", "nope"]), &mut c, TEST_SECRET),
            Resp::Int(1)
        );
        // NX on an existing key → null.
        assert_eq!(
            execute(&store, &a(&["SET", "k", "v2", "NX"]), &mut c, TEST_SECRET),
            Resp::Null
        );
        // INCR path.
        assert_eq!(
            execute(&store, &a(&["SET", "n", "10"]), &mut c, TEST_SECRET),
            Resp::Simple("OK".into())
        );
        assert_eq!(
            execute(&store, &a(&["INCR", "n"]), &mut c, TEST_SECRET),
            Resp::Int(11)
        );
        assert_eq!(
            execute(&store, &a(&["DECR", "n"]), &mut c, TEST_SECRET),
            Resp::Int(10)
        );
        // EXPIRE/TTL.
        assert_eq!(
            execute(&store, &a(&["EXPIRE", "k", "100"]), &mut c, TEST_SECRET),
            Resp::Int(1)
        );
        assert!(
            matches!(execute(&store, &a(&["TTL", "k"]), &mut c, TEST_SECRET), Resp::Int(t) if t > 0 && t <= 100)
        );
        // DEL.
        assert_eq!(
            execute(&store, &a(&["DEL", "k", "n"]), &mut c, TEST_SECRET),
            Resp::Int(2)
        );
        assert_eq!(
            execute(&store, &a(&["GET", "k"]), &mut c, TEST_SECRET),
            Resp::Bulk(None)
        );
    }

    #[test]
    fn eg174_hash_list_set_zset_exec() {
        let store = mem_store();
        let mut c = conn3();
        // HSET / HGET / HGETALL.
        assert_eq!(
            execute(
                &store,
                &a(&["HSET", "h", "f1", "v1", "f2", "v2"]),
                &mut c,
                TEST_SECRET
            ),
            Resp::Int(2)
        );
        assert_eq!(
            execute(&store, &a(&["HGET", "h", "f1"]), &mut c, TEST_SECRET),
            Resp::Bulk(Some(b"v1".to_vec()))
        );
        // Duplicate fields in one batch update in argument order, but count as a
        // single new field and retain the original field position.
        assert_eq!(
            execute(
                &store,
                &a(&["HSET", "h", "f1", "v3", "f1", "v4", "f3", "v3"]),
                &mut c,
                TEST_SECRET
            ),
            Resp::Int(1)
        );
        assert_eq!(
            execute(&store, &a(&["HGET", "h", "f1"]), &mut c, TEST_SECRET),
            Resp::Bulk(Some(b"v4".to_vec()))
        );
        assert_eq!(
            execute(&store, &a(&["HGETALL", "h"]), &mut c, TEST_SECRET),
            Resp::Map(vec![
                (Resp::bulk_str("f1"), Resp::bulk_str("v4")),
                (Resp::bulk_str("f2"), Resp::bulk_str("v2")),
                (Resp::bulk_str("f3"), Resp::bulk_str("v3")),
            ])
        );
        // LPUSH / RPUSH / LRANGE / LLEN. LPUSH a b c → head order c b a.
        assert_eq!(
            execute(&store, &a(&["RPUSH", "l", "a", "b"]), &mut c, TEST_SECRET),
            Resp::Int(2)
        );
        assert_eq!(
            execute(&store, &a(&["LPUSH", "l", "y", "z"]), &mut c, TEST_SECRET),
            Resp::Int(4)
        );
        assert_eq!(
            execute(&store, &a(&["LLEN", "l"]), &mut c, TEST_SECRET),
            Resp::Int(4)
        );
        assert_eq!(
            execute(&store, &a(&["LRANGE", "l", "0", "-1"]), &mut c, TEST_SECRET),
            Resp::Array(Some(vec![
                Resp::bulk_str("z"),
                Resp::bulk_str("y"),
                Resp::bulk_str("a"),
                Resp::bulk_str("b")
            ]))
        );
        // SADD / SMEMBERS / SREM (dedup).
        assert_eq!(
            execute(
                &store,
                &a(&["SADD", "s", "x", "y", "x"]),
                &mut c,
                TEST_SECRET
            ),
            Resp::Int(2)
        );
        assert_eq!(
            execute(&store, &a(&["SREM", "s", "x"]), &mut c, TEST_SECRET),
            Resp::Int(1)
        );
        assert_eq!(
            execute(&store, &a(&["SMEMBERS", "s"]), &mut c, TEST_SECRET),
            Resp::Set(vec![Resp::bulk_str("y")])
        );
        // ZADD / ZRANGE / ZSCORE (sorted by score).
        assert_eq!(
            execute(
                &store,
                &a(&["ZADD", "z", "2", "b", "1", "a"]),
                &mut c,
                TEST_SECRET
            ),
            Resp::Int(2)
        );
        assert_eq!(
            execute(&store, &a(&["ZRANGE", "z", "0", "-1"]), &mut c, TEST_SECRET),
            Resp::Array(Some(vec![Resp::bulk_str("a"), Resp::bulk_str("b")]))
        );
        assert_eq!(
            execute(&store, &a(&["ZSCORE", "z", "b"]), &mut c, TEST_SECRET),
            Resp::bulk_str("2")
        );
        // Duplicate members in one ZADD use the last score and count only a
        // previously absent member.
        assert_eq!(
            execute(
                &store,
                &a(&["ZADD", "z", "3", "b", "0", "b", "4", "c"]),
                &mut c,
                TEST_SECRET
            ),
            Resp::Int(1)
        );
        assert_eq!(
            execute(&store, &a(&["ZRANGE", "z", "0", "-1"]), &mut c, TEST_SECRET),
            Resp::Array(Some(vec![
                Resp::bulk_str("b"),
                Resp::bulk_str("a"),
                Resp::bulk_str("c")
            ]))
        );
    }

    // CXA-EG-03 characterization: `execute_data` (CCN 90) arms not already
    // exercised above -- MGET/MSET/HDEL/RPUSH/SCAN, the unknown-command
    // fallback, SET's EX/PX/XX option parsing, ZADD's odd-arg-count error,
    // ZRANGE WITHSCORES, and a ZSCORE miss -- ahead of decomposing the match.

    #[test]
    fn eg174_mget_mset() {
        let store = mem_store();
        let mut c = conn3();
        assert_eq!(
            execute(
                &store,
                &a(&["MSET", "k1", "v1", "k2", "v2"]),
                &mut c,
                TEST_SECRET
            ),
            Resp::Simple("OK".into())
        );
        assert_eq!(
            execute(
                &store,
                &a(&["MGET", "k1", "k2", "nope"]),
                &mut c,
                TEST_SECRET
            ),
            Resp::Array(Some(vec![
                Resp::Bulk(Some(b"v1".to_vec())),
                Resp::Bulk(Some(b"v2".to_vec())),
                Resp::Bulk(None),
            ]))
        );
        // Odd argument count is a wrong-number-of-arguments error.
        assert!(matches!(
            execute(&store, &a(&["MSET", "k1", "v1", "k2"]), &mut c, TEST_SECRET),
            Resp::Error(_)
        ));
    }

    #[test]
    fn eg174_hdel_and_rpush() {
        let store = mem_store();
        let mut c = conn3();
        execute(
            &store,
            &a(&["HSET", "h", "f1", "v1", "f2", "v2"]),
            &mut c,
            TEST_SECRET,
        );
        assert_eq!(
            execute(&store, &a(&["HDEL", "h", "f1"]), &mut c, TEST_SECRET),
            Resp::Int(1)
        );
        assert_eq!(
            execute(&store, &a(&["RPUSH", "l", "a", "b"]), &mut c, TEST_SECRET),
            Resp::Int(2)
        );
        assert_eq!(
            execute(&store, &a(&["LRANGE", "l", "0", "-1"]), &mut c, TEST_SECRET),
            Resp::Array(Some(vec![Resp::bulk_str("a"), Resp::bulk_str("b")]))
        );
        // RPUSH/LPUSH with no values is a wrong-number-of-arguments error.
        assert!(matches!(
            execute(&store, &a(&["RPUSH", "l"]), &mut c, TEST_SECRET),
            Resp::Error(_)
        ));
    }

    #[test]
    fn eg174_scan_and_unknown_command() {
        let store = mem_store();
        let mut c = conn3();
        execute(&store, &a(&["SET", "alpha", "1"]), &mut c, TEST_SECRET);
        execute(&store, &a(&["SET", "beta", "1"]), &mut c, TEST_SECRET);
        match execute(&store, &a(&["SCAN", "0"]), &mut c, TEST_SECRET) {
            Resp::Array(Some(items)) => {
                assert_eq!(items.len(), 2);
                assert_eq!(items[0], Resp::bulk_str("0"));
            }
            other => panic!("unexpected SCAN reply: {other:?}"),
        }
        assert!(matches!(
            execute(
                &store,
                &a(&["SCAN", "0", "MATCH", "al*"]),
                &mut c,
                TEST_SECRET
            ),
            Resp::Array(Some(_))
        ));
        assert!(matches!(
            execute(&store, &a(&["NOSUCHCOMMAND"]), &mut c, TEST_SECRET),
            Resp::Error(_)
        ));
    }

    #[test]
    fn eg174_set_with_ex_px_options() {
        let store = mem_store();
        let mut c = conn3();
        assert_eq!(
            execute(
                &store,
                &a(&["SET", "k", "v", "EX", "100"]),
                &mut c,
                TEST_SECRET
            ),
            Resp::Simple("OK".into())
        );
        assert!(
            matches!(execute(&store, &a(&["TTL", "k"]), &mut c, TEST_SECRET), Resp::Int(t) if t > 0 && t <= 100)
        );
        assert_eq!(
            execute(
                &store,
                &a(&["SET", "k2", "v", "PX", "100000"]),
                &mut c,
                TEST_SECRET
            ),
            Resp::Simple("OK".into())
        );
        // Unknown SET option is a syntax error.
        assert!(matches!(
            execute(
                &store,
                &a(&["SET", "k3", "v", "BOGUS"]),
                &mut c,
                TEST_SECRET
            ),
            Resp::Error(_)
        ));
    }

    #[test]
    fn eg174_zadd_odd_args_and_zrange_withscores_and_zscore_miss() {
        let store = mem_store();
        let mut c = conn3();
        assert!(matches!(
            execute(
                &store,
                &a(&["ZADD", "z", "1", "a", "2"]),
                &mut c,
                TEST_SECRET
            ),
            Resp::Error(_)
        ));
        execute(&store, &a(&["ZADD", "z", "1", "a"]), &mut c, TEST_SECRET);
        assert_eq!(
            execute(
                &store,
                &a(&["ZRANGE", "z", "0", "-1", "WITHSCORES"]),
                &mut c,
                TEST_SECRET
            ),
            // `c` (conn3()) is proto 3, so scores come back as native Resp::Double.
            Resp::Array(Some(vec![Resp::bulk_str("a"), Resp::Double(1.0)]))
        );
        assert_eq!(
            execute(&store, &a(&["ZSCORE", "z", "nope"]), &mut c, TEST_SECRET),
            Resp::Null
        );
    }

    #[test]
    fn eg174_wrongtype_is_reported() {
        let store = mem_store();
        let mut c = conn3();
        execute(&store, &a(&["SET", "k", "v"]), &mut c, TEST_SECRET);
        // A hash op against a string key → WRONGTYPE.
        match execute(&store, &a(&["HGET", "k", "f"]), &mut c, TEST_SECRET) {
            Resp::Error(e) => assert!(e.starts_with("WRONGTYPE"), "{e}"),
            other => panic!("expected WRONGTYPE, got {other:?}"),
        }
    }

    #[test]
    fn eg174_auth_gate_and_hello_upgrade() {
        let store = mem_store();
        let mut c = ConnState::new(2);
        // Data command before AUTH → NOAUTH.
        match execute(&store, &a(&["GET", "k"]), &mut c, TEST_SECRET) {
            Resp::Error(e) => assert!(e.starts_with("NOAUTH"), "{e}"),
            other => panic!("expected NOAUTH, got {other:?}"),
        }
        // Wrong password rejected.
        assert!(matches!(
            execute(
                &store,
                &a(&["AUTH", TEST_PRINCIPAL, "not-a-valid-credential"]),
                &mut c,
                TEST_SECRET
            ),
            Resp::Error(_)
        ));
        // Correct password accepted → subsequent command allowed.
        let credential = derive_redis_password(TEST_SECRET, TEST_PRINCIPAL);
        assert_eq!(
            execute(
                &store,
                &a(&["AUTH", TEST_PRINCIPAL, &credential]),
                &mut c,
                TEST_SECRET
            ),
            Resp::Simple("OK".into())
        );
        assert_eq!(
            execute(&store, &a(&["GET", "k"]), &mut c, TEST_SECRET),
            Resp::Bulk(None)
        );
        // HELLO 3 upgrades the protocol.
        assert!(matches!(
            execute(&store, &a(&["HELLO", "3"]), &mut c, TEST_SECRET),
            Resp::Map(_)
        ));
        assert_eq!(c.proto, 3);
    }

    #[test]
    fn redis_principals_have_isolated_keys_and_pubsub() {
        let store = mem_store();
        let pubsub = ps();
        let mut first = authenticated_connection(3, "synthetic-principal-one");
        let mut second = authenticated_connection(3, "synthetic-principal-two");

        assert_eq!(
            execute(
                &store,
                &a(&["SET", "shared-name", "first"]),
                &mut first,
                TEST_SECRET
            ),
            Resp::Simple("OK".into())
        );
        assert_eq!(
            execute(
                &store,
                &a(&["GET", "shared-name"]),
                &mut second,
                TEST_SECRET
            ),
            Resp::Bulk(None)
        );

        first.id = pubsub
            .register(mpsc::channel(PUBSUB_MAILBOX_CAPACITY).0)
            .unwrap();
        second.id = pubsub
            .register(mpsc::channel(PUBSUB_MAILBOX_CAPACITY).0)
            .unwrap();
        assert_eq!(
            dispatch(
                &store,
                &pubsub,
                &a(&["SUBSCRIBE", "events"]),
                &mut first,
                TEST_SECRET
            )
            .len(),
            1
        );
        assert_eq!(
            dispatch(
                &store,
                &pubsub,
                &a(&["PUBLISH", "events", "private"]),
                &mut second,
                TEST_SECRET
            ),
            vec![Resp::Int(0)]
        );
    }

    // ── in-process listener smoke test over a real socket ────────────────────────

    async fn read_reply(stream: &mut TcpStream) -> Vec<u8> {
        // Read once; the smoke test replies are small and fit one read.
        let mut buf = [0u8; 4096];
        let n = stream.read(&mut buf).await.unwrap();
        buf[..n].to_vec()
    }

    async fn authenticate(stream: &mut TcpStream) {
        let credential = derive_redis_password(TEST_SECRET, TEST_PRINCIPAL);
        let frame = format!("AUTH {TEST_PRINCIPAL} {credential}\r\n");
        stream.write_all(frame.as_bytes()).await.unwrap();
        assert_eq!(read_reply(stream).await, b"+OK\r\n");
    }

    #[tokio::test]
    async fn eg174_listener_roundtrip_over_tcp() {
        let probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = probe.local_addr().unwrap().to_string();
        drop(probe);
        let serve_addr = addr.clone();
        tokio::spawn(async move {
            let _ = serve_listener(&serve_addr, mem_store(), TEST_SECRET.to_string()).await;
        });
        // GOC-70: bounded-retry connect instead of a fixed pre-connect sleep —
        // a flat 150ms wait assumes the listener bound within an arbitrary
        // window, not guaranteed on a contended/low-core host. 1s budget
        // (50 * 20ms), matching the already-correct pattern in
        // tests/mysql_roundtrip.rs::spawn_listener.
        let mut s = {
            let mut last_err = None;
            let mut connected = None;
            for _ in 0..50 {
                match TcpStream::connect(&addr).await {
                    Ok(stream) => {
                        connected = Some(stream);
                        break;
                    }
                    Err(e) => {
                        last_err = Some(e);
                        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                    }
                }
            }
            connected
                .unwrap_or_else(|| panic!("connect to {addr} after bounded retry: {last_err:?}"))
        };
        authenticate(&mut s).await;
        // PING (inline).
        s.write_all(b"PING\r\n").await.unwrap();
        assert_eq!(read_reply(&mut s).await, b"+PONG\r\n");
        // SET then GET (array form).
        s.write_all(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$5\r\nhello\r\n")
            .await
            .unwrap();
        assert_eq!(read_reply(&mut s).await, b"+OK\r\n");
        s.write_all(b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n")
            .await
            .unwrap();
        assert_eq!(read_reply(&mut s).await, b"$5\r\nhello\r\n");
    }

    // ── pub/sub + transactions (CONCEPT:EG-KG.txn.pubsub-transactions) ──────────────────────────────────

    fn ps() -> PubSub {
        PubSub::default()
    }

    /// Read a reply, but fail (rather than hang forever) if none arrives.
    async fn read_reply_timeout(stream: &mut TcpStream) -> Vec<u8> {
        tokio::time::timeout(std::time::Duration::from_secs(3), read_reply(stream))
            .await
            .expect("timed out waiting for a reply")
    }

    /// Bind the authenticated listener on an ephemeral port and return the address.
    async fn spawn_listener() -> String {
        let probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = probe.local_addr().unwrap().to_string();
        drop(probe);
        let serve_addr = addr.clone();
        tokio::spawn(async move {
            let _ = serve_listener(&serve_addr, mem_store(), TEST_SECRET.to_string()).await;
        });
        // GOC-70: see eg174_listener_roundtrip_over_tcp's comment above — bounded
        // retry instead of a fixed sleep, same 1s budget.
        for _ in 0..50 {
            if TcpStream::connect(&addr).await.is_ok() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        addr
    }

    #[test]
    fn eg307_publish_with_no_subscribers_returns_zero() {
        let store = mem_store();
        let pubsub = ps();
        let mut c = conn3();
        assert_eq!(
            dispatch(
                &store,
                &pubsub,
                &a(&["PUBLISH", "ch", "hi"]),
                &mut c,
                TEST_SECRET
            ),
            vec![Resp::Int(0)]
        );
    }

    #[test]
    fn eg307_subscribe_confirm_and_count() {
        let store = mem_store();
        let pubsub = ps();
        let mut c = conn3();
        c.id = pubsub
            .register(mpsc::channel(PUBSUB_MAILBOX_CAPACITY).0)
            .unwrap();
        let replies = dispatch(
            &store,
            &pubsub,
            &a(&["SUBSCRIBE", "a", "b"]),
            &mut c,
            TEST_SECRET,
        );
        // One confirmation per channel, with a running total count.
        assert_eq!(
            replies,
            vec![
                Resp::Push(vec![
                    Resp::bulk_str("subscribe"),
                    Resp::bulk_str("a"),
                    Resp::Int(1)
                ]),
                Resp::Push(vec![
                    Resp::bulk_str("subscribe"),
                    Resp::bulk_str("b"),
                    Resp::Int(2)
                ]),
            ]
        );
        // UNSUBSCRIBE with no args drops all, count falls back to 0.
        let un = dispatch(&store, &pubsub, &a(&["UNSUBSCRIBE"]), &mut c, TEST_SECRET);
        assert_eq!(un.len(), 2);
        assert!(c.sub_channels.is_empty());
    }

    #[tokio::test]
    async fn eg307_publish_subscribe_delivery_over_tcp() {
        let addr = spawn_listener().await;
        // Subscriber connection.
        let mut sub = TcpStream::connect(&addr).await.unwrap();
        authenticate(&mut sub).await;
        sub.write_all(b"*2\r\n$9\r\nSUBSCRIBE\r\n$4\r\nnews\r\n")
            .await
            .unwrap();
        // The subscribe confirmation proves the registration landed.
        let confirm = read_reply_timeout(&mut sub).await;
        let confirm = String::from_utf8_lossy(&confirm);
        assert!(confirm.contains("subscribe"), "{confirm}");

        // Publisher connection.
        let mut pubc = TcpStream::connect(&addr).await.unwrap();
        authenticate(&mut pubc).await;
        pubc.write_all(b"*3\r\n$7\r\nPUBLISH\r\n$4\r\nnews\r\n$5\r\nhello\r\n")
            .await
            .unwrap();
        // PUBLISH reports exactly one receiver.
        assert_eq!(read_reply_timeout(&mut pubc).await, b":1\r\n");

        // The subscriber is pushed the message frame.
        let msg = read_reply_timeout(&mut sub).await;
        let msg = String::from_utf8_lossy(&msg);
        assert!(msg.contains("message"), "{msg}");
        assert!(msg.contains("news"), "{msg}");
        assert!(msg.contains("hello"), "{msg}");
    }

    #[tokio::test]
    async fn eg307_psubscribe_glob_delivery_over_tcp() {
        let addr = spawn_listener().await;
        let mut sub = TcpStream::connect(&addr).await.unwrap();
        authenticate(&mut sub).await;
        // PSUBSCRIBE news.* — a glob pattern.
        sub.write_all(b"*2\r\n$10\r\nPSUBSCRIBE\r\n$6\r\nnews.*\r\n")
            .await
            .unwrap();
        let confirm = read_reply_timeout(&mut sub).await;
        assert!(String::from_utf8_lossy(&confirm).contains("psubscribe"));

        let mut pubc = TcpStream::connect(&addr).await.unwrap();
        authenticate(&mut pubc).await;
        // Publish to news.tech — matches the glob.
        pubc.write_all(b"*3\r\n$7\r\nPUBLISH\r\n$9\r\nnews.tech\r\n$2\r\nhi\r\n")
            .await
            .unwrap();
        assert_eq!(read_reply_timeout(&mut pubc).await, b":1\r\n");

        let msg = read_reply_timeout(&mut sub).await;
        let msg = String::from_utf8_lossy(&msg);
        // A pattern delivery carries the pattern AND the concrete channel.
        assert!(msg.contains("pmessage"), "{msg}");
        assert!(msg.contains("news.*"), "{msg}");
        assert!(msg.contains("news.tech"), "{msg}");
        assert!(msg.contains("hi"), "{msg}");

        // A non-matching publish must NOT be delivered (0 receivers).
        pubc.write_all(b"*3\r\n$7\r\nPUBLISH\r\n$4\r\nchat\r\n$1\r\nx\r\n")
            .await
            .unwrap();
        assert_eq!(read_reply_timeout(&mut pubc).await, b":0\r\n");
    }

    #[test]
    fn eg307_multi_exec_atomic() {
        let store = mem_store();
        let pubsub = ps();
        let mut c = conn3();
        // MULTI opens the transaction.
        assert_eq!(
            dispatch(&store, &pubsub, &a(&["MULTI"]), &mut c, TEST_SECRET),
            vec![Resp::Simple("OK".into())]
        );
        // Commands queue rather than execute.
        assert_eq!(
            dispatch(&store, &pubsub, &a(&["SET", "k", "1"]), &mut c, TEST_SECRET),
            vec![Resp::Simple("QUEUED".into())]
        );
        assert_eq!(
            dispatch(&store, &pubsub, &a(&["INCR", "k"]), &mut c, TEST_SECRET),
            vec![Resp::Simple("QUEUED".into())]
        );
        assert_eq!(
            dispatch(&store, &pubsub, &a(&["GET", "k"]), &mut c, TEST_SECRET),
            vec![Resp::Simple("QUEUED".into())]
        );
        // Nothing ran yet.
        assert!(c.in_multi);
        // EXEC runs them back-to-back, replies as one array.
        let out = dispatch(&store, &pubsub, &a(&["EXEC"]), &mut c, TEST_SECRET);
        assert_eq!(
            out,
            vec![Resp::Array(Some(vec![
                Resp::Simple("OK".into()),
                Resp::Int(2),
                Resp::Bulk(Some(b"2".to_vec())),
            ]))]
        );
        assert!(!c.in_multi);
        // State really changed.
        assert_eq!(
            dispatch(&store, &pubsub, &a(&["GET", "k"]), &mut c, TEST_SECRET),
            vec![Resp::Bulk(Some(b"2".to_vec()))]
        );
    }

    #[test]
    fn eg307_discard_clears_queue() {
        let store = mem_store();
        let pubsub = ps();
        let mut c = conn3();
        dispatch(&store, &pubsub, &a(&["MULTI"]), &mut c, TEST_SECRET);
        dispatch(
            &store,
            &pubsub,
            &a(&["SET", "k", "99"]),
            &mut c,
            TEST_SECRET,
        );
        assert_eq!(
            dispatch(&store, &pubsub, &a(&["DISCARD"]), &mut c, TEST_SECRET),
            vec![Resp::Simple("OK".into())]
        );
        assert!(!c.in_multi);
        // The queued SET never ran.
        assert_eq!(
            dispatch(&store, &pubsub, &a(&["GET", "k"]), &mut c, TEST_SECRET),
            vec![Resp::Bulk(None)]
        );
        // EXEC / DISCARD outside a transaction are errors.
        assert!(matches!(
            dispatch(&store, &pubsub, &a(&["EXEC"]), &mut c, TEST_SECRET).as_slice(),
            [Resp::Error(_)]
        ));
    }

    #[test]
    fn eg307_multi_aborts_on_bad_command() {
        let store = mem_store();
        let pubsub = ps();
        let mut c = conn3();
        dispatch(&store, &pubsub, &a(&["MULTI"]), &mut c, TEST_SECRET);
        // An unknown command taints the transaction.
        assert!(matches!(
            dispatch(&store, &pubsub, &a(&["BOGUS", "x"]), &mut c, TEST_SECRET).as_slice(),
            [Resp::Error(_)]
        ));
        // EXEC then aborts with EXECABORT.
        match dispatch(&store, &pubsub, &a(&["EXEC"]), &mut c, TEST_SECRET).as_slice() {
            [Resp::Error(e)] => assert!(e.starts_with("EXECABORT"), "{e}"),
            other => panic!("expected EXECABORT, got {other:?}"),
        }
    }
}
