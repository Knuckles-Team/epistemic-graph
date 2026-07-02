//! Redis RESP wire-protocol listener (CONCEPT:EG-174) — a native, HAND-ROLLED
//! Redis server so a Redis client, an ORM cache layer, or `redis-cli` connects
//! DIRECTLY to the engine and runs the core Redis command set.
//!
//! ## What this is (and is NOT)
//!
//! Like the SQL wire shims (pgwire / mysql-wire, CONCEPT:EG-074) and the Bolt
//! adapter (CONCEPT:EG-159), this is an ADAPTER — NOT a re-implemented Redis. The
//! bytes are stored on the engine's own durable Key→Value substrate
//! ([`crate::server::kv::KvStore`], CONCEPT:EG-022): every Redis key lives as a
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
//! ## Protocol subset (CONCEPT:EG-174)
//!
//! LANDED: the RESP2 + RESP3 codec (simple strings, errors, integers, bulk
//! strings, arrays, and the RESP3 map/set/double/boolean/null types) + inline
//! commands, the `HELLO`/`COMMAND` handshake so `redis-cli` connects, and the core
//! command set: `PING`, `ECHO`, `GET`/`SET` (`EX`/`PX`/`NX`/`XX`), `DEL`, `EXISTS`,
//! `EXPIRE`/`TTL`, `INCR`/`DECR`, `MGET`/`MSET`, `HSET`/`HGET`/`HGETALL`/`HDEL`,
//! `LPUSH`/`RPUSH`/`LRANGE`/`LLEN`, `SADD`/`SMEMBERS`/`SREM`,
//! `ZADD`/`ZRANGE`/`ZSCORE`, `SCAN`, `TYPE`, plus `AUTH`/`SELECT`/`QUIT`/`CONFIG`/
//! `CLIENT` niceties. DEFERRED: pub/sub, transactions (`MULTI`/`EXEC`), Lua
//! scripting, streams, and cluster commands.

use std::sync::Arc;

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::RwLock;

use crate::server::kv::KvStore;
use crate::server::ServerState;

/// Env var: when set (and built `--features redis-wire`) the Redis RESP listener
/// binds this address (documented loopback default `127.0.0.1:6379`, the Redis
/// default port). Unset ⇒ no listener.
pub const REDIS_ADDR_ENV: &str = "EPISTEMIC_GRAPH_REDIS_ADDR";
/// Env var: an optional password. When set, a connection must `AUTH <password>`
/// (or `HELLO ... AUTH default <password>`) before running data commands.
pub const REDIS_PASSWORD_ENV: &str = "EPISTEMIC_GRAPH_REDIS_PASSWORD";

/// The single KV namespace every Redis key lives under. One msgpack [`Entry`] per
/// key, so `SCAN`/`EXISTS`/`TYPE`/`DEL` are a namespace scan / point read.
const NS: &str = "redis";

/// Wall-clock milliseconds since the epoch (TTL clock; same tolerance as the
/// blob/txn TTL sweeps).
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

// ── the stored value model (CONCEPT:EG-174) ─────────────────────────────────────

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

/// The WRONGTYPE error text (verbatim Redis wording, so clients that match on it
/// still work).
const WRONGTYPE: &str =
    "WRONGTYPE Operation against a key holding the wrong kind of value";
const NOT_INT: &str = "ERR value is not an integer or out of range";

// ── the store (CONCEPT:EG-174) — Redis types over the engine KV surface ──────────

/// A Redis keyspace layered over one [`KvStore`] namespace. Durable when the KV
/// store is (a persist dir), else in-memory scratch — exactly the KV contract.
pub struct RedisStore {
    kv: Arc<KvStore>,
    /// Serializes read-modify-write ops so multi-step mutations (e.g. `HSET`,
    /// `INCR`, `ZADD`) are atomic against concurrent connections.
    lock: Mutex<()>,
}

impl RedisStore {
    /// Open the Redis keyspace. `Some(dir)` ⇒ durable `{dir}/redis-kv/kv.redb`;
    /// `None` ⇒ in-memory scratch (CONCEPT:EG-174).
    pub fn open(persist_dir: Option<&str>) -> Result<Self, String> {
        let sub = persist_dir.map(|d| format!("{d}/redis-kv"));
        let kv = KvStore::open(sub.as_deref())?;
        Ok(Self {
            kv: Arc::new(kv),
            lock: Mutex::new(()),
        })
    }

    /// `true` if writes land durably on disk.
    pub fn is_durable(&self) -> bool {
        self.kv.is_durable()
    }

    // ── internal (no-lock) primitives ────────────────────────────────────────

    /// Load the live entry for `key`, transparently expiring (and deleting) a
    /// stale one. MUST be called with the store lock held.
    fn load(&self, key: &str) -> Result<Option<Entry>, String> {
        match self.kv.get(NS, key)? {
            Some(bytes) => {
                let entry: Entry = rmp_serde::from_slice(&bytes).map_err(|e| e.to_string())?;
                if entry.expired(now_ms()) {
                    self.kv.delete(NS, key)?;
                    Ok(None)
                } else {
                    Ok(Some(entry))
                }
            }
            None => Ok(None),
        }
    }

    fn store(&self, key: &str, entry: &Entry) -> Result<(), String> {
        let bytes = rmp_serde::to_vec_named(entry).map_err(|e| e.to_string())?;
        self.kv.put(NS, key, bytes)
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
            if self.load(k)?.is_some() && self.kv.delete(NS, k)? {
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
                    self.kv.delete(NS, key)?;
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
        for (k, v) in self.kv.scan(NS, "", 0)? {
            // Drop expired entries lazily as we walk.
            if let Ok(entry) = rmp_serde::from_slice::<Entry>(&v) {
                if entry.expired(now) {
                    continue;
                }
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
        let mut added = 0;
        for (f, val) in pairs {
            match h.iter_mut().find(|(ef, _)| ef == f) {
                Some(slot) => slot.1 = val.clone(),
                None => {
                    h.push((f.clone(), val.clone()));
                    added += 1;
                }
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

    fn hgetall(&self, key: &str) -> Result<Vec<(Vec<u8>, Vec<u8>)>, String> {
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
                let before = h.len();
                h.retain(|(f, _)| !fields.iter().any(|d| d == f));
                let removed = (before - h.len()) as i64;
                if h.is_empty() {
                    self.kv.delete(NS, key)?;
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
        for v in values {
            if left {
                list.insert(0, v.clone());
            } else {
                list.push(v.clone());
            }
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
        let mut added = 0;
        for m in members {
            if !set.iter().any(|e| e == m) {
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
                let before = s.len();
                s.retain(|e| !members.iter().any(|m| m == e));
                let removed = (before - s.len()) as i64;
                if s.is_empty() {
                    self.kv.delete(NS, key)?;
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
        let mut added = 0;
        for (score, member) in pairs {
            match z.iter_mut().find(|(m, _)| m == member) {
                Some(slot) => slot.1 = *score,
                None => {
                    z.push((member.clone(), *score));
                    added += 1;
                }
            }
        }
        z.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal).then(a.0.cmp(&b.0)));
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

// ── RESP2 / RESP3 codec (CONCEPT:EG-174) ─────────────────────────────────────────

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
            Resp::Bulk(None) | Resp::Null => {
                if proto >= 3 {
                    out.extend_from_slice(b"_\r\n");
                } else {
                    out.extend_from_slice(b"$-1\r\n");
                }
            }
            Resp::Array(Some(items)) => {
                out.push(b'*');
                out.extend_from_slice(items.len().to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
                for it in items {
                    it.encode(proto, out);
                }
            }
            Resp::Array(None) => {
                if proto >= 3 {
                    out.extend_from_slice(b"_\r\n");
                } else {
                    out.extend_from_slice(b"*-1\r\n");
                }
            }
            Resp::Map(pairs) => {
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
            Resp::Set(items) => {
                out.push(if proto >= 3 { b'~' } else { b'*' });
                out.extend_from_slice(items.len().to_string().as_bytes());
                out.extend_from_slice(b"\r\n");
                for it in items {
                    it.encode(proto, out);
                }
            }
            Resp::Double(d) => {
                if proto >= 3 {
                    out.push(b',');
                    out.extend_from_slice(fmt_double(*d).as_bytes());
                    out.extend_from_slice(b"\r\n");
                } else {
                    Resp::bulk_str(fmt_double(*d)).encode(proto, out);
                }
            }
            Resp::Bool(b) => {
                if proto >= 3 {
                    out.extend_from_slice(if *b { b"#t\r\n" } else { b"#f\r\n" });
                } else {
                    Resp::Int(if *b { 1 } else { 0 }).encode(proto, out);
                }
            }
        }
    }
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
fn try_parse_command(buf: &[u8]) -> Result<Option<(Vec<Vec<u8>>, usize)>, String> {
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

fn parse_inline_command(buf: &[u8]) -> Result<Option<(Vec<Vec<u8>>, usize)>, String> {
    match read_crlf_line(buf, 0) {
        Some((line, next)) => {
            let args = line
                .split(|b| *b == b' ' || *b == b'\t')
                .filter(|s| !s.is_empty())
                .map(|s| s.to_vec())
                .collect();
            Ok(Some((args, next)))
        }
        None => {
            // Guard against an unbounded inline line with no terminator.
            if buf.len() > 64 * 1024 {
                Err("ERR Protocol error: too big inline request".into())
            } else {
                Ok(None)
            }
        }
    }
}

fn parse_array_command(buf: &[u8]) -> Result<Option<(Vec<Vec<u8>>, usize)>, String> {
    let (header, mut pos) = match read_crlf_line(buf, 0) {
        Some(v) => v,
        None => return Ok(None),
    };
    let count: i64 = std::str::from_utf8(&header[1..])
        .ok()
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| "ERR Protocol error: invalid multibulk length".to_string())?;
    if count < 0 {
        return Ok(Some((Vec::new(), pos)));
    }
    let mut args = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let (blen_line, after_len) = match read_crlf_line(buf, pos) {
            Some(v) => v,
            None => return Ok(None),
        };
        if blen_line.first() != Some(&b'$') {
            return Err("ERR Protocol error: expected '$'".into());
        }
        let blen: i64 = std::str::from_utf8(&blen_line[1..])
            .ok()
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| "ERR Protocol error: invalid bulk length".to_string())?;
        if blen < 0 {
            args.push(Vec::new());
            pos = after_len;
            continue;
        }
        let start = after_len;
        let end = start + blen as usize;
        if buf.len() < end + 2 {
            return Ok(None); // bytes + trailing CRLF not all here yet
        }
        args.push(buf[start..end].to_vec());
        pos = end + 2; // skip the value + CRLF
    }
    Ok(Some((args, pos)))
}

// ── command dispatch (CONCEPT:EG-174) ─────────────────────────────────────────────

/// Per-connection mutable state threaded through command execution.
struct ConnState {
    proto: u8,
    authed: bool,
    quit: bool,
}

/// Uppercase an argument for case-insensitive command / option matching.
fn upper(b: &[u8]) -> String {
    String::from_utf8_lossy(b).to_ascii_uppercase()
}

/// Execute ONE parsed command against the store, returning the reply. Handshake /
/// session commands (`HELLO`/`AUTH`/`QUIT`/`SELECT`) mutate `conn`.
fn execute(store: &RedisStore, args: &[Vec<u8>], conn: &mut ConnState, password: Option<&str>) -> Resp {
    if args.is_empty() {
        return Resp::Error("ERR empty command".into());
    }
    let cmd = upper(&args[0]);

    // Session/handshake commands are always allowed (auth gate below is skipped).
    match cmd.as_str() {
        "PING" => {
            return match args.get(1) {
                Some(msg) => Resp::Bulk(Some(msg.clone())),
                None => Resp::Simple("PONG".into()),
            };
        }
        "ECHO" => {
            return match args.get(1) {
                Some(msg) => Resp::Bulk(Some(msg.clone())),
                None => Resp::Error("ERR wrong number of arguments for 'echo'".into()),
            };
        }
        "QUIT" => {
            conn.quit = true;
            return Resp::Simple("OK".into());
        }
        "HELLO" => return hello(args, conn, password),
        "AUTH" => {
            let given = args.last().map(|b| String::from_utf8_lossy(b).to_string());
            return match (password, given) {
                (Some(pw), Some(g)) if pw == g => {
                    conn.authed = true;
                    Resp::Simple("OK".into())
                }
                (None, _) => {
                    Resp::Error("ERR Client sent AUTH, but no password is set".into())
                }
                _ => Resp::Error("WRONGPASS invalid username-password pair".into()),
            };
        }
        "SELECT" => return Resp::Simple("OK".into()),
        "COMMAND" => return Resp::Array(Some(Vec::new())),
        "CONFIG" => return Resp::Array(Some(Vec::new())),
        "CLIENT" => return Resp::Simple("OK".into()),
        _ => {}
    }

    if !conn.authed {
        return Resp::Error("NOAUTH Authentication required.".into());
    }

    match execute_data(store, &cmd, args, conn.proto) {
        Ok(r) => r,
        Err(e) => Resp::Error(e),
    }
}

/// The `HELLO` handshake: optionally upgrade to RESP3 and (optionally) `AUTH`, then
/// reply with the server-info map.
fn hello(args: &[Vec<u8>], conn: &mut ConnState, password: Option<&str>) -> Resp {
    let mut i = 1;
    if let Some(v) = args.get(1) {
        // The protover is the first arg when it parses as a number.
        if let Ok(p) = String::from_utf8_lossy(v).parse::<u8>() {
            if p != 2 && p != 3 {
                return Resp::Error("NOPROTO unsupported protocol version".into());
            }
            conn.proto = p;
            i = 2;
        }
    }
    // Optional `AUTH <user> <pass>` clause.
    while i < args.len() {
        if upper(&args[i]) == "AUTH" && i + 2 < args.len() {
            let given = String::from_utf8_lossy(&args[i + 2]).to_string();
            match password {
                Some(pw) if pw == given => conn.authed = true,
                Some(_) => return Resp::Error("WRONGPASS invalid username-password pair".into()),
                None => {}
            }
            i += 3;
        } else {
            i += 1;
        }
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
fn execute_data(store: &RedisStore, cmd: &str, args: &[Vec<u8>], proto: u8) -> Result<Resp, String> {
    let key = |n: usize| -> Result<String, String> {
        args.get(n)
            .map(|b| String::from_utf8_lossy(b).into_owned())
            .ok_or_else(|| format!("ERR wrong number of arguments for '{}'", cmd.to_ascii_lowercase()))
    };
    match cmd {
        "SET" => {
            let k = key(1)?;
            let v = args.get(2).cloned().ok_or_else(|| "ERR wrong number of arguments for 'set'".to_string())?;
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
        "GET" => Ok(Resp::Bulk(store.get(&key(1)?)?)),
        "DEL" => {
            let keys: Vec<String> = args[1..].iter().map(|b| String::from_utf8_lossy(b).into_owned()).collect();
            let refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
            Ok(Resp::Int(store.del(&refs)?))
        }
        "EXISTS" => {
            let keys: Vec<String> = args[1..].iter().map(|b| String::from_utf8_lossy(b).into_owned()).collect();
            let refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
            Ok(Resp::Int(store.exists(&refs)?))
        }
        "EXPIRE" => {
            let k = key(1)?;
            let secs: i64 = parse_num(args.get(2))?;
            Ok(Resp::Int(store.expire(&k, secs)? as i64))
        }
        "TTL" => Ok(Resp::Int(store.ttl(&key(1)?)?)),
        "INCR" => Ok(Resp::Int(store.incr_by(&key(1)?, 1)?)),
        "DECR" => Ok(Resp::Int(store.incr_by(&key(1)?, -1)?)),
        "MGET" => {
            let mut out = Vec::new();
            for a in &args[1..] {
                let k = String::from_utf8_lossy(a);
                out.push(Resp::Bulk(store.get(&k)?));
            }
            Ok(Resp::Array(Some(out)))
        }
        "MSET" => {
            if args.len() < 3 || (args.len() - 1) % 2 != 0 {
                return Err("ERR wrong number of arguments for 'mset'".into());
            }
            let mut i = 1;
            while i + 1 < args.len() {
                let k = String::from_utf8_lossy(&args[i]).into_owned();
                store.set(&k, args[i + 1].clone(), None, false, false)?;
                i += 2;
            }
            Ok(Resp::Simple("OK".into()))
        }
        "HSET" => {
            let k = key(1)?;
            if args.len() < 4 || (args.len() - 2) % 2 != 0 {
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
        "HGET" => {
            let k = key(1)?;
            let field = args.get(2).ok_or_else(|| "ERR wrong number of arguments for 'hget'".to_string())?;
            Ok(Resp::Bulk(store.hget(&k, field)?))
        }
        "HGETALL" => {
            let pairs = store.hgetall(&key(1)?)?;
            let map = pairs
                .into_iter()
                .map(|(f, v)| (Resp::Bulk(Some(f)), Resp::Bulk(Some(v))))
                .collect();
            Ok(Resp::Map(map))
        }
        "HDEL" => {
            let k = key(1)?;
            let fields: Vec<Vec<u8>> = args[2..].to_vec();
            Ok(Resp::Int(store.hdel(&k, &fields)?))
        }
        "LPUSH" | "RPUSH" => {
            let k = key(1)?;
            let vals: Vec<Vec<u8>> = args[2..].to_vec();
            if vals.is_empty() {
                return Err(format!("ERR wrong number of arguments for '{}'", cmd.to_ascii_lowercase()));
            }
            Ok(Resp::Int(store.push(&k, &vals, cmd == "LPUSH")?))
        }
        "LRANGE" => {
            let k = key(1)?;
            let start: i64 = parse_num(args.get(2))?;
            let stop: i64 = parse_num(args.get(3))?;
            let items = store.lrange(&k, start, stop)?.into_iter().map(|v| Resp::Bulk(Some(v))).collect();
            Ok(Resp::Array(Some(items)))
        }
        "LLEN" => Ok(Resp::Int(store.llen(&key(1)?)?)),
        "SADD" => {
            let k = key(1)?;
            let members: Vec<Vec<u8>> = args[2..].to_vec();
            if members.is_empty() {
                return Err("ERR wrong number of arguments for 'sadd'".into());
            }
            Ok(Resp::Int(store.sadd(&k, &members)?))
        }
        "SMEMBERS" => {
            let items = store.smembers(&key(1)?)?.into_iter().map(|v| Resp::Bulk(Some(v))).collect();
            Ok(Resp::Set(items))
        }
        "SREM" => {
            let k = key(1)?;
            let members: Vec<Vec<u8>> = args[2..].to_vec();
            Ok(Resp::Int(store.srem(&k, &members)?))
        }
        "ZADD" => {
            let k = key(1)?;
            if args.len() < 4 || (args.len() - 2) % 2 != 0 {
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
        "ZRANGE" => {
            let k = key(1)?;
            let start: i64 = parse_num(args.get(2))?;
            let stop: i64 = parse_num(args.get(3))?;
            let withscores = args.get(4).map(|b| upper(b) == "WITHSCORES").unwrap_or(false);
            let z = store.zrange(&k, start, stop)?;
            let mut out = Vec::new();
            for (m, s) in z {
                out.push(Resp::Bulk(Some(m)));
                if withscores {
                    out.push(if proto >= 3 { Resp::Double(s) } else { Resp::bulk_str(fmt_double(s)) });
                }
            }
            Ok(Resp::Array(Some(out)))
        }
        "ZSCORE" => {
            let k = key(1)?;
            let member = args.get(2).ok_or_else(|| "ERR wrong number of arguments for 'zscore'".to_string())?;
            match store.zscore(&k, member)? {
                Some(s) => Ok(Resp::bulk_str(fmt_double(s))),
                None => Ok(Resp::Null),
            }
        }
        "SCAN" => {
            // SCAN cursor [MATCH pattern] [COUNT n] [TYPE t] — one-shot (cursor 0).
            let mut pattern = None;
            let mut i = 2;
            while i < args.len() {
                match upper(&args[i]).as_str() {
                    "MATCH" => {
                        pattern = args.get(i + 1).map(|b| String::from_utf8_lossy(b).into_owned());
                        i += 2;
                    }
                    "COUNT" | "TYPE" => i += 2, // accepted, ignored (one-shot scan)
                    _ => i += 1,
                }
            }
            let keys = store.scan(pattern.as_deref())?;
            let items = keys.into_iter().map(|k| Resp::bulk_str(k)).collect();
            Ok(Resp::Array(Some(vec![Resp::bulk_str("0"), Resp::Array(Some(items))])))
        }
        "TYPE" => Ok(Resp::Simple(store.type_of(&key(1)?)?.into())),
        other => Err(format!("ERR unknown command '{}'", other.to_ascii_lowercase())),
    }
}

fn parse_num<T: std::str::FromStr>(arg: Option<&Vec<u8>>) -> Result<T, String> {
    arg.and_then(|b| std::str::from_utf8(b).ok())
        .and_then(|s| s.trim().parse().ok())
        .ok_or_else(|| "ERR value is not an integer or out of range".to_string())
}

// ── the per-connection driver + listener ──────────────────────────────────────────

/// Drive ONE Redis connection: parse commands from the socket, execute, reply,
/// until the client quits or the socket closes. Generic over the byte stream so an
/// in-process test can drive it over any duplex transport (CONCEPT:EG-174).
async fn handle_connection<S>(s: &mut S, store: Arc<RedisStore>, password: Option<String>) -> std::io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut conn = ConnState {
        proto: 2,
        authed: password.is_none(),
        quit: false,
    };
    let mut buf: Vec<u8> = Vec::new();
    let mut tmp = [0u8; 8192];
    loop {
        // Try to carve a complete command out of what we already have.
        let parsed = match try_parse_command(&buf) {
            Ok(Some(v)) => Some(v),
            Ok(None) => None,
            Err(e) => {
                let mut out = Vec::new();
                Resp::Error(e).encode(conn.proto, &mut out);
                s.write_all(&out).await?;
                return Ok(());
            }
        };
        let (args, consumed) = match parsed {
            Some(v) => v,
            None => {
                let n = s.read(&mut tmp).await?;
                if n == 0 {
                    return Ok(()); // client closed
                }
                buf.extend_from_slice(&tmp[..n]);
                if buf.len() > 512 * 1024 * 1024 {
                    return Ok(()); // runaway request guard
                }
                continue;
            }
        };
        buf.drain(..consumed);
        if args.is_empty() {
            continue;
        }
        let reply = execute(&store, &args, &mut conn, password.as_deref());
        let mut out = Vec::new();
        reply.encode(conn.proto, &mut out);
        s.write_all(&out).await?;
        if conn.quit {
            let _ = s.shutdown().await;
            return Ok(());
        }
    }
}

/// Bind `addr` and serve Redis RESP connections until the process exits. Spawned by
/// `main.rs` only when built `--features redis-wire` AND `EPISTEMIC_GRAPH_REDIS_ADDR`
/// is set (CONCEPT:EG-174). The Redis keyspace is durable when a persist dir is
/// configured on [`ServerState`], else in-memory scratch.
pub async fn serve(addr: &str, state: Arc<RwLock<ServerState>>) -> std::io::Result<()> {
    let persist_dir = { state.read().await.persist_dir.clone() };
    let store = Arc::new(
        RedisStore::open(persist_dir.as_deref())
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?,
    );
    let password = std::env::var(REDIS_PASSWORD_ENV).ok().filter(|s| !s.is_empty());
    serve_with_store(addr, store, password).await
}

/// `serve` with an EXPLICIT store + password (CONCEPT:EG-174) — tests bind an
/// ephemeral store on a random port without touching process env / `ServerState`.
pub async fn serve_with_store(
    addr: &str,
    store: Arc<RedisStore>,
    password: Option<String>,
) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    tracing::info!(
        "redis-wire: serving Redis RESP protocol on {} (durable={}, auth={})",
        addr,
        store.is_durable(),
        password.is_some()
    );
    loop {
        let (mut socket, peer) = listener.accept().await?;
        let store = store.clone();
        let password = password.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_connection(&mut socket, store, password).await {
                tracing::debug!("redis-wire connection from {peer} ended: {e}");
            }
        });
    }
}

#[cfg(test)]
mod tests {
    //! CONCEPT:EG-174 — RESP2/RESP3 codec round-trips, command-parse coverage, the
    //! Redis data-type command execution over the KV store, plus an in-process
    //! listener smoke test driving the real `serve_with_store` over a TCP socket
    //! with hand-built RESP frames (no redis client crate).
    use super::*;
    use tokio::net::TcpStream;

    fn mem_store() -> Arc<RedisStore> {
        Arc::new(RedisStore::open(None).unwrap())
    }

    fn conn3() -> ConnState {
        ConnState {
            proto: 3,
            authed: true,
            quit: false,
        }
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
            enc(&Resp::Array(Some(vec![Resp::Int(1), Resp::bulk_str("a")])), 2),
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
        assert_eq!(execute(&store, &a(&["SET", "k", "v"]), &mut c, None), Resp::Simple("OK".into()));
        assert_eq!(execute(&store, &a(&["GET", "k"]), &mut c, None), Resp::Bulk(Some(b"v".to_vec())));
        assert_eq!(execute(&store, &a(&["TYPE", "k"]), &mut c, None), Resp::Simple("string".into()));
        assert_eq!(execute(&store, &a(&["EXISTS", "k", "nope"]), &mut c, None), Resp::Int(1));
        // NX on an existing key → null.
        assert_eq!(execute(&store, &a(&["SET", "k", "v2", "NX"]), &mut c, None), Resp::Null);
        // INCR path.
        assert_eq!(execute(&store, &a(&["SET", "n", "10"]), &mut c, None), Resp::Simple("OK".into()));
        assert_eq!(execute(&store, &a(&["INCR", "n"]), &mut c, None), Resp::Int(11));
        assert_eq!(execute(&store, &a(&["DECR", "n"]), &mut c, None), Resp::Int(10));
        // EXPIRE/TTL.
        assert_eq!(execute(&store, &a(&["EXPIRE", "k", "100"]), &mut c, None), Resp::Int(1));
        assert!(matches!(execute(&store, &a(&["TTL", "k"]), &mut c, None), Resp::Int(t) if t > 0 && t <= 100));
        // DEL.
        assert_eq!(execute(&store, &a(&["DEL", "k", "n"]), &mut c, None), Resp::Int(2));
        assert_eq!(execute(&store, &a(&["GET", "k"]), &mut c, None), Resp::Bulk(None));
    }

    #[test]
    fn eg174_hash_list_set_zset_exec() {
        let store = mem_store();
        let mut c = conn3();
        // HSET / HGET / HGETALL.
        assert_eq!(execute(&store, &a(&["HSET", "h", "f1", "v1", "f2", "v2"]), &mut c, None), Resp::Int(2));
        assert_eq!(execute(&store, &a(&["HGET", "h", "f1"]), &mut c, None), Resp::Bulk(Some(b"v1".to_vec())));
        assert_eq!(
            execute(&store, &a(&["HGETALL", "h"]), &mut c, None),
            Resp::Map(vec![
                (Resp::bulk_str("f1"), Resp::bulk_str("v1")),
                (Resp::bulk_str("f2"), Resp::bulk_str("v2")),
            ])
        );
        // LPUSH / RPUSH / LRANGE / LLEN. LPUSH a b c → head order c b a.
        assert_eq!(execute(&store, &a(&["RPUSH", "l", "a", "b"]), &mut c, None), Resp::Int(2));
        assert_eq!(execute(&store, &a(&["LPUSH", "l", "z"]), &mut c, None), Resp::Int(3));
        assert_eq!(execute(&store, &a(&["LLEN", "l"]), &mut c, None), Resp::Int(3));
        assert_eq!(
            execute(&store, &a(&["LRANGE", "l", "0", "-1"]), &mut c, None),
            Resp::Array(Some(vec![Resp::bulk_str("z"), Resp::bulk_str("a"), Resp::bulk_str("b")]))
        );
        // SADD / SMEMBERS / SREM (dedup).
        assert_eq!(execute(&store, &a(&["SADD", "s", "x", "y", "x"]), &mut c, None), Resp::Int(2));
        assert_eq!(execute(&store, &a(&["SREM", "s", "x"]), &mut c, None), Resp::Int(1));
        assert_eq!(execute(&store, &a(&["SMEMBERS", "s"]), &mut c, None), Resp::Set(vec![Resp::bulk_str("y")]));
        // ZADD / ZRANGE / ZSCORE (sorted by score).
        assert_eq!(execute(&store, &a(&["ZADD", "z", "2", "b", "1", "a"]), &mut c, None), Resp::Int(2));
        assert_eq!(
            execute(&store, &a(&["ZRANGE", "z", "0", "-1"]), &mut c, None),
            Resp::Array(Some(vec![Resp::bulk_str("a"), Resp::bulk_str("b")]))
        );
        assert_eq!(execute(&store, &a(&["ZSCORE", "z", "b"]), &mut c, None), Resp::bulk_str("2"));
    }

    #[test]
    fn eg174_wrongtype_is_reported() {
        let store = mem_store();
        let mut c = conn3();
        execute(&store, &a(&["SET", "k", "v"]), &mut c, None);
        // A hash op against a string key → WRONGTYPE.
        match execute(&store, &a(&["HGET", "k", "f"]), &mut c, None) {
            Resp::Error(e) => assert!(e.starts_with("WRONGTYPE"), "{e}"),
            other => panic!("expected WRONGTYPE, got {other:?}"),
        }
    }

    #[test]
    fn eg174_auth_gate_and_hello_upgrade() {
        let store = mem_store();
        let mut c = ConnState {
            proto: 2,
            authed: false,
            quit: false,
        };
        // Data command before AUTH → NOAUTH.
        match execute(&store, &a(&["GET", "k"]), &mut c, Some("secret")) {
            Resp::Error(e) => assert!(e.starts_with("NOAUTH"), "{e}"),
            other => panic!("expected NOAUTH, got {other:?}"),
        }
        // Wrong password rejected.
        assert!(matches!(execute(&store, &a(&["AUTH", "nope"]), &mut c, Some("secret")), Resp::Error(_)));
        // Correct password accepted → subsequent command allowed.
        assert_eq!(execute(&store, &a(&["AUTH", "secret"]), &mut c, Some("secret")), Resp::Simple("OK".into()));
        assert_eq!(execute(&store, &a(&["GET", "k"]), &mut c, Some("secret")), Resp::Bulk(None));
        // HELLO 3 upgrades the protocol.
        assert!(matches!(execute(&store, &a(&["HELLO", "3"]), &mut c, None), Resp::Map(_)));
        assert_eq!(c.proto, 3);
    }

    // ── in-process listener smoke test over a real socket ────────────────────────

    async fn read_reply(stream: &mut TcpStream) -> Vec<u8> {
        // Read once; the smoke test replies are small and fit one read.
        let mut buf = [0u8; 4096];
        let n = stream.read(&mut buf).await.unwrap();
        buf[..n].to_vec()
    }

    #[tokio::test]
    async fn eg174_listener_roundtrip_over_tcp() {
        let probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = probe.local_addr().unwrap().to_string();
        drop(probe);
        let serve_addr = addr.clone();
        tokio::spawn(async move {
            let _ = serve_with_store(&serve_addr, mem_store(), None).await;
        });
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;

        let mut s = TcpStream::connect(&addr).await.unwrap();
        // PING (inline).
        s.write_all(b"PING\r\n").await.unwrap();
        assert_eq!(read_reply(&mut s).await, b"+PONG\r\n");
        // SET then GET (array form).
        s.write_all(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$5\r\nhello\r\n").await.unwrap();
        assert_eq!(read_reply(&mut s).await, b"+OK\r\n");
        s.write_all(b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n").await.unwrap();
        assert_eq!(read_reply(&mut s).await, b"$5\r\nhello\r\n");
    }
}
