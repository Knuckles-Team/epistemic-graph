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
//! `CLIENT` niceties.
//!
//! ## Pub/sub + transactions (CONCEPT:EG-307)
//!
//! LANDED (EG-307): the publish/subscribe surface — `SUBSCRIBE`/`UNSUBSCRIBE`,
//! `PSUBSCRIBE`/`PUNSUBSCRIBE` (glob-pattern channels), and `PUBLISH`, backed by a
//! per-listener [`PubSub`] registry (an mpsc channel per connection; the connection
//! driver `select!`s between the socket and its subscriber mailbox so published
//! messages are pushed out as they arrive). Plus `MULTI`/`EXEC`/`DISCARD`
//! transactions: commands after `MULTI` are queued (`+QUEUED`) and executed
//! back-to-back on `EXEC` (no other connection interleaves), returning the array of
//! replies; `DISCARD` drops the queue; a malformed queued command aborts the whole
//! transaction with `EXECABORT`. DEFERRED: Lua scripting, streams, `WATCH`
//! optimistic locking (parsed/no-op), and cluster commands.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
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

/// A set of `(field, value)` byte pairs (hash entries / bulk arg pairs).
type BytePairs = Vec<(Vec<u8>, Vec<u8>)>;
/// The result of parsing one command out of the read buffer: `Ok(None)` ⇒
/// incomplete (read more), `Ok(Some((args, consumed)))` ⇒ a complete command.
type CommandParse = Result<Option<(Vec<Vec<u8>>, usize)>, String>;

/// The WRONGTYPE error text (verbatim Redis wording, so clients that match on it
/// still work).
const WRONGTYPE: &str = "WRONGTYPE Operation against a key holding the wrong kind of value";
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

// ── pub/sub registry (CONCEPT:EG-307) ────────────────────────────────────────────

/// One message delivered to a subscriber's mailbox (CONCEPT:EG-307). A `Channel`
/// message is rendered as a RESP `message` push; a `Pattern` message (from a glob
/// `PSUBSCRIBE`) as a `pmessage` push carrying the originating pattern.
#[derive(Clone, Debug)]
enum PubMessage {
    Channel {
        channel: String,
        payload: Vec<u8>,
    },
    Pattern {
        pattern: String,
        channel: String,
        payload: Vec<u8>,
    },
}

impl PubMessage {
    /// Render this delivery as the RESP push frame Redis clients expect.
    fn to_resp(&self) -> Resp {
        match self {
            PubMessage::Channel { channel, payload } => Resp::Push(vec![
                Resp::bulk_str("message"),
                Resp::bulk_str(channel.clone().into_bytes()),
                Resp::Bulk(Some(payload.clone())),
            ]),
            PubMessage::Pattern {
                pattern,
                channel,
                payload,
            } => Resp::Push(vec![
                Resp::bulk_str("pmessage"),
                Resp::bulk_str(pattern.clone().into_bytes()),
                Resp::bulk_str(channel.clone().into_bytes()),
                Resp::Bulk(Some(payload.clone())),
            ]),
        }
    }
}

#[derive(Default)]
struct PubSubInner {
    next_id: u64,
    /// conn-id → the mailbox sender for that connection.
    conns: HashMap<u64, mpsc::UnboundedSender<PubMessage>>,
    /// exact channel → the set of conn-ids subscribed to it.
    channels: HashMap<String, HashSet<u64>>,
    /// glob pattern → the set of conn-ids subscribed to it.
    patterns: HashMap<String, HashSet<u64>>,
}

/// The per-listener publish/subscribe registry (CONCEPT:EG-307). Shared (via `Arc`)
/// across every connection the listener accepts; each connection registers an
/// unbounded mpsc mailbox on connect and drops it on disconnect. `PUBLISH` fans a
/// payload out to every exact-channel subscriber plus every glob-pattern subscriber
/// whose pattern matches, returning the delivery count. All state lives under one
/// `parking_lot::Mutex` — the sends are non-blocking (unbounded), so the lock is
/// never held across an `.await`.
#[derive(Default)]
pub struct PubSub {
    inner: Mutex<PubSubInner>,
}

impl PubSub {
    /// Register a fresh connection mailbox, returning its unique connection id.
    fn register(&self, tx: mpsc::UnboundedSender<PubMessage>) -> u64 {
        let mut g = self.inner.lock();
        g.next_id += 1;
        let id = g.next_id;
        g.conns.insert(id, tx);
        id
    }

    /// Drop a connection: remove its mailbox and prune it from every channel /
    /// pattern subscription (garbage-collecting now-empty entries).
    fn unregister(&self, id: u64) {
        let mut g = self.inner.lock();
        g.conns.remove(&id);
        g.channels.retain(|_, ids| {
            ids.remove(&id);
            !ids.is_empty()
        });
        g.patterns.retain(|_, ids| {
            ids.remove(&id);
            !ids.is_empty()
        });
    }

    fn subscribe(&self, id: u64, channel: &str) {
        self.inner
            .lock()
            .channels
            .entry(channel.to_string())
            .or_default()
            .insert(id);
    }

    fn unsubscribe(&self, id: u64, channel: &str) {
        let mut g = self.inner.lock();
        if let Some(ids) = g.channels.get_mut(channel) {
            ids.remove(&id);
            if ids.is_empty() {
                g.channels.remove(channel);
            }
        }
    }

    fn psubscribe(&self, id: u64, pattern: &str) {
        self.inner
            .lock()
            .patterns
            .entry(pattern.to_string())
            .or_default()
            .insert(id);
    }

    fn punsubscribe(&self, id: u64, pattern: &str) {
        let mut g = self.inner.lock();
        if let Some(ids) = g.patterns.get_mut(pattern) {
            ids.remove(&id);
            if ids.is_empty() {
                g.patterns.remove(pattern);
            }
        }
    }

    /// Fan `payload` out to every exact subscriber of `channel` and every pattern
    /// subscriber whose glob matches it. Returns the number of deliveries (the
    /// integer `PUBLISH` replies with). A dropped receiver (a connection that has
    /// gone away but not yet unregistered) simply isn't counted.
    fn publish(&self, channel: &str, payload: &[u8]) -> i64 {
        let g = self.inner.lock();
        let mut count = 0i64;
        if let Some(ids) = g.channels.get(channel) {
            for id in ids {
                if let Some(tx) = g.conns.get(id) {
                    let msg = PubMessage::Channel {
                        channel: channel.to_string(),
                        payload: payload.to_vec(),
                    };
                    if tx.send(msg).is_ok() {
                        count += 1;
                    }
                }
            }
        }
        for (pat, ids) in g.patterns.iter() {
            if !glob_match(pat, channel) {
                continue;
            }
            for id in ids {
                if let Some(tx) = g.conns.get(id) {
                    let msg = PubMessage::Pattern {
                        pattern: pat.clone(),
                        channel: channel.to_string(),
                        payload: payload.to_vec(),
                    };
                    if tx.send(msg).is_ok() {
                        count += 1;
                    }
                }
            }
        }
        count
    }
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
    /// A RESP3 push message (`>`, used for pub/sub delivery + subscribe confirms,
    /// CONCEPT:EG-307). Downgrades to a plain array (`*`) on RESP2 — the RESP2
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
            Resp::Push(items) => {
                out.push(if proto >= 3 { b'>' } else { b'*' });
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

fn parse_array_command(buf: &[u8]) -> CommandParse {
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

/// Per-connection mutable state threaded through command execution. Carries the
/// RESP version + auth flag, the pub/sub subscription sets, and the `MULTI`
/// transaction queue (CONCEPT:EG-174 core; CONCEPT:EG-307 pub/sub + transactions).
struct ConnState {
    proto: u8,
    authed: bool,
    quit: bool,
    /// This connection's unique id in the [`PubSub`] registry (0 until registered;
    /// the pure-`execute` unit tests never register, which is fine).
    id: u64,
    /// Channels this connection is `SUBSCRIBE`d to (CONCEPT:EG-307).
    sub_channels: HashSet<String>,
    /// Glob patterns this connection is `PSUBSCRIBE`d to (CONCEPT:EG-307).
    sub_patterns: HashSet<String>,
    /// `true` between `MULTI` and `EXEC`/`DISCARD`: commands are queued not run.
    in_multi: bool,
    /// The queued commands awaiting `EXEC` (CONCEPT:EG-307).
    queued: Vec<Vec<Vec<u8>>>,
    /// Set when a queued command was malformed → `EXEC` aborts with `EXECABORT`.
    multi_dirty: bool,
}

impl ConnState {
    fn new(proto: u8, authed: bool) -> Self {
        ConnState {
            proto,
            authed,
            quit: false,
            id: 0,
            sub_channels: HashSet::new(),
            sub_patterns: HashSet::new(),
            in_multi: false,
            queued: Vec::new(),
            multi_dirty: false,
        }
    }

    /// Total live subscriptions (channels + patterns) — the count Redis echoes in
    /// every subscribe/unsubscribe confirmation.
    fn sub_count(&self) -> i64 {
        (self.sub_channels.len() + self.sub_patterns.len()) as i64
    }
}

/// Uppercase an argument for case-insensitive command / option matching.
fn upper(b: &[u8]) -> String {
    String::from_utf8_lossy(b).to_ascii_uppercase()
}

/// Execute ONE parsed command against the store, returning the reply. Handshake /
/// session commands (`HELLO`/`AUTH`/`QUIT`/`SELECT`) mutate `conn`.
fn execute(
    store: &RedisStore,
    args: &[Vec<u8>],
    conn: &mut ConnState,
    password: Option<&str>,
) -> Resp {
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
                (None, _) => Resp::Error("ERR Client sent AUTH, but no password is set".into()),
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
fn execute_data(
    store: &RedisStore,
    cmd: &str,
    args: &[Vec<u8>],
    proto: u8,
) -> Result<Resp, String> {
    let key = |n: usize| -> Result<String, String> {
        args.get(n)
            .map(|b| String::from_utf8_lossy(b).into_owned())
            .ok_or_else(|| {
                format!(
                    "ERR wrong number of arguments for '{}'",
                    cmd.to_ascii_lowercase()
                )
            })
    };
    match cmd {
        "SET" => {
            let k = key(1)?;
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
        "GET" => Ok(Resp::Bulk(store.get(&key(1)?)?)),
        "DEL" => {
            let keys: Vec<String> = args[1..]
                .iter()
                .map(|b| String::from_utf8_lossy(b).into_owned())
                .collect();
            let refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
            Ok(Resp::Int(store.del(&refs)?))
        }
        "EXISTS" => {
            let keys: Vec<String> = args[1..]
                .iter()
                .map(|b| String::from_utf8_lossy(b).into_owned())
                .collect();
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
            if args.len() < 3 || !(args.len() - 1).is_multiple_of(2) {
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
        "HGET" => {
            let k = key(1)?;
            let field = args
                .get(2)
                .ok_or_else(|| "ERR wrong number of arguments for 'hget'".to_string())?;
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
                return Err(format!(
                    "ERR wrong number of arguments for '{}'",
                    cmd.to_ascii_lowercase()
                ));
            }
            Ok(Resp::Int(store.push(&k, &vals, cmd == "LPUSH")?))
        }
        "LRANGE" => {
            let k = key(1)?;
            let start: i64 = parse_num(args.get(2))?;
            let stop: i64 = parse_num(args.get(3))?;
            let items = store
                .lrange(&k, start, stop)?
                .into_iter()
                .map(|v| Resp::Bulk(Some(v)))
                .collect();
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
            let items = store
                .smembers(&key(1)?)?
                .into_iter()
                .map(|v| Resp::Bulk(Some(v)))
                .collect();
            Ok(Resp::Set(items))
        }
        "SREM" => {
            let k = key(1)?;
            let members: Vec<Vec<u8>> = args[2..].to_vec();
            Ok(Resp::Int(store.srem(&k, &members)?))
        }
        "ZADD" => {
            let k = key(1)?;
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
        "ZRANGE" => {
            let k = key(1)?;
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
                    out.push(if proto >= 3 {
                        Resp::Double(s)
                    } else {
                        Resp::bulk_str(fmt_double(s))
                    });
                }
            }
            Ok(Resp::Array(Some(out)))
        }
        "ZSCORE" => {
            let k = key(1)?;
            let member = args
                .get(2)
                .ok_or_else(|| "ERR wrong number of arguments for 'zscore'".to_string())?;
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
                        pattern = args
                            .get(i + 1)
                            .map(|b| String::from_utf8_lossy(b).into_owned());
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
        "TYPE" => Ok(Resp::Simple(store.type_of(&key(1)?)?.into())),
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

// ── pub/sub + transaction dispatch (CONCEPT:EG-307) ───────────────────────────────

/// Commands that are ALWAYS run immediately, never queued, even inside a `MULTI`
/// block (they steer the transaction / session itself).
fn is_multi_control(cmd: &str) -> bool {
    matches!(
        cmd,
        "MULTI" | "EXEC" | "DISCARD" | "WATCH" | "UNWATCH" | "RESET" | "QUIT"
    )
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

/// Top-level command dispatch used by the connection driver (CONCEPT:EG-307).
/// Unlike [`execute`] (one reply) this returns a VECTOR of replies — subscribe /
/// unsubscribe emit one confirmation per channel — and threads the [`PubSub`]
/// registry plus the connection's transaction/subscription state. Non-pub/sub,
/// non-transaction commands delegate to [`execute`] for their single reply.
fn dispatch(
    store: &RedisStore,
    pubsub: &PubSub,
    args: &[Vec<u8>],
    conn: &mut ConnState,
    password: Option<&str>,
) -> Vec<Resp> {
    if args.is_empty() {
        return vec![Resp::Error("ERR empty command".into())];
    }
    let cmd = upper(&args[0]);

    // Queue everything (except control verbs) while inside MULTI.
    if conn.in_multi && !is_multi_control(&cmd) {
        if !is_known_command(&cmd) && !allowed_in_subscribe(&cmd) {
            conn.multi_dirty = true;
            return vec![Resp::Error(format!(
                "ERR unknown command '{}'",
                cmd.to_ascii_lowercase()
            ))];
        }
        // SUBSCRIBE-family commands are not allowed inside a transaction.
        if matches!(
            cmd.as_str(),
            "SUBSCRIBE" | "UNSUBSCRIBE" | "PSUBSCRIBE" | "PUNSUBSCRIBE"
        ) {
            conn.multi_dirty = true;
            return vec![Resp::Error(format!(
                "ERR {} is not allowed in transactions",
                cmd
            ))];
        }
        conn.queued.push(args.to_vec());
        return vec![Resp::Simple("QUEUED".into())];
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

    match cmd.as_str() {
        "MULTI" => {
            if conn.in_multi {
                vec![Resp::Error("ERR MULTI calls can not be nested".into())]
            } else {
                conn.in_multi = true;
                conn.queued.clear();
                conn.multi_dirty = false;
                vec![Resp::Simple("OK".into())]
            }
        }
        "DISCARD" => {
            if !conn.in_multi {
                vec![Resp::Error("ERR DISCARD without MULTI".into())]
            } else {
                conn.in_multi = false;
                conn.queued.clear();
                conn.multi_dirty = false;
                vec![Resp::Simple("OK".into())]
            }
        }
        "EXEC" => exec_transaction(store, conn, password),
        "SUBSCRIBE" | "PSUBSCRIBE" => {
            if password.is_some() && !conn.authed {
                return vec![Resp::Error("NOAUTH Authentication required.".into())];
            }
            subscribe(pubsub, conn, &args[1..], cmd == "PSUBSCRIBE")
        }
        "UNSUBSCRIBE" | "PUNSUBSCRIBE" => {
            unsubscribe(pubsub, conn, &args[1..], cmd == "PUNSUBSCRIBE")
        }
        "PUBLISH" => {
            if password.is_some() && !conn.authed {
                return vec![Resp::Error("NOAUTH Authentication required.".into())];
            }
            match (args.get(1), args.get(2)) {
                (Some(chan), Some(payload)) => {
                    let channel = String::from_utf8_lossy(chan).into_owned();
                    vec![Resp::Int(pubsub.publish(&channel, payload))]
                }
                _ => vec![Resp::Error(
                    "ERR wrong number of arguments for 'publish'".into(),
                )],
            }
        }
        // WATCH/UNWATCH: accepted as no-ops (no optimistic locking yet, EG-307).
        "WATCH" | "UNWATCH" => vec![Resp::Simple("OK".into())],
        "RESET" => {
            for c in conn.sub_channels.drain().collect::<Vec<_>>() {
                pubsub.unsubscribe(conn.id, &c);
            }
            for p in conn.sub_patterns.drain().collect::<Vec<_>>() {
                pubsub.punsubscribe(conn.id, &p);
            }
            conn.in_multi = false;
            conn.queued.clear();
            conn.multi_dirty = false;
            conn.proto = 2;
            vec![Resp::Simple("RESET".into())]
        }
        _ => vec![execute(store, args, conn, password)],
    }
}

/// Execute a queued `MULTI` transaction atomically (CONCEPT:EG-307): run every
/// queued command in order with no other connection interleaving, returning the
/// array of their replies. A prior malformed queued command aborts with
/// `EXECABORT`; `EXEC` outside a transaction is an error.
fn exec_transaction(store: &RedisStore, conn: &mut ConnState, password: Option<&str>) -> Vec<Resp> {
    if !conn.in_multi {
        return vec![Resp::Error("ERR EXEC without MULTI".into())];
    }
    conn.in_multi = false;
    let queued = std::mem::take(&mut conn.queued);
    if std::mem::take(&mut conn.multi_dirty) {
        return vec![Resp::Error(
            "EXECABORT Transaction discarded because of previous errors.".into(),
        )];
    }
    let mut results = Vec::with_capacity(queued.len());
    for qargs in queued {
        results.push(execute(store, &qargs, conn, password));
    }
    vec![Resp::Array(Some(results))]
}

/// `SUBSCRIBE` / `PSUBSCRIBE` (CONCEPT:EG-307): add each channel/pattern to the
/// registry + the connection's set, emitting one confirmation push per channel with
/// the running total subscription count.
fn subscribe(pubsub: &PubSub, conn: &mut ConnState, chans: &[Vec<u8>], pattern: bool) -> Vec<Resp> {
    let kind = if pattern { "psubscribe" } else { "subscribe" };
    if chans.is_empty() {
        return vec![Resp::Error(format!(
            "ERR wrong number of arguments for '{kind}'"
        ))];
    }
    let mut out = Vec::with_capacity(chans.len());
    for c in chans {
        let name = String::from_utf8_lossy(c).into_owned();
        if pattern {
            if conn.sub_patterns.insert(name.clone()) {
                pubsub.psubscribe(conn.id, &name);
            }
        } else if conn.sub_channels.insert(name.clone()) {
            pubsub.subscribe(conn.id, &name);
        }
        out.push(Resp::Push(vec![
            Resp::bulk_str(kind),
            Resp::bulk_str(name.into_bytes()),
            Resp::Int(conn.sub_count()),
        ]));
    }
    out
}

/// `UNSUBSCRIBE` / `PUNSUBSCRIBE` (CONCEPT:EG-307): drop the named channels (or ALL
/// of this kind when none are named), one confirmation push each. Unsubscribing from
/// nothing still emits a single null-channel confirmation, matching Redis.
fn unsubscribe(
    pubsub: &PubSub,
    conn: &mut ConnState,
    chans: &[Vec<u8>],
    pattern: bool,
) -> Vec<Resp> {
    let kind = if pattern { "punsubscribe" } else { "unsubscribe" };
    let targets: Vec<String> = if chans.is_empty() {
        if pattern {
            conn.sub_patterns.iter().cloned().collect()
        } else {
            conn.sub_channels.iter().cloned().collect()
        }
    } else {
        chans
            .iter()
            .map(|c| String::from_utf8_lossy(c).into_owned())
            .collect()
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
        if pattern {
            conn.sub_patterns.remove(&name);
            pubsub.punsubscribe(conn.id, &name);
        } else {
            conn.sub_channels.remove(&name);
            pubsub.unsubscribe(conn.id, &name);
        }
        out.push(Resp::Push(vec![
            Resp::bulk_str(kind),
            Resp::bulk_str(name.into_bytes()),
            Resp::Int(conn.sub_count()),
        ]));
    }
    out
}

// ── the per-connection driver + listener ──────────────────────────────────────────

/// Drive ONE Redis connection: parse commands from the socket, execute, reply,
/// until the client quits or the socket closes. Generic over the byte stream so an
/// in-process test can drive it over any duplex transport (CONCEPT:EG-174).
async fn handle_connection<S>(
    s: &mut S,
    store: Arc<RedisStore>,
    pubsub: Arc<PubSub>,
    password: Option<String>,
) -> std::io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut conn = ConnState::new(2, password.is_none());
    // Register this connection's pub/sub mailbox so PUBLISH can reach it (EG-307).
    let (tx, mut rx) = mpsc::unbounded_channel::<PubMessage>();
    conn.id = pubsub.register(tx);
    let result = drive_connection(s, &store, &pubsub, &mut conn, &mut rx, password).await;
    // Always release the registry slot + all subscriptions on the way out.
    pubsub.unregister(conn.id);
    result
}

/// The inner connection loop (split out so [`handle_connection`] can guarantee the
/// [`PubSub`] unregister runs on every exit path). `select!`s between the socket and
/// this connection's subscriber mailbox: buffered client commands are executed
/// first, then it awaits either more bytes or a published message to push out
/// (CONCEPT:EG-307).
async fn drive_connection<S>(
    s: &mut S,
    store: &Arc<RedisStore>,
    pubsub: &Arc<PubSub>,
    conn: &mut ConnState,
    rx: &mut mpsc::UnboundedReceiver<PubMessage>,
    password: Option<String>,
) -> std::io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut buf: Vec<u8> = Vec::new();
    let mut tmp = [0u8; 8192];
    loop {
        // Drain and execute every complete command already in the buffer.
        loop {
            let (args, consumed) = match try_parse_command(&buf) {
                Ok(Some(v)) => v,
                Ok(None) => break,
                Err(e) => {
                    let mut out = Vec::new();
                    Resp::Error(e).encode(conn.proto, &mut out);
                    s.write_all(&out).await?;
                    return Ok(());
                }
            };
            buf.drain(..consumed);
            if args.is_empty() {
                continue;
            }
            let replies = dispatch(store, pubsub, &args, conn, password.as_deref());
            let mut out = Vec::new();
            for reply in &replies {
                reply.encode(conn.proto, &mut out);
            }
            s.write_all(&out).await?;
            if conn.quit {
                let _ = s.shutdown().await;
                return Ok(());
            }
        }
        // Nothing more to parse: wait for either new bytes or a published message.
        tokio::select! {
            read = s.read(&mut tmp) => {
                let n = read?;
                if n == 0 {
                    return Ok(()); // client closed
                }
                buf.extend_from_slice(&tmp[..n]);
                if buf.len() > 512 * 1024 * 1024 {
                    return Ok(()); // runaway request guard
                }
            }
            msg = rx.recv() => {
                match msg {
                    Some(m) => {
                        let mut out = Vec::new();
                        m.to_resp().encode(conn.proto, &mut out);
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
/// is set (CONCEPT:EG-174). The Redis keyspace is durable when a persist dir is
/// configured on [`ServerState`], else in-memory scratch.
pub async fn serve(addr: &str, state: Arc<RwLock<ServerState>>) -> std::io::Result<()> {
    let persist_dir = { state.read().await.persist_dir.clone() };
    let store = Arc::new(RedisStore::open(persist_dir.as_deref()).map_err(std::io::Error::other)?);
    let password = std::env::var(REDIS_PASSWORD_ENV)
        .ok()
        .filter(|s| !s.is_empty());
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
    // One pub/sub registry per listener, shared across every accepted connection
    // (CONCEPT:EG-307).
    let pubsub = Arc::new(PubSub::default());
    tracing::info!(
        "redis-wire: serving Redis RESP protocol on {} (durable={}, auth={})",
        addr,
        store.is_durable(),
        password.is_some()
    );
    loop {
        let (mut socket, peer) = listener.accept().await?;
        let store = store.clone();
        let pubsub = pubsub.clone();
        let password = password.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_connection(&mut socket, store, pubsub, password).await {
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
        ConnState::new(3, true)
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
            execute(&store, &a(&["SET", "k", "v"]), &mut c, None),
            Resp::Simple("OK".into())
        );
        assert_eq!(
            execute(&store, &a(&["GET", "k"]), &mut c, None),
            Resp::Bulk(Some(b"v".to_vec()))
        );
        assert_eq!(
            execute(&store, &a(&["TYPE", "k"]), &mut c, None),
            Resp::Simple("string".into())
        );
        assert_eq!(
            execute(&store, &a(&["EXISTS", "k", "nope"]), &mut c, None),
            Resp::Int(1)
        );
        // NX on an existing key → null.
        assert_eq!(
            execute(&store, &a(&["SET", "k", "v2", "NX"]), &mut c, None),
            Resp::Null
        );
        // INCR path.
        assert_eq!(
            execute(&store, &a(&["SET", "n", "10"]), &mut c, None),
            Resp::Simple("OK".into())
        );
        assert_eq!(
            execute(&store, &a(&["INCR", "n"]), &mut c, None),
            Resp::Int(11)
        );
        assert_eq!(
            execute(&store, &a(&["DECR", "n"]), &mut c, None),
            Resp::Int(10)
        );
        // EXPIRE/TTL.
        assert_eq!(
            execute(&store, &a(&["EXPIRE", "k", "100"]), &mut c, None),
            Resp::Int(1)
        );
        assert!(
            matches!(execute(&store, &a(&["TTL", "k"]), &mut c, None), Resp::Int(t) if t > 0 && t <= 100)
        );
        // DEL.
        assert_eq!(
            execute(&store, &a(&["DEL", "k", "n"]), &mut c, None),
            Resp::Int(2)
        );
        assert_eq!(
            execute(&store, &a(&["GET", "k"]), &mut c, None),
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
                None
            ),
            Resp::Int(2)
        );
        assert_eq!(
            execute(&store, &a(&["HGET", "h", "f1"]), &mut c, None),
            Resp::Bulk(Some(b"v1".to_vec()))
        );
        assert_eq!(
            execute(&store, &a(&["HGETALL", "h"]), &mut c, None),
            Resp::Map(vec![
                (Resp::bulk_str("f1"), Resp::bulk_str("v1")),
                (Resp::bulk_str("f2"), Resp::bulk_str("v2")),
            ])
        );
        // LPUSH / RPUSH / LRANGE / LLEN. LPUSH a b c → head order c b a.
        assert_eq!(
            execute(&store, &a(&["RPUSH", "l", "a", "b"]), &mut c, None),
            Resp::Int(2)
        );
        assert_eq!(
            execute(&store, &a(&["LPUSH", "l", "z"]), &mut c, None),
            Resp::Int(3)
        );
        assert_eq!(
            execute(&store, &a(&["LLEN", "l"]), &mut c, None),
            Resp::Int(3)
        );
        assert_eq!(
            execute(&store, &a(&["LRANGE", "l", "0", "-1"]), &mut c, None),
            Resp::Array(Some(vec![
                Resp::bulk_str("z"),
                Resp::bulk_str("a"),
                Resp::bulk_str("b")
            ]))
        );
        // SADD / SMEMBERS / SREM (dedup).
        assert_eq!(
            execute(&store, &a(&["SADD", "s", "x", "y", "x"]), &mut c, None),
            Resp::Int(2)
        );
        assert_eq!(
            execute(&store, &a(&["SREM", "s", "x"]), &mut c, None),
            Resp::Int(1)
        );
        assert_eq!(
            execute(&store, &a(&["SMEMBERS", "s"]), &mut c, None),
            Resp::Set(vec![Resp::bulk_str("y")])
        );
        // ZADD / ZRANGE / ZSCORE (sorted by score).
        assert_eq!(
            execute(&store, &a(&["ZADD", "z", "2", "b", "1", "a"]), &mut c, None),
            Resp::Int(2)
        );
        assert_eq!(
            execute(&store, &a(&["ZRANGE", "z", "0", "-1"]), &mut c, None),
            Resp::Array(Some(vec![Resp::bulk_str("a"), Resp::bulk_str("b")]))
        );
        assert_eq!(
            execute(&store, &a(&["ZSCORE", "z", "b"]), &mut c, None),
            Resp::bulk_str("2")
        );
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
        let mut c = ConnState::new(2, false);
        // Data command before AUTH → NOAUTH.
        match execute(&store, &a(&["GET", "k"]), &mut c, Some("secret")) {
            Resp::Error(e) => assert!(e.starts_with("NOAUTH"), "{e}"),
            other => panic!("expected NOAUTH, got {other:?}"),
        }
        // Wrong password rejected.
        assert!(matches!(
            execute(&store, &a(&["AUTH", "nope"]), &mut c, Some("secret")),
            Resp::Error(_)
        ));
        // Correct password accepted → subsequent command allowed.
        assert_eq!(
            execute(&store, &a(&["AUTH", "secret"]), &mut c, Some("secret")),
            Resp::Simple("OK".into())
        );
        assert_eq!(
            execute(&store, &a(&["GET", "k"]), &mut c, Some("secret")),
            Resp::Bulk(None)
        );
        // HELLO 3 upgrades the protocol.
        assert!(matches!(
            execute(&store, &a(&["HELLO", "3"]), &mut c, None),
            Resp::Map(_)
        ));
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
        s.write_all(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$5\r\nhello\r\n")
            .await
            .unwrap();
        assert_eq!(read_reply(&mut s).await, b"+OK\r\n");
        s.write_all(b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n")
            .await
            .unwrap();
        assert_eq!(read_reply(&mut s).await, b"$5\r\nhello\r\n");
    }

    // ── pub/sub + transactions (CONCEPT:EG-307) ──────────────────────────────────

    fn ps() -> PubSub {
        PubSub::default()
    }

    /// Read a reply, but fail (rather than hang forever) if none arrives.
    async fn read_reply_timeout(stream: &mut TcpStream) -> Vec<u8> {
        tokio::time::timeout(std::time::Duration::from_secs(3), read_reply(stream))
            .await
            .expect("timed out waiting for a reply")
    }

    /// Bind `serve_with_store` on an ephemeral port and return the address.
    async fn spawn_listener() -> String {
        let probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = probe.local_addr().unwrap().to_string();
        drop(probe);
        let serve_addr = addr.clone();
        tokio::spawn(async move {
            let _ = serve_with_store(&serve_addr, mem_store(), None).await;
        });
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        addr
    }

    #[test]
    fn eg307_publish_with_no_subscribers_returns_zero() {
        let store = mem_store();
        let pubsub = ps();
        let mut c = conn3();
        assert_eq!(
            dispatch(&store, &pubsub, &a(&["PUBLISH", "ch", "hi"]), &mut c, None),
            vec![Resp::Int(0)]
        );
    }

    #[test]
    fn eg307_subscribe_confirm_and_count() {
        let store = mem_store();
        let pubsub = ps();
        let mut c = conn3();
        c.id = pubsub.register(mpsc::unbounded_channel().0);
        let replies = dispatch(
            &store,
            &pubsub,
            &a(&["SUBSCRIBE", "a", "b"]),
            &mut c,
            None,
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
        let un = dispatch(&store, &pubsub, &a(&["UNSUBSCRIBE"]), &mut c, None);
        assert_eq!(un.len(), 2);
        assert!(c.sub_channels.is_empty());
    }

    #[tokio::test]
    async fn eg307_publish_subscribe_delivery_over_tcp() {
        let addr = spawn_listener().await;
        // Subscriber connection.
        let mut sub = TcpStream::connect(&addr).await.unwrap();
        sub.write_all(b"*2\r\n$9\r\nSUBSCRIBE\r\n$4\r\nnews\r\n")
            .await
            .unwrap();
        // The subscribe confirmation proves the registration landed.
        let confirm = read_reply_timeout(&mut sub).await;
        let confirm = String::from_utf8_lossy(&confirm);
        assert!(confirm.contains("subscribe"), "{confirm}");

        // Publisher connection.
        let mut pubc = TcpStream::connect(&addr).await.unwrap();
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
        // PSUBSCRIBE news.* — a glob pattern.
        sub.write_all(b"*2\r\n$10\r\nPSUBSCRIBE\r\n$6\r\nnews.*\r\n")
            .await
            .unwrap();
        let confirm = read_reply_timeout(&mut sub).await;
        assert!(String::from_utf8_lossy(&confirm).contains("psubscribe"));

        let mut pubc = TcpStream::connect(&addr).await.unwrap();
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
            dispatch(&store, &pubsub, &a(&["MULTI"]), &mut c, None),
            vec![Resp::Simple("OK".into())]
        );
        // Commands queue rather than execute.
        assert_eq!(
            dispatch(&store, &pubsub, &a(&["SET", "k", "1"]), &mut c, None),
            vec![Resp::Simple("QUEUED".into())]
        );
        assert_eq!(
            dispatch(&store, &pubsub, &a(&["INCR", "k"]), &mut c, None),
            vec![Resp::Simple("QUEUED".into())]
        );
        assert_eq!(
            dispatch(&store, &pubsub, &a(&["GET", "k"]), &mut c, None),
            vec![Resp::Simple("QUEUED".into())]
        );
        // Nothing ran yet.
        assert!(c.in_multi);
        // EXEC runs them back-to-back, replies as one array.
        let out = dispatch(&store, &pubsub, &a(&["EXEC"]), &mut c, None);
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
            dispatch(&store, &pubsub, &a(&["GET", "k"]), &mut c, None),
            vec![Resp::Bulk(Some(b"2".to_vec()))]
        );
    }

    #[test]
    fn eg307_discard_clears_queue() {
        let store = mem_store();
        let pubsub = ps();
        let mut c = conn3();
        dispatch(&store, &pubsub, &a(&["MULTI"]), &mut c, None);
        dispatch(&store, &pubsub, &a(&["SET", "k", "99"]), &mut c, None);
        assert_eq!(
            dispatch(&store, &pubsub, &a(&["DISCARD"]), &mut c, None),
            vec![Resp::Simple("OK".into())]
        );
        assert!(!c.in_multi);
        // The queued SET never ran.
        assert_eq!(
            dispatch(&store, &pubsub, &a(&["GET", "k"]), &mut c, None),
            vec![Resp::Bulk(None)]
        );
        // EXEC / DISCARD outside a transaction are errors.
        assert!(matches!(
            dispatch(&store, &pubsub, &a(&["EXEC"]), &mut c, None).as_slice(),
            [Resp::Error(_)]
        ));
    }

    #[test]
    fn eg307_multi_aborts_on_bad_command() {
        let store = mem_store();
        let pubsub = ps();
        let mut c = conn3();
        dispatch(&store, &pubsub, &a(&["MULTI"]), &mut c, None);
        // An unknown command taints the transaction.
        assert!(matches!(
            dispatch(&store, &pubsub, &a(&["BOGUS", "x"]), &mut c, None).as_slice(),
            [Resp::Error(_)]
        ));
        // EXEC then aborts with EXECABORT.
        match dispatch(&store, &pubsub, &a(&["EXEC"]), &mut c, None).as_slice() {
            [Resp::Error(e)] => assert!(e.starts_with("EXECABORT"), "{e}"),
            other => panic!("expected EXECABORT, got {other:?}"),
        }
    }
}
