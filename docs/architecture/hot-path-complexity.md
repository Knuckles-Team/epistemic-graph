# Hot-path complexity ledger

This ledger records algorithmic bounds, not benchmark claims. Latency and memory
numbers still require representative production traces; no speedup multiplier is
claimed from source inspection alone.

Rows named **selection/order** bound only that component, not the complete query.
Their notes name unchanged scoring, decoding, I/O or traversal work. `expected`
marks hash-table or partial-selection bounds; ordered-map bounds are deterministic.

## Implemented row identities

These identifiers are the stable machine-readable identity of every row in the
implemented-bounds table. The G-37 scenario manifest must cover each identifier
exactly once. Renaming, adding, removing, or reordering an implemented row without
updating this registry and the scenario contract makes certification fail closed.

| Row ID | Implemented path |
|---|---|
| `G37-HP-001` | Served modality streaming ingest |
| `G37-HP-002` | Served modality page after an occurrence cursor |
| `G37-HP-003` | Served modality unfiltered CDC after a scalar sequence |
| `G37-HP-004` | Served modality snapshot idempotency validation |
| `G37-HP-005` | Served modality active length |
| `G37-HP-006` | Analytics-job next id on normal restart |
| `G37-HP-007` | Analytics-job repeated worker claim |
| `G37-HP-008` | Truth-maintenance generator retirement |
| `G37-HP-009` | Canonical `GENERATED_BY` reconciliation |
| `G37-HP-010` | Analytics-job selection |
| `G37-HP-011` | Tenant active quota lookup |
| `G37-HP-012` | Result-cache LRU eviction |
| `G37-HP-013` | Result-cache hit mutex hold |
| `G37-HP-014` | PromQL instant predecessor |
| `G37-HP-015` | Exact edge cardinality |
| `G37-HP-016` | Single-node logical delete |
| `G37-HP-017` | Resident bulk eviction |
| `G37-HP-018` | Endpoint-pair removal with `P` parallel edges |
| `G37-HP-019` | Induced subgraph extraction |
| `G37-HP-020` | Composite property posting intersection over `Q_p` predicates |
| `G37-HP-021` | Unlabeled keyset page of `P` rows |
| `G37-HP-022` | Lazy node-derived cache invalidation |
| `G37-HP-023` | Flat-vector id lookup/delete/repeated exact rerank |
| `G37-HP-024` | TSDB late/out-of-order bucket batch |
| `G37-HP-025` | TSDB bounded range over `H` stored buckets and `H_c` covering chunks |
| `G37-HP-026` | TSDB time-bucket aggregation |
| `G37-HP-027` | Multi-rate sensor fusion over `S` sorted streams and `T` samples |
| `G37-HP-028` | Flat exact vector top-k / exact rerank, including distance scoring |
| `G37-HP-029` | IVF-PQ coarse-cell / ADC / SQ8 **selection/order only** |
| `G37-HP-030` | Native HNSW neighbor/result **selection/order only** |
| `G37-HP-031` | HNSW small-set exact path, including cosine scoring |
| `G37-HP-032` | LeanRAG bounded child/leaf ranking **selection/order only** |
| `G37-HP-033` | Redb paged lazy recovery over page size `B` |
| `G37-HP-034` | Complex state-backed `MutationBatch` commit |
| `G37-HP-035` | QoS admission of `A` winners from `N` pending requests |
| `G37-HP-036` | Analytics-job pool/region placement scan |
| `G37-HP-037` | Redb edge-ordinal cold seed / cache invalidation |
| `G37-HP-038` | Trace batch ingest and newest-`L_t` search |
| `G37-HP-039` | SQL `INSERT ... ON CONFLICT` batch over `N` existing rows, `B` inputs and `U_c` unique columns |
| `G37-HP-040` | User-table schema column lookup over width `W` |
| `G37-HP-041` | Change notification callback fan-out |
| `G37-HP-042` | Validated knowledge-batch score projection / schema validation |
| `G37-HP-043` | AMQP/MQTT topic wildcard match |
| `G37-HP-044` | Broker route queue de-duplication over `B_r` bindings |
| `G37-HP-045` | Bounded append-log read of `L_r` rows from `N_r` candidates |
| `G37-HP-046` | Append-log count/age retention over `N_r` rows and `D_r` removals |
| `G37-HP-047` | Redis-wire batched hash/set/zset membership mutation |
| `G37-HP-048` | Redis-wire `LPUSH` of `B_v` values before a list of `N_v` rows |
| `G37-HP-049` | gSpan extension over a `K_m`-node embedding, `E_m` pattern edges and `F_m` incident host edges |
| `G37-HP-050` | Exact GDS all-pairs neighbor preparation over `V_s` nodes / `E_sg` adjacency rows |
| `G37-HP-051` | Exact per-node GDS similarity **selection/order only** from `V_s` scored neighbors |
| `G37-HP-052` | Confidence-weighted semantic result prefix of `N_w` candidates / limit `k_w` |
| `G37-HP-053` | Post-filter observability result **selection/order only** over `N_o` records / limit `k_o` |
| `G37-HP-054` | Parsed-symbol call-site retention over `C_s` discovered calls |

## Implemented bounds

| Path | Before | After | Notes |
|---|---:|---:|---|
| Served modality streaming ingest | `O(S + B log N)` time and `O(S)` extra state, where `S` is the complete runtime and `B` is the batch | `O(Y_B + (B + F) log N)` time and `O(B + F + Y_U)` touched-state memory | A reverse undo journal records only prior touched rows, idempotency entries, event tails and derived counts. `Y_B` is the aggregate input-validation, fingerprinting and payload-copy work; `Y_U` is the prior payload bytes retained for rollback. `F` is the touched bundles' modality/segment index fan-out. Rollback restores the exact pre-batch snapshot. |
| Served modality page after an occurrence cursor | `O(N)` prefix scan before page filtering | `O(log N + P log N + Y_P)` | A native ordered range seeks directly past the cursor, but every examined index id still performs an ordered `records.get`. `P` includes rows examined for policy/lifecycle filtering and `Y_P` is returned payload-copy work; per-record bundle/policy inspection is additional when bundle size is not bounded. |
| Served modality unfiltered CDC after a scalar sequence | `O(N + L)` | `O(1 + L)` | Event sequences are contiguous and one-based, so the sequence is the vector slice offset. Policy-filtered replay is instead `O(X)` for the `X` suffix events examined to find up to `L` authorized rows; in the worst case `X` can be the complete remaining suffix. |
| Served modality snapshot idempotency validation | `O(E * I)` | `O(E + I)` | Contiguous event validation is linear and each idempotency outcome addresses its event directly by sequence. |
| Served modality active length | `O(N)` | `O(1)` after construction or official recovery | The count is derived and intentionally excluded from authoritative snapshots. Direct deserialization computes an exact `O(N)` count until its first ingest/delete materializes the cache. |
| Analytics-job next id on normal restart | `O(N)` job-key scan | `O(1)` fixed metadata lookup | Existing stores pay one `O(N log N)` transactional index/counter backfill. The schema marker is published atomically with the completed backfill. |
| Analytics-job repeated worker claim | `O(N)` decode/search | `O(log N)` | A durable `worker_ref -> job_id` lease index returns the still-live fenced claim. Worker references are already server-pseudonymized. |
| Truth-maintenance generator retirement | `O(N)` materialization scan plus dependency closure | `O(log G + M log N + U_cl)` | Both the pure `TruthMaintenance` algorithm and durable `IncrementalReasoningIndex` maintain `generator -> materialization ids` reverse indexes. The durable index is part of the versioned projection snapshot, is checked against the forward map on recovery, and is updated through the same mutation helpers before atomic snapshot publication. `M` is only the retired generator's outputs; `U_cl` is dependency-closure traversal plus its ordered-index updates. |
| Canonical `GENERATED_BY` reconciliation | `O(P_r)` complete provenance-edge scan | `O(log P_r + P_m)` | The ordered `(materialization,target)` map range-seeks to one source prefix and stops at the next source. Canonical selection remains the first opaque `GENERATED_BY` target, independent of mutation arrival order. `P_m` is only that materialization's provenance rows. |
| Analytics-job selection | `O(N log N)` after decoding every durable job | `O(E log N + C_w log C_w + sum_a(log N + S_a(log N + D + log T)))` | Every ready job has exactly one deterministic placement anchor. A worker range-seeks only the unconstrained anchor plus its `C_w` hashed capability/pool/region tokens and merges the first eligible row per range. Jobs behind anchors the worker cannot satisfy are never decoded. `E` is due reconciliation, `S_a` is the exact placement/quota-skipped prefix for one visible anchor, and `D` is candidate decode. The exact-placement worst case remains linear only within worker-visible anchors. |
| Tenant active quota lookup | `O(N)` job scan | `O(log T)` per candidate tenant | Transactional `(active_count, reserved_cpu)` counters are keyed by a one-way tenant digest. Lease acquire/renew/release/expiry and the counter update share the authoritative job transaction. |
| Result-cache LRU eviction | `O(C)` minimum-tick scan | `O(log C)` | A bounded `BTreeMap<tick, key>` mirrors the hash map. Hits and replacement also update recency in `O(log C)`; capacity and RLS actor scoping are unchanged. |
| Result-cache hit mutex hold | `O(log C + payload bytes)` | `O(log C)` | Entries retain an `Arc<Vec<u8>>`; the required owned response copy occurs after recency bookkeeping releases the mutex. Cache bounds and returned bytes are unchanged. |
| PromQL instant predecessor | worst-case `O(N)` reverse scan | `O(1)` conforming tail, `O(log N)` defensive predecessor | `SeriesSource` promises a sorted, upper-bounded slice, making its tail the fast path. A wider ordered cache slice uses `partition_point`. |
| Exact edge cardinality | `O(E)` property-row walk | `O(1)` | `StableGraph` maintains the authoritative topology count in the same `GraphTxn` as edge properties. Parallel-edge behavior is covered by unit tests. |
| Single-node logical delete | `O(E)` endpoint-map retain | expected `O(deg(v))` | Incident endpoint pairs are derived from incoming/outgoing adjacency; unrelated edge partitions are never visited. Parallel edges and self-loops are collapsed before property removal. |
| Resident bulk eviction | `O(E + K)` after the prior bulk fix | expected `O(K + sum(deg(v)))` for the selected nodes | The resident projection now removes only incident endpoint partitions. It still emits no logical-delete ledger records and releases semantic rows under one lock. |
| Endpoint-pair removal with `P` parallel edges | worst-case `O(P * deg(src))` repeated adjacency search | `O(deg(src) + P)` | One adjacency walk collects the stable edge indexes, then removes them without rescanning. |
| Induced subgraph extraction | `O(K + E)` | expected `O(K + sum(outdeg(selected)) + E_i)` | Only selected nodes' outgoing adjacency is visited; endpoint pairs are deduplicated so `E_i` parallel property rows are copied exactly once. |
| Composite property posting intersection over `Q_p` predicates | expected `O(Q_p log Q_p + sum postings)` time and `O(sum cloned postings + max posting)` memory | deterministic `O(Q_p log Q_p + sum postings)` time and `O(sum cloned postings)` memory | The posting vectors are first ordered by length in `O(Q_p log Q_p)`. Sorted/deduplicated postings then use a two-pointer in-place intersection, eliminating each additional hash set; the existing API-owned posting clones are unchanged. |
| Unlabeled keyset page of `P` rows | `O(N log N + P + Y_P)` per page | cold `O(N log N + P + Y_P)`, warm expected `O(log N + P + Y_P)` | A lazy sorted node-id directory is built from topology under its read guard and invalidated with committed node-derived caches. It stores identifiers only; `Y_P` is the property payload copied for the requested page. |
| Lazy node-derived cache invalidation | Every write discards label, property and JSON-path caches, causing up to three later `O(N)` rebuilds | Pure edge/embedding writes retain all three in `O(1)`; exact field updates inspect `O(F_c + J)` metadata and discard only covering caches | Adds, removes and unknown/full-image updates remain conservative. `F_c` is the exact changed-field set and `J` the number of warm JSON paths inspected. Heavy server indexes still receive the same complete `ChangeSet`. |
| Flat-vector id lookup/delete/repeated exact rerank | `O(N)` point lookup/delete and `O(N + C*d + C log C)` per rerank | cold `O(N)`, then expected `O(1 + D_i)` point lookup/delete and `O(C*d + D_C + k log k)` rerank | A serde-skipped lazy `id -> row offsets` directory preserves duplicate-id and tombstone semantics. Warm appends maintain it incrementally. `d` is embedding dimension, `D_i` is one id's stored duplicate rows and `D_C` is the total duplicate-row offsets examined for the distinct rerank candidates. |
| TSDB late/out-of-order bucket batch | `O(B * N * F)` suffix shifts | `O(B log B + (N+B) * F)` | Incoming rows are stably sorted only when needed, then merged with the ordered chunk. Existing rows still precede newly arrived equal-timestamp rows. |
| TSDB bounded range over `H` stored buckets and `H_c` covering chunks | up to `O(log H + sum(points in covering and future chunks)*F)` | `O(log H + H_c + sum_j(N_j*F) + R*F)` | The durable range now ends at the caller's exclusive upper timestamp, so future chunks are not fetched. Every covering packed chunk must still be fully decoded (`N_j` points in chunk `j`) before `partition_point` selects its exact slice; the seek removes per-point boundary filtering but not packed decode cost. `R` is returned rows. |
| TSDB time-bucket aggregation | `O(N)` time and `O(max bucket)` scratch | `O(N)` time and `O(1)` scratch | First/last/min/max/mean/sum/count are accumulated while locating each bucket boundary; no per-bucket value vector is materialized. |
| Multi-rate sensor fusion over `S` sorted streams and `T` samples | `O(T log T + S*Q)` time and `O(T + S*Q)` extra memory | `O(T log S + S*Q)` time and `O(S + T)` extra memory beyond the result | A heap k-way merges the union clock, then monotonic per-stream ASOF cursors write final fused rows directly. The prior index-point copies and transposed `S*Q` channel matrix are gone. |
| Flat exact vector top-k / exact rerank, including distance scoring | `O(N*d + N log N)` | expected `O(N*d + N + k log k)` | Linear partial selection retains `k`; only that prefix is sorted under the total `(finite distance, id, NaN-last)` order. `N` is the live corpus for exact search or the candidate population for rerank. Full ordering remains `O(N log N)` when the caller asks for every row. |
| IVF-PQ coarse-cell / ADC / SQ8 **selection/order only** | ADC-only `O(nlist log nlist + C_a log C_a)`; refined expected `O(nlist log nlist + C_a + R log R)` | ADC-only expected `O(nlist + p log p + C_a + k log k)`; refined expected `O(nlist + p log p + C_a + R + k log k)` | Total-order partial selection keeps and orders only `p = nprobe` cells, retains `R = refine_factor*k` ADC candidates, and orders the final `k` SQ8 hits. This is not an end-to-end search bound: unchanged rotation, coarse scoring, ADC-table/candidate scoring and SQ8 refinement add `O(d^2 + nlist*d + p*d + C_a*m + R*d)`. |
| Native HNSW neighbor/result **selection/order only** | `O(E_f log E_f)` per selection | expected `O(E_f + k_h log k_h)` | Build-time neighbor selection/pruning and query-time result ordering partial-select their exact internal `(distance,id,node)` prefix. This is not an end-to-end HNSW bound: heap-bounded beam traversal, visited-set work and `d`-dimensional distance evaluations are unchanged. `E_f` is the beam or candidate fan-out and `k_h` the retained degree/result count. |
| HNSW small-set exact path, including cosine scoring | `O(N_s*d + N_s log N_s)` | expected `O(N_s*d + N_s + k log k)` | The below-threshold cosine path partial-selects the exact score-desc/id-asc prefix and sorts only returned hits. The complete deterministic embedding snapshot still sorts all rows by id because its API returns the full image. |
| LeanRAG bounded child/leaf ranking **selection/order only** | `O(sum(F_i log F_i) + L log L)` | expected `O(sum(F_i + b log b) + L + l log l)` | Every provenance parent partial-selects only drill breadth `b`; the de-duplicated leaf set partial-selects context budget `l`. This bound excludes unchanged graph-child cloning/access, embedding lookup and `d`-dimensional scoring work. Score order remains total `(finite score desc, id asc, NaN last)`. |
| Redb paged lazy recovery over page size `B` | `O(N^2 / B)` rows examined | `O((N/B) log N + N)` | The internal cursor carries the last composite node/edge key (including parallel-edge ordinal), so every redb page range-seeks after the prior key. Portable offsets remain progress counters and the protocol never exposes durable row keys. |
| Complex state-backed `MutationBatch` commit | Isolated execution plus full `O(N+E+S*d)` image serialization, all-row durable replacement and full resident install | expected `O(N+E+S*d + S log S + D log D)` delta discovery, `O(D log(N+E+S))` durable row updates and `O(D log D + U_D)` resident publication | Isolation/OCC still prove the complete result before durability. Deterministic embedding snapshots still sort all `S` ids, so discovery is not delta-only. The gain is that only the authenticated affected-row delta is serialized and persisted. `U_D` includes operation-specific adjacency/parallel-edge removal, payload-copy and ANN-index maintenance; it is not generally `O(D)`. Exceptional projection recovery still installs the full snapshot. |
| QoS admission of `A` winners from `N` pending requests | `O(N log N)` | expected `O(N + A log A)` | A total priority/deadline/arrival comparator selects the winning prefix, then sorts only admitted rows. Zero available slots return without ordering work. |
| Analytics-job pool/region placement scan | `O(C)` time and up to `O(C)` transient formatted strings | `O(C)` time and `O(1)` scratch | Capability prefixes are compared with `strip_prefix`; no candidate-loop formatting/allocation remains. |
| Redb edge-ordinal cold seed / cache invalidation | `O(P)` parallel-row scan; `O(K)` retain for source/graph invalidation | `O(log E)` ordered tail seek; expected `O(1)` pair/source/graph invalidation | The writer cache is hierarchical `graph -> source -> target`, and redb's composite-key range seeks its last ordinal directly. Ordinal exhaustion fails closed instead of wrapping. |
| Trace batch ingest and newest-`L_t` search | `B` ingest and up to `M_t` search mutex acquisitions; `O(M_t log M_t)` final match sort plus duplicate filtered-trace clone | one mutex acquisition per batch/search; ingest `O(B(log U_s + log U_o))`; posting intersection `O(A_t + O_t)`; final selection expected `O(M_t + L_t log L_t)` | The mutex reduction does not make ingestion `O(B)` independent of store size: service/operation `BTreeSet` inserts retain their logarithmic cost. Search also clones, filters and assembles `Z_t` candidate spans, including per-trace service/child ordering; those costs are outside the final-prefix selection bound. A total `(start desc, trace id)` comparator makes equal-time results deterministic. |
| SQL `INSERT ... ON CONFLICT` batch over `N` existing rows, `B` inputs and `U_c` unique columns | `O(B * U_c * N)` conflict scans plus `O(B * W^2)` supplied-column checks | expected `O(N * U_c + B * (U_c + W))` | Transaction-local unique-value and row-id directories mirror staged inserts/updates; a supplied-column bitmap removes repeated target scans. The final authoritative uniqueness pass and redb transaction rollback remain unchanged. |
| User-table schema column lookup over width `W` | `O(W)` per name lookup | cold `O(W)`, then expected `O(1)` | A serde-skipped `OnceLock` name-to-offset directory is derived from the canonical ordered columns. Duplicate, empty and NUL-bearing names fail validation; controlled column mutation invalidates the directory before changes. |
| Change notification callback fan-out | callbacks execute while holding the subscriber mutex | `O(S)` upgrade/prune under lock, callbacks out of lock | Reentrant subscription is safe and slow sinks no longer block subscriber-list maintenance. The no-subscriber path remains one atomic load. |
| Validated knowledge-batch score projection / schema validation | `O(R_b * S_c * E_s)` projection and `O(S_c^2)` duplicate validation | expected `O(S_c + R_b * (S_c + E_s))` projection and `O(S_c)` validation | A schema-sized name directory transposes every row once; native stream score names use a single hash set instead of repeated prefix scans. The projection bound assumes the public stream validator has first rejected duplicate schema names. Direct unvalidated `to_record_batch` calls preserve duplicate-column behavior and add work proportional to duplicate-name fan-out. |
| AMQP/MQTT topic wildcard match | exponential backtracking and caller-controlled recursion for ambiguous `#` chains | `O(P_t * K_t)` worst-case time and `O(P_t)` memory | An iterative NFA frontier de-duplicates every reachable pattern state for each key word. Common exact/`*` patterns retain a narrow frontier; adversarial `#.#...literal` near misses cannot enumerate partitions or exhaust the stack. |
| Broker route queue de-duplication over `B_r` bindings | `O(B_r * Q_r)` queue-name comparisons | expected `O(B_r)` membership work | A transient borrowed queue-name set preserves first-binding order without cloning names into the directory. Topic-pattern matching cost remains separate. |
| Bounded append-log read of `L_r` rows from `N_r` candidates | `O(N_r log N_r + Y_r)` | warm-index expected `O(N_r + L_r log L_r + Y_r)` | Offset partial selection discards the unordered tail and sorts only the returned prefix. `Y_r` includes property decode and payload hex decode/copy for all `N_r` candidates. An unbounded read still orders the complete result. A cold shared label index additionally scans/decodes the graph's `N_g` nodes and sorts its label postings. |
| Append-log count/age retention over `N_r` rows and `D_r` removals | `O(N_r log N_r + N_r * D_r + Y_r)` plus deletion work | warm-index expected `O(N_r + D_r log D_r + Y_r + sum(deg(v)))` | Count retention partial-selects the oldest overflow; count and age candidates union in a hash set; only the deterministic deletion-id list is sorted. Removing the selected message nodes still pays their general graph deletion cost (normally zero-degree stream nodes). A cold shared label index additionally pays the graph-wide build described above. |
| Redis-wire batched hash/set/zset membership mutation | `O(B_v * N_v)` membership scans, plus the required zset order | expected `O(N_v + B_v)`, plus the required `O((N_v+B_v) log(N_v+B_v))` zset order | Transient field/member directories preserve the current serialized insertion order and last-update-wins behavior. Single-entry serialization remains linear in the stored aggregate by design. |
| Redis-wire `LPUSH` of `B_v` values before a list of `N_v` rows | `O(B_v * N_v + B_v^2)` tail shifts | `O(N_v + B_v)` | The reversed command prefix is built once and the prior list tail is moved once, preserving Redis multi-value head order. |
| gSpan extension over a `K_m`-node embedding, `E_m` pattern edges and `F_m` incident host edges | whole extension `O(F_m * (K_m + E_m))` | lookup/membership work expected `O(K_m + E_m + F_m)`; whole extension remains `O(K_m + E_m + F_m*(K_m + E_m))` | One reverse host-to-local directory and one borrowed pattern-edge set remove repeated membership scans. Every emitted candidate still clones its node-label and edge vectors, so candidate materialization preserves the whole-path asymptotic bound. Canonicalization and the 64-embedding discovery cap are unchanged. |
| Exact GDS all-pairs neighbor preparation over `V_s` nodes / `E_sg` adjacency rows | `O(V_s * E_sg)` copied/merged neighbor entries, `O(V_s^2)` transient allocations, and repeated cosine norms | directed `O(V_s + E_sg)`; undirected expected `O(V_s + E_sg + sum_v(d_v log d_v))` preparation/norm work, with `O(V_s)` row allocations | Directed queries borrow already-sorted adjacency. Each undirected row currently hash-merges and sorts its combined adjacency once. Every pair then uses allocation-free two-pointer intersection/dot scoring. Exact pair-comparison complexity remains `O(V_s^2*d_bar)` and public pointwise helpers retain their established behavior. |
| Exact per-node GDS similarity **selection/order only** from `V_s` scored neighbors | `O(V_s log V_s)` per node | expected `O(V_s + k_s log k_s)` per node | Similarity scoring remains exact `O(V_s^2*d_bar)` across the graph; global pair folding/final ordering is also unchanged. Partial selection only removes the unnecessary order of discarded per-node neighbors. |
| Confidence-weighted semantic result prefix of `N_w` candidates / limit `k_w` | `O(Y_w + N_w log N_w)` | expected `O(Y_w + N_w + k_w log k_w)` | `Y_w` is property decode/inspection work across candidates. Stale filtering/decay remains a single pass; only the selected prefix is ordered. Original input ordinal preserves stable equal-score behavior, NaN sorts last, and a zero limit returns before property decoding. |
| Post-filter observability result **selection/order only** over `N_o` records / limit `k_o` | `O(N_o log N_o)` | expected `O(N_o + k_o log k_o)` | Manifest pruning, CAS/Parquet reads, full-text/structured filtering and record cloning are unchanged and excluded from this bound. Partial selection orders only the earliest returned prefix, with the original tier-union ordinal preserving stable equal-timestamp behavior. Unbounded/small results still take the direct stable-sort path. |
| Parsed-symbol call-site retention over `C_s` discovered calls | `O(C_s log C_s)` time and `O(C_s)` retained rows before truncation | `O(C_s log 64)` time and `O(64)` retained rows | A bounded ordered set maintains the exact lexicographically smallest 64 unique structured sites during traversal under the current serialized order and cap. |

`N` denotes rows/items in the relevant store, `E` graph edges (or events where
the row says so), `I` idempotency entries, `L` returned events, `P` parallel
edges/page candidates, `C` cache capacity, `T` active tenants, `K` selected
nodes, `B` points in one append batch, `F` fields per point, `N_b` points in one
time bucket, `E_i` induced parallel edge rows, `S` semantic rows, `D` affected
graph/ledger row operations, `C_w` worker placement tokens, `S_a` one visible anchor's skipped
prefix, `F_c` exact changed fields, `J` warm JSON paths, `W` schema columns,
`D_i` duplicate rows for one vector id, `D_C` duplicate offsets visited by a rerank,
`d` embedding dimension, `Q` union-clock ticks, `Q_p` posting predicates, `M_t`
matching traces, `L_t` returned traces and `U_c` unique SQL columns. Symbols such as `B`,
`C` and `S` take the path-specific meaning stated in their row. `R_b` is rows in
a knowledge batch, `S_c` its schema score columns, and `E_s` score entries per row.
`G` is the number of indexed generators and `M` the outputs for the selected generator.
`U_cl` is dependency-closure traversal and ordered-index update work.
`P_r` is the complete durable provenance-edge population and `P_m` the provenance
rows owned by one materialization.
`P_t` and `K_t` are topic-pattern and routing-key word counts; `B_r` is broker
bindings and `Q_r` distinct matched queues. `N_r`, `L_r` and `D_r` are scanned,
returned and deleted append-log rows. `N_v` is a stored Redis aggregate and `B_v`
is one command's field/member/value batch.
`K_m`, `E_m` and `F_m` are one mined pattern's mapped nodes, existing pattern
edges and incident host-edge visits during extension discovery.
`V_s` is the similarity graph's node count (and one node's scored population),
`E_sg` its total adjacency rows, `d_bar` its average neighbor-row width and `k_s`
its requested per-node neighbor count. `N_w` is the semantic candidate population,
`Y_w` its property decode/inspection work and `k_w` the returned result limit.
`N_o` is the post-filter observability record population and `k_o` its hit cap.
`C_s` is the number of call expressions visited within one parsed symbol.
For vector/hierarchical retrieval, `p` is IVF probe count, `C_a` is ADC-scored
postings, `R` the retained refine band, `N_s` the small-set exact population,
`F_i` one provenance parent's child fan-out, `b` drill breadth, `L` unique drilled
leaves and `l` the leaf budget.
`Y_B`, `Y_U`, `Y_P` and `Y_r` denote the byte-sensitive work identified in their
rows. For TSDB range reads, `N_j` is the decoded point count in covering chunk `j`.
For trace indexing/search, `U_s`/`U_o` are per-service/per-operation posting sizes,
`A_t`/`O_t` are the two postings scanned and `Z_t` is candidate span volume.
`U_D` is the sum of operation-specific resident publication costs for a graph delta.

## Serialized benchmark lane

The source changes above carry equivalence/unit proofs but intentionally make no
wall-clock multiplier claim. G-37 operationalizes the ledger through the strict
`protocols/performance/v1/scenarios.json` contract: 30 serialized scenario
families cover `G37-HP-001` through `G37-HP-054` exactly once, and the certifier
binds every raw row result to the exact binary, dataset, thresholds, manifest,
schema, ledger, authority, and hardware class. The lane benchmarks one binary at
a time on these bounded synthetic fixtures:

- sparse high-edge-count graphs: delete degree `0/2/32`, evict `1/64/4096`
  nodes, and extract `K=10/1_000` induced sets while varying total `E`;
- parallel endpoint pairs at `P=1/100/10_000` to verify one adjacency scan;
- flat vector search at `N=10k/1m`, `k=1/10/100`, including filtered rerank;
- IVF-PQ ADC-only and SQ8-refined search at `k=1/10/100` while sweeping `nprobe`,
  plus native HNSW `(k,ef)` sweeps so final-prefix selection regressions are visible;
- LeanRAG with fixed `b=8`, `l=16` while scaling per-summary fan-out from `64` to
  `4_096`, recording visited children as throughput rather than claiming wall time;
- repeated flat rerank at fixed candidate counts while scaling untouched `N`,
  verifying warm id-directory latency is independent of corpus size;
- unlabeled keyset pages at fixed `P=100` while scaling `N`, separating cold
  directory construction from warm page seeks;
- TS chunks with `N_b=1k/100k`, sorted and reverse-ordered append batches, plus
  narrow earlier windows with many later buckets;
- sensor fusion with fixed total samples while varying stream count and timestamp
  overlap, recording peak scratch memory as well as latency;
- trace batch ingest/search with fixed result `L_t` while scaling candidates, and
  SQL conflict batches while scaling existing rows and unique columns;
- Arrow knowledge-batch conversion while independently scaling `R_b`, `S_c` and
  sparse/dense `E_s`, including reordered, missing and duplicate row score names;
- lazy redb materialization across multiple page sizes, recording rows examined
  as well as latency so positional-cursor rescans cannot hide behind cache effects.
- canonical generator replacement at fixed `P_m` while scaling unrelated `P_r`,
  including mixed `DERIVED_FROM`/`GENERATED_BY` prefixes and reverse arrival order;
- topic routes with exact, `*`, trailing-`#` and adversarial alternating-`#`
  patterns while independently scaling pattern/key word counts; record reachable
  frontier width as well as latency;
- append-log bounded reads at fixed `L_r` while scaling `N_r`, overlapping
  count/age trims, and Redis aggregate batches at fixed `B_v` while scaling
  existing `N_v` (including multi-value `LPUSH`);
- gSpan extension with fixed embedding count while independently scaling mapped
  pattern size and incident host fan-out, recording candidates and canonical
  forms to prove semantic equivalence as well as latency;
- exact GDS similarity with fixed graph/metric while sweeping `k_s`, recording
  neighbor-preparation allocations, score evaluations and selected-prefix work;
- confidence-weighted semantic reranking at fixed `k_w` while scaling candidates,
  including equal-score, stale-row and non-finite-score fixtures;
- observability searches at fixed `k_o` across hot/cold tier unions while scaling
  post-filter records and equal-timestamp runs;
- source symbols with fixed AST shape while scaling duplicate/unique call sites,
  verifying the serialized lexicographic 64-site prefix;
- complex mutations at fixed `D=1/10/1_000` while scaling untouched
  `N+E+S=10k/1m`, recording serialized bytes and durable rows touched;
- scheduler claims with `C_w=0/8/64` and large ready populations behind both
  visible and unsatisfied placement anchors, recording candidate decodes;
- cold/warm column projections at `W=8/128/4_096`, including duplicate-name
  current ordering, and warm-cache behavior for edge-only versus field-scoped CAS.

## Durability and privacy invariants

The scheduler secondary rows and monotonic sequence counter are updated in the
same redb write transaction as `JOBS` and its MutationBatch status/fence/outbox.
No index is acknowledged before the authoritative row commits. Startup either
observes the complete schema marker or rebuilds all derived scheduler indexes;
decode failure aborts the migration before any partial index is published.
Tenant-accounting keys are SHA-256-derived rather than raw tenant identifiers.
Capability anchors are also one-way SHA-256 tokens, and exactly one anchor row is
maintained per ready job in the same transaction as its authoritative state.

Complex runtime mutations authenticate a versioned affected-row payload with the
digest stored in `MutationStateDescriptor`. The delta rows, graph version/fence,
idempotency receipt, status and outbox commit in one redb write transaction. The
canonical batch and audit retain only the opaque operation receipt, not a second
copy of sensitive node properties. Only the affected-row descriptor is accepted
during replay.

The modality journal changes only in-memory staging. The authoritative redb
transaction remains the commit boundary, and policy validation, legal holds,
idempotency fingerprints and event ordering are unchanged.

## Intentional exact-order paths

- Cross-shard merge heaps sort their already-bounded `k` outputs; removing that
  final order would violate the nearest-first API without improving the scan bound.
- `SemanticStore::embeddings_snapshot` returns a complete deterministic image, so
  its all-row id sort is required. Callers needing deltas use the mutation journal.
- `GraphTopology::children` is an established owned-`Vec` API. LeanRAG now avoids
  sorting the whole fan-out, but changing the clone/allocation bound needs a separate
  lending-iterator API rather than a lifetime-breaking replacement.

## Audited path requiring no structural change

- TSDB retention already uses `partition_point`, so its ordered boundary lookup is
  `O(log N)` and the required deletion work is proportional to the removed suffix.
  An additional recomputation cache would not improve the asymptotic bound and would
  add an invalidation authority; the audited implementation is the selected design.
