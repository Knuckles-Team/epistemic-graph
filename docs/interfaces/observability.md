# Observability interface

epistemic-graph is a full **observability backend** — an OpenObserve-class store for the logs + metrics +
traces trilogy — served from the same durable engine (feature `obs`, folded into node/full, out of the
lean `pi` tier). Logs land as time-series + full-text documents, metrics answer PromQL, spans answer trace
search, and a super-cluster `/federated` fan-out unifies many regions. It is cross-modal: an ingest
pipeline can enrich a record from the graph, and a query can join logs against graph/SQL data.

> Status snapshot: log ingestion (EG-160), Parquet-on-object-store segments (EG-161), log search/query API
> (EG-162), VRL-style ingest pipelines (EG-165), PromQL (EG-172), and distributed traces + trace search
> (EG-163) are shipped. See the [capability matrix](../capabilities.md).

All the HTTP surfaces below share **one** obs listener: `EPISTEMIC_GRAPH_OBS_ADDR` (`--obs-addr`, default
`127.0.0.1:5080`).

## Log ingestion (EG-160)

A hand-rolled HTTP ingestion listener (no axum/hyper — the Pi contract) accepting **OTLP/HTTP**
(`/v1/logs`), Elasticsearch-**`_bulk`**/`_doc`, syslog, and JSON-lines records. Records land as
schema-on-read streams: an eg-tsdb time-series **and** an eg-text full-text index, with rolling Parquet
columnar segments persisted into the blob CAS / S3 backend (EG-161) — the "140× cheaper storage"
object-store architecture, still DataFusion-queryable.

```bash
EPISTEMIC_GRAPH_OBS_ADDR=127.0.0.1:5080 \
  epistemic-graph-server --persist-dir /var/lib/eg   # --features "obs server"

curl -s -XPOST 'http://127.0.0.1:5080/v1/logs' \
  -H 'content-type: application/json' --data-binary @otlp-logs.json
```

`EPISTEMIC_GRAPH_OBS_FLUSH_RECORDS` tunes the segment-flush batch size.

## VRL-style ingest pipelines (EG-165)

An OpenObserve/Vector-style transform pipeline runs over incoming records **before** they land: a small
pure-Rust pipeline DSL (parse / json-extract, filter / drop, set / rename / remove fields, coerce types,
route-to-stream) compiled to a staged executor. It surpasses OpenObserve VRL by being cross-modal — a
transform can enrich a record from the graph.

## Log search & query API (EG-162)

SQL + full-text search over the ingested streams: a DataFusion scan over the Parquet segments (pruned by
the segment manifest's time/stream range) UNIONed with the hot eg-tsdb series + the eg-text BM25 index,
plus an OpenObserve/Elasticsearch-shaped `_search` endpoint.

```bash
curl -s -XPOST 'http://127.0.0.1:5080/api/<org>/<stream>/_search' \
  -H 'content-type: application/json' \
  --data '{"query":{"sql":"SELECT * FROM logs WHERE level = '\''error'\'' LIMIT 50"}}'
```

## Metrics — PromQL (EG-172)

A PromQL evaluator (instant/range vectors, selectors, `rate`/`sum`/`avg`/`max`/`min`/histogram functions,
binary ops) over the eg-tsdb metric series, exposed as a Prometheus-compatible HTTP API:

```bash
curl -s 'http://127.0.0.1:5080/api/v1/query?query=up'
curl -s 'http://127.0.0.1:5080/api/v1/query_range?query=rate(http_requests[5m])&start=…&end=…&step=15s'
curl -s 'http://127.0.0.1:5080/api/v1/labels'
```

Point a Grafana **Prometheus** data source at the obs listener. (Feature `promql`, implies `obs`.) See
also [time-series](timeseries.md#promql--prometheus-http-api-eg-172-feature-promql-on-the-obs-listener).

## Traces (EG-163)

OTLP / OTLP-JSON span ingest on `/v1/traces` into a span store (spans as nodes/rows keyed by
`trace_id`/`span_id` with parent links, attributes, timings), plus trace-assembly + search (by
service/operation/duration/tag, returning span trees) and service-dependency-graph derivation.

```bash
# ingest
curl -s -XPOST 'http://127.0.0.1:5080/v1/traces' \
  -H 'content-type: application/json' --data-binary @spans.json

# search + single-trace assembly + service graph
curl -s 'http://127.0.0.1:5080/api/traces?service=my-svc'
```

(Feature `traces`, implies `obs`.)

## Super-cluster federated search (EG-243)

A read query fans out across a peer registry of engine instances (regions/clusters) and the local store,
then unions/de-dups + RRF-re-ranks the partials — a slow/dead peer degrades to `partial: true`, never
fails. This is its **own** listener (`EPISTEMIC_GRAPH_FEDERATED_ADDR`, feature `federation-search`), not on
the obs listener:

```bash
EPISTEMIC_GRAPH_FEDERATED_ADDR=127.0.0.1:7900 \
EPISTEMIC_GRAPH_FEDERATION_PEERS='http://peer-b:7900,http://peer-c:7900' \
EPISTEMIC_GRAPH_FEDERATION_ALLOW='peer-b,peer-c' \
  epistemic-graph-server --persist-dir /var/lib/eg   # --features "federation-search server"

curl -s -XPOST 'http://127.0.0.1:7900/federated' \
  -H 'content-type: application/json' \
  --data '{"query":"SELECT * FROM logs LIMIT 100","lang":"sql"}'
```

`EPISTEMIC_GRAPH_FEDERATION_ALLOW` is a fail-closed SSRF allowlist for the peer fan-out. Peers answer each
other over `/federated?local=1` (run-locally-only). See
[connecting](connecting.md#super-cluster-federated-search-federation-search).

## The engine's own telemetry

Distinct from ingesting *other* systems' observability, the engine exports its **own** metrics/traces:
Prometheus `/metrics` (feature `metrics`, default-on, `GRAPH_SERVICE_METRICS_ADDR`) and OTLP span export
(feature `otel`, `EPISTEMIC_GRAPH_OTLP_ENDPOINT`) + a slow-query log (`EPISTEMIC_GRAPH_SLOW_QUERY_MS`).
See the [runbook](../operations/runbook.md#8-observability-of-the-engine-itself).
