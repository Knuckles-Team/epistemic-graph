# syntax=docker/dockerfile:1.7
# ─────────────────────────────────────────────────────────────────────────────
# Production server image for the epistemic-graph DB engine (CONCEPT:OS-5.64).
#
# This builds a REAL standalone database server — the `cluster`-tier
# `epistemic-graph-server` binary (durable redb-authoritative store + cypher +
# SQL/DataFusion + ann + tsdb + blob + the compute domains + raft + pgwire), NOT
# the dev/pytest runner (that lives in docker/Dockerfile.test). The binary is the
# whole product: pure Rust, no Python, no PyO3.
#
# MULTI-ARCH: built with `docker buildx` for linux/amd64 + linux/arm64. The build
# stage compiles natively for each platform leg (TARGETPLATFORM), so no cross
# linker is needed inside the image build.
#
#   docker buildx build --platform linux/amd64,linux/arm64 \
#       -t <registry>/epistemic-graph:<tag> -f docker/Dockerfile .
# ─────────────────────────────────────────────────────────────────────────────

# ── Stage 1: build the server binary ─────────────────────────────────────────
# rust:slim is multi-arch (amd64+arm64). Pin to TARGETPLATFORM so each buildx leg
# compiles natively for its arch.
# rust:1-slim tracks the latest stable 1.x (needs edition2024 for transitive deps
# like anndists; do NOT pin to an old minor that predates it).
FROM --platform=$TARGETPLATFORM rust:1-slim-bookworm AS build

# Build the cluster tier by default; override at build time for a leaner image:
#   --build-arg EG_FEATURES="node,ast-extended"  (single-node, no raft/pgwire)
#   --build-arg EG_FEATURES="pi,ast-extended"    (Raspberry-Pi-lean, no DataFusion)
ARG EG_FEATURES="cluster,ast-extended"

RUN apt-get update && apt-get install -y --no-install-recommends \
        pkg-config \
        libssl-dev \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /src

# Copy manifests + sources. (Cargo's incremental cache mount below does the real
# dependency caching; copying crates/src is enough for a clean, reproducible build.)
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY src ./src

# Build the release server binary. `--no-default-features` so the feature set is
# EXACTLY EG_FEATURES (mirrors the maturin pi-recipe discipline — never union the
# default tier on top). `--locked` pins Cargo.lock for reproducibility.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked --no-default-features --features "${EG_FEATURES}" \
    && cp /src/target/release/epistemic-graph-server /usr/local/bin/epistemic-graph-server \
    && strip /usr/local/bin/epistemic-graph-server || true

# ── Stage 2: minimal runtime ─────────────────────────────────────────────────
# debian-slim (multi-arch) carries the glibc/openssl the dynamically-linked binary
# needs, plus a shell for the entrypoint + nc for the healthcheck.
FROM --platform=$TARGETPLATFORM debian:bookworm-slim AS runtime

LABEL org.opencontainers.image.title="epistemic-graph" \
      org.opencontainers.image.description="Durable Rust-native epistemic graph DB engine (cluster tier)" \
      org.opencontainers.image.source="https://github.com/Knuckles-Team/epistemic-graph"

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        libssl3 \
        netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

# Non-root user — the engine never needs root; the persist dir is owned by it.
RUN groupadd --system --gid 10001 epistemic \
    && useradd  --system --uid 10001 --gid epistemic --home-dir /var/lib/epistemic-graph \
        --shell /usr/sbin/nologin epistemic \
    && mkdir -p /var/lib/epistemic-graph/data \
    && chown -R epistemic:epistemic /var/lib/epistemic-graph

COPY --from=build /usr/local/bin/epistemic-graph-server /usr/local/bin/epistemic-graph-server

# Durable persist dir (the redb-authoritative source of truth) — VOLUME so it
# survives container recreation and can be backed by a named/host volume.
ENV GRAPH_SERVICE_PERSIST_DIR=/var/lib/epistemic-graph/data \
    GRAPH_SERVICE_TCP_ADDR=0.0.0.0:9100 \
    GRAPH_SERVICE_METRICS_ADDR=0.0.0.0:9101
VOLUME ["/var/lib/epistemic-graph/data"]

# RPC (9100) for clients; metrics (9101) for Prometheus.
EXPOSE 9100 9101

# Probe the live RPC port — a TCP connect to 9100 proves the accept loop is up.
HEALTHCHECK --interval=15s --timeout=3s --start-period=20s --retries=5 \
    CMD nc -z 127.0.0.1 9100 || exit 1

USER epistemic
WORKDIR /var/lib/epistemic-graph

# TCP is the remote transport (env-driven so a bare `docker run` with the env set
# is a working single-node DB); the engine also opens its per-platform UDS for
# local clients. An auth secret MUST be supplied via GRAPH_SERVICE_AUTH_SECRET or
# the server refuses to start. Extra args after the image name are forwarded ("$@").
ENTRYPOINT ["/bin/sh", "-c", "exec epistemic-graph-server --tcp-addr \"${GRAPH_SERVICE_TCP_ADDR}\" --persist-dir \"${GRAPH_SERVICE_PERSIST_DIR}\" --metrics-addr \"${GRAPH_SERVICE_METRICS_ADDR}\" \"$@\"", "--"]

# ─────────────────────────────────────────────────────────────────────────────
# RUN — single-node prod (durable, redb-authoritative by default):
#
#   docker volume create eg-data
#   docker run -d --name epistemic-graph \
#     -e GRAPH_SERVICE_AUTH_SECRET="$(openssl rand -hex 32)" \
#     -p 127.0.0.1:9100:9100 -p 127.0.0.1:9101:9101 \
#     -v eg-data:/var/lib/epistemic-graph/data \
#     <registry>/epistemic-graph:<tag>
#
# RUN — enterprise / remote (expose RPC to the cluster network; raft HA):
#
#   docker run -d --name eg-node-1 \
#     -e GRAPH_SERVICE_AUTH_SECRET="$SECRET" \
#     -e EPISTEMIC_GRAPH_RAFT_NODE_ID=1 \
#     -e EPISTEMIC_GRAPH_RAFT_PEERS="1@eg-node-1:9200,2@eg-node-2:9200,3@eg-node-3:9200" \
#     -p 9100:9100 -p 9101:9101 \
#     -v eg-data-1:/var/lib/epistemic-graph/data \
#     <registry>/epistemic-graph:<tag>
#
# (Repeat per node with the matching RAFT_NODE_ID; the cluster tier replicates the
#  authoritative redb store via openraft — CONCEPT:KG-2.188.)
# ─────────────────────────────────────────────────────────────────────────────
