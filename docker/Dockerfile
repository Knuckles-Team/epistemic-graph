# syntax=docker/dockerfile:1
# ─────────────────────────────────────────────────────────────────────────────
# Production server image for the epistemic-graph DB engine (CONCEPT:OS-5.64).
#
# This image INSTALLS THE PREBUILT `node`-tier wheel — it does NOT compile Rust.
# The wheel (maturin `bindings = "bin"`) ships the `epistemic-graph-server` binary,
# which IS the whole product: pure Rust, no Python runtime code, no PyO3. The `node`
# tier is a COMPLETE single-node DB — durable redb-authoritative store + cypher +
# SQL/DataFusion + GraphQL + ann + tsdb + blob + text + the compute domains + the
# Postgres-wire listener (pgwire, CONCEPT:KG-2.189 / EG-352) — with NO raft (single
# node). The published default wheel is exactly this `node` tier (pyproject
# `[tool.maturin] features = ["node", "ast-extended"]`).
#
# Following the agents/* fleet standard, the package is pulled from PyPI with `uv`
# (`uv pip install --system`). `uv` auto-selects the per-platform manylinux wheel,
# so a single `docker buildx --platform linux/amd64,linux/arm64` build is multi-arch
# clean with NO per-arch wheel juggling. The build is a seconds-long download+install
# instead of a ~25-min/arch `cargo build --release`.
#
#   docker buildx build --platform linux/amd64,linux/arm64 \
#       -t <registry>/epistemic-graph:<tag> -f docker/Dockerfile .
#
# Pre-publish / local sanity: install a locally-built wheel instead of PyPI by
# pointing EG_INSTALL_SPEC at a path under the build context (bind-mounted at /ctx
# during the install), e.g. with a wheel in ./wheels of the build context:
#   docker build -f docker/Dockerfile \
#       --build-arg EG_INSTALL_SPEC=/ctx/wheels/epistemic_graph-2.5.0-...whl .
# (The DEFAULT image always installs the published PyPI package.)
#
# For an in-engine-Raft HA cluster the deployed binary needs the `cluster` tier
# (raft + compute-dist + the extra wire protocols), which is NOT published as the
# default wheel — build that variant from source (see docs/deployment.md). The
# DEFAULT image here is the fast, prebuilt single-node `node` wheel.
# ─────────────────────────────────────────────────────────────────────────────

FROM python:3.11-slim AS runtime

# uv: the fleet-standard fast installer (agents/* COPY it from the published image).
COPY --from=ghcr.io/astral-sh/uv:0.11.7 /uv /uvx /bin/

LABEL org.opencontainers.image.title="epistemic-graph" \
      org.opencontainers.image.description="Durable Rust-native epistemic graph DB engine (node tier — pgwire-capable single-node)" \
      org.opencontainers.image.source="https://github.com/Knuckles-Team/epistemic-graph"

# The published node-tier package + its minimum version. CI passes the exact just-
# published version via --build-arg EG_VERSION=<tag without v>. Override EG_INSTALL_SPEC
# to point at a local wheel for a pre-publish sanity build (see header).
ARG EG_VERSION=2.2.0
ARG EG_INSTALL_SPEC="epistemic-graph>=${EG_VERSION}"

ENV UV_SYSTEM_PYTHON=1 \
    UV_HTTP_TIMEOUT=3600 \
    UV_LINK_MODE=copy \
    PYTHONUNBUFFERED=1 \
    GRAPH_SERVICE_PERSIST_DIR=/var/lib/epistemic-graph/data \
    GRAPH_SERVICE_TCP_ADDR=0.0.0.0:9100 \
    GRAPH_SERVICE_METRICS_ADDR=0.0.0.0:9101

# Runtime libs only: glibc/openssl for the dynamically-linked binary + nc for the
# healthcheck. No build-essential, no rust — nothing compiles here.
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        libssl3 \
        netcat-openbsd \
    && rm -rf /var/lib/apt/lists/*

# Install the prebuilt wheel — seconds, no compile. uv selects the manylinux wheel
# matching this buildx platform leg. `--prerelease=allow` matches the fleet standard.
# Smoke the binary in-build so a broken install fails the image build.
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,target=/ctx \
    uv pip install --system --upgrade --break-system-packages --prerelease=allow ${EG_INSTALL_SPEC} \
    && epistemic-graph-server --help >/dev/null

# Non-root user — the engine never needs root; the persist dir is owned by it.
RUN groupadd --system --gid 10001 epistemic \
    && useradd  --system --uid 10001 --gid epistemic --home-dir /var/lib/epistemic-graph \
        --shell /usr/sbin/nologin epistemic \
    && mkdir -p /var/lib/epistemic-graph/data \
    && chown -R epistemic:epistemic /var/lib/epistemic-graph

# Durable persist dir (the redb-authoritative source of truth) — VOLUME so it
# survives container recreation and can be backed by a named/host volume.
VOLUME ["/var/lib/epistemic-graph/data"]

# RPC (9100) for clients; metrics (9101) for Prometheus.
EXPOSE 9100 9101

# Probe the live RPC port — a TCP connect to 9100 proves the accept loop is up.
HEALTHCHECK --interval=15s --timeout=3s --start-period=20s --retries=5 \
    CMD nc -z 127.0.0.1 9100 || exit 1

USER epistemic
WORKDIR /var/lib/epistemic-graph

# TCP is the remote transport (env-driven so a bare `docker run` with the env set is
# a working single-node DB); the engine also opens its per-platform UDS for local
# clients. An auth secret MUST be supplied via GRAPH_SERVICE_AUTH_SECRET or the
# server refuses to start. Set EPISTEMIC_GRAPH_PGWIRE_ADDR (e.g. 0.0.0.0:5433) to
# expose the Postgres-wire listener. Extra args after the image name are forwarded.
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
# RUN — with the Postgres-wire listener (psql / BI tools / ORMs):
#
#   docker run -d --name epistemic-graph \
#     -e GRAPH_SERVICE_AUTH_SECRET="$(openssl rand -hex 32)" \
#     -e EPISTEMIC_GRAPH_PGWIRE_ADDR=0.0.0.0:5433 \
#     -p 127.0.0.1:9100:9100 -p 127.0.0.1:5433:5433 \
#     -v eg-data:/var/lib/epistemic-graph/data \
#     <registry>/epistemic-graph:<tag>
#
# (The node-tier wheel is a COMPLETE single-node DB incl. the Postgres wire shim.
#  For in-engine-Raft HA replication — CONCEPT:KG-2.188 — build the `cluster` tier
#  from source; see docs/deployment.md.)
# ─────────────────────────────────────────────────────────────────────────────
