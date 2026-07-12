# syntax=docker/dockerfile:1
# ─────────────────────────────────────────────────────────────────────────────
# Production server image for the epistemic-graph DB engine (CONCEPT:OS-5.64).
#
# This image INSTALLS THE PREBUILT main wheel — it does NOT compile Rust.
# The wheel (maturin `bindings = "bin"`) ships the `epistemic-graph-server` binary,
# which IS the whole product: pure Rust, no Python runtime code, no PyO3. The `node`
# tier is a COMPLETE single-node DB — durable redb-authoritative store + cypher +
# SQL/DataFusion + GraphQL + ann + tsdb + blob + text + the compute domains + the
# Postgres-wire listener (pgwire, CONCEPT:KG-2.189 / EG-352) — with NO raft (single
# node). The published default wheel is exactly this one main build (pyproject
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
      org.opencontainers.image.description="Durable Rust-native epistemic graph DB engine (the one main full-featured single-node build)" \
      org.opencontainers.image.source="https://github.com/Knuckles-Team/epistemic-graph"

# The main published package + its minimum version. CI passes the exact release version via
# --build-arg EG_VERSION=<tag without v>. The PRIMARY install source is a LOCAL WHEEL
# built in the same CI run (see EG_WHEEL_DIR / the install step below) — the image does
# NOT round-trip through PyPI, so it can build the moment the wheels are built (no
# dependency on publish-pypi). EG_INSTALL_SPEC is only the FALLBACK (a plain `docker
# build` with no wheels present pulls the published package from PyPI).
ARG EG_VERSION=2.22.0
ARG EG_INSTALL_SPEC="epistemic-graph>=${EG_VERSION}"
# Directory (relative to the build context) holding CI-built wheels. CI downloads the
# `wheel-linux-x86_64` + `wheel-linux-aarch64` main-build artifacts here; the install
# step picks the wheel matching this buildx platform leg (TARGETARCH). For a plain
# build this dir holds only `.gitkeep`, so the step falls back to EG_INSTALL_SPEC.
ARG EG_WHEEL_DIR=docker/wheels
# Provided automatically by buildx per platform leg (amd64 / arm64).
ARG TARGETARCH

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

# Copy any CI-built wheels into the image so a multi-arch buildx build can select the
# per-arch wheel without a PyPI round-trip. Always present (.gitkeep) so this COPY
# never fails on a plain build.
COPY ${EG_WHEEL_DIR}/ /wheels/

# Install the prebuilt wheel — seconds, no compile. PRIMARY: install the arch-matching
# main wheel built in THIS CI run (from /wheels). FALLBACK: if no matching wheel is
# present (plain build), install EG_INSTALL_SPEC from PyPI. `--prerelease=allow` matches
# the fleet standard. Smoke the binary in-build so a broken install fails the image build.
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,target=/ctx \
    set -eu; \
    case "${TARGETARCH:-}" in \
      amd64) WHEELARCH=x86_64 ;; \
      arm64) WHEELARCH=aarch64 ;; \
      *)     WHEELARCH="" ;; \
    esac; \
    wheel=""; \
    if [ -n "${WHEELARCH}" ]; then \
      wheel=$(ls /wheels/epistemic_graph-*"${WHEELARCH}"*.whl 2>/dev/null | head -n1 || true); \
    fi; \
    if [ -n "${wheel}" ]; then \
      echo "Installing epistemic-graph from CI wheel artifact: ${wheel}"; \
      uv pip install --system --upgrade --break-system-packages --prerelease=allow "${wheel}"; \
    else \
      echo "No local wheel for arch '${TARGETARCH:-?}' in /wheels — falling back to PyPI: ${EG_INSTALL_SPEC}"; \
      uv pip install --system --upgrade --break-system-packages --prerelease=allow ${EG_INSTALL_SPEC}; \
    fi; \
    epistemic-graph-server --help >/dev/null

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
# (The main wheel is a COMPLETE single-node DB incl. the whole wire family.
#  For in-engine-Raft HA replication — CONCEPT:KG-2.188 — build the `cluster` layer
#  from source; see docs/deployment.md.)
# ─────────────────────────────────────────────────────────────────────────────
