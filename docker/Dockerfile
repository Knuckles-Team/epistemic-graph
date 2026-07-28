# Production server image for the epistemic-graph DB engine (CONCEPT:OS-5.64).
#
# The normal ``runtime`` target installs the exact published full wheel, preferring a
# platform-matching CI wheel when one is staged in ``docker/wheels``. The
# ``release-local`` target installs a complete hash-locked wheel closure without an
# index and is the only target admitted to exact release assembly. Neither target
# compiles Rust or accepts an arbitrary install command.
#
#   docker buildx build --platform linux/amd64,linux/arm64 \
#       -t <registry>/epistemic-graph:<tag> -f docker/Dockerfile .
#
#   docker build -f docker/Dockerfile --target release-local \
#     --build-arg RELEASE_REQUIREMENTS=epistemic-graph-requirements.txt \
#     --build-arg RELEASE_REQUIREMENTS_SHA256=<requirements-sha256> \
#     --build-arg EPISTEMIC_GRAPH_VERSION=<exact-version> .

FROM python:3.14-slim@sha256:cea0e6040540fb2b965b6e7fb5ffa00871e632eef63719f0ea54bca189ce14a6 AS builder-base
COPY --from=ghcr.io/astral-sh/uv:0.11.7@sha256:240fb85ab0f263ef12f492d8476aa3a2e4e1e333f7d67fbdd923d00a506a516a /uv /uvx /bin/
ENV UV_SYSTEM_PYTHON=1 \
    UV_HTTP_TIMEOUT=3600 \
    UV_LINK_MODE=copy \
    PYTHONDONTWRITEBYTECODE=1

# Published/CI-wheel input for ordinary image publication.
FROM builder-base AS builder-release
ARG EG_VERSION=2.23.2
ARG EG_WHEEL_DIR=docker/wheels
ARG TARGETARCH
COPY ${EG_WHEEL_DIR}/ /wheels/
RUN --mount=type=cache,target=/root/.cache/uv \
    set -eu; \
    case "${EG_VERSION}" in ''|*[!0-9.]*) echo 'EG_VERSION must be a numeric release version' >&2; exit 2 ;; esac; \
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
      uv pip install --system --break-system-packages "${wheel}"; \
    else \
      echo "No local wheel for arch '${TARGETARCH:-?}' in /wheels; installing the exact published release"; \
      uv pip install --system --break-system-packages "epistemic-graph[full]==${EG_VERSION}"; \
    fi; \
    python -c 'import sys; assert sys.version_info[:2] == (3, 14)'; \
    epistemic-graph-server --help >/dev/null

# Exact offline release input. The caller supplies the complete component closure and
# a hash-locked requirements file. Only wheels are accepted; local-source provenance,
# credentials, certificate material, and workstation paths never cross this boundary.
FROM builder-base AS builder-release-local
ARG RELEASE_REQUIREMENTS=epistemic-graph-requirements.txt
ARG RELEASE_REQUIREMENTS_SHA256
ARG EPISTEMIC_GRAPH_VERSION
COPY release-wheelhouse/ /tmp/release-wheelhouse/
RUN test "$(basename -- "${RELEASE_REQUIREMENTS}")" = "${RELEASE_REQUIREMENTS}" \
    && case "${RELEASE_REQUIREMENTS}" in *.txt) ;; *) exit 64 ;; esac \
    && test -n "${EPISTEMIC_GRAPH_VERSION}" \
    && printf '%s  %s\n' "${RELEASE_REQUIREMENTS_SHA256}" "/tmp/release-wheelhouse/${RELEASE_REQUIREMENTS}" \
        | sha256sum --check --strict - \
    && uv pip install --system --break-system-packages --offline --no-index \
        --find-links /tmp/release-wheelhouse --only-binary :all: --require-hashes \
        --requirement "/tmp/release-wheelhouse/${RELEASE_REQUIREMENTS}" \
    && python -c 'import importlib.metadata as m, pathlib, shutil, sys; expected = sys.argv[1]; assert sys.version_info[:2] == (3, 14); assert m.version("epistemic-graph") == expected; assert shutil.which("epistemic-graph-server"); roots = {pathlib.Path(d.locate_file("")) for d in m.distributions()}; assert not any(path for root in roots for path in root.glob("*.dist-info/direct_url.json")); import epistemic_graph.numeric' \
        "${EPISTEMIC_GRAPH_VERSION}"

FROM python:3.14-slim@sha256:cea0e6040540fb2b965b6e7fb5ffa00871e632eef63719f0ea54bca189ce14a6 AS runtime-base

LABEL org.opencontainers.image.title="epistemic-graph" \
      org.opencontainers.image.description="Durable Rust-native epistemic graph DB engine (the one full CPU artifact)" \
      org.opencontainers.image.source="https://github.com/Knuckles-Team/epistemic-graph"

ENV PYTHONUNBUFFERED=1 \
    GRAPH_SERVICE_PERSIST_DIR=/var/lib/epistemic-graph/data \
    GRAPH_SERVICE_TCP_ADDR=127.0.0.1:9100 \
    GRAPH_SERVICE_METRICS_ADDR=127.0.0.1:9101

# The digest-pinned Python base already supplies the runtime used by the folded wheel.
# Keep the exact target network-independent: no mutable apt index or unpinned OS package
# may enter release-local.  Python's standard library also supplies the health probe.
RUN groupadd --system --gid 10001 epistemic \
    && useradd --system --uid 10001 --gid epistemic \
        --home-dir /var/lib/epistemic-graph --shell /usr/sbin/nologin epistemic \
    && mkdir -p /var/lib/epistemic-graph/data \
    && chown -R epistemic:epistemic /var/lib/epistemic-graph

VOLUME ["/var/lib/epistemic-graph/data"]
EXPOSE 9100 9101
HEALTHCHECK --interval=15s --timeout=3s --start-period=20s --retries=5 \
    CMD ["python", "-c", "import socket; socket.create_connection(('127.0.0.1', 9100), timeout=2).close()"]
WORKDIR /var/lib/epistemic-graph

# Native TCP is loopback-only by default. Routable deployments set
# GRAPH_SERVICE_TCP_ADDR plus GRAPH_SERVICE_TLS_CERT / _KEY (and optional
# _CLIENT_CA for mTLS). A host-loopback-only port mapping may explicitly set the
# dangerous plaintext acknowledgement. An auth secret remains mandatory.
ENTRYPOINT ["/bin/sh", "-c", "exec epistemic-graph-server --tcp-addr \"${GRAPH_SERVICE_TCP_ADDR}\" --persist-dir \"${GRAPH_SERVICE_PERSIST_DIR}\" --metrics-addr \"${GRAPH_SERVICE_METRICS_ADDR}\" \"$@\"", "--"]

# Exact release target: only the closed, hash-locked wheelhouse reaches this image.
FROM runtime-base AS release-local
COPY --from=builder-release-local /usr/local /usr/local
USER 10001:10001

# Ordinary published/CI-wheel target remains the default final stage.
FROM runtime-base AS runtime
COPY --from=builder-release /usr/local /usr/local
USER 10001:10001

# Example — single-node production remains loopback-safe unless the operator
# explicitly configures a routable, authenticated TLS/mTLS endpoint:
#
#   docker volume create eg-data
#   docker run -d --name epistemic-graph \
#     -e GRAPH_SERVICE_AUTH_SECRET="$(openssl rand -hex 32)" \
#     -p 127.0.0.1:9100:9100 -p 127.0.0.1:9101:9101 \
#     -v eg-data:/var/lib/epistemic-graph/data \
#     <registry>/epistemic-graph:<tag>
