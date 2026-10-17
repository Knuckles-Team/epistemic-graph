# Image for the native epistemic-graph-server executable.
#
# Stage 1 installs the built `epistemic-graph-server` wheel (staged in docker/wheels by the
# release workflow, else the exact published release). The final stage is a slim base carrying
# only the binary.
#
#   docker build -f docker/Dockerfile -t <registry>/epistemic-graph-server:<tag> .

FROM python:3.14-slim@sha256:cea0e6040540fb2b965b6e7fb5ffa00871e632eef63719f0ea54bca189ce14a6 AS builder
COPY --from=ghcr.io/astral-sh/uv:0.11.7@sha256:240fb85ab0f263ef12f492d8476aa3a2e4e1e333f7d67fbdd923d00a506a516a /uv /bin/uv
ARG EGS_VERSION=2.27.3
COPY docker/wheels/ /wheels/
RUN set -eu; \
    case "${EGS_VERSION}" in ''|*[!0-9.]*) echo 'EGS_VERSION must be numeric' >&2; exit 2 ;; esac; \
    uv venv /opt/venv; \
    wheel=$(ls /wheels/epistemic_graph_server-*.whl 2>/dev/null | head -n1 || true); \
    if [ -n "${wheel}" ]; then \
      uv pip install --python /opt/venv/bin/python "${wheel}"; \
    else \
      uv pip install --python /opt/venv/bin/python "epistemic-graph-server==${EGS_VERSION}"; \
    fi; \
    /opt/venv/bin/epistemic-graph-server --help >/dev/null; \
    cp "$(readlink -f /opt/venv/bin/epistemic-graph-server)" /epistemic-graph-server

FROM debian:bookworm-slim AS runtime
LABEL org.opencontainers.image.title="epistemic-graph-server" \
      org.opencontainers.image.description="Native epistemic-graph server executable" \
      org.opencontainers.image.source="https://github.com/Knuckles-Team/epistemic-graph"
ENV GRAPH_SERVICE_PERSIST_DIR=/var/lib/epistemic-graph/data \
    GRAPH_SERVICE_TCP_ADDR=127.0.0.1:9100 \
    GRAPH_SERVICE_METRICS_ADDR=127.0.0.1:9101
RUN groupadd --system --gid 10001 epistemic \
    && useradd --system --uid 10001 --gid epistemic \
        --home-dir /var/lib/epistemic-graph --shell /usr/sbin/nologin epistemic \
    && mkdir -p /var/lib/epistemic-graph/data \
    && chown -R epistemic:epistemic /var/lib/epistemic-graph
COPY --from=builder /epistemic-graph-server /usr/local/bin/epistemic-graph-server
VOLUME ["/var/lib/epistemic-graph/data"]
EXPOSE 9100 9101
# The runtime has no interpreter: probe the TCP port with bash.
HEALTHCHECK --interval=15s --timeout=3s --start-period=20s --retries=5 \
    CMD ["bash", "-c", "exec 3<>/dev/tcp/127.0.0.1/9100"]
WORKDIR /var/lib/epistemic-graph
USER 10001:10001
# Loopback-only by default; routable deployments set GRAPH_SERVICE_TCP_ADDR with TLS/mTLS and an auth secret.
ENTRYPOINT ["epistemic-graph-server"]
