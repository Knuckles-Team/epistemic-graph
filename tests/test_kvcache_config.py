"""Environment precedence pins for ``KvCacheConfig.from_env``
(CONCEPT:EG-KG.backend.shipped-pip-installable-python).

Pure configuration resolution: no HTTP server and no native graph engine.
"""

from __future__ import annotations

import pytest

from epistemic_graph.kvcache import KvCacheConfig

pytestmark = pytest.mark.no_engine

_TEST_TOKEN = "test-kvcache-token"

_KVCACHE_ENV = (
    "EPISTEMIC_GRAPH_KVCACHE_URL",
    "EPISTEMIC_GRAPH_KVCACHE_ADDR",
    "EPISTEMIC_GRAPH_KVCACHE_TIMEOUT_S",
    "EPISTEMIC_GRAPH_KVCACHE_MAX_CONNECTIONS",
    "EPISTEMIC_GRAPH_KVCACHE_CLIENT_CERT",
    "EPISTEMIC_GRAPH_KVCACHE_CLIENT_KEY",
    "EPISTEMIC_GRAPH_KVCACHE_CLIENT_KEY_PASSWORD",
    "SSL_CERT_FILE",
    "REQUESTS_CA_BUNDLE",
    "SSL_CERT_DIR",
)


@pytest.mark.parametrize(
    ("environment", "overrides", "expected"),
    [
        # Explicit arguments win over every environment variable.
        (
            {
                "EPISTEMIC_GRAPH_KVCACHE_URL": "http://127.0.0.1:1111",
                "EPISTEMIC_GRAPH_KVCACHE_TIMEOUT_S": "9",
                "EPISTEMIC_GRAPH_KVCACHE_MAX_CONNECTIONS": "9",
            },
            {
                "base_url": " http://localhost:2222/ ",
                "addr": "3333",
                "timeout_s": 0.5,
                "max_connections": 4,
            },
            {
                "base_url": "http://localhost:2222",
                "timeout_s": 0.5,
                "max_connections": 4,
            },
        ),
        # An explicit addr argument wins over the URL environment variable.
        (
            {"EPISTEMIC_GRAPH_KVCACHE_URL": "http://127.0.0.1:1111"},
            {"addr": "on"},
            {"base_url": "http://127.0.0.1:9130"},
        ),
        # The URL environment variable wins over the ADDR environment variable.
        (
            {
                "EPISTEMIC_GRAPH_KVCACHE_URL": " http://127.0.0.1:1111/ ",
                "EPISTEMIC_GRAPH_KVCACHE_ADDR": "2222",
            },
            {},
            {"base_url": "http://127.0.0.1:1111"},
        ),
        # Defaults, then numeric environment overrides.
        (
            {},
            {},
            {
                "base_url": "http://127.0.0.1:9130",
                "timeout_s": 2.0,
                "max_connections": 32,
            },
        ),
        (
            {
                "EPISTEMIC_GRAPH_KVCACHE_TIMEOUT_S": "0.25",
                "EPISTEMIC_GRAPH_KVCACHE_MAX_CONNECTIONS": "7",
            },
            {},
            {"timeout_s": 0.25, "max_connections": 7},
        ),
        # Empty values fall back; SSL_CERT_FILE wins over REQUESTS_CA_BUNDLE.
        (
            {
                "EPISTEMIC_GRAPH_KVCACHE_TIMEOUT_S": "",
                "EPISTEMIC_GRAPH_KVCACHE_MAX_CONNECTIONS": "",
                "SSL_CERT_FILE": "file-ca.pem",
                "REQUESTS_CA_BUNDLE": "bundle-ca.pem",
                "SSL_CERT_DIR": "",
            },
            {},
            {
                "timeout_s": 2.0,
                "max_connections": 32,
                "ca_bundle": "file-ca.pem",
                "ca_directory": None,
            },
        ),
        (
            {
                "SSL_CERT_DIR": "ca-dir",
                "EPISTEMIC_GRAPH_KVCACHE_CLIENT_CERT": "client.pem",
                "EPISTEMIC_GRAPH_KVCACHE_CLIENT_KEY": "client.key",
                "EPISTEMIC_GRAPH_KVCACHE_CLIENT_KEY_PASSWORD": "",
            },
            {},
            {
                "ca_bundle": None,
                "ca_directory": "ca-dir",
                "client_cert": "client.pem",
                "client_key": "client.key",
                "client_key_password": None,
            },
        ),
    ],
)
def test_config_from_env_precedence(monkeypatch, environment, overrides, expected):
    for name in _KVCACHE_ENV:
        monkeypatch.delenv(name, raising=False)
    for name, value in environment.items():
        monkeypatch.setenv(name, value)
    config = KvCacheConfig.from_env(token=_TEST_TOKEN, **overrides)
    assert {key: getattr(config, key) for key in expected} == expected
