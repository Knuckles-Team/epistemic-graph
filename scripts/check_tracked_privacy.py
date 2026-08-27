#!/usr/bin/env python3
"""Reject host-specific data and local identities from tracked public artifacts.

Identifiers are derived in memory from the current account, home, checkout and
host. Findings report only ``file:line`` and a category; the sensitive matched
value is never written or printed. Machine paths and persisted path fields are
checked independently, so the gate remains useful in clean CI environments.

Ported verbatim from agent-utilities' ``scripts/check_tracked_privacy.py``
(guardrail-tracked-privacy), which already passes green on agent-utilities'
own ``main`` -- reused rather than reinvented per this fleet's
Extend-Before-Invent convention (near-duplicate gate helpers have drifted
apart before; see MEMORY ``gate-tool-published-before-gate-adopted``). The
ONLY behavioral change from the agent-utilities original is
``_is_runtime_source_path`` below, which no longer gates on
agent-utilities' OWN top-level directory allowlist (``agent_utilities``,
``deploy``, ``docker``, ...) -- that allowlist is specific to that repo's
tree shape, and porting it unmodified into a repo with a different tree
(e.g. this repo's own top-level source directories) would silently scan an
EMPTY universe for every directory not on the borrowed list: a confident
green verdict over zero files, the exact "gates that report more coverage
than they have" failure this fleet has hit before. See that function's own
docstring for the replacement rule.
"""

from __future__ import annotations

import argparse
import getpass
import hashlib
import ipaddress
import os
import re
import socket
import stat
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

# R-07: `pwd` is POSIX-only and raises ImportError at import time on Windows.
# It only ever supplies one extra candidate identifier (the passwd-db
# username) alongside several already-portable ones (getpass.getuser(),
# hostname, env vars, home-dir name) below, so on Windows this module simply
# runs with one fewer redundant source instead of failing to import at all.
if sys.platform != "win32":
    import pwd
else:  # pragma: no cover - exercised only on Windows
    pwd = None  # type: ignore[assignment]

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _git_subprocess_env import (  # noqa: E402
    sanitized_git_env,
    strip_inherited_git_repository_env,
)

# NE-059 (sibling of BUG-180/D-LGI-1, ported from agent-utilities' own fix):
# every ``git`` subprocess this module shells out to -- ``derive_local_
# identifiers``'s ``git rev-parse --git-common-dir``/``git config --get
# user.name|email`` and ``_git_file_names``'s ``git ls-files`` -- can inherit
# a real ``git commit``/``git push``'s exported ``GIT_DIR``/``GIT_INDEX_FILE``
# unmodified when this script runs as its own pre-commit hook: neither call
# passed its own ``env=``, and those vars win over ``cwd=``'s path-based
# repository discovery. As a *privacy/security* gate, a poisoned resolution
# is worse than a crash -- it can silently swap in a different repository's
# tracked-file inventory or let a decoy's exclude rules drop a real leaking
# file from the sweep, so the gate reports PASS having never looked at the
# file that leaks. Strip once, process-wide, at import time *and* pass an
# explicit sanitized ``env=`` at each call site, so no future call in this
# module can regress silently by omitting the strip precondition.
strip_inherited_git_repository_env()

ROOT = Path(__file__).resolve().parent.parent

_TEXT_SUFFIXES = frozenset({".md", ".json", ".yaml", ".yml", ".toml"})
_SOURCE_SUFFIXES = frozenset(
    {".js", ".md", ".ps1", ".py", ".rs", ".sh", ".ts", ".yaml", ".yml"}
)
_GENERIC_IDENTIFIERS = frozenset(
    {
        "admin",
        "agent",
        "apps",
        "build",
        "developer",
        "genius",
        "home",
        "localhost",
        "maintainer",
        "maintainers",
        "root",
        "runner",
        "service",
        "user",
        "workspace",
    }
)
_HOME_PATH_PATTERN = (
    r"(?:(?<![A-Za-z0-9_.-])/home/(?P<home_user>[A-Za-z0-9_.-]+)(?:/|\b)|"
    r"(?<![A-Za-z0-9_.-])/Users/(?P<users_user>[A-Za-z0-9_.-]+)(?:/|\b)|"
    r"(?<![A-Za-z0-9_.-])/mnt/[A-Za-z]/Users/(?P<mnt_user>[A-Za-z0-9_.-]+)(?:/|\b)|"
    # BUG-228: this branch had no left boundary guard (unlike the three
    # above), so a REST route id like ``"route:GET:/users/{id}"`` spuriously
    # matched it -- the "T" ending "GET" read as a fake drive letter. The
    # same lookbehind the other branches already use fixes it: a real drive
    # letter is never itself preceded by another identifier character.
    r"(?<![A-Za-z0-9_.-])[A-Za-z]:[\\/]Users[\\/](?P<win_user>[^\\/\s]+)(?:[\\/]|\b))"
)
_HOME_PATH_RE = re.compile(_HOME_PATH_PATTERN, re.IGNORECASE)
# BUG-228: usernames this repo's own tests use, over and over, as a
# documented "this is not a real account" stand-in -- generic role nouns
# (operator/user/local/app/account/person), the RFC 2606 "example" word and
# its natural variants (example/example-user/exampleuser), classic
# protocol-documentation personas (alice/bob, same convention IETF RFCs
# use), this repo's own synthetic-account naming idiom (agent-user and
# other ``*-account`` fixtures), and single-letter stand-ins (a/u). None of
# these can identify a real person or host; only the ambient candidates
# :func:`derive_local_identifiers` derives (the actual current account) and
# a handful of specific real names (e.g. the developer account, the real
# workspace root) do that, and those are NOT in this set on purpose.
#
# D-W12-AU-EXCEPTIONS-3: "someone" was in this set from BUG-228 through
# 2026-08-16 but was never actually named in this comment's own rationale
# above (it does not fit "generic role noun", the "example" family, the
# alice/bob personas, the ``*-account``/agent-user idiom, or a single-letter
# stand-in) -- a stray addition, not a documented convention. It silently
# regressed tests/gates/test_docs_contract_gate.py's
# ``test_privacy_gate_scans_unchanged_runtime_source_not_only_the_diff``,
# whose fixture (``/home/`` + the placeholder account ``someone`` + ``/state/tree``)
# exists specifically to prove ``classify_runtime_source_line`` detects an
# arbitrary, non-reserved home path -- reserving that exact username made the
# positive fixture invisible, the same "verdict narrower than its name"
# failure mode BUG-241 named. A repo-wide sweep at fix time found zero
# remaining sites depending on the reservation, so removing it does not
# reopen BUG-228's ~130-false-positive flood; if a genuine placeholder-account
# home-path fixture is ever needed again, use one of the ALREADY-reserved
# words above instead of re-adding this one.
_RESERVED_HOME_USERS = frozenset(
    {
        "a",
        "a-different-account",
        "account",
        "agent-user",
        "alice",
        "app",
        "bob",
        "example",
        "example-user",
        "exampleuser",
        "local",
        "local-account",
        "operator",
        "person",
        "sensitive-account",
        "some-account",
        "u",
        "user",
    }
)


def _home_path_user(match: re.Match[str]) -> str | None:
    for name in ("home_user", "users_user", "mnt_user", "win_user"):
        value = match.groupdict().get(name)
        if value:
            return value
    return None


def _is_reserved_home_user(user: str) -> bool:
    return user.strip("\\/").casefold() in _RESERVED_HOME_USERS


def _has_real_home_path(line: str) -> bool:
    """True if any home-path match on this line is NOT a reserved placeholder.

    A line can carry more than one match (e.g. a before/after pair); it is
    only a real leak if at least one of them is not a documented stand-in.
    """
    return any(
        not _is_reserved_home_user(user)
        for match in _HOME_PATH_RE.finditer(line)
        for user in (_home_path_user(match),)
        if user is not None
    )


# BUG-228: RFC 2606/6761 documentation domains, RFC 5737/3927/3849 reserved
# address blocks, and localhost can never resolve to (or disclose) a real
# host, so a fixture built on one of these is provably synthetic -- never a
# leak, regardless of what appears before the ``://``. Recognizing them is
# what keeps the widened runtime-source/credential/endpoint passes from
# being noisy enough to end up in someone's SKIP= list (the exact failure
# mode this widening exists to avoid repeating).
_RESERVED_DOCUMENTATION_TLDS = frozenset({"test", "example", "invalid", "localhost"})
_RESERVED_EXAMPLE_DOMAINS = frozenset({"example.com", "example.net", "example.org"})
_RESERVED_IPV4_NETWORKS = tuple(
    ipaddress.ip_network(cidr)
    for cidr in (
        "127.0.0.0/8",  # RFC 5735 loopback (0.0.0.0/32 below is separate)
        "0.0.0.0/32",
        "192.0.2.0/24",  # RFC 5737 TEST-NET-1
        "198.51.100.0/24",  # RFC 5737 TEST-NET-2
        "203.0.113.0/24",  # RFC 5737 TEST-NET-3
        "169.254.0.0/16",  # RFC 3927 link-local
    )
)
_RESERVED_IPV6_NETWORKS = tuple(
    ipaddress.ip_network(cidr)
    for cidr in (
        "fe80::/10",  # RFC 4291 link-local
        "2001:db8::/32",  # RFC 3849 documentation
    )
)


def _is_reserved_hostname(host: str) -> bool:
    """True for an RFC-reserved-for-documentation hostname/address.

    Covers example.com/.net/.org and any ``*.test``/``*.example``/
    ``*.invalid``/``*.localhost`` name (RFC 2606, 6761), plus the RFC
    5737/3927/3849 documentation address blocks and bare ``localhost``.

    BUG-241: also covers this repo's own extension of the RFC 2606
    convention -- a hostname whose LEADING label IS ``example`` or is
    prefixed ``example-`` (e.g. ``example-host-prod.internal.arpa``, the
    adversarial-fixture convention already used in
    ``tests/unit/mcp/test_compliance_tools.py``, and mirroring the existing
    ``_RESERVED_HOME_USERS`` "example"/"example-user" handling for home
    paths). Scoped to the FIRST label only, never any label anywhere in the
    name: a multi-label internal-looking value can legitimately carry
    "example" as an unrelated interior segment (a pre-existing fixture in
    ``tests/gates/test_docs_contract_gate.py`` builds a value with an interior
    ``example`` label ahead of the ``svc``/``cluster``/``local`` triplet this
    way to prove a genuine leak is still caught) -- only the convention's
    actual signal position,
    the label that names the fake host itself, counts.
    """
    candidate = host.strip().rstrip(".").casefold()
    if not candidate:
        return False
    if candidate == "localhost" or candidate.endswith(".localhost"):
        return True
    # CX-RAT-09: ``host.docker.internal`` is Docker's OWN published, universal
    # convention (Docker Desktop's `extra_hosts: host.docker.internal:host-
    # gateway` special value, documented at docs.docker.com and used in
    # millions of public docker-compose.yml files) -- it names no host
    # specific to any one deployment; every Docker install answers to it
    # identically. A single fixed literal, not a wildcard/prefix rule, so
    # this cannot mask a genuine ``*.internal`` leak the way widening the
    # general suffix match would.
    if candidate == "host.docker.internal":
        return True
    if candidate in _RESERVED_EXAMPLE_DOMAINS or any(
        candidate.endswith(f".{domain}") for domain in _RESERVED_EXAMPLE_DOMAINS
    ):
        return True
    labels = candidate.split(".")
    if labels[0] == "example" or labels[0].startswith("example-"):
        return True
    last_label = labels[-1]
    if last_label in _RESERVED_DOCUMENTATION_TLDS:
        return True
    try:
        address = ipaddress.ip_address(candidate)
    except ValueError:
        return False
    networks = (
        _RESERVED_IPV6_NETWORKS if address.version == 6 else _RESERVED_IPV4_NETWORKS
    )
    return any(address in network for network in networks)


_PERSISTED_FIELD_RE = re.compile(
    r"[\"']?(?P<field>workspace_path|source_path|skill_path|local_path|source_file|"
    r"eg_ledger_path)[\"']?\s*[:=]\s*(?P<value>.+)",
    re.IGNORECASE,
)
_NEUTRAL_URI_RE = re.compile(
    r"^[\s\"']*(?:repo|skill|connector|design)://", re.IGNORECASE
)
_INTERNAL_ENDPOINT_RE = re.compile(
    r"(?i)\b(?:[A-Za-z0-9-]+\.)+(?:arpa|internal)\b|"
    r"\b(?:[A-Za-z0-9-]+\.)*svc\.cluster\.local\b|"
    r"\b(?:10(?:\.\d{1,3}){3}|192\.168(?:\.\d{1,3}){2}|"
    r"172\.(?:1[6-9]|2\d|3[01])(?:\.\d{1,3}){2})\b"
)
# BUG-241: the internal-endpoint pass used to be ``_SOURCE_INTERNAL_URL_RE``,
# which required a URL scheme (``https?://``) before the host -- so a BARE
# hostname literal (no ``scheme://`` prefix at all, e.g. a YAML
# ``ingress_host: example-graph-os.arpa`` value or a Python string literal
# ``"example-vllm.arpa"``) was structurally invisible to it. A scheme is not
# what makes a hostname sensitive; the hostname is. This pattern drops the
# scheme requirement entirely and matches the hostname shape wherever it
# appears on the line -- inside a URL, a bare literal, an f-string, a YAML
# scalar, all of it -- covering the same suffix family the docs-path
# ``_INTERNAL_ENDPOINT_RE`` already recognizes bare (``.arpa``/``.internal``)
# plus the two extra suffixes (``.corp``/``.lan``) the old scheme-gated
# pattern additionally covered, so nothing already-caught regresses. At
# least one label must precede ``arpa``/``internal``/``corp``/``lan`` (a
# bare "internal" is an ordinary English word, not a hostname), while the
# three-label Kubernetes cluster-DNS suffix (``svc``, ``cluster``, ``local``)
# is specific enough on its own to match with zero or more leading labels,
# same as the docs-path pattern.
#
# Deliberately CASE-SENSITIVE on the suffix (no ``(?i)``): a first corpus run
# of this pattern with ``(?i)`` produced 19 false positives, every one of
# them ``DataClassification.INTERNAL``/``SomeEnum.INTERNAL`` -- a Python
# UPPER_SNAKE_CASE enum member access, not a hostname (``DataClassification``
# is itself a syntactically valid DNS label, so nothing about the identifier
# shape alone rules it out). Every genuine hostname literal in this corpus,
# real or synthetic, is written lowercase (``example-graph-os.arpa``,
# ``example-vllm.arpa``, ``example-host.docker.internal``,
# ``example-model.internal``, ...) -- matching DNS/hostname
# convention -- while every enum-attribute access in this codebase is
# UPPER_SNAKE_CASE by convention, so requiring an exact-case lowercase
# suffix separates the two shapes cleanly without a Python-syntax-aware
# parse. The preceding label(s) stay case-insensitive (a hostname label
# itself may legitimately mix case), only the fixed suffix word is
# case-locked.
_HOSTNAME_LABEL_RE = r"[A-Za-z0-9](?:[A-Za-z0-9-]*[A-Za-z0-9])?"
_BARE_INTERNAL_HOSTNAME_RE = re.compile(
    r"(?<![A-Za-z0-9_.-])"
    r"(?:"
    rf"(?:{_HOSTNAME_LABEL_RE}\.)+(?:arpa|internal|corp|lan)"
    r"|"
    rf"(?:{_HOSTNAME_LABEL_RE}\.)*svc\.cluster\.local"
    r")"
    r"(?![A-Za-z0-9_.-])"
)


def _internal_endpoint_in_line(line: str) -> bool:
    """True if ``line`` contains a non-reserved internal-hostname literal.

    Scans EVERY candidate match, not just the first, so a reserved
    placeholder earlier on the line (e.g. ``example-host-prod.internal.arpa``
    used in an adversarial-fixture comment) never masks a real leak later on
    the same line.
    """
    return any(
        not _is_reserved_hostname(match.group(0))
        for match in _BARE_INTERNAL_HOSTNAME_RE.finditer(line)
    )


_PRIVATE_KEY_LINE_RE = re.compile(r"^\s*-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----\s*$")
_CREDENTIAL_URI_RE = re.compile(
    r"(?i)\b[a-z][a-z0-9+.-]*://[^\s/@:]+:(?P<secret>[^\s/@]+)"
    r"@(?P<cred_host>[^\s/@:\"'<>]+)"
)
# D-CIP-15 / BUG-228: a documented template placeholder (the repo's own
# convention -- see deploy_wizard.py's ``_warn_production_safety`` and
# agent_utilities.observability.langfuse_trust's ``_CREDENTIAL_SENTINELS``)
# is never a live credential, so a URI shaped like one must not be flagged as
# though it were. This set had drifted out of sync with
# scripts/check_wheel_privacy.py's own ``_CREDENTIAL_PLACEHOLDER_TOKENS`` --
# the comment already claimed they mirrored each other, but that script's
# set additionally recognizes "agent" (this repo's own
# ``postgresql://agent:agent@localhost:5432/agent_kg`` documented example
# DSN, README.md / docs/architecture/graph_backends_architecture.md),
# "password", "secret", "test", and "sample" as placeholder words. Restored
# so the two gates actually agree, as documented.
_CREDENTIAL_PLACEHOLDER_TOKENS = frozenset(
    {
        "agent",
        "changeme",
        "change_me",
        "example",
        "fixme",
        "masked",
        "password",
        "placeholder",
        "redacted",
        "replace",
        "sample",
        "secret",
        "test",
        "todo",
        "xxxx",
        "your",
    }
)
_HOST_IDENTITY_RE = re.compile(r"(?i)\bssh://(?!\$\{)[^\s/@]+@")
_MACHINE_HOST_ID_RE = re.compile(r"(?i)(?<![a-z0-9])(?:rw?|host)[0-9]{3,}(?![a-z0-9])")
_NEUTRAL_AUTHOR_NAME = "repository maintainers"
_NEUTRAL_AUTHOR_EMAIL_SUFFIX = "@example.invalid"
_SCAN_EXCLUDED_DIRECTORIES = frozenset(
    {
        ".acp-sessions",
        ".benchmarks",
        ".git",
        ".hypothesis",
        ".mypy_cache",
        ".nox",
        ".pytest_cache",
        ".pytest_tmp",
        ".ruff_cache",
        ".tox",
        ".venv",
        "__pycache__",
        "build",
        "dist",
        "htmlcov",
        "node_modules",
        "site",
        "target",
        "venv",
        "workspace",
    }
)
_MAX_SCAN_FILES = 500_000


@dataclass(frozen=True)
class Violation:
    path: str
    line: int
    category: str
    content_hash: str
    ordinal: int

    def render(self) -> str:
        return f"{self.path}:{self.line}: {self.category}"


def _content_hash(text: str) -> str:
    """Irreversible fingerprint of a line's content, never the value itself.

    A SHA-256 digest cannot be inverted back to the sensitive substring that
    produced it, so it is safe to compute and hold in memory even though
    ``render()``/every printed message still withholds the matched value.
    """
    return hashlib.sha256(text.encode("utf-8", errors="replace")).hexdigest()[:16]


def _is_credential_placeholder(secret: str) -> bool:
    rendered = secret.strip()
    if not rendered:
        return True
    if re.fullmatch(r"(?:\*+|#+|x{4,})", rendered, flags=re.IGNORECASE):
        return True
    tokens = set(re.findall(r"[a-z0-9]+", rendered.lower()))
    return bool(tokens & _CREDENTIAL_PLACEHOLDER_TOKENS)


def _is_credential_exempt(match: re.Match[str]) -> bool:
    """A credential-shaped URI is not a real leak when either the secret
    token is a documented placeholder word (existing behaviour) OR the host
    it targets is RFC-reserved-for-documentation (BUG-228) -- a made-up
    password pointed at ``example.test``/``localhost``/etc. cannot be a live
    credential no matter what the password portion looks like.
    """
    if _is_credential_placeholder(match.group("secret")):
        return True
    host = match.group("cred_host")
    return bool(host) and _is_reserved_hostname(host)


def _identifier_from_path(value: str) -> set[str]:
    identifiers: set[str] = set()
    normalized = value.replace("\\", "/")
    for pattern in (r"/home/([^/]+)", r"/Users/([^/]+)"):
        identifiers.update(re.findall(pattern, normalized, flags=re.IGNORECASE))
    return identifiers


def derive_local_identifiers(root: Path = ROOT) -> frozenset[str]:
    """Identifiers this gate treats as sensitive if they appear in tracked text.

    D-ORC-57: purely ambient derivation (OS username, hostname, the CALLING
    process's local git config) makes the verdict depend on who/where the
    scan runs, not on the tree being scanned -- the same tree can PASS
    standalone and FAIL under pre-commit for no reason other than the two
    invocations seeing different ambient identities. Observed live: a lane's
    throwaway sandbox had ``git config user.name`` set to "Guard Test" (an
    isolated test identity, unrelated to this repository), and "guard test"
    then substring-matched the unrelated phrase "architecture guard test"
    already present in a tracked comment -- 279 manufactured false leaks from
    one ambient identity accident.

    ``AGENT_UTILITIES_PRIVACY_IDENTIFIERS`` is a DECLARED, stable override:
    when set (comma- or newline-separated), it is used INSTEAD of the
    ambient OS/git-config-derived candidates below, so CI/pre-commit can pin
    one deterministic identity set regardless of which sandbox or host runs
    the scan. Unset, behaviour is unchanged from before this fix -- additive
    only, so a caller that has not opted in loses no coverage (the real
    absolute-homelab-path leak D-GDI-1 caught stays caught either way; that
    detector is independent of this identifier set).

    ``git log -1 --format=%an%n%ae`` (HEAD's own committer identity) is
    deliberately NOT one of the candidate sources below -- it was removed
    2026-08-17 after it produced a ~450-line-across-~180-file false-positive
    flood, the same D-ORC-57 shape (a wrong ambient value substring-matching
    ordinary prose) but from a different source. ``git config user.name``/
    ``user.email`` capture the CALLING PROCESS's own stable, deliberately-set
    identity -- what this docstring means by "the current account". HEAD's
    last-commit author is not that: it is whichever identity committed most
    recently, which in this repo's own multi-agent-authored history rotates
    per commit and is, at the moment this was fixed, literally the single
    word "claude" (``git log -1 --format=%an`` on `main`) -- an ordinary,
    extremely common word in a Claude-focused agent-framework codebase, so
    every doc/comment/design-note that so much as names the tool became a
    manufactured leak. Unlike the "Guard Test" incident, adding it to
    ``_GENERIC_IDENTIFIERS`` would not fix this class -- the next commit's
    author could just as easily be "codex", "sonnet", "opus", or any other
    tool/model name, each requiring its own reactive exclusion. ``git
    config``'s two sources remain and already capture the real, stable
    developer identity (proven: this checkout's ``user.name``/``user.email``
    resolve to a real name + email, independent of whatever authored HEAD),
    so removing the HEAD-author fallback loses no genuine detection here --
    it only removes a signal that was never "the current account" in the
    first place.
    """
    override = os.environ.get("AGENT_UTILITIES_PRIVACY_IDENTIFIERS", "").strip()
    if override:
        declared = {
            value.strip() for value in re.split(r"[,\n]", override) if value.strip()
        }
        return frozenset(
            value.casefold()
            for value in declared
            if len(value) >= 4 and value.casefold() not in _GENERIC_IDENTIFIERS
        )

    candidates = {
        getpass.getuser(),
        socket.gethostname(),
        socket.gethostname().split(".", 1)[0],
        os.environ.get("USER", ""),
        os.environ.get("LOGNAME", ""),
        os.environ.get("USERNAME", ""),
        Path.home().name,
    }
    if pwd is not None:  # POSIX: one more redundant source, see import above
        candidates.add(pwd.getpwuid(os.getuid()).pw_name)
    candidates.update(_identifier_from_path(str(Path.home())))
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--path-format=absolute", "--git-common-dir"],
            cwd=root,
            check=True,
            capture_output=True,
            text=True,
            env=sanitized_git_env(),
        )
        candidates.update(_identifier_from_path(result.stdout.strip()))
    except (OSError, subprocess.SubprocessError):
        pass
    for command in (
        ["git", "config", "--get", "user.name"],
        ["git", "config", "--get", "user.email"],
    ):
        try:
            result = subprocess.run(
                command,
                cwd=root,
                check=False,
                capture_output=True,
                text=True,
                env=sanitized_git_env(),
            )
            for value in result.stdout.splitlines():
                candidates.add(value.strip())
        except OSError:
            pass
    return frozenset(
        value.casefold()
        for value in candidates
        if value and len(value) >= 4 and value.casefold() not in _GENERIC_IDENTIFIERS
    )


def _is_deployment_doc(path: Path) -> bool:
    value = path.as_posix().casefold()
    return value.startswith("docs/recipes/") or any(
        marker in value
        for marker in (
            "deploy",
            "runbook",
            "configuration",
            "workspace-config",
            "mcp_auth",
            "secrets-auth",
        )
    )


def classify_line(
    line: str,
    *,
    identifiers: frozenset[str],
    deployment_doc: bool,
) -> frozenset[str]:
    categories: set[str] = set()
    persisted = _PERSISTED_FIELD_RE.search(line)
    if persisted and not _NEUTRAL_URI_RE.search(persisted.group("value")):
        value = persisted.group("value").strip(" \t,;)}]\"'").casefold()
        field = persisted.group("field")
        # A shell/template interpolation placeholder (``${WORKSPACE_ROOT}``, closing
        # brace already stripped above) is resolved at runtime by whatever consumes
        # the file, never a baked-in machine path — safe regardless of the field's
        # own casing, same reasoning as the existing uppercase-field exemption.
        is_template_placeholder = value.startswith("${")
        runtime_relative = (
            field.isupper() or is_template_placeholder
        ) and not re.match(r"^(?:[a-z]:|[/\\]|~)", value, re.IGNORECASE)
        if value not in {"", "none", "null", "unset"} and not runtime_relative:
            categories.add("persisted machine path")
    if "persisted machine path" not in categories and _has_real_home_path(line):
        categories.add("machine-specific home path")
    folded = line.casefold()
    if any(
        re.search(rf"(?<![\w-]){re.escape(value)}(?![\w-])", folded)
        for value in identifiers
    ):
        categories.add("local account or host identifier")
    if _MACHINE_HOST_ID_RE.search(line):
        categories.add("machine-specific host identifier")
    if deployment_doc and _INTERNAL_ENDPOINT_RE.search(line):
        categories.add("hard-coded internal endpoint")
    if deployment_doc:
        credential_match = _CREDENTIAL_URI_RE.search(line)
        if credential_match and not _is_credential_exempt(credential_match):
            categories.add("credential-bearing URI")
    if deployment_doc and _HOST_IDENTITY_RE.search(line):
        categories.add("hard-coded remote account")
    return frozenset(categories)


def classify_runtime_source_line(
    line: str, *, identifiers: frozenset[str]
) -> frozenset[str]:
    """Classify runtime/deployment source without applying public-doc path heuristics.

    Source code legitimately manipulates path-shaped values, so the generic
    ``source_path = ...`` rule would be noisy here. Concrete account paths,
    environment endpoints, credential-bearing URLs, and local identities are
    never legitimate package defaults and are checked for every shipped runtime
    and deployment file instead.

    D-CIP-10: the categories below used to be suffixed ``in changed source``,
    from when :func:`_runtime_source_artifacts` only looked at ``git diff``.
    That scope was the bug; the label is now ``in runtime source`` so the gate's
    own output cannot imply a narrower guarantee than it actually gives.
    """

    categories: set[str] = set()
    if _has_real_home_path(line):
        categories.add("machine-specific home path in runtime source")
    folded = line.casefold()
    if any(
        re.search(rf"(?<![\w-]){re.escape(value)}(?![\w-])", folded)
        for value in identifiers
    ):
        categories.add("local account or host identifier in runtime source")
    if _internal_endpoint_in_line(line):
        categories.add("hard-coded internal endpoint in runtime source")
    credential_match = _CREDENTIAL_URI_RE.search(line)
    if credential_match and not _is_credential_exempt(credential_match):
        categories.add("credential-bearing URI in runtime source")
    if _PRIVATE_KEY_LINE_RE.fullmatch(line):
        categories.add("private key material in runtime source")
    return frozenset(categories)


# BUG-228: this used to be {"docs", ".github"} plus top-level files and any
# *.toml, which is why a leak that landed under `tests/` or `.specify/` was
# structurally invisible to this pass -- the gate's own selection excluded
# the tree, not the file type. A tracked test fixture in a PUBLIC repo
# discloses exactly as much as tracked source: the file's location was never
# a legitimate signal for whether its content is safe to publish. Widened to
# every tracked-but-previously-excluded text tree that can carry a fixture
# (tests/), a design/spec artifact (.specify/), or a runnable example
# (examples/) -- not just the two trees someone happened to think of first.
_PUBLIC_TEXT_TREES = frozenset({"docs", ".github", "tests", ".specify", "examples"})


def _is_public_artifact(name: str) -> bool:
    path = Path(name)
    if path.suffix.casefold() not in _TEXT_SUFFIXES:
        return False
    if "skills" in path.parts:
        return False
    return (
        path.parts[0] in _PUBLIC_TEXT_TREES
        or len(path.parts) == 1
        or path.suffix.casefold() == ".toml"
    )


def _filesystem_files(root: Path) -> list[Path]:
    """Enumerate a bounded no-Git source snapshot without following links."""

    files: list[Path] = []
    for directory, directory_names, file_names in os.walk(root, topdown=True):
        current = Path(directory)
        traversable: list[str] = []
        for name in sorted(directory_names):
            if name in _SCAN_EXCLUDED_DIRECTORIES or name.endswith(".egg-info"):
                continue
            metadata = (current / name).lstat()
            if stat.S_ISLNK(metadata.st_mode):
                continue
            if stat.S_ISDIR(metadata.st_mode):
                traversable.append(name)
        directory_names[:] = traversable
        for name in sorted(file_names):
            path = current / name
            metadata = path.lstat()
            if not stat.S_ISREG(metadata.st_mode):
                continue
            files.append(path)
            if len(files) > _MAX_SCAN_FILES:
                raise RuntimeError("privacy source inventory exceeds its file bound")
    return files


def _git_file_names(root: Path, command: list[str]) -> list[str] | None:
    """Return Git inventory names, or ``None`` for an immutable no-Git snapshot."""

    if not (root / ".git").exists():
        return None
    result = subprocess.run(
        command,
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
        env=sanitized_git_env(),
    )
    if result.returncode != 0:
        return None
    return [name for name in result.stdout.splitlines() if name]


def _tracked_artifacts(root: Path) -> list[Path]:
    names = _git_file_names(
        root,
        ["git", "ls-files", "--cached", "--others", "--exclude-standard"],
    )
    candidates = (
        _filesystem_files(root) if names is None else [root / name for name in names]
    )
    return [
        path
        for path in candidates
        if _is_public_artifact(path.relative_to(root).as_posix())
    ]


def _runtime_source_artifacts(root: Path) -> list[Path]:
    """Every **tracked** runtime/deployment source path, not merely the changed ones.

    D-CIP-10. This pass used to be scoped to ``git diff``/untracked files, which
    made the gate structurally blind to its own back-catalogue: a machine path
    that landed in an earlier commit was never re-examined, so it stayed public
    forever. Combined with :func:`_is_public_artifact`'s *location* filter — which
    admits only ``docs/``, ``.github/``, top-level and ``*.toml`` — the two passes
    left a hole exactly where the real leaks live. ``docker/*.yaml`` passes the
    suffix test and fails the location test, so nothing scanned it; the gate
    reported 7 lines in 2 ``docs/`` files while 11 tracked files carried machine
    paths, two of them a **personal** account name (D-PCC-1).

    Scanning the whole tracked tree here closes that hole without inventing a
    heuristic: the classification (:func:`classify_changed_source_line`) and the
    scope (:func:`_is_runtime_source_path`, which already lists ``docker``) were
    both already correct and already noise-tuned. Only the *diff* restriction was
    wrong.

    A public GitHub repository publishes its history, not just its tip, so
    "changed in this commit" was never the right boundary for a privacy gate.
    """

    names = _git_file_names(
        root,
        ["git", "ls-files", "--cached", "--others", "--exclude-standard"],
    )
    candidates = (
        _filesystem_files(root) if names is None else [root / name for name in names]
    )
    return sorted(
        (
            path
            for path in candidates
            if path.suffix.casefold() in _SOURCE_SUFFIXES
            and _is_runtime_source_path(path.relative_to(root))
        ),
        key=lambda path: path.as_posix(),
    )


def _is_runtime_source_path(path: Path) -> bool:
    """Scope the source-literal pass to every tree that can carry a real leak.

    PORT NOTE (this file is ported from agent-utilities' identically-named
    function -- see the module docstring): the upstream version gates this
    pass on agent-utilities' OWN top-level directory allowlist
    (``agent_utilities``, ``deploy``, ``docker``, ``helm``, ``k8s``, ``tests``,
    ``.security``, ``.specify``, ``examples``). That allowlist encodes ONE
    repo's tree shape; reusing it unmodified here would scope this pass to
    directories that mostly do not exist in THIS repo, so it would measure an
    empty (or near-empty) universe and pass green while scanning nothing --
    exactly the "gate reports more coverage than it has" failure class this
    fleet has hit before (a hook silently discovering zero files). Upstream's
    own BUG-228 history argues against a location-based filter on the merits
    too: "a tracked test fixture in a PUBLIC repo discloses exactly as much
    as tracked source; the file's location was never a legitimate signal for
    whether its content is safe to publish." So this port drops the
    allowlist and scopes purely by suffix (``_SOURCE_SUFFIXES``, applied by
    the caller) plus git's own tracked/exclude-standard file set -- a
    directory that is gitignored (build output, ``node_modules``,
    ``target``, a virtualenv, ...) is already invisible to
    ``git ls-files --cached --others --exclude-standard`` and never reaches
    this function at all, so widening this check costs nothing and closes
    the coverage gap the borrowed allowlist would otherwise leave silently
    open.
    """

    return bool(path.parts)


def _is_bundled_connector_profile(path: Path) -> bool:
    parts = tuple(part.casefold() for part in path.parts)
    return parts[:4] == (
        "agent_utilities",
        "protocols",
        "source_connectors",
        "profiles",
    ) and path.suffix.casefold() in {".py", ".json", ".yaml", ".yml"}


def _author_metadata_lines(path: Path, lines: list[str]) -> list[int]:
    """Return non-neutral package-author lines without returning their values."""
    if path.suffix.casefold() != ".toml":
        return []
    violations: list[int] = []
    in_project_authors = False
    for number, line in enumerate(lines, 1):
        stripped = line.strip()
        if stripped == "[[project.authors]]":
            in_project_authors = True
            continue
        if in_project_authors and stripped.startswith("["):
            in_project_authors = False
        folded = stripped.casefold()
        if re.match(r"authors\s*=", folded):
            if (
                _NEUTRAL_AUTHOR_NAME not in folded
                or _NEUTRAL_AUTHOR_EMAIL_SUFFIX not in folded
            ):
                violations.append(number)
        elif in_project_authors and re.match(r"name\s*=", folded):
            if _NEUTRAL_AUTHOR_NAME not in folded:
                violations.append(number)
        elif in_project_authors and re.match(r"email\s*=", folded):
            if _NEUTRAL_AUTHOR_EMAIL_SUFFIX not in folded:
                violations.append(number)
    return violations


def _next_ordinal(
    counts: dict[tuple[str, str, str], int], group: tuple[str, str, str]
) -> int:
    ordinal = counts.get(group, 0)
    counts[group] = ordinal + 1
    return ordinal


def scan(root: Path = ROOT) -> list[Violation]:
    identifiers = derive_local_identifiers(root)
    violations: list[Violation] = []
    # (path, category, content_hash) -> count so far, i.e. an ordinal
    # disambiguating two genuinely-identical lines in the same file/category —
    # never the line number, which drifts under unrelated edits and would
    # otherwise report a moved (not new) leak as a phantom NEW finding.
    ordinals: dict[tuple[str, str, str], int] = {}
    for path in _tracked_artifacts(root):
        if not path.is_file():
            continue
        relative = path.relative_to(root)
        rel_str = relative.as_posix()
        deployment_doc = _is_deployment_doc(relative)
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
        for number in _author_metadata_lines(path, lines):
            category = "non-neutral package author identity"
            content_hash = _content_hash(lines[number - 1])
            ordinal = _next_ordinal(ordinals, (rel_str, category, content_hash))
            violations.append(
                Violation(rel_str, number, category, content_hash, ordinal)
            )
        for number, line in enumerate(lines, 1):
            for category in classify_line(
                line,
                identifiers=identifiers,
                deployment_doc=deployment_doc,
            ):
                content_hash = _content_hash(line)
                ordinal = _next_ordinal(ordinals, (rel_str, category, content_hash))
                violations.append(
                    Violation(rel_str, number, category, content_hash, ordinal)
                )
    for path in _runtime_source_artifacts(root):
        if not path.is_file():
            continue
        relative = path.relative_to(root)
        rel_str = relative.as_posix()
        if _is_bundled_connector_profile(relative):
            category = "bundled environment-specific connector profile"
            # Whole-file finding, not line-anchored: hash the path itself so
            # it stays stable regardless of the file's own line churn.
            content_hash = _content_hash(rel_str)
            ordinal = _next_ordinal(ordinals, (rel_str, category, content_hash))
            violations.append(Violation(rel_str, 1, category, content_hash, ordinal))
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
        for number, line in enumerate(lines, 1):
            for category in classify_runtime_source_line(line, identifiers=identifiers):
                content_hash = _content_hash(line)
                ordinal = _next_ordinal(ordinals, (rel_str, category, content_hash))
                violations.append(
                    Violation(rel_str, number, category, content_hash, ordinal)
                )
    return violations


# CX-RAT-09 (2026-08-27): the baseline/ratchet mechanism (frozen
# ``tracked_privacy_baseline.txt``, ``--update-baseline``) is DELETED, not
# merely emptied. A count-based allowance is the wrong instrument for a
# leak-prevention gate on a repo that publishes to a PUBLIC GitHub org: "N
# leaks are acceptable" is not a defensible end state at any N, and a count
# lets a NEW hostname leak hide behind a pre-existing FIXED one (fixing one
# baselined entry "spends" the budget a genuinely new leak could then use
# without tripping the gate). All findings visible at conversion time were
# burned to zero in the same commit as this rewrite -- see MEMORY
# ``no-ratchets-expose-tech-debt``: baseline/ratchet gates are not allowed in
# this workspace; findings are debt to burn down, never to freeze behind an
# allowance. ``MAX`` is an ABSOLUTE constant (not a per-repo drift budget)
# enforced regardless of how this script is invoked -- some callers in this
# fleet run gates through a zero-argument glob runner that never passes a
# ``--max`` flag, so the threshold lives in code, not in an argument a caller
# could omit.
MAX = 0


def main() -> int:
    parser = argparse.ArgumentParser(prog="check-tracked-privacy")
    parser.parse_args()

    violations = scan()
    count = len(violations)
    # Printed unconditionally -- pass or fail -- so the real count is always
    # visible in CI/pre-commit output, never only on failure.
    print(f"Tracked artifact privacy gate: {count} finding(s) (MAX={MAX}).")

    if count > MAX:
        print("Tracked artifact privacy gate FAILED:")
        for violation in violations:
            print(f"  - {violation.render()}")
        print("Matched values are intentionally suppressed.")
        print(f"{count} leak(s) found; absolute maximum is {MAX}.")
        # D-ORC-53: this gate returned a DIFFERENT verdict for the SAME tree
        # depending on invocation method (standalone script vs. via
        # pre-commit) at least once, with no code change in between — a
        # non-reproducible privacy verdict trains people to skip a gate that
        # is right often enough to be dangerous when it is silent. Printing
        # exactly what was resolved turns the NEXT occurrence into evidence
        # instead of another "prove it happened after the fact" investigation.
        print(
            "resolution (D-ORC-53 forensic breadcrumb): "
            f"ROOT={ROOT} cwd={Path.cwd()} total_violations={count} "
            f"PRE_COMMIT_HOME={os.environ.get('PRE_COMMIT_HOME', '<unset>')!r}"
        )
        return 1
    print("Tracked artifact privacy gate PASSED.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
