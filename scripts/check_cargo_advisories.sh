#!/usr/bin/env bash
# Fail-closed Rust dependency CVE gate (W0.12 / CONCEPT:EG-KG.storage.cargo-dependency-cve-gate).
#
# Mirrors, on the Rust side, the exact fail-closed convention
# `scripts/audit_dependencies.py` (agent-utilities) runs on the Python side:
# every dependency advisory is a hard failure unless it has a justified,
# <=90-day-expiring risk acceptance recorded in a ledger file — here
# `.cargo-audit-allow.txt`, the Rust twin of the Python side's
# `.security-audit-allow.txt`.
#
# `deny.toml`'s `[advisories].ignore` list is the thing cargo-deny actually
# reads. `.cargo-audit-allow.txt` is the auditable paperwork trail: this script
# cross-validates the two so they can never drift —
#   * every deny.toml ignore entry must have a justified, unexpired ledger line
#     (an ignore with no accountable justification is a build failure), and
#   * every ledger line must correspond to a live deny.toml ignore (a stale
#     acceptance nobody is using is also a build failure), and
#   * an expired or >90-day-horizon ledger line fails even if still listed.
#
# If cargo-deny itself is not installed, this fails CLOSED with install
# instructions -- it never reports a silent pass in its absence.
set -euo pipefail
cd "$(dirname "$0")/.."

ALLOW_FILE=".cargo-audit-allow.txt"
DENY_TOML="deny.toml"

if ! command -v cargo-deny >/dev/null 2>&1; then
  cat >&2 <<'EOF'
FAIL: cargo-deny is not installed -- Rust dependency advisory validation cannot run.

Install it (requires network access to crates.io / index.crates.io / static.crates.io):
  cargo install cargo-deny --locked

Then re-run:
  scripts/check_cargo_advisories.sh
  # or directly:  cargo deny check advisories

This is a hard failure, not a skip -- we never report a pass we didn't verify.
EOF
  exit 2
fi

if [[ ! -f "$DENY_TOML" ]]; then
  echo "FAIL: $DENY_TOML is missing at the repo root." >&2
  exit 2
fi

# ── Cross-validate the ledger (.cargo-audit-allow.txt) against deny.toml's
#    [advisories].ignore list. Pure stdlib (tomllib, Python 3.11+) -- no new dep.
python3 - "$ALLOW_FILE" "$DENY_TOML" <<'PYEOF'
import datetime as dt
import re
import sys
import tomllib

allow_path, deny_path = sys.argv[1], sys.argv[2]
MAX_ACCEPTANCE_DAYS = 90
ADVISORY_RE = re.compile(r"^RUSTSEC-\d{4}-\d{3,}$")
EXPIRY_RE = re.compile(r"^expires=(\d{4}-\d{2}-\d{2})$")


def fail(message: str) -> None:
    print(f"FAIL: {message}", file=sys.stderr)
    global failed
    failed = True


failed = False
today = dt.date.today()

# 1. Parse deny.toml's [advisories].ignore -- entries are either a bare
#    "RUSTSEC-..." string or an { id = "RUSTSEC-...", reason = "..." } table.
with open(deny_path, "rb") as fh:
    deny_doc = tomllib.load(fh)
raw_ignore = deny_doc.get("advisories", {}).get("ignore", [])
ignored_ids: set[str] = set()
for entry in raw_ignore:
    if isinstance(entry, str):
        ignored_ids.add(entry)
    elif isinstance(entry, dict) and isinstance(entry.get("id"), str):
        ignored_ids.add(entry["id"])
    else:
        fail(f"{deny_path} has an [advisories].ignore entry in an unrecognized shape: {entry!r}")

# 2. Parse the ledger: "RUSTSEC-YYYY-NNNN expires=YYYY-MM-DD # justification".
ledger: dict[str, dt.date] = {}
try:
    lines = open(allow_path, encoding="utf-8").read().splitlines()
except FileNotFoundError:
    lines = []

for number, raw_line in enumerate(lines, 1):
    stripped = raw_line.strip()
    if not stripped or stripped.startswith("#"):
        continue
    declaration, separator, justification = stripped.partition("#")
    fields = declaration.split()
    if not separator or len(fields) != 2 or len(justification.strip()) < 12:
        fail(f"{allow_path} line {number} is not a justified 'RUSTSEC-ID expires=YYYY-MM-DD # reason (>=12 chars)' entry")
        continue
    advisory_id, expiry_field = fields
    expiry_match = EXPIRY_RE.fullmatch(expiry_field)
    if not ADVISORY_RE.fullmatch(advisory_id) or expiry_match is None:
        fail(f"{allow_path} line {number} is invalid: {stripped!r}")
        continue
    try:
        expires = dt.date.fromisoformat(expiry_match.group(1))
    except ValueError:
        fail(f"{allow_path} line {number} has an invalid date")
        continue
    if expires < today:
        fail(f"{allow_path} line {number} ({advisory_id}) expired on {expires.isoformat()} -- remove the acceptance or renew it")
        continue
    if expires > today + dt.timedelta(days=MAX_ACCEPTANCE_DAYS):
        fail(f"{allow_path} line {number} ({advisory_id}) exceeds the {MAX_ACCEPTANCE_DAYS}-day review horizon")
        continue
    if advisory_id in ledger:
        fail(f"{allow_path} line {number} duplicates an existing entry for {advisory_id}")
        continue
    ledger[advisory_id] = expires

# 3. Cross-check: every deny.toml ignore needs a live ledger justification, and
#    every ledger entry needs a live deny.toml ignore (no stale paperwork).
for advisory_id in sorted(ignored_ids - ledger.keys()):
    fail(f"{deny_path} ignores {advisory_id} with no justified, unexpired {allow_path} entry")
for advisory_id in sorted(ledger.keys() - ignored_ids):
    fail(f"{allow_path} accepts {advisory_id} but {deny_path} does not ignore it (stale acceptance)")

if failed:
    print(f"audit: FAILED - {deny_path}/{allow_path} advisory allowlist is inconsistent", file=sys.stderr)
    sys.exit(1)
print(f"audit: allowlist ledger consistent ({len(ledger)} accepted advisories)")
PYEOF

# ── The actual advisory gate: whatever deny.toml permits, cargo-deny enforces.
cargo deny check advisories
