# EG-T4-GREEN — Design and implementation plan

Status: IN REVIEW. Governing [spec](spec.md).

## Existing system and reuse

Reuse EG's `eg_types::contract` error classification, method error declarations, `Response`, feature-gated crates, and current release workflow. The PR introduces `classify_refusal` at the existing contract owner and `Response::refusal_text` at the response owner. It restores only Raft shard methods with live callers; do not add another error registry or a second compatibility path.

## Architecture and live path

Client request → envelope verification → declared method handler → response classification → serialized protocol response → client. The code prefix must be selected at the producer from that method's published error set; unrecognized free text must not escape as a fabricated public code. The response text helper preserves detail when an internal handler must convert a response to text. Verification must redact node identifiers in its refusal.

Train 4 generated codec, Python wheel, Go/JavaScript clients and served consumer proof use the same public method contract. Static source repair is one input; EH-592 packaging and downstream clients, EH-655 duplication and EH-656 hooks must be checked at the exact candidate revision.

## Data and interface impact

The wire response keeps its declared `error` field and human detail; tests must assert both. Registry census changes to 58 graph-shard tables and 167 full registry entries reflect four `repository_enrichment_*` tables; `candidate.redb` is an upgrade candidate. Single-use attempt nonces require a fresh nonce on replay. These are test expectations tied to actual owners, not independent authorities.

## Quality and release gates

Run `cargo fmt --check`, the feature-set Cargo checks/tests in CI, Python client tests, Go/JavaScript client tests, `scripts/check_dispatch_decomposition.py`, and the repository's CCCC, KISS, dupehound, jscpd and release workflow gates. Record exact commit and check URL. Do not suppress scanner findings to make a green badge. Review all required and advisory failures against `plans/refactor/QUALITY.md`.

## Open decision

TxnUql and TxnUnifiedQuery omit `CONFLICT` from their declared contracts. Their lifecycle refusals can still become `INTERNAL`; accepting a wider contract requires a separate reviewed decision and test update.
