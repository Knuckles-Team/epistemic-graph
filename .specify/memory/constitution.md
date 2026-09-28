# epistemic-graph constitution

Status: PROPOSED. Drafted: 2026-09-28. Version: 0.1.0. Specification index: [specs/README.md](../../specs/README.md).

## I. Single owner and reusable architecture

This repository owns durable graph/ontology storage, governed ingestion, query and compute, and public protocols. A design must inventory existing public components and callers before adding code, name one legal owner, preserve dependency direction, and reuse the existing live wiring. Cross-repository capabilities cite the owner spec by stable ID; no second authority or dormant parallel implementation is accepted.

## II. Complete specification before construction

A contribution records behavior and acceptance in `spec.md`, architecture/data/security/failure design in `plan.md`, exact positive and negative proof in `test-spec.md`, and sequenced work in `tasks.md`. Unresolved choices are explicit. The checked-in spec, not a local plan draft or generated report, is the contributor-facing build contract.

## III. Observable integration and evidence

Acceptance follows a real entrypoint through the intended owner and effect boundary. Tests cover refusal, authorization, idempotency and recovery where applicable. Evidence names the exact revision, command, environment and result; source-only success never claims served or release acceptance.

## IV. Quality and release discipline

Apply this repository's CCCC, jscpd, dupehound, KISS, language-native, contract, package and release gates as relevant. Fix real complexity and duplication at the owner; do not suppress findings or split logic merely to satisfy a scanner. Update public docs and delete superseded paths after consumers move.

## V. Amendment and status

Amend this constitution in a reviewed change that states the reason and impact. Mark a spec `LANDED` only when its source is merged at an exact owning-repository revision. Mark it `ACCEPTED` only after the required consumer, runtime, quality and release receipts meet the checked-in spec's acceptance gates. Record both states separately.
