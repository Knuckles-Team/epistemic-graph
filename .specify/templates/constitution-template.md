# [Repository] constitution

This repository inherits the [organization constitution](https://github.com/Knuckles-Team/pipelines/blob/main/.specify/memory/constitution.md) version 1.1.0. Every article of that constitution applies here. Copy this file to `.specify/memory/constitution.md` and keep it to one page. `.specify/templates/` is vended by pipelines; do not edit it.

**Inherits**: organization constitution v1.1.0 | **Version**: 1.0.0 | **Ratified**: [date] | **Last amended**: [date]

## Organization standards

- [Spec standard](https://github.com/Knuckles-Team/pipelines/blob/main/reference/spec-standard.md): one owning spec per capability, computed states, `Spec:` trailers.
- [Ecosystem boundaries](https://github.com/Knuckles-Team/pipelines/blob/main/reference/ecosystem-boundaries.md): which repository owns which requirement.
- [Wording standard](https://github.com/Knuckles-Team/pipelines/blob/main/reference/ste.md): sentences of 20 words or fewer, with `must`, `should`, or `may`.

## Repository articles

### R-I. [Title of a rule that only this repository adds]

[The rule in one or two sentences.]

**Enforced by:** [A gate hook ID, a standard section, or a bound test.]

**Plan check:** [One yes or no question that a `plan.md` Constitution Check answers.]

## Repository limits

State only limits that are stricter than the organization limits, or that the organization does not set. Name the configuration file that holds each value.

| Limit | Value | Source |
|---|---|---|
| [Limit] | [Value] | [`.config/file`] |

Amend this file in a reviewed change. State the impact in the pull request. When the organization constitution changes its version, update the **Inherits** line in the same change.
