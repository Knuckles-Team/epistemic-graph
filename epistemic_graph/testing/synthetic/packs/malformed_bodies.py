"""Body defects: encoding, JSON shape, SKILL.md front matter, tool schemas."""

from __future__ import annotations

from dataclasses import dataclass

from .._digest import canonical_json
from .malformed import MalformedPack, Mutation, case, edited, first
from .spec import PackSpec

_BODY = ("MALFORMED_BODY",)


@dataclass(frozen=True)
class BodyCase:
    """Replace one field of the first entry of ``kind`` and expect ``codes``."""

    rule: str
    variant: str
    description: str
    kind: str
    field: str
    value: bytes | None
    codes: tuple[str, ...] = _BODY
    permitted: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()
    accepted: bool = False

    def __call__(self, spec: PackSpec) -> MalformedPack:
        value = self.value
        if value is not None:
            # `@NAME@` stands for the edited entry's own name (front matter cases).
            value = value.replace(b"@NAME@", first(spec, self.kind).name.encode())
        pack = edited(spec, self.kind, **{self.field: value})
        return case(
            self.rule,
            self.variant,
            self.description,
            pack,
            self.codes,
            permitted=self.permitted,
            warnings=self.warnings,
            accepted=self.accepted,
        )


def _nested(depth: int) -> bytes:
    return b'{"a":' * depth + b"1" + b"}" * depth


def _skill(front_matter: str) -> bytes:
    return f"---\n{front_matter}---\n# body\n".encode()


def _descriptor(description: str) -> bytes:
    return canonical_json(
        {"description": description, "name": "tool_000", "title": "tool_000"}
    )


BODY_MUTATIONS: tuple[Mutation, ...] = (
    BodyCase(
        "G9",
        "yaml_alias",
        "front matter using a YAML anchor and alias",
        "skill",
        "body",
        _skill("name: &n @NAME@\ndescription: *n\n"),
    ),
    BodyCase(
        "G9",
        "front_matter_over_16kib",
        "front matter over 16 KiB",
        "skill",
        "body",
        _skill("name: @NAME@\ndescription: " + "d" * 16_400 + "\n"),
    ),
    BodyCase(
        "G9",
        "name_mismatch",
        "front matter name differs from the entry name",
        "skill",
        "body",
        _skill("name: another-skill\ndescription: synthetic\n"),
    ),
    BodyCase(
        "G9",
        "missing_front_matter",
        "a SKILL.md with no front matter",
        "skill",
        "body",
        b"# no front matter\n",
    ),
    BodyCase(
        "G7",
        "utf8_bom",
        "a body starting with a byte-order mark",
        "prompt",
        "body",
        b"\xef\xbb\xbf" + canonical_json({"messages": []}),
    ),
    BodyCase(
        "G7",
        "invalid_utf8",
        "a body that is not UTF-8",
        "prompt",
        "body",
        b'{"messages":"\xff"}',
    ),
    BodyCase(
        "G8", "json_depth_65", "JSON nested 65 levels", "prompt", "body", _nested(65)
    ),
    BodyCase(
        "G8",
        "duplicate_keys",
        "a JSON object with a repeated key",
        "prompt",
        "body",
        b'{"messages":[],"messages":[]}',
    ),
    BodyCase(
        "G8",
        "nan_literal",
        "a JSON body containing NaN",
        "prompt",
        "body",
        b'{"messages":NaN}',
    ),
    BodyCase(
        "G8",
        "array_input_schema",
        "a tool input schema of type array",
        "tool",
        "input_schema",
        canonical_json({"items": {"type": "string"}, "type": "array"}),
    ),
    BodyCase(
        "G10",
        "missing_input_schema",
        "a tool with no input schema section",
        "tool",
        "input_schema",
        None,
        codes=("MISSING_TOOL_SCHEMA",),
    ),
    BodyCase(
        "G11",
        "empty_description",
        "a tool with an empty description",
        "tool",
        "body",
        _descriptor(""),
        codes=(),
        warnings=("EMPTY_DESCRIPTION",),
        accepted=True,
    ),
    BodyCase(
        "G18",
        "control_character_summary",
        "a description whose summary fails draft validation",
        "tool",
        "body",
        _descriptor("bell \x07 inside"),
        codes=("INVALID_COMPONENT",),
        permitted=_BODY,
    ),
)
