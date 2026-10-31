"""File-level `crate::` reference graph of the engine sources (MEDULLA-CONTRACT-R056).

Every reference is resolved to the module file that defines (or re-exports from) the
referenced path. Re-exports are followed so `crate::server::PersistenceBackend` lands on
the file that defines the trait. The result feeds `edges.py` and `resolve.py`.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

LEAF = "<leaf>"
BLANK = re.compile(
    r"//[^\n]*|/\*.*?\*/|r(#*)\".*?\"\1|\"(?:\\.|[^\"\\])*\"|'(?:\\.|[^'\\])'"
    r"|pub\(in [^)]*\)",
    re.S,
)
USE_START = re.compile(r"(?m)^([ \t]*)(?:pub(?:\([^)]*\))?\s+)?use\s+")
TOKEN = re.compile(r"::|[{},*]|[A-Za-z_]\w*")
INLINE = re.compile(r"(?<![$\w:])crate((?:::[A-Za-z_]\w*)+)")
TEST_MOD = re.compile(
    r"(?m)^#\[cfg\(test\)\]\s*\n\s*(?:pub(?:\([^)]*\))?\s+)?mod\s+\w+\s*\{"
)
Leaf = tuple[tuple[str, ...], int, str | None]


@dataclass(frozen=True)
class Ref:
    src: str
    dst: str
    item: str
    line: int
    in_test: bool


def blank_non_code(text: str) -> str:
    """Replace comments, literals and `pub(in ...)` with spaces, keeping newlines."""

    def blank(m: re.Match[str]) -> str:
        return re.sub(r"[^\n]", " ", m.group(0))

    return BLANK.sub(blank, text)


def parse_use_tree(body: str) -> list[Leaf]:
    """Expand a use tree into (segments, line offset, alias) leaves."""
    tokens = [
        (m.group(0), body.count("\n", 0, m.start())) for m in TOKEN.finditer(body)
    ]
    leaves: list[Leaf] = []
    _parse_items(tokens, 0, (), leaves)
    return leaves


def _parse_items(
    tokens: list[tuple[str, int]], i: int, prefix: tuple[str, ...], leaves: list[Leaf]
) -> int:
    segs = list(prefix)
    line = 0
    alias = None
    while i < len(tokens):
        tok, line = tokens[i]
        if tok in (",", "}"):
            break
        if tok == "{":
            return _parse_group(tokens, i + 1, tuple(segs), leaves)
        if tok == "as":
            alias = tokens[i + 1][0] if i + 1 < len(tokens) else None
            i += 2
            continue
        if tok not in ("::", "self"):
            segs.append(tok)
        i += 1
    if segs:
        leaves.append((tuple(segs), line, alias))
    return i


def _parse_group(
    tokens: list[tuple[str, int]], i: int, prefix: tuple[str, ...], leaves: list[Leaf]
) -> int:
    while i < len(tokens) and tokens[i][0] != "}":
        i = _parse_items(tokens, i, prefix, leaves)
        if i < len(tokens) and tokens[i][0] == ",":
            i += 1
    return i + 1


def find_use_statements(code: str) -> list[tuple[int, int, str, str]]:
    """(start, end, indent, body) for every `use ...;` statement."""
    found = []
    for m in USE_START.finditer(code):
        end = code.find(";", m.end())
        if end != -1:
            found.append((m.start(), end + 1, m.group(1), code[m.end() : end]))
    return found


def module_path(rel: str) -> list[str] | None:
    """Module path of a lib-crate source file, None for bin roots."""
    if rel == "src/main.rs" or rel.startswith("src/bin/"):
        return None
    parts = rel[len("src/") : -len(".rs")].split("/")
    if parts[-1] in ("mod", "lib"):
        parts.pop()
    return parts


def absolutize(
    segs: tuple[str, ...], modpath: list[str] | None
) -> tuple[str, ...] | None:
    """Turn crate::/self::/super:: leading forms into crate-relative segments."""
    if not segs:
        return None
    head, rest = segs[0], list(segs[1:])
    if head == "crate":
        return tuple(rest)
    if modpath is None or head not in ("self", "super"):
        return None
    base = list(modpath)
    if head == "super":
        while rest and rest[0] == "super":
            rest.pop(0)
            base = base[:-1]
        base = base[:-1]
    return tuple(base + rest)


def scan_file(rel: str, text: str) -> list[tuple[tuple[str, ...], int]]:
    """All crate-relative referenced paths in a file with their 1-based line."""
    code = blank_non_code(text)
    modpath = module_path(rel)
    refs: list[tuple[tuple[str, ...], int]] = []
    masked = list(code)
    for start, end, indent, body in find_use_statements(code):
        base = code.count("\n", 0, start) + 1
        for segs, off, _alias in parse_use_tree(body):
            if indent and segs[:1] in (("self",), ("super",)):
                continue
            absolute = absolutize(segs, modpath)
            if absolute:
                refs.append((absolute, base + off))
        for k in range(start, end):
            masked[k] = " " if masked[k] != "\n" else "\n"
    rest = "".join(masked)
    for m in INLINE.finditer(rest):
        segs = tuple(m.group(1).split("::")[1:])
        refs.append((segs, rest.count("\n", 0, m.start()) + 1))
    return refs


def test_region_start(text: str) -> int:
    """1-based first line of an inline `#[cfg(test)] mod x {`, or a huge number."""
    m = TEST_MOD.search(blank_non_code(text))
    return text.count("\n", 0, m.start()) + 1 if m else 1 << 30


class Index:
    """Source texts, module files and re-export tables for one checkout."""

    def __init__(self, root: Path, files: list[str]):
        self.root = root
        self.files = set(files)
        self.text = {
            f: (root / f).read_text(encoding="utf-8", errors="replace") for f in files
        }
        lib = blank_non_code(self.text.get("src/lib.rs", ""))
        self.lib_names = {
            leaf[2] or leaf[0][-1]
            for _s, _e, _i, body in find_use_statements(lib)
            for leaf in parse_use_tree(body)
        }
        self._reexports: dict[str, dict[str, tuple[str, ...]]] = {}
        self._globs: dict[str, list[tuple[str, ...]]] = {}
        self._defines: dict[tuple[str, str], bool] = {}

    def walk(self, segs: tuple[str, ...]) -> tuple[str | None, int]:
        """Deepest module file along `segs` and how many segments it consumed."""
        file, depth = None, 0
        for k in range(1, len(segs) + 1):
            base = "src/" + "/".join(segs[:k])
            if base + ".rs" in self.files:
                file = base + ".rs"
            elif base + "/mod.rs" in self.files:
                file = base + "/mod.rs"
            else:
                break
            depth = k
        return file, depth

    def _load_uses(self, file: str) -> None:
        names: dict[str, tuple[str, ...]] = {}
        globs: list[tuple[str, ...]] = []
        modpath = module_path(file)
        for _s, _e, indent, body in find_use_statements(
            blank_non_code(self.text[file])
        ):
            if indent:
                continue
            for segs, _line, alias in parse_use_tree(body):
                full = self._to_absolute(segs, modpath)
                if full is None:
                    continue
                if segs[-1] == "*":
                    globs.append(full[:-1])
                else:
                    names[alias or segs[-1]] = full
        self._reexports[file], self._globs[file] = names, globs

    def _to_absolute(
        self, segs: tuple[str, ...], modpath: list[str] | None
    ) -> tuple[str, ...] | None:
        full = absolutize(segs, modpath)
        if full is not None:
            return full
        if (
            modpath is not None
            and self.walk(tuple(modpath) + segs[:1])[1] == len(modpath) + 1
        ):
            return tuple(modpath) + segs
        return None

    def defines(self, file: str, name: str) -> bool:
        key = (file, name)
        if key not in self._defines:
            kinds = r"(?:fn|struct|enum|trait|type|const|static|macro_rules!|mod)"
            pat = rf"\b{kinds}\s+{re.escape(name)}\b"
            self._defines[key] = re.search(pat, self.text[file]) is not None
        return self._defines[key]

    def locate(
        self, segs: tuple[str, ...], depth: int = 0
    ) -> tuple[str, tuple[str, ...]]:
        """Resolve `segs` to (defining module file or LEAF, remaining item path)."""
        file, used = self.walk(segs)
        if file is None:
            first = segs[0] if segs else ""
            return ("src/lib.rs", segs) if first not in self.lib_names else (LEAF, segs)
        rest = segs[used:]
        if rest and depth < 8:
            target = self._follow(file, rest[0])
            if target is not None:
                return self.locate(target + rest[1:], depth + 1)
        return file, rest

    def _follow(self, file: str, name: str) -> tuple[str, ...] | None:
        if file not in self._reexports:
            self._load_uses(file)
        if self.defines(file, name) and name not in self._reexports[file]:
            return None
        if name in self._reexports[file]:
            return self._reexports[file][name]
        for glob in self._globs[file]:
            gfile, _ = self.walk(glob)
            if gfile and gfile != file and self.defines(gfile, name):
                return glob + (name,)
        return None


def collect_refs(root: Path, files: list[str]) -> list[Ref]:
    """Every resolved crate reference between engine source files."""
    index = Index(root, files)
    out: list[Ref] = []
    cache: dict[tuple[str, ...], str] = {}
    for rel in files:
        start = test_region_start(index.text[rel])
        for segs, line in scan_file(rel, index.text[rel]):
            dst = cache.get(segs) or cache.setdefault(segs, index.locate(segs)[0])
            if dst == LEAF or dst == rel:
                continue
            canon = "::".join(("crate",) + segs)
            out.append(Ref(rel, dst, canon, line, line >= start))
    return out
