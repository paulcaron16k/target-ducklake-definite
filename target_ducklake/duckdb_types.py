"""DuckDB column types as trees: scalars, STRUCTs and lists (DUCKLKD-108).

``parse_type`` reads the type strings the column DDL is built from and those
``PRAGMA table_info`` reports, such as
``STRUCT(a INTEGER, "at" TIMESTAMP WITH TIME ZONE)[]``, so nested columns can
be staged field by field. ``render_type`` writes a tree back as DDL, with
every nested name quoted (``at`` is a reserved word).
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import NoReturn, Union

__all__ = [
    "DuckDBList",
    "DuckDBScalar",
    "DuckDBStruct",
    "DuckDBTypeTree",
    "is_nested",
    "parse_type",
    "quote_identifier",
    "render_type",
]


@dataclass(frozen=True)
class DuckDBScalar:
    """A type with no nested parts, as written: ``DECIMAL(10,2)``, ``JSON``."""

    name: str


@dataclass(frozen=True)
class DuckDBStruct:
    """``STRUCT(name type, ...)``: its fields in declared order."""

    fields: tuple[tuple[str, DuckDBTypeTree], ...]


@dataclass(frozen=True)
class DuckDBList:
    """``T[]``: a list of the element type (a fixed-size ``T[n]`` too)."""

    element: DuckDBTypeTree


DuckDBTypeTree = Union[DuckDBScalar, DuckDBStruct, DuckDBList]  # noqa: UP007

# Types whose parentheses hold types, not sizes. MAP and UNION aren't staged
# field by field; they parse as scalars, as before.
_STRUCT = "STRUCT"
_SUFFIX = re.compile(r"\[\s*\d*\s*\]")


def quote_identifier(name: str) -> str:
    """The name as a quoted DuckDB identifier (``"at"``), a quote doubled."""
    return '"' + name.replace('"', '""') + '"'


def is_nested(tree: DuckDBTypeTree) -> bool:
    """Whether the type is a STRUCT or a list."""
    return not isinstance(tree, DuckDBScalar)


def render_type(tree: DuckDBTypeTree) -> str:
    """The type as DDL, every struct field name quoted."""
    if isinstance(tree, DuckDBStruct):
        fields = ", ".join(
            f"{quote_identifier(name)} {render_type(field)}"
            for name, field in tree.fields
        )
        return f"STRUCT({fields})"
    if isinstance(tree, DuckDBList):
        return f"{render_type(tree.element)}[]"
    return tree.name


class _Parser:
    """A recursive-descent reader of one DuckDB type string."""

    def __init__(self, text: str) -> None:
        self.text = text
        self.pos = 0

    def fail(self, what: str) -> NoReturn:
        """Raise a ValueError naming the text, what was wrong and where."""
        msg = f"Can't read DuckDB type {self.text!r}: {what} at position {self.pos}."
        raise ValueError(msg)

    def skip_spaces(self) -> None:
        while self.pos < len(self.text) and self.text[self.pos].isspace():
            self.pos += 1

    def peek(self) -> str:
        self.skip_spaces()
        return self.text[self.pos] if self.pos < len(self.text) else ""

    def expect(self, char: str) -> None:
        if self.peek() != char:
            self.fail(f"expected {char!r}")
        self.pos += 1

    def parse_type(self) -> DuckDBTypeTree:
        """A type, then any number of ``[]`` suffixes."""
        self.skip_spaces()
        if self.text[self.pos : self.pos + len(_STRUCT)].upper() == _STRUCT and (
            self.text[self.pos + len(_STRUCT) :].lstrip().startswith("(")
        ):
            self.pos += len(_STRUCT)
            tree: DuckDBTypeTree = self.parse_struct_fields()
        else:
            tree = DuckDBScalar(self.parse_scalar())
        while True:
            self.skip_spaces()
            suffix = _SUFFIX.match(self.text, self.pos)
            if suffix is None:
                return tree
            self.pos = suffix.end()
            tree = DuckDBList(tree)

    def parse_struct_fields(self) -> DuckDBStruct:
        self.expect("(")
        fields: list[tuple[str, DuckDBTypeTree]] = []
        names: set[str] = set()
        while True:
            name = self.parse_identifier()
            if name.lower() in names:
                self.fail(f"duplicate struct field {name!r}")
            names.add(name.lower())
            fields.append((name, self.parse_type()))
            if self.peek() == ",":
                self.pos += 1
                continue
            self.expect(")")
            return DuckDBStruct(tuple(fields))

    def parse_identifier(self) -> str:
        self.skip_spaces()
        if self.peek() == '"':
            self.pos += 1
            name = []
            while True:
                end = self.text.find('"', self.pos)
                if end < 0:
                    self.fail("unterminated quoted name")
                name.append(self.text[self.pos : end])
                self.pos = end + 1
                if self.text[self.pos : self.pos + 1] != '"':
                    return "".join(name)
                name.append('"')  # a doubled quote stands for one
                self.pos += 1
        match = re.compile(r"[^\s(),\[\]\"]+").match(self.text, self.pos)
        if match is None:
            self.fail("expected a field name")
        self.pos = match.end()
        return match.group(0)

    def parse_scalar(self) -> str:
        """Words and parenthesised arguments up to a top-level ``,`` ``)`` or ``[``.

        ``TIMESTAMP WITH TIME ZONE``, ``DECIMAL(10,2)``, ``MAP(VARCHAR, INT)``.
        """
        start, depth = self.pos, 0
        while self.pos < len(self.text):
            char = self.text[self.pos]
            if char == "(":
                depth += 1
            elif char == ")":
                if depth == 0:
                    break
                depth -= 1
            elif char in ",[" and depth == 0:
                break
            self.pos += 1
        name = " ".join(self.text[start : self.pos].split())
        if not name or depth:
            self.fail("expected a type")
        return name


def parse_type(text: str) -> DuckDBTypeTree:
    """Parse a DuckDB type string into a tree.

    Struct field names may be quoted (``"at"``, a quote in one doubled) or bare, as
    ``PRAGMA table_info`` writes them; nesting may go to any depth. A type
    that isn't a STRUCT or a list, such as ``DECIMAL(10,2)`` or
    ``MAP(VARCHAR, INTEGER)``, is a ``DuckDBScalar`` holding its text with
    spaces normalised.

    Raises:
        ValueError: If the text isn't a type (unbalanced parentheses, a
            struct without fields, a duplicate field name).
    """
    parser = _Parser(text)
    tree = parser.parse_type()
    if parser.peek():
        parser.fail("unexpected text")
    return tree
