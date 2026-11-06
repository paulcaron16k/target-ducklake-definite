"""Column names conformed as the singer-sdk's SQL targets conform them.

With ``conform_column_names`` on, each column name is rewritten by the rules
of the singer-sdk's ``SQLSink.conform_name``, which target-iceberg follows
too: characters other than ASCII letters, digits, ``_``, ``-``, ``.`` and
whitespace are dropped, the name is lowercased and stripped, ``.``, ``-`` and
spaces become ``_``, and a leading digit becomes a letter (0 to a, 1 to b, and
so on). The same tap then gives the same column names in both targets, and
they can be queried without quotes (reserved words such as ``order`` still
need them).

Two of those rules lose information: dropped characters and a replaced
leading digit. A name changed that way is reported. Two names that conform to
the same column are refused, as the singer-sdk refuses them.
"""

from __future__ import annotations

import re
import string
from collections import defaultdict
from typing import TYPE_CHECKING

from singer_sdk.exceptions import ConformedNameClashException

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

__all__ = ["conform_name", "conform_names", "describe_renames", "fold_case", "is_lossy"]

# What conform_name keeps; anything else is dropped.
_DROPPED = re.compile(r"[^a-zA-Z0-9_\-\.\s]")
_SEPARATORS = re.compile(r"[.\- ]")
_ASCII_LOWER = str.maketrans(string.ascii_uppercase, string.ascii_lowercase)


def fold_case(name: str) -> str:
    """``name`` as DuckDB compares identifiers: ASCII letters lowercased only.

    DuckDB matches ``Id`` and ``id`` as one column, but ``É`` and ``é`` (or
    ``ß`` and ``SS``) as two, unlike Python's ``str.lower``.
    """
    return name.translate(_ASCII_LOWER)


def conform_name(name: str) -> str:
    """``name`` as the singer-sdk's ``SQLSink.conform_name`` conforms it.

    Unlike the singer-sdk's, it returns ``""`` for a name with nothing left
    to conform, rather than failing with an IndexError; ``conform_names``
    refuses such a name.
    """
    conformed = _SEPARATORS.sub("_", _DROPPED.sub("", name).lower().strip())
    if conformed[:1].isdigit():
        conformed = string.ascii_lowercase[int(conformed[0])] + conformed[1:]
    return conformed


def describe_renames(renames: Mapping[str, str]) -> str:
    """Old to new names for a message: ``'Order ID' → 'order_id', ...``."""
    return ", ".join(f"{old!r} → {new!r}" for old, new in renames.items())


def is_lossy(name: str) -> bool:
    """Whether conforming ``name`` drops characters or replaces a leading digit.

    Lowercasing and separators made underscores are not counted: the name
    still reads the same.
    """
    return bool(_DROPPED.search(name)) or name.strip()[:1].isdigit()


def conform_names(names: Iterable[str]) -> dict[str, str]:
    """Each name mapped to its conformed name, for the names that change.

    Raises:
        ConformedNameClashException: If two names conform to the same name.
        ValueError: If a name conforms to the empty name.
    """
    conformed = {name: conform_name(name) for name in names}
    if empty := [name for name, new in conformed.items() if not new]:
        msg = (
            f"Column name(s) {empty} have no ASCII letter, digit or separator, "
            "so they conform to an empty name. Rename them in the tap (or with a "
            "stream map), or turn conform_column_names off."
        )
        raise ValueError(msg)
    by_conformed: defaultdict[str, list[str]] = defaultdict(list)
    for name, new in conformed.items():
        by_conformed[new].append(name)
    if clashes := {new: old for new, old in by_conformed.items() if len(old) > 1}:
        msg = (
            "Column names that conform to the same name: "
            + "; ".join(f"{old} → {new!r}" for new, old in sorted(clashes.items()))
            + ". Rename one in the tap (or with a stream map), or turn "
            "conform_column_names off."
        )
        raise ConformedNameClashException(msg)
    return {name: new for name, new in conformed.items() if new != name}
