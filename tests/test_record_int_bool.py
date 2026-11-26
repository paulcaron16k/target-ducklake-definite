"""Integer and boolean values on RECORD messages (DUCKLKD-162, DUCKLKD-163).

Integer text follows PostgreSQL's integer input, so a value that lands in
PostgreSQL lands the same here; booleans follow one rule shared with
map-conform: a word in any case, or a number, true when greater than 0.
BATCH files are cast by DuckDB and are not covered here.
"""

from __future__ import annotations

import io
import json
from decimal import Decimal
from typing import TYPE_CHECKING, Any

import pytest

from target_ducklake.record_lines import LineColumn, RecordLines, value_rules
from target_ducklake.target import Targetducklake

if TYPE_CHECKING:
    from pathlib import Path

    from tests.catalogs import Lake

# Text PostgreSQL 17 reads as a bigint, and the value (probed against it).
INTEGER_TEXT = [
    ("7", 7),
    (" -7 ", -7),
    ("+7", 7),
    ("\t+7\n", 7),
    ("07", 7),
    ("-0", 0),
    ("1_000", 1000),
    ("0_7", 7),
    ("0x10", 16),
    ("0X1f", 31),
    ("0x_1F", 31),
    ("-0x10", -16),
    ("0b101", 5),
    ("0b_101", 5),
    ("0o17", 15),
    ("0o_17", 15),
    ("9223372036854775807", 2**63 - 1),
]
# Text PostgreSQL 17 refuses as a bigint.
NOT_INTEGER_TEXT = [
    "7.0",
    "7.5",
    "1e3",
    "",
    " ",
    "- 7",
    "+-7",
    "1_",
    "_10",
    "1__0",
    "0x",
    "0b",
    "0xG",
    "0x1F_",
    "00x10",
    "\u0663",  # an Arabic-Indic digit
    "\uff17",  # a fullwidth digit
    "\u00a07",  # a no-break space
    "abc",
]
BOOLEAN_VALUES: list[tuple[Any, bool]] = [
    *((word, True) for word in ("true", "TRUE", "True", "t", "T", "yes", "YES")),
    *((word, True) for word in ("y", "Y", "on", "ON", "1", " true ", "\ttrue\n")),
    *((word, False) for word in ("false", "FALSE", "f", "F", "no", "No", "n")),
    *((word, False) for word in ("N", "off", "OFF", "0", " false ")),
    ("2", True),
    ("-1", False),
    ("0.0", False),
    ("0.5", True),
    ("-0.5", False),
    ("1e3", True),
    (True, True),
    (False, False),
    (1, True),
    (2, True),
    (0, False),
    (-1, False),
    (Decimal("0.5"), True),
    (Decimal("-0.5"), False),
    (Decimal("0.0"), False),
    (0.5, True),
]
NOT_BOOLEAN_VALUES: list[Any] = [
    "",
    " ",
    "maybe",
    "tru",
    "2x",
    "nan",
    "inf",
    float("nan"),
    Decimal("NaN"),
    {"a": 1},
    [1],
]


def _written(tmp_path: Path, column_type: str, value: object) -> object:
    """What a record's value ``v`` becomes in its line, for a column of the type."""
    rules = value_rules([LineColumn(("v",), "v", column_type, container=False)])
    lines = RecordLines(str(tmp_path / "r.jsonl"), rules, stream="t")
    lines.write({"v": value})
    lines.close()
    return json.loads((tmp_path / "r.jsonl").read_text())["v"]


class TestIntegerText:
    """Integer text is read by PostgreSQL's rules, into every integer type."""

    @pytest.mark.parametrize(("text", "number"), INTEGER_TEXT)
    def test_postgresql_integer_text_is_its_number(
        self, tmp_path: Path, text: str, number: int
    ) -> None:
        """Each form PostgreSQL reads is written as its number."""
        assert _written(tmp_path, "BIGINT", text) == number

    @pytest.mark.parametrize("text", NOT_INTEGER_TEXT)
    def test_other_text_is_refused(self, tmp_path: Path, text: str) -> None:
        """Text PostgreSQL refuses fails the batch, naming row, column and value."""
        with pytest.raises(
            ValueError,
            match=r"stream 't', batch of records, row 1, column 'v': value .* can't "
            r"be loaded as BIGINT, so the batch wasn't loaded: integer text is",
        ):
            _written(tmp_path, "BIGINT", text)

    @pytest.mark.parametrize(
        "column_type", ["TINYINT", "SMALLINT", "INTEGER", "HUGEINT", "UBIGINT"]
    )
    def test_every_integer_type(self, tmp_path: Path, column_type: str) -> None:
        """Every integer type follows the rule; the cast checks the range."""
        assert _written(tmp_path, column_type, "0x10") == INTEGER_TEXT[8][1]
        with pytest.raises(ValueError, match=f"can't be loaded as {column_type}"):
            _written(tmp_path, column_type, "7.0")

    @pytest.mark.parametrize(
        ("value", "line"),
        [(7, 7), (Decimal("7.5"), "7.5"), (True, True), (None, None)],
    )
    def test_values_other_than_text_are_as_before(
        self, tmp_path: Path, value: object, line: object
    ) -> None:
        """A number with a fraction is its exact text, cast and rounded as before."""
        assert _written(tmp_path, "BIGINT", value) == line


class TestBooleans:
    """A word in any case, or a number: true when greater than 0."""

    @pytest.mark.parametrize(("value", "boolean"), BOOLEAN_VALUES)
    def test_values(self, tmp_path: Path, value: object, boolean: bool) -> None:  # noqa: FBT001
        """Each accepted value is written as its JSON boolean."""
        assert _written(tmp_path, "BOOLEAN", value) is boolean

    @pytest.mark.parametrize("value", NOT_BOOLEAN_VALUES)
    def test_other_values_are_refused(self, tmp_path: Path, value: object) -> None:
        """Anything else fails the batch, naming the column and value."""
        with pytest.raises(
            ValueError,
            match=r"column 'v': value .* can't be loaded as BOOLEAN, so the batch "
            r"wasn't loaded: a boolean is true",
        ):
            _written(tmp_path, "BOOLEAN", value)

    def test_null_stays_null(self, tmp_path: Path) -> None:
        """A null is written as null."""
        assert _written(tmp_path, "BOOLEAN", None) is None


def _load(lake: Lake, properties: dict, records: list[dict], **config: object) -> None:
    messages: list[dict[str, Any]] = [
        {
            "type": "SCHEMA",
            "stream": "t",
            "schema": {"type": "object", "properties": properties},
            "key_properties": ["id"],
        },
        *({"type": "RECORD", "stream": "t", "record": r} for r in records),
    ]
    target = Targetducklake(config={**lake.config(), **config})
    try:
        target.listen(
            file_input=io.StringIO("".join(json.dumps(m) + "\n" for m in messages))
        )
    finally:
        target.target_connector.close()


ID = {"id": {"type": "integer"}}
INTEGER = {"type": ["null", "integer"]}
BOOLEAN = {"type": ["null", "boolean"]}


class TestLoaded:
    """Into the lake, with validation off (the default)."""

    def test_integer_and_boolean_columns(self, lake: Lake) -> None:
        """Both rules, loaded into the lake."""
        _load(
            lake,
            {**ID, "i": INTEGER, "b": BOOLEAN},
            [
                {"id": 1, "i": "0o17", "b": "YES"},
                {"id": 2, "i": " 1_000 ", "b": -1},
                {"id": 3, "i": "-0x10", "b": "2"},
                {"id": 4, "i": None, "b": "Off"},
            ],
        )
        assert lake.query("SELECT id, i, b FROM lake.s.t ORDER BY id") == [
            (1, 15, True),
            (2, 1000, False),
            (3, -16, True),
            (4, None, False),
        ]

    def test_a_key_column(self, lake: Lake) -> None:
        """Key text is the number, so two spellings of one key are one row."""
        _load(lake, ID | {"v": INTEGER}, [{"id": "0x10", "v": 1}, {"id": "16", "v": 2}])
        assert lake.query("SELECT id, v FROM lake.s.t") == [(16, 2)]

    def test_a_flattened_column(self, lake: Lake) -> None:
        """A flattened column follows its type's rule."""
        properties = {
            **ID,
            "o": {
                "type": "object",
                "properties": {"n": INTEGER, "b": BOOLEAN},
            },
        }
        _load(
            lake,
            properties,
            [{"id": 1, "o": {"n": "0b11", "b": "t"}}],
            flatten_max_level=1,
        )
        assert lake.query("SELECT o__n, o__b FROM lake.s.t") == [(3, True)]

    @pytest.mark.parametrize("checking", ["permissive", "strict"])
    def test_fraction_text_fails_the_batch(self, lake: Lake, checking: str) -> None:
        """Whatever value_type_checking is, as PostgreSQL refuses it."""
        with pytest.raises(ValueError, match=r"value '7\.0' can't be loaded as BIGINT"):
            _load(
                lake,
                {**ID, "i": INTEGER},
                [{"id": 1, "i": 5}, {"id": 2, "i": "7.0"}],
                value_type_checking=checking,
            )

    def test_an_out_of_range_integer_fails(self, lake: Lake) -> None:
        """2**63 is BIGINT's range plus one: the cast refuses it."""
        with pytest.raises(ValueError, match="column 'i'"):
            _load(lake, {**ID, "i": INTEGER}, [{"id": 1, "i": "0x8000000000000000"}])
