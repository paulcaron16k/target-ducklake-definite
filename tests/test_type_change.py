"""A property declared a string, whose column the table has as another type.

DUCKLKD-137.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

import pytest

from tests.test_mid_run_schema import _records, _run, _schema

if TYPE_CHECKING:
    from tests.catalogs import Lake

STRING = {"type": ["null", "string"]}
# The type a property had in an earlier run, with a value of it, its column
# type and how the value reads back as text.
EARLIER: dict[str, tuple[dict, object, str, str]] = {
    "integer": ({"type": ["null", "integer"]}, 5, "BIGINT", "5"),
    "number": ({"type": ["null", "number"]}, 1.5, "DECIMAL(38,9)", "1.500000000"),
    "boolean": ({"type": ["null", "boolean"]}, True, "BOOLEAN", "true"),
    "date": (
        {"type": ["null", "string"], "format": "date"},
        "2024-01-02",
        "DATE",
        "2024-01-02",
    ),
    "json": ({"type": ["null", "object"]}, {"a": 1}, "JSON", '{"a": 1}'),
}
LOAD_METHODS = ["append", "merge", "overwrite"]


def _properties(value: dict, key: dict | None = None) -> dict:
    return {"id": key or {"type": "integer"}, "v": value}


def _load_earlier(lake: Lake, kind: str) -> None:
    """A table an earlier run loaded with ``v`` of ``kind``."""
    schema, value, _, _ = EARLIER[kind]
    _run(lake, [_schema(_properties(schema)), *_records([{"id": 7, "v": value}])])


def _rows(lake: Lake) -> list[tuple]:
    return lake.query("SELECT id, v::VARCHAR FROM lake.s.orders ORDER BY id")


def _refusal(column_type: str) -> str:
    return (
        rf"stream 'orders' declares string properties whose columns table orders "
        rf"has as another type: 'v' \({re.escape(column_type)}\)"
    )


class TestStringIntoTypedColumn:
    """Refused at setup, naming the column and both types; the table is untouched."""

    @pytest.mark.parametrize("load_method", LOAD_METHODS)
    @pytest.mark.parametrize("kind", list(EARLIER))
    def test_a_later_run_is_refused_before_any_record(
        self, lake: Lake, kind: str, load_method: str
    ):
        """At the SCHEMA message, so no record is staged, even for an overwrite."""
        _load_earlier(lake, kind)
        column_type, earlier = EARLIER[kind][2], EARLIER[kind][3]
        with pytest.raises(ValueError, match=_refusal(column_type)):
            _run(lake, [_schema(_properties(STRING))], load_method=load_method)
        assert _rows(lake) == [(7, earlier)]
        assert lake.query(
            "SELECT data_type FROM information_schema.columns "
            "WHERE table_name = 'orders' AND column_name = 'v'"
        ) == [(column_type,)]

    @pytest.mark.parametrize("load_method", LOAD_METHODS)
    def test_the_error_says_what_to_do(self, lake: Lake, load_method: str):
        """DuckLake's limit, and the three ways out."""
        _load_earlier(lake, "integer")
        with pytest.raises(ValueError, match=_refusal("BIGINT")) as refused:
            _run(
                lake,
                [_schema(_properties(STRING)), *_records([{"id": 1, "v": "x"}])],
                load_method=load_method,
            )
        message = str(refused.value)
        assert "can't change a column's type to VARCHAR in place" in message
        assert "Declare each property as its column's type" in message
        assert "load the stream into a new table" in message
        assert "recreate the column as VARCHAR" in message
        assert _rows(lake) == [(7, "5")]

    @pytest.mark.parametrize("load_method", LOAD_METHODS)
    def test_a_mid_run_schema_is_refused_when_it_arrives(
        self, lake: Lake, load_method: str
    ):
        """Refused at the new SCHEMA; the records before it load or are dropped.

        An append or a merge loads them; an overwrite replaces nothing, so the
        table keeps the earlier run's rows.
        """
        _load_earlier(lake, "integer")
        integer = EARLIER["integer"][0]
        with pytest.raises(ValueError, match=_refusal("BIGINT")):
            _run(
                lake,
                [
                    _schema(_properties(integer)),
                    *_records([{"id": 1, "v": 1}]),
                    _schema(_properties(STRING)),
                    *_records([{"id": 2, "v": "x"}]),
                ],
                load_method=load_method,
            )
        before_change = [] if load_method == "overwrite" else [(1, "1")]
        assert _rows(lake) == [*before_change, (7, "5")]

    @pytest.mark.parametrize("load_method", LOAD_METHODS)
    def test_a_key_declared_a_string_is_refused(self, lake: Lake, load_method: str):
        """A merge would compare the keys cast to the column's type and fail."""
        _load_earlier(lake, "integer")
        with pytest.raises(
            ValueError,
            match=r"declares string properties whose columns table orders has as "
            r"another type: 'id' \(BIGINT\)",
        ):
            _run(
                lake,
                [_schema(_properties(EARLIER["integer"][0], key=STRING))],
                load_method=load_method,
            )
        assert _rows(lake) == [(7, "5")]

    def test_every_refused_column_is_named(self, lake: Lake):
        """In one error, in the stream's order."""
        _run(
            lake,
            [
                _schema(
                    {
                        "id": {"type": "integer"},
                        "a": {"type": "integer"},
                        "b": {"type": "boolean"},
                    }
                ),
                *_records([{"id": 1, "a": 1, "b": True}]),
            ],
        )
        with pytest.raises(ValueError, match=r"'a' \(BIGINT\), 'b' \(BOOLEAN\)\."):
            _run(lake, [_schema({"id": {"type": "integer"}, "a": STRING, "b": STRING})])


class TestStillLoads:
    """Changes the table's column already holds, or that widen it in place."""

    @pytest.mark.parametrize("load_method", LOAD_METHODS)
    @pytest.mark.parametrize("kind", ["integer", "number", "boolean", "date"])
    def test_any_property_into_a_text_column(
        self, lake: Lake, kind: str, load_method: str
    ):
        """A VARCHAR column holds the text of any value."""
        _run(lake, [_schema(_properties(STRING)), *_records([{"id": 7, "v": "x"}])])
        schema, value, _, text = EARLIER[kind]
        _run(
            lake,
            [_schema(_properties(schema)), *_records([{"id": 1, "v": value}])],
            load_method=load_method,
        )
        earlier = [] if load_method == "overwrite" else [(7, "x")]
        assert _rows(lake) == [(1, text), *earlier]

    def test_a_key_declared_an_integer_merges_into_a_text_key_column(self, lake: Lake):
        """The staged key is cast to VARCHAR, so it matches the row it replaces."""
        _run(
            lake,
            [
                _schema(_properties(STRING, key=STRING)),
                *_records([{"id": "1", "v": "old"}, {"id": "2", "v": "kept"}]),
            ],
        )
        _run(
            lake,
            [
                _schema(_properties(STRING, key={"type": "integer"})),
                *_records([{"id": 1, "v": "new"}]),
            ],
            load_method="merge",
        )
        assert lake.query("SELECT id, v FROM lake.s.orders ORDER BY id") == [
            ("1", "new"),
            ("2", "kept"),
        ]

    @pytest.mark.parametrize("load_method", LOAD_METHODS)
    def test_an_integer_column_is_widened(self, lake: Lake, load_method: str):
        """INTEGER to BIGINT, which DuckLake changes in place."""
        small = {"type": ["null", "integer"], "minimum": 0, "maximum": 100}
        _run(lake, [_schema(_properties(small)), *_records([{"id": 7, "v": 5}])])
        _run(
            lake,
            [
                _schema(_properties(EARLIER["integer"][0])),
                *_records([{"id": 1, "v": 2**40}]),
            ],
            load_method=load_method,
        )
        earlier = [] if load_method == "overwrite" else [(7, "5")]
        assert _rows(lake) == [(1, str(2**40)), *earlier]
        assert lake.query(
            "SELECT data_type FROM information_schema.columns "
            "WHERE table_name = 'orders' AND column_name = 'v'"
        ) == [("BIGINT",)]

    def test_an_unsized_decimal_string_into_a_decimal_column(self, lake: Lake):
        """Staged at the column's own type, as before (see _staged_columns)."""
        sized = {"type": ["null", "number"], "precision": 10, "scale": 2}
        _run(lake, [_schema(_properties(sized)), *_records([{"id": 7, "v": 1.5}])])
        unsized = {"type": ["null", "string"], "format": "x-singer.decimal"}
        _run(lake, [_schema(_properties(unsized)), *_records([{"id": 1, "v": "2.25"}])])
        assert _rows(lake) == [(1, "2.25"), (7, "1.50")]
